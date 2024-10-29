# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
# SPDX-License-Identifier: MPL-2.0
"""LDAP Connection handling."""

import asyncio
import signal
from collections import ChainMap
from contextlib import suppress
from ssl import CERT_NONE
from ssl import CERT_REQUIRED
from typing import Any
from uuid import UUID

import ldap3.core.exceptions
import structlog
from fastramqpi.context import Context
from fastramqpi.ramqp.utils import RequeueMessage
from jinja2 import Template
from ldap3 import ASYNC
from ldap3 import BASE
from ldap3 import NO_ATTRIBUTES
from ldap3 import NTLM
from ldap3 import RANDOM
from ldap3 import SIMPLE
from ldap3 import Connection
from ldap3 import Server
from ldap3 import ServerPool
from ldap3 import Tls
from ldap3 import set_config_parameter
from ldap3.core.exceptions import LDAPInvalidDnError
from ldap3.utils.dn import parse_dn
from ldap3.utils.dn import safe_dn
from more_itertools import always_iterable
from more_itertools import one
from more_itertools import only

from .config import AuthBackendEnum
from .config import ServerConfig
from .config import Settings
from .exceptions import MultipleObjectsReturnedException
from .exceptions import NoObjectsReturnedException
from .exceptions import TimeOutException
from .ldap_classes import LdapObject
from .processors import _hide_cpr as hide_cpr
from .types import DN
from .types import RDN
from .utils import combine_dn_strings
from .utils import ensure_list
from .utils import is_list

logger = structlog.stdlib.get_logger()


def construct_server(server_config: ServerConfig) -> Server:
    """Construct an LDAP3 server from settings.

    Args:
        server_config: The settings to construct the server instance from.

    Returns:
        The constructed server instance used for LDAP connections.
    """
    tls_configuration = Tls(
        validate=CERT_NONE if server_config.insecure else CERT_REQUIRED,
        ca_certs_data=server_config.ca_certs_data,
    )

    host = server_config.host
    logger.info("Setting up server", host=host)
    return Server(
        host=server_config.host,
        port=server_config.port,
        use_ssl=server_config.use_ssl,
        tls=tls_configuration,
        connect_timeout=server_config.timeout,
    )


def get_client_strategy():
    # NOTE: We probably want to use REUABLE, but it introduces issues with regards to
    #       presumed lazily fetching of the schema. See the comment in get_ldap_schema.
    return ASYNC


def construct_server_pool(settings: Settings) -> ServerPool:
    servers = list(map(construct_server, settings.ldap_controllers))
    # Pick the next server to use at random, retry connections 10 times,
    # discard non-active servers.
    server_pool = ServerPool(
        servers,
        RANDOM,
        active=10,  # type: ignore[arg-type]
        exhaust=True,
    )
    return server_pool


def configure_ldap_connection(settings: Settings) -> Connection:
    """Configure an LDAP connection.

    Args:
        settings: The Settings instance to configure our ad connection with.

    Returns:
        ContextManager that can be opened to establish an LDAP connection.
    """

    def alarm_handler(signum, frame):
        raise TimeOutException(
            "Timeout while configuring LDAP connection. Try 'sudo tailscale up'?"
        )

    # Set a timeout alarm
    signal.signal(signal.SIGALRM, alarm_handler)
    signal.alarm(max([c.timeout for c in settings.ldap_controllers]))

    server_pool = construct_server_pool(settings)
    client_strategy = get_client_strategy()

    logger.info(
        "Connecting to server",
        server_pool=server_pool,
        client_strategy=client_strategy,
        auth_strategy=settings.ldap_auth_method.value,
    )

    connection_kwargs = {
        "server": server_pool,
        "client_strategy": client_strategy,
        "password": settings.ldap_password.get_secret_value(),
        "auto_bind": True,
        # TODO: Raise exceptions whenever a query does not run OK
        # "raise_exceptions": True,
        # Configure non-blocking IO, with maximum time to wait for each reply
        "receive_timeout": settings.ldap_receive_timeout,
        # NOTE: It appears that this flag does not in fact work
        # See: https://github.com/cannatag/ldap3/issues/1008
        "read_only": settings.ldap_read_only,
    }
    set_config_parameter("RESPONSE_WAITING_TIMEOUT", settings.ldap_response_timeout)
    match settings.ldap_auth_method:
        case AuthBackendEnum.NTLM:
            connection_kwargs.update(
                {
                    "user": settings.ldap_domain + "\\" + settings.ldap_user,
                    "authentication": NTLM,
                }
            )
        case AuthBackendEnum.SIMPLE:
            connection_kwargs.update(
                {
                    "user": settings.ldap_user,
                    "authentication": SIMPLE,
                }
            )
        case _:
            # Turn off the alarm
            signal.alarm(0)
            raise ValueError("Unknown authentication backend")

    try:
        connection = Connection(**connection_kwargs)
    except ldap3.core.exceptions.LDAPBindError as exc:
        logger.exception("Exception during LDAP auth")
        raise exc
    finally:
        # Turn off the alarm
        signal.alarm(0)

    return connection


async def ldap_healthcheck(context: dict | Context) -> bool:
    """LDAP connection Healthcheck.

    Args:
        context: To lookup ldap_connection in.

    Returns:
        Whether the LDAP connection is OK.
    """
    logger.debug("Running LDAP healthcheck")
    ldap_connection = context["user_context"]["ldap_connection"]
    if ldap_connection.bound is False:
        logger.warning("LDAP connection not bound")
        return False
    if ldap_connection.listening is False:
        logger.warning("LDAP connection not listening")
        return False
    if ldap_connection.closed is True:
        logger.warning("LDAP connection not open")
        return False
    try:
        # Try to do a 'SELECT 1' like query, selecting the empty DN
        response, result = await ldap_search(
            ldap_connection,
            search_base="",
            search_filter="(objectclass=*)",
            attributes=NO_ATTRIBUTES,
            search_scope=BASE,
            size_limit=1,
        )
    except Exception:
        logger.exception("LDAP connection was unable to search")
        return False
    if result["type"] != "searchResDone":
        logger.warning(
            "LDAP connection search returned unexpected result type",
            response=response,
            result=result,
        )
        return False
    if result["description"] != "success":
        logger.warning(
            "LDAP connection did not search sucessfully",
            response=response,
            result=result,
        )
        return False
    logger.debug("LDAP healthcheck passed", response=response, result=result)
    return True


async def wait_for_message_id(
    ldap_connection: Connection, message_id: int
) -> tuple[Any, Any]:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, ldap_connection.get_response, message_id)


async def ldap_modify(
    ldap_connection: Connection, dn: DN, changes: dict
) -> tuple[dict, dict]:
    message_id = ldap_connection.modify(dn, changes)
    response, result = await wait_for_message_id(ldap_connection, message_id)
    # TODO: this does not currently raise exceptions on errors due to
    # `raise_exceptions=False` on the ldap connection.
    return response, result


async def ldap_modify_dn(
    ldap_connection: Connection, dn: DN, relative_dn: RDN
) -> tuple[dict, dict]:
    message_id = ldap_connection.modify_dn(dn, relative_dn)
    response, result = await wait_for_message_id(ldap_connection, message_id)
    # TODO: this does not currently raise exceptions on errors due to
    # `raise_exceptions=False` on the ldap connection.
    return response, result


async def ldap_add(
    ldap_connection: Connection, dn: DN, object_class, attributes=None
) -> tuple[dict, dict]:
    message_id = ldap_connection.add(dn, object_class, attributes)
    response, result = await wait_for_message_id(ldap_connection, message_id)
    # TODO: this does not currently raise exceptions on errors due to
    # `raise_exceptions=False` on the ldap connection.
    return response, result


async def ldap_delete(ldap_connection: Connection, dn: DN) -> tuple[dict, dict]:
    message_id = ldap_connection.delete(dn)
    response, result = await wait_for_message_id(ldap_connection, message_id)
    # TODO: this does not currently raise exceptions on errors due to
    # `raise_exceptions=False` on the ldap connection.
    return response, result


async def ldap_search(
    ldap_connection: Connection, **kwargs
) -> tuple[list[dict[str, Any]], dict]:
    message_id = ldap_connection.search(**kwargs)
    response, result = await wait_for_message_id(ldap_connection, message_id)
    # TODO: this does not currently raise exceptions on errors due to
    # `raise_exceptions=False` on the ldap connection.
    return response, result


def get_ldap_schema(ldap_connection: Connection):
    # On OpenLDAP this returns a ldap3.protocol.rfc4512.SchemaInfo
    schema = ldap_connection.server.schema
    # NOTE: The schema seems sometimes be unbound here if we use the REUSABLE async
    #       strategy. I think it is because the connections are lazy in that case, and
    #       as such the schema is only fetched on the first operation.
    #       In this case we would probably have to asynchronously fetch the schema info,
    #       but the documentation provides slim to no information on how to do so.
    assert schema is not None
    return schema


def get_ldap_object_schema(ldap_connection: Connection, ldap_object: str):
    schema = get_ldap_schema(ldap_connection)
    return schema.object_classes[ldap_object]


def get_ldap_superiors(ldap_connection: Connection, root_ldap_object: str) -> list:
    object_schema = get_ldap_object_schema(ldap_connection, root_ldap_object)
    ldap_objects = list(always_iterable(object_schema.superior))
    superiors = []
    for ldap_object in ldap_objects:
        superiors.append(ldap_object)
        superiors.extend(get_ldap_superiors(ldap_connection, ldap_object))
    return superiors


def get_ldap_attributes(ldap_connection: Connection, root_ldap_object: str):
    """
    ldap_connection : ldap connection object
    ldap_object : ldap class to fetch attributes for. for example "organizationalPerson"
    """

    all_attributes = []
    superiors = get_ldap_superiors(ldap_connection, root_ldap_object)

    for ldap_object in [root_ldap_object] + superiors:
        object_schema = get_ldap_object_schema(ldap_connection, ldap_object)
        all_attributes += object_schema.may_contain
    return all_attributes


async def apply_discriminator(
    settings: Settings, ldap_connection: Connection, dns: set[DN]
) -> DN | None:
    """Find the account to synchronize from a set of DNs.

    The DNs are evaluated depending on the configuration of the discriminator.

    Args:
        dns: The set of DNs to evaluate.

    Raises:
        RequeueMessage: If the provided DNs could not be read from LDAP.
        ValueError: If too many or too few LDAP accounts are found.

    Returns:
        The account to synchronize (if any).
    """
    assert isinstance(dns, set)

    # Empty input-set means we have no accounts to consider
    if not dns:
        return None

    discriminator_field = settings.discriminator_field
    # If discriminator is not configured, there can be only one user
    if discriminator_field is None:
        return one(dns)

    # These settings must be set for the function to work
    # This should always be the case, as they are enforced by pydantic
    # But no guarantees are given as pydantic is lenient with run validators
    assert settings.discriminator_function is not None
    assert settings.discriminator_values != []

    # Fetch the discriminator attribute for all DNs
    # NOTE: While it is possible to fetch multiple DNs in a single operation
    #       (by doing a complex search operation), some "guy on the internet" claims
    #       that it is better to lookup DNs individually using the READ operation.
    #       See: https://stackoverflow.com/a/58834059
    try:
        attributes = [discriminator_field]
        ldap_objects = await asyncio.gather(
            *[get_ldap_object(ldap_connection, dn, attributes=attributes) for dn in dns]
        )
    except NoObjectsReturnedException as exc:
        # There could be multiple reasons why our DNs cannot be read.
        # * The DNs could have been found by CPR number and changed since then.
        #
        # In this case, we wish to retry the message, so we can refetch by CPR.
        #
        # * The DNs could have been found by ITUsers and those could be wrong in MO
        #
        # In this case, we wish to retry the message until someone has fixed the
        # problem in MO itself, and thus we will be retrying for a long time, likely
        # raising an alarm due to messages not being processed, and thus ensuring that
        # someone will look into the issue.
        raise RequeueMessage("Unable to lookup DN(s)") from exc

    def ldapobject2discriminator(ldap_object: LdapObject) -> str | None:
        # The value can either be a string or a list
        value = getattr(ldap_object, discriminator_field)
        # TODO: Figure out when it is a string instead of a list
        #       Maybe it is an AD only thing?
        if isinstance(value, str):  # pragma: no cover
            return value
        # If it is a list, we assume it is
        unpacked_value = only(value)
        if unpacked_value is None:
            logger.debug("Discriminator value is None", dn=ldap_object.dn)
            return None
        assert isinstance(unpacked_value, str)
        return unpacked_value

    mapping: dict[DN, str | None] = {
        ldap_object.dn: ldapobject2discriminator(ldap_object)
        for ldap_object in ldap_objects
    }
    assert dns == set(mapping.keys())

    discriminator_values = settings.discriminator_values
    # If the discriminator_function is exclude, discriminator_values will be a
    # list of disallowed values, and we will want to find an account that does not
    # have any of these disallowed values whatsoever.
    # NOTE: We assume that at most one such account exists.
    if settings.discriminator_function == "exclude":
        return only(
            {
                dn
                for dn, value in mapping.items()
                if str(value) not in discriminator_values or value is None
            }
        )

    if settings.discriminator_function == "include":
        # If the discriminator_function is include, discriminator_values will be a
        # prioritized list of values (first meaning most important), and we will want
        # to find the best (most important) account.
        # NOTE: We assume that no two accounts are equally important.
        # This is implemented using our template system below, so we simply wrap our
        # values into simple jinja-templates.
        discriminator_values = [
            '{{ value == "' + str(dn_value) + '" }}'
            for dn_value in discriminator_values
        ]

    assert settings.discriminator_function in ["include", "template"]
    # If the discriminator_function is template, discriminator values will be a
    # prioritized list of jinja templates (first meaning most important), and we will
    # want to find the best (most important) account.
    # We do this by evaluating the jinja template and looking for outcomes with "True".
    # NOTE: We assume no two accounts are equally important.
    for discriminator in discriminator_values:
        template = Template(discriminator)
        dns_passing_template = {
            dn
            for dn, dn_value in mapping.items()
            if template.render(dn=dn, value=dn_value).strip() == "True"
        }
        if dns_passing_template:
            return one(dns_passing_template)

    return None


def ldapresponse2entries(ldap_response: list[dict[str, Any]]) -> list[dict[str, Any]]:
    # TODO: Handle other response types
    # See: https://ldap3.readthedocs.io/en/latest/searches.html#response
    return [entry for entry in ldap_response if entry["type"] == "searchResEntry"]


async def _paged_search(
    ldap_connection: Connection,
    searchParameters: dict,
    search_base: str,
    mute: bool,
) -> list:
    # TODO: Consider using upstream paged_search_generator instead of this?
    # TODO: Eliminate mute argument? - Should be logger configuration?
    # TODO: Find max. paged_size number from LDAP rather than hard-code it?
    searchParameters["paged_size"] = 500
    searchParameters["search_base"] = search_base

    search_filter = searchParameters["search_filter"]

    if not mute:
        logger.info(
            "Executing paged_search",
            search_filter=search_filter,
            search_base=search_base,
        )

    # Max 10_000 pages to avoid eternal loops
    # TODO: Why would we get eternal loops?
    responses = []
    for page in range(0, 10_000):
        if not mute:
            logger.info("Searching page", page=page)
        # TODO: Fetch multiple pages in parallel using asyncio.gather?
        response, result = await ldap_search(ldap_connection, **searchParameters)

        if result["description"] == "operationsError":
            # TODO: Should this be an exception?
            #       Currently we just return half the result?
            logger.warn(
                "Search failed",
                search_filter=search_filter,
                result=result,
            )
            break

        # TODO: Handle this error more gracefully
        assert response is not None
        entries = ldapresponse2entries(response)
        responses.extend(entries)

        try:
            # TODO: Skal "1.2.840.113556.1.4.319" være Configurerbar?
            extension = "1.2.840.113556.1.4.319"
            cookie = result["controls"][extension]["value"]["cookie"]
        except KeyError:
            break

        if cookie and isinstance(cookie, bytes):
            searchParameters["paged_cookie"] = cookie
        else:
            break

    return responses


async def paged_search(
    settings: Settings,
    ldap_connection: Connection,
    searchParameters: dict,
    search_base: str | None = None,
    mute: bool = False,
) -> list:
    """
    Execute a search on the LDAP server.

    Args:
        context: The FastRAMQPI context.
        searchParameters:
            Dict with the following keys:
                * search_filter
                * attributes
        search_base:
            Search base to search in.
            If empty, uses settings.search_base combined with settings.ous_to_search_in.
        mute: Whether to log process information

    Returns:
        A list of search results.
    """
    # NOTE: It seems like this function is purely used for manual endpoints
    #       Except from a single call from usernames.py
    # TODO: Consider moving this to its own module separate from business logic
    # TODO: Make a class for the searchParameters if it has a fixed format?

    if search_base:
        # If the search base is explicitly defined: Don't try anything fancy.
        results = await _paged_search(
            ldap_connection, searchParameters, search_base, mute
        )
        return results

    # Otherwise, loop over all OUs to search in
    search_bases = [
        combine_dn_strings([ou, settings.ldap_search_base])
        for ou in settings.ldap_ous_to_search_in
    ]
    results = []
    for search_base in search_bases:
        results.extend(
            await _paged_search(
                ldap_connection, searchParameters.copy(), search_base, mute
            )
        )

    return results


async def object_search(
    searchParameters: dict[str, Any], ldap_connection: Connection
) -> list[dict[str, Any]]:
    """Performs an LDAP search and return the result.

    Notes:
        If you want to be 100% sure that the search only returns one result;
        Supply an object's dn (distinguished name) as the search base and set
        searchFilter = "(objectclass=*)" and search_scope = BASE

    Args:
        searchParameters:
            Dictionary with the following keys:
                * search_base
                * search_filter
                * attributes
                * see https://ldap3.readthedocs.io/en/latest/searches.html for more keys
        ldap_connection: The LDAP Connection to run our search on.

    Returns:
        A list of found objects.
    """
    search_bases = ensure_list(searchParameters["search_base"])

    responses = []
    # TODO: Asyncio.gather this? - or combine the filters?
    for search_base in search_bases:
        response, _ = await ldap_search(
            ldap_connection, **ChainMap(searchParameters, {"search_base": search_base})
        )
        if response:
            responses.extend(response)
    search_entries = ldapresponse2entries(responses)
    return search_entries


async def single_object_search(
    searchParameters: dict[str, Any],
    ldap_connection: Connection,
) -> dict[str, Any]:
    """Performs an LDAP search and ensure that it returns one result.

    Notes:
        If you want to be 100% sure that the search only returns one result;
        Supply an object's dn (distinguished name) as the search base and set
        searchFilter = "(objectclass=*)" and search_scope = BASE

    Args:
        searchParameters:
            Dictionary with the following keys:
                * search_base
                * search_filter
                * attributes
                * see https://ldap3.readthedocs.io/en/latest/searches.html for more keys
        context: The FastRAMQPI context.

    Raises:
        MultipleObjectsReturnedException: If multiple objects were found.
        NoObjectsReturnedException: If no objects were found.

    Returns:
        The found object.
    """
    search_entries = await object_search(searchParameters, ldap_connection)

    too_long_exception = MultipleObjectsReturnedException(
        hide_cpr(f"Found multiple entries for {searchParameters}: {search_entries}")
    )
    too_short_exception = NoObjectsReturnedException(
        hide_cpr(f"Found no entries for {searchParameters}")
    )
    return one(
        search_entries, too_short=too_short_exception, too_long=too_long_exception
    )


def is_dn(value):
    """
    Determine if a value is a dn (distinguished name) string
    """
    if not isinstance(value, str):
        return False

    try:
        safe_dn(value)
        parse_dn(value)
    except LDAPInvalidDnError:
        return False
    return True


async def get_ldap_object(
    ldap_connection: Connection,
    dn: DN,
    attributes: list | None = None,
    nest: bool = True,
) -> LdapObject:
    """Gets a ldap object based on its DN.

    Args:
        dn: The DN to read.
        context: The FastRAMQPI context.
        nest: Whether to also fetch and nest related objects.
        attributes: The list of attributes to read.

    Returns:
        The LDAP object fetched from the LDAP server.
    """
    if attributes is None:
        attributes = ["*"]

    searchParameters = {
        "search_base": dn,
        "search_filter": "(objectclass=*)",
        "attributes": attributes,
        "search_scope": BASE,
    }
    search_result = await single_object_search(searchParameters, ldap_connection)
    dn = search_result["dn"]
    logger.info("Found DN", dn=dn)
    return await make_ldap_object(search_result, ldap_connection, nest=nest)


async def make_ldap_object(
    response: dict, ldap_connection: Connection, nest: bool = True
) -> LdapObject:
    """Takes an LDAP response and formats it as an LdapObject.

    Args:
        response: The LDAP response.
        context: The FastRAMQPI context.
        nest: Whether to also fetch and nest related objects.

    Returns:
        The LDAP object constructed from the response.
    """
    attributes = sorted(list(response["attributes"].keys()))
    ldap_dict = {"dn": response["dn"]}

    async def get_nested_ldap_object(dn):
        """
        Gets a ldap object based on its DN - unless we are in a nested loop
        """

        if nest:
            logger.info("Loading nested ldap object", dn=dn)
            return await get_ldap_object(ldap_connection, dn, nest=False)
        raise Exception("Already running in nested loop")  # pragma: no cover

    def is_other_dn(value):
        """
        Determine if the value is a dn (distinguished name)
        But not the dn of the main object itself

        This is to avoid that the code requests information about itself
        """
        return is_dn(value) and value != response["dn"]

    for attribute in attributes:
        value = response["attributes"][attribute]
        if is_other_dn(value) and nest:
            ldap_dict[attribute] = await get_nested_ldap_object(value)
        elif is_list(value):
            ldap_dict[attribute] = [
                (await get_nested_ldap_object(v)) if is_other_dn(v) and nest else v
                for v in value
            ]
        else:
            ldap_dict[attribute] = value

    return LdapObject(**ldap_dict)


def get_attribute_types(ldap_connection: Connection):
    """
    Returns a dictionary with attribute type information for all attributes in LDAP
    """
    # On OpenLDAP this returns a ldap3.utils.ciDict.CaseInsensitiveWithAliasDict
    # Mapping from str to ldap3.protocol.rfc4512.AttributeTypeInfo
    schema = get_ldap_schema(ldap_connection)
    return schema.attribute_types


def is_uuid(entity: Any) -> bool:
    """
    Check if a entity is a valid UUID
    """
    with suppress(ValueError):
        UUID(str(entity))
        return True
    return False


def check_ou_in_list_of_ous(ou_to_check, list_of_ous):
    """
    Checks if a specific OU exists in a list of OUs. Raises ValueError if it does not
    """
    any_ok = any(ou_to_check.endswith(ou) for ou in list_of_ous)
    if not any_ok:
        raise ValueError(f"{ou_to_check} is not in {list_of_ous}")
