# OS2mo LDAP Import/Export

Integration for bi-directional synchronization of data between [OS2mo](https://os2.eu/produkt/os2mo) and LDAP / Active Directory.

The integration listens for changes on both sides and applies them to the other, translating objects through user-configurable [Jinja](https://jinja.palletsprojects.com/) templates.

## Getting started

The repository ships with a `docker compose` stack that builds the integration from source and brings up the services it needs to talk to (OpenLDAP / Samba AD DC, and a Postgres database for FastRAMQPI):

```sh
docker compose up --build --detach
```

The integration listens on `127.0.0.1:8000`. The default settings in `docker-compose.yml` point it at the bundled OpenLDAP server.

The compose stack does not include OS2mo itself; the `mo_ldap_import_export` service joins the external `os2mo_default` network and expects an OS2mo stack (with FastRAMQPI, Keycloak, and an AMQP broker) reachable on that network. To exercise the integration against an Active Directory test instance, see [`docker-compose.override.addev.yml`](docker-compose.override.addev.yml).

## Configuration

Settings are supplied as environment variables on the `mo_ldap_import_export` service. The full schema with all defaults and validation rules lives in [`mo_ldap_import_export/config.py`](mo_ldap_import_export/config.py); the most commonly set variables are listed below.

### LDAP connection

| Variable | Description |
| --- | --- |
| `LDAP_CONTROLLERS` | JSON list with a single domain controller, e.g. `[{"host": "ldap.example.org", "use_ssl": true}]`. See `ServerConfig` in `config.py` for all per-host fields. |
| `LDAP_DOMAIN` | Domain used when authenticating with the domain controller. |
| `LDAP_USER` | Username used when authenticating (default `os2mo`). May be a DN for `simple` auth or a username for `ntlm`. |
| `LDAP_PASSWORD` | Password for the bind user. |
| `LDAP_AUTH_METHOD` | `ntlm` (default) or `simple`. |
| `LDAP_DIALECT` | `AD` (default) or `Standard`. Drives defaults for `LDAP_UNIQUE_ID_FIELD` and `LDAP_USER_OBJECTCLASS`. |

### Synchronization scope

| Variable | Description |
| --- | --- |
| `LDAP_SEARCH_BASE` | Search base used for all LDAP requests. |
| `LDAP_OUS_TO_SEARCH_IN` | List of OUs to read from. A list containing an empty string means "all OUs in the search base" (default). |
| `LDAP_OUS_TO_WRITE_TO` | List of OUs to write to. Same empty-string semantics as above. |
| `LDAP_OU_FOR_NEW_USERS` | OU in which newly created users are placed. |
| `LDAP_OBJECT_CLASS` | LDAP object class that holds the CPR number. |
| `LDAP_CPR_ATTRIBUTE` | LDAP attribute holding the CPR number. |
| `LDAP_IT_SYSTEM` | User-key of the MO IT-system used to correlate MO users with LDAP objects. At least one of `LDAP_CPR_ATTRIBUTE` and `LDAP_IT_SYSTEM` must be set. |
| `LISTEN_TO_CHANGES_IN_MO` | Whether to write to LDAP when MO changes (default `True`). |
| `LISTEN_TO_CHANGES_IN_LDAP` | Whether to write to MO when LDAP changes (default `True`). |
| `ADD_OBJECTS_TO_LDAP` | If `False`, only modify existing LDAP objects instead of creating new ones (default `True`). |

### What to synchronize

| Variable | Description |
| --- | --- |
| `CONVERSION_MAPPING` | Required. JSON describing the `ldap_to_mo`, `mo_to_ldap`, and `username_generator` mappings; field values are Jinja templates. See `ConversionMapping` in `config.py` and the override files under the repository root for examples. |

### FastRAMQPI / OS2mo

These are settings of the [FastRAMQPI](https://pypi.org/project/fastramqpi/) library; the integration forwards them as-is.

| Variable | Description |
| --- | --- |
| `FASTRAMQPI__MO_URL` | URL of the OS2mo instance. |
| `FASTRAMQPI__AMQP__URL` | AMQP broker URL. |
| `FASTRAMQPI__AUTH_SERVER` | Keycloak auth server URL. |
| `FASTRAMQPI__CLIENT_ID` / `FASTRAMQPI__CLIENT_SECRET` | OS2mo client credentials. |
| `FASTRAMQPI__DATABASE__USER` / `__PASSWORD` / `__HOST` / `__NAME` | Postgres connection used by FastRAMQPI. |

## Persistence

The LDAP side of the integration is polling-based, not event-driven: the integration periodically scans LDAP for entries modified since its last poll. To know "since when" across restarts, it persists a small amount of bookkeeping in the Postgres database configured via `FASTRAMQPI__DATABASE__*`.

A single table `last_run_gregorian` holds one row per LDAP search base, recording the timestamp of the last poll and the LDAP UUIDs already seen at that timestamp. No business data from OS2mo or LDAP is persisted; the table contains only polling state. FastRAMQPI itself may create additional tables in the same database for its own use.

## Full synchronization

When the integration is first deployed, or when a "full" sync is needed for other reasons (such as a changed configuration), the two directions must be triggered separately.

### LDAP to OS2mo

Call the integration's sync endpoint:

```sh
curl -X POST http://localhost:8000/sync/ldap2mo
```

The endpoint clears the LDAP poller's bookkeeping in `last_run_gregorian`, so the next poll treats every entry in the configured search bases as new and emits events for them.

To trigger a sync for a single LDAP UUID instead of a full resync, see [HTTP API](#http-api) below.

### OS2mo to LDAP

Call the integration's sync endpoint:

```sh
curl -X POST http://localhost:8000/sync/mo2ldap
```

The endpoint inspects the integration's MO event listeners (which are derived from `CONVERSION_MAPPING.mo_to_ldap` together with the static listeners registered at startup) and fires the matching `*_refresh_all` mutator on OS2mo for each one. Only the routing keys this integration actually listens on are refreshed.

## HTTP API

The integration is a FastAPI application; interactive OpenAPI documentation is available at `/docs`.

It does not subscribe to OS2mo's event system itself. Instead, FastRAMQPI runs as a separate bridge that consumes events from OS2mo and translates each one into an HTTP request against the integration's API. The integration is therefore best understood as the HTTP-callable handler for those events; FastRAMQPI handles delivery, retries, and acknowledgement.

The endpoints fall into three groups:

| Group | Endpoints | Caller |
| --- | --- | --- |
| MO to LDAP event handlers | `POST /mo2ldap/{address,engagement,ituser,person,org_unit,reconcile}`, `POST /mo_to_ldap/{identifier}` (dynamic, one per entry in `CONVERSION_MAPPING.mo_to_ldap`) | FastRAMQPI, on MO events |
| LDAP to MO event handlers | `POST /ldap2mo/uuid`, `POST /ldap2mo/reconcile` | FastRAMQPI, on events emitted by the integration's LDAP poller |
| Operational endpoints | `POST /sync/ldap2mo` (full LDAP to OS2mo resync), `POST /sync/mo2ldap` (full OS2mo to LDAP resync), `POST /ldap_event_generator/emit/{uuid}` (manually emit a single LDAP UUID), `GET /ldap_event_generator/{since}` (changes since a timestamp), `GET /Inspect/...` (read-only debugging: `dn2uuid`, `uuid2dn`, `mo2ldap`, `ldap2mo`, `overview`, ...) | Human operators |

See `mo_ldap_import_export/main.py`, `mo_ldap_import_export/ldap_amqp.py`, and `mo_ldap_import_export/routes.py` for the full list.

## Development

See [AGENTS.md](AGENTS.md) for the project's architecture, how to run tests, and contribution conventions.

## License

This project is licensed under [MPL-2.0](LICENSES/MPL-2.0.txt).
