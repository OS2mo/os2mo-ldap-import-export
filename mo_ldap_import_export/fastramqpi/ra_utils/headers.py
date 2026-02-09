# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import time
from functools import lru_cache
from typing import Any
from typing import Dict
from typing import Optional
from typing import Tuple
from warnings import warn

import requests
from pydantic import AnyHttpUrl
from pydantic import BaseSettings
from pydantic import Field
from pydantic import parse_obj_as
from pydantic import root_validator


# Exception
class AuthError(Exception):
    """Raised when errors in authentication occurs."""


# Settings


class TokenSettings(BaseSettings):
    """Collection of settings required for authentication against OS2mo.

    Example:
        ```Python
        import requests
        from fastramqpi.ra_utils.headers import TokenSettings

        session = requests.Session()
        session.headers = TokenSettings().get_headers()
        response = session.get("https://moratest.magenta.dk/service/o/")
        response.raise_for_status()
        print(response.json())
        ```
    """

    client_id: str = "mo"
    client_secret: Optional[str]  # in the future, this should be required
    auth_realm: str = "mo"
    auth_server: AnyHttpUrl = Field(
        parse_obj_as(AnyHttpUrl, "http://localhost:8081/auth")
    )
    saml_token: Optional[str]  # deprecate when fully on keycloak?

    # Re-new token this many seconds before it actually expires
    oidc_token_lifespan_offset: int = 30

    class Config:
        frozen = True

    @root_validator
    def validate_settings(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """Validate token settings by checking that either `client_secret`,
        `saml_token`, or both exist.

        This validation occurs when `TokenSettings` are initialized and does
        not mutate values.

        Args:
            values: Initialized `TokenSettings` values.

        Raises:
            UserWarning: If none of `CLIENT_SECRET` or `SAML_TOKEN` are given
                during initialisation.
            PendingDeprecationWarning: If `SAML_TOKEN` is used.

        Returns:
            `TokenSettings` values, unmodified.
        """

        keycloak, saml = values.get("client_secret"), values.get("saml_token")
        if not any([keycloak, saml]):
            warn("No secret or token given", stacklevel=2)
        if saml:
            warn(
                "Using SAML tokens will be deprecated",
                PendingDeprecationWarning,
                stacklevel=2,
            )
        return values

    @lru_cache(maxsize=None)
    def _fetch_keycloak_token(self) -> Tuple[float, str]:
        """Fetch a keycloak token and its expiry time.

        Raises:
            AuthError: If no client secret is given or the response from
                the authentication server raises an error.

        Returns:
            Tuple of token-expiry time in seconds and the token itself.
        """
        token_url = (
            f"{self.auth_server}/realms/{self.auth_realm}/protocol/openid-connect/token"
        )
        if self.client_secret is None:
            raise AuthError("No client secret given")
        payload: Dict[str, str] = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        try:
            response = requests.post(token_url, data=payload)
            response.raise_for_status()
        except requests.RequestException as err:
            raise AuthError(f"Failed to get Keycloak token: {err}")
        response_payload: Dict[str, Any] = response.json()
        expires: int = response_payload["expires_in"]
        token: str = response_payload["access_token"]
        return time.monotonic() + float(expires), token

    def _fetch_bearer(self, force: bool = False, logger: Any = None) -> str:
        """Fetch a Keycloak bearer token.

        Automatically refetches the token after it expires.

        Args:
            force: always refresh token if true
            logger: logger used for logging token refresh info

        Raises:
            AuthError: If no client secret is given or the response from
                the authentication server raises an error.

        Returns:
            The Bearer token itself.
        """
        expires, token = self._fetch_keycloak_token()
        if force or expires - self.oidc_token_lifespan_offset < time.monotonic():
            self._fetch_keycloak_token.cache_clear()
            expires, token = self._fetch_keycloak_token()
            if logger:
                logger.debug("New token fetched", expires=expires, token=token)
        return "Bearer " + token

    def get_headers(self, force: bool = False, logger: Any = None) -> Dict[str, str]:
        """Get authorization headers based on configured tokens.

        If both a client secret and a SAML token are configured,
        they will both exist in the headers dict, with their respective keys:

        * `Authorization: Bearer ${TOKEN}`, and
        * `Session: ${TOKEN}`

        Args:
            force: always refresh token if true
            logger: logger used for logging token refresh info

        Raises:
            AuthError: If `client_secret` is given, but the response from the
                authentication server raises an error.

        Returns:
            Header dictionary.
        """
        headers: Dict[str, str] = {}
        if self.saml_token:
            headers["Session"] = self.saml_token
        if self.client_secret:
            headers["Authorization"] = self._fetch_bearer(force, logger)
        return headers
