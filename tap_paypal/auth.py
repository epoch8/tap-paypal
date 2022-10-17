"""paypal Authentication."""

import requests
from singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta
from singer_sdk.helpers._util import utc_now


class PaypalAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """Authenticator class for paypal."""

    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the paypal API."""
        # TODO: Define the request body needed for the API.
        return {
            'client_id': self.config["client_id"],
            'client_secret': self.config["client_secret"],
            'grant_type': 'client_credentials',
        }

    def update_access_token(self) -> None:
        """Update `access_token` along with: `last_refreshed` and `expires_in`.

        Raises:
            RuntimeError: When OAuth login fails.
        """
        request_time = utc_now()
        auth_request_payload = self.oauth_request_payload
        client_id = auth_request_payload.pop("client_id")
        client_secret = auth_request_payload.pop("client_secret")
        token_response = requests.post(self.auth_endpoint, data=auth_request_payload, auth=(client_id, client_secret))
        try:
            token_response.raise_for_status()
            self.logger.info("OAuth authorization attempt was successful.")
        except Exception as ex:
            raise RuntimeError(
                f"Failed OAuth login, response was '{token_response.json()}'. {ex}"
            )
        token_json = token_response.json()
        self.access_token = token_json["access_token"]
        self.expires_in = token_json.get("expires_in", self._default_expiration)
        if self.expires_in is None:
            self.logger.debug(
                "No expires_in receied in OAuth response and no "
                "default_expiration set. Token will be treated as if it never "
                "expires."
            )
        self.last_refreshed = request_time

    @classmethod
    def create_for_stream(cls, stream, auth_endpoint) -> "PaypalAuthenticator":
        return cls(
            stream=stream,
            auth_endpoint=auth_endpoint,
        )
