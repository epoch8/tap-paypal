"""REST client handling, including paypalStream base class."""

import requests
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from memoization import cached

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream

from tap_paypal.auth import PaypalAuthenticator


SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class PaypalStream(RESTStream):
    """paypal stream class."""

    url_base = "https://api-m.paypal.com"
    records_jsonpath = "$.items.[*]"  # Or override `parse_response`.
    page_size = 100

    @property
    @cached
    def authenticator(self) -> PaypalAuthenticator:
        auth_endpoint = self.url_base + "/v1/oauth2/token"
        return PaypalAuthenticator.create_for_stream(stream=self, auth_endpoint=auth_endpoint)

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        response = response.json()

        if "items" not in response:
                return None
        if len(response["items"]) < self.page_size:
            return None

        if not previous_token:
            previous_token = 1

        next_page_token = previous_token + 1
        return next_page_token

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        params["page_size"] = self.page_size
        if next_page_token:
            params["page"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key
        return params

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        """Prepare the data payload for the REST API request.

        By default, no payload will be sent (return None).
        """
        # TODO: Delete this method if no payload is required. (Most REST APIs.)
        return None

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records."""
        # TODO: Parse response body and return a set of records.
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        # TODO: Delete this method if not needed.
        return row
