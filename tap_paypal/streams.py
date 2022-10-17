"""Stream type classes for tap-paypal."""
import requests
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.helpers._util import utc_now

from datetime import timedelta
from tap_paypal.client import PaypalStream
from pendulum import parser


class InvoicesStream(PaypalStream):
    """Define custom stream."""
    name = "invoices"
    path = "/v2/invoicing/search-invoices"
    primary_keys = ["id"]
    replication_key = "create_time"
    rest_method = "POST"
    schema = th.PropertiesList(
        th.Property(
            "id",
            th.StringType,
        ),
        th.Property(
            "create_time",
            th.DateTimeType,
        )
    ).to_dict()


    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        """Prepare the data payload for the REST API request.

        By default, no payload will be sent (return None).
        """
        start = self.config.get("start_date", (utc_now() - timedelta(days=1)).strftime('%Y-%m-%d'))
        end = self.config.get("end_date", utc_now().strftime('%Y-%m-%d'))
        data = {
            "invoice_date_range": {
                "start": start,
                "end": end
            },
            "fields": [{"field": "items"}]
        }

        return data

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records."""
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        new_row  = {"id": row["id"], "create_time": row["detail"]["metadata"]["create_time"]}
        return new_row

