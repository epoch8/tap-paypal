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

URL = str

class InvoicesStream(PaypalStream):
    """Define custom stream."""
    name = "invoices"
    path = "/v2/invoicing/search-invoices"
    primary_keys = ["invoice_id", "item_name"]
    replication_key = "updated_at"
    rest_method = "POST"
    schema = th.PropertiesList(
        th.Property(
            "invoice_id",
            th.StringType,
        ),
        th.Property(
            "invoice_date",
            th.DateTimeType,
        ),
        th.Property(
            "updated_at",
            th.DateTimeType
        ),
        th.Property(
            "email",
            th.StringType,
        ),
        th.Property(
            "invoice_number",
            th.StringType,
        ),
        th.Property(
            "item_name",
            th.StringType,
        ),
        th.Property(
            "item_qty",
            th.StringType,
        ),
        th.Property(
            "item_total",
            th.StringType,
        ),
        th.Property(
            "item_unit_price",
            th.StringType,
        ),
        th.Property(
            "refund_amount",
            th.StringType,
        ),
        th.Property(
            "name",
            th.StringType,
        ),
        th.Property(
            "status",
            th.StringType,
        ),
        th.Property(
            "terms_note",
            th.StringType,
        ),
        th.Property(
            "refund_amount",
            th.StringType,
        ),
        th.Property(
            "total_invoice",
            th.StringType,
        ),
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
        records = extract_jsonpath(self.records_jsonpath, input=response.json())
        filtered_record = [record for record in records if record['status'] != "DRAFT"]
        flatten_and_detailed_records = []
        for record in filtered_record:
            detailed_invoice = self.get_invoice_detail(record["links"][0]["href"])
            try:
                flatten_and_detailed_records += self.prepare_invoice_rows(detailed_invoice)
            except:
                continue
        yield from flatten_and_detailed_records


    def get_invoice_detail(self, detail_url: URL):
        r = requests.get(detail_url, headers={"Authorization": "Bearer " + self.authenticator.access_token})
        return r.json()

    @staticmethod
    def prepare_invoice_rows(invoice_data):
        rows = []
        invoice_info = {}
        invoice_info["invoice_date"] = invoice_data["detail"]["invoice_date"]
        invoice_info["invoice_id"] = invoice_data["id"]
        invoice_info["status"] = invoice_data["status"]
        invoice_info["updated_at"] = invoice_data['detail']["metadata"]["last_update_time"]
        try:
            invoice_info["email"] = invoice_data["primary_recipients"][0]["billing_info"]["email_address"]
        except KeyError:
            invoice_info["email"] = ""
        try:
            invoice_info["name"] = invoice_data["primary_recipients"][0]["billing_info"]["name"]["full_name"]
        except KeyError:
            invoice_info["name"] = ""
        invoice_info["invoice_number"] = invoice_data["detail"]["invoice_number"]
        invoice_info["item_name"] = ""
        invoice_info["item_qty"] = ""
        invoice_info["item_unit_price"] = None
        try:
            invoice_info["item_total"] = invoice_data["amount"]["breakdown"]["item_total"]["value"]
        except KeyError:
            invoice_info["item_total"] = 0
        invoice_info["refund_amount"] = None
        invoice_info["total_invoice"] = invoice_data["amount"]["value"]
        invoice_info["terms_note"] = invoice_data["detail"].get("note", "")

        rows.append(invoice_info)
        for line_item in invoice_data['items']:
            new_invoice_info = invoice_info.copy()
            new_invoice_info["item_name"] = line_item["name"]
            new_invoice_info["item_qty"] = int(line_item["quantity"])
            new_invoice_info["item_unit_price"] = float(line_item["unit_amount"]["value"])
            new_invoice_info["item_total"] = new_invoice_info["item_qty"] * new_invoice_info["item_unit_price"]
            new_invoice_info["total_invoice"] = None
            rows.append(new_invoice_info)

        if "refunds" in invoice_data:
            new_invoice_info = invoice_info.copy()
            new_invoice_info["item_name"] = "refund"
            new_invoice_info["refund_amount"] = float(invoice_data["refunds"]["refund_amount"]["value"])
            new_invoice_info["total_invoice"] = None
            rows.append(new_invoice_info)
        return rows

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        return row
    