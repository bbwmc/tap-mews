"""Custom HTTP client for Mews API.

Mews API uses POST requests with authentication tokens in the request body,
and cursor-based pagination via the Limitation object.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import requests
from singer_sdk.streams import RESTStream

if TYPE_CHECKING:
    from singer_sdk import Tap


class MewsStream(RESTStream):
    """Base stream class for Mews API endpoints.

    All Mews API endpoints:
    - Use POST method with JSON body
    - Require ClientToken, AccessToken, and Client in request body
    - Use cursor-based pagination via Limitation object
    """

    rest_method = "POST"
    records_jsonpath = "$[*]"  # Override in subclasses based on response structure
    page_size = 1000

    # Override in child streams that need ServiceIds
    requires_service_id: bool = False

    @property
    def url_base(self) -> str:
        """Return the base URL for the API."""
        return f"{self.config.get('api_url', 'https://api.mews.com')}/api/connector/v1"

    @property
    def http_headers(self) -> dict[str, str]:
        """Return headers for HTTP requests."""
        return {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def prepare_request_payload(
        self,
        context: dict | None,
        next_page_token: str | None,
    ) -> dict | None:
        """Prepare the request payload for Mews API.

        Args:
            context: Stream partition context (contains parent data like service_id).
            next_page_token: Pagination cursor from previous response.

        Returns:
            Request body dictionary.
        """
        body: dict[str, Any] = {
            "ClientToken": self.config["client_token"],
            "AccessToken": self.config["access_token"],
            "Client": self.config.get("client_name", "BBGMeltano 1.0.0"),
            "Limitation": {
                "Count": self.page_size,
            },
        }

        if next_page_token:
            body["Limitation"]["Cursor"] = next_page_token

        # Add ServiceIds if this stream requires it and context has service_id
        if self.requires_service_id and context and "service_id" in context:
            body["ServiceIds"] = [context["service_id"]]

        return body

    def get_next_page_token(
        self,
        response: requests.Response,
        previous_token: str | None,
    ) -> str | None:
        """Extract cursor for next page from response.

        Args:
            response: The HTTP response object.
            previous_token: The previous pagination token.

        Returns:
            Next page cursor or None if no more pages.
        """
        data = response.json()
        return data.get("Cursor")

    def parse_response(self, response: requests.Response) -> list[dict]:
        """Parse the response and yield records.

        Override this in subclasses to specify which key contains the records.

        Args:
            response: The HTTP response object.

        Yields:
            Parsed record dictionaries.
        """
        data = response.json()
        records_key = getattr(self, "records_key", None)
        if records_key and records_key in data:
            yield from data[records_key]
        else:
            # If no records_key specified, try common patterns
            for key in data:
                if isinstance(data[key], list):
                    yield from data[key]
                    break

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: str | None,
    ) -> dict[str, Any]:
        """Return URL query parameters (empty for Mews, uses POST body)."""
        return {}


class MewsChildStream(MewsStream):
    """Base class for streams that depend on a parent service stream.

    Child streams are partitioned by service_id and require ServiceIds
    in the request body.
    """

    requires_service_id = True
    parent_stream_type: type[MewsStream] | None = None  # Set in subclass

    @property
    def partitions(self) -> list[dict] | None:
        """Return partitions for this stream based on parent services.

        Returns None to let the SDK handle partitioning via parent_stream_type.
        """
        return None

    def get_child_context(self, record: dict, context: dict | None) -> dict | None:
        """Return context for child streams (not used for leaf streams)."""
        return None
