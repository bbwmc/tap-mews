"""Mews tap class."""

from __future__ import annotations

from singer_sdk import Stream, Tap
from singer_sdk import typing as th

from tap_mews.streams import (
    BillsStream,
    CustomersStream,
    OrderItemsStream,
    PaymentsStream,
    ReservationsStream,
    ResourceCategoriesStream,
    ResourcesStream,
    ServicesStream,
)


class TapMews(Tap):
    """Singer tap for Mews PMS API."""

    name = "tap-mews"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "client_token",
            th.StringType,
            required=True,
            secret=True,
            description="Mews API Client Token",
        ),
        th.Property(
            "access_token",
            th.StringType,
            required=True,
            secret=True,
            description="Mews API Access Token",
        ),
        th.Property(
            "api_url",
            th.StringType,
            default="https://api.mews.com",
            description="Mews API base URL",
        ),
        th.Property(
            "client_name",
            th.StringType,
            default="BBGMeltano 1.0.0",
            description="Client identifier sent with API requests",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            required=True,
            description="Start date for retrieving reservations (ISO format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ)",
        ),
        th.Property(
            "enterprise_ids",
            th.ArrayType(th.StringType),
            required=False,
            description="List of Enterprise IDs to query (used for reservations and other enterprise-scoped endpoints)",
        ),
    ).to_dict()

    def discover_streams(self) -> list[Stream]:
        """Return a list of discovered streams.

        Returns:
            A list of stream instances.
        """
        return [
            # Independent parent streams
            ServicesStream(self),
            CustomersStream(self),
            ReservationsStream(self),
            # Service-dependent child streams
            ResourceCategoriesStream(self),
            ResourcesStream(self),
            # Reservation-dependent child streams
            OrderItemsStream(self),
            # Order Item-dependent child streams
            BillsStream(self),
            # Bill-dependent child streams
            PaymentsStream(self),
        ]


if __name__ == "__main__":
    TapMews.cli()
