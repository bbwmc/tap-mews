"""Stream classes for tap-mews.

Each stream corresponds to a Mews API endpoint.
"""

from __future__ import annotations

from singer_sdk import typing as th

from tap_mews.client import MewsChildStream, MewsStream


class ServicesStream(MewsStream):
    """Stream for services (accommodation, spa, etc.).

    This is a parent stream - child streams like resource_categories
    and resources depend on it.
    """

    name = "services"
    path = "/services/getAll"
    primary_keys = ("Id",)
    replication_key = "UpdatedUtc"
    records_key = "Services"

    schema = th.PropertiesList(
        th.Property("Id", th.StringType, description="Unique identifier"),
        th.Property("EnterpriseId", th.StringType, description="Enterprise ID"),
        th.Property("IsActive", th.BooleanType, description="Whether active"),
        th.Property("Name", th.StringType, description="Service name"),
        th.Property("Names", th.ObjectType(), description="Localized names"),
        th.Property("ShortNames", th.ObjectType(), description="Localized short names"),
        th.Property("Descriptions", th.ObjectType(), description="Localized descriptions"),
        th.Property("StartTime", th.StringType, description="Start time"),
        th.Property("EndTime", th.StringType, description="End time"),
        th.Property("Type", th.StringType, description="Service type"),
        th.Property("Ordering", th.IntegerType, description="Sort order"),
        th.Property("Options", th.ObjectType(), description="Service options"),
        th.Property("Promotions", th.ObjectType(), description="Service promotions"),
        th.Property("Data", th.ObjectType(), description="Service data"),
        th.Property("ExternalIdentifier", th.StringType, description="External ID"),
        th.Property("CreatedUtc", th.DateTimeType, description="Creation timestamp"),
        th.Property("UpdatedUtc", th.DateTimeType, description="Last update timestamp"),
    ).to_dict()

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        """Pass service_id to child streams."""
        return {"service_id": record["Id"]}


class ResourceCategoriesStream(MewsChildStream):
    """Stream for resource categories (room types, etc.).

    Requires ServiceIds from parent services stream.
    Note: This endpoint doesn't return UpdatedUtc, so no incremental sync.
    """

    name = "resource_categories"
    path = "/resourceCategories/getAll"
    primary_keys = ("Id",)
    replication_key = None  # API doesn't return UpdatedUtc for this endpoint
    records_key = "ResourceCategories"
    parent_stream_type = ServicesStream

    schema = th.PropertiesList(
        th.Property("Id", th.StringType, description="Unique identifier"),
        th.Property("EnterpriseId", th.StringType, description="Enterprise ID"),
        th.Property("ServiceId", th.StringType, description="Service ID"),
        th.Property("IsActive", th.BooleanType, description="Whether active"),
        th.Property("Type", th.StringType, description="Category type"),
        th.Property("Names", th.ObjectType(), description="Localized names"),
        th.Property("ShortNames", th.ObjectType(), description="Localized short names"),
        th.Property("Descriptions", th.ObjectType(), description="Localized descriptions"),
        th.Property("Ordering", th.IntegerType, description="Sort order"),
        th.Property("Capacity", th.IntegerType, description="Capacity"),
        th.Property("ExtraCapacity", th.IntegerType, description="Extra capacity"),
        th.Property("ExternalIdentifier", th.StringType, description="External ID"),
    ).to_dict()


class ResourcesStream(MewsChildStream):
    """Stream for resources (rooms, parking spaces, etc.).

    Requires ServiceIds from parent services stream.
    """

    name = "resources"
    path = "/resources/getAll"
    primary_keys = ("Id",)
    replication_key = "UpdatedUtc"
    records_key = "Resources"
    parent_stream_type = ServicesStream

    schema = th.PropertiesList(
        th.Property("Id", th.StringType, description="Unique identifier"),
        th.Property("EnterpriseId", th.StringType, description="Enterprise ID"),
        th.Property("ServiceId", th.StringType, description="Service ID"),
        th.Property("ResourceCategoryId", th.StringType, description="Resource category ID"),
        th.Property("IsActive", th.BooleanType, description="Whether active"),
        th.Property("Name", th.StringType, description="Resource name"),
        th.Property("ParentResourceId", th.StringType, description="Parent resource ID"),
        th.Property("State", th.StringType, description="Current state"),
        th.Property("StateReason", th.StringType, description="State reason"),
        th.Property("Descriptions", th.ObjectType(), description="Localized descriptions"),
        th.Property("FloorNumber", th.StringType, description="Floor number"),
        th.Property("BuildingNumber", th.StringType, description="Building number"),
        th.Property("ExternalIdentifier", th.StringType, description="External ID"),
        th.Property("CreatedUtc", th.DateTimeType, description="Creation timestamp"),
        th.Property("UpdatedUtc", th.DateTimeType, description="Last update timestamp"),
    ).to_dict()


class ReservationsStream(MewsChildStream):
    """Stream for reservations.

    Requires ServiceIds from parent services stream.
    This is also a parent stream - customers stream depends on it.
    """

    name = "reservations"
    path = "/reservations/getAll"
    primary_keys = ("Id",)
    replication_key = "UpdatedUtc"
    records_key = "Reservations"
    parent_stream_type = ServicesStream

    schema = th.PropertiesList(
        th.Property("Id", th.StringType, description="Unique identifier"),
        th.Property("ServiceId", th.StringType, description="Service ID"),
        th.Property("AccountId", th.StringType, description="Account ID"),
        th.Property("AccountType", th.StringType, description="Account type"),
        th.Property("BookerId", th.StringType, description="Booker ID"),
        th.Property("StartUtc", th.DateTimeType, description="Start timestamp"),
        th.Property("EndUtc", th.DateTimeType, description="End timestamp"),
        th.Property("ReleasedUtc", th.DateTimeType, description="Released timestamp"),
        th.Property("CancelledUtc", th.DateTimeType, description="Cancelled timestamp"),
        th.Property("RequestedResourceCategoryId", th.StringType, description="Requested category"),
        th.Property("AssignedResourceId", th.StringType, description="Assigned resource ID"),
        th.Property("AssignedResourceLocked", th.BooleanType, description="Resource locked"),
        th.Property("BusinessSegmentId", th.StringType, description="Business segment ID"),
        th.Property("CompanyId", th.StringType, description="Company ID"),
        th.Property("TravelAgencyId", th.StringType, description="Travel agency ID"),
        th.Property("AvailabilityBlockId", th.StringType, description="Availability block ID"),
        th.Property("RateId", th.StringType, description="Rate ID"),
        th.Property("VoucherId", th.StringType, description="Voucher ID"),
        th.Property("CreditCardId", th.StringType, description="Credit card ID"),
        th.Property("GroupId", th.StringType, description="Reservation group ID"),
        th.Property("Number", th.StringType, description="Confirmation number"),
        th.Property("ChannelNumber", th.StringType, description="Channel confirmation number"),
        th.Property("ChannelManagerNumber", th.StringType, description="Channel manager number"),
        th.Property("ChannelManagerGroupNumber", th.StringType, description="Channel manager group"),
        th.Property("ChannelManager", th.StringType, description="Channel manager name"),
        th.Property("State", th.StringType, description="Reservation state"),
        th.Property("Origin", th.StringType, description="Origin of reservation"),
        th.Property("OriginDetails", th.StringType, description="Origin details"),
        th.Property("Purpose", th.StringType, description="Purpose of stay"),
        th.Property("CreatedUtc", th.DateTimeType, description="Creation timestamp"),
        th.Property("UpdatedUtc", th.DateTimeType, description="Last update timestamp"),
        th.Property("CancellationReason", th.StringType, description="Cancellation reason"),
        th.Property("Options", th.ObjectType(), description="Reservation options"),
    ).to_dict()

    def __init__(self, *args, **kwargs):
        """Initialize and set up customer IDs collection."""
        super().__init__(*args, **kwargs)
        self._current_customer_ids = []

    def prepare_request_payload(
        self,
        context: dict | None,
        next_page_token: str | None,
    ) -> dict | None:
        """Prepare request payload with time interval filtering.

        The reservations endpoint requires UpdatedUtc time interval
        with a maximum of 3 months. We enforce this limit to avoid API errors.
        """
        from datetime import datetime, timedelta, timezone

        body = super().prepare_request_payload(context, next_page_token)

        # Get start_date from config
        start_date_str = self.config.get("start_date")
        if start_date_str:
            # Parse the start date - handle both date-only and datetime formats
            if isinstance(start_date_str, str):
                # datetime.fromisoformat handles both YYYY-MM-DD and full ISO formats
                start_date = datetime.fromisoformat(start_date_str.replace("Z", "+00:00"))
            else:
                start_date = start_date_str

            # Ensure start_date is timezone-aware (convert naive to UTC)
            if start_date.tzinfo is None:
                start_date = start_date.replace(tzinfo=timezone.utc)

            # Use current time as end date (or you could make this configurable)
            end_date = datetime.now(timezone.utc)

            # Enforce 3-month maximum as per API limits
            max_interval = timedelta(days=90)  # ~3 months
            if (end_date - start_date) > max_interval:
                # Cap the end date to 3 months from start
                end_date = start_date + max_interval
                self.logger.warning(
                    f"Date range exceeds 3-month API limit. "
                    f"Capping to {start_date.date()} - {end_date.date()}. "
                    f"Use incremental sync for ongoing updates."
                )

            # Format dates as ISO 8601 with Z suffix (required by Mews API)
            # Use timespec='seconds' to avoid microseconds in the output
            start_utc = start_date.isoformat(timespec='seconds').replace("+00:00", "Z")
            end_utc = end_date.isoformat(timespec='seconds').replace("+00:00", "Z")

            # Add UpdatedUtc time interval (API requires this for reservations)
            body["UpdatedUtc"] = {
                "StartUtc": start_utc,
                "EndUtc": end_utc,
            }

            self.logger.info(
                f"Querying reservations with UpdatedUtc: {start_utc} to {end_utc}"
            )

        # Log the full request body (masking sensitive data)
        import json
        safe_body = {k: v for k, v in body.items() if k not in ["ClientToken", "AccessToken"]}
        safe_body["ClientToken"] = "***"
        safe_body["AccessToken"] = "***"
        self.logger.info(f"Full reservations request body: {json.dumps(safe_body, indent=2)}")

        return body

    def parse_response(self, response):
        """Parse response and collect customer IDs from Customers array.

        The reservations API returns both Reservations and Customers arrays.
        We collect customer IDs here to pass to child streams.
        """
        data = response.json()

        # Collect customer IDs from the Customers array
        self._current_customer_ids = []
        if "Customers" in data and data["Customers"]:
            self._current_customer_ids = [c["Id"] for c in data["Customers"] if "Id" in c]

        # Yield reservations as normal
        yield from super().parse_response(response)

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        """Pass customer IDs to child streams.

        Returns customer IDs collected from the Customers array in the API response.
        """
        return {
            "customer_ids": self._current_customer_ids,
            "service_id": context.get("service_id") if context else None,
        }


class CustomersStream(MewsChildStream):
    """Stream for customers (guests).

    Child of ReservationsStream - uses customer IDs from reservations response
    to query the customers endpoint with CustomerIds filter.
    """

    name = "customers"
    path = "/customers/getAll"
    primary_keys = ("Id",)
    replication_key = "UpdatedUtc"
    records_key = "Customers"
    parent_stream_type = ReservationsStream
    requires_service_id = False  # Customers endpoint doesn't use ServiceIds

    schema = th.PropertiesList(
        th.Property("Id", th.StringType, description="Unique identifier"),
        th.Property("EnterpriseId", th.StringType, description="Enterprise ID"),
        th.Property("ChainId", th.StringType, description="Chain ID"),
        th.Property("Number", th.StringType, description="Customer number"),
        th.Property("Title", th.StringType, description="Title"),
        th.Property("Sex", th.StringType, description="Sex"),
        th.Property("FirstName", th.StringType, description="First name"),
        th.Property("LastName", th.StringType, description="Last name"),
        th.Property("SecondLastName", th.StringType, description="Second last name"),
        th.Property("NationalityCode", th.StringType, description="Nationality code"),
        th.Property("LanguageCode", th.StringType, description="Language code"),
        th.Property("BirthDate", th.StringType, description="Birth date"),
        th.Property("BirthPlace", th.StringType, description="Birth place"),
        th.Property("Email", th.StringType, description="Email address"),
        th.Property("Phone", th.StringType, description="Phone number"),
        th.Property("TaxIdentificationNumber", th.StringType, description="Tax ID"),
        th.Property("LoyaltyCode", th.StringType, description="Loyalty code"),
        th.Property("AccountingCode", th.StringType, description="Accounting code"),
        th.Property("BillingCode", th.StringType, description="Billing code"),
        th.Property("Notes", th.StringType, description="Notes"),
        th.Property("CarRegistrationNumber", th.StringType, description="Car registration"),
        th.Property("DietaryRequirements", th.StringType, description="Dietary requirements"),
        th.Property("CompanyId", th.StringType, description="Company ID"),
        th.Property("Classifications", th.ArrayType(th.StringType), description="Classifications"),
        th.Property("Options", th.ArrayType(th.StringType), description="Options"),
        th.Property("Address", th.ObjectType(), description="Address object"),
        th.Property("CreatedUtc", th.DateTimeType, description="Creation timestamp"),
        th.Property("UpdatedUtc", th.DateTimeType, description="Last update timestamp"),
        th.Property("ActivityState", th.StringType, description="Activity state"),
    ).to_dict()

    def prepare_request_payload(
        self,
        context: dict | None,
        next_page_token: str | None,
    ) -> dict | None:
        """Prepare request payload with CustomerIds filter.

        The customers endpoint requires at least one filter. We use CustomerIds
        from the parent reservations response.
        """
        body = super().prepare_request_payload(context, next_page_token)

        # Add CustomerIds filter from context
        if context and "customer_ids" in context and context["customer_ids"]:
            body["CustomerIds"] = context["customer_ids"]
        else:
            # If no customer IDs, skip this request
            return None

        # Remove ServiceIds as customers endpoint doesn't use it
        body.pop("ServiceIds", None)

        return body
