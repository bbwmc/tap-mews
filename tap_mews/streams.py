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


class ReservationsStream(MewsStream):
    """Stream for reservations.

    Independent stream that queries all reservations using EnterpriseIds.
    """

    name = "reservations"
    path = "/reservations/getAll/2023-06-06"
    primary_keys = ("Id",)
    replication_key = "UpdatedUtc"
    records_key = "Reservations"

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

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        """Pass reservation_id to child streams (Order Items)."""
        return {"reservation_id": record["Id"]}

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

        # Add EnterpriseIds filter (required for querying all reservations)
        enterprise_ids = self.config.get("enterprise_ids")
        if enterprise_ids:
            body["EnterpriseIds"] = enterprise_ids if isinstance(enterprise_ids, list) else [enterprise_ids]

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


class CustomersStream(MewsStream):
    """Stream for customers (guests).

    Independent stream that uses UpdatedUtc time interval filtering.
    """

    name = "customers"
    path = "/customers/getAll"
    primary_keys = ("Id",)
    replication_key = "UpdatedUtc"
    records_key = "Customers"

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
        """Prepare request payload with time interval filtering.

        The customers endpoint requires UpdatedUtc time interval
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

            # Add UpdatedUtc time interval (API requires this for customers)
            body["UpdatedUtc"] = {
                "StartUtc": start_utc,
                "EndUtc": end_utc,
            }

            self.logger.info(
                f"Querying customers with UpdatedUtc: {start_utc} to {end_utc}"
            )

        # Log the full request body (masking sensitive data)
        import json
        safe_body = {k: v for k, v in body.items() if k not in ["ClientToken", "AccessToken"]}
        safe_body["ClientToken"] = "***"
        safe_body["AccessToken"] = "***"
        self.logger.info(f"Full customers request body: {json.dumps(safe_body, indent=2)}")

        return body


class OrderItemsStream(MewsChildStream):
    """Stream for order items.

    Child stream of Reservations - uses ServiceOrderIds (reservation IDs) to query order items.
    """

    name = "order_items"
    path = "/orderItems/getAll"
    primary_keys = ("Id",)
    replication_key = "UpdatedUtc"
    records_key = "OrderItems"
    parent_stream_type = ReservationsStream

    schema = th.PropertiesList(
        th.Property("Id", th.StringType, description="Unique identifier"),
        th.Property("EnterpriseId", th.StringType, description="Enterprise ID"),
        th.Property("AccountId", th.StringType, description="Account ID"),
        th.Property("AccountType", th.StringType, description="Account type"),
        th.Property("ServiceId", th.StringType, description="Service ID"),
        th.Property("ServiceOrderId", th.StringType, description="Service order ID (reservation ID)"),
        th.Property("Notes", th.StringType, description="Notes"),
        th.Property("BillId", th.StringType, description="Bill ID"),
        th.Property("AccountingCategoryId", th.StringType, description="Accounting category ID"),
        th.Property("BillingName", th.StringType, description="Billing name"),
        th.Property("ExternalIdentifier", th.StringType, description="External identifier"),
        th.Property("UnitCount", th.IntegerType, description="Unit count"),
        th.Property("UnitAmount", th.ObjectType(
            th.Property("Currency", th.StringType),
            th.Property("NetValue", th.NumberType),
            th.Property("GrossValue", th.NumberType),
            th.Property("TaxValues", th.ArrayType(th.ObjectType())),
            th.Property("Breakdown", th.ObjectType()),
        ), description="Unit amount"),
        th.Property("Amount", th.ObjectType(
            th.Property("Currency", th.StringType),
            th.Property("NetValue", th.NumberType),
            th.Property("GrossValue", th.NumberType),
            th.Property("TaxValues", th.ArrayType(th.ObjectType())),
            th.Property("Breakdown", th.ObjectType()),
        ), description="Total amount"),
        th.Property("OriginalAmount", th.ObjectType(
            th.Property("Currency", th.StringType),
            th.Property("NetValue", th.NumberType),
            th.Property("GrossValue", th.NumberType),
            th.Property("TaxValues", th.ArrayType(th.ObjectType())),
            th.Property("Breakdown", th.ObjectType()),
        ), description="Original amount"),
        th.Property("RevenueType", th.StringType, description="Revenue type"),
        th.Property("CreatorProfileId", th.StringType, description="Creator profile ID"),
        th.Property("UpdaterProfileId", th.StringType, description="Updater profile ID"),
        th.Property("CreatedUtc", th.DateTimeType, description="Creation timestamp"),
        th.Property("UpdatedUtc", th.DateTimeType, description="Last update timestamp"),
        th.Property("ConsumedUtc", th.DateTimeType, description="Consumption timestamp"),
        th.Property("CanceledUtc", th.DateTimeType, description="Cancellation timestamp"),
        th.Property("ClosedUtc", th.DateTimeType, description="Closed timestamp"),
        th.Property("StartUtc", th.DateTimeType, description="Start timestamp"),
        th.Property("ClaimedUtc", th.DateTimeType, description="Claimed timestamp"),
        th.Property("AccountingState", th.StringType, description="Accounting state"),
        th.Property("Type", th.StringType, description="Order item type"),
        th.Property("Options", th.ObjectType(), description="Options"),
        th.Property("Data", th.ObjectType(
            th.Property("Discriminator", th.StringType),
            th.Property("Rebate", th.ObjectType()),
            th.Property("Product", th.ObjectType(
                th.Property("ProductId", th.StringType),
                th.Property("AgeCategoryId", th.StringType),
                th.Property("ProductType", th.StringType),
            )),
            th.Property("AllowanceDiscount", th.ObjectType()),
            th.Property("AllowanceProfits", th.ObjectType()),
        ), description="Order item data"),
        th.Property("TaxExemptionReason", th.StringType, description="Tax exemption reason"),
        th.Property("TaxExemptionLegalReference", th.StringType, description="Tax exemption legal reference"),
    ).to_dict()

    def get_child_context(self, record: dict, context: dict | None) -> dict | None:
        """Pass bill_id to child streams (Bills).

        Only returns context if the order item has a BillId.
        """
        bill_id = record.get("BillId")
        if bill_id:
            return {"bill_id": bill_id}
        return None

    def prepare_request_payload(
        self,
        context: dict | None,
        next_page_token: str | None,
    ) -> dict | None:
        """Prepare request payload with ServiceOrderIds filter.

        Order items are queried using ServiceOrderIds (reservation IDs from parent).
        We override the default ServiceIds logic from MewsChildStream.
        """
        # Start with base payload (ClientToken, AccessToken, Client, Limitation)
        body = {
            "ClientToken": self.config["client_token"],
            "AccessToken": self.config["access_token"],
            "Client": self.config.get("client_name", "BBGMeltano 1.0.0"),
            "Limitation": {
                "Count": self.page_size,
            },
        }

        if next_page_token:
            body["Limitation"]["Cursor"] = next_page_token

        # Add ServiceOrderIds filter (reservation IDs from parent context)
        if context and "reservation_id" in context:
            body["ServiceOrderIds"] = [context["reservation_id"]]

        return body


class BillsStream(MewsChildStream):
    """Stream for bills.

    Child stream of Order Items - uses BillIds to query bill details.
    """

    name = "bills"
    path = "/bills/getAll"
    primary_keys = ("Id",)
    replication_key = "UpdatedUtc"
    records_key = "Bills"
    parent_stream_type = OrderItemsStream

    schema = th.PropertiesList(
        th.Property("Id", th.StringType, description="Unique identifier"),
        th.Property("EnterpriseId", th.StringType, description="Enterprise ID"),
        th.Property("AccountId", th.StringType, description="Account ID"),
        th.Property("AccountType", th.StringType, description="Account type"),
        th.Property("Name", th.StringType, description="Bill name"),
        th.Property("Number", th.StringType, description="Bill number"),
        th.Property("State", th.StringType, description="Bill state"),
        th.Property("Type", th.StringType, description="Bill type (Receipt or Invoice)"),
        th.Property("CorrectionState", th.StringType, description="Correction state"),
        th.Property("Notes", th.StringType, description="Notes"),
        th.Property("Revenue", th.ObjectType(
            th.Property("Currency", th.StringType),
            th.Property("NetValue", th.NumberType),
            th.Property("GrossValue", th.NumberType),
            th.Property("TaxValues", th.ArrayType(th.ObjectType())),
        ), description="Revenue amounts"),
        th.Property("CreatedUtc", th.DateTimeType, description="Creation timestamp"),
        th.Property("UpdatedUtc", th.DateTimeType, description="Last update timestamp"),
        th.Property("IssuedUtc", th.DateTimeType, description="Issued timestamp"),
        th.Property("PaidUtc", th.DateTimeType, description="Paid timestamp"),
        th.Property("DueUtc", th.DateTimeType, description="Due timestamp"),
        th.Property("TaxedUtc", th.DateTimeType, description="Taxed timestamp"),
        th.Property("Options", th.ObjectType(), description="Bill options"),
        th.Property("OriginBillId", th.StringType, description="Origin bill ID (for corrections)"),
        th.Property("CompanyId", th.StringType, description="Company ID"),
        th.Property("CustomerId", th.StringType, description="Customer ID"),
        th.Property("CounterId", th.StringType, description="Counter ID"),
    ).to_dict()

    def prepare_request_payload(
        self,
        context: dict | None,
        next_page_token: str | None,
    ) -> dict | None:
        """Prepare request payload with BillIds filter.

        Bills are queried using BillIds from parent order items.
        We override the default ServiceIds logic from MewsChildStream.
        """
        # Start with base payload (ClientToken, AccessToken, Client, Limitation)
        body = {
            "ClientToken": self.config["client_token"],
            "AccessToken": self.config["access_token"],
            "Client": self.config.get("client_name", "BBGMeltano 1.0.0"),
            "Limitation": {
                "Count": self.page_size,
            },
        }

        if next_page_token:
            body["Limitation"]["Cursor"] = next_page_token

        # Add BillIds filter (bill IDs from parent context)
        if context and "bill_id" in context:
            body["BillIds"] = [context["bill_id"]]

        return body
