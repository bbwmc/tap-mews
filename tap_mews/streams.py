"""Stream classes for tap-mews.

Each stream corresponds to a Mews API endpoint.
"""

from __future__ import annotations

from singer_sdk import typing as th

from tap_mews.client import MewsChildStream, MewsStream


def _require_enterprise_ids(config: dict, stream_name: str) -> list[str]:
    """Return enterprise IDs from config or raise if missing."""

    enterprise_ids = config.get("enterprise_ids")
    if not enterprise_ids:
        raise ValueError(
            f"{stream_name} stream requires 'enterprise_ids' to be set in the tap config."
        )

    if isinstance(enterprise_ids, (list, tuple)):
        return list(enterprise_ids)

    return [enterprise_ids]



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

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        """Pass service and category ids to child streams."""
        return {
            "service_id": record.get("ServiceId"),
            "resource_category_id": record.get("Id"),
            "enterprise_id": record.get("EnterpriseId"),
        }


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


class ProductsStream(MewsChildStream):
    """Stream for products (orderable items) linked to services."""

    name = "products"
    path = "/products/getAll"
    primary_keys = ("Id",)
    replication_key = "UpdatedUtc"
    records_key = "Products"
    parent_stream_type = ServicesStream

    schema = th.PropertiesList(
        th.Property("Id", th.StringType, description="Unique identifier"),
        th.Property("ServiceId", th.StringType, description="Service ID"),
        th.Property("CategoryId", th.StringType, description="Product category ID"),
        th.Property("AccountingCategoryId", th.StringType, description="Accounting category ID"),
        th.Property("IsActive", th.BooleanType, description="Whether active"),
        th.Property("IsDefault", th.BooleanType, description="Whether default product"),
        th.Property("Name", th.StringType, description="Product name"),
        th.Property("Names", th.ObjectType(), description="Localized names"),
        th.Property("ExternalName", th.StringType, description="External product name"),
        th.Property("ExternalNames", th.ObjectType(), description="Localized external names"),
        th.Property("ShortName", th.StringType, description="Short name"),
        th.Property("ShortNames", th.ObjectType(), description="Localized short names"),
        th.Property("Description", th.StringType, description="Description"),
        th.Property("Descriptions", th.ObjectType(), description="Localized descriptions"),
        th.Property("Charging", th.StringType, description="Charging rule"),
        th.Property("ChargingMode", th.StringType, description="Charging mode"),
        th.Property("Posting", th.StringType, description="Posting rule"),
        th.Property("PostingMode", th.StringType, description="Posting mode"),
        th.Property(
            "Options",
            th.ObjectType(
                th.Property("BillAsPackage", th.BooleanType),
            ),
            description="Product options",
        ),
        th.Property(
            "Promotions",
            th.ObjectType(
                th.Property("BeforeCheckIn", th.BooleanType),
                th.Property("AfterCheckIn", th.BooleanType),
                th.Property("DuringStay", th.BooleanType),
                th.Property("BeforeCheckOut", th.BooleanType),
                th.Property("AfterCheckOut", th.BooleanType),
                th.Property("DuringCheckOut", th.BooleanType),
            ),
            description="Promotion availability flags",
        ),
        th.Property(
            "Classifications",
            th.ObjectType(
                th.Property("Food", th.BooleanType),
                th.Property("Beverage", th.BooleanType),
                th.Property("Wellness", th.BooleanType),
                th.Property("CityTax", th.BooleanType),
                th.Property("Fee", th.BooleanType),
            ),
            description="Classification flags",
        ),
        th.Property(
            "Price",
            th.ObjectType(
                th.Property("Value", th.NumberType),
                th.Property("Net", th.NumberType),
                th.Property("Tax", th.NumberType),
                th.Property("TaxRate", th.NumberType),
                th.Property("Currency", th.StringType),
                th.Property("NetValue", th.NumberType),
                th.Property("GrossValue", th.NumberType),
                th.Property(
                    "TaxValues",
                    th.ArrayType(
                        th.ObjectType(
                            th.Property("Code", th.StringType),
                            th.Property("Value", th.NumberType),
                        ),
                    ),
                ),
                th.Property(
                    "Breakdown",
                    th.ObjectType(
                        th.Property(
                            "Items",
                            th.ArrayType(
                                th.ObjectType(
                                    th.Property("TaxRateCode", th.StringType),
                                    th.Property("NetValue", th.NumberType),
                                    th.Property("TaxValue", th.NumberType),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
            description="Current price details",
        ),
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

    @property
    def partitions(self) -> list[dict] | None:
        """Return 90-day windows from bookmark/start_date up to now."""
        from datetime import datetime, timedelta, timezone

        max_interval = timedelta(days=90)
        now = datetime.now(timezone.utc)

        start = self.get_starting_timestamp(context=None)
        if start is None:
            start_str = self.config.get("start_date")
            if start_str:
                start = datetime.fromisoformat(str(start_str).replace("Z", "+00:00"))
            else:
                start = now - max_interval

        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)

        if start > now:
            start = now - max_interval

        partitions: list[dict] = []
        cursor = start
        while cursor < now:
            window_end = min(cursor + max_interval, now)
            partitions.append({"window_start": cursor, "window_end": window_end})
            cursor = window_end

        if not partitions:
            partitions.append({"window_start": start, "window_end": now})

        return partitions

    schema = th.PropertiesList(
        th.Property("Id", th.StringType, description="Reservation identifier"),
        th.Property("ServiceId", th.StringType, description="Service identifier"),
        th.Property("AccountId", th.StringType, description="Account identifier"),
        th.Property("AccountType", th.StringType, description="Account type"),
        th.Property("CreatorProfileId", th.StringType, description="Creator profile identifier"),
        th.Property("UpdaterProfileId", th.StringType, description="Updater profile identifier"),
        th.Property("BookerId", th.StringType, description="Booker identifier"),
        th.Property("Number", th.StringType, description="Reservation confirmation number"),
        th.Property("State", th.StringType, description="Reservation state"),
        th.Property("Origin", th.StringType, description="Reservation origin"),
        th.Property("CommanderOrigin", th.StringType, description="Commander origin"),
        th.Property("OriginDetails", th.StringType, description="Origin details"),
        th.Property("CreatedUtc", th.DateTimeType, description="Creation timestamp"),
        th.Property("UpdatedUtc", th.DateTimeType, description="Last update timestamp"),
        th.Property("CancelledUtc", th.DateTimeType, description="Cancellation timestamp"),
        th.Property("VoucherId", th.StringType, description="Voucher identifier"),
        th.Property("BusinessSegmentId", th.StringType, description="Business segment identifier"),
        th.Property(
            "Options",
            th.ObjectType(
                th.Property("OwnerCheckedIn", th.BooleanType),
                th.Property("AllCompanionsCheckedIn", th.BooleanType),
                th.Property("AnyCompanionCheckedIn", th.BooleanType),
                th.Property("ConnectorCheckIn", th.BooleanType),
            ),
            description="Reservation options",
        ),
        th.Property("RateId", th.StringType, description="Rate identifier"),
        th.Property("CreditCardId", th.StringType, description="Credit card identifier"),
        th.Property("GroupId", th.StringType, description="Reservation group identifier"),
        th.Property("RequestedResourceCategoryId", th.StringType, description="Requested resource category identifier"),
        th.Property("AssignedResourceId", th.StringType, description="Assigned resource identifier"),
        th.Property("AvailabilityBlockId", th.StringType, description="Availability block identifier"),
        th.Property("PartnerCompanyId", th.StringType, description="Partner company identifier"),
        th.Property("TravelAgencyId", th.StringType, description="Travel agency identifier"),
        th.Property("AssignedResourceLocked", th.BooleanType, description="Whether assigned resource is locked"),
        th.Property("ChannelNumber", th.StringType, description="Channel confirmation number"),
        th.Property("ChannelManagerNumber", th.StringType, description="Channel manager number"),
        th.Property("ChannelManagerGroupNumber", th.StringType, description="Channel manager group number"),
        th.Property("ChannelManager", th.StringType, description="Channel manager name"),
        th.Property("CancellationReason", th.StringType, description="Cancellation reason"),
        th.Property("ReleasedUtc", th.DateTimeType, description="Released timestamp"),
        th.Property("StartUtc", th.DateTimeType, description="Deprecated start timestamp"),
        th.Property("EndUtc", th.DateTimeType, description="Deprecated end timestamp"),
        th.Property("ScheduledStartUtc", th.DateTimeType, description="Scheduled start timestamp"),
        th.Property("ActualStartUtc", th.DateTimeType, description="Actual start timestamp"),
        th.Property("ScheduledEndUtc", th.DateTimeType, description="Scheduled end timestamp"),
        th.Property("ActualEndUtc", th.DateTimeType, description="Actual end timestamp"),
        th.Property("Purpose", th.StringType, description="Purpose of stay"),
        th.Property("QrCodeData", th.StringType, description="QR code data"),
        th.Property(
            "PersonCounts",
            th.ArrayType(
                th.ObjectType(
                    th.Property("AgeCategoryId", th.StringType),
                    th.Property("Count", th.IntegerType),
                )
            ),
            description="Counts per age category",
        ),
    ).to_dict()

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        """Pass reservation_id and group_id to child streams."""
        return {
            "reservation_id": record["Id"],
            "group_id": record.get("GroupId"),
        }

    def prepare_request_payload(
        self,
        context: dict | None,
        next_page_token: str | None,
    ) -> dict | None:
        """Prepare request payload with time interval filtering.

        The reservations endpoint requires UpdatedUtc time interval
        with a maximum of 3 months. We enforce this limit to avoid API errors
        while covering the full historical range via chunked partitions.
        """
        from datetime import datetime, timedelta, timezone

        body = super().prepare_request_payload(context, next_page_token)

        # Add EnterpriseIds filter (required for querying all reservations)
        enterprise_ids = self.config.get("enterprise_ids")
        if enterprise_ids:
            body["EnterpriseIds"] = enterprise_ids if isinstance(enterprise_ids, list) else [enterprise_ids]

        max_interval = timedelta(days=90)
        now = datetime.now(timezone.utc)

        window_start = None
        window_end = None
        if context:
            window_start = context.get("window_start")
            window_end = context.get("window_end")

        if window_start is None or window_end is None:
            # Fallback: derive a recent 90-day window from bookmark/config
            derived_start = self.get_starting_timestamp(context)
            if derived_start is None:
                start_date_str = self.config.get("start_date")
                if start_date_str:
                    derived_start = datetime.fromisoformat(str(start_date_str).replace("Z", "+00:00"))
                else:
                    derived_start = now - max_interval

            if derived_start.tzinfo is None:
                derived_start = derived_start.replace(tzinfo=timezone.utc)

            window_start = max(derived_start, now - max_interval)
            window_end = min(window_start + max_interval, now)

        if window_start.tzinfo is None:
            window_start = window_start.replace(tzinfo=timezone.utc)
        if window_end.tzinfo is None:
            window_end = window_end.replace(tzinfo=timezone.utc)

        start_utc = window_start.isoformat(timespec='seconds').replace("+00:00", "Z")
        end_utc = window_end.isoformat(timespec='seconds').replace("+00:00", "Z")

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

    @property
    def partitions(self) -> list[dict] | None:
        """Return 90-day windows from bookmark/start_date up to now."""
        from datetime import datetime, timedelta, timezone

        max_interval = timedelta(days=90)
        now = datetime.now(timezone.utc)

        start = self.get_starting_timestamp(context=None)
        if start is None:
            start_str = self.config.get("start_date")
            if start_str:
                start = datetime.fromisoformat(str(start_str).replace("Z", "+00:00"))
            else:
                start = now - max_interval

        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)

        if start > now:
            start = now - max_interval

        partitions: list[dict] = []
        cursor = start
        while cursor < now:
            window_end = min(cursor + max_interval, now)
            partitions.append({"window_start": cursor, "window_end": window_end})
            cursor = window_end

        if not partitions:
            partitions.append({"window_start": start, "window_end": now})

        return partitions

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

        max_interval = timedelta(days=90)
        now = datetime.now(timezone.utc)

        window_start = None
        window_end = None
        if context:
            window_start = context.get("window_start")
            window_end = context.get("window_end")

        if window_start is None or window_end is None:
            derived_start = self.get_starting_timestamp(context)
            if derived_start is None:
                start_date_str = self.config.get("start_date")
                if start_date_str:
                    derived_start = datetime.fromisoformat(str(start_date_str).replace("Z", "+00:00"))
                else:
                    derived_start = now - max_interval

            if derived_start.tzinfo is None:
                derived_start = derived_start.replace(tzinfo=timezone.utc)

            window_start = max(derived_start, now - max_interval)
            window_end = min(window_start + max_interval, now)

        if window_start.tzinfo is None:
            window_start = window_start.replace(tzinfo=timezone.utc)
        if window_end.tzinfo is None:
            window_end = window_end.replace(tzinfo=timezone.utc)

        start_utc = window_start.isoformat(timespec='seconds').replace("+00:00", "Z")
        end_utc = window_end.isoformat(timespec='seconds').replace("+00:00", "Z")

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


class RatesStream(MewsStream):
    """Stream for rates definitions."""

    name = "rates"
    path = "/rates/getAll"
    primary_keys = ("Id",)
    replication_key = "UpdatedUtc"
    records_key = "Rates"

    schema = th.PropertiesList(
        th.Property("Id", th.StringType, description="Rate identifier"),
        th.Property("GroupId", th.StringType, description="Rate group identifier"),
        th.Property("ServiceId", th.StringType, description="Service identifier"),
        th.Property("BaseRateId", th.StringType, description="Base rate identifier"),
        th.Property("IsBaseRate", th.BooleanType, description="Whether this is the base rate"),
        th.Property("BusinessSegmentId", th.StringType, description="Business segment identifier"),
        th.Property("IsActive", th.BooleanType, description="Active state flag"),
        th.Property("IsEnabled", th.BooleanType, description="Enabled flag"),
        th.Property("IsPublic", th.BooleanType, description="Publicly available flag"),
        th.Property("IsDefault", th.BooleanType, description="Default rate flag"),
        th.Property("Type", th.StringType, description="Rate type"),
        th.Property("Name", th.StringType, description="Primary rate name"),
        th.Property("Names", th.ObjectType(), description="Localized names"),
        th.Property("ExternalNames", th.ObjectType(), description="External names"),
        th.Property("ShortName", th.StringType, description="Short name"),
        th.Property("Description", th.ObjectType(), description="Localized descriptions"),
        th.Property("ExternalIdentifier", th.StringType, description="External identifier"),
        th.Property(
            "Options",
            th.ObjectType(
                th.Property("HidePriceFromGuest", th.BooleanType),
                th.Property("IsBonusPointsEligible", th.BooleanType),
            ),
            description="Rate options",
        ),
        th.Property(
            "Pricing",
            th.ObjectType(
                th.Property("Discriminator", th.StringType),
                th.Property(
                    "BaseRatePricing",
                    th.ObjectType(
                        th.Property(
                            "Amount",
                            th.ObjectType(
                                th.Property("Currency", th.StringType),
                                th.Property("NetValue", th.NumberType),
                                th.Property("GrossValue", th.NumberType),
                                th.Property(
                                    "TaxValues",
                                    th.ArrayType(
                                        th.ObjectType(
                                            th.Property("Code", th.StringType),
                                            th.Property("Value", th.NumberType),
                                        )
                                    ),
                                ),
                                th.Property(
                                    "Breakdown",
                                    th.ObjectType(
                                        th.Property(
                                            "Items",
                                            th.ArrayType(
                                                th.ObjectType(
                                                    th.Property("TaxRateCode", th.StringType),
                                                    th.Property("NetValue", th.NumberType),
                                                    th.Property("TaxValue", th.NumberType),
                                                )
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                th.Property("DependentRatePricing", th.ObjectType()),
            ),
            description="Pricing definition",
        ),
        th.Property("TaxExemptionReason", th.StringType, description="Tax exemption reason"),
        th.Property("TaxExemptionLegalReference", th.StringType, description="Tax exemption legal reference"),
        th.Property("UpdatedUtc", th.DateTimeType, description="Last updated timestamp"),
    ).to_dict()

    def prepare_request_payload(
        self,
        context: dict | None,
        next_page_token: str | None,
    ) -> dict | None:
        body = super().prepare_request_payload(context, next_page_token)
        body["EnterpriseIds"] = _require_enterprise_ids(self.config, self.name)
        return body


class RateGroupsStream(MewsChildStream):
    """Stream for rate groups."""

    name = "rate_groups"
    path = "/rateGroups/getAll"
    primary_keys = ("Id",)
    replication_key = "UpdatedUtc"
    records_key = "RateGroups"
    parent_stream_type = ServicesStream

    schema = th.PropertiesList(
        th.Property("Id", th.StringType, description="Rate group identifier"),
        th.Property("ServiceId", th.StringType, description="Service identifier"),
        th.Property("IsActive", th.BooleanType, description="Whether active"),
        th.Property("CreatedUtc", th.DateTimeType, description="Creation timestamp"),
        th.Property("UpdatedUtc", th.DateTimeType, description="Last update timestamp"),
        th.Property("Ordering", th.IntegerType, description="Display ordering"),
        th.Property("Names", th.ObjectType(), description="Localized names"),
        th.Property("ShortNames", th.ObjectType(), description="Localized short names"),
        th.Property("Descriptions", th.ObjectType(), description="Localized descriptions"),
        th.Property("ExternalIdentifier", th.StringType, description="External rate group ID"),
    ).to_dict()

    def prepare_request_payload(
        self,
        context: dict | None,
        next_page_token: str | None,
    ) -> dict | None:
        """Prepare request payload with service, enterprise, and date filtering."""
        from datetime import datetime, timedelta, timezone

        body = super().prepare_request_payload(context, next_page_token)

        enterprise_ids = self.config.get("enterprise_ids")
        if enterprise_ids:
            body["EnterpriseIds"] = (
                enterprise_ids if isinstance(enterprise_ids, list) else [enterprise_ids]
            )

        start_date_str = self.config.get("start_date")
        if start_date_str:
            if isinstance(start_date_str, str):
                start_date = datetime.fromisoformat(start_date_str.replace("Z", "+00:00"))
            else:
                start_date = start_date_str

            if start_date.tzinfo is None:
                start_date = start_date.replace(tzinfo=timezone.utc)

            end_date = datetime.now(timezone.utc)
            max_interval = timedelta(days=90)
            if (end_date - start_date) > max_interval:
                end_date = start_date + max_interval
                self.logger.warning(
                    "Date range exceeds 3-month API limit. "
                    f"Capping to {start_date.date()} - {end_date.date()}."
                )

            start_utc = start_date.isoformat(timespec="seconds").replace("+00:00", "Z")
            end_utc = end_date.isoformat(timespec="seconds").replace("+00:00", "Z")

            body["UpdatedUtc"] = {"StartUtc": start_utc, "EndUtc": end_utc}

        return body


class AgeCategoriesStream(MewsChildStream):
    """Stream for age categories."""

    name = "age_categories"
    path = "/ageCategories/getAll"
    primary_keys = ("Id",)
    replication_key = "UpdatedUtc"
    records_key = "AgeCategories"
    parent_stream_type = ServicesStream

    schema = th.PropertiesList(
        th.Property("Id", th.StringType, description="Age category identifier"),
        th.Property("ServiceId", th.StringType, description="Service identifier"),
        th.Property("MinimalAge", th.IntegerType, description="Minimum age"),
        th.Property("MaximalAge", th.IntegerType, description="Maximum age"),
        th.Property("Names", th.ObjectType(), description="Localized names"),
        th.Property("ShortNames", th.ObjectType(), description="Localized short names"),
        th.Property("CreatedUtc", th.DateTimeType, description="Creation timestamp"),
        th.Property("UpdatedUtc", th.DateTimeType, description="Last update timestamp"),
        th.Property("Classification", th.StringType, description="Age category classification"),
        th.Property("IsActive", th.BooleanType, description="Whether active"),
        th.Property("ExternalIdentifier", th.StringType, description="External identifier"),
    ).to_dict()

    def prepare_request_payload(
        self,
        context: dict | None,
        next_page_token: str | None,
    ) -> dict | None:
        """Prepare request payload with optional enterprise and date filters."""
        from datetime import datetime, timedelta, timezone

        body = super().prepare_request_payload(context, next_page_token)

        enterprise_ids = self.config.get("enterprise_ids")
        if enterprise_ids:
            body["EnterpriseIds"] = (
                enterprise_ids if isinstance(enterprise_ids, list) else [enterprise_ids]
            )

        start_date_str = self.config.get("start_date")
        if start_date_str:
            if isinstance(start_date_str, str):
                start_date = datetime.fromisoformat(start_date_str.replace("Z", "+00:00"))
            else:
                start_date = start_date_str

            if start_date.tzinfo is None:
                start_date = start_date.replace(tzinfo=timezone.utc)

            end_date = datetime.now(timezone.utc)
            max_interval = timedelta(days=90)
            if (end_date - start_date) > max_interval:
                end_date = start_date + max_interval
                self.logger.warning(
                    "Date range exceeds 3-month API limit. "
                    f"Capping to {start_date.date()} - {end_date.date()}."
                )

            start_utc = start_date.isoformat(timespec="seconds").replace("+00:00", "Z")
            end_utc = end_date.isoformat(timespec="seconds").replace("+00:00", "Z")
            body["UpdatedUtc"] = {"StartUtc": start_utc, "EndUtc": end_utc}

        return body


class RestrictionsStream(MewsChildStream):
    """Stream for restrictions."""

    name = "restrictions"
    path = "/restrictions/getAll"
    primary_keys = ("Id",)
    replication_key = None
    records_key = "Restrictions"
    parent_stream_type = ServicesStream

    schema = th.PropertiesList(
        th.Property("Id", th.StringType, description="Restriction identifier"),
        th.Property("ServiceId", th.StringType, description="Service identifier"),
        th.Property("ExternalIdentifier", th.StringType, description="External identifier"),
        th.Property("Origin", th.StringType, description="Restriction origin"),
        th.Property(
            "Conditions",
            th.ObjectType(
                th.Property("Type", th.StringType),
                th.Property("ExactRateId", th.StringType),
                th.Property("BaseRateId", th.StringType),
                th.Property("RateGroupId", th.StringType),
                th.Property("ResourceCategoryId", th.StringType),
                th.Property("ResourceCategoryType", th.StringType),
                th.Property("StartUtc", th.DateTimeType),
                th.Property("EndUtc", th.DateTimeType),
                th.Property("Days", th.ArrayType(th.StringType)),
                th.Property("Hours", th.ObjectType()),
            ),
            description="Restriction conditions",
        ),
        th.Property(
            "Exceptions",
            th.ObjectType(
                th.Property("MinAdvance", th.StringType),
                th.Property("MaxAdvance", th.StringType),
                th.Property("MinLength", th.StringType),
                th.Property("MaxLength", th.StringType),
                th.Property(
                    "MinPrice",
                    th.ObjectType(
                        th.Property("Value", th.NumberType),
                        th.Property("Currency", th.StringType),
                    ),
                ),
                th.Property(
                    "MaxPrice",
                    th.ObjectType(
                        th.Property("Value", th.NumberType),
                        th.Property("Currency", th.StringType),
                    ),
                ),
            ),
            description="Restriction exceptions",
        ),
    ).to_dict()

    def prepare_request_payload(
        self,
        context: dict | None,
        next_page_token: str | None,
    ) -> dict | None:
        """Prepare request payload with enterprise filter and date window."""
        from datetime import datetime, timedelta, timezone

        body = super().prepare_request_payload(context, next_page_token)

        enterprise_ids = self.config.get("enterprise_ids")
        if enterprise_ids:
            body["EnterpriseIds"] = (
                enterprise_ids if isinstance(enterprise_ids, list) else [enterprise_ids]
            )

        start_date_str = self.config.get("start_date")
        if start_date_str:
            if isinstance(start_date_str, str):
                start_date = datetime.fromisoformat(start_date_str.replace("Z", "+00:00"))
            else:
                start_date = start_date_str

            if start_date.tzinfo is None:
                start_date = start_date.replace(tzinfo=timezone.utc)

            end_date = datetime.now(timezone.utc)
            max_interval = timedelta(days=90)
            if (end_date - start_date) > max_interval:
                end_date = start_date + max_interval
                self.logger.warning(
                    "Date range exceeds 3-month API limit. "
                    f"Capping to {start_date.date()} - {end_date.date()}."
                )

            start_utc = start_date.isoformat(timespec="seconds").replace("+00:00", "Z")
            end_utc = end_date.isoformat(timespec="seconds").replace("+00:00", "Z")
            body["UpdatedUtc"] = {"StartUtc": start_utc, "EndUtc": end_utc}

        return body


class ProductServiceOrdersStream(MewsStream):
    """Stream for product service orders.

    Independent stream that queries all product service orders using UpdatedUtc time interval.
    The API requires ServiceIds, so we fetch all Orderable services and pass their IDs.
    """

    name = "product_service_orders"
    path = "/productServiceOrders/getAll"
    primary_keys = ("Id",)
    replication_key = "UpdatedUtc"
    records_key = "ProductServiceOrders"

    # Cache for orderable service IDs (fetched once per sync)
    _orderable_service_ids: list[str] | None = None

    schema = th.PropertiesList(
        th.Property("Id", th.StringType, description="Product service order identifier"),
        th.Property("ServiceId", th.StringType, description="Service identifier"),
        th.Property("AccountId", th.StringType, description="Account identifier"),
        th.Property("AccountType", th.StringType, description="Account type"),
        th.Property("CreatorProfileId", th.StringType, description="Creator profile identifier"),
        th.Property("UpdaterProfileId", th.StringType, description="Updater profile identifier"),
        th.Property("BookerId", th.StringType, description="Booker identifier"),
        th.Property("Number", th.StringType, description="Confirmation number"),
        th.Property("State", th.StringType, description="Service order state"),
        th.Property("Origin", th.StringType, description="Service order origin"),
        th.Property("CommanderOrigin", th.StringType, description="Commander origin"),
        th.Property("OriginDetails", th.StringType, description="Origin details"),
        th.Property("CreatedUtc", th.DateTimeType, description="Creation timestamp"),
        th.Property("UpdatedUtc", th.DateTimeType, description="Last update timestamp"),
        th.Property("CancelledUtc", th.DateTimeType, description="Cancellation timestamp"),
        th.Property("VoucherId", th.StringType, description="Voucher identifier"),
        th.Property("BusinessSegmentId", th.StringType, description="Business segment identifier"),
        th.Property("LinkedReservationId", th.StringType, description="Linked reservation identifier"),
        th.Property("Options", th.ObjectType(), description="Service order options"),
    ).to_dict()

    def _fetch_orderable_service_ids(self) -> list[str]:
        """Fetch all Orderable service IDs from the API.

        Returns:
            List of service IDs for Orderable-type services.
        """
        import requests

        if self._orderable_service_ids is not None:
            return self._orderable_service_ids

        url = f"{self.url_base}/services/getAll"
        body = {
            "ClientToken": self.config["client_token"],
            "AccessToken": self.config["access_token"],
            "Client": self.config.get("client_name", "BBGMeltano 1.0.0"),
            "Limitation": {"Count": 1000},
        }

        enterprise_ids = self.config.get("enterprise_ids")
        if enterprise_ids:
            body["EnterpriseIds"] = (
                enterprise_ids if isinstance(enterprise_ids, list) else [enterprise_ids]
            )

        self.logger.info("Fetching Orderable service IDs for product_service_orders")
        response = requests.post(url, json=body, timeout=60)
        response.raise_for_status()

        data = response.json()
        services = data.get("Services", [])

        # Filter to only Orderable services (Type == "Orderable")
        orderable_ids = [s["Id"] for s in services if s.get("Type") == "Orderable"]
        self.logger.info(f"Found {len(orderable_ids)} Orderable services")

        self._orderable_service_ids = orderable_ids
        return orderable_ids

    @property
    def partitions(self) -> list[dict] | None:
        """Return 90-day windows from bookmark/start_date up to now."""
        from datetime import datetime, timedelta, timezone

        # First check if we have any Orderable services
        service_ids = self._fetch_orderable_service_ids()
        if not service_ids:
            self.logger.warning("No Orderable services found, skipping product_service_orders")
            return []

        max_interval = timedelta(days=90)
        now = datetime.now(timezone.utc)

        start = self.get_starting_timestamp(context=None)
        if start is None:
            start_str = self.config.get("start_date")
            if start_str:
                start = datetime.fromisoformat(str(start_str).replace("Z", "+00:00"))
            else:
                start = now - max_interval

        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)

        if start > now:
            start = now - max_interval

        partitions: list[dict] = []
        cursor = start
        while cursor < now:
            window_end = min(cursor + max_interval, now)
            partitions.append({"window_start": cursor, "window_end": window_end})
            cursor = window_end

        if not partitions:
            partitions.append({"window_start": start, "window_end": now})

        return partitions

    def prepare_request_payload(
        self,
        context: dict | None,
        next_page_token: str | None,
    ) -> dict | None:
        """Prepare request payload with ServiceIds, enterprise, and date filters."""
        from datetime import datetime, timedelta, timezone

        body = super().prepare_request_payload(context, next_page_token)

        # Add ALL Orderable ServiceIds (required by API)
        service_ids = self._fetch_orderable_service_ids()
        if service_ids:
            body["ServiceIds"] = service_ids
        else:
            self.logger.warning("No Orderable service IDs available")

        # Add EnterpriseIds filter
        enterprise_ids = self.config.get("enterprise_ids")
        if enterprise_ids:
            body["EnterpriseIds"] = (
                enterprise_ids if isinstance(enterprise_ids, list) else [enterprise_ids]
            )

        # Use window-based filtering from partitions
        max_interval = timedelta(days=90)
        now = datetime.now(timezone.utc)

        window_start = None
        window_end = None
        if context:
            window_start = context.get("window_start")
            window_end = context.get("window_end")

        if window_start is None or window_end is None:
            derived_start = self.get_starting_timestamp(context)
            if derived_start is None:
                start_date_str = self.config.get("start_date")
                if start_date_str:
                    derived_start = datetime.fromisoformat(
                        str(start_date_str).replace("Z", "+00:00")
                    )
                else:
                    derived_start = now - max_interval

            if derived_start.tzinfo is None:
                derived_start = derived_start.replace(tzinfo=timezone.utc)

            window_start = max(derived_start, now - max_interval)
            window_end = min(window_start + max_interval, now)

        if window_start.tzinfo is None:
            window_start = window_start.replace(tzinfo=timezone.utc)
        if window_end.tzinfo is None:
            window_end = window_end.replace(tzinfo=timezone.utc)

        start_utc = window_start.isoformat(timespec="seconds").replace("+00:00", "Z")
        end_utc = window_end.isoformat(timespec="seconds").replace("+00:00", "Z")

        body["UpdatedUtc"] = {"StartUtc": start_utc, "EndUtc": end_utc}

        self.logger.info(
            f"Querying product_service_orders with {len(service_ids)} ServiceIds, "
            f"UpdatedUtc: {start_utc} to {end_utc}"
        )

        return body


class AccountingCategoriesStream(MewsStream):
    """Stream for accounting categories."""

    name = "accounting_categories"
    path = "/accountingCategories/getAll"
    primary_keys = ("Id",)
    replication_key = "UpdatedUtc"
    records_key = "AccountingCategories"

    schema = th.PropertiesList(
        th.Property("Id", th.StringType, description="Accounting category identifier"),
        th.Property("EnterpriseId", th.StringType, description="Enterprise identifier"),
        th.Property("IsActive", th.BooleanType, description="Active state flag"),
        th.Property("Name", th.StringType, description="Name"),
        th.Property("Code", th.StringType, description="Code"),
        th.Property("ExternalCode", th.StringType, description="External code"),
        th.Property("LedgerAccountCode", th.StringType, description="Ledger account code"),
        th.Property("PostingAccountCode", th.StringType, description="Posting account code"),
        th.Property("CostCenterCode", th.StringType, description="Cost center code"),
        th.Property("Classification", th.StringType, description="Classification"),
        th.Property("CreatedUtc", th.DateTimeType, description="Creation timestamp"),
        th.Property("UpdatedUtc", th.DateTimeType, description="Last update timestamp"),
    ).to_dict()

    def prepare_request_payload(
        self,
        context: dict | None,
        next_page_token: str | None,
    ) -> dict | None:
        body = super().prepare_request_payload(context, next_page_token)
        body["EnterpriseIds"] = _require_enterprise_ids(self.config, self.name)
        return body


class LedgerBalancesStream(MewsStream):
    """Stream for ledger balances (opening/closing) by day."""

    name = "ledger_balances"
    path = "/ledgerBalances/getAll"
    page_size = 100
    primary_keys = ("EnterpriseId", "Date", "LedgerType")
    replication_key = "Date"
    records_key = "LedgerBalances"

    @property
    def partitions(self) -> list[dict] | None:
        """Return daily date windows from bookmark/start_date up to today (UTC)."""
        from datetime import datetime, timedelta, timezone

        now = datetime.now(timezone.utc)
        end_date = now.date()

        start = self.get_starting_timestamp(context=None)
        if start is None:
            start_str = self.config.get("start_date")
            if start_str:
                start = datetime.fromisoformat(str(start_str).replace("Z", "+00:00"))
            else:
                start = now - timedelta(days=30)

        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)

        start_date = start.date()
        if start_date > end_date:
            start_date = end_date

        partitions: list[dict] = []
        cursor_date = start_date
        while cursor_date <= end_date:
            partitions.append(
                {
                    "window_start": datetime(
                        cursor_date.year,
                        cursor_date.month,
                        cursor_date.day,
                        tzinfo=timezone.utc,
                    ),
                    "window_end": datetime(
                        cursor_date.year,
                        cursor_date.month,
                        cursor_date.day,
                        tzinfo=timezone.utc,
                    ),
                }
            )
            cursor_date = cursor_date + timedelta(days=1)

        if not partitions:
            partitions.append({"window_start": now, "window_end": now})

        return partitions

    def prepare_request_payload(
        self,
        context: dict | None,
        next_page_token: str | None,
    ) -> dict | None:
        """Prepare request payload with date interval, ledger types, and enterprise filter."""
        from datetime import datetime, timezone

        body = super().prepare_request_payload(context, next_page_token)

        ledger_types = self.config.get("ledger_types") or [
            "Revenue",
            "Tax",
            "Payment",
            "Deposit",
            "Guest",
            "City",
        ]
        if isinstance(ledger_types, str):
            ledger_types = [ledger_types]

        filtered_ledger_types = [
            ledger_type
            for ledger_type in ledger_types
            if str(ledger_type).lower() != "nonrevenue".lower()
        ]
        if len(filtered_ledger_types) != len(list(ledger_types)):
            self.logger.warning(
                "Removing unsupported LedgerType=%s from ledger_balances request",
                "NonRevenue",
            )

        body["LedgerTypes"] = list(filtered_ledger_types)

        enterprise_ids = self.config.get("enterprise_ids")
        if enterprise_ids:
            body["EnterpriseIds"] = (
                enterprise_ids if isinstance(enterprise_ids, list) else [enterprise_ids]
            )

        window_start = context.get("window_start") if context else None
        window_end = context.get("window_end") if context else None
        if not isinstance(window_start, datetime) or not isinstance(window_end, datetime):
            raise ValueError(
                "ledger_balances requires partition context with window_start/window_end"
            )

        start_date = window_start.astimezone(timezone.utc).date()
        end_date = window_end.astimezone(timezone.utc).date()

        body["Date"] = {"Start": start_date.isoformat(), "End": end_date.isoformat()}

        # Log request params once per partition (not for every paginated page).
        if next_page_token is None:
            self.logger.info(
                "Querying ledger_balances with Date=%s..%s, LedgerTypes=%s, EnterpriseIds=%s",
                body["Date"]["Start"],
                body["Date"]["End"],
                body.get("LedgerTypes"),
                body.get("EnterpriseIds"),
            )

            import json

            safe_body = {
                k: v for k, v in body.items() if k not in ["ClientToken", "AccessToken"]
            }
            safe_body["ClientToken"] = "***"
            safe_body["AccessToken"] = "***"
            self.logger.info(
                "Full ledger_balances request body: %s", json.dumps(safe_body, indent=2)
            )

        return body

    _amount_schema = th.ObjectType(
        th.Property("Currency", th.StringType),
        th.Property("NetValue", th.NumberType),
        th.Property("GrossValue", th.NumberType),
        th.Property(
            "TaxValues",
            th.ArrayType(
                th.ObjectType(
                    th.Property("Code", th.StringType),
                    th.Property("Value", th.NumberType),
                )
            ),
        ),
        th.Property(
            "Breakdown",
            th.ObjectType(
                th.Property(
                    "Items",
                    th.ArrayType(
                        th.ObjectType(
                            th.Property("TaxRateCode", th.StringType),
                            th.Property("NetValue", th.NumberType),
                            th.Property("TaxValue", th.NumberType),
                        )
                    ),
                )
            ),
        ),
    )

    schema = th.PropertiesList(
        th.Property(
            "EnterpriseId",
            th.StringType,
            description="Unique identifier of the Enterprise.",
        ),
        th.Property("Date", th.DateType, description="Day for which the ledger balance applies."),
        th.Property("LedgerType", th.StringType, description="Type of accounting ledger."),
        th.Property(
            "OpeningBalance",
            _amount_schema,
            description="Opening balance at the start of the day.",
        ),
        th.Property(
            "ClosingBalance",
            _amount_schema,
            description="Closing balance at the end of the day.",
        ),
    ).to_dict()


class LedgerEntriesStream(MewsStream):
    """Stream for ledger entries (individual accounting entries/transactions)."""

    name = "ledger_entries"
    path = "/ledgerEntries/getAll"
    page_size = 100
    primary_keys = ("Id",)
    replication_key = "CreatedUtc"
    records_key = "LedgerEntries"

    @property
    def partitions(self) -> list[dict] | None:
        """Return daily date windows from bookmark/start_date up to today (UTC)."""
        from datetime import datetime, timedelta, timezone

        now = datetime.now(timezone.utc)
        end_date = now.date()

        start = self.get_starting_timestamp(context=None)
        if start is None:
            start_str = self.config.get("start_date")
            if start_str:
                start = datetime.fromisoformat(str(start_str).replace("Z", "+00:00"))
            else:
                start = now - timedelta(days=30)

        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)

        start_date = start.date()
        if start_date > end_date:
            start_date = end_date

        partitions: list[dict] = []
        cursor_date = start_date
        while cursor_date <= end_date:
            partitions.append(
                {
                    "window_start": datetime(
                        cursor_date.year,
                        cursor_date.month,
                        cursor_date.day,
                        tzinfo=timezone.utc,
                    ),
                    "window_end": datetime(
                        cursor_date.year,
                        cursor_date.month,
                        cursor_date.day,
                        tzinfo=timezone.utc,
                    ),
                }
            )
            cursor_date = cursor_date + timedelta(days=1)

        if not partitions:
            partitions.append({"window_start": now, "window_end": now})

        return partitions

    def prepare_request_payload(
        self,
        context: dict | None,
        next_page_token: str | None,
    ) -> dict | None:
        """Prepare request payload with PostingDate interval, ledger types, and enterprise filter."""
        from datetime import datetime, timezone

        body = super().prepare_request_payload(context, next_page_token)

        ledger_types = self.config.get("ledger_types") or [
            "Revenue",
            "Tax",
            "Payment",
            "Deposit",
            "Guest",
            "City",
        ]
        if isinstance(ledger_types, str):
            ledger_types = [ledger_types]

        body["LedgerTypes"] = list(ledger_types)

        enterprise_ids = self.config.get("enterprise_ids")
        if enterprise_ids:
            body["EnterpriseIds"] = (
                enterprise_ids if isinstance(enterprise_ids, list) else [enterprise_ids]
            )

        window_start = context.get("window_start") if context else None
        window_end = context.get("window_end") if context else None
        if not isinstance(window_start, datetime) or not isinstance(window_end, datetime):
            raise ValueError(
                "ledger_entries requires partition context with window_start/window_end"
            )

        start_date = window_start.astimezone(timezone.utc).date()
        end_date = window_end.astimezone(timezone.utc).date()

        body["PostingDate"] = {"Start": start_date.isoformat(), "End": end_date.isoformat()}

        if next_page_token is None:
            self.logger.info(
                "Querying ledger_entries with PostingDate=%s..%s, LedgerTypes=%s, EnterpriseIds=%s",
                body["PostingDate"]["Start"],
                body["PostingDate"]["End"],
                body.get("LedgerTypes"),
                body.get("EnterpriseIds"),
            )

        return body

    schema = th.PropertiesList(
        th.Property("Id", th.StringType, description="Unique identifier"),
        th.Property("EnterpriseId", th.StringType, description="Enterprise identifier"),
        th.Property("AccountId", th.StringType, description="Account identifier"),
        th.Property("BillId", th.StringType, description="Bill identifier"),
        th.Property("AccountingCategoryId", th.StringType, description="Accounting category identifier"),
        th.Property("AccountingItemId", th.StringType, description="Accounting item identifier"),
        th.Property("AccountingItemType", th.StringType, description="Type of accounting item"),
        th.Property("LedgerType", th.StringType, description="Type of ledger (Revenue, Tax, etc.)"),
        th.Property("LedgerEntryType", th.StringType, description="Entry type"),
        th.Property("PostingDate", th.DateType, description="Posting date"),
        th.Property("Value", th.NumberType, description="Monetary value"),
        th.Property("NetBaseValue", th.NumberType, description="Net base value"),
        th.Property("TaxRateCode", th.StringType, description="Tax rate code"),
        th.Property("CreatedUtc", th.DateTimeType, description="Creation timestamp (UTC)"),
    ).to_dict()


class BusinessSegmentsStream(MewsStream):
    """Stream for business segments."""

    name = "business_segments"
    path = "/businessSegments/getAll"
    primary_keys = ("Id",)
    replication_key = "UpdatedUtc"
    records_key = "BusinessSegments"

    schema = th.PropertiesList(
        th.Property("Id", th.StringType, description="Business segment identifier"),
        th.Property("EnterpriseId", th.StringType, description="Enterprise identifier"),
        th.Property("ServiceId", th.StringType, description="Service identifier"),
        th.Property("IsActive", th.BooleanType, description="Active flag"),
        th.Property("Name", th.StringType, description="Business segment name"),
        th.Property("CreatedUtc", th.DateTimeType, description="Creation timestamp"),
        th.Property("UpdatedUtc", th.DateTimeType, description="Last update timestamp"),
    ).to_dict()

    def prepare_request_payload(
        self,
        context: dict | None,
        next_page_token: str | None,
    ) -> dict | None:
        body = super().prepare_request_payload(context, next_page_token)
        body["EnterpriseIds"] = _require_enterprise_ids(self.config, self.name)
        return body


class SourcesStream(MewsStream):
    """Stream for sources (distribution channels)."""

    name = "sources"
    path = "/sources/getAll"
    primary_keys = ("Id",)
    replication_key = "UpdatedUtc"
    records_key = "Sources"

    schema = th.PropertiesList(
        th.Property("Id", th.StringType, description="Source identifier"),
        th.Property("EnterpriseId", th.StringType, description="Enterprise identifier"),
        th.Property("Name", th.StringType, description="Source name"),
        th.Property("Type", th.StringType, description="Source type"),
        th.Property("Code", th.IntegerType, description="Numeric code"),
        th.Property("IsActive", th.BooleanType, description="Active flag"),
        th.Property("CreatedUtc", th.DateTimeType, description="Creation timestamp"),
        th.Property("UpdatedUtc", th.DateTimeType, description="Last update timestamp"),
    ).to_dict()

    def prepare_request_payload(
        self,
        context: dict | None,
        next_page_token: str | None,
    ) -> dict | None:
        body = super().prepare_request_payload(context, next_page_token)
        body["EnterpriseIds"] = _require_enterprise_ids(self.config, self.name)
        return body


class CompaniesStream(MewsStream):
    """Stream for companies (corporate accounts)."""

    name = "companies"
    path = "/companies/getAll"
    primary_keys = ("Id",)
    replication_key = "UpdatedUtc"
    records_key = "Companies"

    schema = th.PropertiesList(
        th.Property("Id", th.StringType, description="Company identifier"),
        th.Property("ChainId", th.StringType, description="Chain identifier"),
        th.Property("Name", th.StringType, description="Company name"),
        th.Property("MotherCompanyId", th.StringType, description="Parent company identifier"),
        th.Property("InvoicingEmail", th.StringType, description="Invoicing email"),
        th.Property("WebsiteUrl", th.StringType, description="Website URL"),
        th.Property("InvoiceDueInterval", th.IntegerType, description="Invoice due interval"),
        th.Property(
            "NchClassifications",
            th.ObjectType(
                th.Property("Corporate", th.BooleanType),
                th.Property("Internal", th.BooleanType),
                th.Property("Private", th.BooleanType),
                th.Property("OnlineTravelAgency", th.BooleanType),
                th.Property("GlobalDistributionSystem", th.BooleanType),
                th.Property("Marketing", th.BooleanType),
                th.Property("Inactive", th.BooleanType),
                th.Property("GovernmentEntity", th.BooleanType),
            ),
            description="NCH classification flags",
        ),
        th.Property(
            "Options",
            th.ObjectType(
                th.Property("Invoiceable", th.BooleanType),
                th.Property("AddFeesToInvoices", th.BooleanType),
                th.Property("AddTaxDeductedPaymentToInvoices", th.BooleanType),
            ),
            description="Company options",
        ),
        th.Property(
            "CreditRating",
            th.ObjectType(
                th.Property("Basic", th.StringType),
            ),
            description="Credit rating data",
        ),
        th.Property("Department", th.StringType, description="Department"),
        th.Property("DunsNumber", th.StringType, description="DUNS number"),
        th.Property("ReferenceIdentifier", th.StringType, description="Reference identifier"),
        th.Property("AccountingCode", th.StringType, description="Accounting code"),
        th.Property("AdditionalTaxIdentifier", th.StringType, description="Additional tax ID"),
        th.Property("BillingCode", th.StringType, description="Billing code"),
        th.Property(
            "Contact",
            th.CustomType({"type": ["object", "string", "null"]}),
            description="Contact details (can be an object or plain string)",
        ),
        th.Property("ContactPerson", th.ObjectType(), description="Contact person details"),
        th.Property("ElectronicInvoiceIdentifier", th.StringType, description="Electronic invoice ID"),
        th.Property("Identifier", th.StringType, description="Identifier"),
        th.Property("Iata", th.StringType, description="IATA number"),
        th.Property("IsActive", th.BooleanType, description="Active flag"),
        th.Property("Notes", th.StringType, description="Notes"),
        th.Property("Number", th.IntegerType, description="Company number"),
        th.Property("TaxIdentifier", th.StringType, description="Tax identifier"),
        th.Property("Telephone", th.StringType, description="Telephone"),
        th.Property("CreatedUtc", th.DateTimeType, description="Creation timestamp"),
        th.Property("UpdatedUtc", th.DateTimeType, description="Last update timestamp"),
        th.Property("Address", th.ObjectType(), description="Address object"),
        th.Property("AddressId", th.StringType, description="Address identifier"),
        th.Property("MergeTargetId", th.StringType, description="Merge target identifier"),
        th.Property("TaxIdentificationNumber", th.StringType, description="Tax identification number"),
        th.Property("ExternalIdentifier", th.StringType, description="External identifier"),
        th.Property("IsUpdatedByMe", th.BooleanType, description="Flag indicating if updated by this client"),
    ).to_dict()

class CompanionshipsStream(MewsChildStream):
    """Stream for companionships linked to reservations."""

    name = "companionships"
    path = "/companionships/getAll"
    primary_keys = ("Id",)
    replication_key = None
    records_key = "Companionships"
    parent_stream_type = ReservationsStream
    requires_service_id = False

    schema = th.PropertiesList(
        th.Property("Id", th.StringType, description="Companionship identifier"),
        th.Property("CustomerId", th.StringType, description="Customer identifier"),
        th.Property("ReservationGroupId", th.StringType, description="Reservation group identifier"),
        th.Property("ReservationId", th.StringType, description="Reservation identifier"),
        th.Property("Reservations", th.ArrayType(th.ObjectType()), description="Reservations payload"),
        th.Property(
            "ReservationGroups",
            th.ArrayType(th.ObjectType()),
            description="Reservation group payload",
        ),
        th.Property("Customers", th.ArrayType(th.ObjectType()), description="Customer payload"),
    ).to_dict()

    def prepare_request_payload(
        self,
        context: dict | None,
        next_page_token: str | None,
    ) -> dict | None:
        body = super().prepare_request_payload(context, next_page_token)

        if context and "reservation_id" in context:
            body["ReservationIds"] = [context["reservation_id"]]

        return body


class ReservationGroupsStream(MewsChildStream):
    """Stream for reservation groups, child of reservations.

    Fetches reservation group details for GroupIds found in reservations.
    Handles deduplication to avoid redundant API calls for the same group.
    """

    name = "reservation_groups"
    path = "/reservationGroups/getAll"
    primary_keys = ("Id",)
    replication_key = None  # No incremental - fetched via parent
    records_key = "ReservationGroups"
    parent_stream_type = ReservationsStream
    requires_service_id = False

    # Track processed group IDs to avoid duplicate API calls
    _seen_group_ids: set[str] = set()

    schema = th.PropertiesList(
        th.Property("Id", th.StringType, description="Unique identifier of the reservation group"),
        th.Property("EnterpriseId", th.StringType, description="Enterprise the reservation group belongs to"),
        th.Property("Name", th.StringType, description="Name of the reservation group"),
        th.Property("ChannelManager", th.StringType, description="Name of the corresponding channel manager"),
        th.Property("ChannelManagerGroupNumber", th.StringType, description="Identifier of the channel manager"),
    ).to_dict()

    def prepare_request_payload(
        self,
        context: dict | None,
        next_page_token: str | None,
    ) -> dict | None:
        """Prepare request with ReservationGroupIds."""
        body = super().prepare_request_payload(context, next_page_token)
        group_id = context.get("group_id") if context else None
        if group_id:
            body["ReservationGroupIds"] = [group_id]
        return body

    def request_records(self, context: dict | None):
        """Override to skip requests for missing or duplicate group IDs."""
        group_id = context.get("group_id") if context else None

        # Skip if no group_id (reservation had no group)
        if not group_id:
            return

        # Skip if already processed (deduplication)
        if group_id in self._seen_group_ids:
            return

        # Mark as seen before making request
        self._seen_group_ids.add(group_id)

        yield from super().request_records(context)


class OrderItemsStream(MewsStream):
    """Stream for order items.

    Independent stream that queries all order items using UpdatedUtc time interval.
    Captures items from ALL services (accommodation, F&B outlets, spa, etc.).
    """

    name = "order_items"
    path = "/orderItems/getAll"
    primary_keys = ("Id",)
    replication_key = "UpdatedUtc"
    records_key = "OrderItems"

    @property
    def partitions(self) -> list[dict] | None:
        """Return 90-day windows from bookmark/start_date up to now."""
        from datetime import datetime, timedelta, timezone

        max_interval = timedelta(days=90)
        now = datetime.now(timezone.utc)

        start = self.get_starting_timestamp(context=None)
        if start is None:
            start_str = self.config.get("start_date")
            if start_str:
                start = datetime.fromisoformat(str(start_str).replace("Z", "+00:00"))
            else:
                start = now - max_interval

        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)

        if start > now:
            start = now - max_interval

        partitions: list[dict] = []
        cursor = start
        while cursor < now:
            window_end = min(cursor + max_interval, now)
            partitions.append({"window_start": cursor, "window_end": window_end})
            cursor = window_end

        if not partitions:
            partitions.append({"window_start": start, "window_end": now})

        return partitions

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
        """Prepare request payload with UpdatedUtc time interval filter.

        Order items are queried using UpdatedUtc time interval (max 3 months).
        This captures ALL order items across all services.
        """
        from datetime import datetime, timedelta, timezone

        body = super().prepare_request_payload(context, next_page_token)

        # Add EnterpriseIds filter
        enterprise_ids = self.config.get("enterprise_ids")
        if enterprise_ids:
            body["EnterpriseIds"] = (
                enterprise_ids if isinstance(enterprise_ids, list) else [enterprise_ids]
            )

        max_interval = timedelta(days=90)
        now = datetime.now(timezone.utc)

        window_start = None
        window_end = None
        if context:
            window_start = context.get("window_start")
            window_end = context.get("window_end")

        if window_start is None or window_end is None:
            derived_start = self.get_starting_timestamp(context)
            if derived_start is None:
                start_date_str = self.config.get("start_date")
                if start_date_str:
                    derived_start = datetime.fromisoformat(
                        str(start_date_str).replace("Z", "+00:00")
                    )
                else:
                    derived_start = now - max_interval

            if derived_start.tzinfo is None:
                derived_start = derived_start.replace(tzinfo=timezone.utc)

            window_start = max(derived_start, now - max_interval)
            window_end = min(window_start + max_interval, now)

        if window_start.tzinfo is None:
            window_start = window_start.replace(tzinfo=timezone.utc)
        if window_end.tzinfo is None:
            window_end = window_end.replace(tzinfo=timezone.utc)

        start_utc = window_start.isoformat(timespec="seconds").replace("+00:00", "Z")
        end_utc = window_end.isoformat(timespec="seconds").replace("+00:00", "Z")

        body["UpdatedUtc"] = {
            "StartUtc": start_utc,
            "EndUtc": end_utc,
        }

        self.logger.info(
            f"Querying order_items with UpdatedUtc: {start_utc} to {end_utc}"
        )

        return body


class BillsStream(MewsStream):
    """Stream for ALL bills.

    Independent stream that queries all bills using UpdatedUtc time interval.
    Captures bills that may not have order items yet.
    """

    name = "bills"
    path = "/bills/getAll"
    primary_keys = ("Id",)
    replication_key = "UpdatedUtc"
    records_key = "Bills"

    @property
    def partitions(self) -> list[dict] | None:
        """Return 90-day windows from bookmark/start_date up to now."""
        from datetime import datetime, timedelta, timezone

        max_interval = timedelta(days=90)
        now = datetime.now(timezone.utc)

        start = self.get_starting_timestamp(context=None)
        if start is None:
            start_str = self.config.get("start_date")
            if start_str:
                start = datetime.fromisoformat(str(start_str).replace("Z", "+00:00"))
            else:
                start = now - max_interval

        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)

        if start > now:
            start = now - max_interval

        partitions: list[dict] = []
        cursor = start
        while cursor < now:
            window_end = min(cursor + max_interval, now)
            partitions.append({"window_start": cursor, "window_end": window_end})
            cursor = window_end

        if not partitions:
            partitions.append({"window_start": start, "window_end": now})

        return partitions

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
        th.Property("Revenue", th.CustomType({"type": ["object", "array", "null"]}), description="Revenue amounts (can be object or empty array)"),
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
        """Prepare request payload with UpdatedUtc time interval filter.

        Bills are queried using UpdatedUtc time interval (max 3 months).
        This captures ALL bills including those without order items.
        """
        from datetime import datetime, timedelta, timezone

        body = super().prepare_request_payload(context, next_page_token)

        # Add EnterpriseIds filter
        enterprise_ids = self.config.get("enterprise_ids")
        if enterprise_ids:
            body["EnterpriseIds"] = (
                enterprise_ids if isinstance(enterprise_ids, list) else [enterprise_ids]
            )

        max_interval = timedelta(days=90)
        now = datetime.now(timezone.utc)

        window_start = None
        window_end = None
        if context:
            window_start = context.get("window_start")
            window_end = context.get("window_end")

        if window_start is None or window_end is None:
            derived_start = self.get_starting_timestamp(context)
            if derived_start is None:
                start_date_str = self.config.get("start_date")
                if start_date_str:
                    derived_start = datetime.fromisoformat(
                        str(start_date_str).replace("Z", "+00:00")
                    )
                else:
                    derived_start = now - max_interval

            if derived_start.tzinfo is None:
                derived_start = derived_start.replace(tzinfo=timezone.utc)

            window_start = max(derived_start, now - max_interval)
            window_end = min(window_start + max_interval, now)

        if window_start.tzinfo is None:
            window_start = window_start.replace(tzinfo=timezone.utc)
        if window_end.tzinfo is None:
            window_end = window_end.replace(tzinfo=timezone.utc)

        start_utc = window_start.isoformat(timespec="seconds").replace("+00:00", "Z")
        end_utc = window_end.isoformat(timespec="seconds").replace("+00:00", "Z")

        body["UpdatedUtc"] = {
            "StartUtc": start_utc,
            "EndUtc": end_utc,
        }

        self.logger.info(
            f"Querying bills with UpdatedUtc: {start_utc} to {end_utc}"
        )

        return body


class PaymentsStream(MewsStream):
    """Stream for ALL payments (including unbilled).

    Independent stream that queries all payments using UpdatedUtc time interval.
    Captures payments that may not be linked to bills (e.g., Cross Settlement).
    """

    name = "payments"
    path = "/payments/getAll"
    primary_keys = ("Id",)
    replication_key = "UpdatedUtc"
    records_key = "Payments"

    @property
    def partitions(self) -> list[dict] | None:
        """Return 90-day windows from bookmark/start_date up to now."""
        from datetime import datetime, timedelta, timezone

        max_interval = timedelta(days=90)
        now = datetime.now(timezone.utc)

        start = self.get_starting_timestamp(context=None)
        if start is None:
            start_str = self.config.get("start_date")
            if start_str:
                start = datetime.fromisoformat(str(start_str).replace("Z", "+00:00"))
            else:
                start = now - max_interval

        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)

        if start > now:
            start = now - max_interval

        partitions: list[dict] = []
        cursor = start
        while cursor < now:
            window_end = min(cursor + max_interval, now)
            partitions.append({"window_start": cursor, "window_end": window_end})
            cursor = window_end

        if not partitions:
            partitions.append({"window_start": start, "window_end": now})

        return partitions

    schema = th.PropertiesList(
        th.Property("Id", th.StringType, description="Unique identifier"),
        th.Property("EnterpriseId", th.StringType, description="Enterprise ID"),
        th.Property("AccountId", th.StringType, description="Account ID"),
        th.Property("AccountType", th.StringType, description="Account type"),
        th.Property("BillId", th.StringType, description="Bill ID"),
        th.Property("ReservationId", th.StringType, description="Reservation ID"),
        th.Property("AccountingCategoryId", th.StringType, description="Accounting category ID"),
        th.Property("Amount", th.ObjectType(
            th.Property("Currency", th.StringType),
            th.Property("NetValue", th.NumberType),
            th.Property("GrossValue", th.NumberType),
            th.Property("TaxValues", th.ArrayType(th.ObjectType())),
            th.Property("Breakdown", th.ObjectType()),
        ), description="Payment amount"),
        th.Property("OriginalAmount", th.ObjectType(
            th.Property("Currency", th.StringType),
            th.Property("NetValue", th.NumberType),
            th.Property("GrossValue", th.NumberType),
            th.Property("TaxValues", th.ArrayType(th.ObjectType())),
            th.Property("Breakdown", th.ObjectType()),
        ), description="Original payment amount"),
        th.Property("Notes", th.StringType, description="Notes"),
        th.Property("SettlementId", th.StringType, description="Settlement ID"),
        th.Property("ConsumedUtc", th.DateTimeType, description="Consumed timestamp"),
        th.Property("ClosedUtc", th.DateTimeType, description="Closed timestamp"),
        th.Property("ChargedUtc", th.DateTimeType, description="Charged timestamp"),
        th.Property("CreatedUtc", th.DateTimeType, description="Creation timestamp"),
        th.Property("UpdatedUtc", th.DateTimeType, description="Last update timestamp"),
        th.Property("SettlementUtc", th.DateTimeType, description="Settlement timestamp"),
        th.Property("AccountingState", th.StringType, description="Accounting state"),
        th.Property("State", th.StringType, description="Payment state"),
        th.Property("Identifier", th.StringType, description="Payment identifier"),
        th.Property("Type", th.StringType, description="Payment type"),
        th.Property("Kind", th.StringType, description="Payment kind"),
        th.Property("Data", th.ObjectType(
            th.Property("Discriminator", th.StringType),
            th.Property("CreditCard", th.ObjectType()),
            th.Property("Invoice", th.ObjectType()),
            th.Property("External", th.ObjectType(
                th.Property("Type", th.StringType),
                th.Property("ExternalIdentifier", th.StringType),
            )),
            th.Property("Ghost", th.ObjectType()),
        ), description="Payment data"),
        th.Property("PaymentOrigin", th.StringType, description="Payment origin"),
    ).to_dict()

    def prepare_request_payload(
        self,
        context: dict | None,
        next_page_token: str | None,
    ) -> dict | None:
        """Prepare request payload with UpdatedUtc time interval filter.

        Payments are queried using UpdatedUtc time interval (max 3 months).
        This captures ALL payments across all services, including unbilled ones.
        """
        from datetime import datetime, timedelta, timezone

        body = super().prepare_request_payload(context, next_page_token)

        # Add EnterpriseIds filter
        enterprise_ids = self.config.get("enterprise_ids")
        if enterprise_ids:
            body["EnterpriseIds"] = (
                enterprise_ids if isinstance(enterprise_ids, list) else [enterprise_ids]
            )

        max_interval = timedelta(days=90)
        now = datetime.now(timezone.utc)

        window_start = None
        window_end = None
        if context:
            window_start = context.get("window_start")
            window_end = context.get("window_end")

        if window_start is None or window_end is None:
            derived_start = self.get_starting_timestamp(context)
            if derived_start is None:
                start_date_str = self.config.get("start_date")
                if start_date_str:
                    derived_start = datetime.fromisoformat(
                        str(start_date_str).replace("Z", "+00:00")
                    )
                else:
                    derived_start = now - max_interval

            if derived_start.tzinfo is None:
                derived_start = derived_start.replace(tzinfo=timezone.utc)

            window_start = max(derived_start, now - max_interval)
            window_end = min(window_start + max_interval, now)

        if window_start.tzinfo is None:
            window_start = window_start.replace(tzinfo=timezone.utc)
        if window_end.tzinfo is None:
            window_end = window_end.replace(tzinfo=timezone.utc)

        start_utc = window_start.isoformat(timespec="seconds").replace("+00:00", "Z")
        end_utc = window_end.isoformat(timespec="seconds").replace("+00:00", "Z")

        body["UpdatedUtc"] = {
            "StartUtc": start_utc,
            "EndUtc": end_utc,
        }

        self.logger.info(
            f"Querying payments with UpdatedUtc: {start_utc} to {end_utc}"
        )

        return body


class PaymentRequestsStream(MewsStream):
    """Stream for payment requests."""

    name = "payment_requests"
    path = "/paymentRequests/getAll"
    primary_keys = ("Id",)
    replication_key = "UpdatedUtc"
    records_key = "PaymentRequests"

    @property
    def partitions(self) -> list[dict] | None:
        """Return 90-day windows from bookmark/start_date up to now."""
        from datetime import datetime, timedelta, timezone

        max_interval = timedelta(days=90)
        now = datetime.now(timezone.utc)

        start = self.get_starting_timestamp(context=None)
        if start is None:
            start_str = self.config.get("start_date")
            if start_str:
                start = datetime.fromisoformat(str(start_str).replace("Z", "+00:00"))
            else:
                start = now - max_interval

        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)

        if start > now:
            start = now - max_interval

        partitions: list[dict] = []
        cursor = start
        while cursor < now:
            window_end = min(cursor + max_interval, now)
            partitions.append({"window_start": cursor, "window_end": window_end})
            cursor = window_end

        if not partitions:
            partitions.append({"window_start": start, "window_end": now})

        return partitions

    schema = th.PropertiesList(
        th.Property("Id", th.StringType, description="Payment request identifier"),
        th.Property("EnterpriseId", th.StringType, description="Enterprise identifier"),
        th.Property("AccountId", th.StringType, description="Account identifier"),
        th.Property("CustomerId", th.StringType, description="Deprecated customer identifier"),
        th.Property("ReservationGroupId", th.StringType, description="Reservation group identifier"),
        th.Property("ReservationId", th.StringType, description="Reservation identifier"),
        th.Property("State", th.StringType, description="Payment request state"),
        th.Property(
            "Amount",
            th.ObjectType(
                th.Property("Currency", th.StringType),
                th.Property("NetValue", th.NumberType),
                th.Property("GrossValue", th.NumberType),
                th.Property(
                    "TaxValues",
                    th.ArrayType(
                        th.ObjectType(
                            th.Property("Code", th.StringType),
                            th.Property("Value", th.NumberType),
                        )
                    ),
                ),
                th.Property(
                    "Breakdown",
                    th.ObjectType(
                        th.Property(
                            "Items",
                            th.ArrayType(
                                th.ObjectType(
                                    th.Property("TaxRateCode", th.StringType),
                                    th.Property("NetValue", th.NumberType),
                                    th.Property("TaxValue", th.NumberType),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
            description="Requested amount",
        ),
        th.Property("Type", th.StringType, description="Payment request type"),
        th.Property("Reason", th.StringType, description="Payment request reason"),
        th.Property("ExpirationUtc", th.DateTimeType, description="Expiration timestamp"),
        th.Property("Description", th.StringType, description="Payment request description"),
        th.Property("Notes", th.StringType, description="Internal notes"),
        th.Property("CreatedUtc", th.DateTimeType, description="Creation timestamp"),
        th.Property("UpdatedUtc", th.DateTimeType, description="Last update timestamp"),
    ).to_dict()

    def prepare_request_payload(
        self,
        context: dict | None,
        next_page_token: str | None,
    ) -> dict | None:
        """Prepare request payload with optional enterprise filter and date window."""
        from datetime import datetime, timedelta, timezone
        import json

        body = super().prepare_request_payload(context, next_page_token)

        enterprise_ids = self.config.get("enterprise_ids")
        if enterprise_ids:
            body["EnterpriseIds"] = (
                enterprise_ids if isinstance(enterprise_ids, list) else [enterprise_ids]
            )

        max_interval = timedelta(days=90)
        now = datetime.now(timezone.utc)

        window_start = None
        window_end = None
        if context:
            window_start = context.get("window_start")
            window_end = context.get("window_end")

        if window_start is None or window_end is None:
            derived_start = self.get_starting_timestamp(context)
            if derived_start is None:
                start_date_str = self.config.get("start_date")
                if start_date_str:
                    derived_start = datetime.fromisoformat(str(start_date_str).replace("Z", "+00:00"))
                else:
                    derived_start = now - max_interval

            if derived_start.tzinfo is None:
                derived_start = derived_start.replace(tzinfo=timezone.utc)

            window_start = max(derived_start, now - max_interval)
            window_end = min(window_start + max_interval, now)

        if window_start.tzinfo is None:
            window_start = window_start.replace(tzinfo=timezone.utc)
        if window_end.tzinfo is None:
            window_end = window_end.replace(tzinfo=timezone.utc)

        start_utc = window_start.isoformat(timespec="seconds").replace("+00:00", "Z")
        end_utc = window_end.isoformat(timespec="seconds").replace("+00:00", "Z")

        body["UpdatedUtc"] = {"StartUtc": start_utc, "EndUtc": end_utc}

        self.logger.info(
            f"Querying payment_requests with UpdatedUtc: {start_utc} to {end_utc}"
        )

        safe_body = {k: v for k, v in body.items() if k not in ["ClientToken", "AccessToken"]}
        safe_body["ClientToken"] = "***"
        safe_body["AccessToken"] = "***"
        self.logger.info(f"Full payment_requests request body: {json.dumps(safe_body, indent=2)}")

        return body


class AvailabilityBlocksStream(MewsStream):
    """Stream for availability blocks."""

    name = "availability_blocks"
    path = "/availabilityBlocks/getAll"
    primary_keys = ("Id",)
    replication_key = "UpdatedUtc"
    records_key = "AvailabilityBlocks"

    schema = th.PropertiesList(
        th.Property("Id", th.StringType, description="Availability block identifier"),
        th.Property("EnterpriseId", th.StringType, description="Enterprise identifier"),
        th.Property("ServiceId", th.StringType, description="Service identifier"),
        th.Property("RateId", th.StringType, description="Rate identifier"),
        th.Property("VoucherId", th.StringType, description="Voucher identifier"),
        th.Property("BookerId", th.StringType, description="Booker identifier"),
        th.Property("CompanyId", th.StringType, description="Company identifier"),
        th.Property("TravelAgencyId", th.StringType, description="Travel agency identifier"),
        th.Property(
            "Budget",
            th.ObjectType(
                th.Property("Currency", th.StringType),
                th.Property("Value", th.NumberType),
                th.Property("Net", th.NumberType),
                th.Property("Tax", th.NumberType),
                th.Property("TaxRate", th.NumberType),
            ),
            description="Tentative budget for reservations in the block",
        ),
        th.Property("State", th.StringType, description="Availability block state"),
        th.Property("ReservationPurpose", th.StringType, description="Purpose of the block"),
        th.Property("CreatedUtc", th.DateTimeType, description="Creation timestamp"),
        th.Property("UpdatedUtc", th.DateTimeType, description="Last update timestamp"),
        th.Property(
            "FirstTimeUnitStartUtc",
            th.DateTimeType,
            description="Start of the first time unit",
        ),
        th.Property(
            "LastTimeUnitStartUtc",
            th.DateTimeType,
            description="Start of the last time unit",
        ),
        th.Property("ReleasedUtc", th.DateTimeType, description="Fixed release timestamp"),
        th.Property("RollingReleaseOffset", th.StringType, description="Rolling release offset"),
        th.Property("ExternalIdentifier", th.StringType, description="External identifier"),
        th.Property("Name", th.StringType, description="Block name"),
        th.Property("Notes", th.StringType, description="Block notes"),
        th.Property("PickupDistribution", th.StringType, description="Pickup distribution"),
        th.Property("IsActive", th.BooleanType, description="Active flag"),
        th.Property("QuoteId", th.StringType, description="Linked quote identifier"),
        th.Property(
            "AvailabilityBlockNumber",
            th.StringType,
            description="Unique number within Mews",
        ),
        th.Property("ReleaseStrategy", th.StringType, description="Release strategy"),
        th.Property("PurchaseOrderNumber", th.StringType, description="Purchase order number"),
        th.Property("BusinessSegmentId", th.StringType, description="Business segment identifier"),
    ).to_dict()

    @property
    def partitions(self) -> list[dict] | None:
        """Return 90-day windows from bookmark/start_date up to now."""
        from datetime import datetime, timedelta, timezone

        max_interval = timedelta(days=90)
        now = datetime.now(timezone.utc)

        start = self.get_starting_timestamp(context=None)
        if start is None:
            start_str = self.config.get("start_date")
            if start_str:
                start = datetime.fromisoformat(str(start_str).replace("Z", "+00:00"))
            else:
                start = now - max_interval

        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)

        if start > now:
            start = now - max_interval

        partitions: list[dict] = []
        cursor = start
        while cursor < now:
            window_end = min(cursor + max_interval, now)
            partitions.append({"window_start": cursor, "window_end": window_end})
            cursor = window_end

        if not partitions:
            partitions.append({"window_start": start, "window_end": now})

        return partitions

    def prepare_request_payload(
        self,
        context: dict | None,
        next_page_token: str | None,
    ) -> dict | None:
        """Prepare request payload with enterprise filter and date window."""
        from datetime import datetime, timedelta, timezone
        import json

        body = super().prepare_request_payload(context, next_page_token)

        enterprise_ids = self.config.get("enterprise_ids")
        if enterprise_ids:
            body["EnterpriseIds"] = (
                enterprise_ids if isinstance(enterprise_ids, list) else [enterprise_ids]
            )

        # Extent is required; request both availability blocks and adjustments.
        body["Extent"] = {"AvailabilityBlocks": True, "Adjustments": True}

        max_interval = timedelta(days=90)
        now = datetime.now(timezone.utc)

        window_start = None
        window_end = None
        if context:
            window_start = context.get("window_start")
            window_end = context.get("window_end")

        if window_start is None or window_end is None:
            derived_start = self.get_starting_timestamp(context)
            if derived_start is None:
                start_date_str = self.config.get("start_date")
                if start_date_str:
                    derived_start = datetime.fromisoformat(str(start_date_str).replace("Z", "+00:00"))
                else:
                    derived_start = now - max_interval

            if derived_start.tzinfo is None:
                derived_start = derived_start.replace(tzinfo=timezone.utc)

            window_start = max(derived_start, now - max_interval)
            window_end = min(window_start + max_interval, now)

        if window_start.tzinfo is None:
            window_start = window_start.replace(tzinfo=timezone.utc)
        if window_end.tzinfo is None:
            window_end = window_end.replace(tzinfo=timezone.utc)

        start_utc = window_start.isoformat(timespec="seconds").replace("+00:00", "Z")
        end_utc = window_end.isoformat(timespec="seconds").replace("+00:00", "Z")

        body["UpdatedUtc"] = {"StartUtc": start_utc, "EndUtc": end_utc}

        self.logger.info(
            f"Querying availability_blocks with UpdatedUtc: {start_utc} to {end_utc}"
        )

        safe_body = {k: v for k, v in body.items() if k not in ["ClientToken", "AccessToken"]}
        safe_body["ClientToken"] = "***"
        safe_body["AccessToken"] = "***"
        self.logger.info(
            f"Full availability_blocks request body: {json.dumps(safe_body, indent=2)}"
        )

        return body


class ResourceBlocksStream(MewsStream):
    """Stream for resource blocks (out-of-order/internal use)."""

    name = "resource_blocks"
    path = "/resourceBlocks/getAll"
    primary_keys = ("Id",)
    replication_key = "UpdatedUtc"
    records_key = "ResourceBlocks"

    @property
    def partitions(self) -> list[dict] | None:
        """Return 90-day windows from bookmark/start_date up to now."""
        from datetime import datetime, timedelta, timezone

        max_interval = timedelta(days=90)
        now = datetime.now(timezone.utc)

        start = self.get_starting_timestamp(context=None)
        if start is None:
            start_str = self.config.get("start_date")
            if start_str:
                start = datetime.fromisoformat(str(start_str).replace("Z", "+00:00"))
            else:
                start = now - max_interval

        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)

        if start > now:
            start = now - max_interval

        partitions: list[dict] = []
        cursor = start
        while cursor < now:
            window_end = min(cursor + max_interval, now)
            partitions.append({"window_start": cursor, "window_end": window_end})
            cursor = window_end

        if not partitions:
            partitions.append({"window_start": start, "window_end": now})

        return partitions

    def prepare_request_payload(
        self,
        context: dict | None,
        next_page_token: str | None,
    ) -> dict | None:
        """Prepare request payload with enterprise filter and UpdatedUtc window."""
        from datetime import datetime, timedelta, timezone

        body = super().prepare_request_payload(context, next_page_token)

        enterprise_ids = self.config.get("enterprise_ids")
        if enterprise_ids:
            body["EnterpriseIds"] = (
                enterprise_ids if isinstance(enterprise_ids, list) else [enterprise_ids]
            )

        # Include both active and deleted blocks to capture full history.
        body["ActivityStates"] = ["Active", "Deleted"]

        max_interval = timedelta(days=90)
        now = datetime.now(timezone.utc)

        window_start = None
        window_end = None
        if context:
            window_start = context.get("window_start")
            window_end = context.get("window_end")

        if window_start is None or window_end is None:
            derived_start = self.get_starting_timestamp(context)
            if derived_start is None:
                start_date_str = self.config.get("start_date")
                if start_date_str:
                    derived_start = datetime.fromisoformat(str(start_date_str).replace("Z", "+00:00"))
                else:
                    derived_start = now - max_interval

            if derived_start.tzinfo is None:
                derived_start = derived_start.replace(tzinfo=timezone.utc)

            window_start = max(derived_start, now - max_interval)
            window_end = min(window_start + max_interval, now)

        if window_start.tzinfo is None:
            window_start = window_start.replace(tzinfo=timezone.utc)
        if window_end.tzinfo is None:
            window_end = window_end.replace(tzinfo=timezone.utc)

        start_utc = window_start.isoformat(timespec="seconds").replace("+00:00", "Z")
        end_utc = window_end.isoformat(timespec="seconds").replace("+00:00", "Z")

        body["UpdatedUtc"] = {"StartUtc": start_utc, "EndUtc": end_utc}

        return body

    schema = th.PropertiesList(
        th.Property("Id", th.StringType, description="Resource block identifier"),
        th.Property("EnterpriseId", th.StringType, description="Enterprise identifier"),
        th.Property(
            "AssignedResourceId",
            th.StringType,
            description="Identifier of the assigned resource",
        ),
        th.Property("IsActive", th.BooleanType, description="Whether the block is active"),
        th.Property("Type", th.StringType, description="Resource block type"),
        th.Property("StartUtc", th.DateTimeType, description="Start of the block (UTC)"),
        th.Property("EndUtc", th.DateTimeType, description="End of the block (UTC)"),
        th.Property("CreatedUtc", th.DateTimeType, description="Creation timestamp (UTC)"),
        th.Property("UpdatedUtc", th.DateTimeType, description="Last update timestamp (UTC)"),
        th.Property("DeletedUtc", th.DateTimeType, description="Deletion timestamp (UTC)"),
        th.Property("Name", th.StringType, description="Name of the block"),
        th.Property("Notes", th.StringType, description="Notes about the block"),
    ).to_dict()


class ResourceCategoryAssignmentsStream(MewsChildStream):
    """Stream for resource category assignments."""

    name = "resource_category_assignments"
    path = "/resourceCategoryAssignments/getAll"
    primary_keys = ("Id",)
    replication_key = "UpdatedUtc"
    records_key = "ResourceCategoryAssignments"
    parent_stream_type = ResourceCategoriesStream
    requires_service_id = True

    def prepare_request_payload(
        self,
        context: dict | None,
        next_page_token: str | None,
    ) -> dict | None:
        """Prepare request payload scoped to a resource category with date window."""
        from datetime import datetime, timedelta, timezone

        body = super().prepare_request_payload(context, next_page_token)

        # ResourceCategoryIds is required by the API; derive from parent context.
        if context and context.get("resource_category_id"):
            body["ResourceCategoryIds"] = [context["resource_category_id"]]
        else:
            raise ValueError("resource_category_id missing from parent context for assignments")

        # Optional enterprise filter from config.
        enterprise_ids = self.config.get("enterprise_ids")
        if enterprise_ids:
            body["EnterpriseIds"] = (
                enterprise_ids if isinstance(enterprise_ids, list) else [enterprise_ids]
            )

        # Include both active and deleted records to capture full history.
        body["ActivityStates"] = ["Active", "Deleted"]

        max_interval = timedelta(days=90)
        now = datetime.now(timezone.utc)

        start = self.get_starting_timestamp(context)
        if start is None:
            start_str = self.config.get("start_date")
            start = datetime.fromisoformat(str(start_str).replace("Z", "+00:00")) if start_str else now - max_interval

        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)

        window_start = max(start, now - max_interval)
        window_end = min(window_start + max_interval, now)

        start_utc = window_start.isoformat(timespec="seconds").replace("+00:00", "Z")
        end_utc = window_end.isoformat(timespec="seconds").replace("+00:00", "Z")

        body["UpdatedUtc"] = {"StartUtc": start_utc, "EndUtc": end_utc}

        return body

    schema = th.PropertiesList(
        th.Property("Id", th.StringType, description="Assignment identifier"),
        th.Property("ResourceId", th.StringType, description="Resource identifier"),
        th.Property("CategoryId", th.StringType, description="Resource category identifier"),
        th.Property("IsActive", th.BooleanType, description="Whether the assignment is active"),
        th.Property("CreatedUtc", th.DateTimeType, description="Creation timestamp (UTC)"),
        th.Property("UpdatedUtc", th.DateTimeType, description="Last update timestamp (UTC)"),
    ).to_dict()
