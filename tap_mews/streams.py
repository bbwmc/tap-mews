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
        th.Property("Contact", th.ObjectType(), description="Contact details"),
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

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        """Pass bill_id to child streams (Payments)."""
        return {"bill_id": record["Id"]}

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


class PaymentsStream(MewsChildStream):
    """Stream for payments.

    Child stream of Bills - uses BillIds to query payments for bills.
    """

    name = "payments"
    path = "/payments/getAll"
    primary_keys = ("Id",)
    replication_key = "UpdatedUtc"
    records_key = "Payments"
    parent_stream_type = BillsStream

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
        """Prepare request payload with BillIds filter.

        Payments are queried using BillIds from parent bills.
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


class PaymentRequestsStream(MewsStream):
    """Stream for payment requests."""

    name = "payment_requests"
    path = "/paymentRequests/getAll"
    primary_keys = ("Id",)
    replication_key = "UpdatedUtc"
    records_key = "PaymentRequests"

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

            self.logger.info(
                f"Querying payment_requests with UpdatedUtc: {start_utc} to {end_utc}"
            )

        safe_body = {k: v for k, v in body.items() if k not in ["ClientToken", "AccessToken"]}
        safe_body["ClientToken"] = "***"
        safe_body["AccessToken"] = "***"
        self.logger.info(f"Full payment_requests request body: {json.dumps(safe_body, indent=2)}")

        return body
