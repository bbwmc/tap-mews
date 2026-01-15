"""Custom HTTP client for Mews API.

Mews API uses POST requests with authentication tokens in the request body,
and cursor-based pagination via the Limitation object.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import logging
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import requests
from singer_sdk.streams import RESTStream

if TYPE_CHECKING:
    from singer_sdk import Tap

logger = logging.getLogger(__name__)


def _format_duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}h{minutes:02d}m{seconds:02d}s"
    if minutes:
        return f"{minutes}m{seconds:02d}s"
    return f"{seconds}s"


def _as_utc_seconds(value: object) -> int | None:
    """Return epoch seconds (UTC) for datetime/ISO string values."""
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def _window_label(window_start: object, window_end: object) -> str | None:
    start_s = _as_utc_seconds(window_start)
    end_s = _as_utc_seconds(window_end)
    if start_s is None or end_s is None:
        return None

    start_iso = (
        datetime.fromtimestamp(start_s, tz=timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )
    end_iso = (
        datetime.fromtimestamp(end_s, tz=timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )
    return f"{start_iso} → {end_iso}"


@dataclass
class _ProgressState:
    stream_started_at: float = field(default_factory=time.monotonic)
    stream_records: int = 0
    stream_pages: int = 0
    last_log_at: float = field(default_factory=time.monotonic)

    active_partition_id: str | None = None
    active_partition_started_at: float | None = None
    active_partition_records: int = 0
    active_partition_pages: int = 0

    partition_index: int | None = None  # 1-based, if known
    partition_total: int | None = None
    completed_partition_durations: list[float] = field(default_factory=list)


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

    _progress: _ProgressState | None = None

    @property
    def url_base(self) -> str:
        """Return the base URL for the API."""
        return f"{self.config.get('api_url', 'https://api.mews.com')}/api/connector/v1"

    def _progress_enabled(self) -> bool:
        return bool(self.config.get("progress_log_enabled", True))

    def _progress_interval_seconds(self) -> int:
        try:
            interval = int(self.config.get("progress_log_interval_seconds", 60))
        except (TypeError, ValueError):
            interval = 60
        return max(5, interval)

    def _get_progress(self) -> _ProgressState:
        if self._progress is None:
            self._progress = _ProgressState()
            self.logger.info("Starting %s sync", self.name)
        return self._progress

    def _partition_id_from_context(self, context: dict | None) -> str | None:
        if not context:
            return None
        if "window_start" in context and "window_end" in context:
            label = _window_label(context.get("window_start"), context.get("window_end"))
            return f"window:{label}" if label else "window:unknown"
        return None

    def _maybe_log_progress(self, *, force: bool = False) -> None:
        if not self._progress_enabled():
            return
        progress = self._get_progress()
        now = time.monotonic()
        if not force and (now - progress.last_log_at) < self._progress_interval_seconds():
            return

        elapsed = now - progress.stream_started_at
        rate = (progress.stream_records / elapsed) if elapsed > 0 else 0.0

        partition_part = ""
        if progress.active_partition_id and progress.active_partition_started_at is not None:
            partition_elapsed = now - progress.active_partition_started_at
            partition_rate = (
                (progress.active_partition_records / partition_elapsed)
                if partition_elapsed > 0
                else 0.0
            )
            if progress.partition_index and progress.partition_total:
                partition_part = f", partition {progress.partition_index}/{progress.partition_total}"
            partition_part += (
                f", partition_records={progress.active_partition_records:,}"
                f", partition_pages={progress.active_partition_pages:,}"
                f", partition_rate={partition_rate:,.0f}/s"
            )

        self.logger.info(
            "Progress %s: records=%s, pages=%s, rate=%s/s, elapsed=%s%s",
            self.name,
            f"{progress.stream_records:,}",
            f"{progress.stream_pages:,}",
            f"{rate:,.0f}",
            _format_duration(elapsed),
            partition_part,
        )
        progress.last_log_at = now

    def _on_partition_start(self, context: dict | None) -> None:
        if not self._progress_enabled():
            return

        partition_id = self._partition_id_from_context(context)
        if partition_id is None:
            return

        progress = self._get_progress()
        if progress.active_partition_id == partition_id:
            return

        progress.active_partition_id = partition_id
        progress.active_partition_started_at = time.monotonic()
        progress.active_partition_records = 0
        progress.active_partition_pages = 0

        progress.partition_index = None
        progress.partition_total = None

        # Try to compute partition index/total for time-window partitioned streams.
        try:
            partitions = self.partitions or []
        except Exception:
            partitions = []

        if partitions and context:
            ctx_start = _as_utc_seconds(context.get("window_start"))
            ctx_end = _as_utc_seconds(context.get("window_end"))
            if ctx_start is not None and ctx_end is not None:
                for i, part in enumerate(partitions, start=1):
                    part_start = _as_utc_seconds(part.get("window_start"))
                    part_end = _as_utc_seconds(part.get("window_end"))
                    if part_start == ctx_start and part_end == ctx_end:
                        progress.partition_index = i
                        progress.partition_total = len(partitions)
                        break

        label = (
            _window_label(context.get("window_start"), context.get("window_end"))
            if context
            else None
        )
        if progress.partition_index and progress.partition_total and label:
            self.logger.info(
                "Starting %s partition %s/%s (%s)",
                self.name,
                progress.partition_index,
                progress.partition_total,
                label,
            )
        elif label:
            self.logger.info("Starting %s partition (%s)", self.name, label)
        else:
            self.logger.info("Starting %s partition", self.name)

    def _on_page_end(self, next_cursor: str | None) -> None:
        if not self._progress_enabled():
            return

        progress = self._get_progress()
        progress.stream_pages += 1
        if progress.active_partition_id:
            progress.active_partition_pages += 1

        if next_cursor:
            return

        if not progress.active_partition_id or progress.active_partition_started_at is None:
            return

        now = time.monotonic()
        duration = now - progress.active_partition_started_at
        progress.completed_partition_durations.append(duration)

        eta_part = ""
        if progress.partition_index and progress.partition_total:
            remaining = max(0, progress.partition_total - progress.partition_index)
            avg = sum(progress.completed_partition_durations) / len(
                progress.completed_partition_durations
            )
            eta_part = f", eta≈{_format_duration(avg * remaining)}"

        self.logger.info(
            "Completed %s partition%s: records=%s, pages=%s, duration=%s%s",
            self.name,
            (
                f" {progress.partition_index}/{progress.partition_total}"
                if progress.partition_index and progress.partition_total
                else ""
            ),
            f"{progress.active_partition_records:,}",
            f"{progress.active_partition_pages:,}",
            _format_duration(duration),
            eta_part,
        )

        # If this is the final partition, log a stream summary.
        if (
            progress.partition_index
            and progress.partition_total
            and progress.partition_index >= progress.partition_total
        ):
            self._maybe_log_progress(force=True)

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
        self._on_partition_start(context)
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
        cursor = data.get("Cursor")
        self._on_page_end(cursor)
        return cursor

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
            records = data[records_key]
            if isinstance(records, list) and self._progress_enabled():
                progress = self._get_progress()
                progress.stream_records += len(records)
                if progress.active_partition_id:
                    progress.active_partition_records += len(records)
                self._maybe_log_progress()
            yield from data[records_key]
        else:
            # If no records_key specified, try common patterns
            for key in data:
                if isinstance(data[key], list):
                    records = data[key]
                    if isinstance(records, list) and self._progress_enabled():
                        progress = self._get_progress()
                        progress.stream_records += len(records)
                        if progress.active_partition_id:
                            progress.active_partition_records += len(records)
                        self._maybe_log_progress()
                    yield from data[key]
                    break

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response, treating 408 timeouts as retriable.

        The Mews API sometimes returns 408 "Transaction has timed out" errors
        when under load. These should be retried rather than treated as fatal.
        """
        if response.status_code == 408:
            from singer_sdk.exceptions import RetriableAPIError

            msg = self.response_error_message(response)
            raise RetriableAPIError(msg, response)
        super().validate_response(response)

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
    in the request body. Some services (e.g., Orderable type) don't support
    certain endpoints, so we handle 400 errors gracefully.
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

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response, allowing 400 errors for unsupported services.

        Some Mews service types (e.g., Orderable) don't support certain endpoints
        like resourceCategories. The API returns 400 "Invalid ServiceIds" for these.
        We skip these gracefully instead of failing.

        Args:
            response: The HTTP response object.

        Raises:
            FatalAPIError: For non-recoverable errors (except 400 for invalid services).
        """
        if response.status_code == 400:
            try:
                error_data = response.json()
                if "Invalid ServiceIds" in error_data.get("Message", ""):
                    # This service doesn't support this endpoint - skip silently
                    logger.info(
                        f"Service doesn't support {self.name} endpoint, skipping"
                    )
                    return
            except Exception:
                pass
        # For all other cases, use default validation
        super().validate_response(response)

    def parse_response(self, response: requests.Response) -> list[dict]:
        """Parse response, returning empty for 400 errors on unsupported services."""
        if response.status_code == 400:
            try:
                error_data = response.json()
                if "Invalid ServiceIds" in error_data.get("Message", ""):
                    return  # Yield nothing for unsupported services
            except Exception:
                pass
        yield from super().parse_response(response)
