"""Exercise ledger pagination through the Singer SDK without network access."""

from collections import Counter
from datetime import datetime, timezone
import json
import unittest
from unittest.mock import patch

import requests

from tap_mews.streams import LedgerBalancesStream, LedgerEntriesStream
from tap_mews.tap import TapMews


class LedgerEntriesPaginationTest(unittest.TestCase):
    def setUp(self):
        self.config = {
            "client_token": "test-client",
            "access_token": "test-access",
            "start_date": "2025-01-01T00:00:00Z",
            "enterprise_ids": ["test-enterprise"],
            "ledger_types": ["Revenue", "Tax", "Payment", "Deposit", "Guest", "City"],
            "progress_log_enabled": False,
        }

    def fetch(self, records, page_size=None):
        tap = TapMews(config=self.config, parse_env_config=False)
        stream = LedgerEntriesStream(tap)
        if page_size is not None:
            stream.page_size = page_size
        day = datetime(2026, 9, 15, tzinfo=timezone.utc)
        context = {"window_start": day, "window_end": day}
        payloads = []
        state_before = json.dumps(tap.state, sort_keys=True)

        def send(request, **kwargs):
            payload = json.loads(request.body)
            payloads.append(payload)
            limitation = payload["Limitation"]
            cursor = limitation.get("Cursor")
            offset = int(cursor.removeprefix("position-")) if cursor else 0
            page = records[offset:offset + limitation["Count"]]
            response = requests.Response()
            response.status_code = 200
            response.request = request
            response.url = request.url
            response.headers["Content-Type"] = "application/json"
            response._content = json.dumps({
                "LedgerEntries": page,
                "Cursor": f"position-{offset + len(page)}" if page else None,
            }).encode()
            return response

        with patch.object(stream.requests_session, "send", side_effect=send):
            actual = list(stream.get_records(context))
        self.assertEqual(json.dumps(tap.state, sort_keys=True), state_before)
        for payload in payloads:
            self.assertEqual(payload["PostingDate"], {"Start": "2026-09-15", "End": "2026-09-15"})
            self.assertEqual(payload["EnterpriseIds"], self.config["enterprise_ids"])
            self.assertEqual(payload["LedgerTypes"], self.config["ledger_types"])
        return actual, payloads

    def test_larger_pages_preserve_every_field_and_duplicate_occurrence(self):
        records = [
            {"Id": str(i), "Value": i / 100, "PostingDate": "2026-09-15", "Extra": {"i": i}}
            for i in range(2504)
        ]
        records.append(dict(records[-1]))
        before, old_payloads = self.fetch(records, page_size=100)
        after, new_payloads = self.fetch(records)
        def canonical(rows):
            return Counter(json.dumps(row, sort_keys=True) for row in rows)
        self.assertEqual(canonical(after), canonical(before))
        self.assertEqual(canonical(after), canonical(records))
        self.assertEqual(len(old_payloads), 27)
        self.assertEqual(len(new_payloads), 4)

    def test_empty_and_boundary_pages_are_complete(self):
        for count in [0, 1, 999, 1000, 1001, 2000]:
            with self.subTest(records=count):
                records = [{"Id": str(i)} for i in range(count)]
                actual, payloads = self.fetch(records)
                self.assertEqual(actual, records)
                self.assertEqual(len(payloads), (count + 999) // 1000 + 1)

    def test_daily_partition_coverage_is_unchanged(self):
        old_tap = TapMews(config=self.config, parse_env_config=False)
        new_tap = TapMews(config=self.config, parse_env_config=False)
        old = LedgerEntriesStream(old_tap)
        old.page_size = 100
        new = LedgerEntriesStream(new_tap)
        self.assertEqual(new.partitions, old.partitions)
        self.assertEqual(new.partitions[0]["window_start"].date().isoformat(), "2025-01-01")
        self.assertEqual(new.partitions[-1]["window_end"].date(), datetime.now(timezone.utc).date())
        self.assertTrue(all(p["window_start"] == p["window_end"] for p in new.partitions))
        self.assertEqual(LedgerBalancesStream(new_tap).page_size, 100)


if __name__ == "__main__":
    unittest.main()
