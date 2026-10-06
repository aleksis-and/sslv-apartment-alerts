import os
import sys
import types
import unittest
from unittest.mock import patch

os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
os.environ.setdefault("SUPABASE_KEY", "anon-test-key")
os.environ.setdefault("SUPABASE_SERVICE_KEY", "service-test-key")
os.environ.setdefault("BOT_TOKEN", "bot-test-token")
os.environ.setdefault("RESEND_API_KEY", "resend-test-key")

class FakeFlask:
    def __init__(self, *args, **kwargs):
        pass

    def route(self, *args, **kwargs):
        def decorator(func):
            return func
        return decorator

    def run(self, *args, **kwargs):
        pass


class FakeScheduler:
    def add_job(self, *args, **kwargs):
        pass

    def start(self):
        pass


class FakeSupabaseClient:
    def table(self, *args, **kwargs):
        return self

    def select(self, *args, **kwargs):
        return self

    def eq(self, *args, **kwargs):
        return self

    def execute(self):
        return types.SimpleNamespace(data=[])


sys.modules.setdefault(
    "flask",
    types.SimpleNamespace(
        Flask=FakeFlask,
        request=types.SimpleNamespace(headers={}, get_json=lambda *args, **kwargs: {}),
        jsonify=lambda payload: payload,
    ),
)
sys.modules.setdefault(
    "feedparser",
    types.SimpleNamespace(parse=lambda *args, **kwargs: types.SimpleNamespace(entries=[])),
)
sys.modules.setdefault(
    "supabase",
    types.SimpleNamespace(create_client=lambda *args, **kwargs: FakeSupabaseClient()),
)
sys.modules.setdefault(
    "requests",
    types.SimpleNamespace(
        get=lambda *args, **kwargs: None,
        post=lambda *args, **kwargs: None,
        delete=lambda *args, **kwargs: None,
    ),
)
sys.modules.setdefault("apscheduler", types.ModuleType("apscheduler"))
sys.modules.setdefault("apscheduler.schedulers", types.ModuleType("apscheduler.schedulers"))
sys.modules.setdefault(
    "apscheduler.schedulers.blocking",
    types.SimpleNamespace(BlockingScheduler=FakeScheduler),
)

import monitor


class SharedLatestFetchTests(unittest.TestCase):
    def test_shared_latest_fetches_each_source_district_once_for_overlapping_users(self):
        users = [
            {
                "chat_id": "user_1",
                "category": "apartment",
                "intent": "buy",
                "districts": ["centrs", "sigulda"],
            },
            {
                "chat_id": "user_2",
                "category": "apartment",
                "intent": "buy",
                "districts": ["centrs"],
            },
            {
                "chat_id": "user_3",
                "category": "house",
                "intent": "buy",
                "districts": ["centrs"],
            },
        ]
        ss_calls = []
        city24_calls = []

        def fake_ss_latest(category, intent, district):
            ss_calls.append((category, intent, district))
            return [{"item_id": f"ss_{category}_{intent}_{district}"}]

        def fake_city24_latest(category, intent, district):
            city24_calls.append((category, intent, district))
            return [{"item_id": f"city24_{category}_{intent}_{district}"}]

        with (
            patch.object(monitor, "fetch_ss_latest_for_district", side_effect=fake_ss_latest),
            patch.object(monitor, "fetch_city24_latest_for_district", side_effect=fake_city24_latest),
        ):
            cache = monitor.fetch_shared_latest_listings(users)

        expected_keys = [
            ("apartment", "buy", "centrs"),
            ("apartment", "buy", "sigulda"),
            ("house", "buy", "centrs"),
        ]
        self.assertCountEqual(ss_calls, expected_keys)
        self.assertCountEqual(city24_calls, expected_keys)
        self.assertEqual(
            cache[("ss", "apartment", "buy", "centrs")][0]["item_id"],
            "ss_apartment_buy_centrs",
        )
        self.assertEqual(
            cache[("city24", "apartment", "buy", "centrs")][0]["item_id"],
            "city24_apartment_buy_centrs",
        )

    def test_cron_run_uses_shared_latest_path_when_full_scan_is_disabled(self):
        users = [
            {
                "chat_id": "user_1",
                "category": "apartment",
                "intent": "buy",
                "districts": ["centrs"],
            },
            {
                "chat_id": "user_2",
                "category": "apartment",
                "intent": "buy",
                "districts": ["centrs"],
            },
        ]

        class FakeUsersTable:
            def select(self, *args, **kwargs):
                return self

            def eq(self, *args, **kwargs):
                return self

            def execute(self):
                return types.SimpleNamespace(data=users)

        class FakeAdmin:
            def table(self, name):
                self.name = name
                return FakeUsersTable()

        with (
            patch.object(monitor, "supabase_admin", FakeAdmin()),
            patch.object(monitor, "RUN_FULL_SS_SCAN", False),
            patch.object(monitor, "process_users_shared_latest") as process_shared,
            patch.object(monitor, "process_user") as process_user,
        ):
            monitor.run()

        process_shared.assert_called_once_with(users)
        process_user.assert_not_called()


class GuestSearchTests(unittest.TestCase):
    def setUp(self):
        monitor.guest_source_cache.clear()
        self.filters = {"category": "apartment", "intent": "buy", "districts": ["centrs"],
                        "rooms": [6], "min_price": 100, "max_price": 500,
                        "min_area": 10, "max_area": 200}

    def search(self, filters=None, authorized=True):
        with (patch.object(monitor, "is_internal_request_authorized", return_value=authorized),
              patch.object(monitor.request, "get_json", return_value=filters if filters is not None else self.filters)):
            return monitor.browse_listings()

    def test_search_projects_public_fields_and_supports_six_plus(self):
        listing = {"item_id": "public-1", "price": 200, "area": 100, "rooms": 7,
                   "url": "https://www.ss.lv/property", "auth_user_id": "private", "push_token": "private"}
        with (patch.object(monitor, "fetch_ss_latest_for_district", return_value=[listing]) as ss,
              patch.object(monitor, "fetch_city24_latest_for_district", return_value=[]),
              patch.object(monitor, "supabase_admin", None)):
            result = self.search()
            again = self.search()
        self.assertEqual(result, again)
        ss.assert_called_once()
        self.assertEqual(result["listings"][0]["rooms"], 7)
        self.assertNotIn("auth_user_id", result["listings"][0])
        self.assertNotIn("push_token", result["listings"][0])

    def test_requires_internal_key(self):
        self.assertEqual(self.search(authorized=False)[1], 401)

    def test_rejects_invalid_districts_and_ranges_before_fetching(self):
        for update in ({"districts": ["../private"]}, {"districts": ["centrs"] * 4},
                       {"min_price": 600}, {"rooms": [True]}, {"max_area": float("nan")}):
            with patch.object(monitor, "fetch_ss_latest_for_district") as fetch:
                self.assertEqual(self.search({**self.filters, **update})[1], 400)
                fetch.assert_not_called()

    def test_fetch_failure_returns_error_and_releases_capacity(self):
        with patch.object(monitor, "fetch_ss_latest_for_district", side_effect=RuntimeError("offline")):
            self.assertEqual(self.search()[1], 502)
        self.assertTrue(monitor.scan_semaphore.acquire(blocking=False))
        monitor.scan_semaphore.release()


if __name__ == "__main__":
    unittest.main()
