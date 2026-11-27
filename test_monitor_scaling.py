import os
import sys
import types
import unittest
from unittest.mock import Mock, patch

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
    def test_email_limits_preview_without_losing_total_or_results_link(self):
        listing = {"title": "Auto", "street": "Auto", "price": 20000, "rooms": None,
                   "area": None, "url": "https://www.ss.lv/msg/example.html",
                   "auto_data": {"year": 2020, "liters": 2, "mileage": 150000}}
        body = monitor.build_email_html([listing] * 63, "auto", "buy", "Tiguan",
                                       "https://pazinojumi.lv/unsubscribe#token=test",
                                       "https://pazinojumi.lv/results#token=test")
        self.assertEqual(body.count("Skatīt sludinājumu →"), 10)
        self.assertIn("Atrasti 63", body)
        self.assertIn("/results#token=test", body)
        self.assertLess(body.index("Atteikties no šī"), body.index("Skatīt sludinājumu →"))
        self.assertLess(len(body.encode("utf-8")), 80000)

    def test_auto_initial_scan_reads_following_pages_without_duplicates(self):
        base = "/lv/transport/cars/volkswagen/tiguan/sell/"
        def row(id, slug):
            return f'''<tr id="tr_{id}"><td><a class="am" href="/msg/lv/transport/cars/volkswagen/tiguan/{slug}.html">Auto</a></td>
              <td class="pp6">2020</td><td class="pp6">2.0D</td><td class="pp6">-</td><td class="pp6">20,000 €</td></tr>'''
        first, second = Mock(), Mock()
        first.text = row(1, "aaa") + f'<a href="{base}page2.html">2</a>'
        second.text = row(1, "aaa") + row(2, "bbb")
        with patch.object(monitor, "source_get", side_effect=[first, second]) as get:
            matches = monitor.fetch_ss_auto_sales("volkswagen/tiguan", 20)
        self.assertEqual(get.call_count, 2)
        self.assertEqual(len(matches), 2)
        self.assertTrue(get.call_args.args[0].endswith("/sell/page2.html"))

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

    def test_commercial_rent_matches_both_sources_without_rooms(self):
        filters = {**self.filters, "category": "commercial", "intent": "rent", "rooms": []}
        office = {"item_id": "office", "price": 300, "area": 100, "source": "SS.lv"}
        shop = {"item_id": "shop", "price": 450, "area": 150, "source": "City24.lv"}
        oversized = {**shop, "item_id": "warehouse", "area": 500}
        with (
            patch.object(monitor, "fetch_ss_latest_for_district", return_value=[office]) as ss,
            patch.object(monitor, "fetch_city24_latest_for_district", return_value=[shop, oversized]) as city24,
        ):
            self.assertEqual([item["id"] for item in self.search(filters)["listings"]], ["office", "shop"])
            ss.assert_called_once_with("commercial", "rent", "centrs")
            city24.assert_called_once_with("commercial", "rent", "centrs")

    def test_guest_can_search_for_land_without_rooms(self):
        filters = {**self.filters, "category": "land", "rooms": []}
        listing = {"item_id": "plot", "price": 300, "area": 150, "rooms": None}
        with (
            patch.object(monitor, "fetch_ss_latest_for_district", return_value=[listing]) as fetch,
            patch.object(monitor, "fetch_city24_latest_for_district", return_value=[]),
        ):
            self.assertEqual(self.search(filters)["listings"][0]["id"], "plot")
            fetch.assert_called_once_with("land", "buy", "centrs")
        self.assertEqual(self.search({**filters, "intent": "rent"})[1], 400)

    def test_guest_auto_search_uses_make_without_districts_or_area(self):
        filters = {**self.filters, "category": "auto", "districts": [], "rooms": [],
                   "auto_filters": {"make": "bmw", "min_liters": 2.5, "engine": "diesel"}}
        car = {"item_id": "car", "title": "BMW 530", "price": 300, "area": None,
               "auto_data": {"model": "530", "model_slug": "530", "liters": 3},
               "specs": {"engine": "diesel"}}
        wrong_engine = {**car, "item_id": "petrol", "specs": {"engine": "petrol"}}
        with patch.object(monitor, "fetch_ss_latest_for_district", return_value=[car, wrong_engine]) as fetch:
            result = self.search(filters)
            self.assertEqual([listing["id"] for listing in result["listings"]], ["car"])
            fetch.assert_called_once_with("auto", "buy", "bmw")
            self.assertEqual(monitor.build_latest_fetch_keys([filters]), [("auto", "buy", "bmw")])

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
