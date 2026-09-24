import io
import gzip
import json
import unittest
import urllib.error
from unittest.mock import MagicMock, Mock, patch, sentinel

from api import index as api


class StaticDashboardTests(unittest.TestCase):
    BASE_URL = "http://static.example.invalid/snapshots"
    HTML_PATHS = (
        "/", "/index.html", "/api/index.py",
        *api.SENTRY_NODE_PATHS, *api.TOP_WALLETS_PATHS,
        *api.EMISSIONS_PATHS, *api.MINERS_PATHS, *api.SN_COMP_PATHS,
    )
    JSON_PATHS = (
        api.TOP_WALLETS_JSON_PATH, api.EMISSIONS_JSON_PATH, api.MINERS_JSON_PATH,
    )

    def setUp(self):
        self.enterContext(patch.dict(api.os.environ, {
            "SYS_TRACKER_STATIC_BASE_URL": self.BASE_URL,
        }, clear=True))
        self.cache = {}
        self.enterContext(patch.object(api, "_static_page_cache", self.cache))
        self.clock = self.enterContext(patch.object(api.time, "monotonic", return_value=1000.0))
        self.urlopen = self.enterContext(patch.object(
            api.urllib.request, "urlopen", side_effect=urllib.error.URLError("private upstream detail"),
        ))
        self.render_only = self.enterContext(patch.object(
            api, "store_for_render_only", return_value=(sentinel.store, 123, "local time"),
        ))
        self.sync = self.enterContext(patch.object(
            api, "sync_for_request", return_value=(sentinel.store, 123, "local time"),
        ))
        self.sync_masternodes = self.enterContext(patch.object(
            api, "sync_masternodes_for_request", return_value=sentinel.store,
        ))
        self.enterContext(patch.object(api, "parse_since_date", return_value=(123, "local time")))
        self.renderers = {
            name: self.enterContext(patch.object(api, name, return_value=f"<html>{name}</html>"))
            for name in (
                "dashboard_html", "masternodes_html", "top_wallets_html",
                "emissions_html", "miners_html", "sn_comp_html",
            )
        }
        self.snapshots = {
            name: self.enterContext(patch.object(api, name, return_value={"local": name}))
            for name in ("top_wallets_snapshot", "emissions_snapshot", "miners_snapshot")
        }
        for name in ("get_store", "get_client", "get_rpc_client"):
            self.enterContext(patch.object(api, name, side_effect=AssertionError("Unexpected local I/O")))

    def successful_upstream(self, body, status=200):
        response = MagicMock()
        response.__enter__.return_value = response
        response.status = status
        response.read.return_value = body
        self.urlopen.side_effect = None
        self.urlopen.return_value = response

    def test_compressed_upstream_is_decoded_before_serving_and_caching(self):
        body = b"<html>Syscoin Top Wallets</html>"
        self.successful_upstream(gzip.compress(body))
        self.urlopen.return_value.headers = {"Content-Encoding": "gzip"}
        status, headers, rendered = self.request("/top-wallets")
        self.assertEqual(status, 200)
        self.assertEqual(rendered, body)
        self.assertEqual(headers["Content-Length"], str(len(body)))
        self.assertEqual(next(iter(self.cache.values()))[1], body)

    def request(self, path="/", *, head=False):
        request = api.handler.__new__(api.handler)
        request.path = path
        request.wfile = io.BytesIO()
        request.send_response = Mock()
        request.send_header = Mock()
        request.end_headers = Mock()
        request.send_error = Mock()
        if head:
            request.do_HEAD()
        else:
            request.do_GET()
        request.send_error.assert_not_called()
        request.send_response.assert_called_once()
        request.end_headers.assert_called_once_with()
        status = request.send_response.call_args.args[0]
        headers = dict(call.args for call in request.send_header.call_args_list)
        body = request.wfile.getvalue()
        if head:
            self.assertEqual(body, b"")
        else:
            self.assertEqual(int(headers["Content-Length"]), len(body))
        return status, headers, body

    def assert_no_local_render(self):
        for mock in (
            self.render_only, self.sync, self.sync_masternodes,
            *self.renderers.values(), *self.snapshots.values(),
        ):
            mock.assert_not_called()

    def assert_maintenance(self, response, *, json_route=False):
        status, headers, body = response
        self.assertEqual(status, 503)
        self.assertEqual(headers["Cache-Control"], "no-store")
        self.assertEqual(headers["Retry-After"], "60")
        self.assertNotIn("X-Tracker-Static-Cache", headers)
        self.assertNotIn(self.BASE_URL.encode(), body)
        self.assertNotIn(b"private upstream detail", body)
        if json_route:
            self.assertEqual(headers["Content-Type"], "application/json; charset=utf-8")
            data = json.loads(body)
            self.assertEqual(data["error"], "static_upstream_unavailable")
            self.assertEqual(data["retry_after_seconds"], 60)
            self.assertIn("try again", data["message"])
        else:
            self.assertEqual(headers["Content-Type"], "text/html; charset=utf-8")
            self.assertIn(b"<!doctype html>", body)
            self.assertIn(b"temporarily unavailable", body)
            self.assertIn(b"try again", body)

    def test_configured_unavailable_html_never_renders_local_indexes(self):
        for path in self.HTML_PATHS:
            with self.subTest(path=path):
                self.assert_maintenance(self.request(path))
        self.assertEqual(self.cache, {})
        self.assert_no_local_render()

    def test_configured_unavailable_json_is_structured_maintenance(self):
        for path in self.JSON_PATHS:
            with self.subTest(path=path):
                self.assert_maintenance(self.request(path), json_route=True)
        self.assert_no_local_render()

    def test_local_sync_flags_do_not_bypass_configured_upstream_failure(self):
        api.os.environ["SYS_TRACKER_REQUEST_SYNC"] = "1"
        self.assert_maintenance(self.request("/?sync=1&force=1"))
        self.assert_no_local_render()

    def test_successful_response_and_fresh_cache_keep_existing_behavior(self):
        body = b"<html>Successful snapshot</html>"
        self.successful_upstream(body)
        for _ in range(2):
            status, headers, actual = self.request()
            self.assertEqual(status, 200)
            self.assertEqual(actual, body)
            self.assertEqual(headers["Cache-Control"], "public, max-age=15, s-maxage=30, stale-while-revalidate=60")
            self.assertNotIn("X-Tracker-Static-Cache", headers)
        self.urlopen.assert_called_once()
        self.assertEqual(self.cache[f"{self.BASE_URL}/index.html"], (1000.0, body))
        self.assert_no_local_render()

    def test_cached_success_is_served_stale_for_html_json_and_aliases(self):
        cases = (
            ("/", "/index.html", b"<html>Dashboard</html>", "index.html"),
            ("/sentrynode", "/sentrynode.html", b"<html>Sentry Nodes</html>", "sentrynode.html"),
            ("/top-wallets", "/top-wallets.html", b"Syscoin Top Wallets", "top-wallets.html"),
            ("/emissions", "/emissions.html", b"Syscoin Network Emissions", "emissions.html"),
            ("/miners", "/miners.html", b"Syscoin Miners", "miners.html"),
            ("/sn-comp", "/sn-comp.html", b"Syscoin SN Comp", "sn-comp.html"),
            *((path, path, b'{"indexed": 42}', path.lstrip("/")) for path in self.JSON_PATHS),
        )
        for path, alias, body, filename in cases:
            with self.subTest(path=path):
                self.clock.return_value = 1000.0
                self.successful_upstream(body)
                self.assertEqual(self.request(path)[0], 200)
                self.clock.return_value = 1061.0
                self.urlopen.side_effect = TimeoutError("private upstream detail")
                status, headers, actual = self.request(alias)
                self.assertEqual(status, 200)
                self.assertEqual(actual, body)
                self.assertEqual(headers["X-Tracker-Static-Cache"], "stale")
                self.assertEqual(headers["X-Tracker-Static-Age-Seconds"], "61")
                self.assertEqual(headers["Cache-Control"], "no-store")
                expected_type = "application/json" if path in self.JSON_PATHS else "text/html"
                self.assertEqual(headers["Content-Type"], f"{expected_type}; charset=utf-8")
                self.assertEqual(self.cache[f"{self.BASE_URL}/{filename}"], (1000.0, body))
        self.assert_no_local_render()

    def test_force_refresh_can_serve_stale_even_inside_fresh_ttl(self):
        self.successful_upstream(b"snapshot")
        self.request()
        self.urlopen.side_effect = TimeoutError()
        status, headers, body = self.request("/?force=1")
        self.assertEqual((status, body), (200, b"snapshot"))
        self.assertEqual(headers["X-Tracker-Static-Cache"], "stale")
        self.assertEqual(headers["X-Tracker-Static-Age-Seconds"], "0")
        self.assertEqual(headers["Cache-Control"], "no-store")
        self.assertIn("?t=", self.urlopen.call_args.args[0].full_url)
        self.assert_no_local_render()

    def test_disabled_fresh_cache_still_retains_last_success_for_recovery(self):
        for ttl in ("0", "-1"):
            with self.subTest(ttl=ttl):
                self.cache.clear()
                api.os.environ["SYS_TRACKER_STATIC_CACHE_SECONDS"] = ttl
                self.successful_upstream(b"snapshot")
                self.request()
                self.urlopen.side_effect = TimeoutError()
                status, headers, body = self.request()
                self.assertEqual((status, body), (200, b"snapshot"))
                self.assertEqual(headers["X-Tracker-Static-Cache"], "stale")
        self.assert_no_local_render()

    def test_stale_cache_expires_after_24_hours_without_renewing_on_failure(self):
        self.successful_upstream(b"snapshot")
        self.request()
        self.urlopen.side_effect = TimeoutError()
        self.clock.return_value = 1000.0 + api.STATIC_CACHE_MAX_AGE_SECONDS
        self.assertEqual(self.request()[0], 200)
        self.assertEqual(self.cache[f"{self.BASE_URL}/index.html"][0], 1000.0)
        self.clock.return_value += 1
        self.assert_maintenance(self.request())
        self.assertEqual(self.cache, {})
        self.assert_no_local_render()

    def test_fresh_cache_ttl_cannot_bypass_24_hour_bound(self):
        api.os.environ["SYS_TRACKER_STATIC_CACHE_SECONDS"] = str(2 * api.STATIC_CACHE_MAX_AGE_SECONDS)
        self.successful_upstream(b"snapshot")
        self.request()
        self.clock.return_value += api.STATIC_CACHE_MAX_AGE_SECONDS + 1
        self.urlopen.side_effect = TimeoutError()
        self.assert_maintenance(self.request())
        self.assert_no_local_render()

    def test_expiry_is_checked_after_failed_fetch(self):
        self.successful_upstream(b"snapshot")
        self.request()
        self.clock.side_effect = (
            1000.0 + api.STATIC_CACHE_MAX_AGE_SECONDS - 1,
            1000.0 + api.STATIC_CACHE_MAX_AGE_SECONDS + 1,
        )
        self.urlopen.side_effect = TimeoutError()
        self.assert_maintenance(self.request())
        self.assert_no_local_render()

    def test_upstream_recovery_replaces_stale_snapshot(self):
        self.successful_upstream(b"old snapshot")
        self.request()
        self.clock.return_value = 1061.0
        self.urlopen.side_effect = TimeoutError()
        self.assertEqual(self.request()[2], b"old snapshot")
        self.clock.return_value = 1100.0
        self.successful_upstream(b"new snapshot")
        status, headers, body = self.request()
        self.assertEqual((status, body), (200, b"new snapshot"))
        self.assertNotIn("X-Tracker-Static-Cache", headers)
        self.assertEqual(self.cache[f"{self.BASE_URL}/index.html"], (1100.0, body))
        self.assert_no_local_render()

    def test_invalid_upstream_responses_are_not_cached(self):
        cases = (
            ("/", "index.html", 503, b"unavailable"),
            ("/", "index.html", 204, b""),
            ("/", "index.html", 200, b" \n"),
            ("/top-wallets", "top-wallets.html", 200, b"wrong page"),
            ("/emissions", "emissions.html", 200, b"wrong page"),
            ("/miners", "miners.html", 200, b"wrong page"),
            ("/sn-comp", "sn-comp.html", 200, b"wrong page"),
            *((path, path.lstrip("/"), 200, body)
              for path in self.JSON_PATHS
              for body in (b"<html>Unavailable</html>", b'{"incomplete":', b"[]")),
        )
        for path, filename, upstream_status, invalid_body in cases:
            for has_cached in (False, True):
                with self.subTest(path=path, status=upstream_status, body=invalid_body, cached=has_cached):
                    self.cache.clear()
                    key = f"{self.BASE_URL}/{filename}"
                    if has_cached:
                        self.cache[key] = (1000.0, b"previous successful snapshot")
                    self.clock.return_value = 1061.0
                    self.successful_upstream(invalid_body, status=upstream_status)
                    result = self.request(path)
                    if has_cached:
                        status, headers, body = result
                        self.assertEqual((status, body), (200, b"previous successful snapshot"))
                        self.assertEqual(headers["X-Tracker-Static-Cache"], "stale")
                        self.assertEqual(self.cache[key], (1000.0, body))
                    else:
                        self.assert_maintenance(result, json_route=path in self.JSON_PATHS)
                        self.assertEqual(self.cache, {})
        self.assert_no_local_render()

    def test_http_error_uses_same_recovery_path_as_connection_failure(self):
        error = urllib.error.HTTPError(
            self.BASE_URL, 503, "private upstream detail", {}, None,
        )
        self.addCleanup(error.close)
        self.urlopen.side_effect = error
        self.assert_maintenance(self.request())
        self.cache[f"{self.BASE_URL}/index.html"] = (900.0, b"last success")
        status, headers, body = self.request()
        self.assertEqual((status, body), (200, b"last success"))
        self.assertEqual(headers["X-Tracker-Static-Cache"], "stale")
        self.assert_no_local_render()

    def test_cache_is_isolated_by_upstream_and_page(self):
        self.successful_upstream(b"Syscoin Miners")
        self.request("/miners")
        self.urlopen.side_effect = TimeoutError()
        self.assert_maintenance(self.request("/"))
        api.os.environ["SYS_TRACKER_STATIC_BASE_URL"] = "http://other.example.invalid/snapshots"
        self.assert_maintenance(self.request("/miners"))
        self.assert_no_local_render()

    def test_head_maintenance_matches_get_headers_without_body(self):
        for path in ("/", *self.JSON_PATHS):
            with self.subTest(path=path):
                status, headers, _ = self.request(path)
                head_status, head_headers, _ = self.request(path, head=True)
                self.assertEqual(status, 503)
                self.assertEqual(head_status, status)
                self.assertEqual(head_headers, headers)
        self.assert_no_local_render()

    def test_head_stale_matches_get_headers_without_body(self):
        self.successful_upstream(b"snapshot")
        self.request()
        self.clock.return_value = 1061.0
        self.urlopen.side_effect = TimeoutError()
        status, headers, _ = self.request()
        head_status, head_headers, _ = self.request(head=True)
        self.assertEqual(status, 200)
        self.assertEqual(headers["X-Tracker-Static-Cache"], "stale")
        self.assertEqual(head_status, status)
        self.assertEqual(head_headers, headers)
        self.assert_no_local_render()

    def test_missing_or_disabled_upstream_preserves_local_preview(self):
        for value in (None, "", "  ", "0", "false", "None", " OFF "):
            with self.subTest(value=value):
                if value is None:
                    api.os.environ.pop("SYS_TRACKER_STATIC_BASE_URL", None)
                else:
                    api.os.environ["SYS_TRACKER_STATIC_BASE_URL"] = value
                self.render_only.reset_mock()
                self.renderers["dashboard_html"].reset_mock()
                status, headers, body = self.request()
                self.assertEqual(status, 200)
                self.assertEqual(body, b"<html>dashboard_html</html>")
                self.assertEqual(headers["Cache-Control"], "no-store")
                self.assertNotIn("X-Tracker-Static-Cache", headers)
                self.assertNotIn("Retry-After", headers)
                self.render_only.assert_called_once_with()
                self.assertIs(self.renderers["dashboard_html"].call_args.args[0], sentinel.store)
        self.urlopen.assert_not_called()
        self.sync.assert_not_called()
        self.sync_masternodes.assert_not_called()

    def test_local_html_routes_keep_existing_renderers_and_masternode_sync(self):
        api.os.environ.pop("SYS_TRACKER_STATIC_BASE_URL")
        cases = (
            (api.SENTRY_NODE_PATHS[0], "masternodes_html", "full"),
            (api.SN_COMP_PATHS[0], "sn_comp_html", "sn_comp"),
            (api.TOP_WALLETS_PATHS[0], "top_wallets_html", None),
            (api.EMISSIONS_PATHS[0], "emissions_html", None),
        )
        for path, renderer, scope in cases:
            with self.subTest(path=path):
                self.render_only.reset_mock()
                self.sync_masternodes.reset_mock()
                status, _, body = self.request(path)
                self.assertEqual((status, body), (200, f"<html>{renderer}</html>".encode()))
                self.assertIs(self.renderers[renderer].call_args.args[0], sentinel.store)
                if scope:
                    self.sync_masternodes.assert_called_once_with(force=False, time_lookup_scope=scope)
                    self.render_only.assert_not_called()
                else:
                    self.render_only.assert_called_once_with()
                    self.sync_masternodes.assert_not_called()
        self.urlopen.assert_not_called()
        self.sync.assert_not_called()

    def test_local_json_routes_pass_store_to_snapshots(self):
        api.os.environ.pop("SYS_TRACKER_STATIC_BASE_URL")
        for path, snapshot in (
            (api.TOP_WALLETS_JSON_PATH, "top_wallets_snapshot"),
            (api.EMISSIONS_JSON_PATH, "emissions_snapshot"),
            (api.MINERS_JSON_PATH, "miners_snapshot"),
        ):
            with self.subTest(path=path):
                status, headers, body = self.request(path)
                self.assertEqual(status, 200)
                self.assertEqual(headers["Content-Type"], "application/json; charset=utf-8")
                self.assertEqual(json.loads(body), {"local": snapshot})
                self.snapshots[snapshot].assert_called_once_with(sentinel.store)
        self.urlopen.assert_not_called()

    def test_local_miners_html_uses_snapshot_with_local_store(self):
        api.os.environ.pop("SYS_TRACKER_STATIC_BASE_URL")
        for path in api.MINERS_PATHS:
            with self.subTest(path=path):
                self.snapshots["miners_snapshot"].reset_mock()
                self.renderers["miners_html"].reset_mock()
                status, _, body = self.request(path)
                self.assertEqual((status, body), (200, b"<html>miners_html</html>"))
                self.snapshots["miners_snapshot"].assert_called_once_with(sentinel.store)
                self.renderers["miners_html"].assert_called_once_with(
                    refresh_seconds=0, snapshot={"local": "miners_snapshot"},
                )
        self.urlopen.assert_not_called()

    def test_local_explicit_sync_is_preserved(self):
        api.os.environ.pop("SYS_TRACKER_STATIC_BASE_URL")
        for path, enabled in (("/?sync=1&force=1", "0"), ("/?force=1", "1")):
            with self.subTest(path=path):
                self.sync.reset_mock()
                api.os.environ["SYS_TRACKER_REQUEST_SYNC"] = enabled
                self.assertEqual(self.request(path)[0], 200)
                self.sync.assert_called_once_with(force=True)
        self.render_only.assert_not_called()
        self.urlopen.assert_not_called()


if __name__ == "__main__":
    unittest.main()
