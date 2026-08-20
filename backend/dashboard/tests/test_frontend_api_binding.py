import json
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
HTML = (ROOT / "static/index.html").read_text(encoding="utf-8")
CSS = (ROOT / "static/styles.css").read_text(encoding="utf-8")
JS = (ROOT / "static/app.js").read_text(encoding="utf-8")
ITEMS = json.loads((ROOT / "tests/fixtures/api-items-response-v5.json").read_text(encoding="utf-8"))
DETAIL = json.loads((ROOT / "tests/fixtures/api-item-detail-v2.json").read_text(encoding="utf-8"))


class ApiBindingTests(unittest.TestCase):
    def test_runtime_reads_only_same_origin_get_api(self):
        self.assertIn('const ITEMS_API_URL = "/api/v1/items"', JS)
        self.assertIn('raw: "0"', JS)
        self.assertNotIn("fixtures/", JS)
        for token in ("method: \"POST\"", "method: \"PATCH\"", "method: \"PUT\"", "method: \"DELETE\""):
            self.assertNotIn(token, JS)

    def test_items_fixture_matches_public_response_envelope(self):
        self.assertTrue(ITEMS["available"])
        self.assertEqual(ITEMS["projection_identity"]["status"], "verified")
        self.assertIsInstance(ITEMS["items"], list)
        self.assertEqual(ITEMS["dashboard_request_counter_scope"], "dashboard_read_only_request")
        for field in (
            "dashboard_request_model_call_count",
            "dashboard_request_provider_request_count",
            "dashboard_request_mcp_tool_call_count",
            "dashboard_request_formal_write_count",
        ):
            self.assertEqual(ITEMS[field], 0)

    def test_detail_fixture_uses_v2_and_keeps_critical_review_independent(self):
        detail = DETAIL["item"]["task_detail"]
        self.assertEqual(detail["schema_version"], "study-intake-dashboard-task-detail-v2")
        self.assertEqual(detail["analysis"]["execution_status"], "completed")
        self.assertEqual(detail["critical_review"]["execution_status"], "not_started")
        self.assertIsNone(detail["critical_review"]["raw_output_sha256"])
        self.assertNotEqual(detail["analysis"], detail["critical_review"])

    def test_three_subjects_have_succeeded_report_available_samples(self):
        succeeded = [item for item in ITEMS["items"] if item["execution_status"] == "succeeded" and item["report_available"]]
        self.assertEqual({item["subject"] for item in succeeded}, {"math", "cs408", "english"})

    def test_quality_and_failure_axes_are_frozen(self):
        for item in ITEMS["items"]:
            if item["quality_status"] == "passed":
                self.assertEqual((item["report_disposition"], item["sol_review_status"]), ("accepted", "not_required"))
            if item["quality_status"] == "issues_found":
                self.assertEqual(item["execution_status"], "succeeded")
                self.assertEqual((item["report_disposition"], item["sol_review_status"]), ("needs_sol_review", "pending"))
            if item["execution_status"] == "failed":
                self.assertEqual(item["report_disposition"], "technical_failure")
                self.assertTrue(item["terminal_error_code"])


class StaticBoundaryTests(unittest.TestCase):
    def test_mock_runtime_and_removed_states_do_not_reappear(self):
        runtime_sources = HTML + JS
        for token in (
            "mock-task-view-model", "Mock TaskViewModel", "awaiting_confirmation",
            "confirmation_status", "report_pending", "next_retry_at", "Attempt 2",
        ):
            self.assertNotIn(token, runtime_sources)

    def test_frontend_does_not_guess_provider_or_mcp_evidence(self):
        self.assertIn("detail v2 未公开独立 Provider 结果", JS)
        self.assertIn("detail v2 未公开独立 MCP 查询字段", JS)
        self.assertIn('key: "mcp", label: "MCP", state: "unverified"', JS)

    def test_generation_preconditions_are_sent_as_complete_groups(self):
        for field in (
            "projection_sha256", "projection_generation", "projection_release_id",
            "task_generation", "task_fence",
        ):
            self.assertIn(field, JS)

    def test_no_drag_drop_or_persistence_surface(self):
        combined = HTML + JS
        for token in ("draggable", "dragstart", "localStorage", "sessionStorage"):
            self.assertNotIn(token, combined)

    def test_status_mapping_fails_closed(self):
        self.assertIn("缺少唯一的等待或处理中映射", JS)
        self.assertIn("技术失败映射不完整", JS)
        self.assertIn("成功质量映射不符合冻结合同", JS)


class AccessibilityAndResponsiveTests(unittest.TestCase):
    def test_landmarks_dialog_and_skip_link_exist(self):
        self.assertIn('lang="zh-CN"', HTML)
        self.assertIn('class="skip-link"', HTML)
        self.assertIn('<main id="main-content"', HTML)
        self.assertIn('<dialog id="task-dialog"', HTML)
        self.assertIn('aria-labelledby="dialog-title"', HTML)
        self.assertIn('role="tablist"', HTML)

    def test_static_ids_are_unique(self):
        ids = re.findall(r'\bid="([^"]+)"', HTML)
        self.assertEqual(len(ids), len(set(ids)))

    def test_focus_restoration_and_keyboard_navigation_remain(self):
        self.assertIn("state.lastDialogTrigger?.isConnected", JS)
        self.assertIn('"ArrowLeft", "ArrowRight", "Home", "End"', JS)
        self.assertIn(":focus-visible", CSS)

    def test_required_responsive_breakpoints_remain(self):
        for width in ("1023px", "720px", "480px"):
            self.assertIn(f"@media (max-width: {width})", CSS)
        self.assertIn("@media (prefers-reduced-motion: reduce)", CSS)


if __name__ == "__main__":
    unittest.main()
