from __future__ import annotations

import io
import tempfile
import unittest
from argparse import Namespace
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import scrape_jpx_supervision_history as target


class SupervisionCsvTests(unittest.TestCase):
    @staticmethod
    def make_row(code: str, start_date: str, end_date: str, reasons: dict[str, int]) -> dict[str, object]:
        return {
            "コード": code,
            "市場": "t",
            "開始日": start_date,
            "終了前営業日": end_date,
            "理由": reasons,
        }

    def test_discover_history_urls_extracts_current_and_archives(self) -> None:
        html = """
        <html>
          <body>
            <select>
              <option value="/listing/market-alerts/supervision/01.html" selected>2026年</option>
              <option value="/listing/market-alerts/supervision/01-archives-01.html">2025年</option>
              <option value="/listing/market-alerts/supervision/01-archives-02.html">2024年</option>
            </select>
          </body>
        </html>
        """

        with patch.object(target, "fetch_html", return_value=html):
            urls = target.discover_history_urls()

        self.assertEqual(
            urls,
            [
                "https://www.jpx.co.jp/listing/market-alerts/supervision/01.html",
                "https://www.jpx.co.jp/listing/market-alerts/supervision/01-archives-01.html",
                "https://www.jpx.co.jp/listing/market-alerts/supervision/01-archives-02.html",
            ],
        )

    def test_discover_history_urls_falls_back_to_sequential_archives(self) -> None:
        empty_html = "<html><body><p>no links</p></body></html>"
        table_html = """
        <html>
          <body>
            <h2>指定履歴</h2>
            <table>
              <tr><th>指定年月日</th><th>コード</th><th>市場区分</th><th>内容</th><th>解除年月日</th></tr>
              <tr><td>2026/04/01</td><td>1234</td><td>プライム</td><td>監理銘柄（確認中）指定</td><td>-</td></tr>
            </table>
          </body>
        </html>
        """

        def fetch_html_side_effect(url: str) -> str:
            if url == target.JPX_HISTORY_CURRENT_URL:
                return empty_html
            if url.endswith("01-archives-01.html") or url.endswith("01-archives-02.html"):
                return table_html
            return ""

        with patch.object(target, "fetch_html", side_effect=fetch_html_side_effect):
            urls = target.discover_history_urls()

        self.assertEqual(
            urls,
            [
                "https://www.jpx.co.jp/listing/market-alerts/supervision/01.html",
                "https://www.jpx.co.jp/listing/market-alerts/supervision/01-archives-01.html",
                "https://www.jpx.co.jp/listing/market-alerts/supervision/01-archives-02.html",
            ],
        )

    def test_merge_rows_reason_flags_are_or_merged(self) -> None:
        existing_rows = [
            {
                "コード": "1234",
                "市場": "t",
                "開始日": "2026/04/01",
                "終了前営業日": "2099/12/31",
                "理由": {"監理銘柄（確認中）指定": 1},
            }
        ]
        new_rows = [
            {
                "コード": "1234",
                "市場": "t",
                "開始日": "2026/04/01",
                "終了前営業日": "2099/12/31",
                "理由": {"監理銘柄（審査中）指定": 1},
            }
        ]

        merged_rows, reason_columns = target.merge_rows(
            existing_rows,
            new_rows,
            ["監理銘柄（確認中）指定"],
        )

        self.assertEqual(
            reason_columns,
            ["監理銘柄（確認中）指定", "監理銘柄（審査中）指定"],
        )
        self.assertEqual(len(merged_rows), 1)
        self.assertEqual(merged_rows[0]["監理銘柄（確認中）指定"], 1)
        self.assertEqual(merged_rows[0]["監理銘柄（審査中）指定"], 1)

    def test_merge_rows_carries_forward_active_reasons_and_closes_previous_row(self) -> None:
        merged_rows, reason_columns = target.merge_rows(
            [
                self.make_row(
                    "3541",
                    "2025/12/25",
                    "2099/12/31",
                    {"監理銘柄（確認中）指定": 1},
                )
            ],
            [
                self.make_row(
                    "3541",
                    "2026/04/10",
                    "2099/12/31",
                    {"上場廃止の決定・整理銘柄指定": 1},
                )
            ],
            ["監理銘柄（確認中）指定"],
        )

        self.assertEqual(
            reason_columns,
            ["監理銘柄（確認中）指定", "上場廃止の決定・整理銘柄指定"],
        )
        self.assertEqual(len(merged_rows), 2)
        rows_by_start = {row["開始日"]: row for row in merged_rows}
        self.assertEqual(rows_by_start["2025/12/25"]["終了前営業日"], "2026/04/09")
        self.assertEqual(rows_by_start["2025/12/25"]["監理銘柄（確認中）指定"], 1)
        self.assertEqual(rows_by_start["2025/12/25"]["上場廃止の決定・整理銘柄指定"], 0)
        self.assertEqual(rows_by_start["2026/04/10"]["終了前営業日"], "2099/12/31")
        self.assertEqual(rows_by_start["2026/04/10"]["監理銘柄（確認中）指定"], 1)
        self.assertEqual(rows_by_start["2026/04/10"]["上場廃止の決定・整理銘柄指定"], 1)

    def test_merge_rows_carries_all_active_reasons_across_multiple_transitions(self) -> None:
        merged_rows, _ = target.merge_rows(
            [],
            [
                self.make_row(
                    "1234",
                    "2026/01/05",
                    "2099/12/31",
                    {"監理銘柄（確認中）指定": 1},
                ),
                self.make_row(
                    "1234",
                    "2026/02/10",
                    "2099/12/31",
                    {"監理銘柄（審査中）指定": 1},
                ),
                self.make_row(
                    "1234",
                    "2026/03/10",
                    "2099/12/31",
                    {"上場廃止の決定・整理銘柄指定": 1},
                ),
            ],
            [],
        )

        rows_by_start = {row["開始日"]: row for row in merged_rows}
        self.assertEqual(rows_by_start["2026/01/05"]["終了前営業日"], "2026/02/09")
        self.assertEqual(rows_by_start["2026/02/10"]["終了前営業日"], "2026/03/09")
        self.assertEqual(rows_by_start["2026/02/10"]["監理銘柄（確認中）指定"], 1)
        self.assertEqual(rows_by_start["2026/02/10"]["監理銘柄（審査中）指定"], 1)
        self.assertEqual(rows_by_start["2026/03/10"]["監理銘柄（確認中）指定"], 1)
        self.assertEqual(rows_by_start["2026/03/10"]["監理銘柄（審査中）指定"], 1)
        self.assertEqual(rows_by_start["2026/03/10"]["上場廃止の決定・整理銘柄指定"], 1)

    def test_merge_rows_does_not_carry_forward_released_reasons(self) -> None:
        merged_rows, _ = target.merge_rows(
            [
                self.make_row(
                    "3541",
                    "2025/12/25",
                    "2026/04/09",
                    {"監理銘柄（確認中）指定": 1},
                )
            ],
            [
                self.make_row(
                    "3541",
                    "2026/04/10",
                    "2099/12/31",
                    {"上場廃止の決定・整理銘柄指定": 1},
                )
            ],
            [],
        )

        rows_by_start = {row["開始日"]: row for row in merged_rows}
        self.assertEqual(rows_by_start["2025/12/25"]["終了前営業日"], "2026/04/09")
        self.assertEqual(rows_by_start["2026/04/10"]["監理銘柄（確認中）指定"], 0)
        self.assertEqual(rows_by_start["2026/04/10"]["上場廃止の決定・整理銘柄指定"], 1)

    def test_main_ignores_state_when_output_csv_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "supervision.csv"
            state_path = Path(temp_dir) / "state.json"
            state_path.write_text('{"last_date":"2026-04-18","last_code":"9999"}', encoding="utf-8")
            args = Namespace(full=False, no_state=False, output=str(output_path), state=str(state_path))

            with (
                patch.object(target, "parse_args", return_value=args),
                patch.object(target, "load_state", return_value={"last_date": "2026-04-18", "last_code": "9999"}) as load_state_mock,
                patch.object(target, "fetch_html", return_value=""),
                patch.object(target, "discover_history_urls", return_value=[target.JPX_HISTORY_CURRENT_URL]),
                patch.object(target, "parse_history_table", return_value=[]),
                patch.object(target, "read_existing_csv", return_value=([], [])),
                patch.object(target, "save_csv"),
                patch.object(target, "save_state"),
            ):
                with redirect_stdout(io.StringIO()):
                    target.main()

            load_state_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
