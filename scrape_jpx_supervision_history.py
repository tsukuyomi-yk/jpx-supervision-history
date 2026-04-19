"""依存ライブラリ: requests, beautifulsoup4, pandas"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag
from requests import RequestException

JPX_HISTORY_CURRENT_URL = "https://www.jpx.co.jp/listing/market-alerts/supervision/01.html"

BASE_CSV_HEADER = ["コード", "市場", "日付", "日付"]
DEFAULT_OUTPUT_PATH = Path("supervision.csv")
DEFAULT_STATE_PATH = Path("state.json")
DEFAULT_FAR_FUTURE_DATE = "2099/12/31"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
)


def warn(message: str) -> None:
    print(f"warning: {message}", file=sys.stderr)


def fetch_html(url: str) -> str:
    try:
        response = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=30)
        response.raise_for_status()
    except RequestException as exc:
        warn(f"取得失敗: {url} ({exc})")
        return ""
    response.encoding = response.apparent_encoding or response.encoding
    return response.text


def normalize_text(text: Any) -> str:
    value = "" if text is None else str(text)
    value = value.replace("\u3000", " ").replace("\xa0", " ")
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def parse_jpx_date(text: str) -> str:
    value = normalize_text(text)
    if value in {"", "-"}:
        return ""
    match = re.search(r"(\d{4})[/-年](\d{1,2})[/-月](\d{1,2})", value)
    if not match:
        return ""
    year_value = int(match.group(1))
    month_value = int(match.group(2))
    day_value = int(match.group(3))
    try:
        parsed = date(year_value, month_value, day_value)
    except ValueError:
        return ""
    return parsed.strftime("%Y/%m/%d")


def canonical_header(text: str) -> str:
    value = normalize_text(text)
    compact = re.sub(r"\s+", "", value)
    compact = compact.replace("（注）", "").replace("(注)", "")
    if "指定年月日" in compact:
        return "指定年月日"
    if "銘柄名" in compact:
        return "銘柄名"
    if compact == "コード":
        return "コード"
    if compact.startswith("市場区分"):
        return "市場区分"
    if "解除年月日" in compact:
        return "解除年月日"
    if compact == "内容":
        return "内容"
    if compact == "備考":
        return "備考"
    return compact


def find_history_table(soup: BeautifulSoup) -> Tag | None:
    section = soup.find(
        lambda tag: isinstance(tag, Tag)
        and tag.name in {"h1", "h2", "h3", "h4", "caption"}
        and "指定履歴" in normalize_text(tag.get_text(" ", strip=True))
    )
    if section is not None:
        table = section.find_next("table")
        if isinstance(table, Tag):
            return table

    best_table: Tag | None = None
    best_score = -1
    for table in soup.find_all("table"):
        header_cells = table.find("tr")
        if header_cells is None:
            continue
        headers = [canonical_header(cell.get_text(" ", strip=True)) for cell in header_cells.find_all(["th", "td"])]
        score = sum(header in headers for header in ("指定年月日", "コード", "内容", "解除年月日"))
        if score > best_score:
            best_score = score
            best_table = table
    return best_table if best_score >= 2 else None


def build_header_index(header_row: Tag) -> dict[int, str]:
    index_map: dict[int, str] = {}
    for index, cell in enumerate(header_row.find_all(["th", "td"])):
        header_name = canonical_header(cell.get_text(" ", strip=True))
        if header_name:
            index_map[index] = header_name
    return index_map


def normalize_row_cells(cells: list[str], expected_size: int) -> list[str]:
    if len(cells) < expected_size:
        return cells + [""] * (expected_size - len(cells))
    if len(cells) > expected_size:
        return cells[:expected_size]
    return cells


def normalize_history_url(candidate_url: str, base_url: str) -> str:
    absolute_url = urljoin(base_url, normalize_text(candidate_url))
    path = urlparse(absolute_url).path
    if re.fullmatch(r"/listing/market-alerts/supervision/01(?:-archives-\d+)?\.html", path):
        return absolute_url
    return ""


def history_url_sort_key(url: str) -> tuple[int, int]:
    path = urlparse(url).path
    if path.endswith("/01.html"):
        return (0, 0)
    match = re.search(r"01-archives-(\d+)\.html$", path)
    if match:
        return (1, int(match.group(1)))
    return (2, 0)


def extract_history_urls_from_html(html: str, base_url: str) -> list[str]:
    if not html:
        return []

    urls: list[str] = []
    seen: set[str] = set()
    soup = BeautifulSoup(html, "html.parser")

    def append_url(candidate_url: str) -> None:
        normalized_url = normalize_history_url(candidate_url, base_url)
        if normalized_url and normalized_url not in seen:
            seen.add(normalized_url)
            urls.append(normalized_url)

    for option in soup.find_all("option"):
        append_url(option.get("value", ""))
    for anchor in soup.find_all("a", href=True):
        append_url(anchor.get("href", ""))
    for candidate_url in re.findall(
        r"(?:https://www\.jpx\.co\.jp)?/listing/market-alerts/supervision/01(?:-archives-\d+)?\.html",
        html,
    ):
        append_url(candidate_url)

    return sorted(urls, key=history_url_sort_key)


def discover_history_archive_urls(base_url: str) -> list[str]:
    archive_urls: list[str] = []
    archive_number = 1

    while True:
        archive_url = urljoin(base_url, f"01-archives-{archive_number:02d}.html")
        html = fetch_html(archive_url)
        if not html:
            break
        if find_history_table(BeautifulSoup(html, "html.parser")) is None:
            break
        archive_urls.append(archive_url)
        archive_number += 1

    return archive_urls


def discover_history_urls() -> list[str]:
    current_html = fetch_html(JPX_HISTORY_CURRENT_URL)
    discovered_urls = extract_history_urls_from_html(current_html, JPX_HISTORY_CURRENT_URL)
    if JPX_HISTORY_CURRENT_URL not in discovered_urls:
        discovered_urls.insert(0, JPX_HISTORY_CURRENT_URL)
    if len(discovered_urls) == 1 and current_html:
        discovered_urls.extend(discover_history_archive_urls(JPX_HISTORY_CURRENT_URL))
    return sorted(set(discovered_urls), key=history_url_sort_key)


def parse_history_table(html: str, source_url: str) -> list[dict[str, str]]:
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    table = find_history_table(soup)
    if table is None:
        warn(f"表未検出: {source_url}")
        return []

    header_row = table.find("tr")
    if header_row is None:
        warn(f"ヘッダー未検出: {source_url}")
        return []

    header_index = build_header_index(header_row)
    if not header_index:
        warn(f"列名未検出: {source_url}")
        return []

    expected_size = len(header_index)
    rows: list[dict[str, str]] = []
    for row_number, tr in enumerate(table.find_all("tr")[1:], start=2):
        cells = [normalize_text(cell.get_text(" ", strip=True)) for cell in tr.find_all(["th", "td"])]
        if not cells or not any(cells):
            continue
        cells = normalize_row_cells(cells, expected_size)
        record = {header_index[index]: value for index, value in enumerate(cells) if index in header_index}

        code = normalize_text(record.get("コード", "")).upper()
        designated_date = parse_jpx_date(record.get("指定年月日", ""))
        market = normalize_text(record.get("市場区分", ""))
        content = normalize_text(record.get("内容", ""))

        if canonical_header(cells[0]) == "指定年月日":
            continue
        if "外国株" in market:
            continue
        if not designated_date or not code:
            warn(f"行スキップ: {source_url} {row_number}行目")
            continue
        if not re.fullmatch(r"[0-9A-Z]{4}", code):
            warn(f"コード形式不正: {source_url} {row_number}行目 ({code})")
            continue
        if content and "監理" not in content and "整理" not in content:
            continue

        rows.append(
            {
                "指定日": designated_date,
                "解除日": parse_jpx_date(record.get("解除年月日", "")),
                "コード": code,
                "市場区分": market,
                "内容": content,
                "銘柄名": normalize_text(record.get("銘柄名", "")),
                "備考": normalize_text(record.get("備考", "")),
                "source_url": source_url,
                "year": designated_date[:4],
            }
        )
    return rows


def nth_weekday(year_value: int, month_value: int, weekday: int, nth: int) -> date:
    current = date(year_value, month_value, 1)
    while current.weekday() != weekday:
        current += timedelta(days=1)
    return current + timedelta(days=7 * (nth - 1))


def vernal_equinox_day(year_value: int) -> int:
    return int(20.8431 + 0.242194 * (year_value - 1980) - ((year_value - 1980) // 4))


def autumnal_equinox_day(year_value: int) -> int:
    return int(23.2488 + 0.242194 * (year_value - 1980) - ((year_value - 1980) // 4))


def build_substitute_holidays(base_holidays: set[date], blocked_holidays: set[date]) -> set[date]:
    substitutes: set[date] = set()
    for holiday in sorted(base_holidays):
        if holiday.weekday() != 6:
            continue
        substitute = holiday + timedelta(days=1)
        while substitute in blocked_holidays or substitute in substitutes:
            substitute += timedelta(days=1)
        if substitute.year == holiday.year:
            substitutes.add(substitute)
    return substitutes


def build_citizen_holidays(holidays: set[date], year_value: int) -> set[date]:
    citizens: set[date] = set()
    current = date(year_value, 1, 2)
    while current < date(year_value, 12, 31):
        if current.weekday() < 5 and current not in holidays:
            if (current - timedelta(days=1)) in holidays and (current + timedelta(days=1)) in holidays:
                citizens.add(current)
        current += timedelta(days=1)
    return citizens


def build_national_holidays(year_value: int) -> set[date]:
    base_holidays = {
        date(year_value, 1, 1),
        nth_weekday(year_value, 1, 0, 2),
        date(year_value, 2, 11),
        date(year_value, 2, 23),
        date(year_value, 3, vernal_equinox_day(year_value)),
        date(year_value, 4, 29),
        date(year_value, 5, 3),
        date(year_value, 5, 4),
        date(year_value, 5, 5),
        nth_weekday(year_value, 7, 0, 3),
        date(year_value, 8, 11),
        nth_weekday(year_value, 9, 0, 3),
        date(year_value, 9, autumnal_equinox_day(year_value)),
        nth_weekday(year_value, 10, 0, 2),
        date(year_value, 11, 3),
        date(year_value, 11, 23),
    }
    provisional_substitutes = build_substitute_holidays(base_holidays, base_holidays)
    provisional_holidays = base_holidays | provisional_substitutes
    citizen_holidays = build_citizen_holidays(provisional_holidays, year_value)
    blocked_holidays = base_holidays | citizen_holidays
    substitute_holidays = build_substitute_holidays(base_holidays, blocked_holidays)
    return base_holidays | citizen_holidays | substitute_holidays


JPX_HOLIDAYS_BY_YEAR: dict[int, set[date]] = {
    year: build_national_holidays(year) | {date(year, 1, 2), date(year, 1, 3), date(year, 12, 31)}
    for year in range(2021, 2028)
}


def get_jpx_holidays(year_value: int) -> set[date]:
    holidays = JPX_HOLIDAYS_BY_YEAR.get(year_value)
    if holidays is None:
        holidays = build_national_holidays(year_value) | {
            date(year_value, 1, 2),
            date(year_value, 1, 3),
            date(year_value, 12, 31),
        }
        JPX_HOLIDAYS_BY_YEAR[year_value] = holidays
    return holidays


def previous_business_day(date_text: str) -> str:
    if not date_text:
        return ""
    current = datetime.strptime(date_text, "%Y/%m/%d").date() - timedelta(days=1)
    while current.weekday() >= 5 or current in get_jpx_holidays(current.year):
        current -= timedelta(days=1)
    return current.strftime("%Y/%m/%d")


def parse_csv_flag(value: Any) -> int:
    normalized = normalize_text(value)
    if not normalized:
        return 0
    try:
        return 1 if int(float(normalized)) != 0 else 0
    except ValueError:
        return 0


def extract_reason_columns(rows: list[dict[str, Any]], existing_columns: list[str] | None = None) -> list[str]:
    reason_columns: list[str] = []
    seen: set[str] = set()

    for reason in existing_columns or []:
        normalized = normalize_text(reason)
        if normalized and normalized not in seen:
            seen.add(normalized)
            reason_columns.append(normalized)

    for row in rows:
        for reason, flag in row.get("理由", {}).items():
            normalized = normalize_text(reason)
            if normalized and flag and normalized not in seen:
                seen.add(normalized)
                reason_columns.append(normalized)
    return reason_columns


def convert_to_csv_rows(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    converted: list[dict[str, Any]] = []
    for row in rows:
        release_date = row.get("解除日", "")
        reason = normalize_text(row.get("内容", ""))
        converted.append(
            {
                "コード": row["コード"],
                "市場": "t",
                "開始日": row["指定日"],
                "終了前営業日": previous_business_day(release_date) if release_date else DEFAULT_FAR_FUTURE_DATE,
                "理由": {reason: 1} if reason else {},
            }
        )
    return converted


def parse_output_date(date_text: str) -> date | None:
    normalized = normalize_text(date_text)
    if not normalized:
        return None
    try:
        return datetime.strptime(normalized, "%Y/%m/%d").date()
    except ValueError:
        return None


def collapse_rows_by_key(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged_map: dict[tuple[str, str, str], dict[str, Any]] = {}

    for row in rows:
        key = (row["コード"], row["開始日"], row["終了前営業日"])
        current = merged_map.setdefault(
            key,
            {
                "コード": row["コード"],
                "市場": normalize_text(row.get("市場", "")) or "t",
                "開始日": row["開始日"],
                "終了前営業日": row["終了前営業日"],
                "理由": {},
            },
        )
        if not normalize_text(current.get("市場", "")):
            current["市場"] = normalize_text(row.get("市場", "")) or "t"
        for reason, flag in row.get("理由", {}).items():
            normalized = normalize_text(reason)
            if normalized and flag:
                current["理由"][normalized] = 1

    return list(merged_map.values())


def normalize_code_transition_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows_by_code: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in collapse_rows_by_key(rows):
        rows_by_code[row["コード"]].append(
            {
                "コード": row["コード"],
                "市場": normalize_text(row.get("市場", "")) or "t",
                "開始日": row["開始日"],
                "終了前営業日": row["終了前営業日"],
                "理由": dict(row.get("理由", {})),
            }
        )

    normalized_rows: list[dict[str, Any]] = []
    for code_rows in rows_by_code.values():
        code_rows.sort(
            key=lambda row: (
                parse_output_date(row["開始日"]) or date.max,
                parse_output_date(row["終了前営業日"]) or date.max,
            )
        )

        normalized_code_rows: list[dict[str, Any]] = []
        index = 0
        while index < len(code_rows):
            start_date_text = code_rows[index]["開始日"]
            start_date = parse_output_date(start_date_text)
            current_group: list[dict[str, Any]] = []
            while index < len(code_rows) and code_rows[index]["開始日"] == start_date_text:
                current_group.append(code_rows[index])
                index += 1

            if start_date is None:
                normalized_code_rows.extend(current_group)
                continue

            active_rows: list[dict[str, Any]] = []
            carried_reasons: dict[str, int] = {}
            for previous_row in normalized_code_rows:
                previous_end = parse_output_date(previous_row["終了前営業日"])
                if previous_end is None or previous_end < start_date:
                    continue
                active_rows.append(previous_row)
                for reason, flag in previous_row["理由"].items():
                    if flag:
                        carried_reasons[reason] = 1

            if active_rows:
                closed_end_date = previous_business_day(start_date_text)
                for previous_row in active_rows:
                    previous_row["終了前営業日"] = closed_end_date

            for current_row in current_group:
                merged_reasons = dict(carried_reasons)
                merged_reasons.update(current_row["理由"])
                current_row["理由"] = merged_reasons
                normalized_code_rows.append(current_row)

        normalized_rows.extend(collapse_rows_by_key(normalized_code_rows))

    return normalized_rows


def read_existing_csv(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    if not path.exists():
        return [], []

    rows: list[dict[str, Any]] = []
    reason_columns: list[str] = []
    try:
        with path.open("r", encoding="cp932", newline="") as file:
            reader = csv.reader(file)
            header = next(reader, None)
            if header is None:
                return [], []
            header = [normalize_text(cell) for cell in header]
            if len(header) < 4:
                warn(f"既存CSVヘッダー不正: {path}")
                return [], []

            seen_reasons: set[str] = set()
            for column_name in header[4:]:
                if column_name and column_name not in seen_reasons:
                    seen_reasons.add(column_name)
                    reason_columns.append(column_name)

            expected_size = len(header)
            for line_number, record in enumerate(reader, start=2):
                if not record or not any(normalize_text(cell) for cell in record):
                    continue
                record = normalize_row_cells(record, expected_size)
                start_date = parse_jpx_date(record[2])
                end_date = parse_jpx_date(record[3])
                code = normalize_text(record[0]).upper()
                if not start_date or not end_date or not code:
                    warn(f"既存CSV行スキップ: {path} {line_number}行目")
                    continue
                reasons: dict[str, int] = {}
                for index, reason in enumerate(reason_columns, start=4):
                    flag = parse_csv_flag(record[index])
                    if flag:
                        reasons[reason] = flag
                rows.append(
                    {
                        "コード": code,
                        "市場": normalize_text(record[1]) or "t",
                        "開始日": start_date,
                        "終了前営業日": end_date,
                        "理由": reasons,
                    }
                )
    except OSError as exc:
        warn(f"既存CSV読込失敗: {path} ({exc})")
    return rows, reason_columns


def merge_rows(
    existing_rows: list[dict[str, Any]],
    new_rows: list[dict[str, Any]],
    existing_reason_columns: list[str],
) -> tuple[list[dict[str, Any]], list[str]]:
    reason_columns = extract_reason_columns(existing_rows + new_rows, existing_reason_columns)
    normalized_rows = normalize_code_transition_rows(existing_rows + new_rows)
    if not normalized_rows:
        return [], reason_columns

    combined_rows = []
    for row in collapse_rows_by_key(normalized_rows):
        csv_row = {
            "コード": row["コード"],
            "市場": row.get("市場", "t"),
            "開始日": row["開始日"],
            "終了前営業日": row["終了前営業日"],
        }
        for reason in reason_columns:
            csv_row[reason] = int(row["理由"].get(reason, 0))
        combined_rows.append(csv_row)

    dataframe = pd.DataFrame(combined_rows)
    dataframe["sort_code"] = dataframe["コード"].astype(str)
    dataframe["sort_start"] = pd.to_datetime(dataframe["開始日"], format="%Y/%m/%d", errors="coerce")
    dataframe["sort_end"] = pd.to_datetime(dataframe["終了前営業日"], format="%Y/%m/%d", errors="coerce")
    dataframe = dataframe.dropna(subset=["sort_start", "sort_end"])
    dataframe = dataframe.sort_values(
        by=["sort_code", "sort_start", "sort_end"],
        kind="mergesort",
    )
    output_columns = ["コード", "市場", "開始日", "終了前営業日", *reason_columns]
    return dataframe[output_columns].to_dict("records"), reason_columns


def save_csv(rows: list[dict[str, Any]], output_path: Path, reason_columns: list[str]) -> None:
    try:
        with output_path.open("w", encoding="cp932", newline="") as file:
            writer = csv.writer(file)
            writer.writerow([*BASE_CSV_HEADER, *reason_columns])
            for row in rows:
                writer.writerow(
                    [
                        row["コード"],
                        row["市場"],
                        row["開始日"],
                        row["終了前営業日"],
                        *[row.get(reason, 0) for reason in reason_columns],
                    ]
                )
    except OSError as exc:
        raise RuntimeError(f"CSV保存失敗: {output_path} ({exc})") from exc


def load_state(path: Path) -> dict[str, str] | None:
    if not path.exists():
        return None

    try:
        with path.open("r", encoding="utf-8") as file:
            state = json.load(file)
    except (OSError, json.JSONDecodeError) as exc:
        warn(f"state.json読込失敗のため初回扱い: {path} ({exc})")
        return None

    if not isinstance(state, dict):
        warn(f"state.json形式不正のため初回扱い: {path}")
        return None

    last_date = normalize_text(state.get("last_date", ""))
    last_code = normalize_text(state.get("last_code", "")).upper()
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", last_date) or not re.fullmatch(r"[0-9A-Z]{4}", last_code):
        warn(f"state.json内容不正のため初回扱い: {path}")
        return None
    return {"last_date": last_date, "last_code": last_code}


def display_date_to_state(date_text: str) -> str:
    return date_text.replace("/", "-")


def should_include_row(row: dict[str, str], state: dict[str, str] | None) -> bool:
    if state is None:
        return True
    current_date = display_date_to_state(row["指定日"])
    last_date = state["last_date"]
    if current_date > last_date:
        return True
    if current_date < last_date:
        return False
    return row["コード"] > state["last_code"]


def build_state_from_saved_rows(rows: list[dict[str, Any]]) -> dict[str, str] | None:
    if not rows:
        return None

    latest = max(
        (
            (display_date_to_state(row["開始日"]), row["コード"])
            for row in rows
            if normalize_text(row.get("開始日", ""))
        ),
        default=None,
    )
    if latest is None:
        return None
    return {"last_date": latest[0], "last_code": latest[1]}


def save_state(state: dict[str, str] | None, path: Path) -> None:
    if state is None:
        return
    try:
        with path.open("w", encoding="utf-8") as file:
            json.dump(state, file, ensure_ascii=False, indent=2)
    except OSError as exc:
        raise RuntimeError(f"state保存失敗: {path} ({exc})") from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="JPX監理・整理銘柄一覧の指定履歴を取得してCSV保存します。")
    parser.add_argument("--full", action="store_true", help="state.jsonを無視して全件取得します。")
    parser.add_argument("--no-state", action="store_true", help="state.jsonを読まずに取得します。")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH), help="出力CSVパス")
    parser.add_argument("--state", default=str(DEFAULT_STATE_PATH), help="state.jsonパス")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_path = Path(args.output)
    state_path = Path(args.state)
    active_state = None if (args.full or args.no_state or not output_path.exists()) else load_state(state_path)
    history_urls = discover_history_urls()

    all_rows: list[dict[str, str]] = []
    year_counts: dict[int, int] = defaultdict(int)
    for url in history_urls:
        html = fetch_html(url)
        parsed_rows = parse_history_table(html, url)
        for year in {int(row["year"]) for row in parsed_rows if row.get("year", "").isdigit()}:
            year_counts[year] += 0
        filtered_rows = [row for row in parsed_rows if should_include_row(row, active_state)]
        for row in filtered_rows:
            if row.get("year", "").isdigit():
                year_counts[int(row["year"])] += 1
        all_rows.extend(filtered_rows)

    converted_rows = convert_to_csv_rows(all_rows)
    existing_rows, existing_reason_columns = ([], []) if args.full else read_existing_csv(output_path)
    merged_rows, reason_columns = merge_rows(existing_rows, converted_rows, existing_reason_columns)
    save_csv(merged_rows, output_path, reason_columns)
    save_state(build_state_from_saved_rows(merged_rows), state_path)

    print(f"総件数: {len(merged_rows)}")
    print(f"保存先: {output_path.resolve()}")
    print("年ごとの取得件数:")
    for year in sorted(year_counts):
        print(f"  {year}: {year_counts.get(year, 0)}")


if __name__ == "__main__":
    main()
