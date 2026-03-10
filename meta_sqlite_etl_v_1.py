import os
import sys
import json
import time
import sqlite3
import hashlib
import argparse
import requests
from datetime import date, datetime, timedelta, UTC
from typing import Any, Dict, Iterable, List, Optional, Tuple


DB_PATH = os.getenv("META_DB_PATH", "meta_ads.db")
ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN", "")
DEFAULT_AD_ACCOUNT_ID = os.getenv("META_AD_ACCOUNT_ID", "act_1234567890")
API_VERSION = os.getenv("META_API_VERSION", "v23.0")
BASE_URL = f"https://graph.facebook.com/{API_VERSION}"

DEFAULT_LOOKBACK_DAYS = int(os.getenv("META_LOOKBACK_DAYS", "14"))
REQUEST_TIMEOUT = int(os.getenv("META_REQUEST_TIMEOUT", "90"))
PAGE_LIMIT = int(os.getenv("META_PAGE_LIMIT", "500"))
ENABLE_PLACEMENT_BREAKDOWN = os.getenv("META_ENABLE_PLACEMENT_BREAKDOWN", "1") == "1"
REQUEST_SLEEP_SECONDS = float(os.getenv("META_REQUEST_SLEEP_SECONDS", "0.0"))


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def to_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def to_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def extract_action_value(values: Any, key_name: str, target: str) -> Optional[float]:
    if not isinstance(values, list):
        return None
    for item in values:
        if item.get(key_name) == target:
            try:
                return float(item.get("value", 0))
            except (TypeError, ValueError):
                return None
    return None


def daterange_for_lookback(days: int) -> Tuple[str, str]:
    today = date.today()
    since = today - timedelta(days=days)
    until = today - timedelta(days=1)
    return since.isoformat(), until.isoformat()


def first_day_of_month(d: date) -> date:
    return d.replace(day=1)


def last_day_of_month(d: date) -> date:
    if d.month == 12:
        next_month = d.replace(year=d.year + 1, month=1, day=1)
    else:
        next_month = d.replace(month=d.month + 1, day=1)
    return next_month - timedelta(days=1)


def month_ranges(start_date: date, end_date: date) -> List[Tuple[str, str]]:
    if start_date > end_date:
        raise ValueError("start_date 不能大於 end_date")

    ranges: List[Tuple[str, str]] = []
    cursor = first_day_of_month(start_date)

    while cursor <= end_date:
        month_start = cursor
        month_end = last_day_of_month(cursor)

        actual_start = max(month_start, start_date)
        actual_end = min(month_end, end_date)

        ranges.append((actual_start.isoformat(), actual_end.isoformat()))

        if cursor.month == 12:
            cursor = cursor.replace(year=cursor.year + 1, month=1, day=1)
        else:
            cursor = cursor.replace(month=cursor.month + 1, day=1)

    return ranges


class MetaAdsETL:
    def __init__(self, db_path: str, ad_account_id: str) -> None:
        self.db_path = db_path
        self.ad_account_id = ad_account_id
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.conn.execute("PRAGMA foreign_keys=OFF;")
        self.create_tables()

    def close(self) -> None:
        self.conn.close()

    def create_tables(self) -> None:
        cur = self.conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS campaigns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id TEXT NOT NULL,
                campaign_id TEXT NOT NULL,
                name TEXT,
                objective TEXT,
                status TEXT,
                effective_status TEXT,
                buying_type TEXT,
                updated_time TEXT,
                raw_json TEXT,
                last_seen_at TEXT,
                UNIQUE(account_id, campaign_id)
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS adsets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id TEXT NOT NULL,
                adset_id TEXT NOT NULL,
                campaign_id TEXT,
                name TEXT,
                status TEXT,
                effective_status TEXT,
                optimization_goal TEXT,
                billing_event TEXT,
                bid_strategy TEXT,
                daily_budget INTEGER,
                lifetime_budget INTEGER,
                start_time TEXT,
                end_time TEXT,
                targeting_json TEXT,
                attribution_spec_json TEXT,
                raw_json TEXT,
                last_seen_at TEXT,
                UNIQUE(account_id, adset_id)
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS ads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id TEXT NOT NULL,
                ad_id TEXT NOT NULL,
                campaign_id TEXT,
                adset_id TEXT,
                name TEXT,
                status TEXT,
                effective_status TEXT,
                creative_id TEXT,
                raw_json TEXT,
                last_seen_at TEXT,
                UNIQUE(account_id, ad_id)
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS object_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id TEXT NOT NULL,
                object_type TEXT NOT NULL,
                object_id TEXT NOT NULL,
                snapshot_time TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                payload_hash TEXT,
                UNIQUE(account_id, object_type, object_id, snapshot_time)
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS insights_daily (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                grain_key TEXT NOT NULL UNIQUE,
                account_id TEXT NOT NULL,
                date_start TEXT NOT NULL,
                date_stop TEXT NOT NULL,
                level TEXT NOT NULL,
                campaign_id TEXT,
                campaign_name TEXT,
                adset_id TEXT,
                adset_name TEXT,
                ad_id TEXT,
                ad_name TEXT,
                objective TEXT,
                optimization_goal TEXT,
                publisher_platform TEXT,
                platform_position TEXT,
                impression_device TEXT,
                spend REAL,
                impressions INTEGER,
                reach INTEGER,
                frequency REAL,
                clicks INTEGER,
                unique_clicks INTEGER,
                ctr REAL,
                cpc REAL,
                cpm REAL,
                outbound_clicks INTEGER,
                landing_page_views INTEGER,
                actions_json TEXT,
                action_values_json TEXT,
                cost_per_action_type_json TEXT,
                attribution_setting TEXT,
                action_report_time TEXT,
                request_time TEXT NOT NULL,
                raw_json TEXT
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS actions_daily (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                grain_key TEXT NOT NULL UNIQUE,
                account_id TEXT NOT NULL,
                date_start TEXT NOT NULL,
                level TEXT NOT NULL,
                campaign_id TEXT,
                adset_id TEXT,
                ad_id TEXT,
                publisher_platform TEXT,
                platform_position TEXT,
                impression_device TEXT,
                action_type TEXT NOT NULL,
                action_category TEXT NOT NULL,
                value REAL,
                attribution_setting TEXT,
                action_report_time TEXT,
                request_time TEXT NOT NULL
            )
            """
        )

        cur.execute("CREATE INDEX IF NOT EXISTS idx_campaigns_account_campaign ON campaigns(account_id, campaign_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_adsets_account_adset ON adsets(account_id, adset_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ads_account_ad ON ads(account_id, ad_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_insights_account_date_level ON insights_daily(account_id, date_start, level)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_insights_campaign_date ON insights_daily(account_id, campaign_id, date_start)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_insights_adset_date ON insights_daily(account_id, adset_id, date_start)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_insights_ad_date ON insights_daily(account_id, ad_id, date_start)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_actions_account_date_type ON actions_daily(account_id, date_start, action_type)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_actions_campaign_date ON actions_daily(account_id, campaign_id, date_start)")

        self.conn.commit()

    def _request(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        if not ACCESS_TOKEN:
            raise RuntimeError("請先設定 META_ACCESS_TOKEN 環境變數")

        params = dict(params)
        params["access_token"] = ACCESS_TOKEN
        url = f"{BASE_URL}/{path.lstrip('/')}"

        resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        try:
            payload = resp.json()
        except Exception:
            payload = None

        if resp.status_code >= 400:
            raise RuntimeError(
                f"Meta API 錯誤: HTTP {resp.status_code}\nURL: {url}\nParams: {json.dumps(params, ensure_ascii=False)}\nResponse: {json.dumps(payload, ensure_ascii=False, indent=2) if payload else resp.text}"
            )

        if REQUEST_SLEEP_SECONDS > 0:
            time.sleep(REQUEST_SLEEP_SECONDS)

        return payload

    def _paginate(self, path: str, params: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        payload = self._request(path, params)
        while True:
            for row in payload.get("data", []):
                yield row

            next_url = payload.get("paging", {}).get("next")
            if not next_url:
                break

            resp = requests.get(next_url, timeout=REQUEST_TIMEOUT)
            try:
                payload = resp.json()
            except Exception:
                raise RuntimeError(f"分頁資料不是有效 JSON：{next_url}")

            if resp.status_code >= 400:
                raise RuntimeError(
                    f"Meta API 分頁錯誤: HTTP {resp.status_code}\nNext URL: {next_url}\nResponse: {json.dumps(payload, ensure_ascii=False, indent=2)}"
                )

            if REQUEST_SLEEP_SECONDS > 0:
                time.sleep(REQUEST_SLEEP_SECONDS)

    def insert_snapshot(self, object_type: str, object_id: Optional[str], payload: Dict[str, Any], snapshot_time: str) -> None:
        if not object_id:
            return
        payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        payload_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()

        self.conn.execute(
            """
            INSERT OR IGNORE INTO object_snapshots (
                account_id, object_type, object_id, snapshot_time, payload_json, payload_hash
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (self.ad_account_id, object_type, object_id, snapshot_time, payload_json, payload_hash),
        )

    def upsert_campaigns(self, rows: Iterable[Dict[str, Any]]) -> int:
        cur = self.conn.cursor()
        now = utc_now_iso()
        count = 0

        for row in rows:
            cur.execute(
                """
                INSERT INTO campaigns (
                    account_id, campaign_id, name, objective, status, effective_status,
                    buying_type, updated_time, raw_json, last_seen_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id, campaign_id) DO UPDATE SET
                    name=excluded.name,
                    objective=excluded.objective,
                    status=excluded.status,
                    effective_status=excluded.effective_status,
                    buying_type=excluded.buying_type,
                    updated_time=excluded.updated_time,
                    raw_json=excluded.raw_json,
                    last_seen_at=excluded.last_seen_at
                """,
                (
                    self.ad_account_id,
                    row.get("id"),
                    row.get("name"),
                    row.get("objective"),
                    row.get("status"),
                    row.get("effective_status"),
                    row.get("buying_type"),
                    row.get("updated_time"),
                    json.dumps(row, ensure_ascii=False),
                    now,
                ),
            )
            self.insert_snapshot("campaign", row.get("id"), row, now)
            count += 1

        self.conn.commit()
        return count

    def upsert_adsets(self, rows: Iterable[Dict[str, Any]]) -> int:
        cur = self.conn.cursor()
        now = utc_now_iso()
        count = 0

        for row in rows:
            cur.execute(
                """
                INSERT INTO adsets (
                    account_id, adset_id, campaign_id, name, status, effective_status,
                    optimization_goal, billing_event, bid_strategy, daily_budget, lifetime_budget,
                    start_time, end_time, targeting_json, attribution_spec_json, raw_json, last_seen_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id, adset_id) DO UPDATE SET
                    campaign_id=excluded.campaign_id,
                    name=excluded.name,
                    status=excluded.status,
                    effective_status=excluded.effective_status,
                    optimization_goal=excluded.optimization_goal,
                    billing_event=excluded.billing_event,
                    bid_strategy=excluded.bid_strategy,
                    daily_budget=excluded.daily_budget,
                    lifetime_budget=excluded.lifetime_budget,
                    start_time=excluded.start_time,
                    end_time=excluded.end_time,
                    targeting_json=excluded.targeting_json,
                    attribution_spec_json=excluded.attribution_spec_json,
                    raw_json=excluded.raw_json,
                    last_seen_at=excluded.last_seen_at
                """,
                (
                    self.ad_account_id,
                    row.get("id"),
                    row.get("campaign_id"),
                    row.get("name"),
                    row.get("status"),
                    row.get("effective_status"),
                    row.get("optimization_goal"),
                    row.get("billing_event"),
                    row.get("bid_strategy"),
                    to_int(row.get("daily_budget")),
                    to_int(row.get("lifetime_budget")),
                    row.get("start_time"),
                    row.get("end_time"),
                    json.dumps(row.get("targeting"), ensure_ascii=False) if row.get("targeting") is not None else None,
                    json.dumps(row.get("attribution_spec"), ensure_ascii=False) if row.get("attribution_spec") is not None else None,
                    json.dumps(row, ensure_ascii=False),
                    now,
                ),
            )
            self.insert_snapshot("adset", row.get("id"), row, now)
            count += 1

        self.conn.commit()
        return count

    def upsert_ads(self, rows: Iterable[Dict[str, Any]]) -> int:
        cur = self.conn.cursor()
        now = utc_now_iso()
        count = 0

        for row in rows:
            creative = row.get("creative") or {}
            cur.execute(
                """
                INSERT INTO ads (
                    account_id, ad_id, campaign_id, adset_id, name, status,
                    effective_status, creative_id, raw_json, last_seen_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id, ad_id) DO UPDATE SET
                    campaign_id=excluded.campaign_id,
                    adset_id=excluded.adset_id,
                    name=excluded.name,
                    status=excluded.status,
                    effective_status=excluded.effective_status,
                    creative_id=excluded.creative_id,
                    raw_json=excluded.raw_json,
                    last_seen_at=excluded.last_seen_at
                """,
                (
                    self.ad_account_id,
                    row.get("id"),
                    row.get("campaign_id"),
                    row.get("adset_id"),
                    row.get("name"),
                    row.get("status"),
                    row.get("effective_status"),
                    creative.get("id"),
                    json.dumps(row, ensure_ascii=False),
                    now,
                ),
            )
            self.insert_snapshot("ad", row.get("id"), row, now)
            count += 1

        self.conn.commit()
        return count

    def fetch_campaigns(self) -> List[Dict[str, Any]]:
        fields = [
            "id", "account_id", "name", "objective", "status",
            "effective_status", "buying_type", "updated_time"
        ]
        return list(self._paginate(f"{self.ad_account_id}/campaigns", {"fields": ",".join(fields), "limit": 200}))

    def fetch_adsets(self) -> List[Dict[str, Any]]:
        fields = [
            "id", "account_id", "campaign_id", "name", "status", "effective_status",
            "optimization_goal", "billing_event", "bid_strategy", "daily_budget",
            "lifetime_budget", "start_time", "end_time", "targeting", "attribution_spec"
        ]
        return list(self._paginate(f"{self.ad_account_id}/adsets", {"fields": ",".join(fields), "limit": 200}))

    def fetch_ads(self) -> List[Dict[str, Any]]:
        fields = [
            "id", "account_id", "campaign_id", "adset_id", "name",
            "status", "effective_status", "creative{id,name}"
        ]
        return list(self._paginate(f"{self.ad_account_id}/ads", {"fields": ",".join(fields), "limit": 200}))

    def fetch_insights(
        self,
        since: str,
        until: str,
        level: str = "ad",
        breakdowns: Optional[List[str]] = None,
        action_report_time: str = "conversion",
    ) -> List[Dict[str, Any]]:
        fields = [
            "campaign_id", "campaign_name",
            "adset_id", "adset_name",
            "ad_id", "ad_name",
            "objective", "optimization_goal",
            "date_start", "date_stop",
            "spend", "impressions", "reach", "frequency",
            "clicks", "unique_clicks", "ctr", "cpc", "cpm",
            "outbound_clicks", "actions", "action_values", "cost_per_action_type"
        ]

        params: Dict[str, Any] = {
            "level": level,
            "fields": ",".join(fields),
            "time_range": json.dumps({"since": since, "until": until}),
            "time_increment": 1,
            "limit": PAGE_LIMIT,
            "action_report_time": action_report_time,
        }

        if breakdowns:
            params["breakdowns"] = ",".join(breakdowns)

        return list(self._paginate(f"{self.ad_account_id}/insights", params))

    def _make_insights_grain_key(self, row: Dict[str, Any], level: str, action_report_time: str) -> str:
        parts = [
            self.ad_account_id,
            row.get("date_start") or "",
            level or "",
            row.get("campaign_id") or "",
            row.get("adset_id") or "",
            row.get("ad_id") or "",
            row.get("publisher_platform") or "",
            row.get("platform_position") or "",
            row.get("impression_device") or "",
            row.get("attribution_setting") or "",
            action_report_time or "",
        ]
        return "|".join(parts)

    def _make_action_grain_key(
        self,
        row: Dict[str, Any],
        level: str,
        action_type: str,
        action_category: str,
        action_report_time: str,
    ) -> str:
        parts = [
            self.ad_account_id,
            row.get("date_start") or "",
            level or "",
            row.get("campaign_id") or "",
            row.get("adset_id") or "",
            row.get("ad_id") or "",
            row.get("publisher_platform") or "",
            row.get("platform_position") or "",
            row.get("impression_device") or "",
            action_type or "",
            action_category or "",
            row.get("attribution_setting") or "",
            action_report_time or "",
        ]
        return "|".join(parts)

    def upsert_insights_rows(
        self,
        rows: Iterable[Dict[str, Any]],
        level: str,
        action_report_time: str,
    ) -> Tuple[int, int]:
        cur = self.conn.cursor()
        request_time = utc_now_iso()
        insight_count = 0
        action_count = 0

        for row in rows:
            grain_key = self._make_insights_grain_key(row, level, action_report_time)
            outbound_clicks = extract_action_value(row.get("outbound_clicks"), key_name="action_type", target="outbound_click")
            landing_page_views = extract_action_value(row.get("actions"), key_name="action_type", target="landing_page_view")

            cur.execute(
                """
                INSERT INTO insights_daily (
                    grain_key, account_id, date_start, date_stop, level,
                    campaign_id, campaign_name, adset_id, adset_name, ad_id, ad_name,
                    objective, optimization_goal,
                    publisher_platform, platform_position, impression_device,
                    spend, impressions, reach, frequency, clicks, unique_clicks, ctr, cpc, cpm,
                    outbound_clicks, landing_page_views,
                    actions_json, action_values_json, cost_per_action_type_json,
                    attribution_setting, action_report_time, request_time, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(grain_key) DO UPDATE SET
                    account_id=excluded.account_id,
                    date_stop=excluded.date_stop,
                    campaign_id=excluded.campaign_id,
                    campaign_name=excluded.campaign_name,
                    adset_id=excluded.adset_id,
                    adset_name=excluded.adset_name,
                    ad_id=excluded.ad_id,
                    ad_name=excluded.ad_name,
                    objective=excluded.objective,
                    optimization_goal=excluded.optimization_goal,
                    publisher_platform=excluded.publisher_platform,
                    platform_position=excluded.platform_position,
                    impression_device=excluded.impression_device,
                    spend=excluded.spend,
                    impressions=excluded.impressions,
                    reach=excluded.reach,
                    frequency=excluded.frequency,
                    clicks=excluded.clicks,
                    unique_clicks=excluded.unique_clicks,
                    ctr=excluded.ctr,
                    cpc=excluded.cpc,
                    cpm=excluded.cpm,
                    outbound_clicks=excluded.outbound_clicks,
                    landing_page_views=excluded.landing_page_views,
                    actions_json=excluded.actions_json,
                    action_values_json=excluded.action_values_json,
                    cost_per_action_type_json=excluded.cost_per_action_type_json,
                    attribution_setting=excluded.attribution_setting,
                    action_report_time=excluded.action_report_time,
                    request_time=excluded.request_time,
                    raw_json=excluded.raw_json
                """,
                (
                    grain_key,
                    self.ad_account_id,
                    row.get("date_start"),
                    row.get("date_stop"),
                    level,
                    row.get("campaign_id"),
                    row.get("campaign_name"),
                    row.get("adset_id"),
                    row.get("adset_name"),
                    row.get("ad_id"),
                    row.get("ad_name"),
                    row.get("objective"),
                    row.get("optimization_goal"),
                    row.get("publisher_platform"),
                    row.get("platform_position"),
                    row.get("impression_device"),
                    to_float(row.get("spend")),
                    to_int(row.get("impressions")),
                    to_int(row.get("reach")),
                    to_float(row.get("frequency")),
                    to_int(row.get("clicks")),
                    to_int(row.get("unique_clicks")),
                    to_float(row.get("ctr")),
                    to_float(row.get("cpc")),
                    to_float(row.get("cpm")),
                    to_int(outbound_clicks),
                    to_int(landing_page_views),
                    json.dumps(row.get("actions"), ensure_ascii=False) if row.get("actions") is not None else None,
                    json.dumps(row.get("action_values"), ensure_ascii=False) if row.get("action_values") is not None else None,
                    json.dumps(row.get("cost_per_action_type"), ensure_ascii=False) if row.get("cost_per_action_type") is not None else None,
                    row.get("attribution_setting"),
                    action_report_time,
                    request_time,
                    json.dumps(row, ensure_ascii=False),
                ),
            )
            insight_count += 1
            action_count += self.upsert_actions_long_table(row, level, action_report_time, request_time)

        self.conn.commit()
        return insight_count, action_count

    def upsert_actions_long_table(
        self,
        row: Dict[str, Any],
        level: str,
        action_report_time: str,
        request_time: str,
    ) -> int:
        count = 0
        count += self._insert_action_group(row, row.get("actions"), "action", level, action_report_time, request_time)
        count += self._insert_action_group(row, row.get("action_values"), "action_value", level, action_report_time, request_time)
        count += self._insert_action_group(row, row.get("cost_per_action_type"), "cost_per_action", level, action_report_time, request_time)
        return count

    def _insert_action_group(
        self,
        row: Dict[str, Any],
        values: Any,
        action_category: str,
        level: str,
        action_report_time: str,
        request_time: str,
    ) -> int:
        if not isinstance(values, list):
            return 0

        cur = self.conn.cursor()
        count = 0

        for item in values:
            action_type = item.get("action_type")
            value = to_float(item.get("value"))
            if not action_type:
                continue

            grain_key = self._make_action_grain_key(row, level, action_type, action_category, action_report_time)

            cur.execute(
                """
                INSERT INTO actions_daily (
                    grain_key, account_id, date_start, level, campaign_id, adset_id, ad_id,
                    publisher_platform, platform_position, impression_device,
                    action_type, action_category, value,
                    attribution_setting, action_report_time, request_time
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(grain_key) DO UPDATE SET
                    account_id=excluded.account_id,
                    date_start=excluded.date_start,
                    level=excluded.level,
                    campaign_id=excluded.campaign_id,
                    adset_id=excluded.adset_id,
                    ad_id=excluded.ad_id,
                    publisher_platform=excluded.publisher_platform,
                    platform_position=excluded.platform_position,
                    impression_device=excluded.impression_device,
                    action_type=excluded.action_type,
                    action_category=excluded.action_category,
                    value=excluded.value,
                    attribution_setting=excluded.attribution_setting,
                    action_report_time=excluded.action_report_time,
                    request_time=excluded.request_time
                """,
                (
                    grain_key,
                    self.ad_account_id,
                    row.get("date_start"),
                    level,
                    row.get("campaign_id"),
                    row.get("adset_id"),
                    row.get("ad_id"),
                    row.get("publisher_platform"),
                    row.get("platform_position"),
                    row.get("impression_device"),
                    action_type,
                    action_category,
                    value,
                    row.get("attribution_setting"),
                    action_report_time,
                    request_time,
                ),
            )
            count += 1

        return count


def sync_dimensions(etl: MetaAdsETL) -> None:
    print(f"同步主檔：{etl.ad_account_id}")

    campaigns = etl.fetch_campaigns()
    c_count = etl.upsert_campaigns(campaigns)

    adsets = etl.fetch_adsets()
    s_count = etl.upsert_adsets(adsets)

    ads = etl.fetch_ads()
    a_count = etl.upsert_ads(ads)

    print(f"  campaigns: {c_count}")
    print(f"  adsets:    {s_count}")
    print(f"  ads:       {a_count}")


def sync_insights_for_range(
    etl: MetaAdsETL,
    since: str,
    until: str,
    level: str = "ad",
    enable_breakdown: bool = True,
) -> None:
    print(f"抓取 insights：{etl.ad_account_id} | {since} ~ {until}")

    rows = etl.fetch_insights(
        since=since,
        until=until,
        level=level,
        breakdowns=None,
        action_report_time="conversion",
    )
    insight_count, action_count = etl.upsert_insights_rows(rows, level=level, action_report_time="conversion")
    print(f"  無 breakdown: insights={insight_count}, actions={action_count}")

    if enable_breakdown:
        placement_rows = etl.fetch_insights(
            since=since,
            until=until,
            level=level,
            breakdowns=["publisher_platform", "platform_position", "impression_device"],
            action_report_time="conversion",
        )
        insight_count_b, action_count_b = etl.upsert_insights_rows(
            placement_rows,
            level=level,
            action_report_time="conversion",
        )
        print(f"  placement breakdown: insights={insight_count_b}, actions={action_count_b}")


def run_update_mode(etl: MetaAdsETL, lookback_days: int) -> None:
    sync_dimensions(etl)
    since, until = daterange_for_lookback(lookback_days)
    sync_insights_for_range(etl, since, until, level="ad", enable_breakdown=ENABLE_PLACEMENT_BREAKDOWN)
    print(f"UPDATE 完成：{etl.ad_account_id} | {since} ~ {until}")


def run_backfill_mode(etl: MetaAdsETL, start_date: date, end_date: date) -> None:
    sync_dimensions(etl)

    ranges = month_ranges(start_date, end_date)
    print(f"{etl.ad_account_id} 共 {len(ranges)} 段月份區間要回補")

    for idx, (since, until) in enumerate(ranges, start=1):
        print(f"[{idx}/{len(ranges)}]")
        sync_insights_for_range(etl, since, until, level="ad", enable_breakdown=ENABLE_PLACEMENT_BREAKDOWN)

    print(f"BACKFILL 完成：{etl.ad_account_id} | {start_date.isoformat()} ~ {end_date.isoformat()}")


def parse_iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise ValueError(f"日期格式錯誤：{value}，請用 YYYY-MM-DD")


def db_scalar(conn: sqlite3.Connection, sql: str, params: Tuple[Any, ...] = ()) -> Any:
    row = conn.execute(sql, params).fetchone()
    if row is None:
        return None
    return row[0]


def run_check_mode(etl: MetaAdsETL) -> None:
    conn = etl.conn
    acct = etl.ad_account_id

    campaigns = db_scalar(conn, "SELECT COUNT(*) FROM campaigns WHERE account_id = ?", (acct,))
    adsets = db_scalar(conn, "SELECT COUNT(*) FROM adsets WHERE account_id = ?", (acct,))
    ads = db_scalar(conn, "SELECT COUNT(*) FROM ads WHERE account_id = ?", (acct,))
    insights = db_scalar(conn, "SELECT COUNT(*) FROM insights_daily WHERE account_id = ?", (acct,))
    actions = db_scalar(conn, "SELECT COUNT(*) FROM actions_daily WHERE account_id = ?", (acct,))
    min_date = db_scalar(conn, "SELECT MIN(date_start) FROM insights_daily WHERE account_id = ?", (acct,))
    max_date = db_scalar(conn, "SELECT MAX(date_start) FROM insights_daily WHERE account_id = ?", (acct,))

    print(f"資料庫檢查：{acct}")
    print(f"  campaigns: {campaigns}")
    print(f"  adsets:    {adsets}")
    print(f"  ads:       {ads}")
    print(f"  insights:  {insights}")
    print(f"  actions:   {actions}")
    print(f"  date range: {min_date} ~ {max_date}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Meta Ads -> SQLite ETL (multi-account)")
    parser.add_argument("--account", default=DEFAULT_AD_ACCOUNT_ID, help="Meta ad account，例如 act_123456789")

    subparsers = parser.add_subparsers(dest="command")

    p_update = subparsers.add_parser("update", help="抓最近 N 天資料")
    p_update.add_argument("--days", type=int, default=DEFAULT_LOOKBACK_DAYS, help="回補最近幾天，預設 14")

    p_backfill = subparsers.add_parser("backfill", help="分月回補歷史資料")
    p_backfill.add_argument("--start", required=True, help="開始日期，例如 2025-01-01")
    p_backfill.add_argument("--end", required=True, help="結束日期，例如 2026-03-08")

    subparsers.add_parser("sync-dimensions", help="只同步 campaign / adset / ad 主檔")
    subparsers.add_parser("check", help="檢查指定 account 的資料庫狀況")

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    command = args.command or "update"
    ad_account_id = args.account

    etl = MetaAdsETL(DB_PATH, ad_account_id=ad_account_id)

    try:
        if command == "update":
            run_update_mode(etl, lookback_days=getattr(args, "days", DEFAULT_LOOKBACK_DAYS))
        elif command == "backfill":
            start_date = parse_iso_date(args.start)
            end_date = parse_iso_date(args.end)
            run_backfill_mode(etl, start_date=start_date, end_date=end_date)
        elif command == "sync-dimensions":
            sync_dimensions(etl)
            print(f"主檔同步完成：{ad_account_id}")
        elif command == "check":
            run_check_mode(etl)
        else:
            raise RuntimeError(f"未知指令：{command}")
    finally:
        etl.close()


if __name__ == "__main__":
    main()