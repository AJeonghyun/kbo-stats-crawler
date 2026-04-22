from __future__ import annotations

import argparse
import getpass
import json
import logging
import re
from collections import deque
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from html import unescape
from typing import Any
from urllib.parse import urljoin, urlparse
from zoneinfo import ZoneInfo

import psycopg
import requests
from bs4 import BeautifulSoup
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

SEOUL = ZoneInfo("Asia/Seoul")
BASE_URL = "https://www.koreabaseball.com"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/135.0.0.0 Safari/537.36"
)

PLAYER_BASIC_1_URL = (
    "https://www.koreabaseball.com/Record/Player/HitterBasic/Basic1.aspx?sort=HRA_RT"
)
PLAYER_BASIC_2_URL = (
    "https://www.koreabaseball.com/Record/Player/HitterBasic/Basic2.aspx?sort=HRA_RT"
)
TEAM_BASIC_1_URL = "https://www.koreabaseball.com/Record/Team/Hitter/Basic1.aspx"
TEAM_BASIC_2_URL = "https://www.koreabaseball.com/Record/Team/Hitter/Basic2.aspx"

TEAM_CODE_BY_NAME = {
    "KIA": "KIA",
    "KT": "KT",
    "LG": "LG",
    "NC": "NC",
    "SSG": "SSG",
    "두산": "DOOSAN",
    "롯데": "LOTTE",
    "삼성": "SAMSUNG",
    "키움": "KIWOOM",
    "한화": "HANWHA",
}

PLAYER_TABLE_HEADERS = {"순위", "선수명", "팀명", "AVG"}
TEAM_TABLE_HEADERS = {"순위", "팀명", "AVG"}

DIRECT_URL_PATTERN = re.compile(r"""['"](?P<url>/Record/[^'"]+\.aspx[^'"]*)['"]""")
POSTBACK_PATTERN = re.compile(r"__doPostBack\('(?P<target>[^']*)','(?P<argument>[^']*)'\)")
PLAYER_ID_PATTERN = re.compile(r"(?:playerId|pcode|playerCode)=([^&]+)", re.IGNORECASE)
HIDDEN_INPUT_PATTERN = re.compile(
    r'<input[^>]+type="hidden"[^>]+name="(?P<name>[^"]+)"[^>]+value="(?P<value>[^"]*)"',
    re.IGNORECASE,
)


@dataclass(frozen=True)
class PageRequest:
    method: str
    url: str
    event_target: str | None = None
    event_argument: str | None = None
    form_state: tuple[tuple[str, str], ...] = ()

    @property
    def key(self) -> tuple[str, str, str | None, str | None]:
        return (self.method, self.url, self.event_target, self.event_argument)


@dataclass
class ParsedPage:
    request: PageRequest
    rows: list[dict[str, Any]]
    next_requests: list[PageRequest]
    form_state: dict[str, str]


def seoul_today() -> date:
    return datetime.now(SEOUL).date()


def normalize_space(value: str | None) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", value.replace("\xa0", " ")).strip()


def to_int(value: str | None) -> int | None:
    text = normalize_space(value).replace(",", "")
    if not text or text in {"-", "—"}:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def to_decimal(value: str | None) -> Decimal | None:
    text = normalize_space(value).replace(",", "")
    if not text or text in {"-", "—"}:
        return None
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None


def safe_json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: safe_json_value(inner) for key, inner in value.items()}
    if isinstance(value, list):
        return [safe_json_value(inner) for inner in value]
    return value


def canonical_team_code(team_name: str) -> str:
    return TEAM_CODE_BY_NAME.get(team_name, re.sub(r"\s+", "_", team_name.upper()))


def extract_player_id(href: str | None) -> str | None:
    if not href:
        return None
    matched = PLAYER_ID_PATTERN.search(href)
    if matched:
        return matched.group(1)
    return None


class KboScraper:
    def __init__(self, timeout: int = 30) -> None:
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": USER_AGENT,
                "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            }
        )
        self.timeout = timeout

    def crawl_table(self, url: str, expected_headers: set[str]) -> list[dict[str, Any]]:
        queue: deque[PageRequest] = deque([PageRequest(method="GET", url=url)])
        visited: set[tuple[str, str, str | None, str | None]] = set()
        combined_rows: list[dict[str, Any]] = []

        while queue:
            page_request = queue.popleft()
            if page_request.key in visited:
                continue
            visited.add(page_request.key)

            parsed_page = self._fetch_and_parse(page_request, expected_headers)
            combined_rows.extend(parsed_page.rows)

            for next_request in parsed_page.next_requests:
                if next_request.key not in visited:
                    queue.append(next_request)

        unique_rows: list[dict[str, Any]] = []
        seen: set[str] = set()
        for row in combined_rows:
            row_key = json.dumps(safe_json_value(row), ensure_ascii=False, sort_keys=True)
            if row_key in seen:
                continue
            seen.add(row_key)
            unique_rows.append(row)

        return unique_rows

    def _fetch_and_parse(
        self, page_request: PageRequest, expected_headers: set[str]
    ) -> ParsedPage:
        if page_request.method == "GET":
            response = self.session.get(page_request.url, timeout=self.timeout)
        else:
            form_state = dict(page_request.form_state)
            form_state["__EVENTTARGET"] = page_request.event_target or ""
            form_state["__EVENTARGUMENT"] = page_request.event_argument or ""
            response = self.session.post(
                page_request.url,
                data=form_state,
                timeout=self.timeout,
            )

        response.raise_for_status()
        response.encoding = response.encoding or response.apparent_encoding or "utf-8"
        html = response.text
        soup = BeautifulSoup(html, "html.parser")

        table = self._select_target_table(soup, expected_headers)
        rows = self._extract_rows(table, page_request.url)
        form_state = self._extract_form_state(html)
        next_requests = self._extract_next_requests(
            soup=soup,
            current_request=page_request,
            current_url=response.url,
            form_state=form_state,
        )

        return ParsedPage(
            request=page_request,
            rows=rows,
            next_requests=next_requests,
            form_state=form_state,
        )

    def _select_target_table(
        self, soup: BeautifulSoup, expected_headers: set[str]
    ) -> Any:
        candidates = soup.find_all("table")
        for table in candidates:
            headers = [
                normalize_space(th.get_text(" ", strip=True))
                for th in table.select("thead th")
            ]
            if not headers:
                first_row = table.find("tr")
                if first_row is not None:
                    headers = [
                        normalize_space(cell.get_text(" ", strip=True))
                        for cell in first_row.find_all(["th", "td"])
                    ]
            if expected_headers.issubset(set(headers)):
                return table
        raise ValueError("기대하는 KBO 기록 테이블을 찾지 못했습니다.")

    def _extract_rows(self, table: Any, current_url: str) -> list[dict[str, Any]]:
        header_cells = table.select("thead th")
        if header_cells:
            headers = [normalize_space(cell.get_text(" ", strip=True)) for cell in header_cells]
            body_rows = table.select("tbody tr")
        else:
            all_rows = table.find_all("tr")
            headers = [
                normalize_space(cell.get_text(" ", strip=True))
                for cell in all_rows[0].find_all(["th", "td"])
            ]
            body_rows = all_rows[1:]

        rows: list[dict[str, Any]] = []
        for tr in body_rows:
            cells = tr.find_all("td")
            if not cells or len(cells) != len(headers):
                continue

            values = [normalize_space(cell.get_text(" ", strip=True)) for cell in cells]
            if values[0] == "합계":
                continue

            row = dict(zip(headers, values))
            entity_anchor = tr.find("a")
            if entity_anchor and entity_anchor.get("href"):
                href = urljoin(current_url, entity_anchor["href"])
                row["_href"] = href
                player_id = extract_player_id(href)
                if player_id:
                    row["_player_id"] = player_id
            rows.append(row)

        return rows

    def _extract_form_state(self, html: str) -> dict[str, str]:
        state: dict[str, str] = {}
        for matched in HIDDEN_INPUT_PATTERN.finditer(html):
            state[matched.group("name")] = unescape(matched.group("value"))
        return state

    def _extract_next_requests(
        self,
        soup: BeautifulSoup,
        current_request: PageRequest,
        current_url: str,
        form_state: dict[str, str],
    ) -> list[PageRequest]:
        requests_found: dict[tuple[str, str, str | None, str | None], PageRequest] = {}
        current_path = urlparse(current_url).path.lower()

        for anchor in soup.find_all("a"):
            label = normalize_space(anchor.get_text(" ", strip=True))
            if not label and anchor.find("img") is not None:
                label = normalize_space(
                    anchor.find("img").get("alt") or anchor.find("img").get("title")
                )

            href = anchor.get("href", "")
            onclick = anchor.get("onclick", "")

            direct_url = self._extract_direct_url(href or onclick, current_url)
            if direct_url and label.isdigit():
                parsed_direct = urlparse(direct_url)
                if parsed_direct.path.lower() == current_path and direct_url != current_url:
                    request = PageRequest(method="GET", url=direct_url)
                    requests_found[request.key] = request

            postback = self._extract_postback_action(href) or self._extract_postback_action(onclick)
            if postback and label.isdigit():
                request = PageRequest(
                    method="POST",
                    url=current_url,
                    event_target=postback[0],
                    event_argument=postback[1],
                    form_state=tuple(sorted(form_state.items())),
                )
                requests_found[request.key] = request

        return list(requests_found.values())

    def _extract_direct_url(self, candidate: str, current_url: str) -> str | None:
        if not candidate:
            return None
        if candidate.startswith("http"):
            return candidate
        if candidate.startswith("/"):
            return urljoin(BASE_URL, candidate)
        if ".aspx" in candidate:
            return urljoin(current_url, candidate)

        matched = DIRECT_URL_PATTERN.search(unescape(candidate))
        if matched:
            return urljoin(BASE_URL, unescape(matched.group("url")))
        return None

    def _extract_postback_action(self, candidate: str) -> tuple[str, str] | None:
        if not candidate:
            return None
        matched = POSTBACK_PATTERN.search(candidate)
        if matched:
            return matched.group("target"), matched.group("argument")
        return None


def merge_player_rows(
    basic_rows: list[dict[str, Any]],
    detail_rows: list[dict[str, Any]],
    stat_date: date,
    season_year: int,
) -> list[dict[str, Any]]:
    merged: dict[tuple[str, str], dict[str, Any]] = {}

    for row in basic_rows:
        key = (row["선수명"], row["팀명"])
        merged[key] = {
            "player_name": row["선수명"],
            "team_name": row["팀명"],
            "kbo_player_id": row.get("_player_id"),
            "stat_date": stat_date,
            "season_year": season_year,
            "stat_rank": to_int(row.get("순위")),
            "games_played": to_int(row.get("G")),
            "plate_appearances": to_int(row.get("PA")),
            "at_bats": to_int(row.get("AB")),
            "runs": to_int(row.get("R")),
            "hits": to_int(row.get("H")),
            "doubles": to_int(row.get("2B")),
            "triples": to_int(row.get("3B")),
            "home_runs": to_int(row.get("HR")),
            "total_bases": to_int(row.get("TB")),
            "runs_batted_in": to_int(row.get("RBI")),
            "sacrifice_bunts": to_int(row.get("SAC")),
            "sacrifice_flies": to_int(row.get("SF")),
            "batting_average": to_decimal(row.get("AVG")),
            "walks": None,
            "intentional_walks": None,
            "hit_by_pitch": None,
            "strikeouts": None,
            "double_plays": None,
            "slugging_percentage": None,
            "on_base_percentage": None,
            "ops": None,
            "multi_hits": None,
            "runners_in_scoring_position_avg": None,
            "pinch_hit_batting_average": None,
            "raw_pages": {"basic1": row},
        }

    for row in detail_rows:
        key = (row["선수명"], row["팀명"])
        if key not in merged:
            merged[key] = {
                "player_name": row["선수명"],
                "team_name": row["팀명"],
                "kbo_player_id": row.get("_player_id"),
                "stat_date": stat_date,
                "season_year": season_year,
                "stat_rank": to_int(row.get("순위")),
                "games_played": None,
                "plate_appearances": None,
                "at_bats": None,
                "runs": None,
                "hits": None,
                "doubles": None,
                "triples": None,
                "home_runs": None,
                "total_bases": None,
                "runs_batted_in": None,
                "sacrifice_bunts": None,
                "sacrifice_flies": None,
                "batting_average": to_decimal(row.get("AVG")),
                "raw_pages": {},
            }

        merged[key].update(
            {
                "walks": to_int(row.get("BB")),
                "intentional_walks": to_int(row.get("IBB")),
                "hit_by_pitch": to_int(row.get("HBP")),
                "strikeouts": to_int(row.get("SO")),
                "double_plays": to_int(row.get("GDP")),
                "slugging_percentage": to_decimal(row.get("SLG")),
                "on_base_percentage": to_decimal(row.get("OBP")),
                "ops": to_decimal(row.get("OPS")),
                "multi_hits": to_int(row.get("MH")),
                "runners_in_scoring_position_avg": to_decimal(row.get("RISP")),
                "pinch_hit_batting_average": to_decimal(row.get("PH-BA")),
            }
        )
        merged[key]["raw_pages"]["basic2"] = row

    return list(merged.values())


def merge_team_rows(
    basic_rows: list[dict[str, Any]],
    detail_rows: list[dict[str, Any]],
    stat_date: date,
    season_year: int,
) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}

    for row in basic_rows:
        key = row["팀명"]
        merged[key] = {
            "team_name": key,
            "stat_date": stat_date,
            "season_year": season_year,
            "stat_rank": to_int(row.get("순위")),
            "games_played": to_int(row.get("G")),
            "plate_appearances": to_int(row.get("PA")),
            "at_bats": to_int(row.get("AB")),
            "runs_scored": to_int(row.get("R")),
            "hits": to_int(row.get("H")),
            "doubles": to_int(row.get("2B")),
            "triples": to_int(row.get("3B")),
            "team_home_runs": to_int(row.get("HR")),
            "total_bases": to_int(row.get("TB")),
            "runs_batted_in": to_int(row.get("RBI")),
            "sacrifice_bunts": to_int(row.get("SAC")),
            "sacrifice_flies": to_int(row.get("SF")),
            "team_batting_average": to_decimal(row.get("AVG")),
            "walks": None,
            "intentional_walks": None,
            "hit_by_pitch": None,
            "strikeouts": None,
            "double_plays": None,
            "team_slugging_percentage": None,
            "team_on_base_percentage": None,
            "team_ops": None,
            "team_multi_hits": None,
            "team_risp_avg": None,
            "team_pinch_hit_batting_average": None,
            "raw_pages": {"basic1": row},
        }

    for row in detail_rows:
        key = row["팀명"]
        if key not in merged:
            merged[key] = {
                "team_name": key,
                "stat_date": stat_date,
                "season_year": season_year,
                "stat_rank": to_int(row.get("순위")),
                "games_played": None,
                "plate_appearances": None,
                "at_bats": None,
                "runs_scored": None,
                "hits": None,
                "doubles": None,
                "triples": None,
                "team_home_runs": None,
                "total_bases": None,
                "runs_batted_in": None,
                "sacrifice_bunts": None,
                "sacrifice_flies": None,
                "team_batting_average": to_decimal(row.get("AVG")),
                "raw_pages": {},
            }

        merged[key].update(
            {
                "walks": to_int(row.get("BB")),
                "intentional_walks": to_int(row.get("IBB")),
                "hit_by_pitch": to_int(row.get("HBP")),
                "strikeouts": to_int(row.get("SO")),
                "double_plays": to_int(row.get("GDP")),
                "team_slugging_percentage": to_decimal(row.get("SLG")),
                "team_on_base_percentage": to_decimal(row.get("OBP")),
                "team_ops": to_decimal(row.get("OPS")),
                "team_multi_hits": to_int(row.get("MH")),
                "team_risp_avg": to_decimal(row.get("RISP")),
                "team_pinch_hit_batting_average": to_decimal(row.get("PH-BA")),
            }
        )
        merged[key]["raw_pages"]["basic2"] = row

    return list(merged.values())


class PostgresWriter:
    def __init__(
        self,
        *,
        dbname: str,
        user: str,
        password: str | None,
        host: str | None,
        port: int,
    ) -> None:
        connect_kwargs: dict[str, Any] = {
            "dbname": dbname,
            "user": user,
            "port": port,
            "row_factory": dict_row,
        }
        if password:
            connect_kwargs["password"] = password
        if host:
            connect_kwargs["host"] = host
        self.connection = psycopg.connect(**connect_kwargs)

    def close(self) -> None:
        self.connection.close()

    def create_crawl_job(
        self,
        *,
        source_name: str,
        source_url: str,
        stat_scope: str,
        target_date: date,
    ) -> int:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO crawl_jobs (
                    source_name,
                    source_url,
                    stat_scope,
                    target_date
                )
                VALUES (%s, %s, %s, %s)
                RETURNING crawl_job_id
                """,
                (source_name, source_url, stat_scope, target_date),
            )
            crawl_job_id = cursor.fetchone()["crawl_job_id"]
        self.connection.commit()
        return crawl_job_id

    def finish_crawl_job(
        self,
        crawl_job_id: int,
        *,
        status: str,
        row_count: int,
        error_message: str | None = None,
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE crawl_jobs
                SET status = %s,
                    row_count = %s,
                    error_message = %s,
                    finished_at = NOW()
                WHERE crawl_job_id = %s
                """,
                (status, row_count, error_message, crawl_job_id),
            )
        self.connection.commit()

    def upsert_players_and_stats(
        self,
        player_rows: list[dict[str, Any]],
        *,
        crawl_job_id: int,
    ) -> None:
        with self.connection.cursor() as cursor:
            team_ids = self._ensure_teams(cursor, [row["team_name"] for row in player_rows])

            for row in player_rows:
                team_id = team_ids[row["team_name"]]
                player_id = self._ensure_player(cursor, row, team_id)

                cursor.execute(
                    """
                    INSERT INTO player_hitter_daily_stats (
                        player_id,
                        team_id,
                        stat_date,
                        season_year,
                        stat_rank,
                        games_played,
                        plate_appearances,
                        at_bats,
                        runs,
                        hits,
                        doubles,
                        triples,
                        home_runs,
                        total_bases,
                        runs_batted_in,
                        walks,
                        intentional_walks,
                        hit_by_pitch,
                        strikeouts,
                        double_plays,
                        sacrifice_bunts,
                        sacrifice_flies,
                        multi_hits,
                        batting_average,
                        on_base_percentage,
                        slugging_percentage,
                        ops,
                        runners_in_scoring_position_avg,
                        pinch_hit_batting_average,
                        crawl_job_id
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (player_id, stat_date)
                    DO UPDATE SET
                        team_id = EXCLUDED.team_id,
                        season_year = EXCLUDED.season_year,
                        stat_rank = EXCLUDED.stat_rank,
                        games_played = EXCLUDED.games_played,
                        plate_appearances = EXCLUDED.plate_appearances,
                        at_bats = EXCLUDED.at_bats,
                        runs = EXCLUDED.runs,
                        hits = EXCLUDED.hits,
                        doubles = EXCLUDED.doubles,
                        triples = EXCLUDED.triples,
                        home_runs = EXCLUDED.home_runs,
                        total_bases = EXCLUDED.total_bases,
                        runs_batted_in = EXCLUDED.runs_batted_in,
                        walks = EXCLUDED.walks,
                        intentional_walks = EXCLUDED.intentional_walks,
                        hit_by_pitch = EXCLUDED.hit_by_pitch,
                        strikeouts = EXCLUDED.strikeouts,
                        double_plays = EXCLUDED.double_plays,
                        sacrifice_bunts = EXCLUDED.sacrifice_bunts,
                        sacrifice_flies = EXCLUDED.sacrifice_flies,
                        multi_hits = EXCLUDED.multi_hits,
                        batting_average = EXCLUDED.batting_average,
                        on_base_percentage = EXCLUDED.on_base_percentage,
                        slugging_percentage = EXCLUDED.slugging_percentage,
                        ops = EXCLUDED.ops,
                        runners_in_scoring_position_avg = EXCLUDED.runners_in_scoring_position_avg,
                        pinch_hit_batting_average = EXCLUDED.pinch_hit_batting_average,
                        crawl_job_id = EXCLUDED.crawl_job_id
                    """,
                    (
                        player_id,
                        team_id,
                        row["stat_date"],
                        row["season_year"],
                        row["stat_rank"],
                        row["games_played"],
                        row["plate_appearances"],
                        row["at_bats"],
                        row["runs"],
                        row["hits"],
                        row["doubles"],
                        row["triples"],
                        row["home_runs"],
                        row["total_bases"],
                        row["runs_batted_in"],
                        row["walks"],
                        row["intentional_walks"],
                        row["hit_by_pitch"],
                        row["strikeouts"],
                        row["double_plays"],
                        row["sacrifice_bunts"],
                        row["sacrifice_flies"],
                        row["multi_hits"],
                        row["batting_average"],
                        row["on_base_percentage"],
                        row["slugging_percentage"],
                        row["ops"],
                        row["runners_in_scoring_position_avg"],
                        row["pinch_hit_batting_average"],
                        crawl_job_id,
                    ),
                )

                cursor.execute(
                    """
                    INSERT INTO raw_crawl_results (
                        crawl_job_id,
                        entity_type,
                        entity_key,
                        stat_date,
                        payload
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        crawl_job_id,
                        "player_hitter",
                        row.get("kbo_player_id") or f"{row['player_name']}|{row['team_name']}",
                        row["stat_date"],
                        Jsonb(safe_json_value(row["raw_pages"])),
                    ),
                )

        self.connection.commit()

    def upsert_teams_and_stats(
        self,
        team_rows: list[dict[str, Any]],
        *,
        crawl_job_id: int,
    ) -> None:
        with self.connection.cursor() as cursor:
            team_ids = self._ensure_teams(cursor, [row["team_name"] for row in team_rows])

            for row in team_rows:
                team_id = team_ids[row["team_name"]]
                cursor.execute(
                    """
                    INSERT INTO team_daily_stats (
                        team_id,
                        stat_date,
                        season_year,
                        stat_rank,
                        games_played,
                        plate_appearances,
                        at_bats,
                        runs_scored,
                        hits,
                        doubles,
                        triples,
                        runs_batted_in,
                        sacrifice_bunts,
                        sacrifice_flies,
                        walks,
                        intentional_walks,
                        hit_by_pitch,
                        strikeouts,
                        double_plays,
                        total_bases,
                        team_batting_average,
                        team_on_base_percentage,
                        team_slugging_percentage,
                        team_ops,
                        team_home_runs,
                        team_multi_hits,
                        team_risp_avg,
                        team_pinch_hit_batting_average,
                        crawl_job_id
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (team_id, stat_date)
                    DO UPDATE SET
                        season_year = EXCLUDED.season_year,
                        stat_rank = EXCLUDED.stat_rank,
                        games_played = EXCLUDED.games_played,
                        plate_appearances = EXCLUDED.plate_appearances,
                        at_bats = EXCLUDED.at_bats,
                        runs_scored = EXCLUDED.runs_scored,
                        hits = EXCLUDED.hits,
                        doubles = EXCLUDED.doubles,
                        triples = EXCLUDED.triples,
                        runs_batted_in = EXCLUDED.runs_batted_in,
                        sacrifice_bunts = EXCLUDED.sacrifice_bunts,
                        sacrifice_flies = EXCLUDED.sacrifice_flies,
                        walks = EXCLUDED.walks,
                        intentional_walks = EXCLUDED.intentional_walks,
                        hit_by_pitch = EXCLUDED.hit_by_pitch,
                        strikeouts = EXCLUDED.strikeouts,
                        double_plays = EXCLUDED.double_plays,
                        total_bases = EXCLUDED.total_bases,
                        team_batting_average = EXCLUDED.team_batting_average,
                        team_on_base_percentage = EXCLUDED.team_on_base_percentage,
                        team_slugging_percentage = EXCLUDED.team_slugging_percentage,
                        team_ops = EXCLUDED.team_ops,
                        team_home_runs = EXCLUDED.team_home_runs,
                        team_multi_hits = EXCLUDED.team_multi_hits,
                        team_risp_avg = EXCLUDED.team_risp_avg,
                        team_pinch_hit_batting_average = EXCLUDED.team_pinch_hit_batting_average,
                        crawl_job_id = EXCLUDED.crawl_job_id
                    """,
                    (
                        team_id,
                        row["stat_date"],
                        row["season_year"],
                        row["stat_rank"],
                        row["games_played"],
                        row["plate_appearances"],
                        row["at_bats"],
                        row["runs_scored"],
                        row["hits"],
                        row["doubles"],
                        row["triples"],
                        row["runs_batted_in"],
                        row["sacrifice_bunts"],
                        row["sacrifice_flies"],
                        row["walks"],
                        row["intentional_walks"],
                        row["hit_by_pitch"],
                        row["strikeouts"],
                        row["double_plays"],
                        row["total_bases"],
                        row["team_batting_average"],
                        row["team_on_base_percentage"],
                        row["team_slugging_percentage"],
                        row["team_ops"],
                        row["team_home_runs"],
                        row["team_multi_hits"],
                        row["team_risp_avg"],
                        row["team_pinch_hit_batting_average"],
                        crawl_job_id,
                    ),
                )

                cursor.execute(
                    """
                    INSERT INTO raw_crawl_results (
                        crawl_job_id,
                        entity_type,
                        entity_key,
                        stat_date,
                        payload
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        crawl_job_id,
                        "team_hitter",
                        row["team_name"],
                        row["stat_date"],
                        Jsonb(safe_json_value(row["raw_pages"])),
                    ),
                )

        self.connection.commit()

    def _ensure_teams(self, cursor: Any, team_names: list[str]) -> dict[str, int]:
        team_ids: dict[str, int] = {}
        for team_name in sorted(set(team_names)):
            team_code = canonical_team_code(team_name)
            cursor.execute(
                """
                INSERT INTO teams (team_code, team_name)
                VALUES (%s, %s)
                ON CONFLICT (team_code)
                DO UPDATE SET
                    team_name = EXCLUDED.team_name,
                    updated_at = NOW()
                RETURNING team_id
                """,
                (team_code, team_name),
            )
            team_ids[team_name] = cursor.fetchone()["team_id"]
        return team_ids

    def _ensure_player(self, cursor: Any, row: dict[str, Any], team_id: int) -> int:
        if row.get("kbo_player_id"):
            cursor.execute(
                """
                INSERT INTO players (
                    kbo_player_id,
                    player_name,
                    team_id
                )
                VALUES (%s, %s, %s)
                ON CONFLICT (kbo_player_id)
                DO UPDATE SET
                    player_name = EXCLUDED.player_name,
                    team_id = EXCLUDED.team_id,
                    updated_at = NOW()
                RETURNING player_id
                """,
                (row["kbo_player_id"], row["player_name"], team_id),
            )
        else:
            cursor.execute(
                """
                INSERT INTO players (
                    player_name,
                    team_id
                )
                VALUES (%s, %s)
                ON CONFLICT (player_name, team_id)
                DO UPDATE SET
                    updated_at = NOW()
                RETURNING player_id
                """,
                (row["player_name"], team_id),
            )
        return cursor.fetchone()["player_id"]


def parse_args() -> argparse.Namespace:
    today = seoul_today()
    parser = argparse.ArgumentParser(description="KBO 타자 기록 크롤러")
    parser.add_argument("--db-name", default="kbo")
    parser.add_argument("--db-user", default=getpass.getuser())
    parser.add_argument("--db-password", default=None)
    parser.add_argument("--db-host", default=None)
    parser.add_argument("--db-port", type=int, default=5432)
    parser.add_argument("--season-year", type=int, default=today.year)
    parser.add_argument("--stat-date", type=date.fromisoformat, default=today)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--skip-db", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    scraper = KboScraper(timeout=args.timeout)
    player_basic_rows = scraper.crawl_table(PLAYER_BASIC_1_URL, PLAYER_TABLE_HEADERS)
    player_detail_rows = scraper.crawl_table(PLAYER_BASIC_2_URL, PLAYER_TABLE_HEADERS)
    team_basic_rows = scraper.crawl_table(TEAM_BASIC_1_URL, TEAM_TABLE_HEADERS)
    team_detail_rows = scraper.crawl_table(TEAM_BASIC_2_URL, TEAM_TABLE_HEADERS)

    player_rows = merge_player_rows(
        basic_rows=player_basic_rows,
        detail_rows=player_detail_rows,
        stat_date=args.stat_date,
        season_year=args.season_year,
    )
    team_rows = merge_team_rows(
        basic_rows=team_basic_rows,
        detail_rows=team_detail_rows,
        stat_date=args.stat_date,
        season_year=args.season_year,
    )

    logging.info("player rows=%s, team rows=%s", len(player_rows), len(team_rows))

    if args.skip_db:
        print(
            json.dumps(
                {
                    "stat_date": args.stat_date.isoformat(),
                    "season_year": args.season_year,
                    "player_rows": len(player_rows),
                    "team_rows": len(team_rows),
                    "sample_player": safe_json_value(player_rows[:2]),
                    "sample_team": safe_json_value(team_rows[:2]),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return

    writer = PostgresWriter(
        dbname=args.db_name,
        user=args.db_user,
        password=args.db_password,
        host=args.db_host,
        port=args.db_port,
    )

    try:
        player_job_id = writer.create_crawl_job(
            source_name="kbo_player_hitter",
            source_url=PLAYER_BASIC_1_URL,
            stat_scope="player_hitter_daily_stats",
            target_date=args.stat_date,
        )
        writer.upsert_players_and_stats(player_rows, crawl_job_id=player_job_id)
        writer.finish_crawl_job(
            player_job_id,
            status="completed",
            row_count=len(player_rows),
        )

        team_job_id = writer.create_crawl_job(
            source_name="kbo_team_hitter",
            source_url=TEAM_BASIC_1_URL,
            stat_scope="team_daily_stats",
            target_date=args.stat_date,
        )
        writer.upsert_teams_and_stats(team_rows, crawl_job_id=team_job_id)
        writer.finish_crawl_job(
            team_job_id,
            status="completed",
            row_count=len(team_rows),
        )
    except Exception as exc:
        logging.exception("crawler failed: %s", exc)
        raise
    finally:
        writer.close()


if __name__ == "__main__":
    main()
