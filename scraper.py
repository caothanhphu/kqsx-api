import argparse
import datetime as dt
import json
import os
import sys
import unicodedata
from typing import Any, Dict, List, Optional, Sequence
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from slugify import slugify
from langchain_core.exceptions import OutputParserException

from scrapegraphai.graphs import SmartScraperGraph


def ensure_ollama_host():
    """Normalize OLLAMA_HOST so local clients can reach the server."""
    default_host = "http://127.0.0.1:11434"
    raw_host = os.environ.get("OLLAMA_HOST", default_host).strip()
    host_url = raw_host or default_host

    if "://" not in host_url:
        host_url = f"http://{host_url}"

    parsed = urlparse(host_url)
    hostname = parsed.hostname or "127.0.0.1"

    if hostname in {"0.0.0.0", "::", "[::]"}:
        hostname = "127.0.0.1"

    port = parsed.port or 11434
    normalized = f"http://{hostname}:{port}"
    os.environ["OLLAMA_HOST"] = normalized
    return normalized


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/127.0.0.1 Safari/537.36"
}

PRIZE_CLASS_TO_LEVEL = {
    "ten_giai_tam": "eighth",
    "ten_giai_bay": "seventh",
    "ten_giai_sau": "sixth",
    "ten_giai_nam": "fifth",
    "ten_giai_tu": "fourth",
    "ten_giai_ba": "third",
    "ten_giai_nhi": "second",
    "ten_giai_nhat": "first",
    "ten_giai_dac_biet": "special",
}

PRIZE_LABELS = {
    "eighth": "Giai tam",
    "seventh": "Giai bay",
    "sixth": "Giai sau",
    "fifth": "Giai nam",
    "fourth": "Giai tu",
    "third": "Giai ba",
    "second": "Giai nhi",
    "first": "Giai nhat",
    "special": "Giai dac biet",
}

PRIZE_ORDER = [
    "eighth",
    "seventh",
    "sixth",
    "fifth",
    "fourth",
    "third",
    "second",
    "first",
    "special",
]

PROVINCE_OVERRIDES: Dict[str, Dict[str, Dict[str, str]]] = {
    "mn": {
        "tp_hcm": {"name": "TP. Ho Chi Minh", "operator": "XSKT TP.HCM"},
    },
    "mt": {},
    "mb": {},
}

REGION_PRIZE_ORDER: Dict[str, List[str]] = {
    "mn": [
        "eighth",
        "seventh",
        "sixth",
        "fifth",
        "fourth",
        "third",
        "second",
        "first",
        "special",
    ],
    "mt": [
        "eighth",
        "seventh",
        "sixth",
        "fifth",
        "fourth",
        "third",
        "second",
        "first",
        "special",
    ],
    "mb": [
        "special",
        "first",
        "second",
        "third",
        "fourth",
        "fifth",
        "sixth",
        "seventh",
    ],
}


def load_env_file(path: str = ".env") -> None:
    """Load simple KEY=VALUE pairs from a .env file if present."""
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as env_file:
            for raw_line in env_file:
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("export "):
                    line = line[len("export "):]
                if "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                if not key:
                    continue
                value = value.strip()
                if value and value[0] in {"'", '"'} and value[-1] == value[0]:
                    value = value[1:-1]
                os.environ.setdefault(key, value)
    except OSError:
        # Ignore unreadable .env files to keep scraper working.
        return


def get_env_value(*keys: str) -> Optional[str]:
    for key in keys:
        val = os.environ.get(key)
        if val:
            return val
    return None


def format_in_clause(values: Sequence[Any]) -> str:
    formatted: List[str] = []
    for value in values:
        if isinstance(value, str):
            if any(ch in value for ch in (",", "(", ")", " ")):
                formatted.append(f'"{value}"')
            else:
                formatted.append(value)
        else:
            formatted.append(str(value))
    return f"in.({','.join(formatted)})"


class SupabaseRestClient:
    """Minimal REST wrapper around Supabase PostgREST endpoints."""

    def __init__(self, base_url: str, api_key: str):
        base_url = base_url.rstrip("/")
        self.rest_url = f"{base_url}/rest/v1"
        self.session = requests.Session()
        self.session.headers.update(
            {
                "apikey": api_key,
                "Authorization": f"Bearer {api_key}",
            }
        )

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, str]] = None,
        payload: Optional[Any] = None,
        prefer: Optional[str] = None,
    ) -> Any:
        url = f"{self.rest_url}/{path.lstrip('/')}"
        headers: Dict[str, str] = {}
        if prefer:
            headers["Prefer"] = prefer
        if method in {"POST", "PATCH", "PUT"}:
            headers["Content-Type"] = "application/json"
        response = self.session.request(
            method,
            url,
            params=params,
            json=payload,
            headers=headers,
            timeout=30,
        )
        if not response.ok:
            raise RuntimeError(f"Supabase request failed ({response.status_code}): {response.text}")
        if response.status_code == 204 or not response.content:
            return []
        content_type = response.headers.get("Content-Type", "")
        if "application/json" in content_type:
            return response.json()
        return response.text

    def select(self, table: str, filters: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
        return self.request("GET", table, params=filters) or []

    def upsert(
        self,
        table: str,
        records: Sequence[Dict[str, Any]],
        on_conflict: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        if not records:
            return []
        params: Dict[str, str] = {}
        if on_conflict:
            params["on_conflict"] = on_conflict
        prefer = "resolution=merge-duplicates,return=representation"
        return self.request("POST", table, params=params, payload=list(records), prefer=prefer) or []

    def insert(self, table: str, records: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not records:
            return []
        prefer = "return=representation"
        return self.request("POST", table, payload=list(records), prefer=prefer) or []

    def delete(self, table: str, filters: Dict[str, str]) -> List[Dict[str, Any]]:
        prefer = "return=representation"
        return self.request("DELETE", table, params=filters, prefer=prefer) or []

def ascii_text(value: str) -> str:
    normalized = unicodedata.normalize("NFD", value)
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii")
    cleaned = " ".join(ascii_value.split())
    return cleaned or value.strip()


def extract_numbers(cell: BeautifulSoup) -> List[str]:
    numbers: List[str] = []
    for node in cell.find_all(attrs={"data": True}):
        data_value = node.get("data")
        text_value = node.get_text(strip=True)
        candidate = (data_value or text_value or "").strip()
        if candidate:
            numbers.append(candidate)
    if not numbers:
        text = cell.get_text(" ", strip=True)
        if text:
            numbers = [part.strip() for part in text.split() if part.strip()]
    return numbers


def find_region_table(soup: BeautifulSoup, region_label: str) -> Optional[BeautifulSoup]:
    """Locate the MinhChinh table matching the region label (e.g. 'Mien Trung')."""
    target = ascii_text(region_label).lower()
    for box in soup.select("div.box_kqxs"):
        title_node = box.find("div", class_="title")
        if not title_node:
            continue
        title_text = ascii_text(title_node.get_text())
        if target in title_text.lower():
            table = box.find("table")
            if table:
                return table
    return None


def parse_multi_province_table(table: BeautifulSoup, region_short: str) -> List[dict]:
    overrides = PROVINCE_OVERRIDES.get(region_short, {})
    prize_order = REGION_PRIZE_ORDER[region_short]

    province_row = None
    for tr in table.find_all("tr"):
        if tr.find("td", class_="tentinh"):
            province_row = tr
            break
    if province_row is None:
        raise ValueError("Could not locate province headers in the results table.")

    province_cells = [td for td in province_row.find_all("td") if "tentinh" in (td.get("class") or [])]
    if not province_cells:
        raise ValueError("No province columns found in the results table.")

    provinces: List[dict] = []
    for cell in province_cells:
        slug_data = cell.find("span", class_="read-result")
        slug = None
        if slug_data and slug_data.has_attr("data"):
            slug = slug_data["data"].split("|", 1)[1]
        elif cell.a and cell.a.has_attr("href"):
            href = cell.a["href"].strip("/")
            slug = href.split("/")[-1].replace("xo-so-", "")
        name_raw = cell.get_text(" ", strip=True)
        code = slug.replace("-", "_") if slug else slugify(name_raw, separator="_")

        province_overrides = overrides.get(code, {})
        province_name = province_overrides.get("name", ascii_text(name_raw))

        provinces.append(
            {
                "code": code,
                "name": province_name,
                "operator": province_overrides.get("operator"),
                "game_code": province_overrides.get("game_code"),
                "game_name": province_overrides.get("game_name"),
                "results": [],
            }
        )

    body_rows = table.select("tbody tr")
    for row in body_rows:
        cells = row.find_all("td")
        if len(cells) < len(provinces) + 1:
            continue
        class_names = cells[0].get("class") or []
        prize_level = next((PRIZE_CLASS_TO_LEVEL.get(cls) for cls in class_names if cls in PRIZE_CLASS_TO_LEVEL), None)
        if not prize_level:
            continue
        if prize_level not in prize_order:
            continue

        prize_name = PRIZE_LABELS[prize_level]
        for idx, cell in enumerate(cells[1 : len(provinces) + 1]):
            numbers = extract_numbers(cell)
            provinces[idx]["results"].append(
                {
                    "prize_level": prize_level,
                    "prize_order": 1,
                    "prize_name": prize_name,
                    "numbers": numbers,
                }
            )

    for province in provinces:
        province["results"].sort(key=lambda item: prize_order.index(item["prize_level"]))
        levels_present = {item["prize_level"] for item in province["results"]}
        missing_levels = [level for level in prize_order if level not in levels_present]
        if missing_levels:
            raise ValueError(
                f"Province {province['code']} missing prize rows: {', '.join(missing_levels)}"
            )

    return provinces


def scrape_region(date_str: str, source_url: str, region_short: str) -> List[dict]:
    resp = requests.get(source_url, headers=REQUEST_HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    region_label = vn_region_label(region_short)
    table = find_region_table(soup, region_label)
    if table is None:
        raise ValueError(f"Could not locate results table for region {region_label}.")

    return parse_multi_province_table(table, region_short)


def scrape_mien_bac(date_str: str, source_url: str) -> List[dict]:
    resp = requests.get(source_url, headers=REQUEST_HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    table = find_region_table(soup, "Mien Bac")
    if table is None:
        raise ValueError("Could not locate northern results table on the page.")

    province_cell = table.find("td", class_="tentinh")
    if province_cell is None:
        raise ValueError("Could not determine province information for northern results.")

    slug_data = province_cell.find("span", class_="read-result")
    slug = None
    if slug_data and slug_data.has_attr("data"):
        slug = slug_data["data"].split("|", 1)[1]
    else:
        link = province_cell.find("a")
        if link and link.has_attr("href"):
            href = link["href"].strip("/")
            slug = href.split("/")[-1].replace("xo-so-", "")

    name_raw = province_cell.get_text(" ", strip=True)
    code = slug.replace("-", "_") if slug else slugify(name_raw, separator="_")

    overrides = PROVINCE_OVERRIDES.get("mb", {}).get(code, {})
    province_name = overrides.get("name", ascii_text(name_raw))

    province: Dict[str, Any] = {
        "code": code,
        "name": province_name,
        "operator": overrides.get("operator"),
        "game_code": overrides.get("game_code"),
        "game_name": overrides.get("game_name"),
        "results": [],
    }

    prize_order = REGION_PRIZE_ORDER["mb"]

    for row in table.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 2:
            continue
        label_cell, value_cell = cells[0], cells[1]
        class_names = label_cell.get("class") or []
        prize_level = next((PRIZE_CLASS_TO_LEVEL.get(cls) for cls in class_names if cls in PRIZE_CLASS_TO_LEVEL), None)
        if not prize_level:
            continue
        if prize_level not in prize_order:
            continue

        numbers = extract_numbers(value_cell)
        if not numbers:
            continue

        province["results"].append(
            {
                "prize_level": prize_level,
                "prize_order": 1,
                "prize_name": PRIZE_LABELS[prize_level],
                "numbers": numbers,
            }
        )

    province["results"].sort(key=lambda item: prize_order.index(item["prize_level"]))
    levels_present = {item["prize_level"] for item in province["results"]}
    missing_levels = [level for level in prize_order if level not in levels_present]
    if missing_levels:
        raise ValueError(
            f"Northern results missing prize rows: {', '.join(missing_levels)}"
        )

    return [province]

# -------- Graph Configuration --------
def build_graph():
    """
    SmartScraperGraph runs a local Llama3 via Ollama.
    Output: JSON following the schema described in the prompt.
    """
    graph_config = {
        "llm": {
            "model": "ollama/llama3",
            "temperature": 0.0,
            "format": "json",
        },
        "embeddings": {
            "model": "ollama/nomic-embed-text"
        },
        "verbose": False,
        "headless": True,    # avoid opening a visible browser
        "max_tokens": 4096,
    }
    return graph_config

# -------- Prompt trích xuất KQSX từ MinhChinh --------
PROMPT_TEMPLATE = """
You are a precise web data extractor for Vietnamese lottery results.
From the given MinhChinh daily results page, extract ALL results for the requested REGION ONLY.
Return a strict JSON array (no commentary) where each item has:
- code: machine slug of province, e.g. "ben_tre"
- name: province display name, e.g. "Ben Tre"
- operator: "XSKT <Province>"
- game_code: "xs_{region}_{code}"
- game_name: "XS {Region VN Label} - {Province Name}"
- source_url: the url you scraped
- draw_date: ISO date YYYY-MM-DD
- sequence: 1
- results: array of objects with fields:
  prize_level in ["eighth","seventh","sixth","fifth","fourth","third","second","first","special"]
  prize_order: 1
  prize_name: Vietnamese label e.g. "Giai tam","Giai dac biet"
  numbers: array of strings (the winning numbers for that prize row, preserve leading zeros)

REGION mapping:
- "mb" -> Miền Bắc (single board)
- "mt" -> Miền Trung (multiple boards)
- "mn" -> Miền Nam (multiple boards)

Rules:
- Only include provinces/boards that belong to the requested region for that date.
- Keep number strings EXACTLY as displayed (preserve leading zeros).
- If multiple numbers appear on a row, put them all in the "numbers" array for that prize.
- Prize labels must follow the region's convention (Miền Nam/Trung: eighth..special; Miền Bắc: seven..special mapping accordingly).

Return ONLY valid JSON.
"""

# -------- Map region info cho SQL (schedule/metadata) --------
REGION_META = {
    "mb": {"region_code": "mien_bac", "region_name": "Mien Bac",
           "draw_days": ["daily"], "draw_time": "18:15"},
    "mt": {"region_code": "mien_trung", "region_name": "Mien Trung",
           "draw_days": ["daily"], "draw_time": "17:15"},
    "mn": {"region_code": "mien_nam", "region_name": "Mien Nam",
           "draw_days": ["daily"], "draw_time": "16:15"},
}

def vn_region_label(short):
    return {"mb": "Mien Bac", "mt": "Mien Trung", "mn": "Mien Nam"}[short]

def build_source_url(date_str):
    # MinhChinh format: DD-MM-YYYY
    d = dt.datetime.fromisoformat(date_str)
    return f"https://www.minhchinh.com/ket-qua-xo-so/{d.strftime('%d-%m-%Y')}.html"


def build_canonical_source_url(date_str: str) -> str:
    d = dt.datetime.fromisoformat(date_str)
    return f"https://kqxs.pmsa.com.vn/kqxs/{d.strftime('%Y-%m-%d')}"


def normalize_items(items, region_short, date_str, source_url):
    """Điền đủ các field game_code/name, operator, slug code..."""
    region_label = vn_region_label(region_short)
    out = []
    for it in items:
        code = it.get("code") or slugify(it["name"], separator="_")
        out.append({
            "code": code,
            "name": it["name"],
            "operator": it.get("operator") or f"XSKT {it['name']}",
            "game_code": it.get("game_code") or f"xs_{region_short}_{code}",
            "game_name": it.get("game_name") or f"XS {region_label} - {it['name']}",
            "source_url": source_url,
            "draw_date": date_str,
            "sequence": 1,
            "results": it["results"],
        })
    return out

def build_sql(data_array, region_short, date_str):
    """
    Tạo SQL migration theo đúng pattern mẫu:
    - upsert region
    - upsert provinces
    - upsert lottery_games
    - xóa draw trùng ngày/sequence
    - insert draw + prizes + results
    """
    meta = REGION_META[region_short]
    region_code = meta["region_code"]
    region_name = meta["region_name"]
    draw_days = meta["draw_days"]
    draw_time = meta["draw_time"]

    # JSON embed vào CTE `source`:
    import json
    json_blob = json.dumps(data_array, ensure_ascii=False)

    sql = f"""begin;

-- Cleanup existing draws/results for the generated date.
with target_draws as (
  select id
  from draws
  where draw_date = '{date_str}'
),
target_prizes as (
  select dp.id
  from draw_prizes dp
  join target_draws td on td.id = dp.draw_id
)
delete from draw_results
where prize_id in (select id from target_prizes);

with target_draws as (
  select id
  from draws
  where draw_date = '{date_str}'
)
delete from draw_prizes
where draw_id in (select id from target_draws);

delete from draws
where draw_date = '{date_str}';

with source as (
  select jsonb_array_elements(
    $${
      json_blob
    }$$::jsonb
  ) as data
),
region_upsert as (
  insert into regions (code, name)
  values ('{region_code}', '{region_name}')
  on conflict (code) do update set name = excluded.name
  returning id
),
province_source as (
  select
    data->>'code' as code,
    data->>'name' as name,
    data->>'operator' as operator,
    data->>'game_code' as game_code,
    data->>'game_name' as game_name,
    data->>'source_url' as source_url,
    (data->>'draw_date')::date as draw_date,
    coalesce((data->>'sequence')::int, 1) as sequence,
    data->'results' as results
  from source
),
province_upsert as (
  insert into provinces (code, name, region_id)
  select
    ps.code,
    ps.name,
    (select id from region_upsert)
  from province_source ps
  on conflict (code) do update
    set name = excluded.name,
        region_id = excluded.region_id
  returning code, id
),
game_upsert as (
  insert into lottery_games (
    code,
    name,
    category,
    operator,
    region_id,
    province_id,
    numbers_per_ticket,
    number_pool,
    has_bonus,
    schedule,
    metadata
  )
  select
    ps.game_code,
    ps.game_name,
    'regional',
    ps.operator,
    (select id from region_upsert),
    pu.id,
    6,
    10,
    false,
    jsonb_build_object(
      'draw_days', array[{",".join([f"'{d}'" for d in draw_days])}],
      'draw_time', '{draw_time}',
      'timezone', 'Asia/Ho_Chi_Minh'
    ),
    jsonb_build_object(
      'province_code', ps.code,
      'notes', 'Inserted from migration for {date_str} results'
    )
  from province_source ps
  join province_upsert pu on pu.code = ps.code
  on conflict (code) do update
    set name = excluded.name,
        operator = excluded.operator,
        region_id = excluded.region_id,
        province_id = excluded.province_id,
        schedule = excluded.schedule,
        metadata = excluded.metadata
  returning code, id
),
deleted_draws as (
  delete from draws d
  using game_upsert gu, province_source ps
  where d.game_id = gu.id
    and gu.code = ps.game_code
    and d.draw_date = ps.draw_date
    and coalesce(d.sequence, 1) = ps.sequence
  returning d.id
),
draw_insert as (
  insert into draws (
    game_id,
    draw_date,
    sequence,
    status,
    source_url,
    raw_feed
  )
  select
    gu.id,
    ps.draw_date,
    ps.sequence,
    'completed',
    ps.source_url,
    jsonb_build_object(
      'import_source', 'scrapegraphai',
      'imported_via', 'automation',
      'created_at', now(),
      'province_code', ps.code,
      'draw_date', ps.draw_date
    )
  from province_source ps
  join game_upsert gu on gu.code = ps.game_code
  returning id, game_id
),
draw_map as (
  select
    di.id as draw_id,
    gu.code as game_code
  from draw_insert di
  join game_upsert gu on gu.id = di.game_id
),
prize_source as (
  select
    ps.code as province_code,
    pu.id as province_id,
    dm.draw_id,
    jsonb_array_elements(ps.results) as prize_data
  from province_source ps
  join province_upsert pu on pu.code = ps.code
  join draw_map dm on dm.game_code = ps.game_code
),
prize_prepared as (
  select
    province_code,
    province_id,
    draw_id,
    (prize_data->>'prize_level')::prize_level as prize_level,
    coalesce((prize_data->>'prize_order')::smallint, 1) as prize_order,
    prize_data->>'prize_name' as prize_name,
    coalesce((prize_data->>'reward_amount')::numeric, 0) as reward_amount,
    coalesce(prize_data->>'reward_currency', 'VND') as reward_currency,
    array(select jsonb_array_elements_text(prize_data->'numbers')) as numbers
  from prize_source
),
prize_insert as (
  insert into draw_prizes (
    draw_id,
    prize_level,
    prize_order,
    prize_name,
    reward_amount,
    reward_currency
  )
  select
    draw_id,
    prize_level,
    prize_order,
    prize_name,
    reward_amount,
    reward_currency
  from prize_prepared
  returning id, draw_id, prize_level, prize_order
)
insert into draw_results (
  prize_id,
  result_numbers,
  province_id
)
select
  pi.id,
  pp.numbers,
  pp.province_id
from prize_prepared pp
join prize_insert pi
  on pi.draw_id = pp.draw_id
 and pi.prize_level = pp.prize_level
 and pi.prize_order = pp.prize_order;

commit;
"""
    return sql

def publish_to_supabase(data_array: List[dict], region_short: str, date_str: str, source_url: str) -> Dict[str, int]:
    load_env_file()
    supabase_url = get_env_value("SUPABASE_URL", "VITE_SUPABASE_URL")
    supabase_key = get_env_value(
        "SUPABASE_SERVICE_ROLE_KEY",
        "VITE_SUPABASE_SERVICE_ROLE_KEY",
        "SUPABASE_ANON_KEY",
        "VITE_SUPABASE_PUBLISHABLE_KEY",
    )
    if not supabase_url or not supabase_key:
        raise RuntimeError(
            "Missing Supabase configuration. Ensure SUPABASE_URL/VITE_SUPABASE_URL and Supabase key variables are set."
        )

    client = SupabaseRestClient(supabase_url, supabase_key)
    meta = REGION_META[region_short]
    region_code = meta["region_code"]
    region_name = meta["region_name"]
    draw_days = meta["draw_days"]
    draw_time = meta["draw_time"]

    client.upsert("regions", [{"code": region_code, "name": region_name}], on_conflict="code")
    region_rows = client.select("regions", {"code": f"eq.{region_code}"})
    if not region_rows:
        raise RuntimeError("Failed to fetch region after upsert.")
    region_id = region_rows[0]["id"]

    province_codes = [item["code"] for item in data_array]
    province_payloads = [
        {
            "code": item["code"],
            "name": item["name"],
            "region_id": region_id,
        }
        for item in data_array
    ]
    province_map: Dict[str, int] = {}
    if province_payloads:
        client.upsert("provinces", province_payloads, on_conflict="code")
        province_filter = {"code": format_in_clause(province_codes)}
        province_rows = client.select("provinces", province_filter)
        province_map = {row["code"]: row["id"] for row in province_rows}

    missing_provinces = [code for code in province_codes if code not in province_map]
    if missing_provinces:
        raise RuntimeError(f"Supabase is missing provinces: {', '.join(missing_provinces)}")

    game_payloads: List[Dict[str, Any]] = []
    game_codes: List[str] = []
    for item in data_array:
        province_code = item["code"]
        operator = item.get("operator") or f"XSKT {item['name']}"
        game_payloads.append(
            {
                "code": item["game_code"],
                "name": item["game_name"],
                "category": "regional",
                "operator": operator,
                "region_id": region_id,
                "province_id": province_map[province_code],
                "numbers_per_ticket": 6,
                "number_pool": 10,
                "has_bonus": False,
                "schedule": {
                    "draw_days": draw_days,
                    "draw_time": draw_time,
                    "timezone": "Asia/Ho_Chi_Minh",
                },
                "metadata": {
                    "province_code": province_code,
                    "notes": f"Inserted via scraper on {date_str}",
                },
            }
        )
        game_codes.append(item["game_code"])

    game_map: Dict[str, int] = {}
    if game_payloads:
        client.upsert("lottery_games", game_payloads, on_conflict="code")
        game_rows = client.select("lottery_games", {"code": format_in_clause(game_codes)})
        game_map = {row["code"]: row["id"] for row in game_rows}

    missing_games = [code for code in game_codes if code not in game_map]
    if missing_games:
        raise RuntimeError(f"Supabase is missing lottery games: {', '.join(missing_games)}")

    game_ids = list(game_map.values())
    existing_draws: List[Dict[str, Any]] = []
    if game_ids:
        draw_filters = {
            "draw_date": f"eq.{date_str}",
            "game_id": format_in_clause(game_ids),
        }
        existing_draws = client.select("draws", draw_filters)
    existing_draw_ids = [row["id"] for row in existing_draws]
    if existing_draw_ids:
        prize_rows = client.select("draw_prizes", {"draw_id": format_in_clause(existing_draw_ids)})
        prize_ids = [row["id"] for row in prize_rows]
        if prize_ids:
            client.delete("draw_results", {"prize_id": format_in_clause(prize_ids)})
        client.delete("draw_prizes", {"draw_id": format_in_clause(existing_draw_ids)})
        client.delete("draws", {"id": format_in_clause(existing_draw_ids)})

    draw_records: List[Dict[str, Any]] = []
    for item in data_array:
        game_id = game_map[item["game_code"]]
        sequence = int(item.get("sequence") or 1)
        draw_records.append(
            {
                "game_id": game_id,
                "draw_date": date_str,
                "sequence": sequence,
                "status": "completed",
                "source_url": source_url,
                "raw_feed": {
                    "import_source": "scrapegraphai",
                    "imported_via": "automation",
                    "created_at": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
                    "province_code": item["code"],
                    "draw_date": date_str,
                },
            }
        )

    inserted_draws = client.insert("draws", draw_records)
    if not inserted_draws:
        raise RuntimeError("Failed to insert draws into Supabase.")

    new_draws = client.select(
        "draws",
        {
            "draw_date": f"eq.{date_str}",
            "game_id": format_in_clause(game_ids),
        },
    )
    if not new_draws:
        raise RuntimeError("No draw records found after insertion.")

    game_id_to_code = {gid: code for code, gid in game_map.items()}
    draw_lookup: Dict[str, Dict[int, int]] = {}
    draw_ids: List[int] = []
    for row in new_draws:
        sequence = int(row.get("sequence") or 1)
        draw_ids.append(row["id"])
        game_code = game_id_to_code.get(row["game_id"])
        if not game_code:
            continue
        draw_lookup.setdefault(game_code, {})[sequence] = row["id"]

    prize_records: List[Dict[str, Any]] = []
    for item in data_array:
        game_code = item["game_code"]
        sequence = int(item.get("sequence") or 1)
        draw_id = draw_lookup.get(game_code, {}).get(sequence)
        if not draw_id:
            raise RuntimeError(f"Missing draw record for game {game_code} sequence {sequence}")
        for result in item["results"]:
            prize_records.append(
                {
                    "draw_id": draw_id,
                    "prize_level": result["prize_level"],
                    "prize_order": int(result.get("prize_order") or 1),
                    "prize_name": result["prize_name"],
                    "reward_amount": result.get("reward_amount") or 0,
                    "reward_currency": result.get("reward_currency") or "VND",
                }
            )

    inserted_prizes = client.insert("draw_prizes", prize_records)
    if not inserted_prizes:
        raise RuntimeError("Failed to insert draw prizes into Supabase.")

    prize_lookup: Dict[int, Dict[tuple, int]] = {}
    if draw_ids:
        new_prizes = client.select("draw_prizes", {"draw_id": format_in_clause(draw_ids)})
        for row in new_prizes:
            key = (row["prize_level"], int(row.get("prize_order") or 1))
            prize_lookup.setdefault(row["draw_id"], {})[key] = row["id"]

    result_records: List[Dict[str, Any]] = []
    for item in data_array:
        province_id = province_map[item["code"]]
        game_code = item["game_code"]
        sequence = int(item.get("sequence") or 1)
        draw_id = draw_lookup[game_code][sequence]
        for result in item["results"]:
            key = (result["prize_level"], int(result.get("prize_order") or 1))
            prize_id = prize_lookup.get(draw_id, {}).get(key)
            if not prize_id:
                raise RuntimeError(
                    f"Missing prize entry for draw {draw_id} - level {result['prize_level']} order {key[1]}"
                )
            numbers = result.get("numbers") or []
            result_records.append(
                {
                    "prize_id": prize_id,
                    "result_numbers": numbers,
                    "province_id": province_id,
                }
            )

    if result_records:
        client.insert("draw_results", result_records)

    return {
        "draws": len(draw_records),
        "prizes": len(prize_records),
        "results": len(result_records),
    }


def run(date_str: str, region_short: str, out_path: Optional[str] = None, use_supabase: bool = True):
    assert region_short in ("mb", "mt", "mn"), "region must be one of: mb, mt, mn"
    scrape_url = build_source_url(date_str)
    canonical_source_url = build_canonical_source_url(date_str)

    if region_short in {"mn", "mt"}:
        raw_items = scrape_region(date_str, scrape_url, region_short)
    elif region_short == "mb":
        raw_items = scrape_mien_bac(date_str, scrape_url)
    else:
        ensure_ollama_host()
        region_label = vn_region_label(region_short)

        prompt = PROMPT_TEMPLATE + f"""

Requested region: {region_short} ({region_label})
Page URL: {scrape_url}
Target date (draw_date): {date_str}
"""

        base_config = build_graph()
        graph = SmartScraperGraph(
            prompt=prompt,
            source=scrape_url,
            config=base_config
        )
        try:
            raw = graph.run()
        except OutputParserException:
            print("Initial scrape response was not valid JSON. Retrying with stricter instructions...")
            retry_prompt = prompt + "\n\nReturn ONLY a valid JSON array. Do not include commentary or explanations."
            graph = SmartScraperGraph(
                prompt=retry_prompt,
                source=scrape_url,
                config=base_config
            )
            raw = graph.run()

        parsed_raw = raw
        if isinstance(raw, str):
            try:
                parsed_raw = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise ValueError("Expected JSON string from SmartScraperGraph.") from exc

        if isinstance(parsed_raw, dict):
            parsed_raw = (
                parsed_raw.get("result")
                or parsed_raw.get("content")
                or parsed_raw.get("data")
                or parsed_raw
            )

        if isinstance(parsed_raw, str):
            try:
                parsed_raw = json.loads(parsed_raw)
            except json.JSONDecodeError as exc:
                raise ValueError("Graph returned nested string that is not valid JSON.") from exc

        if not isinstance(parsed_raw, list):
            raise ValueError(f"Expected list of province entries, received {type(parsed_raw).__name__}")

        raw_items = parsed_raw

    data_array = normalize_items(raw_items, region_short, date_str, canonical_source_url)

    supabase_stats: Optional[Dict[str, int]] = None
    if use_supabase:
        supabase_stats = publish_to_supabase(data_array, region_short, date_str, canonical_source_url)
        print(
            f"Supabase upload complete: {supabase_stats['draws']} draws, "
            f"{supabase_stats['prizes']} prizes, {supabase_stats['results']} results."
        )

    if out_path:
        sql = build_sql(data_array, region_short, date_str)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(sql)
        print(f"Wrote SQL export to: {out_path}")
    elif not use_supabase:
        print("Nothing to do: Supabase upload disabled and no output path provided.")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--region", dest="region", required=True, choices=["mb","mt","mn"])
    ap.add_argument("--out", help="Optional path to write SQL export")
    ap.add_argument("--no-supabase", action="store_true", help="Skip Supabase upload and only generate SQL")
    args = ap.parse_args()
    if args.no_supabase and not args.out:
        ap.error("--out is required when --no-supabase is set.")
    run(args.date, args.region, args.out, use_supabase=not args.no_supabase)
