import datetime as dt
import logging
import os
import random
from functools import lru_cache
from typing import Annotated, Any, Dict, Iterable, List, Optional, Set, Tuple
from threading import Event, RLock, Thread

import requests
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from cachetools import TTLCache


def load_env_file(path: str = ".env") -> None:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as env_file:
        for raw_line in env_file:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export ") :]
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


load_env_file()


def normalize_env_quotes() -> None:
    for key, value in list(os.environ.items()):
        if not value:
            continue
        stripped = value.strip()
        if len(stripped) >= 2 and stripped[0] == stripped[-1] and stripped[0] in {"'", '"'}:
            os.environ[key] = stripped[1:-1]


normalize_env_quotes()


logger = logging.getLogger("kqsx_api")


class PrizeSummary(BaseModel):
    label: str
    level: str
    numbers: List[str]


class DrawSummary(BaseModel):
    region: str
    region_label: str
    province_code: Optional[str] = None
    province_name: str
    operator: Optional[str] = None
    game_code: Optional[str] = None
    game_name: Optional[str] = None
    sequence: int
    prizes: List[PrizeSummary]
    source_url: Optional[str] = None


class LotterySummaryResponse(BaseModel):
    requested_date: str
    date: str
    region: str
    region_label: str
    draws: List[DrawSummary]
    summary_text: str
    fallback_offset_days: int


class PrivacyPolicyResponse(BaseModel):
    title: str
    description: str
    data_usage: str
    limitations: str
    contact: str
    last_updated: str


class RandomNumberResponse(BaseModel):
    min_value: int
    max_value: int
    count: int
    unique: bool
    numbers: List[int]


PRIZE_DISPLAY_LABELS: Dict[str, str] = {
    "eighth": "Giải 8",
    "seventh": "Giải 7",
    "sixth": "Giải 6",
    "fifth": "Giải 5",
    "fourth": "Giải 4",
    "third": "Giải 3",
    "second": "Giải 2",
    "first": "Giải 1",
    "special": "Giải Đặc Biệt",
    "consolation": "Giải Khuyến Khích",
    "jackpot": "Giải Jackpot",
    "other": "Giải Khác",
}

REGION_CONFIG: Dict[str, Dict[str, Any]] = {
    "mn": {
        "label": "Miền Nam",
        "code_prefix": "xs_mn_",
        "prize_order": [
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
    },
    "mt": {
        "label": "Miền Trung",
        "code_prefix": "xs_mt_",
        "prize_order": [
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
    },
    "mb": {
        "label": "Miền Bắc",
        "code_prefix": "xs_mb_",
        "prize_order": [
            "special",
            "first",
            "second",
            "third",
            "fourth",
            "fifth",
            "sixth",
            "seventh",
        ],
    },
}


DEFAULT_FALLBACK_DAYS = 2
HOURLY_CHECK_INTERVAL_SECONDS = 60 * 60
REGION_CHECK_SEQUENCE = ["mb", "mt", "mn"]
REGION_ORDER = {region_key: idx for idx, region_key in enumerate(REGION_CONFIG.keys())}

CACHE_TTL_SECONDS = 60 * 60 * 24 * 10  # 10 days
_draw_cache: TTLCache = TTLCache(maxsize=512, ttl=CACHE_TTL_SECONDS)
_cache_lock = RLock()
_hourly_stop_event: Event = Event()
_hourly_thread: Optional[Thread] = None


def cache_key(date_value: dt.date, region: str) -> str:
    return f"{region}:{date_value.isoformat()}"


def get_cached_draws(date_value: dt.date, region: str) -> Optional[List[Dict[str, Any]]]:
    key = cache_key(date_value, region)
    with _cache_lock:
        try:
            return _draw_cache[key]
        except KeyError:
            return None


def set_cached_draws(date_value: dt.date, region: str, draws: List[Dict[str, Any]]) -> None:
    key = cache_key(date_value, region)
    with _cache_lock:
        _draw_cache[key] = draws


def invalidate_draw_cache(date_value: dt.date, region: str) -> None:
    key = cache_key(date_value, region)
    with _cache_lock:
        _draw_cache.pop(key, None)


@lru_cache()
def supabase_config() -> Dict[str, Any]:
    base_url = os.environ.get("VITE_SUPABASE_URL")
    api_key = os.environ.get("VITE_SUPABASE_SERVICE_ROLE_KEY") or os.environ.get(
        "VITE_SUPABASE_PUBLISHABLE_KEY"
    )
    if not base_url or not api_key:
        raise RuntimeError("Supabase configuration is missing environment variables.")
    base_url = base_url.rstrip("/")
    headers = {
        "apikey": api_key,
        "Authorization": f"Bearer {api_key}",
    }
    return {"rest_url": f"{base_url}/rest/v1", "headers": headers}


def supabase_get(resource: str, params: Dict[str, str]) -> Any:
    cfg = supabase_config()
    url = f"{cfg['rest_url']}/{resource}"
    response = requests.get(url, headers=cfg["headers"], params=params, timeout=20)
    if response.status_code != 200:
        raise HTTPException(
            status_code=response.status_code,
            detail=f"Supabase request failed for {resource}: {response.text}",
        )
    return response.json()


def parse_date(date_str: Optional[str]) -> dt.date:
    if not date_str:
        return dt.date.today()
    try:
        return dt.date.fromisoformat(date_str)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="Invalid date format, expected YYYY-MM-DD.") from exc


def fetch_draws_for_date(target_date: dt.date, region: str) -> List[Dict[str, Any]]:
    region_info = REGION_CONFIG[region]

    cached = get_cached_draws(target_date, region)
    if cached is not None:
        return cached

    params = {
        "draw_date": f"eq.{target_date.isoformat()}",
        "select": (
            "id,draw_date,sequence,source_url,"
            "lottery_games(code,name,operator,metadata,province_id),"
            "draw_prizes(id,prize_level,prize_order,prize_name,"
            "draw_results(result_numbers,bonus_numbers,province_id))"
        ),
        "order": "sequence.asc",
    }
    data = supabase_get("draws", params)
    prefix = region_info["code_prefix"]
    filtered = []
    for item in data:
        game = item.get("lottery_games") or {}
        game_code = (game.get("code") or "").lower()
        if prefix and game_code and not game_code.startswith(prefix):
            continue
        filtered.append(item)

    set_cached_draws(target_date, region, filtered)
    return filtered


def trigger_scrape_for_region(target_date: dt.date, region: str) -> bool:
    try:
        from scraper import run as scraper_run
    except Exception as exc:  # ImportError or circular issues
        logger.exception("Unable to import scraper module: %s", exc)
        return False

    try:
        logger.info(
            "Triggering scraper for region=%s date=%s",
            region,
            target_date.isoformat(),
        )
        invalidate_draw_cache(target_date, region)
        scraper_run(target_date.isoformat(), region, out_path=None, use_supabase=True)
        logger.info(
            "Scraper completed for region=%s date=%s",
            region,
            target_date.isoformat(),
        )
        invalidate_draw_cache(target_date, region)
        return True
    except Exception as exc:
        logger.exception(
            "Scraper run failed for region=%s date=%s: %s",
            region,
            target_date.isoformat(),
            exc,
        )
        return False


def ensure_today_draws_available(now: Optional[dt.datetime] = None) -> None:
    """Check current-day draws per region and trigger scraper when missing."""
    target_date = (now or dt.datetime.now()).date()
    for region in REGION_CHECK_SEQUENCE:
        try:
            draws = fetch_draws_for_date(target_date, region)
        except Exception:  # noqa: BLE001
            logger.exception(
                "Background check failed to fetch draws for region=%s date=%s",
                region,
                target_date.isoformat(),
            )
            continue

        if draws:
            logger.debug(
                "Background check: data already present for region=%s date=%s",
                region,
                target_date.isoformat(),
            )
            continue

        logger.info(
            "Background check: missing draws for region=%s date=%s, triggering scraper.",
            region,
            target_date.isoformat(),
        )
        success = trigger_scrape_for_region(target_date, region)
        if not success:
            logger.warning(
                "Background scraper attempt failed for region=%s date=%s",
                region,
                target_date.isoformat(),
            )


def _hourly_scrape_worker(stop_event: Event) -> None:
    """Loop that ensures current-day data is refreshed roughly once per hour."""
    logger.info("Starting hourly scraper watchdog thread.")
    while not stop_event.is_set():
        cycle_started = dt.datetime.now()
        try:
            ensure_today_draws_available(now=cycle_started)
        except Exception:  # noqa: BLE001
            logger.exception("Unexpected error during hourly scraper check.")

        elapsed = (dt.datetime.now() - cycle_started).total_seconds()
        remaining = HOURLY_CHECK_INTERVAL_SECONDS - elapsed
        if remaining <= 0:
            remaining = HOURLY_CHECK_INTERVAL_SECONDS
        if stop_event.wait(remaining):
            break
    logger.info("Hourly scraper watchdog thread stopped.")


def gather_draws_for_regions(
    requested_date: dt.date,
    regions: List[str],
    fallback_limit: int = DEFAULT_FALLBACK_DAYS,
) -> Tuple[dt.date, int, Dict[str, List[Dict[str, Any]]]]:
    attempted_scrape: Set[str] = set()
    offset = 0
    while offset <= fallback_limit:
        candidate_date = requested_date - dt.timedelta(days=offset)
        region_draws: Dict[str, List[Dict[str, Any]]] = {}
        missing_regions: List[str] = []

        for region in regions:
            draws = fetch_draws_for_date(candidate_date, region)
            if draws:
                region_draws[region] = draws
            else:
                missing_regions.append(region)

        if region_draws:
            return candidate_date, offset, region_draws

        if offset == 0 and missing_regions:
            attempted_any = False
            for region in missing_regions:
                if region in attempted_scrape:
                    continue
                attempted_scrape.add(region)
                attempted_any = True
                trigger_scrape_for_region(candidate_date, region)
            if attempted_any:
                # Re-evaluate the same date after scraping attempts.
                continue

        offset += 1
    raise HTTPException(status_code=404, detail="Không tìm thấy dữ liệu xổ số cho yêu cầu.")


def format_game_code(game_code: Optional[str]) -> Optional[str]:
    if not game_code:
        return None
    if game_code.startswith("xs_"):
        game_code = game_code[3:]
    return game_code.upper().replace("_", " ")


def collect_province_map(draws: Iterable[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    province_ids: List[int] = []
    for item in draws:
        game = item.get("lottery_games") or {}
        province_id = game.get("province_id")
        if isinstance(province_id, int):
            province_ids.append(province_id)
    unique_ids = sorted({pid for pid in province_ids})
    if not unique_ids:
        return {}
    id_list = ",".join(str(pid) for pid in unique_ids)
    provinces = supabase_get("provinces", {"id": f"in.({id_list})", "select": "id,code,name"})
    return {prov["id"]: prov for prov in provinces}


def build_draw_summaries(draws: List[Dict[str, Any]], region: str) -> List[DrawSummary]:
    province_lookup = collect_province_map(draws)
    region_info = REGION_CONFIG[region]
    prize_order = region_info["prize_order"]
    summaries: List[DrawSummary] = []

    for item in draws:
        game = item.get("lottery_games") or {}
        province_id = game.get("province_id")
        province_meta = province_lookup.get(province_id, {})
        metadata = game.get("metadata") or {}
        province_code = province_meta.get("code") or metadata.get("province_code")
        province_name = province_meta.get("name") or game.get("name") or "Không rõ"

        raw_prizes = item.get("draw_prizes") or []
        prizes: List[PrizeSummary] = []

        for level in prize_order:
            level_entries = [
                entry for entry in raw_prizes if entry.get("prize_level") == level
            ]
            if not level_entries:
                continue
            level_entries.sort(key=lambda entry: entry.get("prize_order") or 0)
            for entry in level_entries:
                numbers: List[str] = []
                for result in entry.get("draw_results") or []:
                    numbers.extend(result.get("result_numbers") or [])
                prize_label = PRIZE_DISPLAY_LABELS.get(level, entry.get("prize_name") or level.title())
                prizes.append(
                    PrizeSummary(
                        label=prize_label,
                        level=level,
                        numbers=numbers,
                    )
                )

        summaries.append(
            DrawSummary(
                region=region,
                region_label=region_info["label"],
                province_code=province_code,
                province_name=province_name,
                operator=game.get("operator"),
                game_code=game.get("code"),
                game_name=game.get("name"),
                sequence=item.get("sequence") or 1,
                prizes=prizes,
                source_url=item.get("source_url"),
            )
        )

    summaries.sort(key=lambda draw: (draw.sequence, draw.province_name))
    return summaries


def render_summary_text(
    draw_date: dt.date,
    requested_region: Optional[str],
    draws: List[DrawSummary],
) -> str:
    date_label = draw_date.strftime("%d/%m/%Y")
    if requested_region:
        title_label = REGION_CONFIG[requested_region]["label"]
    else:
        title_label = "3 Miền"

    if not draws:
        return f"🎯 Chưa có dữ liệu kết quả xổ số {title_label} cho ngày {date_label}."

    lines: List[str] = [f"🎯 Kết quả Xổ Số {title_label} – {date_label}"]
    multiple_regions = len({draw.region for draw in draws}) > 1
    draws_sorted = sorted(
        draws,
        key=lambda draw: (
            REGION_ORDER.get(draw.region, 99),
            draw.sequence,
            draw.province_name,
        ),
    )

    for draw in draws_sorted:
        lines.append("")
        details: List[str] = []
        formatted_code = format_game_code(draw.game_code)
        if formatted_code:
            details.append(formatted_code)
        if draw.operator:
            details.append(draw.operator)
        header = draw.province_name
        if multiple_regions:
            header = f"{draw.region_label} – {header}"
        if details:
            header = f"{header} ({' – '.join(details)})"
        lines.append(header)

        for prize in draw.prizes:
            numbers_text = " – ".join(prize.numbers) if prize.numbers else "Đang cập nhật"
            lines.append(f"{prize.label}: {numbers_text}")

    return "\n".join(lines)


app = FastAPI(title="KQSX API", version="0.1.0")


@app.on_event("startup")
def start_hourly_watchdog() -> None:
    """Start the background thread that keeps daily data fresh."""
    global _hourly_thread  # noqa: PLW0603
    if _hourly_thread and _hourly_thread.is_alive():
        return

    _hourly_stop_event.clear()
    try:
        ensure_today_draws_available()
    except Exception:  # noqa: BLE001
        logger.exception("Initial hourly scraper check failed during startup.")

    thread = Thread(
        target=_hourly_scrape_worker,
        args=(_hourly_stop_event,),
        name="hourly-scraper-watchdog",
        daemon=True,
    )
    thread.start()
    _hourly_thread = thread


@app.on_event("shutdown")
def stop_hourly_watchdog() -> None:
    """Gracefully stop the background thread when the app shuts down."""
    global _hourly_thread  # noqa: PLW0603
    _hourly_stop_event.set()
    thread = _hourly_thread
    if thread and thread.is_alive():
        thread.join(timeout=10)
    _hourly_thread = None


@app.get("/healthz")
def healthcheck() -> Dict[str, str]:
    return {"status": "ok"}


@app.get(
    "/v1/random_numbers",
    response_model=RandomNumberResponse,
)
def random_numbers(
    min_value: Annotated[int, Query(ge=0, description="Giá trị nhỏ nhất (mặc định 0).")] = 0,
    max_value: Annotated[int, Query(gt=0, description="Giá trị lớn nhất (mặc định 99).")] = 99,
    count: Annotated[int, Query(gt=0, le=20, description="Số lượng số cần lấy (tối đa 20).")] = 1,
    unique: Annotated[bool, Query(description="Tránh trùng lặp số nếu đặt thành true.")] = False,
) -> RandomNumberResponse:
    if min_value > max_value:
        raise HTTPException(status_code=422, detail="min_value phải nhỏ hơn hoặc bằng max_value.")

    span = max_value - min_value + 1
    if unique and count > span:
        raise HTTPException(
            status_code=422,
            detail="Không thể tạo đủ số ngẫu nhiên không trùng lặp trong khoảng đã cho.",
        )

    if unique:
        numbers = random.sample(range(min_value, max_value + 1), count)
    else:
        numbers = [random.randint(min_value, max_value) for _ in range(count)]

    return RandomNumberResponse(
        min_value=min_value,
        max_value=max_value,
        count=count,
        unique=unique,
        numbers=numbers,
    )


@app.get(
    "/privacy_policy",
    response_model=PrivacyPolicyResponse,
)
def privacy_policy() -> PrivacyPolicyResponse:
    today_iso = dt.date.today().isoformat()
    return PrivacyPolicyResponse(
        title="Chính sách quyền riêng tư",
        description=(
            "Ứng dụng này chỉ thu thập và hiển thị dữ liệu kết quả xổ số. "
            "Chúng tôi không yêu cầu, lưu trữ hay xử lý thông tin cá nhân của người dùng."
        ),
        data_usage=(
            "Dữ liệu được sử dụng duy nhất để phản hồi câu hỏi về kết quả xổ số. "
            "Không có dữ liệu cá nhân hay hành vi người dùng nào được thu thập."
        ),
        limitations=(
            "Kết quả xổ số được cung cấp mang tính tham khảo. Người dùng nên đối chiếu với nguồn chính thức "
            "khi cần xác minh."
        ),
        contact="Liên hệ: clientsupport@pmsa.com.vn",
        last_updated=today_iso,
    )


@app.get(
    "/v1/kqsx/summary",
    response_model=LotterySummaryResponse,
    response_model_exclude_none=True,
)
def get_lottery_summary(
    date: Annotated[
        Optional[str],
        Query(description="Ngày kết quả (YYYY-MM-DD). Mặc định là hôm nay."),
    ] = None,
    region: Annotated[
        Optional[str],
        Query(
            pattern="^(mn|mt|mb)$",
            description="mn (Miền Nam), mt (Miền Trung), mb (Miền Bắc). Bỏ trống để lấy tất cả.",
        ),
    ] = None,
) -> LotterySummaryResponse:
    requested_date = parse_date(date)
    regions_to_fetch = [region] if region else list(REGION_CONFIG.keys())
    resolved_date, fallback_offset, region_draws = gather_draws_for_regions(requested_date, regions_to_fetch)

    draw_summaries: List[DrawSummary] = []
    for region_key, draw_items in region_draws.items():
        draw_summaries.extend(build_draw_summaries(draw_items, region_key))

    if region:
        region_value = region
        region_label = REGION_CONFIG[region]["label"]
    else:
        region_value = "all"
        region_label = "3 Miền"
    summary_text = render_summary_text(resolved_date, region, draw_summaries)

    return LotterySummaryResponse(
        requested_date=requested_date.isoformat(),
        date=resolved_date.isoformat(),
        region=region_value,
        region_label=region_label,
        draws=draw_summaries,
        summary_text=summary_text,
        fallback_offset_days=fallback_offset,
    )
