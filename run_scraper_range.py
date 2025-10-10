import argparse
import datetime as dt
import os
from typing import Iterable, List

from scraper import run as run_scraper


def iterate_dates(start: dt.date, end: dt.date) -> Iterable[dt.date]:
    current = start
    while current <= end:
        yield current
        current += dt.timedelta(days=1)


def parse_date(value: str, flag: str) -> dt.date:
    try:
        return dt.datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"{flag} must be in YYYY-MM-DD format.") from exc


def resolve_regions(selected: List[str]) -> List[str]:
    return selected or ["mb", "mt", "mn"]


def build_sql_path(directory: str, date_str: str, region: str) -> str:
    filename = f"{date_str}_{region}.sql"
    return os.path.join(directory, filename)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run scraper.py over a date range for one or more regions."
    )
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD).")
    parser.add_argument("--end", help="End date (YYYY-MM-DD). Defaults to today.")
    parser.add_argument(
        "--region",
        dest="regions",
        action="append",
        choices=["mb", "mt", "mn"],
        help="Region code to scrape. Repeat for multiple regions. Defaults to all.",
    )
    parser.add_argument(
        "--no-supabase",
        action="store_true",
        help="Skip Supabase upload and only generate SQL exports.",
    )
    parser.add_argument(
        "--out-dir",
        help="Directory to store SQL exports for each date/region.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Stop immediately if any scrape fails. Defaults to skipping failures.",
    )
    args = parser.parse_args()

    start_date = parse_date(args.start, "--start")
    end_date = parse_date(args.end, "--end") if args.end else dt.date.today()

    if end_date < start_date:
        parser.error("--end must be on or after --start.")

    regions = resolve_regions(args.regions or [])

    out_dir = None
    if args.out_dir:
        out_dir = os.path.abspath(args.out_dir)
        os.makedirs(out_dir, exist_ok=True)

    failures: List[str] = []

    for current_date in iterate_dates(start_date, end_date):
        date_str = current_date.strftime("%Y-%m-%d")
        for region in regions:
            print(f"Running scraper for {date_str} ({region})...")
            out_path = build_sql_path(out_dir, date_str, region) if out_dir else None
            try:
                run_scraper(
                    date_str,
                    region,
                    out_path=out_path,
                    use_supabase=not args.no_supabase,
                )
            except Exception as exc:  # pylint: disable=broad-except
                message = f"{date_str} ({region}): {exc}"
                failures.append(message)
                print(f"  -> FAILED: {message}")
                if args.strict:
                    raise

    if failures:
        joined = "\n".join(failures)
        print(
            "\nCompleted with failures on the following runs:\n"
            f"{joined}\n"
            "Use --strict to stop on first failure if you need to investigate."
        )


if __name__ == "__main__":
    main()
