"""Download NYC TLC Yellow Taxi trip data and profile it with DuckDB.

Usage:
    python ingestion/download_tlc.py --year-month 2023-01
"""

import argparse
import sys
from pathlib import Path

import duckdb
import requests

# duckdb's .show() renders tables with Unicode box-drawing characters, which
# crash on Windows terminals still defaulting to the legacy cp1252 codepage.
sys.stdout.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


def download_file(url: str, dest_path: Path) -> Path:
    """Stream-download a file to dest_path, skipping if it already exists.

    Streaming (instead of loading the whole response into memory) matters
    here because trip-data files are tens of MB each and this script will
    later be called once per month of data.
    """
    if dest_path.exists():
        print(f"[skip] {dest_path.name} đã tồn tại, không tải lại")
        return dest_path

    print(f"[download] {url} -> {dest_path}")
    response = requests.get(url, stream=True, timeout=60)
    response.raise_for_status()

    dest_path.parent.mkdir(parents=True, exist_ok=True)
    with open(dest_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)

    return dest_path


def download_trip_data(year_month: str) -> Path:
    """Download the Yellow Taxi trip-data Parquet file for a given year-month (e.g. '2023-01')."""
    filename = f"yellow_tripdata_{year_month}.parquet"
    url = f"{TLC_BASE_URL}/{filename}"
    return download_file(url, RAW_DATA_DIR / filename)


def download_zone_lookup() -> Path:
    """Download the Taxi Zone Lookup table (used later to build dim_location)."""
    filename = "taxi_zone_lookup.csv"
    return download_file(ZONE_LOOKUP_URL, RAW_DATA_DIR / filename)


def profile_trip_data(parquet_path: Path) -> None:
    """Run a quick data profiling pass over the downloaded Parquet file using DuckDB.

    Data profiling = nhìn nhanh vào shape, kiểu dữ liệu, và phân bố giá trị của
    dữ liệu THẬT trước khi viết bất kỳ transform nào. Mục đích: phát hiện sớm
    các giả định sai (vd: cột tưởng luôn dương nhưng thực ra có số âm) để quyết
    định luật lọc/clean ở Silver layer (Step 3), thay vì đoán mò.
    """
    con = duckdb.connect()

    print("\n=== Schema ===")
    con.sql(f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}')").show()

    row_count = con.sql(
        f"SELECT COUNT(*) AS row_count FROM read_parquet('{parquet_path}')"
    ).fetchone()[0]
    print(f"\n=== Row count: {row_count:,} ===")

    print("\n=== Phân bố fare_amount & trip_distance ===")
    con.sql(
        f"""
        SELECT
            min(fare_amount)      AS fare_min,
            max(fare_amount)      AS fare_max,
            avg(fare_amount)      AS fare_avg,
            median(fare_amount)   AS fare_median,
            min(trip_distance)    AS distance_min,
            max(trip_distance)    AS distance_max,
            avg(trip_distance)    AS distance_avg
        FROM read_parquet('{parquet_path}')
        """
    ).show()

    print("=== Giá trị rác tiềm ẩn ===")
    con.sql(
        f"""
        SELECT
            sum(CASE WHEN fare_amount <= 0 THEN 1 ELSE 0 END)     AS fare_non_positive,
            sum(CASE WHEN trip_distance <= 0 THEN 1 ELSE 0 END)   AS distance_non_positive,
            sum(CASE WHEN passenger_count = 0 THEN 1 ELSE 0 END)  AS zero_passenger,
            sum(CASE WHEN tpep_dropoff_datetime <= tpep_pickup_datetime THEN 1 ELSE 0 END)
                AS dropoff_before_pickup
        FROM read_parquet('{parquet_path}')
        """
    ).show()

    con.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--year-month",
        required=True,
        help="Tháng cần tải, định dạng YYYY-MM (vd: 2023-01)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    trip_data_path = download_trip_data(args.year_month)
    download_zone_lookup()

    profile_trip_data(trip_data_path)


if __name__ == "__main__":
    main()
