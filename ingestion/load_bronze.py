"""Load raw NYC TLC Parquet files vào Bronze layer (DuckDB warehouse), idempotent theo tháng.

Usage:
    python ingestion/load_bronze.py --year-month 2023-01

Yêu cầu: đã chạy download_tlc.py cho tháng này trước (file Parquet nằm ở data/raw/).
"""

import argparse
import sys
from pathlib import Path

import duckdb

sys.stdout.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
WAREHOUSE_PATH = PROJECT_ROOT / "warehouse.duckdb"

BRONZE_TRIPS_TABLE = "bronze_yellow_trips"
BRONZE_ZONE_LOOKUP_TABLE = "bronze_taxi_zone_lookup"


def load_bronze_trips(con: duckdb.DuckDBPyConnection, year_month: str) -> None:
    """Nạp 1 tháng dữ liệu trip vào bảng bronze_yellow_trips, idempotent theo tháng.

    Pattern "delete-by-partition rồi insert": trước khi insert, xóa hết dòng cũ
    của đúng tháng này (nếu có). Nhờ vậy, chạy lại script này 10 lần cho cùng
    1 tháng vẫn cho ra đúng 1 bản ghi cho mỗi trip — không nhân đôi dữ liệu.
    Đây chính là ý nghĩa của "idempotent": áp dụng nhiều lần cho cùng kết quả
    như áp dụng 1 lần.

    year_month được lưu thành 1 cột riêng (partition key) để có thể xóa/insert
    theo đúng phạm vi 1 tháng, thay vì phải so sánh từng dòng.
    """
    parquet_path = RAW_DATA_DIR / f"yellow_tripdata_{year_month}.parquet"
    if not parquet_path.exists():
        raise FileNotFoundError(
            f"Không tìm thấy {parquet_path}. Chạy download_tlc.py --year-month {year_month} trước."
        )

    table_exists = con.sql(
        f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{BRONZE_TRIPS_TABLE}'"
    ).fetchone()[0] > 0

    if not table_exists:
        print(f"[create] Tạo bảng {BRONZE_TRIPS_TABLE} từ tháng {year_month}")
        con.sql(
            f"""
            CREATE TABLE {BRONZE_TRIPS_TABLE} AS
            SELECT '{year_month}' AS year_month, *
            FROM read_parquet('{parquet_path}')
            """
        )
    else:
        deleted = con.sql(
            f"SELECT count(*) FROM {BRONZE_TRIPS_TABLE} WHERE year_month = '{year_month}'"
        ).fetchone()[0]
        print(f"[delete] Xóa {deleted:,} dòng cũ của tháng {year_month} (nếu có)")
        con.sql(f"DELETE FROM {BRONZE_TRIPS_TABLE} WHERE year_month = '{year_month}'")

        print(f"[insert] Nạp lại dữ liệu tháng {year_month}")
        con.sql(
            f"""
            INSERT INTO {BRONZE_TRIPS_TABLE}
            SELECT '{year_month}' AS year_month, *
            FROM read_parquet('{parquet_path}')
            """
        )

    row_count = con.sql(
        f"SELECT count(*) FROM {BRONZE_TRIPS_TABLE} WHERE year_month = '{year_month}'"
    ).fetchone()[0]
    total_count = con.sql(f"SELECT count(*) FROM {BRONZE_TRIPS_TABLE}").fetchone()[0]
    print(f"[ok] Tháng {year_month}: {row_count:,} dòng. Tổng bảng: {total_count:,} dòng")


def load_bronze_zone_lookup(con: duckdb.DuckDBPyConnection) -> None:
    """Nạp bảng zone lookup, idempotent bằng CREATE OR REPLACE (không partition theo tháng).

    Khác với trip data (thay đổi theo từng tháng), zone lookup là 1 bảng tĩnh,
    dùng chung cho mọi tháng -> mỗi lần nạp lại chỉ cần thay thế toàn bộ bảng,
    không cần logic delete-by-partition.
    """
    csv_path = RAW_DATA_DIR / "taxi_zone_lookup.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"Không tìm thấy {csv_path}. Chạy download_tlc.py trước.")

    con.sql(
        f"""
        CREATE OR REPLACE TABLE {BRONZE_ZONE_LOOKUP_TABLE} AS
        SELECT * FROM read_csv_auto('{csv_path}')
        """
    )
    row_count = con.sql(f"SELECT count(*) FROM {BRONZE_ZONE_LOOKUP_TABLE}").fetchone()[0]
    print(f"[ok] {BRONZE_ZONE_LOOKUP_TABLE}: {row_count:,} dòng")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--year-month",
        required=True,
        help="Tháng cần nạp vào bronze, định dạng YYYY-MM (vd: 2023-01)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    con = duckdb.connect(str(WAREHOUSE_PATH))
    try:
        load_bronze_trips(con, args.year_month)
        load_bronze_zone_lookup(con)
    finally:
        con.close()


if __name__ == "__main__":
    main()
