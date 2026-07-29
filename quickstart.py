"""Chạy thử toàn bộ pipeline (phần local, không cần Docker) chỉ bằng 1 lệnh.

Dành cho người mới clone repo (hoặc AI được nhờ chạy hộ) muốn thấy pipeline
chạy thật và có số liệu demo ngay, thay vì phải tự đọc README rồi gõ tay từng
lệnh. Script này chỉ làm đúng phần không cần Docker: ingest 1 tháng dữ liệu,
validate bằng Great Expectations, rồi build Silver/Gold bằng dbt — đúng thứ
tự thật của pipeline (xem orchestration/dags/nyc_taxi_pipeline.py). Airflow
và Dashboard (Metabase) cần Docker Desktop chạy sẵn nên KHÔNG tự động hóa ở
đây — script chỉ in hướng dẫn bước tiếp theo.

Yêu cầu trước khi chạy (xem README mục "Cách chạy", bước 1):
    python -m venv venv
    venv\\Scripts\\activate
    python quickstart.py [--year-month 2023-01]
"""

import argparse
import subprocess
import sys
from pathlib import Path

import duckdb

sys.stdout.reconfigure(encoding="utf-8")

ROOT = Path(__file__).resolve().parent
WAREHOUSE_PATH = ROOT / "warehouse.duckdb"

# dbt là 1 console-script cài cùng thư mục với chính interpreter đang chạy
# script này (venv/Scripts trên Windows, venv/bin trên Unix) -- gọi thẳng
# theo đường dẫn đó thay vì tên "dbt" trần, vì subprocess không tự biết PATH
# của venv khi venv chưa được activate ở shell gọi script (chỉ chạy đúng
# python.exe của venv không đủ để "dbt" trần được tìm thấy).
DBT_BIN = str(Path(sys.executable).parent / ("dbt.exe" if sys.platform == "win32" else "dbt"))


def run_step(description: str, cmd: list) -> None:
    print(f"\n=== {description} ===")
    try:
        subprocess.run(cmd, cwd=ROOT, check=True)
    except subprocess.CalledProcessError:
        print(
            f"\n[dừng] Bước '{description}' thất bại -- xem log lỗi ở trên rồi xử lý "
            "trước khi chạy lại (script không đoán mù chạy tiếp bước sau)."
        )
        sys.exit(1)


def show_demo_query() -> None:
    """In nhanh 1 query demo (top zone đông khách) để thấy ngay dữ liệu thật,
    không chỉ dòng "thành công" khô khan -- lấy đúng câu hỏi đã có trong README.
    """
    print("\n=== Demo: top 3 zone đón khách đông nhất ===")
    con = duckdb.connect(str(WAREHOUSE_PATH), read_only=True)
    try:
        con.sql(
            """
            SELECT z.borough, z.zone, count(*) AS so_chuyen
            FROM fact_trips f
            JOIN dim_location z ON f.pickup_location_id = z.location_id
            GROUP BY z.borough, z.zone
            ORDER BY so_chuyen DESC
            LIMIT 3
            """
        ).show()
    finally:
        con.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--year-month",
        default="2023-01",
        help="Tháng demo để tải + nạp, định dạng YYYY-MM (mặc định: 2023-01)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ym = args.year_month

    run_step(
        "Cài dependencies (requirements.txt)",
        [sys.executable, "-m", "pip", "install", "-r", "requirements.txt"],
    )
    run_step(
        f"Tải dữ liệu tháng {ym}",
        [sys.executable, "ingestion/download_tlc.py", "--year-month", ym],
    )
    run_step(
        f"Nạp Bronze layer tháng {ym}",
        [sys.executable, "ingestion/load_bronze.py", "--year-month", ym],
    )
    run_step(
        f"Validate Bronze bằng Great Expectations tháng {ym}",
        [sys.executable, "quality/validate_bronze.py", "--year-month", ym],
    )
    run_step(
        "dbt deps",
        [DBT_BIN, "deps", "--project-dir", "transform", "--profiles-dir", "transform"],
    )
    run_step(
        "dbt seed",
        [DBT_BIN, "seed", "--project-dir", "transform", "--profiles-dir", "transform"],
    )
    run_step(
        "dbt run (Silver + Gold)",
        [DBT_BIN, "run", "--project-dir", "transform", "--profiles-dir", "transform"],
    )
    run_step(
        "dbt test",
        [DBT_BIN, "test", "--project-dir", "transform", "--profiles-dir", "transform"],
    )

    show_demo_query()

    print(
        "\n=== Xong! Pipeline local (không cần Docker) đã chạy đầy đủ. ===\n"
        "Bước tiếp theo (cần Docker Desktop):\n"
        "  - Airflow (toàn bộ pipeline tự động):  xem README mục 3, docs/commands.md\n"
        "  - Dashboard (Metabase):                xem README mục 4, docs/commands.md"
    )


if __name__ == "__main__":
    main()
