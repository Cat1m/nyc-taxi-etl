"""Validate 1 tháng dữ liệu Bronze bằng Great Expectations (GX), chạy như 1 bước
thật trong pipeline: sau load_bronze, trước dbt_run (xem
orchestration/dags/nyc_taxi_pipeline.py).

Mục đích: dbt test hiện có (`transform/models/staging/_staging_models.yml`,
`_marts_models.yml`) chỉ kiểm tra ràng buộc nhị phân (not_null, unique,
accepted_values...) — đúng/sai, không có khái niệm "gần đúng nhưng lệch bất
thường". GX bổ sung expectation về *phân bố* (mean, median, quantile), hữu ích
để phát hiện drift dữ liệu giữa các tháng mà dbt test built-in không biểu đạt
được.

Dùng persistent File Data Context (`quality/gx/`, tự scaffold lần đầu chạy)
thay vì Ephemeral: mỗi lần chạy được gắn run_name = year_month, nên Data Docs
(`quality/gx/uncommitted/data_docs/local_site/index.html`) cộng dồn lịch sử
theo tháng thay vì ghi đè -- đã verify khi bàn với user trước khi tích hợp.
`quality/gx/expectations/`, `quality/gx/checkpoints/` là cấu hình (source, nên
commit); `quality/gx/uncommitted/` là output sinh ra, gitignore (xem
.gitignore -- GX tự sinh đúng quy ước này khi scaffold).

Đọc thẳng từ Bronze (trước khi Silver lọc rác) bằng kết nối read-only, để tuyệt
đối không có rủi ro ghi nhầm vào warehouse.duckdb (DuckDB single-writer).

Exit code khác 0 nếu có expectation fail -- để Airflow BashOperator nhận đúng
tín hiệu và chặn dbt_run chạy tiếp, nhất quán với cách dbt_test hiện đang
chặn pipeline.

Usage (chạy từ project root):
    python quality/validate_bronze.py --year-month 2023-01
"""

import argparse
import sys
from pathlib import Path

import duckdb
import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.checkpoint import UpdateDataDocsAction

sys.stdout.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
WAREHOUSE_PATH = PROJECT_ROOT / "warehouse.duckdb"
GX_PROJECT_ROOT_DIR = Path(__file__).resolve().parent

SUITE_NAME = "bronze_yellow_trips"
DATASOURCE_NAME = "bronze_source"
ASSET_NAME = "bronze_yellow_trips_asset"
BATCH_DEFINITION_NAME = "whole_month"
VALIDATION_DEFINITION_NAME = "bronze_yellow_trips_validation"
CHECKPOINT_NAME = "bronze_yellow_trips_checkpoint"
DATA_DOCS_SITE_NAME = "local_site"


def load_bronze_month(year_month: str):
    """Đọc 1 tháng dữ liệu bronze_yellow_trips vào pandas DataFrame, read-only.

    read_only=True đảm bảo bước validate này không thể ghi nhầm vào
    warehouse.duckdb dù có lỗi code -- DuckDB chỉ cho 1 writer tại 1 thời
    điểm, và bước này không có lý do gì cần ghi.
    """
    con = duckdb.connect(str(WAREHOUSE_PATH), read_only=True)
    try:
        df = con.sql(
            f"SELECT * FROM bronze_yellow_trips WHERE year_month = '{year_month}'"
        ).df()
    finally:
        con.close()
    return df


def build_expectation_suite(context) -> gx.ExpectationSuite:
    """Định nghĩa 1 suite gồm cả expectation phân bố (điểm mới so với dbt test)
    lẫn 1 expectation boundary quen thuộc (để đối chiếu trực tiếp với dbt test).

    Ngưỡng dựa trên profiling thật của bronze tháng 2023-01 (đo trực tiếp qua
    DuckDB, xem CLAUDE.md/docs/commands.md để tái tạo), đã verify tiếp trên
    2023-02 vẫn pass -- phân bố ổn định qua các tháng:
        fare_amount:   min -900, max 1160.1, avg ~18.37, median 12.8, stddev ~17.8
        trip_distance: median 1.8

    add_or_update thay vì add: script chạy lại nhiều lần (mỗi tháng, hoặc
    Airflow retry) trên cùng 1 persistent context không được lỗi "đã tồn tại".
    """
    suite = context.suites.add_or_update(gx.ExpectationSuite(name=SUITE_NAME))
    suite.expectations = []

    # -- Expectation PHÂN BỐ (điểm khác biệt của GX so với dbt test hiện có) --
    suite.add_expectation(
        gxe.ExpectColumnMeanToBeBetween(column="fare_amount", min_value=15, max_value=22)
    )
    suite.add_expectation(
        gxe.ExpectColumnMedianToBeBetween(column="fare_amount", min_value=10, max_value=16)
    )
    suite.add_expectation(
        gxe.ExpectColumnQuantileValuesToBeBetween(
            column="trip_distance",
            quantile_ranges={
                "quantiles": [0.5],
                "value_ranges": [[1.0, 3.0]],
            },
        )
    )

    # -- Expectation BOUNDARY quen thuộc, để so sánh với dbt test trên Silver --
    # dbt test hiện có lọc trip_distance > 0 AND < 100 ở stg_trips.sql; ở đây
    # kiểm tra chính khoảng đó nhưng trên Bronze (chưa lọc) để thấy rõ dữ liệu
    # rác mà Silver sẽ loại bỏ.
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeBetween(
            column="trip_distance", min_value=0, max_value=100, mostly=0.95
        )
    )
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="fare_amount"))

    return suite


def get_or_create_asset(datasource, name: str):
    """add_dataframe_asset lỗi nếu asset đã tồn tại -- không có add_or_update
    cho asset/batch definition, nên tự bắt LookupError để idempotent."""
    try:
        return datasource.get_asset(name)
    except LookupError:
        return datasource.add_dataframe_asset(name=name)


def get_or_create_batch_definition(asset, name: str):
    for batch_definition in asset.batch_definitions:
        if batch_definition.name == name:
            return batch_definition
    return asset.add_batch_definition_whole_dataframe(name)


def run_checkpoint(context, suite, df, year_month: str):
    datasource = context.data_sources.add_or_update_pandas(DATASOURCE_NAME)
    asset = get_or_create_asset(datasource, ASSET_NAME)
    batch_definition = get_or_create_batch_definition(asset, BATCH_DEFINITION_NAME)

    validation_definition = context.validation_definitions.add_or_update(
        gx.ValidationDefinition(
            name=VALIDATION_DEFINITION_NAME, data=batch_definition, suite=suite
        )
    )

    checkpoint = context.checkpoints.add_or_update(
        gx.Checkpoint(
            name=CHECKPOINT_NAME,
            validation_definitions=[validation_definition],
            actions=[
                UpdateDataDocsAction(name="update_data_docs", site_names=[DATA_DOCS_SITE_NAME])
            ],
        )
    )
    return checkpoint.run(
        batch_parameters={"dataframe": df},
        run_id=gx.RunIdentifier(run_name=year_month),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--year-month",
        required=True,
        help="Tháng bronze cần validate, định dạng YYYY-MM (vd: 2023-01)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    year_month = args.year_month

    print(f"[load] Đọc bronze_yellow_trips tháng {year_month} (read-only)...")
    df = load_bronze_month(year_month)
    print(f"[load] {len(df):,} dòng")

    context = gx.get_context(mode="file", project_root_dir=GX_PROJECT_ROOT_DIR)
    suite = build_expectation_suite(context)

    print("[validate] Chạy checkpoint GX...")
    result = run_checkpoint(context, suite, df, year_month)

    print(f"\n=== Kết quả tổng: {'PASS' if result.success else 'FAIL'} ===")
    for validation_result in result.run_results.values():
        for expectation_result in validation_result["results"]:
            status = "OK  " if expectation_result.success else "FAIL"
            exp_type = expectation_result.expectation_config.type
            column = expectation_result.expectation_config.kwargs.get("column")
            observed = expectation_result.result.get("observed_value")
            print(f"[{status}] {exp_type} ({column}) -> observed={observed}")

    data_docs_path = GX_PROJECT_ROOT_DIR / "gx" / "uncommitted" / "data_docs" / "local_site" / "index.html"
    print(f"\n[data-docs] Xem báo cáo HTML tại: {data_docs_path}")

    sys.exit(0 if result.success else 1)


if __name__ == "__main__":
    main()
