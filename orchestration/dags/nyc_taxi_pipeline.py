"""DAG orchestrate toàn bộ pipeline NYC Taxi: download -> load_bronze -> dbt_run -> dbt_test.

year_month của mỗi lần chạy lấy từ chính lịch chạy của DAG (data_interval_start),
không truyền tay -- nhờ vậy backfill nhiều tháng chỉ cần dùng lệnh
`airflow dags backfill`, Airflow tự tạo 1 DAG run cho mỗi tháng với đúng
year_month tương ứng.

LƯU Ý KỸ THUẬT: DuckDB là single-writer (chỉ 1 tiến trình được ghi file
warehouse.duckdb cùng lúc). Vì vậy:
    - max_active_runs=1: không cho 2 DAG run (2 tháng khác nhau) chạy song
      song, tránh 2 task cùng ghi vào warehouse.duckdb cùng lúc.
    - Trong mỗi task, các script (download_tlc.py, load_bronze.py, dbt) đều tự
      mở connection -> làm việc -> đóng connection ngay khi xong (xem
      finally: con.close() trong load_bronze.py) -- không giữ connection mở
      xuyên suốt DAG run.
"""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="nyc_taxi_pipeline",
    description="Download -> Bronze load -> dbt run -> dbt test, 1 lần/tháng",
    default_args=default_args,
    schedule="@monthly",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    tags=["nyc-taxi", "etl"],
) as dag:

    # {{ ds[:7] }} = 7 ký tự đầu của execution date "YYYY-MM-DD" -> "YYYY-MM"
    year_month = "{{ ds[:7] }}"

    download = BashOperator(
        task_id="download",
        bash_command=f"python /opt/airflow/ingestion/download_tlc.py --year-month {year_month}",
    )

    load_bronze = BashOperator(
        task_id="load_bronze",
        bash_command=f"python /opt/airflow/ingestion/load_bronze.py --year-month {year_month}",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow && dbt run --project-dir transform --profiles-dir transform",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow && dbt test --project-dir transform --profiles-dir transform",
    )

    download >> load_bronze >> dbt_run >> dbt_test
