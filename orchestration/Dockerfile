# Base image Airflow chính thức, cài thêm dependency riêng của project (duckdb,
# dbt-duckdb, requests, pandas) để task gọi được ingestion/download_tlc.py,
# ingestion/load_bronze.py, và dbt trực tiếp bên trong container.
FROM apache/airflow:2.10.4-python3.11

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
