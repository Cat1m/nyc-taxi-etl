# NYC Taxi ETL Pipeline

Batch ETL pipeline xử lý dữ liệu NYC Taxi (NYC TLC Trip Record Data), xây dựng
theo kiến trúc medallion (Bronze → Silver → Gold). Project vừa để học các khái
niệm nền tảng của Data Engineering, vừa làm portfolio.

## Mục tiêu

- Nạp dữ liệu chuyến đi taxi (Yellow Taxi trip records) từ NYC TLC vào một
  warehouse local (DuckDB).
- Làm sạch, chuẩn hóa và mô hình hóa dữ liệu thành star schema bằng dbt.
- Tự động hóa toàn bộ pipeline bằng Airflow (download → load → transform → test).

## Kiến trúc

```
NYC TLC (Parquet files)
        │  download
        ▼
   Bronze layer        raw data, giữ nguyên gốc, chưa qua xử lý
        │  dbt (staging models)
        ▼
   Silver layer        đã ép kiểu, đổi tên cột, lọc dòng rác
        │  dbt (marts models)
        ▼
   Gold layer          star schema: fact_trips + dim_datetime,
                       dim_location, dim_payment, dim_vendor
```

Mỗi layer là một bảng/model DuckDB riêng, không ghi đè lên nhau — cho phép
truy vết lại (traceability) và tái chạy transform mà không cần tải lại data.

## Tech stack

- **Python 3.10+**
- **DuckDB** — vừa làm compute engine vừa làm warehouse local
- **dbt (dbt-duckdb)** — transform, modeling, testing
- **Airflow (LocalExecutor)** — orchestration
- **Git/GitHub** — version control

Chưa dùng Spark/BigQuery/dashboard ở giai đoạn này (để dành mở rộng sau).

## Cấu trúc thư mục

```
nyc-taxi-etl/
├── data/
│   ├── raw/           # dữ liệu Parquet tải về (gitignored)
│   └── staging/       # dữ liệu trung gian (gitignored)
├── ingestion/          # script tải dữ liệu từ NYC TLC
├── transform/          # dbt project (staging + marts models)
├── orchestration/       # Airflow DAGs
├── requirements.txt
└── .gitignore
```

## Trạng thái hiện tại

- [x] Step 0 — Scaffold & môi trường
- [ ] Step 1 — Ingestion (tải dữ liệu)
- [ ] Step 2 — Bronze load + Idempotency
- [ ] Step 3 — dbt: Silver + Gold (star schema)
- [ ] Step 4 — Airflow orchestration
- [ ] Step 5 — Đánh bóng portfolio
