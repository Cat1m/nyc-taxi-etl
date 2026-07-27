# Cheatsheet — các lệnh hay dùng

Tất cả lệnh dưới đây chạy từ **project root** (`nyc-taxi-etl/`), trừ khi ghi chú
khác. Trên Windows, dùng `venv\Scripts\...` (không phải `venv/bin/...`).

## Môi trường

```bash
# tạo venv (chỉ cần 1 lần)
python -m venv venv

# kích hoạt venv (mỗi lần mở terminal mới)
venv\Scripts\activate

# tắt venv
deactivate

# cài lại toàn bộ dependency (vd sau khi clone repo mới, hoặc xóa venv cũ)
pip install -r requirements.txt
```

## Ingestion (Step 1-2)

```bash
# tải Parquet + zone lookup cho 1 tháng, kèm data profiling
python ingestion/download_tlc.py --year-month 2023-01

# nạp vào bronze layer (idempotent — chạy lại bao nhiêu lần cũng không nhân đôi)
python ingestion/load_bronze.py --year-month 2023-01
```

## Great Expectations (Step 6)

```bash
# validate bronze 1 tháng — chạy sau load_bronze, trước dbt run
python quality/validate_bronze.py --year-month 2023-01
```

Exit code khác 0 nếu có expectation fail (Airflow dựa vào đây để chặn `dbt_run`
chạy tiếp). Xem báo cáo trực quan (cộng dồn lịch sử theo tháng, không ghi đè):

```bash
# mở bằng trình duyệt (Windows)
start quality/gx/uncommitted/data_docs/local_site/index.html
```

`quality/gx/expectations/`, `quality/gx/checkpoints/` là cấu hình (commit vào
git); `quality/gx/uncommitted/` là output sinh ra (gitignored).

## dbt (Step 3)

```bash
# cài package (dbt_utils) — chỉ cần chạy lại khi packages.yml đổi
dbt deps --project-dir transform --profiles-dir transform

# nạp seed (bảng lookup tĩnh: vendor, payment_type)
dbt seed --project-dir transform --profiles-dir transform

# chạy toàn bộ model (silver + gold)
dbt run --project-dir transform --profiles-dir transform

# chạy toàn bộ test
dbt test --project-dir transform --profiles-dir transform

# tạo dữ liệu cho docs UI (catalog.json, manifest.json)
dbt docs generate --project-dir transform --profiles-dir transform

# mở web UI xem docs + lineage graph tại http://localhost:8080
dbt docs serve --project-dir transform --profiles-dir transform --port 8080
```

**Lineage graph:** sau khi mở `localhost:8080`, bấm icon graph (hình các node
nối nhau) ở góc dưới bên phải để xem sơ đồ phụ thuộc giữa các model.

## DuckDB CLI — tự query như 1 analyst

```bash
# mở đúng warehouse đã có sẵn data (không phải database rỗng)
duckdb warehouse.duckdb

# nếu bị lock bởi 1 tiến trình khác (vd dbt docs serve đang chạy)
duckdb warehouse.duckdb -readonly
```

Trong session `duckdb` (dấu nhắc `D `), mọi câu lệnh phải kết thúc bằng `;`:

```sql
SHOW TABLES;
SELECT * FROM fact_trips LIMIT 10;
.quit   -- thoát
```

## Airflow (Step 4, chạy trong Docker)

```bash
# tất cả lệnh docker compose chạy từ orchestration/
cd orchestration

# build lại image (cần chạy lại khi sửa Dockerfile hoặc requirements.txt)
docker compose build

# khởi tạo metadata DB + tạo user admin (chỉ cần 1 lần, hoặc khi đổi DB)
docker compose up airflow-init

# khởi động webserver + scheduler (chạy nền)
docker compose up -d airflow-webserver airflow-scheduler

# xem trạng thái container
docker compose ps

# tắt toàn bộ (giữ lại data trong volume)
docker compose down

# tắt và xóa luôn volume Postgres (mất lịch sử DAG run, KHÔNG mất warehouse.duckdb
# vì nó là bind mount từ host, không phải volume)
docker compose down -v
```

Web UI: **http://localhost:8081** (login `airflow` / `airflow`).

```bash
# liệt kê DAG đã nhận diện
docker compose exec airflow-scheduler airflow dags list

# xem các lần chạy của 1 DAG
docker compose exec airflow-scheduler airflow dags list-runs -d nyc_taxi_pipeline

# xem trạng thái từng task trong 1 lần chạy cụ thể
docker compose exec airflow-scheduler airflow tasks states-for-dag-run nyc_taxi_pipeline 2023-01-01T00:00:00+00:00

# trigger tay 1 lần chạy cho ngày cụ thể (thường không cần vì catchup=True tự backfill)
docker compose exec airflow-scheduler airflow dags trigger nyc_taxi_pipeline -e 2023-03-01T00:00:00+00:00
```

## Lưu ý quan trọng

- **DuckDB single-writer:** chỉ 1 tiến trình được mở/ghi `warehouse.duckdb`
  cùng lúc. Nếu gặp lỗi "file is being used by another process", tìm và đóng
  session `duckdb`/`dbt` khác đang mở file này trước (hoặc mở `-readonly`
  nếu chỉ cần đọc).
- `profiles.yml` resolve đường dẫn `warehouse.duckdb` theo **thư mục đang
  đứng (cwd)** khi gọi lệnh — luôn chạy dbt/duckdb từ project root, không
  phải từ trong `transform/`.
