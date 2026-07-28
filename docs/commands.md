# Cheatsheet — các lệnh hay dùng

Tất cả lệnh dưới đây chạy từ **project root** (`nyc-taxi-etl/`), trừ khi ghi chú
khác. Trên Windows, dùng `venv\Scripts\...` (không phải `venv/bin/...`).

## Bắt đầu lại phiên làm việc (mở máy lại / hôm sau)

1. Mở Docker Desktop, đợi chạy xong hẳn.
2. **Kiểm tra container nào đang tự chạy ngầm** — cả 2 docker-compose
   (`orchestration/`, `dashboard/`) đều đặt `restart: always`, nghĩa là nếu
   chúng đang chạy lúc tắt máy, Docker Desktop sẽ **tự bật lại** khi mở lại,
   kể cả khi không chủ động `docker compose up`:
   ```bash
   docker ps
   ```
   Biết trước `metabase`/`airflow-*` có đang chạy ngầm không, tránh bị lock
   `warehouse.duckdb` mà không hiểu vì sao (xem mục DuckDB single-writer).
3. Chọn 1 trong 2 việc — **không làm đồng thời**:
   - **Muốn chạy/ghi dữ liệu** (ingest tháng mới, `dbt run`, GX, Airflow):
     đảm bảo Metabase đang tắt trước (nếu đang chạy):
     ```bash
     cd dashboard && docker compose down
     ```
     rồi làm theo các mục Ingestion/dbt/Airflow bên dưới.
   - **Chỉ muốn xem Dashboard**:
     ```bash
     cd dashboard && docker compose up -d   # không cần build lại
     ```
     mở `http://localhost:3000`.
4. DAG Airflow (`nyc_taxi_pipeline`) hiện đang **paused** (tắt sau sự cố
   backfill vượt tháng khi test GX) — bật `airflow-webserver`/
   `airflow-scheduler` lên là an toàn, DAG sẽ không tự chạy gì cho tới khi
   chủ động unpause qua UI hoặc `airflow dags unpause nyc_taxi_pipeline`.

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

## Dashboard (Metabase, Step 7)

```bash
# chạy từ dashboard/
cd dashboard
docker compose build
docker compose up -d
```

Web UI: **http://localhost:3000**. Setup lần đầu: tạo admin account, "Add a
database" → DuckDB → file path `/home/metabase/data/warehouse.duckdb`,
Advanced options bật **"Establish a read-only connection"**.

Driver DuckDB cho Metabase là driver cộng đồng
([motherduckdb/metabase_duckdb_driver](https://github.com/motherduckdb/metabase_duckdb_driver)),
không phải chính thức — vì vậy `dashboard/Dockerfile` build riêng từ base
Debian (`eclipse-temurin`) thay vì image `metabase/metabase` (Alpine, không
tương thích glibc với driver này).

**Quan trọng — khác với Airflow:** Metabase giữ connection tới
`warehouse.duckdb` **sống liên tục** (không mở-đóng nhanh như các script
ingestion/dbt/GX). Đã verify thực tế: hễ Metabase đang chạy, `dbt run`/`dbt
test`/`load_bronze.py` chạy từ host sẽ lỗi ngay
`IO Error: ... being used by another process` — không phải hiếm khi, mà là
**luôn luôn** nếu Metabase đang mở. Trước khi chạy pipeline ghi dữ liệu
(local hoặc Airflow), tắt Metabase trước:

```bash
cd dashboard && docker compose down
# ... chạy dbt/Airflow xong ...
cd dashboard && docker compose up -d
```

## Lưu ý quan trọng

- **DuckDB single-writer:** chỉ 1 tiến trình được mở/ghi `warehouse.duckdb`
  cùng lúc. Nếu gặp lỗi "file is being used by another process", tìm và đóng
  session `duckdb`/`dbt`/**Metabase** khác đang mở file này trước (hoặc mở
  `-readonly` nếu chỉ cần đọc).
- `profiles.yml` resolve đường dẫn `warehouse.duckdb` theo **thư mục đang
  đứng (cwd)** khi gọi lệnh — luôn chạy dbt/duckdb từ project root, không
  phải từ trong `transform/`.
