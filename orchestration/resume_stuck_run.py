"""Hỗ trợ xử lý 1 DAG run của `nyc_taxi_pipeline` bị "kẹt" giữa chừng -- kịch
bản: lỡ unpause DAG cho chạy, phát hiện rồi pause lại, nhưng 1 vài task chưa
kịp chạy xong.

Bối cảnh: khi DAG đang paused, Airflow không tự xử lý task của run đang dở
dang -- task còn lại sẽ đứng mãi ở state `scheduled`/`None`/`queued`. Cách xử
lý đúng: chạy tay từng task còn lại theo đúng thứ tự phụ thuộc bằng
`airflow tasks run -A` (bỏ qua kiểm tra dependency, vì chính kiểm tra đó đang
là cái chặn task lại).

Script này CHỈ tự động phần lặp lại (check state -> force-run task đang kẹt),
KHÔNG tự động 100%: nếu gặp 1 task đã `failed` thật (không phải do bị kẹt bởi
pause), script dừng ngay và in đường dẫn log để tự xem xét -- tránh chạy mù
tiếp task sau trên dữ liệu có thể đã sai.

Usage (chạy từ đâu cũng được, script tự cd vào orchestration/):
    python orchestration/resume_stuck_run.py --execution-date 2023-09-01T00:00:00+00:00
"""

import argparse
import subprocess
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

ORCHESTRATION_DIR = Path(__file__).resolve().parent
DAG_ID = "nyc_taxi_pipeline"

# Đúng thứ tự phụ thuộc trong DAG (xem dags/nyc_taxi_pipeline.py). Không được
# đổi thứ tự: -A bỏ qua kiểm tra dependency của Airflow, nên chính script này
# phải tự đảm bảo chạy đúng chuỗi thay cho scheduler.
TASK_ORDER = ["download", "load_bronze", "validate_bronze", "dbt_run", "dbt_test"]

# Các task có ghi vào warehouse.duckdb -- cần chắc Metabase đang tắt trước khi
# chạy, vì nó giữ connection sống liên tục và sẽ gây lỗi lock (xem docs/commands.md).
WRITES_TO_DUCKDB = {"load_bronze", "validate_bronze", "dbt_run", "dbt_test"}


def run_airflow_cli(args: list) -> tuple:
    """Chạy 1 lệnh `airflow ...` bên trong container airflow-scheduler."""
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "airflow-scheduler", "airflow", *args],
        cwd=ORCHESTRATION_DIR,
        capture_output=True,
        text=True,
    )
    return result.stdout, result.stderr


def check_metabase_not_running() -> None:
    result = subprocess.run(
        ["docker", "ps", "--format", "{{.Names}}"], capture_output=True, text=True
    )
    running = [name for name in result.stdout.strip().splitlines() if name]
    if any("metabase" in name for name in running):
        print(
            "[error] Metabase đang chạy và giữ lock warehouse.duckdb. "
            "Chạy `cd dashboard && docker compose down` trước rồi thử lại."
        )
        sys.exit(1)


def parse_task_states(execution_date: str) -> dict:
    stdout, stderr = run_airflow_cli(["tasks", "states-for-dag-run", DAG_ID, execution_date])
    lines = [line for line in stdout.splitlines() if line.strip()]
    header_idx = next((i for i, line in enumerate(lines) if "task_id" in line), None)
    if header_idx is None:
        print(f"[error] Không đọc được task state.\nstdout:\n{stdout}\nstderr:\n{stderr}")
        sys.exit(1)

    states = {}
    for line in lines[header_idx + 2:]:
        cols = [c.strip() for c in line.split("|")]
        if len(cols) >= 4 and cols[2]:
            states[cols[2]] = cols[3]
    return states


def log_path_hint(execution_date: str, task_id: str) -> Path:
    run_id = f"scheduled__{execution_date}"
    return (
        ORCHESTRATION_DIR / "logs" / f"dag_id={DAG_ID}" / f"run_id={run_id}"
        / f"task_id={task_id}" / "attempt=1.log"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--execution-date",
        required=True,
        help="Execution date của run bị kẹt, đúng định dạng Airflow hiển thị "
             "(vd 2023-09-01T00:00:00+00:00)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    execution_date = args.execution_date

    for task_id in TASK_ORDER:
        states = parse_task_states(execution_date)
        state = states.get(task_id, "unknown")

        if state == "success":
            print(f"[skip] {task_id}: đã success")
            continue

        if state == "failed":
            print(
                f"[stop] {task_id}: đã FAILED thật (không phải do bị kẹt) -- "
                f"xem log tại {log_path_hint(execution_date, task_id)} rồi tự xử lý."
            )
            sys.exit(1)

        if task_id in WRITES_TO_DUCKDB:
            check_metabase_not_running()

        print(f"[run ] {task_id}: đang ở state '{state}' -- force-run...")
        run_airflow_cli(["tasks", "run", "-A", "-l", DAG_ID, task_id, execution_date])

        new_state = parse_task_states(execution_date).get(task_id, "unknown")
        if new_state != "success":
            print(
                f"[stop] {task_id}: chạy xong nhưng state là '{new_state}' (không phải "
                f"success) -- xem log tại {log_path_hint(execution_date, task_id)} rồi tự xử lý."
            )
            sys.exit(1)
        print(f"[ok  ] {task_id}: success")

    print(f"\n=== Hoàn tất: cả {len(TASK_ORDER)} task của run {execution_date} đã success ===")


if __name__ == "__main__":
    main()
