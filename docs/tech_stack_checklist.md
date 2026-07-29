# Checklist: chọn tech stack trước khi xây pipeline

Ghi chú học tập cá nhân — không mô tả cách vận hành project `nyc-taxi-etl`
(xem `commands.md`/`data_profiling_2023-01.md` cho việc đó). File này là bộ
câu hỏi tổng quát nên tự hỏi (và hỏi lại sếp/stakeholder) *trước khi* chọn
compute engine, warehouse, orchestrator... cho bất kỳ pipeline nào.

Nguyên tắc gốc: **độ phức tạp thừa cũng là nợ kỹ thuật, không khác gì thiếu.**
Câu hỏi không phải "công cụ nào mạnh nhất", mà là "công cụ nào là mức tối
thiểu đủ để giải đúng bài toán". Giống việc không ai dùng Bloc để viết 1 bộ
đếm tăng/giảm ở Flutter (Cubit là đủ) — không nên mặc định chọn Spark/BigQuery
chỉ vì chúng "chuẩn enterprise" trong khi dữ liệu chỉ vài GB/tháng.

Thứ tự hỏi: **dữ liệu → người dùng → ràng buộc tổ chức → SLA → chi phí →
cuối cùng mới tới công cụ cụ thể.**

## 1. Dữ liệu (data shape)

- Volume: bao nhiêu dòng/GB mỗi lần chạy? Tăng trưởng dự kiến theo tháng/năm?
  (dữ liệu hôm nay nhỏ nhưng năm sau x10 thì stack chọn khác)
- Velocity: batch (hàng ngày/hàng giờ) hay cần near-real-time/streaming?
- Variety: bao nhiêu nguồn khác nhau, định dạng gì (Parquet, CSV, API, CDC)?
- Con số này đã được **đo thật** chưa, hay đang đoán? Đừng thiết kế theo
  tưởng tượng — luôn ưu tiên profiling dữ liệu thật trước khi quyết định.

## 2. Ai dùng, dùng để làm gì (consumers)

- Ai đọc output cuối: 1-2 analyst nội bộ, hay hàng trăm người dùng dashboard
  cùng lúc? (quyết định mức concurrency cần)
- Output cần độ trễ (freshness) tới đâu: "sáng mai có là được" hay "trễ 5
  phút là mất tiền"?
- Có cần nhiều team/phòng ban cùng truy cập, cần phân quyền chi tiết
  (governance/IAM), hay chỉ 1 team nhỏ dùng?

## 3. Ràng buộc tổ chức (org constraints)

Phần hay bị bỏ qua nhất khi chỉ nhìn từ góc độ kỹ thuật thuần túy:

- Ngân sách: có budget cloud hàng tháng, hay phải tối ưu chi phí gần 0?
- Team hiện tại biết gì: ép 1 stack lạ vào team chỉ biết SQL là tự tạo
  bottleneck tuyển dụng/onboarding.
- Ai maintain sau khi mình rời dự án? (stack quá mới/quá lạ mà chỉ 1 người
  hiểu là rủi ro bàn giao)
- Compliance/bảo mật: dữ liệu có nhạy cảm (PII, tài chính, y tế) cần ràng
  buộc lưu ở đâu, ai được xem?

## 4. SLA & độ tin cậy

- Nếu pipeline lỗi/chậm 1 ngày, hậu quả là gì — "không ai để ý" hay "báo cáo
  lên ban giám đốc trễ"?
- Cần retry/alerting tới mức nào? (quyết định có cần orchestrator mạnh như
  Airflow, hay 1 cron job đơn giản là đủ)

## 5. Chi phí vận hành thực tế

Không chỉ chi phí công cụ (license/cloud bill), mà cả:

- Chi phí compute: theo giờ chạy, hay theo byte quét (vd BigQuery tính phí
  theo dữ liệu scan — 1 câu SQL viết ẩu có thể tốn tiền thật).
- Chi phí vận hành/maintain: cluster Spark cần người canh, tune liên tục;
  DuckDB gần như 0 ops.
- Chi phí cơ hội: xây phức tạp hơn cần thiết hôm nay = tốn thời gian lẽ ra
  dùng để ship tính năng khác.

## 6. Câu hỏi công cụ cụ thể (chỉ hỏi sau cùng)

Chỉ sau khi trả lời xong 5 mục trên, mới hỏi: compute engine nào, warehouse
nào, orchestrator nào phù hợp đúng với những con số/ràng buộc vừa liệt kê —
không hỏi ngược lại.

Ví dụ áp dụng 3 trục quyết định "có cần distributed compute (Spark) không":

1. Dữ liệu có nằm vừa trong 1 máy không? (máy hiện đại có thể có 1-2TB RAM,
   hàng chục TB NVMe — "1 máy" bây giờ nghĩa là hàng trăm GB tới vài TB, không
   chỉ vài bảng nhỏ)
2. Có nhiều người/tiến trình cần ghi cùng lúc không (concurrency)? Đây là chỗ
   engine single-writer kiểu DuckDB yếu — cần warehouse quản lý (BigQuery,
   Snowflake) hoặc Spark khi có nhiều pipeline/analyst ghi cùng lúc.
3. Có cần chịu lỗi khi job chạy rất lâu, trên nhiều máy không? Job dài trên
   50 máy, 1 máy chết giữa chừng — Spark tự retry phần đó; single-machine
   engine thì máy chết là job chết, chạy lại từ đầu.

Quyết định KHÔNG phải "công ty nhỏ hay lớn" — 1 công ty tỷ đô vẫn có thể có
pipeline nội bộ vài GB/tháng dùng DuckDB ngon lành, còn 1 startup nhỏ xử lý
clickstream có thể đã cần Spark vì volume quá lớn dù công ty chỉ 10 người.

## Câu hỏi tổng kết để trình sếp

> "Với volume X, tốc độ tăng trưởng Y, N người dùng cần truy cập đồng thời,
> ngân sách Z, và team hiện có kỹ năng W — em chọn stack A vì nó đáp ứng đúng
> mức cần, không thừa không thiếu. Nếu volume/velocity tăng gấp K lần trong M
> tháng tới, điểm cần đánh giá lại là [chỗ cụ thể sẽ nghẽn trước]."

## Ghi nhớ

Bức tranh thực tế 2026 là **hybrid**, không phải chọn 1 công cụ cho cả sự
nghiệp: phần lớn công ty dùng cloud warehouse quản lý (BigQuery, Snowflake,
Databricks SQL) cho phần lớn khối lượng, transform vẫn viết bằng SQL qua dbt;
Spark/Databricks dành cho phần thực sự cần distributed (chục-trăm TB, ML
feature engineering, streaming); DuckDB/single-node đang nổi lên cho dev/test
nhanh, công cụ nội bộ, và cả production ở quy mô vừa. Hiểu rõ *khi nào* cần
đổi engine — thay vì mặc định chọn công cụ "nghe sang nhất" — là dấu hiệu
senior hơn junior.
