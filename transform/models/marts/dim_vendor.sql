{#
  Lấy toàn bộ vendor_id THẬT xuất hiện trong dữ liệu (thay vì hardcode chỉ
  1, 2 từ tài liệu TLC) rồi left join với bảng tên (seed) — nếu ngày sau TLC
  thêm vendor mới chưa kịp cập nhật seed, dim vẫn có dòng cho vendor đó
  (tên = 'Unknown') thay vì fact_trips bị mất foreign key hợp lệ.
#}

with distinct_vendors as (

    select distinct vendor_id from {{ ref('stg_trips') }}

),

vendor_names as (

    select * from {{ ref('vendor_lookup') }}

)

select
    v.vendor_id,
    coalesce(n.vendor_name, 'Unknown') as vendor_name
from distinct_vendors v
left join vendor_names n using (vendor_id)
