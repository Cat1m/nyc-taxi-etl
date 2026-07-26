{#
  Dùng thẳng LocationID làm khóa (natural key) thay vì tạo surrogate key mới:
  LocationID đã là mã định danh ổn định, duy nhất do chính NYC TLC quản lý —
  không có lý do để "phát minh lại" một khóa kỹ thuật khác. Surrogate key chỉ
  cần thiết khi nguồn không có sẵn khóa đáng tin cậy (như dim_datetime).
#}

select
    LocationID  as location_id,
    Borough     as borough,
    Zone        as zone,
    service_zone
from {{ source('bronze', 'taxi_zone_lookup') }}
