{#
  Silver layer: ép kiểu, đổi tên cột về snake_case, và lọc CHỈ những dòng
  chắc chắn là lỗi vật lý/nhập liệu (theo docs/data_profiling_2023-01.md, đã
  double-check lại số liệu trước khi áp dụng).

  Cố ý KHÔNG lọc theo passenger_count hay payment_type/RatecodeID bất thường:
  double-check trên bronze cho thấy nhóm payment_type = 0 (71,743 dòng, NULL ở
  RatecodeID/passenger_count/congestion_surcharge) có avg_fare = 20.82 và tỷ lệ
  fare <= 0 chỉ 0.09% — tức đây là các trip HỢP LỆ, chỉ thiếu metadata do cách
  ghi nhận khác của hệ thống TLC, không phải rác. Lọc bỏ sẽ mất ~2.3% doanh thu
  thật một cách âm thầm. Các giá trị lạ này được xử lý ở tầng dim (map thành
  "Unknown") thay vì xóa dòng — giữ đúng tinh thần medallion: không đánh mất
  dữ liệu ở tầng dưới, mọi quyết định lọc phải có căn cứ profiling rõ ràng.
#}

with source as (

    select * from {{ source('bronze', 'yellow_trips') }}

),

renamed as (

    select
        year_month,
        VendorID                as vendor_id,
        tpep_pickup_datetime    as pickup_datetime,
        tpep_dropoff_datetime   as dropoff_datetime,
        passenger_count,
        trip_distance,
        RatecodeID              as ratecode_id,
        store_and_fwd_flag,
        PULocationID            as pickup_location_id,
        DOLocationID            as dropoff_location_id,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee
    from source

),

cleaned as (

    select
        *,
        date_diff('minute', pickup_datetime, dropoff_datetime) as trip_duration_minutes,
        extract(hour from pickup_datetime)                     as pickup_hour
    from renamed
    where
        -- thời lượng phải dương (loại dropoff <= pickup: 1,121 dòng)
        dropoff_datetime > pickup_datetime
        -- pickup phải nằm trong đúng tháng của partition (loại 48 dòng lẫn năm khác)
        and pickup_datetime >= strptime(year_month || '-01', '%Y-%m-%d')
        and pickup_datetime <  strptime(year_month || '-01', '%Y-%m-%d') + interval '1 month'
        -- tiền phải dương (loại 26,159 dòng fare <= 0, phần lớn là refund/void)
        and fare_amount > 0
        -- khoảng cách phải dương và trong ngưỡng hợp lý (loại 45,862 dòng = 0
        -- và các outlier tới 258,928 dặm — rõ ràng lỗi GPS)
        and trip_distance > 0
        and trip_distance < 100

)

select * from cleaned
