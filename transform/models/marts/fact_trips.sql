{#
  fact_trips: grain = 1 dòng / 1 chuyến taxi. Foreign key trỏ tới từng dim:
  pickup/dropoff_datetime_key -> dim_datetime, pickup/dropoff_location_id ->
  dim_location, vendor_id -> dim_vendor, payment_type -> dim_payment.

  trip_key là surrogate key của chính fact_trips: stg_trips không có sẵn 1
  cột nào định danh duy nhất 1 trip (NYC TLC không cung cấp trip_id), nên
  phải tự sinh từ tổ hợp các cột gần như chắc chắn là duy nhất cho 1 chuyến.
  Giới hạn: về lý thuyết 2 trip trùng y hệt vendor + pickup + dropoff + 2
  location có thể bị coi là 1 -- chấp nhận được ở quy mô portfolio, nhưng
  nếu TLC có trip_id thật thì nên dùng trực tiếp thay vì tổ hợp này.
#}

with trips as (

    select * from {{ ref('stg_trips') }}

),

pickup_dt as (

    select datetime_key as pickup_datetime_key, full_datetime
    from {{ ref('dim_datetime') }}

),

dropoff_dt as (

    select datetime_key as dropoff_datetime_key, full_datetime
    from {{ ref('dim_datetime') }}

)

select
    {{ dbt_utils.generate_surrogate_key([
        't.vendor_id', 't.pickup_datetime', 't.dropoff_datetime',
        't.pickup_location_id', 't.dropoff_location_id'
    ]) }}                            as trip_key,
    t.year_month,
    t.vendor_id,
    pdt.pickup_datetime_key,
    ddt.dropoff_datetime_key,
    t.pickup_location_id,
    t.dropoff_location_id,
    t.payment_type,
    t.passenger_count,
    t.trip_distance,
    t.trip_duration_minutes,
    t.pickup_hour,
    t.fare_amount,
    t.extra,
    t.mta_tax,
    t.tip_amount,
    t.tolls_amount,
    t.improvement_surcharge,
    t.total_amount,
    t.congestion_surcharge,
    t.airport_fee
from trips t
left join pickup_dt pdt on t.pickup_datetime = pdt.full_datetime
left join dropoff_dt ddt on t.dropoff_datetime = ddt.full_datetime
