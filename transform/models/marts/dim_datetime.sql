{#
  dim_datetime chứa MỌI timestamp xuất hiện trong stg_trips (cả pickup lẫn
  dropoff, gộp bằng UNION) — vì fact_trips cần 2 foreign key riêng
  (pickup_datetime_key, dropoff_datetime_key) trỏ vào cùng 1 dimension này,
  thay vì tạo 2 dim trùng lặp.

  Surrogate key (datetime_key) thay vì dùng thẳng timestamp làm khóa: tách
  biệt khóa kỹ thuật (dùng để join, không mang ý nghĩa nghiệp vụ) khỏi giá trị
  nghiệp vụ (full_datetime) — chuẩn thực hành star schema.
#}

with datetimes as (

    select pickup_datetime as full_datetime from {{ ref('stg_trips') }}
    union
    select dropoff_datetime as full_datetime from {{ ref('stg_trips') }}

),

distinct_datetimes as (

    select distinct full_datetime from datetimes

)

select
    {{ dbt_utils.generate_surrogate_key(['full_datetime']) }} as datetime_key,
    full_datetime,
    date_trunc('day', full_datetime)      as trip_date,
    extract(year from full_datetime)      as year,
    extract(month from full_datetime)     as month,
    extract(day from full_datetime)       as day,
    extract(hour from full_datetime)      as hour,
    extract(dow from full_datetime)       as day_of_week,
    extract(dow from full_datetime) in (0, 6) as is_weekend
from distinct_datetimes
