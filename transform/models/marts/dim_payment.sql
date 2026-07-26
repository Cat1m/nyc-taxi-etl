{#
  payment_type là enum cố định theo tài liệu NYC TLC (0-6), đã liệt kê đủ
  trong seed payment_type_lookup.csv -- không cần derive từ distinct values
  như dim_vendor vì không có rủi ro giá trị mới phát sinh ngoài enum này.
#}

select
    payment_type,
    payment_type_name
from {{ ref('payment_type_lookup') }}
