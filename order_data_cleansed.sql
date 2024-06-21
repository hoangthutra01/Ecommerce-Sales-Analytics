with cast_type as
(select 
  order_id
  ,customer_id
  ,parse_date("%d/%m/%Y",replace(order_date,"-","/")) as order_date
  ,market
  ,category_name
  ,order_region
  ,order_quantity
  ,round(cast(product_price as numeric),2) as product_price
  ,round(cast(profit_per_order as numeric),2) as profit_per_order
  ,round(cast(profit_margin as numeric),2) as profit_margin
from `tra-lam-data.public_data_challenge.ecommerce_order`
)

,concat_column as
(select 
  *
  ,concat(category_name,order_id,order_date) as concated_col
from cast_type
)

,rank_row as
(select 
  *
  ,row_number() over (partition by concated_col order by order_quantity desc) as rank_no
from concat_column
)

,deduplicate as
(select *
from rank_row
where rank_no = 1
)

,enrich_data as
(select 
  order_id
  ,customer_id
  ,order_date
  ,market
  ,category_name
  ,order_region
  ,order_quantity
  ,product_price
  ,order_quantity*product_price as revenue
  ,product_price*order_quantity-profit_per_order as COGS
  ,profit_per_order
  ,profit_margin
from deduplicate
)

select *
from enrich_data