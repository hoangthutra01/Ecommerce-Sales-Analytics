with rename_column as
(select
  customer_id
  ,customer_fname as customer_name
  ,customer_state
  ,customer_country
  ,customer_city
  ,customer_segment
from `tra-lam-data.public_data_challenge.ecommerce_customer`
)

,deduplicate as
(select  distinct *
from rename_column
)

,handle_null as
(select 
 customer_id
,customer_name
,replace(replace(customer_state,"95758","CA"),"91732","CA") as customer_state
,customer_country
,customer_city
,customer_segment
from deduplicate
)

select *
from handle_null