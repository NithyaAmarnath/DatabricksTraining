
create materialized view naval_gold.customers_active as 
select * except(`__START_AT`,`__END_AT`) from lakehouse.naval_silver.customers_scd2 where `__END_AT` is null;



create view naval_gold.customers_inactive as 
select * except(`__START_AT`,`__END_AT`)  from lakehouse.naval_silver.customers_scd2 where `__END_AT` is not null;


create materialized view naval_gold.top3_customer as 
SELECT
  c.customer_id,
  c.customer_name,
  c.customer_email,
  c.customer_city,
  c.customer_state,
  ROUND(SUM(s.total_amount)) AS total_amount
FROM
  lakehouse.naval_silver.sales_cleaned s
JOIN
  lakehouse.naval_gold.customers_active c
ON
  s.customer_id = c.customer_id
GROUP BY
  c.customer_id,
  c.customer_name,
  c.customer_email,
  c.customer_city,
  c.customer_state
ORDER BY
  total_amount DESC
LIMIT 3
