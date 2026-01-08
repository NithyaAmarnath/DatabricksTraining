
CREATE MATERIALIZED VIEW dk_gold.customers_active1 as 
select * except (`__START_AT`,`__END_AT`) from lakehouse.dk_silver.customers_scd2_2 where `__END_AT` is null;

CREATE MATERIALIZED VIEW dk_gold.customers_inactive1 as 
select * except (`__START_AT`,`__END_AT`) from lakehouse.dk_silver.customers_scd2_2 where `__END_AT` is not null ;


create materialized view dk_gold.top3_customer1 as 
SELECT
  c.customer_id,
  c.customer_name,
  c.customer_email,
  c.customer_city,
  c.customer_state,
  ROUND(SUM(s.total_amount)) AS total_amount
FROM
  lakehouse.dk_silver.sales_cleaned1 s
JOIN
  lakehouse.dk_gold.customers_active1 c
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