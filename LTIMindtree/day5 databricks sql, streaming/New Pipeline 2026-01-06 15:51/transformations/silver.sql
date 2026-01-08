CREATE OR REFRESH STREAMING TABLE dk_silver.sales_cleaned1
(
  CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW
)
AS
SELECT DISTINCT * EXCEPT (_rescued_data, ingestion_date)
FROM STREAM(dk_bronze.sales2);

CREATE OR REFRESH STREAMING TABLE dk_silver.product_scd1_1;

CREATE FLOW product_flow AS AUTO CDC INTO
  dk_silver.product_scd1_1
FROM
  stream(dk_bronze.products1)
KEYS
  (product_id)
APPLY AS DELETE WHEN
  operation = "DELETE"
APPLY AS TRUNCATE WHEN
  operation = "TRUNCATE"
SEQUENCE BY
  seqNum
COLUMNS * EXCEPT
  (operation, seqNum, _rescued_data, ingestion_date)
STORED AS
  SCD TYPE 1;

CREATE OR REFRESH STREAMING TABLE dk_silver.customers_scd2_2;

CREATE FLOW customer_flow AS AUTO CDC INTO
  dk_silver.customers_scd2_2
FROM
  stream(dk_bronze.customers1)
KEYS
  (customer_id)
APPLY AS DELETE WHEN
  operation = "DELETE"
SEQUENCE BY
  sequenceNum
COLUMNS * EXCEPT
  (operation, sequenceNum, _rescued_data, ingestion_date)
STORED AS
  SCD TYPE 2;

-- Combine sales, customer, and product tables
CREATE OR REFRESH STREAMING TABLE dk_silver.sales_customer_product_combined1 AS
SELECT
  s.order_id,
  s.customer_id,
  c.customer_name,
  c.customer_email,
  c.customer_city,
  c.customer_state,
  c.__START_AT,
  c.__END_AT,
  s.transaction_id,
  s.product_id,
  p.product_name,
  p.product_category,
  p.product_price,
  s.quantity,
  s.discount_amount,
  s.total_amount,
  s.order_date
FROM STREAM(dk_silver.sales_cleaned1) s
LEFT JOIN STREAM(dk_silver.customers_scd2_2) c
  ON s.customer_id = c.customer_id
LEFT JOIN STREAM(dk_silver.product_scd1_1) p
  ON s.product_id = p.product_id;