

CREATE OR REFRESH STREAMING TABLE naval_silver.sales_cleaned
(
  CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW
)
AS
SELECT DISTINCT * EXCEPT (_rescued_data, ingestion_date)
FROM STREAM(naval_bronze.sales);



-- Create and populate the target table.
CREATE OR REFRESH STREAMING TABLE naval_silver.product_scd1;

CREATE FLOW product_flow AS AUTO CDC INTO
  naval_silver.product_scd1
FROM
  stream(naval_bronze.products)
KEYS
  (product_id)
APPLY AS DELETE WHEN
  operation = "DELETE"
APPLY AS TRUNCATE WHEN
  operation = "TRUNCATE"
SEQUENCE BY
  seqNum
COLUMNS * EXCEPT
  (operation, seqNum,_rescued_data,ingestion_date)
STORED AS
  SCD TYPE 1;


CREATE OR REFRESH STREAMING TABLE naval_silver.customers_scd2;

CREATE FLOW customer_flow AS AUTO CDC INTO
  naval_silver.customers_scd2
FROM
  stream(naval_bronze.customers)
KEYS
  (customer_id)
APPLY AS DELETE WHEN
  operation = "DELETE"
SEQUENCE BY
  sequenceNum
COLUMNS * EXCEPT
  (operation, sequenceNum,_rescued_data,ingestion_date)
STORED AS
  SCD TYPE 2;