CREATE OR REFRESH STREAMING TABLE ayushi_silver.sales_cleaned
(
  CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW
)
AS
SELECT DISTINCT * EXCEPT (_rescued_data)
FROM STREAM(ayushi_bronze.sales);