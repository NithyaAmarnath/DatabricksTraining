create streaming table ayushi_bronze.sales as 
select * from stream read_files("abfss://raw@adlsnaval123.dfs.core.windows.net/ecom/sales")

create streaming table ayushi_bronze.products as 
select * from stream read_files("abfss://raw@adlsnaval123.dfs.core.windows.net/ecom/products")

create streaming table ayushi_bronze.customers as 
select * from stream read_files("abfss://raw@adlsnaval123.dfs.core.windows.net/ecom/customers")


create streaming table naval_bronze.sales as 
select *, current_timestamp() as ingestion_date from stream read_files("abfss://raw@adlsnaval123.dfs.core.windows.net/ecom/sales");