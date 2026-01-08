create streaming table dk_bronze.sales2 as 
select *, current_timestamp() as ingestion_date from stream read_files("abfss://raw@adlsnaval123.dfs.core.windows.net/ecom/sales");


create streaming table dk_bronze.products1 as 
select *, current_timestamp() as ingestion_date from stream read_files("abfss://raw@adlsnaval123.dfs.core.windows.net/ecom/products");

create streaming table dk_bronze.customers1 as 
select *, current_timestamp() as ingestion_date from stream read_files("abfss://raw@adlsnaval123.dfs.core.windows.net/ecom/customers");