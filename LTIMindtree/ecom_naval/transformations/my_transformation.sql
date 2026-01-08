
--create streaming table or materalized view or view

--create  table naval_bronze.sales as 
--select * from  read_files("abfss://raw@adlsnaval123.dfs.core.windows.net/ecom/sales");


create streaming table naval_bronze.sales as 
select *, current_timestamp() as ingestion_date from stream read_files("abfss://raw@adlsnaval123.dfs.core.windows.net/ecom/sales");


create streaming table naval_bronze.products as 
select *, current_timestamp() as ingestion_date from stream read_files("abfss://raw@adlsnaval123.dfs.core.windows.net/ecom/products");

create streaming table naval_bronze.customers as 
select *, current_timestamp() as ingestion_date from stream read_files("abfss://raw@adlsnaval123.dfs.core.windows.net/ecom/customers");
