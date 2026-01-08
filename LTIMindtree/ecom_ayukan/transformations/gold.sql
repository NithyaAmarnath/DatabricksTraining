create materialized view ayushi_gold.customers_active as 
select * except(`__START_AT`,`__END_AT`) from lakehouse.ayushi_silver.customers_scd2 where `__END_AT` is null;