SET mapred.job.queue.name=prod-high;

 -- DDL Create statement for the table (Note: end with semicolon)
CREATE TABLE [IF NOT EXISTS]  fact_stock_data (
id int,
symbol varchar(5),
stock_name varchar(20) ,
price numeric(12,2),
timestamp datetime
);


CREATE TABLE [IF NOT EXISTS] agg_data_stocks (
   id int ,
   symbol varchar(5) ,
   stock_name varchar(20) ,
   max_stock_price numeric(12,2),
   min_stock_price numeric(12,2),
   avg_stock_price numeric(12,2),
   business_date date
);
 
 -- Insert data into the table

INSERT OVERWRITE TABLE warehouse.agg_table PARTITION (dt='YYY-MM-DD')
SELECT
    id,
    symbol,
	stock_name,
    max(price) as max_stock_price,
	min(price) as min_stock_price,
	avg(price) as avg_stock_price
FROM
    fact_stock_data
GROUP BY
	id,
    symbol,
	stock_name;
