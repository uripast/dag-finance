# dag-finance
- The project had this steps done:
- Using docker compose i ran both airflow and postgres.
- I have added connection to the dag for the postgres env.
- I have organized all the tasks inside one dag file finance_dag.py
- I chose to work with csv files because they are easy to handle format wise.
- I added the yfinance library to collect data each 1 minute using yfinance.py
- I have scheduled the dag to work on a hourly base using cron 0 * * * *
- Created two tables one for the stocks data from the stream as a fact table and the second was aggregation table using max,min,avg functions to get hourly data regarding the stocks using indexes on the id and the symbol.
- I used modular design becuase i have seperated the functions that are taking data from yahoo finance apart from the dynamic handling and used different module for collecting data stream and another module to insert data to postgres db. 
