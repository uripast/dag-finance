# dag-finance
The project had this steps done:
Using docker compose i ran both airflow and postgres.
I have added connection to the dag for the postgres env.
My first task was to create function that will return data from yahoo finance (stock name, price, timestamp).
My second task was to manage the dynamic variables that i can manage the list of stocks for example
I chose to work with csv files because they are easy to handle format wise
I added the yfinance library to collect data each 1 minute
I have scheduled the dag to work on a hourly base using cron 0 * * * *
Created two tables one for the stocks to insert the data from the stream as a fact table and the second was aggregation table using max,min,avg functions to get hourly data regarding the stocks
I used modular design becuase i seperated the the functions that are taking data from yahoo finance apart from the dynamic handling and i used different module for collecting data stream and another module that insert the data to the postgres db.
