from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

import yfinance as yf
from datetime import datetime
from pandas_datareader import data as pdr

yf.pdr_override()

# download dataframe
data = pdr.get_data_yahoo("AAPL", start="2021-01-01", end="2021-04-30")

def __finance(self):
    return (data)

with DAG("dag_finance", start_date=datetime(2021, 1, 1),
    schedule_interval="@hourly", catchup=False) as dag:

        import_stock_data = PythonOperator(
            task_id="import_stock_data",
            python_callable = __finance
        )
        yfinance_interval = PythonOperator(
            task_id="yfinance_interval",
            python_callable=__finance
        )
        create_tables = PostgresOperator(
                   task_id="create_ddl",
                   postgres_conn_id='postgres_default',
                   sql='''
                       CREATE TABLE  fact_stock_data (
                            id int unique not null,
                            symbol varchar(5) primary key,
                            stock_name varchar(20) ,
                            price numeric(12,2),
                            timestamp TIMESTAMP
                       );              
                       '''
        )
        insert_fact = PostgresOperator(
                    task_id="insert_fact_table",
                    postgres_conn_id='postgres_default',
                    sql='''
                        CREATE TABLE  agg_data_stocks (
                            id int unique not null,
                            symbol varchar(5) primary key,
                            max_stock_price numeric(12,2),
                            min_stock_price numeric(12,2),
                            avg_stock_price numeric(12,2),
                            business_date date
                );
                    '''
        )

        insert_agg = PostgresOperator(
        task_id="insert_agg_table",
        postgres_conn_id='postgres_default',
        sql='''
            INSERT TABLE agg_data_stocks
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
            '''
        )
        import_stock_data >> yfinance_interval >> create_tables >> insert_fact >> insert_agg
