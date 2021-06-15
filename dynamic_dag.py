from airflow import DAG

import re
import os
from datetime import datetime, timedelta
from dynamic_query_config import config

'''
To Do:
'''

default_args = {
    'owner': 'name',
    'email': ['data@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2, #Total 3 attempts

}


user_defined_macros.update(local_macros)

def send_task_success(ds, **kwargs):
    print(ds)
    return 'Any custom operation, when this task completes'

# Read list of all the configs


for i,config in enumerate(config):

    source = config["source"]

    # Dag Id should not have any non alphanumeric chars
    dag_id = '{}_{}'.format(PREFIX, config["name"])

    dag = DAG(dag_id=dag_id,
              default_args=default_args,
              user_defined_macros=user_defined_macros,
              schedule_interval=source["schedule_interval"],
              start_date=datetime.strptime(source["start_date"], '%Y-%m-%d')
              )

       

    all_tasks.set_upstream(run_hive_query)


    #So that mulitple dags could be created
    # https://airflow.incubator.apache.org/faq.html#how-can-i-create-dags-dynamically
    globals()[dag_id] = dag
