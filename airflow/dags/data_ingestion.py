from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
import requests

default_args = {
  'owners': 'airscholar',
  'start_date': datetime.datetime.now()
}

def fetch_data():
    

  pass

# with DAG(dag_id='data_ingestion', default_args=default_args, schedule='@hourly', catchup=False) as dag:
#   streaming_data = PythonOperator(
#     task_id='stream_data_from_api',
#     python_callable=fetch_data
#   )

#   streaming_data

fetch_data()