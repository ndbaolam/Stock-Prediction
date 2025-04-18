from airflow import DAG
from datetime import datetime, timedelta
import pandas as pd
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging
import glob
import json
import time

default_args = {
  'owner': 'stock-airflow',
  'start_date': datetime(2025, 4, 1),
  'retries': 1,
  'email_on_failure': True,
  'email_on_retry': True,
  'retry_delay': timedelta(minutes=5)
}

def stream_data():
  logging.basicConfig(level=logging.INFO)

  # Connect to Kafka
  try:
    producer = KafkaProducer(
      bootstrap_servers=['localhost:9092'],
      value_serializer=lambda v: json.dumps(v).encode('utf-8'),
      max_block_ms=5000
    )
  except KafkaError as e:
    logging.error(f"Kafka connection error: {e}")
    return

  # Read all CSVs from the folder
  files = glob.glob("../../data/realtime_simulator/*.csv")
  if not files:
    logging.warning("No CSV files found.")
    return

  for file in files:    
    df = pd.read_csv(file)
    for _, row in df.iterrows():
      data = row.to_dict()
      try:
        # Send message to Kafka topic
        producer.send(topic='stock_stream', value=data)
        logging.info(f"Sent: {data}")        
        time.sleep(0.5)
      except KafkaError as e:
        logging.error(f"Failed to send: {e}")

  producer.flush()
  producer.close()

with DAG(
  dag_id='spark_processor',
  default_args=default_args,
  schedule_interval='@daily',
  catchup=False # backfill
) as dag:
    
  stream_data_task = PythonOperator(
    task_id="stream_data_from_file",
    python_callable=stream_data
  )  
