# STOCK PREDICTION

## Overview
- This is my GR1 project at HUST (Hanoi University of Science and Technologies). This project is about big data ingestion, processing and analysis (just for learning purpose)

## Architecture
<img src="./images/architecture.png"/>

- Follow the Lambda Architecture

## Techonologies
- Minio for Object Storage
- Apache Spark, Spark Streaming for data processing
- Apache Kafka for real-time data ingestion
- Apache Airflow for job scheduling

## Prerequisites
- Python, Docker, Docker Compose, Ubuntu

## Dataset
- Download the dataset and save into the `data/raw` directory 
- This dataset belong to [Oleg Shpagin](https://www.kaggle.com/olegshpagin) from [Kaggle](https://www.kaggle.com/)
### USA 514 Stocks Prices NASDAQ NYSE

The USA 514 Stocks Prices Dataset provides historical Open, High, Low, Close, and Volume (OHLCV) prices of 514 stocks traded in the United States financial markets NASDAQ NYSE. You can use price movements and trading volumes for stock price predictions.

### ~11 Gb of market data for you and your analysis with NN or other methods 

Here is the link to kaggle dataset [USA 514 Stocks Prices NASDAQ NYSE](https://www.kaggle.com/datasets/olegshpagin/usa-stocks-prices-ohlcv) (weekly updates)

#### 4626 CSV files for MN1, W1, D1, H4, H1, M30, M15, M10 and M5 timeframes

## To run this project
- Create python virtual enviroment
```sh
python3 -m venv venv
source venv/bin/activate

pip install -r requirements.txt
```

- Run Docker containers
```sh
docker network create stock_default
```

## Port using on Localhost
- Minio: 9000, 9001
- Trino: 8081
- Superset: 8088
- Airflow Web Server: 8080
- Broker: 9092, 9101
- Control Center: 9021
- Flower: 5555

## Trino 474
```sh
docker compose -f serving/trino/docker-compose.yaml up -d

docker exec -it trino bash

trino --server localhost:8080 --catalog stock
```
- Access [Minio UI](http://localhost:9001)

- To upload file to Minio:
```sh
python utils/upload_to_minio.py <folder_path>
```

```sql
CREATE SCHEMA IF NOT EXISTS <schema_name>
WITH (location = 's3://<bucket_name>/');

CREATE TABLE IF NOT EXISTS stock.<schema_name>.<table_name> (
  formatted_timestamp TIMESTAMP,
  open DOUBLE,
  high DOUBLE,
  low DOUBLE,
  close DOUBLE,
  volume DOUBLE,
  ticket_name VARCHAR
)
WITH (
  format = 'PARQUET',
  external_location = 's3://<bucket_name>/'
);
```

## SuperSet
```sh
docker compose -f serving/superset/docker-compose.yaml up -d
```
- Access [Minio UI](http://localhost:8088)
