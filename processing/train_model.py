from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StandardScaler

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Train Pipeline Linear Regression") \
        .getOrCreate()
    
    df = spark.read.parquet("data/processed/historic_data/*")
    df = df.dropna(subset=["close"])
    
    tickets = [row["ticket"] for row in df.select("ticket").distinct().collect()]
    
    feature_cols = [
        "open", "high", "low", "volume",
        "price_change", "daily_return", "log_return",
        "VMA", "vol_change", "TR", "SMA", "RSI"
    ]

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")
    scaler = StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True)
    lr = LinearRegression(featuresCol="features", labelCol="close")

    pipeline = Pipeline(stages=[assembler, scaler, lr])
    
    evaluators = {
      "RMSE": RegressionEvaluator(labelCol="close", predictionCol="prediction", metricName="rmse"),
      "MAE": RegressionEvaluator(labelCol="close", predictionCol="prediction", metricName="mae"),
      "R2": RegressionEvaluator(labelCol="close", predictionCol="prediction", metricName="r2")
    }
    
    for ticket in tickets:
        if ticket != "AAPL":
            continue
          
        print("\n", 10 * "=", f"Ticket: {ticket}", 10 * "=")
        ticket_df = df.filter(F.col("ticket") == ticket).dropna(subset=feature_cols)

        if ticket_df.count() < 30:            
            continue

        train_data, test_data = ticket_df.randomSplit([0.7, 0.3], seed=42)
        print(f"Train: {train_data.count()} rows, Test: {test_data.count()} rows")

        model = pipeline.fit(train_data)        
        predictions = model.transform(test_data)
        
        for metric, evaluator in evaluators.items():
          score = evaluator.evaluate(predictions)
          print(f"{metric}: {score:.4f}")

        
        model.write().overwrite().save(f"models/{ticket}_lr_pipeline")
