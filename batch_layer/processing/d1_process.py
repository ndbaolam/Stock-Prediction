from spark_application import SparkApplication, get_basename
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window

if __name__ == "__main__":
  spark = SparkApplication()
  file = "data/raw/archive/D1/*.csv"  

  df = spark.read_data(file)

  df = df.withColumn("ticket_name", get_basename(F.input_file_name()))
  window_spec = Window.partitionBy(F.col("ticket_name")).orderBy(F.col("ticket_name"))

  df = df.withColumn("price_change", F.col("close") - F.lag("close").over(window_spec)) \
    .withColumn("daily_return", F.col("price_change") / F.lag("close").over(window_spec)) \
    .withColumn("log_return", F.log(F.col("close") / F.lag("close").over(window_spec)))

  df = df.withColumn("VMA", F.avg("volume").over(window_spec.rowsBetween(-2, 0))) \
    .withColumn("vol_change", (F.col("volume") - F.lag("volume").over(window_spec)) / F.lag("volume").over(window_spec))
  
  df = df.withColumn("TR", 
    F.greatest(
      F.col("high") - F.col("low"), 
      F.abs(F.col('high') - F.lag('close').over(window_spec)), 
      F.abs(F.col('low') - F.lag('close').over(window_spec))
    ))
  
  df = df.withColumn("SMA", F.avg(F.col("close")).over(window_spec.rowsBetween(-2, 0))) # Last 3 days

  df = df.withColumn("delta", F.col("close") - F.lag(col="close").over(window_spec)) \
    .withColumn("gain", F.when(F.col("delta") > 0, F.col("delta")).otherwise(0)) \
    .withColumn("loss", F.when(F.col("delta") < 0, -F.col("delta")).otherwise(0)) \
    .withColumn("average_gain", F.avg("gain").over(window_spec.rowsBetween(-3, 0))) \
    .withColumn("average_loss", F.avg("loss").over(window_spec.rowsBetween(-3, 0))) \
    .withColumn("RS", F.col("average_gain") / F.col("average_loss")) \
    .withColumn("RSI", 100 - 100/(F.col("RS") + 1)) \
    .drop("delta", "gain", "loss", "average_gain", "average_loss", "RS")  #RSI over 3 days
  
  df = df.withColumn("formatted_timestamp", F.to_timestamp("datetime", "MM/dd/yyyy hh:mm:ss")) \
    .withColumn("is_weekday", (F.dayofweek("datetime") >=2) & (F.dayofweek("datetime") <= 6)) \
    .drop("datetime")

  spark.write_data(df=df, path="data/processed/D1/")