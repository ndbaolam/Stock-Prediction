from spark_application import SparkApplication, get_basename
from pyspark.sql import functions as F
from pyspark.sql import Window

def process(df):
    df = df.withColumn("ticket", get_basename(F.input_file_name())) \
           .withColumn("datetime", F.to_timestamp("datetime", "MM/dd/yyyy hh:mm:ss")) \
           .dropDuplicates(["ticket", "datetime"])

    window_spec = Window.partitionBy("ticket").orderBy("datetime")

    df = df.withColumn("price_change", F.col("close") - F.lag("close").over(window_spec)) \
           .withColumn("daily_return", F.col("price_change") / F.lag("close").over(window_spec)) \
           .withColumn("log_return", F.log(F.col("close") / F.lag("close").over(window_spec)))

    df = df.withColumn("VMA", F.avg("volume").over(window_spec.rowsBetween(-6, 0))) \
           .withColumn("vol_change", (F.col("volume") - F.lag("volume").over(window_spec)) / F.lag("volume").over(window_spec))

    df = df.withColumn("TR", 
            F.greatest(
                F.col("high") - F.col("low"), 
                F.abs(F.col("high") - F.lag("close").over(window_spec)), 
                F.abs(F.col("low") - F.lag("close").over(window_spec))
            ))

    df = df.withColumn("SMA", F.avg("close").over(window_spec.rowsBetween(-6, 0)))

    df = df.withColumn("delta", F.col("close") - F.lag("close").over(window_spec)) \
           .withColumn("gain", F.when(F.col("delta") > 0, F.col("delta")).otherwise(0)) \
           .withColumn("loss", F.when(F.col("delta") < 0, -F.col("delta")).otherwise(0)) \
           .withColumn("average_gain", F.avg("gain").over(window_spec.rowsBetween(-6, 0))) \
           .withColumn("average_loss", F.avg("loss").over(window_spec.rowsBetween(-6, 0))) \
           .withColumn("RS", F.col("average_gain") / F.col("average_loss")) \
           .withColumn("RSI", 100 - 100 / (F.col("RS") + 1)) \
           .drop("delta", "gain", "loss", "average_gain", "average_loss", "RS")

    df = df.withColumn("is_weekday", (F.dayofweek("datetime") >= 2) & (F.dayofweek("datetime") <= 6)) \
           .drop("datetime")

    return df

if __name__ == "__main__":
    spark = SparkApplication()
    file = "data/raw/*.csv"  

    raw_df = spark.read_data(file)
    processed_df = process(raw_df)

    tickets = [row['ticket'] for row in processed_df.select("ticket").distinct().collect()]

    for ticket in tickets:
      df_ticket = processed_df.filter(F.col("ticket") == ticket)

      total_count = df_ticket.count()
      cutoff = int(total_count * 0.9)

      df_indexed = df_ticket.rdd.zipWithIndex().toDF(["row", "idx"])
      history_df = df_indexed.filter(F.col("idx") < cutoff).select("row.*")
      realtime_df = df_indexed.filter(F.col("idx") >= cutoff).select("row.*")

      spark.write_data(df=history_df, path=f"data/processed/historic_data/{ticket}", partition_by=None)
      spark.write_data(df=realtime_df, path=f"data/processed/realtime_simulator_data/{ticket}", partition_by=None)
      
    processed_df.printSchema()          
    spark.stop()