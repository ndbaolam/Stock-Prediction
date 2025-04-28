from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
import os
import logging

@F.udf(returnType=T.StringType())
def get_basename(path):  
  filename = os.path.basename(path)
  filename_without_ext = os.path.splitext(filename)[0]
  return filename_without_ext.split('.')[0]

class SparkApplication():
  def __init__(self):
    self.spark = SparkSession.builder \
        .appName("Stock Prediction") \
        .getOrCreate()
    
    self.schema = T.StructType([
      T.StructField("datetime", T.DateType()),
      T.StructField("open", T.DoubleType()),
      T.StructField("high", T.DoubleType()),
      T.StructField("low", T.DoubleType()),
      T.StructField("close", T.DoubleType()),
      T.StructField("volume", T.DoubleType()),
    ])      

  def read_data(self, path) -> DataFrame | None:
    """Read data from csv file

    Args:
        path (str): local filepath

    Returns:
        DataFrame:
    """
    try:
      return self.spark.read \
      .format("csv") \
      .option("header", True) \
      .option("mode", "FAILFAST") \
      .schema(self.schema) \
      .load(path)
    except Exception as e:
      logging.error(e)
  
  def process(self, df: DataFrame) -> DataFrame | None:
    """Overwrite this function to process data after reading

    Args:
        df (DataFrame): DataFrame after being read

    Returns:
        DataFrame | None: DataFrame after being processed
    """
    pass

  def write_data(self, df: DataFrame, path) -> None:
    """Write DataFrame to Parquet formatted file

    Args:
        df (DataFrame): DataFrame
        path (str): path to stored
    """
    try:
      df.write \
      .format("parquet") \
      .option("compression", "snappy") \
      .mode("overwrite") \
      .save(path)
    except Exception as e:
      logging.error(e)