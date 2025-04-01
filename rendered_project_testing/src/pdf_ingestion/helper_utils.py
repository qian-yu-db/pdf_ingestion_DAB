from pyspark.sql import SparkSession

def get_dbutils():
  spark = SparkSession.builder.getOrCreate()
  if "local" not in spark.sparkContext.master:
    try:
      import IPython
      return IPython.get_ipython().user_ns["dbutils"]
    except Exception as ex:
      from pyspark.dbutils import DBUtils
      return DBUtils(spark)
  return None
