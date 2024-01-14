#---------------------------------------- dataframe_setup.py -----------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp

def session_setup(executors,MyappName):
    # Create a Spark session
    spark = SparkSession.builder \
        .appName(MyappName) \
        .config("spark.executor.instances", str(executors)) \
        .getOrCreate()

    # Define the date-time format
    datetime_format = "MM/dd/yyyy hh:mm:ss a"

    file_path1 = 'hdfs:///user/data/crime-data-from-2010-to-2019.csv'
    file_path2 = 'hdfs:///user/data/crime-data-from-2020-to-present.csv'

    df1 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file_path1)
    df2 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file_path2)

    # Convert the date-time strings to TimestampType and then to DateType
    df1 = df1.withColumn("Date Rptd", to_timestamp("Date Rptd", datetime_format).cast("date"))\
            .withColumn("DATE OCC", to_timestamp("DATE OCC", datetime_format).cast("date"))
    df2 = df2.withColumn("Date Rptd", to_timestamp("Date Rptd", datetime_format).cast("date"))\
            .withColumn("DATE OCC", to_timestamp("DATE OCC", datetime_format).cast("date"))

    return (df1.union(df2),spark)