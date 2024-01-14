from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp

# Create a Spark session
spark = SparkSession.builder.appName("DATAFRAME").getOrCreate()

# Define the date-time format
datetime_format = "MM/dd/yyyy hh:mm:ss a"

# HDFS paths to the data files
file_path1 = 'hdfs:///user/data/crime-data-from-2010-to-2019.csv'
file_path2 = 'hdfs:///user/data/crime-data-from-2020-to-present.csv'

# Read the data into DataFrames with inferred schema
df1 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file_path1)
df2 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file_path2)

# Convert the date-time strings to TimestampType and then to DateType
df1 = df1.withColumn("Date Rptd", to_timestamp("Date Rptd", datetime_format).cast("date")).withColumn("DATE OCC", to_timestamp("DATE OCC", datetime_format).cast("date"))   

df2 = df2.withColumn("Date Rptd", to_timestamp("Date Rptd", datetime_format).cast("date")).withColumn("DATE OCC", to_timestamp("DATE OCC", datetime_format).cast("date"))   

# Union the two DataFrames
df = df1.union(df2)

row_count = df.count()
print(f"Number of rows in the DataFrame: {row_count}")

for column, dtype in df.dtypes:
        print(f"{column}, {dtype}")

spark.stop()