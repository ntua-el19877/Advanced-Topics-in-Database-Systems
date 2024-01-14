from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,count,desc,col
from pyspark.sql.types import StringType
from pythonScripts import dataframe_setup
import time
# Create a Spark session
df,spark=dataframe_setup.session_setup(4,"Query2_Df_implementation")

start_time=time.time()

def query2Group(_temp):
    if 500 <= _temp <= 1159:
        return 'Πρωί'
    elif 1200 <= _temp <= 1659:
        return 'Απόγευμα'
    elif 1700 <= _temp <= 2059:
        return 'Βράδυ'
    elif 2100 <= _temp or _temp <= 459:
        return 'Νυχτα'
    else:
        return 'Wrong time stamp'

# Register the function as a UDF
query2Group_udf = udf(query2Group, StringType())

street_df = df.filter(col("Premis Desc")=="STREET")\
    .withColumn("TIME_OCC_GROUP", query2Group_udf("TIME OCC"))

query2 = (
    street_df
    .groupBy("TIME_OCC_GROUP")
    .agg(count("*").alias("count"))
    .orderBy(desc("count"))
    )

# Show the result
query2.show()

end_time=time.time()
print(f"Execution time with 4 executors: {end_time - start_time} seconds")

spark.stop()