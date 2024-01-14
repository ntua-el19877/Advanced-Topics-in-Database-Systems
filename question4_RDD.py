from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,count,desc
import time
from pythonScripts import dataframe_setup

df,spark=dataframe_setup.session_setup(4,"Query2_RDD_implementation")

# Start the timer
start_time = time.time()

# Function to classify time into groups
def query2Group(time_occ):
    if 500 <= time_occ <= 1159:
        return 'Πρωί'
    elif 1200 <= time_occ <= 1659:
        return 'Απόγευμα'
    elif 1700 <= time_occ <= 2059:
        return 'Βράδυ'
    elif 2100 <= time_occ or time_occ <= 459:
        return 'Νυχτα'
    else:
        return 'Wrong time stamp'

# Convert DataFrame to RDD
rdd = df.rdd

result_rdd = rdd \
    .filter(lambda row: row['Premis Desc'] == 'STREET')\
    .map(lambda row: (query2Group(row['TIME OCC']), 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False)

results = result_rdd.collect()

# Display results
for time_group, count in results:
    print(time_group, count)

end_time=time.time()
print(f"Execution time with 4 executors: {end_time - start_time} seconds")

spark.stop()
