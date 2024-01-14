from pyspark.sql import SparkSession
import time
from pythonScripts import dataframe_setup

df,spark=dataframe_setup.session_setup(4,"Query1_SQL_implementation")

 # Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("crime_data")

start_time = time.time()
 # SQL query
query1_sql = """
SELECT Year, Month, Crime_Total, Rank
FROM (
    SELECT YEAR(`DATE OCC`) AS Year, MONTH(`DATE OCC`) AS Month, COUNT(*) AS Crime_Total,
           RANK() OVER (PARTITION BY YEAR(`DATE OCC`) ORDER BY COUNT(*) DESC) AS Rank    FROM crime_data
    GROUP BY YEAR(`DATE OCC`), MONTH(`DATE OCC`)
)
WHERE Rank <= 3
ORDER BY Year, Crime_Total DESC
"""
# Execute the SQL query
query1_result = spark.sql(query1_sql)

# Show the result
query1_result.show(query1_result.count(), truncate=False)


# End the timer and print execution time
end_time = time.time()
print(f"Execution time with 4 executors: {end_time - start_time} seconds")
spark.stop()