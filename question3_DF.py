from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, col, desc, rank
from pyspark.sql.window import Window
import time
from pythonScripts import dataframe_setup

df,spark=dataframe_setup.session_setup(4,"Query1_DF_implementation")

 # Start the timer
start_time = time.time()

  # Group by year and month, and count the crimes
crime_counts = df.groupBy(year("DATE OCC").alias("Year"), month("DATE OCC").alias("Month")) \
                                             .count() \
                                             .withColumnRenamed("count", "Crime Total")

  # Define a window spec partitioned by year and ordered by crime total (descending) 
windowSpec = Window.partitionBy("Year").orderBy(desc("Crime Total"))

   # Apply the window spec to rank the months within each year
crime_counts = crime_counts.withColumn("Rank", rank().over(windowSpec))

 # Filter for top 3 months for each year
top_months = crime_counts.filter(col("Rank") <= 3)
  # Order by year and crime total
query1 = top_months.orderBy("Year", desc("Crime Total"))

# Show the result
query1.show(df.count(), truncate=False)


 # End the timer and print execution time
end_time = time.time()
print(f"Execution time with 4 executors: {end_time - start_time} seconds")
spark.stop()