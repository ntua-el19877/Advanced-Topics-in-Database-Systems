from pythonScripts import dataframe_setup
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,format_number,udf,count,desc,year,avg,desc, broadcast
from pyspark.sql.types import FloatType
import time
from math import radians, sin, cos, sqrt, atan2,inf


def haversine_distance(lat1, lon1, lat2, lon2):
    # Radius of the Earth in kilometers
    R = 6371.0

    # Convert latitude and longitude from degrees to radians
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

    # Calculate differences in coordinates
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    # Haversine formula
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c

    return distance

file_path1='hdfs:///user/data/LAPD_Police_Stations.csv'
file_path2='hdfs:///user/data/revgecoding.csv'

join_hints = ["broadcast", "MERGE", "SHUFFLE_HASH", "SHUFFLE_REPLICATE_NL"]
for hint in join_hints:

    df,spark=dataframe_setup.session_setup(4,"Query4_1b_"+hint)

    # Read the data into DataFrames with inferred schema
    police_stations = spark.read.format("csv")\
         .option("header", "true").option("inferSchema", "true").load(file_path1)
    revgecoding = spark.read.format("csv")\
         .option("header", "true").option("inferSchema", "true").load(file_path2)

    null_island_rows = df.filter((col("LAT") != 0.0) & (col("LON") != 0.0))

    # Register the function as a UDF
    custom_udf = udf(haversine_distance, FloatType())


    start_time = time.time()
    print(f"\nApplying join hint {hint}:")
    # Apply join with different hints
    if hint == "broadcast":
        police_and_crimes = null_island_rows\
            .join(broadcast(police_stations), 
                police_stations["PREC"] == null_island_rows["AREA "])
    else:
        police_and_crimes = null_island_rows\
            .join(police_stations.hint(hint), 
                police_stations["PREC"] == null_island_rows["AREA "])

    police_and_crimes.explain()

    # Add a new column to the DataFrame using the UDF
    police_and_crimes_distance = police_and_crimes\
        .withColumn("Distance from station", custom_udf("LAT", "LON", "Y","X"))

    query4_1b = police_and_crimes_distance.groupBy("DIVISION") \
               .agg(
                    format_number(avg(col("Distance from station")), 4)\
                        .alias("average_distance (km)"),
                    count(col("Distance from station")).alias("crime_count")
               )\
               .orderBy(desc("crime_count"))

    query4_1b.show()

    end_time=time.time()
    print(f"Execution time with 4 executors: {end_time - start_time} seconds")
    spark.stop()