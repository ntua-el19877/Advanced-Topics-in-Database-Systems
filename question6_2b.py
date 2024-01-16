from pyspark.sql.functions import broadcast
from math import radians, sin, cos, sqrt, atan2,inf
from pythonScripts import dataframe_setup
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, avg, count, year, format_number, desc
from pyspark.sql.types import FloatType, IntegerType
import time

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

def smallest_haversine_distance_return_code(lat1, lon1):
    smallest = float('inf')
    return_prec=0
    # Access the broadcasted stations list
    for station in broadcast_stations_code.value:
        lat2, lon2 ,prec_code = station
        _temp = haversine_distance(lat1, lon1, lat2, lon2)
        if _temp < smallest:
            return_prec=prec_code
            smallest = _temp

    return int(return_prec)

df,spark=dataframe_setup.session_setup(4,"Query4_2b_implementation")

start_time=time.time()

null_island_rows = df.filter((col("LAT") != 0.0) & (col("LON") != 0.0))

null_island_rows = null_island_rows.where(
    (null_island_rows["Weapon Used Cd"]>=100) & 
    (null_island_rows["Weapon Used Cd"]<200))

file_path1='hdfs:///user/data/LAPD_Police_Stations.csv'
file_path2='hdfs:///user/data/revgecoding.csv'

# Read the data into DataFrames with inferred schema
police_stations = spark.read.format("csv")\
        .option("header", "true").option("inferSchema", "true").load(file_path1)
revgecoding = spark.read.format("csv")\
        .option("header", "true").option("inferSchema", "true").load(file_path2)

# Convert the police stations DataFrame to a list of tuples
stations_list = [(row['Y'], row['X'],row['PREC']) for row in police_stations.collect()]

# Broadcast the list
broadcast_stations_code = spark.sparkContext.broadcast(stations_list)

custom_udf = udf(smallest_haversine_distance_return_code, IntegerType())

# Add a new column to the DataFrame using the UDF
crimes_with_closest_prec = null_island_rows.withColumn("Prec code", custom_udf("LAT", "LON"))

custom_udf = udf(haversine_distance, FloatType())

crimes_with_closest_prec_distance=crimes_with_closest_prec\
    .join(police_stations,crimes_with_closest_prec["Prec code"]==police_stations["PREC"])\
    .withColumn("Distance from prec",custom_udf("LAT","LON","Y","X"))

query4_2b = crimes_with_closest_prec_distance.groupBy("DIVISION") \
    .agg(
        format_number(avg(col("Distance from prec")), 4).alias("average_distance (km)"),
        count(col("Distance from prec")).alias("crime_count")
    )\
    .orderBy(desc("crime_count"))

query4_2b.show(query4_2b.count(), truncate=False)

end_time=time.time()
print(f"Execution time with 4 executors: {end_time - start_time} seconds")

broadcast_stations_code.unpersist()
spark.stop()
