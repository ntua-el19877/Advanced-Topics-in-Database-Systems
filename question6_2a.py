from pyspark.sql.functions import broadcast
from pythonScripts import dataframe_setup
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, avg, count, year, format_number
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

def smallest_haversine_distance(lat1, lon1):
    smallest = float('inf')
    # Access the broadcasted stations list
    for station in broadcast_stations.value:
        lat2, lon2 = station
        _temp = haversine_distance(lat1, lon1, lat2, lon2)
        if _temp < smallest:
            smallest = _temp
    return smallest

df,spark=dataframe_setup.session_setup(4,"Query4_2a_implementation")

start_time=time.time()

null_island_rows = df.filter((col("LAT") != 0.0) & (col("LON") != 0.0))

file_path1='hdfs:///user/data/LAPD_Police_Stations.csv'
file_path2='hdfs:///user/data/revgecoding.csv'

# Read the data into DataFrames with inferred schema
police_stations = spark.read.format("csv")\
        .option("header", "true").option("inferSchema", "true").load(file_path1)
revgecoding = spark.read.format("csv")\
        .option("header", "true").option("inferSchema", "true").load(file_path2)

# Convert the police stations DataFrame to a list of tuples
stations_list = [(row['Y'], row['X']) for row in police_stations.collect()]

broadcast_stations = spark.sparkContext.broadcast(stations_list)

df_upper = null_island_rows\
    .where((null_island_rows["Weapon Used Cd"] >= 100) & (null_island_rows["Weapon Used Cd"] < 200))
custom_udf = udf(smallest_haversine_distance, FloatType())

# Add a new column to the DataFrame using the UDF
crimes_connected_closest_station = df_upper.withColumn("Distance from crime", custom_udf("LAT", "LON"))

query4_2a = crimes_connected_closest_station.groupBy(year("DATE OCC").alias("Year")) \
            .agg(
                format_number(avg(col("Distance from crime")), 4).alias("average_distance (km)"),
                count(col("Distance from crime")).alias("crime_count")
            )\
            .orderBy("Year")

query4_2a.show()

end_time=time.time()
print(f"Execution time with 4 executors: {end_time - start_time} seconds")

broadcast_stations.unpersist()
spark.stop()
              