from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp,col
from pythonScripts import dataframe_setup
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,upper,median,format_number,udf,count,desc,year,avg
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

df,spark=dataframe_setup.session_setup(4,"Query4_1a_implementation")

start_time=time.time()

file_path1='hdfs:///user/data/LAPD_Police_Stations.csv'
file_path2='hdfs:///user/data/revgecoding.csv'

# Read the data into DataFrames with inferred schema
police_stations = spark.read.format("csv")\
        .option("header", "true").option("inferSchema", "true").load(file_path1)
revgecoding = spark.read.format("csv")\
        .option("header", "true").option("inferSchema", "true").load(file_path2)

null_island_rows = df.filter((col("LAT") != 0.0) & (col("LON") != 0.0))

# Remove non gun crimes
df_upper = null_island_rows.where(
    (null_island_rows["Weapon Used Cd"]>=100) & 
    (null_island_rows["Weapon Used Cd"]<200))

# Connect stations to crimes X= LON and Y= LAT
crimes_connected_stations=df_upper.join(police_stations, police_stations["PREC"] == df_upper["AREA "])

# Register the function as a UDF
custom_udf = udf(haversine_distance, FloatType())
# Register the function as a UDF
custom_udf = udf(haversine_distance, FloatType())

# Add a new column to the DataFrame using the UDF
crimes_connected_stations_with_distance = crimes_connected_stations\
    .withColumn("Distance from crime", custom_udf("LAT", "LON", "Y","X"))

query4_1a = crimes_connected_stations_with_distance.groupBy(year("DATE OCC").alias("Year")) \
            .agg(
                format_number(avg(col("Distance from crime")), 4).alias("average_distance (km)"),
                count(col("Distance from crime")).alias("crime_count")
            )\
            .orderBy("Year")
query4_1a.show()

end_time=time.time()
print(f"Execution time with 4 executors: {end_time - start_time} seconds")
spark.stop()