from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp,year, col, desc, broadcast
import time
from pythonScripts import dataframe_setup

file_path1='hdfs:///user/data/LA_income_2015.csv'
file_path2='hdfs:///user/data/revgecoding.csv'


for i in range(2,5):
    df,spark=dataframe_setup.session_setup(i,"Query_3_" +str(i)+ "_executors_implementation")

    df1 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file_path1)
    df2 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file_path2)

    start_time=time.time()

    df2 = df2.withColumn('ZIPcode', col('ZIPcode').substr(1, 5))

    # Filter year 2015
    crime_data_2015 = df.filter(year("DATE OCC") == 2015) \
        .filter(col("Vict Descent").isNotNull())

    crime_with_zip = crime_data_2015.join(df2, (crime_data_2015["LAT"] == df2["LAT"]) & (crime_data_2015["LON"] == df2["LON"]))

    crime_with_income = crime_with_zip.join(df1.withColumnRenamed("Zip Code", "ZIPcode"), "ZIPcode")

    # Identify Top and Bottom 3 ZIP Codes by Income
    top_zip_codes = df1.orderBy(desc("Estimated Median Income")).select("Zip Code").limit(3)
    bottom_zip_codes = df1.orderBy("Estimated Median Income").select("Zip Code").limit(3)
    selected_zip_codes = top_zip_codes.union(bottom_zip_codes).distinct()

        # Convert selected_zip_codes DataFrame to a list of ZIP code values
    zip_code_list = [row['Zip Code'] for row in selected_zip_codes.collect()]

        # Analyze Crime Data by Descent
    selected_crimes = crime_with_income.filter(col("ZIPcode").isin(zip_code_list))
    query3 = selected_crimes.groupBy("Vict Descent").count().orderBy(desc("count"))

    query3.show()
    # End the timer and print execution time
    end_time = time.time()

    print("Execution time with ",str(i)," executors:", end_time - start_time, "seconds")

    spark.stop()