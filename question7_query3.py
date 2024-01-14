from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp,year, col, desc, broadcast
import time
from pythonScripts import dataframe_setup

file_path1='hdfs:///user/data/LA_income_2015.csv'
file_path2='hdfs:///user/data/revgecoding.csv'

join_hints = ["broadcast", "MERGE", "SHUFFLE_HASH", "SHUFFLE_REPLICATE_NL"]

for i in range(2,5):
    for hint in join_hints:
        df,spark=dataframe_setup.session_setup(i,"Query3_"+str(i)+"_executors_"+hint)

        df1 = spark.read.format("csv").option("header", "true")\
            .option("inferSchema", "true").load(file_path1)
        df2 = spark.read.format("csv").option("header", "true")\
            .option("inferSchema", "true").load(file_path2)

        start_time=time.time()

        # Filter year 2015
        crime_data_2015 = df.filter(year("DATE OCC") == 2015) \
            .filter(col("Vict Descent").isNotNull())

        print(f"\nApplying join hint {hint} for first join:")
        # First Join
        if hint == "broadcast":
            crime_with_zip = crime_data_2015.join(broadcast(df2), ["LAT", "LON"])
        else:
            crime_with_zip = crime_data_2015.join(df2.hint(hint), ["LAT", "LON"])
        crime_with_zip.explain()

        print(f"\nApplying join hint {hint} for second join:")
        # Second Join
        if hint == "broadcast":
            crime_with_income = crime_with_zip\
                .join(broadcast(df1.withColumnRenamed("Zip Code", "ZIPcode")), "ZIPcode")
        else:
            crime_with_income = crime_with_zip\
                .join(df1.withColumnRenamed("Zip Code", "ZIPcode").hint(hint), "ZIPcode")
        crime_with_income.explain()

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
