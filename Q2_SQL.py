import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Start measuring execution time
start_timestamp = time.time()

# Create Spark session
spark = SparkSession.builder.appName("Q2 - Crime Analysis by Time of Day SQL").getOrCreate()

# Read data (csv) into spark DataFrames
df1 = spark.read.csv("hdfs://master:9000/home/user/Data/Crime_Data_from_2010_to_2019.csv", 
                     escape = '"', header = True, inferSchema=True)
df2 = spark.read.csv("hdfs://master:9000/home/user/Data/Crime_Data_from_2020_to_Present.csv",
                     escape = '"', header = True, inferSchema=True)

# Union the DataFrames
df = df1.union(df2)

# Register the DataFrame as a temporary SQL table
df.createOrReplaceTempView("crimes")

# Define the query: categorize time of day, filter for nulls and ensure `Premis Desc` = 'STREET'
query = """
WITH categorized_times AS (
    SELECT 
        CASE
            WHEN CAST(`TIME OCC` AS INT) BETWEEN 500 AND 1159 THEN 'Πρωί'
            WHEN CAST(`TIME OCC` AS INT) BETWEEN 1200 AND 1659 THEN 'Απόγευμα'
            WHEN CAST(`TIME OCC` AS INT) BETWEEN 1700 AND 2059 THEN 'Βράδυ'
            ELSE 'Νύχτα'
        END AS time_of_day
    FROM crimes
    WHERE `TIME OCC` IS NOT NULL AND `Premis Desc` = 'STREET'
),
crime_counts AS (
    SELECT 
        time_of_day,
        COUNT(*) AS crime_total
    FROM categorized_times
    GROUP BY time_of_day
)
SELECT 
    time_of_day,
    crime_total
FROM crime_counts
ORDER BY crime_total DESC
"""

# Execute the query
result = spark.sql(query)

# Calculate the execution time
end_timestamp = time.time()
execution_time = end_timestamp - start_timestamp

# Print the execution time
print("Execution Time:", execution_time, "seconds")

# Show the result
result.show()

# Stop Spark session
spark.stop()