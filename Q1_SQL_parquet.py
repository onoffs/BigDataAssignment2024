import time
from pyspark.sql import SparkSession

# Start measuring execution time
start_timestamp = time.time()

# Create Spark session
spark = SparkSession.builder.appName("Q1 - Top 3 Crime Months per Year SQL / Parquet").getOrCreate()

# Read data (parquet) into spark Dataframes
df1 = spark.read.parquet("hdfs://master:9000/home/user/Data/Crime_Data_from_2010_to_2019.parquet")
df2 = spark.read.parquet("hdfs://master:9000/home/user/Data/Crime_Data_from_2020_to_Present.parquet")

# Union the two DataFrames
df = df1.union(df2)

# Register the DataFrame as a temporary SQL table
df.createOrReplaceTempView("crimes")

# Use SQL to extract year and month, and count crimes, handling invalid (null) dates and convert to timestamp
query = """
WITH valid_dates AS (
    SELECT 
        *,
        TO_DATE(`Date Rptd`, 'MM/dd/yyyy hh:mm:ss a') AS date_reported
    FROM crimes
    WHERE `Date Rptd` IS NOT NULL AND TO_TIMESTAMP(`Date Rptd`, 'MM/dd/yyyy hh:mm:ss a') IS NOT NULL
),
crime_counts AS (
    SELECT 
        YEAR(date_reported) AS year,
        MONTH(date_reported) AS month,
        COUNT(*) AS crime_total
    FROM valid_dates
    GROUP BY year, month
),
ranked_crimes AS (
    SELECT
        year,
        month,
        crime_total,
        RANK() OVER (PARTITION BY year ORDER BY crime_total DESC) AS ranking
    FROM crime_counts
)
SELECT year, month, crime_total, ranking
FROM ranked_crimes
WHERE ranking <= 3
ORDER BY year ASC, crime_total DESC
"""

# Execute the SQL query and get the result
top_crimes = spark.sql(query)

# Show the result
top_crimes.show()

# Calculate the execution time
end_timestamp = time.time()
execution_time = end_timestamp - start_timestamp

# Print the execution time
print("Execution Time:", execution_time, "seconds")

# Show the result
top_crimes.show()

# Stop Spark session
spark.stop()