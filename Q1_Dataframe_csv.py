import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, rank, to_timestamp
from pyspark.sql.window import Window

# Start measuring execution time
start_timestamp = time.time()

# Create Spark session
spark = SparkSession.builder.appName("Q1 - Top 3 Crime Months per Year - Dataframe / CSV").getOrCreate()

# Read data (csv) into spark Dataframes
df1 = spark.read.csv("hdfs://master:9000/home/user/Data/Crime_Data_from_2010_to_2019.csv", 
                     header=True, inferSchema=True)
df2 = spark.read.csv("hdfs://master:9000/home/user/Data/Crime_Data_from_2020_to_Present.csv", 
                     header=True, inferSchema=True)

# Union the two Dataframes
df = df1.union(df2)

# Filter out invalid (null) dates and convert to timestamp 
df = df.filter(df["Date Rptd"].isNotNull())
df = df.withColumn("date_reported", to_timestamp(col("Date Rptd"), "MM/dd/yyyy hh:mm:ss a"))

# Extract year and month
df = df.withColumn("year", year("date_reported")).withColumn("month", month("date_reported"))

# Count crimes per year and month
crime_counts = df.groupBy("year", "month").count().withColumnRenamed("count", "crime_total")

# Rank months within each year
window_spec = Window.partitionBy("year").orderBy(col("crime_total").desc())
ranked_crimes = crime_counts.withColumn("ranking", rank().over(window_spec))

# Filter top 3 months per year
top_crimes = ranked_crimes.filter(col("ranking") <= 3).orderBy("year", col("crime_total").desc())

# Calculate the execution time
end_timestamp = time.time()
execution_time = end_timestamp - start_timestamp

# Print the execution time
print("Execution Time:", execution_time, "seconds")

# Show the result
top_crimes.show()

# Stop Spark session
spark.stop()