import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, udf, avg, count, round 
from pyspark.sql.types import DoubleType
import geopy.distance

# Start measuring execution time
start_timestamp = time.time()

# Function given from exercise to calculate distance between two points 
def get_distance(lat1, lon1, lat2, lon2):
    return geopy.distance.geodesic((lat1, lon1), (lat2, lon2)).km

# Register User Defined Function (UDF) to be able to use get_distance and define the result as double
get_distance_udf = udf(get_distance, DoubleType())

# Create Spark Session
spark = SparkSession.builder.appName("Q4 - Crime Analysis by Police Station, Distance from it and Weapons used - Dataframe").getOrCreate()

# Read data (csv) into spark Dataframes
crime1 = spark.read.csv("hdfs://master:9000/home/user/Data/Crime_Data_from_2010_to_2019.csv", header=True, inferSchema=True)
crime2 = spark.read.csv("hdfs://master:9000/home/user/Data/Crime_Data_from_2020_to_Present.csv", header=True, inferSchema=True)
crime = crime1.union(crime2) 

police_stations = spark.read.csv("hdfs://master:9000/home/user/Data/LAPD_Police_Stations.csv", header=True, inferSchema=True)

# Filter crime data for 'Weapon Used Cd' starting with '1' (1xx)
crime_with_guns = crime.filter(col('Weapon Used Cd').startswith('1'))
crime_with_guns = crime_with_guns.filter((crime_with_guns['LAT'] != '0') | (crime_with_guns['LON'] != '0'))

# Join police stations and crime with guns on columns with the same info (code of police department)
crime_joined_police = crime_with_guns.join(police_stations, crime_with_guns['AREA '] == police_stations["PREC"], "inner")

# Use get_distance to calculate distances and add the result as new column
crime_joined_police = crime_joined_police.withColumn('distance', get_distance_udf(col('LAT'), col('LON'), col('Y'), col('X')))

# Group by division, count total incidents, and calculate average distance (round by 3 decimals)
result_df = crime_joined_police.groupby('division').agg(count('*').alias('incidents_total'),
    round(avg('distance'),3).alias('average_distance')
)

# Sort by incidents_total in descending order
result_df = result_df.orderBy(desc('incidents_total'))

# Calculate the execution time
end_timestamp = time.time()
execution_time = end_timestamp - start_timestamp

# Print the execution time
print("Execution Time:", execution_time, "seconds")

# Show the result
result_df.show()

spark.stop() 