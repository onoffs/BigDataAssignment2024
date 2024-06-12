import time
from pyspark.sql import SparkSession
import geopy.distance
from io import StringIO
import csv

# Start measuring execution time
start_timestamp = time.time()

# Create Spark session
spark = SparkSession.builder \
    .appName("Q4 - Crime Analysis by Police Station, Distance from it and Weapons used - RDD / BROADCAST") \
    .getOrCreate() \
    .sparkContext

# Function given from exercise instructions to calculate distance between two points
def get_distance(lat1, lon1, lat2, lon2):
    return geopy.distance.geodesic((lat1, lon1), (lat2, lon2)).km

# Function to parse CSV lines correctly
def parse_csv_line(line):
    if not line.strip():
        return None
    reader = csv.reader(StringIO(line))
    return next(reader)

# Read data (csv) into RDDs (textFile does not support handling quoted commas or we didn't find the way)
crime1 = spark.textFile("hdfs://master:9000/home/user/Data/Crime_Data_from_2010_to_2019.csv")
crime2 = spark.textFile("hdfs://master:9000/home/user/Data/Crime_Data_from_2020_to_Present.csv")
police_stations = spark.textFile("hdfs://master:9000/home/user/Data/LAPD_Police_Stations.csv")

# Filter out the header and parse data with custom function
crime_header1 = crime1.first()
crime_header2 = crime2.first()
police_header = police_stations.first()

crime1 = crime1.filter(lambda row: row != crime_header1).map(parse_csv_line).filter(lambda x: x is not None)
crime2 = crime2.filter(lambda row: row != crime_header2).map(parse_csv_line).filter(lambda x: x is not None)
police_stations = police_stations.filter(lambda row: row != police_header).map(parse_csv_line).filter(lambda x: x is not None)

# Union the two RDDs
crime = crime1.union(crime2)

# Filter crime data for 'Weapon Used Cd' starting with '1' (1xx), for null island and non null coordinates
crime_with_guns = crime.filter(lambda row: row[16].startswith('1') if row[16] else False)
crime_with_guns = crime_with_guns.filter(lambda row: (row[26] != '0' and row[26] is not None) or (row[27] != '0' and row[27] is not None))

# Key police stations and crime by the attribute we want to join on
small_dataset = police_stations.keyBy(lambda x: int(x[5]))  # 5: 'PREC'
large_dataset = crime_with_guns.keyBy(lambda x: int(x[4]))  # 4: 'AREA'

# Broadcast the SMALL dataset (police_stations) --> join, that means ensure each crime has a police station
broadcast_small = spark.broadcast(small_dataset.collectAsMap())
large_joined_small = large_dataset.map(lambda x: (x[0], (x[1], broadcast_small.value.get(x[0])))).filter(lambda x: x[1][1] is not None)

# Calculate distance and add as a new column
def calculate_distance(row):
    crime_coords = (float(row[1][0][26]), float(row[1][0][27]))
    station_coords = (float(row[1][1][1]), float(row[1][1][0]))
    distance = get_distance(*crime_coords, *station_coords)
    return (row[0], (row[1][0][3], distance))

joined_with_distance = large_joined_small.map(calculate_distance)

# Group by division, count incidents and add the distances
reduced_rdd = joined_with_distance.map(lambda row: (row[0], (row[1][0], 1, row[1][1]))) \
                                       .reduceByKey(lambda a, b: (a[0], a[1] + b[1], a[2] + b[2]))

# Calculate the average distance by dividing with the number of incidents and rounding to 3 decimals
average_dist_rdd = reduced_rdd.map(lambda row: (row[0], row[1][1], round(row[1][2] / row[1][1], 3)))

# Sort by incidents_total in descending order
sorted_rdd = average_dist_rdd.sortBy(lambda row: row[1], ascending=False)

# Create a mapping from PREC to DIVISION (to show the name of police station while printing)
prec_to_division = police_stations.map(lambda row: (int(row[5]), row[3])).collectAsMap()

# Collect (--> list) and show the result
result = sorted_rdd.collect()

# Print result
for row in result:
    division_name = prec_to_division.get(row[0], 'Unknown')
    print(f"Division: {division_name}, Incidents Total: {row[1]}, Average Distance: {row[2]} km")

# Calculate the execution time
end_timestamp = time.time()
execution_time = end_timestamp - start_timestamp

# Print the execution time
print("Execution Time:", execution_time, "seconds")

# Stop the Spark session
spark.stop()