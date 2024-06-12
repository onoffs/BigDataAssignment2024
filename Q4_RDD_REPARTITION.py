import time
from pyspark.sql import SparkSession
import geopy.distance
from io import StringIO
import csv

# Start measuring execution time
start_timestamp = time.time()

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Q4 - Crime Analysis by Police Station, Distance from it and Weapons used - RDD / REPARTITION") \
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

# Parse CSV files, filter out headers and empty lines in crime and police stations RDDs
crime_header1 = crime1.first()
crime_header2 = crime2.first()
police_header = police_stations.first()

crime1 = crime1.filter(lambda row: row != crime_header1) \
                    .map(parse_csv_line) \
                    .filter(lambda x: x is not None)

crime2 = crime2.filter(lambda row: row != crime_header2) \
                    .map(parse_csv_line) \
                    .filter(lambda x: x is not None)

police_stations = police_stations.filter(lambda row: row != police_header) \
                                 .map(parse_csv_line) \
                                 .filter(lambda x: x is not None)

# Union the RDDs
crime = crime1.union(crime2)

# Filter crime data for 'Weapon Used Cd' starting with '1' (1xx), for null island and non null coordinates
crime_with_guns = crime.filter(lambda row: row[16].startswith('1') if row[16] else False)
crime_with_guns = crime_with_guns.filter(lambda row: (row[26] != '0' and row[26] is not None) or (row[27] != '0' and row[27] is not None))


# Strip Area column from initial 0
crime_data_guns = crime_with_guns.map(
                        lambda row: tuple(row[i].lstrip('0') if i == 4 else row[i] for i in range(len(row))))

# Prepare for repartition join --> construct key, value pairs
crime_rdd = crime_with_guns.map(lambda x: (x[4], ('crime', x)))  # 4: 'AREA'
police_rdd = police_stations.map(lambda x: (x[5], ('police', x))) # 5: 'PREC'

# Union the RDDs and group by key
repartition_rdd = crime_rdd.union(police_rdd).groupByKey()

# Function that separates 'police' and 'crime' records, 
# then creates pairs for each combination of 'crime' and 'police' records with the same key
def process_tags(key, values):
    values_police = [v for tag, v in values if tag == 'police']
    values_crime = [v for tag, v in values if tag == 'crime']
    return [(key, (r, l)) for r in values_crime for l in values_police]

# Join
joined_rdd = repartition_rdd.flatMap(lambda x: process_tags(x[0], list(x[1])))

# Calculate distance and add as a new column
joined_with_distance_rdd = joined_rdd.map(lambda row: (row[0], (row[1][1][3], 
                                                                get_distance(float(row[1][0][26]), float(row[1][0][27]), 
                                                                             float(row[1][1][1]), float(row[1][1][0])))))

# Group by division, count incidents, and add distances
reduced_rdd = joined_with_distance_rdd.map(lambda row: (row[0], (row[1][0], 1, row[1][1]))) \
                                       .reduceByKey(lambda a, b: (a[0], a[1] + b[1], a[2] + b[2]))

# Calculate the average distance and round by 3 decimals
average_dist_rdd = reduced_rdd.map(lambda row: (row[1][0], row[1][1], round(row[1][2] / row[1][1], 3)))

# Sort by incidents_total in descending order
sorted_rdd = average_dist_rdd.sortBy(lambda row: row[1], ascending=False)

# Collect and show the result
result = sorted_rdd.collect()

# Print result
for row in result:
    print(f"Division: {row[0]}, Incidents Total: {row[1]}, Average Distance: {row[2]} km")

print("LENGTH OF RESULT",len(result))

# Calculate the execution time
end_timestamp = time.time()
execution_time = end_timestamp - start_timestamp

# Print the execution time
print("Execution Time:", execution_time, "seconds")

# Stop SparkSession
spark.stop()