import time
from pyspark.sql import SparkSession, Row
from io import StringIO
import csv

# Start measuring execution time
start_timestamp = time.time()

# Create Spark session
spark = SparkSession.builder \
    .appName("Q2 - Crime Analysis by Time of Day RDD") \
    .getOrCreate() \
    .sparkContext


# Function to parse a CSV line correctly 
def parse_csv_line(line):
    reader = csv.reader(StringIO(line))  # since we need to handle quoted commas
    return next(reader)

# Function to create the time of day categories
def categorize_time(time_occ):
    time_occ = int(time_occ) # because 'TIME OCC' values are strings
    if 500 <= time_occ <= 1159:
        return "Πρωί"
    elif 1200 <= time_occ <= 1659:
        return "Απόγευμα"
    elif 1700 <= time_occ <= 2059:
        return "Βράδυ"
    else:
        return "Νύχτα"

# Read data (csv) into RDDs (textFile does not support handling quoted commas or we didn't find the way)
rdd1 = spark.textFile("hdfs://master:9000/home/user/Data/Crime_Data_from_2010_to_2019.csv")
rdd2 = spark.textFile("hdfs://master:9000/home/user/Data/Crime_Data_from_2020_to_Present.csv")

# Filter out the header and parse data with custom function
header1 = rdd1.first()
header2 = rdd2.first()
rdd1 = rdd1.filter(lambda row: row != header1).map(parse_csv_line)
rdd2 = rdd2.filter(lambda row: row != header2).map(parse_csv_line)

# Union the two RDDs
data = rdd1.union(rdd2)

# Filter for STREET and map to (categorized time, 1) pairs
data_filtered = data.filter(lambda x: x[15] == "STREET")
mapped_time_data = data_filtered.map(lambda row: (categorize_time(row[3]), 1))

# # Print mapping for debugging
# print("Mapped Data Sample:")
# print(filtered_time_data.count())

# # Reduce by key to get the count of crimes in each category
reduced_time = mapped_time_data.reduceByKey(lambda x, y: x + y)

# # Print for debugging
# print("Reduced data samples:")
# print(reduced_time.count())
# print(reduced_time.take(4))

# Sort 
ranked_time = reduced_time.sortBy(lambda x: x[1], ascending = False)

# Convert to a DataFrame to show the result
ranked_time_df = ranked_time.map(lambda row: Row(time=row[0], count=row[1])).toDF()
ranked_time_df.show()

# Calculate the execution time
end_timestamp = time.time()
execution_time = end_timestamp - start_timestamp

# Print the execution time
print("Execution Time:", execution_time, "seconds")

# Stop Spark session
spark.stop()