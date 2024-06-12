import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, count, collect_list, regexp_replace, year, to_timestamp

# Start measuring execution time
start_timestamp = time.time()

# Create Spark session
spark = SparkSession.builder \
    .appName("Q3 - Crime Analysis by Victim Descent in LA 2015 - BROADCAST") \
    .getOrCreate()

# Read data (csv) into spark Dataframes
crime_data = spark.read.csv("hdfs://master:9000/home/user/Data/Crime_Data_from_2010_to_2019.csv",
                             header=True, inferSchema=True)
revgeo_data = spark.read.csv("hdfs://master:9000/home/user/Data/revgecoding.csv", 
                             header=True, inferSchema=True)
income_data = spark.read.csv("hdfs://master:9000/home/user/Data/LA_income_2015.csv", 
                             header=True, inferSchema=True)


# Filter crime data for non-null values and for year = 2015
crime_data_filtered = crime_data.filter(col('Vict Descent').isNotNull() & col('LAT').isNotNull() & col('LON').isNotNull())
crime_data_filtered = crime_data_filtered.withColumn('DATE OCC', to_timestamp(col('DATE OCC'), 'MM/dd/yyyy hh:mm:ss a'))
crime_data_filtered = crime_data_filtered.filter(year(col('DATE OCC')) == 2015)

# Filter revgecoding to only keep one ZIPcode where two exist and filter out nulls
revgeo_data = revgeo_data.withColumn('ZIPcode', substring('ZIPcode', 1,5))
revgeo_data = revgeo_data.filter(col('ZIPcode').isNotNull())

# Remove dollar sign and comma and cast to integer for better comparison 
income_data = income_data.withColumn(
    'Estimated Median Income',
    regexp_replace(col('Estimated Median Income'), '[$,]', '').cast('int')
)

# Perform an inner join between crime data and revgecoding to map coordinates to ZIP codes and drop duplicate columns
crime_with_zip = crime_data_filtered.join(revgeo_data.hint("broadcast"), (crime_data_filtered['LAT'] == revgeo_data['LAT']) 
    & (crime_data_filtered['LON'] == revgeo_data['LON']), 'inner').drop(revgeo_data.LAT).drop(revgeo_data.LON)

# Join with income data to get median household income by ZIP code, drop duplicate columns and drop null income
crime_with_zip_income = crime_with_zip.join(income_data.hint("broadcast"), crime_with_zip['ZIPcode'] == income_data['Zip Code'], 'inner').drop(crime_with_zip.ZIPcode)
crime_with_zip_income = crime_with_zip_income.filter(col('Estimated Median Income').isNotNull())

# Get the top 3 and bottom 3 ZIP codes by median household income
top_3_zip = crime_with_zip_income.orderBy(col('Estimated Median Income').desc()).limit(3).select(collect_list('Zip Code')).first()[0]
bottom_3_zip = crime_with_zip_income.orderBy(col('Estimated Median Income').asc()).limit(3).select(collect_list('Zip Code')).first()[0]

# Filter crime data to change Vict Descent names
crime_with_zip_income = crime_with_zip_income.replace(['A', 'B', 'C', 'D', 'F', 'G', 'H', 'I', 'J', 'K',
                                                       'L', 'O', 'P', 'S', 'U', 'V', 'W', 'X', 'Z'], 
                              ['Other Asian', 'Black', 'Chinese', 'Cambodian', 'Filipino', 'Guamanian',
                               'Hispanic/Latin/Mexican', 'American Indian/Alaskan Native', 'Japanese',
                               'Korean', 'Laotian', 'Other', 'Pacific Islander', 'Samoan', 'Hawaiian',
                               'Vietnamese', 'White', 'Unknown', 'Asian Indian']
                              ,'Vict Descent')


# Filter crime data for top 3 and bottom 3 ZIP codes
top_3_crime_data = crime_with_zip_income.filter(col('ZIPcode').isin(top_3_zip))
bottom_3_crime_data = crime_with_zip_income.filter(col('ZIPcode').isin(bottom_3_zip))

# Aggregate by victim descent
top_3_aggregated = top_3_crime_data.groupBy('Vict Descent').agg(count('*').alias('total victims')).orderBy(col('total victims').desc())
bottom_3_aggregated = bottom_3_crime_data.groupBy('Vict Descent').agg(count('*').alias('total victims')).orderBy(col('total victims').desc())

# Calculate the execution time
end_timestamp = time.time()
execution_time = end_timestamp - start_timestamp

# Print the execution time
print("Execution Time:", execution_time, "seconds")

# Show results
top_3_aggregated.show()
bottom_3_aggregated.show()

# Stop Spark session
spark.stop()