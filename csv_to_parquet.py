from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Read the crimes CSV files into DataFrames
crimes_1 = spark.read.csv('hdfs://master:9000/home/user/Data/Crime_Data_from_2010_to_2019.csv',
                           header = True, inferSchema = True, escape='"') #in case there are quotes inside quotes
crimes_2 = spark.read.csv('hdfs://master:9000/home/user/Data/Crime_Data_from_2020_to_Present.csv',
                           header = True, inferSchema = True, escape='"')
 
# Convert DataFrames to Parquet format
crimes_1.write.parquet('hdfs://master:9000/home/user/Data/Crime_Data_from_2010_to_2019.parquet')
crimes_2.write.parquet('hdfs://master:9000/home/user/Data/Crime_Data_from_2020_to_Present.parquet')

spark.stop()