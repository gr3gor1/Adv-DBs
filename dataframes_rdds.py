from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.master("spark://192.168.0.1:7077").appName("DFs-RDDs").getOrCreate()

#read all parquets into a single dataframe containing the trips data
trips=spark.read.parquet("hdfs://192.168.0.1:9000/data/parq")

#read the csv file from the directory we created in HDFS and turn it into a dataframe
zones=spark.read.option("header","true").option("delimiter",",").option("inferSchema","true").csv("hdfs://192.168.0.1:9000/data/csv/taxi+_zone_lookup.csv")

#now all we have to do is turn our dataframes to RDDs
rddtrips = trips.rdd
rddzones = zones.rdd

#then we can check if everything is okay
trips.show()
zones.show()

a = rddtrips.take(10)
print(a)

b = rddzones.take(10)
print(b)

#finally we can also save the final dataframes as csv files if we want to
