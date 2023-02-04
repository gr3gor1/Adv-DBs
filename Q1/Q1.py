from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.appName("Q1").getOrCreate()

#create dataframe of yellow taxi trips
trips=spark.read.parquet("hdfs://192.168.0.1:9000/data/parq")
#create dataframe of zones
zones=spark.read.option("header","true").option("delimiter",",").option("inferSchema","true").csv("hdfs://192.168.0.1:9000/data/csv/taxi+_zone_lookup.csv")


#create the year and month columns based on the pickup timestamps
trips = trips.withColumn("month", date_format(to_date("tpep_pickup_datetime", "yyyy-MM-dd"), "MMMM")).withColumn("year", date_format(to_date("tpep_pickup_datetime", "yyyy-MM-dd"), "yyyy"))
#keep records regarding only March 2022
trips = trips.filter(trips["month"]=="March").filter(trips["year"]==2022)
#join the two dataframes on the column regarding the drop-off ID so as to get the name of the zone
trips = trips.join(zones,trips["DOLocationID"] == zones["LocationID"],"inner")
#keep only records with drop-off zone "Battery Park"
trips = trips.filter(trips["Zone"]=="Battery Park")
#get the maximum tip_amount value
a = trips.agg(max("tip_amount")).collect()[0][0]
#get the trip with the maximum tip_amount value
trips = trips.filter(trips['tip_amount']== str(a)).collect()
