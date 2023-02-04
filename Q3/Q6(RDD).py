from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from datetime import datetime
from operator import add
import time
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.master("spark://192.168.0.1:7077").appName("Q6").getOrCreate()

#create dataframe of yellow taxi trips
trips=spark.read.parquet("hdfs://192.168.0.1:9000/data/parq")

trips = trips.withColumn("year", date_format(to_date("tpep_pickup_datetime", "yyyy-MM-dd"), "yyyy"))\
    .withColumn("month", date_format(to_date("tpep_pickup_datetime", "yyyy-MM-dd"), "MMMM"))

trips = trips.filter(trips["year"]=="2022").filter(trips["month"]!="July")

rdd = trips.rdd
#pick specific columns
rdd = rdd.map(lambda row: (row[1], row[4], row[7], row[8], row[16]))

#find day of the year per row and get records with different drop off and pick up  locations
rdd = rdd.filter(lambda x: x[2]!=x[3])

rdd = rdd.map(lambda row: (int(row[0].strftime("%j")) // 15, (row[1], row[4],1)))

#find the sum of x[0] (trip distance) and x[1] (total amount) and then the sum of count
rdd = rdd.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2]+y[2]))

#divide the sum by the count to get the mean
rdd = rdd.map(lambda x: (x[0], x[1][0] / x[1][2], x[1][1]/x[1][2]))

#sort by day of the year
rdd = rdd.sortBy(lambda x: x[0]).collect()

