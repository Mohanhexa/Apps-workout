# -*- coding: utf-8 -*-
"""
Created on Sun Jul 20 12:00:47 2025

@author: Administrator
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

import os
os.environ['PYSPARK_PYTHON'] = "C:/Users/Administrator/anaconda3/python.exe"
os.environ['PYSPARK_DRIVER_PYTHON'] = "C:/Users/Administrator/anaconda3/python.exe"

spark = SparkSession.builder \
    .appName("Local Broadcast join Example") \
        .master("local[*]") \
            .getOrCreate()
            
            
transaction = spark.read.csv("file:///C:/Users/Administrator/Python_workouts/Pyspark_workouts/Spark_application/data/transactions.csv", header = True, inferSchema = True)
countryLookup = spark.read.csv("file:///C:/Users/Administrator/Python_workouts/Pyspark_workouts/Spark_application/data/country_codes.csv", header = True, inferSchema = True)

broadcosted_country = broadcast(countryLookup)
enriched = transaction.join(countryLookup, on = "country_code",how ="left")
enriched.show(truncate= False)

"""
+------------+--------------+-------+------+-------------------+-------------+
|country_code|transaction_id|user_id|amount|timestamp          |country_name |
+------------+--------------+-------+------+-------------------+-------------+
|US          |tx001         |101    |150.75|2025-07-01 10:00:00|United States|
|IN          |tx002         |102    |80.0  |2025-07-01 10:05:00|India        |
|DE          |tx003         |103    |200.0 |2025-07-01 10:10:00|Germany      |
|US          |tx004         |104    |120.5 |2025-07-01 10:12:00|United States|
|FR          |tx005         |105    |300.0 |2025-07-01 10:20:00|France       |
+------------+--------------+-------+------+-------------------+-------------+
"""
enriched.write.mode("overwrite").csv("file:///C:/Users/Administrator/Python_workouts/Pyspark_workouts/Spark_application/output/enriched_transactions", header = True)

spark.stop()