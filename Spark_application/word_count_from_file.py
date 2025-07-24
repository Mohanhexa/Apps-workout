# -*- coding: utf-8 -*-
"""
Created on Sun Jul 20 23:01:34 2025

@author: Administrator
"""
import os
os.environ['PYSPARK_PYTHON'] = "C:/Users/Administrator/anaconda3/python.exe"
os.environ['PYSPARK_DRIVER_PYTHON'] = "C:/Users/Administrator/anaconda3/python.exe"
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Word_Count_program").master("local[*]").getOrCreate()

input_path ="file:///C:/Users/Administrator/Python_workouts/Pyspark_workouts/Spark_application/data/Words_count_inputfile.txt"

rdd = spark.sparkContext.textFile(input_path)

word_count= (rdd.flatMap(lambda line: line.split())
                .map(lambda word: (word.lower(),1))
                .reduceByKey(lambda a,b : a + b )
                )

for word,count in word_count.collect():
    print(f"{word}: {count}")
    

spark.stop()