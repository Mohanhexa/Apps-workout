# -*- coding: utf-8 -*-
"""
Created on Sat Jul 19 20:31:03 2025

@author: Administrator
"""

import logging
from pyspark.sql import SparkSession
import sys
import json
import os
from utils.transformations import clean_data, filter_high_value_customers

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger("CustomerETLApp")


os.environ['PYSPARK_PYTHON'] = "C:/Users/Administrator/anaconda3/python.exe"
os.environ['PYSPARK_DRIVER_PYTHON'] = "C:/Users/Administrator/anaconda3/python.exe"

def load_config(path):
    try:
        with open(path, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load config file: {e}")
        sys.exit(1)

def main(config_path):
    config = load_config(config_path)

    spark = SparkSession.builder \
        .appName("CustomerETLApp") \
        .master("local[*]") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")

    input_path = config["input_path"]
    output_path = config["output_path"]

    logging.info(f"Reading input file from: {input_path}")
    print(f"Reading data from: {input_path}")

    try:
        df = spark.read.option("header", True).csv(input_path)
        df.show()  # Display the data
    except Exception as e:
        logger.error(f"Error reading CSV: {e}")
        spark.stop()
        sys.exit(1)

    logging.info("Cleaning data...")
    df_clean = clean_data(df)
    df_clean.show()

    logging.info("Filtering high value customers...")
    df_filtered = filter_high_value_customers(df_clean)
    df_filtered.show()

    logging.info("Writing output...")
    print(f"Write result to : {output_path}")
    try:
        df_filtered.write.mode("overwrite").parquet(output_path)
        #df_filtered.write.mode("overwrite").csv(output_path)
    except Exception as e:
        logger.error(f"Error writing output: {e}")
        spark.stop()
        sys.exit(1)

    logging.info("Job completed.")
    
    df = spark.read.parquet("output/")
    df.show()

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit main.py <config_path>")
        sys.exit(1)
    main(sys.argv[1])
    
    
"""
Reading data from: file:///C:/Users/Administrator/Python_workouts/Pyspark_workouts/Spark_application/data/customers.csv
+-----------+-------+-------------------+---------------+
|customer_id|   name|              email|purchase_amount|
+-----------+-------+-------------------+---------------+
|          1|  Alice|  alice@example.com|        1200.50|
|          2|    Bob|    bob@example.com|         950.00|
|          3|Charlie|charlie@example.com|        2000.75|
|          4|  Diana|  diana@example.com|         500.00|
|          5|    Eve|    eve@example.com|        1800.00|
|          6|  mohan|    moh@example.com|         600.98|
+-----------+-------+-------------------+---------------+

+-----------+-------+-------------------+---------------+
|customer_id|   name|              email|purchase_amount|
+-----------+-------+-------------------+---------------+
|          1|  Alice|  alice@example.com|        1200.50|
|          2|    Bob|    bob@example.com|         950.00|
|          3|Charlie|charlie@example.com|        2000.75|
|          4|  Diana|  diana@example.com|         500.00|
|          5|    Eve|    eve@example.com|        1800.00|
|          6|  mohan|    moh@example.com|         600.98|
+-----------+-------+-------------------+---------------+

+-----------+-------+-------------------+---------------+
|customer_id|   name|              email|purchase_amount|
+-----------+-------+-------------------+---------------+
|          1|  Alice|  alice@example.com|         1200.5|
|          3|Charlie|charlie@example.com|        2000.75|
|          5|    Eve|    eve@example.com|         1800.0|
+-----------+-------+-------------------+---------------+

Write result to : file:///C:/Users/Administrator/Python_workouts/Pyspark_workouts/Spark_application/output
+-----------+-------+-------------------+---------------+
|customer_id|   name|              email|purchase_amount|
+-----------+-------+-------------------+---------------+
|          1|  Alice|  alice@example.com|         1200.5|
|          3|Charlie|charlie@example.com|        2000.75|
|          5|    Eve|    eve@example.com|         1800.0|
+-----------+-------+-------------------+---------------+

"""
