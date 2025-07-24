# utils/transformations.py

from pyspark.sql.functions import col

def clean_data(df):
    # Drop rows with nulls and trim strings
    for column in df.columns:
        df = df.withColumn(column, col(column).cast("string"))
        df = df.withColumn(column, col(column).alias(column))
    return df.dropna()

def filter_high_value_customers(df):
    # Convert amount column and filter customers with purchases > 1000
    df = df.withColumn("purchase_amount", col("purchase_amount").cast("double"))
    return df.filter(col("purchase_amount") > 1000)