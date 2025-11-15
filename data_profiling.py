# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct

spark = SparkSession.builder.appName("DataProfiling").getOrCreate()

tables = [
    "lead_log",
    "user_referrals",
    "user_referral_logs",
    "user_logs",
    "user_referral_statuses",
    "referral_rewards",
    "paid_transactions"
]

for table in tables:
    path = f"/Volumes/workspace/default/springercapital_data/{table}.csv"
    df = spark.read.csv(
        path,
        header=True,
        inferSchema=True,   # All columns are strings
        nullValue="",        # Treat empty strings as null
        nanValue="null"
    )
    print(f"\nProfiling {table}:")
    total_rows = df.count()
    total_cols = len(df.columns)
    print(f"Rows: {total_rows}, Columns: {total_cols}")
    for c in df.columns:
        nulls = df.filter((col(c).cast("string")=="null") | (col(c).cast("string")=="" )).count()
        distincts = df.select(countDistinct(c)).collect()[0][0]
        print(f"  {c}: nulls={nulls}, distincts={distincts}")
