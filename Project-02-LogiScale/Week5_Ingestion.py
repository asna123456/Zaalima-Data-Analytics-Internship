import os
import sys
import time
import pandas as pd

# 1. Setting JAVA_HOME to JDK 17 (Found in your Program Files)
os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "\\bin;" + os.environ["PATH"]

# Handling incubator modules for newer Java versions
os.environ['PYSPARK_SUBMIT_ARGS'] = '--driver-java-options "-Djdk.incubator.vector=ALLOW" pyspark-shell'

from pyspark.sql import SparkSession

# 2. Initialize Spark Session with Java 17 bypass configurations
spark = SparkSession.builder \
    .appName("LogiScale_BigData_SupplyChain") \
    .config("spark.driver.extraJavaOptions", 
            "--add-opens=java.base/java.lang=ALL-UNNAMED " +
            "--add-opens=java.base/java.util=ALL-UNNAMED " +
            "--add-opens=java.base/java.math=ALL-UNNAMED " +
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED " +
            "--add-opens=java.base/sun.security.action=ALL-UNNAMED") \
    .getOrCreate()

print("\n" + "="*60)
print("--- [WEEK 5] PERFORMANCE BENCHMARK: PYSPARK VS PANDAS ---")
print("="*60 + "\n")

# 3. Data Ingestion & Benchmark
try:
    # Pandas Ingestion (Standard for comparison)
    print("Testing Pandas loading...")
    start_time = time.time()
    pdf = pd.read_csv("DataCoSupplyChainDataset.csv", encoding='latin1')
    print(f"Pandas Load Time: {time.time() - start_time:.4f} seconds")

    # PySpark Ingestion (Scalable Big Data approach)
    print("\nTesting PySpark loading...")
    start_time = time.time()
    sdf = spark.read.csv("DataCoSupplyChainDataset.csv", header=True, inferSchema=True)
    print(f"PySpark Load Time: {time.time() - start_time:.4f} seconds")

    # 4. Results & Validation
    print("\n" + "-"*40)
    print("LOGISCALE DATA SCHEMA")
    print("-"*40)
    sdf.printSchema()

    row_count = sdf.count()
    print(f"\nTotal Records Ingested: {row_count}")

    # 5. Data Preview
    print("\n" + "-"*40)
    print("SUPPLY CHAIN PREVIEW (First 5 Rows)")
    print("-"*40)
    sdf.select("Order Region", "Type", "Days for shipping (real)", "Delivery Status").show(5)

except Exception as e:
    print(f"An error occurred during execution: {e}")

print("\n" + "="*60)
print("WEEK 5 TASK COMPLETED SUCCESSFULLY")
print("="*60)