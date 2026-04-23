import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# 1. Java Settings (JDK 17 according to your system)
os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "\\bin;" + os.environ["PATH"]

# Initialize Spark Session
spark = SparkSession.builder.appName("LogiScale_Week6_Analysis") \
    .config("spark.driver.extraJavaOptions", 
            "--add-opens=java.base/java.lang=ALL-UNNAMED " +
            "--add-opens=java.base/java.util=ALL-UNNAMED " +
            "--add-opens=java.base/java.math=ALL-UNNAMED " +
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED " +
            "--add-opens=java.base/sun.security.action=ALL-UNNAMED") \
    .getOrCreate()

# 2. Load the Dataset
print("Loading data for Week 6 analysis...")
df = spark.read.csv("DataCoSupplyChainDataset.csv", header=True, inferSchema=True)

# 3. Shipping Delay Logic
# 'Days for shipping (real)' is actual time. 
# 'Days for shipment (scheduled)' is estimated time.
# Difference > 0 means it's a delay.

analysis_df = df.withColumn("Shipping_Gap", col("Days for shipping (real)") - col("Days for shipment (scheduled)")) \
                .withColumn("Delay_Status", when(col("Shipping_Gap") > 0, "Delayed").otherwise("On-Time/Early"))

print("\n" + "="*60)
print("--- [WEEK 6] SHIPPING DELAY ANALYSIS REPORT ---")
print("="*60)

# Displaying the top 10 results to check calculations
analysis_df.select("Order Id", "Days for shipping (real)", "Days for shipment (scheduled)", "Shipping_Gap", "Delay_Status").show(10)

# 4. Calculating the Metrics
total_records = 180519
delayed_count = analysis_df.filter(col("Delay_Status") == "Delayed").count()
delay_percentage = (delayed_count / total_records) * 100

print(f"\nTotal Records Analyzed: {total_records}")
print(f"Total Delayed Shipments: {delayed_count}")
print(f"Overall Delay Percentage: {delay_percentage:.2f}%")

print("\n" + "="*60)
print("WEEK 6 ANALYSIS COMPLETED")
print("="*60)