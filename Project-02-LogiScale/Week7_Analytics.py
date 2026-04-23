import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rank, avg, count
from pyspark.sql.window import Window

# 1. Java Settings (JDK 17)
os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "\\bin;" + os.environ["PATH"]

spark = SparkSession.builder.appName("LogiScale_Week7_Final") \
    .config("spark.driver.extraJavaOptions", "--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.math=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED") \
    .getOrCreate()

# 2. Load Data
df = spark.read.csv("DataCoSupplyChainDataset.csv", header=True, inferSchema=True)
original_count = df.count()

print("\n" + "="*60)
print("--- [WEEK 7] ADVANCED ANALYTICS & DATA VALIDATION ---")
print("="*60)

# 3. Advanced Analytics: Window Functions (Ranking Sales)
windowSpec = Window.partitionBy("Order Region").orderBy(col("Sales").desc())
analysis_df = df.withColumn("Sales_Rank", rank().over(windowSpec))

# 4. Aggregation: Regional Logistics Insights
avg_window = Window.partitionBy("Order Region")
insight_df = analysis_df.withColumn("Avg_Shipping_Days", avg("Days for shipping (real)").over(avg_window))

# 5. DATA VALIDATION LAYER (Cross-check for Integrity)
print("\n[VALIDATION] Running integrity checks...")
processed_count = insight_df.count()
null_check = insight_df.filter(col("Avg_Shipping_Days").isNull()).count()

if original_count == processed_count and null_check == 0:
    print(f"✔️ Validation Success: Integrity maintained ({original_count} records).")
    print(f"✔️ No Data Loss detected during PySpark processing.")
else:
    print(f"⚠️ Validation Warning: Potential data issues found.")

# 6. VISUALIZATION LAYER EXPORT (Manageable CSV for Power BI)
# Summarizing the data to a manageable insight layer
aggregated_csv_df = insight_df.select("Order Region", "Avg_Shipping_Days").distinct()

# Exporting to a standard CSV file
print("\n[EXPORT] Saving aggregated insights for Power BI...")
aggregated_csv_df.toPandas().to_csv("Zaalima_Week7_Aggregated_Insights.csv", index=False)
print("✔️ File Saved: Zaalima_Week7_Aggregated_Insights.csv")

# 7. Final Output Preview
print("\nFinal Insights Preview (Zaalima Week 7 Requirements):")
aggregated_csv_df.show(10)

print("\n" + "="*60)
print("WEEK 7 TASK COMPLETED SUCCESSFULLY")
print("="*60)