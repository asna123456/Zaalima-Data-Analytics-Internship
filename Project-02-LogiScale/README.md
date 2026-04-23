# Project 02: LogiScale BigData - Global Logistics & Forecasting

## Project Overview
This project focuses on analyzing high-velocity shipping and logistics data using **PySpark** to handle big data volumes. The goal is to monitor route efficiency, calculate delivery delays, and forecast demand to optimize supply chain operations.

## Tech Stack
- **Data Processing:** PySpark (Spark SQL & DataFrames)
- **Programming:** Python (JDK 17 Environment)
- **Libraries:** Pandas (for benchmarking), PySpark SQL Functions
- **Visualization:** Power BI (Planned)
- **Dataset:** DataCo Smart Supply Chain Dataset (180k+ records)

## Weekly Progress & Insights

### Week 5: Environment Setup & Data Ingestion
- Successfully configured a Big Data environment by integrating **PySpark with JDK 17**.
- Ingested a total of **180,519 records** from the Supply Chain dataset.
- **Performance Benchmark:**
  - **Pandas Load Time:** 1.44s
  - **PySpark Load Time:** 6.21s 
  *(Note: PySpark's initial overhead is compensated by its ability to scale with larger datasets.)*

### Week 6: Shipping Delay Analysis
- Developed an analytical logic to compare `Days for shipping (real)` against `Days for shipment (scheduled)`.
- **Core Findings:**
  - **Total Delayed Shipments:** 103,400 orders.
  - **Overall Delay Percentage:** **57.28%**.
- This high delay rate identifies a critical bottleneck in the current logistics model that requires route optimization.

### Week 7: Advanced Analytics
- Implemented **PySpark Window Functions** for granular data analysis.
- Calculated **Sales Ranking** per region to identify high-performing orders.
- Derived **Average Shipping Days** per region, highlighting logistics trends across different global markets (e.g., Canada: 3.33 days, West Africa: 3.53 days).

### Week 8: Final Dashboard & Reporting (Upcoming)
- Transitioning insights into an executive Power BI dashboard for real-time monitoring.

---
*Internship Project at Zaalima Development Pvt. Ltd.*
