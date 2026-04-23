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

### Week 7: Advanced Analytics & Visualization Layer Export
Objective: Perform advanced data analysis and prepare data for the final dashboard.
Tech Used: PySpark (Window Functions), Pandas (for final CSV export).
**Key Tasks:**
- Implemented Window Functions to rank sales by region.
- Calculated Average Shipping Days for global logistics monitoring.
- Conducted Data Integrity Validation (180,519 records verified).
Exported summarized insights to a CSV file for Power BI connectivity.
### Week 8: Final Dashboard & Reporting
- Successfully transitioned big data insights into an executive Power BI Control Tower Dashboard for real-time logistics monitoring.
- Key Visuals & Metrics:
- KPI Cards: Real-time tracking of Total Shipments (180.52K) and Total Delayed Shipments (98.98K).
- Delivery Target Achievement (Gauge Chart): Monitored performance against a target of 150K shipments.
- Market Share Analysis (Treemap): Identified LATAM and Europe as the highest-volume markets.
- Shipping Status Distribution (Pie Chart): Visualized the ratio of Late, On-time, and Canceled deliveries.
- Interactive Slicers: Enabled dynamic filtering by Payment Method (Type) for granular analysis.
- Outcome: The dashboard provides a comprehensive "Control Tower" view, allowing stakeholders to identify delay bottlenecks and market trends at a glance.

---
*Internship Project at Zaalima Development Pvt. Ltd.*
