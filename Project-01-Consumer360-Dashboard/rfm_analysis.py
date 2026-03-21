import pandas as pd
from sqlalchemy import create_engine

# 1. DATABASE CONNECTION
# Connecting to your MySQL database 'Zaalima_Retail_DB'
engine = create_engine('mysql+mysqlconnector://root:mysql%40123@localhost/Zaalima_Retail_DB')

print("--- Starting RFM Analysis ---")

try:
    # 2. LOADING DATA FROM SQL
    print("Loading Sales data from MySQL...")
    query = "SELECT * FROM fact_sales"
    df = pd.read_sql(query, con=engine)

    # Ensure purchase_date is in the correct datetime format
    df['purchase_date'] = pd.to_datetime(df['purchase_date'])

    # 3. CALCULATING RFM METRICS
    # Set the snapshot date to one day after the most recent purchase
    snapshot_date = df['purchase_date'].max() + pd.Timedelta(days=1)

    print("Calculating Recency, Frequency, and Monetary values...")
    rfm = df.groupby('customer_id').agg({
        # Recency: Days since the last order
        'purchase_date': lambda x: (snapshot_date - x.max()).days, 
        # Frequency: Total number of orders
        'order_id': 'count',                                      
        # Monetary: Total revenue from the customer
        'price': 'sum'                                            
    })

    # Rename columns for professional reporting
    rfm.rename(columns={
        'purchase_date': 'Recency',
        'order_id': 'Frequency',
        'price': 'Monetary'
    }, inplace=True)

    # 4. CUSTOMER SEGMENTATION LOGIC
    print("Segmenting customers based on RFM scores...")
    # Scoring from 1 to 5 (Higher is generally better, except for Recency)
    rfm['R_Score'] = pd.qcut(rfm['Recency'], 5, labels=[5, 4, 3, 2, 1])
    rfm['F_Score'] = pd.qcut(rfm['Frequency'].rank(method='first'), 5, labels=[1, 2, 3, 4, 5])
    rfm['M_Score'] = pd.qcut(rfm['Monetary'], 5, labels=[1, 2, 3, 4, 5])
    rfm = rfm.reset_index()

    # 5. SAVING RESULTS TO MYSQL
    print("Saving RFM analysis results back to MySQL...")
    # NOTE: We set index=False to avoid the BLOB/TEXT column error in MySQL
    rfm.to_sql('rfm_segmented_data', con=engine, if_exists='replace', index=False)

    print("--- SUCCESS: RFM Analysis completed and saved to MySQL! ---")

except Exception as e:
    # Error handling to catch any connection or logic issues
    print(f"--- ERROR: {e} ---")