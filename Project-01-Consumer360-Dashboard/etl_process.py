import pandas as pd
from sqlalchemy import create_engine

# 1. DATABASE CONNECTION
# The '@' symbol in the password is encoded as '%40' for the connection string
engine = create_engine('mysql+mysqlconnector://root:mysql%40123@localhost/Zaalima_Retail_DB')

print("--- Starting ETL Process ---")

try:
    # 2. READING CSV FILES
    print("Reading CSV files from the local directory...")
    # Loading the main datasets into Pandas DataFrames
    orders = pd.read_csv('olist_orders_dataset.csv')
    items = pd.read_csv('olist_order_items_dataset.csv')
    customers = pd.read_csv('olist_customers_dataset.csv')
    products = pd.read_csv('olist_products_dataset.csv')

    # 3. PUSHING DATA TO MYSQL
    print("Pushing data to MySQL database (Zaalima_Retail_DB)...")
    
    # Writing DataFrames to SQL tables. 'if_exists=replace' ensures fresh data.
    orders.to_sql('raw_orders', con=engine, if_exists='replace', index=False)
    items.to_sql('raw_order_items', con=engine, if_exists='replace', index=False)
    customers.to_sql('raw_customers', con=engine, if_exists='replace', index=False)
    products.to_sql('raw_products', con=engine, if_exists='replace', index=False)

    print("--- SUCCESS: Raw data loaded into MySQL successfully! ---")

except Exception as e:
    # 4. ERROR HANDLING
    print(f"--- ERROR: {e} ---")