# import psycopg2
# import pandas as pd


# def create_database():
#     try:
#         conn = psycopg2.connect(dbname='postgres', user='postgres', password='qwerty123', host='localhost', port='5432')
#         conn.autocommit = True
#         cursor = conn.cursor()

#         cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'cobaexsport_db';")
#         exists = cursor.fetchone()
#         if not exists:
#             cursor.execute("CREATE DATABASE cobaexsport_db;")
#             print("Database 'cobaexsport_db' created successfully!")
#         else:
#             print("Database 'cobaexsport_db' already exists.")

#         cursor.close()
#         conn.close()
#     except Exception as e:
#         print(f"Error creating database: {e}")

# def create_tables():
#     try:
#         conn = psycopg2.connect(dbname='cobaexsport_db', user='postgres', password='qwerty123', host='localhost', port='5432')
#         cursor = conn.cursor()


#         cursor.execute("""
#         CREATE TABLE IF NOT EXISTS data_category_exsport (
#             category_id VARCHAR(255) PRIMARY KEY,
#             category VARCHAR(255) NOT NULL
#         );

#         CREATE TABLE IF NOT EXISTS data_color_exsport (
#             color_id VARCHAR(255) PRIMARY KEY,
#             color VARCHAR(255) NOT NULL
#         );

#         CREATE TABLE IF NOT EXISTS data_name_exsport (
#             product_id VARCHAR(255) PRIMARY KEY,
#             name_product VARCHAR(255) NOT NULL
#         );

#         CREATE TABLE IF NOT EXISTS data_product_exsport (
#             product_id VARCHAR(255) PRIMARY KEY,
#             color_id VARCHAR(255) REFERENCES data_color_exsport(color_id),
#             price_original FLOAT,
#             price_sale FLOAT,
#             discount TEXT,
#             sold INT,
#             rating TEXT
#         );

#         CREATE TABLE IF NOT EXISTS data_stock_exsport (
#             product_id VARCHAR(255) REFERENCES data_product_exsport(product_id),
#             category_id VARCHAR(255) REFERENCES data_category_exsport(category_id),
#             color_id VARCHAR(255) REFERENCES data_color_exsport(color_id),
#             stock INT,
#             PRIMARY KEY (product_id, category_id, color_id)
#         );

#         CREATE TABLE IF NOT EXISTS data_user_tokped (
#             customer_id VARCHAR(255) PRIMARY KEY,
#             name VARCHAR(255),
#             age INT,
#             gender VARCHAR(10),
#             location VARCHAR(255),
#             browsing_history TEXT,
#             purchase_history TEXT
#         );

#         CREATE TABLE IF NOT EXISTS data_order_tokped (
#             order_id VARCHAR(255) PRIMARY KEY,
#             customer_id VARCHAR(255) REFERENCES data_user_tokped(customer_id),
#             product_id VARCHAR(255) REFERENCES data_product_exsport(product_id),
#             quantity INT,
#             order_date DATE,
#             order_status VARCHAR(50),
#             shipping_method VARCHAR(50),
#             payment_method VARCHAR(50),
#             total_price FLOAT
#         );

#         CREATE TABLE IF NOT EXISTS data_review_order_tokped (
#             order_id VARCHAR(255) REFERENCES data_order_tokped(order_id),
#             reviewer_id VARCHAR(255) REFERENCES data_user_tokped(customer_id),
#             product_id VARCHAR(255) REFERENCES data_product_exsport(product_id),
#             review_text TEXT,
#             rating FLOAT,
#             review_date DATE,
#             PRIMARY KEY (order_id, reviewer_id, product_id)
#         );
#         """)

#         conn.commit()
#         cursor.close()
#         conn.close()
#         print("Tables created successfully!")
    
#     except Exception as e:
#         print(f"Error creating tables: {e}")

# def load_data_from_csv(table_name, file_path):
#     try:
#         print(f"Loading data from {file_path}")  
#         conn = psycopg2.connect(dbname='cobaexsport_db', user='postgres', password='qwerty123', host='localhost', port='5432')
#         cursor = conn.cursor()


#         df = pd.read_csv(file_path)
#         print(f"CSV Data: {df.head()}")  


#         columns = ", ".join(df.columns)
#         values_placeholder = ", ".join(["%s"] * len(df.columns))
#         query = f"INSERT INTO {table_name} ({columns}) VALUES ({values_placeholder}) ON CONFLICT DO NOTHING;"

#         for _, row in df.iterrows():
#             cursor.execute(query, tuple(row))

#         conn.commit()
#         cursor.close()
#         conn.close()
#         print(f"Data inserted into {table_name} successfully!")

#     except Exception as e:
#         print(f"Error inserting data into {table_name}: {e}")


# if __name__ == "__main__":
#     create_database()
#     create_tables()
    
#     load_data_from_csv("data_category_exsport", "clean-data/data_category_exsport.csv")
#     load_data_from_csv("data_color_exsport", "clean-data/data_color_exsport.csv")
#     load_data_from_csv("data_name_exsport", "clean-data/data_name_exsport.csv")
#     load_data_from_csv("data_product_exsport", "clean-data/data_product_exsport.csv")
#     load_data_from_csv("data_stock_exsport", "clean-data/data_stock_exsport.csv")
#     load_data_from_csv("data_user_tokped", "clean-data/data_user_tokped.csv")
#     load_data_from_csv("data_order_tokped", "clean-data/data_order_tokped.csv")
#     load_data_from_csv("data_review_order_tokped", "clean-data/data_review_order_tokped.csv")

#     print("All processes completed successfully!")
