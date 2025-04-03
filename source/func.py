from datetime import datetime
from datetime import date
from faker import Faker
import hashlib
import random
import time


def extract_category(name_product):
    name_lower = name_product.lower()
    if any (keyword in name_lower for keyword in ['laptop', 'sleeve']):
        return 'TAS LAPTOP'
    elif any (keyword in name_lower for keyword in ['sling bag', 'sling pouch']):
        return 'TAS SLING BAG'
    elif any (keyword in name_lower for keyword in ['backpack', 'rucksack']):
        return 'TAS BACKPACK'
    elif any (keyword in name_lower for keyword in ['pouch']):
        return 'TAS POUCH'
    elif any (keyword in name_lower for keyword in ['tote bag']):
        return 'TAS TOTE BAG'
    else:
        return 'SEMUA ETALASE'

def extract_color(name_product):
    color_list = [
        'DARK GREEN', 'SKY BLUE', 'BIRU MUDA', 'BLUE', 'ORANGE', 'LIGHT YELLOW', 'GREEN ARMY', 'FUCHSIA',
        'LIGHT BROWN', 'DARK BLUE', 'MINT GREEN', 'DARK GREY', 'UNGU MUDA', 'FUCSHIA', 'LIGHT PINK', 'CREAM',
        'GOLD', 'DARK OLIVE', 'RED', 'GREY', 'WHITE', 'DARK PURPLE', 'SALEM', 'FUSCHIA', 'HITAM', 'LIME',
        'BROWN', 'BLUEBERRY', 'SOFT BLUE', 'BLACK', 'DARK SALEM', 'YELLOW', 'MAROON', 'DARK BROWN', 'BEIGE',
        'LIGHT BLUE', 'GREEN', 'COKELAT MUDA', 'LIGHT GREEN', 'CURRY', 'PINK', 'KREM', 'BIRU TUA', 'KHAKI',
        'PURPLE', 'EMERALD GREEN', 'DARK ORANGE', 'FUCHIA', 'NAVY', 'LIGHT PURPLE', 'OLIVE', 'LIGHT GREY'
    ]

    color_translation = {
        'KREM': 'CREAM',
        'COKELAT MUDA': 'LIGHT BROWN',
        'BIRU TUA': 'DARK BLUE',
        'HITAM': 'BLACK',
        'BIRU MUDA': 'LIGHT BLUE',
        'SALEM': 'SALMON',
        'UNGU MUDA': 'LIGHT PURPLE',
        'FUCSHIA': 'FUCHSIA',
        'FUCHSIA': 'FUCHSIA',
        'FUCHIA': 'FUCHSIA',
    }

    name_product = str(name_product).strip()

    # Konversi ke huruf besar
    name_upper = name_product.upper()

    # Ambil warna setelah tanda '-'
    color_product = name_upper.split('-')[-1].strip()

    # Urutan pengecekan:
    if color_product in color_translation:  # 1. Cek di color_translation dulu
        return color_translation[color_product]
    elif color_product in color_list:  # 2. Cek di color_list
        return color_product
    else:  # 3. Jika tidak ada di keduanya
        return 'Tidak ada spesifikasi warna'
    
def generate_product_id(index):
    base_string = f"PRODUCT-{index}"
    unique_hash = hashlib.md5(base_string.encode()).hexdigest()[:6]  # Ambil 6 karakter pertama
    return f"ETWS{unique_hash.upper()}"

def generate_color_id(index):
    base_string = f"COLOR-{index}"
    unique_hash = hashlib.md5(base_string.encode()).hexdigest()[:6]
    return f"ECLR{unique_hash.upper()}"

def generate_category_id(index):
    base_string = f"CATEGORY-{index}"
    unique_hash = hashlib.md5(base_string.encode()).hexdigest()[:6]
    return f"ECAT{unique_hash.upper()}"

fake = Faker('id_ID')

def generate_customer_id(existing_ids):
    while True:
        unique_string = f"CSTM{random.randint(10000, 99999)}{time.time_ns()}"
        customer_id = "CSTM" + hashlib.md5(unique_string.encode()).hexdigest()[:5].upper()
        if customer_id not in existing_ids:
            existing_ids.add(customer_id)
            return customer_id

def generate_full_name(gender):
    first_name = fake.first_name_male() if gender == 'Laki-laki' else fake.first_name_female()
    last_name = fake.last_name()
    middle_name = fake.first_name() if random.random() > 0.5 else '' 
    full_name = ' '.join(filter(None, [first_name, middle_name, last_name]))
    return full_name

def generate_order_id(existing_order_ids):
    while True:
        unique_string = f"{time.time_ns()}{random.randint(1000, 9999)}"
        order_hash = hashlib.md5(unique_string.encode()).hexdigest()[:10]  # Ambil 10 karakter pertama
        order_id = "ORDT" + order_hash.upper()  # Tambahkan prefix

        if order_id not in existing_order_ids:
            existing_order_ids.add(order_id)
            return order_id

def generate_order_date():
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2025, 2, 28)
    return fake.date_between(start_date=start_date, end_date=end_date)

def determine_order_status(order_date):
    if order_date < date(2025, 2, 15):
        return 'Selesai'
    elif order_date < date(2025, 2, 25):
        return 'Dikirim'
    else:
        return 'Pending'

import psycopg2
import pandas as pd


def create_database():
    try:
        conn = psycopg2.connect(dbname='postgres', user='postgres', password='qwerty123', host='localhost', port='5432')
        conn.autocommit = True
        cursor = conn.cursor()

        cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'ecommerce_exsport_db';")
        exists = cursor.fetchone()
        if not exists:
            cursor.execute("CREATE DATABASE ecommerce_exsport_db;")
            print("Database 'ecommerce_exsport_db' created successfully!")
        else:
            print("Database 'ecommerce_exsport_db' already exists.")

        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error creating database: {e}")

def create_tables():
    try:
        conn = psycopg2.connect(dbname='ecommerce_exsport_db', user='postgres', password='qwerty123', host='localhost', port='5432')
        cursor = conn.cursor()


        cursor.execute("""
        CREATE TABLE IF NOT EXISTS df_category_exsport (
            category_id VARCHAR(255) PRIMARY KEY,
            category VARCHAR(255) NOT NULL
        );

        CREATE TABLE IF NOT EXISTS df_color_exsport (
            color_id VARCHAR(255) PRIMARY KEY,
            color VARCHAR(255) NOT NULL
        );

        CREATE TABLE IF NOT EXISTS df_name_exsport (
            product_id VARCHAR(255) PRIMARY KEY,
            name_product VARCHAR(255) NOT NULL
        );
                       
        CREATE TABLE IF NOT EXISTS df_product_exsport (
            product_id VARCHAR(255) PRIMARY KEY,
            color_id VARCHAR(255) REFERENCES df_color_exsport(color_id),
            price_original FLOAT,
            price_sale FLOAT,
            discount TEXT,
            sold INT,
            rating TEXT
        );
                       
        CREATE TABLE IF NOT EXISTS df_stock_exsport (
            product_id VARCHAR(255) REFERENCES df_product_exsport(product_id),
            category_id VARCHAR(255) REFERENCES df_category_exsport(category_id),
            color_id VARCHAR(255) REFERENCES df_color_exsport(color_id),
            stock INT,
            PRIMARY KEY (product_id, category_id, color_id)
        );

        CREATE TABLE IF NOT EXISTS df_user_tokped (
            customer_id VARCHAR(255) PRIMARY KEY,
            name VARCHAR(255),
            age INT,
            gender VARCHAR(10),
            location VARCHAR(255),
            browsing_history TEXT,
            purchase_history TEXT
        );

        CREATE TABLE IF NOT EXISTS df_order_tokped (
            order_id VARCHAR(255) PRIMARY KEY,
            customer_id VARCHAR(255) REFERENCES df_user_tokped(customer_id),
            product_id VARCHAR(255) REFERENCES df_product_exsport(product_id),
            color_id VARCHAR(255) REFERENCES df_color_exsport(color_id),
            quantity INT,
            order_date DATE,
            order_status VARCHAR(50),
            shipping_method VARCHAR(50),
            payment_method VARCHAR(50),
            total_price FLOAT
        );

        CREATE TABLE IF NOT EXISTS df_review_order_tokped (
            order_id VARCHAR(255) REFERENCES df_order_tokped(order_id),
            reviewer_id VARCHAR(255) REFERENCES df_user_tokped(customer_id),
            product_id VARCHAR(255) REFERENCES df_product_exsport(product_id),
            review_text TEXT,
            rating FLOAT,
            review_date DATE,
            PRIMARY KEY (order_id, reviewer_id, product_id)
        );
        """)

        conn.commit()
        cursor.close()
        conn.close()
        print("Tables created successfully!")
    
    except Exception as e:
        print(f"Error creating tables: {e}")

def load_data_from_csv(table_name, file_path):
    try:
        print(f"Loading data from {file_path}")  
        conn = psycopg2.connect(dbname='ecommerce_exsport_db', user='postgres', password='qwerty123', host='localhost', port='5432')
        cursor = conn.cursor()


        df = pd.read_csv(file_path)
        print(f"CSV Data: {df.head()}")  


        columns = ", ".join(df.columns)
        values_placeholder = ", ".join(["%s"] * len(df.columns))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({values_placeholder}) ON CONFLICT DO NOTHING;"

        for _, row in df.iterrows():
            cursor.execute(query, tuple(row))

        conn.commit()
        cursor.close()
        conn.close()
        print(f"Data inserted into {table_name} successfully!")

    except Exception as e:
        print(f"Error inserting data into {table_name}: {e}")
