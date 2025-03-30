from func import generate_customer_id, generate_full_name, generate_order_id, generate_order_date, determine_order_status
from transform_exsport import TransformTokpedExportData
from datetime import datetime
from faker import Faker
import pandas as pd
import luigi  
import random
import os

fake = Faker('id_ID')

class ExtractUserData(luigi.Task):
    def requires(self):
        return TransformTokpedExportData()
    
    def output(self):
        BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'clean-data'))
        os.makedirs(BASE_DIR, exist_ok=True)

        return luigi.LocalTarget(os.path.join(BASE_DIR, 'data_user_tokped.csv'))
    
    def run(self):

        df_category = pd.read_csv(self.input()[4].path)
        categories = df_category['category_id'].tolist()
        df_purchase = pd.read_csv(self.input()[0].path)
        purchase = df_purchase['product_id'].tolist()

        def generate_customer_data(num_records=100):
            data = []
            existing_ids = set()
            existing_names = set()
            
            for _ in range(num_records):
                customer_id = generate_customer_id(existing_ids)
                age = random.randint(16, 40)
                gender = random.choice(['Laki-laki', 'Perempuan'])
                while True:
                    name = generate_full_name(gender)
                    if name not in existing_names:
                        existing_names.add(name)
                        break

                location = fake.city()
                browsing_history = random.sample(categories, random.randint(1, 3))
                purchase_history = random.sample(purchase, random.randint(1, 2))

                data.append({
                    'customer_id': customer_id,
                    'name': name,
                    'age': age,
                    'gender': gender,
                    'location': location,
                    'browsing_history': ', '.join(browsing_history),
                    'purchase_history': ', '.join(purchase_history)
                })
    
            return pd.DataFrame(data)
        
        user_df = generate_customer_data(1000)
        user_df.to_csv(self.output().path, index=False)

class ExtractOrderData(luigi.Task):
    def requires(self):
        return TransformTokpedExportData(), ExtractUserData()
    
    def output(self):
        BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'clean-data'))
        os.makedirs(BASE_DIR, exist_ok=True)

        return luigi.LocalTarget(os.path.join(BASE_DIR, 'data_order_tokped.csv'))
    
    def run(self):

        df_product = pd.read_csv(self.input()[0][0].path)
        df_product['sold'] = df_product['sold'].astype(int)
        df_product = df_product[df_product['sold'] > 0]

        df_customer = pd.read_csv(self.input()[1].path)
        customer_ids = df_customer['customer_id'].tolist() 

        shipping_methods = ['JNE', 'J&T', 'SiCepat']
        payment_methods = ['BCA', 'Mandiri', 'BNI', 'BRI', 'GoPay', 'OVO', 'DANA', 'COD']

        def create_order(customer_id, sold_tracker):
            order_id = generate_order_id()
            order_date = generate_order_date()
            order_status = determine_order_status(order_date)
            shipping_method = random.choice(shipping_methods)
            payment_method = random.choice(payment_methods)
            
            num_products = random.randint(1, 3)  # Maks 3 produk dalam satu order
            available_products = [p for p, s in sold_tracker.items() if s > 0]
            random.shuffle(available_products)
            
            order_data = []
            for _ in range(num_products):
                if not available_products:
                    break
                
                product_id = available_products.pop()
                quantity = random.randint(1, min(3, sold_tracker[product_id]))
                price_sale = df_product.loc[df_product['product_id'] == product_id, 'price_sale'].values[0]
                
                order_data.append({
                    'order_id': order_id,
                    'customer_id': customer_id,
                    'product_id': product_id,
                    'quantity': quantity,
                    'order_date': order_date.strftime('%Y-%m-%d'),
                    'order_status': order_status,
                    'shipping_method': shipping_method,
                    'payment_method': payment_method,
                    'total_price': price_sale * quantity
                })
                
                sold_tracker[product_id] -= quantity
            
            return order_data
        
        def generate_order_data(num_orders=3000):
            data = []
            sold_tracker = df_product.set_index('product_id')['sold'].astype(int).to_dict()
            
            selected_customers = random.sample(customer_ids, 1000)
            
            for customer_id in selected_customers:
                order_data = create_order(customer_id, sold_tracker)
                if order_data:
                    data.extend(order_data)
            
            remaining_orders = num_orders - len(data)
            for _ in range(remaining_orders):
                customer_id = random.choice(customer_id)
                order_data = create_order(customer_id, sold_tracker)
                if order_data:
                    data.extend(order_data)
            
            return pd.DataFrame(data)

        order_df = generate_order_data(3000)
        order_df.to_csv(self.output().path, index=False)

class ExtractReviewOrder(luigi.Task):
    def requires(self):
        return TransformTokpedExportData(), ExtractOrderData()
    
    def output(self):
        BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'clean-data'))
        os.makedirs(BASE_DIR, exist_ok=True)

        return luigi.LocalTarget(os.path.join(BASE_DIR, 'data_review_order_tokped.csv'))
    
    def run(self):
        df_product = pd.read_csv(self.input()[0][0].path)
        df_order = pd.read_csv(self.input()[1].path)

        df_product = df_product[df_product['rating'] != 'No Rating'].copy()
        df_product['rating'] = df_product['rating'].astype(float)
        product_rating_map = df_product.set_index('product_id')['rating'].to_dict()

        review_texts = [
            'Tasnya sangat bagus dan kuat!',
            'Bahan berkualitas, jahitan rapi. Sangat puas!',
            'Ukuran sesuai deskripsi, bisa muat banyak barang.',
            'Warnanya cantik, persis seperti di foto.',
            'Pengiriman cepat dan kemasannya rapi!',
            'Lumayan, tapi kualitas tali kurang kuat.',
            'Tasnya nyaman dipakai, cocok untuk sehari-hari.',
            'Harga sesuai dengan kualitas, tidak mengecewakan.',
            'Bagus, tapi harap diperhatikan kualitas resletingnya.',
            'Modelnya trendi, saya suka!'
        ]

        def generate_review(order):
            order_id = order['order_id']
            customer_id = order['customer_id']
            product_id = order['product_id']
            order_date = datetime.strptime(order['order_date'], '%Y-%m-%d').date()  # Konversi ke date
            
            max_rating = product_rating_map.get(product_id, 5)
            rating = round(random.uniform(1, max_rating) * 2) / 2 
            
            return {
                'order_id': order_id,
                'reviewer_id': customer_id,
                'product_id': product_id,
                'review_text': random.choice(review_texts),
                'rating': rating,
                'review_date': fake.date_between(start_date=order_date)
            }
        
        df_review = df_order.apply(generate_review, axis=1)
        df_review = pd.DataFrame(df_review.tolist())\
        
        df_review.to_csv(self.output().path, index=False)


if __name__ == '__main__':
    luigi.build([ExtractUserData(),
                 ExtractOrderData(),
                 ExtractReviewOrder()],
                 workers=1, local_scheduler=True)