from faker import Faker
import pandas as pd
import luigi  
import random

class ExtractUserData(luigi.Task):
    def requires(self):
        return super().requires()
    
    def output(self):
        return luigi.LocalTarget("customer_profiles.csv")
    
    def run(self):
        # Generate data pelanggan
        df = generate_customer_data(1000)
        
        # Simpan ke CSV
        df.to_csv(self.output().path, index=False)


# Inisialisasi Faker untuk Indonesia
fake = Faker("id_ID")

# Daftar kategori untuk Browsing_History dan Purchase_History
categories = pd.read_csv("transform-data/data_category_exsport.csv")['category_id'].tolist()
purchase = pd.read_csv("transform-data/data_product_exsport.csv")['product_id'].tolist()



# Fungsi untuk membuat Customer_ID yang unik
def generate_customer_id(existing_ids):
    while True:
        letters = "CSTM"
        numbers = ''.join(random.choices("0123456789", k=3))
        customer_id = letters + numbers
        if customer_id not in existing_ids:
            existing_ids.add(customer_id)
            return customer_id

# Fungsi untuk membuat nama lengkap (2-3 kata)
def generate_full_name(gender):
    first_name = fake.first_name_male() if gender == "Laki-laki" else fake.first_name_female()
    last_name = fake.last_name()
    middle_name = fake.first_name() if random.random() > 0.5 else ""
    full_name = " ".join(filter(None, [first_name, middle_name, last_name]))
    return full_name

# Fungsi untuk membuat data pelanggan
def generate_customer_data(num_records=100):
    data = []
    existing_ids = set()
    existing_names = set()
    
    for _ in range(num_records):
        customer_id = generate_customer_id(existing_ids)
        age = random.randint(18, 60)
        gender = random.choice(["Laki-laki", "Perempuan"])
        while True:
            name = generate_full_name(gender)
            if name not in existing_names:
                existing_names.add(name)
                break
        location = fake.city()
        browsing_history = random.sample(categories, random.randint(1, 3))
        purchase_history = random.sample(purchase, random.randint(1, 2))

        data.append({
            "customer_id": customer_id,
            "name": name,
            "age": age,
            "gender": gender,
            "location": location,
            "browsing_history": ", ".join(browsing_history),
            "purchase_history": ", ".join(purchase_history)
        })
    
    return pd.DataFrame(data)

# Generate 100 data pelanggan
df = generate_customer_data(1000)

# Simpan ke CSV
df.to_csv("customer_profiles.csv", index=False)