from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from func import extract_category
from selenium import webdriver
from faker import Faker
import pandas as pd
import random
import luigi
import time
import os


class ExtractTokpedExsportData(luigi.Task):
    def requires(self):
        pass # Tidak ada task yang diperlukan
    
    def output(self):
        BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "extract-raw-data"))
        OUTPUT_PATH = os.path.join(BASE_DIR, "exsport_tokped_raw.csv")
        
        return luigi.LocalTarget(OUTPUT_PATH) # Tempat penyimpanan data yang diekstrak


    def run(self):
        base_url = "https://www.tokopedia.com/exsportstore/product/page/{}" # URL dasar untuk mengambil data produk exsport dari Tokopedia

        # Mengatur opsi untuk webdriver Chrome
        options = webdriver.ChromeOptions()
        options.add_argument('--disable-blink-features=AutomationControlled') # Menonaktifkan fitur otomatisasi
        options.add_experimental_option('useAutomationExtension', False) # Menonaktifkan ekstensi otomatisasi
        options.add_experimental_option("excludeSwitches", ["enable-automation"]) # Mengecualikan switch otomatisasi
        driver = webdriver.Chrome(options=options) # Membuat instance dari webdriver Chrome

        product_data = [] # List untuk menyimpan data produk

        try:
            for page in range(1, 12): # Mengambil data dari halaman 1 hingga 11
                url = base_url.format(page) # Membuat URL untuk halaman saat ini
                driver.get(url) # Mengakses URL

                # Menunggu hingga elemen body muncul
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.TAG_NAME, 'body'))
                )

                # Menggulir halaman untuk memuat lebih banyak produk
                for _ in range(5):
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);") # Menggulir ke bawah
                    time.sleep(2) # Menunggu 2 detik
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 2);") # Menggulir ke atas
                    time.sleep(2) # Menunggu 2 detik

                # Mengambil elemen produk
                product_containers = driver.find_elements(By.CSS_SELECTOR, "[data-testid='divProductWrapper']")

                for container in product_containers: # Iterasi setiap elemen produk
                    try:
                        name = container.find_element(By.CSS_SELECTOR, "[data-testid='linkProductName']").text # Mengambil nama produk
                    except:
                        name = None # Jika gagal, set nama menjadi None
                    
                    # Mengambil link produk dari elemen
                    try:
                        link = container.find_element(By.CSS_SELECTOR, "a.pcv3__info-content").get_attribute('href') # Mencari elemen link produk dan mengambil atribut 'href'
                    except:
                        link = None # Jika gagal, set link menjadi None

                    # Mengambil harga jual produk dari elemen
                    try:
                        price_sale_elem = container.find_element(By.CSS_SELECTOR, "[data-testid='linkProductPrice']") # Mencari elemen harga jual produk
                        price_sale = price_sale_elem.text if price_sale_elem else None # Mengambil teks dari elemen harga jual produk
                    except:
                        price_sale = None # Jika gagal, set harga jual menjadi None

                    # Mengambil harga asli produk dari elemen
                    try:
                        price_elem = container.find_element(By.CSS_SELECTOR, "[data-testid='lblProductSlashPrice']") # Mencari elemen harga asli produk
                        price = price_elem.text if price_elem else None # Mengambil teks dari elemen harga asli produk
                    except:
                        price = None # Jika gagal, set harga asli menjadi None

                    try:
                        discount_elem = container.find_element(By.CSS_SELECTOR, "[data-testid='lblProductDiscount']") # Mencari elemen diskon produk
                        discount = discount_elem.text if discount_elem else None # Mengambil teks dari elemen diskon produk
                    except:
                        discount = None # Jika gagal, set diskon menjadi None

                    # Mengambil rating produk dari elemen
                    try:
                        rating_elem = container.find_element(By.CSS_SELECTOR, ".prd_rating-average-text") # Mencari elemen rating produk
                        rating = rating_elem.text if rating_elem else None # Mengambil teks dari elemen rating produk
                    except:
                        rating = None # Jika gagal, set rating menjadi None
                    
                    # Mengambil jumlah produk yang terjual dari elemen
                    try:
                        sold_elem = container.find_element(By.CSS_SELECTOR, ".prd_label-integrity") # Mencari elemen jumlah produk yang terjual
                        sold = sold_elem.text if sold_elem else None # Mengambil teks dari elemen jumlah produk yang terjual
                    except:
                        sold = None # Jika gagal, set jumlah produk yang terjual menjadi None

                    # Mengambil link gambar produk dari elemen
                    try:
                        image_elem = container.find_element(By.CSS_SELECTOR, ".css-1q90pod") # Mencari elemen gambar produk
                        image = image_elem.get_attribute('src') if image_elem else None # Mengambil atribut 'src' dari elemen gambar produk
                    except:
                        image = None # Jika gagal, set link gambar menjadi None

                    # Menambahkan data produk ke dalam list product_data
                    product_data.append({
                        'name_product': name, # Nama produk
                        'product_link': link, # Link produk
                        'price_sale': price_sale, # Harga jual
                        'price_original': price, # Harga asli
                        'discount': discount, # Diskon
                        'sold': sold, # Jumlah produk yang terjual
                        'rating': rating, # Rating produk
                        'image_link': image # Link gambar produk
                    })

            # Mengonversi list product_data ke dalam DataFrame
            exsport_tokped_df = pd.DataFrame(product_data)

            # Menyimpan DataFrame ke dalam file CSV
            exsport_tokped_df.to_csv(self.output().path, index=False)

        except Exception as e:
            print(f"Terjadi kesalahan: {e}") # Menampilkan pesan kesalahan jika terjadi kesalahan
        
        finally:
            driver.quit() # Menutup browser


# class ExtractTokpedStockExsportData(luigi.Task):
#     def requires(self):
#         return ExtractTokpedExsportData() # Task yang diperlukan
    
#     def output(self):
#         BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "extract-raw-data"))
#         OUTPUT_PATH = os.path.join(BASE_DIR, "exsport_stock_tokped.csv")

#         return luigi.LocalTarget(OUTPUT_PATH) # Tempat penyimpanan data yang diekstrak


#     def run(self):
#         extract_data = pd.read_csv(self.input().path) # Membaca data produk yang diekstrak sebelumnya
#         pages = extract_data['product_link'].tolist()

#         options = webdriver.ChromeOptions()
#         options.add_argument('--disable-blink-features=AutomationControlled')
#         options.add_experimental_option('useAutomationExtension', False)
#         options.add_experimental_option("excludeSwitches", ["enable-automation"])
#         options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
#         # options.add_argument("--headless=new")  # Headless mode untuk mencegah deteksi bot
#         options.page_load_strategy = 'eager'  # Mempercepat loading

#         driver = webdriver.Chrome(options=options)

#         stock_data = []

#         try:
#             for page in pages:  # Loop untuk setiap link dalam list
#                 driver.get(page)
#                 time.sleep(5)   

#                 try:
#                     WebDriverWait(driver, 30).until(
#                         EC.presence_of_element_located((By.TAG_NAME, 'body'))
#                     )
#                 except:
#                     print(f"Gagal memuat halaman {page}. Lewati")
#                     continue  # Lanjutkan ke halaman berikutnya jika halaman tidak dapat dimuat

#                         # Scroll untuk memuat halaman
#                 for _ in range(5):
#                     driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
#                     time.sleep(2)
#                     driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 2);")
#                     time.sleep(2)  # Tunggu sebentar untuk memastikan elemen muncul

#                 try:
#                     container = driver.find_element(By.CLASS_NAME, "css-856ghu")
#                 except:
#                     print(f"Elemen utama tidak ditemukan di {page}")
#                     continue  # Skip ke URL berikutnya jika elemen tidak ada

#                 try:
#                     name = container.find_element(By.CSS_SELECTOR, "[data-testid='lblPDPDetailProductName']").text.strip()
#                 except:
#                     name = None

#                 try:
#                     stock_elem = container.find_element(By.CSS_SELECTOR, "[data-testid='stock-label']")
#                     stock_text = stock_elem.text.strip()
#                     stock = stock_text.split(":")[-1].strip() if "Stok Total" in stock_text else stock_text
#                 except:
#                     stock = None

#                 try:
#                     etalase_elem = container.find_element(By.CSS_SELECTOR, "li[class*='css-'] a b")
#                     etalase = etalase_elem.text.strip()
#                 except:
#                     etalase = None

#                 # try:
#                 #     description_elem = container.find_element(By.CSS_SELECTOR, "[data-testid='lblPDPDescriptionProduk']")
#                 #     text_description = description_elem.text.strip()
#                 # except:
#                 #     text_description = None

#                 stock_data.append({
#                     'name_product': name,
#                     'stock': stock,
#                     'kategori': etalase
#                 })

#             exsport_stock_tokped_df = pd.DataFrame(stock_data)
#             exsport_stock_tokped_df.to_csv(self.output().path, index=False)
            
#         except Exception as e:
#             print(f"Terjadi kesalahan: {e}")

#         finally:
#             driver.quit() # Menutup browser

# class ExtractTokpedStockExsportData(luigi.Task):
#     def requires(self):
#         return ExtractTokpedExsportData()

#     def output(self):
#         BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "extract-raw-data"))
#         OUTPUT_PATH = os.path.join(BASE_DIR, "exsport_stock_tokped.csv")
#         return luigi.LocalTarget(OUTPUT_PATH)

#     def run(self):
#         extract_data = pd.read_csv(self.input().path)
#         pages = extract_data['product_link'].tolist()

#         options = webdriver.ChromeOptions()
#         options.add_argument('--disable-blink-features=AutomationControlled')
#         options.add_experimental_option('useAutomationExtension', False)
#         options.add_experimental_option("excludeSwitches", ["enable-automation"])
#         options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
#         options.page_load_strategy = 'eager'

#         driver = webdriver.Chrome(options=options)
#         driver.set_page_load_timeout(60)
#         driver.implicitly_wait(10)

#         stock_data = []

#         def slow_scroll(driver):
#             for _ in range(5):
#                 driver.execute_script("window.scrollBy(0, document.body.scrollHeight/5);")
#                 time.sleep(1.5)

#         try:
#             for idx, page in enumerate(pages):
#                 # Restart driver setiap 10 halaman untuk menghindari crash
#                 if idx > 0 and idx % 10 == 0:
#                     driver.quit()
#                     driver = webdriver.Chrome(options=options)

#                 try:
#                     driver.get(page)
#                     time.sleep(random.uniform(5, 7))  # Random delay untuk menghindari deteksi bot
                    
#                     # Tunggu hingga elemen utama produk muncul
#                     WebDriverWait(driver, 30).until(
#                         EC.presence_of_element_located((By.CSS_SELECTOR, "[data-testid='lblPDPDetailProductName']"))
#                     )
#                     print(f"✅ Halaman berhasil dimuat: {page}")

#                     # Scroll untuk memuat konten lebih baik
#                     slow_scroll(driver)

#                     # Ambil elemen utama
#                     try:
#                         container = driver.find_element(By.CLASS_NAME, "css-856ghu")
#                     except:
#                         print(f"⚠️ Elemen utama tidak ditemukan di {page}")
#                         continue

#                     # Ambil Nama Produk
#                     try:
#                         name = container.find_element(By.CSS_SELECTOR, "[data-testid='lblPDPDetailProductName']").text.strip()
#                     except:
#                         name = None

#                     # Ambil Stok Produk
#                     try:
#                         stock_elem = container.find_element(By.CSS_SELECTOR, "[data-testid='stock-label']")
#                         stock_text = stock_elem.text.strip()
#                         stock = stock_text.split(":")[-1].strip() if "Stok Total" in stock_text else stock_text
#                     except:
#                         stock = None

#                     # Ambil Kategori Produk
#                     try:
#                         etalase_elem = container.find_element(By.CSS_SELECTOR, "li[class*='css-'] a b")
#                         etalase = etalase_elem.text.strip()
#                     except:
#                         etalase = None

#                     stock_data.append({
#                         'name_product': name,
#                         'stock': stock,
#                         'kategori': etalase
#                     })

#                 except Exception as e:
#                     print(f"❌ Gagal mengambil data dari {page} - {str(e)}")

#             # Simpan hasil scraping ke CSV
#             exsport_stock_tokped_df = pd.DataFrame(stock_data)
#             exsport_stock_tokped_df.to_csv(self.output().path, index=False)

#         finally:
#             driver.quit()

class ExtractTokpedStockExsportData(luigi.Task):
    def requires(self):
        return ExtractTokpedExsportData()
    
    def output(self):
        BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "extract-raw-data"))
        OUTPUT_PATH = os.path.join(BASE_DIR, "exsport_stock_tokped.csv")

        return luigi.LocalTarget(OUTPUT_PATH)
    
    def run(self):
        fake = Faker()  # Instance dari Faker
        extract_data = pd.read_csv(self.input().path)
        num_products = len(extract_data)

        dummy_data = []
        for i in range(num_products):
            name_product = fake.sentence(nb_words=3).replace(".", "")  # Nama produk acak tanpa titik
            stock = random.randint(0, 50)  # Stok antara 0-50
            description = fake.sentence(nb_words=10)  # Deskripsi produk singkat
            kategori = extract_category(name_product)  # Menentukan kategori berdasarkan nama produk
            
            dummy_data.append({
                "name_product": name_product,
                "stock": stock,
                "description": description,
                "kategori": kategori
            })

        # Konversi ke DataFrame dan simpan ke CSV
        dummy_df = pd.DataFrame(dummy_data)
        dummy_df.to_csv(self.output().path, index=False)
            
if __name__ == '__main__':
    luigi.build([ExtractTokpedExsportData(),
                 ExtractTokpedStockExsportData()],
                 workers=1, local_scheduler=True)
