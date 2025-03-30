from extract_exsport import ExtractTokpedExsportData, ExtractTokpedStockExsportData
from func import extract_color, generate_product_id, generate_color_id, generate_category_id, extract_category
import pandas as pd
import luigi
import os

class TransformTokpedExportData(luigi.Task):
    def requires(self):
        return ExtractTokpedExsportData(), ExtractTokpedStockExsportData()
    
    def output(self):
        BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "transform-data"))
        os.makedirs(BASE_DIR, exist_ok=True)  # Pastikan folder ada

        return [
            luigi.LocalTarget(os.path.join(BASE_DIR, "data_product_exsport.csv")),
            luigi.LocalTarget(os.path.join(BASE_DIR, "data_stock_exsport.csv")),
            luigi.LocalTarget(os.path.join(BASE_DIR, "data_name_exsport.csv")),
            luigi.LocalTarget(os.path.join(BASE_DIR, "data_color_exsport.csv")),
            luigi.LocalTarget(os.path.join(BASE_DIR, "data_category_exsport.csv"))
        ]
    
    def run(self):
        # Mengekstrak data dari file csv
        df_product = pd.read_csv(self.input()[0].path)
        df_stock = pd.read_csv(self.input()[1].path)

        # Transformasi data
        # Ekstrak color dari nama produk
        df_color = {
            'color_id' : [generate_color_id(i) for i in range(len(df_product['name_product'].apply(extract_color).dropna().unique()))],
            'color' : df_product['name_product'].apply(extract_color).dropna().unique()
        }
        df_color = pd.DataFrame(df_color)

        # Ekstrak kategori dari stock
        df_category = {
            'category_id' : [generate_category_id(i) for i in range(len(df_stock['category'].unique()))],
            'category' : df_stock['category'].str.upper().dropna().unique()
        }
        df_category = pd.DataFrame(df_category)

        # Ekstrak nama produk
        name_product_unique = df_product['name_product'].str.replace('"', '').str.split('-').str[0].str.upper().dropna().unique()

        df_name = pd.DataFrame({
            'product_id': [generate_product_id(i) for i in range(len(name_product_unique))],
            'name_product': list(name_product_unique)  # Pastikan ini berbentuk list agar panjangnya sama
        })

        df_name = pd.DataFrame(df_name)

        # Transformasi df_product
        df_product['color'] = df_product['name_product'].apply(extract_color)
        df_product['color_id'] = df_product['color'].map(df_color.set_index('color')['color_id'])
        df_product['name_product'] = df_product['name_product'].str.replace('"', '').str.split('-').str[0].str.upper()
        df_product = df_product.merge(df_name, on='name_product', how='left')
        df_product = df_product.merge(df_color, on='color_id', how='left')
        df_product['price_original'] = df_product['price_original'].fillna(df_product['price_sale'])
        df_product['price_original'] = df_product['price_original'].astype(str).str.replace('Rp', '', regex=False).str.replace('.', '', regex=False).str.strip().apply(pd.to_numeric)
        df_product['price_sale'] = df_product['price_sale'].astype(str).str.replace('Rp', '', regex=False).str.replace('.', '', regex=False).str.strip().apply(pd.to_numeric)
        df_product['discount'] = df_product['discount'].fillna('No Discount')
        df_product['sold'] = df_product['sold'].fillna('0')
        df_product['sold'] = df_product['sold'].str.replace('terjual', '').str.strip().astype(str)
        df_product['rating'] = df_product['rating'].fillna('No Discount')
        df_product = df_product[['product_id', 'color_id', 'price_original', 'price_sale', 'discount', 'sold', 'rating']]

        # Transformasi df_stock
        df_stock['color'] = df_stock['name_product'].apply(extract_color)
        df_stock['name_product'] = df_stock['name_product'].str.replace('"', '').str.split('-').str[0].str.upper()
        df_stock = df_stock.merge(df_name, on='name_product', how='left')
        df_stock = df_stock.merge(df_category, on='category', how='left')
        df_stock = df_stock.merge(df_color, on='color', how='left')
        df_stock['stock'] = df_stock['stock'].fillna('0')
        df_stock = df_stock[['product_id', 'category_id', 'color_id', 'stock']]

        # Menyimpan data
        os.makedirs('transform-data', exist_ok=True)
        df_product.to_csv(self.output()[0].path, index=False)
        df_stock.to_csv(self.output()[1].path, index=False)
        df_name.to_csv(self.output()[2].path, index=False)
        df_color.to_csv(self.output()[3].path, index=False)
        df_category.to_csv(self.output()[4].path, index=False)

if __name__ == '__main__':
    luigi.build([TransformTokpedExportData()], workers=1, local_scheduler=True)