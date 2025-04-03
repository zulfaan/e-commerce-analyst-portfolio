from transform_exsport import TransformTokpedExportData
from generate_data import *
from func import *
import pandas as pd
import psycopg2
import luigi
import os

class LoadExsportDatabase(luigi.Task):
    def requires(self):
        return TransformTokpedExportData(), ExtractUserData(), ExtractOrderData(), ExtractReviewOrder() 
    
    def output(self):
        return print('All processes completed successfully!')
    
    def run(self):
        create_database()
        create_tables()

        load_data_from_csv('df_category_exsport', self.input()[0][4].path)
        load_data_from_csv('df_color_exsport', self.input()[0][3].path)
        load_data_from_csv('df_name_exsport', self.input()[0][2].path)
        load_data_from_csv('df_product_exsport', self.input()[0][0].path)
        load_data_from_csv('df_stock_exsport', self.input()[0][1].path)
        load_data_from_csv('df_user_tokped', self.input()[1].path)
        load_data_from_csv('df_order_tokped', self.input()[2].path)
        load_data_from_csv('df_review_order_tokped', self.input()[3].path)

class LoadExsportExcel(luigi.Task):
    def requires(self):
        pass

    def output(self):
        BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'database'))
        os.makedirs(BASE_DIR, exist_ok=True)

        file_path = os.path.join(BASE_DIR, 'ecommerce_exsport.xlsx')
        return luigi.LocalTarget(file_path)  
    
    def run(self):
        db_params = {
            'dbname': 'ecommerce_exsport_db',
            'user': 'postgres',
            'password': 'qwerty123',
            'host': 'localhost',
            'port': '5432'
        }

        conn = psycopg2.connect(**db_params)

        tables = [
            'df_category_exsport',
            'df_color_exsport',
            'df_name_exsport',
            'df_product_exsport',
            'df_stock_exsport',
            'df_user_tokped',
            'df_order_tokped',
            'df_review_order_tokped'
        ]

        excel_path = self.output().path

        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            for table in tables:
                query = f'SELECT * FROM {table}'
                df = pd.read_sql(query, conn)

                df.to_excel(writer, sheet_name=table, index=False)


        conn.close()


if __name__ == '__main__':
    luigi.build([
        LoadExsportDatabase(),
        LoadExsportExcel()], local_scheduler=True)

