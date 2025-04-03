from transform_exsport import TransformTokpedExportData
from generate_data import *
from func import *
import luigi

class LoadExsportDatabase(luigi.Task):
    def requires(self):
        return TransformTokpedExportData(), ExtractUserData(), ExtractOrderData(), ExtractReviewOrder() 
    
    def output(self):
        return print("All processes completed successfully!")
    
    def run(self):
        create_database()
        create_tables()

        load_data_from_csv("df_category_exsport", self.input()[0][4].path)
        load_data_from_csv("df_color_exsport", self.input()[0][3].path)
        load_data_from_csv("df_name_exsport", self.input()[0][2].path)
        load_data_from_csv("df_product_exsport", self.input()[0][0].path)
        load_data_from_csv("df_stock_exsport", self.input()[0][1].path)
        load_data_from_csv("df_user_tokped", self.input()[1].path)
        load_data_from_csv("df_order_tokped", self.input()[2].path)
        load_data_from_csv("df_review_order_tokped", self.input()[3].path)

if __name__ == '__main__':
    luigi.run([LoadExsportDatabase()], workers=1, local_scheduler=True)
