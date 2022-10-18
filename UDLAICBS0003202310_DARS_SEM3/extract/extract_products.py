from distutils.util import execute
from util.db_connection import Db_Connection
import pandas as pd
import traceback


def ext_products():
    try:
    #Variables
        type = 'mysql'
        host = 'localhost'
        port = '3306'
        user = 'root'
        pwd = '1234'
        db = 'darsdbstg'


        con_db_stg = Db_Connection(type, host, port, user, pwd, db)
        ses_db_stg = con_db_stg.start()
        if ses_db_stg == -1:
            raise Exception (f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception ("Error trying to connect to the b2b_dwh_staging")

        # Dictionary for values of channels_ext
        products_dict = {
            "prod_id" : [],
            "prod_name" : [],
            "prod_desc":[],
            "prod_category" :[],
            "prod_category_id": [],
            "prod_category_desc":[],
            "prod_weight_class":[],
            "supplier_id":[],
            "prod_status":[],
            "prod_list_price":[],
            "prod_min_price":[]
        }
        # Reading the CSV file
        products_csv = pd.read_csv("csvs/products.csv")
        #print (channel_csv)
        #Processing the CSV file content
        if not products_csv.empty:
            for id,name,prod_desc,cat,cat_id,desc,weight,supp_id,sta,list,min \
                in zip(products_csv['PROD_ID'],products_csv['PROD_NAME'],
                products_csv['PROD_DESC'],products_csv['PROD_CATEGORY'],
                products_csv['PROD_CATEGORY_ID'],products_csv['PROD_CATEGORY_DESC'],
                products_csv['PROD_WEIGHT_CLASS'],products_csv['SUPPLIER_ID'],
                products_csv['PROD_STATUS'],products_csv['PROD_LIST_PRICE'],
                products_csv['PROD_MIN_PRICE']):
                products_dict["prod_id"].append(id)
                products_dict["prod_name"].append(name)
                products_dict["prod_desc"].append(prod_desc)
                products_dict["prod_category"].append(cat)
                products_dict["prod_category_id"].append(cat_id)
                products_dict["prod_category_desc"].append(desc)
                products_dict["prod_weight_class"].append(weight)
                products_dict["supplier_id"].append(supp_id)
                products_dict["prod_status"].append(sta)
                products_dict["prod_list_price"].append(list)
                products_dict["prod_min_price"].append(min)



        if products_dict ["prod_id"]:
            ses_db_stg.connect().execute("TRUNCATE TABLE products_ext")
            df_channels_ext = pd.DataFrame(products_dict)
            df_channels_ext.to_sql('products_ext', ses_db_stg, if_exists='append',index=False)
    except:
        traceback.print_exc()
    finally:
        pass     
