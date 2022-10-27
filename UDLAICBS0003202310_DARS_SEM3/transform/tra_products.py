from distutils.util import execute
from util.db_connection import Db_Connection
import pandas as pd
import traceback
import configparser

confParam = configparser.ConfigParser()
confParam.read('conf.properties')

type = confParam ['MySqlParameters']['type']
host =confParam ['MySqlParameters']['host']
port = confParam ['MySqlParameters']['port']
user = confParam ['MySqlParameters']['user']
pwd = confParam ['MySqlParameters']['pwd']
dbStg = confParam ['MySqlParameters']['dbStg']
con_db_stg = Db_Connection(type, host, port, user, pwd, dbStg)

def products_tra(codigoETL):
    try:
   
        ses_db_stg = con_db_stg.start()
        if ses_db_stg == -1:
            raise Exception (f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception ("Error trying to connect to the b2b_dwh_staging")

        # Dictionary for values of channels_ext
        products_dict = {
            "codigo_etl":[],
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
         # Reading de extract customers from de sql table
        products_ext = pd.read_sql("SELECT PROD_ID, PROD_NAME, PROD_DESC, PROD_CATEGORY, PROD_CATEGORY_ID, PROD_CATEGORY_DESC, PROD_WEIGHT_CLASS,SUPPLIER_ID,PROD_STATUS, PROD_LIST_PRICE, PROD_MIN_PRICE FROM products_ext",ses_db_stg)
        #Processing the sql  content
        if not products_ext.empty:
            for id,name,prod_desc,cat,cat_id,desc,weight,supp_id,sta,list,min \
                in zip(products_ext['PROD_ID'],products_ext['PROD_NAME'],
                products_ext['PROD_DESC'],products_ext['PROD_CATEGORY'],
                products_ext['PROD_CATEGORY_ID'],products_ext['PROD_CATEGORY_DESC'],
                products_ext['PROD_WEIGHT_CLASS'],products_ext['SUPPLIER_ID'],
                products_ext['PROD_STATUS'],products_ext['PROD_LIST_PRICE'],
                products_ext['PROD_MIN_PRICE']):
                products_dict["codigo_etl"].append(codigoETL)
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
            df_products_ext = pd.DataFrame(products_dict)
            df_products_ext.to_sql('products_tra', ses_db_stg, if_exists='append',index=False)
            
    except:
        traceback.print_exc()
    finally:
        pass     
