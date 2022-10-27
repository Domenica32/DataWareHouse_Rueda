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

confParam_sor = configparser.ConfigParser()
confParam_sor.read('conf.properties')
type = confParam_sor ['MySqlParameters']['type']
host =confParam_sor ['MySqlParameters']['host']
port = confParam_sor ['MySqlParameters']['port']
user = confParam_sor ['MySqlParameters']['user']
pwd = confParam_sor ['MySqlParameters']['pwd']
dbSor = confParam_sor ['MySqlParameters']['dbSor']
con_db_sor = Db_Connection(type, host, port, user, pwd, dbSor)

def load_products(codigoETL):
    try:
    
        ses_db_stg = con_db_stg.start()
        if ses_db_stg == -1:
            raise Exception (f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception ("Error trying to connect to the b2b_dwh_staging")


        ses_db_sor = con_db_sor.start()
        if ses_db_sor == -1:
            raise Exception (f"The give database type {type} is not valid")
        elif ses_db_sor == -2:
            raise Exception ("Error trying to connect to the b2b_dwh_sor")
      
        dim_product_dict = {
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
        
        product_tra = pd.read_sql(f"SELECT PROD_ID, PROD_NAME, PROD_DESC, PROD_CATEGORY, PROD_CATEGORY_ID, PROD_CATEGORY_DESC, PROD_WEIGHT_CLASS,SUPPLIER_ID,PROD_STATUS, PROD_LIST_PRICE, PROD_MIN_PRICE  FROM products_tra where CODIGO_ETL={codigoETL}",ses_db_stg)

        if not product_tra.empty:
            for id,name,prod_desc,cat,cat_id,desc,weight,supp_id,sta,list,min \
                in zip(product_tra['PROD_ID'],product_tra['PROD_NAME'],
                product_tra['PROD_DESC'],product_tra['PROD_CATEGORY'],
                product_tra['PROD_CATEGORY_ID'],product_tra['PROD_CATEGORY_DESC'],
                product_tra['PROD_WEIGHT_CLASS'],product_tra['SUPPLIER_ID'],
                product_tra['PROD_STATUS'],product_tra['PROD_LIST_PRICE'],
                product_tra['PROD_MIN_PRICE']):
                dim_product_dict["prod_id"].append(id)
                dim_product_dict["prod_name"].append(name)
                dim_product_dict["prod_desc"].append(prod_desc)
                dim_product_dict["prod_category"].append(cat)
                dim_product_dict["prod_category_id"].append(cat_id)
                dim_product_dict["prod_category_desc"].append(desc)
                dim_product_dict["prod_weight_class"].append(weight)
                dim_product_dict["supplier_id"].append(supp_id)
                dim_product_dict["prod_status"].append(sta)
                dim_product_dict["prod_list_price"].append(list)
                dim_product_dict["prod_min_price"].append(min)
        if dim_product_dict ["prod_id"]:
            df_dim_product = pd.DataFrame(dim_product_dict)
            df_dim_product.to_sql('dim_products', ses_db_sor, if_exists='append',index=False)
    except:
        traceback.print_exc()
    finally:
        pass     
