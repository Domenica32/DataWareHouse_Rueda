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
            "PROD_ID" : [],
            "PROD_NAME" : [],
            "PROD_DESC":[],
            "PROD_CATEGORY" :[],
            "PROD_CATEGORY_ID": [],
            "PROD_CATEGORY_DESC":[],
            "PROD_WEIGHT_CLASS":[],
            "SUPPLIER_ID":[],
            "PROD_STATUS":[],
            "PROD_LIST_PRICE":[],
            "PROD_MIN_PRICE":[]

        }
        
        product_tra = pd.read_sql(f"SELECT PROD_ID, PROD_NAME, PROD_DESC, PROD_CATEGORY, PROD_CATEGORY_ID, PROD_CATEGORY_DESC, PROD_WEIGHT_CLASS,SUPPLIER_ID,PROD_STATUS, PROD_LIST_PRICE, PROD_MIN_PRICE  FROM products_tra where CODIGO_ETL={codigoETL}",ses_db_stg)
        product_sor=pd.read_sql(f"SELECT PROD_ID, PROD_NAME, PROD_DESC, PROD_CATEGORY, PROD_CATEGORY_ID, PROD_CATEGORY_DESC, PROD_WEIGHT_CLASS,SUPPLIER_ID,PROD_STATUS, PROD_LIST_PRICE, PROD_MIN_PRICE FROM dim_products", ses_db_sor)
        product_sor.to_dict()
        if not product_tra.empty:
            for id,name,prod_desc,cat,cat_id,desc,weight,supp_id,sta,list,min \
                in zip(product_tra['PROD_ID'],product_tra['PROD_NAME'],
                product_tra['PROD_DESC'],product_tra['PROD_CATEGORY'],
                product_tra['PROD_CATEGORY_ID'],product_tra['PROD_CATEGORY_DESC'],
                product_tra['PROD_WEIGHT_CLASS'],product_tra['SUPPLIER_ID'],
                product_tra['PROD_STATUS'],product_tra['PROD_LIST_PRICE'],
                product_tra['PROD_MIN_PRICE']):
                dim_product_dict["PROD_ID"].append(id)
                dim_product_dict["PROD_NAME"].append(name)
                dim_product_dict["PROD_DESC"].append(prod_desc)
                dim_product_dict["PROD_CATEGORY"].append(cat)
                dim_product_dict["PROD_CATEGORY_ID"].append(cat_id)
                dim_product_dict["PROD_CATEGORY_DESC"].append(desc)
                dim_product_dict["PROD_WEIGHT_CLASS"].append(weight)
                dim_product_dict["SUPPLIER_ID"].append(supp_id)
                dim_product_dict["PROD_STATUS"].append(sta)
                dim_product_dict["PROD_LIST_PRICE"].append(list)
                dim_product_dict["PROD_MIN_PRICE"].append(min)
        if dim_product_dict ["PROD_ID"]:
            df_dim_product = pd.DataFrame(dim_product_dict)
            merge_product= df_dim_product.merge(product_sor, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)            
            merge_product.to_sql('dim_products', ses_db_sor, if_exists="append",index=False) 
    except:
        traceback.print_exc()
    finally:
        pass     
