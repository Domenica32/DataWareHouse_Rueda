from distutils.util import execute
from util.db_connection import Db_Connection
import pandas as pd
import traceback
from load.subKey_FK import *

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

def load_sales(codigoETL):
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
      
        dim_sales_dict = {
            "prod_id" : [],
            "cust_id" : [],
            "time_id" :[],
            "channel_id": [],
            "promo_id":[],
            "quantity_sold":[],
            "amount_sold":[]
        }
        
        sales_tra = pd.read_sql(f"SELECT PROD_ID, CUST_ID, TIME_ID, CHANNEL_ID, PROMO_ID,QUANTITY_SOLD, AMOUNT_SOLD   FROM sales_tra where CODIGO_ETL={codigoETL}",ses_db_stg)
        promo_data=pd.read_sql(f"SELECT ID,PROMO_ID FROM dim_promotions", ses_db_sor)
        customer_data=pd.read_sql(f"SELECT ID,CUST_ID FROM dim_customers", ses_db_sor)
        channel_data=pd.read_sql(f"SELECT ID,CHANNEL_ID FROM dim_channels", ses_db_sor)
        product_data=pd.read_sql(f"SELECT ID,PROD_ID FROM dim_products", ses_db_sor)

        if not sales_tra.empty:
            for prod,cust,time,channel,promo,quan,amou \
                in zip(sales_tra['PROD_ID'],sales_tra['CUST_ID'],
                sales_tra['TIME_ID'],sales_tra['CHANNEL_ID'],
                sales_tra["PROMO_ID"],sales_tra["QUANTITY_SOLD"],
                sales_tra["AMOUNT_SOLD"]):

                dim_sales_dict["prod_id"].append(product_subKey(prod,product_data))
                dim_sales_dict["cust_id"].append(customer_subKey(cust,customer_data))
                dim_sales_dict["time_id"].append(time)
                dim_sales_dict["channel_id"].append(channel_subKey (channel,channel_data))
                dim_sales_dict["promo_id"].append(promo_subKey(promo,promo_data))
                dim_sales_dict["quantity_sold"].append(quan)
                dim_sales_dict["amount_sold"].append(amou)
        if dim_sales_dict ["prod_id"]:
            df_dim_sales = pd.DataFrame(dim_sales_dict)
            df_dim_sales.to_sql('dim_sales', ses_db_sor, if_exists='append',index=False)
    except:
        traceback.print_exc()
    finally:
        pass     
