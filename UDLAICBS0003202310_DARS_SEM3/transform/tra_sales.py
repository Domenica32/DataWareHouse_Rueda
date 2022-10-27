from distutils.util import execute
from util.db_connection import Db_Connection
from transform.transformation import *
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

def sales_tra(codigoETL):
    try:
    
        ses_db_stg = con_db_stg.start()
        if ses_db_stg == -1:
            raise Exception (f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception ("Error trying to connect to the b2b_dwh_staging")

        # Dictionary for values of channels_ext
        sales_dict = {
            "codigo_etl" : [],
            "prod_id" : [],
            "cust_id" : [],
            "time_id" :[],
            "channel_id": [],
            "promo_id":[],
            "quantity_sold":[],
            "amount_sold":[]
        }
        # Reading de extract customers from de sql table
        sales_ext = pd.read_sql("SELECT PROD_ID, CUST_ID, TIME_ID, CHANNEL_ID, PROMO_ID,QUANTITY_SOLD, AMOUNT_SOLD   FROM sales_ext",ses_db_stg)
        #Processing the sql  content
        if not sales_ext.empty:
            for prod,cust,time,channel,promo,quan,amou \
                in zip(sales_ext['PROD_ID'],sales_ext['CUST_ID'],
                sales_ext['TIME_ID'],sales_ext['CHANNEL_ID'],
                sales_ext["PROMO_ID"],sales_ext["QUANTITY_SOLD"],
                sales_ext["AMOUNT_SOLD"]):
                sales_dict["codigo_etl"].append(codigoETL)
                sales_dict["prod_id"].append(prod)
                sales_dict["cust_id"].append(cust)
                sales_dict["time_id"].append(date_numeric(time))
                sales_dict["channel_id"].append(channel)
                sales_dict["promo_id"].append(promo)
                sales_dict["quantity_sold"].append(quan)
                sales_dict["amount_sold"].append(amou)
        if sales_dict ["prod_id"]:
            df_sales_ext = pd.DataFrame(sales_dict)
            df_sales_ext.to_sql('sales_tra', ses_db_stg, if_exists='append',index=False)
           
    except:
        traceback.print_exc()
    finally:
        pass     
