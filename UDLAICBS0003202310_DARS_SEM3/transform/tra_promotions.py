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
promotions_conf = confParam ['csvs']['promotions_csv']
con_db_stg = Db_Connection(type, host, port, user, pwd, dbStg)

def promo_tra(codigoETL):
    try:
    
        ses_db_stg = con_db_stg.start()
        if ses_db_stg == -1:
            raise Exception (f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception ("Error trying to connect to the b2b_dwh_staging")

        # Dictionary for values of channels_ext
        promo_dict = {
            "codigo_etl" : [],
            "promo_id" : [],
            "promo_name" : [],
            "promo_cost" :[],
            "promo_begin_date": [],
            "promo_end_date":[]
        }
         # Reading de extract promotions from de sql table
        promo_ext = pd.read_sql("SELECT PROMO_ID, PROMO_NAME, PROMO_COST, PROMO_BEGIN_DATE, PROMO_END_DATE  FROM promotions_ext",ses_db_stg)
        #Processing the sql  content
        if not promo_ext.empty:
            for id,name,cost,begin,end \
                in zip(promo_ext['PROMO_ID'],promo_ext['PROMO_NAME'],
                promo_ext['PROMO_COST'],promo_ext['PROMO_BEGIN_DATE'],
                promo_ext['PROMO_END_DATE']):
                promo_dict["codigo_etl"].append(codigoETL)
                promo_dict["promo_id"].append(id)
                promo_dict["promo_name"].append(name)
                promo_dict["promo_cost"].append(cost)
                promo_dict["promo_begin_date"].append(month_date_numeric(begin))
                promo_dict["promo_end_date"].append(month_date_numeric(end))
        if promo_dict ["promo_id"]:
            df_promo_ext = pd.DataFrame(promo_dict)
            df_promo_ext.to_sql('promotions_tra', ses_db_stg, if_exists='append',index=False)
           
    except:
        traceback.print_exc()
    finally:
        pass     
