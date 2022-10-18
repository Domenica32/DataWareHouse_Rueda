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
promotions_conf = confParam ['csvs']['promotions_csv']
con_db_stg = Db_Connection(type, host, port, user, pwd, dbStg)

def ext_promo():
    try:
    
        ses_db_stg = con_db_stg.start()
        if ses_db_stg == -1:
            raise Exception (f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception ("Error trying to connect to the b2b_dwh_staging")

        # Dictionary for values of channels_ext
        promo_dict = {
            "promo_id" : [],
            "promo_name" : [],
            "promo_cost" :[],
            "promo_begin_date": [],
            "promo_end_date":[]
        }
        # Reading the CSV file
        promo_csv = pd.read_csv(promotions_conf)
        #print (channel_csv)
        #Processing the CSV file content
        if not promo_csv.empty:
            for id,name,cost,begin,end \
                in zip(promo_csv['PROMO_ID'],promo_csv['PROMO_NAME'],
                promo_csv['PROMO_COST'],promo_csv['PROMO_BEGIN_DATE'],
                promo_csv['PROMO_END_DATE']):
                promo_dict["promo_id"].append(id)
                promo_dict["promo_name"].append(name)
                promo_dict["promo_cost"].append(cost)
                promo_dict["promo_begin_date"].append(begin)
                promo_dict["promo_end_date"].append(end)
        if promo_dict ["promo_id"]:
            ses_db_stg.connect().execute("TRUNCATE TABLE promotions_ext")
            df_channels_ext = pd.DataFrame(promo_dict)
            df_channels_ext.to_sql('promotions_ext', ses_db_stg, if_exists='append',index=False)
    except:
        traceback.print_exc()
    finally:
        pass     
