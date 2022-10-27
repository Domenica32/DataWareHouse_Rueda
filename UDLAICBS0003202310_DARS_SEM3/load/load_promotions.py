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

def load_promotions(codigoETL):
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
      
        dim_promo_dict = {
           "PROMO_ID" : [],
            "PROMO_NAME" : [],
            "PROMO_COST" :[],
            "PROMO_BEGIN_DATE": [],
            "PROMO_END_DATE":[]

        }
        
        promo_tra = pd.read_sql(f"SELECT PROMO_ID, PROMO_NAME, PROMO_COST, PROMO_BEGIN_DATE, PROMO_END_DATE  FROM promotions_tra where CODIGO_ETL={codigoETL}",ses_db_stg)
        promo_sor=pd.read_sql(f"SELECT PROMO_ID, PROMO_NAME, PROMO_COST, PROMO_BEGIN_DATE, PROMO_END_DATE FROM dim_promotions", ses_db_sor)
        promo_sor.to_dict()
        if not promo_tra.empty:
            for id,name,cost,begin,end \
                in zip(promo_tra['PROMO_ID'],promo_tra['PROMO_NAME'],
                promo_tra['PROMO_COST'],promo_tra['PROMO_BEGIN_DATE'],
                promo_tra['PROMO_END_DATE']):
                dim_promo_dict["PROMO_ID"].append(id)
                dim_promo_dict["PROMO_NAME"].append(name)
                dim_promo_dict["PROMO_COST"].append(cost)
                dim_promo_dict["PROMO_BEGIN_DATE"].append(begin)
                dim_promo_dict["PROMO_END_DATE"].append(end)
        if dim_promo_dict ["PROMO_ID"]:
            df_dim_promo = pd.DataFrame(dim_promo_dict)
            merge_promo= df_dim_promo.merge(promo_sor, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)            
            merge_promo.to_sql('dim_promotions', ses_db_sor, if_exists="append",index=False) 
    except:
        traceback.print_exc()
    finally:
        pass     
