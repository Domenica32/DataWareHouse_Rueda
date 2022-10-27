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

def countries_tra(codigoETL):
    try:
    
        ses_db_stg = con_db_stg.start()
        if ses_db_stg == -1:
            raise Exception (f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception ("Error trying to connect to the b2b_dwh_staging")

        # Dictionary for values of channels_ext
        country_dict = {
            "codigo_etl" : [],
            "country_id" : [],
            "country_name" : [],
            "country_region" :[],
            "country_region_id": []
        }
        # Reading de extract countries from de sql table
        country_ext = pd.read_sql("SELECT COUNTRY_ID, COUNTRY_NAME , COUNTRY_REGION, COUNTRY_REGION_ID FROM countries_ext",ses_db_stg)
        #Processing the sql  content
        if not country_ext.empty:
            for id,name,region,coun_id \
                in zip(country_ext['COUNTRY_ID'],country_ext['COUNTRY_NAME'],
                country_ext['COUNTRY_REGION'],country_ext['COUNTRY_REGION_ID']):
                country_dict["codigo_etl"].append(codigoETL)
                country_dict["country_id"].append(id)
                country_dict["country_name"].append(name)
                country_dict["country_region"].append(region)
                country_dict["country_region_id"].append(coun_id)
        if country_dict ["country_id"]:
            df_country_ext = pd.DataFrame(country_dict)
            df_country_ext.to_sql('countries_tra', ses_db_stg, if_exists='append',index=False)
    except:
        traceback.print_exc()
    finally:
        pass     
