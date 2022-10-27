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

def load_countries(codigoETL):
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
      
        dim_country_dict = {
            "COUNTRY_ID" : [],
            "COUNTRY_NAME" : [],
            "COUNTRY_REGION" :[],
            "COUNTRY_REGION_ID": []
        }
        
        country_tra = pd.read_sql(f"SELECT COUNTRY_ID, COUNTRY_NAME , COUNTRY_REGION, COUNTRY_REGION_ID FROM countries_tra where CODIGO_ETL={codigoETL}",ses_db_stg)
        country_sor=pd.read_sql(f"SELECT COUNTRY_ID, COUNTRY_NAME , COUNTRY_REGION, COUNTRY_REGION_ID FROM dim_countries", ses_db_sor)
        country_sor.to_dict()
        if not country_tra.empty:
            for id,name,region,coun_id \
                in zip(country_tra['COUNTRY_ID'],country_tra['COUNTRY_NAME'],
                country_tra['COUNTRY_REGION'],country_tra['COUNTRY_REGION_ID']):
                dim_country_dict["COUNTRY_ID"].append(id)
                dim_country_dict["COUNTRY_NAME"].append(name)
                dim_country_dict["COUNTRY_REGION"].append(region)
                dim_country_dict["COUNTRY_REGION_ID"].append(coun_id)
        if dim_country_dict ["COUNTRY_ID"]:
            df_dim_countries = pd.DataFrame(dim_country_dict)
            merge_country = df_dim_countries.merge(country_sor, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)            
            merge_country.to_sql('dim_countries', ses_db_sor, if_exists="append",index=False)
    except:
        traceback.print_exc()
    finally:
        pass     
