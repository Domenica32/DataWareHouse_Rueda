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
countries_conf = confParam ['csvs']['countries_csv']
con_db_stg = Db_Connection(type, host, port, user, pwd, dbStg)

def ext_countries():
    try:
    
        ses_db_stg = con_db_stg.start()
        if ses_db_stg == -1:
            raise Exception (f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception ("Error trying to connect to the b2b_dwh_staging")

        # Dictionary for values of channels_ext
        channel_dict = {
            "country_id" : [],
            "country_name" : [],
            "country_region" :[],
            "country_region_id": []
        }
        # Reading the CSV file
        country_csv = pd.read_csv(countries_conf)
        #print (channel_csv)
        #Processing the CSV file content
        if not country_csv.empty:
            for id,name,region,coun_id \
                in zip(country_csv['COUNTRY_ID'],country_csv['COUNTRY_NAME'],
                country_csv['COUNTRY_REGION'],country_csv['COUNTRY_REGION_ID']):
                channel_dict["country_id"].append(id)
                channel_dict["country_name"].append(name)
                channel_dict["country_region"].append(region)
                channel_dict["country_region_id"].append(coun_id)
        if channel_dict ["country_id"]:
            ses_db_stg.connect().execute("TRUNCATE TABLE countries_ext")
            df_channels_ext = pd.DataFrame(channel_dict)
            df_channels_ext.to_sql('countries_ext', ses_db_stg, if_exists='append',index=False)
    except:
        traceback.print_exc()
    finally:
        pass     
