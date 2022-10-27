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
            "cust_id" : [],
            "cust_complete_name":[], 
            "cust_gen_tra":[],
            "cust_year_of_birth":[],
            "cust_marital_status": [],
            "cust_street_address":[],
            "cust_postal_code":[],
            "cust_city":[],
            "cust_state_province":[],
            "country_id":[],
            "cust_main_phone_number":[],
            "cust_income_level":[],
            "cust_credit_limit":[],
            "cust_email":[]
        }
        
        customers_tra = pd.read_sql("SELECT COUNTRY_ID, COUNTRY_NAME , COUNTRY_REGION, COUNTRY_REGION_ID FROM countries_tra",ses_db_stg)
        customers_ext = pd.read_sql("SELECT CUST_ID, CUST_COMPLETE_NAME, CUST_GENDER_TRA, CUST_YEAR_OF_BIRTH, CUST_MARITAL_STATUS, CUST_STREET_ADDRESS,CUST_POSTAL_CODE, CUST_CITY, CUST_STATE_PROVINCE, COUNTRY_ID, CUST_MAIN_PHONE_NUMBER, CUST_INCOME_LEVEL, CUST_CREDIT_LIMIT, CUST_EMAIL  FROM customers_ext",ses_db_stg)

        if not country_tra.empty:
            for id,name,region,coun_id \
                in zip(country_tra['COUNTRY_ID'],country_tra['COUNTRY_NAME'],
                country_tra['COUNTRY_REGION'],country_tra['COUNTRY_REGION_ID']):
                dim_country_dict["country_id"].append(id)
                dim_country_dict["country_name"].append(name)
                dim_country_dict["country_region"].append(region)
                dim_country_dict["country_region_id"].append(coun_id)
        if dim_country_dict ["country_id"]:
            df_dim_countries = pd.DataFrame(dim_country_dict)
            df_dim_countries.to_sql('dim_countries', ses_db_sor, if_exists='append',index=False)
    except:
        traceback.print_exc()
    finally:
        pass     
