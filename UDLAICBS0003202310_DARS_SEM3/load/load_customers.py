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

def load_customers(codigoETL):
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
      
        dim_customers_dict = {
            "cust_id" : [],
            "cust_complete_name":[], 
            "cust_gender":[],
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
        
        customers_tra = pd.read_sql(f"SELECT CUST_ID, CUST_COMPLETE_NAME, CUST_GEN_TRA, CUST_YEAR_OF_BIRTH, CUST_MARITAL_STATUS,\
        CUST_STREET_ADDRESS,CUST_POSTAL_CODE, CUST_CITY, CUST_STATE_PROVINCE, COUNTRY_ID, CUST_MAIN_PHONE_NUMBER, CUST_INCOME_LEVEL,\
        CUST_CREDIT_LIMIT, CUST_EMAIL  FROM customers_tra where CODIGO_ETL={codigoETL}",ses_db_stg)

        if not customers_tra.empty:
            for id,name,gen,birth,status,address,postal,city,province,coun_id,phone,level,credit,email \
                in zip(customers_tra['CUST_ID'], customers_tra['CUST_COMPLETE_NAME'],customers_tra['CUST_GEN_TRA'],
                customers_tra['CUST_YEAR_OF_BIRTH'],customers_tra['CUST_MARITAL_STATUS'],
                customers_tra['CUST_STREET_ADDRESS'],customers_tra['CUST_POSTAL_CODE'],
                customers_tra['CUST_CITY'],customers_tra['CUST_STATE_PROVINCE'],
                customers_tra['COUNTRY_ID'],customers_tra['CUST_MAIN_PHONE_NUMBER'],
                customers_tra['CUST_INCOME_LEVEL'],customers_tra['CUST_CREDIT_LIMIT'],
                customers_tra['CUST_EMAIL']):
                dim_customers_dict["cust_id"].append(id)
                dim_customers_dict["cust_complete_name"].append(name)
                dim_customers_dict["cust_gender"].append(gen)
                dim_customers_dict["cust_year_of_birth"].append(birth)
                dim_customers_dict["cust_marital_status"].append(status)
                dim_customers_dict["cust_street_address"].append(address)
                dim_customers_dict["cust_postal_code"].append(postal)
                dim_customers_dict["cust_city"].append(city)
                dim_customers_dict["cust_state_province"].append(province)
                dim_customers_dict["country_id"].append(coun_id)
                dim_customers_dict["cust_main_phone_number"].append(phone)
                dim_customers_dict["cust_income_level"].append(level)
                dim_customers_dict["cust_credit_limit"].append(credit)
                dim_customers_dict["cust_email"].append(email)
        if dim_customers_dict ["cust_id"]:
            df_dim_customers = pd.DataFrame(dim_customers_dict)
            df_dim_customers.to_sql('dim_customers', ses_db_sor, if_exists='append',index=False)
    except:
        traceback.print_exc()
    finally:
        pass     
