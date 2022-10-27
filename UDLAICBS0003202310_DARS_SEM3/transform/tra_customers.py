import code
from distutils.util import execute
from util.db_connection import Db_Connection
import pandas as pd
import traceback
from transform.transformation import *

import configparser

confParam = configparser.ConfigParser()
confParam.read('conf.properties')

type = confParam ['MySqlParameters']['type']
host =confParam ['MySqlParameters']['host']
port = confParam ['MySqlParameters']['port']
user = confParam ['MySqlParameters']['user']
pwd = confParam ['MySqlParameters']['pwd']
dbStg = confParam ['MySqlParameters']['dbStg']
customers_conf = confParam ['csvs']['customers_csv']
con_db_stg = Db_Connection(type, host, port, user, pwd, dbStg)
def customers_tra(codigoETL):
    try:

        ses_db_stg = con_db_stg.start()
        if ses_db_stg == -1:
            raise Exception (f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception ("Error trying to connect to the b2b_dwh_staging")

        # Dictionary for values of channels_ext
        customers_dict = {
            "codigo_etl" : [],
            "cust_id" : [],
            "cust_first_name" : [],
            "cust_last_name" :[],
            "cust_complete_name":[], 
            "cust_gender": [],
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
         # Reading de extract customers from de sql table
        customers_ext = pd.read_sql("SELECT CUST_ID, CUST_FIRST_NAME, CUST_LAST_NAME, CUST_GENDER, CUST_YEAR_OF_BIRTH,\
             CUST_MARITAL_STATUS, CUST_STREET_ADDRESS,CUST_POSTAL_CODE, CUST_CITY, CUST_STATE_PROVINCE, COUNTRY_ID,\
             CUST_MAIN_PHONE_NUMBER, CUST_INCOME_LEVEL, CUST_CREDIT_LIMIT, CUST_EMAIL  FROM customers_ext",ses_db_stg)
        #Processing the sql  content
        if not customers_ext.empty:
            for id,first,last,gen,birth,status,address,postal,city,province,coun_id,phone,level,credit,email \
                in zip(customers_ext['CUST_ID'], customers_ext['CUST_FIRST_NAME'],
                customers_ext['CUST_LAST_NAME'],customers_ext['CUST_GENDER'],
                customers_ext['CUST_YEAR_OF_BIRTH'],customers_ext['CUST_MARITAL_STATUS'],
                customers_ext['CUST_STREET_ADDRESS'],customers_ext['CUST_POSTAL_CODE'],
                customers_ext['CUST_CITY'],customers_ext['CUST_STATE_PROVINCE'],
                customers_ext['COUNTRY_ID'],customers_ext['CUST_MAIN_PHONE_NUMBER'],
                customers_ext['CUST_INCOME_LEVEL'],customers_ext['CUST_CREDIT_LIMIT'],
                customers_ext['CUST_EMAIL']):
                customers_dict["codigo_etl"].append(codigoETL)
                customers_dict["cust_id"].append(id)
                customers_dict["cust_first_name"].append(first)
                customers_dict["cust_last_name"].append(last)
                customers_dict["cust_complete_name"].append(join_2_strings(first, last))
                customers_dict["cust_gender"].append(gen)
                customers_dict["cust_gen_tra"].append(obt_gender(gen))
                customers_dict["cust_year_of_birth"].append(birth)
                customers_dict["cust_marital_status"].append(status)
                customers_dict["cust_street_address"].append(address)
                customers_dict["cust_postal_code"].append(postal)
                customers_dict["cust_city"].append(city)
                customers_dict["cust_state_province"].append(province)
                customers_dict["country_id"].append(coun_id)
                customers_dict["cust_main_phone_number"].append(phone)
                customers_dict["cust_income_level"].append(level)
                customers_dict["cust_credit_limit"].append(credit)
                customers_dict["cust_email"].append(email)
        if customers_dict ["cust_id"]:
            df_customers_ext = pd.DataFrame(customers_dict)
            df_customers_ext.to_sql('customers_tra', ses_db_stg, if_exists='append',index=False)
            
    except:
        traceback.print_exc()
    finally:
        pass     
