from distutils.util import execute
from util.db_connection import Db_Connection
import pandas as pd
import traceback
from load.subKey_FK import country_subKey

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
            "CUST_ID" : [],
            "CUST_COMPLETE_NAME":[], 
            "CUST_GENDER":[],
            "CUST_YEAR_OF_BIRTH":[],
            "CUST_MARITAL_STATUS": [],
            "CUST_STREET_ADDRESS":[],
            "CUST_POSTAL_CODE":[],
            "CUST_CITY":[],
            "CUST_STATE_PROVINCE":[],
            "COUNTRY_ID":[],
            "CUST_MAIN_PHONE_NUMBER":[],
            "CUST_INCOME_LEVEL":[],
            "CUST_CREDIT_LIMIT":[],
            "CUST_EMAIL":[]

        }
        
        customers_tra = pd.read_sql(f"SELECT CUST_ID, CUST_COMPLETE_NAME, CUST_GEN_TRA, CUST_YEAR_OF_BIRTH, CUST_MARITAL_STATUS,\
        CUST_STREET_ADDRESS,CUST_POSTAL_CODE, CUST_CITY, CUST_STATE_PROVINCE, COUNTRY_ID, CUST_MAIN_PHONE_NUMBER, CUST_INCOME_LEVEL,\
        CUST_CREDIT_LIMIT, CUST_EMAIL  FROM customers_tra where CODIGO_ETL={codigoETL}",ses_db_stg)

        country_data=pd.read_sql(f"SELECT ID,COUNTRY_ID FROM dim_countries", ses_db_sor)

        customer_sor=pd.read_sql(f"SELECT CUST_ID, CUST_COMPLETE_NAME, CUST_GENDER, CUST_YEAR_OF_BIRTH, CUST_MARITAL_STATUS,\
        CUST_STREET_ADDRESS,CUST_POSTAL_CODE, CUST_CITY, CUST_STATE_PROVINCE, COUNTRY_ID, CUST_MAIN_PHONE_NUMBER, CUST_INCOME_LEVEL,\
        CUST_CREDIT_LIMIT, CUST_EMAIL  FROM dim_customers", ses_db_sor)
        customer_sor.to_dict()
        if not customers_tra.empty:
            for id,name,gen,birth,status,address,postal,city,province,coun_id,phone,level,credit,email \
                in zip(customers_tra['CUST_ID'], customers_tra['CUST_COMPLETE_NAME'],customers_tra['CUST_GEN_TRA'],
                customers_tra['CUST_YEAR_OF_BIRTH'],customers_tra['CUST_MARITAL_STATUS'],
                customers_tra['CUST_STREET_ADDRESS'],customers_tra['CUST_POSTAL_CODE'],
                customers_tra['CUST_CITY'],customers_tra['CUST_STATE_PROVINCE'],
                customers_tra['COUNTRY_ID'],customers_tra['CUST_MAIN_PHONE_NUMBER'],
                customers_tra['CUST_INCOME_LEVEL'],customers_tra['CUST_CREDIT_LIMIT'],
                customers_tra['CUST_EMAIL']):
                dim_customers_dict["CUST_ID"].append(id)
                dim_customers_dict["CUST_COMPLETE_NAME"].append(name)
                dim_customers_dict["CUST_GENDER"].append(gen)
                dim_customers_dict["CUST_YEAR_OF_BIRTH"].append(birth)
                dim_customers_dict["CUST_MARITAL_STATUS"].append(status)
                dim_customers_dict["CUST_STREET_ADDRESS"].append(address)
                dim_customers_dict["CUST_POSTAL_CODE"].append(postal)
                dim_customers_dict["CUST_CITY"].append(city)
                dim_customers_dict["CUST_STATE_PROVINCE"].append(province)
                dim_customers_dict["COUNTRY_ID"].append(country_subKey(coun_id,country_data))
                dim_customers_dict["CUST_MAIN_PHONE_NUMBER"].append(phone)
                dim_customers_dict["CUST_INCOME_LEVEL"].append(level)
                dim_customers_dict["CUST_CREDIT_LIMIT"].append(credit)
                dim_customers_dict["CUST_EMAIL"].append(email)
        if dim_customers_dict ["CUST_ID"]:
            df_dim_customers = pd.DataFrame(dim_customers_dict)
            merge_customers = df_dim_customers.merge(customer_sor, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)            
            merge_customers.to_sql('dim_customers', ses_db_sor, if_exists="append",index=False)
    except:
        traceback.print_exc()
    finally:
        pass     
