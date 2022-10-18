import code
from distutils.util import execute
from util.db_connection import Db_Connection
import pandas as pd
import traceback


def ext_customers():
    try:
    #Variables
        type = 'mysql'
        host = 'localhost'
        port = '3306'
        user = 'root'
        pwd = '1234'
        db = 'darsdbstg'


        con_db_stg = Db_Connection(type, host, port, user, pwd, db)
        ses_db_stg = con_db_stg.start()
        if ses_db_stg == -1:
            raise Exception (f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception ("Error trying to connect to the b2b_dwh_staging")

        # Dictionary for values of channels_ext
        customers_dict = {
            "cust_id" : [],
            "cust_first_name" : [],
            "cust_last_name" :[],
            "cust_gender": [],
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
        # Reading the CSV file
        customers_csv = pd.read_csv("csvs/customers.csv")
        #print (channel_csv)
        #Processing the CSV file content
        if not customers_csv.empty:
            for id,first,last,gen,birth,status,address,postal,city,province,coun_id,phone,level,credit,email \
                in zip(customers_csv['CUST_ID'],customers_csv['CUST_FIRST_NAME'],
                customers_csv['CUST_LAST_NAME'],customers_csv['CUST_GENDER'],
                customers_csv['CUST_YEAR_OF_BIRTH'],customers_csv['CUST_MARITAL_STATUS'],
                customers_csv['CUST_STREET_ADDRESS'],customers_csv['CUST_POSTAL_CODE'],
                customers_csv['CUST_CITY'],customers_csv['CUST_STATE_PROVINCE'],
                customers_csv['COUNTRY_ID'],customers_csv['CUST_MAIN_PHONE_NUMBER'],
                customers_csv['CUST_INCOME_LEVEL'],customers_csv['CUST_CREDIT_LIMIT'],
                customers_csv['CUST_EMAIL']):
                customers_dict["cust_id"].append(id)
                customers_dict["cust_first_name"].append(first)
                customers_dict["cust_last_name"].append(last)
                customers_dict["cust_gender"].append(gen)
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
            ses_db_stg.connect().execute("TRUNCATE TABLE customers_ext")
            df_channels_ext = pd.DataFrame(customers_dict)
            df_channels_ext.to_sql('customers_ext', ses_db_stg, if_exists='append',index=False)
    except:
        traceback.print_exc()
    finally:
        pass     
