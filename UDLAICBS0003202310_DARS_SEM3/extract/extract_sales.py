from distutils.util import execute
from util.db_connection import Db_Connection
import pandas as pd
import traceback


def ext_sales():
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
        sales_dict = {
            "prod_id" : [],
            "cust_id" : [],
            "time_id" :[],
            "channel_id": [],
            "promo_id":[],
            "quantity_sold":[],
            "amount_sold":[]
        }
        # Reading the CSV file
        sales_csv = pd.read_csv("csvs/sales.csv")
        #print (channel_csv)
        #Processing the CSV file content
        if not sales_csv.empty:
            for prod,cust,time,channel,promo,quan,amou \
                in zip(sales_csv['PROD_ID'],sales_csv['CUST_ID'],
                sales_csv['TIME_ID'],sales_csv['CHANNEL_ID'],
                sales_csv["PROMO_ID"],sales_csv["QUANTITY_SOLD"],
                sales_csv["AMOUNT_SOLD"]):
                sales_dict["prod_id"].append(prod)
                sales_dict["cust_id"].append(cust)
                sales_dict["time_id"].append(time)
                sales_dict["channel_id"].append(channel)
                sales_dict["promo_id"].append(promo)
                sales_dict["quantity_sold"].append(quan)
                sales_dict["amount_sold"].append(amou)
        if sales_dict ["prod_id"]:
            ses_db_stg.connect().execute("TRUNCATE TABLE sales_ext")
            df_channels_ext = pd.DataFrame(sales_dict)
            df_channels_ext.to_sql('sales_ext', ses_db_stg, if_exists='append',index=False)
    except:
        traceback.print_exc()
    finally:
        pass     
