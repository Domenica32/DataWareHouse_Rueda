from distutils.util import execute
from os import times_result
from time import time
from util.db_connection import Db_Connection
from transform.transformation import *
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
times_conf = confParam ['csvs']['times_csv']
con_db_stg = Db_Connection(type, host, port, user, pwd, dbStg)

def time_tra(codigoETL):
    try:
        ses_db_stg = con_db_stg.start()
        if ses_db_stg == -1:
            raise Exception (f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception ("Error trying to connect to the b2b_dwh_staging")

        # Dictionary for values of channels_ext
        time_dict = {
            "codigo_etl" : [],
            "time_id" : [],
            "day_name" : [],
            "day_number_in_week" :[],
            "day_number_in_month": [],
            "calendar_week_number":[],
            "calendar_month_number":[],
            "calendar_month_desc":[],
            "end_of_cal_month":[],
            "calendar_quarter_desc":[],
            "calendar_year":[]
        }
        # Reading de extract promotions from de sql table
        time_ext = pd.read_sql("SELECT TIME_ID,DAY_NAME, DAY_NUMBER_IN_WEEK, DAY_NUMBER_IN_MONTH,CALENDAR_WEEK_NUMBER, CALENDAR_MONTH_NUMBER, CALENDAR_MONTH_DESC, END_OF_CAL_MONTH, CALENDAR_QUARTER_DESC, CALENDAR_YEAR  FROM time_ext",ses_db_stg)
        #Processing the sql  content
        if not time_ext.empty:
            for id,name,numWeek,numMonth,calWeek,calMonth,calDesc,end,quarter,year \
                in zip(time_ext['TIME_ID'],time_ext['DAY_NAME'],
                time_ext['DAY_NUMBER_IN_WEEK'],time_ext['DAY_NUMBER_IN_MONTH'],time_ext["CALENDAR_WEEK_NUMBER"],
                time_ext["CALENDAR_MONTH_NUMBER"],time_ext["CALENDAR_MONTH_DESC"],
                time_ext["END_OF_CAL_MONTH"],time_ext["CALENDAR_QUARTER_DESC"],time_ext["CALENDAR_YEAR"]):
                time_dict["codigo_etl"].append(codigoETL)
                time_dict["time_id"].append( date_numeric(id))
                time_dict["day_name"].append(name)
                time_dict["day_number_in_week"].append(numWeek)
                time_dict["day_number_in_month"].append(numMonth)
                time_dict["calendar_week_number"].append(calWeek)
                time_dict["calendar_month_number"].append(calMonth)
                time_dict["calendar_month_desc"].append(calDesc)
                time_dict["end_of_cal_month"].append(month_date_numeric(end))
                time_dict["calendar_quarter_desc"].append(quarter)
                time_dict["calendar_year"].append(year)
        if time_dict ["time_id"]:
            df_time_ext = pd.DataFrame(time_dict)
            df_time_ext.to_sql('times_tra', ses_db_stg, if_exists='append',index=False)
           
    except:
        traceback.print_exc()
    finally:
        pass     
