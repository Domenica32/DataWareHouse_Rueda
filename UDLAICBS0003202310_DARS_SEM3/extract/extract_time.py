from distutils.util import execute
from os import times_result
from time import time
from util.db_connection import Db_Connection
import pandas as pd
import traceback


def ext_time():
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
        time_dict = {
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
        # Reading the CSV file
        time_csv = pd.read_csv("csvs/times.csv")
        #print (channel_csv)
        #Processing the CSV file content
        if not time_csv.empty:
            for id,name,numWeek,numMonth,calWeek,calMonth,calDesc,end,quarter,year \
                in zip(time_csv['TIME_ID'],time_csv['DAY_NAME'],
                time_csv['DAY_NUMBER_IN_WEEK'],time_csv['DAY_NUMBER_IN_MONTH'],time_csv["CALENDAR_WEEK_NUMBER"],
                time_csv["CALENDAR_MONTH_NUMBER"],time_csv["CALENDAR_MONTH_DESC"],
                time_csv["END_OF_CAL_MONTH"],time_csv["CALENDAR_QUARTER_DESC"],time_csv["CALENDAR_YEAR"]):
                time_dict["time_id"].append(id)
                time_dict["day_name"].append(name)
                time_dict["day_number_in_week"].append(numWeek)
                time_dict["day_number_in_month"].append(numMonth)
                time_dict["calendar_week_number"].append(calWeek)
                time_dict["calendar_month_number"].append(calMonth)
                time_dict["calendar_month_desc"].append(calDesc)
                time_dict["end_of_cal_month"].append(end)
                time_dict["calendar_quarter_desc"].append(quarter)
                time_dict["calendar_year"].append(year)
        if time_dict ["time_id"]:
            ses_db_stg.connect().execute("TRUNCATE TABLE time_ext")
            df_channels_ext = pd.DataFrame(time_dict)
            df_channels_ext.to_sql('time_ext', ses_db_stg, if_exists='append',index=False)
    except:
        traceback.print_exc()
    finally:
        pass     
