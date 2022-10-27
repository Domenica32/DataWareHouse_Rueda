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

def load_time(codigoETL):
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
      
        dim_time_dict = {
            "TIME_ID" : [],
            "DAY_NAME" : [],
            "DAY_NUMBER_IN_WEEK" :[],
            "DAY_NUMBER_IN_MONTH": [],
            "CALENDAR_WEEK_NUMBER":[],
            "CALENDAR_MONTH_NUMBER":[],
            "CALENDAR_MONTH_DESC":[],
            "END_OF_CAL_MONTH":[],
            "CALENDAR_QUARTER_DESC":[],
            "CALENDAR_YEAR":[]

        }
        time_tra = pd.read_sql(f"SELECT TIME_ID,DAY_NAME, DAY_NUMBER_IN_WEEK, DAY_NUMBER_IN_MONTH,CALENDAR_WEEK_NUMBER, CALENDAR_MONTH_NUMBER, CALENDAR_MONTH_DESC, END_OF_CAL_MONTH, CALENDAR_QUARTER_DESC, CALENDAR_YEAR  FROM times_tra where CODIGO_ETL={codigoETL}",ses_db_stg)
        times_sor=pd.read_sql(f"SELECT TIME_ID,DAY_NAME, DAY_NUMBER_IN_WEEK, DAY_NUMBER_IN_MONTH,CALENDAR_WEEK_NUMBER, CALENDAR_MONTH_NUMBER, CALENDAR_MONTH_DESC, END_OF_CAL_MONTH, CALENDAR_QUARTER_DESC, CALENDAR_YEAR FROM dim_times", ses_db_sor)
        times_sor.to_dict()
        if not time_tra.empty:
            for id,name,numWeek,numMonth,calWeek,calMonth,calDesc,end,quarter,year \
                in zip(time_tra['TIME_ID'],time_tra['DAY_NAME'],
                time_tra['DAY_NUMBER_IN_WEEK'],time_tra['DAY_NUMBER_IN_MONTH'],time_tra["CALENDAR_WEEK_NUMBER"],
                time_tra["CALENDAR_MONTH_NUMBER"],time_tra["CALENDAR_MONTH_DESC"],
                time_tra["END_OF_CAL_MONTH"],time_tra["CALENDAR_QUARTER_DESC"],time_tra["CALENDAR_YEAR"]):
                dim_time_dict["TIME_ID"].append( id)
                dim_time_dict["DAY_NAME"].append(name)
                dim_time_dict["DAY_NUMBER_IN_WEEK"].append(numWeek)
                dim_time_dict["DAY_NUMBER_IN_MONTH"].append(numMonth)
                dim_time_dict["CALENDAR_WEEK_NUMBER"].append(calWeek)
                dim_time_dict["CALENDAR_MONTH_NUMBER"].append(calMonth)
                dim_time_dict["CALENDAR_MONTH_DESC"].append(calDesc)
                dim_time_dict["END_OF_CAL_MONTH"].append(end)
                dim_time_dict["CALENDAR_QUARTER_DESC"].append(quarter)
                dim_time_dict["CALENDAR_YEAR"].append(year)
        if dim_time_dict ["TIME_ID"]:
            df_dim_time = pd.DataFrame(dim_time_dict)
            merge_times= df_dim_time.merge(times_sor, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)            
            merge_times.to_sql('dim_times', ses_db_sor, if_exists="append",index=False)   
    except:
        traceback.print_exc()
    finally:
        pass     
