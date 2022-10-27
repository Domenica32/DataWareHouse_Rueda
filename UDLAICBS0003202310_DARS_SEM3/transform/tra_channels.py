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


def channels_tra(codigoETL):
    try:
   
        ses_db_stg = con_db_stg.start()
        if ses_db_stg == -1:
            raise Exception (f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception ("Error trying to connect to the b2b_dwh_staging")

        # Dictionary for values of channels_ext
        channel_dict = {
            "channel_id" : [],
            "codigo_etl_id":[],
            "channel_desc" : [],
            "channel_class" :[],
            "channel_class_id": []
        }

        # Reading de extract channel from de sql table
        channel_ext = pd.read_sql("SELECT CHANNEL_ID, CHANNEL_DESC, CHANNEL_CLASS, CHANNEL_CLASS_ID FROM channels_ext",ses_db_stg)
        #Processing the sql  content
        if not channel_ext.empty:
            for id,des,cla,cla_id \
                in zip(channel_ext['CHANNEL_ID'],channel_ext['CHANNEL_DESC'],
                channel_ext['CHANNEL_CLASS'],channel_ext['CHANNEL_CLASS_ID']):
                channel_dict["channel_id"].append(id)
                channel_dict["codigo_etl"].append(codigoETL)
                channel_dict["channel_desc"].append(des)
                channel_dict["channel_class"].append(cla)
                channel_dict["channel_class_id"].append(cla_id)
        if channel_dict ["channel_id"]:
            df_channels_ext = pd.DataFrame(channel_dict)
            df_channels_ext.to_sql('channels_tra', ses_db_stg, if_exists='append',index=False)
    except:
        traceback.print_exc()
    finally:
        pass     
