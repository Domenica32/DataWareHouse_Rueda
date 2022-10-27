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


def load_channels(codigoETL):
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
      

        dim_channel_dict = {
            "CHANNEL_ID" : [],
            "CHANNEL_DESC" : [],
            "CHANNEL_CLASS" :[],
            "CHANNEL_CLASS_ID": []
        }
        channel_tra = pd.read_sql(f"SELECT CHANNEL_ID, CHANNEL_DESC, CHANNEL_CLASS, CHANNEL_CLASS_ID FROM channels_tra where CODIGO_ETL={codigoETL} ", ses_db_stg)
        channel_sor=pd.read_sql(f"SELECT CHANNEL_ID, CHANNEL_DESC, CHANNEL_CLASS, CHANNEL_CLASS_ID FROM dim_channels", ses_db_sor)
        channel_sor.to_dict()
        if not channel_tra.empty:
            for id,des,cla,cla_id \
                in zip(channel_tra['CHANNEL_ID'],channel_tra['CHANNEL_DESC'],
                channel_tra['CHANNEL_CLASS'],channel_tra['CHANNEL_CLASS_ID']):
                dim_channel_dict["CHANNEL_ID"].append(id)
                dim_channel_dict["CHANNEL_DESC"].append(des)
                dim_channel_dict["CHANNEL_CLASS"].append(cla)
                dim_channel_dict["CHANNEL_CLASS_ID"].append(cla_id)


        if dim_channel_dict ["CHANNEL_ID"]:
            df_dim_channels = pd.DataFrame(dim_channel_dict)
            merge_channel = df_dim_channels.merge(channel_sor, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)
            merge_channel.to_sql('dim_channels', ses_db_sor, if_exists="append",index=False)

    except:
        traceback.print_exc()
    finally:
        pass     
