from util.db_connection import Db_Connection
from extract.extract_channels import ext_channels
from extract.extract_countries import ext_countries
from extract.extract_customers import ext_customers
from extract.extract_products import ext_products
from extract.extract_promotions import ext_promo
from extract.extract_sales import ext_sales
from extract.extract_time import ext_time
from datetime import datetime
import pandas as pd
import traceback
from transform.proceso_etl import proceso_etl
from transform.tra_channels import channels_tra
from transform.tra_countries import countries_tra
from transform.tra_customers import customers_tra
from transform.tra_products import products_tra
from transform.tra_promotions import  promo_tra
from transform.tra_sales import  sales_tra
from transform.tra_time import  time_tra
from load.load_channels import load_channels
from load.load_countries import load_countries





codigoETL=proceso_etl()
try:
    # EXTRACTS
        #ext_channels()
        #ext_countries()
        #ext_customers()
        #ext_products()
        #ext_promo()
        #ext_sales()
        #ext_time()
   # TRANSFORMS 
        #channels_tra(codigoETL)
        #countries_tra(codigoETL)
        #customers_tra(codigoETL)
        #products_tra(codigoETL)
        #promo_tra(codigoETL)
        #sales_tra(codigoETL)
        #time_tra(codigoETL)
   #LOADS
        #load_channels(codigoETL)
        load_countries(codigoETL)




except:
    traceback.print_exc()
finally:
    pass     