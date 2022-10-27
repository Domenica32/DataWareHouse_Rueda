from transform.proceso_etl import proceso_etl
from transform.tra_channels import channels_tra
from transform.tra_countries import countries_tra
from transform.tra_customers import customers_tra
from transform.tra_products import products_tra
from transform.tra_promotions import  promo_tra
from transform.tra_sales import  sales_tra
from transform.tra_time import  time_tra
from load.load_process import load_process

import traceback

codigoETL=proceso_etl()

def transform_process():

    try:
        # TRANSFORMS 
        channels_tra(codigoETL)
        countries_tra(codigoETL)
        customers_tra(codigoETL)
        products_tra(codigoETL)
        promo_tra(codigoETL)
        sales_tra(codigoETL)
        time_tra(codigoETL)
        load_process(codigoETL)
    except:
        traceback.print_exc()
    finally:
        pass