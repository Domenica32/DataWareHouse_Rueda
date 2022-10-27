from load.load_channels import load_channels
from load.load_countries import load_countries
from load.load_customers import load_customers
from load.load_products import load_products
from load.load_promotions import load_promotions
from load.load_sales import load_sales
from load.load_time import load_time


def load_process(codigoETL):
    try:
    #LOADS
        load_channels(codigoETL)
        load_countries(codigoETL)
        load_customers(codigoETL)
        load_products(codigoETL)
        load_promotions(codigoETL)
        #load_sales(codigoETL)
        load_time(codigoETL)
    except:
        traceback.print_exc()
    finally:
        pass  