from extract.extract_channels import ext_channels
from extract.extract_countries import ext_countries
from extract.extract_customers import ext_customers
from extract.extract_products import ext_products
from extract.extract_promotions import ext_promo
from extract.extract_sales import ext_sales
from extract.extract_time import ext_time

import traceback

def extraction_process():
    try:
        #EXTRACTS
        ext_channels()
        ext_countries()
        ext_customers()
        ext_products()
        ext_promo()
        ext_sales()
        ext_time()
    except:
        traceback.print_exc()
    finally:
        pass