from util.db_connection import Db_Connection
from datetime import datetime
import pandas as pd
import traceback
from extract.extract_process import extraction_process
from transform.transform_process import transform_process



try:
    #EXTRACTS
        extraction_process()
    #TRANSFORMS AND LOAD
        transform_process() #La funcion transform llama a la funcion load para cargar los datos despues de la transformacion
    #load_sales(codigoETL)
        



except:
    traceback.print_exc()
finally:
    pass     