from util.db_connection import Db_Connection
from extract.extract_channels import ext_channels
from datetime import datetime
import pandas as pd
import traceback


try:
   ext_channels()
except:
    traceback.print_exc()
finally:
    pass     