from datetime import datetime
from util import db_connection
import pandas as pd
import traceback

#Extraccion


try:
    #Variables de sesion
    type = 'mysql'
    host = 'localhost'
    port = '3306'
    user = 'esteban_udla'
    pwd = 'esteban_udla'
    db = 'essgdbsor'

    con_db_stg = db_connection(type, host, port, user, pwd, db)
    ses_db_stg = con_db_stg.start()
    if ses_db_stg == -1:
        raise Exception(f"The give database type {type} is not valid")
    elif ses_db_stg == -2:
        raise Exception("Error trying to connect to the b2b_dwh_staging database")
except:
    traceback.print_exc()
finally:
    pass