import traceback

from extract.full_extraction import full_extraction
from load.full_load import full_load
from transform.full_transform import full_transform
from util.sql_etl_methods import etl_code_method
from util.db_type_connection import db_type_connection

try:
    conn = db_type_connection()
    con_db_stg = conn["con_db_stg"]
    con_db_sor = conn["con_db_sor"]
    if con_db_stg is not None and con_db_sor is not None:
        etl_code = etl_code_method(con_db_stg=con_db_stg)
        full_extraction(con_db_stg=con_db_stg) 
        full_transform(etl_code=etl_code, con_db_stg=con_db_stg)
        full_load(etl_code=etl_code, con_db_stg=con_db_stg, con_db_sor=con_db_sor)
        
except KeyError:
    traceback.print_exc()
finally:
    print("SUCESSFULL")