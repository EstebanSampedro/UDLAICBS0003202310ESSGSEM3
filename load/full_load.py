#Load methods imports
from load.load_channels import load_channels
from load.load_countries import load_countries
from load.load_customers import load_customers
from load.load_products import load_products
from load.load_promotions import load_promotions
from load.load_sales import load_sales
from load.load_times import load_times
import traceback


def full_load(etl_code, con_db_stg, con_db_sor):
    try:
        load_channels(etl_code, con_db_stg, con_db_sor)
        load_countries(etl_code, con_db_stg, con_db_sor)
        load_customers(etl_code, con_db_stg, con_db_sor)
        load_products(etl_code, con_db_stg, con_db_sor)
        load_promotions(etl_code, con_db_stg, con_db_sor)
        load_times(etl_code, con_db_stg, con_db_sor)
        load_sales(etl_code, con_db_stg, con_db_sor)
    except:
        traceback.print_exc()
    finally:
        pass