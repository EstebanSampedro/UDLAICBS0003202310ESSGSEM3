#Transform tables methods imports
from transform.transform_channels import transform_channels
from transform.transform_countries import transform_countries
from transform.transform_customers import transform_customers
from transform.transform_products import transform_products
from transform.transform_promotions import transform_promotions
from transform.transform_times import transform_times
from transform.transform_sales import transform_sales

import traceback


def full_transform(etl_code, con_db_stg):
    try:
        transform_channels(etl_code, con_db_stg)
        transform_countries(etl_code, con_db_stg)
        transform_customers(etl_code, con_db_stg)
        transform_products(etl_code, con_db_stg)
        transform_promotions(etl_code, con_db_stg)
        transform_times(etl_code, con_db_stg)
        transform_sales(etl_code, con_db_stg)
    except:
        traceback.print_exc()
    finally:
        pass