from extract.extract_products import ext_products
from extract.extract_promotions import ext_promotions
from extract.extract_sales import ext_sales
from extract.extract_times import ext_times
from extract.extract_channels import ext_channels
from extract.extract_countries import ext_countries
from extract.extract_customers import ext_customers
import traceback

#Extract all tables method
def full_extraction(con_db_stg):
    try:
        ext_channels(con_db_stg)
        ext_countries(con_db_stg)
        ext_customers(con_db_stg)
        ext_products(con_db_stg)
        ext_promotions(con_db_stg)
        ext_sales(con_db_stg)
        ext_times(con_db_stg)
    except:
        traceback.print_exc()
    finally:
        pass
    