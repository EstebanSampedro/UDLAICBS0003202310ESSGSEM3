import traceback
import pandas as pd
from transform.transform_method import str_int, str_float

def transform_products(cod_etl, con_db_stg):
    try:
        products_dict = {
            "prod_id": [],
            "prod_name": [],
            "prod_desc": [],
            "prod_category": [],
            "prod_category_id": [],
            "prod_category_desc": [],
            "prod_weight_class": [],
            "supplier_id": [],
            "prod_status": [],
            "prod_list_price": [],
            "prod_min_price": [],
            "cod_etl": [],
        }

        # SqlCommand & stg connection
        product_ext = pd.read_sql(
            "SELECT PROD_ID,PROD_NAME, PROD_DESC, PROD_CATEGORY,PROD_CATEGORY_ID,PROD_CATEGORY_DESC,PROD_WEIGHT_CLASS,SUPPLIER_ID,PROD_STATUS,PROD_LIST_PRICE,PROD_MIN_PRICE FROM products_ext",
            con_db_stg,
        )
        
        if not product_ext.empty:
            for id, pro_name, pro_desc, pro_cat, pro_cat_id, pro_cat_desc, pro_w_class, supp_id, pro_status, pro_list, pro_min, in zip(
                product_ext["PROD_ID"],
                product_ext["PROD_NAME"],
                product_ext["PROD_DESC"],
                product_ext["PROD_CATEGORY"],
                product_ext["PROD_CATEGORY_ID"],
                product_ext["PROD_CATEGORY_DESC"],
                product_ext["PROD_WEIGHT_CLASS"],
                product_ext["SUPPLIER_ID"],
                product_ext["PROD_STATUS"],
                product_ext["PROD_LIST_PRICE"],
                product_ext["PROD_MIN_PRICE"],
            ):
                products_dict["prod_id"].append(str_int(id))
                products_dict["prod_name"].append(pro_name)
                products_dict["prod_desc"].append(pro_desc)
                products_dict["prod_category"].append(pro_cat)
                products_dict["prod_category_id"].append(str_int(pro_cat_id))
                products_dict["prod_category_desc"].append(pro_cat_desc)
                products_dict["prod_weight_class"].append(str_int(pro_w_class))
                products_dict["supplier_id"].append(str_int(supp_id))
                products_dict["prod_status"].append(pro_status)
                products_dict["prod_list_price"].append(str_float(pro_list))
                products_dict["prod_min_price"].append(str_float(pro_min))
                products_dict["cod_etl"].append(cod_etl)

        if products_dict["prod_id"]:
            df_products = pd.DataFrame(products_dict)
            df_products.to_sql(
                "products_tra", con_db_stg, if_exists="append", index=False
            )
            con_db_stg.dispose()
    except:
        traceback.print_exc()
    finally:
        pass