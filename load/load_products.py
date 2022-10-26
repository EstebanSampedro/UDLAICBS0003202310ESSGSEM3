import traceback
import pandas as pd
from util.sql_etl_methods import merge

def load_products(cod_etl, con_db_stg, con_db_sor):
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

        #SqlCommand & stg connection
        product_transform = pd.read_sql(
            "SELECT prod_id,prod_name, prod_desc, prod_category,prod_category_id,prod_category_desc,prod_weight_class,supplier_id,prod_status,prod_list_price,prod_min_price FROM products_tra",
            con_db_stg,
        )
        if not product_transform.empty:
            for id, pro_name, pro_desc, pro_cat, pro_cat_id, pro_cat_desc, pro_w_class, supp_id, pro_status, pro_list, pro_min, in zip(
                product_transform["prod_id"],
                product_transform["prod_name"],
                product_transform["prod_desc"],
                product_transform["prod_category"],
                product_transform["prod_category_id"],
                product_transform["prod_category_desc"],
                product_transform["prod_weight_class"],
                product_transform["supplier_id"],
                product_transform["prod_status"],
                product_transform["prod_list_price"],
                product_transform["prod_min_price"],
            ):
                products_dict["prod_id"].append(id)
                products_dict["prod_name"].append(pro_name)
                products_dict["prod_desc"].append(pro_desc)
                products_dict["prod_category"].append(pro_cat)
                products_dict["prod_category_id"].append(pro_cat_id)
                products_dict["prod_category_desc"].append(pro_cat_desc)
                products_dict["prod_weight_class"].append(pro_w_class)
                products_dict["supplier_id"].append(supp_id)
                products_dict["prod_status"].append(pro_status)
                products_dict["prod_list_price"].append(pro_list)
                products_dict["prod_min_price"].append(pro_min)
                products_dict["cod_etl"].append(cod_etl)

        if products_dict["prod_id"]:
            df_products_tra = pd.DataFrame(products_dict)
            
            #Load process
            df_products_tra.to_sql(
                "new_products", con_db_sor, if_exists="append", index=False
            )
    except:
        traceback.print_exc()
    finally:
        pass