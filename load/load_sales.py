import traceback
import pandas as pd
from util.sql_etl_methods import merge

def load_sales(cod_etl, con_db_stg, con_db_sor):
    try:
        sales_dict = {
            "prod_id": [],
            "cust_id": [],
            "time_id": [],
            "channel_id": [],
            "promo_id": [],
            "quantity_sold": [],
            "amount_sold": [],
            "cod_etl": [],
        }
        sales_transform = pd.read_sql(
            f"SELECT prod_id,cust_id, time_id, channel_id,promo_id,quantity_sold,amount_sold FROM sales_tra WHERE cod_etl = {cod_etl}",con_db_stg,
        )

        # Surrogate keys process from every table that relates to sales context
        surr_key_prod = pd.read_sql_query("SELECT surr_id, prod_id from new_products", con_db_sor).set_index("prod_id").to_dict()["surr_id"]
        surr_key_cust = pd.read_sql_query("SELECT surr_id, cust_id from new_customers", con_db_sor).set_index("cust_id").to_dict()["surr_id"]
        surr_key_time = pd.read_sql_query("SELECT surr_id, time_id from new_times", con_db_sor).set_index("time_id").to_dict()["surr_id"]
        surr_key_chann = pd.read_sql_query("SELECT surr_id, channel_id from new_channels", con_db_sor).set_index("channel_id").to_dict()["surr_id"]
        surr_key_prom = pd.read_sql_query("SELECT surr_id, promo_id from new_promotions", con_db_sor).set_index("promo_id").to_dict()["surr_id"]
        

        sales_transform["prod_id"] = sales_transform["prod_id"].apply(lambda key: surr_key_prod[key])
        sales_transform["cust_id"] = sales_transform["cust_id"].apply(lambda key: surr_key_cust[key])
        sales_transform["time_id"] = sales_transform["time_id"].apply(lambda key: surr_key_time[key])
        sales_transform["channel_id"] = sales_transform["channel_id"].apply(lambda key: surr_key_chann[key])
        sales_transform["promo_id"] = sales_transform["promo_id"].apply(lambda key: surr_key_prom[key])
        

        if not sales_transform.empty:
            for id, cus_id, time_id, cha_id, prom_id, quant_sold, amt_sold, in zip(
                    sales_transform["prod_id"],
                    sales_transform["cust_id"],
                    sales_transform["time_id"],
                    sales_transform["channel_id"],
                    sales_transform["promo_id"],
                    sales_transform["quantity_sold"],
                    sales_transform["amount_sold"],
            ):
                sales_dict["prod_id"].append(id)
                sales_dict["cust_id"].append(cus_id)
                sales_dict["time_id"].append(time_id)
                sales_dict["channel_id"].append(cha_id)
                sales_dict["promo_id"].append(prom_id)
                sales_dict["quantity_sold"].append(quant_sold)
                sales_dict["amount_sold"].append(amt_sold)
                sales_dict["cod_etl"].append(cod_etl)

        if sales_dict["prod_id"]:

            df_new_sales = pd.DataFrame(sales_dict)
            #Merge method for loading
            merge(table_name="new_sales",key_col=["prod_id", "cust_id", "time_id", "channel_id", "promo_id"],dataframe=df_new_sales, db_context=con_db_sor)
    except:
        traceback.print_exc()
    finally:
        pass