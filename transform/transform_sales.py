import traceback
import pandas as pd
from transform.transform_method import str_float, str_int, str_date


def transform_sales(cod_etl, con_db_stg):
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

        #SqlCommand & stg connection
        sales_ext = pd.read_sql(
            "SELECT PROD_ID,CUST_ID, TIME_ID, CHANNEL_ID,PROMO_ID,QUANTITY_SOLD,AMOUNT_SOLD FROM sales_ext", con_db_stg,
        )

        if not sales_ext.empty:
            for id, cus_id, time_id, cha_id, prom_id, quant_sold, amt_sold, in zip(
                    sales_ext["PROD_ID"],
                    sales_ext["CUST_ID"],
                    sales_ext["TIME_ID"],
                    sales_ext["CHANNEL_ID"],
                    sales_ext["PROMO_ID"],
                    sales_ext["QUANTITY_SOLD"],
                    sales_ext["AMOUNT_SOLD"],
            ):
                sales_dict["prod_id"].append(str_int(id))
                sales_dict["cust_id"].append(str_int(cus_id))
                sales_dict["time_id"].append(str_date(time_id))
                sales_dict["channel_id"].append(str_int(cha_id))
                sales_dict["promo_id"].append(str_int(prom_id))
                sales_dict["quantity_sold"].append(str_float(quant_sold))
                sales_dict["amount_sold"].append(str_float(amt_sold))
                sales_dict["cod_etl"].append(cod_etl)

        if sales_dict["prod_id"]:
            df_sales_tra = pd.DataFrame(sales_dict)
            df_sales_tra.to_sql(
                "sales_tra", con_db_stg, if_exists="append", index=False
            )
    except:
        traceback.print_exc()
    finally:
        pass