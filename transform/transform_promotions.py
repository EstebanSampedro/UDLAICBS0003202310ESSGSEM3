import traceback
import pandas as pd
from transform.transform_method import str_float, str_int


def transform_promotions(cod_etl, con_db_stg):
    try:
        promos_dict = {
            "promo_id": [],
            "promo_name": [],
            "promo_cost": [],
            "promo_begin_date": [],
            "promo_end_date": [],
            "cod_etl": [],
        }

        #SqlCommand & stg connection
        promos_ext = pd.read_sql(
            "SELECT PROMO_ID,PROMO_NAME, PROMO_COST, PROMO_BEGIN_DATE,PROMO_END_DATE FROM promotions_ext",
            con_db_stg,
        )
        
        #Verify empty promos table
        if not promos_ext.empty:
            for id, prom_name, prom_cost, prom_begin, prom_end, in zip(
                    promos_ext["PROMO_ID"],
                    promos_ext["PROMO_NAME"],
                    promos_ext["PROMO_COST"],
                    promos_ext["PROMO_BEGIN_DATE"],
                    promos_ext["PROMO_END_DATE"],
            ):
                promos_dict["promo_id"].append(str_int(id))
                promos_dict["promo_name"].append(prom_name)
                promos_dict["promo_cost"].append(str_float(prom_cost))
                promos_dict["promo_begin_date"].append(prom_begin)
                promos_dict["promo_end_date"].append(prom_end)
                promos_dict["cod_etl"].append(cod_etl)

        if promos_dict["promo_id"]:
            df_promotions = pd.DataFrame(promos_dict)
            df_promotions.to_sql(
                "promotions_tra", con_db_stg, if_exists="append", index=False
            )
    except:
        traceback.print_exc()
    finally:
        pass