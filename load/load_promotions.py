import traceback
import pandas as pd


def load_promotions(cod_etl, con_db_stg, con_db_sor):
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
        promos_transform = pd.read_sql(
            "SELECT promo_id,promo_name, promo_cost, promo_begin_date,promo_end_date FROM promotions_tra", con_db_stg,
        )

        if not promos_transform.empty:
            for id, prom_name, prom_cost, prom_begin, prom_end, in zip(
                    promos_transform["promo_id"],
                    promos_transform["promo_name"],
                    promos_transform["promo_cost"],
                    promos_transform["promo_begin_date"],
                    promos_transform["promo_end_date"],
            ):
                promos_dict["promo_id"].append(id)
                promos_dict["promo_name"].append(prom_name)
                promos_dict["promo_cost"].append(prom_cost)
                promos_dict["promo_begin_date"].append(prom_begin)
                promos_dict["promo_end_date"].append(prom_end)
                promos_dict["cod_etl"].append(cod_etl)

        if promos_dict["promo_id"]:
            df_promotions = pd.DataFrame(promos_dict)

            #Load process
            df_promotions.to_sql(
                "new_promotions", con_db_sor, if_exists="append", index=False
            )
    except:
        traceback.print_exc()
    finally:
        pass