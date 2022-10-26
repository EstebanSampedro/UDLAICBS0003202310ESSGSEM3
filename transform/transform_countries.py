import traceback
import pandas as pd

from transform.transform_method import str_int

def transform_countries(cod_etl, con_db_stg):
    try:
        countries_dict = {
            "country_id": [],
            "country_name": [],
            "country_region": [],
            "country_region_id": [],
            "cod_etl": [],
        }

        #SqlCommand & stg connection
        country_ext = pd.read_sql(
            "SELECT COUNTRY_ID,COUNTRY_NAME, COUNTRY_REGION, COUNTRY_REGION_ID FROM countries_ext",
            con_db_stg,
        )

       #Channel_ext table in database
        if not country_ext.empty:
            for id, name, reg, reg_id in zip(
                    country_ext["COUNTRY_ID"],
                    country_ext["COUNTRY_NAME"],
                    country_ext["COUNTRY_REGION"],
                    country_ext["COUNTRY_REGION_ID"],
                    
            ):
                countries_dict["country_id"].append(str_int(id))
                countries_dict["country_name"].append(name)
                countries_dict["country_region"].append(reg)
                countries_dict["country_region_id"].append(str_int(reg_id))
                countries_dict["cod_etl"].append(cod_etl)

        if countries_dict["country_id"]:
            df_country_tra = pd.DataFrame(countries_dict)
            df_country_tra.to_sql(
                "countries_tra", con_db_stg, if_exists="append", index=False
            )

    except:
        traceback.print_exc()
    finally:
        pass