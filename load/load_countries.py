import traceback
import pandas as pd

def load_countries(cod_etl, con_db_stg, con_db_sor):
    try:
        country_dict = {
            "country_id": [],
            "country_name": [],
            "country_region": [],
            "country_region_id": [],
            "cod_etl": [],
        }
        
        #SqlCommand & stg connection
        country_ext = pd.read_sql(
            "SELECT country_id,country_name, country_region, country_region_id FROM countries_tra", con_db_stg,
        )

        if not country_ext.empty:
            for coun_id, name, reg, reg_id in zip(
                    country_ext["country_id"],
                    country_ext["country_name"],
                    country_ext["country_region"],
                    country_ext["country_region_id"],
            ):
                country_dict["country_id"].append(coun_id)
                country_dict["country_name"].append(name)
                country_dict["country_region"].append(reg)
                country_dict["country_region_id"].append(reg_id)
                country_dict["cod_etl"].append(cod_etl)

        if country_dict["country_id"]:

            #Load process
            df_countries_tra = pd.DataFrame(country_dict)
            df_countries_tra.to_sql("new_countries", con_db_sor, if_exists="append", index=False)
    except:
        traceback.print_exc()
    finally:
        pass