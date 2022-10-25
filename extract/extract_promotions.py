#Libraries imports
from util import db_connection
import pandas as pd
from datetime import datetime

import configparser

import traceback

#Uso de configuracion Parser para .properties
config = configparser.ConfigParser()
config.read(".properties")
config.get("DatabaseCredentials", "DB_TYPE")

#Ruta de los archivos CSV
cvsName = "CSVFiles"


def ext_promotions(con_db_stg):
    try:
        promo_dict = {
            "promo_id": [],
            "promo_name": [],
            "promo_cost": [],
            "promo_begin_date": [],
            "promo_end_date": [],
        }

        #Leer el archivo CSV
        promo_csv = pd.read_csv(config.get(cvsName, "PROMOTIONS_PATH"))

        #Procesa el contenido del archivo CSV 
        if not promo_csv.empty:
            for (id, prom_name, prom_cost, prom_begin, prom_end) in zip(
                promo_csv["PROMO_ID"],
                promo_csv["PROMO_NAME"],
                promo_csv["PROMO_COST"],
                promo_csv["PROMO_BEGIN_DATE"],
                promo_csv["PROMO_END_DATE"],
            ):
                promo_dict["promo_id"].append(id)
                promo_dict["promo_name"].append(prom_name)
                promo_dict["promo_cost"].append(prom_cost)
                promo_dict["promo_begin_date"].append(prom_begin)
                promo_dict["promo_end_date"].append(prom_end)

        if promo_dict["promo_id"]:
            con_db_stg.connect().execute("TRUNCATE TABLE promotions_ext")
            
            df_channels = pd.DataFrame(promo_dict)
            df_channels.to_sql("promotions_ext", con_db_stg, if_exists="append", index=False)
         
            # con_db_stg.dispose()
    except:
        traceback.print_exc()
    finally:
        pass