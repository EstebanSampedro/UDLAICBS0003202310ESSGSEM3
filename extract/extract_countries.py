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


def ext_countries(con_db_stg):
    try:
        countries_dict = {
            "country_id": [],
            "country_name": [],
            "country_region": [],
            "country_region_id": [],
        }

        #Leer el archivo CSV
        country_csv = pd.read_csv(config.get(cvsName, "COUNTRIES_PATH"))

        #Procesa el contenido del archivo CSV 
        if not country_csv.empty:
            for id, name, reg, reg_id in zip(
                country_csv["COUNTRY_ID"],
                country_csv["COUNTRY_NAME"],
                country_csv["COUNTRY_REGION"],
                country_csv["COUNTRY_REGION_ID"],
            ):
                countries_dict["country_id"].append(id)
                countries_dict["country_name"].append(name)
                countries_dict["country_region"].append(reg)
                countries_dict["country_region_id"].append(reg_id)
        if countries_dict["country_id"]:
            con_db_stg.connect().execute("TRUNCATE TABLE countries_ext")
            
            df_channels = pd.DataFrame(countries_dict)
            df_channels.to_sql("countries_ext", con_db_stg, if_exists="append", index=False)
         
    except:
        traceback.print_exc()
    finally:
        pass