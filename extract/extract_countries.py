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
databaseName = "DatabaseCredentials"
#Credenciales de la base de datos
stg_connection = db_connection.Db_Connection(
    config.get(databaseName, "DB_TYPE"),
    config.get(databaseName, "DB_HOST"),
    config.get(databaseName, "DB_PORT"),
    config.get(databaseName, "DB_USER"),
    config.get(databaseName, "DB_PWD"),
    config.get(databaseName, "STG_NAME"),
)
#Ruta de los archivos CSV
cvsName = "CSVFiles"


def ext_countries():
    try:
        con = stg_connection.start()
        if con == -1:
            raise Exception(f"The database type {stg_connection.type} is not valid")
        elif con == -2:
            raise Exception("Error trying to connect to essgdbstaging")
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
            con.connect().execute("TRUNCATE TABLE countries")
            
            df_channels = pd.DataFrame(countries_dict)
            df_channels.to_sql("countries", con, if_exists="append", index=False)
         
            con.dispose()
    except:
        traceback.print_exc()
    finally:
        pass