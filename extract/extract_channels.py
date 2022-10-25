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


def ext_channels(con_db_stg):
    try:
        cha_dict = {
            "channel_id": [],
            "channel_desc": [],
            "channel_class": [],
            "channel_class_id": [],
        }

        #Leer el archivo CSV
        channel_csv = pd.read_csv(config.get(cvsName, "CHANNELS_PATH"))

        #Procesa el contenido del archivo CSV 
        if not channel_csv.empty:
            for id, des, cla, cla_id in zip(
                channel_csv["CHANNEL_ID"],
                channel_csv["CHANNEL_DESC"],
                channel_csv["CHANNEL_CLASS"],
                channel_csv["CHANNEL_CLASS_ID"],
            ):
                cha_dict["channel_id"].append(id)
                cha_dict["channel_desc"].append(des)
                cha_dict["channel_class"].append(cla)
                cha_dict["channel_class_id"].append(cla_id)
        if cha_dict["channel_id"]:
            con_db_stg.connect().execute("TRUNCATE TABLE channels_ext")
            
            df_channels = pd.DataFrame(cha_dict)
            df_channels.to_sql("channels_ext", con_db_stg, if_exists="append", index=False)
         
  
    except:
        traceback.print_exc()
    finally:
        pass