import traceback

import pandas as pd

from transform.transform_method import str_int

def transform_channels(cod_etl, con_db_stg):
    try:
        channel_dict = {
            "channel_id": [],
            "channel_desc": [],
            "channel_class": [],
            "channel_class_id": [],
            "cod_etl": [],
        }

        #SqlCommand & stg connection
        channel_ext = pd.read_sql(
            "SELECT CHANNEL_ID,CHANNEL_DESC, CHANNEL_CLASS, CHANNEL_CLASS_ID FROM channels_ext",
            con_db_stg,
        )

       #Channel_ext table in database
        if not channel_ext.empty:
            for id, des, cla, cla_id in zip(
                    channel_ext["CHANNEL_ID"],
                    channel_ext["CHANNEL_DESC"],
                    channel_ext["CHANNEL_CLASS"],
                    channel_ext["CHANNEL_CLASS_ID"],
                    
            ):
                channel_dict["channel_id"].append(str_int(id))
                channel_dict["channel_desc"].append(des)
                channel_dict["channel_class"].append(cla)
                channel_dict["channel_class_id"].append(str_int(cla_id))
                channel_dict["cod_etl"].append(cod_etl)

        if channel_dict["channel_id"]:
            df_ch_tra = pd.DataFrame(channel_dict)
            df_ch_tra.to_sql(
                "channels_tra", con_db_stg, if_exists="append", index=False
            )

    except:
        traceback.print_exc()
    finally:
        pass