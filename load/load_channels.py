import traceback
import pandas as pd

def load_channels(cod_etl, con_db_stg, con_db_sor):
    try:
        channel_dict = {
            "channel_id": [],
            "channel_desc": [],
            "channel_class": [],
            "channel_class_id": [],
            "cod_etl": [],
        }

        #SqlCommand for channel table
        channel_tra = pd.read_sql(
            "SELECT channel_id,channel_desc, channel_class, channel_class_id FROM channels_tra", con_db_stg,
        )

        if not channel_tra.empty:
            for id, des, cla, cla_id  in zip(
                    channel_tra["channel_id"],
                    channel_tra["channel_desc"],
                    channel_tra["channel_class"],
                    channel_tra["channel_class_id"],
            ):
                channel_dict["channel_id"].append(id)
                channel_dict["channel_desc"].append(des)
                channel_dict["channel_class"].append(cla)
                channel_dict["channel_class_id"].append(cla_id)
                channel_dict["cod_etl"].append(cod_etl)

        if channel_dict["channel_id"]:

            #Load process
            df_channel_tra = pd.DataFrame(channel_dict)
            df_channel_tra.to_sql(
                "new_channels", con_db_sor, if_exists="append", index=False
            )
    except:
        traceback.print_exc()
    finally:
        pass