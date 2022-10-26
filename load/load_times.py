import traceback
import pandas as pd

# Db stays the same
def load_times(cod_etl, con_db_stg, con_db_sor):
    try:
        times_dict = {
            "time_id": [],
            "day_name": [],
            "day_number_in_week": [],
            "day_number_in_month": [],
            "calendar_week_number": [],
            "calendar_month_number": [],
            "calendar_month_desc": [],
            "end_of_cal_month": [],
            "calendar_month_name": [],
            "calendar_quarter_desc": [],
            "calendar_year": [],
            'cod_etl': [],
        }

        #SqlCommand & stg connection
        times_transform = pd.read_sql(
            "SELECT time_id,day_name, day_number_in_week, day_number_in_month,calendar_week_number,calendar_month_number,calendar_month_desc,end_of_cal_month,calendar_month_name,calendar_quarter_desc,calendar_year FROM times_tra",con_db_stg,
        )

        if not times_transform.empty:
            for id,day_n,day_n_week,day_n_month,cal_week_n,cal_month_n,cal_month_des,cal_end,cal_month_name,cal_qua_desc,cal_year, in zip(
                times_transform["time_id"],
                times_transform["day_name"],
                times_transform["day_number_in_week"],
                times_transform["day_number_in_month"],
                times_transform["calendar_week_number"],
                times_transform["calendar_month_number"],
                times_transform["calendar_month_desc"],
                times_transform["end_of_cal_month"],
                times_transform["calendar_month_name"],
                times_transform["calendar_quarter_desc"],
                times_transform["calendar_year"],
            ):
                times_dict["time_id"].append(id)
                times_dict["day_name"].append(day_n)
                times_dict["day_number_in_week"].append(day_n_week)
                times_dict["day_number_in_month"].append(day_n_month
                                                             )
                times_dict["calendar_week_number"].append(cal_week_n)        
                times_dict["calendar_month_number"].append(cal_month_n)
                times_dict["calendar_month_desc"].append(cal_month_des)
                times_dict["end_of_cal_month"].append(cal_end)
                times_dict["calendar_month_name"].append(cal_month_name)
                times_dict["calendar_quarter_desc"].append(cal_qua_desc)
                times_dict["calendar_year"].append(cal_year)
                times_dict['cod_etl'].append(cod_etl)

        if times_dict["time_id"]:

            df_times = pd.DataFrame(times_dict)

            #Load process
            df_times.to_sql("new_times", con_db_sor, if_exists="append", index=False)

    except:
        traceback.print_exc()
    finally:
        pass