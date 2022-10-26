import traceback
import pandas as pd
from transform.transform_method import str_int, obt_month_name, str_date

def transform_times(cod_etl, con_db_stg):
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
        times_ext = pd.read_sql(
            "SELECT TIME_ID,DAY_NAME, DAY_NUMBER_IN_WEEK, DAY_NUMBER_IN_MONTH,CALENDAR_WEEK_NUMBER,CALENDAR_MONTH_NUMBER,CALENDAR_MONTH_DESC,END_OF_CAL_MONTH,CALENDAR_QUARTER_DESC,CALENDAR_YEAR FROM times_ext", con_db_stg,
        )
    
        if not times_ext.empty:
            for id,day_n,day_n_week,day_n_month,cal_week_n,cal_month_n,cal_month_des,cal_end,cal_qua_desc,cal_year, in zip(
                times_ext["TIME_ID"],
                times_ext["DAY_NAME"],
                times_ext["DAY_NUMBER_IN_WEEK"],
                times_ext["DAY_NUMBER_IN_MONTH"],
                times_ext["CALENDAR_WEEK_NUMBER"],
                times_ext["CALENDAR_MONTH_NUMBER"],
                times_ext["CALENDAR_MONTH_DESC"],
                times_ext["END_OF_CAL_MONTH"],
                times_ext["CALENDAR_QUARTER_DESC"],
                times_ext["CALENDAR_YEAR"],
            ):
                times_dict["time_id"].append(str_date(id))
                times_dict["day_name"].append(day_n)
                times_dict["day_number_in_week"].append(str_int(day_n_week))
                times_dict["day_number_in_month"].append(str_int(day_n_month))
                times_dict["calendar_week_number"].append(str_int(cal_week_n))
                times_dict["calendar_month_number"].append(str_int(cal_month_n))
                times_dict["calendar_month_desc"].append(cal_month_des)
                times_dict["end_of_cal_month"].append(str_date(cal_end))
                times_dict["calendar_month_name"].append(obt_month_name(cal_month_n))
                times_dict["calendar_quarter_desc"].append(cal_qua_desc)
                times_dict["calendar_year"].append(str_int(cal_year))
                times_dict['cod_etl'].append(cod_etl)

        if times_dict["time_id"]:
            df_times = pd.DataFrame(times_dict)
            df_times.to_sql("times_tra", con_db_stg, if_exists="append", index=False)

    except:
        traceback.print_exc()
    finally:
        pass