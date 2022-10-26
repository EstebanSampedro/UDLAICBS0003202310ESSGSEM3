import traceback
import pandas as pd

from transform.transform_method import str_int, str_char, join_two_strings


#
def transform_customers(cod_etl, con_db_stg):
    try:

        customers_dict = {
            "cust_id": [],
            "cust_name": [],
            "cust_gender": [],
            "cust_year_of_birth": [],
            "cust_marital_status": [],
            "cust_street_address": [],
            "cust_postal_code": [],
            "cust_city": [],
            "cust_state_province": [],
            "country_id": [],
            "cust_main_phone_number": [],
            "cust_income_level": [],
            "cust_credit_limit": [],
            "cust_email": [],
            "cod_etl": [],
        }

        #SqlCommand & stg connection
        customers_ext = pd.read_sql(
            "SELECT CUST_ID,CUST_FIRST_NAME, CUST_LAST_NAME, CUST_GENDER,CUST_YEAR_OF_BIRTH,CUST_MARITAL_STATUS,CUST_STREET_ADDRESS,CUST_POSTAL_CODE,CUST_CITY,CUST_STATE_PROVINCE,COUNTRY_ID,CUST_MAIN_PHONE_NUMBER,CUST_INCOME_LEVEL,CUST_CREDIT_LIMIT,CUST_EMAIL FROM customers_ext",
            con_db_stg,
        )

        if not customers_ext.empty:
            for id,first_name,last_name,gender,year_birth,m_status,street,postal,city,state_province,country_id,phone,income,credit,email, in zip(
                customers_ext["CUST_ID"],
                customers_ext["CUST_FIRST_NAME"],
                customers_ext["CUST_LAST_NAME"],
                customers_ext["CUST_GENDER"],
                customers_ext["CUST_YEAR_OF_BIRTH"],
                customers_ext["CUST_MARITAL_STATUS"],
                customers_ext["CUST_STREET_ADDRESS"],
                customers_ext["CUST_POSTAL_CODE"],
                customers_ext["CUST_CITY"],
                customers_ext["CUST_STATE_PROVINCE"],
                customers_ext["COUNTRY_ID"],
                customers_ext["CUST_MAIN_PHONE_NUMBER"],
                customers_ext["CUST_INCOME_LEVEL"],
                customers_ext["CUST_CREDIT_LIMIT"],
                customers_ext["CUST_EMAIL"],
            ):
                customers_dict["cust_id"].append(str_int(id))
                customers_dict["cust_name"].append(join_two_strings(first_name, last_name))
                customers_dict["cust_gender"].append(str_char(gender))
                customers_dict["cust_year_of_birth"].append(str_int(year_birth))
                customers_dict["cust_marital_status"].append(m_status)
                customers_dict["cust_street_address"].append(street)
                customers_dict["cust_postal_code"].append(postal)
                customers_dict["cust_city"].append(city)
                customers_dict["cust_state_province"].append(state_province)
                customers_dict["country_id"].append(str_int(country_id))
                customers_dict["cust_main_phone_number"].append(phone)
                customers_dict["cust_income_level"].append(income)
                customers_dict["cust_credit_limit"].append(str_int(credit))
                customers_dict["cust_email"].append(email)
                customers_dict["cod_etl"].append(cod_etl)

        if customers_dict["cust_id"]:
            df_customers = pd.DataFrame(customers_dict)
            df_customers.to_sql(
                "customers_tra", con_db_stg, if_exists="append", index=False
            )
            con_db_stg.dispose()
    except:
        traceback.print_exc()
    finally:
        pass