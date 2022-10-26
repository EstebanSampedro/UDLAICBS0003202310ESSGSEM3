import traceback
import pandas as pd
from util.sql_etl_methods import merge

def load_customers(cod_etl, con_db_stg, con_db_sor):
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
        customers_tra = pd.read_sql(
            f"SELECT cust_id,cust_name,cust_gender,cust_year_of_birth,cust_marital_status,cust_street_address,cust_postal_code,cust_city,cust_state_province,country_id,cust_main_phone_number,cust_income_level,cust_credit_limit,cust_email FROM customers_tra WHERE cod_etl = {cod_etl}", con_db_stg,
        )

        #Surrogate keys config
        customers_countries_surrogate_key = \
            pd.read_sql_query("SELECT surr_id, country_id from new_countries", con_db_sor).set_index(
                "country_id").to_dict()["surr_id"]

        customers_tra["country_id"] = customers_tra["country_id"].apply(lambda key: customers_countries_surrogate_key[key])
        
        #Customers transform table 
        if not customers_tra.empty:
            for id, name,gender,year_birth,m_status,street,postal,city,state_province,country_id,phone,income,credit,email, in zip(
                customers_tra["cust_id"],
                customers_tra["cust_name"],
                customers_tra["cust_gender"],
                customers_tra["cust_year_of_birth"],
                customers_tra["cust_marital_status"],
                customers_tra["cust_street_address"],
                customers_tra["cust_postal_code"],
                customers_tra["cust_city"],
                customers_tra["cust_state_province"],
                customers_tra["country_id"],
                customers_tra["cust_main_phone_number"],
                customers_tra["cust_income_level"],
                customers_tra["cust_credit_limit"],
                customers_tra["cust_email"],
            ):
                customers_dict["cust_id"].append(id)
                customers_dict["cust_name"].append(name)
                customers_dict["cust_gender"].append(gender)
                customers_dict["cust_year_of_birth"].append(year_birth)
                customers_dict["cust_marital_status"].append(m_status)
                customers_dict["cust_street_address"].append(street)
                customers_dict["cust_postal_code"].append(postal)
                customers_dict["cust_city"].append(city)
                customers_dict["cust_state_province"].append(state_province)
                customers_dict["country_id"].append(country_id)
                customers_dict["cust_main_phone_number"].append(phone)
                customers_dict["cust_income_level"].append(income)
                customers_dict["cust_credit_limit"].append(credit)
                customers_dict["cust_email"].append(email)
                customers_dict["cod_etl"].append(cod_etl)

        
        if customers_dict["cust_id"]:
            df_new_customer = pd.DataFrame(customers_dict)
            
            #merge method for loading 
            merge(table_name="new_customers", business_key_col=["cust_id"], dataframe=df_new_customer,db_context=con_db_sor)
        con_db_stg.dispose()
        con_db_sor.dispose()
    except Exception:
        traceback.print_exc()
    finally:
        pass