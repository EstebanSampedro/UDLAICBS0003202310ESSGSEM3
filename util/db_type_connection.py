import configparser
#Connection import
from util import db_connection

def db_type_connection():
    config = configparser.ConfigParser()
    config.read(".properties")
    sectionName = "DatabaseCredentials"

    #SOR DATABASE CONNECTION
    sor_conn = db_connection.Db_Connection(
        config.get(sectionName, "DB_TYPE"),
        config.get(sectionName, "DB_HOST"),
        config.get(sectionName, "DB_PORT"),
        config.get(sectionName, "DB_USER"),
        config.get(sectionName, "DB_PWD"),
        config.get(sectionName, "SOR_NAME"),
    )

    #STAGING DATABASE CONNECTION
    stg_conn = db_connection.Db_Connection(
        config.get(sectionName, "DB_TYPE"),
        config.get(sectionName, "DB_HOST"),
        config.get(sectionName, "DB_PORT"),
        config.get(sectionName, "DB_USER"),
        config.get(sectionName, "DB_PWD"),
        config.get(sectionName, "STG_NAME"),
    )
    


    #sessions config
    con_db_sor = sor_conn.start()
    con_db_stg = stg_conn.start()
    
    

    if con_db_stg == -1:
        raise Exception(f"The database type {con_db_stg.type} is not valid")
    elif con_db_stg == -2:
        raise Exception("Error trying to connect to essgdbstg")

    if con_db_sor == -1:
        raise Exception(f"The database type {con_db_sor.type} is not valid")
    elif con_db_sor == -2:
        raise Exception("Error trying to connect to essgdbsor")

    con_dict = {"con_db_stg": con_db_stg, "con_db_sor": con_db_sor}
    return con_dict 