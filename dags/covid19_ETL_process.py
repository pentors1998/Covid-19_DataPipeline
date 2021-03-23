import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
import sqlite3
from datetime import datetime

#Validate
def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if DataFrame is empty
    if df.empty:
        print("No data downloaded, Finishing Exeution")
        return False

    # Primary Key (update_date column) Check
    if pd.Series(df["UpdateDate"]).is_unique:
        pass
    else:
        raise Exception("Primary key check is violated")
    
    return True

#Extract
def get_covid19_report_daily():
    
    url = 'https://covid19.th-stat.com/api/open/today'
    response = requests.get(url)
    
    report_dict = {}

    for key, value in response.json().items():
        report_dict[key] = value
    
    print(report_dict)
    
    report_df = pd.DataFrame(report_dict, index=[0])
    
    # Check Validate
    print(report_df)
    if check_if_valid_data(report_df):
        print("Data valid, proceed to load stage..")

    return report_df

def load_data_into_db():
    # Load
    report_df = get_covid19_report_daily()
    database_location = 'sqlite:///covid19_daily.sqlite'

    engine = sqlalchemy.create_engine(database_location)
    conn = sqlite3.connect('covid19_daily.sqlite')
    cursor = conn.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS daily_covid19_reports(
            Confirmed VARCHAR(200),
            Recovered VARCHAR(200),
            Hospitalized VARCHAR(200),
            Deaths VARCHAR(200),
            NewConfirmed VARCHAR(200),
            NewRecovered VARCHAR(200),
            NewHospitalized VARCHAR(200),
            NewDeaths VARCHAR(200),
            UpdateDate VARCHAR(200),
            Source VARCHAR(200),
            DevBy VARCHAR(200),
            SeverBy VARCHAR(200),
            CONSTRAINT primary_key_constraint PRIMARY KEY (UpdateDate)
    );
    """

    cursor.execute(sql_query)
    print("Opened Database successfully")

    try:
        report_df.to_sql("daily_covid19_reports", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the database")

    conn.close()
    print("Closed database successfully")