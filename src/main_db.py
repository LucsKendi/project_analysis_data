from modules.libs import execute_query, send_dataframe_to_db
import pandas as pd 
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine

if __name__ == "__main__":
    
    load_dotenv(".env")
    # Database connection parameters
    db_params = {
    'dbname': os.getenv("DB_NAME"),
    'user': os.getenv("DB_USERNAME"),
    'password': os.getenv("DB_PASS"),
    'host': os.getenv("DB_HOST"),  # or your PostgreSQL server IP
    'port': 5432       # default PostgreSQL port
    }

    dataframe = pd.read_csv("raw_files/credit_features_subset.csv")
    dataframe1 = pd.read_csv("raw_files/loan_applications.csv")
    send_dataframe_to_db(dataframe, "credit_features" , db_params)
    send_dataframe_to_db(dataframe1, "loan_applications" , db_params)