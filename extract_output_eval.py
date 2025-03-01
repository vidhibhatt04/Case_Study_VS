import pandas as pd
from sqlalchemy import create_engine
from db import engine, write_data_to_db
import numpy as np
import sqlalchemy

# Load the data from MySQL tables
def load_data():
    buyer_df = pd.read_sql("SELECT * FROM buyers", engine)
    suppliers_df = pd.read_sql("SELECT * FROM suppliers", engine)
    recommendations_df = pd.read_sql("SELECT * FROM recommendations", engine)
    recommendations_with_score_df = pd.read_sql("SELECT * FROM recommendations_with_score", engine)

    buyer_df.to_csv('output_csv/buyer_data.csv')
    suppliers_df.to_csv('output_csv/suppliers_data.csv')
    recommendations_df.to_csv('output_csv/recommendations.csv')
    recommendations_with_score_df.to_csv('output_csv/recommendations_with_score_df.csv')

if __name__ == "__main__":
    load_data()