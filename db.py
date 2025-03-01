from sqlalchemy import URL
from sqlalchemy import create_engine
import yaml
import pandas as pd 
import sqlalchemy

with open("vanilla/profiles.yml") as stream:
    try:
        profile_config = yaml.safe_load(stream)
        database_config = profile_config['vanilla']['outputs']['dev']
        database_connect_url = URL.create(
            "postgresql",
            username=database_config['user'],
            password=database_config['password'],
            host=database_config['host'],
            database=database_config['dbname'],
        )
        engine = create_engine(database_connect_url)

    except yaml.YAMLError as exc:
        print(exc)


def write_data_to_db(df: pd.DataFrame, table_name: str, check_if_exists=True) -> None:
    """Writes the dataframe to the database """
    if (check_if_exists):
        if sqlalchemy.inspect(engine).has_table(table_name):
            df.to_sql(
                name=table_name,
                con=engine,
                if_exists='append',
                index=False
            )
            print(f"Successfully inserted {len(df)} records for {table_name}")
        
        else: 
            print(f"{table_name} table doest not exists, create it first")
    else:
        df.to_sql(
                name=table_name,
                con=engine,
                if_exists='append',
                index=False
            )
        print(f"Successfully created table and inserted {len(df)} records for {table_name}")