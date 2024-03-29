import yaml 
import json
import pandas as pd

def load_config(file_path):
    with open(file_path, 'r') as stream:
        try:
            config_data = yaml.safe_load(stream)
            return config_data
        except yaml.YAMLError as exc:
            print(f"Error loading YAML file: {exc}")
            return None

# Example usage
config_file_path = 'config.yaml'
config = load_config(config_file_path)

if config:
    print("Configuration loaded successfully.")
else:
    print("Failed to load configuration.")

#################################
# postgres database
#################################
from sqlalchemy import create_engine, inspect, text

def pg_connect():
    db_username = config['postgres']['username']
    db_password = config['postgres']['password']
    db_host = config['postgres']['host']
    db_port = config['postgres']['port']
    db_name = config['postgres']['name']
    connection_string = f'postgresql+psycopg2://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}'
    return create_engine(connection_string)

def pg_tables():
    engine = pg_connect()
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    engine.dispose()
    return sorted(tables)

def pg_query(query):
    engine = pg_connect()
    data = pd.read_sql(query,con=engine)
    engine.dispose()
    return data

def pg_execute_query(query):
    sql = text(query)
    engine = pg_connect()
    with engine.connect() as connection:
        result = connection.execute(sql)
        data = result.fetchall()
    engine.dispose()

    return data

def pg_execute(query):
    sql = text(query)
    engine = pg_connect()
    with engine.connect() as connection:
        result = connection.execute(sql)
        connection.commit()
    engine.dispose()
    return "done"

def pg_table_info(table_name):
    engine = pg_connect()
    inspector = inspect(engine)

    # Get table columns
    columns = inspector.get_columns(table_name)
    for c in columns:
        print(c['name'])


    # Get table primary key
    primary_key = inspector.get_pk_constraint(table_name)
    print(f"Primary Key: {primary_key}")

    # Get table foreign keys
    foreign_keys = inspector.get_foreign_keys(table_name)
    print(f"Foreign Keys: {foreign_keys}")

    engine.dispose()

def pg_clean_table(tbl):
    pg_execute(f"DROP TABLE IF EXISTS {tbl}_backup")
    pg_execute(f"""CREATE TABLE {tbl}_backup AS
    SELECT DISTINCT * FROM {tbl};
    """)
    pg_execute(f"DELETE FROM {tbl};")
    pg_execute(f"INSERT INTO {tbl} SELECT * FROM {tbl}_backup;")
    pg_execute(f"DROP TABLE {tbl}_backup;")
    print(f"Duplicates removed from {tbl}")
