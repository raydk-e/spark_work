import pandas as pd
import psycopg2
from sqlalchemy import create_engine

database = "analytics"
user = "postgres"
password ="password"
host =  "localhost"
port = "5432"

connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
engine = create_engine(connection_string)

query = "SELECT * FROM real_expense"

try:
    df = pd.read_sql(query,engine)
    print("Data loaded Succesfully")
    print(df.head())
except Exception as e:
    print(f"Error as {e}")
finally:
    engine.dispose()

