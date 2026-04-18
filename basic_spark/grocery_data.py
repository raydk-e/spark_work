from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import ArrayType, FloatType
# from   import sentencetransformer
import psycopg2
import os

jar_path = os.path.expanduser("~/spark_jars")
pg_jar = os.path.join(jar_path,"postgresql-42.7.2.jar")


spark = SparkSession.builder\
    .appName("grocery_data")\
    .config("spark.jars", pg_jar)\
    .config("spark.driver.extraClassPath",pg_jar)\
    .getOrCreate()

db_url =  "jdbc:postgresql://localhost:5432/analytics"
db_properties = {
        "user":"postgres",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }

def ctreate_postgres_table():
    conn = psycopg2.connect(database = 'analytics', user="postgres", password="password", host="localhost")
    cur =  conn.cursor()
    cur.execute("""
                CREATE TABLE IF NOT EXISTS product_embeddings (
            item_name TEXT,
            brand TEXT,
            type TEXT,
            weight TEXT,
            volume TEXT,
            unit TEXT,
            price INT,
            currency TEXT
            );
        """)
    conn.commit()
    cur.close()
    conn.close()

ctreate_postgres_table()
df = spark.read\
    .option("multiline", "true")\
    .json("/home/deepak/projects/sparkwork/data/grocery.json")

df.show()

df.write\
    .jdbc(url= db_url, table= "product_info", mode="append", properties =db_properties)
print("Data loaded Successfully")
