"""Module for processing DataExp expense data using Pandas."""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, date_format, dayofmonth
import os

jar_dir = os.path.expanduser("~/spark_jars")
postgres_jar = os.path.join(jar_dir, "postgresql-42.7.2.jar")
duckdb_jar = os.path.join(jar_dir, "duckdb_jdbc-1.1.3.jar")
all_jars = f"{postgres_jar},{duckdb_jar}"
spark = SparkSession.builder\
    .appName("pg_expense_to_duckdb")\
    .config("spark.jars",all_jars)\
    .config("spark.driver.extraClassPath", all_jars)\
    .getOrCreate()
pg_options = {
    "url": "jdbc:postgresql://localhost:5432/analytics",
    "dbtable": "real_expense",
    "user":"postgres",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

df_source =  spark.read.format("jdbc").options(**pg_options).load()

df_transformed = df_source.filter(col("expense_debit").isNotNull() & (col("expense_debit")>0))\
    .withColumn("year", year(col("date")))\
    .withColumn("month", date_format(col("date"), "MMMM"))\
    .withColumn("day",dayofmonth(col("date")))\
    .select("year", "month", "day", "description", "category","expense_debit")\

temp_path = "/tmp/transformed_expenses"
df_transformed.write.mode("overwrite").parquet(temp_path)

print("Transformation Completes. Data saved in temp Parquet")
input("Press Enter to stop Spark")
spark.stop()
