import requests
import json
import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col


api_url = 'https://gorest.co.in/public/v2/posts'
file_path = f"data/gorestapi.json"
jar_path = os.path.expanduser("~/spark_jars")
pg_jar = os.path.join(jar_path,"postgresql-42.7.2.jar")
try:
    response= requests.get(url=api_url)

    if response.status_code == 200:
        response_json = response.json()
        # print(response_json)
        os.makedirs("data",exist_ok=True)
        with open(file_path, "w") as file:
            json.dump(response_json,file, indent=4)
        print("response is captured in file")
        spark = SparkSession.builder\
            .appName("gorestapi_transform")\
            .config("spark.jars", pg_jar)\
            .config("spark.driver.extraClassPath", pg_jar)\
            .getOrCreate()

        df = spark.read.option("multiline", "true").json(file_path)
        df_body_word_count = df.withColumn("body_word_count", F.size(F.split(F.col("body"),"\\s+")))
        db_url =  "jdbc:postgresql://localhost:5432/analytics"
        db_properties = {
            "user":"postgres",
            "password": "password",
            "driver": "org.postgresql.Driver"
        }
        print("Writing to Postgres...")
        df_body_word_count.write.jdbc(
            url=db_url,
            table="gorest_posts",
            mode="overwrite", # Use "append" if you don't want to delete existing data
            properties=db_properties
        )
        print("Data successfully loaded to Postgres table: gorest_posts")
    else:
        print(f"failed to fetch the data. Status Code: {response.status_code}")

except Exception as e:
    print(f" Response is not capture,{e} ")



df_body_word_count.show()


