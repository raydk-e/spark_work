from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, current_timestamp

spark = SparkSession.builder\
    .appName("postgresgrocerytobq")\
    .getOrCreate()

conf = spark._jsc.hadoopConfiguration()
conf.set("google.cloud.auth.service.account.enable", "true")
conf.set("google.cloud.auth.service.account.json.keyfile", "/home/deepak/projects/sparkwork/keys/creds.json")
conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

gcs_destination = "gs://dataexpdot"

pg_df = spark.read\
    .format("jdbc")\
    .option("url", "jdbc:postgresql://localhost:5432/analytics")\
    .option("dbtable", "grocery_stream_results")\
    .option("user", "postgres")\
    .option("password", "password")\
    .option("driver", "org.postgresql.Driver")\
    .load()
agg_df = pg_df.groupBy("brand")\
    .agg(sum("price").alias("total_revenue"))\
    .withColumn("processed_at",current_timestamp())

agg_df.write\
    .format("bigquery")\
    .option("temporaryGcsBucket", "dataexp")\
    .option("table", "raydeepak0206.dataexp.brand_revenue_summary")\
    .mode("overwrite")\
    .save()

print("Aggregation Complete")