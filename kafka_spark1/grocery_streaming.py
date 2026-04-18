from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# 1. Initialize Spark Session
spark = SparkSession.builder\
    .appName("GroceryStreaming")\
    .config("spark.sql.shuffle.partitions", "2")\
    .getOrCreate()

# 2. Define the Schema (Must match the structure in your grocery.json)
schema = StructType([
    StructField("item_name", StringType()),
    StructField("brand", StringType()),
    StructField("type", StringType()),
    StructField("price", IntegerType()),
    StructField("currency", StringType()),
])

# 3. Read from Kafka
kafka_df = spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "grocery-topic")\
    .option("startingOffsets", "latest")\
    .load()

# 4. Parse the JSON 'value' column
parse_df = kafka_df.selectExpr("CAST(value AS STRING)")\
    .select(from_json(col("value"), schema).alias("data"))\
    .select("data.*")

# 5. Output to Console (For immediate feedback)
console_query = parse_df.writeStream\
    .outputMode("append")\
    .format("console")\
    .trigger(processingTime='5 seconds')\
    .start()

# 6. Function to write each micro-batch to PostgreSQL
def write_to_postgres(batch_df, batch_id):
    # Only write if the batch contains data
    if batch_df.count() > 0:
        print(f"Writing Batch {batch_id} to database...")
        batch_df.write\
            .format("jdbc")\
            .option("url", "jdbc:postgresql://localhost:5432/analytics")\
            .option("dbtable", "grocery_stream_results")\
            .option("user", "postgres")\
            .option("password", "password")\
            .option("driver", "org.postgresql.Driver")\
            .mode("append")\
            .save()

# 7. Start the PostgreSQL stream
db_query = parse_df.writeStream\
    .foreachBatch(write_to_postgres)\
    .option("checkpointLocation", "/tmp/spark_checkpoints/grocery_db")\
    .outputMode("append")\
    .start()

print("Spark Listening to Kafka Message -- Check console every 5 seconds...")

# 8. Keep the stream alive
spark.streams.awaitAnyTermination()
