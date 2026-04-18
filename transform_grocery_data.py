from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, current_date

# Create Spark session
spark = SparkSession.builder \
    .appName("Transform Grocery Data") \
    .master("local[2]") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

# Read grocery.json
grocery_df = spark.read.json("data/grocery.json")

# Calculate total price per category
total_price_per_category = grocery_df.groupBy("category").agg(sum("price").alias("total_price"))

# Add a column for processing date
processed_date_df = total_price_per_category.withColumn("processing_date", current_date())

# Show the result
processed_date_df.show()

# Save as Parquet
processed_date_df.write.parquet("output/transformed_grocery_data")

# Stop Spark session
spark.stop()
