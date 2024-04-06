from pyspark.sql import SparkSession

# Start Spark session
spark = SparkSession.builder \
    .appName("TransactionAnalytics") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .getOrCreate()

# Read Parquet file as DataFrame
df = spark.read.parquet("transactions_backup.parquet")

# Create a temporary view
df.createOrReplaceTempView("transactions")

# Run SparkSQL queries
result = spark.sql("SELECT * FROM transactions WHERE amount > 100")
result.show()

# Stop Spark session
spark.stop()

