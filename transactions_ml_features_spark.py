from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("TransactionsAnalytics") \
    .getOrCreate()

# Load data from Parquet file
parquet_file_path = "transactions_ml_features.parquet"
df = spark.read.parquet(parquet_file_path)

# Create a temporary view to perform SQL queries
df.createOrReplaceTempView("transactions")

# Example SQL queries
# 1. Count the total number of transactions per user
total_transactions_per_user = spark.sql("SELECT user_id, SUM(total_transactions_count) AS total_transactions FROM transactions GROUP BY user_id")

# 2. Get the user with the highest number of transactions
max_transactions_user = spark.sql("SELECT user_id, total_transactions_count FROM transactions ORDER BY total_transactions_count DESC LIMIT 1")

# Show the results
total_transactions_per_user.show()
max_transactions_user.show()

# Stop SparkSession
spark.stop()

