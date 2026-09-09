from pyspark.sql import SparkSession

# Improved Spark configuration for OpenShift connectivity
spark = SparkSession.builder \
    .appName("devops") \
    .master("spark://172.18.141.44:30077") \
    .config("spark.executor.memory", "512m") \
    .config("spark.driver.memory", "512m") \
    .config("spark.executor.instances", "2") \
    .config("spark.executor.cores", "1") \
    .config("spark.driver.host", "your-local-ip") \
    .config("spark.driver.port", "7001") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .config("spark.network.timeout", "300s") \
    .getOrCreate()

try:
    # Simple test to verify connectivity
    print("Testing Spark connectivity...")
    df = spark.range(10)
    df.show()
    print("✅ Spark job completed successfully!")

except Exception as e:
    print(f"❌ Spark job failed: {e}")

finally:
    spark.stop()