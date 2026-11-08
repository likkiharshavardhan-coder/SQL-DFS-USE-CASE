# colab:
# ============ 1. Install & Java ============
!pip install -q pyspark==3.5.1 setuptools
!java -version 2>&1 | head -1 || (apt-get update -qq && apt-get install -y -qq openjdk-11-jdk-headless)

# ============ 2. Download Kafka connector JARs ============
!mkdir -p /content/jars
!cd /content/jars && \
  wget -nc -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
  wget -nc -q https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
  wget -nc -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
  wget -nc -q https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar
!ls -lh /content/jars

# ============ 3. Imports ============
import os, sys, uuid, glob
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable

# ============ 4. Kafka config ============
kafka_bootstrap_servers = "pkc-41p56.asia-south1.gcp.confluent.cloud:9092"
kafka_topic = "Kafkanew"
kafka_api_key = "***4U3J4IN2PYHDGPTA********"
kafka_api_secret = "***cfltFB+G4zG7RBx8bUkQ5j7/kGbxEXorIx5jvOUoF0jQ7RJbUNe0wX8m7f4bIsPw*******"
kafka_group_id = f"spark-consumer-{uuid.uuid4()}"

# ============ 5. Spark session (with connector JARs) ============
jars = ",".join(sorted(glob.glob("/content/jars/*.jar")))

spark = (
    SparkSession.builder
    .appName("pyspark")
    .master("local[*]")
    .config("spark.jars", jars)
    .config("spark.driver.host", "localhost")
    .config("spark.default.parallelism", "1")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# ============ 6. Read from Kafka (streaming) ============
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option(
        "kafka.sasl.jaas.config",
        f'org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="{kafka_api_key}" password="{kafka_api_secret}";'
    )
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "earliest")
    .option("kafka.group.id", kafka_group_id)
    .load()
)

# ============ 7. Print each micro-batch as a DataFrame ============
def show_batch(batch_df, batch_id):
    df = batch_df.selectExpr("CAST(value AS STRING) AS value")
    if df.isEmpty():
        return  # skip empty batches
    print(f"Batch {batch_id}")
    df.show(truncate=False)
    sys.stdout.flush()

query = (
    kafka_df.writeStream
    .outputMode("append")
    .foreachBatch(show_batch)
    .trigger(processingTime="5 seconds")
    .start()
)

print("Streaming started. Waiting for messages... (press the stop button to end)")
try:
    query.awaitTermination()
except KeyboardInterrupt:
    query.stop()
    print("Stream stopped.")



