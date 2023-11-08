import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Initialize logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s:%(funcName)s:%(levelname)s:%(message)s"
)
logger = logging.getLogger("spark_structured_streaming")


def initialize_spark_session(
    app_name: str, access_key: str, secret_key: str
) -> SparkSession | None:
    """
    Initialize the Spark Session with provided configurations.

    Args:
        app_name (str): Name of the spark application.
        access_key (str): Access key for S3.
        secret_key (str): Secret key for S3.

    Returns:
        SparkSession: Spark session object or None if there's an error.
    """

    try:
        spark = (
            SparkSession.builder.appName(app_name)
            .config("spark.hadoop.fs.s3a.access.key", access_key)
            .config("spark.hadoop.fs.s3a.secret.key", secret_key)
            .config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("ERROR")
        logger.info("Spark session initialized successfully")
        return spark

    except Exception as e:
        logger.error(f"Spark session initialization failed. Error: {e}")
        return None


def get_streaming_dataframe(
    spark: SparkSession, brokers: str, topic: str
) -> DataFrame | None:
    """
    Get a streaming dataframe from Kafka.

    Args:
        spark (SparkSession): Initialized Spark session.
        brokers (str): Comma-separated list of Kafka brokers.
        topic (str): Kafka topic to subscribe to.

    Returns:
        DataFrame: Dataframe object or None if there's an error.
    """

    try:
        df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", brokers)
            .option("subscribe", topic)
            .option("delimiter", ",")
            .option("startingOffsets", "earliest")
            .load()
        )
        logger.info("Streaming dataframe fetched successfully")
        df.show()
        return df
    except Exception as e:
        logger.warning(f"Failed to fetch streaming dataframe. Error: {e}")
        return None


def transform_streaming_data(df: DataFrame) -> DataFrame:
    """
    Transform the initial dataframe to get the final structure.

    Args:
        df (DataFrame): Initial dataframe with raw data.

    Returns:
        DataFrame: Transformed dataframe.
    """

    schema = StructType(
        [
            StructField("full_name", StringType(), False),
            StructField("gender", StringType(), False),
            StructField("location", StringType(), False),
            StructField("city", StringType(), False),
            StructField("country", StringType(), False),
            StructField("postcode", IntegerType(), False),
            StructField("latitude", FloatType(), False),
            StructField("longitude", FloatType(), False),
            StructField("email", StringType(), False),
        ]
    )

    transformed_df = (
        df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )
    transformed_df.show()
    return transformed_df


def initiate_sreaming_to_bucket(
    df: DataFrame, path: str, checkpoint_location: str
) -> None:
    """
    Start streaming the transformed data to the specified S3 bucket in parquet format.

    Args:
        df (DataFrame): Transformed dataframe.
        path (str): S3 bucket path.
        checkpoint_location (str): Checkpoint location for streaming.

    Returns:
        None
    """

    logger.info("Initiating streaming process...")
    stream_query = (
        df.writeStream.format("parquet")
        .outputMode("append")
        .option("path", path)
        .option("checkpointLocation", checkpoint_location)
        .start()
    )
    stream_query.awaitTermination()


def main():
    app_name = "SparkStructuredStreamingToS3"
    access_key = "ENTER_YOUR_ACCESS_KEY"
    secret_key = "ENTER_YOUR_SECRET_KEY"
    brokers = "kafka_broker_1:19092,kafka_broker_2:19093,kafka_broker_3:19094"
    topic = "names_topic"
    path = "BUCKET_PATH"
    checkpoint_location = "CHECKPOINT_LOCATION"

    spark = initialize_spark_session(app_name, access_key, secret_key)
    if spark:
        df = get_streaming_dataframe(spark, brokers, topic)
        if df:
            transformed_df = transform_streaming_data(df)
            initiate_sreaming_to_bucket(transformed_df, path, checkpoint_location)


# Execute the main function if this script is run as the main module
if __name__ == "__main__":
    main()
