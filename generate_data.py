from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import random
import uuid


def initialize_spark():
    """Initialize a Spark session for data processing."""
    return (
        SparkSession.builder.appName("AB Testing Data Generation")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def generate_user_data(spark, num_users=10000):
    """Generate synthetic user data for A/B testing."""

    # Define schema for user data
    user_schema = StructType(
        [
            StructField("user_id", StringType(), False),
            StructField("variant", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("session_duration_sec", IntegerType(), True),
            StructField("pages_viewed", IntegerType(), True),
            StructField("clicked_cta", BooleanType(), True),
            StructField("converted", BooleanType(), True),
            StructField("device_type", StringType(), True),
            StructField("referrer", StringType(), True),
            StructField("country", StringType(), True),
        ]
    )

    # Generate data
    data = []
    devices = ["mobile", "desktop", "tablet"]
    referrers = ["google", "direct", "social", "email", "other"]
    countries = ["US", "UK", "CA", "DE", "FR", "ES", "IN", "BR", "JP", "AU"]

    # Conversion rates - Variant B has higher conversion
    conv_rate_a = 0.12
    conv_rate_b = 0.15

    for i in range(num_users):
        user_id = str(uuid.uuid4())
        variant = random.choice(["A", "B"])
        timestamp = current_timestamp()
        device = random.choice(devices)
        referrer = random.choice(referrers)
        country = random.choice(countries)

        # Simulate different behaviors based on variant
        if variant == "A":
            pages_viewed = random.randint(1, 8)
            session_duration = random.randint(10, 300)
            clicked_cta = random.random() < 0.25
            converted = random.random() < conv_rate_a
        else:  # Variant B
            pages_viewed = random.randint(1, 10)
            session_duration = random.randint(15, 400)
            clicked_cta = random.random() < 0.35
            converted = random.random() < conv_rate_b

        data.append(
            (
                user_id,
                variant,
                timestamp,
                session_duration,
                pages_viewed,
                clicked_cta,
                converted,
                device,
                referrer,
                country,
            )
        )

    # Create DataFrame
    df = spark.createDataFrame(data, schema=user_schema)
    return df


def save_data(df, output_path):
    """Save the generated data to a CSV file."""
    df.write.mode("overwrite").csv(output_path, header=True)


if __name__ == "__main__":
    spark = initialize_spark()
    user_df = generate_user_data(spark)
    save_data(user_df, "data/raw/user_simulation.csv")
    spark.stop()
