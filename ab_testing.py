from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import numpy as np
from scipy import stats


class ABTester:
    """Class for performing A/B test analysis using PySpark."""

    def __init__(self, spark_session=None):
        """Initialize the AB tester with a Spark session."""
        if spark_session:
            self.spark = spark_session
        else:
            self.spark = SparkSession.builder.appName(
                "AB Testing Analysis"
            ).getOrCreate()

    def load_data(self, data_path):
        """Load experiment data from CSV."""
        return self.spark.read.csv(data_path, header=True, inferSchema=True)

    def calculate_metrics(self, df):
        """Calculate key metrics for each variant."""
        # Group by variant and calculate metrics
        metrics = df.groupBy("variant").agg(
            count("user_id").alias("users"),
            sum(when(col("converted"), 1).otherwise(0)).alias("conversions"),
            avg("session_duration_sec").alias("avg_session_duration"),
            avg("pages_viewed").alias("avg_pages_viewed"),
            sum(when(col("clicked_cta"), 1).otherwise(0)).alias("cta_clicks"),
        )

        # Calculate conversion rate
        metrics = metrics.withColumn(
            "conversion_rate", col("conversions") / col("users")
        )

        # Calculate CTR
        metrics = metrics.withColumn("ctr", col("cta_clicks") / col("users"))

        return metrics

    def perform_hypothesis_test(self, df, metric="converted", alpha=0.05):
        """
        Perform hypothesis testing to determine if there's a significant
        difference between variants.
        """
        # Filter data for each variant
        variant_a = df.filter(col("variant") == "A").select(metric).collect()
        variant_b = df.filter(col("variant") == "B").select(metric).collect()

        # Convert to numpy arrays
        a_values = np.array([row[0] if row[0] is not None else 0 for row in variant_a])
        b_values = np.array([row[0] if row[0] is not None else 0 for row in variant_b])

        # For boolean metrics like conversion, use chi-square
        if df.schema[metric].dataType == BooleanType():
            a_success = np.sum(a_values)
            a_total = len(a_values)
            b_success = np.sum(b_values)
            b_total = len(b_values)

            # Chi-square test
            chi2, p_value, _, _ = stats.chi2_contingency(
                [[a_success, a_total - a_success], [b_success, b_total - b_success]]
            )

            test_type = "Chi-square"

        # For continuous metrics, use t-test
        else:
            _, p_value = stats.ttest_ind(a_values, b_values, equal_var=False)
            test_type = "T-test"

        # Determine if result is significant
        is_significant = p_value < alpha

        return {
            "test_type": test_type,
            "p_value": p_value,
            "is_significant": is_significant,
            "alpha": alpha,
        }

    def analyze_segments(self, df, segment_col):
        """Analyze how different segments respond to variants."""
        return (
            df.groupBy(segment_col, "variant")
            .agg(
                count("user_id").alias("users"),
                avg(when(col("converted"), 1.0).otherwise(0.0)).alias(
                    "conversion_rate"
                ),
                avg("session_duration_sec").alias("avg_session_duration"),
            )
            .orderBy(segment_col, "variant")
        )

    def get_recommendation(self, metrics_df, test_results):
        """Generate a recommendation based on test results."""
        if not test_results["is_significant"]:
            return (
                "No significant difference between variants. More testing recommended."
            )

        # Get metrics for comparison
        metrics = metrics_df.collect()
        a_metrics = next(m for m in metrics if m["variant"] == "A")
        b_metrics = next(m for m in metrics if m["variant"] == "B")

        if b_metrics["conversion_rate"] > a_metrics["conversion_rate"]:
            return "Variant B performs significantly better. Recommend implementing Variant B."
        else:
            return (
                "Variant A performs significantly better. Recommend keeping Variant A."
            )

    def stop(self):
        """Stop the Spark session."""
        self.spark.stop()
