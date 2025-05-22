from pyspark.sql import SparkSession, DataFrame


from pyspark.sql.functions import col
from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def enrich_data(spark: SparkSession) -> DataFrame:
    data = spark.read.csv("/FileStore/data_group_*.csv", header=True, inferSchema=True)
    # remove null values from any cell
    data = data.dropna()
    # drop duplicates 
    data = data.dropDuplicates(["timestamp", "turbine_id"])

    # https://medium.com/@pp1222001/outlier-detection-and-removal-using-the-iqr-method-6fab2954315d
    quantiles = data.approxQuantile(["wind_speed", "power_output"], [0.25, 0.75], 0.05)
    Q1_wind_speed, Q3_wind_speed = quantiles[0]
    Q1_power_output, Q3_power_output = quantiles[1]
    IQR_wind_speed = Q3_wind_speed - Q1_wind_speed
    IQR_power_output = Q3_power_output - Q1_power_output
    
    lower_bound_wind_speed = Q1_wind_speed - 1.5 * IQR_wind_speed
    upper_bound_wind_speed = Q3_wind_speed + 1.5 * IQR_wind_speed
    lower_bound_power_output = Q1_power_output - 1.5 * IQR_power_output
    upper_bound_power_output = Q3_power_output + 1.5 * IQR_power_output
    
    data = data.withColumn(
        "outlier_ind",
        F.when(
            (~col("wind_speed").between(lower_bound_wind_speed, upper_bound_wind_speed)) |
            (~col("power_output").between(lower_bound_power_output, upper_bound_power_output)),
            "Y"
        ).otherwise("N")
    )
    
    data = data.withColumn("event_date", F.to_date(col("timestamp")))
    return data.select("event_date", "turbine_id", "wind_speed", "wind_direction", "power_output", "outlier_ind").filter(col("outlier_ind") == "N")


# Create a new Databricks Connect session. If this fails,
# check that you have configured Databricks Connect correctly.
# See https://docs.databricks.com/dev-tools/databricks-connect.html.
def get_spark() -> SparkSession:
    try:
        from databricks.connect import DatabricksSession

        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        return SparkSession.builder.getOrCreate()


def main():
    enrich_data(get_spark())


if __name__ == "__main__":
    main()
