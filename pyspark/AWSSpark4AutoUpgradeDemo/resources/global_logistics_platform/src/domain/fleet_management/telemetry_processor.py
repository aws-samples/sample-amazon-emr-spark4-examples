"""
Fleet Management Domain - Vehicle Telemetry Processing
Handles GPS tracking, fuel consumption, driver behavior analytics.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, avg, sum, count, max, min, stddev,
    window, lag, lead, when, lit, round as spark_round,
    unix_timestamp, from_unixtime, datediff, current_timestamp,
    row_number, dense_rank, percent_rank
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    TimestampType, IntegerType, LongType
)


class VehicleTelemetryProcessor:
    """
    Processes real-time vehicle telemetry data from fleet GPS devices.
    Calculates fuel efficiency, driver scores, route optimization metrics.
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.telemetry_schema = StructType([
            StructField("vehicle_id", StringType(), False),
            StructField("driver_id", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("latitude", DoubleType(), False),
            StructField("longitude", DoubleType(), False),
            StructField("speed_kmh", DoubleType(), True),
            StructField("fuel_level_pct", DoubleType(), True),
            StructField("engine_rpm", IntegerType(), True),
            StructField("odometer_km", LongType(), True),
            StructField("engine_temp_celsius", DoubleType(), True),
            StructField("harsh_braking_count", IntegerType(), True),
            StructField("harsh_acceleration_count", IntegerType(), True),
            StructField("idle_time_seconds", IntegerType(), True),
            StructField("route_id", StringType(), True),
            StructField("cargo_weight_kg", DoubleType(), True)
        ])
    
    def load_telemetry_data(self, input_path: str) -> DataFrame:
        """Load raw telemetry data from CSV"""
        return self.spark.read \
            .schema(self.telemetry_schema) \
            .option("header", "true") \
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
            .csv(input_path)
    
    def calculate_trip_metrics(self, df: DataFrame) -> DataFrame:
        """Calculate per-trip metrics using window functions"""
        vehicle_window = Window.partitionBy("vehicle_id", "route_id").orderBy("timestamp")
        vehicle_partition = Window.partitionBy("vehicle_id", "route_id")
        
        return df \
            .withColumn("prev_odometer", lag("odometer_km").over(vehicle_window)) \
            .withColumn("prev_fuel", lag("fuel_level_pct").over(vehicle_window)) \
            .withColumn("prev_timestamp", lag("timestamp").over(vehicle_window)) \
            .withColumn("distance_km", 
                when(col("prev_odometer").isNotNull(), 
                     col("odometer_km") - col("prev_odometer")).otherwise(0)) \
            .withColumn("fuel_consumed_pct",
                when(col("prev_fuel").isNotNull(),
                     col("prev_fuel") - col("fuel_level_pct")).otherwise(0)) \
            .withColumn("time_elapsed_sec",
                when(col("prev_timestamp").isNotNull(),
                     unix_timestamp("timestamp") - unix_timestamp("prev_timestamp")).otherwise(0)) \
            .withColumn("trip_avg_speed", avg("speed_kmh").over(vehicle_partition)) \
            .withColumn("trip_max_speed", max("speed_kmh").over(vehicle_partition)) \
            .withColumn("trip_total_distance", sum("distance_km").over(vehicle_partition)) \
            .withColumn("trip_total_fuel", sum("fuel_consumed_pct").over(vehicle_partition))
    
    def calculate_driver_behavior_score(self, df: DataFrame) -> DataFrame:
        """
        Calculate driver behavior score based on:
        - Harsh braking events
        - Harsh acceleration events
        - Speeding incidents
        - Idle time
        """
        driver_window = Window.partitionBy("driver_id")
        
        return df \
            .withColumn("speeding_flag", 
                when(col("speed_kmh") > 120, 1).otherwise(0)) \
            .withColumn("excessive_idle_flag",
                when(col("idle_time_seconds") > 300, 1).otherwise(0)) \
            .withColumn("total_harsh_events",
                col("harsh_braking_count") + col("harsh_acceleration_count")) \
            .withColumn("driver_harsh_events_avg", 
                avg("total_harsh_events").over(driver_window)) \
            .withColumn("driver_speeding_rate",
                avg("speeding_flag").over(driver_window)) \
            .withColumn("driver_idle_rate",
                avg("excessive_idle_flag").over(driver_window)) \
            .withColumn("driver_safety_score",
                spark_round(
                    lit(100) - 
                    (col("driver_harsh_events_avg") * 5) -
                    (col("driver_speeding_rate") * 20) -
                    (col("driver_idle_rate") * 10), 2))
    
    def calculate_fuel_efficiency(self, df: DataFrame) -> DataFrame:
        """Calculate fuel efficiency metrics per vehicle"""
        vehicle_partition = Window.partitionBy("vehicle_id")
        
        return df \
            .withColumn("fuel_efficiency_km_per_pct",
                when(col("fuel_consumed_pct") > 0,
                     col("distance_km") / col("fuel_consumed_pct")).otherwise(0)) \
            .withColumn("vehicle_avg_efficiency",
                avg("fuel_efficiency_km_per_pct").over(vehicle_partition)) \
            .withColumn("vehicle_efficiency_stddev",
                stddev("fuel_efficiency_km_per_pct").over(vehicle_partition)) \
            .withColumn("efficiency_percentile",
                percent_rank().over(vehicle_partition.orderBy("fuel_efficiency_km_per_pct")))
    
    def generate_fleet_summary(self, df: DataFrame) -> DataFrame:
        """Generate aggregated fleet summary statistics"""
        return df.groupBy("vehicle_id", "driver_id", "route_id") \
            .agg(
                count("*").alias("total_readings"),
                spark_round(avg("speed_kmh"), 2).alias("avg_speed_kmh"),
                spark_round(max("speed_kmh"), 2).alias("max_speed_kmh"),
                spark_round(sum("distance_km"), 2).alias("total_distance_km"),
                spark_round(sum("fuel_consumed_pct"), 2).alias("total_fuel_consumed_pct"),
                spark_round(avg("engine_temp_celsius"), 2).alias("avg_engine_temp"),
                sum("harsh_braking_count").alias("total_harsh_braking"),
                sum("harsh_acceleration_count").alias("total_harsh_acceleration"),
                sum("idle_time_seconds").alias("total_idle_seconds"),
                spark_round(avg("cargo_weight_kg"), 2).alias("avg_cargo_weight")
            )
    
    def write_telemetry_parquet(self, df: DataFrame, output_path: str):
        """Write processed telemetry data to Parquet with lz4raw compression."""
        df.coalesce(4) \
            .write \
            .mode("overwrite") \
            .option("compression", "lz4raw") \
            .partitionBy("vehicle_id") \
            .parquet(output_path)
    
    def process_fleet_telemetry(self, input_path: str, output_path: str) -> DataFrame:
        """Main processing pipeline for fleet telemetry"""
        # Load raw data
        raw_df = self.load_telemetry_data(input_path)
        
        # Apply transformations
        trip_metrics_df = self.calculate_trip_metrics(raw_df)
        driver_scores_df = self.calculate_driver_behavior_score(trip_metrics_df)
        efficiency_df = self.calculate_fuel_efficiency(driver_scores_df)
        
        # Generate summary
        summary_df = self.generate_fleet_summary(efficiency_df)
        
        # Write with lz4raw compression
        self.write_telemetry_parquet(summary_df, output_path + "fleet_summary/")
        self.write_telemetry_parquet(efficiency_df, output_path + "detailed_telemetry/")
        
        return summary_df
