"""
Historical Compliance Domain - Regulatory Audit Processing
Handles historical shipment records, regulatory compliance audits.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, when, lit, concat, year, month, dayofmonth, quarter,
    to_date, to_timestamp, date_format, datediff, months_between,
    current_date, current_timestamp, add_months, date_add, date_sub,
    unix_timestamp, from_unixtime,
    sum, count, avg, max, min, first, last,
    row_number, dense_rank, lag, lead
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType, IntegerType, DateType, LongType
)


class HistoricalComplianceProcessor:
    """Processes historical compliance and audit records."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self._configure_legacy_datetime()
        
        self.audit_schema = StructType([
            StructField("record_id", StringType(), False),
            StructField("document_date", StringType(), True),  # String to handle ancient dates
            StructField("filing_date", StringType(), True),
            StructField("company_id", StringType(), False),
            StructField("company_name", StringType(), True),
            StructField("document_type", StringType(), True),
            StructField("jurisdiction", StringType(), True),
            StructField("regulatory_body", StringType(), True),
            StructField("compliance_status", StringType(), True),
            StructField("penalty_amount", DoubleType(), True),
            StructField("shipment_count", IntegerType(), True),
            StructField("total_value", DoubleType(), True),
            StructField("inspector_id", StringType(), True),
            StructField("notes", StringType(), True)
        ])
    
    def _configure_legacy_datetime(self):
        """Configure Spark for legacy datetime handling."""
        pass
    
    def load_audit_records(self, input_path: str) -> DataFrame:
        """Load historical audit records"""
        return self.spark.read \
            .schema(self.audit_schema) \
            .option("header", "true") \
            .csv(input_path)
    
    def parse_historical_dates(self, df: DataFrame) -> DataFrame:
        """Parse historical dates including pre-1582 Julian calendar dates."""
        return df \
            .withColumn("parsed_document_date", 
                col("document_date").cast("date")) \
            .withColumn("parsed_filing_date",
                col("filing_date").cast("date")) \
            .withColumn("calendar_type",
                when(col("document_date") < "1582-10-15", "Julian")
                .otherwise("Gregorian")) \
            .withColumn("is_historical",
                when(col("document_date") < "1900-01-01", lit(True))
                .otherwise(lit(False)))
    
    def classify_compliance_era(self, df: DataFrame) -> DataFrame:
        """Classify records by regulatory era"""
        return df \
            .withColumn("document_year", year(col("parsed_document_date"))) \
            .withColumn("document_quarter", quarter(col("parsed_document_date"))) \
            .withColumn("document_month", month(col("parsed_document_date"))) \
            .withColumn("regulatory_era",
                when(col("document_year") < 1900, "Pre-Modern")
                .when(col("document_year") < 1950, "Early-Modern")
                .when(col("document_year") < 1980, "Industrial")
                .when(col("document_year") < 2000, "Digital-Transition")
                .when(col("document_year") < 2010, "Early-Digital")
                .otherwise("Modern-Digital")) \
            .withColumn("days_since_filing",
                datediff(current_date(), col("parsed_filing_date"))) \
            .withColumn("months_since_document",
                months_between(current_date(), col("parsed_document_date")))
    
    def calculate_compliance_metrics(self, df: DataFrame) -> DataFrame:
        """Calculate compliance metrics with historical context"""
        company_window = Window.partitionBy("company_id")
        jurisdiction_window = Window.partitionBy("jurisdiction")
        era_window = Window.partitionBy("regulatory_era")
        
        return df \
            .withColumn("company_total_penalties",
                sum("penalty_amount").over(company_window)) \
            .withColumn("company_violation_count",
                count(when(col("compliance_status") == "VIOLATION", 1)).over(company_window)) \
            .withColumn("jurisdiction_avg_penalty",
                avg("penalty_amount").over(jurisdiction_window)) \
            .withColumn("era_total_shipments",
                sum("shipment_count").over(era_window)) \
            .withColumn("era_total_value",
                sum("total_value").over(era_window)) \
            .withColumn("compliance_score",
                when(col("compliance_status") == "COMPLIANT", 100)
                .when(col("compliance_status") == "MINOR_VIOLATION", 75)
                .when(col("compliance_status") == "VIOLATION", 50)
                .when(col("compliance_status") == "MAJOR_VIOLATION", 25)
                .otherwise(0))
    
    def generate_audit_timeline(self, df: DataFrame) -> DataFrame:
        """Generate audit timeline with date-based analysis"""
        company_time_window = Window.partitionBy("company_id").orderBy("parsed_document_date")
        
        return df \
            .withColumn("prev_audit_date",
                lag("parsed_document_date").over(company_time_window)) \
            .withColumn("next_audit_date",
                lead("parsed_document_date").over(company_time_window)) \
            .withColumn("days_between_audits",
                datediff(col("parsed_document_date"), col("prev_audit_date"))) \
            .withColumn("audit_sequence",
                row_number().over(company_time_window)) \
            .withColumn("formatted_date",
                date_format(col("parsed_document_date"), "yyyy-MM-dd"))
    
    def aggregate_by_era(self, df: DataFrame) -> DataFrame:
        """Aggregate compliance data by regulatory era"""
        return df.groupBy("regulatory_era", "jurisdiction", "calendar_type") \
            .agg(
                count("*").alias("record_count"),
                sum("penalty_amount").alias("total_penalties"),
                avg("penalty_amount").alias("avg_penalty"),
                sum("shipment_count").alias("total_shipments"),
                sum("total_value").alias("total_value"),
                avg("compliance_score").alias("avg_compliance_score"),
                min("parsed_document_date").alias("earliest_date"),
                max("parsed_document_date").alias("latest_date"),
                count(when(col("is_historical"), 1)).alias("historical_record_count")
            )
    
    def write_with_legacy_datetime(self, df: DataFrame, output_path: str):
        """Write data with legacy datetime rebasing."""
        df.coalesce(2) \
            .write \
            .mode("overwrite") \
            .parquet(output_path)
    
    def process_compliance_records(self, input_path: str, output_path: str) -> DataFrame:
        """Main processing pipeline for historical compliance records"""
        # Load raw data
        raw_df = self.load_audit_records(input_path)
        
        # Parse historical dates
        dated_df = self.parse_historical_dates(raw_df)
        
        # Classify by era
        era_df = self.classify_compliance_era(dated_df)
        
        # Calculate metrics
        metrics_df = self.calculate_compliance_metrics(era_df)
        
        # Generate timeline
        timeline_df = self.generate_audit_timeline(metrics_df)
        
        # Aggregate by era
        era_summary_df = self.aggregate_by_era(timeline_df)
        
        # Write outputs with legacy datetime handling
        self.write_with_legacy_datetime(timeline_df, output_path + "audit_timeline/")
        self.write_with_legacy_datetime(era_summary_df, output_path + "era_summary/")
        
        return timeline_df


class RegulatoryReportGenerator:
    """Generates regulatory compliance reports with historical date handling"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def generate_annual_report(self, df: DataFrame, year: int) -> DataFrame:
        """Generate annual compliance report"""
        return df \
            .filter(col("document_year") == year) \
            .groupBy("company_id", "company_name", "jurisdiction") \
            .agg(
                count("*").alias("audit_count"),
                sum("penalty_amount").alias("total_penalties"),
                avg("compliance_score").alias("avg_compliance_score"),
                first("regulatory_era").alias("era")
            )
    
    def generate_historical_trend(self, df: DataFrame) -> DataFrame:
        """Generate historical compliance trend analysis"""
        return df \
            .groupBy("document_year", "regulatory_era") \
            .agg(
                count("*").alias("record_count"),
                avg("compliance_score").alias("avg_score"),
                sum("penalty_amount").alias("total_penalties")
            ) \
            .orderBy("document_year")
