"""
International Shipping Domain - Cross-Border Document Processing

Handles customs documentation, multi-language addresses, carrier manifests.


"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, expr, when, lit, concat, concat_ws, trim, upper, lower,
    regexp_replace, split, size, array_contains, explode,
    sum, count, avg, max, min, collect_list, collect_set,
    row_number, dense_rank, first, last,
    to_date, date_format, datediff, current_date
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType, IntegerType, ArrayType, MapType, DateType
)


class InternationalShipmentProcessor:
    """
    Processes international shipping documents with multi-language support.
    Handles customs declarations, carrier manifests, and address standardization.
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.shipment_schema = StructType([
            StructField("origin_country", StringType(), False),
            StructField("destination_country", StringType(), False),
            StructField("shipper_name", StringType(), True),
            StructField("shipper_address", StringType(), True),
            StructField("consignee_name", StringType(), True),
            StructField("consignee_address", StringType(), True),
            StructField("product_description", StringType(), True),
            StructField("hs_code", StringType(), True),
            StructField("declared_value_usd", DoubleType(), True),
            StructField("weight_kg", DoubleType(), True),
            StructField("package_count", IntegerType(), True),
            StructField("carrier_code", StringType(), True),
            StructField("ship_date", DateType(), True),
            StructField("estimated_delivery", DateType(), True),
            StructField("customs_status", StringType(), True),
            StructField("special_instructions", StringType(), True)
        ])
        
        # Charset mappings for different regions
        self.region_charset_map = {
            "JP": "Shift_JIS",      # Japan
            "CN": "GB2312",         # China
            "KR": "EUC-KR",         # Korea
            "TW": "Big5",           # Taiwan
            "RU": "KOI8-R",         # Russia
            "TH": "TIS-620",        # Thailand
        }
    
    def load_shipment_data(self, input_path: str) -> DataFrame:
        """Load international shipment data"""
        return self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("dateFormat", "yyyy-MM-dd") \
            .option("quote", '"') \
            .option("escape", '"') \
            .csv(input_path)
    
    def standardize_addresses_with_charset(self, df: DataFrame) -> DataFrame:
        """
        Standardize international addresses using region-specific charsets.
        
        Encode text using regional charset
        Supported charsets in 4.0: US-ASCII, ISO-8859-1, UTF-8, UTF-16BE, UTF-16LE, UTF-16, UTF-32
        """
        # Process Japanese addresses with Shift_JIS
        df = df.withColumn(
            "shipper_address_normalized",
            when(col("origin_country") == "JP",
                 expr("decode(encode(shipper_address, 'Shift_JIS'), 'UTF-8')"))
            .when(col("origin_country") == "CN",
                 expr("decode(encode(shipper_address, 'GB2312'), 'UTF-8')"))
            .when(col("origin_country") == "KR",
                 expr("decode(encode(shipper_address, 'EUC-KR'), 'UTF-8')"))
            .otherwise(col("shipper_address"))
        )
        
        # Process consignee addresses
        df = df.withColumn(
            "consignee_address_normalized",
            when(col("destination_country") == "JP",
                 expr("decode(encode(consignee_address, 'Shift_JIS'), 'UTF-8')"))
            .when(col("destination_country") == "CN",
                 expr("decode(encode(consignee_address, 'GB2312'), 'UTF-8')"))
            .when(col("destination_country") == "KR",
                 expr("decode(encode(consignee_address, 'EUC-KR'), 'UTF-8')"))
            .otherwise(col("consignee_address"))
        )
        
        return df
    
    def process_carrier_manifests(self, df: DataFrame) -> DataFrame:
        """
        Process carrier manifest data with character encoding conversion.

        """
        # Encode product descriptions for customs systems
        df = df.withColumn(
            "product_description_encoded",
            expr("decode(encode(product_description, 'ISO-8859-1'), 'UTF-8')")
        )
        
        # Process special instructions with potential unmappable characters
        df = df.withColumn(
            "special_instructions_processed",
            expr("decode(encode(special_instructions, 'ISO-8859-1'), 'UTF-8')")
        )
        
        # Validate character encoding quality
        df = df.withColumn(
            "encoding_quality",
            when(
                expr("decode(encode(shipper_name, 'ISO-8859-1'), 'UTF-8')").rlike("[\\uFFFD]"),
                "contains_replacement_chars"
            ).when(
                expr("decode(encode(shipper_name, 'ISO-8859-1'), 'UTF-8')").rlike("[\\u00C0-\\u017F]"),
                "valid_international"
            ).otherwise("ascii_only")
        )
        
        return df
    
    def calculate_customs_metrics(self, df: DataFrame) -> DataFrame:
        """Calculate customs and compliance metrics"""
        country_window = Window.partitionBy("destination_country")
        carrier_window = Window.partitionBy("carrier_code")
        
        return df \
            .withColumn("transit_days", 
                datediff(col("estimated_delivery"), col("ship_date"))) \
            .withColumn("country_total_value",
                sum("declared_value_usd").over(country_window)) \
            .withColumn("country_shipment_count",
                count("*").over(country_window)) \
            .withColumn("carrier_avg_transit",
                avg("transit_days").over(carrier_window)) \
            .withColumn("value_per_kg",
                when(col("weight_kg") > 0, 
                     col("declared_value_usd") / col("weight_kg")).otherwise(0)) \
            .withColumn("high_value_flag",
                when(col("declared_value_usd") > 10000, lit(True)).otherwise(lit(False)))
    
    def generate_customs_declaration(self, df: DataFrame) -> DataFrame:
        """Generate customs declaration documents with encoded fields"""
        return df.select(
            col("shipment_id"),
            col("origin_country"),
            col("destination_country"),
            col("shipper_name"),
            col("shipper_address_normalized").alias("shipper_address"),
            col("consignee_name"),
            col("consignee_address_normalized").alias("consignee_address"),
            col("product_description_encoded").alias("product_description"),
            col("hs_code"),
            col("declared_value_usd"),
            col("weight_kg"),
            col("package_count"),
            col("encoding_quality"),
            col("customs_status")
        )
    
    def aggregate_by_trade_lane(self, df: DataFrame) -> DataFrame:
        """Aggregate shipment data by trade lane (origin-destination pair)"""
        return df.groupBy("origin_country", "destination_country", "carrier_code") \
            .agg(
                count("*").alias("shipment_count"),
                sum("declared_value_usd").alias("total_value_usd"),
                sum("weight_kg").alias("total_weight_kg"),
                sum("package_count").alias("total_packages"),
                avg("transit_days").alias("avg_transit_days"),
                collect_set("hs_code").alias("hs_codes_shipped"),
                first("encoding_quality").alias("sample_encoding_quality")
            )
    
    def process_international_shipments(self, input_path: str, output_path: str) -> DataFrame:
        """Main processing pipeline for international shipments"""
        # Load raw data
        raw_df = self.load_shipment_data(input_path)
        
        # Apply charset encoding transformations
        encoded_df = self.standardize_addresses_with_charset(raw_df)
        manifest_df = self.process_carrier_manifests(encoded_df)
        
        # Calculate metrics
        metrics_df = self.calculate_customs_metrics(manifest_df)
        
        # Generate outputs
        declarations_df = self.generate_customs_declaration(metrics_df)
        trade_lane_df = self.aggregate_by_trade_lane(metrics_df)
        
        # Write outputs
        declarations_df.coalesce(2) \
            .write.mode("overwrite") \
            .parquet(output_path + "customs_declarations/")
        
        trade_lane_df.coalesce(1) \
            .write.mode("overwrite") \
            .parquet(output_path + "trade_lane_summary/")
        
        return declarations_df


class CarrierManifestValidator:
    """Validates carrier manifest data for encoding issues"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def validate_character_encoding(self, df: DataFrame, column: str) -> DataFrame:
        """
        Validate character encoding and detect mojibake.
        """
        return df.withColumn(
            f"{column}_validation",
            when(
                expr(f"decode(encode({column}, 'ISO-8859-1'), 'UTF-8')") != col(column),
                "encoding_mismatch"
            ).when(
                expr(f"decode(encode({column}, 'ISO-8859-1'), 'UTF-8')").contains("�"),
                "contains_mojibake"
            ).otherwise("valid")
        )
    
    def detect_unmappable_characters(self, df: DataFrame) -> DataFrame:

        text_columns = ["shipper_name", "consignee_name", "product_description", 
                       "shipper_address", "consignee_address", "special_instructions"]
        
        for col_name in text_columns:
            df = self.validate_character_encoding(df, col_name)
        
        return df
