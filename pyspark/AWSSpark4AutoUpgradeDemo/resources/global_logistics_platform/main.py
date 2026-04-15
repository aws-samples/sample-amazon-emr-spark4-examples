"""
Global Logistics Platform - Main Orchestrator
Runs all domain pipelines: Fleet Management, International Shipping, Historical Compliance
"""
import sys
from pyspark.sql import SparkSession
from src.utils import create_spark_session, get_logger
from src.domain.fleet_management import VehicleTelemetryProcessor
from src.domain.international_shipping import InternationalShipmentProcessor
from src.domain.historical_compliance import HistoricalComplianceProcessor


def run_fleet_management_pipeline(spark: SparkSession, input_base: str, output_base: str):
    """Run fleet management pipeline."""
    logger = get_logger("fleet_management")
    logger.info("Starting Fleet Management Pipeline...")
    
    processor = VehicleTelemetryProcessor(spark)
    input_path = input_base + "fleet_telemetry/"
    output_path = output_base + "fleet_analytics/"
    
    result = processor.process_fleet_telemetry(input_path, output_path)
    logger.info(f"Fleet Management Pipeline completed. Records: {result.count()}")
    return result


def run_international_shipping_pipeline(spark: SparkSession, input_base: str, output_base: str):
    """Run international shipping pipeline."""
    logger = get_logger("international_shipping")
    logger.info("Starting International Shipping Pipeline...")
    
    processor = InternationalShipmentProcessor(spark)
    input_path = input_base + "international_shipments/"
    output_path = output_base + "shipping_analytics/"
    
    result = processor.process_international_shipments(input_path, output_path)
    logger.info(f"International Shipping Pipeline completed. Records: {result.count()}")
    return result


def run_historical_compliance_pipeline(spark: SparkSession, input_base: str, output_base: str):
    """Run historical compliance pipeline."""
    logger = get_logger("historical_compliance")
    logger.info("Starting Historical Compliance Pipeline...")
    
    processor = HistoricalComplianceProcessor(spark)
    input_path = input_base + "compliance_records/"
    output_path = output_base + "compliance_analytics/"
    
    result = processor.process_compliance_records(input_path, output_path)
    logger.info(f"Historical Compliance Pipeline completed. Records: {result.count()}")
    return result


def main():
    """Main entry point for the Global Logistics Platform"""
    if len(sys.argv) < 3:
        print("Usage: main.py <input_s3_path> <output_s3_path>")
        sys.exit(1)
    
    input_base = sys.argv[1].rstrip('/') + '/'
    output_base = sys.argv[2].rstrip('/') + '/'
    
    logger = get_logger("global_logistics_platform")
    logger.info("=" * 70)
    logger.info("GLOBAL LOGISTICS PLATFORM - Starting All Pipelines")
    logger.info(f"Input: {input_base} | Output: {output_base}")
    logger.info("=" * 70)
    
    spark = create_spark_session(app_name="GlobalLogisticsPlatform", enable_legacy_datetime=True)
    logger.info(f"Spark Version: {spark.version}")
    
    try:
        logger.info("[1/3] Running Fleet Management Pipeline...")
        run_fleet_management_pipeline(spark, input_base, output_base)
        
        logger.info("[2/3] Running International Shipping Pipeline...")
        run_international_shipping_pipeline(spark, input_base, output_base)
        
        logger.info("[3/3] Running Historical Compliance Pipeline...")
        run_historical_compliance_pipeline(spark, input_base, output_base)
        
        logger.info("=" * 70)
        logger.info("ALL PIPELINES COMPLETED SUCCESSFULLY")
        logger.info("=" * 70)
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
