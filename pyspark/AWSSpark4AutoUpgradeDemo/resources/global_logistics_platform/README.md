# Global Logistics Platform

A PySpark-based data processing platform for global logistics operations, handling fleet management, international shipping, and historical compliance analytics.

## Project Structure

```
global_logistics_platform/
├── src/
│   ├── domain/
│   │   ├── fleet_management/        # Vehicle telemetry processing
│   │   ├── international_shipping/  # Cross-border shipment processing
│   │   └── historical_compliance/   # Regulatory compliance analytics
│   ├── core/
│   │   ├── transformers/
│   │   ├── validators/
│   │   └── aggregators/
│   └── utils/
│       └── spark_config.py          # Spark session configuration
├── jobs/
│   └── submit_spark35.sh            # EMR Serverless submission script
├── data/sample/                     # Sample input data
├── tests/
├── config/
├── main.py                          # Main entry point
├── requirements.txt
└── emr_resource_setup.py            # Test data generator for S3
```

## Features

### Fleet Management
- Real-time vehicle telemetry processing
- Trip metrics calculation
- Driver behavior scoring
- Fuel efficiency analysis
- Fleet summary generation

### International Shipping
- Multi-region shipment processing
- Address encoding for Japanese, Chinese, and Korean markets
- Cross-border logistics analytics

### Historical Compliance
- Regulatory audit record processing
- Historical date handling (including pre-1900 records)
- Compliance scoring and trend analysis
- Era-based aggregation

## Requirements

- Python 3.8+
- PySpark 3.5.0
- pandas, numpy, pyarrow, boto3

Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Local Development
```python
from src.utils import create_spark_session
from src.domain.fleet_management import VehicleTelemetryProcessor

spark = create_spark_session("LocalTest", enable_legacy_datetime=True)
processor = VehicleTelemetryProcessor(spark)
result = processor.process_fleet_telemetry("data/sample/", "output/")
```

### EMR Serverless

1. Upload code and data to S3:
```bash
zip -r global_logistics_platform.zip src main.py
aws s3 cp global_logistics_platform.zip s3://<bucket>/global_logistics/
aws s3 cp main.py s3://<bucket>/global_logistics/
```

2. Generate test data:
```bash
python emr_resource_setup.py create_resource s3://<bucket>/global_logistics/input/
```

3. Submit job:
```bash
./jobs/submit_spark35.sh
```

### Command Line
```bash
spark-submit \
  --py-files global_logistics_platform.zip \
  main.py \
  s3://<bucket>/input/ \
  s3://<bucket>/output/
```

## Output

The platform generates the following outputs:

- `fleet_analytics/` - Vehicle performance metrics and fleet summaries
- `shipping_analytics/` - International shipment processing results
- `compliance_analytics/` - Audit timelines and era-based summaries

## Configuration

Spark session configuration is managed in `src/utils/spark_config.py`. Key settings:
- Adaptive query execution enabled
- Parquet compression: snappy (default), lz4raw (telemetry)
- Legacy datetime handling for historical records

## Testing

```bash
pytest tests/ -v
```

## License

Internal use only.
