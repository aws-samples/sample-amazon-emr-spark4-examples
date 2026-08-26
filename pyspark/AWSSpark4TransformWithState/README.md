
# IoT Heartbeat Monitoring with Spark 4.0 transformWithState on Amazon EMR Serverless

Real-time IoT device monitoring solution that detects offline devices using Apache Spark 4.0's `transformWithState` API on Amazon EMR Serverless. The application ingests heartbeat events from Kinesis Data Streams, tracks device activity using stateful processing, and publishes alerts to SNS when devices stop responding.

## Architecture

Kinesis Data Stream → Spark Structured Streaming → Stateful Processor → SNS Alerts (Ingest) (Parse & Group) (Track & Detect) (Notify)


## Use Case

Monitor IoT devices that send periodic heartbeat messages. When a device stops sending heartbeats within a configurable threshold (default: 30 seconds), the system generates a `DEVICE_OFFLINE` alert and publishes it to an SNS topic for notification and action.

## Key Features

- **Stateful Processing** — Maintains per-device state (last heartbeat time) across micro-batches
- **Timer-Based Detection** — Watchdog pattern using processing-time timers to detect missing heartbeats
- **Exactly-Once Semantics** — Checkpointing ensures no duplicate or missed alerts during failures
- **Repeat Alerts** — Configurable repeat interval (default: 60 seconds) for ongoing offline devices
- **Multi-Format Timestamp Parsing** — Handles multiple timestamp formats gracefully

## Prerequisites

- **Amazon EMR Serverless** application (Spark 4.0+)
- **Amazon Kinesis Data Stream** — `iot-heartbeats`
- **Amazon SNS Topic** — for alert notifications
- **Amazon S3 Bucket** — for checkpoint storage
- **IAM Role** with permissions for Kinesis, SNS, and S3 access
- **Python 3.8+**
- **protobuf 5.29.3** (to avoid version conflicts with Spark 4.0)

## Configuration

Update the following variables in `heartbeat_monitor_final.py`:

| Variable | Description | Default |
|----------|-------------|---------|
| `KINESIS_STREAM_NAME` | Kinesis stream name | `iot-heartbeats` |
| `KINESIS_REGION` | AWS region | `us-east-1` |
| `SNS_TOPIC_ARN` | SNS topic ARN for alerts | Update with your ARN |
| `CHECKPOINT_LOCATION` | S3 path for checkpoints | Update with your S3 path |
| `HEARTBEAT_INTERVAL_MS` | Offline detection threshold | `30000` (30 seconds) |
| `ALERT_REPEAT_INTERVAL_MS` | Repeat alert interval | `60000` (60 seconds) |

## Input Schema

The application expects JSON heartbeat messages in Kinesis with the following structure:

```json
{
  "device_id": "device-001",
  "timestamp": "2026-01-15T10:30:00Z",
  "battery_level": 85.5,
  "signal_strength": -67.2,
  "firmware_version": "2.1.0"
}
```
## Output (Alert Schema)

When a device goes offline, the following alert is published to SNS:

```json

{
  "device_id": "device-001",
  "alert_type": "DEVICE_OFFLINE",
  "last_seen": "2026-01-15T10:30:00",
  "offline_duration_seconds": 45.0,
  "alert_timestamp": "2026-01-15T10:30:45"
}
```

## Deployment

### Step 1: Create AWS Resources

```bash
# Create Kinesis stream
aws kinesis create-stream --stream-name iot-heartbeats --shard-count 2 --region us-east-1

# Create SNS topic
aws sns create-topic --name iot-alerts --region us-east-1

# Create S3 bucket for checkpoints
aws s3 mb s3://your-bucket-name --region us-east-1
```

### Step 2: Create the EMR Serverless application with custom Spark 4.0 image

This solution uses a custom Docker image that includes Spark streaming dependencies. Follow these steps:

#### 2.1 Create ECR repository and push the image

Download the [Spark 4.0 custom image archive](https://aws-blogs-artifacts-public.s3.us-east-1.amazonaws.com/artifacts/BDB-5849/spark4-variant-latest.tar.gz), load it into Docker, then create a private ECR repository and push the image. For detailed steps, see [Pushing a Docker image to an Amazon ECR private repository](https://docs.aws.amazon.com/AmazonECR/latest/userguide/docker-push-ecr-image.html).

```bash
# Load the image archive
docker load -i spark4-variant-latest.tar.gz

# Tag and push to ECR
docker tag spark4-variant:latest <ACCOUNT_ID>.dkr.ecr.us-east-1.amazonaws.com/spark4-variant:latest
docker push <ACCOUNT_ID>.dkr.ecr.us-east-1.amazonaws.com/spark4-variant:latest
```

#### 2.2 Grant EMR Serverless access to the image
Add the required ECR repository policy following [Customizing an EMR Serverless image — Allow EMR Serverless to access the custom image repository](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/application-custom-image.html).

#### 2.3 Create EMR Serverless application

```bash
aws emr-serverless create-application \  
--name "iot-heartbeat-monitor" \  
--release-label "emr-spark-8.0.0" \
--type "SPARK" \
--network-configuration '{    
  "subnetIds": ["subnet-xxxxx", "subnet-yyyyy"],    
  "securityGroupIds": ["sg-zzzzz"]
  }' \   
--image-configuration '{ "imageUri": "<ACCOUNT_ID>.dkr.ecr.us-east-1.amazonaws.com/spark4-variant:latest"}' \
--region us-east-1
```

Note the `applicationId` from the output for use in subsequent steps.

### Step 3: Upload external dependencies

Download the required dependencies and upload them to your S3 bucket:

- [Spark-Kinesis-connector JAR](https://aws-blogs-artifacts-public.s3.us-east-1.amazonaws.com/artifacts/BDB-5849/spark-streaming-sql-kinesis-connector_2.13-2.0.0-SNAPSHOT.jar) → `s3://your-bucket/jars/spark-kinesis-connector.jar`
- [Protobuf dependency](https://aws-blogs-artifacts-public.s3.us-east-1.amazonaws.com/artifacts/BDB-5849/protobuf_pkg.tar) → `s3://your-bucket/pyfiles/protobuf_pkg.tar.gz`

### Step 4: Submit Job to EMR Serverless

```bash
aws emr-serverless start-job-run \
  --application-id <APPLICATION_ID> \
  --execution-role-arn <ROLE_ARN> \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://your-bucket/heartbeat_monitor_final.py",
      "sparkSubmitParameters": "--packages org.apache.spark:spark-sql-kinesis_2.13:4.0.0"
    }
  }'
```

## How It Works

    Ingest — Reads heartbeat events from Kinesis Data Stream in real-time
    Parse — Converts JSON payloads and handles multiple timestamp formats
    Group — Groups events by device_id for stateful processing
    Track — Stores last-seen timestamp per device and registers a watchdog timer
    Detect — When a timer expires (no heartbeat within threshold), generates an alert
    Notify — Publishes DEVICE_OFFLINE alerts to SNS topic
    Repeat — Continues sending alerts every 60 seconds until the device comes back online

## Cleanup

Delete resources in this order to avoid ongoing charges:

```bash
# 1. Stop the streaming job
aws emr-serverless stop-job-run --application-id <APP_ID> --job-run-id <JOB_RUN_ID>

# 2. Delete EMR Serverless application
aws emr-serverless delete-application --application-id <APP_ID>

# 3. Delete Kinesis stream
aws kinesis delete-stream --stream-name iot-heartbeats --region us-east-1

# 4. Delete SNS topic
aws sns delete-topic --topic-arn arn:aws:sns:us-east-1:<ACCOUNT_ID>:iot-alerts

# 5. Remove checkpoint data
aws s3 rm s3://your-bucket/checkpoint-heartbeat/ --recursive

# 6. Delete S3 bucket (if created for this example)
aws s3 rb s3://your-bucket --force
```

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

## Disclaimer

These examples are provided for educational and reference purposes. They are not production-ready and are not covered by AWS Support. Please review and test thoroughly before using in your own environments.
