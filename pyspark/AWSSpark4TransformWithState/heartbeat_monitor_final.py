#!/usr/bin/env python3
"""
IoT Heartbeat Monitoring with Spark 4.0 transformWithState on Amazon EMR Serverless



===================================================================================
HIGH-LEVEL OVERVIEW
===================================================================================

USE CASE:
Monitor IoT devices that send periodic heartbeat messages to detect when devices
go offline. Generate alerts via SNS when devices stop sending heartbeats.

APPROACH:
1. Ingest heartbeat events from Kinesis Data Stream in real-time
2. Use Spark Structured Streaming with stateful processing to track last-seen time per device
3. Register timers that expire if no heartbeat arrives within threshold period
4. Generate DEVICE_OFFLINE alerts when timers expire
5. Publish alerts to SNS topic for notification/action

KEY CONCEPTS:
- Stateful Processing: Maintains per-device state (last heartbeat time) across micro-batches
- Timer Mechanism: Watchdog pattern - timers expire if heartbeats stop arriving
- Exactly-Once Semantics: Checkpointing ensures no duplicate/missed alerts during failures

FLOW:
Kinesis Stream → Parse JSON → Group by device_id → Stateful Processor (track heartbeats + timers)
→ Generate alerts on timer expiry → Publish to SNS

===================================================================================
INTENDED AUDIENCE & PREREQUISITES
===================================================================================

AUDIENCE:
This example is designed for builders familiar with Spark Structured Streaming and
stateful processing concepts. If you're new to these topics, we recommend reviewing
foundational materials first.

PREREQUISITES:
- Understanding of Spark Structured Streaming basics (DataFrames, streaming queries)
- Familiarity with AWS services: Kinesis Data Streams, SNS, S3, EMR Serverless
- Basic knowledge of stateful stream processing concepts
- Python programming experience

LEARNING PATH:
📚 Beginner: Start with Spark Structured Streaming Programming Guide
   https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html

📚 Intermediate: Learn about stateful operations (mapGroupsWithState)
   https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#arbitrary-stateful-operations

📚 Advanced: Explore transformWithState (Spark 4.0+) - this example's approach
   https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.transformWithStateInPandas.html

📚 AWS Integration: EMR Serverless with Spark Structured Streaming
   https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/

===================================================================================
GLOSSARY OF KEY TERMS
===================================================================================

STATEFUL PROCESSING:
A streaming pattern where the system maintains state (data) across multiple micro-batches.
In this example, we store each device's last-seen timestamp and use it to detect offline status.
Unlike stateless processing (each event processed independently), stateful processing remembers
previous events to make decisions.
Reference: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#stateful-operations

MICRO-BATCH:
Spark Structured Streaming processes data in small batches (micro-batches) rather than
one event at a time. Each micro-batch contains events that arrived within a trigger interval
(e.g., 10 seconds in this example). This balances latency and throughput.
Reference: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#triggers

EXACTLY-ONCE SEMANTICS:
Guarantee that each event is processed exactly one time, even if failures occur.
Achieved through checkpointing: Spark saves progress to S3, so after restart it resumes
from the last checkpoint without reprocessing or skipping data. Critical for alert systems
to avoid duplicate or missed alerts.
Reference: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#fault-tolerance-semantics

CHECKPOINTING:
Mechanism to save streaming query progress (offsets, state) to durable storage (S3).
Enables fault tolerance and exactly-once processing. The CHECKPOINT_LOCATION stores
metadata about processed Kinesis records and device state.
Reference: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#recovering-from-failures-with-checkpointing

TIMER (PROCESSING TIME):
A mechanism to trigger actions after a specified time interval. In this example, timers
implement the "watchdog" pattern: if no heartbeat arrives within 30 seconds, the timer
expires and triggers an alert. Timers are managed per device key.
Reference: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.streaming.stateful_processor.StatefulProcessorHandle.html

TRANSFORMWITHSTATE:
Spark 4.0+ API for arbitrary stateful processing with timer support. Provides fine-grained
control over state management and time-based triggers. Replaces older mapGroupsWithState API.
This example uses transformWithStateInPandas for Pandas UDF integration.
Reference: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.transformWithStateInPandas.html

KINESIS DATA STREAM:
AWS managed service for real-time data streaming. IoT devices publish heartbeat events
to Kinesis, which Spark consumes using the aws-kinesis connector.
Reference: https://docs.aws.amazon.com/kinesis/latest/dev/introduction.html

SNS (SIMPLE NOTIFICATION SERVICE):
AWS pub/sub messaging service. This example publishes DEVICE_OFFLINE alerts to an SNS topic,
which can trigger emails, SMS, Lambda functions, or other notification mechanisms.
Reference: https://docs.aws.amazon.com/sns/latest/dg/welcome.html


"""

import sys

# Module cleanup for protobuf
# EXPLANATION (1): Why protobuf cleanup is needed
# Spark may have loaded an incompatible version of protobuf in its classpath.
# Deleting these modules forces Python to reload protobuf from our specified version (5.29.3).
# This prevents version conflicts that cause "descriptor pool" errors in Spark 4.0.
# Without this cleanup, you may see: "Can't add file descriptor to a descriptor pool"
if 'google.protobuf' in sys.modules:
    del sys.modules['google.protobuf']
if 'google' in sys.modules:
    del sys.modules['google']

import json
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, coalesce, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DoubleType
)
from pyspark.sql.streaming.stateful_processor import (
    StatefulProcessor, StatefulProcessorHandle
)

from datetime import datetime
from typing import Iterator
import pandas as pd



# Configuration
KINESIS_STREAM_NAME = "iot-heartbeats"
KINESIS_REGION = "us-east-1"
SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:123456789012:iot-alerts"
CHECKPOINT_LOCATION = "s3://emrserverless-streaming-blog-123456789012-us-east-1/checkpoint-heartbeat/"

# EXPLANATION (4): Why specific threshold values were chosen
# HEARTBEAT_INTERVAL_MS = 30 seconds
#   - Devices send heartbeats every ~20 seconds in normal operation
#   - 30 second threshold allows for network delays and minor timing variations
#   - Triggers alert if no heartbeat received within this window
#   - Balance: Too short = false alarms, too long = delayed detection
#
# ALERT_REPEAT_INTERVAL_MS = 60 seconds
#   - After initial offline alert, repeat alerts every 60 seconds
#   - Prevents alert fatigue while maintaining visibility of ongoing issues
#   - Allows time for operators to acknowledge and respond
#   - Can be adjusted based on operational requirements (e.g., 5 min for less critical devices)

HEARTBEAT_INTERVAL_MS = 30000  # 30 seconds
ALERT_REPEAT_INTERVAL_MS = 60000  # 60 seconds

heartbeat_schema = StructType([
    StructField("device_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("battery_level", DoubleType(), True),
    StructField("signal_strength", DoubleType(), True),
    StructField("firmware_version", StringType(), True)
])


alert_output_schema = StructType([
    StructField("device_id", StringType(), True),
    StructField("alert_type", StringType(), True),
    StructField("last_seen", TimestampType(), True),
    StructField("offline_duration_seconds", DoubleType(), True),
    StructField("alert_timestamp", TimestampType(), True)
])


class HeartbeatMonitor(StatefulProcessor):
    
    def init(self, handle: StatefulProcessorHandle) -> None:
        self.handle = handle
        
        last_seen_schema = StructType([
            StructField("timestamp", TimestampType(), True)
        ])
        
        device_info_schema = StructType([
            StructField("battery_level", StringType(), True),
            StructField("firmware_version", StringType(), True)
        ])
        
        self.last_seen = handle.getValueState("last_seen", last_seen_schema)
        self.device_info = handle.getValueState("device_info", device_info_schema)
    
    def handleInputRows(self, key: tuple, rows: Iterator[pd.DataFrame], timerValues) -> Iterator[pd.DataFrame]:
        device_id = key[0]
        
        latest_timestamp = None
        latest_battery = None
        latest_firmware = None
        
        for pdf in rows:
            for _, row in pdf.iterrows():
                ts = row['timestamp']
                # LOGIC EXPLANATION (1): Why NaT values are skipped
                # NaT (Not a Time) represents invalid/missing timestamps from malformed data.
                # Skipping them prevents state corruption and ensures we only track valid heartbeats.
                # Without this check, invalid timestamps could trigger false offline alerts.
                # Skip NaT (Not a Time) values
                if pd.isna(ts) or pd.isnull(ts):
                    continue

                # LOGIC EXPLANATION (2): Purpose of finding the latest timestamp
                # Multiple heartbeats may arrive in the same micro-batch for a device.
                # We find the LATEST timestamp to ensure state reflects the most recent heartbeat.
                # This prevents out-of-order events from incorrectly marking devices as offline.
                if latest_timestamp is None or ts > latest_timestamp:
                    latest_timestamp = ts
                    latest_battery = str(row.get('battery_level', ''))
                    latest_firmware = row.get('firmware_version', '')
        
        # If no valid timestamp found, skip
        if latest_timestamp is None or pd.isna(latest_timestamp):
            yield pd.DataFrame()
            return
        
        # Convert pandas Timestamp to Python datetime if needed
        if hasattr(latest_timestamp, 'to_pydatetime'):
            latest_timestamp = latest_timestamp.to_pydatetime()
        
        existing_timestamp = None
        if self.last_seen.exists():
            existing_state = self.last_seen.get()
            existing_timestamp = existing_state[0]
        
        if existing_timestamp is None or latest_timestamp > existing_timestamp:
            # LOGIC EXPLANATION (3): Why existing timers are deleted before registering new ones
            # When a new heartbeat arrives, we delete old timers to "reset the countdown".
            # This prevents multiple timers from firing and generating duplicate alerts.
            # Only the most recent timer (based on latest heartbeat) should remain active.
            for timer in self.handle.listTimers():
                self.handle.deleteTimer(timer)
            
            self.last_seen.update((latest_timestamp,))
            self.device_info.update((latest_battery, latest_firmware))

        # LOGIC EXPLANATION (4): How timer deadline is calculated and what it triggers
        # Timer deadline = current processing time + HEARTBEAT_INTERVAL_MS (30 seconds)
        # If no new heartbeat arrives within 30 seconds, the timer expires and triggers
        # handleExpiredTimer(), which generates a DEVICE_OFFLINE alert to SNS.
        # This implements the "watchdog" pattern for detecting missing heartbeats.
        
        current_time_ms = timerValues.getCurrentProcessingTimeInMs()
        deadline_ms = current_time_ms + HEARTBEAT_INTERVAL_MS
        self.handle.registerTimer(deadline_ms)
        
        yield pd.DataFrame()

#The handleExpiredTimer()method is triggered automatically when a device's inactivity timer expires, retrieving the last_seen state to calculate the offline duration and yielding an alert DataFrame to the output stream. 
#It also registers a follow-up timer for repeat alerts every 60 seconds, which continues until a new heartbeat arrives and cancels the timer via handleInputRows(). 

    def handleExpiredTimer(self, key: tuple, timerValues, expiredTimerInfo) -> Iterator[pd.DataFrame]:
        device_id = key[0]
        current_time_ms = timerValues.getCurrentProcessingTimeInMs()
        
        if not self.last_seen.exists():
            yield pd.DataFrame()
            return
        
        last_seen_state = self.last_seen.get()
        last_seen_timestamp = last_seen_state[0]
        
        if last_seen_timestamp is None or pd.isna(last_seen_timestamp):
            yield pd.DataFrame()
            return
        
        last_seen_ms = int(last_seen_timestamp.timestamp() * 1000)
        offline_duration_ms = current_time_ms - last_seen_ms
        offline_duration_seconds = offline_duration_ms / 1000.0
        
        alert_timestamp = datetime.fromtimestamp(current_time_ms / 1000.0)
        
        alert_df = pd.DataFrame({
            "device_id": [device_id],
            "alert_type": ["DEVICE_OFFLINE"],
            "last_seen": [last_seen_timestamp],
            "offline_duration_seconds": [offline_duration_seconds],
            "alert_timestamp": [alert_timestamp]
        })
        
        next_alert_time = current_time_ms + ALERT_REPEAT_INTERVAL_MS
        self.handle.registerTimer(next_alert_time)
        
        yield alert_df
    
    def close(self) -> None:
        pass


def send_to_sns(batch_df, batch_id):
    if batch_df.count() > 0:
        sns_client = boto3.client('sns', region_name=KINESIS_REGION)
        
        for row in batch_df.collect():
            message = {
                "device_id": row["device_id"],
                "alert_type": row["alert_type"],
                "last_seen": str(row["last_seen"]),
                "offline_duration_seconds": row["offline_duration_seconds"],
                "alert_timestamp": str(row["alert_timestamp"])
            }
            
            sns_client.publish(
                TopicArn=SNS_TOPIC_ARN,
                Message=json.dumps(message),
                Subject=f"Device Offline Alert: {row['device_id']}"
            )


def main():
    spark = SparkSession.builder \
        .appName("IoT-Heartbeat-Monitor-Spark4-v7") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    kinesis_df = spark.readStream \
        .format("aws-kinesis") \
        .option("kinesis.streamName", KINESIS_STREAM_NAME) \
        .option("kinesis.region", KINESIS_REGION) \
        .option("kinesis.endpointUrl", f"https://kinesis.{KINESIS_REGION}.amazonaws.com") \
        .option("kinesis.startingPosition", "LATEST") \
        .option("kinesis.describeShardInterval", "30s") \
        .load()
    
    # Parse JSON and handle timestamp conversion with multiple formats
    parsed_df = kinesis_df \
        .selectExpr("CAST(data AS STRING) as json_data") \
        .select(from_json(col("json_data"), heartbeat_schema).alias("heartbeat")) \
        .select(
            col("heartbeat.device_id"),
            coalesce(
                to_timestamp(col("heartbeat.timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
                to_timestamp(col("heartbeat.timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
                to_timestamp(col("heartbeat.timestamp"), "yyyy-MM-dd HH:mm:ss"),
                to_timestamp(col("heartbeat.timestamp")),
                current_timestamp()
            ).alias("timestamp"),
            col("heartbeat.battery_level"),
            col("heartbeat.signal_strength"),
            col("heartbeat.firmware_version")
        ) \
        .filter(col("device_id").isNotNull())
    
    alerts_df = parsed_df \
        .groupBy("device_id") \
        .transformWithStateInPandas(
            statefulProcessor=HeartbeatMonitor(),
            outputStructType=alert_output_schema,
            outputMode="append",
            timeMode="processingTime"
        )
    
    query = alerts_df.writeStream \
        .outputMode("append") \
        .foreachBatch(send_to_sns) \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .trigger(processingTime="10 seconds") \
        .start()
    
    query.awaitTermination()

if __name__ == "__main__":
    main()

"""
CLEANUP INSTRUCTIONS
====================

⚠️ IMPORTANT: Delete resources in this order to avoid ongoing charges.

COMPREHENSIVE RESOURCE INVENTORY
---------------------------------
Configuration Variable → AWS Resource → Cleanup Command

1. STREAMING QUERY (Application Runtime)
   Variable: N/A
   Resource: Running Spark job
   Command: Press Ctrl+C (interactive) OR
            aws emr-serverless stop-job-run \
              --application-id <APPLICATION_ID> \
              --job-run-id <JOB_RUN_ID>

2. EMR SERVERLESS APPLICATION
   Variable: N/A (created separately)
   Resource: EMR Serverless application
   Command: aws emr-serverless delete-application \
              --application-id <APPLICATION_ID>

3. KINESIS_STREAM_NAME = "iot-heartbeats"
   Variable: KINESIS_STREAM_NAME
   Resource: Kinesis Data Stream
   Command: aws kinesis delete-stream \
              --stream-name iot-heartbeats \
              --region us-east-1

4. SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:123456789012:iot-alerts"
   Variable: SNS_TOPIC_ARN
   Resource: SNS Topic for alerts
   Command: aws sns delete-topic \
              --topic-arn arn:aws:sns:us-east-1:123456789012:iot-alerts

5. CHECKPOINT_LOCATION = "s3://amzn-s3-demo-bucket-emrserverless-streaming/checkpoint-heartbeat-monitor-v7/"
   Variable: CHECKPOINT_LOCATION
   Resource: S3 checkpoint data
   Command: aws s3 rm s3://amzn-s3-demo-bucket-emrserverless-streaming/checkpoint-heartbeat-monitor-v7/ --recursive

6. S3 BUCKET (if created for this example)
   Variable: Extracted from CHECKPOINT_LOCATION
   Resource: S3 bucket "amzn-s3-demo-bucket-emrserverless-streaming"
   Command: aws s3 rb s3://amzn-s3-demo-bucket-emrserverless-streaming --force

Note: Replace account IDs (123456789012) and application/job IDs with your actual values.
"""