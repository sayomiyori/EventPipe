from prometheus_client import Counter, Gauge, Histogram

events_processed_total = Counter("events_processed_total", "Count of successfully processed events")
events_failed_total = Counter("events_failed_total", "Count of failed transform events")
transform_duration_seconds = Histogram("transform_duration_seconds", "Duration of transform processing")
kafka_consumer_lag = Gauge("kafka_consumer_lag", "Consumer lag for processed partition")
s3_upload_duration_seconds = Histogram("s3_upload_duration_seconds", "Duration of S3 upload")
