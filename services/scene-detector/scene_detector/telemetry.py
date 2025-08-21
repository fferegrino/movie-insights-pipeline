# telemetry.py
from opentelemetry import metrics
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.resources import Resource
from prometheus_client import start_http_server

# Initialize Prometheus reader
reader = PrometheusMetricReader()

# Attach resource info (optional, but recommended)
resource = Resource.create({"service.name": "scene-detector"})

# Create provider and set it as global
provider = MeterProvider(resource=resource, metric_readers=[reader])
metrics.set_meter_provider(provider)


# Start the /metrics endpoint in the background
def start_metrics_server(port: int = 9464):
    """
    Start the Prometheus metrics server.

    Args:
        port: The port to run the server on.

    """
    start_http_server(port)
    print(f"✅ Prometheus metrics server running at http://localhost:{port}/metrics")


# Create instruments
meter = metrics.get_meter("scene-detector")

video_chunks_processed = meter.create_counter(
    name="video_chunks_processed_total",
    description="Total number of video chunks processed",
    unit="1",
)

video_chunks_processed_duration = meter.create_histogram(
    name="video_chunks_processed_duration_seconds",
    description="Duration of video chunks processed",
    unit="s",
)

detected_scenes = meter.create_counter(
    name="detected_scenes_total",
    description="Total number of scenes detected",
    unit="1",
)

detect_scenes_duration = meter.create_histogram(
    name="detect_scenes_duration_seconds",
    description="Duration of detect_scenes",
    unit="s",
)
