from prometheus_client import start_http_server
import time
import os
import structlog

structlog.configure(
    processors=[
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=False),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.dev.ConsoleRenderer(colors=True) 
    ],
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

log = structlog.get_logger(__name__)

DEFAULT_PORT = 8082

def start_metrics_server(port: int = DEFAULT_PORT):
    """Starts the Prometheus metrics HTTP server."""
    actual_port = int(os.environ.get("APP_METRICS_PORT", port))
    log.info(f"Attempting to start Prometheus metrics server on port {actual_port}...")
    try:

        start_http_server(actual_port)
        log.info(f"Prometheus metrics server started successfully on port {actual_port}.")
        while True:
            time.sleep(60)
    except OSError as e:
        log.error(f"Failed to start metrics server on port {actual_port}. Port likely in use.", error=str(e), exc_info=True)
        raise
    except Exception as e:
        log.error("Metrics server encountered an unexpected error", error=str(e), exc_info=True)
        raise


if __name__ == "__main__":
    start_metrics_server()