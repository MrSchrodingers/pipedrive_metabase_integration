from prometheus_client import start_http_server
import time

def start_metrics_server(port: int = 8082):
    start_http_server(port)
    print(f"Métricas expostas na porta {port}.")
    while True:
        time.sleep(10)

if __name__ == "__main__":
    start_metrics_server(8082)
