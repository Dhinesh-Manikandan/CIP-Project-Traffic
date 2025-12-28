import os
import json
import yaml
import time
from kafka import KafkaConsumer
import pandas as pd

from dask.distributed import Client, LocalCluster
import dask.dataframe as dd


# --------------------------------------------------
# Traffic status function
# --------------------------------------------------
def traffic_status(avg_severity):
    if avg_severity >= 4:
        return "RED - Severe Congestion"
    elif avg_severity >= 3:
        return "YELLOW - Moderate Traffic"
    else:
        return "GREEN - Smooth Traffic"


def main():
    # --------------------------------------------------
    # Load configuration
    # --------------------------------------------------
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, "..", "config", "config.yaml")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    KAFKA_TOPIC = config["kafka"]["topic"]
    KAFKA_SERVER = config["kafka"]["bootstrap_servers"]

    # --------------------------------------------------
    # Start Dask cluster (Windows-safe)
    # --------------------------------------------------
    cluster = LocalCluster(
        n_workers=4,
        threads_per_worker=1,
        processes=True
    )
    client = Client(cluster)

    print("✅ Dask dashboard:", client.dashboard_link)

    # --------------------------------------------------
    # Kafka Consumer
    # --------------------------------------------------
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True
    )

    print(f"✅ Listening to Kafka topic: {KAFKA_TOPIC}")

    buffer = []
    BATCH_SIZE = 100
    batch_id = 0

    # --------------------------------------------------
    # Streaming loop
    # --------------------------------------------------
    for msg in consumer:
        buffer.append(msg.value)

        if len(buffer) >= BATCH_SIZE:
            batch_id += 1
            batch_start_time = time.time()   # ⏱ START TIMER

            # Convert to Pandas
            pdf = pd.DataFrame(buffer)

            # Ensure numeric columns
            pdf["Severity"] = pd.to_numeric(pdf["Severity"], errors="coerce")
            pdf["Visibility"] = pd.to_numeric(pdf["Visibility"], errors="coerce")

            # Convert to Dask DataFrame
            ddf = dd.from_pandas(pdf, npartitions=4)

            # --------------------------------------------------
            # Distributed Traffic Analysis
            # --------------------------------------------------
            traffic_analysis = (
                ddf
                .groupby("Street")
                .agg({
                    "Severity": "mean",
                    "Visibility": "mean",
                    "Street": "count"
                })
                .rename(columns={
                    "Severity": "avg_severity",
                    "Visibility": "avg_visibility",
                    "Street": "vehicle_count"
                })
                .reset_index()
            )

            result = traffic_analysis.compute()

            # Traffic status
            result["Traffic_Status"] = result["avg_severity"].apply(traffic_status)

            # ⏱ END TIMER
            batch_time = time.time() - batch_start_time
            throughput = len(buffer) / batch_time

            # --------------------------------------------------
            # Output with performance metrics
            # --------------------------------------------------
            print("\n========== Traffic Analysis ==========")
            print(f"Batch ID            : {batch_id}")
            print(f"Records processed   : {len(buffer)}")
            print(f"Processing time     : {batch_time:.3f} seconds")
            print(f"Throughput          : {throughput:.2f} records/sec")
            print("-------------------------------------")
            print(result)
            print("=====================================")

            buffer.clear()


# 🔴 REQUIRED ON WINDOWS 🔴
if __name__ == "__main__":
    main()
