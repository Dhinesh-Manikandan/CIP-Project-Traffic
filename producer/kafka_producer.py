import pandas as pd
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import json, time, yaml, os, shutil, logging, kagglehub

# ---------------- Logging ----------------
logging.getLogger("kafka").setLevel(logging.WARNING)
LOG_FILE = os.path.join(os.path.dirname(__file__), "..", "logs", "producer.log")
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, mode="a"), logging.StreamHandler()],
    force=True
)

# ---------------- Config ----------------
script_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(script_dir, "..", "config", "config.yaml")
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

KAFKA_TOPIC = config["kafka"]["topic"]
KAFKA_SERVER = config["kafka"]["bootstrap_servers"]

# ---------------- Ensure Kafka topic ----------------
try:
    admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_SERVER)
    if KAFKA_TOPIC not in admin_client.list_topics():
        topic = NewTopic(name=KAFKA_TOPIC, num_partitions=1, replication_factor=1)
        admin_client.create_topics([topic])
        logging.info(f"Kafka topic '{KAFKA_TOPIC}' created.")
    else:
        logging.info(f"Kafka topic '{KAFKA_TOPIC}' already exists.")
except Exception as e:
    logging.error(f"Error checking/creating Kafka topic: {e}")
    raise

# ---------------- Kafka Producer ----------------
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

# ---------------- Dataset ----------------
DATA_DIR = os.path.join(script_dir, "..", "data", "us_traffic_congestions")
os.makedirs(DATA_DIR, exist_ok=True)

csv_file = None
for root, _, files in os.walk(DATA_DIR):
    for file in files:
        if file.endswith(".csv"):
            csv_file = os.path.join(root, file)
            break

if csv_file is None:
    logging.info("Downloading dataset from Kaggle...")
    dataset_path = kagglehub.dataset_download("sobhanmoosavi/us-traffic-congestions-2016-2022")
    for root, _, files in os.walk(dataset_path):
        for file in files:
            if file.endswith(".csv"):
                shutil.copy(os.path.join(root, file), DATA_DIR)
                csv_file = os.path.join(DATA_DIR, file)
                break

# ---------------- Stream in chunks ----------------
CHUNK_SIZE = 500  # number of rows per chunk (adjust based on memory)
important_cols = ["City","Street","Severity","Start_Lat","Start_Lng","Visibility(mi)","Weather_Conditions"]

batch_count = 0
try:
    for chunk in pd.read_csv(csv_file, usecols=important_cols, chunksize=CHUNK_SIZE, low_memory=False):
        chunk = chunk.dropna()
        chunk = chunk.rename(columns={"Visibility(mi)":"Visibility", "Weather_Conditions":"Weather_Condition"})
        records = chunk.to_dict(orient="records")
        for record in records:
            producer.send(KAFKA_TOPIC, record)
        producer.flush()
        batch_count += 1
        print(f"✅ Published {len(records)} cars data to Kafka topic '{KAFKA_TOPIC}' [Batch {batch_count}]")
        time.sleep(2)  # optional delay for smooth streaming
except KeyboardInterrupt:
    print("🛑 Streaming stopped by user")
finally:
    producer.close()
