
# CIP Traffic Project

This project simulates real-time processing of US traffic congestion data using Python, Kafka, and Dask.  

It includes:

- Kafka producer to send traffic data in chunks  
- Dask consumer for parallel data processing  
- Support for KaggleHub dataset download  

---

## Environment

- Python: 3.14.2  
- Messaging: Kafka  
- Processing: Dask  
- Dataset: US traffic congestion CSV (downloaded via KaggleHub)  

---

## Prerequisites

1. **Install Docker**  
   - Download and install Docker Desktop: https://www.docker.com/get-started  
   - Ensure Docker is running before starting Kafka or other services.

2. **Kafka setup using Docker**  
   - Create a `docker-compose.yml` file with Kafka and Zookeeper configuration (if not already provided).  
   - Start Kafka and Zookeeper:

```

docker-compose up -d

```



- Verify Kafka is running:

```

docker ps

```

---

## Setup

Follow these steps to set up the project environment:

### 1️⃣ Clone the project

```

git clone [https://github.com/Dhinesh-Manikandan/CIP-Project.git](https://github.com/Dhinesh-Manikandan/CIP-Project.git)
cd CIP-Project

```

### 2️⃣ Create Python virtual environment (Python 3.14.2)



#### Create virtual environment
```
python -m venv venv
```
#### Activate virtual environment (Windows PowerShell)
```
venv\Scripts\Activate.ps1
```
###@ OR activate virtual environment (Windows Command Prompt)
```
venv\Scripts\activate.bat
```
###@ OR activate virtual environment (Linux / MacOS)
```
source venv/bin/activate
```


### 3️⃣ Upgrade pip (optional but recommended)

```

python -m pip install --upgrade pip

```

### 4️⃣ Install required Python packages

```

pip install -r requirements.txt

```

> This installs all required dependencies, including:  
> `pandas`, `numpy`, `kafka-python`, `kagglehub`, `PyYAML`, `dask[complete]`, etc.

---

## Dataset

- Dataset is **not included** in the repository.  
- It is automatically downloaded using **KaggleHub** when you run the producer.  
- Ensure you have your Kaggle API credentials set up in your environment.

---

## Running the Project

### 1️⃣ Start Kafka Broker (Docker)

```

docker-compose up -d

```

- This starts Kafka and Zookeeper in detached mode (`-d`).  
- Ensure Kafka broker is accessible at `localhost:9092` or update the code accordingly.

### 2️⃣ Run the Producer

Open **terminal 1** and activate your virtual environment:

```

# Activate environment

venv\Scripts\Activate.ps1

```

Run the producer script:

```

python producer.py

```

- The producer reads CSV chunks and sends messages to the Kafka topic.  

### 3️⃣ Run the Dask Consumer

Open **terminal 2** and activate the **same virtual environment**:

```

# Activate environment

venv\Scripts\Activate.ps1

```

Run the consumer script:

```

python consumer_dask.py

```

- The Dask consumer processes messages from Kafka in parallel.  
- You can monitor progress and logs in the terminal.

---

## Logging

- Logs are automatically written to the `logs/` folder.  
- `producer.log` captures producer events.  
- `consumer.log` (if implemented) captures consumer events.  

> Note: Logs are **not tracked in Git** (see `.gitignore`).

---

## Notes

- Always run **producer first**, then the **consumer**.  
- Use the **same virtual environment** for all terminals.  
- If the project is run in a Docker container, ensure the container has Python 3.14.2 and the virtual environment is created inside it.  

---

## Clean Git Setup

- Virtual environment, data, and logs are excluded from Git.  
- All users can reproduce the project by cloning the repo and running the above setup steps.

---

## References

- [Kafka Python Documentation](https://kafka-python.readthedocs.io/en/master/)  
- [Dask Documentation](https://docs.dask.org/en/stable/)  
- [KaggleHub Documentation](https://pypi.org/project/kagglehub/)  
- [Docker Documentation](https://docs.docker.com/)
```

