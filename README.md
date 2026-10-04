# ITO5202 Assessment 2

Student ID: 27602966

Unit: ITO5202 Data Processing for Big Data  
Teaching Period: TP5  
Year: 2026

Development stage: Environment setup.

## Project

Assessment 2 reuses the approved Assessment 1 dataset: the twelve monthly 2023 NYC TLC Yellow Taxi Parquet files and the Taxi Zone Lookup CSV. The assessment objective is to train a Spark ML pipeline on historical records and apply the saved pipeline to held-out records replayed through Kafka.

This README documents the current Docker environment, data placement and Kafka connection checks.

## Environment

The settings below reflect the Spark output reported during Assessment 2 setup.

| Setting | Value |
| --- | --- |
| Spark | 3.5.5 |
| Scala | 2.12.18 |
| Spark master | local[2] |
| Driver memory | 2g |
| Spark session timezone | America/New_York |
| Structured Streaming Kafka connector | org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 |
| Docker network | ito5202 |
| Jupyter container | ito5202-a2 |
| ZooKeeper container | zookeeper |
| Kafka container | kafka |
| Kafka address inside the Docker network | kafka:9092 |

| Service | Docker image | Host port |
| --- | --- | --- |
| PySpark and Jupyter | monashfit/ito5202-pyspark:latest | 5202 and 4040 |
| ZooKeeper | monashfit/ito5202-zookeeper:latest | 2181 |
| Kafka | monashfit/ito5202-kafka:latest | 9092 |

The installed Python package versions are recorded in **requirements.txt**. The image names and image IDs used by the containers are recorded in **docker-images.txt**. Check the actual Spark and Scala versions after a fresh image pull; the Kafka connector must match them.

## Project folder and files

Clone or download this repository into a folder of your choice. Open PowerShell in the **repository root**, the folder containing this README.

Run the host commands below from that folder. The container creation command uses the current folder as its bind mount source.

The repository root is mounted inside the PySpark container at:

```text
/home/student/work
```

| Item | Purpose |
| --- | --- |
| README.md | Environment setup, startup, verification and shutdown |
| data/README.md | Dataset sources and restoration instructions |
| data/ | Local copies of the thirteen Assessment 1 source files |
| requirements.txt | Snapshot of installed Python packages |
| docker-images.txt | Container image names and image IDs |
| .gitignore | Rules for large Parquet inputs, caches and temporary files |
| Saved environment check notebook | Spark and Kafka verification cells and their outputs |

## Starting the existing containers

Start Docker Desktop with Linux containers enabled. From the repository root, run the following in **PowerShell**:

```powershell
docker start zookeeper
Start-Sleep -Seconds 30
docker start kafka
Start-Sleep -Seconds 30
docker start ito5202-a2

docker ps
docker logs --tail 50 kafka
docker logs --tail 50 ito5202-a2
```

Open [Jupyter](http://localhost:5202). If a token is requested, use the token shown in the Jupyter container logs. The [Spark UI](http://localhost:4040) becomes available while a Spark application is running.

Use the following commands to inspect the network and service logs:

```powershell
docker network inspect ito5202
docker logs --tail 30 zookeeper
docker logs --tail 50 kafka
```

## Creating the environment on a new installation

Use this section when the named containers have not yet been created. Existing installations use the startup section above.

Install and start [Docker Desktop for Windows](https://docs.docker.com/desktop/setup/install/windows-install/) with Linux containers enabled. Clone or download the repository and open PowerShell in its root folder before running these commands:

```powershell
$assessment2Path = (Get-Location).Path
New-Item -ItemType Directory -Force -Path data, models, checkpoints, outputs

docker network create ito5202

docker run -d --name zookeeper --network ito5202 -p 2181:2181 monashfit/ito5202-zookeeper:latest
Start-Sleep -Seconds 30

docker run -d --name kafka --network ito5202 -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 -e KAFKA_ADVERTISED_HOST_NAME=kafka -p 9092:9092 monashfit/ito5202-kafka:latest
Start-Sleep -Seconds 30

docker run -d --name ito5202-a2 --network ito5202 --mount "type=bind,source=$assessment2Path,target=/home/student/work" --workdir /home/student/work -p 5202:5202 -p 4040:4040 monashfit/ito5202-pyspark:latest jupyter notebook --ip=0.0.0.0 --port=5202 --no-browser

docker ps
docker logs --tail 50 kafka
docker logs --tail 50 ito5202-a2
```

The **$assessment2Path** variable captures your repository's actual location. Restore the data using [data/README.md](data/README.md).

## Python packages

Open Jupyter and create a Python notebook for the environment checks. In a notebook cell:

```python
%pip install kafka-python==3.0.11 pyarrow
```

Restart the notebook kernel after installing packages.

The Kafka Python client is imported from **kafka**. Run the following cells inside Jupyter in the PySpark container so that the broker hostname **kafka** resolves on the Docker network.

## Spark initialisation and version check

Run this in a fresh notebook kernel, before creating a Spark session:

```python
import os
import sys
import pyspark

assert pyspark.__version__ == "3.5.5", (
    f"Expected PySpark 3.5.5; found {pyspark.__version__}. "
    "Check the image version and matching Kafka connector."
)

connector = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    f"--driver-memory 2g --packages {connector} pyspark-shell"
)

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("ITO5202 Assessment 2 Environment Check")
    .master("local[2]")
    .config("spark.ui.port", "4040")
    .config("spark.sql.session.timeZone", "America/New_York")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("Python:", sys.version.split()[0])
print("Spark:", spark.version)
print("Scala:", spark._jvm.scala.util.Properties.versionNumberString())
print("Master:", spark.sparkContext.master)
print("Driver memory:", spark.sparkContext.getConf().get("spark.driver.memory"))
print("Timezone:", spark.conf.get("spark.sql.session.timeZone"))
assert spark.version == pyspark.__version__
spark.range(3).show()
```

The output should match the environment table and show the values 0, 1 and 2. Changing the connector requires restarting the kernel before rerunning this cell.

To check Java and the installed Spark distribution, open **New → Terminal** from the Jupyter file browser and run:

```bash
pwd
java -version
spark-submit --version
```

The working folder should be **/home/student/work**.

## Kafka test topic

On a fresh installation, create the test topic once. Run this in the **Jupyter terminal**:

```bash
python3 -m kafka.admin -b kafka:9092 topics create -t a2_setup_test
```

## Publish a test message

Run this in a notebook cell:

```python
import json
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=["kafka:9092"])

try:
    payload = [{"status": "environment_ready"}]
    result = producer.send(
        "a2_setup_test",
        value=json.dumps(payload).encode("utf-8")
    ).get(timeout=20)

    print("Topic:", result.topic)
    print("Partition:", result.partition)
    print("Offset:", result.offset)
finally:
    producer.close()
```

Successful publication prints the topic, partition and offset.

## Read the message with Spark Structured Streaming

In the same notebook, run:

```python
test_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "a2_setup_test")
    .option("startingOffsets", "earliest")
    .load()
    .selectExpr("CAST(value AS STRING) AS payload")
)

query = (
    test_stream.writeStream
    .format("memory")
    .queryName("a2_environment_check")
    .outputMode("append")
    .start()
)

try:
    query.processAllAvailable()
    spark.sql(
        "SELECT payload FROM a2_environment_check"
    ).show(truncate=False)
finally:
    query.stop()
```

The displayed payload should contain:

```json
[{"status": "environment_ready"}]
```

The memory sink makes the result visible in the notebook. The cell stops the test query when it finishes.

## Record the installed environment

In a notebook cell:

```python
import subprocess
import sys
from pathlib import Path

packages = subprocess.check_output(
    [sys.executable, "-m", "pip", "freeze"],
    text=True
)
requirements_path = Path("/home/student/work/requirements.txt")
requirements_path.write_text(packages, encoding="utf-8")
print("Created:", requirements_path)
```

From the repository root in PowerShell:

```powershell
docker inspect --format '{{.Name}} {{.Config.Image}} {{.Image}}' ito5202-a2 zookeeper kafka | Set-Content -Encoding utf8 docker-images.txt
Get-Content docker-images.txt
```

Save the environment check notebook with its outputs visible.

## Shutdown

Save open notebooks. If the current notebook still has active streaming queries, stop them before stopping Spark:

```python
for active_query in spark.streams.active:
    active_query.stop()
spark.stop()
```

Then run in Windows PowerShell:

```powershell
docker stop ito5202-a2
docker stop kafka
docker stop zookeeper
```

Reuse the existing containers with the startup commands when resuming work.

## References

- [PowerShell Get-Location](https://learn.microsoft.com/en-us/powershell/module/microsoft.powershell.management/get-location)
- [Docker bind mounts](https://docs.docker.com/engine/storage/bind-mounts/)
- [Docker container execution](https://docs.docker.com/reference/cli/docker/container/exec/)
- [Spark 3.5.5 Structured Streaming Kafka integration](https://archive.apache.org/dist/spark/docs/3.5.5/structured-streaming-kafka-integration.html)
- [kafka-python](https://pypi.org/project/kafka-python/)
- [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
