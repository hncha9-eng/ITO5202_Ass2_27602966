# ITO5202 Assessment 2: Machine Learning and Streaming Analytics

Student ID: `27602966` · Unit: ITO5202 Data Processing for Big Data · Teaching period: TP5 · Year: `2026`

Repository: [ITO5202_Ass2_27602966](https://github.com/hncha9-eng/ITO5202_Ass2_27602966)

## Project overview

Assessment 2 reuses the approved Assessment 1 dataset: the twelve monthly NYC TLC Yellow Taxi files for `2023` and the Taxi Zone Lookup CSV. Part A compares Linear Regression and Random Forest for predicting recorded base fare (`fare_amount`, USD). Part B loads the saved Random Forest pipeline and scores previously held-out completed-trip records replayed through Kafka. The task is completed-trip fare prediction; actual distance and duration are available when each record arrives.

The notebook includes validation MAE comparisons by rate code, full final-test evaluation, an explicitly labelled outlier sensitivity check, and model persistence verification using two seeded fitting records per rate-code category. The full-test metrics remain the reported result. Streaming includes prediction sinks, watermarked sliding windows, reconciliation against a batch baseline, and the embedded reflection.

## Repository contents

| Path | Purpose and Git treatment |
| --- | --- |
| `27602966_assessment2.ipynb`, `27602966_assessment2.pdf` | Executed notebook notebook containing Parts A and B, results and reflection and PDF export; tracked. |
| `producer.py` | Standalone Kafka producer; tracked. The notebook's Part B, Section 2.1 writes this file. |
| `models/a2_model/` | Complete persisted Spark `PipelineModel`, including all metadata, stages and internal Parquet files; tracked. |
| `requirements.txt` | Recorded Python package versions; tracked. |
| `docker-images.txt` | Recorded container image names and image IDs; tracked. |
| `.gitignore`, `README.md`, `data/README.md` | Ignore rules and reproduction instructions; tracked. |
| `data/taxi_zone_lookup.csv` | Small reference lookup; tracked. |
| `data/yellow_tripdata_2023-*.parquet` | Original monthly files; restored locally and excluded from Git. |
| `data/train_data.parquet/`, `data/stream_data.parquet/` | Generated training/evaluation pool and streaming holdout; excluded from Git. |
| `data/split_metadata.json` | Verified split settings, counts and schemas; tracked. |
| `outputs/streaming/<run_id>/` | Generated run evidence. Prediction and window Parquet directories are ignored; small JSON/JSONL evidence is eligible for Git. |
| `checkpoints/<run_id>/` | Spark streaming state and offsets; generated locally and ignored. |

## Environment

The configuration matches the assessment environment and notebook checks.

| Component | Configuration |
| --- | --- |
| Python | `3.10` |
| Spark / Scala | `3.5.5` / `2.12.18` |
| Spark master / driver memory | `local[2]` / `2g` |
| Spark session timezone | `America/New_York` |
| Kafka connector | `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5` |
| Python Kafka client / PyArrow | `kafka-python==3.0.11` / `pyarrow==23.0.1` |
| Docker network | `ito5202` |
| Internal Kafka address | `kafka:9092` |
| Repository mount | `/home/student/work` |

| Service | Container name | Docker image | Published host ports |
| --- | --- | --- | --- |
| PySpark and Jupyter | `ito5202-a2` | `monashfit/ito5202-pyspark:latest` | `5202`, `4040` |
| ZooKeeper | `zookeeper` | `monashfit/ito5202-zookeeper:latest` | `2181` |
| Kafka | `kafka` | `monashfit/ito5202-kafka:latest` | `9092` |

The image tags above are the recorded setup tags. `docker-images.txt` identifies the images used for the assessment run. The notebook checks Spark versions before starting; the connector version and Scala suffix match the verified Spark distribution.

## 1. Prepare the checkout

Install and start [Docker Desktop](https://docs.docker.com/desktop/setup/install/windows-install/) with Linux containers enabled. Clone the repository with Git or GitHub Desktop, or download and extract its ZIP. Open PowerShell in the repository root, the folder containing this README. All host commands below use that folder and contain no personal Windows directory.

Restore the approved dataset using [data/README.md](data/README.md). Keep space available for the original files, both generated Parquet subsets, and the notebook's disk-backed Spark caches. Initial image pulls, package installation and Spark connector resolution require internet access.

## 2. Create the containers once

Use this section when the three named containers do not already exist. Existing containers use the next section.

```powershell
$assessment2Path = (Get-Location).Path
New-Item -ItemType Directory -Force -Path data, models, checkpoints, outputs

docker network inspect ito5202 *> $null
if ($LASTEXITCODE -ne 0) {
    docker network create ito5202
}

docker run -d `
    --name zookeeper `
    --network ito5202 `
    -p 2181:2181 `
    monashfit/ito5202-zookeeper:latest
Start-Sleep -Seconds 30

docker run -d `
    --name kafka `
    --network ito5202 `
    -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 `
    -e KAFKA_ADVERTISED_HOST_NAME=kafka `
    -p 9092:9092 `
    monashfit/ito5202-kafka:latest
Start-Sleep -Seconds 30

docker run -d `
    --name ito5202-a2 `
    --network ito5202 `
    --mount "type=bind,source=$assessment2Path,target=/home/student/work" `
    --workdir /home/student/work `
    -p 5202:5202 `
    -p 4040:4040 `
    monashfit/ito5202-pyspark:latest `
    jupyter notebook --ip=0.0.0.0 --port=5202 --no-browser

docker ps --filter network=ito5202
docker logs --tail 50 kafka
docker logs --tail 50 ito5202-a2
```

The startup pauses allow service initialisation; the broker readiness check below confirms connectivity. Both the producer and Spark run inside `ito5202-a2`, where the advertised broker hostname `kafka` resolves on the Docker network.

## 3. Start existing containers

After starting Docker Desktop, run:

```powershell
docker start zookeeper
Start-Sleep -Seconds 30
docker start kafka
Start-Sleep -Seconds 30
docker start ito5202-a2

docker ps --filter network=ito5202
docker logs --tail 50 kafka
docker logs --tail 50 ito5202-a2
```

Open [Jupyter](http://localhost:5202) and use the token displayed in the Jupyter logs if requested. The [Spark UI](http://localhost:4040) is available while the notebook's Spark application runs. A Jupyter terminal opens through **New → Terminal** on the file browser.

## 4. Install client packages and check Kafka

On a new container, run these host PowerShell commands:

```powershell
docker exec ito5202-a2 python -m pip install kafka-python==3.0.11 pyarrow==23.0.1
docker exec ito5202-a2 python -c "import pyspark, kafka, pyarrow; print('PySpark:', pyspark.__version__); print('kafka-python:', kafka.__version__); print('PyArrow:', pyarrow.__version__)"
docker exec ito5202-a2 python -m kafka.admin -b kafka:9092 topics list
```

Restart an already open notebook kernel after installation. The container supplies Spark, Jupyter, pandas and Matplotlib; `requirements.txt` records the full installed environment. Kafka topic listing must complete before a live producer runs. If the broker is still starting, inspect its logs and repeat that readiness command.

## 5. Execute the notebook

Open `27602966_assessment2.ipynb` from the Jupyter root and use a fresh kernel. Run cells in order, beginning with **Execution Environment**. The cleaned notebook retains reference outputs from the earlier executed version and has cleared execution counts. A complete run refreshes the outputs and timings before submission.

| Notebook section | Execution and evidence |
| --- | --- |
| Execution Environment | Configures Spark and the matching Kafka connector, centralises notebook imports and shared helpers, and configures a writable Matplotlib cache. |
| Dataset Reuse and Train/Stream Split | Validates all source files, standardises monthly schemas, applies eligibility rules, joins taxi zones, saves the seeded train/stream split, and verifies reloaded counts and schemas. It also creates the internal fitting, validation and final-test split. |
| Part A, Sections 1–3 | Justifies regression, fits preprocessing on the fitting subset, compares both regressors on the same validation records, and presents MAE by rate code. |
| Part A, Section 4 | Reports the complete final test, diagnoses the largest errors, and computes a separately labelled single-record sensitivity check. |
| Part A, Section 5 | Saves `models/a2_model/`, reloads it, and checks output schema, feature vectors and predictions on the seeded stratified sample. |
| Part B, Sections 1–2 | Checks the saved holdout and model, declares the JSON schema, writes `producer.py`, runs a dry run, creates missing topics, and verifies a live producer test. |
| Part B, Section 3 | Loads the saved pipeline, parses Kafka arrays with an explicit schema, applies all preprocessing and the regressor, writes console/Parquet predictions, and reconciles the integration test. |
| Part B, Section 4 | Runs the bounded monitoring demonstration, stops its queries, and verifies predictions, offsets, watermarks and finalised windows against a batch calculation. |
| Part B, Section 5 | Contains the assessment reflection. Review its evidence against the completed run. |

Part B consumes the persisted pipeline directly. It contains no model fitting. The observed fare remains available for auditing and is excluded from the input feature vector.

The notebook starts the producer for its live demonstrations. Run the standalone examples below separately from those demonstrations; an extra producer changes the expected record counts and offsets.

### Kafka topic setup

Part B, Section 2.3 creates missing topics and waits for partition leaders. For a fresh broker, the equivalent host PowerShell commands are:

```powershell
docker exec ito5202-a2 python -m kafka.admin -b kafka:9092 topics create -t a2_taxi_events --num-partitions 2 --replication-factor 1
docker exec ito5202-a2 python -m kafka.admin -b kafka:9092 topics create -t a2_taxi_producer_test --num-partitions 2 --replication-factor 1
docker exec ito5202-a2 python -m kafka.admin -b kafka:9092 topics list
```

Topic creation is a one-time operation on a fresh broker. The notebook checks for existing topics before creating them.

### Standalone producer commands

These commands require the saved `data/stream_data.parquet/` directory and `producer.py`. The script is included in the repository; if it is absent, execute the `%%writefile producer.py` cell in Part B, Section 2.1 first.

Inspect two JSON batches without publishing or sleeping:

```powershell
docker exec -w /home/student/work ito5202-a2 python producer.py --data-path data/stream_data.parquet --batch-size 1000 --max-batches 2 --dry-run
```

Publish the separate producer verification test:

```powershell
docker exec -w /home/student/work ito5202-a2 python producer.py --data-path data/stream_data.parquet --bootstrap-servers kafka:9092 --topic a2_taxi_producer_test --batch-size 1000 --max-batches 3
```

The notebook's monitoring demonstration starts its queries and runs the equivalent main-topic command below. It manages the producer process and captures its logs; this block documents that invocation and is not an additional step during the notebook run.

```powershell
docker exec -w /home/student/work ito5202-a2 python producer.py --data-path data/stream_data.parquet --bootstrap-servers kafka:9092 --topic a2_taxi_events --batch-size 1000 --max-batches 60
```

| Producer option | Behaviour |
| --- | --- |
| `--data-path` | Reads the saved streaming subset; no runtime split. |
| `--bootstrap-servers`, `--topic` | Select the broker and destination topic. |
| `--batch-size` | Defaults to `1000` records; batching is parameterised. |
| `--max-batches` | Defaults to `60`; `0` selects the complete holdout. Notebook demonstration audits use their configured bounded counts. |
| `--dry-run` | Validates serialisation locally without Kafka or timing pauses. |

Each Kafka message contains one JSON array. Every record in a batch receives the same UTC `event_timestamp` at publication, while the original pickup timestamp is preserved separately. The producer sleeps for exactly `5` seconds between live batches; reading, serialisation and delivery add to the observed publication interval. JSON stdout logs include record counts, timestamps, payload sizes, acknowledgements, partitions and offsets. The script rejects payloads above its `900000`-byte guard; larger volumes require appropriately sized batches.

## 6. Monitor and inspect results

The main demonstration uses `60` batches of `1000` records, a `5`-second processing trigger, `60`-second windows, a `10`-second slide and a `30`-second watermark delay. Windows report trip count and mean predicted base fare by pickup borough. Producer publication timestamps drive both windowing and watermarking.

Every demonstration captures fresh Kafka end offsets and creates a unique run directory and checkpoint location. This prevents earlier test messages from entering its expected-count audit.

| Generated path under `outputs/streaming/<run_id>/` | Contents |
| --- | --- |
| `predictions/` | Individual predictions and Kafka/event identifiers in Parquet. |
| `finalised_windows/` | Finalised watermarked window aggregates in Parquet for the monitoring run. |
| `producer_stdout.jsonl` | Producer publication and acknowledgement logs. |
| `query_progress.json` | Captured micro-batch progress and processing durations. |
| `run_metadata.json` | Run settings, offsets, queries, output paths and completion status. |
| `verification_summary.json` | Monitoring reconciliation, integrity checks, watermark/state evidence and batch-baseline comparison. |

Prediction and window Parquet sinks are regenerated locally and ignored by Git. Retain the small evidence files for the assessed run in the repository, together with visible notebook result tables. Processing durations are printed or saved from each run; no measured execution time is fixed in this README. The trigger interval is a scheduling setting, and query progress records the actual processing durations.

The console sink writes to the Spark driver's output. In this Docker setup, view it from another PowerShell window:

```powershell
docker logs -f ito5202-a2
```

Press `Ctrl+C` to stop following logs. The notebook also displays saved prediction/window previews and verification tables for the notebook and PDF. A stopped producer leaves trailing windows pending because the watermark advances with event time; the audit compares only windows eligible for finalisation at the recorded watermark.

## 7. Shut down

The notebook's demonstration cleanup stops its producer and queries. For any additional active queries, run this in a notebook cell and save the notebook:

```python
for active_query in spark.streams.active:
    active_query.stop()
spark.stop()
```

Then run in host PowerShell:

```powershell
docker stop ito5202-a2
docker stop kafka
docker stop zookeeper
```

Restart the retained containers using Section 3. Notebook, model and data files remain in the bind-mounted checkout.

## References

- [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [Spark 3.5.5 Structured Streaming Kafka integration](https://archive.apache.org/dist/spark/docs/3.5.5/structured-streaming-kafka-integration.html)
- [Docker bind mounts](https://docs.docker.com/engine/storage/bind-mounts/)
- [Docker container execution](https://docs.docker.com/reference/cli/docker/container/exec/)
- [kafka-python documentation](https://kafka-python.readthedocs.io/en/master/)
- [Jupyter nbconvert usage](https://nbconvert.readthedocs.io/en/latest/usage.html)
