# Dataset restoration and split documentation

## Approved source data

Assessment 2 reuses the approved Assessment 1 dataset: all twelve monthly NYC TLC Yellow Taxi Parquet files for `2023`, together with the Taxi Zone Lookup CSV. Reuse the existing Assessment 1 copies when available. The monthly source files and generated Parquet subsets remain local because of their size.

The [official NYC TLC Trip Record Data page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) provides the source downloads and the Yellow Taxi data dictionary. The table below lists the exact expected filenames.

| Month or reference | Filename and official download |
| --- | --- |
| January `2023` | [yellow_tripdata_2023-01.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet) |
| February `2023` | [yellow_tripdata_2023-02.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-02.parquet) |
| March `2023` | [yellow_tripdata_2023-03.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-03.parquet) |
| April `2023` | [yellow_tripdata_2023-04.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-04.parquet) |
| May `2023` | [yellow_tripdata_2023-05.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-05.parquet) |
| June `2023` | [yellow_tripdata_2023-06.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-06.parquet) |
| July `2023` | [yellow_tripdata_2023-07.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-07.parquet) |
| August `2023` | [yellow_tripdata_2023-08.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-08.parquet) |
| September `2023` | [yellow_tripdata_2023-09.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-09.parquet) |
| October `2023` | [yellow_tripdata_2023-10.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-10.parquet) |
| November `2023` | [yellow_tripdata_2023-11.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-11.parquet) |
| December `2023` | [yellow_tripdata_2023-12.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-12.parquet) |
| Taxi zones | [taxi_zone_lookup.csv](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv) |

Place these thirteen files directly in the repository's `data/` folder. The main README mounts the repository at `/home/student/work`, so the same files appear inside the container at `/home/student/work/data`.

Follow the [main README](../README.md) for container creation, packages and startup. In `27602966_assessment2.ipynb`, **Dataset Sources and Input Verification** prints all expected filenames and rejects missing files. **Schema Inspection and Standardised Loading** checks file contents and reconciles the monthly schemas, including numeric types and the `airport_fee` / `Airport_fee` naming difference.

## Eligibility and zone enrichment

Run the notebook's **Execution Environment** and then **Dataset Reuse and Train/Stream Split** sections in order. Eligibility requires a pickup in `2023`, finite positive trip distance and duration, and a finite non-negative fare. Missing passenger counts and rate codes are handled by the fitted preprocessing pipeline rather than removing those records. The Taxi Zone Lookup enriches pickup and drop-off locations; its unique keys and join cardinality are checked before splitting.

The executed source analysis contains `38,310,226` raw trips and `37,192,160` eligible trips. The lookup contains `265` unique location IDs, and the validated joins preserve the eligible count. Each rerun prints its source, eligibility and join checks; `data/split_metadata.json` records the regenerated split evidence.

## Reproduce the saved subsets

The outer split uses seed `42` with weights `[0.75, 0.25]` after eligibility and enrichment. The training/evaluation pool and streaming holdout are disjoint. The internal Part A split uses seed `43` with weights `[0.70, 0.15, 0.15]` within the training/evaluation pool.

| Subset | Reference record count | Use and location |
| --- | --- | --- |
| Training/evaluation pool | `27,895,816` | Saved as `data/train_data.parquet/`; supplies only Part A fitting and evaluation. |
| Streaming holdout | `9,296,344` | Saved as `data/stream_data.parquet/`; supplies the Part B producer. |
| Fitting | `19,528,442` | Internal Part A subset for exploration, preprocessing and regressor fitting. |
| Validation | `4,182,941` | Internal Part A subset for comparison and final model selection. |
| Final test | `4,184,433` | Internal Part A subset for the selected model's reported evaluation. |

Reference counts describe the executed analysis. The notebook checks actual proportions and reconciliation on each run. The outer split exceeds the assessment's minimum training/evaluation and streaming proportions. Validation and final-test records stay within Part A; streaming records remain outside all model fitting and evaluation.

**Subset Persistence and Reload Verification** writes both outer subsets, reloads them, checks counts and column signatures, and saves `data/split_metadata.json`. These Parquet paths are directories containing part files, not single files. The internal fitting, validation and final-test subsets are derived in the notebook; they are not separately saved datasets.

Use the same source copies, Spark version, standardisation logic and seeds when regenerating. Part B reads the persisted holdout directly; the producer does not repeat the split. If only a generated subset is missing, run the preparation and split sections before Part B. Full model reproduction also runs Part A and regenerates `models/a2_model/`.

## Data use during inference

The saved datasets retain enriched trip columns. The producer selects the model's eight raw feature inputs and the observed fare for auditing, then adds `event_timestamp` at publication. The saved pipeline performs numerical handling, categorical encoding, vector assembly and scaling during inference. The model does not use the observed fare as a feature.

Historical pickup timestamps and publication timestamps remain separate. Pickup timestamps supply the historical temporal features; publication timestamps drive streaming windows and watermarking. The notebook uses `America/New_York` for timestamp display and feature extraction, while the producer preserves the stored pickup instant when serialising it as UTC.

The eligibility rules do not impose an upper fare limit. The largest-error final-test record remains in the official evaluation; its exclusion appears only in the separately labelled sensitivity analysis. That analysis does not alter either persisted subset or the streaming source.

## Files retained in Git

Track this README, `taxi_zone_lookup.csv` and `split_metadata.json`. The root `.gitignore` excludes source Parquet files, generated Parquet directories and incomplete downloads. Restore large datasets with these instructions and the notebook instead of committing them. The complete model under `models/a2_model/` remains tracked, including its internal Parquet files.