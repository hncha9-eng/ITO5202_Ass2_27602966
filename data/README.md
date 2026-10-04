# Dataset Instructions

## Dataset reuse

Assessment 2 uses the same approved dataset as Assessment 1: the twelve monthly 2023 New York City Yellow Taxi Parquet files published by the NYC Taxi and Limousine Commission, together with the Taxi Zone Lookup CSV.

Reuse the existing Assessment 1 files when setting up Assessment 2. All thirteen expected filenames were confirmed as visible inside the Assessment 2 Docker container during setup.

The Parquet source files are kept locally because of their size. These instructions allow a reviewer to restore the inputs.

## Required files

| Month | Filename and official download |
| --- | --- |
| January 2023 | [yellow_tripdata_2023-01.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet) |
| February 2023 | [yellow_tripdata_2023-02.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-02.parquet) |
| March 2023 | [yellow_tripdata_2023-03.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-03.parquet) |
| April 2023 | [yellow_tripdata_2023-04.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-04.parquet) |
| May 2023 | [yellow_tripdata_2023-05.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-05.parquet) |
| June 2023 | [yellow_tripdata_2023-06.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-06.parquet) |
| July 2023 | [yellow_tripdata_2023-07.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-07.parquet) |
| August 2023 | [yellow_tripdata_2023-08.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-08.parquet) |
| September 2023 | [yellow_tripdata_2023-09.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-09.parquet) |
| October 2023 | [yellow_tripdata_2023-10.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-10.parquet) |
| November 2023 | [yellow_tripdata_2023-11.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-11.parquet) |
| December 2023 | [yellow_tripdata_2023-12.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-12.parquet) |
| Taxi zones | [taxi_zone_lookup.csv](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv) |

The lookup table associates pickup and dropoff location IDs with borough and zone information.

The monthly links are also available on the [official NYC TLC Trip Record Data page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). Select **2023 → Yellow Taxi Trip Records** for January through December.

## File placement

Place all thirteen source files directly inside the **data** folder in your local repository:

```text
data/
```

The repository can be stored in a folder of your choice. Follow the container creation commands in the [main README](../README.md) from the repository root to mount that checkout.

The Docker bind mount exposes its data folder at:

```text
/home/student/work/data
```

## Verify file access

Run the following in a Jupyter notebook inside the PySpark container:

```python
from pathlib import Path

data_dir = Path("/home/student/work/data")
required_files = [
    f"yellow_tripdata_2023-{month:02d}.parquet"
    for month in range(1, 13)
] + ["taxi_zone_lookup.csv"]

missing_files = [
    name for name in required_files
    if not (data_dir / name).is_file()
]

for name in required_files:
    status = "FOUND" if (data_dir / name).is_file() else "MISSING"
    print(f"{status}: {name}")

assert not missing_files, f"Missing source files: {missing_files}"
```

All thirteen filenames should be reported as **FOUND**. This checks file visibility; data schemas and contents are validated during assessment development.
