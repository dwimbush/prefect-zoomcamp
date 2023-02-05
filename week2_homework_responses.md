# Week 2 Homework  
## Question 1: Load January 2020 data  
**Answer:** 447, 770  
**Code:**  

```Feb 1st, 2023
INFO
Created task run 'fetch-b4598a4a-0' for task 'fetch'
12:40:45 AM
INFO
Executing 'fetch-b4598a4a-0' immediately...
12:40:45 AM
INFO
Finished in state Completed()
12:40:50 AM
fetch-b4598a4a-0
INFO
Created task run 'clean-b9fd7e03-0' for task 'clean'
12:40:50 AM
INFO
Executing 'clean-b9fd7e03-0' immediately...
12:40:50 AM
INFO
   VendorID lpep_pickup_datetime lpep_dropoff_datetime store_and_fwd_flag  ...  total_amount  payment_type  trip_type  congestion_surcharge
0       2.0  2019-12-18 15:52:30   2019-12-18 15:54:39                  N  ...          4.81           1.0        1.0                   0.0
1       2.0  2020-01-01 00:45:58   2020-01-01 00:56:39                  N  ...         24.36           1.0        2.0                   0.0

[2 rows x 20 columns]
12:40:50 AM
clean-b9fd7e03-0
INFO
columns: VendorID                        float64
lpep_pickup_datetime     datetime64[ns]
lpep_dropoff_datetime    datetime64[ns]
store_and_fwd_flag               object
RatecodeID                      float64
PULocationID                      int64
DOLocationID                      int64
passenger_count                 float64
trip_distance                   float64
fare_amount                     float64
extra                           float64
mta_tax                         float64
tip_amount                      float64
tolls_amount                    float64
ehail_fee                       float64
improvement_surcharge           float64
total_amount                    float64
payment_type                    float64
trip_type                       float64
congestion_surcharge            float64
dtype: object
12:40:50 AM
clean-b9fd7e03-0
INFO
rows: 447770
12:40:50 AM
clean-b9fd7e03-0
INFO
Finished in state Completed()
12:40:50 AM
clean-b9fd7e03-0
INFO
Created task run 'write_local-f322d1be-0' for task 'write_local'
12:40:50 AM
INFO
Executing 'write_local-f322d1be-0' immediately...
12:40:50 AM
INFO
Finished in state Completed()
12:40:52 AM
write_local-f322d1be-0
INFO
Created task run 'write_gcs-1145c921-0' for task 'write_gcs'
12:40:52 AM
INFO
Executing 'write_gcs-1145c921-0' immediately...
12:40:52 AM
INFO
Getting bucket 'prefect-de_zoomcamp'.
12:40:52 AM
write_gcs-1145c921-0
INFO
Uploading from 'data\\green\\green_tripdata_2020-01.parquet' to the bucket 'prefect-de_zoomcamp' path 'data\\green\\green_tripdata_2020-01.parquet'.
12:40:52 AM
write_gcs-1145c921-0
INFO
Finished in state Completed()
12:40:59 AM
write_gcs-1145c921-0
INFO
Finished in state Completed('All states completed.')

Prompt:
dwimb@Laptop-Studio MINGW64 ~/Documents/Courses/data-engineering-zoomcamp-2023/data-engineering-zoomcamp/week_2_workflow_orchestration/prefect-zoomcamp/flows/02_gcp (main)
$ python etl_web_to_gcs_copy.py
00:40:45.163 | INFO    | prefect.engine - Created flow run 'beige-griffin' for flow 'etl-web-to-gcs'
00:40:45.496 | INFO    | Flow run 'beige-griffin' - Created task run 'fetch-b4598a4a-0' for task 'fetch'
00:40:45.501 | INFO    | Flow run 'beige-griffin' - Executing 'fetch-b4598a4a-0' immediately...
C:\Users\dwimb\Documents\Courses\data-engineering-zoomcamp-2023\data-engineering-zoomcamp\week_2_workflow_orchestration\prefect-zoomcamp\flows\02_gcp\etl_web_to_gcs_copy.py:13: DtypeWarning: Columns (3) have mixed types. Specify dtype option on import or set low_memory=False.
  df = pd.read_csv(dataset_url)
00:40:50.171 | INFO    | Task run 'fetch-b4598a4a-0' - Finished in state Completed()
00:40:50.236 | INFO    | Flow run 'beige-griffin' - Created task run 'clean-b9fd7e03-0' for task 'clean'
00:40:50.237 | INFO    | Flow run 'beige-griffin' - Executing 'clean-b9fd7e03-0' immediately...
00:40:50.727 | INFO    | Task run 'clean-b9fd7e03-0' -    VendorID lpep_pickup_datetime lpep_dropoff_datetime store_and_fwd_flag  ...  total_amount  payment_type  trip_type  congestion_surcharge
0       2.0  2019-12-18 15:52:30   2019-12-18 15:54:39                  N  ...          4.81           1.0        1.0                   0.0
1       2.0  2020-01-01 00:45:58   2020-01-01 00:56:39                  N  ...         24.36           1.0        2.0                   0.0

[2 rows x 20 columns]
00:40:50.731 | INFO    | Task run 'clean-b9fd7e03-0' - columns: VendorID                        float64
lpep_pickup_datetime     datetime64[ns]
lpep_dropoff_datetime    datetime64[ns]
store_and_fwd_flag               object
RatecodeID                      float64
PULocationID                      int64
DOLocationID                      int64
passenger_count                 float64
trip_distance                   float64
fare_amount                     float64
extra                           float64
mta_tax                         float64
tip_amount                      float64
tolls_amount                    float64
ehail_fee                       float64
improvement_surcharge           float64
total_amount                    float64
payment_type                    float64
trip_type                       float64
congestion_surcharge            float64
dtype: object
00:40:50.734 | INFO    | Task run 'clean-b9fd7e03-0' - rows: 447770
00:40:50.770 | INFO    | Task run 'clean-b9fd7e03-0' - Finished in state Completed()
00:40:50.806 | INFO    | Flow run 'beige-griffin' - Created task run 'write_local-f322d1be-0' for task 'write_local'
00:40:50.807 | INFO    | Flow run 'beige-griffin' - Executing 'write_local-f322d1be-0' immediately...
00:40:52.021 | INFO    | Task run 'write_local-f322d1be-0' - Finished in state Completed()
00:40:52.075 | INFO    | Flow run 'beige-griffin' - Created task run 'write_gcs-1145c921-0' for task 'write_gcs'
00:40:52.076 | INFO    | Flow run 'beige-griffin' - Executing 'write_gcs-1145c921-0' immediately...
00:40:52.218 | INFO    | Task run 'write_gcs-1145c921-0' - Getting bucket 'prefect-de_zoomcamp'.
00:40:52.648 | INFO    | Task run 'write_gcs-1145c921-0' - Uploading from 'data\\green\\green_tripdata_2020-01.parquet' to the bucket 'prefect-de_zoomcamp' path 'data\\green\\green_tripdata_2020-01.parquet'.
00:40:59.402 | INFO    | Task run 'write_gcs-1145c921-0' - Finished in state Completed()
00:40:59.473 | INFO    | Flow run 'beige-griffin' - Finished in state Completed('All states completed.')
(zoomcamp) 
dwimb@Laptop-Studio MINGW64 ~/Documents/Courses/data-engineering-zoomcamp-2023/data-engineering-zoomcamp/week_2_workflow_orchestration/prefect-zoomcamp/flows/02_gcp (main)
$
```

**Code:**  
```
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from pandas DataFrame"""
    # if randint(0,1 ) > 0:
    #     raise Exception

    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    """Fix dtyp issues"""
    # for yellow dataset
    # df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    # df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    # for green dataset
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Wtite DataFrame out locally as parquet file"""
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path
```
## Question 2: Scheduling with Cron  
**Answer:** 0 5 1 * *  
**Command:**  `prefect deployment build ./etl_web_to_gcs_HW2.py:etl_web_to_gcs -n "etl_web_to_gcs_cron_job" --cron "0 5 1 * *" -a`  
**Code:**  

```
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.storage import GitHub

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    # for green dataset
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task(log_prints=True)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path

@task(log_prints=True)
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=f"{path}", to_path=path)
    return

@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    color = "green"
    year = 2020
    month = 11
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)

    path = write_local(df_clean, color, dataset_file)
    
    write_gcs(path)


if __name__ == '__main__':
    etl_web_to_gcs()
```
  
## Question 3: Loading data to BigQuery
**Answer:** 14, 851, 920  
**Code:**  
```
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from prefect.tasks import task_input_hash
from datetime import timedelta

@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_from_gcs(month: int, year: int, color: str) -> pd.DataFrame:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    path = Path(f"../data/{gcs_path}")
    df = pd.read_parquet(path)
    return df

@task
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="tribal-incline-374717",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )

@flow()
def etl_gcs_to_bq(month, year, color):
    """Main ETL flow to load data into Big Query"""
    df = extract_from_gcs(month, year, color)
    write_bq(df)

@flow(log_prints=True)
def etl_parent_flow(months: list[int] = [1, 2], year: int = 2021, color: str ="yellow"):
    total_rows = 0
    for month in months:
        df = etl_gcs_to_bq(month, year, color)
        total_rows += len(df)
    print(f"Total rows processed: {total_rows}")

if __name__ == "__main__":
    months = [2, 3]
    year = 2019
    color = "yellow"

    etl_parent_flow(months, year, color)
```
## Question 4 Github Storage Block  
**Answer:** 88,605  
**Code:**  
```
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.filesystems import GitHub

github_block = GitHub.load("github-etl-web-to-gcs")

# flow.storage = GitHub(repo="dwimbush/prefect-zoomcamp", path="flows/02_gcp/etl_web_to_gcs_HW4.py")

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    # for green dataset
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task(log_prints=True)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path

@task(log_prints=True)
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=f"{path}", to_path=path)
    return

@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    color = "green"
    year = 2020
    month = 11
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)

    path = write_local(df_clean, color, dataset_file)
    
    write_gcs(path)

if __name__ == '__main__':
    etl_web_to_gcs()
```

## Question 5 Email or Slack notifications 
**Answer:** 514, 392  
**Code:**  
```
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.filesystems import GitHub

github_block = GitHub.load("github-etl-web-to-gcs")

# flow.storage = GitHub(repo="dwimbush/prefect-zoomcamp", path="flows/02_gcp/etl_web_to_gcs_HW4.py")

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    # for green dataset
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task(log_prints=True)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path

@task(log_prints=True)
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=f"{path}", to_path=path)
    return

@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    color = "green"
    year = 2019
    month = 4
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)

    path = write_local(df_clean, color, dataset_file)
    
    write_gcs(path)

if __name__ == '__main__':
    etl_web_to_gcs()
```
## Question 6 Secrets
**Answer:** 8
![image](https://user-images.githubusercontent.com/120685666/216809718-946bb1a3-d74a-42fe-87e2-697024fe0389.png)



