from datetime import datetime
import pandas as pd
import os


def dt(hour, minute, second=0):
    return datetime(2022, 1, 1, hour, minute, second)


data = [
        (None, None, dt(1, 2), dt(1, 10)),
        (1, None, dt(1, 2), dt(1, 10)),
        (1, 2, dt(2, 2), dt(2, 3)),
        (None, 1, dt(1, 2, 0), dt(1, 2, 50)),
        (2, 3, dt(1, 2, 0), dt(1, 2, 59)),
        (3, 4, dt(1, 2, 0), dt(2, 2, 1)),
    ]



columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
df_input = pd.DataFrame(data, columns=columns)

options = {
    'client_kwargs': {
        'endpoint_url': 'http://localhost:4566'
    }
}


def get_input_path(year, month):
    default_input_pattern = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet'
    input_pattern = os.getenv('INPUT_FILE_PATTERN', default_input_pattern)
    input_pattern = "s3://nyc-duration/in/{year:04d}-{month:02d}.parquet"
    input_pattern = "{year:04d}-{month:02d}.parquet"
    return input_pattern.format(year=year, month=month)

input_file = get_input_path(2022, 2)


df_input.to_parquet(
    input_file,
    engine='pyarrow',
    compression=None,
    index=False,
    # storage_options=options
)