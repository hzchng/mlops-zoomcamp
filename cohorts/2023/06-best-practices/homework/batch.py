#!/usr/bin/env python
# coding: utf-8

import sys
import pickle
import pandas as pd
import os

from datetime import datetime


def dt(hour, minute, second=0):
    return datetime(2022, 1, 1, hour, minute, second)


options = {
    'client_kwargs': {
        'endpoint_url': 'http://localhost:4566',
    }
}


def read_data(filename, categorical):
    if filename.startswith('s3'):
        df = pd.read_parquet(filename, storage_options=options)
    else:
        df = pd.read_parquet(filename)

    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')

    return df


def get_input_path(year, month):
    default_input_pattern = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet'
    input_pattern = os.getenv('INPUT_FILE_PATTERN', default_input_pattern)
    input_pattern = "s3://nyc-duration/in/{year:04d}-{month:02d}.parquet"
    # as I cannot configure the AWS CLI, I will use the local path
    input_pattern = "{year:04d}-{month:02d}.parquet"
    return input_pattern.format(year=year, month=month)


def get_output_path(year, month):
    default_output_pattern = 's3://nyc-duration-prediction-alexey/taxi_type=fhv/year={year:04d}/month={month:02d}/predictions.parquet'
    output_pattern = os.getenv('OUTPUT_FILE_PATTERN', default_output_pattern)
    output_pattern = "s3://nyc-duration/out/{year:04d}-{month:02d}.parquet"
    # as I cannot configure the AWS CLI, I will use the local path
    output_pattern = "{year:04d}-{month:02d}-out.parquet"
    return output_pattern.format(year=year, month=month)


def prepare_data(df, categorical):
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')

    return df


def main(year, month):

    # input_file = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet'
    # output_file = f'output/taxi_type=yellow_year={year:04d}_month={month:02d}.parquet'

    input_file = get_input_path(year, month)
    output_file = get_output_path(year, month)

    print(input_file)

    categorical = ['PULocationID', 'DOLocationID']


    with open('model.bin', 'rb') as f_in:
        dv, lr = pickle.load(f_in)

    df = read_data(input_file, categorical)
    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')


    dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(dicts)
    y_pred = lr.predict(X_val)


    print('predicted mean duration:', y_pred.mean())

    df_result = pd.DataFrame()
    df_result['ride_id'] = df['ride_id']
    df_result['predicted_duration'] = y_pred

    df_result.to_parquet(output_file, engine='pyarrow', index=False)


if __name__ == '__main__':
    main(int(sys.argv[1]), int(sys.argv[2]))