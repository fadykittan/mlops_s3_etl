import boto3 # Boto3 lib
import pandas as pd

AWS_ACCESS_KEY_ID = "KEY_ID"
AWS_SECRET_ACCESS_KEY = "ACCESS_KEY"
region = 'us-east-1'

def create_bucket(bucket_name):
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=region,
    )
    s3.create_bucket(Bucket=bucket_name)

def upload_file(bucket_name, file_path, key):
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    s3.upload_file(file_path, bucket_name, key)

def copy_file(bucket_name, file_path, target_bucket_name, target_file_path):
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    s3.copy_object(Bucket=target_bucket_name, Key=target_file_path, CopySource=f"{bucket_name}/{file_path}")

def read_file_as_pandas(bucket_name, file_path):
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    obj = s3.get_object(Bucket=bucket_name, Key=file_path)
    return pd.read_csv(obj['Body'])

def join_dataframes_by_key(df1, df2, key):
    return pd.merge(df1, df2, on=key)
