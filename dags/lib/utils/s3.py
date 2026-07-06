"""Generic S3 helpers for S3-compatible storage, including MinIO."""

from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def upload_bytes(data, key, bucket_name, aws_conn_id, replace=True):
    S3Hook(aws_conn_id=aws_conn_id).load_bytes(
        data,
        key=key,
        bucket_name=bucket_name,
        replace=replace,
    )


def download_object(key, bucket_name, aws_conn_id):
    return S3Hook(aws_conn_id=aws_conn_id).get_key(
        key,
        bucket_name=bucket_name,
    ).get()['Body'].read()


def object_exists(key, bucket_name, aws_conn_id):
    return S3Hook(aws_conn_id=aws_conn_id).check_for_key(
        key,
        bucket_name=bucket_name,
    )


def list_keys(prefix, bucket_name, aws_conn_id):
    return S3Hook(aws_conn_id=aws_conn_id).list_keys(
        bucket_name=bucket_name,
        prefix=prefix,
    ) or []
