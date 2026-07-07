"""Generic S3 helpers for S3-compatible storage, including MinIO."""

from __future__ import annotations

from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def _get_s3_hook(aws_conn_id: str) -> S3Hook:
    return S3Hook(aws_conn_id=aws_conn_id)


def upload_bytes(
    data: bytes,
    key: str,
    bucket_name: str,
    aws_conn_id: str,
    replace: bool = True,
) -> None:
    """Upload bytes to S3/MinIO."""
    _get_s3_hook(aws_conn_id).load_bytes(
        data,
        key=key,
        bucket_name=bucket_name,
        replace=replace,
    )


def download_object(
    key: str,
    bucket_name: str,
    aws_conn_id: str,
) -> bytes:
    """Download an object from S3/MinIO as bytes."""
    s3_object = _get_s3_hook(aws_conn_id).get_key(
        key=key,
        bucket_name=bucket_name,
    )

    if s3_object is None:
        raise FileNotFoundError(f'S3 object not found: s3://{bucket_name}/{key}')

    return s3_object.get()['Body'].read()


def object_exists(
    key: str,
    bucket_name: str,
    aws_conn_id: str,
) -> bool:
    """Check whether an object exists in S3/MinIO."""
    return _get_s3_hook(aws_conn_id).check_for_key(
        key=key,
        bucket_name=bucket_name,
    )


def list_keys(
    prefix: str,
    bucket_name: str,
    aws_conn_id: str,
) -> list[str]:
    """List object keys under a prefix in S3/MinIO."""
    return _get_s3_hook(aws_conn_id).list_keys(
        bucket_name=bucket_name,
        prefix=prefix,
    ) or []
