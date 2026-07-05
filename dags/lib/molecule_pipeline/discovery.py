"""Dataset discovery and input validation."""

from __future__ import annotations

import io
import logging
from typing import Any

import pandas as pd

from lib.molecule_pipeline.constants import (
    BRONZE_BUCKET,
    INPUT_PREFIX,
    R_GROUPS_FILE_TEMPLATE,
    S3_CONN_ID,
    SCAFFOLDS_FILE_TEMPLATE,
    SMILES_COLUMN,
)
from lib.utils.s3 import download_object, object_exists


def resolve_dataset(**context) -> dict[str, Any]:
    """
    Resolve dataset_id from DAG params and build expected input file keys.

    Expected files:
        input/<dataset_id>_scaffolds.csv
        input/<dataset_id>_r_groups.csv
    """
    dataset_id = context['params'].get('dataset_id')

    if not dataset_id:
        raise ValueError(
            'dataset_id is required. Please trigger the DAG with a dataset_id parameter.'
        )

    dataset_id = str(dataset_id).strip()

    if not dataset_id:
        raise ValueError('dataset_id cannot be empty.')

    scaffolds_key = f'{INPUT_PREFIX}/{SCAFFOLDS_FILE_TEMPLATE.format(dataset_id=dataset_id)}'
    r_groups_key = f'{INPUT_PREFIX}/{R_GROUPS_FILE_TEMPLATE.format(dataset_id=dataset_id)}'

    logging.info('Resolved dataset_id: %s', dataset_id)
    logging.info('Expected scaffolds file: s3://%s/%s', BRONZE_BUCKET, scaffolds_key)
    logging.info('Expected R-groups file: s3://%s/%s', BRONZE_BUCKET, r_groups_key)

    return {
        'dataset_id': dataset_id,
        'bucket_name': BRONZE_BUCKET,
        'scaffolds_key': scaffolds_key,
        'r_groups_key': r_groups_key,
    }


def _read_csv_from_s3(key: str) -> pd.DataFrame:
    logging.info('Downloading file from s3://%s/%s', BRONZE_BUCKET, key)

    file_bytes = download_object(
        key=key,
        bucket_name=BRONZE_BUCKET,
        aws_conn_id=S3_CONN_ID,
    )

    df = pd.read_csv(io.BytesIO(file_bytes))
    df.columns = [column.strip().lower() for column in df.columns]

    logging.info('Downloaded %s with %s rows and columns: %s', key, len(df), list(df.columns))

    return df


def _validate_smiles_file(df: pd.DataFrame, file_name: str) -> int:
    if df.empty:
        raise ValueError(f'{file_name} is empty.')

    if SMILES_COLUMN not in df.columns:
        raise ValueError(
            f'{file_name} must contain a "{SMILES_COLUMN}" column. '
            f'Available columns: {list(df.columns)}'
        )

    smiles_series = df[SMILES_COLUMN].dropna().astype(str).str.strip()
    valid_rows = smiles_series[smiles_series != '']

    if valid_rows.empty:
        raise ValueError(f'{file_name} does not contain valid SMILES values.')

    return len(valid_rows)


def check_input_files(**context) -> dict[str, Any]:
    """
    Check that required input files exist and have valid structure.

    This task only returns small metadata through XCom.
    Full CSV content is not passed through XCom.
    """
    task_instance = context['ti']
    dataset_metadata = task_instance.xcom_pull(task_ids='resolve_dataset')

    if not dataset_metadata:
        raise ValueError('Could not find dataset metadata from resolve_dataset task.')

    scaffolds_key = dataset_metadata['scaffolds_key']
    r_groups_key = dataset_metadata['r_groups_key']

    logging.info('Checking input files for dataset_id: %s', dataset_metadata['dataset_id'])

    missing_files = []

    for key in [scaffolds_key, r_groups_key]:
        logging.info('Checking if file exists: s3://%s/%s', BRONZE_BUCKET, key)

        if not object_exists(
            key=key,
            bucket_name=BRONZE_BUCKET,
            aws_conn_id=S3_CONN_ID,
        ):
            missing_files.append(key)

    if missing_files:
        raise FileNotFoundError(
            f'Missing required input file(s) in bucket "{BRONZE_BUCKET}": {missing_files}'
        )

    logging.info('All required input files exist.')

    scaffolds_df = _read_csv_from_s3(scaffolds_key)
    r_groups_df = _read_csv_from_s3(r_groups_key)

    scaffolds_count = _validate_smiles_file(scaffolds_df, 'scaffolds file')
    r_groups_count = _validate_smiles_file(r_groups_df, 'r_groups file')

    logging.info('Validated scaffolds file. Valid rows: %s', scaffolds_count)
    logging.info('Validated R-groups file. Valid rows: %s', r_groups_count)

    return {
        **dataset_metadata,
        'scaffolds_count': scaffolds_count,
        'r_groups_count': r_groups_count,
    }
