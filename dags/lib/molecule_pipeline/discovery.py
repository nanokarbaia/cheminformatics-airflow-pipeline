"""Dataset discovery and input validation."""

from __future__ import annotations

import io
import logging
import re
from typing import Any

import pandas as pd

from lib.molecule_pipeline.constants import (
    BRONZE_BUCKET,
    CLUSTERED_FILE_TEMPLATE,
    INPUT_PREFIX,
    R_GROUPS_FILE_TEMPLATE,
    S3_CONN_ID,
    SCAFFOLDS_FILE_TEMPLATE,
    SMILES_COLUMN,
)
from lib.utils.s3 import download_object, list_keys, object_exists


DATASET_FILE_PATTERN = re.compile(
    rf'^{INPUT_PREFIX}/(.+)_(scaffolds|r_groups)\.csv$'
)


def _parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value

    return str(value).strip().lower() == 'true'


def _build_dataset_metadata(dataset_id: str) -> dict[str, Any]:
    scaffolds_key = f'{INPUT_PREFIX}/{SCAFFOLDS_FILE_TEMPLATE.format(dataset_id=dataset_id)}'
    r_groups_key = f'{INPUT_PREFIX}/{R_GROUPS_FILE_TEMPLATE.format(dataset_id=dataset_id)}'

    return {
        'dataset_id': dataset_id,
        'bucket_name': BRONZE_BUCKET,
        'scaffolds_key': scaffolds_key,
        'r_groups_key': r_groups_key,
    }


def _discover_dataset_pairs(overwrite: bool) -> list[dict[str, Any]]:
    logging.info('Discovering dataset pairs in s3://%s/%s/', BRONZE_BUCKET, INPUT_PREFIX)

    keys = list_keys(
        prefix=f'{INPUT_PREFIX}/',
        bucket_name=BRONZE_BUCKET,
        aws_conn_id=S3_CONN_ID,
    )

    logging.info('Found %s objects under input prefix.', len(keys))

    datasets: dict[str, dict[str, str]] = {}

    for key in keys:
        match = DATASET_FILE_PATTERN.match(key)

        if not match:
            logging.info('Skipping non-matching input object: %s', key)
            continue

        dataset_id, file_type = match.groups()
        datasets.setdefault(dataset_id, {})[file_type] = key

    complete_datasets = []

    for dataset_id, files in sorted(datasets.items()):
        scaffolds_key = files.get('scaffolds')
        r_groups_key = files.get('r_groups')

        if not scaffolds_key or not r_groups_key:
            logging.warning(
                'Skipping dataset_id=%s because file pair is incomplete. Files: %s',
                dataset_id,
                files,
            )
            continue

        clustered_key = CLUSTERED_FILE_TEMPLATE.format(dataset_id=dataset_id)

        if (
            object_exists(
                key=clustered_key,
                bucket_name=BRONZE_BUCKET,
                aws_conn_id=S3_CONN_ID,
            )
            and not overwrite
        ):
            logging.info(
                'Skipping dataset_id=%s because clustered output already exists '
                'and overwrite=False: s3://%s/%s',
                dataset_id,
                BRONZE_BUCKET,
                clustered_key,
            )
            continue

        complete_datasets.append(
            {
                'dataset_id': dataset_id,
                'bucket_name': BRONZE_BUCKET,
                'scaffolds_key': scaffolds_key,
                'r_groups_key': r_groups_key,
            }
        )

    logging.info('Datasets selected for processing: %s', len(complete_datasets))

    return complete_datasets


def resolve_dataset(**context) -> dict[str, Any]:
    """
    Resolve datasets to process.

    If dataset_id is provided, process only that dataset.
    If dataset_id is not provided, discover all complete input file pairs in S3/MinIO.
    """
    dataset_id = context['params'].get('dataset_id')
    overwrite = _parse_bool(context['params'].get('overwrite', False))

    if dataset_id:
        dataset_id = str(dataset_id).strip()

    if dataset_id:
        dataset_metadata = _build_dataset_metadata(dataset_id)

        logging.info('Resolved manually provided dataset_id: %s', dataset_id)
        logging.info(
            'Expected scaffolds file: s3://%s/%s',
            BRONZE_BUCKET,
            dataset_metadata['scaffolds_key'],
        )
        logging.info(
            'Expected R-groups file: s3://%s/%s',
            BRONZE_BUCKET,
            dataset_metadata['r_groups_key'],
        )

        return {
            'datasets': [dataset_metadata],
            'discovery_mode': 'manual',
        }

    logging.info(
        'No dataset_id was provided. Switching to automatic dataset discovery.'
    )

    datasets = _discover_dataset_pairs(overwrite=overwrite)

    return {
        'datasets': datasets,
        'discovery_mode': 'automatic',
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

    logging.info(
        'Downloaded %s with %s rows and columns: %s',
        key,
        len(df),
        list(df.columns),
    )

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
    discovery_metadata = task_instance.xcom_pull(task_ids='resolve_dataset')

    if not discovery_metadata:
        raise ValueError('Could not find metadata from resolve_dataset task.')

    datasets = discovery_metadata.get('datasets', [])

    if not datasets:
        logging.info('No datasets selected for processing. Nothing to validate.')
        return {
            **discovery_metadata,
            'datasets': [],
        }

    validated_datasets = []

    for dataset_metadata in datasets:
        dataset_id = dataset_metadata['dataset_id']
        scaffolds_key = dataset_metadata['scaffolds_key']
        r_groups_key = dataset_metadata['r_groups_key']

        logging.info('Checking input files for dataset_id: %s', dataset_id)

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
                f'Missing required input file(s) in bucket "{BRONZE_BUCKET}" '
                f'for dataset_id={dataset_id}: {missing_files}'
            )

        logging.info('All required input files exist for dataset_id: %s', dataset_id)

        scaffolds_df = _read_csv_from_s3(scaffolds_key)
        r_groups_df = _read_csv_from_s3(r_groups_key)

        scaffolds_count = _validate_smiles_file(scaffolds_df, 'scaffolds file')
        r_groups_count = _validate_smiles_file(r_groups_df, 'r_groups file')

        logging.info(
            'Validated dataset_id=%s. Scaffolds rows: %s, R-groups rows: %s',
            dataset_id,
            scaffolds_count,
            r_groups_count,
        )

        validated_datasets.append(
            {
                **dataset_metadata,
                'scaffolds_count': scaffolds_count,
                'r_groups_count': r_groups_count,
            }
        )

    return {
        **discovery_metadata,
        'datasets': validated_datasets,
    }