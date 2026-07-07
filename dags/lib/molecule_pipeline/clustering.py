"""K-means clustering step."""

from __future__ import annotations

import io
import logging
from typing import Any

import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

from lib.molecule_pipeline.constants import (
    BRONZE_BUCKET,
    CLUSTERED_FILE_TEMPLATE,
    DEFAULT_N_CLUSTERS,
    FEATURE_COLUMNS,
    S3_CONN_ID,
)
from lib.utils.s3 import download_object, object_exists, upload_bytes


def _parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value

    return str(value).strip().lower() == 'true'


def _parse_positive_int(value: Any, default_value: int, parameter_name: str) -> int:
    parsed_value = int(value or default_value)

    if parsed_value < 1:
        raise ValueError(f'{parameter_name} must be greater than or equal to 1.')

    return parsed_value


def _read_properties_from_s3(key: str) -> pd.DataFrame:
    logging.info('Reading molecular properties from s3://%s/%s', BRONZE_BUCKET, key)

    file_bytes = download_object(
        key=key,
        bucket_name=BRONZE_BUCKET,
        aws_conn_id=S3_CONN_ID,
    )

    df = pd.read_csv(
        io.BytesIO(file_bytes),
        dtype={
            'molecule_id': str,
            'dataset_id': str,
            'scaffold_smiles': str,
            'r_group_smiles': str,
            'generated_smiles': str,
        },
    )

    df.columns = [column.strip().lower() for column in df.columns]

    required_columns = [
        'molecule_id',
        'dataset_id',
        'scaffold_smiles',
        'r_group_smiles',
        'generated_smiles',
        'lipinski_pass',
        *FEATURE_COLUMNS,
    ]

    missing_columns = [
        column for column in required_columns if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f'Molecular properties file is missing required columns: {missing_columns}. '
            f'Available columns: {list(df.columns)}'
        )

    if df.empty:
        raise ValueError('Molecular properties file is empty.')

    df = df.copy()

    for column in FEATURE_COLUMNS:
        df[column] = pd.to_numeric(df[column], errors='coerce')

    rows_with_missing_features = df[FEATURE_COLUMNS].isna().any(axis=1).sum()

    if rows_with_missing_features > 0:
        raise ValueError(
            f'Molecular properties file contains {rows_with_missing_features} rows '
            f'with missing or invalid clustering feature values.'
        )

    logging.info('Loaded %s molecules with molecular properties.', len(df))

    return df


def _get_actual_n_clusters(
    properties_df: pd.DataFrame,
    requested_n_clusters: int,
) -> int:
    distinct_feature_rows = properties_df[FEATURE_COLUMNS].drop_duplicates().shape[0]

    actual_n_clusters = min(
        requested_n_clusters,
        len(properties_df),
        distinct_feature_rows,
    )

    if actual_n_clusters < 1:
        raise ValueError('At least one valid feature row is required for clustering.')

    return actual_n_clusters


def _cluster_single_dataset(
    dataset_metadata: dict[str, Any],
    requested_n_clusters: int,
    overwrite: bool,
) -> dict[str, Any]:
    dataset_id = dataset_metadata['dataset_id']
    properties_key = dataset_metadata['properties_key']
    clustered_key = CLUSTERED_FILE_TEMPLATE.format(dataset_id=dataset_id)

    logging.info('Starting molecule clustering for dataset_id: %s', dataset_id)
    logging.info('Input file: s3://%s/%s', BRONZE_BUCKET, properties_key)
    logging.info('Output file: s3://%s/%s', BRONZE_BUCKET, clustered_key)
    logging.info('Requested clusters: %s', requested_n_clusters)
    logging.info('Overwrite existing output: %s', overwrite)

    if object_exists(
        key=clustered_key,
        bucket_name=BRONZE_BUCKET,
        aws_conn_id=S3_CONN_ID,
    ) and not overwrite:
        logging.info(
            'Clustered molecules output already exists and overwrite=False. '
            'Skipping clustering for dataset_id=%s: s3://%s/%s',
            dataset_id,
            BRONZE_BUCKET,
            clustered_key,
        )

        return {
            **dataset_metadata,
            'clustered_key': clustered_key,
            'clustered_count': None,
            'cluster_count': None,
            'clustering_skipped': True,
        }

    if not object_exists(
        key=properties_key,
        bucket_name=BRONZE_BUCKET,
        aws_conn_id=S3_CONN_ID,
    ):
        raise FileNotFoundError(
            f'Molecular properties file does not exist for dataset_id={dataset_id}: '
            f's3://{BRONZE_BUCKET}/{properties_key}'
        )

    properties_df = _read_properties_from_s3(properties_key)

    actual_n_clusters = _get_actual_n_clusters(
        properties_df=properties_df,
        requested_n_clusters=requested_n_clusters,
    )

    logging.info(
        'Actual clusters used for dataset_id=%s: %s',
        dataset_id,
        actual_n_clusters,
    )

    scaled_features = StandardScaler().fit_transform(properties_df[FEATURE_COLUMNS])

    model = KMeans(
        n_clusters=actual_n_clusters,
        random_state=42,
        n_init=10,
    )

    properties_df['cluster_id'] = model.fit_predict(scaled_features).astype(int)

    output_buffer = io.StringIO()
    properties_df.to_csv(output_buffer, index=False)

    upload_bytes(
        data=output_buffer.getvalue().encode('utf-8'),
        key=clustered_key,
        bucket_name=BRONZE_BUCKET,
        aws_conn_id=S3_CONN_ID,
        replace=True,
    )

    logging.info(
        'Clustered molecules for dataset_id=%s: %s',
        dataset_id,
        len(properties_df),
    )
    logging.info(
        'Uploaded clustered molecules to s3://%s/%s',
        BRONZE_BUCKET,
        clustered_key,
    )

    return {
        **dataset_metadata,
        'clustered_key': clustered_key,
        'clustered_count': len(properties_df),
        'cluster_count': actual_n_clusters,
        'clustering_skipped': False,
    }


def cluster_molecules(**context) -> dict[str, Any]:
    """
    Cluster molecules using K-means based on calculated molecular properties.

    This task supports one manually selected dataset or multiple automatically discovered datasets.
    It writes one output file per dataset and only returns metadata through XCom.
    """
    task_instance = context['ti']
    properties_metadata = task_instance.xcom_pull(task_ids='calculate_properties')

    if not properties_metadata:
        raise ValueError('Could not find metadata from calculate_properties task.')

    datasets = properties_metadata.get('datasets', [])

    if not datasets:
        logging.info('No datasets selected for clustering.')
        return {
            **properties_metadata,
            'datasets': [],
        }

    requested_n_clusters = _parse_positive_int(
        value=context['params'].get('n_clusters'),
        default_value=DEFAULT_N_CLUSTERS,
        parameter_name='n_clusters',
    )
    overwrite = _parse_bool(context['params'].get('overwrite', False))

    processed_datasets = [
        _cluster_single_dataset(
            dataset_metadata=dataset_metadata,
            requested_n_clusters=requested_n_clusters,
            overwrite=overwrite,
        )
        for dataset_metadata in datasets
    ]

    return {
        **properties_metadata,
        'datasets': processed_datasets,
    }
