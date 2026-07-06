"""Pipeline data quality checks using Pandera."""

from __future__ import annotations

import io
import logging
from typing import Any

import pandas as pd
import pandera as pa
from pandera import Check, Column, DataFrameSchema
from rdkit import Chem

from lib.molecule_pipeline.constants import (
    BRONZE_BUCKET,
    GENERATED_SMILES_COLUMN,
    S3_CONN_ID,
)
from lib.utils.s3 import download_object


def _is_valid_smiles(smiles: str) -> bool:
    if not isinstance(smiles, str) or not smiles.strip():
        return False

    return Chem.MolFromSmiles(smiles.strip()) is not None


def _read_csv_from_s3(key: str) -> pd.DataFrame:
    logging.info('Reading file for quality checks from s3://%s/%s', BRONZE_BUCKET, key)

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
            GENERATED_SMILES_COLUMN: str,
        },
    )

    df.columns = [column.strip().lower() for column in df.columns]

    logging.info('Loaded %s rows and columns: %s', len(df), list(df.columns))

    return df


def _validate_schema(df: pd.DataFrame, schema: DataFrameSchema, dataset_id: str, check_name: str) -> None:
    if df.empty:
        raise ValueError(f'{check_name} failed for dataset_id={dataset_id}: file is empty.')

    try:
        schema.validate(df, lazy=True)
    except pa.errors.SchemaErrors as exc:
        logging.error(
            '%s failed for dataset_id=%s. Failure cases: %s',
            check_name,
            dataset_id,
            exc.failure_cases,
        )
        raise

    logging.info('%s passed for dataset_id=%s. Rows checked: %s', check_name, dataset_id, len(df))


GENERATED_MOLECULES_SCHEMA = DataFrameSchema(
    {
        'molecule_id': Column(str, nullable=False, unique=True),
        'dataset_id': Column(str, nullable=False),
        'scaffold_smiles': Column(str, nullable=False),
        'r_group_smiles': Column(str, nullable=False),
        GENERATED_SMILES_COLUMN: Column(
            str,
            nullable=False,
            checks=Check(
                lambda series: series.map(_is_valid_smiles),
                error='generated_smiles must contain valid RDKit SMILES values',
            ),
        ),
    },
    strict=False,
)


MOLECULAR_PROPERTIES_SCHEMA = DataFrameSchema(
    {
        'molecule_id': Column(str, nullable=False, unique=True),
        'dataset_id': Column(str, nullable=False),
        'scaffold_smiles': Column(str, nullable=False),
        'r_group_smiles': Column(str, nullable=False),
        GENERATED_SMILES_COLUMN: Column(str, nullable=False),
        'mol_weight': Column(float, nullable=False, checks=Check.gt(0), coerce=True),
        'log_p': Column(float, nullable=False, coerce=True),
        'tpsa': Column(float, nullable=False, checks=Check.ge(0), coerce=True),
        'hba': Column(int, nullable=False, checks=Check.ge(0), coerce=True),
        'hbd': Column(int, nullable=False, checks=Check.ge(0), coerce=True),
        'rotatable_bonds': Column(int, nullable=False, checks=Check.ge(0), coerce=True),
        'aromatic_rings': Column(int, nullable=False, checks=Check.ge(0), coerce=True),
        'lipinski_pass': Column(bool, nullable=False, coerce=True),
    },
    strict=False,
)


CLUSTERED_MOLECULES_SCHEMA = DataFrameSchema(
    {
        'molecule_id': Column(str, nullable=False, unique=True),
        'dataset_id': Column(str, nullable=False),
        'scaffold_smiles': Column(str, nullable=False),
        'r_group_smiles': Column(str, nullable=False),
        GENERATED_SMILES_COLUMN: Column(str, nullable=False),
        'mol_weight': Column(float, nullable=False, checks=Check.gt(0), coerce=True),
        'log_p': Column(float, nullable=False, coerce=True),
        'tpsa': Column(float, nullable=False, checks=Check.ge(0), coerce=True),
        'hba': Column(int, nullable=False, checks=Check.ge(0), coerce=True),
        'hbd': Column(int, nullable=False, checks=Check.ge(0), coerce=True),
        'rotatable_bonds': Column(int, nullable=False, checks=Check.ge(0), coerce=True),
        'aromatic_rings': Column(int, nullable=False, checks=Check.ge(0), coerce=True),
        'lipinski_pass': Column(bool, nullable=False, coerce=True),
        'cluster_id': Column(int, nullable=False, checks=Check.ge(0), coerce=True),
    },
    strict=False,
)


def _get_datasets_from_xcom(context: dict[str, Any], task_id: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    task_instance = context['ti']
    metadata = task_instance.xcom_pull(task_ids=task_id)

    if not metadata:
        raise ValueError(f'Could not find metadata from {task_id} task.')

    datasets = metadata.get('datasets', [])

    return metadata, datasets


def check_generated_molecules(**context) -> dict[str, Any]:
    """
    Validate generated_molecules.csv for every selected dataset.
    """
    metadata, datasets = _get_datasets_from_xcom(context, task_id='generate_molecules')

    if not datasets:
        logging.info('No datasets selected for generated molecule quality checks.')
        return {
            **metadata,
            'datasets': [],
        }

    checked_datasets = []

    for dataset_metadata in datasets:
        dataset_id = dataset_metadata['dataset_id']
        generated_key = dataset_metadata['generated_key']

        df = _read_csv_from_s3(generated_key)

        _validate_schema(
            df=df,
            schema=GENERATED_MOLECULES_SCHEMA,
            dataset_id=dataset_id,
            check_name='Generated molecules quality check',
        )

        checked_datasets.append(
            {
                **dataset_metadata,
                'generated_quality_checked': True,
            }
        )

    return {
        **metadata,
        'datasets': checked_datasets,
    }


def check_molecular_properties(**context) -> dict[str, Any]:
    """
    Validate molecular_properties.csv for every selected dataset.
    """
    metadata, datasets = _get_datasets_from_xcom(context, task_id='calculate_properties')

    if not datasets:
        logging.info('No datasets selected for molecular property quality checks.')
        return {
            **metadata,
            'datasets': [],
        }

    checked_datasets = []

    for dataset_metadata in datasets:
        dataset_id = dataset_metadata['dataset_id']
        properties_key = dataset_metadata['properties_key']

        df = _read_csv_from_s3(properties_key)

        _validate_schema(
            df=df,
            schema=MOLECULAR_PROPERTIES_SCHEMA,
            dataset_id=dataset_id,
            check_name='Molecular properties quality check',
        )

        checked_datasets.append(
            {
                **dataset_metadata,
                'properties_quality_checked': True,
            }
        )

    return {
        **metadata,
        'datasets': checked_datasets,
    }


def check_clustered_molecules(**context) -> dict[str, Any]:
    """
    Validate clustered_molecules.csv for every selected dataset.
    """
    metadata, datasets = _get_datasets_from_xcom(context, task_id='cluster_molecules')

    if not datasets:
        logging.info('No datasets selected for clustered molecule quality checks.')
        return {
            **metadata,
            'datasets': [],
        }

    checked_datasets = []

    for dataset_metadata in datasets:
        dataset_id = dataset_metadata['dataset_id']
        clustered_key = dataset_metadata['clustered_key']

        df = _read_csv_from_s3(clustered_key)

        _validate_schema(
            df=df,
            schema=CLUSTERED_MOLECULES_SCHEMA,
            dataset_id=dataset_id,
            check_name='Clustered molecules quality check',
        )

        checked_datasets.append(
            {
                **dataset_metadata,
                'clustered_quality_checked': True,
            }
        )

    return {
        **metadata,
        'datasets': checked_datasets,
    }
