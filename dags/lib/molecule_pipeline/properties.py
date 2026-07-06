"""Molecular properties calculation step."""

from __future__ import annotations

import io
import logging
from typing import Any

import pandas as pd
from rdkit import Chem
from rdkit.Chem import Descriptors, rdMolDescriptors

from lib.molecule_pipeline.constants import (
    BRONZE_BUCKET,
    GENERATED_SMILES_COLUMN,
    PROPERTIES_FILE_TEMPLATE,
    S3_CONN_ID,
)
from lib.utils.s3 import download_object, object_exists, upload_bytes


def _parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value

    return str(value).strip().lower() == 'true'


def _read_generated_molecules_from_s3(key: str) -> pd.DataFrame:
    logging.info('Reading generated molecules from s3://%s/%s', BRONZE_BUCKET, key)

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

    required_columns = [
        'molecule_id',
        'dataset_id',
        'scaffold_smiles',
        'r_group_smiles',
        GENERATED_SMILES_COLUMN,
    ]

    missing_columns = [
        column for column in required_columns if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f'Generated molecules file is missing required columns: {missing_columns}. '
            f'Available columns: {list(df.columns)}'
        )

    if df.empty:
        raise ValueError('Generated molecules file is empty.')

    logging.info('Loaded %s generated molecules.', len(df))

    return df


def _calculate_properties(smiles: str) -> dict[str, Any]:
    molecule = Chem.MolFromSmiles(smiles)

    if molecule is None:
        raise ValueError(f'Invalid generated SMILES: {smiles}')

    mol_weight = Descriptors.MolWt(molecule)
    log_p = Descriptors.MolLogP(molecule)
    tpsa = Descriptors.TPSA(molecule)
    hba = rdMolDescriptors.CalcNumHBA(molecule)
    hbd = rdMolDescriptors.CalcNumHBD(molecule)
    rotatable_bonds = rdMolDescriptors.CalcNumRotatableBonds(molecule)
    aromatic_rings = rdMolDescriptors.CalcNumAromaticRings(molecule)

    lipinski_pass = all(
        (
            mol_weight <= 500,
            log_p <= 5,
            hba <= 10,
            hbd <= 5,
        )
    )

    return {
        'mol_weight': round(mol_weight, 4),
        'log_p': round(log_p, 4),
        'tpsa': round(tpsa, 4),
        'hba': hba,
        'hbd': hbd,
        'rotatable_bonds': rotatable_bonds,
        'aromatic_rings': aromatic_rings,
        'lipinski_pass': lipinski_pass,
    }


def _calculate_properties_for_single_dataset(
    dataset_metadata: dict[str, Any],
    overwrite: bool,
) -> dict[str, Any]:
    dataset_id = dataset_metadata['dataset_id']
    generated_key = dataset_metadata['generated_key']
    properties_key = PROPERTIES_FILE_TEMPLATE.format(dataset_id=dataset_id)

    logging.info('Starting molecular property calculation for dataset_id: %s', dataset_id)
    logging.info('Input file: s3://%s/%s', BRONZE_BUCKET, generated_key)
    logging.info('Output file: s3://%s/%s', BRONZE_BUCKET, properties_key)
    logging.info('Overwrite existing output: %s', overwrite)

    if object_exists(
        key=properties_key,
        bucket_name=BRONZE_BUCKET,
        aws_conn_id=S3_CONN_ID,
    ) and not overwrite:
        logging.info(
            'Molecular properties output already exists and overwrite=False. '
            'Skipping calculation for dataset_id=%s: s3://%s/%s',
            dataset_id,
            BRONZE_BUCKET,
            properties_key,
        )

        return {
            **dataset_metadata,
            'properties_key': properties_key,
            'properties_count': None,
            'properties_skipped': True,
        }

    if not object_exists(
        key=generated_key,
        bucket_name=BRONZE_BUCKET,
        aws_conn_id=S3_CONN_ID,
    ):
        raise FileNotFoundError(
            f'Generated molecules file does not exist for dataset_id={dataset_id}: '
            f's3://{BRONZE_BUCKET}/{generated_key}'
        )

    generated_df = _read_generated_molecules_from_s3(generated_key)

    rows = []
    failed_properties_count = 0

    for row in generated_df.to_dict(orient='records'):
        generated_smiles = str(row[GENERATED_SMILES_COLUMN]).strip()

        try:
            properties = _calculate_properties(generated_smiles)
        except Exception as exc:
            failed_properties_count += 1
            logging.warning(
                'Failed to calculate properties for dataset_id=%s, molecule_id=%s, smiles=%s. Error: %s',
                dataset_id,
                row.get('molecule_id'),
                generated_smiles,
                exc,
            )
            continue

        rows.append(
            {
                'molecule_id': row['molecule_id'],
                'dataset_id': row['dataset_id'],
                'scaffold_smiles': row['scaffold_smiles'],
                'r_group_smiles': row['r_group_smiles'],
                'generated_smiles': generated_smiles,
                **properties,
            }
        )

    if not rows:
        raise ValueError(
            f'No molecular properties were calculated for dataset_id={dataset_id}. '
            'Please check generated SMILES values.'
        )

    output_df = pd.DataFrame(rows)

    output_buffer = io.StringIO()
    output_df.to_csv(output_buffer, index=False)

    upload_bytes(
        data=output_buffer.getvalue().encode('utf-8'),
        key=properties_key,
        bucket_name=BRONZE_BUCKET,
        aws_conn_id=S3_CONN_ID,
        replace=True,
    )

    logging.info('Calculated properties for dataset_id=%s: %s molecules.', dataset_id, len(output_df))
    logging.info('Failed property calculations for dataset_id=%s: %s', dataset_id, failed_properties_count)
    logging.info('Uploaded molecular properties to s3://%s/%s', BRONZE_BUCKET, properties_key)

    return {
        **dataset_metadata,
        'properties_key': properties_key,
        'properties_count': len(output_df),
        'failed_properties_count': failed_properties_count,
        'properties_skipped': False,
    }


def calculate_properties(**context) -> dict[str, Any]:
    """
    Calculate molecular properties for generated molecules.

    This task supports one manually selected dataset or multiple automatically discovered datasets.
    It writes one output file per dataset and only returns metadata through XCom.
    """
    task_instance = context['ti']
    generation_metadata = task_instance.xcom_pull(task_ids='generate_molecules')

    if not generation_metadata:
        raise ValueError('Could not find metadata from generate_molecules task.')

    datasets = generation_metadata.get('datasets', [])

    if not datasets:
        logging.info('No datasets selected for molecular property calculation.')
        return {
            **generation_metadata,
            'datasets': [],
        }

    overwrite = _parse_bool(context['params'].get('overwrite', False))

    processed_datasets = [
        _calculate_properties_for_single_dataset(
            dataset_metadata=dataset_metadata,
            overwrite=overwrite,
        )
        for dataset_metadata in datasets
    ]

    return {
        **generation_metadata,
        'datasets': processed_datasets,
    }
