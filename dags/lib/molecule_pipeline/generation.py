"""Molecule generation step."""

from __future__ import annotations

import hashlib
import io
import itertools
import logging
from typing import Any

import pandas as pd
from rdkit import Chem

from lib.molecule_pipeline.constants import (
    BRONZE_BUCKET,
    DEFAULT_MAX_MOLECULES,
    GENERATED_FILE_TEMPLATE,
    S3_CONN_ID,
    SMILES_COLUMN,
)
from lib.utils.s3 import download_object, object_exists, upload_bytes


def _read_smiles_from_s3(key: str) -> list[str]:
    logging.info('Reading SMILES file from s3://%s/%s', BRONZE_BUCKET, key)

    file_bytes = download_object(
        key=key,
        bucket_name=BRONZE_BUCKET,
        aws_conn_id=S3_CONN_ID,
    )

    df = pd.read_csv(io.BytesIO(file_bytes))
    df.columns = [column.strip().lower() for column in df.columns]

    if SMILES_COLUMN not in df.columns:
        raise ValueError(
            f'File {key} must contain a "{SMILES_COLUMN}" column. '
            f'Available columns: {list(df.columns)}'
        )

    smiles_values = (
        df[SMILES_COLUMN]
        .dropna()
        .astype(str)
        .str.strip()
    )

    smiles_values = smiles_values[smiles_values != ''].tolist()

    if not smiles_values:
        raise ValueError(f'File {key} does not contain valid SMILES values.')

    logging.info('Loaded %s valid SMILES values from %s', len(smiles_values), key)

    return smiles_values


def _get_single_dummy_attachment(mol: Chem.Mol, smiles: str, molecule_type: str) -> tuple[int, int]:
    dummy_atoms = [
        atom.GetIdx()
        for atom in mol.GetAtoms()
        if atom.GetAtomicNum() == 0
    ]

    if len(dummy_atoms) != 1:
        raise ValueError(
            f'{molecule_type} SMILES must contain exactly one dummy atom [*]. '
            f'Found {len(dummy_atoms)} in: {smiles}'
        )

    dummy_idx = dummy_atoms[0]
    dummy_atom = mol.GetAtomWithIdx(dummy_idx)
    neighbors = list(dummy_atom.GetNeighbors())

    if len(neighbors) != 1:
        raise ValueError(
            f'Dummy atom [*] in {molecule_type} must have exactly one neighbor. '
            f'Found {len(neighbors)} in: {smiles}'
        )

    neighbor_idx = neighbors[0].GetIdx()

    return dummy_idx, neighbor_idx


def _generate_molecule_smiles(scaffold_smiles: str, r_group_smiles: str) -> str:
    scaffold_mol = Chem.MolFromSmiles(scaffold_smiles)
    r_group_mol = Chem.MolFromSmiles(r_group_smiles)

    if scaffold_mol is None:
        raise ValueError(f'Invalid scaffold SMILES: {scaffold_smiles}')

    if r_group_mol is None:
        raise ValueError(f'Invalid R-group SMILES: {r_group_smiles}')

    scaffold_dummy_idx, scaffold_neighbor_idx = _get_single_dummy_attachment(
        scaffold_mol,
        scaffold_smiles,
        'scaffold',
    )

    r_group_dummy_idx, r_group_neighbor_idx = _get_single_dummy_attachment(
        r_group_mol,
        r_group_smiles,
        'R-group',
    )

    combined_mol = Chem.CombineMols(scaffold_mol, r_group_mol)
    editable_mol = Chem.RWMol(combined_mol)

    r_group_offset = scaffold_mol.GetNumAtoms()

    editable_mol.AddBond(
        scaffold_neighbor_idx,
        r_group_offset + r_group_neighbor_idx,
        Chem.BondType.SINGLE,
    )

    dummy_atoms_to_remove = sorted(
        [scaffold_dummy_idx, r_group_offset + r_group_dummy_idx],
        reverse=True,
    )

    for atom_idx in dummy_atoms_to_remove:
        editable_mol.RemoveAtom(atom_idx)

    product_mol = editable_mol.GetMol()
    Chem.SanitizeMol(product_mol)

    return Chem.MolToSmiles(product_mol)


def _build_molecule_id(
    dataset_id: str,
    scaffold_smiles: str,
    r_group_smiles: str,
    row_number: int,
) -> str:
    hash_input = f'{dataset_id}|{row_number}|{scaffold_smiles}|{r_group_smiles}'
    hash_value = hashlib.sha1(hash_input.encode('utf-8')).hexdigest()[:16]

    return f'{dataset_id}_{hash_value}'


def generate_molecules(**context) -> dict[str, Any]:
    """
    Generate molecules from scaffold and R-group input files.

    This task writes one output file to S3/MinIO and only returns metadata through XCom.
    """
    task_instance = context['ti']
    dataset_metadata = task_instance.xcom_pull(task_ids='check_input_files')

    if not dataset_metadata:
        raise ValueError('Could not find dataset metadata from check_input_files task.')

    dataset_id = dataset_metadata['dataset_id']
    scaffolds_key = dataset_metadata['scaffolds_key']
    r_groups_key = dataset_metadata['r_groups_key']

    max_molecules = int(context['params'].get('max_molecules') or DEFAULT_MAX_MOLECULES)
    overwrite_param = context['params'].get('overwrite', False)
    overwrite = overwrite_param if isinstance(overwrite_param, bool) else str(overwrite_param).lower() == 'true'

    generated_key = GENERATED_FILE_TEMPLATE.format(dataset_id=dataset_id)

    logging.info('Starting molecule generation for dataset_id: %s', dataset_id)
    logging.info('Output file: s3://%s/%s', BRONZE_BUCKET, generated_key)
    logging.info('Max molecules: %s', max_molecules)
    logging.info('Overwrite existing output: %s', overwrite)

    if object_exists(
        key=generated_key,
        bucket_name=BRONZE_BUCKET,
        aws_conn_id=S3_CONN_ID,
    ) and not overwrite:
        logging.info(
            'Generated molecules output already exists and overwrite=False. '
            'Skipping generation: s3://%s/%s',
            BRONZE_BUCKET,
            generated_key,
        )

        return {
            **dataset_metadata,
            'generated_key': generated_key,
            'generated_count': None,
            'generation_skipped': True,
        }

    scaffold_smiles_values = _read_smiles_from_s3(scaffolds_key)
    r_group_smiles_values = _read_smiles_from_s3(r_groups_key)

    rows = []
    failed_combinations = 0

    for row_number, (scaffold_smiles, r_group_smiles) in enumerate(
        itertools.product(scaffold_smiles_values, r_group_smiles_values),
        start=1,
    ):
        if len(rows) >= max_molecules:
            logging.info('Reached max_molecules limit: %s', max_molecules)
            break

        try:
            generated_smiles = _generate_molecule_smiles(
                scaffold_smiles=scaffold_smiles,
                r_group_smiles=r_group_smiles,
            )
        except Exception as exc:
            failed_combinations += 1
            logging.warning(
                'Failed to generate molecule for scaffold=%s and r_group=%s. Error: %s',
                scaffold_smiles,
                r_group_smiles,
                exc,
            )
            continue

        rows.append(
            {
                'molecule_id': _build_molecule_id(
                    dataset_id=dataset_id,
                    scaffold_smiles=scaffold_smiles,
                    r_group_smiles=r_group_smiles,
                    row_number=row_number,
                ),
                'dataset_id': dataset_id,
                'scaffold_smiles': scaffold_smiles,
                'r_group_smiles': r_group_smiles,
                'generated_smiles': generated_smiles,
            }
        )

    if not rows:
        raise ValueError(
            'No molecules were generated. Please check scaffold and R-group SMILES values.'
        )

    output_df = pd.DataFrame(rows)

    output_buffer = io.StringIO()
    output_df.to_csv(output_buffer, index=False)

    upload_bytes(
        data=output_buffer.getvalue().encode('utf-8'),
        key=generated_key,
        bucket_name=BRONZE_BUCKET,
        aws_conn_id=S3_CONN_ID,
        replace=True,
    )

    logging.info('Generated molecules: %s', len(output_df))
    logging.info('Failed combinations: %s', failed_combinations)
    logging.info('Uploaded generated molecules to s3://%s/%s', BRONZE_BUCKET, generated_key)

    return {
        **dataset_metadata,
        'generated_key': generated_key,
        'generated_count': len(output_df),
        'failed_generation_count': failed_combinations,
        'generation_skipped': False,
    }
