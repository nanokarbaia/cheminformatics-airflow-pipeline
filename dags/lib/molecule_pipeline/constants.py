"""Molecule pipeline configuration."""

# Airflow connections
S3_CONN_ID = 'aws_s3'

# Object storage
BRONZE_BUCKET = 'bronze'
INPUT_PREFIX = 'input'
OUTPUT_PREFIX = 'output'

SCAFFOLDS_FILE_TEMPLATE = '{dataset_id}_scaffolds.csv'
R_GROUPS_FILE_TEMPLATE = '{dataset_id}_r_groups.csv'

GENERATED_FILE_TEMPLATE = f'{OUTPUT_PREFIX}/{{dataset_id}}/generated_molecules.csv'
PROPERTIES_FILE_TEMPLATE = f'{OUTPUT_PREFIX}/{{dataset_id}}/molecular_properties.csv'
CLUSTERED_FILE_TEMPLATE = f'{OUTPUT_PREFIX}/{{dataset_id}}/clustered_molecules.csv'

# DAG defaults
DEFAULT_OVERWRITE = False
DEFAULT_MAX_MOLECULES = 50_000
DEFAULT_N_CLUSTERS = 5

# Clustering features
FEATURE_COLUMNS = [
    'mol_weight',
    'log_p',
    'tpsa',
    'hba',
    'hbd',
    'rotatable_bonds',
    'aromatic_rings',
]

# Input and output file columns
SMILES_COLUMN = 'smiles'
GENERATED_SMILES_COLUMN = 'generated_smiles'
