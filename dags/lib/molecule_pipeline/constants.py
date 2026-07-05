"""Molecule pipeline configuration."""

# Airflow connections
S3_CONN_ID = 'aws_s3'
DWH_CONN_ID = 'dwh_connection'

# Object storage
BRONZE_BUCKET = 'bronze'
INPUT_PREFIX = 'input'
OUTPUT_PREFIX = 'output'

SCAFFOLDS_FILE_TEMPLATE = '{dataset_id}_scaffolds.csv'
R_GROUPS_FILE_TEMPLATE = '{dataset_id}_r_groups.csv'

GENERATED_FILE_TEMPLATE = 'output/{dataset_id}/generated_molecules.csv'
PROPERTIES_FILE_TEMPLATE = 'output/{dataset_id}/molecular_properties.csv'
CLUSTERED_FILE_TEMPLATE = 'output/{dataset_id}/clustered_molecules.csv'

# Database
PIPELINE_SCHEMA = 'molecule_pipeline'
DATASET_RUNS_TABLE = 'dataset_runs'
GENERATED_TABLE = 'generated_molecules'
PROPERTIES_TABLE = 'molecular_properties'
CLUSTERED_TABLE = 'clustered_molecules'

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

# Soda checks
MOLECULE_DATA_SOURCE = 'molecule_pipeline'
GENERATED_CHECKS_FILE = 'molecule_pipeline/checks_generated.yml'
PROPERTIES_CHECKS_FILE = 'molecule_pipeline/checks_properties.yml'
CLUSTERED_CHECKS_FILE = 'molecule_pipeline/checks_clustered.yml'
