# Cheminformatics Airflow Pipeline

This project implements an Airflow pipeline for molecule generation, molecular property calculation, molecule clustering, and data quality validation.

Scientists upload paired scaffold and R-group CSV files to S3-compatible object storage. The Airflow DAG validates the input files, generates molecules with RDKit, calculates molecular descriptors, clusters the molecules with K-means, validates each output with Pandera, and writes one CSV output file per processing step.

## Architecture

```text
Input CSV files in MinIO/S3
        ↓
Airflow DAG
        ↓
Input validation
        ↓
Molecule generation with RDKit
        ↓
Pandera quality check
        ↓
Molecular property calculation
        ↓
Pandera quality check
        ↓
K-means clustering
        ↓
Pandera quality check
        ↓
Output CSV files in MinIO/S3
```

The project uses:

- Apache Airflow for orchestration
- MinIO as local S3-compatible object storage
- RDKit for molecule generation and molecular descriptors
- Pandas for CSV processing
- Scikit-learn for K-means clustering
- Pandera for data quality checks
- PostgreSQL as Airflow metadata database
- MS Teams webhook callback for task failure notifications

Application data is not stored in PostgreSQL. MinIO/S3 is the source of truth for input and output files.

## Repository structure

```text
cheminformatics-airflow-pipeline/
├── config/
│   └── .gitkeep
├── dags/
│   ├── .airflowignore
│   ├── molecule_pipeline_dag.py
│   └── lib/
│       ├── molecule_pipeline/
│       │   ├── clustering.py
│       │   ├── constants.py
│       │   ├── discovery.py
│       │   ├── generation.py
│       │   ├── properties.py
│       │   └── quality.py
│       └── utils/
│           ├── s3.py
│           └── teams.py
├── plugins/
│   └── .gitkeep
├── .dockerignore
├── .env.example
├── .gitignore
├── docker-compose.yml
├── Dockerfile
├── local-storage-init.Dockerfile
├── README.md
└── requirements.txt
```

## Input files

Input files must be uploaded to the `bronze` bucket under the `input/` prefix.

Each dataset must contain two files with the same dataset id:

```text
input/<dataset_id>_scaffolds.csv
input/<dataset_id>_r_groups.csv
```

Example:

```text
input/001_scaffolds.csv
input/001_r_groups.csv
```

Both files must contain a `smiles` column.

Example scaffold file:

```csv
smiles
c1cc([*])ccc1
C1CC([*])CC1
```

Example R-group file:

```csv
smiles
[*]C
[*]O
[*]N
```

The molecule generation logic expects exactly one dummy atom `[*]` in each scaffold and R-group SMILES value.

## Output files

For every processed dataset, the pipeline writes one output file per step:

```text
output/<dataset_id>/generated_molecules.csv
output/<dataset_id>/molecular_properties.csv
output/<dataset_id>/clustered_molecules.csv
```

Example:

```text
output/001/generated_molecules.csv
output/001/molecular_properties.csv
output/001/clustered_molecules.csv
```

## DAG

DAG id:

```text
molecule_pipeline_dag
```

Schedule:

```text
@weekly
```

The DAG supports both manual processing and automatic discovery.

## DAG parameters

| Parameter | Description | Default |
|---|---|---|
| `dataset_id` | Dataset id to process manually. If `null`, the DAG discovers complete unprocessed file pairs from MinIO/S3. | `null` |
| `overwrite` | If `true`, existing output files are overwritten. | `false` |
| `max_molecules` | Maximum number of molecules to generate per dataset. | `50000` |
| `n_clusters` | Requested number of K-means clusters. | `5` |

## Processing modes

### Manual mode

Use manual mode to process one specific dataset.

Example DAG run configuration:

```json
{
  "dataset_id": "001",
  "overwrite": true,
  "max_molecules": 50000,
  "n_clusters": 5
}
```

### Automatic discovery mode

Use automatic discovery mode to process all complete unprocessed input pairs.

Example DAG run configuration:

```json
{
  "dataset_id": null,
  "overwrite": false,
  "max_molecules": 50000,
  "n_clusters": 5
}
```

In automatic mode, the DAG scans the `input/` prefix, finds complete scaffold/R-group file pairs, and processes datasets that do not already have the final clustered output file.

A dataset is treated as already processed if this file exists:

```text
output/<dataset_id>/clustered_molecules.csv
```

If `overwrite` is set to `true`, existing outputs are regenerated.

## Data quality checks

Pandera checks are executed after every main processing step.

### Generated molecules check

The pipeline validates that:

- Required columns exist
- `molecule_id` is unique
- `dataset_id` is present and matches the expected dataset
- `generated_smiles` contains valid RDKit SMILES values

### Molecular properties check

The pipeline validates that:

- Required descriptor columns exist
- `molecule_id` is unique
- `dataset_id` matches the expected dataset
- `generated_smiles` remains valid
- `mol_weight` is greater than 0
- `tpsa` is non-negative
- HBA, HBD, rotatable bonds, and aromatic rings are non-negative
- `lipinski_pass` is boolean

### Clustered molecules check

The pipeline validates that:

- All molecular property checks pass
- `cluster_id` exists
- `cluster_id` is a non-negative integer

If a quality check fails, the Airflow task fails.

## Local setup

### 1. Create `.env`

Copy the example environment file:

```powershell
Copy-Item .env.example .env
```

The local MinIO connection is already defined in `.env.example`:

```env
AIRFLOW_CONN_AWS_S3=aws://minio_access_key:minio_secret_key@/?endpoint_url=http%3A%2F%2Fstorage%3A9000&region_name=us-east-1
```

For MS Teams notifications, replace the placeholder webhook URL in `.env` when an active webhook is available:

```env
AIRFLOW_CONN_MSTEAMS_WEBHOOK=https://ask-your-team-to-provide-you-with-this-url
```

Do not commit `.env`.

### 2. Start the project

```powershell
docker compose up --build -d
```

Check containers:

```powershell
docker compose ps -a
```

Expected running services:

```text
airflow-webserver
airflow-scheduler
airflow-dag-processor
airflow-triggerer
postgres
storage
```

The following setup containers should exit successfully with code `0`:

```text
airflow-init
storage-init
```

### 3. Open Airflow

Airflow UI:

```text
http://localhost:8081
```

The username is:

```text
admin
```

The password is generated by Airflow and can be found in the webserver logs:

```powershell
docker compose logs airflow-webserver --tail=200 | findstr /i "password"
```

Use the printed password to log in.

### 4. Open MinIO

MinIO UI:

```text
http://localhost:9091
```

Login:

```text
minio_access_key / minio_secret_key
```

The local bucket is created automatically:

```text
bronze
```

Upload input files under:

```text
bronze/input/
```

## Useful commands

Start containers:

```powershell
docker compose up -d
```

Start containers with rebuild:

```powershell
docker compose up --build -d
```

Stop containers:

```powershell
docker compose down
```

Check service status:

```powershell
docker compose ps -a
```

Check DAG list:

```powershell
docker compose exec airflow-webserver airflow dags list
```

Check Airflow webserver logs:

```powershell
docker compose logs airflow-webserver --tail=100
```

Check DAG processor logs:

```powershell
docker compose logs airflow-dag-processor --tail=100
```

Get Airflow login password:

```powershell
docker compose logs airflow-webserver --tail=200 | findstr /i "password"
```

## Important note about volumes

Do not run this command unless you intentionally want to delete local Airflow and MinIO data:

```powershell
docker compose down -v
```

The `-v` flag removes Docker volumes, including uploaded MinIO files and Airflow metadata.

## Failure notifications

The DAG uses an Airflow failure callback:

```text
send_teams_alert
```

When a task fails, the callback attempts to send a notification to MS Teams using the webhook configured in `.env`.

If the webhook URL is missing, deleted, or disabled, the task failure still appears in Airflow, but the Teams message will not be delivered.

## How to verify the pipeline

After the containers are running, confirm that Airflow can parse the DAG:

```powershell
docker compose exec airflow-webserver airflow dags list
```

The DAG list should contain:

```text
molecule_pipeline_dag
```

To test manual processing, trigger the DAG with one dataset id:

```json
{
  "dataset_id": "001",
  "overwrite": true,
  "max_molecules": 50000,
  "n_clusters": 5
}
```

To test automatic discovery, trigger the DAG with `dataset_id` set to `null`:

```json
{
  "dataset_id": null,
  "overwrite": false,
  "max_molecules": 50000,
  "n_clusters": 5
}
```

After a successful run, check that the following files were created in MinIO:

```text
bronze/output/<dataset_id>/generated_molecules.csv
bronze/output/<dataset_id>/molecular_properties.csv
bronze/output/<dataset_id>/clustered_molecules.csv
```

The DAG run is successful only if molecule generation, property calculation, clustering, and all Pandera quality checks pass.
