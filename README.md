# Cheminformatics Airflow Pipeline

Airflow pipeline for generating molecules from scaffolds and R-groups, calculating molecular properties and clustering molecules with K-means.

## Pipeline steps

1. Read scaffold and R-group input files from MinIO/S3.
2. Generate molecules with RDKit.
3. Calculate molecular properties.
4. Cluster molecules using K-means.
5. Run quality checks.
6. Send MS Teams alerts on failures.

## Local services

- Airflow UI: http://localhost:8080
- MinIO UI: http://localhost:9090
- Local DWH Postgres: localhost:5434

## Input files

Files should be uploaded to the `bronze` bucket:

```bash
input/<dataset_id>_scaffolds.csv
input/<dataset_id>_r_groups.csv 
```

Both files should contain a `smiles` column.

## Output files

```bash
output/<dataset_id>/generated_molecules.csv
output/<dataset_id>/molecular_properties.csv
output/<dataset_id>/clustered_molecules.csv
```
