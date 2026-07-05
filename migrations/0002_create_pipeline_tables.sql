-- depends: 0001_create_schemas

CREATE TABLE IF NOT EXISTS molecule_pipeline.dataset_runs (
    dataset_id VARCHAR(255) PRIMARY KEY,
    scaffold_file_key TEXT NOT NULL,
    r_group_file_key TEXT NOT NULL,
    generated_file_key TEXT,
    properties_file_key TEXT,
    clustered_file_key TEXT,
    status VARCHAR(50) NOT NULL,
    error_message TEXT,
    overwrite BOOLEAN NOT NULL DEFAULT FALSE,
    generated_count INT,
    calculated_count INT,
    clustered_count INT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    processed_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS molecule_pipeline.generated_molecules (
    molecule_id VARCHAR(255) PRIMARY KEY,
    dataset_id VARCHAR(255) NOT NULL,
    scaffold_smiles TEXT NOT NULL,
    r_group_smiles TEXT NOT NULL,
    generated_smiles TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS molecule_pipeline.molecular_properties (
    molecule_id VARCHAR(255) PRIMARY KEY,
    dataset_id VARCHAR(255) NOT NULL,
    scaffold_smiles TEXT NOT NULL,
    r_group_smiles TEXT NOT NULL,
    generated_smiles TEXT NOT NULL,
    mol_weight DOUBLE PRECISION,
    log_p DOUBLE PRECISION,
    tpsa DOUBLE PRECISION,
    hba INT,
    hbd INT,
    rotatable_bonds INT,
    aromatic_rings INT,
    lipinski_pass BOOLEAN,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS molecule_pipeline.clustered_molecules (
    molecule_id VARCHAR(255) PRIMARY KEY,
    dataset_id VARCHAR(255) NOT NULL,
    scaffold_smiles TEXT NOT NULL,
    r_group_smiles TEXT NOT NULL,
    generated_smiles TEXT NOT NULL,
    mol_weight DOUBLE PRECISION,
    log_p DOUBLE PRECISION,
    tpsa DOUBLE PRECISION,
    hba INT,
    hbd INT,
    rotatable_bonds INT,
    aromatic_rings INT,
    lipinski_pass BOOLEAN,
    cluster_id INT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
