-- Bronze layer: the four required ChEMBL tables, landed from the
-- chembl_downloader SQLite dump with no business logic applied.

CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.chembl_id_lookup (
    chembl_id TEXT PRIMARY KEY,
    entity_type TEXT,
    entity_id BIGINT,
    status TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.molecule_dictionary (
    chembl_id TEXT PRIMARY KEY,
    molecule_type TEXT,
    pref_name TEXT,
    max_phase NUMERIC,
    therapeutic_flag BOOLEAN,
    withdrawn_flag BOOLEAN,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.compound_properties (
    chembl_id TEXT PRIMARY KEY,
    mw_freebase NUMERIC,
    alogp NUMERIC,
    psa NUMERIC,
    cx_logp NUMERIC,
    molecular_species TEXT,
    full_mwt NUMERIC,
    aromatic_rings INTEGER,
    heavy_atoms INTEGER,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.compound_structures (
    chembl_id TEXT PRIMARY KEY,
    canonical_smiles TEXT,
    standard_inchi TEXT,
    standard_inchi_key TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.ingestion_runs (
    run_id BIGSERIAL PRIMARY KEY,
    chembl_release TEXT NOT NULL,
    s3_bronze_prefix TEXT NOT NULL,
    row_counts JSONB,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'running'
);
