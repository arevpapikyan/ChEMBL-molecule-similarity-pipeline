-- Gold layer: the data mart.
-- Only molecules referenced by fact_similarity
-- (as source or target) belong in dim_molecule.

CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS mart.dim_molecule (
    chembl_id TEXT PRIMARY KEY,
    molecule_type TEXT,
    mw_freebase NUMERIC,
    alogp NUMERIC,
    psa NUMERIC,
    cx_logp NUMERIC,
    molecular_species TEXT,
    full_mwt NUMERIC,
    aromatic_rings INTEGER,
    heavy_atoms INTEGER,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS mart.fact_similarity (
    fact_id BIGSERIAL PRIMARY KEY,
    source_chembl_id TEXT NOT NULL REFERENCES mart.dim_molecule (chembl_id),
    target_chembl_id TEXT NOT NULL REFERENCES mart.dim_molecule (chembl_id),
    similarity_score NUMERIC NOT NULL,
    rank_within_source SMALLINT NOT NULL,
    has_duplicates_of_last_largest_score BOOLEAN NOT NULL DEFAULT FALSE,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source_chembl_id, target_chembl_id)
);

CREATE INDEX IF NOT EXISTS ix_fact_similarity_source
    ON mart.fact_similarity (source_chembl_id, rank_within_source);
CREATE INDEX IF NOT EXISTS ix_fact_similarity_target
    ON mart.fact_similarity (target_chembl_id);
