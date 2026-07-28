CREATE EXTENSION IF NOT EXISTS tablefunc;

CREATE TABLE IF NOT EXISTS mart.pivot_source_selection (
    chembl_id TEXT PRIMARY KEY,
    selected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE OR REPLACE VIEW mart.v8b_next_and_second_target AS
SELECT
    f.source_chembl_id,
    f.target_chembl_id,
    f.similarity_score,
    f_next.target_chembl_id AS next_most_similar_target_chembl_id,
    f_second.target_chembl_id AS second_most_similar_target_chembl_id
FROM mart.fact_similarity f
LEFT JOIN mart.fact_similarity f_next
       ON f_next.source_chembl_id = f.source_chembl_id
      AND f_next.rank_within_source = f.rank_within_source + 1
LEFT JOIN mart.fact_similarity f_second
       ON f_second.source_chembl_id = f.source_chembl_id
      AND f_second.rank_within_source = 2;

CREATE OR REPLACE VIEW mart.v8c_avg_similarity_grouped AS
SELECT
    CASE WHEN GROUPING(ds.chembl_id) = 1 THEN 'TOTAL' ELSE ds.chembl_id END
        AS source_chembl_id,
    CASE WHEN GROUPING(ds.aromatic_rings) = 1 THEN 'TOTAL'
         ELSE ds.aromatic_rings::TEXT END AS source_aromatic_rings,
    CASE WHEN GROUPING(ds.heavy_atoms) = 1 THEN 'TOTAL'
         ELSE ds.heavy_atoms::TEXT END AS source_heavy_atoms,
    AVG(f.similarity_score) AS avg_similarity_score,
    COUNT(*) AS n_rows
FROM mart.fact_similarity f
JOIN mart.dim_molecule ds ON ds.chembl_id = f.source_chembl_id
GROUP BY GROUPING SETS (
    (ds.chembl_id), -- i. per source_molecule
    (ds.aromatic_rings, ds.heavy_atoms), -- ii. per (aromatic_rings, heavy_atoms)
    (ds.heavy_atoms), -- iii. per heavy_atoms
    () -- iv. whole dataset
);
