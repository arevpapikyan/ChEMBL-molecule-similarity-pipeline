CREATE OR REPLACE VIEW mart.v7a_avg_similarity_per_source AS
SELECT
    f.source_chembl_id,
    AVG(f.similarity_score) AS avg_similarity_score,
    COUNT(*) AS n_targets
FROM mart.fact_similarity f
GROUP BY f.source_chembl_id;

CREATE OR REPLACE VIEW mart.v7b_avg_alogp_deviation AS
SELECT
    f.source_chembl_id,
    AVG(ABS(dt.alogp - ds.alogp)) AS avg_abs_alogp_deviation,
    AVG(dt.alogp - ds.alogp) AS avg_signed_alogp_deviation
FROM mart.fact_similarity f
JOIN mart.dim_molecule ds ON ds.chembl_id = f.source_chembl_id
JOIN mart.dim_molecule dt ON dt.chembl_id = f.target_chembl_id
GROUP BY f.source_chembl_id;
