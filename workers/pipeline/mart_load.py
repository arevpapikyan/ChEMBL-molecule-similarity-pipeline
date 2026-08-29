import logging

import pyarrow as pa
from psycopg2 import sql

from .config import Settings, get_settings
from .db_utils import get_connection, upsert_rows
from .s3_utils import list_keys, read_parquet
from .similarity import TOPK_PREFIX

logger = logging.getLogger(__name__)


def load_dim_molecule(conn, chembl_ids: set[str]) -> int:
    """Loads one dim row per referenced molecule."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT md.chembl_id, md.molecule_type, cp.mw_freebase, cp.alogp, cp.psa,
                   cp.cx_logp, cp.molecular_species, cp.full_mwt, cp.aromatic_rings,
                   cp.heavy_atoms
            FROM raw.molecule_dictionary md
            LEFT JOIN raw.compound_properties cp ON cp.chembl_id = md.chembl_id
            WHERE md.chembl_id = ANY(%s)
            """,
            (list(chembl_ids),),
        )
        rows = cur.fetchall()

    columns = [
        "chembl_id", "molecule_type", "mw_freebase", "alogp", "psa",
        "cx_logp", "molecular_species", "full_mwt", "aromatic_rings", "heavy_atoms",
    ]
    return upsert_rows(conn, "mart.dim_molecule", columns, rows, conflict_cols=["chembl_id"])


def load_fact_similarity(conn, top_k_table: pa.Table) -> int:
    columns = [
        "source_chembl_id", "target_chembl_id", "similarity_score",
        "rank_within_source", "has_duplicates_of_last_largest_score",
    ]
    rows = list(zip(*[top_k_table.column(c).to_pylist() for c in columns], strict=True))
    return upsert_rows(
        conn, "mart.fact_similarity", columns, rows,
        conflict_cols=["source_chembl_id", "target_chembl_id"],
    )


def load_mart_batch(top_k_tables: list[pa.Table], settings: Settings | None = None) -> dict:
    """Called once with every source's top-k table, after the top-k fan-in."""
    settings = settings or get_settings()

    chembl_ids: set[str] = set()
    for t in top_k_tables:
        chembl_ids.update(t.column("source_chembl_id").to_pylist())
        chembl_ids.update(t.column("target_chembl_id").to_pylist())

    with get_connection(settings) as conn:
        n_dim = load_dim_molecule(conn, chembl_ids)
        n_fact = 0
        for t in top_k_tables:
            n_fact += load_fact_similarity(conn, t)

    logger.info("Loaded %s dim_molecule rows, %s fact_similarity rows", n_dim, n_fact)
    return {"dim_molecule_rows": n_dim, "fact_similarity_rows": n_fact}


def load_mart_from_s3(settings: Settings | None = None) -> dict:
    settings = settings or get_settings()
    prefix = f"{settings.s3_prefix}/{TOPK_PREFIX}/"
    keys = [k for k in list_keys(settings, prefix) if k.endswith("top10.parquet")]
    if not keys:
        raise ValueError(f"No top-10 files found under s3://{settings.s3_bucket}/{prefix}")

    tables = [read_parquet(settings, key) for key in keys]
    logger.info("Fan-in loading %s top-10 files into the mart", len(tables))
    return load_mart_batch(tables, settings)


def regenerate_pivot_view(settings: Settings | None = None) -> str:
    settings = settings or get_settings()

    with get_connection(settings) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT source_chembl_id FROM mart.fact_similarity")
            sources = [r[0] for r in cur.fetchall()]

        chosen = sorted(sources)[:10]

        with conn.cursor() as cur:
            cur.execute("TRUNCATE mart.pivot_source_selection")
            cur.executemany(
                "INSERT INTO mart.pivot_source_selection (chembl_id) VALUES (%s)",
                [(c,) for c in chosen],
            )

            col_defs = sql.SQL(",\n                    ").join(
                sql.SQL("{} NUMERIC").format(sql.Identifier(c)) for c in chosen
            )
            cur.execute("DROP VIEW IF EXISTS mart.v8a_similarity_pivot")
            cur.execute(sql.SQL("""
                CREATE VIEW mart.v8a_similarity_pivot AS
                SELECT * FROM crosstab(
                    $ct$SELECT target_chembl_id, source_chembl_id, similarity_score
                          FROM mart.fact_similarity
                         WHERE source_chembl_id IN (
                                   SELECT chembl_id FROM mart.pivot_source_selection)
                         ORDER BY 1, 2$ct$,
                    $ct$SELECT chembl_id FROM mart.pivot_source_selection
                         ORDER BY chembl_id$ct$
                ) AS pivot(
                    target_chembl_id TEXT,
                    {col_defs}
                );
            """).format(col_defs=col_defs))

    logger.info("Regenerated v8a_similarity_pivot for sources: %s", chosen)
    return ", ".join(chosen)
