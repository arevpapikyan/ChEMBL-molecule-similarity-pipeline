"""
Compute Morgan fingerprints (radius=2, nBits=2048) for every compound
structure and write them to Silver on S3 as a single Parquet file rather than
one file per molecule.
"""

import logging
import os
from functools import cache

import numpy as np
import pyarrow as pa
from rdkit import Chem
from rdkit.Chem import rdFingerprintGenerator

from .config import Settings, get_settings
from .db_utils import get_connection
from .s3_utils import object_exists, read_json, read_parquet, write_json, write_parquet

logger = logging.getLogger(__name__)


@cache
def _morgan_generator(radius: int, n_bits: int):
    """Cached Morgan generator."""
    return rdFingerprintGenerator.GetMorganGenerator(radius=radius, fpSize=n_bits)


def _smiles_to_fingerprint(smiles: str, radius: int, n_bits: int) -> bytes | None:
    mol = Chem.MolFromSmiles(smiles)
    if mol is None:
        return None
    fp = _morgan_generator(radius, n_bits).GetFingerprint(mol)
    arr = np.zeros((n_bits,), dtype=np.uint8)
    Chem.DataStructs.ConvertToNumpyArray(fp, arr)
    return np.packbits(arr).tobytes()


def _eligible_molecule_count(settings: Settings) -> int:
    """Cheap COUNT of the molecules fingerprinting would process.

    Used to detect that Bronze changed since the cached fingerprints were
    written, without downloading the ~350 MB fingerprint file.
    """
    with get_connection(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)
                FROM raw.compound_structures cs
                JOIN raw.molecule_dictionary md ON md.chembl_id = cs.chembl_id
                WHERE cs.canonical_smiles IS NOT NULL
                  AND cs.canonical_smiles <> ''
                """
            )
            return cur.fetchone()[0]


def _fingerprints_cache_is_valid(settings: Settings, s3_key: str, manifest_key: str) -> bool:
    """True if the cached fingerprint file can be reused as-is."""
    if os.environ.get("FINGERPRINTS_FORCE_RECOMPUTE", "").lower() == "true":
        logger.info("FINGERPRINTS_FORCE_RECOMPUTE set; recomputing fingerprints")
        return False

    if not object_exists(settings, s3_key):
        return False

    manifest = read_json(settings, manifest_key)
    if not manifest:
        logger.info("Fingerprints exist at %s but have no manifest; recomputing", s3_key)
        return False

    if (manifest.get("radius") != settings.morgan_radius
            or manifest.get("n_bits") != settings.morgan_n_bits):
        logger.info(
            "Cached fingerprints were built with radius=%s n_bits=%s but settings ask for "
            "radius=%s n_bits=%s; recomputing",
            manifest.get("radius"), manifest.get("n_bits"),
            settings.morgan_radius, settings.morgan_n_bits,
        )
        return False

    if manifest.get("sample_size") != settings.fingerprint_sample_size:
        logger.info(
            "Cached fingerprints were built with sample_size=%s but settings ask for "
            "sample_size=%s; recomputing",
            manifest.get("sample_size"), settings.fingerprint_sample_size,
        )
        return False

    current_count = _eligible_molecule_count(settings)
    if manifest.get("source_row_count") != current_count:
        logger.info(
            "Bronze has %s eligible molecules but cached fingerprints were built from %s; "
            "recomputing",
            current_count, manifest.get("source_row_count"),
        )
        return False

    logger.info(
        "Reusing cached fingerprints at %s (%s fingerprints from %s molecules, "
        "radius=%s n_bits=%s). Set FINGERPRINTS_FORCE_RECOMPUTE=1 to override.",
        s3_key, manifest.get("n_fingerprints"), current_count,
        manifest.get("radius"), manifest.get("n_bits"),
    )
    return True


def compute_fingerprints(settings: Settings | None = None) -> str:
    settings = settings or get_settings()

    s3_key = f"{settings.silver_fingerprints_prefix}/fingerprints.parquet"
    manifest_key = f"{settings.silver_fingerprints_prefix}/fingerprints.manifest.json"

    if _fingerprints_cache_is_valid(settings, s3_key, manifest_key):
        return f"s3://{settings.s3_bucket}/{s3_key}"

    # Recorded in the manifest regardless of sampling, so cache validity tracks
    # Bronze changes even when only a sample is materialised.
    full_eligible = _eligible_molecule_count(settings)

    base_query = """
        -- RDKit parses '' into a valid zero-atom molecule, which would
        -- yield an all-zero fingerprint counted as valid, so exclude it
        -- here rather than relying on IS NOT NULL alone.
        SELECT md.chembl_id, cs.canonical_smiles
        FROM raw.compound_structures cs
        JOIN raw.molecule_dictionary md ON md.chembl_id = cs.chembl_id
        WHERE cs.canonical_smiles IS NOT NULL
          AND cs.canonical_smiles <> ''
    """

    with get_connection(settings) as conn:
        with conn.cursor() as cur:
            if settings.fingerprint_sample_size is not None:
                logger.info(
                    "FINGERPRINT_SAMPLE_SIZE=%s set; fingerprinting a reproducible sample "
                    "of at most %s of %s eligible structures (seed=%s)",
                    settings.fingerprint_sample_size, settings.fingerprint_sample_size,
                    full_eligible, settings.random_seed,
                )
                # Deterministic hash-ordered sample: same seed + data -> same rows.
                cur.execute(
                    base_query + " ORDER BY md5(md.chembl_id || %s) LIMIT %s",
                    (str(settings.random_seed), settings.fingerprint_sample_size),
                )
            else:
                cur.execute(base_query)
            rows = cur.fetchall()

    chembl_ids: list[str] = []
    fingerprints: list[bytes] = []
    n_invalid = 0

    for chembl_id, smiles in rows:
        fp_bytes = _smiles_to_fingerprint(smiles, settings.morgan_radius, settings.morgan_n_bits)
        if fp_bytes is None:
            n_invalid += 1
            continue
        chembl_ids.append(chembl_id)
        fingerprints.append(fp_bytes)

    logger.info(
        "Computed %s fingerprints (%s SMILES failed to parse)",
        len(chembl_ids), n_invalid,
    )

    table = pa.table({
        "chembl_id": chembl_ids,
        "fingerprint": pa.array(fingerprints, type=pa.binary()),
        "radius": pa.array([settings.morgan_radius] * len(chembl_ids), type=pa.int8()),
        "n_bits": pa.array([settings.morgan_n_bits] * len(chembl_ids), type=pa.int32()),
    })

    uri = write_parquet(settings, table, s3_key)

    # Manifest is written only after the fingerprints themselves land, so an
    # interrupted run leaves no manifest and the next run recomputes rather
    # than trusting a partial file.
    write_json(
        settings,
        {
            "radius": settings.morgan_radius,
            "n_bits": settings.morgan_n_bits,
            "sample_size": settings.fingerprint_sample_size,
            "random_seed": settings.random_seed,
            "source_row_count": full_eligible,
            "n_selected_for_fingerprinting": len(rows),
            "n_fingerprints": len(chembl_ids),
            "n_invalid_smiles": n_invalid,
        },
        manifest_key,
    )

    logger.info("Wrote %s fingerprints to %s", len(chembl_ids), uri)
    return uri


def seed_fingerprint_manifest(settings: Settings | None = None, verify_file: bool = False) -> dict:
    """Writes a manifest for fingerprints that already exist in S3."""
    settings = settings or get_settings()
    s3_key = f"{settings.silver_fingerprints_prefix}/fingerprints.parquet"
    manifest_key = f"{settings.silver_fingerprints_prefix}/fingerprints.manifest.json"

    if not object_exists(settings, s3_key):
        raise FileNotFoundError(f"No fingerprints at s3://{settings.s3_bucket}/{s3_key}")

    source_row_count = _eligible_molecule_count(settings)
    n_fingerprints = None

    if verify_file:
        logger.info("Downloading fingerprints to verify them (this takes several minutes)...")
        table = read_parquet(settings, s3_key)
        n_fingerprints = table.num_rows
        file_radius = table.column("radius")[0].as_py()
        file_n_bits = table.column("n_bits")[0].as_py()

        if file_radius != settings.morgan_radius or file_n_bits != settings.morgan_n_bits:
            raise ValueError(
                f"Existing fingerprints use radius={file_radius} n_bits={file_n_bits}, but "
                f"settings ask for radius={settings.morgan_radius} "
                f"n_bits={settings.morgan_n_bits}. Not seeding a manifest; recompute instead."
            )
        if n_fingerprints > source_row_count:
            raise ValueError(
                f"Existing fingerprints have {n_fingerprints} rows but Bronze only has "
                f"{source_row_count} eligible molecules; the file does not match this data. "
                "Not seeding a manifest; recompute instead."
            )

    manifest = {
        "radius": settings.morgan_radius,
        "n_bits": settings.morgan_n_bits,
        "sample_size": settings.fingerprint_sample_size,
        "random_seed": settings.random_seed,
        "source_row_count": source_row_count,
        "n_fingerprints": n_fingerprints,
        "seeded": True,
        "file_verified": verify_file,
    }
    write_json(settings, manifest, manifest_key)
    logger.info(
        "Seeded fingerprint manifest at %s (file_verified=%s): %s",
        manifest_key, verify_file, manifest,
    )
    return manifest
