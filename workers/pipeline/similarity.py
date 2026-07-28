import logging

import numpy as np
import pyarrow as pa

from .config import Settings, get_settings
from .s3_utils import delete_keys, list_keys, read_parquet, write_parquet

logger = logging.getLogger(__name__)

TOPK_PREFIX = "silver/topk"

# Lookup table for counting set bits in packed fingerprint bytes.
_POPCOUNT = np.unpackbits(np.arange(256, dtype=np.uint8)[:, None], axis=1).sum(axis=1).astype(np.uint16)

# Molecules per scoring block to limit peak memory usage.
SIMILARITY_CHUNK_SIZE = 200_000


def _tail_mask(n_bits: int) -> np.ndarray | None:
    """
    Returns a mask for clearing padding bits in the final byte, if needed.

    Packed fingerprints may contain padding bits when n_bits is not divisible by 8.
    The mask keeps packed popcounting equivalent to unpacked fingerprints.
    """
    rem = n_bits % 8
    if rem == 0:
        return None
    n_bytes = (n_bits + 7) // 8
    mask = np.full(n_bytes, 0xFF, dtype=np.uint8)
    mask[-1] = (0xFF << (8 - rem)) & 0xFF
    return mask


def tanimoto_packed(
    query_packed: np.ndarray,
    others_packed: np.ndarray,
    n_bits: int,
    chunk: int = SIMILARITY_CHUNK_SIZE,
) -> np.ndarray:
    """Tanimoto of one query against many, computed on packed fingerprints."""
    mask = _tail_mask(n_bits)
    if mask is not None:
        query_packed = query_packed & mask

    n = others_packed.shape[0]
    out = np.empty(n, dtype=np.float64)

    for start in range(0, n, chunk):
        stop = min(start + chunk, n)
        block = others_packed[start:stop]
        if mask is not None:
            block = block & mask
        intersection = _POPCOUNT[block & query_packed].sum(axis=1, dtype=np.int64)
        union = _POPCOUNT[block | query_packed].sum(axis=1, dtype=np.int64)
        with np.errstate(divide="ignore", invalid="ignore"):
            out[start:stop] = np.where(union > 0, intersection / union, 0.0)
    return out


def _load_packed_matrix(settings: Settings):
    """Loads the fingerprint corpus once as a packed uint8 matrix."""
    fp_table = read_parquet(settings, f"{settings.silver_fingerprints_prefix}/fingerprints.parquet")
    chembl_ids = fp_table.column("chembl_id").to_pylist()
    fp_bytes = fp_table.column("fingerprint").to_pylist()
    n_bits = fp_table.column("n_bits")[0].as_py()

    n_bytes = (n_bits + 7) // 8
    packed = np.frombuffer(b"".join(fp_bytes), dtype=np.uint8).reshape(len(fp_bytes), n_bytes)
    return chembl_ids, packed, n_bits


def _score_one_source(source_chembl_id, chembl_ids, packed, n_bits, settings) -> str:
    try:
        source_idx = chembl_ids.index(source_chembl_id)
    except ValueError as exc:
        raise ValueError(f"{source_chembl_id} has no fingerprint in Silver") from exc

    query = packed[source_idx]
    scores = tanimoto_packed(query, packed, n_bits)

    keep = np.arange(len(chembl_ids)) != source_idx # exclude self-similarity
    result = pa.table({
        "source_chembl_id": [source_chembl_id] * int(keep.sum()),
        "target_chembl_id": [c for c, k in zip(chembl_ids, keep, strict=True) if k],
        "similarity_score": scores[keep],
    })

    s3_key = f"{settings.silver_similarity_prefix}/source_chembl_id={source_chembl_id}/full.parquet"
    uri = write_parquet(settings, result, s3_key)
    logger.info("Wrote full similarity table for %s to %s", source_chembl_id, uri)
    return uri


def compute_similarity_for_source(source_chembl_id: str, settings: Settings | None = None) -> str:
    """Full per-source similarity table -> Silver (single source)."""
    settings = settings or get_settings()
    chembl_ids, packed, n_bits = _load_packed_matrix(settings)
    return _score_one_source(source_chembl_id, chembl_ids, packed, n_bits, settings)


def _source_id_from_key(key: str) -> str | None:
    """Extracts CHEMBLxxx from a .../source_chembl_id=CHEMBLxxx/file.parquet key."""
    for part in key.split("/"):
        if part.startswith("source_chembl_id="):
            return part.split("=", 1)[1]
    return None


def _prune_orphaned_outputs(settings: Settings, prefix: str, keep: set[str]) -> int:
    """Deletes per-source outputs under prefix whose source is not in 'keep'."""
    orphans = []
    for key in list_keys(settings, prefix):
        source = _source_id_from_key(key)
        if source is not None and source not in keep:
            orphans.append(key)

    if not orphans:
        return 0

    orphan_sources = sorted({_source_id_from_key(k) for k in orphans})
    logger.info(
        "Pruning %d orphaned object(s) under %s from %d source(s) no longer selected: %s%s",
        len(orphans), prefix, len(orphan_sources), orphan_sources[:5],
        " ..." if len(orphan_sources) > 5 else "",
    )
    return delete_keys(settings, orphans)


def compute_similarity_for_all_sources(
    source_chembl_ids: list[str],
    settings: Settings | None = None,
    skip_existing: bool = True,
) -> list[str]:
    settings = settings or get_settings()

    # Drop outputs for sources that are no longer selected, so this prefix ends
    # up containing exactly the current source set.
    _prune_orphaned_outputs(
        settings, f"{settings.silver_similarity_prefix}/", set(source_chembl_ids)
    )

    already: set[str] = set()
    if skip_existing:
        for key in list_keys(settings, f"{settings.silver_similarity_prefix}/"):
            source = _source_id_from_key(key)
            if source is not None:
                already.add(source)

    todo = [s for s in source_chembl_ids if s not in already]
    logger.info(
        "compute_similarity: %d sources requested, %d already present, %d to compute",
        len(source_chembl_ids), len(source_chembl_ids) - len(todo), len(todo),
    )
    if not todo:
        return []

    chembl_ids, packed, n_bits = _load_packed_matrix(settings)
    logger.info(
        "Loaded fingerprint matrix once: %d molecules x %d bits (%.2f GB packed)",
        len(chembl_ids), n_bits, packed.nbytes / 1024**3,
    )

    uris: list[str] = []
    for i, source in enumerate(todo, start=1):
        uris.append(_score_one_source(source, chembl_ids, packed, n_bits, settings))
        logger.info("Scored source %d/%d (%s)", i, len(todo), source)
    return uris


def rank_top_k(
    scores: np.ndarray, targets: list[str], k: int = 10
) -> tuple[np.ndarray, list[str], list[bool]]:
    """
    Pure ranking logic: returns (sorted top-k scores,
    sorted top-k targets, has_duplicates_of_last_largest_score per row).
    """
    order = np.argsort(-scores, kind="stable")
    scores_sorted = scores[order]
    targets_sorted = [targets[i] for i in order]

    if len(scores_sorted) <= k:
        cutoff_score = scores_sorted[-1] if len(scores_sorted) else None
    else:
        cutoff_score = scores_sorted[k - 1]

    # Anyone at or above the cutoff score is a candidate; if that set is
    # larger than k, everyone sharing the cutoff score gets flagged.
    at_or_above = scores_sorted >= cutoff_score if cutoff_score is not None else np.array([], dtype=bool)
    candidate_idx = np.where(at_or_above)[0]
    has_extra_ties = len(candidate_idx) > k

    top_idx = list(range(min(k, len(scores_sorted))))
    flag = [bool(has_extra_ties and scores_sorted[i] == cutoff_score) for i in top_idx]

    return scores_sorted[top_idx], [targets_sorted[i] for i in top_idx], flag


def select_top_k(source_chembl_id: str, k: int = 10, settings: Settings | None = None) -> pa.Table:
    """
    Top-k neighbors with the has_duplicates_of_last_largest_score
    flag applied to every row sharing the score at the k-th boundary.
    """
    settings = settings or get_settings()

    s3_key = f"{settings.silver_similarity_prefix}/source_chembl_id={source_chembl_id}/full.parquet"
    full = read_parquet(settings, s3_key)

    scores = full.column("similarity_score").to_numpy()
    targets = full.column("target_chembl_id").to_pylist()

    top_scores, top_targets, flag = rank_top_k(scores, targets, k)
    top_idx = range(len(top_targets))

    result = pa.table({
        "source_chembl_id": [source_chembl_id] * len(top_targets),
        "target_chembl_id": top_targets,
        "similarity_score": [float(s) for s in top_scores],
        "rank_within_source": [i + 1 for i in top_idx],
        "has_duplicates_of_last_largest_score": flag,
    })

    s3_key = f"{settings.s3_prefix}/{TOPK_PREFIX}/source_chembl_id={source_chembl_id}/top10.parquet"
    write_parquet(settings, result, s3_key)
    logger.info("Wrote top-%s for %s to %s", k, source_chembl_id, s3_key)
    return result


def select_top_k_for_all_sources(
    source_chembl_ids: list[str], k: int = 10, settings: Settings | None = None
) -> int:
    """Runs select_top_k for many sources in one process."""
    settings = settings or get_settings()

    _prune_orphaned_outputs(
        settings, f"{settings.s3_prefix}/{TOPK_PREFIX}/", set(source_chembl_ids)
    )

    for i, source in enumerate(source_chembl_ids, start=1):
        select_top_k(source, k, settings)
        logger.info("Top-%s written %d/%d (%s)", k, i, len(source_chembl_ids), source)
    return len(source_chembl_ids)
