import argparse
import json
import logging

from pipeline.config import get_settings
from pipeline.fingerprints import compute_fingerprints, seed_fingerprint_manifest
from pipeline.ingest import ingest_bronze
from pipeline.mart_load import load_mart_from_s3, regenerate_pivot_view
from pipeline.similarity import (
    compute_similarity_for_all_sources,
    compute_similarity_for_source,
    select_top_k,
    select_top_k_for_all_sources,
)


def _read_source_ids(args) -> list[str]:
    if getattr(args, "source_ids_file", None):
        with open(args.source_ids_file) as fh:
            return json.load(fh)
    return json.loads(args.source_ids)


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("ingest")
    sub.add_parser("fingerprints")

    sim = sub.add_parser("similarity")
    sim.add_argument("--source-chembl-id", required=True)

    sim_all = sub.add_parser("similarity-all")
    sim_all.add_argument("--source-ids")
    sim_all.add_argument("--source-ids-file")

    topk = sub.add_parser("topk")
    topk.add_argument("--source-chembl-id", required=True)
    topk.add_argument("--k", type=int, default=10)

    topk_all = sub.add_parser("topk-all")
    topk_all.add_argument("--source-ids")
    topk_all.add_argument("--source-ids-file")
    topk_all.add_argument("--k", type=int, default=10)

    sub.add_parser("mart-load")
    sub.add_parser("regenerate-pivot")

    seed = sub.add_parser("seed-fingerprint-manifest")
    seed.add_argument("--verify-file", action="store_true",
                      help="download the fingerprint file to verify it (slow, several minutes)")

    args = parser.parse_args()
    settings = get_settings()

    if args.command == "ingest":
        print(json.dumps(ingest_bronze(settings)))
    elif args.command == "fingerprints":
        print(json.dumps({"uri": compute_fingerprints(settings)}))
    elif args.command == "similarity":
        print(json.dumps({"uri": compute_similarity_for_source(args.source_chembl_id, settings)}))
    elif args.command == "similarity-all":
        uris = compute_similarity_for_all_sources(_read_source_ids(args), settings)
        print(json.dumps({"sources_written": len(uris)}))
    elif args.command == "topk":
        table = select_top_k(args.source_chembl_id, args.k, settings)
        print(json.dumps({"source_chembl_id": args.source_chembl_id, "rows_written": table.num_rows}))
    elif args.command == "topk-all":
        n = select_top_k_for_all_sources(_read_source_ids(args), args.k, settings)
        print(json.dumps({"sources_written": n}))
    elif args.command == "mart-load":
        print(json.dumps(load_mart_from_s3(settings)))
    elif args.command == "regenerate-pivot":
        print(json.dumps({"chosen_sources": regenerate_pivot_view(settings)}))
    elif args.command == "seed-fingerprint-manifest":
        print(json.dumps(seed_fingerprint_manifest(settings, args.verify_file)))


if __name__ == "__main__":
    main()
