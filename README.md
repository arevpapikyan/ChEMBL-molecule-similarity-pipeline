# ChEMBL Molecule Similarity Pipeline

What the pipeline does

The goal is to identify, for each of a chosen set of source molecules, the **top-10 most structurally similar molecules** across ChEMBL. End to end, the pipeline:

1. **Ingests four ChEMBL tables** (`chembl_id_lookup`, `molecule_dictionary`, `compound_properties`, `compound_structures`) into the warehouse.
2. **Computes Morgan fingerprints** for every compound structure (radius 2, 2048 bits, via RDKit) and stores them as Parquet on S3.
3. **Computes Tanimoto similarity** of each source molecule against all other ChEMBL molecules that have a structure, and saves each source's full similarity table as Parquet on S3.
4. **Selects the top-10** most similar targets per source. When several molecules tie at the lowest kept score and not all fit in the top-10, the kept tied rows are flagged with `has_duplicates_of_last_largest_score`.
5. **Builds a data mart**: a dimension table of molecules and their properties (restricted to molecules the facts reference) and a fact table of source→target similarities with the tie flag, plus analytical views (see [Results](#results)).

Inputs and outputs live under an S3 prefix of your choosing, set via `S3_PREFIX` (e.g. `chembl/similarity`).

> **A note on scale.** A full all-vs-all similarity over ChEMBL is an O(n²) problem that is not tractable on a single machine. This pipeline fingerprints the full corpus but computes similarity for a configurable subset of source molecules. The reasoning, benchmark, and the parameters that control it are in [Scale and compute limitations](#scale-and-compute-limitations).

## Architecture

```
                  ┌─────────────┐      ┌───────────────┐      ┌───────────────┐
 ChEMBL           │  BRONZE     │      │  vSILVER       │      │  GOLD         │
 (chembl_         │  raw        │      │  fingerprints │      │  dim_molecule │
 downloader)  ──> │  tables     │ ───> │  + full       │ ───> │  fact_        │
                  │  (S3 +      │      │  
                  similarity   │      │  similarity   │
                  │  Postgres   │      │  tables       │      │  (Postgres)   │
                  │  raw schema │      │  (S3 parquet) │      │  + views      │
                  └─────────────┘      └───────────────┘      └───────────────┘
```

Orchestrated end-to-end by Airflow (DAG id `chembl_similarity_pipeline`, defined in `dags/chembl_molecule_similarity_pipeline/dag.py`). The heavy compute (ingestion, RDKit, similarity) runs in a separate `pipeline_worker` Docker image that the DAG launches per task, so the Airflow containers stay light and the compute environment is pinned independently.

### Why three medallion layers

- **Bronze (raw):** ChEMBL's `chembl_id_lookup`, `molecule_dictionary`, `compound_properties`, `compound_structures`, landed with only column selection and key normalisation. Written to both S3 (`s3://<bucket>/<prefix>/bronze/`) and a Postgres `raw` schema. This is a replay buffer: if anything downstream breaks we never re-pull from ChEMBL.
- **Silver (intermediate):** Morgan fingerprints and the *full* pairwise Tanimoto tables (one per source molecule, before top-10 filtering). Large, expensive, and mostly disposable once the mart is built, so they live as Parquet on S3 rather than in Postgres, keeping the warehouse small and fast.
- **Gold (mart):** `dim_molecule` + `fact_similarity` in Postgres, built only from the top-10-per-source subset, plus the analytical views on top. This is what BI tooling actually queries.

### Design decisions

- **Full similarity table schema:** each source's full pairwise table is self-contained, with `source_chembl_id`, `target_chembl_id`, and `similarity_score` stored as real columns rather than being identified by filename alone.
- **Fixed source set for the pivot:** the pivot view uses a fixed set of source molecules (first 10 by sorted `chembl_id`), not a different random 10 each run, so the pivot's column set is stable between runs.
- **`cx_logp` and `molecular_species` are populated as NULL:** these two dimension columns are kept in the schema for shape but are not populated by ingestion. See the query in `workers/pipeline/ingest.py`; adjust there if your ChEMBL release supplies them and you want them carried through.

### Data mart and views

**Dimension** (`dim_molecule`): one row per molecule with `chembl_id`, `molecule_type`, `mw_freebase`, `alogp`, `psa`, `cx_logp`, `molecular_species`, `full_mwt`, `aromatic_rings`, `heavy_atoms`. Restricted to molecules referenced by the fact table.

**Fact** (`fact_similarity`): source molecule, target molecule, Tanimoto score, `rank_within_source`, and the `has_duplicates_of_last_largest_score` flag. Holds the top-10 per source.

**Supporting table** (`pivot_source_selection`): the 10 source molecules the pivot view is built over. Truncated and repopulated by `regenerate_pivot_view()` on every run; `crosstab` reads it for both the row source and the column list, so the view's columns and its data can never disagree.

Five analytical views sit on top (all over the top-10 subset; sample output in [Results](#results)):

- `v7a_avg_similarity_per_source`: average similarity score per source molecule.
- `v7b_avg_alogp_deviation`: average deviation of a similar molecule's `alogp` from its source molecule's `alogp` (both absolute and signed).
- `v8a_similarity_pivot`: pivot with rows as target molecules, columns as the 10 fixed source molecules, cells as similarity scores. Generated at runtime (see Design decisions).
- `v8b_next_and_second_target`: per row: the source, target, score, the next most similar target after this one, and the source's second most similar target overall.
- `v8c_avg_similarity_grouped`: average similarity grouped four ways (per source; per source's aromatic-rings + heavy-atoms; per source's heavy-atoms; whole dataset), with aggregation NULLs shown as `TOTAL`, built with `GROUPING SETS` and no `UNION`.

### Performance and reliability decisions

The DWH sits in a private subnet reached through an AWS SSM tunnel. SSM is built for interactive sessions, not bulk transfer, and drops under load (`broken pipe`, `connection reset`). Most choices below exist to push less data through that tunnel, or to survive it dropping mid-run.

- **Source molecules are picked inside the database.** `choose_source_molecules` samples in SQL (`ORDER BY md5(chembl_id || :seed) LIMIT :n`), so only the chosen rows cross the tunnel instead of all ~2.9M. Hashing with `RANDOM_SEED` keeps the choice random but reproducible.
- **Bulk data moves in batches.** `_ingest_table` streams with `fetchmany` and `COPY`s each batch, so a dropped tunnel costs one batch, not the whole table.
- **The fingerprint corpus loads once.** All sources are scored against a single in-memory matrix, instead of one container per source re-downloading the corpus. Fingerprints stay packed as `uint8` (~0.69 GB, not ~5.5 GB), keeping peak RSS near 1 GB.
- **Slow stages are cached and resumable.** The ChEMBL tarball, its extraction, and the fingerprint file are reused when still valid (guarded by completion markers and a manifest of `radius`/`n_bits`/`sample_size`/row count). Per-source similarity results already on S3 are skipped, so an interrupted run resumes.
- **Stale outputs are pruned, and invalidated when the corpus changes.** Changing `N_SOURCE_MOLECULES` or `RANDOM_SEED` selects different molecules; old files are deleted first. This matters because `load_mart_from_s3` loads *every* `top10.parquet` under the prefix, so one orphan would silently add a source to `fact_similarity`. Separately, the per-source similarity outputs record a token identifying the fingerprint corpus they were built against (sample size, seed, count, Morgan params); if the current corpus no longer matches, for example when switching between a full run and a sample, the cached outputs are recomputed rather than reused, so the mart can never mix results from two different corpora.

### Open assumptions

- **View `v8b`** returns, for each row, two reference points: the *next* most similar target after the one in that row, and the *second* most similar target overall for that same source molecule. Both are ranked against the same source molecule the row belongs to, using positions within that source's own top-10 list. They are deliberately not computed as a chain from one target to the next: the rankings only exist for the chosen source molecules, so a molecule that appears only as a target has no ranked list of its own to continue a chain into.
- **Transaction isolation for similarity writes:** no elevated isolation level is needed, because the design removes the possibility of write conflicts rather than guarding against them. Similarity is computed one source at a time in a single worker, each source's results are written to a separate location, and one final step gathers them and loads the fact table in a single pass. Since no two writers ever touch the same rows, Postgres's default `READ COMMITTED` is sufficient.

## Scale and compute limitations

Computing similarity for *every* ChEMBL molecule against every other is an O(n²) problem. With ~2.9M structures in the corpus (an upper bound from `compound_structures`; the exact figure is written to the fingerprint manifest as `n_fingerprints`, and it moves with the ChEMBL release), that is on the order of 8 trillion Tanimoto comparisons.

**Benchmark** (single thread, packed `uint8` fingerprints, one machine):

| Workload                           | Comparisons    | Wall time | Output  |
| ---------------------------------- | -------------- | --------- | ------- |
| Measured (10k × 10k)              | 100 M          | 17.6 s    | 0.30 GB |
| Extrapolated full matrix (n²)     | ~8.4 × 10¹² | ~17 days  | ~25 TB  |
| Chosen subset (100 × full corpus) | ~2.9 × 10⁸   | ~1 min    | ~0.9 GB |

Throughput is ~5.7M comparisons/s; output is ~3.0 bytes/comparison on disk; peak RSS is ~1 GB (the packed corpus is ~0.69 GB and must stay resident).

**Why the full matrix is not run here.** At ~17 days of single-thread wall time and ~25 TB of output, it is impractical on one machine: the output alone exceeds what is reasonable to store and upload, and parallelism is bounded by RAM because every worker must hold the whole corpus, so adding workers does not shorten the wall-clock within these constraints. Distributing across a cluster, GPU Tanimoto kernels, or LSH/MinHash blocking to prune candidate pairs before exact scoring would all help, but none are set up here.

**What the pipeline does instead.**

- **Fingerprints** are computed for the **full** compound set and stored once, so a later "find neighbours of one target molecule" query is cheap. This is the default.
- **Similarity** is computed for a configurable **subset of `N_SOURCE_MOLECULES` source molecules** (default 100) against the full corpus, then reduced to top-10 per source. This runs in about a minute and under a gigabyte.

**Controlling the corpus size for local runs.** `FINGERPRINT_SAMPLE_SIZE` (unset by default) caps fingerprinting to a reproducible random sample of that many structures, for fast iteration on a laptop. Sample selection is deterministic for a given `RANDOM_SEED`. Because `choose_source_molecules` draws from the full warehouse (not the sample), a sample smaller than the source set means some sources have no fingerprint, so the similarity stage checks every source against the corpus up front and fails fast with a clear message (naming the count and the likely cause) *before* deleting anything, rather than erroring deep in the loop or silently producing partial results. For a coherent end-to-end sample run, keep `N_SOURCE_MOLECULES` at or below the sample size. Leave `FINGERPRINT_SAMPLE_SIZE` unset for full, production-shaped runs.

## Repository layout

```
dags/chembl_molecule_similarity_pipeline/
  dag.py                     # Airflow DAG: ingest -> fingerprints -> similarity -> mart -> checks
  docker-compose.airflow.yml # Airflow api-server/scheduler/dag-processor stack + tunnel + keepalive
  Dockerfile.airflow         # apache/airflow:3.3.0 + FAB provider (admin/admin login)
  .env.example               # template -- copy to .env and fill in real values
  .airflowignore             # keeps compose/env files out of the DAG parser
  teams_cards.py             # Adaptive Card builders for the Teams failure alert
  ssm_tunnel/
    Dockerfile               # aws-cli + session-manager-plugin + socat
    tunnel-loop.sh           # self-healing SSM port-forward, reconnects on drop
workers/
  pipeline/
    ingest.py                # chembl_downloader -> bronze (S3 parquet + Postgres raw schema)
    fingerprints.py          # RDKit Morgan fingerprints -> S3 silver (full corpus or sample)
    similarity.py            # Tanimoto similarity, top-10 + duplicate flag
    mart_load.py             # loads dim_molecule / fact_similarity, regenerates the pivot
    s3_utils.py              # S3 client + parquet/json helpers (explicit-credential session)
    db_utils.py              # DWH connection helper
    config.py                # env-driven Settings + validation
  run.py                     # CLI: ingest | fingerprints | similarity | similarity-all |
                             #      topk | topk-all | mart-load | regenerate-pivot |
                             #      seed-fingerprint-manifest
  Dockerfile
  requirements.txt           # runtime + dev deps together (pytest, ruff included)
sql/
  01_schema_raw.sql          # bronze schema DDL
  02_schema_mart.sql         # gold schema DDL (dim_molecule, fact_similarity)
  03_views_basic.sql         # avg-similarity + alogp-deviation views
  04_views_advanced.sql      # next/second-target + grouped-average views, plus the
                             #   pivot_source_selection table (pivot view generated at runtime)
tests/
  test_config.py             # settings validation + S3 layer-prefix contract
  test_ingest.py             # Bronze ingestion: schema contract, batching, CSV/COPY encoding
  test_fingerprints.py       # Morgan generation, packing contract, determinism
  test_similarity.py         # Tanimoto kernel (packed), padding bits, chunking
  test_similarity_io.py      # S3 key handling, orphan pruning, top-k shaping
  test_topk_ranking.py       # ranking + has_duplicates_of_last_largest_score
  test_mart_load.py          # dim/fact row shaping
scripts/
  verify_outputs.py          # post-run verification + README result tables (needs live DWH/S3)
```

## The SSM tunnel: where it is needed and where it is not

The pipeline talks to two different AWS services, and only one of them goes through the tunnel:

- **Postgres DWH (through the tunnel).** The database is in a private subnet with no public endpoint, so it cannot be reached directly from your host over the internet. This project reaches it through an SSM port-forward, which is what the pipeline is wired for (other routes into a private VPC exist in general, such as a VPN or an in-VPC client, but are not configured here). Every task that touches Postgres (`ingest_bronze`, `choose_source_molecules`, `load_mart`, `regenerate_pivot_view`, and the DDL/verification steps) needs the tunnel up. With it down you get `connection refused` (nothing listening) or `server closed the connection unexpectedly` (tunnel up but its SSM session died).
- **S3 (NOT through the tunnel).** S3 is a public endpoint reached over the normal internet. Fingerprint checks and all Silver Parquet reads/writes talk to S3 directly and work whether or not the tunnel is up. They need valid AWS credentials, not the tunnel.

The two share the same SSO identity, which is why they can look entangled; but the tunnel authenticates an SSM *session*, while S3 authenticates *API calls*, and they fail independently.

**The tunnel container.** `docker-compose.airflow.yml` ships a containerised tunnel (`ssm_tunnel/`) that reconnects on its own: `tunnel-loop.sh` runs `aws ssm start-session` in a loop with a `socat` relay in front, so a dropped session re-establishes within ~2s instead of failing every downstream task. This is what `dag.py` means by "restart the ssm_tunnel service", and why `_dwh_connect()` retries rather than failing immediately; a retry usually lands after the loop reconnects. The container reuses the host's SSO token (mounted read-only from `AWS_CONFIG_DIR`) and cannot refresh it, so keep a valid `aws sso login` on the host; the token lasts ~8 hours.

The tunnel is configured entirely from `.env`: `AWS_REGION`, `AWS_PROFILE`, `SSM_TARGET` (the bastion EC2 instance id), `DWH_HOST` (the RDS endpoint), `DWH_PORT`, and `AWS_CONFIG_DIR` (absolute path to your host `.aws`). `AWS_PROFILE` is set only for this container; see the credentials note below for why the Airflow containers deliberately do not use it.

## Running the project

### 1. AWS credentials

```powershell
# Sign in once; the SSO session lasts ~8 hours.
aws sso login --profile <profile>

# Export temporary keys straight into .env-compatible form.
aws configure export-credentials --profile <profile> --format env-no-export
```

The second command prints `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`; copy them into `.env`. These are what boto3 uses to reach S3. Because SSO keys expire, re-export before a fresh run if they have gone stale.

### 2. Fill in `.env`

```powershell
Copy-Item dags/chembl_molecule_similarity_pipeline/.env.example dags/chembl_molecule_similarity_pipeline/.env
```

Fill in the S3/DWH values, the AWS keys from step 1, `AWS_CONFIG_DIR` (absolute path; Compose does not expand `~` or `${HOME}`), `AIRFLOW__API_AUTH__JWT_SECRET`, and `TEAMS_WEBHOOK_URL`. See [Environment variables](#environment-variables) for the full list.

### 3. Loading `.env` into your shell

`.env` is a Docker convention, not a shell feature; no shell reads one on its own. `docker compose --env-file` and each `docker run --env-file` handle it for the containers, so the only variable that needs to reach *your* shell is `PGPASSWORD` (`psql` has no password flag, and the DDL step below applies four files, so without it you get four prompts):

```powershell
$env:PGPASSWORD = (Get-Content dags/chembl_molecule_similarity_pipeline/.env |
    Where-Object { $_ -like 'PGPASSWORD=*' } | ForEach-Object { ($_ -split '=', 2)[1] })
```

### 4. Build and start

```powershell
# Worker image (used by every compute task)
docker build -t pipeline_worker ./workers

# Airflow stack + tunnel + keepalive
docker compose -f dags/chembl_molecule_similarity_pipeline/docker-compose.airflow.yml `
  --env-file dags/chembl_molecule_similarity_pipeline/.env up -d --build
```

Open `http://localhost:8080` and log in with `admin`/`admin` (configurable via `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD`).

**`.env`, compose, or image changes require recreating the containers (`down`/`up`), not `restart`.** Environment variables are injected when a container is *created*, so `docker compose restart` reuses the already-loaded environment and your change silently won't apply. DAG *code* changes don't need this, since the scheduler reparses the dags folder on its own every ~30s and picks them up live. The one exception is when a `dag.py` change also introduces a **new environment variable**: the code reparses, but the variable won't be present until you recreate, so recreate then too.

```powershell
docker compose -f dags/chembl_molecule_similarity_pipeline/docker-compose.airflow.yml down
docker compose -f dags/chembl_molecule_similarity_pipeline/docker-compose.airflow.yml up -d
```

### 5. Apply the DDL

The `sql/` files are **not** applied by the pipeline -- run them once, in order, against the DWH through the tunnel started in step 4. `PGPASSWORD` from step 3 is what keeps this from prompting four times:

```powershell
$dwh = "-h localhost -p 5432 -U <user> -d <database>"
psql $dwh.Split() -v ON_ERROR_STOP=1 -f sql/01_schema_raw.sql
psql $dwh.Split() -v ON_ERROR_STOP=1 -f sql/02_schema_mart.sql
psql $dwh.Split() -v ON_ERROR_STOP=1 -f sql/03_views_basic.sql
psql $dwh.Split() -v ON_ERROR_STOP=1 -f sql/04_views_advanced.sql
```

Fill in `<user>` and `<database>` to match `DWH_URL`. All four files are idempotent (`CREATE ... IF NOT EXISTS`, `CREATE OR REPLACE VIEW`), so re-running them is safe.

If you have no local `psql`, use a stock Postgres image -- the worker image will not work for this, since it ships `libpq-dev` for building `psycopg` but not the `psql` client binary:

```powershell
docker run --rm -v "${PWD}/sql:/sql" -e PGPASSWORD=$env:PGPASSWORD `
  --add-host=host.docker.internal:host-gateway postgres:16 `
  psql -h host.docker.internal -p 5432 -U <user> -d <database> -v ON_ERROR_STOP=1 -f /sql/01_schema_raw.sql
```

Without this step the first run fails in `ingest_bronze` on a missing `raw` schema.

### 6. Trigger the DAG

Trigger the `chembl_similarity_pipeline` DAG from the UI. It is capped at `max_active_runs=1`, so a fresh trigger can never collide with an older run.

### Configuration flow: two paths

Config reaches the pipeline two ways, which is why not every variable is handled the same:

- **In-Airflow code** (source selection, the DWH connection, the Teams preflight/callback) reads the full environment the scheduler loads from compose `env_file: .env`.
- **Worker containers** start with an empty environment and receive only the explicit allowlist `WORKER_ENV_VARS` (S3/DWH settings, pipeline parameters, and the AWS keys), forwarded as `--env` arguments.

`_require_env()` runs at DAG-parse time and fails loudly if a required variable (or, unless `TEAMS_ALERTS_OPTIONAL=true`, the webhook) is missing, so a deployment without this `env_file` fails immediately rather than mid-run.

### Failure notifications

Failures are posted to a Microsoft Teams channel via an incoming Workflow webhook (`TEAMS_WEBHOOK_URL`). The card is built in `teams_cards.py` as a multiple-choice "pop quiz": the real exception is mixed in with fixed joke distractors and the options are shuffled, so the true cause is not always in the same slot. A preflight task, `check_teams_webhook`, runs first and fails the whole run if the webhook is missing or unreachable, so a long run never proceeds unable to report its own outcome. Delivery is strict: any non-2xx response counts as a failure (a revoked webhook still accepts the connection and returns 4xx). Set `TEAMS_ALERTS_OPTIONAL=true` to run deliberately without alerting.

### Credential safety

Several tasks run `pipeline_worker` with `docker run`, passing `DWH_URL` and the AWS keys as `--env=KEY=value` arguments. When a `docker run` fails, `subprocess.CalledProcessError` includes the full command (secrets and all) in its message; unhandled, that would land in Airflow's logs and, via the Teams callback, in a chat channel. `run_worker()` catches it and re-raises with `redact_cmd()` masking `DWH_URL` and the AWS keys, plus `from None` to drop the original exception; this is essential, because Airflow prints the full exception chain, so keeping the original would leak the unredacted command right back out.

Separately, S3 clients are built from **explicit credentials**: `AWS_PROFILE` is present in the environment for the tunnel, and boto3 would otherwise try to load that profile (whose config files are not in the Airflow/worker containers) and fail with `ProfileNotFound`. `get_s3_client()` removes `AWS_PROFILE` and passes the `AWS_*` keys directly, so every S3 call authenticates with the keys regardless of the ambient profile.

## Tests

Run inside a virtual environment so the project's dependencies (RDKit, boto3, psycopg2, pytest, ruff) stay isolated from your system Python:

```PowerShell
python -m venv venv
venv/Scripts/Activate
pip install -r workers/requirements.txt
pytest
deactivate
```

No `.env`, network, or credentials required: S3 and Postgres are monkeypatched at the module boundary. (The same tests also run inside the `pipeline_worker` image, which is built from the same `requirements.txt`, so the venv is only for running them directly on your host.)

| File                      | Covers                                                                                                                                                                      |
| ------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `test_similarity.py`    | Tanimoto on packed fingerprints: known values, padding-bit masking at non-byte-aligned widths, chunk-size invariance, symmetry, and agreement with a brute-force reference  |
| `test_similarity_io.py` | S3 key parsing, orphan pruning, self-exclusion, skip-existing, and that the batch path produces byte-identical output to the single-source path                             |
| `test_topk_ranking.py`  | Ranking order and the`has_duplicates_of_last_largest_score` boundary semantics, including ties that fit inside k, overflow it, or land exactly on it                      |
| `test_fingerprints.py`  | Morgan parameters, the packing contract the similarity kernel depends on, determinism across equivalent SMILES spellings, and generator caching                             |
| `test_config.py`        | Required-variable validation, defaults, and the S3 layer-prefix contract every stage shares                                                                                 |
| `test_ingest.py`        | Bronze ingestion: the declared column/schema contract, entity filtering, batching invariance, and CSV encoding for`COPY` (nulls, quotes, newlines, backslashes in SMILES) |
| `test_mart_load.py`     | Fact-row shaping and that`dim_molecule` receives exactly the molecules the facts reference                                                                                |

## Verifying a run

`scripts/verify_outputs.py` inspects a completed run end-to-end and prints the tables under [Results](#results). It needs a live DWH, live S3, and real credentials, so it lives under `scripts/` and is never collected by `pytest`. Run it in the worker container (which supplies the env and can resolve `host.docker.internal`):

```bash
docker run --rm --env-file dags/chembl_molecule_similarity_pipeline/.env `
  --add-host=host.docker.internal:host-gateway `
  -v "$(pwd):/work" --entrypoint python pipeline_worker /work/scripts/verify_outputs.py
```

It checks: exactly `N_SOURCE_MOLECULES` distinct sources and `N × TOP_K` fact rows; exactly `TOP_K` targets per source with unique ranks; no self-similarity rows; `dim_molecule` holds only molecules referenced by `fact_similarity`; one `top10.parquet` per source in S3 (catches orphans); and that the pivot has 10 source columns and every view returns rows. Exit codes: `0` all passed, `1` a check failed, `2` could not verify at all (bad config, no DWH, no S3). `--json` appends a machine-readable summary; `--top-k` / `--expected-sources` override the expected counts.

## Environment variables

Grouped by what reads them. Everything lives in `.env` (see `.env.example`); nothing here is committed with real values.

**S3 and DWH (used by the pipeline)**

| Variable                                                                  | Purpose                                                                                                                                                                  |
| ------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN` | Temporary SSO credentials from`aws configure export-credentials`. boto3 uses these for S3 (the ambient `AWS_PROFILE` is deliberately ignored; see Credential safety) |
| `S3_BUCKET`                                                             | Object-store bucket for Bronze/Silver Parquet                                                                                                                            |
| `S3_REGION`                                                             | AWS region the bucket lives in (default`us-east-1` if unset)                                                                                                           |
| `S3_PREFIX`                                                             | Namespace within the bucket, e.g.`chembl/similarity`                                                                                                                   |
| `DWH_URL`                                                               | Postgres connection string for the DWH (host stays`host.docker.internal` so containers reach it via the tunnel)                                                        |
| `PGPASSWORD`                                                            | Same password as in`DWH_URL`, for manual `psql`/DDL sessions                                                                                                         |

**Pipeline parameters**

| Variable                              | Purpose                                                                                                                                                                                                                                |
| ------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `N_SOURCE_MOLECULES`                | Number of source molecules to sample. Default`100`                                                                                                                                                                                   |
| `TOP_K`                             | Neighbours kept per source. Default`10`; read by the DAG and `verify_outputs.py`                                                                                                                                                   |
| `RANDOM_SEED`                       | Fixed seed for reproducible source and fingerprint-sample selection. Default`42`                                                                                                                                                     |
| `FINGERPRINT_SAMPLE_SIZE`           | Unset (default) fingerprints the full compound set. Set to a positive integer to fingerprint only a reproducible random sample of that size for fast local/dev runs. See[Scale and compute limitations](#scale-and-compute-limitations) |
| `FINGERPRINTS_FORCE_RECOMPUTE`      | Set`1` (or `true`) to ignore the fingerprint manifest and recompute from scratch                                                                                                                                                   |
| `INGEST_MIN_FREE_BYTES`             | Disk the extraction preflight requires. Default 50 GiB                                                                                                                                                                                 |
| `INGEST_DOWNLOAD_MAX_ATTEMPTS`      | Resumable-download retries. Default 10                                                                                                                                                                                                 |
| `INGEST_DOWNLOAD_RETRY_DELAY`       | Seconds between download retries. Default 15                                                                                                                                                                                           |
| `INGEST_DOWNLOAD_SOCKET_TIMEOUT`    | Socket timeout for the ChEMBL download, seconds. Default 120                                                                                                                                                                           |
| `INGEST_EXTRACTION_TIMEOUT_SECONDS` | Ceiling on SQLite extraction before failing loudly. Default 7200 (2h)                                                                                                                                                                  |
| `PYSTOW_HOME`                       | Where the ChEMBL tarball and its extraction are cached. Default`~/.data`; point at a larger disk if space is tight                                                                                                                   |

**SSM tunnel (`ssm_tunnel` container only)**

| Variable           | Purpose                                                                                           |
| ------------------ | ------------------------------------------------------------------------------------------------- |
| `AWS_REGION`     | Region for the SSM session                                                                        |
| `AWS_PROFILE`    | SSO profile the tunnel authenticates with. Set for the tunnel; blanked for the Airflow containers |
| `SSM_TARGET`     | Bastion EC2 instance id the tunnel forwards through                                               |
| `DWH_HOST`       | Private RDS endpoint the tunnel targets                                                           |
| `DWH_PORT`       | Remote/local forwarded port. Default 5432                                                         |
| `SSM_LOCAL_PORT` | Port`socat` relays from inside the container. Default 15432                                     |
| `AWS_CONFIG_DIR` | Absolute path to your host`.aws`, mounted so the tunnel reuses your SSO token                   |

**Airflow and notifications**

| Variable                                                        | Purpose                                                                                                                                                                                                                 |
| --------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `TEAMS_WEBHOOK_URL`                                           | Teams Workflow webhook. Required unless`TEAMS_ALERTS_OPTIONAL=true`                                                                                                                                                   |
| `TEAMS_ALERTS_OPTIONAL`                                       | Set`true` to run deliberately without alerting. Default `false`                                                                                                                                                     |
| `AIRFLOW__API_AUTH__JWT_SECRET`                               | Signs the scheduler↔api-server tokens; must be a fixed value identical across all Airflow services, or tasks fail with "Invalid auth token". Generate with`python -c "import secrets; print(secrets.token_hex(32))"` |
| `AIRFLOW_UID`                                                 | Host UID the Airflow containers run as, to keep bind-mounted`logs`/`dags` writable on Linux/macOS. Ignored on Windows                                                                                               |
| `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD` | Airflow UI login, read once by`airflow_init`                                                                                                                                                                          |
| `DAG_OWNER`                                                   | Shown as the DAG owner in the UI and on Teams cards                                                                                                                                                                     |

## Results

Figures below are from a full run over ChEMBL (release pinned by `chembl_downloader.latest()`) with `N_SOURCE_MOLECULES=100`, `RANDOM_SEED=42`, `FINGERPRINT_SAMPLE_SIZE` unset (full corpus).
Reproduce with `scripts/verify_outputs.py` (see [Verifying a run](#verifying-a-run)).

### Volumes

| Layer  | Object                      | Rows / objects                              |
| ------ | --------------------------- | ------------------------------------------- |
| Bronze | `raw.chembl_id_lookup`    | 3,082,236 rows                              |
| Bronze | `raw.molecule_dictionary` | 2,921,148 rows                              |
| Bronze | `raw.compound_properties` | 2,901,464 rows                              |
| Bronze | `raw.compound_structures` | 2,897,819 rows                              |
| Silver | `silver/fingerprints/`    | 1 Parquet + 1 manifest (2 objects)          |
| Silver | `silver/similarity/`      | 100 Parquet + 1 corpus marker (101 objects) |
| Silver | `silver/topk/`            | 100 Parquet (one per source, top-10)        |
| Gold   | `mart.dim_molecule`       | 1,100 rows                                  |
| Gold   | `mart.fact_similarity`    | 1,000 rows (100 sources x 10)               |

38 of the 1,000 fact rows carry `has_duplicates_of_last_largest_score = true`.

### Top-10 for one source molecule (`mart.fact_similarity`)

| source_chembl_id | target_chembl_id | similarity_score   | rank | has_duplicates_of_last_largest_score |
| ---------------- | ---------------- | ------------------ | ---- | ------------------------------------ |
| CHEMBL10528      | CHEMBL9788       | 0.6153846153846154 | 1    | false                                |
| CHEMBL10528      | CHEMBL1620120    | 0.6140350877192983 | 2    | false                                |
| CHEMBL10528      | CHEMBL9603       | 0.6140350877192983 | 3    | false                                |
| CHEMBL10528      | CHEMBL269173     | 0.609375           | 4    | false                                |
| CHEMBL10528      | CHEMBL1415873    | 0.603448275862069  | 5    | false                                |
| CHEMBL10528      | CHEMBL275917     | 0.5666666666666667 | 6    | false                                |
| CHEMBL10528      | CHEMBL275274     | 0.5483870967741935 | 7    | false                                |
| CHEMBL10528      | CHEMBL275282     | 0.5333333333333333 | 8    | false                                |
| CHEMBL10528      | CHEMBL9764       | 0.53125            | 9    | false                                |
| CHEMBL10528      | CHEMBL9842       | 0.5294117647058824 | 10   | false                                |

### Dimension table (`mart.dim_molecule`)

| chembl_id     | molecule_type  | mw_freebase | alogp | psa    | cx_logp | molecular_species | aromatic_rings | heavy_atoms |
| ------------- | -------------- | ----------- | ----- | ------ | ------- | ----------------- | -------------- | ----------- |
| CHEMBL104830  | Small molecule | 337.43      | 3.56  | 82.33  | NULL    | NULL              | 2              | 25          |
| CHEMBL10528   | Small molecule | 370.48      | 2.07  | 66.24  | NULL    | NULL              | 3              | 26          |
| CHEMBL1080049 | Small molecule | 285.73      | 3.26  | 37.61  | NULL    | NULL              | 3              | 20          |
| CHEMBL1083557 | Small molecule | 209.21      | -1.17 | 120.94 | NULL    | NULL              | 2              | 15          |
| CHEMBL1162057 | Small molecule | 624.87      | 8.47  | 104.06 | NULL    | NULL              | 3              | 43          |

`cx_logp` and `molecular_species` are NULL by design; see [Design decisions](#design-decisions).

### Average similarity score per source (`mart.v7a_avg_similarity_per_source`), 100 rows

| source_chembl_id | avg_similarity_score | n_targets |
| ---------------- | -------------------- | --------- |
| CHEMBL10528      | 0.5765326928165357   | 10        |
| CHEMBL1185749    | 0.5418323552502884   | 10        |
| CHEMBL1203657    | 0.7940715292459478   | 10        |
| CHEMBL1205932    | 0.7413001611581381   | 10        |
| CHEMBL1206185    | 0.7848974646650300   | 10        |

### Average alogp deviation from source (`mart.v7b_avg_alogp_deviation`), 100 rows

| source_chembl_id | avg_abs_alogp_deviation | avg_signed_alogp_deviation |
| ---------------- | ----------------------- | -------------------------- |
| CHEMBL10528      | 0.711                   | 0.711                      |
| CHEMBL1185749    | 0.993                   | -0.993                     |
| CHEMBL1203657    | 1.095                   | 1.095                      |
| CHEMBL1205932    | 0.565                   | 0.565                      |
| CHEMBL1206185    | 0.626                   | -0.016                     |

### Similarity pivot (`mart.v8a_similarity_pivot`), 100 rows x 10 source columns

First column is the target molecule; each remaining column is one of the 10 fixed source molecules; cells are similarity scores (NULL where that target is not in that source's top-10). Abridged to 4 source columns for width (columns chosen to show representative non-NULL cells, not the leftmost four):

| target_chembl_id | CHEMBL10528 | CHEMBL1185749      | CHEMBL1203657      | CHEMBL1321029 |
| ---------------- | ----------- | ------------------ | ------------------ | ------------- |
| CHEMBL1080049    | NULL        | NULL               | NULL               | 0.58          |
| CHEMBL1162057    | NULL        | NULL               | NULL               | NULL          |
| CHEMBL1179389    | NULL        | 0.509090909090909  | NULL               | NULL          |
| CHEMBL1179390    | NULL        | 0.5178571428571429 | NULL               | NULL          |
| CHEMBL1203637    | NULL        | NULL               | 0.7954545454545454 | NULL          |

### Next and second most similar target (`mart.v8b_next_and_second_target`), 1,000 rows

| source_chembl_id | target_chembl_id | similarity_score   | next_most_similar_target_chembl_id | second_most_similar_target_chembl_id |
| ---------------- | ---------------- | ------------------ | ---------------------------------- | ------------------------------------ |
| CHEMBL10528      | CHEMBL9788       | 0.6153846153846154 | CHEMBL1620120                      | CHEMBL1620120                        |
| CHEMBL10528      | CHEMBL1620120    | 0.6140350877192983 | CHEMBL9603                         | CHEMBL1620120                        |
| CHEMBL10528      | CHEMBL9603       | 0.6140350877192983 | CHEMBL269173                       | CHEMBL1620120                        |
| CHEMBL10528      | CHEMBL269173     | 0.609375           | CHEMBL1415873                      | CHEMBL1620120                        |
| CHEMBL10528      | CHEMBL1415873    | 0.603448275862069  | CHEMBL275917                       | CHEMBL1620120                        |

### Grouped averages (`mart.v8c_avg_similarity_grouped`), 188 rows

`GROUPING SETS` produce all four required groupings in one query with no `UNION`; aggregation NULLs are replaced with the literal `TOTAL`.

| source_chembl_id | source_aromatic_rings | source_heavy_atoms | avg_similarity_score | n_rows |
| ---------------- | --------------------- | ------------------ | -------------------- | ------ |
| CHEMBL10528      | TOTAL                 | TOTAL              | 0.5765326928165357   | 10     |
| CHEMBL1185749    | TOTAL                 | TOTAL              | 0.5418323552502884   | 10     |
| CHEMBL1203657    | TOTAL                 | TOTAL              | 0.7940715292459478   | 10     |
| CHEMBL1205932    | TOTAL                 | TOTAL              | 0.7413001611581381   | 10     |
| CHEMBL1206185    | TOTAL                 | TOTAL              | 0.7848974646650300   | 10     |
