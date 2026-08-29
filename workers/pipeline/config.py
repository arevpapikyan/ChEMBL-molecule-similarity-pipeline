import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    s3_bucket: str
    dwh_url: str
    s3_prefix: str

    s3_region: str = "us-east-1"
    n_source_molecules: int = 100
    random_seed: int = 42

    morgan_radius: int = 2
    morgan_n_bits: int = 2048

    # When set, fingerprints are computed for a reproducible random sample of
    # this many eligible structures instead of the full corpus. Intended for
    # fast local/dev runs; None (the default) means the full compound set.
    fingerprint_sample_size: int | None = None

    @property
    def bronze_prefix(self) -> str:
        return f"{self.s3_prefix}/bronze"

    @property
    def silver_fingerprints_prefix(self) -> str:
        return f"{self.s3_prefix}/silver/fingerprints"

    @property
    def silver_similarity_prefix(self) -> str:
        return f"{self.s3_prefix}/silver/similarity"


def get_settings() -> Settings:
    missing = [var for var in ("S3_BUCKET", "DWH_URL", "S3_PREFIX") if var not in os.environ]
    if missing:
        raise RuntimeError(
            f"Missing required environment variable(s): {', '.join(missing)}. "
            "See dags/chembl_molecule_similarity_pipeline/.env.example."
        )

    sample_raw = os.environ.get("FINGERPRINT_SAMPLE_SIZE", "").strip()
    fingerprint_sample_size = int(sample_raw) if sample_raw else None
    if fingerprint_sample_size is not None and fingerprint_sample_size <= 0:
        raise RuntimeError(
            "FINGERPRINT_SAMPLE_SIZE must be a positive integer when set "
            f"(got {sample_raw!r}); leave it unset for the full corpus."
        )

    return Settings(
        s3_bucket=os.environ["S3_BUCKET"],
        dwh_url=os.environ["DWH_URL"],
        s3_prefix=os.environ["S3_PREFIX"],
        s3_region=os.environ.get("S3_REGION", "us-east-1"),
        n_source_molecules=int(os.environ.get("N_SOURCE_MOLECULES", "100")),
        random_seed=int(os.environ.get("RANDOM_SEED", "42")),
        fingerprint_sample_size=fingerprint_sample_size,
    )
