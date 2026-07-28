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

    return Settings(
        s3_bucket=os.environ["S3_BUCKET"],
        dwh_url=os.environ["DWH_URL"],
        s3_prefix=os.environ["S3_PREFIX"],
        s3_region=os.environ.get("S3_REGION", "us-east-1"),
        n_source_molecules=int(os.environ.get("N_SOURCE_MOLECULES", "100")),
        random_seed=int(os.environ.get("RANDOM_SEED", "42")),
    )
