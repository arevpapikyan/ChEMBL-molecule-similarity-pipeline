import dataclasses

import pytest

from workers.pipeline.config import Settings, get_settings

REQUIRED = {
    "S3_BUCKET": "test-bucket",
    "DWH_URL": "postgresql://test:test@localhost:5432/testdb",
    "S3_PREFIX": "test/prefix",
}


@pytest.fixture
def clean_env(monkeypatch):
    for var in (
        "S3_BUCKET", "DWH_URL", "S3_PREFIX", "S3_REGION",
        "N_SOURCE_MOLECULES", "RANDOM_SEED",
    ):
        monkeypatch.delenv(var, raising=False)
    return monkeypatch


def test_all_required_variables_present(clean_env):
    for key, value in REQUIRED.items():
        clean_env.setenv(key, value)

    settings = get_settings()

    assert settings.s3_bucket == REQUIRED["S3_BUCKET"]
    assert settings.dwh_url == REQUIRED["DWH_URL"]
    assert settings.s3_prefix == REQUIRED["S3_PREFIX"]


@pytest.mark.parametrize("missing", sorted(REQUIRED))
def test_missing_required_variable_is_named_in_the_error(clean_env, missing):
    for key, value in REQUIRED.items():
        if key != missing:
            clean_env.setenv(key, value)

    with pytest.raises(RuntimeError, match=missing):
        get_settings()


def test_error_lists_every_missing_variable_at_once(clean_env):
    with pytest.raises(RuntimeError) as excinfo:
        get_settings()

    message = str(excinfo.value)
    for key in REQUIRED:
        assert key in message


def test_optional_variables_fall_back_to_defaults(clean_env):
    for key, value in REQUIRED.items():
        clean_env.setenv(key, value)

    settings = get_settings()

    assert settings.s3_region == "us-east-1"
    assert settings.n_source_molecules == 100
    assert settings.random_seed == 42


def test_optional_variables_are_read_from_the_environment(clean_env):
    for key, value in REQUIRED.items():
        clean_env.setenv(key, value)
    clean_env.setenv("S3_REGION", "eu-central-1")
    clean_env.setenv("N_SOURCE_MOLECULES", "5")
    clean_env.setenv("RANDOM_SEED", "1234")

    settings = get_settings()

    assert settings.s3_region == "eu-central-1"
    assert settings.n_source_molecules == 5
    assert settings.random_seed == 1234


def test_non_integer_numeric_setting_fails_loudly(clean_env):
    for key, value in REQUIRED.items():
        clean_env.setenv(key, value)
    clean_env.setenv("N_SOURCE_MOLECULES", "not-a-number")

    with pytest.raises(ValueError):
        get_settings()


def test_layer_prefixes_are_derived_from_the_root_prefix():
    settings = Settings(s3_bucket="b", dwh_url="postgresql://x", s3_prefix="root/sub")

    assert settings.bronze_prefix == "root/sub/bronze"
    assert settings.silver_fingerprints_prefix == "root/sub/silver/fingerprints"
    assert settings.silver_similarity_prefix == "root/sub/silver/similarity"


def test_settings_are_immutable():
    settings = Settings(s3_bucket="b", dwh_url="postgresql://x", s3_prefix="p")

    with pytest.raises(dataclasses.FrozenInstanceError):
        settings.s3_bucket = "other"
