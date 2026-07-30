import numpy as np
import pytest

from workers.pipeline.config import Settings
from workers.pipeline.fingerprints import _morgan_generator, _smiles_to_fingerprint

ETHANOL = "CCO"
ASPIRIN = "CC(=O)Oc1ccccc1C(=O)O"
CAFFEINE = "CN1C=NC2=C1C(=O)N(C)C(=O)N2C"


def test_valid_smiles_returns_packed_bytes():
    # ethanol
    fp = _smiles_to_fingerprint(ETHANOL, radius=2, n_bits=2048)
    assert fp is not None
    assert len(fp) == 2048 // 8


def test_invalid_smiles_returns_none():
    fp = _smiles_to_fingerprint("not_a_smiles!!", radius=2, n_bits=2048)
    assert fp is None


@pytest.mark.parametrize("bad_smiles", ["   ", "C(", "[", "CC(((", "Zz"])
def test_unparseable_smiles_return_none_rather_than_raising(bad_smiles):
    # compute_fingerprints() counts these as n_invalid_smiles and skips them,
    # so they must come back as None instead of propagating an exception.
    assert _smiles_to_fingerprint(bad_smiles, radius=2, n_bits=2048) is None


def test_empty_smiles_yields_an_all_zero_fingerprint_not_none():
    fp = _smiles_to_fingerprint("", radius=2, n_bits=2048)
    assert fp is not None
    assert set(fp) == {0}


@pytest.mark.parametrize("n_bits", [256, 512, 1024, 2048])
def test_packed_length_is_one_eighth_of_bit_width(n_bits):
    fp = _smiles_to_fingerprint(ASPIRIN, radius=2, n_bits=n_bits)
    assert len(fp) == n_bits // 8


def test_fingerprint_is_bit_packed_not_byte_per_bit():
    fp = _smiles_to_fingerprint(ASPIRIN, radius=2, n_bits=2048)
    unpacked = np.unpackbits(np.frombuffer(fp, dtype=np.uint8))
    assert unpacked.size == 2048
    assert set(np.unique(unpacked).tolist()) <= {0, 1}
    assert unpacked.sum() > 0  # aspirin sets at least some bits


def test_returns_bytes_so_it_can_land_in_a_parquet_binary_column():
    fp = _smiles_to_fingerprint(ETHANOL, radius=2, n_bits=2048)
    assert isinstance(fp, bytes)


def test_same_smiles_gives_identical_fingerprint():
    assert _smiles_to_fingerprint(CAFFEINE, 2, 2048) == _smiles_to_fingerprint(CAFFEINE, 2, 2048)


def test_equivalent_smiles_spellings_give_the_same_fingerprint():
    # RDKit canonicalises on parse, so these two spellings of benzene must agree.
    assert _smiles_to_fingerprint("c1ccccc1", 2, 2048) == _smiles_to_fingerprint("C1=CC=CC=C1", 2, 2048)


def test_different_molecules_give_different_fingerprints():
    assert _smiles_to_fingerprint(ETHANOL, 2, 2048) != _smiles_to_fingerprint(CAFFEINE, 2, 2048)


def test_radius_changes_the_fingerprint():
    # A larger radius captures more substructure, so the bit pattern must differ.
    assert _smiles_to_fingerprint(ASPIRIN, 1, 2048) != _smiles_to_fingerprint(ASPIRIN, 2, 2048)


def test_generator_is_cached_per_parameter_pair():
    assert _morgan_generator(2, 2048) is _morgan_generator(2, 2048)
    assert _morgan_generator(2, 2048) is not _morgan_generator(3, 2048)


def test_settings_default():
    settings = Settings(s3_bucket="b", dwh_url="postgresql://x", s3_prefix="p")
    assert settings.morgan_radius == 2
    assert settings.morgan_n_bits == 2048
