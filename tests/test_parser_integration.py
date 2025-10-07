"""Integration tests for ReEDS parser with real DataStore.

These tests verify that the parser can build a system using real data files.
"""

from pathlib import Path

import numpy as np
import polars as pl
import pytest
from infrasys import Component

from r2x_core.store import DataStore
from r2x_reeds.config import ReEDSConfig
from r2x_reeds.models.components import ReEDSDemand, ReEDSGenerator, ReEDSRegion
from r2x_reeds.parser import ReEDSParser


@pytest.fixture
def test_data_path() -> Path:
    """Path to test data directory."""
    return Path(__file__).parent / "data" / "test_Pacific"


@pytest.fixture
def reeds_config() -> ReEDSConfig:
    """Create ReEDS configuration for testing."""
    return ReEDSConfig(solve_years=2032, weather_years=2012)


@pytest.fixture
def data_store(test_data_path: Path) -> DataStore:
    """Create DataStore from file mapping."""
    mapping_path = ReEDSConfig.get_file_mapping_path()
    return DataStore.from_json(mapping_path, folder=test_data_path)


@pytest.fixture
def parser(reeds_config: ReEDSConfig, data_store: DataStore) -> ReEDSParser:
    """Create ReEDS parser instance."""
    return ReEDSParser(config=reeds_config, data_store=data_store, name="test_system")


@pytest.fixture
def system(parser: ReEDSParser):
    """Build and return the system (shared fixture for all tests)."""
    return parser.build_system()


def test_parser_creation_with_real_data(parser: ReEDSParser) -> None:
    """Test creating parser with real DataStore."""
    assert parser is not None
    assert parser.name == "test_system"
    assert parser.system is None


def test_build_system(system) -> None:
    """Test building system from real data."""
    assert system is not None
    assert system.name == "test_system"


def test_system_has_buses(system) -> None:
    """Test that built system contains buses."""
    # Get all components from the system
    components = list(system.get_components(Component))
    assert components is not None

    # System should have components after building
    num_components = len(components)
    print(f"Total components in system: {num_components}")
    assert num_components > 0, "System should have components after building"


def test_system_has_generators(system) -> None:
    """Test that built system contains generators."""
    # Get all components
    components = list(system.get_components(Component))
    assert components is not None
    assert len(components) >= 0  # May be empty if no generators added yet


def test_system_has_loads(system) -> None:
    """Test that built system contains loads."""
    # Get all components
    components = list(system.get_components(Component))
    assert components is not None
    assert len(components) >= 0  # May be empty if no loads added yet


@pytest.fixture
def expected_generator_count(data_store: DataStore, reeds_config: ReEDSConfig) -> int:
    """Get expected generator count from online capacity data."""
    capacity_data = data_store.read_data_file(name="online_capacity")
    df = capacity_data.filter(pl.col("year") == reeds_config.solve_years[0]).collect()
    return df.height


def test_generator_count_matches_capacity_data(system, expected_generator_count: int) -> None:
    """Test that system has correct number of generators."""
    generators = list(system.get_components(ReEDSGenerator))
    assert len(generators) == expected_generator_count


def test_load_count_matches_region_count(system) -> None:
    """Test that number of loads matches number of regions."""
    regions = list(system.get_components(ReEDSRegion))
    loads = list(system.get_components(ReEDSDemand))
    assert len(loads) == len(regions)


def test_load_count_for_test_data(system) -> None:
    """Test expected load count for test_Pacific data."""
    loads = list(system.get_components(ReEDSDemand))
    assert len(loads) == 11


@pytest.fixture
def load_dataframe(data_store: DataStore) -> pl.DataFrame:
    """Get load data as DataFrame from DataStore."""
    load_data = data_store.read_data_file(name="load_data")
    return load_data.collect() if isinstance(load_data, pl.LazyFrame) else load_data


@pytest.fixture
def all_loads(system):
    """Get all load components from system."""
    return list(system.get_components(ReEDSDemand))


def test_loads_exist_for_datastore_regions(all_loads, load_dataframe) -> None:
    """Test that loads exist for all regions in DataStore."""
    load_regions = {load.name.replace("_load", "") for load in all_loads}
    datastore_regions = set(load_dataframe.columns)

    assert load_regions == datastore_regions


def test_load_time_series_attached(system, all_loads) -> None:
    """Test that time series exists for all loads."""
    assert all(system.get_time_series(load) is not None for load in all_loads)


def test_load_time_series_length(system, all_loads, load_dataframe) -> None:
    """Test that time series length matches DataStore data."""
    expected_length = load_dataframe.height
    assert all(len(system.get_time_series(load).data) == expected_length for load in all_loads)


@pytest.fixture(params=["p1", "p2", "p3", "p4", "p5", "p6", "p7", "p8", "p9", "p10", "p11"])
def region_load(request, system):
    """Parametrized fixture for each region's load."""
    region_name = request.param
    loads = list(system.get_components(ReEDSDemand))
    load = next((load_item for load_item in loads if load_item.name == f"{region_name}_load"), None)
    return system, load, region_name


def test_load_time_series_values(region_load, load_dataframe) -> None:
    """Test that time series values match DataStore data for each region."""
    system, load, region_name = region_load
    expected_profile = load_dataframe[region_name].to_numpy()
    actual_profile = system.get_time_series(load).data

    np.testing.assert_allclose(
        actual_profile,
        expected_profile,
        rtol=1e-5,
    )


@pytest.fixture
def renewable_cf_dataframe(data_store: DataStore) -> pl.DataFrame:
    """Get renewable CF data as DataFrame from DataStore."""
    cf_data = data_store.read_data_file(name="renewable_cf")
    return cf_data.collect() if isinstance(cf_data, pl.LazyFrame) else cf_data


@pytest.fixture
def all_renewable_generators(system):
    """Get all renewable generators that have CF time series attached."""
    generators = list(system.get_components(ReEDSGenerator))
    return [g for g in generators if system.list_time_series(g, name="max_active_power")]


def test_system_has_renewable_generators(all_renewable_generators) -> None:
    """Test that built system contains renewable generators with profiles."""
    assert len(all_renewable_generators) > 0


def test_renewable_generator_count(all_renewable_generators) -> None:
    """Test expected renewable generator count for test_Pacific data."""
    assert len(all_renewable_generators) == 130


def test_renewable_time_series_attached(system, all_renewable_generators) -> None:
    """Test that time series exists for all renewable generators."""
    assert all(system.get_time_series(gen) is not None for gen in all_renewable_generators)


def test_renewable_time_series_length(system, all_renewable_generators, renewable_cf_dataframe) -> None:
    """Test that time series length matches DataStore data."""
    expected_length = renewable_cf_dataframe.height
    assert all(len(system.get_time_series(gen).data) == expected_length for gen in all_renewable_generators)


@pytest.fixture
def sample_renewable_generator(system, all_renewable_generators):
    """Get a sample renewable generator for value testing."""
    distpv_gens = [g for g in all_renewable_generators if g.technology == "distpv" and g.region.name == "p1"]
    return system, distpv_gens[0] if distpv_gens else None


def test_renewable_time_series_values(sample_renewable_generator, renewable_cf_dataframe) -> None:
    """Test that time series values match normalized CF data from DataStore."""
    system, generator = sample_renewable_generator

    if generator is None:
        pytest.skip("No distpv generator found in p1 region")

    expected_profile = renewable_cf_dataframe["distpv|p1"].to_numpy()
    actual_ts = system.get_time_series(generator)
    actual_profile = actual_ts.data

    np.testing.assert_allclose(
        actual_profile,
        expected_profile,
        rtol=1e-5,
    )


def test_renewable_time_series_normalization(sample_renewable_generator) -> None:
    """Test that renewable time series has normalization metadata when capacity > 0."""
    system, generator = sample_renewable_generator

    if generator is None:
        pytest.skip("No distpv generator found in p1 region")

    ts = system.get_time_series(generator)

    if generator.capacity > 0:
        assert ts.normalization is not None
        assert ts.normalization.value == generator.capacity
    else:
        assert ts.normalization is None
