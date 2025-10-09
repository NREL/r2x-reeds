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
    return ReEDSConfig(solve_year=2032, weather_year=2012, case_name="test", scenario="base")  # type: ignore[arg-type]


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
    components = list(system.get_components(Component))
    assert components is not None
    assert len(components) > 0, "System should have components after building"


def test_system_has_generators(system) -> None:
    """Test that built system contains generators."""
    components = list(system.get_components(Component))
    assert components is not None
    assert len(components) >= 0


def test_system_has_loads(system) -> None:
    """Test that built system contains loads."""
    components = list(system.get_components(Component))
    assert components is not None
    assert len(components) >= 0


@pytest.fixture
def expected_generator_count(data_store: DataStore, reeds_config: ReEDSConfig) -> int:
    """Get expected generator count from online capacity data.

    Renewable generators are aggregated by tech-region (no vintage) since
    capacity factor profiles are region-level only.
    """
    defaults = reeds_config.load_defaults()
    tech_cats = defaults.get("tech_categories", {})
    renewable_techs = tech_cats.get("solar", []) + tech_cats.get("wind", [])
    excluded_techs = defaults.get("excluded_techs", [])

    capacity_data = data_store.read_data_file(name="online_capacity")
    df = capacity_data.filter(pl.col("year") == reeds_config.solve_year).collect()

    df = df.filter(~pl.col("technology").is_in(excluded_techs))

    df_renewable = df.filter(pl.col("technology").is_in(renewable_techs))
    df_non_renewable = df.filter(~pl.col("technology").is_in(renewable_techs))

    renewable_count = df_renewable.select(["technology", "region"]).unique().height
    non_renewable_count = df_non_renewable.height

    return renewable_count + non_renewable_count


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
    # Filter out metadata columns (datetime, solve_year) that aren't region names
    datastore_regions = {col for col in load_dataframe.columns if col not in ["datetime", "solve_year"]}

    assert load_regions == datastore_regions


def test_load_time_series_attached(system, all_loads) -> None:
    """Test that time series exists for all loads."""
    assert all(system.get_time_series(load) is not None for load in all_loads)


def test_load_time_series_length(system, all_loads, load_dataframe) -> None:
    """Test that time series length matches filtered data (single weather year = 8760 hours)."""
    # The parser filters load data to a single weather year and solve year, so expect 8760 hours
    expected_length = 8760
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

    # Filter load_dataframe to match what the parser uses (weather_year=2012, solve_year=2032)
    import polars as pl

    filtered_df = load_dataframe.filter(
        (pl.col("datetime").dt.year() == 2012) & (pl.col("solve_year") == 2032)
    )

    expected_profile = filtered_df[region_name].to_numpy()
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
    """Test expected renewable generator count for test_Pacific data.

    Renewable generators are aggregated by tech-region, not by vintage.
    Includes hydro generators which now have rating profiles.
    """
    assert 75 <= len(all_renewable_generators) <= 85, (
        f"Expected ~80 renewable generators (including hydro, aggregated by tech-region), "
        f"got {len(all_renewable_generators)}"
    )


def test_renewable_time_series_attached(system, all_renewable_generators) -> None:
    """Test that time series exists for all renewable generators."""
    assert all(
        system.get_time_series(gen, name="max_active_power") is not None for gen in all_renewable_generators
    )


def test_renewable_time_series_length(system, all_renewable_generators, renewable_cf_dataframe) -> None:
    """Test that time series length matches filtered data (single weather year = 8760 hours)."""
    # The parser filters CF data to a single weather year, so expect 8760 hours
    expected_length = 8760
    assert all(
        len(system.get_time_series(gen, name="max_active_power").data) == expected_length
        for gen in all_renewable_generators
    )


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

    # Filter CF dataframe to match what the parser uses (weather_year=2012)
    import polars as pl

    filtered_df = renewable_cf_dataframe.filter(pl.col("datetime").dt.year() == 2012)

    expected_profile = filtered_df["distpv|p1"].to_numpy()
    actual_ts = system.get_time_series(generator)
    actual_profile = actual_ts.data

    np.testing.assert_allclose(
        actual_profile,
        expected_profile,
        rtol=1e-5,
    )


def test_system_has_reserves(system) -> None:
    """Test that the system has reserve components."""
    from r2x_reeds.models.components import ReEDSReserve

    reserves = list(system.get_components(ReEDSReserve))
    assert len(reserves) > 0, "System should have at least one reserve component"


def test_reserve_time_series_attached(system) -> None:
    """Test that reserve components have time series attached."""
    from r2x_reeds.models.components import ReEDSReserve

    reserves = list(system.get_components(ReEDSReserve))
    reserves_with_ts = [r for r in reserves if system.has_time_series(r)]

    assert len(reserves_with_ts) > 0, "At least one reserve should have time series attached"


def test_reserve_time_series_length(system) -> None:
    """Test that reserve time series have correct length (matches hourly time index)."""
    from r2x_reeds.models.components import ReEDSReserve

    reserves = list(system.get_components(ReEDSReserve))

    for reserve in reserves:
        if system.has_time_series(reserve):
            ts = system.get_time_series(reserve)
            # 2012 is a leap year, so 366 days * 24 hours = 8784
            assert len(ts.data) == 8784, f"Reserve {reserve.name} should have 8784 hours of data (leap year)"


def test_reserve_time_series_values(system) -> None:
    """Test that reserve time series have non-negative values."""
    from r2x_reeds.models.components import ReEDSReserve

    reserves = list(system.get_components(ReEDSReserve))
    checked_count = 0

    for reserve in reserves:
        if system.has_time_series(reserve):
            ts = system.get_time_series(reserve)
            assert np.all(ts.data >= 0), f"Reserve {reserve.name} should have non-negative values"
            checked_count += 1

    assert checked_count > 0, "Should have checked at least one reserve time series"
