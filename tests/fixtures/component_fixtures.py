"""Component creation fixtures for testing."""

import pytest


@pytest.fixture
def sample_region():
    """Create a sample ReEDS region."""
    from r2x_reeds.models import ReEDSRegion

    return ReEDSRegion(
        name="p1",
        state="CA",
        nerc_region="WECC_CA",
        transmission_region="CAISO",
        transmission_group="CAISO",
        interconnect="western",
        country="USA",
        cendiv="Pacific",
        usda_region="pacific",
        h2ptc_region="California",
        hurdle_region="CAISO",
        cc_region="CAISO",
    )


@pytest.fixture
def thermal_generator(sample_region):
    """Create a sample thermal generator."""
    from r2x_reeds.models import ReEDSThermalGenerator

    return ReEDSThermalGenerator(
        name="gas-cc_init-1_p1",
        region=sample_region,
        technology="gas-cc",
        capacity=500.0,
        heat_rate=7.5,
        fuel_type="naturalgas",
    )


@pytest.fixture
def renewable_generator(sample_region):
    """Create a sample renewable generator."""
    from r2x_reeds.models import ReEDSVariableGenerator

    return ReEDSVariableGenerator(
        name="upv_p1",
        region=sample_region,
        technology="upv",
        capacity=300.0,
    )


@pytest.fixture
def storage_generator(sample_region):
    """Create a sample storage generator."""
    from r2x_reeds.models import ReEDSStorage

    return ReEDSStorage(
        name="battery_li_p1",
        region=sample_region,
        technology="battery_li",
        capacity=100.0,
        storage_duration=4.0,
        round_trip_efficiency=0.85,
    )


@pytest.fixture
def hydro_generator(sample_region):
    """Create a sample hydro generator."""
    from r2x_reeds.models import ReEDSHydroGenerator

    return ReEDSHydroGenerator(
        name="hyd_p1",
        region=sample_region,
        technology="hyd",
        capacity=200.0,
        is_dispatchable=True,
    )


@pytest.fixture
def consuming_technology(sample_region):
    """Create a sample consuming technology."""
    from r2x_reeds.models import ReEDSConsumingTechnology

    return ReEDSConsumingTechnology(
        name="electrolyzer_p1",
        region=sample_region,
        technology="electrolyzer",
        capacity=150.0,
        electricity_consumption_rate=51.45,
    )


@pytest.fixture
def h2_storage(sample_region):
    """Create a sample H2 storage."""
    from r2x_reeds.models import ReEDSH2Storage

    return ReEDSH2Storage(
        name="h2_storage_saltcavern_p1",
        region=sample_region,
        storage_type="saltcavern",
        capacity=1000.0,
        capital_cost=26304.0,
        fom_cost=763.0,
        parasitic_load=0.008,
    )


@pytest.fixture
def h2_pipeline(sample_region):
    """Create a sample H2 pipeline."""
    from r2x_reeds.models import ReEDSH2Pipeline

    region2 = sample_region.model_copy(update={"name": "p2"})
    return ReEDSH2Pipeline(
        name="h2_pipeline_p1_p2",
        from_region=sample_region,
        to_region=region2,
        capacity=500.0,
        distance=100.0,
        capital_cost=34045.0,
        fom_cost=963.0,
    )


@pytest.fixture
def emission():
    """Create a sample emission attribute."""
    from r2x_reeds.models import EmissionSource, EmissionType, ReEDSEmission

    return ReEDSEmission(rate=0.45, source=EmissionSource.COMBUSTION, type=EmissionType.CO2)
