"""ReEDS parser implementation for r2x-core framework."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import h5py
import numpy as np
import polars as pl
from loguru import logger

from r2x_core.parser import BaseParser

from .models.base import FromTo_ToFrom
from .models.components import (
    ReEDSDemand,
    ReEDSEmission,
    ReEDSGenerator,
    ReEDSInterface,
    ReEDSRegion,
    ReEDSReserve,
    ReEDSTransmissionLine,
)

if TYPE_CHECKING:
    from r2x_core.store import DataStore

    from .config import ReEDSConfig


def read_reeds_load_h5(file_path: Path) -> pl.DataFrame:
    """Read ReEDS load.h5 files with complex HDF5 structure.

    ReEDS load files have a complex HDF5 structure with:
    - 'columns': array of region names
    - 'data': 2D array of load values (timesteps x regions)
    - 'index_datetime': datetime index
    - 'index_year': year index

    Parameters
    ----------
    file_path : Path
        Path to the load.h5 file

    Returns
    -------
    pl.DataFrame
        DataFrame with columns as region names and rows as timesteps
    """
    with h5py.File(file_path, "r") as f:
        # Read column names (region names) and data matrix
        columns = f["columns"][:].astype(str).tolist()
        data = f["data"][:]

        # Create DataFrame with region columns
        return pl.DataFrame({col: data[:, i] for i, col in enumerate(columns)})


class ReEDSParser(BaseParser):
    """Parser for ReEDS model data following r2x-core framework patterns.

    This parser builds an infrasys.System from ReEDS model data by:
    1. Reading component data from CSV files via DataStore
    2. Creating ReEDS-specific component instances
    3. Attaching time series data

    Parameters
    ----------
    config : ReEDSConfig
        ReEDS-specific configuration with solve years, weather years, etc.
    data_store : DataStore
        Initialized DataStore with ReEDS file mappings loaded
    name : str, optional
        Name for the system being built
    auto_add_composed_components : bool, default=True
        Whether to automatically add composed components
    skip_validation : bool, default=False
        Skip Pydantic validation for performance (use with caution)

    Examples
    --------
    Basic usage:

    >>> import json
    >>> from pathlib import Path
    >>> from r2x_core.store import DataStore
    >>> from r2x_reeds.config import ReEDSConfig
    >>> from r2x_reeds.parser import ReEDSParser
    >>>
    >>> # Load defaults and create DataStore
    >>> defaults = ReEDSConfig.load_defaults()
    >>> mapping_path = ReEDSConfig.get_file_mapping_path()
    >>> data_folder = Path("tests/data/test_Pacific")
    >>>
    >>> # Create config (defaults loaded separately, not passed to config)
    >>> config = ReEDSConfig(
    ...     solve_years=2030,
    ...     weather_years=2012,
    ...     case_name="High_Renewable",
    ... )
    >>>
    >>> # Create DataStore from file mapping
    >>> data_store = DataStore.from_json(mapping_path, folder=data_folder)
    >>>
    >>> # Create parser and build system
    >>> parser = ReEDSParser(config, data_store, name="ReEDS_System")
    >>> system = parser.build_system()
    """

    def __init__(
        self,
        config: ReEDSConfig,
        data_store: DataStore,
        *,
        name: str | None = None,
        auto_add_composed_components: bool = True,
        skip_validation: bool = False,
    ) -> None:
        """Initialize ReEDS parser.

        Parameters
        ----------
        config : ReEDSConfig
            ReEDS-specific configuration instance.
        data_store : DataStore
            Initialized DataStore with file mappings.
        name : str, optional
            Name for the system being built.
        auto_add_composed_components : bool, default=True
            Whether to automatically add composed components.
        skip_validation : bool, default=False
            Skip Pydantic validation for performance.
        """
        super().__init__(
            config,
            data_store,
            name=name,
            auto_add_composed_components=auto_add_composed_components,
            skip_validation=skip_validation,
        )

        # Update load_data DataFile with custom reader function
        # JSON can't store callable functions, so we update it programmatically
        if "load_data" in data_store:
            existing_load_file = data_store.get_data_file_by_name("load_data")
            updated_load_file = existing_load_file.model_copy(update={"reader_function": read_reeds_load_h5})
            data_store.add_data_file(updated_load_file, overwrite=True)

    def _setup_time_indices(self) -> None:
        """Create time indices for hourly and daily data."""
        # Use primary weather year for time series
        weather_year = self.config.primary_weather_year

        self.hourly_time_index = np.arange(
            f"{weather_year}",
            f"{weather_year + 1}",
            dtype="datetime64[h]",
        )

        self.daily_time_index = np.arange(
            f"{weather_year}",
            f"{weather_year + 1}",
            dtype="datetime64[D]",
        )

        logger.debug(
            "Created time indices for weather year {}: {} hours, {} days",
            weather_year,
            len(self.hourly_time_index),
            len(self.daily_time_index),
        )

    def validate_inputs(self) -> None:
        """Validate input data before building system.

        This method is called before build_system_components() and can be
        overridden to add custom validation logic. The base implementation
        does basic validation.
        """
        logger.info("Validating ReEDS input data...")

        # Check that required files exist in DataStore
        required_files = ["hierarchy", "modeledyears"]
        for file_name in required_files:
            if file_name not in self.data_store:
                msg = f"Required file '{file_name}' not found in DataStore"
                raise ValueError(msg)

        # Validate solve years are in modeled years
        modeled_years_data = self.data_store.read_data_file(name="modeledyears")
        if modeled_years_data is not None:
            # Extract modeled years from the data
            # This will depend on the actual structure of modeledyears file
            logger.debug("Found modeled years data: {}", modeled_years_data)

        logger.info("Input validation complete")

    def build_system_components(self) -> None:
        """Create all system components from ReEDS data.

        This method reads component data from the DataStore and creates:
        - Regions (buses)
        - Generators
        - Transmission interfaces
        - Loads
        - Reserves
        - Emissions
        """
        logger.info("Building ReEDS system components...")

        # Setup time indices (needed for component building)
        self._setup_time_indices()

        # Initialize component caches for building phase
        self._region_cache: dict[str, Any] = {}
        self._generator_cache: dict[str, Any] = {}

        # Build components in dependency order
        self._build_regions()
        self._build_generators()
        self._build_transmission()
        self._build_loads()
        self._build_reserves()
        self._build_emissions()

        # Count total components
        total_components = len(list(self.system.get_components()))
        logger.info(
            "Built {} total components: regions, generators, transmission, loads, reserves, emissions",
            total_components,
        )

    def _build_regions(self) -> None:
        """Build region components from hierarchy data."""
        logger.info("Building regions...")

        # Read hierarchy data (returns LazyFrame, need to collect)
        hierarchy_data = self.read_data_file("hierarchy").collect()
        if hierarchy_data is None:
            logger.warning("No hierarchy data found, skipping regions")
            return

        # Process each region
        region_count = 0
        for row in hierarchy_data.iter_rows(named=True):
            # Column mapping should map *r to region_id
            region_name = row.get("region_id") or row.get("region") or row.get("r") or row.get("*r")
            if not region_name:
                continue

            # Create region component
            region = self.create_component(
                ReEDSRegion,
                name=region_name,
                description=f"ReEDS region {region_name}",  # Keep f-string for component data
                category=row.get("region_type"),
                state=row.get("state") or row.get("st"),
            )

            self.add_component(region)
            self._region_cache[region_name] = region
            region_count += 1

        logger.info("Built {} regions", region_count)

    def _build_generators(self) -> None:
        """Build generator components from capacity data."""
        logger.info("Building generators...")

        # Read capacity data for non-renewable resources (returns LazyFrame)
        cap_data = self.read_data_file("existing_capacity").collect()
        if cap_data is None:
            logger.warning("No capacity data found, skipping generators")
            return

        # Note: existing_capacity is static data without year field
        gen_count = 0

        for row in cap_data.iter_rows(named=True):
            region = row.get("region") or row.get("r")
            tech = row.get("technology") or row.get("tech") or row.get("i")
            capacity = row.get("capacity_mw") or row.get("capacity") or row.get("value", 0.0)

            if not region or not tech:
                continue

            # Lookup the region object from cache
            region_obj = self._region_cache.get(region)
            if not region_obj:
                logger.warning("Region '{}' not found for generator {}, skipping", region, tech)
                continue

            # Create generator component
            gen_name = f"{region}_{tech}"
            generator = self.create_component(
                ReEDSGenerator,
                name=gen_name,
                description=f"Generator {tech} in {region}",  # Keep f-string for component data
                region=region_obj,  # Pass the ReEDSRegion object, not string
                technology=tech,
                capacity_mw=float(capacity),
            )

            self.add_component(generator)
            self._generator_cache[gen_name] = generator
            gen_count += 1

        logger.info("Built {} generators", gen_count)

    def _build_transmission(self) -> None:
        """Build transmission interface and line components."""
        logger.info("Building transmission interfaces...")

        # Read transmission data files
        trancap_data = self.read_data_file("transmission_capacity")
        if trancap_data is None:
            logger.warning("No transmission capacity data found, skipping transmission")
            return

        trancap = trancap_data.collect()
        if trancap.is_empty():
            logger.warning("Transmission capacity data is empty, skipping")
            return

        # Build transmission lines
        line_count = 0
        interface_cache: dict[str, Any] = {}

        for row in trancap.iter_rows(named=True):
            from_region_name = row.get("from_region") or row.get("r")
            to_region_name = row.get("to_region") or row.get("rr")

            if not from_region_name or not to_region_name:
                continue

            # Get region objects
            from_region = self._region_cache.get(from_region_name)
            to_region = self._region_cache.get(to_region_name)

            if not from_region or not to_region:
                logger.debug("Skipping line {}-{}: region not found", from_region_name, to_region_name)
                continue

            # Create or get interface
            interface_name = f"{from_region_name}||{to_region_name}"
            if interface_name not in interface_cache:
                interface = self.create_component(
                    ReEDSInterface,
                    name=interface_name,
                    from_region=from_region,
                    to_region=to_region,
                )
                self.add_component(interface)
                interface_cache[interface_name] = interface
            else:
                interface = interface_cache[interface_name]

            # Create transmission line
            line_type = row.get("trtype", "AC")
            capacity = row.get("capacity_mw") or row.get("value", 0.0)

            line_name = f"{interface_name}_{line_type}"

            # Create bidirectional capacity (assuming symmetric for now)
            max_active_power = FromTo_ToFrom(
                name=f"{line_name}_capacity", from_to=float(capacity), to_from=float(capacity)
            )

            line = self.create_component(
                ReEDSTransmissionLine,
                name=line_name,
                interface=interface,
                max_active_power=max_active_power,
                line_type=line_type,
            )

            self.add_component(line)
            line_count += 1

        logger.info("Built {} interfaces and {} transmission lines", len(interface_cache), line_count)

    def _build_loads(self) -> None:
        """Build load components from demand data."""
        logger.info("Building loads...")

        # Read load data using the custom reader function configured in file_mapping.json
        load_data = self.read_data_file("load_data")
        if load_data is None:
            logger.warning("No load data found, skipping loads")
            return

        # Ensure we have a DataFrame (the custom reader returns pl.DataFrame)
        df = load_data.collect() if isinstance(load_data, pl.LazyFrame) else load_data

        if df.is_empty():
            logger.warning("Load data is empty, skipping loads")
            return

        logger.debug("Load data has {} regions and {} timesteps", len(df.columns), df.height)

        load_count = 0

        # For each region that has a load column
        for region_name, region_obj in self._region_cache.items():
            if region_name not in df.columns:
                logger.debug("No load data for region {}", region_name)
                continue

            # Extract load profile for this region
            load_profile = df[region_name].to_numpy()
            peak_load = float(load_profile.max())

            # Create demand component
            demand = self.create_component(
                ReEDSDemand,
                name=f"{region_name}_load",
                region=region_obj,
                max_active_power=peak_load,
            )

            self.add_component(demand)
            load_count += 1

            # Note: Time series attachment will be implemented in build_time_series()

        logger.info("Built {} load components", load_count)

    def _build_reserves(self) -> None:
        """Build reserve requirement components."""
        logger.info("Building reserves...")

        # Read hierarchy data to get transmission regions
        hierarchy_data = self.read_data_file("hierarchy")
        if hierarchy_data is None:
            logger.warning("No hierarchy data found, skipping reserves")
            return

        df = hierarchy_data.collect()
        if df.is_empty():
            logger.warning("Hierarchy data is empty, skipping reserves")
            return

        # Get defaults for reserve configuration
        defaults = self.config.load_defaults()
        reserve_types = defaults.get("default_reserve_types", [])
        reserve_duration = defaults.get("reserve_duration", {})
        reserve_time_frame = defaults.get("reserve_time_frame", {})
        reserve_vors = defaults.get("reserve_vors", {})

        if not reserve_types:
            logger.debug("No reserve types configured, skipping reserves")
            return

        # Get unique transmission regions
        if "transmission_region" in df.columns:
            transmission_regions = df["transmission_region"].unique().to_list()
        else:
            transmission_regions = []

        reserve_count = 0
        for region_name in transmission_regions:
            for reserve_type in reserve_types:
                duration = reserve_duration.get(reserve_type)
                time_frame = reserve_time_frame.get(reserve_type)
                vors = reserve_vors.get(reserve_type)

                reserve = self.create_component(
                    ReEDSReserve,
                    name=f"{region_name}_{reserve_type}",
                    reserve_type=reserve_type,
                    duration=duration,
                    time_frame=time_frame,
                    vors=vors,
                )

                self.add_component(reserve)
                reserve_count += 1

        logger.info("Built {} reserve components", reserve_count)

    def _build_emissions(self) -> None:
        """Build emission components and attach to generators as supplemental attributes."""
        logger.info("Building emissions...")

        # Read emission rates data
        emit_data = self.read_data_file("emission_rates")
        if emit_data is None:
            logger.warning("No emission rates data found, skipping emissions")
            return

        df = emit_data.collect()
        if df.is_empty():
            logger.warning("Emission rates data is empty, skipping emissions")
            return

        # Attach emissions as supplemental attributes to generators
        emission_count = 0

        for row in df.iter_rows(named=True):
            tech = row.get("technology") or row.get("tech") or row.get("i")
            region = row.get("region") or row.get("r")
            emission_type = row.get("emission_type") or row.get("e")
            rate = row.get("emission_rate") or row.get("rate") or row.get("value")
            emission_source = row.get("emission_source", "combustion")

            if not tech or not region or not emission_type or rate is None:
                continue

            # Only process combustion emissions (following old parser logic)
            if emission_source != "combustion":
                continue

            # Normalize emission type to uppercase for enum validation
            emission_type = str(emission_type).upper()

            # Find the generator to attach this emission to
            gen_name = f"{region}_{tech}"
            generator = self._generator_cache.get(gen_name)

            if not generator:
                logger.debug("Generator {} not found for emission {}, skipping", gen_name, emission_type)
                continue

            # Create emission supplemental attribute
            emission = ReEDSEmission(
                emission_type=emission_type,
                rate=float(rate),
            )

            # Add as supplemental attribute to the generator
            self.system.add_supplemental_attribute(generator, emission)
            emission_count += 1

        logger.info("Attached {} emissions to generators", emission_count)

    def build_time_series(self) -> None:
        """Attach time series data to components.

        This method reads time series data from DataStore and attaches it
        to the appropriate components:
        - Load profiles
        - Renewable generation profiles
        - Reserve requirements
        """
        logger.info("Building time series data...")

        self._attach_load_profiles()
        self._attach_renewable_profiles()
        self._attach_reserve_profiles()

        logger.info("Time series attachment complete")

    def _attach_load_profiles(self) -> None:
        """Attach load time series to demand components."""
        logger.info("Attaching load profiles...")
        # Placeholder - implement load profile attachment
        logger.debug("Load profile attachment not yet implemented")

    def _attach_renewable_profiles(self) -> None:
        """Attach renewable capacity factor profiles to generators."""
        logger.info("Attaching renewable profiles...")
        # Placeholder - implement renewable profile attachment
        # Will read recf.h5
        logger.debug("Renewable profile attachment not yet implemented")

    def _attach_reserve_profiles(self) -> None:
        """Attach reserve requirement profiles."""
        logger.info("Attaching reserve profiles...")
        # Placeholder - implement reserve profile attachment
        logger.debug("Reserve profile attachment not yet implemented")

    def post_process_system(self) -> None:
        """Perform post-processing on the built system.

        This optional hook can be used for:
        - Data validation and consistency checks
        - Derived attribute calculations
        - System-level aggregations
        - Metadata enrichment
        """
        logger.info("Post-processing ReEDS system...")

        # Set system metadata
        self.system.data_format_version = "ReEDS v1.0"
        self.system.description = (
            f"ReEDS model system for case '{self.config.case_name}', "
            f"scenario '{self.config.scenario}', "
            f"solve years: {self.config.solve_years}, "
            f"weather years: {self.config.weather_years}"
        )

        # Log summary statistics
        total_components = len(list(self.system.get_components()))
        logger.info("System name: {}", self.system.name)
        logger.info("Total components: {}", total_components)
        logger.info("Post-processing complete")
