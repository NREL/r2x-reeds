"""ReEDS parser implementation for r2x-core framework."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import numpy as np
from loguru import logger

from r2x_core.parser import BaseParser

if TYPE_CHECKING:
    from r2x_core.store import DataStore

    from .config import ReEDSConfig


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
            f"Created time indices for weather year {weather_year}: "
            f"{len(self.hourly_time_index)} hours, {len(self.daily_time_index)} days"
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
            logger.debug(f"Found modeled years data: {modeled_years_data}")

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
            f"Built {total_components} total components: "
            f"regions, generators, transmission, loads, reserves, emissions"
        )

    def _build_regions(self) -> None:
        """Build region components from hierarchy data."""
        from .models import ReEDSRegion

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
                description=f"ReEDS region {region_name}",
                category=row.get("region_type"),
                state=row.get("state") or row.get("st"),
            )

            self.add_component(region)
            self._region_cache[region_name] = region
            region_count += 1

        logger.info(f"Built {region_count} regions")

    def _build_generators(self) -> None:
        """Build generator components from capacity data."""
        from .models import ReEDSGenerator

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
                logger.warning(f"Region '{region}' not found for generator {tech}, skipping")
                continue

            # Create generator component
            gen_name = f"{region}_{tech}"
            generator = self.create_component(
                ReEDSGenerator,
                name=gen_name,
                description=f"Generator {tech} in {region}",
                region=region_obj,  # Pass the ReEDSRegion object, not string
                technology=tech,
                capacity_mw=float(capacity),
            )

            self.add_component(generator)
            self._generator_cache[gen_name] = generator
            gen_count += 1

        logger.info(f"Built {gen_count} generators")

    def _build_transmission(self) -> None:
        """Build transmission interface components."""
        logger.info("Building transmission interfaces...")
        # Placeholder - implement transmission parsing
        # Will read transmission_capacity_init_AC_r.csv and tranloss.csv
        logger.debug("Transmission parsing not yet implemented")

    def _build_loads(self) -> None:
        """Build load components from demand data."""
        logger.info("Building loads...")
        # Placeholder - implement load parsing
        # Will read load.h5 file
        logger.debug("Load parsing not yet implemented")

    def _build_reserves(self) -> None:
        """Build reserve requirement components."""
        logger.info("Building reserves...")
        # Placeholder - implement reserve parsing
        logger.debug("Reserve parsing not yet implemented")

    def _build_emissions(self) -> None:
        """Build emission components."""
        logger.info("Building emissions...")
        # Placeholder - implement emission parsing
        # Will read co2_cap.csv
        logger.debug("Emission parsing not yet implemented")

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
        logger.info(f"System name: {self.system.name}")
        logger.info(f"Total components: {total_components}")
        logger.info("Post-processing complete")
