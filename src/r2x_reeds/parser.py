"""ReEDS parser implementation for r2x-core framework."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

import numpy as np
import polars as pl
from infrasys import Component
from infrasys.time_series_models import SingleTimeSeries
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
from .models.enums import EmissionType, ReserveDirection, ReserveType

if TYPE_CHECKING:
    from r2x_core.store import DataStore

    from .config import ReEDSConfig


class ReEDSParser(BaseParser):
    """Parser for ReEDS model data following r2x-core framework patterns.

    The parser builds an infrasys.System from ReEDS model output by:

    1. **Component Building** (`build_system_components`):
       - Regions from hierarchy data (buses with regional attributes)
       - Generators split into renewable (aggregated by tech-region) and non-renewable (with vintage)
       - Transmission interfaces and lines with bi-directional ratings
       - Loads with peak demand by region
       - Reserves by transmission region and type
       - Emissions as supplemental attributes on generators

    2. **Time Series Attachment** (`build_time_series`):
       - Load profiles filtered by weather year and solve year
       - Renewable capacity factors from CF data
       - Reserve requirements calculated from wind/solar/load contributions

    3. **Post-Processing** (`post_process_system`):
       - System metadata and description

    Key Implementation Details
    --------------------------
    - Renewable generators are aggregated by technology and region (no vintage)
    - Non-renewable generators retain vintage information
    - Reserve requirements are calculated dynamically based on wind capacity, solar capacity,
      and load data with configurable percentage contributions
    - Time series data is filtered to match configured weather years and solve years
    - Component caches are used during building for efficient cross-referencing

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
        """Initialize ReEDS parser."""
        super().__init__(
            config,
            data_store,
            name=name,
            auto_add_composed_components=auto_add_composed_components,
            skip_validation=skip_validation,
        )

        self.config: ReEDSConfig = config

        self._filtered_load_data: pl.DataFrame | None = None
        self._filtered_cf_data: pl.DataFrame | None = None

    def validate_inputs(self) -> None:
        """Validate input data before building system."""
        logger.info("Validating ReEDS input data...")

        # Get file paths from DataStore
        modeledyears_file = self.data_store.get_data_file_by_name("modeledyears")
        hour_map_file = self.data_store.get_data_file_by_name("hour_map")

        modeled_years_data = self.read_data_file("modeledyears")
        if modeled_years_data is None or modeled_years_data.limit(1).collect().is_empty():
            msg = f"{modeledyears_file.fpath} is empty or missing. Check input folder."
            raise ValueError(msg)

        hour_map_data = self.read_data_file("hour_map")
        if hour_map_data is None or hour_map_data.limit(1).collect().is_empty():
            msg = f"{hour_map_file.fpath} is empty or missing. Check input folder."
            raise ValueError(msg)

        solve_years = (
            [self.config.solve_year]
            if isinstance(self.config.solve_year, int)
            else list(self.config.solve_year)
        )
        available_solve_years = set(modeled_years_data.collect().row(0))
        missing_solve_years = [y for y in solve_years if y not in available_solve_years]
        if missing_solve_years:
            msg = f"Solve year(s) {missing_solve_years} not found in {modeledyears_file.fpath}. "
            msg += f"Available years: {sorted(available_solve_years)}"
            raise ValueError(msg)

        weather_years = (
            [self.config.weather_year]
            if isinstance(self.config.weather_year, int)
            else list(self.config.weather_year)
        )
        available_weather_years = set(
            hour_map_data.select(pl.col("year")).unique().collect().to_series().to_list()
        )
        missing_weather_years = [y for y in weather_years if y not in available_weather_years]
        if missing_weather_years:
            msg = f"Weather year(s) {missing_weather_years} not found in {hour_map_file.fpath}. "
            msg += f"Available years: {sorted(available_weather_years)}"
            raise ValueError(msg)

        logger.info("Input validation complete")

    def _tech_matches_category(self, tech: str, category_name: str, defaults: dict[str, Any]) -> bool:
        """Check if a technology matches a category using prefix or exact matching.

        Parameters
        ----------
        tech : str
            Technology name to check
        category_name : str
            Category name from tech_categories
        defaults : dict
            Defaults dictionary containing tech_categories

        Returns
        -------
        bool
            True if technology matches the category
        """
        tech_categories = defaults.get("tech_categories", {})
        if category_name not in tech_categories:
            return False

        category = tech_categories[category_name]

        if isinstance(category, list):
            return tech in category

        prefixes = category.get("prefixes", [])
        exact = category.get("exact", [])

        if tech in exact:
            return True

        return any(tech.startswith(prefix) for prefix in prefixes)

    def _get_tech_category(self, tech: str, defaults: dict[str, Any]) -> str | None:
        """Get the category for a technology.

        Parameters
        ----------
        tech : str
            Technology name
        defaults : dict
            Defaults dictionary containing tech_categories

        Returns
        -------
        str | None
            Category name if found, None otherwise
        """
        tech_categories = defaults.get("tech_categories", {})
        for category_name in tech_categories:
            category_name_str: str = str(category_name)
            if self._tech_matches_category(tech, category_name_str, defaults):
                return category_name_str
        return None

    def build_system_components(self) -> None:
        """Create all system components from ReEDS data.

        Components are built in dependency order:
        regions → generators → transmission → loads → reserves → emissions
        """
        logger.info("Building ReEDS system components...")

        self._setup_time_indices()

        self._region_cache: dict[str, Any] = {}
        self._generator_cache: dict[str, Any] = {}
        self._interface_cache: dict[str, Any] = {}

        self._build_regions()
        self._build_generators()
        self._build_transmission()
        self._build_loads()
        self._build_reserves()
        self._build_emissions()

        total_components = len(list(self.system.get_components(Component)))
        logger.info(
            "Built {} total components: regions, generators, transmission, loads, reserves, emissions",
            total_components,
        )

    def build_time_series(self) -> None:
        """Attach time series data to components."""
        logger.info("Building time series data...")
        self._attach_load_profiles()
        self._attach_renewable_profiles()
        self._attach_reserve_profiles()
        self._attach_hydro_budgets()
        self._attach_hydro_rating_profiles()
        logger.info("Time series attachment complete")

    def post_process_system(self) -> None:
        """Perform post-processing on the built system."""
        logger.info("Post-processing ReEDS system...")

        self.system.data_format_version = "ReEDS v1.0"
        self.system.description = (
            f"ReEDS model system for case '{self.config.case_name}', "
            f"scenario '{self.config.scenario}', "
            f"solve years: {self.config.solve_year}, "
            f"weather years: {self.config.weather_year}"
        )

        total_components = len(list(self.system.get_components(Component)))
        logger.info("System name: {}", self.system.name)
        logger.info("Total components: {}", total_components)
        logger.info("Post-processing complete")

    def _setup_time_indices(self) -> None:
        """Create time indices for hourly and daily data."""
        weather_year = self.config.primary_weather_year

        self.hourly_time_index = np.arange(f"{weather_year}", f"{weather_year + 1}", dtype="datetime64[h]")
        self.daily_time_index = np.arange(f"{weather_year}", f"{weather_year + 1}", dtype="datetime64[D]")

        logger.debug(
            "Created time indices for weather year {}: {} hours, {} days",
            weather_year,
            len(self.hourly_time_index),
            len(self.daily_time_index),
        )

    def _build_regions(self) -> None:
        """Build region components from hierarchy data.

        Creates ReEDSRegion components with all hierarchical attributes
        (state, NERC region, transmission region, interconnect, etc.).
        """
        logger.info("Building regions...")

        hierarchy_data = self.read_data_file("hierarchy").collect()
        if hierarchy_data is None:
            logger.warning("No hierarchy data found, skipping regions")
            return

        region_count = 0
        for row in hierarchy_data.iter_rows(named=True):
            region_name = row.get("region_id") or row.get("region") or row.get("r") or row.get("*r")
            if not region_name:
                continue

            region = self.create_component(
                ReEDSRegion,
                name=region_name,
                description=f"ReEDS region {region_name}",
                category=row.get("region_type"),
                state=row.get("state") or row.get("st"),
                nerc_region=row.get("nerc_region") or row.get("nercr"),
                transmission_region=row.get("transmission_region") or row.get("transreg"),
                transmission_group=row.get("transmission_group") or row.get("transgrp"),
                interconnect=row.get("interconnect"),
                country=row.get("country"),
                timezone=row.get("timezone"),
                cendiv=row.get("cendiv"),
                usda_region=row.get("usda_region"),
                h2ptc_region=row.get("h2ptc_region") or row.get("h2ptcreg"),
                hurdle_region=row.get("hurdle_region") or row.get("hurdlereg"),
                cc_region=row.get("cc_region") or row.get("ccreg"),
            )

            self.add_component(region)
            self._region_cache[region_name] = region
            region_count += 1

        logger.info("Built {} regions", region_count)

    def _build_generators(self) -> None:
        """Build generator components from capacity data.

        Renewable generators (wind/solar) are aggregated by technology and region.
        Non-renewable generators retain vintage information for tracking.
        Joins capacity with fuel prices, heat rates, outage rates, and other attributes.
        """
        logger.info("Building generators...")

        capacity_data = self.read_data_file("online_capacity")
        if capacity_data is None:
            logger.warning("No capacity data found, skipping generators")
            return

        solve_year = (
            self.config.solve_year[0] if isinstance(self.config.solve_year, list) else self.config.solve_year
        )
        capacity_data = capacity_data.filter(pl.col("year") == solve_year)

        df = capacity_data
        fuel_price = self.read_data_file("fuel_price")
        biofuel = self.read_data_file("biofuel_price")
        gen_fuel = self.read_data_file("fuel_tech_map")
        if biofuel is not None and gen_fuel is not None:
            biofuel_mapped = (
                biofuel.with_columns(pl.lit("biomass").alias("fuel_type"))
                .join(gen_fuel, on="fuel_type")
                .select(pl.exclude("fuel_type"))
            )
            if not biofuel_mapped.collect().is_empty():
                fuel_price = pl.concat([fuel_price, biofuel_mapped], how="diagonal")

        for next_df in [
            self.read_data_file("fuel_tech_map"),
            fuel_price,
            self.read_data_file("heat_rate"),
            self.read_data_file("cost_vom"),
            self.read_data_file("forced_outages"),
            self.read_data_file("planned_outages"),
            self.read_data_file("maxage"),
        ]:
            if next_df is not None:
                common_cols = list(set(df.collect_schema().names()) & set(next_df.collect_schema().names()))
                df = df.join(next_df, how="left", on=common_cols)

        df = df.collect()
        if df.is_empty():
            logger.warning("Generator data is empty, skipping generators")
            return

        # Filter out excluded technologies
        defaults = self.config.load_defaults()
        excluded_techs = defaults.get("excluded_techs", [])
        if excluded_techs:
            initial_count = len(df)
            df = df.filter(~pl.col("technology").is_in(excluded_techs))
            excluded_count = initial_count - len(df)
            if excluded_count > 0:
                logger.info("Excluded {} generators with technologies in excluded_techs list", excluded_count)

        if df.is_empty():
            logger.warning("All generators were excluded, skipping generators")
            return

        # Assign categories to technologies using pattern matching
        df = df.with_columns(
            pl.col("technology")
            .map_elements(
                lambda tech: self._get_tech_category(tech, defaults),
                return_dtype=pl.String,
            )
            .alias("category")
        )

        # Separate renewable from non-renewable generators
        df_renewable = df.filter(pl.col("category").is_in(["wind", "solar"]))
        df_non_renewable = df.filter(
            (~pl.col("category").is_in(["wind", "solar"])) | pl.col("category").is_null()
        )

        renewable_count = 0
        if not df_renewable.is_empty():
            agg_cols = [
                "heat_rate",
                "forced_outage_rate",
                "planned_outage_rate",
                "maxage_years",
                "fuel_type",
                "fuel_price",
                "vom_price",
            ]
            agg_exprs = [pl.col("value").sum().alias("capacity_mw")] + [
                pl.col(col).first() if col in df_renewable.columns else pl.lit(None).alias(col)
                for col in agg_cols
            ]
            for row in (
                df_renewable.group_by(["technology", "region", "category"])
                .agg(agg_exprs)
                .iter_rows(named=True)
            ):
                if (tech := row.get("technology")) and (region := row.get("region")):
                    self._create_generator(tech, region, None, row, gen_suffix=f"{region}")
                    renewable_count += 1

        gen_count = 0
        for row in df_non_renewable.iter_rows(named=True):
            if (tech := row.get("technology")) and (region := row.get("region")):
                self._create_generator(tech, region, row.get("vintage"), row)
                gen_count += 1

        total_gen_count = renewable_count + gen_count
        if total_gen_count == 0:
            logger.warning("No generators were created")
        else:
            logger.info(
                "Built {} generators ({} renewable aggregated, {} non-renewable)",
                total_gen_count,
                renewable_count,
                gen_count,
            )

    def _build_transmission(self) -> None:
        """Build transmission interface and line components.

        Creates bi-directional transmission lines with separate forward/reverse ratings.
        Uses canonical alphabetical ordering for interface naming to avoid duplicates.
        """
        logger.info("Building transmission interfaces...")

        trancap_data = self.read_data_file("transmission_capacity")
        if trancap_data is None:
            logger.warning("No transmission capacity data found, skipping transmission")
            return

        trancap = trancap_data.collect()

        if trancap.is_empty():
            logger.warning("Transmission capacity data is empty, skipping transmission")
            return

        line_count = 0
        interface_count = 0

        for row in trancap.iter_rows(named=True):
            from_region_name = row.get("from_region")
            to_region_name = row.get("to_region")
            line_type = row.get("trtype", "AC")
            capacity_from_to = float(row.get("capacity_mw") or row.get("value") or 0.0)

            if not from_region_name or not to_region_name:
                continue

            from_region = self._region_cache.get(from_region_name)
            to_region = self._region_cache.get(to_region_name)

            if not from_region or not to_region:
                logger.debug("Skipping line {}-{}: region not found", from_region_name, to_region_name)
                continue

            reverse_row = trancap.filter(
                (pl.col("from_region") == to_region_name)
                & (pl.col("to_region") == from_region_name)
                & (pl.col("trtype") == line_type)
            )
            if not reverse_row.is_empty():
                val = (
                    reverse_row["capacity_mw"][0]
                    if "capacity_mw" in reverse_row.columns
                    else reverse_row["value"][0]
                )
                capacity_to_from = float(val)
            else:
                capacity_to_from = capacity_from_to

            regions_sorted = sorted([from_region_name, to_region_name])
            interface_name = f"{regions_sorted[0]}||{regions_sorted[1]}"

            if from_region_name == regions_sorted[0]:
                interface_from = from_region
                interface_to = to_region
                forward_cap = capacity_from_to
                reverse_cap = capacity_to_from
            else:
                interface_from = to_region
                interface_to = from_region
                forward_cap = capacity_to_from
                reverse_cap = capacity_from_to

            if interface_name in self._interface_cache:
                continue

            interface = ReEDSInterface(
                name=interface_name,
                from_region=interface_from,
                to_region=interface_to,
                category=line_type,
            )
            self.system.add_component(interface)
            self._interface_cache[interface_name] = interface
            interface_count += 1

            line_name = f"{from_region_name}_{to_region_name}_{line_type}"
            line = ReEDSTransmissionLine(
                name=line_name,
                interface=interface,
                max_active_power=FromTo_ToFrom(
                    name=f"{line_name}_capacity", from_to=forward_cap, to_from=reverse_cap
                ),
                category=line_type,
                line_type=line_type,
            )
            self.system.add_component(line)
            line_count += 1

        logger.info("Built {} transmission interfaces and {} lines", interface_count, line_count)

    def _build_loads(self) -> None:
        """Build load components from demand data.

        Filters load data by weather year and solve year.
        Stores filtered data for later time series attachment.
        """
        logger.info("Building loads...")

        load_data = self.read_data_file("load_data")
        if load_data is None:
            logger.warning("No load data found, skipping loads")
            return

        df = load_data.collect() if isinstance(load_data, pl.LazyFrame) else load_data

        if df.is_empty():
            logger.warning("Load data is empty, skipping loads")
            return

        weather_year = self.config.primary_weather_year
        solve_year = (
            self.config.solve_year[0] if isinstance(self.config.solve_year, list) else self.config.solve_year
        )

        df = df.filter((pl.col("datetime").dt.year() == weather_year) & (pl.col("solve_year") == solve_year))

        logger.debug(
            "Filtered load data to weather year {} and solve year {}: {} hours",
            weather_year,
            solve_year,
            df.height,
        )

        self._filtered_load_data = df

        load_count = 0

        for region_name, region_obj in self._region_cache.items():
            if region_name not in df.columns:
                logger.debug("No load data for region {}", region_name)
                continue

            load_profile = df[region_name].to_numpy()
            peak_load = float(load_profile.max())

            demand = self.create_component(
                ReEDSDemand,
                name=f"{region_name}_load",
                region=region_obj,
                max_active_power=peak_load,
            )

            self.add_component(demand)
            load_count += 1

        logger.info("Built {} load components", load_count)

    def _build_reserves(self) -> None:
        """Build reserve requirement components.

        Creates reserve components for each transmission region and reserve type.
        Configuration loaded from defaults.json includes types, duration, timeframe, etc.
        """
        logger.info("Building reserves...")

        hierarchy_data = self.read_data_file("hierarchy")
        if hierarchy_data is None:
            logger.warning("No hierarchy data found, skipping reserves")
            return

        df = hierarchy_data.collect()
        if df.is_empty():
            logger.warning("Hierarchy data is empty, skipping reserves")
            return

        defaults = self.config.load_defaults()
        reserve_types = defaults.get("default_reserve_types", [])
        reserve_duration = defaults.get("reserve_duration", {})
        reserve_time_frame = defaults.get("reserve_time_frame", {})
        reserve_vors = defaults.get("reserve_vors", {})
        reserve_direction = defaults.get("reserve_direction", {})

        if not reserve_types:
            logger.debug("No reserve types configured, skipping reserves")
            return

        reserve_type_map = {
            "SPINNING": ReserveType.SPINNING,
            "FLEXIBILITY": ReserveType.FLEXIBILITY_UP,
            "REGULATION": ReserveType.REGULATION,
        }

        direction_map = {"Up": ReserveDirection.UP, "Down": ReserveDirection.DOWN}

        if "transmission_region" in df.columns:
            transmission_regions = df["transmission_region"].unique().to_list()
        else:
            transmission_regions = []

        reserve_count = 0
        for region_name in transmission_regions:
            for reserve_type_name in reserve_types:
                reserve_type = reserve_type_map.get(reserve_type_name)
                if not reserve_type:
                    logger.warning("Unknown reserve type: {}", reserve_type_name)
                    continue

                duration = reserve_duration.get(reserve_type_name)
                time_frame = reserve_time_frame.get(reserve_type_name)
                vors = reserve_vors.get(reserve_type_name)
                direction_str = reserve_direction.get(reserve_type_name, "Up")
                direction = direction_map.get(direction_str, ReserveDirection.UP)

                reserve = self.create_component(
                    ReEDSReserve,
                    name=f"{region_name}_{reserve_type_name}",
                    reserve_type=reserve_type,
                    duration=duration,
                    time_frame=time_frame,
                    vors=vors,
                    direction=direction,
                )

                self.add_component(reserve)
                reserve_count += 1

        logger.info("Built {} reserve components", reserve_count)

    def _build_emissions(self) -> None:
        """Attach emission supplemental attributes to generators.

        Only processes combustion emissions. Emission types are normalized to uppercase
        for enum validation.
        """
        logger.info("Building emissions...")

        emit_data = self.read_data_file("emission_rates")
        if emit_data is None:
            logger.warning("No emission rates data found, skipping emissions")
            return

        df = emit_data.collect()
        if df.is_empty():
            logger.warning("Emission rates data is empty, skipping emissions")
            return

        emission_count = 0

        for row in df.iter_rows(named=True):
            tech = row.get("technology") or row.get("tech") or row.get("i")
            region = row.get("region") or row.get("r")
            emission_type = row.get("emission_type") or row.get("e")
            rate = row.get("emission_rate") or row.get("rate") or row.get("value")
            emission_source = row.get("emission_source", "combustion")

            if not tech or not region or not emission_type or rate is None:
                continue

            if emission_source != "combustion":
                continue

            emission_type = str(emission_type).upper()

            gen_name = f"{region}_{tech}"
            generator = self._generator_cache.get(gen_name)

            if not generator:
                logger.debug("Generator {} not found for emission {}, skipping", gen_name, emission_type)
                continue

            emission = ReEDSEmission(
                emission_type=EmissionType(emission_type),
                rate=float(rate),
            )

            self.system.add_supplemental_attribute(generator, emission)
            emission_count += 1

        logger.info("Attached {} emissions to generators", emission_count)

    def _attach_load_profiles(self) -> None:
        """Attach load time series to demand components using filtered data from _build_loads()."""
        logger.info("Attaching load profiles...")

        if self._filtered_load_data is None:
            logger.warning("No filtered load data available, skipping load profile attachment")
            return

        df = self._filtered_load_data
        demands = list(self.system.get_components(ReEDSDemand))

        if df.is_empty() or not demands:
            logger.warning("Load data empty or no demands found, skipping load profile attachment")
            return

        initial_timestamp = self.hourly_time_index[0].astype("datetime64[us]").item()
        resolution = (self.hourly_time_index[1] - self.hourly_time_index[0]).astype("timedelta64[us]").item()

        attached_count = 0
        for demand in demands:
            region_name = demand.name.replace("_load", "")
            if region_name in df.columns:
                ts = SingleTimeSeries.from_array(
                    data=df[region_name].to_numpy(),
                    name="max_active_power",
                    initial_timestamp=initial_timestamp,
                    resolution=resolution,
                )
                self.system.add_time_series(ts, demand)
                attached_count += 1

        logger.info("Attached {} load profiles to demand components", attached_count)

    def _attach_renewable_profiles(self) -> None:
        """Attach renewable capacity factor profiles to generators.

        Matches CF data columns (format: tech|region) to generators.
        """
        logger.info("Attaching renewable profiles to generators...")

        renewable_cf_data = self.read_data_file("renewable_cf")
        if renewable_cf_data is None:
            logger.warning("No renewable CF data found, skipping renewable profiles")
            return

        cf_df = renewable_cf_data.collect()
        if cf_df.is_empty():
            logger.warning("Renewable CF data is empty, skipping profiles")
            return

        weather_year = self.config.primary_weather_year
        cf_df = cf_df.filter(pl.col("datetime").dt.year() == weather_year)

        logger.debug("Filtered CF data to weather year {}: {} hours", weather_year, cf_df.height)

        self._filtered_cf_data = cf_df

        initial_timestamp = self.hourly_time_index[0].astype("datetime64[s]").astype(datetime)
        resolution = timedelta(
            seconds=int((self.hourly_time_index[1] - self.hourly_time_index[0]) / np.timedelta64(1, "s"))
        )

        profile_count = 0
        for col_name in cf_df.columns:
            if col_name == "datetime":
                continue

            parts = col_name.split("|")
            if len(parts) != 2:
                continue

            tech, region_name = parts
            matching_generators = [
                gen
                for gen in self._generator_cache.values()
                if gen.technology == tech and gen.region.name == region_name
            ]

            for generator in matching_generators:
                ts = SingleTimeSeries.from_array(
                    data=cf_df[col_name].to_numpy(),
                    name="max_active_power",
                    initial_timestamp=initial_timestamp,
                    resolution=resolution,
                    normalization=None,
                )
                self.system.add_time_series(ts, generator)
                profile_count += 1

        logger.info("Attached {} renewable profiles", profile_count)

    def _attach_reserve_profiles(self) -> None:
        """Attach reserve requirement profiles based on wind, solar, and load data."""
        logger.info("Attaching reserve profiles...")

        defaults = self.config.load_defaults()
        excluded_from_reserves = defaults.get("excluded_from_reserves", {})

        if not excluded_from_reserves:
            logger.warning("No excluded_from_reserves configured in defaults, skipping reserve profiles")
            return

        for reserve in self.system.get_components(ReEDSReserve):
            requirement_profile = self._calculate_reserve_requirement(reserve)

            if requirement_profile is None or len(requirement_profile) == 0:
                logger.warning(f"No reserve requirement calculated for {reserve.name}, skipping")
                continue

            initial_timestamp = self.hourly_time_index[0].astype("datetime64[s]").astype(datetime)
            resolution = timedelta(hours=1)

            ts = SingleTimeSeries.from_array(
                data=requirement_profile,
                name="requirement",
                initial_timestamp=initial_timestamp,
                resolution=resolution,
            )
            self.system.add_time_series(ts, reserve)

        logger.info("Reserve profile attachment complete")

    def _calculate_reserve_requirement(self, reserve: ReEDSReserve) -> np.ndarray | None:
        """Calculate reserve requirement based on wind, solar, and load profiles.

        Reserve requirement = (wind_capacity * wind_pct) + (solar_capacity * solar_pct) + (load * load_pct)

        Wind contribution: Sum of individual generator capacities weighted by percentage
        Solar contribution: Total capacity when any solar is active, weighted by percentage
        Load contribution: Load value weighted by percentage

        Parameters
        ----------
        reserve : ReEDSReserve
            Reserve component to calculate requirement for

        Returns
        -------
        np.ndarray | None
            Array of reserve requirements over time, or None if cannot be calculated
        """
        reserve_type_name = reserve.reserve_type.value.upper()
        if reserve_type_name in ("FLEXIBILITY_UP", "FLEXIBILITY_DOWN"):
            reserve_type_name = "FLEXIBILITY"

        region_name = reserve.name.rsplit("_", 1)[0]

        logger.debug(
            f"Calculating reserve requirement for {reserve.name} (region: {region_name}, type: {reserve_type_name})"
        )

        defaults = self.config.load_defaults()
        wind_percentages = defaults.get("wind_reserves", {})
        solar_percentages = defaults.get("solar_reserves", {})
        load_percentages = defaults.get("load_reserves", {})

        wind_pct = wind_percentages.get(reserve_type_name, 0.0)
        solar_pct = solar_percentages.get(reserve_type_name, 0.0)
        load_pct = load_percentages.get(reserve_type_name, 0.0)

        num_hours = len(self.hourly_time_index)
        requirement = np.zeros(num_hours)

        if wind_pct > 0:
            wind_generators = [
                gen
                for gen in self.system.get_components(ReEDSGenerator)
                if gen.region
                and gen.region.transmission_region == region_name
                and self._tech_matches_category(gen.technology, "wind", defaults)
            ]

            for gen in wind_generators:
                if self.system.has_time_series(gen):
                    ts = self.system.get_time_series(gen)
                    data_length = min(len(ts.data), len(requirement))
                    requirement[:data_length] += ts.data[:data_length] * wind_pct

        if solar_pct > 0:
            solar_generators = [
                gen
                for gen in self.system.get_components(ReEDSGenerator)
                if gen.region
                and gen.region.transmission_region == region_name
                and self._tech_matches_category(gen.technology, "solar", defaults)
            ]

            total_solar_capacity = sum(gen.capacity for gen in solar_generators)
            solar_active = np.zeros(num_hours)

            for gen in solar_generators:
                if self.system.has_time_series(gen):
                    ts = self.system.get_time_series(gen)
                    data_length = min(len(ts.data), len(solar_active))
                    solar_active[:data_length] = np.maximum(
                        solar_active[:data_length], (ts.data[:data_length] > 0).astype(float)
                    )

            requirement += solar_active * total_solar_capacity * solar_pct

        if load_pct > 0:
            loads = [
                load
                for load in self.system.get_components(ReEDSDemand)
                if load.region and load.region.transmission_region == region_name
            ]

            for load in loads:
                if self.system.has_time_series(load):
                    ts = self.system.get_time_series(load)
                    data_length = min(len(ts.data), len(requirement))
                    requirement[:data_length] += ts.data[:data_length] * load_pct

        if requirement.sum() == 0:
            logger.warning(f"Reserve requirement for {reserve.name} is zero")
            return None

        return requirement

    def _create_generator(
        self,
        tech: str,
        region_name: str,
        vintage: str | None,
        row: dict[str, object],
        gen_suffix: str = "",
    ) -> None:
        """Helper to create and cache a generator component."""
        region_obj = self._region_cache.get(region_name)
        if not region_obj:
            logger.warning("Region '{}' not found for generator {}, skipping", region_name, tech)
            return

        gen_name = f"{tech}_{vintage}_{region_name}" if vintage else f"{tech}_{region_name}"
        if gen_suffix:
            gen_name = f"{tech}_{gen_suffix}"

        generator = self.create_component(
            ReEDSGenerator,
            name=gen_name,
            category=row.get("category"),
            region=region_obj,
            technology=tech,
            capacity=float(row.get("capacity_mw", 0.0)),  # type: ignore[arg-type]
            heat_rate=row.get("heat_rate"),
            forced_outage_rate=row.get("forced_outage_rate"),
            planned_outage_rate=row.get("planned_outage_rate"),
            max_age=row.get("maxage_years"),
            fuel_type=row.get("fuel_type"),
            fuel_price=row.get("fuel_price"),
            vom_cost=row.get("vom_price"),
            vintage=vintage,
        )

        self.add_component(generator)
        self._generator_cache[gen_name] = generator

    def _attach_hydro_budgets(self) -> None:
        """Attach daily energy budgets to hydro dispatch generators.

        Creates daily energy constraints based on monthly capacity factors.
        Budget = capacity * monthly_cf * hours_in_month
        """
        logger.info("Attaching hydro budget profiles...")

        hydro_cf = self.read_data_file("hydro_cf")
        if hydro_cf is None:
            logger.warning("No hydro_cf data found, skipping hydro budgets")
            return

        defaults = self.config.load_defaults()
        month_map = defaults.get("month_map", {})
        month_hours = defaults.get("month_hours", {})
        if not month_map or not month_hours:
            logger.warning("No month_map or month_hours in defaults, skipping hydro budgets")
            return

        hydro_cf_columns = hydro_cf.collect_schema().names()

        hydro_generators = list(
            self.system.get_components(
                ReEDSGenerator,
                filter_func=lambda gen: self._tech_matches_category(gen.technology, "hydro", defaults),
            )
        )

        if not hydro_generators:
            logger.warning("No hydro generators found, skipping hydro budgets")
            return

        for generator in hydro_generators:
            region_id = generator.region.name
            tech = generator.technology

            gen_cf_data = (
                hydro_cf.filter((pl.col("region") == region_id) & (pl.col("technology") == tech))
                if "technology" in hydro_cf_columns
                else hydro_cf.filter(pl.col("region") == region_id)
            )

            gen_cf_collected = gen_cf_data.collect()
            if len(gen_cf_collected) == 0:
                continue

            month_budgets = []
            for month_num in range(1, 13):
                month_str = str(month_num)
                month_name = month_map.get(month_str, month_str)

                month_cf = gen_cf_collected.filter(pl.col("month") == month_name)
                if len(month_cf) == 0:
                    month_budgets.append(0.0)
                    continue

                cf_value = float(month_cf.select(pl.col("hydro_cf")).item())
                hours = month_hours.get(month_name, 730.0)

                budget_gwh = generator.capacity * cf_value * hours / 1000.0
                month_budgets.append(budget_gwh)

            daily_budgets = self._expand_monthly_to_daily(month_budgets)
            hourly_budgets = self._expand_daily_to_hourly(daily_budgets)

            initial_timestamp = self.hourly_time_index[0].astype("datetime64[s]").astype(datetime)
            resolution = timedelta(hours=1)

            ts = SingleTimeSeries.from_array(
                data=np.array(hourly_budgets),
                name="hydro_budget",
                initial_timestamp=initial_timestamp,
                resolution=resolution,
            )
            self.system.add_time_series(ts, generator)

        logger.info("Hydro budget attachment complete")

    def _attach_hydro_rating_profiles(self) -> None:
        """Attach hourly max active power profiles to hydro energy reservoir generators.

        Creates capacity limits based on monthly capacity factors.
        Rating = capacity * monthly_cf
        """
        logger.info("Attaching hydro rating profiles...")

        hydro_cf = self.read_data_file("hydro_cf")
        if hydro_cf is None:
            logger.warning("No hydro_cf data found, skipping hydro rating profiles")
            return

        defaults = self.config.load_defaults()
        month_map = defaults.get("month_map", {})
        if not month_map:
            logger.warning("No month_map in defaults, skipping hydro rating profiles")
            return

        hydro_cf_columns = hydro_cf.collect_schema().names()

        hydro_generators = list(
            self.system.get_components(
                ReEDSGenerator,
                filter_func=lambda gen: self._tech_matches_category(gen.technology, "hydro", defaults),
            )
        )

        if not hydro_generators:
            logger.warning("No hydro generators found, skipping hydro rating profiles")
            return

        for generator in hydro_generators:
            region_id = generator.region.name
            tech = generator.technology

            gen_cf_data = (
                hydro_cf.filter((pl.col("region") == region_id) & (pl.col("technology") == tech))
                if "technology" in hydro_cf_columns
                else hydro_cf.filter(pl.col("region") == region_id)
            )

            gen_cf_collected = gen_cf_data.collect()
            if len(gen_cf_collected) == 0:
                continue

            month_ratings = []
            for month_num in range(1, 13):
                month_str = str(month_num)
                month_name = month_map.get(month_str, month_str)

                month_cf = gen_cf_collected.filter(pl.col("month") == month_name)
                if len(month_cf) == 0:
                    month_ratings.append(generator.capacity)
                    continue

                cf_value = float(month_cf.select(pl.col("hydro_cf")).item())
                rating_mw = generator.capacity * cf_value
                month_ratings.append(rating_mw)

            hourly_ratings = self._expand_monthly_to_hourly(month_ratings)

            initial_timestamp = self.hourly_time_index[0].astype("datetime64[s]").astype(datetime)
            resolution = timedelta(hours=1)

            ts = SingleTimeSeries.from_array(
                data=np.array(hourly_ratings),
                name="max_active_power",
                initial_timestamp=initial_timestamp,
                resolution=resolution,
            )
            self.system.add_time_series(ts, generator)

        logger.info("Hydro rating profile attachment complete")

    def _expand_monthly_to_daily(self, monthly_values: list[float]) -> list[float]:
        """Expand monthly values to daily values based on days in each month."""
        defaults = self.config.load_defaults()
        month_days = defaults.get("month_days", {})

        if not month_days:
            days_per_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        else:
            days_per_month = [month_days.get(f"M{i}", 30) for i in range(1, 13)]

        daily_values = []
        for month_idx, value in enumerate(monthly_values):
            days = days_per_month[month_idx]
            daily_values.extend([value] * days)

        return daily_values

    def _expand_daily_to_hourly(self, daily_values: list[float]) -> list[float]:
        """Expand daily values to hourly values (24 hours per day)."""
        hourly_values = []
        for value in daily_values:
            hourly_values.extend([value] * 24)
        return hourly_values

    def _expand_monthly_to_hourly(self, monthly_values: list[float]) -> list[float]:
        """Expand monthly values to hourly values based on hours in each month."""
        defaults = self.config.load_defaults()
        month_hours = defaults.get("month_hours", {})

        if not month_hours:
            hours_per_month = [744, 672, 744, 720, 744, 720, 744, 744, 720, 744, 720, 744]
        else:
            hours_per_month = [month_hours.get(f"M{i}", 730) for i in range(1, 13)]

        hourly_values = []
        for month_idx, value in enumerate(monthly_values):
            hours = hours_per_month[month_idx]
            hourly_values.extend([value] * hours)

        return hourly_values
