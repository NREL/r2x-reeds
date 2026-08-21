"""
ReEDS parser for building infrasys.System from ReEDS model output.

The :class:`ReEDSParser` is used to build an infrasys.System from ReEDS model output
using the new Plugin[ReEDSConfig] pattern with lifecycle hooks.
"""

from __future__ import annotations

import calendar
from collections.abc import Iterable, Mapping
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
from infrasys import Component, SingleTimeSeries, SupplementalAttribute
from loguru import logger
from rust_ok import Err, Ok, Result

from r2x_core import Plugin, Rule, System

from .enum_mappings import RESERVE_TYPE_MAP
from .getters import (
    build_generator_name,
    build_load_name,
    build_region_name,
    build_reserve_name,
    build_transmission_interface_name,
    build_transmission_line_name,
    resolve_emission_generator_identifier,
)
from .models import (
    ReEDSCapacityExpansion,
    ReEDSInitialCapacity,
    ReEDSPlanningPeriod,
    ReEDSPlanningSwitches,
    ReEDSPlantCharacteristics,
    ReEDSRepresentativeTimepoint,
    ReEDSStorageDuration,
    ReEDSStorageDurationOverride,
)
from .models.components import (
    ReEDSConsumingTechnology,
    ReEDSDemand,
    ReEDSEmission,
    ReEDSGenerator,
    ReEDSInterface,
    ReEDSRegion,
    ReEDSReserve,
    ReEDSReservePercentages,
    ReEDSReserveRegion,
    ReEDSTransmissionLine,
)
from .models.enums import ReserveType
from .parser_checks import check_dataset_non_empty, check_required_values_in_column
from .parser_utils import (
    build_capacity_expansion_initial_capacity_frame,
    build_capacity_expansion_periods_frame,
    build_capacity_expansion_plant_characteristics_frame,
    build_capacity_expansion_representative_timepoints_frame,
    build_reserve_rows,
    build_synthetic_hour_map,
    calculate_reserve_requirement,
    get_rule_for_target,
    get_rules_by_target,
    monthly_to_hourly_polars,
    prepare_generator_datasets,
    select_generator_rule,
    tech_matches_category,
    truncate_and_cast_time_series,
)
from .plugin_config import ReEDSConfig
from .rule_construction import create_and_attach_from_rule
from .rules_helper import create_parser_context
from .upgrader.data_upgrader import run_reeds_upgrades


class ReEDSParser(Plugin[ReEDSConfig]):
    """Parser for ReEDS model data using the Plugin[ReEDSConfig] pattern.

    This parser builds an :class:`infrasys.System` from ReEDS model output through three main phases:

    1. **Validation** (:meth:`on_validate`):
       - Presence of required datasets
       - Validity of configured solve and weather years
       - Loading and parsing of configuration assets

    2. **Preparation** (:meth:`on_prepare`):
       - Parser context with configuration and defaults
       - Time indices and calendar mappings for the weather year
       - Generator datasets separated into variable and non-variable groups
       - Hydro capacity factor data for budget calculations
       - Reserve requirement configuration and costs

    3. **Building** (:meth:`on_build`):
       - Regions from hierarchy data with regional attributes
       - Generators split into renewable (aggregated by tech-region) and non-renewable (with vintage)
       - Transmission interfaces and lines with bi-directional ratings
       - Loads with peak demand by region
       - Reserves by transmission region and type
       - Emissions as supplemental attributes on generators
       - Time series data (load profiles, capacity factors, reserve requirements, hydro budgets)
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
    store : DataStore
        Initialized DataStore with ReEDS file mappings loaded
    overrides : dict[str, Any] | None, optional
        Configuration overrides to apply, by default None

    Attributes
    ----------
    config : ReEDSConfig
        The ReEDS configuration instance
    store : DataStore
        The DataStore for accessing ReEDS data files

    Methods
    -------
    on_validate()
        Validate input data and configuration before building the system
    on_prepare()
        Prepare and normalize configuration, time arrays, and component datasets
    on_build()
        Build and return the complete infrasys.System

    See Also
    --------
    :class:`Plugin` : Parent class with lifecycle hooks
    :class:`ReEDSConfig` : Configuration class for ReEDS parser
    :class:`DataStore` : Data storage and access interface

    Examples
    --------
    Build a ReEDS system from test data:

    >>> from pathlib import Path
    >>> from r2x_core import PluginContext, DataStore
    >>> from r2x_reeds import ReEDSConfig, ReEDSParser
    >>>
    >>> config = ReEDSConfig(solve_year=2030, weather_year=2012, case_name="High_Renewable")
    >>> store = DataStore(path=Path("tests/data/test_Pacific"))
    >>> ctx = PluginContext(config=config, store=store)
    >>> parser = ReEDSParser.from_context(ctx)
    >>> result_ctx = parser.run()
    >>> system = result_ctx.system

    Notes
    -----
    The parser uses internal caches for regions and generators to optimize cross-referencing
    during component construction. These caches are populated during the build phase
    and are used for all subsequent operations.
    """

    def __init__(self) -> None:
        """Initialize the ReEDS parser with internal state.

        Note: Configuration and store are provided via from_context() class method.
        """
        self._config_overrides: dict[str, Any] | None = None
        self._config_assets: dict[str, Any] | None = None
        self._defaults: dict[str, Any] = {}
        self._parser_rules: list[Rule] | None = None
        self._rules_by_target: dict[str, list[Rule]] = {}

        # Internal caches for component building
        self._variable_generator_df: pl.DataFrame | None = None
        self._non_variable_generator_df: pl.DataFrame | None = None
        self._region_cache: dict[str, ReEDSRegion] = {}
        self._generator_cache: dict[str, ReEDSGenerator | ReEDSConsumingTechnology] = {}
        self._interface_cache: dict[str, ReEDSInterface] = {}
        self._reserve_region_cache: dict[str, ReEDSReserveRegion] = {}
        self._hydro_cf_prepared: pl.DataFrame | None = None
        self._reserve_percentages: dict[ReserveType, ReEDSReservePercentages] = {}
        self._generator_dataset_names: dict[str, str] = {}

        # Parser context and time indices
        self._ctx: Any = None
        self.solve_years: list[int] = []
        self.weather_years: list[int] = []
        self.hourly_time_index: np.ndarray = np.array([])
        self.daily_time_index: np.ndarray = np.array([])
        self.initial_timestamp: datetime = datetime.now()
        self.month_map: dict[str, int] = {}
        self.year_month_day_hours: pl.DataFrame = pl.DataFrame()

        # Technology categorization
        self._tech_categories: dict[str, list[str]] = {}
        self._excluded_techs: list[str] = []

    def on_validate_config(self) -> Result[None, str]:
        """Validate configuration assets before upgrades or data I/O."""
        logger.debug("Validating parser configuration")
        logger.trace(
            "Solve year(s): {}, weather year(s): {}, case: {}, scenario: {}",
            self.config.solve_year,
            self.config.weather_year,
            self.config.case_name,
            self.config.scenario,
        )

        res = self._ensure_config_assets()
        if res.is_err():
            return Err(str(res.err()))

        res = self._prepare_rules_by_target()
        if res.is_err():
            return Err(str(res.err()))

        return Ok(None)

    def on_upgrade(self) -> Result[System | None, str]:
        """Run ReEDS file upgrades before input validation."""
        logger.debug("Determining if upgrade is needed.")
        result = run_reeds_upgrades(store=self.store, ctx=self.ctx)
        if result.is_err():
            return Err(str(result.err()))
        return Ok(None)

    def read_data_file(self, name: str) -> Any:
        """Read a data file from the store.

        Parameters
        ----------
        name : str
            Name of the data file to load

        Returns
        -------
        Any
            The loaded data (typically a polars LazyFrame or DataFrame)
        """
        placeholders = {
            "solve_year": self.config.solve_year,
            "weather_year": self.config.weather_year,
        }

        return self.store.read_data(name, placeholders=placeholders)

    def build_capacity_expansion(self) -> ReEDSCapacityExpansion:
        """Create validated capacity-expansion models from the ReEDS run inputs.

        Raises
        ------
        ValueError
            If a required mapped input is absent, malformed, or inconsistent.
        """
        logger.trace("Reading capacity-expansion source tables")
        modeled_years_data = self.read_data_file("modeled_years")
        present_value_factors_data = self.read_data_file("planning_present_value_factors")
        switches_data = self.read_data_file("planning_switches")
        representative_timepoints_data = self.read_data_file("planning_representative_timepoints")
        plant_characteristics_data = self.read_data_file("planning_plant_characteristics")
        power_capacity_data = self.read_data_file("existing_capacity")
        required_data = (
            ("modeled_years", modeled_years_data),
            ("planning_present_value_factors", present_value_factors_data),
            ("planning_switches", switches_data),
            ("planning_representative_timepoints", representative_timepoints_data),
            ("planning_plant_characteristics", plant_characteristics_data),
            ("existing_capacity", power_capacity_data),
        )
        missing_data = next((name for name, data in required_data if data is None), None)
        if missing_data is not None:
            raise ValueError(f"required capacity-expansion input '{missing_data}' is missing")

        modeled_years = modeled_years_data.collect()
        present_value_factors = present_value_factors_data.collect()
        switches = switches_data.collect()
        representative_timepoints = representative_timepoints_data.collect()
        plant_characteristics = plant_characteristics_data.collect()
        power_capacity = power_capacity_data.collect()
        logger.trace(
            "Capacity-expansion source rows: modeled_years={}, present_value_factors={}, switches={}, "
            "representative_timepoints={}, plant_characteristics={}, existing_capacity={}",
            modeled_years.height,
            present_value_factors.height,
            switches.height,
            representative_timepoints.height,
            plant_characteristics.height,
            power_capacity.height,
        )

        if "modeled_years" not in modeled_years.columns:
            raise ValueError("modeled_years is missing required columns ['modeled_years']")
        planning_year_values: list[int] = []
        for value in modeled_years["modeled_years"].to_list():
            try:
                numeric_year = float(value)
                year = int(numeric_year)
            except (TypeError, ValueError, OverflowError) as exc:
                raise ValueError("modeled_years has a non-integral modeled_years") from exc
            if numeric_year != year:
                raise ValueError("modeled_years has a non-integral modeled_years")
            planning_year_values.append(year)
        planning_years = tuple(planning_year_values)
        if not planning_years:
            raise ValueError("modeled_years contains no planning years")

        if not {"switch", "value"}.issubset(switches.columns):
            raise ValueError("planning_switches is missing required columns ['switch', 'value']")
        source_switch_names = {
            field.validation_alias
            for field in ReEDSPlanningSwitches.model_fields.values()
            if isinstance(field.validation_alias, str)
        }
        switch_values: dict[str, object] = {}
        for row in switches.iter_rows(named=True):
            name = str(row["switch"]).strip()
            if name not in source_switch_names:
                continue
            if name in switch_values:
                raise ValueError(f"planning_switches must contain exactly one {name} value")
            switch_values[name] = row["value"]
        planning_switches = ReEDSPlanningSwitches.model_validate(switch_values)
        logger.debug(
            "Capacity-expansion switches: annual_cap={}, storage_enabled={}, duration_overrides_enabled={}",
            planning_switches.annual_cap.name,
            planning_switches.storage_enabled,
            planning_switches.hydro_psh_duration_data_enabled,
        )

        co2_cap_data = self.read_data_file("co2_cap") if planning_switches.emission_type is not None else None
        co2_cap = co2_cap_data.collect() if co2_cap_data is not None else None
        periods_frame = build_capacity_expansion_periods_frame(
            modeled_years,
            present_value_factors,
            co2_cap,
        )
        missing_factor_years = periods_frame.filter(pl.col("present_value_factor").is_null())[
            "year"
        ].to_list()
        if missing_factor_years:
            raise ValueError(
                f"planning_present_value_factors is missing modeled years {missing_factor_years}"
            )
        if planning_switches.emission_type is not None:
            missing_cap_years = periods_frame.filter(pl.col("emission_cap").is_null())["year"].to_list()
            if missing_cap_years:
                raise ValueError(f"co2_cap is missing modeled years {missing_cap_years}")

        planning_periods = tuple(
            ReEDSPlanningPeriod.model_validate(row) for row in periods_frame.iter_rows(named=True)
        )

        representative_frame = build_capacity_expansion_representative_timepoints_frame(
            representative_timepoints
        )
        representative_models = tuple(
            ReEDSRepresentativeTimepoint.model_validate(row)
            for row in representative_frame.iter_rows(named=True)
        )

        source_variables = {
            field.validation_alias
            for field_name, field in ReEDSPlantCharacteristics.model_fields.items()
            if field_name not in {"technology", "year"} and isinstance(field.validation_alias, str)
        }
        required_variables = {
            field.validation_alias
            for field_name, field in ReEDSPlantCharacteristics.model_fields.items()
            if field_name not in {"technology", "year"}
            and isinstance(field.validation_alias, str)
            and (field.is_required() or bool((field.json_schema_extra or {}).get("source_required")))
        }
        unsupported_variables = set(plant_characteristics["variable"].unique()) - source_variables
        if unsupported_variables:
            variable = sorted(unsupported_variables)[0]
            raise ValueError(f"planning_plant_characteristics has unsupported variable '{variable}'")

        plant_frame = build_capacity_expansion_plant_characteristics_frame(
            plant_characteristics,
            planning_years=planning_years,
        )
        if plant_frame.is_empty():
            raise ValueError("planning_plant_characteristics has no records for modeled years")
        plant_models: list[ReEDSPlantCharacteristics] = []
        for row in plant_frame.iter_rows(named=True):
            missing = {variable for variable in required_variables if row.get(variable) is None}
            if missing:
                raise ValueError(
                    "planning_plant_characteristics is missing variables "
                    f"{sorted(missing)} for technology '{row['technology']}', year {row['year']}"
                )
            for placeholder in ("heatrate", "rte"):
                if row.get(placeholder) == 0:
                    row[placeholder] = None
            plant_models.append(ReEDSPlantCharacteristics.model_validate(row))

        energy_capacity_data = self.read_data_file("existing_energy_capacity")
        energy_capacity = energy_capacity_data.collect() if energy_capacity_data is not None else None
        initial_capacity_frame = build_capacity_expansion_initial_capacity_frame(
            power_capacity,
            energy_capacity,
        )
        initial_capacity_models = tuple(
            ReEDSInitialCapacity.model_validate(row) for row in initial_capacity_frame.iter_rows(named=True)
        )

        storage_enabled = planning_switches.storage_enabled
        duration_overrides_enabled = planning_switches.hydro_psh_duration_data_enabled

        storage_duration_data = self.read_data_file("planning_storage_durations")
        storage_duration_frame = (
            storage_duration_data.collect() if storage_duration_data is not None else None
        )
        storage_duration_models: tuple[ReEDSStorageDuration, ...] = ()
        if storage_duration_frame is not None and not storage_duration_frame.is_empty():
            if "technology" not in storage_duration_frame.columns:
                raise ValueError("planning_storage_durations is missing required column 'technology'")
            technologies = storage_duration_frame["technology"].cast(pl.String).str.strip_chars()
            if technologies.n_unique() != storage_duration_frame.height:
                raise ValueError("planning_storage_durations has duplicate duration")
            storage_duration_models = tuple(
                ReEDSStorageDuration.model_validate(row)
                for row in storage_duration_frame.iter_rows(named=True)
            )

        pumped_duration = None
        duration_override_models: tuple[ReEDSStorageDurationOverride, ...] = ()
        if storage_enabled:
            pumped_duration_data = self.read_data_file("planning_pumped_storage_supply_curve_duration")
            pumped_duration_frame = (
                pumped_duration_data.collect() if pumped_duration_data is not None else None
            )
            if pumped_duration_frame is not None and not pumped_duration_frame.is_empty():
                if pumped_duration_frame.height != 1 or "duration" not in pumped_duration_frame.columns:
                    raise ValueError(
                        "planning_pumped_storage_supply_curve_duration must contain exactly one duration"
                    )
                pumped_duration = float(pumped_duration_frame["duration"][0])

            if duration_overrides_enabled:
                override_data = self.read_data_file("planning_storage_duration_overrides")
                override_frame = override_data.collect() if override_data is not None else None
                if override_frame is not None and not override_frame.is_empty():
                    duration_override_models = tuple(
                        ReEDSStorageDurationOverride.model_validate(row)
                        for row in override_frame.iter_rows(named=True)
                    )

        expansion = ReEDSCapacityExpansion(
            name="capacity_expansion",
            planning_periods=planning_periods,
            representative_timepoints=representative_models,
            switches=planning_switches,
            plant_characteristics=tuple(plant_models),
            initial_capacities=initial_capacity_models,
            storage_durations=storage_duration_models,
            pumped_storage_supply_curve_duration=pumped_duration,
            storage_duration_overrides=duration_override_models,
        )
        logger.trace(
            "Created capacity-expansion component: planning_periods={}, representative_timepoints={}, "
            "plant_characteristics={}, initial_capacities={}, storage_durations={}, duration_overrides={}",
            len(expansion.planning_periods),
            len(expansion.representative_timepoints),
            len(expansion.plant_characteristics),
            len(expansion.initial_capacities),
            len(expansion.storage_durations),
            len(expansion.storage_duration_overrides),
        )
        return expansion

    def _build_capacity_expansion(self, system: System) -> Result[None, str]:
        """Build and attach the capacity-expansion component when inputs exist."""
        if not self._capacity_expansion_is_available():
            logger.debug("Capacity-expansion source inputs are unavailable; skipping component")
            return Ok(None)
        try:
            expansion = self.build_capacity_expansion()
        except ValueError as exc:
            logger.error("Failed to build capacity-expansion component: {}", exc)
            return Err(f"Capacity-expansion inputs: {exc}")
        system.add_component(expansion)
        logger.debug(
            "Attached capacity-expansion component {} with {} planning periods and {} plant characteristics",
            expansion.name,
            len(expansion.planning_periods),
            len(expansion.plant_characteristics),
        )
        return Ok(None)

    def _create_from_rule(
        self,
        row: Mapping[str, Any],
        rule: Rule,
        system: System,
        *,
        attachment_source: Component | None = None,
    ) -> Result[Component | SupplementalAttribute, str]:
        """Create and attach the outputs declared by one parser rule."""
        if self._ctx is None:
            return Err("Parser context is missing")
        self._ctx.system = system
        self._ctx.target_system = system

        return create_and_attach_from_rule(
            row,
            rule,
            system=system,
            context=self._ctx,
            attachment_source=attachment_source,
        )

    def on_validate(self) -> Result[None, str]:
        """Validate input data after upgrades, before building the system.

        Performs comprehensive validation including:
        - Presence of required datasets
        - Validity of configured solve and weather years
        - Rule validation from parser configuration (already loaded)

        Returns
        -------
        Result[None, str]
            Ok() if all validation checks pass, Err() with detailed error message otherwise
        """
        logger.debug("Validating input files")

        placeholders = self.config.model_dump()
        solve_years: Iterable[int] = (
            [self.config.solve_year]
            if isinstance(self.config.solve_year, int)
            else list(self.config.solve_year)
        )
        res = check_required_values_in_column(
            store=self.store,
            dataset="modeled_years",
            required_values=solve_years,
            what="Solve year(s)",
            placeholders=placeholders,
        )
        if res.is_err():
            return Err(str(res.err()))

        weather_years: list[int] = (
            [self.config.weather_year]
            if isinstance(self.config.weather_year, int)
            else list(self.config.weather_year)
        )
        try:
            hour_map_data = self.read_data_file("hour_map")
        except FileNotFoundError as exc:
            hour_map_df = build_synthetic_hour_map(weather_years)
            logger.warning(
                "hour_map file is missing ({}). Using a synthetic in-memory hour_map for validation.",
                exc,
            )
        except Exception as exc:
            return Err(f"Weather year(s): hour_map could not be read: {exc}")
        else:
            if hour_map_data is None:
                hour_map_df = build_synthetic_hour_map(weather_years)
                logger.warning("hour_map is not mapped. Using a synthetic in-memory hour_map for validation.")
            else:
                try:
                    hour_map_df = (
                        hour_map_data.collect() if isinstance(hour_map_data, pl.LazyFrame) else hour_map_data
                    )
                except Exception as exc:
                    return Err(f"Weather year(s): hour_map could not be collected: {exc}")
                if not isinstance(hour_map_df, pl.DataFrame):
                    return Err("Weather year(s): hour_map did not return a Polars DataFrame")
                if hour_map_df.is_empty():
                    return Err("Weather year(s): hour_map file exists but contains no rows")
                if "year" not in hour_map_df.columns:
                    return Err("Weather year(s): hour_map file exists but is missing required 'year' column")

        available_weather_years = {
            int(val)
            for val in hour_map_df.select("year").drop_nulls().to_series().to_list()
            if val is not None
        }
        missing_weather_years = [year for year in weather_years if year not in available_weather_years]
        if missing_weather_years:
            return Err(
                "Weather year(s) "
                f"{missing_weather_years} not found in hour_map.year. "
                f"Available values: {sorted(available_weather_years)}"
            )

        required_inputs = []
        for dataset_name in self.store.list_data():
            data_file = self.store[dataset_name]
            info = data_file.info
            if info and info.is_optional:
                continue
            if info and not info.is_input:
                continue
            required_inputs.append(dataset_name)

        logger.trace("Validating presence of {} required datasets", len(required_inputs))
        for dataset_name in required_inputs:
            if dataset_name == "hour_map":
                # hour_map can be synthesized in-memory when the source file is unavailable.
                continue
            presence_result = check_dataset_non_empty(self.store, dataset_name, placeholders=placeholders)
            if presence_result.is_err():
                return Err(f"{dataset_name}: {presence_result.err()}")

        return Ok(None)

    def on_prepare(self) -> Result[None, str]:
        """Prepare and normalize configuration, time arrays, and component datasets.

        Initializes internal caches and datasets required for component building:
        - Parser context with configuration and defaults
        - Time indices and calendar mappings for the weather year
        - Generator datasets separated into variable and non-variable groups
        - Hydro capacity factor data for budget calculations
        - Reserve requirement configuration and costs

        Returns
        -------
        Result[None, str]
            Ok() on successful preparation, Err() with error message if any step fails
        """
        logger.trace("Preparing parser data caches and context")
        self._initialize_caches()
        logger.trace("Parser caches initialized")

        res = self._initialize_parser_globals()
        if res.is_err():
            return Err(str(res.err()))

        res = self._prepare_time_arrays()
        if res.is_err():
            return Err(str(res.err()))

        # Create a temporary system for context creation
        temp_system = System(name=self.config.case_name or "ReEDS")
        self._ctx = create_parser_context(temp_system, self.config, self._defaults, store=self._ctx.store)
        logger.trace("Parser context created")

        generator_data_result = self._prepare_generator_datasets()
        if generator_data_result.is_err():
            return Err(str(generator_data_result.err()))
        logger.trace("Generator datasets prepared")

        hydro_result = self._prepare_hydro_datasets()
        if hydro_result.is_err():
            return Err(str(hydro_result.err()))
        logger.trace("Hydro datasets prepared")

        reserve_data_result = self._prepare_reserve_datasets()
        if reserve_data_result.is_err():
            return Err(str(reserve_data_result.err()))
        logger.trace("Reserve datasets prepared")

        return Ok(None)

    def on_build(self) -> Result[System, str]:
        """Build and return the complete infrasys.System.

        Constructs all system components in dependency order:
        1. Regions (required by all other components)
        2. Generators (requires regions)
        3. Transmission interfaces and lines
        4. Loads (requires regions)
        5. Reserves and reserve regions (requires transmission regions)
        6. Emissions (requires generators)
        7. Time series data (load profiles, capacity factors, reserve requirements, hydro budgets)
        8. Post-processing (metadata and description)

        Returns
        -------
        Result[System, str]
            Ok(system) with the constructed power system, or Err() with error message
        """
        logger.info("Building ReEDS system")
        system = System(name=self.config.case_name or "ReEDS")
        if self._ctx is None:
            return Err("Parser context is missing")
        self._ctx.system = system
        self._ctx.target_system = system

        capacity_expansion_result = self._build_capacity_expansion(system)
        if capacity_expansion_result.is_err():
            return Err(str(capacity_expansion_result.err()))

        # Build all components
        region_result = self._build_regions(system)
        if region_result.is_err():
            return Err(str(region_result.err()))

        generator_result = self._build_generators(system)
        if generator_result.is_err():
            return Err(str(generator_result.err()))

        transmission_result = self._build_transmission(system)
        if transmission_result.is_err():
            return Err(str(transmission_result.err()))

        load_result = self._build_loads(system)
        if load_result.is_err():
            return Err(str(load_result.err()))

        reserve_result = self._build_reserves(system)
        if reserve_result.is_err():
            return Err(str(reserve_result.err()))

        emission_result = self._build_emissions(system)
        if emission_result.is_err():
            return Err(str(emission_result.err()))

        # Attach time series data
        reserve_membership_result = self._attach_reserve_membership(system)
        if reserve_membership_result.is_err():
            return Err(str(reserve_membership_result.err()))

        load_ts_result = self._attach_load_profiles(system)
        if load_ts_result.is_err():
            return Err(str(load_ts_result.err()))

        renewable_ts_result = self._attach_renewable_profiles(system)
        if renewable_ts_result.is_err():
            return Err(str(renewable_ts_result.err()))

        reserve_ts_result = self._attach_reserve_profiles(system)
        if reserve_ts_result.is_err():
            return Err(str(reserve_ts_result.err()))

        hydro_ts_result = self._attach_hydro_budgets(system)
        if hydro_ts_result.is_err():
            return Err(str(hydro_ts_result.err()))

        # Post-process the system
        self._postprocess_system(system)

        logger.info("ReEDS system build complete")
        return Ok(system)

    def _build_regions(self, system: System) -> Result[None, str]:
        """Build region components from hierarchy data."""
        logger.info("Building regions...")

        hierarchy_data = self.read_data_file("hierarchy").collect()
        logger.trace("Hierarchy dataset rows: {}", hierarchy_data.height)

        region_rule_result = get_rule_for_target(
            self._rules_by_target, name="region", target_type=ReEDSRegion.__name__
        )
        if region_rule_result.is_err():
            return Err(str(region_rule_result.err()))
        region_rules = region_rule_result.ok()
        if region_rules is None:
            return Err("Region rules missing")

        if self._ctx is None:
            return Err("Parser context is missing")

        creation_errors: list[str] = []
        created = 0
        for row in hierarchy_data.iter_rows(named=True):
            identifier_result = build_region_name(row, context=self._ctx)
            if identifier_result.is_err():
                creation_errors.append(str(identifier_result.err()))
                continue
            identifier = identifier_result.ok()
            if not identifier:
                creation_errors.append("Region row has no identifier")
                continue
            creation_result = self._create_from_rule(row, region_rules, system)
            if creation_result.is_err():
                creation_errors.append(f"{identifier}: {creation_result.err()}")
                continue
            region = creation_result.ok()
            if not isinstance(region, ReEDSRegion):
                creation_errors.append(f"{identifier}: rule did not create ReEDSRegion")
                continue
            self._region_cache[region.name] = region
            created += 1

        if creation_errors:
            failure_list = "; ".join(creation_errors)
            return Err(f"Failed to build the following regions: {failure_list}")

        logger.info("Attached {} region components", created)
        return Ok(None)

    def _build_generators(self, system: System) -> Result[None, str]:
        """Build generator components from cached datasets."""
        logger.info("Building ReEDS generator components")

        if self._variable_generator_df is None or self._non_variable_generator_df is None:
            return Err("Generator datasets were not prepared")
        logger.trace(
            "Generator dataset sizes - variable: {}, non-variable: {}",
            self._variable_generator_df.height,
            self._non_variable_generator_df.height,
        )

        variable_result = self._build_generator_group(
            system, self._variable_generator_df, "variable renewable"
        )
        if variable_result.is_err():
            return Err(str(variable_result.err()))
        variable_count = variable_result.ok() or 0

        non_variable_result = self._build_generator_group(
            system, self._non_variable_generator_df, "non-variable"
        )
        if non_variable_result.is_err():
            return Err(str(non_variable_result.err()))
        non_variable_count = non_variable_result.ok() or 0

        total_built = variable_count + non_variable_count
        if total_built == 0:
            logger.warning("No generators were created")
            logger.info("Attached 0 generator components")
        else:
            logger.info(
                "Attached {} generator components ({} variable renewables, {} others)",
                total_built,
                variable_count,
                non_variable_count,
            )
        return Ok(None)

    def _build_generator_group(
        self,
        system: System,
        df: pl.DataFrame,
        label: str,
    ) -> Result[int, str]:
        """Build a group of generators (variable renewable or non-variable) from DataFrame."""
        logger.info("Starting {} generator build", label)
        logger.trace("Processing {} {} generator rows", df.height, label)
        if df.is_empty():
            logger.info("No {} generator data found; attached 0 generators", label)
            return Ok(0)

        if self._ctx is None:
            return Err("Parser context is missing")
        self._ctx.system = system
        self._ctx.target_system = system

        creation_errors: list[str] = []
        built = 0
        for row in df.iter_rows(named=True):
            identifier_result = build_generator_name(row, context=self._ctx)
            if identifier_result.is_err():
                creation_errors.append(str(identifier_result.err()))
                continue
            identifier = identifier_result.ok()
            if not identifier:
                creation_errors.append("Generator row has no identifier")
                continue
            if identifier in self._generator_cache:
                logger.debug("Duplicate generator '{}' detected, skipping.", identifier)
                continue

            rule_result = select_generator_rule(
                self._parser_rules or [],
                row,
                context=self._ctx,
            )
            if rule_result.is_err():
                creation_errors.append(f"{identifier}: {rule_result.err()}")
                continue
            rule = rule_result.ok()
            if rule is None:
                creation_errors.append(f"{identifier}: generator rule resolution returned None")
                continue
            creation_result = self._create_from_rule(row, rule, system)
            if creation_result.is_err():
                creation_errors.append(f"{identifier}: {creation_result.err()}")
                continue

            generator = creation_result.ok()
            if not isinstance(generator, (ReEDSGenerator, ReEDSConsumingTechnology)):
                creation_errors.append(f"{identifier}: rule did not create a generator technology")
                continue
            self._generator_cache[generator.name] = generator
            built += 1

        if creation_errors:
            failure_list = "; ".join(creation_errors)
            return Err(f"Failed to create the following {label} generators: {failure_list}")

        logger.info("Attached {} {} generators", built, label)
        return Ok(built)

    def _build_transmission(self, system: System) -> Result[None, str]:
        """Build transmission interface and line components with bi-directional ratings."""
        logger.info("Building transmission interfaces...")

        trancap_data = self.read_data_file("transmission_capacity")
        if trancap_data is None:
            logger.warning("No transmission capacity data found, skipping transmission")
            logger.info("Attached 0 transmission interfaces and 0 lines")
            return Ok(None)

        trancap = trancap_data.collect()
        logger.trace("Transmission capacity rows: {}", trancap.height)

        if trancap.is_empty():
            logger.warning("Transmission capacity data is empty, skipping transmission")
            logger.info("Attached 0 transmission interfaces and 0 lines")
            return Ok(None)

        if self._ctx is None:
            return Err("Parser context is missing")

        tranloss_data = self.read_data_file("transmission_losses")
        if tranloss_data is not None:
            tranloss = tranloss_data.collect()
            if not tranloss.is_empty():
                rename_map = {}
                if "r" in tranloss.columns and "from_region" not in tranloss.columns:
                    rename_map["r"] = "from_region"
                if "value" in tranloss.columns and "losses" not in tranloss.columns:
                    rename_map["value"] = "losses"
                if rename_map:
                    tranloss = tranloss.rename(rename_map)

                required_loss_cols = {"from_region", "to_region", "trtype", "losses"}
                missing = required_loss_cols - set(tranloss.columns)
                if missing:
                    logger.warning(
                        "Transmission losses data missing expected columns {}; skipping loss join",
                        missing,
                    )
                else:
                    trancap = trancap.join(
                        tranloss.select(["from_region", "to_region", "trtype", "losses"]),
                        on=["from_region", "to_region", "trtype"],
                        how="left",
                    )
                    logger.trace("Joined transmission losses into trancap, columns: {}", trancap.columns)

        # Join reverse capacity from the same trancap table
        # reverse row: swap from_region/to_region and use its capacity as reverse_capacity
        reverse_df = (
            trancap.rename({"from_region": "_to", "to_region": "_from", "capacity": "reverse_capacity"})
            .rename({"_to": "to_region", "_from": "from_region"})
            .select(["from_region", "to_region", "trtype", "reverse_capacity"])
        )
        trancap = trancap.join(
            reverse_df,
            on=["from_region", "to_region", "trtype"],
            how="left",
        ).with_columns(
            # If no reverse row exists, fall back to symmetric (same capacity both ways)
            pl.col("reverse_capacity").fill_null(pl.col("capacity"))
        )
        logger.trace("Joined reverse capacity into trancap, columns: {}", trancap.columns)

        interfaces_result = self._build_transmission_interfaces(system, trancap)
        if interfaces_result.is_err():
            return Err(str(interfaces_result.err()))
        interfaces_data = interfaces_result.ok()
        if interfaces_data is None:
            return Err("Transmission interfaces result was unexpectedly None")
        interface_count, creation_errors = interfaces_data

        line_result = self._build_transmission_lines(system, trancap)
        if line_result.is_err():
            return Err(str(line_result.err()))
        line_data = line_result.ok()
        if line_data is None:
            return Err("Transmission lines result was unexpectedly None")
        line_count, line_errors = line_data
        creation_errors.extend(line_errors)

        logger.info("Attached {} transmission interfaces and {} lines", interface_count, line_count)

        if creation_errors:
            failure_list = "; ".join(creation_errors)
            return Err(f"Failed to create transmission components: {failure_list}")

        return Ok(None)

    def _build_transmission_interfaces(
        self, system: System, trancap: pl.DataFrame
    ) -> Result[tuple[int, list[str]], str]:
        """Create interface components from deduplicated transmission rows."""
        if self._ctx is None:
            return Err("Parser context is missing")

        interface_rule_result = get_rule_for_target(
            self._rules_by_target, name="transmission_interface", target_type=ReEDSInterface.__name__
        )
        if interface_rule_result.is_err():
            return Err(str(interface_rule_result.err()))
        interface_rule = interface_rule_result.ok()
        if interface_rule is None:
            return Err("Transmission interface rule is missing")

        interface_rows = (
            trancap.select(
                pl.col("from_region"),
                pl.col("to_region"),
                pl.col("trtype"),
            )
            .with_columns(
                pl.when(pl.col("from_region") <= pl.col("to_region"))
                .then(pl.col("from_region"))
                .otherwise(pl.col("to_region"))
                .alias("region_a"),
                pl.when(pl.col("from_region") <= pl.col("to_region"))
                .then(pl.col("to_region"))
                .otherwise(pl.col("from_region"))
                .alias("region_b"),
            )
            .select(
                pl.col("region_a"),
                pl.col("region_b"),
                pl.col("trtype"),
            )
            .unique(subset=["region_a", "region_b"])
            .select(
                pl.col("region_a").alias("from_region"),
                pl.col("region_b").alias("to_region"),
                pl.col("trtype"),
            )
        )
        logger.trace("Derived {} unique transmission interface rows", interface_rows.height)

        creation_errors: list[str] = []
        interface_count = 0
        for row in interface_rows.iter_rows(named=True):
            identifier_result = build_transmission_interface_name(row, context=self._ctx)
            if identifier_result.is_err():
                creation_errors.append(str(identifier_result.err()))
                continue
            identifier = identifier_result.ok()
            if not identifier:
                creation_errors.append("Transmission interface row has no identifier")
                continue
            result = self._create_from_rule(row, interface_rule, system)
            if result.is_err():
                creation_errors.append(f"{identifier}: {result.err()}")
                continue
            interface = result.ok()
            if not isinstance(interface, ReEDSInterface):
                creation_errors.append(f"{identifier}: rule did not create ReEDSInterface")
                continue
            self._interface_cache[identifier] = interface
            interface_count += 1

        return Ok((interface_count, creation_errors))

    def _build_transmission_lines(
        self, system: System, trancap: pl.DataFrame
    ) -> Result[tuple[int, list[str]], str]:
        """Instantiate transmission lines using the raw transmission capacity table."""
        if self._ctx is None:
            return Err("Parser context is missing")

        line_rule_result = get_rule_for_target(
            self._rules_by_target, name="transmission_line", target_type=ReEDSTransmissionLine.__name__
        )
        if line_rule_result.is_err():
            return Err(str(line_rule_result.err()))
        line_rule = line_rule_result.ok()
        if line_rule is None:
            return Err("Transmission line rule is missing")

        unique_rows: dict[str, Mapping[str, Any]] = {}
        for row in trancap.iter_rows(named=True):
            identifier_result = build_transmission_line_name(row, context=self._ctx)
            if identifier_result.is_err():
                return Err(str(identifier_result.err()))
            identifier = identifier_result.ok()
            if not identifier:
                return Err("Transmission line row has no identifier")
            if identifier in unique_rows:
                logger.warning(
                    "Duplicate transmission line identifier '{}' detected; keeping the first occurrence and discarding subsequent entries.",
                    identifier,
                )
                continue
            unique_rows[identifier] = row

        creation_errors: list[str] = []
        line_count = 0
        for identifier, row in unique_rows.items():
            result = self._create_from_rule(row, line_rule, system)
            if result.is_err():
                creation_errors.append(f"{identifier}: {result.err()}")
                continue
            line = result.ok()
            if not isinstance(line, ReEDSTransmissionLine):
                creation_errors.append(f"{identifier}: rule did not create ReEDSTransmissionLine")
                continue
            line_count += 1

        return Ok((line_count, creation_errors))

    def _build_loads(self, system: System) -> Result[None, str]:
        """Build load components from demand data."""
        logger.info("Building loads...")

        load_profiles = self.read_data_file("load_profiles").collect()
        logger.trace("Load profile columns: {}", load_profiles.columns)

        region_columns = [col for col in load_profiles.columns if col not in {"datetime", "solve_year"}]
        if not region_columns:
            msg = "Load data has no region columns"
            logger.warning(msg)
            return Err(msg)
        logger.trace("Found {} load regions", len(region_columns))

        rows = []
        for region in region_columns:
            peak_load = float(load_profiles.select(pl.col(region)).max().item())
            rows.append({"region": region, "max_active_power": peak_load})

        loads_df = pl.DataFrame(rows)
        if self._ctx is None:
            return Err("Parser context is missing")

        load_rule_result = get_rule_for_target(
            self._rules_by_target, name="load", target_type=ReEDSDemand.__name__
        )
        if load_rule_result.is_err():
            return Err(str(load_rule_result.err()))
        load_rule = load_rule_result.ok()
        if load_rule is None:
            return Err("Load rule is missing")

        creation_errors: list[str] = []
        load_count = 0
        for row in loads_df.iter_rows(named=True):
            identifier_result = build_load_name(row, context=self._ctx)
            if identifier_result.is_err():
                creation_errors.append(str(identifier_result.err()))
                continue
            identifier = identifier_result.ok()
            if not identifier:
                creation_errors.append("Load row has no identifier")
                continue
            result = self._create_from_rule(row, load_rule, system)
            if result.is_err():
                creation_errors.append(f"{identifier}: {result.err()}")
                continue
            demand = result.ok()
            if not isinstance(demand, ReEDSDemand):
                creation_errors.append(f"{identifier}: rule did not create ReEDSDemand")
                continue
            load_count += 1

        if creation_errors:
            failure_list = "; ".join(creation_errors)
            return Err(f"Failed to build the following loads: {failure_list}")

        logger.info("Attached {} load components", load_count)
        return Ok(None)

    def _build_reserves(self, system: System) -> Result[None, str]:
        """Build reserve requirement components for each transmission region and type."""
        logger.info("Building reserves...")
        reserve_region_count = 0
        reserve_count = 0

        hierarchy_data = self.read_data_file("hierarchy")
        if hierarchy_data is None:
            logger.warning("No hierarchy data found, skipping reserves")
            return Ok(None)

        df = hierarchy_data.collect()
        if df.is_empty():
            logger.warning("Hierarchy data is empty, skipping reserves")
            return Ok(None)

        # Load defaults via classmethod to keep PluginConfig as a pure model
        defaults = self._defaults
        reserve_types = defaults.get("default_reserve_types", [])
        reserve_duration = defaults.get("reserve_duration", {})
        reserve_time_frame = defaults.get("reserve_time_frame", {})
        reserve_vors = defaults.get("reserve_vors", {})
        reserve_direction = defaults.get("reserve_direction", {})

        if not reserve_types:
            logger.debug("No reserve types configured, skipping reserves")
            return Ok(None)

        if "transmission_region" in df.columns:
            transmission_regions = df["transmission_region"].unique().to_list()
        else:
            transmission_regions = []
        logger.trace(
            "Preparing reserves for {} transmission regions and {} reserve types",
            len(transmission_regions),
            len(reserve_types),
        )

        reserve_region_rule_result = get_rule_for_target(
            self._rules_by_target, name="reserve_region", target_type=ReEDSReserveRegion.__name__
        )
        if reserve_region_rule_result.is_err():
            return Err(str(reserve_region_rule_result.err()))
        reserve_region_rule = reserve_region_rule_result.ok()
        if reserve_region_rule is None:
            return Err("Reserve-region rule is missing")

        reserve_region_errors: list[str] = []
        for region_name in transmission_regions:
            if region_name in self._reserve_region_cache:
                continue
            result = self._create_from_rule({"name": region_name}, reserve_region_rule, system)
            if result.is_err():
                reserve_region_errors.append(f"{region_name}: {result.err()}")
                continue
            reserve_region = result.ok()
            if not isinstance(reserve_region, ReEDSReserveRegion):
                reserve_region_errors.append(f"{region_name}: rule did not create ReEDSReserveRegion")
                continue
            self._reserve_region_cache[region_name] = reserve_region
            reserve_region_count += 1

        if reserve_region_errors:
            failure_list = "; ".join(reserve_region_errors)
            return Err(f"Failed to create reserve regions: {failure_list}")

        rows = build_reserve_rows(
            transmission_regions,
            reserve_types,
            reserve_duration=reserve_duration,
            reserve_time_frame=reserve_time_frame,
            reserve_vors=reserve_vors,
            reserve_direction=reserve_direction,
            reserve_percentages=self._reserve_percentages,
        )

        if not rows:
            logger.debug("No reserve rows generated, skipping reserves")
            return Ok(None)
        logger.trace("Generated {} reserve component rows", len(rows))

        rows_df = pl.DataFrame(rows)
        if self._ctx is None:
            return Err("Parser context is missing")

        reserve_rule_result = get_rule_for_target(
            self._rules_by_target, name="reserve", target_type=ReEDSReserve.__name__
        )
        if reserve_rule_result.is_err():
            return Err(str(reserve_rule_result.err()))
        reserve_rule = reserve_rule_result.ok()
        if reserve_rule is None:
            return Err("Reserve rule is missing")

        creation_errors: list[str] = []
        for row in rows_df.iter_rows(named=True):
            identifier_result = build_reserve_name(row, context=self._ctx)
            if identifier_result.is_err():
                creation_errors.append(str(identifier_result.err()))
                continue
            identifier = identifier_result.ok()
            if not identifier:
                creation_errors.append("Reserve row has no identifier")
                continue
            result = self._create_from_rule(row, reserve_rule, system)
            if result.is_err():
                creation_errors.append(f"{identifier}: {result.err()}")
                continue
            reserve = result.ok()
            if not isinstance(reserve, ReEDSReserve):
                creation_errors.append(f"{identifier}: rule did not create ReEDSReserve")
                continue
            reserve_count += 1

        if creation_errors:
            failure_list = "; ".join(creation_errors)
            return Err(f"Failed to build the following reserves: {failure_list}")

        logger.info(
            "Attached {} reserve regions and {} reserve components",
            reserve_region_count,
            reserve_count,
        )
        return Ok(None)

    def _build_emissions(self, system: System) -> Result[None, str]:
        """Attach emission supplemental attributes to generators."""
        logger.info("Building emission components")

        emit_data = self.read_data_file("emission_rates")
        if emit_data is None:
            logger.warning("No emission rates data found, skipping emissions")
            return Ok(None)

        df = emit_data.collect()
        logger.trace("Emission dataset rows: {}", df.height)
        if df.is_empty():
            logger.warning("Emission rates data is empty, skipping emissions")
            return Ok(None)

        if self._ctx is None:
            return Err("Parser context is missing")

        emission_rule_result = get_rule_for_target(
            self._rules_by_target, name="emission", target_type=ReEDSEmission.__name__
        )
        if emission_rule_result.is_err():
            return Err(str(emission_rule_result.err()))
        emission_rule = emission_rule_result.ok()
        if emission_rule is None:
            return Err("Emission rule is missing")

        generated = list(self._generator_cache.values())
        if not generated:
            logger.warning("No generators available for emission matching")
            return Ok(None)

        rename_map = {
            "i": "technology",
            "v": "vintage",
            "r": "region",
        }
        emit_df = df.rename({k: v for k, v in rename_map.items() if k in df.columns}).with_columns(
            pl.col("vintage").fill_null("__missing_vintage__").alias("vintage_key")
        )

        generator_lookup: dict[tuple[str | None, str | None, str], list[str]] = {}
        for generated_name in generated:
            vintage_key = generated_name.identity.vintage or "__missing_vintage__"
            key = (generated_name.technology, generated_name.region.name, vintage_key)
            generator_lookup.setdefault(key, []).append(generated_name.name)

        matched_rows: list[dict[str, Any]] = []
        for row in emit_df.iter_rows(named=True):
            key = (row.get("technology"), row.get("region"), row.get("vintage_key"))
            generator_names = generator_lookup.get(key)
            if not generator_names:
                continue
            row_data = dict(row)
            row_data["name"] = generator_names[0]
            matched_rows.append(row_data)

        if not matched_rows:
            logger.warning("No emission rows matched existing generators, skipping emissions")
            return Ok(None)

        emission_matches = pl.DataFrame(matched_rows).drop("vintage_key")

        if emission_matches.is_empty():
            logger.warning("No emission rows matched existing generators, skipping emissions")
            return Ok(None)

        creation_errors: list[str] = []
        attached = 0
        for row in emission_matches.iter_rows(named=True):
            identifier_result = resolve_emission_generator_identifier(row, context=self._ctx)
            if identifier_result.is_err():
                creation_errors.append(str(identifier_result.err()))
                continue
            identifier = identifier_result.ok()
            generator = self._generator_cache.get(identifier)
            if generator is None:
                logger.trace("Generator {} not found for emission, skipping", identifier)
                continue

            result = self._create_from_rule(row, emission_rule, system, attachment_source=generator)
            if result.is_err():
                creation_errors.append(f"{identifier}: {result.err()}")
                continue
            emission = result.ok()
            if not isinstance(emission, SupplementalAttribute):
                creation_errors.append(f"{identifier}: rule did not create ReEDSEmission")
                continue
            attached += 1

        if creation_errors:
            failure_list = "; ".join(creation_errors)
            return Err(f"Failed to attach the following emissions: {failure_list}")

        logger.info("Attached {} emission components to generators", attached)
        return Ok(None)

    def _attach_load_profiles(self, system: System) -> Result[None, str]:
        """Attach load time series to demand components."""
        logger.info("Starting load profile attachment")

        load_profiles = self.read_data_file("load_profiles").collect()
        logger.trace(
            "Load profile dataset size: {} rows, {} columns",
            load_profiles.height,
            load_profiles.width,
        )

        attached_count = 0
        for demand in system.get_components(ReEDSDemand):
            region_name = demand.name.replace("_load", "")
            if region_name in load_profiles.columns:
                base_data = truncate_and_cast_time_series(load_profiles[region_name].to_numpy())

                ts = SingleTimeSeries.from_array(
                    data=base_data,
                    name="max_active_power",
                    initial_timestamp=self.initial_timestamp,
                    resolution=timedelta(hours=1),
                )
                system.add_time_series(ts, demand)
                attached_count += 1

        logger.info("Attached load profiles to {} demand components", attached_count)
        return Ok(None)

    def _attach_renewable_profiles(self, system: System) -> Result[None, str]:
        """Attach renewable capacity factor profiles to generator components.

        Matches renewable profile columns (format: technology|region) to generators.
        Validates that weather years in profiles match configured weather years.

        Returns
        -------
        Result[None, str]
            Ok() on success, Err() with error message if data is empty or years mismatch
        """
        logger.info("Starting renewable profile attachment")

        renewable_profiles = self.read_data_file("renewable_profiles").collect()
        logger.trace(
            "Renewable profiles dataset size: {} rows, {} columns",
            renewable_profiles.height,
            renewable_profiles.width,
        )

        profile_count = 0
        tech_regions = (
            pl.DataFrame({"col": renewable_profiles.columns})
            .filter(pl.col("col") != "datetime")
            .with_columns(
                [
                    pl.col("col").str.split("|").alias("parts"),
                    pl.col("col").str.split("|").list.len().alias("len"),
                ]
            )
            .filter(pl.col("len") == 2)
            .with_columns(
                [
                    pl.col("parts").list.get(0).alias("tech"),
                    pl.col("parts").list.get(1).alias("region"),
                ]
            )
            .select("col", "tech", "region")
        )
        logger.trace("Parsed {} tech-region renewable profile columns", tech_regions.height)

        for row in tech_regions.iter_rows(named=True):
            tech = row["tech"]
            region_name = row["region"]
            col_name = row["col"]

            matching_generators = [
                gen
                for gen in self._generator_cache.values()
                if gen.technology == tech and gen.region.name == region_name
            ]

            if not matching_generators:
                logger.debug("No matching generators for renewable profile {}|{}", tech, region_name)
                continue

            cf_data = truncate_and_cast_time_series(renewable_profiles[col_name].to_numpy())

            # Scale profiles by rated capaciy
            for generator in matching_generators:
                data = cf_data * generator.capacity
                ts = SingleTimeSeries.from_array(
                    data=data,
                    name="max_active_power",
                    initial_timestamp=self.initial_timestamp,
                    resolution=timedelta(hours=1),
                )
                system.add_time_series(ts, generator)
                profile_count += 1

        logger.info("Attached renewable profiles to {} generator components", profile_count)
        return Ok(None)

    def _attach_reserve_profiles(self, system: System) -> Result[None, str]:
        """Attach reserve requirement time series to reserve components.

        Calculates dynamic reserve requirements from wind, solar, and load contributions
        using configurable percentages from defaults. Applies requirements to reserve
        components by transmission region.

        Returns
        -------
        Result[None, str]
        """
        logger.info("Starting reserve requirement attachment")
        attached_count = 0
        reserves = list(system.get_components(ReEDSReserve))
        logger.trace("Attaching reserve requirements for {} reserve components", len(reserves))
        for reserve in reserves:
            logger.debug("Calculating reserve requirement for {}", reserve.name)
            percentages = self._reserve_percentages.get(
                reserve.reserve_type,
                ReEDSReservePercentages(reserve_type=reserve.reserve_type),
            )

            region_name = reserve.name.rsplit("_", 1)[0]

            wind_pct = float(percentages.or_wind_percentage)
            solar_pct = float(percentages.or_pv_percentage)
            load_pct = float(percentages.or_load_percentage)

            wind_generators = [
                {
                    "capacity": gen.capacity,
                    "time_series": system.get_time_series(gen).data if system.has_time_series(gen) else None,
                }
                for gen in system.get_components(ReEDSGenerator)
                if gen.region
                and gen.region.transmission_region == region_name
                and tech_matches_category(gen.technology, "wind", self._tech_categories)
            ]

            solar_generators = [
                {
                    "capacity": gen.capacity,
                    "time_series": system.get_time_series(gen).data if system.has_time_series(gen) else None,
                }
                for gen in system.get_components(ReEDSGenerator)
                if gen.region
                and gen.region.transmission_region == region_name
                and tech_matches_category(gen.technology, "solar", self._tech_categories)
            ]

            loads = [
                {
                    "time_series": system.get_time_series(load).data
                    if system.has_time_series(load)
                    else None,
                }
                for load in system.get_components(ReEDSDemand)
                if load.region and load.region.transmission_region == region_name
            ]
            logger.trace(
                "Reserve {} inputs - wind: {}, solar: {}, loads: {}",
                reserve.name,
                len(wind_generators),
                len(solar_generators),
                len(loads),
            )

            calc_result = calculate_reserve_requirement(
                wind_generators=wind_generators,
                solar_generators=solar_generators,
                loads=loads,
                hourly_time_index=self.hourly_time_index,
                wind_pct=wind_pct,
                solar_pct=solar_pct,
                load_pct=load_pct,
            )
            if calc_result.is_err():
                return Err(str(calc_result.err()))

            requirement_profile = calc_result.ok()
            if requirement_profile is None or len(requirement_profile) == 0:
                logger.warning("No reserve requirement calculated for {}, skipping", reserve.name)
                continue

            data = truncate_and_cast_time_series(requirement_profile)
            ts = SingleTimeSeries.from_array(
                data=data,
                name="requirement",
                initial_timestamp=self.initial_timestamp,
                resolution=timedelta(hours=1),
            )
            system.add_time_series(ts, reserve)
            attached_count += 1

        logger.info("Attached reserve requirements to {} reserve components", attached_count)
        return Ok(None)

    def _attach_reserve_membership(self, system: System) -> Result[None, str]:
        """Attach reserves to generators in the same transmission region, excluding configured techs."""
        excluded_map = self._defaults.get("excluded_from_reserves", {})
        logger.info("Starting reserve membership attachment")
        attached_memberships = 0
        if not excluded_map:
            logger.info("Attached 0 reserve membership links")
            return Ok(None)

        reserves = list(system.get_components(ReEDSReserve))
        generators = list(system.get_components(ReEDSGenerator))
        logger.trace(
            "Evaluating reserve membership for {} reserves across {} generators",
            len(reserves),
            len(generators),
        )

        for reserve in reserves:
            reserve_type_name = reserve.reserve_type.value.upper()
            if reserve_type_name in ("FLEXIBILITY_UP", "FLEXIBILITY_DOWN"):
                reserve_type_name = "FLEXIBILITY"

            excluded_categories = excluded_map.get(reserve_type_name, [])
            region_name = reserve.name.rsplit("_", 1)[0]

            for gen in generators:
                if not gen.region or gen.region.transmission_region != region_name:
                    continue

                if any(
                    tech_matches_category(gen.technology, cat, self._tech_categories)
                    for cat in excluded_categories
                ):
                    continue

                existing_reserves = gen.ext.get("reserves")
                reserve_list: list[str] = (
                    [str(value) for value in existing_reserves] if isinstance(existing_reserves, list) else []
                )
                if reserve.name not in reserve_list:
                    reserve_list.append(reserve.name)
                    gen.ext["reserves"] = reserve_list
                    attached_memberships += 1

        logger.info("Attached {} reserve membership links", attached_memberships)
        return Ok(None)

    def _attach_hydro_budgets(self, system: System) -> Result[None, str]:
        """Attach daily energy budgets to hydro dispatch generators.

        Creates daily energy constraints based on monthly capacity factors.
        Budget = capacity * monthly_cf * hours_in_month
        """
        logger.info("Starting hydro budget attachment")
        attached_budgets = 0

        hydro_generators = [
            gen
            for gen in self._generator_cache.values()
            if isinstance(gen, ReEDSGenerator)
            and tech_matches_category(gen.technology, "hydro", self._tech_categories)
        ]
        logger.trace("Hydro generators detected: {}", len(hydro_generators))

        if not hydro_generators:
            logger.warning("No hydro generators found, skipping hydro budgets")
            return Ok(None)

        if self._hydro_cf_prepared is None:
            logger.warning("Hydro CF data not prepared, skipping hydro budgets")
            return Ok(None)
        logger.trace("Hydro CF prepared rows: {}", self._hydro_cf_prepared.height)

        hydro_capacity = pl.DataFrame(
            [
                (
                    gen.name,
                    gen.technology,
                    gen.region.name,
                    gen.capacity,
                    gen.identity.vintage,
                )
                for gen in hydro_generators
            ],
            schema=["name", "technology", "region", "capacity", "vintage"],
            orient="row",
        )

        hydro_data = hydro_capacity.join(self._hydro_cf_prepared, on=["technology", "region"], how="left")

        # Drop rows where no CF data was found (null from the left join).
        # These are generators whose technology/region has no entry in hydcf.csv.
        missing_cf = hydro_data.filter(pl.col("hydro_cf").is_null())
        if not missing_cf.is_empty():
            missing_names = missing_cf["name"].unique().to_list()
            logger.warning(
                "No hydro CF data for {} generators (missing technology/region in hydcf.csv): {}",
                len(missing_names),
                ", ".join(missing_names),
            )
        hydro_data = hydro_data.filter(pl.col("hydro_cf").is_not_null())

        hydro_data = hydro_data.with_columns(
            (
                pl.col("hydro_cf") * pl.col("hours_in_month") * pl.col("capacity") / pl.col("days_in_month")
            ).alias("daily_energy_budget")
        )

        for generator in hydro_generators:
            tech_region_filter = (
                (pl.col("technology") == generator.technology)
                & (pl.col("region") == generator.region.name)
                & (pl.col("vintage") == generator.identity.vintage)
            )
            for row, month_budget_by_vintage in hydro_data.filter(tech_region_filter).group_by(
                ["year", "vintage"]
            ):
                year = row[0]
                monthly_profile = month_budget_by_vintage["daily_energy_budget"].to_list()
                if len(monthly_profile) != 12 or any(value is None for value in monthly_profile):
                    logger.warning(
                        "Skipping hydro budget for {} in {} because monthly profile length {}",
                        generator.name,
                        year,
                        len(monthly_profile),
                    )
                    continue
                hourly_budget_result = monthly_to_hourly_polars(year, monthly_profile)
                if hourly_budget_result.is_err():
                    logger.warning(
                        "Skipping hydro budget for {} in {}: {}",
                        generator.name,
                        year,
                        hourly_budget_result.err(),
                    )
                    continue
                hourly_budget_arr = hourly_budget_result.ok()
                if hourly_budget_arr is None:
                    logger.warning("Skipping hydro budget for {} in {}: empty result", generator.name, year)
                    continue
                hourly_budget = truncate_and_cast_time_series(hourly_budget_arr)
                ts = SingleTimeSeries.from_array(
                    data=hourly_budget,
                    name="hydro_budget",
                    initial_timestamp=self.initial_timestamp,
                    resolution=timedelta(hours=1),
                )

                system.add_time_series(ts, generator, solve_year=year)
                logger.trace("Adding hydro budget to {}", generator.label)
                attached_budgets += 1
        logger.info("Attached {} hydro budget profiles", attached_budgets)
        return Ok(None)

    def _postprocess_system(self, system: System) -> None:
        """Perform post-processing and finalization of the constructed system.

        Sets system-level metadata and logs summary statistics:
        - Data format version (from meta.csv tag column)
        - System description incorporating case name, scenario, solve/weather years
        - Component statistics and system validation information

        This is the final step after components and time series are built,
        ensuring the system is properly documented and ready for export.
        """
        logger.info("Post-processing ReEDS system...")
        logger.trace("Setting data format version to {}", self.ctx.current_version)

        if self.ctx.current_version is not None:
            system.data_format_version = self.ctx.current_version
        system.description = (
            f"ReEDS model system for case '{self.config.case_name}', "
            f"scenario '{self.config.scenario}', "
            f"solve years: {self.config.solve_year}, "
            f"weather years: {self.config.weather_year}"
        )

        # Count component types for summary
        component_counts = {
            comp_type.__name__: sum(1 for _ in system.get_components(comp_type))
            for comp_type in system.get_component_types()
        }

        logger.info("System name: {}", system.name)
        logger.info(
            "System composition: {} regions, {} generators, {} demands, {} reserves, {} transmission lines ({} total components)",
            component_counts.get("ReEDSRegion", 0),
            component_counts.get("ReEDSGenerator", 0),
            component_counts.get("ReEDSDemand", 0),
            component_counts.get("ReEDSReserve", 0),
            component_counts.get("ReEDSTransmissionLine", 0),
            sum(component_counts.values()),
        )
        logger.info("Post-processing complete")

    def _ensure_config_assets(self) -> Result[None, str]:
        """Load and cache plugin config assets."""
        if self._config_assets is not None:
            logger.trace("Config assets already loaded.")
            return Ok(None)

        try:
            assets = self.config.load_config(overrides=self._config_overrides)
        except FileNotFoundError as exc:
            return Err(f"Plugin config files missing: {exc}")

        self._config_assets = assets
        self._defaults = assets.get("defaults", {})
        logger.trace("Config assets loading successful")
        return Ok(None)

    def _initialize_caches(self) -> None:
        """Reset parser caches for a fresh build."""
        self._variable_generator_df = None
        self._non_variable_generator_df = None
        self._region_cache = {}
        self._generator_cache = {}
        self._interface_cache = {}
        self._reserve_region_cache = {}
        self._hydro_cf_prepared = None
        self._reserve_percentages = {}

    def _capacity_expansion_is_available(self) -> bool:
        """Return whether all canonical capacity-expansion files exist."""
        required_names = (
            "modeled_years",
            "planning_present_value_factors",
            "planning_switches",
            "planning_representative_timepoints",
            "planning_plant_characteristics",
            "existing_capacity",
        )
        for name in required_names:
            if name not in self.store:
                return False
            data_file = self.store[name]
            if data_file.fpath is not None:
                if not data_file.fpath.exists():
                    return False
            elif data_file.relative_fpath is not None:
                if not (self.store.folder / Path(data_file.relative_fpath)).exists():
                    return False
            elif data_file.glob is None or not any(self.store.folder.glob(data_file.glob)):
                return False
        return True

    def _initialize_parser_globals(self) -> Result[None, str]:
        """Initialize parser configuration, rules, and metadata from loaded assets."""
        res = self._ensure_config_assets()
        if res.is_err():
            return res

        self.solve_years = (
            [self.config.solve_year]
            if isinstance(self.config.solve_year, int)
            else list(self.config.solve_year)
        )

        self.weather_years = (
            [self.config.weather_year]
            if isinstance(self.config.weather_year, int)
            else list(self.config.weather_year)
        )
        res = self._prepare_rules_by_target()
        if res.is_err():
            return res

        res = self._prepare_default_metadata()
        if res.is_err():
            return res
        logger.trace("Parser global variables set")
        return Ok(None)

    def _prepare_default_metadata(self) -> Result[None, str]:
        """Initialize important configuration from the parser."""
        self._tech_categories = self._defaults.get("tech_categories", {})
        self._excluded_techs = self._defaults.get("excluded_techs", [])
        configured_names = self._defaults.get("generator_datasets", {})
        required_names = {
            "capacity",
            "fuel_price",
            "biofuel_price",
            "fuel_tech_map",
            "heat_rate",
            "cost_vom",
            "forced_outages",
            "planned_outages",
            "maxage",
            "storage_duration",
            "storage_efficiency",
            "storage_duration_out",
            "consume_characteristics",
            "ramp_rate",
        }
        if not isinstance(configured_names, dict) or not required_names <= configured_names.keys():
            return Err("generator_datasets must configure every generator input dataset")
        self._generator_dataset_names = {name: str(configured_names[name]) for name in required_names}
        return Ok(None)

    def _prepare_generator_datasets(self) -> Result[None, str]:
        """Load and preprocess generator-related datasets."""
        logger.trace("Preparing generator datasets with excluded techs: {}", self._excluded_techs)
        dataset_names = self._generator_dataset_names
        datasets = {role: self.read_data_file(dataset_name) for role, dataset_name in dataset_names.items()}
        generator_data_result = prepare_generator_datasets(
            datasets,
            excluded_technologies=self._excluded_techs,
            technology_categories=self._tech_categories,
            default_values=self._defaults.get("default_values", {}),
        )
        if generator_data_result.is_err():
            return Err(str(generator_data_result.err()))
        generator_data = generator_data_result.ok()
        if generator_data is None:
            return Err("Generator data result was unexpectedly None")
        variable_df, non_variable_df = generator_data

        self._variable_generator_df = variable_df
        self._non_variable_generator_df = non_variable_df

        logger.trace(
            "Prepared generator datasets - variable rows: {}, non-variable rows: {}",
            self._variable_generator_df.height,
            self._non_variable_generator_df.height,
        )
        return Ok(None)

    def _prepare_hydro_datasets(self) -> Result[None, str]:
        """Prepare hydro capacity factor data for later budget attachment."""
        hydro_cf = self.read_data_file("hydro_cf")
        if hydro_cf is None:
            self._hydro_cf_prepared = None
            logger.trace("No hydro CF dataset available")
            return Ok(None)

        hydro_cf = (
            hydro_cf.with_columns(
                pl.col("month")
                .map_elements(lambda x: self.month_map.get(x, x), return_dtype=pl.Int16)
                .alias("month_num"),
            )
            .sort(["year", "technology", "region", "month_num"])
            .collect()
        )
        hydro_cf_joined = hydro_cf.join(self.year_month_day_hours, on=["year", "month_num"], how="left")
        self._hydro_cf_prepared = hydro_cf_joined
        logger.trace("Hydro CF prepared rows: {}", hydro_cf_joined.height)
        return Ok(None)

    def _prepare_reserve_datasets(self) -> Result[None, str]:
        """Prepare reserve-related inputs (percentages, costs)."""
        logger.trace("Preparing reserve dataset defaults")
        reserve_percentages: dict[ReserveType, ReEDSReservePercentages] = {}
        reserve_pct_df = self.read_data_file("reserve_percentages")
        if reserve_pct_df is not None:
            df = reserve_pct_df.collect()
            for row in df.iter_rows(named=True):
                reserve_type_result = RESERVE_TYPE_MAP.get(str(row.get("reserve_type") or "").upper())
                if reserve_type_result is None:
                    continue
                reserve_percentages[reserve_type_result] = ReEDSReservePercentages.model_validate(
                    {**row, "reserve_type": reserve_type_result}
                )
        else:
            load_reserves = self._defaults.get("load_reserves", {})
            wind_reserves = self._defaults.get("wind_reserves", {})
            solar_reserves = self._defaults.get("solar_reserves", {})
            for reserve_name in set(load_reserves) | set(wind_reserves) | set(solar_reserves):
                reserve_type = RESERVE_TYPE_MAP.get(str(reserve_name).upper())
                if reserve_type is None:
                    continue
                reserve_percentages[reserve_type] = ReEDSReservePercentages(
                    reserve_type=reserve_type,
                    or_load_percentage=load_reserves.get(reserve_name, 0.0) or 0.0,
                    or_wind_percentage=wind_reserves.get(reserve_name, 0.0) or 0.0,
                    or_pv_percentage=solar_reserves.get(reserve_name, 0.0) or 0.0,
                )
        self._reserve_percentages = reserve_percentages
        logger.trace("Prepared reserve percentage models: {}", len(self._reserve_percentages))

        return Ok(None)

    def _prepare_rules_by_target(self) -> Result[None, str]:
        """Load config assets and parse rules once."""

        parser_rules_raw = self._config_assets.get("parser_rules") if self._config_assets else None
        if parser_rules_raw is None:
            return Err("Parser rules are missing from plugin config")

        try:
            rules = Rule.from_records(parser_rules_raw)
            self._parser_rules = rules
        except Exception as exc:
            return Err(f"Failed to parse parser rules: {exc}")

        rules_result = get_rules_by_target(rules)
        if rules_result.is_err():
            return Err(str(rules_result.err()))

        rules_by_target = rules_result.ok()
        if rules_by_target is None:
            return Err("Rules by target result was unexpectedly None")
        self._rules_by_target = rules_by_target
        logger.trace("Rule(s) by target type set.")
        return Ok(None)

    def _prepare_time_arrays(self) -> Result[None, str]:
        """Set up solve/weather year lists and derived time indices."""

        self.hourly_time_index = np.arange(
            np.datetime64(f"{self.config.primary_weather_year}"),
            np.datetime64(f"{self.config.primary_weather_year + 1}"),
            dtype="datetime64[h]",
        )
        self.daily_time_index = np.arange(
            np.datetime64(f"{self.config.primary_weather_year}"),
            np.datetime64(f"{self.config.primary_weather_year + 1}"),
            dtype="datetime64[D]",
        )
        self.initial_timestamp = self.hourly_time_index[0].astype("datetime64[s]").astype(datetime)
        self.month_map = {calendar.month_abbr[i].lower(): i for i in range(1, 13)}
        self.year_month_day_hours = pl.DataFrame(
            {
                "year": [y for y in self.solve_years for _ in range(1, 13)],
                "month_num": [m for _ in self.solve_years for m in range(1, 13)],
                "days_in_month": [
                    calendar.monthrange(y, m)[1] for y in self.solve_years for m in range(1, 13)
                ],
                "hours_in_month": [
                    calendar.monthrange(y, m)[1] * 24 for y in self.solve_years for m in range(1, 13)
                ],
            }
        )
        logger.debug(
            "Created time indixes for weather year {}: {} hours, {} days starting at {}",
            self.config.primary_weather_year,
            len(self.hourly_time_index),
            len(self.daily_time_index),
            self.initial_timestamp,
        )
        return Ok(None)
