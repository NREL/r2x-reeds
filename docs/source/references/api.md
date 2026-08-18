(api-reference)=

# API Reference

Complete API documentation for all r2x-reeds classes and functions.

## Parser

```{eval-rst}
.. autoclass:: r2x_reeds.ReEDSParser
   :members:
   :undoc-members:
   :show-inheritance:
```

## Configuration

```{eval-rst}
.. autopydantic_model:: r2x_reeds.ReEDSConfig
   :model-show-json: False
   :model-show-config-summary: False
   :model-show-validator-members: False
   :model-show-validator-summary: False
   :field-list-validators: False
```

## Upgrader

```{eval-rst}
.. autoclass:: r2x_reeds.upgrader.data_upgrader.ReEDSVersionDetector
   :members:
   :undoc-members:
   :show-inheritance:
```

```{eval-rst}
.. autoclass:: r2x_reeds.upgrader.data_upgrader.ReEDSUpgrader
   :members:
   :undoc-members:
   :show-inheritance:
```

## Component Models

### Generator

```{eval-rst}
.. autopydantic_model:: r2x_reeds.ReEDSGenerator
   :model-show-json: False
   :model-show-config-summary: False
   :model-show-validator-members: False
   :model-show-validator-summary: False
   :field-list-validators: False
```

### Capacity expansion

Use ``ReEDSParser.read_capacity_expansion_inputs()`` to read the canonical
``modeledyears.csv``, ``pvf_cap.csv``, ``switches.csv``, ``co2_cap.csv``,
``plantcharout.csv``, ``rep/numhours.csv``, ``capnonrsc.csv``,
``capnonrsc_energy.csv``, ``storage_duration.csv``, ``psh_sc_duration.csv``,
and ``storage_duration_pshdata.csv`` inputs.
``ReEDSCapacityExpansionInputs`` keeps technology-year plant characteristics
and non-resource initial capacity separate; ReEDS does not provide one complete
regional investment-candidate feasibility table. Identifiers use the plugin's
normalization rules. An inactive ``GSw_AnnualCap`` leaves planning-period
emission caps unset. Modes ``2`` and ``3`` both map to ``CO2E``; the latter's
hydrogen-leakage inclusion is not represented as a separate input field.

Use ``ReEDSCapacityExpansion`` and a concrete candidate subtype to represent a
formulation in an ``infrasys.System``. Candidates are not installed generators,
so zero ``initial_capacity`` is valid. ``ReEDSStorageCapacityExpansionResource``
represents fixed-duration storage; battery energy capacity and costs are
represented by the input data rather than inferred from duration. Technology
storage-duration defaults, selected PSH supply-curve duration, and
regional/vintage overrides remain distinct. ReEDS reads PSH supply-curve data
only when ``GSw_Storage`` is enabled, and applies regional PSH duration data
only when both ``GSw_Storage`` and ``GSw_HydroPSHDurData`` are enabled.

```{eval-rst}
.. autopydantic_model:: r2x_reeds.ReEDSCapacityExpansion
   :model-show-json: False
   :model-show-config-summary: False
   :model-show-validator-members: False
   :model-show-validator-summary: False
   :field-list-validators: False

.. autopydantic_model:: r2x_reeds.ReEDSPlanningPeriod
   :model-show-json: False
   :model-show-config-summary: False
   :model-show-validator-members: False
   :model-show-validator-summary: False
   :field-list-validators: False

.. autopydantic_model:: r2x_reeds.ReEDSRepresentativeTimepoint
   :model-show-json: False
   :model-show-config-summary: False
   :model-show-validator-members: False
   :model-show-validator-summary: False
   :field-list-validators: False

.. autopydantic_model:: r2x_reeds.ReEDSCapacityExpansionInputs
   :model-show-json: False
   :model-show-config-summary: False
   :model-show-validator-members: False
   :model-show-validator-summary: False
   :field-list-validators: False

.. autopydantic_model:: r2x_reeds.ReEDSPlantCharacteristics
   :model-show-json: False
   :model-show-config-summary: False
   :model-show-validator-members: False
   :model-show-validator-summary: False
   :field-list-validators: False

.. autopydantic_model:: r2x_reeds.ReEDSInitialCapacity
   :model-show-json: False
   :model-show-config-summary: False
   :model-show-validator-members: False
   :model-show-validator-summary: False
   :field-list-validators: False

.. autopydantic_model:: r2x_reeds.ReEDSStorageDuration
   :model-show-json: False
   :model-show-config-summary: False
   :model-show-validator-members: False
   :model-show-validator-summary: False
   :field-list-validators: False

.. autopydantic_model:: r2x_reeds.ReEDSStorageDurationOverride
   :model-show-json: False
   :model-show-config-summary: False
   :model-show-validator-members: False
   :model-show-validator-summary: False
   :field-list-validators: False

.. autopydantic_model:: r2x_reeds.ReEDSCapacityExpansionResource
   :model-show-json: False
   :model-show-config-summary: False
   :model-show-validator-members: False
   :model-show-validator-summary: False
   :field-list-validators: False

.. autopydantic_model:: r2x_reeds.ReEDSDispatchableCapacityExpansionResource
   :model-show-json: False
   :model-show-config-summary: False
   :model-show-validator-members: False
   :model-show-validator-summary: False
   :field-list-validators: False

.. autopydantic_model:: r2x_reeds.ReEDSVariableCapacityExpansionResource
   :model-show-json: False
   :model-show-config-summary: False
   :model-show-validator-members: False
   :model-show-validator-summary: False
   :field-list-validators: False

.. autopydantic_model:: r2x_reeds.ReEDSStorageCapacityExpansionResource
   :model-show-json: False
   :model-show-config-summary: False
   :model-show-validator-members: False
   :model-show-validator-summary: False
   :field-list-validators: False
```

### Region (Bus)

```{eval-rst}
.. autopydantic_model:: r2x_reeds.ReEDSRegion
   :model-show-json: False
   :model-show-config-summary: False
   :model-show-validator-members: False
   :model-show-validator-summary: False
   :field-list-validators: False
```

### Transmission Line

```{eval-rst}
.. autopydantic_model:: r2x_reeds.ReEDSTransmissionLine
   :model-show-json: False
   :model-show-config-summary: False
   :model-show-validator-members: False
   :model-show-validator-summary: False
   :field-list-validators: False
```

### Reserve Requirement

```{eval-rst}
.. autopydantic_model:: r2x_reeds.ReEDSReserve
   :model-show-json: False
   :model-show-config-summary: False
   :model-show-validator-members: False
   :model-show-validator-summary: False
   :field-list-validators: False
```

### Demand Profile

```{eval-rst}
.. autopydantic_model:: r2x_reeds.ReEDSDemand
   :model-show-json: False
   :model-show-config-summary: False
   :model-show-validator-members: False
   :model-show-validator-summary: False
   :field-list-validators: False
```

### Emission Rate

```{eval-rst}
.. autopydantic_model:: r2x_reeds.ReEDSEmission
   :model-show-json: False
   :model-show-config-summary: False
   :model-show-validator-members: False
   :model-show-validator-summary: False
   :field-list-validators: False
```

## Enumerations

### Emission Type

```{eval-rst}
.. autoclass:: r2x_reeds.EmissionType
   :members:
   :undoc-members:
   :show-inheritance:
```

### Reserve Type

```{eval-rst}
.. autoclass:: r2x_reeds.ReserveType
   :members:
   :undoc-members:
   :show-inheritance:
```

### Reserve Direction

```{eval-rst}
.. autoclass:: r2x_reeds.ReserveDirection
   :members:
   :undoc-members:
   :show-inheritance:
```

## See Also

- {doc}`../how-tos/index` - How-to guides
- {doc}`transforms` - Transform behavior and config overview
