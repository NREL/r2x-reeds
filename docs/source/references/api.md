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

### Planning inputs

The parser reads canonical planning datasets through the ``r2x-core``
``DataStore`` and materializes first-class planning records in the target
``infrasys.System``. A run-level ``ReEDSPlanningSwitches`` component owns the
switches, while ``ReEDSPlanningPeriod`` supplemental attributes can be attached
to the switches and to every technology-year ``ReEDSPlantCharacteristics``
component. Representative timepoints are global components.

Planning inputs include modeled years, present-value factors, annual emissions
caps, representative-timepoint weights, plant characteristics, initial
capacity, and storage-duration records. The parser preserves technology-level
durations, selected pumped-storage supply-curve duration, and
regional/vintage-specific overrides as separate components. PSH supply-curve
data is read only when ``GSw_Storage`` is enabled; regional PSH duration data is
read only when both ``GSw_Storage`` and ``GSw_HydroPSHDurData`` are enabled.

```{eval-rst}
.. autopydantic_model:: r2x_reeds.ReEDSPlanningSwitches
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

.. autopydantic_model:: r2x_reeds.ReEDSPumpedStorageSupplyCurveDuration
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
