<div align="center">

# r2x-reeds

**ReEDS parser and transforms plugin for the `r2x-core` plugin framework.**

[![CI](https://img.shields.io/github/actions/workflow/status/NREL/r2x-reeds/ci.yaml?branch=main&label=CI)](https://github.com/NREL/r2x-reeds/actions/workflows/ci.yaml)
[![Actions Quality](https://img.shields.io/github/actions/workflow/status/NREL/r2x-reeds/workflow-quality.yaml?branch=main&label=actions-quality)](https://github.com/NREL/r2x-reeds/actions/workflows/workflow-quality.yaml)
[![Python](https://img.shields.io/badge/python-3.11%20to%203.13-blue)](https://pypi.org/project/r2x-reeds/)
[![PyPI](https://img.shields.io/pypi/v/r2x-reeds)](https://pypi.org/project/r2x-reeds/)
[![License](https://img.shields.io/badge/license-BSD--3--Clause-green)](./LICENSE.txt)
[![codecov](https://codecov.io/gh/NREL/r2x-reeds/branch/main/graph/badge.svg)](https://codecov.io/gh/NREL/r2x-reeds)
[![Documentation](https://github.com/NREL/r2x-reeds/actions/workflows/docs.yaml/badge.svg?branch=main)](https://nrel.github.io/r2x-reeds/)

</div>

> [!WARNING]
> This project is currently optimized for internal R2X workflows. You are welcome
> to use it, but APIs and behavior may continue to evolve as `r2x-core` evolves.

`r2x-reeds` integrates [NREL ReEDS](https://github.com/NREL/ReEDS-2.0) model data
with `r2x-core` and `infrasys`. It provides a parser plugin for building
`infrasys.System` objects from ReEDS data, plus a set of reusable post-parse
transforms for common system-modification workflows.

<p align="center">
  <a href="#quickstart">Quickstart</a> ·
  <a href="#installation">Installation</a> ·
  <a href="#what-it-provides">What It Provides</a> ·
  <a href="#usage-with-r2x-core">Usage with r2x-core</a> ·
  <a href="#development">Development</a> ·
  <a href="#license">License</a>
</p>

## Quickstart

Install:

```bash
pip install r2x-reeds
```

Parse a ReEDS run directory into an `infrasys.System`:

```python
from pathlib import Path

from r2x_core import DataStore, PluginContext
from r2x_reeds import ReEDSConfig, ReEDSParser

run_path = Path("path/to/reeds_run")

config = ReEDSConfig(
    solve_year=2030,
    weather_year=2012,
    case_name="test_Pacific",
)
ctx = PluginContext(
    config=config,
    store=DataStore.from_plugin_config(config, path=run_path),
)

system = ReEDSParser.from_context(ctx).run().system
print(system.name)
```

## Installation

### From PyPI

Python requirement: `>=3.11, <3.14`.

```bash
pip install r2x-reeds
```

Using `uv`:

```bash
uv add r2x-reeds
```

### From Source

```bash
git clone https://github.com/NREL/r2x-reeds.git
cd r2x-reeds
uv sync --all-groups
```

## What It Provides

- `ReEDSParser`: reads ReEDS outputs and inputs (CSV/HDF5-backed mappings) into
  `infrasys.System` components and time series.
- `ReEDSUpgrader` and `run_reeds_upgrades(...)`: input version-detection and
  upgrade pipeline run during parser lifecycle.
- Plugin entry point for `r2x-core` under the `r2x_plugin` group:
  - `reeds-parser = r2x_reeds:ReEDSParser`
- Transform entry points under `r2x.transforms`:
  - `add-pcm-defaults`
  - `add-emission-cap`
  - `add-electrolyzer-load`
  - `add-purchaser-load`
  - `add-ccs-credit`
  - `break-gens`
  - `add-imports`
  - `add-optimal-siting`

## Usage with r2x-core

`r2x-reeds` follows the `r2x-core` plugin lifecycle.

- Build parser instances with `PluginContext`.
- Run lifecycle hooks with `.run()`.
- Configure parsing through `ReEDSConfig`.
- Apply optional `r2x.transforms` after parsing for scenario/system modifiers.

## Development

Install dev dependencies:

```bash
uv sync --all-groups
```

Run the same checks used in CI:

```bash
uv run prek run --all-files --hook-stage pre-push
```

Targeted commands:

```bash
uv run pytest -q -m "not slow" --maxfail=1 --disable-warnings
uv run ty check ./src/r2x_reeds/
```

## License

BSD 3-Clause. See `LICENSE.txt`.
