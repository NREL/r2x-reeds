# Transforms Reference

Transforms are optional post-parse system modifiers exposed under the `r2x.transforms` entry point group.

## Available Transforms

- `add-pcm-defaults`
- `add-emission-cap`
- `add-electrolyzer-load`
- `add-purchaser-load`
- `add-ccs-credit`
- `break-gens`
- `add-imports`
- `add-optimal-siting`

## Purchaser Load Transform

Transform id: `add-purchaser-load`

Module: `r2x_reeds.sysmod.purchaser_load`

Function: `add_purchaser_load(system, config)`

Purpose:

- Adds/updates electrolyzer and data center purchaser-load components
- Attaches hourly load profiles for those components

Important behavior:

- If electrolyzer-demand components already exist, cap-based electrolyzer creation is skipped to avoid double counting.

Config model: `PurchaserLoadConfig`

Main fields:

- `solve_year`
- `weather_year`
- `hour_map_myr_fpath`
- `electrolyzer_capacity_fpath`
- `consume_characteristics_fpath`
- `electrolyzer_prod_load_fpath`
- `electrolyzer_prod_load_ann_fpath`
- `loadsite_op_fpath`

## Legacy Electrolyzer Transform

Transform id: `add-electrolyzer-load`

Module: `r2x_reeds.sysmod.electrolyzer`

Purpose:

- Legacy workflow for adding electrolyzer load and hydrogen fuel price time series

## Other Transforms

- `add-pcm-defaults`: apply PCM default attributes to generators
- `add-emission-cap`: add annual emissions constraints and optional precombustion adjustments
- `add-ccs-credit`: apply CCS credit economics
- `break-gens`: split generators into reference units
- `add-imports`: apply import/export handling
- `add-optimal-siting`: add loadsite increments to existing load profiles
