"""Ingestion patterns and source-specific ingestion modules.

Each source is its own module (e.g., `cdc_nwss.py`, `census_acs.py`). Modules
return PySpark DataFrames or write to raw layer tables directly via Delta.
LDP entrypoints in `bundles/<subject>/src/` import from here.
"""
