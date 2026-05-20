"""Reference data generation logic (ADR 0014).

Pure, testable functions that produce canonical reference data — time,
geography, code systems. Thin entrypoints in `bundles/_reference/src/` call
these and write the results to the integrated catalog (`ecdh_model_<env>`).

Keeping the logic here (not in the bundle) means it's unit-testable without a
Spark session or a workspace.
"""
