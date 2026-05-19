"""Data quality framework helpers (ADR 0009).

LDP's `@dlt.expect` decorators are the default mechanism inside LDP pipelines.
This module provides helpers for plain-Jobs pipelines and for the quarantine
table pattern that LDP's `expect_or_drop` doesn't fully cover natively.
"""
