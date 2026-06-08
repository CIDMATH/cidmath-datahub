#!/usr/bin/env python3
"""Validate Databricks bundle config against dev.

`just validate-all` runs this with no args (every bundle); `just validate
<name>` passes one bundle name. A "bundle" is any directory under ``bundles/``
that contains a ``databricks.yml``. CI validates the same set in parallel via a
dynamic matrix (.github/workflows/ci.yml); this is the sequential local
equivalent so the command lives in one place.

Requires the Databricks CLI on PATH and a valid auth session (see onboarding).
"""

from __future__ import annotations

import pathlib
import subprocess
import sys

BUNDLES_DIR = pathlib.Path(__file__).resolve().parents[2] / "bundles"


def _discover() -> list[pathlib.Path]:
    if not BUNDLES_DIR.is_dir():
        return []
    return sorted(d for d in BUNDLES_DIR.iterdir() if (d / "databricks.yml").is_file())


def main(argv: list[str]) -> int:
    only = argv[1] if len(argv) > 1 else None
    bundles = _discover()
    if only:
        bundles = [b for b in bundles if b.name == only]
        if not bundles:
            print(f"No bundle named {only!r} (with a databricks.yml) under bundles/.")
            return 1
    if not bundles:
        print("No bundles found under bundles/.")
        return 1

    failed: list[str] = []
    for d in bundles:
        print(f"== validating {d.name} ==", flush=True)
        result = subprocess.run(  # noqa: S603 — fixed argv, no shell
            ["databricks", "bundle", "validate", "--target", "dev"], cwd=d
        )
        if result.returncode != 0:
            failed.append(d.name)

    if failed:
        print(f"\nFAILED: {', '.join(failed)}")
        return 1
    print(f"\nAll {len(bundles)} bundle(s) valid.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
