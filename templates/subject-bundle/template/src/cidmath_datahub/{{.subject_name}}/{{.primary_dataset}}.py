"""Pure parse / transform / conform logic for {{.provider_code}} {{.primary_dataset}} ({{.subject_name}}).

Logic lives here (unit-tested, no Spark) so the {{.subject_name}} bundle entrypoints
stay thin (ADR 0011). The entrypoints import and orchestrate these functions via
the ``run_build`` seam (ADR 0027). Keep IO (HTTP, Spark) out of this module.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any


def parse_records(lines: Iterable[str]) -> list[dict[str, Any]]:
    """Parse raw source records into typed long-form rows.

    TODO({{.subject_name}}): implement against the documented source format, and
    unit-test against real sample records in tests/unit/{{.subject_name}}/.
    """
    raise NotImplementedError("implement {{.primary_dataset}} parsing")
