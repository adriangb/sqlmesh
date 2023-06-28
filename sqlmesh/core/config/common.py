from __future__ import annotations

import typing as t

from pydantic import field_validator
from sqlmesh.utils.errors import ConfigError


def _concurrent_tasks_validator(v: t.Any) -> int:
    if isinstance(v, str):
        v = int(v)
    if not isinstance(v, int) or v <= 0:
        raise ConfigError(
            f"The number of concurrent tasks must be an integer value greater than 0. '{v}' was provided"
        )
    return v


concurrent_tasks_validator = field_validator(
    "backfill_concurrent_tasks",
    "ddl_concurrent_tasks",
    "concurrent_tasks",
    mode='before',
    check_fields=False,
)(_concurrent_tasks_validator)


def _http_headers_validator(v: t.Any) -> t.Any:
    if isinstance(v, dict):
        return [(key, value) for key, value in v.items()]
    return v


http_headers_validator = field_validator(
    "http_headers",
    mode='before',
    check_fields=False,
)(_http_headers_validator)
