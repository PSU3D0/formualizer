"""Helpers that produce canonical FIO manifests, without a YAML dependency."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
from typing import Any, cast

from . import SheetPortSession


def _dict(value: Any) -> Any:
    if hasattr(value, "to_dict"):
        return value.to_dict()
    if isinstance(value, Mapping):
        return {key: _dict(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_dict(item) for item in value]
    return deepcopy(value)


class _Object:
    def __init__(self, value: Mapping[str, Any]) -> None:
        self._value: dict[str, Any] = _dict(value)

    def to_dict(self) -> dict[str, Any]:
        return deepcopy(self._value)


class Manifest(_Object):
    def __init__(
        self,
        id: str,
        *,
        ports: Sequence[Any],
        name: str | None = None,
        description: str | None = None,
        workbook: Mapping[str, Any] | None = None,
        tags: Sequence[str] | None = None,
    ) -> None:
        meta: dict[str, Any] = {"id": id, "name": id if name is None else name}
        for key, value in (
            ("description", description),
            ("workbook", workbook),
            ("tags", tags),
        ):
            if value is not None:
                meta[key] = value
        super().__init__(
            {
                "spec": "fio",
                "spec_version": "0.3.0",
                "manifest": meta,
                "ports": [_dict(port) for port in ports],
            }
        )

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> Manifest:
        result = cls.__new__(cls)
        _Object.__init__(result, value)
        return result

    @classmethod
    def from_yaml(cls, yaml: str) -> Manifest:
        return cls.from_dict(SheetPortSession.manifest_from_yaml(yaml))

    def to_dict(self) -> dict[str, Any]:
        # Let the canonical serde model supply optional defaults rather than
        # duplicating them here. This also makes YAML round trips exactly equal.
        return cast(dict[str, Any], SheetPortSession.manifest_from_yaml(self.to_yaml()))

    def to_yaml(self) -> str:
        return SheetPortSession.manifest_to_yaml(super().to_dict())


class _Port(_Object):
    direction: str

    def __init__(
        self,
        id: str,
        location: str | Mapping[str, Any],
        *,
        type: str | None = None,
        schema: Mapping[str, Any] | _Object | None = None,
        description: str | None = None,
        required: bool = True,
        default: Any = None,
        constraints: Mapping[str, Any] | None = None,
        units: Mapping[str, Any] | None = None,
    ) -> None:
        if (type is None) == (schema is None):
            raise TypeError("provide exactly one of type or schema")
        canonical_schema = {"type": type} if schema is None else _dict(schema)
        value = {
            "id": id,
            "dir": self.direction,
            "shape": (
                "range"
                if "cell_type" in canonical_schema
                else "table"
                if "columns" in canonical_schema
                else "record"
                if "fields" in canonical_schema
                else "scalar"
            ),
            "location": {"a1": location}
            if isinstance(location, str)
            else _dict(location),
            "schema": canonical_schema,
            "required": required,
        }
        for key, item in (
            ("description", description),
            ("default", default),
            ("constraints", constraints),
            ("units", units),
        ):
            if item is not None:
                value[key] = item
        super().__init__(value)


class Input(_Port):
    direction = "in"


class Output(_Port):
    direction = "out"


def name(value: str) -> dict[str, Any]:
    return {"name": value}


def table(value: str, area: str | None = None) -> dict[str, Any]:
    return {"table": {"name": value, **({"area": area} if area is not None else {})}}


def struct_ref(value: str) -> dict[str, Any]:
    return {"struct_ref": value}


def layout(**descriptor: Any) -> dict[str, Any]:
    return {"layout": descriptor}


def range_of(type: str) -> _Object:
    return _Object({"kind": "range", "cell_type": type})


def column(name: str, type: str, **options: Any) -> _Object:
    return _Object({"name": name, "type": type, **options})


def table_of(*, columns: Sequence[Any], keys: Sequence[str] | None = None) -> _Object:
    return _Object(
        {
            "kind": "table",
            "columns": [_dict(c) for c in columns],
            **({"keys": list(keys)} if keys is not None else {}),
        }
    )


def record_of(fields: Mapping[str, Any]) -> _Object:
    return _Object(
        {"kind": "record", "fields": {k: _dict(v) for k, v in fields.items()}}
    )
