from __future__ import annotations

import typing as t
from enum import Enum

from pydantic import (
    FieldValidationInfo,
    field_validator as pydantic_field_validator,
    model_validator,
)
from typing_extensions import Literal, Self
from sqlglot import exp
from sqlglot.time import format_time

from sqlmesh.core import dialect as d
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.pydantic import PydanticModel


class ModelKindMixin:
    @property
    def model_kind_name(self) -> ModelKindName:
        """Returns the model kind name."""
        raise NotImplementedError

    @property
    def is_incremental_by_time_range(self) -> bool:
        return self.model_kind_name == ModelKindName.INCREMENTAL_BY_TIME_RANGE

    @property
    def is_incremental_by_unique_key(self) -> bool:
        return self.model_kind_name == ModelKindName.INCREMENTAL_BY_UNIQUE_KEY

    @property
    def is_full(self) -> bool:
        return self.model_kind_name == ModelKindName.FULL

    @property
    def is_view(self) -> bool:
        return self.model_kind_name == ModelKindName.VIEW

    @property
    def is_embedded(self) -> bool:
        return self.model_kind_name == ModelKindName.EMBEDDED

    @property
    def is_seed(self) -> bool:
        return self.model_kind_name == ModelKindName.SEED

    @property
    def is_external(self) -> bool:
        return self.model_kind_name == ModelKindName.EXTERNAL

    @property
    def is_symbolic(self) -> bool:
        """A symbolic model is one that doesn't execute at all."""
        return self.model_kind_name in (ModelKindName.EMBEDDED, ModelKindName.EXTERNAL)

    @property
    def is_materialized(self) -> bool:
        return not (self.is_symbolic or self.is_view)

    @property
    def only_latest(self) -> bool:
        """Whether or not this model only cares about latest date to render."""
        return self.is_view or self.is_full


class ModelKindName(str, ModelKindMixin, Enum):
    """The kind of model, determining how this data is computed and stored in the warehouse."""

    INCREMENTAL_BY_TIME_RANGE = "INCREMENTAL_BY_TIME_RANGE"
    INCREMENTAL_BY_UNIQUE_KEY = "INCREMENTAL_BY_UNIQUE_KEY"
    FULL = "FULL"
    VIEW = "VIEW"
    EMBEDDED = "EMBEDDED"
    SEED = "SEED"
    EXTERNAL = "EXTERNAL"

    @property
    def model_kind_name(self) -> ModelKindName:
        return self


class ModelKind(PydanticModel, ModelKindMixin):
    name: ModelKindName

    @classmethod
    def field_validator(cls) -> classmethod:
        def _model_kind_validator(cls, v: t.Any) -> ModelKind | None:
            if v is None:
                return None

            if isinstance(v, ModelKind):
                return v

            if isinstance(v, d.ModelKind):
                name = v.this
                props = {prop.name: prop.args.get("value") for prop in v.expressions}
                klass: t.Type[ModelKind] = ModelKind
                if name == ModelKindName.INCREMENTAL_BY_TIME_RANGE:
                    klass = IncrementalByTimeRangeKind
                elif name == ModelKindName.INCREMENTAL_BY_UNIQUE_KEY:
                    klass = IncrementalByUniqueKeyKind
                elif name == ModelKindName.SEED:
                    klass = SeedKind
                elif name == ModelKindName.VIEW:
                    klass = ViewKind
                else:
                    props["name"] = ModelKindName(name)
                return klass(**props)

            if isinstance(v, dict):
                if v.get("name") == ModelKindName.INCREMENTAL_BY_TIME_RANGE:
                    klass = IncrementalByTimeRangeKind
                elif v.get("name") == ModelKindName.INCREMENTAL_BY_UNIQUE_KEY:
                    klass = IncrementalByUniqueKeyKind
                elif v.get("name") == ModelKindName.SEED:
                    klass = SeedKind
                elif v.get("name") == ModelKindName.VIEW:
                    klass = ViewKind
                else:
                    klass = ModelKind
                return klass(**v)

            name = (v.name if isinstance(v, exp.Expression) else str(v)).upper()

            try:
                return ModelKind(name=ModelKindName(name))
            except ValueError:
                raise ConfigError(f"Invalid model kind '{name}'")

        return pydantic_field_validator("kind", mode='before')(classmethod(_model_kind_validator))

    @property
    def model_kind_name(self) -> ModelKindName:
        return self.name

    def to_expression(self, **kwargs: t.Any) -> d.ModelKind:
        return d.ModelKind(this=self.name.value.upper(), **kwargs)


class TimeColumn(PydanticModel):
    column: str
    format: t.Optional[str] = None

    @classmethod
    def field_validator(cls) -> classmethod:
        def _time_column_validator(cls, v: t.Any) -> TimeColumn:
            if isinstance(v, exp.Tuple):
                kwargs = {
                    key: v.expressions[i].name
                    for i, key in enumerate(("column", "format")[: len(v.expressions)])
                }
                return TimeColumn(**kwargs)

            if isinstance(v, exp.Identifier):
                return TimeColumn(column=v.name)

            if isinstance(v, exp.Expression):
                return TimeColumn(column=v.name)

            if isinstance(v, str):
                return TimeColumn(column=v)
            return v

        return pydantic_field_validator("time_column", mode='before')(_time_column_validator)

    @pydantic_field_validator("column", mode="before")
    @classmethod
    def _column_validator(cls, v: str) -> str:
        if not v:
            raise ConfigError("Time Column cannot be empty.")
        return v

    @property
    def expression(self) -> exp.Column | exp.Tuple:
        """Convert this pydantic model into a time_column SQLGlot expression."""
        column = exp.to_column(self.column)
        if not self.format:
            return column

        return exp.Tuple(expressions=[column, exp.Literal.string(self.format)])

    def to_expression(self, dialect: str) -> exp.Column | exp.Tuple:
        """Convert this pydantic model into a time_column SQLGlot expression."""
        column = exp.to_column(self.column)
        if not self.format:
            return column

        return exp.Tuple(
            expressions=[
                column,
                exp.Literal.string(
                    format_time(self.format, d.Dialect.get_or_raise(dialect).INVERSE_TIME_MAPPING)
                ),
            ]
        )

    def to_property(self, dialect: str = "") -> exp.Property:
        return exp.Property(this="time_column", value=self.to_expression(dialect))


class _Incremental(ModelKind):
    batch_size: t.Optional[int] = None
    lookback: t.Optional[int] = None

    @pydantic_field_validator("batch_size", "lookback", mode="before")
    @classmethod
    def _int_validator(cls, v: t.Any, info: FieldValidationInfo) -> t.Optional[int]:
        if v is None:
            return None
        elif isinstance(v, exp.Expression):
            num = int(v.name)
        else:
            num = int(v)
        if num < 0:
            raise ConfigError(
                f"Invalid value {num} for {info.field_name}. The value should be greater than 0"
            )
        return num

    @model_validator(mode="after")
    def _kind_validator(self: Self) -> Self:
        if self.batch_size and self.lookback and self.batch_size < self.lookback:
            raise ValueError("batch_size cannot be less than lookback")
        return self


class IncrementalByTimeRangeKind(_Incremental):
    name: Literal[ModelKindName.INCREMENTAL_BY_TIME_RANGE] = ModelKindName.INCREMENTAL_BY_TIME_RANGE
    time_column: TimeColumn

    _time_column_validator = TimeColumn.field_validator()

    def to_expression(self, dialect: str = "", **kwargs: t.Any) -> d.ModelKind:
        return super().to_expression(expressions=[self.time_column.to_property(dialect)])


class IncrementalByUniqueKeyKind(_Incremental):
    name: Literal[ModelKindName.INCREMENTAL_BY_UNIQUE_KEY] = ModelKindName.INCREMENTAL_BY_UNIQUE_KEY
    unique_key: t.List[str]

    @pydantic_field_validator("unique_key", mode="before")
    def _parse_unique_key(cls, v: t.Any) -> t.List[str]:
        if isinstance(v, exp.Identifier):
            return [v.this]
        if isinstance(v, exp.Tuple):
            return [e.this for e in v.expressions]
        return [i.this if isinstance(i, exp.Identifier) else str(i) for i in v]


class ViewKind(ModelKind):
    name: Literal[ModelKindName.VIEW] = ModelKindName.VIEW
    materialized: bool = False

    @pydantic_field_validator("materialized", mode="before")
    def _parse_materialized(cls, v: t.Any) -> bool:
        if isinstance(v, exp.Expression):
            return bool(v.this)
        return bool(v)


class SeedKind(ModelKind):
    name: Literal[ModelKindName.SEED] = ModelKindName.SEED
    path: str
    batch_size: int = 1000

    @pydantic_field_validator("batch_size", mode="before")
    def _parse_batch_size(cls, v: t.Any) -> int:
        if isinstance(v, exp.Expression) and v.is_int:
            v = int(v.name)
        if not isinstance(v, int):
            raise ValueError("Seed batch size must be an integer value")
        if v <= 0:
            raise ValueError("Seed batch size must be a positive integer")
        return v

    @pydantic_field_validator("path", mode="before")
    @classmethod
    def _parse_path(cls, v: t.Any) -> str:
        if isinstance(v, exp.Literal):
            return v.this
        return str(v)

    def to_expression(self, **kwargs: t.Any) -> d.ModelKind:
        """Convert the seed kind into a SQLGlot expression."""
        return super().to_expression(
            expressions=[
                exp.Property(this=exp.Var(this="path"), value=exp.Literal.string(self.path)),
                exp.Property(
                    this=exp.Var(this="batch_size"),
                    value=exp.Literal.number(self.batch_size),
                ),
            ],
        )
