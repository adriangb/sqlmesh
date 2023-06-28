import typing as t
from typing_extensions import Annotated

from pydantic import BaseModel, ConfigDict, PlainSerializer, InstanceOf
from pydantic.fields import FieldInfo
from sqlglot import exp

DEFAULT_ARGS = {"exclude_none": True, "by_alias": True}


Expression = Annotated[
    exp.Expression,
    InstanceOf,
    PlainSerializer(lambda exp: exp.sql()),
]


class PydanticModel(BaseModel):
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="forbid",
        protected_namespaces=(),
        validate_default=True,
    )

    def dict(
        self,
        **kwargs: t.Any,
    ) -> t.Dict[str, t.Any]:
        return super().model_dump(**{**DEFAULT_ARGS, **kwargs})  # type: ignore

    def json(
        self,
        **kwargs: t.Any,
    ) -> str:
        return super().model_dump_json(**{**DEFAULT_ARGS, **kwargs})  # type: ignore

    @classmethod
    def missing_required_fields(
        cls: t.Type["PydanticModel"], provided_fields: t.Set[str]
    ) -> t.Set[str]:
        return cls.required_fields() - provided_fields

    @classmethod
    def extra_fields(cls: t.Type["PydanticModel"], provided_fields: t.Set[str]) -> t.Set[str]:
        return provided_fields - cls.all_fields()

    @classmethod
    def all_fields(cls: t.Type["PydanticModel"]) -> t.Set[str]:
        return cls._fields()

    @classmethod
    def required_fields(cls: t.Type["PydanticModel"]) -> t.Set[str]:
        return cls._fields(lambda field: field.is_required())

    @classmethod
    def _fields(
        cls: t.Type["PydanticModel"],
        predicate: t.Callable[[FieldInfo], bool] = lambda _: True,
    ) -> t.Set[str]:
        return {
            field.alias if field.alias else field_name
            for field_name, field in cls.model_fields.items()
            if predicate(field)
        }
