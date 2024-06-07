# coding: utf-8

"""
    GraphScope FLEX HTTP SERVICE API

    This is a specification for GraphScope FLEX HTTP service based on the OpenAPI 3.0 specification. You can find out more details about specification at [doc](https://swagger.io/specification/v3/).  Some useful links: - [GraphScope Repository](https://github.com/alibaba/GraphScope) - [The Source API definition for GraphScope Interactive](https://github.com/GraphScope/portal/tree/main/httpservice)

    The version of the OpenAPI document: 0.9.1
    Contact: graphscope@alibaba-inc.com
    Generated by OpenAPI Generator (https://openapi-generator.tech)

    Do not edit the class manually.
"""  # noqa: E501


from __future__ import annotations
import pprint
import re  # noqa: F401
import json

from pydantic import BaseModel, StrictStr, field_validator
from typing import Any, ClassVar, Dict, List, Optional
from typing import Optional, Set
from typing_extensions import Self

class VertexDataSource(BaseModel):
    """
    VertexDataSource
    """ # noqa: E501
    data_source: Optional[StrictStr] = None
    type_name: Optional[StrictStr] = None
    location: Optional[StrictStr] = None
    property_mapping: Optional[Dict[str, Any]] = None
    __properties: ClassVar[List[str]] = ["data_source", "type_name", "location", "property_mapping"]

    @field_validator('data_source')
    def data_source_validate_enum(cls, value):
        """Validates the enum"""
        if value is None:
            return value

        if value not in set(['ODPS', 'FILE']):
            raise ValueError("must be one of enum values ('ODPS', 'FILE')")
        return value

    model_config = {
        "populate_by_name": True,
        "validate_assignment": True,
        "protected_namespaces": (),
    }


    def to_str(self) -> str:
        """Returns the string representation of the model using alias"""
        return pprint.pformat(self.model_dump(by_alias=True))

    def to_json(self) -> str:
        """Returns the JSON representation of the model using alias"""
        # TODO: pydantic v2: use .model_dump_json(by_alias=True, exclude_unset=True) instead
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str) -> Optional[Self]:
        """Create an instance of VertexDataSource from a JSON string"""
        return cls.from_dict(json.loads(json_str))

    def to_dict(self) -> Dict[str, Any]:
        """Return the dictionary representation of the model using alias.

        This has the following differences from calling pydantic's
        `self.model_dump(by_alias=True)`:

        * `None` is only added to the output dict for nullable fields that
          were set at model initialization. Other fields with value `None`
          are ignored.
        """
        excluded_fields: Set[str] = set([
        ])

        _dict = self.model_dump(
            by_alias=True,
            exclude=excluded_fields,
            exclude_none=True,
        )
        return _dict

    @classmethod
    def from_dict(cls, obj: Optional[Dict[str, Any]]) -> Optional[Self]:
        """Create an instance of VertexDataSource from a dict"""
        if obj is None:
            return None

        if not isinstance(obj, dict):
            return cls.model_validate(obj)

        _obj = cls.model_validate({
            "data_source": obj.get("data_source"),
            "type_name": obj.get("type_name"),
            "location": obj.get("location"),
            "property_mapping": obj.get("property_mapping")
        })
        return _obj


