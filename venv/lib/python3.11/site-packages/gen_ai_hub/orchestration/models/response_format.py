import re
from typing import Optional
from enum import Enum

from gen_ai_hub.orchestration.models.base import JSONSerializable


class ResponseFormatType(str, Enum):
    """
    Enumerates the supported response format.

    Response format that the model output should adhere to. This is the same as the OpenAI definition.

    Values:
        TEXT: Response format as text
        JSON_OBJECT: Response format as json object
        JSON_SCHEMA: Response format as defined json schema
    """
    TEXT = "text"
    JSON_OBJECT = "json_object"
    JSON_SCHEMA = "json_schema"


class ResponseFormatText(JSONSerializable):
    """
    Response format that the model output should adhere to. 
    """

    def to_dict(self):
        return {"type": ResponseFormatType.TEXT}


class ResponseFormatJsonObject(JSONSerializable):
    """
    Response format JSON Object that the model output should adhere to.
    """

    def to_dict(self):
        return {"type": ResponseFormatType.JSON_OBJECT}


class ResponseFormatJsonSchema(JSONSerializable):
    """
    Response format JSON Schema that the model output should adhere to.

    Args:
        name: The name of the response format.
        description: A description of what the response format is for.
        schema: A schema for the response format described as a JSON Schema object.
        strict: Whether to enable strict schema adherence when generating the output.
    """

    def __init__(
            self,
            name,
            schema: object = None,
            description: Optional[str] = None,
            strict: bool = False
    ):
        self.name = Validator.validate_name(name)
        self.desciption = description
        self.schema = schema
        self.strict = strict

    def to_dict(self):
        json_schema = {
                "name": self.name,
                "strict": self.strict,
                "schema": self.schema
        }
        if self.desciption:
            json_schema['description'] = self.desciption
        return {
            "type": ResponseFormatType.JSON_SCHEMA,
            "json_schema": json_schema
        }


class ResponseFormatFactory():
    """
    Factory class that maps response format input to classes that can handle to_dict conversion.
    """
    @staticmethod
    def create_response_format_object(response_format):
        """Map the response format to corresponding class."""
        if response_format == ResponseFormatType.TEXT:
            return ResponseFormatText()

        if response_format == ResponseFormatType.JSON_OBJECT:
            return ResponseFormatJsonObject()

        if isinstance(response_format, ResponseFormatJsonSchema):
            return response_format
        
        return None

 
class Validator():
    """
    A utility class for validating response format names.

    This class provides methods to validate the names of response formats to ensure 
    they adhere to specified patterns and length constraints.
    """
    @staticmethod
    def validate_name(name):
        """
        Validates the name of the response format.

        Args:
            name (str): The name to validate.

        Returns:
            str: The validated name.

        Raises:
            ValueError: If the name does not match the required pattern or exceeds the maximum length.
        """
        pattern = r'^[a-zA-Z0-9_-]+$'
        if re.match(pattern, name):
            if len(name) > 64:
                raise ValueError("The name of the response format must be a-z, A-Z, 0-9, "
                                 "or contain underscores and dashes, with a maximum length of 64.")
        else:
            raise ValueError("The name of the response format must be a-z, A-Z, 0-9, "
                             "or contain underscores and dashes, with a maximum length of 64.")

        return name
    