from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any


class PromptTemplate(BaseModel):
    """
    Represents a prompt template.

    Args:
        role: The role of the prompt template.
        content: The content of the prompt template.
    """
    role: str
    content: str


class PromptTemplateSpec(BaseModel):
    """
    Represents a prompt template specification.

    Args:
        template: The list of prompt templates.
        defaults: The default values for the prompt template fields.
        additional_fields: Additional fields for the prompt template.
    """
    template: List[PromptTemplate]
    defaults: Optional[Dict[Any, Any]] = Field(default_factory=dict)
    additional_fields: Optional[Dict[Any, Any]] = Field(default_factory=dict)


class PromptTemplatePostRequest(BaseModel):
    """
    Represents a request to create a prompt template.

    Args:
        name: The name of the prompt template.
        version: The version of the prompt template.
        scenario: The scenario of the prompt template.
        spec: The specification of the prompt template.
    """
    name: str
    version: str
    scenario: str
    spec: PromptTemplateSpec


class PromptTemplatePostResponse(BaseModel):
    """
    Represents a response to a request to create a prompt template.

    Args:
        message: The message of the response.
        id: The ID of the prompt template.
        scenario: The scenario of the prompt template.
        name: The name of the prompt template.
        version: The version of the prompt template.
    """
    message: str
    id: str
    scenario: str
    name: str
    version: str


class PromptTemplateGetResponse(BaseModel):
    """
    Represents a response to a request to get a prompt template.

    Args:
        id: The ID of the prompt template.
        name: The name of the prompt template.
        version: The version of the prompt template.
        scenario: The scenario of the prompt template.
        creation_timestamp: The creation timestamp of the prompt template.
        managed_by: The manager of the prompt template.
        is_version_head: Whether the version is the head version.
        spec: The specification of the prompt template.
    """
    id: str
    name: str
    version: str
    scenario: str
    creation_timestamp: Optional[str] = None
    managed_by: Optional[str] = None
    is_version_head: Optional[bool] = None
    spec: Optional[PromptTemplateSpec] = None


class PromptTemplateListResponse(BaseModel):
    """
    Represents a response to a request to list prompt templates.

    Args:
        count: The number of prompt templates.
        resources: The list of PromptGetResponse objects.
    """
    count: int
    resources: List[PromptTemplateGetResponse]


class PromptTemplateDeleteResponse(BaseModel):
    """
    Represents a response to a request to delete a prompt template.

    Args:
        message: The message of the response.
    """
    message: str


class PromptTemplateSubstitutionRequest(BaseModel):
    """
    Represents a request to substitute a prompt template.

    Args:
        input_params: User provided values to replace the placeholders of the prompt template.
    """
    input_params: Optional[Dict[Any, Any]] = Field(default_factory=dict)


class PromptTemplateSubstitutionResponse(BaseModel):
    """
    Represents a response to a request to substitute a prompt template.

    Args:
        parsed_prompt: The parsed prompt.
        resource: List of TemplateGetResponse objects.
    """
    parsed_prompt: List[PromptTemplate]
    resource: Optional[PromptTemplateGetResponse] = None
