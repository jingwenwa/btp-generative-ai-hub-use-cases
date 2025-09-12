from typing import Optional

from gen_ai_hub import GenAIHubProxyClient
from gen_ai_hub.proxy import get_proxy_client
from .models.prompt_template import (
    PromptTemplatePostRequest,
    PromptTemplatePostResponse,
    PromptTemplateGetResponse,
    PromptTemplateListResponse,
    PromptTemplateDeleteResponse,
    PromptTemplateSubstitutionRequest,
    PromptTemplateSubstitutionResponse,
    PromptTemplateSpec,
)

# Constants
PATH_SCENARIOS = "/lm/scenarios"
PATH_PROMPT_TEMPLATES = "/lm/promptTemplates"
CONTENT_TYPE_JSON_ = "application/json"


class PromptTemplateClient:
    """
    Client for interacting with the Prompt Registry API.

    Args:
        proxy_client: The proxy client to use for making requests.
    """

    def __init__(
            self,
            proxy_client: Optional[GenAIHubProxyClient] = None,
    ):
        """
        Initializes the PromptTemplateClient
        Args:
            proxy_client: optional proxy client to use for requests
        """
        self.proxy_client = proxy_client or get_proxy_client(proxy_version="gen-ai-hub")
        self.rest_client = self.proxy_client.ai_core_client.rest_client

    def create_prompt_template(self, name: str, version: str, scenario: str,
                               prompt_template_spec: PromptTemplateSpec ) -> PromptTemplatePostResponse:
        """
        Create or update a prompt template.
        Args:
            scenario: The scenario name of the prompt template.
            name: The name of the prompt template.
            version: The version of the prompt template.
            prompt_template_spec: The specification of the prompt template.
        Returns:
            PromptTemplatePostResponse object
        """

        request = PromptTemplatePostRequest(scenario=scenario, name=name, version=version, spec=prompt_template_spec)
        response = self.rest_client.post(path=PATH_PROMPT_TEMPLATES, body=request.model_dump())

        return PromptTemplatePostResponse(**response)

    def get_prompt_templates(self, scenario: str, name: str, version: str, retrieve: str = None,
                             include_spec: bool = None) -> PromptTemplateListResponse:
        """
        Retrieve the latest version of every prompt template based on the filters.
        Args:
            scenario: scenario name
            name: template name
            version: template version
            retrieve: both(default), imperative, declarative
            include_spec: false(default), true
        Returns:
            PromptTemplateListResponse object
        """
        query_params = {
            "scenario": scenario,
            "name": name,
            "version": version,
            "retrieve": retrieve,
            "include_spec": include_spec
        }

        response = self.rest_client.get(path=PATH_PROMPT_TEMPLATES, params=query_params)

        return PromptTemplateListResponse(**response)

    def get_prompt_template_by_id(self, template_id: str) -> PromptTemplateGetResponse:
        """
        Retrieve a specific version of the prompt template by ID.
        Args:
            template_id: The ID of the prompt template to retrieve.
        Returns:
            PromptTemplateGetResponse object
        """
        response = self.rest_client.get(path=f"{PATH_PROMPT_TEMPLATES}/{template_id}")

        return PromptTemplateGetResponse(**response)

    def get_prompt_template_history(self, scenario: str, name: str, version: str) -> PromptTemplateListResponse:
        """
        List history of edits to the prompt template. Only for imperative managed prompt templates.
        Args:
            scenario: The scenario name of the prompt template.
            name: The name of the prompt template.
            version: The version ID of the prompt template.
        Returns:
            PromptTemplateListResponse object
        """
        response = self.rest_client.get(f"{PATH_SCENARIOS}/{scenario}/promptTemplates/{name}/versions/{version}/history")

        return PromptTemplateListResponse(**response)

    def delete_prompt_template_by_id(self, template_id: str) -> PromptTemplateDeleteResponse:
        """
        Delete a specific version of the prompt template. Only for imperative prompt templates.
        Args:
            template_id: The ID of the prompt template to delete.
        Returns:
            PromptTemplateDeleteResponse object
        """
        response = self.rest_client.delete(f"{PATH_PROMPT_TEMPLATES}/{template_id}")

        return PromptTemplateDeleteResponse(**response)

    def import_prompt_template(self, file: bytes) -> PromptTemplatePostResponse:
        """
        Import a runtime/declarative prompt template into the design time environment.
        Supports only single file import as of now.
        Args:
            file: binary file content
        Returns:
            PromptTemplatePostResponse object
        """

        # Content-Type: multipart/form-data is added automatically by requests when a file is passed in the request.
        kwargs = {"files": {"file": file}}
        response = self.rest_client.post(path=f"{PATH_PROMPT_TEMPLATES}/import", **kwargs)
        return PromptTemplatePostResponse(**response)

    def export_prompt_template(self, template_id: str) -> bytes:
        """
        Export a design time template in a declarative compatible yaml file. Supports only single file export.
        Args:
            template_id: The id of the prompt template to export.
        Returns:
            bytes: The content of the exported file
        """

        response = self.rest_client.get(path=f"{PATH_PROMPT_TEMPLATES}/{template_id}/export", return_bytes_content=True)

        return response

    def fill_prompt_template(self, scenario: str, name: str, version: str, input_params: dict,
                             metadata: bool = False) -> PromptTemplateSubstitutionResponse:
        """
        Replace the placeholders of the prompt template referenced via scenario-name-version with user provided values.
        Args:
            scenario: The scenario name of the prompt template.
            name: The name of the prompt template.
            version: The version of the prompt template.
            input_params: User provided values to replace the placeholders of the prompt template.
            metadata: False(default), True return resource object with all details.
        Returns:
            PromptTemplateSubstitutionResponse object
        """

        request =PromptTemplateSubstitutionRequest(input_params=input_params)
        kwargs = {'params': {"metadata": metadata}} if metadata else {}
        response = self.rest_client.post(path=(f"{PATH_SCENARIOS}/{scenario}/promptTemplates/{name}/versions/"
                                               f"{version}/substitution"),
                                         headers={"Content-Type": CONTENT_TYPE_JSON_},
                                         body=request.model_dump(),
                                         **kwargs)

        return PromptTemplateSubstitutionResponse(**response)

    def fill_prompt_template_by_id(self,
                                   template_id: str,
                                   input_params: dict,
                                   metadata: bool = False, ) -> PromptTemplateSubstitutionResponse:
        """
        Replace the placeholders of the prompt template referenced via template_id with user provided values.
        Args:
            template_id: The ID of the prompt template.
            input_params: User provided values to replace the placeholders of the prompt template.
            metadata: False(default), True return resource object with all details.
        Returns:
            PromptTemplateSubstitutionResponse object
        """

        request =PromptTemplateSubstitutionRequest(input_params=input_params)
        kwargs = {'params': {"metadata": metadata}}
        response = self.rest_client.post(path=f"{PATH_PROMPT_TEMPLATES}/{template_id}/substitution",
                                         headers={"Content-Type": CONTENT_TYPE_JSON_},
                                         body=request.model_dump(),
                                         **kwargs)

        return PromptTemplateSubstitutionResponse(**response)
