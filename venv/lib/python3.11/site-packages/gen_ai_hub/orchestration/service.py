"""
Module for orchestration service handling requests and responses.

Provides synchronous and asynchronous methods to run orchestration pipelines.
"""

from typing import List, Optional, Iterable, Union
from dataclasses import dataclass
from enum import Enum
from functools import wraps
from copy import deepcopy

import dacite
import httpx

from ai_api_client_sdk.models.status import Status

from gen_ai_hub import GenAIHubProxyClient
from gen_ai_hub.orchestration.models.base import JSONSerializable
from gen_ai_hub.orchestration.models.config import OrchestrationConfig
from gen_ai_hub.orchestration.models.message import Message
from gen_ai_hub.orchestration.models.template import TemplateValue
from gen_ai_hub.orchestration.models.response import OrchestrationResponse, OrchestrationResponseStreaming
from gen_ai_hub.orchestration.sse_client import SSEClient, AsyncSSEClient, _handle_http_error
from gen_ai_hub.proxy import get_proxy_client


COMPLETION_SUFFIX = "/completion"


@dataclass
class OrchestrationRequest(JSONSerializable):
    """
    Represents a request for the orchestration process, including configuration,
    template values, and message history.
    """
    config: OrchestrationConfig
    template_values: List[TemplateValue]
    history: List[Message]

    def to_dict(self):
        return {
            "orchestration_config": self.config.to_dict(),
            "input_params": {value.name: value.value for value in self.template_values},
            "messages_history": [message.to_dict() for message in self.history],
        }


def cache_if_not_none(func):
    """Custom cache decorator that only caches non-None results"""
    cache = {}

    @wraps(func)
    def wrapper(*args, **kwargs):
        key = (args, frozenset(kwargs.items()))  # Create hashable key for cache
        if key not in cache:
            result = func(*args, **kwargs)
            if result is not None:  # Only cache if result is not None
                cache[key] = result
            return result
        return cache[key]

    def cache_clear():
        cache.clear()

    wrapper.cache_clear = cache_clear
    return wrapper

# pylint: disable=too-many-arguments,too-many-positional-arguments
@cache_if_not_none
def discover_orchestration_api_url(base_url: str,
                                   auth_url: str,
                                   client_id: str,
                                   client_secret: str,
                                   resource_group: str,
                                   config_id: Optional[str] = None,
                                   config_name: Optional[str] = None,
                                   orchestration_scenario: str = "orchestration",
                                   executable_id: str = "orchestration"):
    """
    Discovers the orchestration API URL based on provided configuration details.

    Args:
        base_url: The base URL for the AI Core API.
        auth_url: The URL for the AI Core authentication service.
        client_id: The client ID for the AI Core API.
        client_secret: The client secret for the AI Core API.
        resource_group: The resource group for the AI Core API.
        config_id: Optional configuration ID.
        config_name: Optional configuration name.
        orchestration_scenario: The orchestration scenario ID.
        executable_id: The orchestration executable ID.

    Returns:
        The orchestration API URL or None if no deployment is found.
    """
    proxy_client = GenAIHubProxyClient(
        base_url=base_url,
        auth_url=auth_url,
        client_id=client_id,
        client_secret=client_secret,
        resource_group=resource_group
    )
    deployments = proxy_client.ai_core_client.deployment.query(
        scenario_id=orchestration_scenario,
        executable_ids=[executable_id],
        status=Status.RUNNING
    )
    if deployments.count > 0:
        sorted_deployments = sorted(deployments.resources, key=lambda x: x.start_time)[::-1]
        check_for = {}
        if config_name:
            check_for["configuration_name"] = config_name
        if config_id:
            check_for["configuration_id"] = config_id
        if not check_for:
            return sorted_deployments[0].deployment_url
        for deployment in sorted_deployments:
            if all(getattr(deployment, key) == value for key, value in check_for.items()):
                return deployment.deployment_url
    return None


def get_orchestration_api_url(proxy_client: GenAIHubProxyClient,
                              deployment_id: Optional[str] = None,
                              config_name: Optional[str] = None,
                              config_id: Optional[str] = None) -> str:
    """
    Retrieves the orchestration API URL based on provided deployment or configuration details.

    Args:
        proxy_client: The GenAIHubProxyClient instance.
        deployment_id: Optional deployment ID.
        config_name: Optional configuration name.
        config_id: Optional configuration ID.

    Returns:
        The orchestration API URL.

    Raises:
        ValueError: If no orchestration deployment is found.
    """
    if deployment_id:
        return f"{proxy_client.ai_core_client.base_url.rstrip('/')}/inference/deployments/{deployment_id}"
    url = discover_orchestration_api_url(
        **proxy_client.model_dump(exclude='ai_core_client'),
        config_name=config_name,
        config_id=config_id
    )
    if url is None:
        raise ValueError('No Orchestration deployment found!')
    return url


class OrchestrationService:
    """
    A service for executing orchestration requests, allowing for the generation of LLM-generated content
    through a pipeline of configured modules.

    This service supports both synchronous and asynchronous request execution. For streaming responses,
    special care is taken to not close the underlying HTTP stream prematurely.

    Args:
        api_url: The base URL for the orchestration API.
        config: The default orchestration configuration.
        proxy_client: A GenAIHubProxyClient instance.
        deployment_id: Optional deployment ID.
        config_name: Optional configuration name.
        config_id: Optional configuration ID.
        timeout: Optional timeout for HTTP requests.
        
    """

    def __init__(self,
                 api_url: Optional[str] = None,
                 config: Optional[OrchestrationConfig] = None,
                 proxy_client: Optional[GenAIHubProxyClient] = None,
                 deployment_id: Optional[str] = None,
                 config_name: Optional[str] = None,
                 config_id: Optional[str] = None,
                 timeout: Union[int, float, httpx.Timeout, None] = None):
        self.proxy_client = proxy_client or get_proxy_client(proxy_version="gen-ai-hub")
        if api_url:
            self.api_url = api_url
        else:
            self.api_url = get_orchestration_api_url(self.proxy_client, deployment_id, config_name, config_id)
        self.config = config
        # create reusable httpx client to improve performance
        self.client = httpx.Client(timeout=timeout)
        self.async_client = httpx.AsyncClient(timeout=timeout)

    def _execute_request(
        self,
        config: OrchestrationConfig,
        template_values: List[TemplateValue],
        history: List[Message],
        stream: bool,
        stream_options: Optional[dict] = None,
    ) -> Union[OrchestrationResponse, Iterable[OrchestrationResponseStreaming]]:
        """
        Executes an orchestration request synchronously.

        For streaming requests, this method creates a single HTTP stream. It manually enters the stream's
        context to obtain the response, checks for HTTP errors, and then passes both the open response and
        a custom close function to the SSE client. The SSEClient will then yield streaming events and
        close the HTTP stream upon completion.

        Args:
            config: The orchestration configuration.
            template_values: Template values for the request.
            history: Message history.
            stream: Whether to stream the response.
            stream_options: Additional streaming options.

        Returns:
            An OrchestrationResponse if not streaming, or an iterable of OrchestrationResponseStreaming
            objects if streaming.

        Raises:
            ValueError: If no configuration is provided.
            OrchestrationError: If the HTTP request fails.
        """
        if config is None:
            raise ValueError("A configuration is required to invoke the orchestration service.")
        config_copy = deepcopy(config)
        config_copy._stream = stream
        if stream_options:
            config_copy.stream_options = stream_options
        request_obj = OrchestrationRequest(
            config=config_copy,
            template_values=template_values or [],
            history=history or [],
        )
        if stream:
            # Create the streaming response context manager.
            response_cm = self.client.stream(
                "POST",
                self.api_url + COMPLETION_SUFFIX,
                headers=self.proxy_client.request_header,
                json=request_obj.to_dict()
            )
            return SSEClient(response_cm, prefix="data: ", final_message="[DONE]")

        response = self.client.post(
            self.api_url + COMPLETION_SUFFIX,
            headers=self.proxy_client.request_header,
            json=request_obj.to_dict()
        )
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as error:
            _handle_http_error(error, response)

        data = response.json()
        return dacite.from_dict(
            data_class=OrchestrationResponse,
            data=data,
            config=dacite.Config(cast=[Enum]),
        )

    async def _a_execute_request(
        self,
        config: OrchestrationConfig,
        template_values: List[TemplateValue],
        history: List[Message],
        stream: bool,
        stream_options: Optional[dict] = None,
    ) -> Union[OrchestrationResponse, AsyncSSEClient]:
        """
        Executes an orchestration request asynchronously.

        For streaming requests, this method creates a single HTTP stream and returns an AsyncSSEClient.
        The AsyncSSEClient manages the stream’s lifecycle (opening it via __aenter__ and closing it via __aexit__)
        and performs error checking upon entering the stream.

        Args:
            config: The orchestration configuration.
            template_values: Template values for the request.
            history: Message history.
            stream: Whether to stream the response.
            stream_options: Additional streaming options.

        Returns:
            An OrchestrationResponse if not streaming, or an AsyncSSEClient for iterating over the streaming response.

        Raises:
            ValueError: If no configuration is provided.
            OrchestrationError: If the HTTP request fails.
        """
        if config is None:
            raise ValueError("A configuration is required to invoke the orchestration service.")
        config_copy = deepcopy(config)
        config_copy._stream = stream
        if stream_options:
            config_copy.stream_options = stream_options
        request_obj = OrchestrationRequest(
            config=config_copy,
            template_values=template_values or [],
            history=history or [],
        )
        if stream:
            response_cm = self.async_client.stream(
                "POST",
                self.api_url + COMPLETION_SUFFIX,
                headers=self.proxy_client.request_header,
                json=request_obj.to_dict()
            )
            return AsyncSSEClient(response_cm, prefix="data: ", final_message="[DONE]")

        response = await self.async_client.post(
            self.api_url + COMPLETION_SUFFIX,
            headers=self.proxy_client.request_header,
            json=request_obj.to_dict()
        )
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as error:
            _handle_http_error(error, response)

        data = response.json()
        return dacite.from_dict(
            data_class=OrchestrationResponse,
            data=data,
            config=dacite.Config(cast=[Enum]),
        )

    def run(
        self,
        config: Optional[OrchestrationConfig] = None,
        template_values: Optional[List[TemplateValue]] = None,
        history: Optional[List[Message]] = None,
    ) -> OrchestrationResponse:
        """
        Executes an orchestration request synchronously (non-streaming).

        Args:
            config: Optional orchestration configuration; if not provided, the default configuration is used.
            template_values: Optional list of template values.
            history: Optional message history.

        Returns:
            An OrchestrationResponse object.
        """
        return self._execute_request(
            config=config or self.config,
            template_values=template_values,
            history=history,
            stream=False,
        )

    def stream(
        self,
        config: Optional[OrchestrationConfig] = None,
        template_values: Optional[List[TemplateValue]] = None,
        history: Optional[List[Message]] = None,
        stream_options: Optional[dict] = None,
    ) -> SSEClient:
        """
        Executes an orchestration request in streaming mode (synchronously).

        The returned SSEClient instance yields OrchestrationResponseStreaming objects.

        Args:
            config: Optional orchestration configuration.
            template_values: Optional list of template values.
            history: Optional message history.
            stream_options: Optional dictionary of additional streaming options.

        Returns:
            An iterable of OrchestrationResponseStreaming objects.
        """
        return self._execute_request(
            config=config or self.config,
            template_values=template_values,
            history=history,
            stream=True,
            stream_options=stream_options,
        )

    async def arun(
        self,
        config: Optional[OrchestrationConfig] = None,
        template_values: Optional[List[TemplateValue]] = None,
        history: Optional[List[Message]] = None,
    ) -> OrchestrationResponse:
        """
        Executes an orchestration request asynchronously (non-streaming).

        Args:
            config: Optional orchestration configuration.
            template_values: Optional list of template values.
            history: Optional message history.

        Returns:
            An OrchestrationResponse object.
        """
        return await self._a_execute_request(
            config=config or self.config,
            template_values=template_values,
            history=history,
            stream=False,
        )

    async def astream(
        self,
        config: Optional[OrchestrationConfig] = None,
        template_values: Optional[List[TemplateValue]] = None,
        history: Optional[List[Message]] = None,
        stream_options: Optional[dict] = None,
    ) -> AsyncSSEClient:
        """
        Executes an orchestration request asynchronously in streaming mode.

        The returned AsyncSSEClient instance yields OrchestrationResponseStreaming objects.

        Args:
            config: Optional orchestration configuration.
            template_values: Optional list of template values.
            history: Optional message history.
            stream_options: Optional dictionary of additional streaming options.

        Returns:
            An AsyncSSEClient instance for iterating over the streaming response.
        """
        return await self._a_execute_request(
            config=config or self.config,
            template_values=template_values,
            history=history,
            stream=True,
            stream_options=stream_options,
        )

    def close_http_connection(self):
        """
        Closes the httpx synchronous client.
        """
        self.client.close()

    async def aclose_http_connection(self):
        """
        Closes the httpx asynchronous client.
        """
        await self.async_client.aclose()
