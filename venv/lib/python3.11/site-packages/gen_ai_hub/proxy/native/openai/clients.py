from __future__ import annotations

import contextvars
import re
import httpx

from contextlib import contextmanager
from typing import Optional, Union, List

from openai import AsyncOpenAI as AsyncOpenAI_
from openai import OpenAI as OpenAI_
from openai import resources
from openai._streaming import Stream
from openai.resources.chat import AsyncChat as AsyncChat_
from openai.resources.chat import Chat as Chat_
from openai.resources.chat.completions import AsyncCompletions as AsyncChatCompletions_
from openai.resources.chat.completions import Completions as ChatCompletions_
from openai.resources.completions import AsyncCompletions as AsyncCompletions_
from openai.resources.completions import Completions as Completions_
from openai.resources.embeddings import AsyncEmbeddings as AsyncEmbeddings_
from openai.resources.embeddings import Embeddings as Embeddings_
from openai.types import Completion, Embedding
from openai.types.chat import ChatCompletion, ChatCompletionMessageParam

from gen_ai_hub.proxy.core import get_proxy_client
from gen_ai_hub.proxy.core.base import BaseProxyClient
from gen_ai_hub.proxy.core.utils import NOT_GIVEN, NotGiven, Omit, if_set, kwargs_if_set

DEFAULT_API_VERSION = '2024-12-01-preview'

_current_deployment = contextvars.ContextVar('current_deployment')



@contextmanager
def set_deployment(value):
    token = _current_deployment.set(value)
    try:
        yield
    finally:
        _current_deployment.reset(token)


def get_current_deployment():
    return _current_deployment.get(None)


class Embeddings(Embeddings_):
    """
    A class that represents the Embeddings. It extends the Embeddings_ class
    and provides functionality to create embeddings based on the provided input.
    """

    def create(self,
               *,
               input: Union[str, List[str], List[int], List[List[int]], None],
               model: str | None | NotGiven = NOT_GIVEN,
               deployment_id: str | None | NotGiven = NOT_GIVEN,
               model_name: str | None | NotGiven = NOT_GIVEN,
               config_id: str | None | NotGiven = NOT_GIVEN,
               config_name: str | None | NotGiven = NOT_GIVEN,
               **kwargs) -> Embedding:
        """
        Creates embeddings based on the provided input and model information.

        Args:
            input (Union[str, List[str], List[int], List[List[int]], None]): Input data to create embeddings.
            model (str | None | NotGiven): The model to use for creating embeddings.
            deployment_id (str | None | NotGiven): The ID of the deployment to use.
            model_name (str | None | NotGiven): The name of the model to use.
            config_id (str | None | NotGiven): The ID of the config to use.
            config_name (str | None | NotGiven): The name of the config to use.
            **kwargs: Additional keyword arguments.

        Returns:
            Embedding: The created embeddings.

        Raises:
            ValueError: If the deployment cannot be selected or the model name is not provided.
        """
        proxy_client: BaseProxyClient = self._client.proxy_client
        model_name = if_set(model_name, if_set(model))
        model_identification = kwargs_if_set(
            deployment_id=deployment_id,
            model_name=model_name,
            config_id=config_id,
            config_name=config_name,
        )
        deployment = proxy_client.select_deployment(**model_identification)
        model_name = deployment.model_name or '???'
        with set_deployment(deployment):
            return super().create(input=input, model=model_name, **kwargs)


class AsyncEmbeddings(AsyncEmbeddings_):
    """
    The AsyncEmbeddings class is a subclass of AsyncEmbeddings_. This class is used for creating
    embeddings asynchronously. It provides an interface for fetching embeddings of a given input
    from a selected deployment on a proxy client.
    """

    async def create(self,
                     *,
                     input: Union[str, List[str], List[int], List[List[int]], None],
                     model: str | None | NotGiven = NOT_GIVEN,
                     deployment_id: str | None | NotGiven = NOT_GIVEN,
                     model_name: str | None | NotGiven = NOT_GIVEN,
                     config_id: str | None | NotGiven = NOT_GIVEN,
                     config_name: str | None | NotGiven = NOT_GIVEN,
                     **kwargs) -> Embedding:
        """
        Asynchronously create embeddings for the given input using a specific model.

        Args:
            input (Union[str, List[str], List[int], List[List[int]], None]): The input for which embeddings are to be
                                                                            created.
            model (str | None | NotGiven, optional): The model to use for creating embeddings. Defaults to NOT_GIVEN.
            deployment_id (str | None | NotGiven, optional): The deployment ID to use for creating embeddings.
                                                            Defaults to NOT_GIVEN.
            model_name (str | None | NotGiven, optional): The name of the model to use for creating embeddings.
                                                        Defaults to NOT_GIVEN.
            config_id (str | None | NotGiven, optional): The configuration ID to use for creating embeddings.
                                                        Defaults to NOT_GIVEN.
            config_name (str | None | NotGiven, optional): The name of the configuration to use for creating embeddings.
                                                        Defaults to NOT_GIVEN.
            **kwargs: Additional parameters to pass to the method.

        Returns:
            Embedding: The embeddings created for the given input.
        """
        proxy_client: BaseProxyClient = self._client.proxy_client
        model_name = if_set(model_name, if_set(model))
        model_identification = kwargs_if_set(
            deployment_id=deployment_id,
            model_name=model_name,
            config_id=config_id,
            config_name=config_name,
        )
        deployment = proxy_client.select_deployment(**model_identification)
        model_name = deployment.model_name or '???'
        with set_deployment(deployment):
            return await super().create(input=input, model=model_name, **kwargs)


class Completions(Completions_):
    """
    The Completions class is a subclass of Completions_. It provides a way to create a completion given a prompt and
     certain other configurations. It extends from the base class Completions_ and overrides the create method to cater
     to the specific requirements.
    """

    def create(self,
               *,
               prompt: Union[str, List[str], List[int], List[List[int]], None],
               model: str | None | NotGiven = NOT_GIVEN,
               deployment_id: str | None | NotGiven = NOT_GIVEN,
               model_name: str | None | NotGiven = NOT_GIVEN,
               config_id: str | None | NotGiven = NOT_GIVEN,
               config_name: str | None | NotGiven = NOT_GIVEN,
               **kwargs) -> Completion | Stream[Completion]:
        """
        This method creates a completion based on the provided parameters. It uses a proxy client to select a
        deployment and then calls the create method of the parent class to generate a completion.

        Args:
        prompt (Union[str, List[str], List[int], List[List[int]], None]): The prompt to use for generating the
                                                                        completion.
        model (str | None | NotGiven, optional): The model to use for generating the completion. Defaults to NOT_GIVEN.
        deployment_id (str | None | NotGiven, optional): The ID of the deployment to use. Defaults to NOT_GIVEN.
        model_name (str | None | NotGiven, optional): The name of the model to use. Defaults to NOT_GIVEN.
        config_id (str | None | NotGiven, optional): The ID of the configuration to use. Defaults to NOT_GIVEN.
        config_name (str | None | NotGiven, optional): The name of the configuration to use. Defaults to NOT_GIVEN.
        **kwargs: Additional keyword arguments that can be used to customize the completion.

        Returns:
        Completion | Stream[Completion]: The generated completion or a stream of completions.
        """
        proxy_client: BaseProxyClient = self._client.proxy_client
        model_name = if_set(model_name, if_set(model))
        model_identification = kwargs_if_set(
            deployment_id=deployment_id,
            model_name=model_name,
            config_id=config_id,
            config_name=config_name,
        )
        deployment = proxy_client.select_deployment(**model_identification)
        model_name = deployment.model_name or '???'
        with set_deployment(deployment):
            return super().create(prompt=prompt, model=model_name, **kwargs)


class AsyncCompletions(AsyncCompletions_):
    """
    AsyncCompletions is a subclass of AsyncCompletions_. It provides a way to create a completion given a prompt and
     certain other configurations in asynchronous way. It extends from the base class Completions_ and overrides the
     create method to cater to the specific requirements.
    """

    async def create(self,
                     *,
                     prompt: Union[str, List[str], List[int], List[List[int]], None],
                     model: str | None | NotGiven = NOT_GIVEN,
                     deployment_id: str | None | NotGiven = NOT_GIVEN,
                     model_name: str | None | NotGiven = NOT_GIVEN,
                     config_id: str | None | NotGiven = NOT_GIVEN,
                     config_name: str | None | NotGiven = NOT_GIVEN,
                     **kwargs) -> Completion | Stream[Completion]:
        """
        Asynchronously creates a completion or a stream of completions based on the given prompt and other parameters.

        Args:
            prompt (Union[str, List[str], List[int], List[List[int]], None]): The input prompt(s) for the completion.
            model (str | None | NotGiven, optional): The model to be used for the completion. Default is NOT_GIVEN.
            deployment_id (str | None | NotGiven, optional): The ID of the deployment. Default is NOT_GIVEN.
            model_name (str | None | NotGiven, optional): The name of the model. Default is NOT_GIVEN.
            config_id (str | None | NotGiven, optional): The configuration ID. Default is NOT_GIVEN.
            config_name (str | None | NotGiven, optional): The configuration name. Default is NOT_GIVEN.
            **kwargs: Any additional keyword arguments will be passed to the completion creation.

        Returns:
            Completion | Stream[Completion]: The completion or stream of completions created based on the provided
                                            prompt.
        """
        proxy_client: BaseProxyClient = self._client.proxy_client
        model_name = if_set(model_name, if_set(model))
        model_identification = kwargs_if_set(
            deployment_id=deployment_id,
            model_name=model_name,
            config_id=config_id,
            config_name=config_name,
        )
        deployment = proxy_client.select_deployment(**model_identification)
        model_name = deployment.model_name or '???'
        with set_deployment(deployment):
            return await super().create(prompt=prompt, model=model_name, **kwargs)


class Chat(Chat_):

    def __init__(self, client: OpenAI) -> None:
        super().__init__(client)
        self.completions = ChatCompletions(client)


class ChatCompletions(ChatCompletions_):
    """
        A class that handles chat completions, extending from the class 'ChatCompletions_'.
    """

    def create(self,
               *,
               messages: List[ChatCompletionMessageParam],
               model: str | None | NotGiven = NOT_GIVEN,
               deployment_id: str | None | NotGiven = NOT_GIVEN,
               model_name: str | None | NotGiven = NOT_GIVEN,
               config_id: str | None | NotGiven = NOT_GIVEN,
               config_name: str | None | NotGiven = NOT_GIVEN,
               **kwargs) -> ChatCompletion:
        """
        Creates a chat completion using the provided parameters.

        Args:
            messages (List[ChatCompletionMessageParam]): A list of chat completion message parameters.
            model (str | None | NotGiven, optional): The model to use for chat completion. Defaults to NOT_GIVEN.
            deployment_id (str | None | NotGiven, optional): The deployment ID to use for chat completion.
                                                            Defaults to NOT_GIVEN.
            model_name (str | None | NotGiven, optional): The model name to use for chat completion.
                                                        Defaults to NOT_GIVEN.
            config_id (str | None | NotGiven, optional): The configuration ID to use for chat completion.
                                                        Defaults to NOT_GIVEN.
            config_name (str | None | NotGiven, optional): The configuration name to use for chat completion.
                                                        Defaults to NOT_GIVEN.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            ChatCompletion: A chat completion created with the provided parameters.
        """
        proxy_client: BaseProxyClient = self._client.proxy_client
        model_name = if_set(model_name, if_set(model))
        model_identification = kwargs_if_set(
            deployment_id=deployment_id,
            model_name=model_name,
            config_id=config_id,
            config_name=config_name,
        )
        deployment = proxy_client.select_deployment(**model_identification)
        model_name = deployment.model_name or '???'

        # Workaround for older gpt models not supporting max_completion_tokens
        if not self.supports_max_completion_tokens(model_name) and 'max_completion_tokens' in kwargs:
            kwargs['max_tokens'] = kwargs.pop('max_completion_tokens')
        # Reasoning models do not support temperature
        if not self.supports_temperature(model_name) and 'temperature' in kwargs:
            kwargs.pop('temperature')

        with set_deployment(deployment):
            return super().create(messages=messages, model=model_name, **kwargs)

    @staticmethod
    def supports_max_completion_tokens(model_name: str) -> bool:
        """
        Checks if the given model supports the `max_completion_tokens parameter.
        gpt4o and o1[-mini], o3[-mini] models and higher support the `max_completion_tokens` parameter.
        Args:
            model_name (str): The name of the model to check.
        Returns:
            bool: True if the model supports `max_completion_tokens`, False otherwise.
        """
        return bool(re.search(r"gpt-4o|^o\d+", model_name))

    @staticmethod
    def supports_temperature(model_name: str) -> bool:
        """
        Checks if the given model supports the `temperature` parameter.
        Reasoning models do not support temperature e.g., o1[-mini], o3[-mini]
        Args:
            model_name (str): The name of the model to check.
        Returns:
            bool: True if the model supports `temperature`, False otherwise
        """
        return not re.search(r"^o\d+", model_name)

class AsyncChat(AsyncChat_):

    def __init__(self, client: OpenAI) -> None:
        super().__init__(client)
        self.completions = AsyncChatCompletions(client)


class AsyncChatCompletions(AsyncChatCompletions_):
    """
    The AsyncChatCompletions class is a derived class which extends AsyncChatCompletions_.
    This class is used to handle asynchronous chat completion requests. It provides methods
    to create and manage chat completions in an asynchronous manner.
    """

    async def create(self,
                     *,
                     messages: List[ChatCompletionMessageParam],
                     model: str | None | NotGiven = NOT_GIVEN,
                     deployment_id: str | None | NotGiven = NOT_GIVEN,
                     model_name: str | None | NotGiven = NOT_GIVEN,
                     config_id: str | None | NotGiven = NOT_GIVEN,
                     config_name: str | None | NotGiven = NOT_GIVEN,
                     **kwargs) -> ChatCompletion:
        """
        Asynchronously creates a new chat completion.

        Args:
            messages (List[ChatCompletionMessageParam]): List of chat completion message parameters.
            model (str | None | NotGiven, optional): The model to be used. Defaults to NOT_GIVEN.
            deployment_id (str | None | NotGiven, optional): The deployment id. Defaults to NOT_GIVEN.
            model_name (str | None | NotGiven, optional): The model name. Defaults to NOT_GIVEN.
            config_id (str | None | NotGiven, optional): The configuration id. Defaults to NOT_GIVEN.
            config_name (str | None | NotGiven, optional): The configuration name. Defaults to NOT_GIVEN.

        Returns:
            ChatCompletion: The created chat completion.
        """
        proxy_client: BaseProxyClient = self._client.proxy_client
        model_name = if_set(model_name, if_set(model))
        model_identification = kwargs_if_set(
            deployment_id=deployment_id,
            model_name=model_name,
            config_id=config_id,
            config_name=config_name,
        )
        deployment = proxy_client.select_deployment(**model_identification)
        model_name = deployment.model_name or '???'
        with set_deployment(deployment):
            return await super().create(messages=messages, model=model_name, **kwargs)


class OpenAIWithRawResponse:
    """
    This class is a wrapper for the OpenAI API client that provides raw responses.
    Note: The properties 'edits', 'files', 'images', 'audio', 'moderations', 'models',
    'fine_tuning', 'fine_tunes' and 'beta' are placeholders and currently do not provide any
    functionality.

    Attributes:
        completions: An instance of CompletionsWithRawResponse class.
        chat: An instance of ChatWithRawResponse class.
        edits: Not currently used.
        embeddings: An instance of EmbeddingsWithRawResponse class if client.embeddings is not None.
        files: Not currently used.
        images: Not currently used.
        audio: Not currently used.
        moderations: Not currently used.
        models: Not currently used.
        fine_tuning: Not currently used.
        fine_tunes: Not currently used.
        beta: Not currently used.

    The class is designed to provide the raw responses from OpenAI's API endpoints. It currently supports completions,
    chat, and embeddings endpoints.
    """

    def __init__(self, client: OpenAI) -> None:
        self.completions = resources.CompletionsWithRawResponse(client.completions)
        self.chat = resources.ChatWithRawResponse(client.chat)
        self.edits = None
        self.embeddings = resources.EmbeddingsWithRawResponse(client.embeddings) if client.embeddings else None
        self.files = None
        self.images = None
        self.audio = None
        self.moderations = None
        self.models = None
        self.fine_tuning = None
        self.fine_tunes = None
        self.beta = None


class AsyncOpenAIWithRawResponse:
    """
    A class that provides an asynchronous interface to the OpenAI API, returning raw responses.

    This class wraps the core functionality of OpenAI's API, offering access to completions,
    chat capabilities, and embeddings. It is designed to work with OpenAI's asynchronous client,
    allowing for concurrent requests to the API.

    Note: The properties 'edits', 'files', 'images', 'audio', 'moderations', 'models',
    'fine_tuning', 'fine_tunes' and 'beta' are placeholders and currently do not provide any
    functionality.

    Attributes:
        completions: An instance of `resources.AsyncCompletionsWithRawResponse` for managing
                completions with the API.
        chat: An instance of `resources.AsyncChatWithRawResponse` for managing chat with the API.
        embeddings: An instance of `resources.AsyncEmbeddingsWithRawResponse` for managing
                embeddings with the API.
        edits: Currently a placeholder with no functionality.
        files: Currently a placeholder with no functionality.
        images: Currently a placeholder with no functionality.
        audio: Currently a placeholder with no functionality.
        moderations: Currently a placeholder with no functionality.
        models: Currently a placeholder with no functionality.
        fine_tuning: Currently a placeholder with no functionality.
        fine_tunes: Currently a placeholder with no functionality.
        beta: Currently a placeholder with no functionality.

    Args:
        client: An instance of `AsyncOpenAI`, the asynchronous OpenAI client.

    Returns:
        An instance of `AsyncOpenAIWithRawResponse`.
    """

    def __init__(self, client: AsyncOpenAI) -> None:
        self.completions = resources.AsyncCompletionsWithRawResponse(client.completions)
        self.chat = resources.AsyncChatWithRawResponse(client.chat)
        self.edits = None
        self.embeddings = resources.AsyncEmbeddingsWithRawResponse(client.embeddings)
        self.files = None
        self.images = None
        self.audio = None
        self.moderations = None
        self.models = None
        self.fine_tuning = None
        self.fine_tunes = None
        self.beta = None


def _prepare_url(url: str) -> httpx.URL:
    deployment = get_current_deployment()
    prediction_url = deployment.prediction_url
    if prediction_url:
        return httpx.URL(prediction_url)
    url = httpx.URL(url)
    if url.is_relative_url:
        deployment_url = httpx.URL(get_current_deployment().url.rstrip('/') + '/')
        url = deployment_url.raw_path + url.raw_path.lstrip(b"/")
        return deployment_url.copy_with(raw_path=url)
    return url


class OpenAI(OpenAI_):
    """
    This is a class for the OpenAI API client. It is designed to handle various services provided by OpenAI such as text
     completions, chat, embeddings etc.

    Attributes:
        proxy_client (BaseProxyClient, optional): An instance of a Proxy Client. Defaults to None.
        api_version (str, optional): API version used for OpenAI API calls. Defaults to DEFAULT_API_VERSION.
        completions (Completions): An instance of the Completions class for text generation.
        chat (Chat): An instance of the Chat class for conversation.
        edits: Placeholder for future use. Currently set to None.
        embeddings (Embeddings): An instance of the Embeddings class for getting text embeddings.
        files: Placeholder for future use. Currently set to None.
        images: Placeholder for future use. Currently set to None.
        audio: Placeholder for future use. Currently set to None.
        moderations: Placeholder for future use. Currently set to None.
        models: Placeholder for future use. Currently set to None.
        fine_tuning: Placeholder for future use. Currently set to None.
        fine_tunes: Placeholder for future use. Currently set to None.
        beta: Placeholder for future use. Currently set to None.
        with_raw_response (OpenAIWithRawResponse): An instance of the OpenAIWithRawResponse class for returning raw
                                                responses from the API.
    """

    def __init__(self,
                 *,
                 proxy_client: Optional[BaseProxyClient] = None,
                 api_version: Optional[str] = DEFAULT_API_VERSION,
                 **kwargs) -> None:
        self.proxy_client = proxy_client or get_proxy_client()
        for kwarg in ('api_key', 'organization', 'base_url'):
            kwargs.pop(kwarg, None)
        default_query = {'api-version': api_version or DEFAULT_API_VERSION, **kwargs.pop('default_query', {})}
        super().__init__(api_key='???', base_url='???', organization='???', default_query=default_query, **kwargs)

        self.completions = Completions(self)
        self.chat = Chat(self)
        self.edits = None
        self.embeddings = Embeddings(self)
        self.files = None
        self.images = None
        self.audio = None
        self.moderations = None
        self.models = None
        self.fine_tuning = None
        self.fine_tunes = None
        self.beta = None
        self.with_raw_response = OpenAIWithRawResponse(self)

    @property
    def default_headers(self) -> dict[str, str | Omit]:
        headers = super().default_headers
        headers.update(self.proxy_client.request_header)
        return headers

    def _prepare_url(self, url: str) -> httpx.URL:
        return _prepare_url(url)

    def request(self, cast_to, options, *args, **kwargs):
        options.json_data.update(get_current_deployment().additional_request_body_kwargs())
        return super().request(cast_to, options, *args, **kwargs)


class AsyncOpenAI(AsyncOpenAI_):
    """
    An async version of the OpenAI API client.

    This class is used to interact with the OpenAI API asynchronously. It supports various operations like creating
    completions, generating chat messages, and getting embeddings.

    Attributes:
        proxy_client (BaseProxyClient): A proxy client to make API requests. If not provided, a default one will be
                                    created.
        api_version (str, optional): The version of the OpenAI API to use. Default is defined by DEFAULT_API_VERSION.
        completions (AsyncCompletions): A client for interacting with the OpenAI API's completions.
        chat (AsyncChat): A client for interacting with the OpenAI API's chat.
        edits (None): Placeholder for future support of "edits" operations.
        embeddings (AsyncEmbeddings): A client for interacting with the OpenAI API's embeddings.
        files (None): Placeholder for future support of "files" operations.
        images (None): Placeholder for future support of "images" operations.
        audio (None): Placeholder for future support of "audio" operations.
        moderations (None): Placeholder for future support of "moderations" operations.
        models (None): Placeholder for future support of "models" operations.
        fine_tuning (None): Placeholder for future support of "fine_tuning" operations.
        fine_tunes (None): Placeholder for future support of "fine_tunes" operations.
        beta (None): Placeholder for future support of "beta" operations.
        with_raw_response (AsyncOpenAIWithRawResponse): A client that returns raw API responses.

    Args:
        proxy_client (BaseProxyClient, optional): A proxy client to make API requests. If not provided,
                                                a default one will be created.
        api_version (str, optional): The version of the OpenAI API to use. Default is defined by DEFAULT_API_VERSION.
        **kwargs: Additional keyword arguments.
    """

    def __init__(self,
                 *,
                 proxy_client: Optional[BaseProxyClient] = None,
                 api_version: Optional[str] = DEFAULT_API_VERSION,
                 **kwargs) -> None:
        self.proxy_client = proxy_client or get_proxy_client()
        for kwarg in ('api_key', 'organization', 'base_url'):
            kwargs.pop(kwarg, None)
        default_query = {'api-version': api_version or DEFAULT_API_VERSION, **kwargs.pop('default_query', {})}
        super().__init__(api_key='???', base_url='???', organization='???', default_query=default_query, **kwargs)

        self.completions = AsyncCompletions(self)
        self.chat = AsyncChat(self)
        self.edits = None
        self.embeddings = AsyncEmbeddings(self)
        self.files = None
        self.images = None
        self.audio = None
        self.moderations = None
        self.models = None
        self.fine_tuning = None
        self.fine_tunes = None
        self.beta = None
        self.with_raw_response = AsyncOpenAIWithRawResponse(self)

    @property
    def default_headers(self) -> dict[str, str | Omit]:
        headers = super().default_headers
        headers.update(self.proxy_client.request_header)
        return headers

    def _prepare_url(self, url: str) -> httpx.URL:
        return _prepare_url(url)

    def request(self, cast_to, options, *args, **kwargs):
        options.json_data.update(get_current_deployment().additional_request_body_kwargs())
        return super().request(cast_to, options, *args, **kwargs)
