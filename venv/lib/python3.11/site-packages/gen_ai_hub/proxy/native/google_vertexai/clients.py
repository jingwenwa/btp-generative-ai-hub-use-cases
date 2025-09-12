import contextvars
from contextlib import contextmanager
from typing import Any, cast, Dict, Optional, Sequence, Tuple, Union
from urllib.parse import urlparse

import google.api_core.path_template
import google.cloud.aiplatform.utils
import vertexai

from google.api_core.path_template import transcode as transcode_
from google.api_core.rest_streaming import ResponseIterator
from google.api_core.rest_streaming_async import AsyncResponseIterator
from google.auth.aio.credentials import Credentials as AioCredentials
from google.cloud.aiplatform import initializer as aiplatform_initializer
from google.cloud.aiplatform_v1 import GenerateContentRequest
from google.cloud.aiplatform_v1.services.prediction_service import (
    PredictionServiceClient, PredictionServiceAsyncClient,
)
from google.cloud.aiplatform_v1.services.prediction_service.transports import (
    PredictionServiceRestInterceptor, AsyncPredictionServiceRestInterceptor, AsyncPredictionServiceRestTransport
)
from google.cloud.aiplatform_v1.services.prediction_service.transports.rest import (
    PredictionServiceRestTransport,
)
from google.cloud.aiplatform_v1.types.prediction_service import (
    GenerateContentResponse,
)
from google.oauth2.credentials import Credentials

from vertexai.generative_models import (
    GenerativeModel as GenerativeModel_,
)

from gen_ai_hub.proxy.core.base import BaseProxyClient
from gen_ai_hub.proxy.core.proxy_clients import get_proxy_client
from gen_ai_hub.proxy.core.utils import if_str_set, kwargs_if_set
from gen_ai_hub.proxy.gen_ai_hub_proxy.client import Deployment
from gen_ai_hub.proxy.native.google_vertexai.streaming import (
    ServerSentEventsResponseIterator, AsyncServerSentEventsResponseIterator,
)

# required for testing framework in llm-commons
_current_deployment = contextvars.ContextVar("current_deployment")


@contextmanager
def set_deployment(value):
    token = _current_deployment.set(value)
    try:
        yield
    finally:
        _current_deployment.reset(token)


def get_current_deployment() -> Deployment:
    return _current_deployment.get(None)


def transcode(http_options, message=None, **request_kwargs):
    """transcode adjusts the original transcode method to remove google
    API specific URL portions in a minimal intrusive way. The function
    is monkey patched into the import hierarchy in the constructor of
    the gemini and vertexai integration.
    """
    transcoded = transcode_(http_options, message=message, **request_kwargs)
    google_path = transcoded["uri"]
    aicore_path = "/models" + google_path.split("/models", 1)[1]
    transcoded["uri"] = aicore_path
    return transcoded


def extend_metadata_headers(
        proxy_client_header: Dict[str, Any],
        credentials_reference: Credentials,
        metadata: Sequence[Tuple[str, str]],
):
    """Extends metadata with headers from the AI Core proxy client and moves the auth
    token from the header to the credentials object."""

    proxy_header = proxy_client_header

    # Move auth token from header to credentials object.
    token = proxy_header["Authorization"].removeprefix("Bearer ")
    credentials_reference.token = token
    del proxy_header["Authorization"]

    metadata_extension = []
    for header_key, header_value in proxy_header.items():
        metadata_extension.append((header_key, header_value))
    metadata_list = list(metadata)
    metadata_list.extend(metadata_extension)
    metadata = cast(Sequence[Tuple[str, str]], metadata_list)
    return metadata, credentials_reference


class GenAIHubPredictionServiceRestInterceptor(PredictionServiceRestInterceptor):
    """Interceptor class compatible with
    google.cloud.aiplatform_v1.services.prediction_service.transports.PredictionServiceRestInterceptor
    and
    google.ai.generativelanguage_v1.services.generative_service.transports.rest.GenerativeServiceRestInterceptor.
    """

    def __init__(
            self,
            *args,
            aicore_proxy_client: BaseProxyClient,
            credentials_reference: Credentials,
            **kwargs,
    ) -> None:
        # AI Core specific header and credentials source.
        self.aicore_proxy_client = aicore_proxy_client
        # Reference to credentials object contained in GenerativeModel._client._transport._session.credentials.
        self.credentials_reference = credentials_reference
        super().__init__(*args, **kwargs)

    def pre_generate_content(
            self,
            request: GenerateContentRequest,
            metadata: Sequence[Tuple[str, str]],
    ) -> Tuple[GenerateContentRequest, Sequence[Tuple[str, str]]]:
        metadata, credentials_ref = extend_metadata_headers(
            proxy_client_header=self.aicore_proxy_client.request_header,
            credentials_reference=self.credentials_reference,
            metadata=metadata,
        )
        self.credentials_reference = credentials_ref
        return request, metadata

    def pre_stream_generate_content(
            self,
            request,
            metadata,
    ):
        metadata, credentials_ref = extend_metadata_headers(
            proxy_client_header=self.aicore_proxy_client.request_header,
            credentials_reference=self.credentials_reference,
            metadata=metadata,
        )
        self.credentials_reference = credentials_ref
        return request, metadata

    def post_stream_generate_content(
            self, response: ResponseIterator
    ) -> ResponseIterator:
        # The AI Core proxy returns the streamed response as server-sent events (SSE) in the format: data: {payload}.
        # This is configured using the alt=sse flag on the Google endpoint.
        # As the Vertex AI SDK does not inherently support this streaming type, we need to replace the default response
        # iterator with a custom one.
        return ServerSentEventsResponseIterator(
            response._response,
            GenerateContentResponse,
        )


class GenAIHubAsyncPredictionServiceRestInterceptor(AsyncPredictionServiceRestInterceptor):
    """Interceptor class compatible with
       google.cloud.aiplatform_v1.services.prediction_service.transports.AsyncPredictionServiceRestInterceptor"""

    def __init__(
            self,
            *args,
            aicore_proxy_client: BaseProxyClient,
            credentials_reference: AioCredentials,
            **kwargs,
    ) -> None:
        # AI Core specific header and credentials source.
        self.aicore_proxy_client = aicore_proxy_client
        # Reference to credentials object contained in GenerativeModel._client._transport._session.credentials.
        self.credentials_reference = credentials_reference
        super().__init__(*args, **kwargs)

    async def pre_generate_content(
            self,
            request: GenerateContentRequest,
            metadata: Sequence[Tuple[str, str]],
    ) -> Tuple[GenerateContentRequest, Sequence[Tuple[str, str]]]:
        metadata, credentials_ref = extend_metadata_headers(
            proxy_client_header=self.aicore_proxy_client.request_header,
            credentials_reference=self.credentials_reference,
            metadata=metadata,
        )
        self.credentials_reference = credentials_ref
        return request, metadata

    async def pre_stream_generate_content(
            self,
            request: GenerateContentRequest,
            metadata: Sequence[Tuple[str, Union[str, bytes]]],
    ) -> Tuple[GenerateContentRequest, Sequence[Tuple[str, Union[str, bytes]]]]:
        metadata, credentials_ref = extend_metadata_headers(
            proxy_client_header=self.aicore_proxy_client.request_header,
            credentials_reference=self.credentials_reference,
            metadata=metadata,
        )
        self.credentials_reference = credentials_ref
        return request, metadata

    async def post_stream_generate_content(
            self, response: AsyncResponseIterator
    ) -> AsyncResponseIterator:
        # The AI Core proxy returns the streamed response as server-sent events (SSE) in the format: data: {payload}.
        # This is configured using the alt=sse flag on the Google endpoint.
        # As the Vertex AI SDK does not inherently support this streaming type, we need to replace the default response
        # iterator with a custom one.
        return AsyncServerSentEventsResponseIterator(
            response._response,  # pylint: disable=protected-access
            GenerateContentResponse,
        )


class GenerativeModel(GenerativeModel_):
    """Drop-in replacement for vertexai.generative_models.GenerativeModel."""

    def __init__(
            self,
            model: str = "",
            deployment_id: str = "",
            model_name: str = "",
            config_id: str = "",
            config_name: str = "",
            proxy_client: Optional[BaseProxyClient] = None,
            *args,
            **kwargs,
    ):
        # Replaces original transcode import with adjusted transcode function.
        google.api_core.path_template.transcode = transcode
        # Disables region validation since not applicable for AI Core.
        google.cloud.aiplatform.utils.validate_region = lambda _: True

        aicore_proxy_client = proxy_client or get_proxy_client()

        # Gets model_name from either of the supported parameters in the correct order.
        model_name = if_str_set(model_name, if_str_set(model))

        model_identification = kwargs_if_set(
            deployment_id=deployment_id,
            model_name=model_name,
            config_id=config_id,
            config_name=config_name,
        )
        aicore_deployment = aicore_proxy_client.select_deployment(
            **model_identification
        )

        with set_deployment(aicore_deployment):
            self.aicore_proxy_client = aicore_proxy_client
            # Sets vertexai parameters globally. Will end up in overwritten _prediction_client method.
            vertexai.init(
                project="not-applicable",
                location="not-applicable",
                credentials=Credentials(
                    token="Placeholder: This token will be replaced by interceptor on every call.",
                ),
                api_transport="rest",
                api_endpoint=get_current_deployment().url,
            )
        super().__init__(*args, model_name=model_name, **kwargs)

    @property
    def _prediction_client(self) -> PredictionServiceClient:
        """Overrides vertexai.generative_models.GenerativeModel._prediction_client."""
        outer_self = self

        class GenAIHubPredictionServiceClient(PredictionServiceClient):
            """Instantiates a PredictionServiceClient object with an adjusted transport
            instance pointing to the AI Core host and having the interceptor class
            added."""

            def __init__(
                    self,
                    *args,
                    transport: PredictionServiceRestTransport,
                    credentials: Credentials,
                    **kwargs,
            ) -> None:
                api_endpoint = urlparse(kwargs["client_options"].api_endpoint)
                host = "{uri.netloc}{uri.path}".format(uri=api_endpoint)
                transport = PredictionServiceRestTransport(
                    host=host,
                    interceptor=GenAIHubPredictionServiceRestInterceptor(
                        aicore_proxy_client=outer_self.aicore_proxy_client,
                        credentials_reference=credentials,
                    ),
                    credentials=credentials,
                )

                super().__init__(*args, transport=transport, **kwargs)

        # Must keep aligning with vertexai.generative_models.GenerativeModel._prediction_client.
        if not getattr(self, "_prediction_client_value", None):
            self._prediction_client_value = (
                aiplatform_initializer.global_config.create_client(
                    client_class=GenAIHubPredictionServiceClient,  # type: ignore
                    location_override=self._location,
                    prediction_client=True,
                )
            )
        return self._prediction_client_value

    @property
    def _prediction_async_client(self) -> PredictionServiceAsyncClient:
        """Overrides vertexai.generative_models.GenerativeModel._prediction_async_client."""
        outer_self = self

        class GenAIHubPredictionServiceAsyncClient(PredictionServiceAsyncClient):
            """Instantiates a PredictionServiceAsyncClient object with an adjusted transport
            instance pointing to the AI Core host and having the interceptor class
            added."""

            def __init__(
                    self,
                    *args,
                    transport: Optional[AsyncPredictionServiceRestTransport] = None,
                    credentials: Optional[AioCredentials] = None,
                    **kwargs,
            ) -> None:
                api_endpoint = urlparse(kwargs["client_options"].api_endpoint)
                host = "{uri.netloc}{uri.path}".format(uri=api_endpoint)
                async_credentials = AioCredentials()
                if not transport:
                    transport = AsyncPredictionServiceRestTransport(
                        host=host,
                        interceptor=GenAIHubAsyncPredictionServiceRestInterceptor(
                            aicore_proxy_client=outer_self.aicore_proxy_client,
                            credentials_reference=async_credentials,
                        ),
                        credentials=async_credentials,
                    )
                self._prediction_async_client_value = None  # Initialize the attribute
                super().__init__(*args, transport=transport, **kwargs)

        # Must keep aligning with vertexai.generative_models.GenerativeModel._prediction_async_client.
        # pylint: disable=attribute-defined-outside-init
        if not getattr(self, "_prediction_async_client_value", None):
            self._prediction_async_client_value = (
                aiplatform_initializer.global_config.create_client(
                    client_class=GenAIHubPredictionServiceAsyncClient,  # type: ignore
                    location_override=self._location,
                    prediction_client=True,
                )
            )
        return self._prediction_async_client_value
