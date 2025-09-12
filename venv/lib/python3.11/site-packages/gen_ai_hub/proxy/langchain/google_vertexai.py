from typing import Optional

from pydantic import model_validator, ConfigDict

from gen_ai_hub.proxy.core.base import BaseProxyClient
from gen_ai_hub.proxy.core.utils import if_str_set
from gen_ai_hub.proxy.gen_ai_hub_proxy.client import Deployment
from gen_ai_hub.proxy.langchain.init_models import catalog
from gen_ai_hub.proxy.native.google_vertexai.clients import GenerativeModel
from langchain_google_vertexai import ChatVertexAI as ChatVertexAI_


class ChatVertexAI(ChatVertexAI_):
    """Drop-in replacement for langchain_google_vertexai.ChatVertexAI."""

    model_config = ConfigDict(extra='allow')

    def __init__(
            self,
            *args,
            model: str = "",  # model parameter from google library.
            proxy_model_name: str = "",  # model parameter for old versions.
            model_id: str = "",
            deployment_id: str = "",
            model_name: str = "",
            config_id: str = "",
            config_name: str = "",
            proxy_client: Optional[BaseProxyClient] = None,
            **kwargs,
    ):
        # Correct model_id fitting to deployment is selected in validate_environment.
        if model_id != "":
            raise ValueError(
                "Parameter not supported. Please use a variation of deployment_id, model_name, config_id and "
                "config_name to identify a deployment."
            )

        # Gets model_name from either of the supported parameters in the correct order.
        model_name = if_str_set(
            model_name, if_str_set(model, if_str_set(proxy_model_name))
        )

        # Creates instance for later reference when accessing the prediction client.
        kwargs["genaihub_client"] = GenerativeModel(
            model_name=model_name,
            deployment_id=deployment_id,
            config_id=config_id,
            config_name=config_name,
            proxy_client=proxy_client,
        )
        kwargs["endpoint_version"] = "v1"

        # Models prefix required by langchain implementation. If not set rpc to rest transformation fails due to mapping issue.
        super().__init__(*args, model=model_name, **kwargs)

    # pylint: disable=no-self-argument
    @model_validator(mode="before")
    def validate_params_base(cls, values: dict) -> dict:
        """Overrides langchain_google_vertexai._base._VertexAIBase -> validate_params_base.
        Original method handles some endpoint specific initialization details not required
        for Generative AI Hub endpoints.
        """
        return values

    @property
    def prediction_client(self):
        """Overrides langchain_google_vertexai._base._VertexAIBase -> prediction_client.
        Returns PredictionServiceClient of Generative AI Hub VertexAI integration.
        """
        if self.client is None:
            # self.genaihub_client is set during __init__() in kwargs.
            self.client = self.genaihub_client._prediction_client  # pyright: ignore
        return self.client

    @property
    def async_prediction_client(self):
        """Overrides langchain_google_vertexai._base._VertexAIBase -> async_prediction_client.
        Returns PredictionServiceAsyncClient of Generative AI Hub VertexAI integration.
        """
        if self.async_client is None:
            # self.genaihub_client is set during __init__() in kwargs.
            self.async_client = self.genaihub_client._prediction_async_client  # pylint: disable=protected-access
        return self.async_client


@catalog.register(
    "gen-ai-hub",
    ChatVertexAI,
    "gemini-1.5-flash",
    "gemini-1.5-pro",
    "gemini-2.0-flash",
    "gemini-2.0-flash-lite",
)
def init_chat_model(
        proxy_client: BaseProxyClient,
        deployment: Deployment,
        temperature: float = 0.0,
        max_tokens: int = 256,
        top_k: Optional[int] = None,
        top_p: float = 1.0,
):
    return ChatVertexAI(
        model_name=deployment.model_name,
        deployment_id=deployment.deployment_id,
        proxy_client=proxy_client,
        temperature=temperature,
        max_output_tokens=max_tokens,
        top_k=top_k,
        top_p=top_p,
    )
