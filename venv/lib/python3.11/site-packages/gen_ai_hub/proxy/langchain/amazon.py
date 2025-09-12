import logging
from typing import Dict, Optional, List, Any

from langchain_aws import ChatBedrock as ChatBedrock_, ChatBedrockConverse as ChatBedrockConverse_
from langchain_community.embeddings import BedrockEmbeddings as BedrockEmbeddings_
from pydantic import BaseModel, ConfigDict, model_validator

from gen_ai_hub.proxy.core.base import BaseProxyClient
from gen_ai_hub.proxy.gen_ai_hub_proxy.client import Deployment
from gen_ai_hub.proxy.langchain.init_models import catalog
from gen_ai_hub.proxy.native.amazon.clients import Session, AsyncSession

MODEL_NAME_TO_MODEL_ID_MAP = {
    "amazon--titan-embed-text": "amazon.titan-embed-text-v1",
    "amazon--titan-text-express": "amazon.titan-text-express-v1",
    "amazon--titan-text-lite": "amazon.titan-text-lite-v1",
    "anthropic--claude-3-haiku": "anthropic.claude-3-haiku-20240307-v1:0",
    "anthropic--claude-3-opus": "anthropic.claude-3-opus-20240229-v1:0",
    "anthropic--claude-3-sonnet": "anthropic.claude-3-sonnet-20240229-v1:0",
    "anthropic--claude-3.5-sonnet": "anthropic.claude-3-5-sonnet-20240620-v1:0",
    "anthropic--claude-3.7-sonnet": "anthropic.claude-3-7-sonnet-20250219-v1:0",
    "amazon--nova-micro": "amazon.nova-micro-v1:0",
    "amazon--nova-lite": "amazon.nova-lite-v1:0",
    "amazon--nova-pro": "amazon.nova-pro-v1:0",
}


class AICoreBedrockBaseModel(BaseModel):
    """AICoreBedrockBaseModel provides all adjustments
    to boto3 based LangChain classes to enable communication
    with SAP AI Core."""

    model_config = ConfigDict(extra='allow')

    def __init__(
            self,
            *args,
            model_id: str = "",
            deployment_id: str = "",
            model_name: str = "",
            config_id: str = "",
            config_name: str = "",
            proxy_client: Optional[BaseProxyClient] = None,
            **kwargs,
    ):
        """Extends the constructor of the base class with aicore specific parameters."""
        client_params = {
            "deployment_id": deployment_id,
            "model_name": model_name,
            "config_id": config_id,
            "config_name": config_name,
            "proxy_client": proxy_client,
        }
        kwargs["client_params"] = client_params
        super().__init__(*args, model_id=model_id, **kwargs)

    @classmethod
    def get_corresponding_model_id(cls, model_name):
        if model_name not in MODEL_NAME_TO_MODEL_ID_MAP:
            raise ValueError("Model specified is not supported.")
        return MODEL_NAME_TO_MODEL_ID_MAP[model_name]

    # pylint: disable=no-self-argument
    @model_validator(mode='before')
    def validate_environment(cls, values: Dict) -> Dict:
        client_params = values["client_params"]
        if not values.get("client"):
            values["client"] = Session().client(**client_params)
        if values.get('model_id') in (None, ''):
            values["model_id"] = AICoreBedrockBaseModel.get_corresponding_model_id(
                values["client"].aicore_deployment.model_name
            )
        return values


class ChatBedrock(AICoreBedrockBaseModel, ChatBedrock_):
    """Drop-in replacement for LangChain ChatBedrock."""

    model_config = ConfigDict(extra='allow')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class ChatBedrockConverse(AICoreBedrockBaseModel, ChatBedrockConverse_):
    """Drop-in replacement for LangChain ChatBedrockConverse."""

    model_config = ConfigDict(extra='allow')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class BedrockEmbeddings(AICoreBedrockBaseModel, BedrockEmbeddings_):
    """Drop-in replacement for LangChain BedrockEmbeddings."""

    model_config = ConfigDict(extra='allow')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


def _build_bedrock_model_kwargs(
        deployment: Deployment,
        temperature: float,
        max_tokens: int,
        top_k: Optional[int],
        top_p: float,
        stop_sequences: Optional[List[str]],
) -> Dict[str, Any]:
    """Builds the model_kwargs dictionary for Bedrock models."""
    if top_k:
        logging.warning(
            "Top-k is disabled for Amazon Bedrock models. Ignoring top-k value."
        )

    model_kwargs = {
        "temperature": temperature,
    }

    if deployment.model_name.startswith("anthropic"):
        model_kwargs["max_tokens"] = max_tokens
        model_kwargs["top_p"] = top_p
    else:  # Assuming Amazon Titan models otherwise
        model_kwargs["maxTokenCount"] = max_tokens
        model_kwargs["topP"] = top_p
        if stop_sequences:
            model_kwargs["stopSequences"] = stop_sequences

    return model_kwargs

@catalog.register(
    "gen-ai-hub",
    ChatBedrock,
    "anthropic--claude-3-haiku",
    "anthropic--claude-3-opus",
    "anthropic--claude-3-sonnet",
    "anthropic--claude-3.5-sonnet",
    "amazon--titan-text-lite",
    "amazon--titan-text-express",
)
def init_chat_model(
        proxy_client: BaseProxyClient,
        deployment: Deployment,
        temperature: float = 0.0,
        max_tokens: int = 256,
        top_k: Optional[int] = None,
        top_p: float = 1.0,
        stop_sequences: List[str] = None,
        model_id: Optional[str] = ''
):
    """Initializes a chat model using the legacy Bedrock Invoke API (`ChatBedrock`)."""
    model_kwargs = _build_bedrock_model_kwargs(
        deployment=deployment,
        temperature=temperature,
        max_tokens=max_tokens,
        top_k=top_k,
        top_p=top_p,
        stop_sequences=stop_sequences,
    )

    return ChatBedrock(
        model_name=deployment.model_name,
        model_id=model_id,
        deployment_id=deployment.deployment_id,
        proxy_client=proxy_client,
        model_kwargs=model_kwargs
    )

@catalog.register(
    "gen-ai-hub",
    ChatBedrockConverse,
    "anthropic--claude-3.7-sonnet",
)
def init_chat_converse_model(
        proxy_client: BaseProxyClient,
        deployment: Deployment,
        temperature: float = 0.0,
        max_tokens: int = 256,
        top_k: Optional[int] = None,
        top_p: float = 1.0,
        stop_sequences: List[str] = None,
        model_id: Optional[str] = ''
):
    """
    Initializes a chat model using the newer Bedrock Converse API (`ChatBedrockConverse`).

    The Converse API offers several advantages over the older Invoke API:
    - Unified interface for different models and modalities.
    - Native support for tool use (function calling).
    - Standardized request/response structure.
    """
    model_kwargs = _build_bedrock_model_kwargs(
        deployment=deployment,
        temperature=temperature,
        max_tokens=max_tokens,
        top_k=top_k,
        top_p=top_p,
        stop_sequences=stop_sequences,
    )

    return ChatBedrockConverse(
        model_name=deployment.model_name,
        model_id=model_id,
        deployment_id=deployment.deployment_id,
        proxy_client=proxy_client,
        model_kwargs=model_kwargs
    )

@catalog.register(
    "gen-ai-hub",
    BedrockEmbeddings,
    "amazon--titan-embed-text"
)
def init_embedding_model(proxy_client: BaseProxyClient, deployment: Deployment, model_id: Optional[str] = ''):
    return BedrockEmbeddings(deployment_id=deployment.deployment_id, proxy_client=proxy_client, model_id=model_id)
