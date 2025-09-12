import os
from ai_api_client_sdk.ai_api_v2_client import RestClient

from ai_core_sdk.exception import AICoreSDKException
from ai_core_sdk.helpers import is_within_aicore


class InternalRestClient(RestClient):
    """InternalRestClient is a class implemented for sending requests to services within aicore. The InternalRestClient should only be used for services that do not require authentication when called within aicore.
    """

    def __init__(
        self,
        base_url=None,
        get_token=None,
        resource_group=None,
        *args,
        **kwargs,
    ):
        # Disallows parameters set by this class. This way this class must not be adjusted as the signiture of its parent class changes.
        if base_url or get_token or resource_group:
            raise AICoreSDKException(
                "InternalRestClient should must not be called with base_url, get_token or resource_group"
            )

        if not is_within_aicore():
            raise AICoreSDKException(
                "Attempted to skip authentication even though SDK is not used within aicore"
            )

        api_base_url = os.getenv("AICORE_TRACKING_ENDPOINT")

        # framing the base url of tracking endpoint
        base_url = f"{api_base_url}/api/v1"

        # dummy token creator function to be passed to rest client
        def dummy_token_creator():
            return ""

        token_creator = dummy_token_creator

        # resource group will be auto detected from the request going to tracking api from the training pod.
        # Hence not passing the resource group id
        resource_group = ""

        super().__init__(
            base_url=base_url,
            get_token=token_creator,
            resource_group=resource_group,
            *args,
            **kwargs,
        )
