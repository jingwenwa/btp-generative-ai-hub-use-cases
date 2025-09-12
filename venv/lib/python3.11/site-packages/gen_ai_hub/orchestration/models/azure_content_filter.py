from enum import Enum
from typing import Union, Literal

from gen_ai_hub.orchestration.models.content_filter import ContentFilter, ContentFilterProvider


class AzureThreshold(int, Enum):
    """
    Enumerates the threshold levels for the Azure Content Safety service.

    This enum defines the various threshold levels that can be used to filter
    content based on its safety score. Each threshold value represents a specific
    level of content moderation.

    Values:
        ALLOW_SAFE: Allows only Safe content.
        ALLOW_SAFE_LOW: Allows Safe and Low content.
        ALLOW_SAFE_LOW_MEDIUM: Allows Safe, Low, and Medium content.
        ALLOW_ALL: Allows all content (Safe, Low, Medium, and High).
    """

    ALLOW_SAFE = 0
    ALLOW_SAFE_LOW = 2
    ALLOW_SAFE_LOW_MEDIUM = 4
    ALLOW_ALL = 6


class AzureContentFilter(ContentFilter):
    """
    Specific implementation of ContentFilter for Azure's content filtering service.

    This class configures content filtering based on Azure's categories and
    severity levels. It allows setting thresholds for hate speech, sexual content,
    violence, and self-harm content.

    Args:
        hate: Threshold for hate speech content.
        sexual: Threshold for sexual content.
        violence: Threshold for violent content.
        self_harm: Threshold for self-harm content.
    """

    def __init__(
            self,
            hate: Union[AzureThreshold, Literal[0, 2, 4, 6]],
            sexual: Union[AzureThreshold, Literal[0, 2, 4, 6]],
            violence: Union[AzureThreshold, Literal[0, 2, 4, 6]],
            self_harm: Union[AzureThreshold, Literal[0, 2, 4, 6]],
            **kwargs
    ):
        hate = hate if isinstance(hate, AzureThreshold) else AzureThreshold(hate)
        sexual = sexual if isinstance(sexual, AzureThreshold) else AzureThreshold(sexual)
        violence = violence if isinstance(violence, AzureThreshold) else AzureThreshold(violence)
        self_harm = self_harm if isinstance(self_harm, AzureThreshold) else AzureThreshold(self_harm)

        super().__init__(
            provider=ContentFilterProvider.AZURE,
            config={
                "Hate": hate,
                "Sexual": sexual,
                "Violence": violence,
                "SelfHarm": self_harm,
                **kwargs
            },
        )
