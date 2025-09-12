from abc import ABC
from enum import Enum
from typing import List

from gen_ai_hub.orchestration.models.base import JSONSerializable


class DataMaskingProviderName(str, Enum):
    """
    Enumerates the available data masking providers.

    This enum defines the supported providers for masking sensitive data in the LLM module.

    Values: SAP_DATA_PRIVACY_INTEGRATION: Refers to the SAP Data Privacy Integration service, which offers
    anonymization and pseudonymization capabilities for sensitive data.
    """
    SAP_DATA_PRIVACY_INTEGRATION = "sap_data_privacy_integration"


class DataMaskingProvider(JSONSerializable, ABC):
    """
    Abstract base class for data masking providers.

    This class serves as a blueprint for implementing different data masking providers. Each provider is responsible
    for masking sensitive or personally identifiable information (PII) according to a specific method.

    Inherited by:
        - SAPDataPrivacyIntegration
    """
    pass


class DataMasking(JSONSerializable):
    """
    Manages data masking operations using the specified providers.

    The DataMasking class is responsible for configuring and executing data masking processes
    by delegating to one or more data masking providers. It supports either anonymization or pseudonymization
    of sensitive information, depending on the provider and method used.

    Args:
        providers: A list of data masking providers to handle the masking process.
                   Currently, only a single provider is supported.

    Raises: ValueError: If more than one provider is specified, as multiple providers are not supported in the
    current version.
    """

    def __init__(
            self,
            providers: List[DataMaskingProvider],
    ):
        if len(providers) > 1:
            raise ValueError("Multiple data masking providers are not supported in the current version.")

        self.providers = providers

    def to_dict(self):
        return {
            "masking_providers": [provider.to_dict() for provider in self.providers]
        }
