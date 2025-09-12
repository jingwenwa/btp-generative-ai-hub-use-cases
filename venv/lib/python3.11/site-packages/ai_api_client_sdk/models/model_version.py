from datetime import datetime
from typing import Any, Dict, Optional


class ModelVersion:
    """The ModelVersion object defines a version for a model
    :param name: Name of the model version
    :type name: str
    :param is_latest: True if model version is latest, otherwise false
    :type is_latest: bool
    :param deprecated: True if model version is deprecated, otherwise false
    :type deprecated: bool
    :param retirement_date: Retirement date of the model version, defaults to None
    :type retirement_date: datetime, optional
    :param `**kwargs`: The keyword arguments are there in case there are additional attributes returned from server
    """

    def __init__(
        self,
        name: str,
        is_latest: bool,
        deprecated: bool,
        retirement_date: Optional[datetime] = None,
        **kwargs,
    ):
        self.name: str = name
        self.is_latest: bool = is_latest
        self.deprecated: bool = deprecated
        self.retirement_date: Optional[datetime] = retirement_date

    def __eq__(self, other):
        if not isinstance(other, ModelVersion):
            return False
        for k in self.__dict__.keys():
            if getattr(self, k) != getattr(other, k):
                return False
        return True

    @staticmethod
    def from_dict(model_version_dict: Dict[str, Any]):
        """Returns a :class:`ai_api_client_sdk.models.model_version.ModelVersion` object, created from the values in the dict
        provided as parameter

        :param model_version_dict: Dict which includes the necessary values to create the object
        :type model_version_dict: Dict[str, Any]
        :return: An object, created from the values provided
        :rtype: class:`ai_api_client_sdk.models.model_version.ModelVersion`
        """
        return ModelVersion(**model_version_dict)
