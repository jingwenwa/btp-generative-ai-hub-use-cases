from typing import Any, Dict, List

from ai_api_client_sdk.models.model_version import ModelVersion


class Model:
    """The Model object defines a model
    :param executable_id: ID of the executable
    :type executable_id: str
    :param model: Unique name of the model
    :type model: str
    :param description: Description of the model, defaults to None
    :type description: str, optional
    :param versions: List of available model versions, defaults to None
    :type versions: List[class:`ai_api_client_sdk.models.model_version.ModelVersion`], optional
    :param `**kwargs`: The keyword arguments are there in case there are additional attributes returned from server
    """

    def __init__(
        self,
        executable_id: str,
        model: str,
        description: str = None,
        versions: List[ModelVersion] = None,
        **kwargs,
    ):
        self.executable_id: str = executable_id
        self.model: str = model
        self.description: str = description
        self.versions: List[ModelVersion] = versions

    @staticmethod
    def from_dict(model_dict: Dict[str, Any]):
        """Returns a :class:`ai_api_client_sdk.models.model.Model` object, created from the values in the dict
        provided as parameter

        :param model_dict: Dict which includes the necessary values to create the object
        :type model_dict: Dict[str, Any]
        :return: An object, created from the values provided
        :rtype: class:`ai_api_client_sdk.models.model.Model`
        """
        if model_dict.get("versions"):
            model_dict["versions"] = [
                ModelVersion.from_dict(ia) for ia in model_dict["versions"]
            ]
        return Model(**model_dict)
