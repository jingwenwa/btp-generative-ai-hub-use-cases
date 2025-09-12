from typing import Optional, Dict

from gen_ai_hub.orchestration.models.base import JSONSerializable


class LLM(JSONSerializable):
    """
    Represents a Large Language Model (LLM) configuration.

    This class encapsulates the details required to specify and configure a particular
    LLM for use in natural language processing tasks. It includes the model's name,
    version, and any additional parameters needed for its operation.

    Args:
        name: The name of the LLM.
        version: The version of the LLM. Defaults to "latest".
        parameters: Additional parameters for configuring the LLM's behavior. Common parameters include:

            - 'temperature': Controls randomness in output. Lower values (e.g., 0.2)
              make output more focused and deterministic, while higher values (e.g., 0.8)
              make output more diverse and creative.
            - 'max_tokens': Sets the maximum number of tokens to generate in the response.
              This can help control the length of the model's output.
    """

    def __init__(
        self,
        name: str,
        version: str = "latest",
        parameters: Optional[Dict] = None,
    ):
        self.name = name
        self.version = version
        self.parameters = parameters or {}

    def to_dict(self):
        return {
            "model_name": self.name,
            "model_version": self.version,
            "model_params": self.parameters,
        }
