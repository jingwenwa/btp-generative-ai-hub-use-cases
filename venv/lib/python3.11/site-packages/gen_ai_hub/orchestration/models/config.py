from typing import Optional, Union

from gen_ai_hub.orchestration.models.base import JSONSerializable
from gen_ai_hub.orchestration.models.content_filtering import ContentFiltering
from gen_ai_hub.orchestration.models.data_masking import DataMasking
from gen_ai_hub.orchestration.models.document_grounding import GroundingModule
from gen_ai_hub.orchestration.models.llm import LLM
from gen_ai_hub.orchestration.models.template import Template
from gen_ai_hub.orchestration.models.template_ref import TemplateRef


class OrchestrationConfig(JSONSerializable):
    """
    Configuration for the Orchestration Service's content generation process.

    Defines modules for a harmonized API that combines LLM-based content generation
    with additional processing functionalities.

    The orchestration service allows for advanced content generation by processing inputs through a series of steps:
    template rendering, text generation via LLMs, and optional input/output transformations such as data masking
    or filtering.

    Args:
        template: Template object for rendering input prompts.
        llm: Language model for text generation.
        filtering: Module for filtering and validating input/output content.
        data_masking: Module for anonymizing or pseudonymizing sensitive information.
        grounding: Module for document grounding.
        stream_options: Global options for controlling streaming behavior.
    """

    def __init__(
            self,
            template: Union[Template, TemplateRef],
            llm: LLM,
            filtering: Optional[ContentFiltering] = None,
            data_masking: Optional[DataMasking] = None,
            grounding: Optional[GroundingModule] = None,
            stream_options: Optional[dict] = None,
    ):
        self.template = template
        self.llm = llm
        self.filtering = filtering
        self.data_masking = data_masking
        self.grounding = grounding
        self.stream_options = stream_options
        self._stream = False

    def _get_module_configurations(self):
        configs = {
            "templating_module_config": self.template.to_dict(),
            "llm_module_config": self.llm.to_dict(),
        }

        if self.data_masking:
            configs["masking_module_config"] = self.data_masking.to_dict()

        if self.filtering:
            configs["filtering_module_config"] = self.filtering.to_dict()

        if self.grounding:
            configs["grounding_module_config"] = self.grounding.to_dict()

        return configs

    def to_dict(self):
        config = {
            "module_configurations": self._get_module_configurations(),
            **({"stream": True} if self._stream else {}),
            **({"stream_options": self.stream_options} if self._stream and self.stream_options else {})
        }

        return config
