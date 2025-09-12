from typing import List, Optional

from gen_ai_hub.orchestration.models.base import JSONSerializable
from gen_ai_hub.orchestration.models.content_filter import ContentFilter


class InputFiltering(JSONSerializable):
    """Module for managing and applying input content filters.

        Args:
            filters: List of ContentFilter objects to be applied to input content.
    """

    def __init__(
            self,
            filters: List[ContentFilter]
    ):
        self.filters = filters

    def to_dict(self):
        return {
            "filters": [f.to_dict() for f in self.filters],
        }


class OutputFiltering(JSONSerializable):
    """Module for managing and applying output content filters.

        Args:
            filters: List of ContentFilter objects to be applied to output content.
            stream_options: Module-specific streaming options.
    """

    def __init__(self,
                 filters: List[ContentFilter],
                 stream_options: Optional[dict] = None
                 ):
        self.filters = filters
        self.stream_options = stream_options

    def to_dict(self):
        config = {
            "filters": [f.to_dict() for f in self.filters],
        }

        if self.stream_options:
            config["stream_options"] = self.stream_options

        return config

class ContentFiltering(JSONSerializable):
    """Module for managing and applying content filters.

    Args:
        input_filtering: Module for filtering and validating input content before processing.
        output_filtering: Module for filtering and validating output content after generation.
    """

    def __init__(
            self,
            input_filtering: Optional[InputFiltering] = None,
            output_filtering: Optional[OutputFiltering] = None
    ):
        self.input_filtering = input_filtering
        self.output_filtering = output_filtering

    def to_dict(self):
        config = {}
        if self.input_filtering:
            config["input"] = self.input_filtering.to_dict()
        if self.output_filtering:
            config["output"] = self.output_filtering.to_dict()

        return config
