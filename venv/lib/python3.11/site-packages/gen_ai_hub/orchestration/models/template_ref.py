from gen_ai_hub.orchestration.models.base import JSONSerializable


class TemplateRef(JSONSerializable):
    """
    Represents a prompt template reference for generating prompts or conversations.

    This is a factory class for creating a reference to a prompt template.
    It is used to reference a template by id, or the tuple: scenario, name, version

    """

    def __init__(
        self,
        **kwargs
    ):
        for key, value in kwargs.items():
            setattr(self, key, value)

    @classmethod
    def from_id(
            cls,
            prompt_template_id: str
    ):
        return cls(id=prompt_template_id)

    @classmethod
    def from_tuple(
            cls,
            scenario: str,
            name: str,
            version: str
    ):
        return cls(scenario=scenario, name=name, version=version)

    def to_dict(self):
        template_ref = {}
        for key, value in self.__dict__.items():
            template_ref[key] = value
        return { "template_ref": template_ref }
