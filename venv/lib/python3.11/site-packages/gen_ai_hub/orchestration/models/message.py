from dataclasses import dataclass
from enum import Enum
from typing import Union

from gen_ai_hub.orchestration.models.base import JSONSerializable


class Role(str, Enum):
    """
    Enumerates supported roles in LLM-based conversations.

    This enum defines the standard roles used in interactions with Large Language Models (LLMs).
    These roles are generally used to structure the input and distinguish between different parts of the conversation.

    Values:
        USER: Represents the human user's input in the conversation.
        SYSTEM: Represents system-level instructions or context setting for the LLM.
        ASSISTANT: Represents the LLM's responses in the conversation.
    """

    USER = "user"
    SYSTEM = "system"
    ASSISTANT = "assistant"


@dataclass
class Message(JSONSerializable):
    """
    Represents a single message in a prompt or conversation template.

    This base class defines the structure for all types of messages in a prompt,
    including content and role.

    Args:
        role: The role of the entity sending the message.
        content: The text content of the message.
    """

    role: Union[Role, str]
    content: str

    def to_dict(self):
        return {
            "role": self.role,
            "content": self.content,
        }


class SystemMessage(Message):
    """
    Represents a system message in a prompt or conversation template.

    System messages typically provide context or instructions to the AI model.

    Args:
        content: The text content of the system message.
    """

    def __init__(self, content: str):
        super().__init__(role=Role.SYSTEM, content=content)


class UserMessage(Message):
    """
    Represents a user message in a prompt or conversation template.

    User messages typically contain queries or inputs from the user.

    Args:
        content: The text content of the user message.
    """

    def __init__(self, content: str):
        super().__init__(role=Role.USER, content=content)


class AssistantMessage(Message):
    """
    Represents an assistant message in a prompt or conversation template.

    Assistant messages typically contain responses or outputs from the AI model.

    Args:
        content: The text content of the assistant message.
    """

    def __init__(self, content: str):
        super().__init__(role=Role.ASSISTANT, content=content)
