"""Streaming response iterators Overriding Google Vertex AI ResponseIterator classes."""

from typing import Union, Deque

import proto
import requests

from google.api_core.rest_streaming import ResponseIterator
from google.api_core.rest_streaming_async import AsyncResponseIterator
from google.auth.aio.transport import Response as AsyncResponse
from google.protobuf.message import Message


def process_chunk(obj: str, ready_objs: Deque[str], prefix: str, suffix: str, chunk: str) -> (str, Deque[str]):
    """Process a chunk of a streaming response.

    This method acts as a helper for `_process_chunk` of `ResponseIterator` and `AsyncResponseIterator`,
    providing an overridable method for `BaseResponseIterator`.
    """
    obj += chunk

    if obj.endswith(suffix):
        if not obj.startswith(prefix):
            raise ValueError(
                f"Invalid streaming format, expected prefix {prefix}, instead got {obj}"
            )
        obj = obj[len(prefix): -len(suffix)]
        if obj[0] != "{" or obj[-1] != "}":
            raise ValueError(
                f"Can only parse JSON objects, instead got {obj}"
            )
        ready_objs.append(obj)
        obj = ""

    return obj, ready_objs


class ServerSentEventsResponseIterator(ResponseIterator):

    def __init__(
            self,
            response: requests.Response,
            response_message_cls: Union[proto.Message, Message],
            prefix: str = "data: ",
            suffix: str = "\n\n",
    ):
        super().__init__(response=response, response_message_cls=response_message_cls)
        self._prefix = prefix
        self._suffix = suffix

    def _process_chunk(self, chunk: str):
        self._obj, self._ready_objs = process_chunk(self._obj, self._ready_objs, self._prefix, self._suffix, chunk)


class AsyncServerSentEventsResponseIterator(AsyncResponseIterator):  # pylint: disable=too-few-public-methods
    """Asynchronous Iterator overriding AsyncResponseIterator of VertexAI.
    This is support AI Core proxy streamed response format"""

    def __init__(self,
                 response: AsyncResponse,
                 response_message_cls: Union[proto.Message, Message],
                 prefix: str = "data: ",
                 suffix: str = "\n\n",
                 ):
        super().__init__(response=response, response_message_cls=response_message_cls)
        self._prefix = prefix
        self._suffix = suffix

    def _process_chunk(self, chunk: str):
        self._obj, self._ready_objs = process_chunk(self._obj, self._ready_objs, self._prefix, self._suffix, chunk)
