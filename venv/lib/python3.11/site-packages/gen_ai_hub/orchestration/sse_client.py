"""
Module for Server-Sent Events (SSE) clients for orchestration responses.

This module provides both synchronous and asynchronous SSE clients for iterating over streaming responses.
Each client is responsible for handling HTTP errors and for closing the underlying HTTP stream
when iteration is complete.
"""

import json
from enum import Enum
from typing import Iterable, Iterator, AsyncIterator

import dacite
import httpx

from gen_ai_hub.orchestration.exceptions import OrchestrationError
from gen_ai_hub.orchestration.models.response import OrchestrationResponseStreaming


def _parse_event_data(event_data: str, final_message: str) -> "OrchestrationResponseStreaming":
    """
    Parses the event data JSON string into an OrchestrationResponseStreaming object.

    Args:
        event_data: The JSON string containing event data.
        final_message: A message indicating the end of the stream.

    Returns:
        An OrchestrationResponseStreaming object parsed from the event data.
        Returns None if the event_data equals the final_message.

    Raises:
        OrchestrationError: If the event data contains an error code.
    """
    if event_data == final_message:
        return None
    event = json.loads(event_data)
    if "code" in event:
        raise OrchestrationError(
            request_id=event.get("request_id"),
            message=event.get("message"),
            code=event.get("code"),
            location=event.get("location"),
            module_results=event.get("module_results", {}),
        )
    return dacite.from_dict(
        data=event,
        data_class=OrchestrationResponseStreaming,
        config=dacite.Config(cast=[Enum]),
    )


class SSEClient:
    """
    A synchronous Server-Sent Events (SSE) client that wraps an httpx.Response for iterating
    over streaming responses.

    This client reads data chunks from the HTTP stream and parses each SSE event.
    For performance reasons the underlying HTTP stream is reused for subsequent calls.
    """

    def __init__(self, response_cm, prefix: str = "data: ", final_message: str = "[DONE]"):
        """
        Initializes the SSEClient.

        Args:
            response_cm: An httpx.Response context manager for the streaming response.
            prefix: The prefix string that identifies SSE event data (default "data: ").
            final_message: The message that indicates the end of the stream (default "[DONE]").
        """
        self.response_cm = response_cm
        self.event_prefix = prefix
        self.final_message = final_message
        self._response = None
        self._iterator = None
        

    def __enter__(self):
        """
        Synchronously enters the context for the streaming response.

        It awaits the response, checks for HTTP errors, and if an error occurs,
        reads the content and raises an OrchestrationError.

        Returns:
            Self, with the streaming response stored.
        """
        self._response = self.response_cm.__enter__()
        try:
            self._response.raise_for_status()
        except httpx.HTTPStatusError as error:
            content = self._response.read()
            error_response = httpx.Response(
                status_code=self._response.status_code,
                headers=self._response.headers,
                content=content,
                request=self._response.request,
            )
            self.response_cm.__exit__(None, None, None)
            _handle_http_error(error, error_response)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Synchronously exits the context, ensuring that the context manager is properly closed.
        """
        self.response_cm.__exit__(exc_type, exc_val, exc_tb)

    def iter_lines(self) -> Iterable[str]:
        """
        Reads data chunks from the HTTP stream and yields complete lines.

        This method accumulates incoming chunks until a newline is encountered, yielding one complete
        line at a time.

        Yields:
            Complete lines of text from the streaming response.
        """
        buffer = ""
        for chunk in self._response.iter_text():
            buffer += chunk
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                yield line.strip()
        if buffer:
            yield buffer.strip()

    def __iter__(self) -> Iterator:
        """
        Returns self as an iterator. Opens the HTTP stream and initializes the internal iterator.
        """
        return self

    def __next__(self):
        """
        Retrieves the next parsed SSE event from the stream.
        It skips any lines that do not start with the expected prefix. When the final message is encountered
        or the stream is exhausted, it closes the stream and raises StopIteration.
        """
        if self._iterator is None:
            self.__enter__()
            self._iterator = self.iter_lines()
        while True:
            try:
                line = next(self._iterator)
            except StopIteration:
                # End of stream; ensure resources are cleaned up.
                self.__exit__(None, None, None)
                raise StopIteration

            if not line or not line.startswith(self.event_prefix):
                continue

            event_data = line[len(self.event_prefix):]
            result = _parse_event_data(event_data, self.final_message)
            if result is None:
                # Final message encountered; close the stream.
                self.__exit__(None, None, None)
                raise StopIteration
            return result


class AsyncSSEClient:
    """
    An asynchronous SSE client for iterating over streaming responses.

    This client wraps an asynchronous HTTP stream (provided as a context manager) and ensures
    that the stream is properly opened and closed. It also checks for HTTP errors upon entering the stream.
    """

    def __init__(self, response_cm, prefix: str = "data: ", final_message: str = "[DONE]"):
        """
        Initializes the AsyncSSEClient.

        Args:
            response_cm: An asynchronous context manager for the HTTP streaming response.
            prefix: The SSE data prefix (default "data: ").
            final_message: The message indicating the end of the stream (default "[DONE]").
        """
        self.response_cm = response_cm
        self.event_prefix = prefix
        self.final_message = final_message
        self._response = None
        self._iterator = None

    async def __aenter__(self):
        """
        Asynchronously enters the context for the streaming response.

        It awaits the response, checks for HTTP errors, and if an error occurs,
        reads the content and raises an OrchestrationError.

        Returns:
            Self, with the streaming response stored.
        """
        self._response = await self.response_cm.__aenter__()
        try:
            self._response.raise_for_status()
        except httpx.HTTPStatusError as error:
            content = await self._response.aread()
            error_response = httpx.Response(
                status_code=self._response.status_code,
                headers=self._response.headers,
                content=content,
                request=self._response.request,
            )
            await self.response_cm.__aexit__(None, None, None)
            _handle_http_error(error, error_response)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Asynchronously exits the context, ensuring that the context manager is properly closed.
        """
        await self.response_cm.__aexit__(exc_type, exc_val, exc_tb)

    async def _internal_iterator(self) -> AsyncIterator:
        """
        Internal asynchronous generator that yields parsed events from the HTTP stream.
        """
        async for line in self._response.aiter_lines():
            line = line.strip()
            if not line or not line.startswith(self.event_prefix):
                continue
            event_data = line[len(self.event_prefix):]
            result = _parse_event_data(event_data, self.final_message)
            if result is None:
                break
            yield result

    def __aiter__(self):
        """
        Returns the async iterator (self). The initialization of the stream is deferred until the first
        call to __anext__.
        """
        return self

    async def __anext__(self):
        """
        Asynchronously retrieves the next event from the stream. On the first call, it enters the asynchronous
        context to start the stream. When the stream is exhausted or the final message is received, it properly
        exits the context.

        Returns:
            The next parsed event from the stream.

        Raises:
            StopAsyncIteration: When the stream is exhausted.
        """
        if self._iterator is None:
            # Lazily initialize the stream.
            await self.__aenter__()
            self._iterator = self._internal_iterator().__aiter__()
        try:
            return await self._iterator.__anext__()
        except StopAsyncIteration:
            await self.__aexit__(None, None, None)
            raise StopAsyncIteration


def _handle_http_error(error, response: httpx.Response):
    """
    Handles HTTP errors by raising an OrchestrationError with details from the response.

    Args:
        error: The original HTTP error.
        response: The httpx.Response object containing error details.

    Raises:
        OrchestrationError with information extracted from the response.
    """
    if not response.content:
        raise error
    error_content = response.json()
    raise OrchestrationError(
        request_id=error_content.get("request_id"),
        message=error_content.get("message"),
        code=error_content.get("code"),
        location=error_content.get("location"),
        module_results=error_content.get("module_results", {}),
    ) from error
