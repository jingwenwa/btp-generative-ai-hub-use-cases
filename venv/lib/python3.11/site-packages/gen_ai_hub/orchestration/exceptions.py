from typing import Dict, Any


class OrchestrationError(Exception):
    """
    This exception is raised when an error occurs during the execution of the
    orchestration service, typically due to incorrect usage, invalid configurations,
    or issues with run parameters defined by the user.

    Args:
        request_id: Unique identifier for the request that encountered the error.
        message: Detailed error message describing the issue.
        code: Error code associated with the specific type of failure.
        location: Specific component or step in the orchestration process where the error occurred.
        module_results: State information and partial results from various modules
                        at the time of the error, useful for debugging.
    """

    def __init__(
        self,
        request_id: str,
        message: str,
        code: int,
        location: str,
        module_results: Dict[str, Any],
    ):
        self.request_id = request_id
        self.message = message
        self.code = code
        self.location = location
        self.module_results = module_results
        super().__init__(message)
