"""
This module contains exception and error classes for the API.

The following classes are available:

    * :class:`Error`
    * :class:`FitIncompleteError`
    * :class:`BadSQLError`
    * :class:`PALUnusableError`

"""
import logging
logger = logging.getLogger(__name__)#pylint: disable=invalid-name

class Error(Exception):
    """Base class for hana_ml exceptions."""

class FitIncompleteError(Error):
    """Exception class raised by performing predict or score without fit first."""
    def __init__(self, message="The model has not been initialized. Please call the fit() method to obtain a model!"):
        self.message = message
        logger.error(message)
        super().__init__(self.message)

    def __str__(self):
        return self.message

class BadSQLError(Error):
    """Raised if malformed tokens or unexpected comments are detected in SQL."""

class PALUnusableError(Error):
    """Raised if hana_ml cannot access a compatible version of PAL."""

class ModelExistingError(Error):
    """Raised if model exists during the model table creation."""
