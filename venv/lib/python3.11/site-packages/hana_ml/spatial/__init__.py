"""
Spatial Package

This package contains specific spatial functions.

The following functions are available:

    * :func:`create_predefined_srs`
    * :func:`is_srs_created`
    * :func:`get_created_srses`
"""
from .srs import (
    create_predefined_srs,
    is_srs_created,
    get_created_srses,
)

__all__ = [
    "create_predefined_srs",
    "is_srs_created",
    "get_created_srses",
]
