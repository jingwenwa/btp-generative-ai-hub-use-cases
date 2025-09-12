from enum import Enum
from typing import List, Dict, Any

from gen_ai_hub.orchestration.models.base import JSONSerializable


class GroundingType(str, Enum):
    """
    Enumerates supported grounding types.
    """
    DOCUMENT_GROUNDING_SERVICE = "document_grounding_service"


class DataRepositoryType(str, Enum):
    """
    Enumerates data repository types.
    """
    VECTOR = "vector"  # DataRepository with vector embeddings
    URL = "url"  # website supporting elastic search
    MS_SHAREPOINT = "MSSHAREPOINT"  # Microsoft SharePoint base documents for embedding


class DocumentMetadata(JSONSerializable):
    """Restrict documents considered during search to those annotated with the given metadata.

    Args:
        key: The key for the metadata.
        value: The list of values for the metadata.
        select_mode: Select mode for search filters.
    """

    def __init__(self, key: str, value: List[str], select_mode: List[str] = None):
        self.key = key
        self.value = value
        self.select_mode = select_mode

    def to_dict(self):
        config = {"key": self.key, "value": self.value}
        if self.select_mode:
            config["select_mode"] = self.select_mode

        return config


class GroundingFilterSearch(JSONSerializable):
    """Search configuration for the data repository.

    Args:
        max_chunk_count: The maximum number of chunks > 0 to return.
        max_document_count: The maximum number of documents > 0 to return.
                            Only supports 'vector' dataRepositoryType.
                            Cannot be used with 'maxChunkCount'.
                            If maxDocumentCount is given, then only one chunk per document is returned.
    """

    def __init__(self, max_chunk_count: int = None, max_document_count: int = None):
        self.max_chunk_count = max_chunk_count
        self.max_document_count = max_document_count
        if self.max_chunk_count and self.max_document_count:
            raise ValueError("Cannot set both max_chunk_count and max_document_count")

    def to_dict(self):
        config = {}
        if self.max_chunk_count:
            config["max_chunk_count"] = self.max_chunk_count
        if self.max_document_count:
            config["max_document_count"] = self.max_document_count
        return config


class DocumentGroundingFilter(JSONSerializable):
    """Module for configuring document grounding filters.
    
    Args:
        id: The unique identifier for the grounding filter.
        search_config: GroundingFilterSearchConfiguration object.
        data_repository_type: Only include DataRepositories with the given type:
                              'vector' or 'url' of website supporting elastic search.
        data_repositories: list of data repositories to search.
                           Specify ['*'] to search across all DataRepositories or
                           give a specific list of DataRepository ids.
        data_repository_metadata: The metadata for the data repository.
                                  Restrict DataRepositories considered during search to those annotated with the given
                                  metadata. Useful when combined with dataRepositories=['*']
        document_metadata: DocumentMetadata object.
        chunk_metadata: Restrict chunks considered during search to those with the given metadata.
    """

    def __init__(self,
                 id: str,
                 data_repository_type: str,
                 search_config: GroundingFilterSearch = None,
                 data_repositories: List[str] = None,
                 data_repository_metadata: List[Dict[str, Any]] = None,
                 document_metadata: DocumentMetadata = None,
                 chunk_metadata: List[Dict[str, Any]] = None
                 ):
        self.id = id
        self.data_repository_type = data_repository_type
        self.search_config = search_config
        self.data_repositories = data_repositories
        self.data_repository_metadata = data_repository_metadata
        self.document_metadata = document_metadata
        self.chunk_metadata = chunk_metadata

    def to_dict(self):
        config = {
            "id": self.id,
            "data_repository_type": self.data_repository_type,
        }
        if self.search_config:
            config["search_config"] = self.search_config.to_dict()
        if self.data_repositories:
            config["data_repositories"] = self.data_repositories
        if self.data_repository_metadata:
            config["data_repository_metadata"] = self.data_repository_metadata
        if self.document_metadata:
            config["document_metadata"] = self.document_metadata.to_dict()
        if self.chunk_metadata:
            config["chunk_metadata"] = self.chunk_metadata
        return config


class DocumentGrounding(JSONSerializable):
    """defines the detailed configuration for the Grounding module.

    Args:
        filters: List of DocumentGroundingFilter objects.
        input_params: The list of input parameters used for grounding input questions.
        output_param: Parameter name used for grounding output.
        metadata_params: Parameter name used for specifying metadata parameters.
    """

    def __init__(self, input_params: List[str], output_param: str, filters: List[DocumentGroundingFilter] = None,
                 metadata_params: [str] = None):
        self.input_params = input_params
        self.output_param = output_param
        self.filters = filters
        self.metadata_params = metadata_params

    def to_dict(self):
        config = {
            "input_params": self.input_params,
            "output_param": self.output_param,
        }
        if self.filters:
            config["filters"] = [filter.to_dict() for filter in self.filters]
        if self.metadata_params:
            config["metadata_params"] = self.metadata_params

        return config


class GroundingModule(JSONSerializable):
    """Module for managing and applying grounding aka RAG configurations.

    Args:
        type: The type of the grounding module.
        config: Configuration dictionary for the grounding module.
    """

    def __init__(self, type: str, config: DocumentGrounding):
        self.type = type
        self.config = config

    def to_dict(self):
        return {
            "type": self.type,
            "config": self.config.to_dict(),
        }
