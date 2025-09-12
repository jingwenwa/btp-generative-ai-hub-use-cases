#pylint:disable=line-too-long, too-many-arguments, too-many-instance-attributes
#pylint:disable=invalid-name, too-few-public-methods, too-many-statements, too-many-locals
#pylint:disable=too-many-branches, c-extension-no-member
'''
This module provides a class of text splitter / chunking. The following class is available:
    * :class:`TextSplitter`
'''
import logging
import uuid
from hdbcli import dbapi
from hana_ml.algorithms.pal.pal_base import (
    ParameterTable,
    PALBase,
    try_drop,
    require_pal_usable,
    pal_param_register
)
logger = logging.getLogger(__name__)

class TextSplitter(PALBase):
    r"""
    For a long text, it may be necessary to transform it to better suit. The text chunking procedure provides methods to split a long text into smaller chunks that can fit into a specific model's context window.

    At a high level, text splitters work as follows:

    1. Split the text into small, semantically meaningful chunks (often sentences).
    2. Combine these small chunks into a larger chunk until you reach a certain size (as measured by some function).
    3. When it reaches that size, make that chunk its own piece of text and then start creating a new chunk of text with some overlap (to keep context between chunks).

    The splitting methods are as follows:

    - Character splitter: Split the text based on a character, even if it splits a whole word into two chunks.
    - Recursive: Recursive chunking based on a list of separators.
    - Document: Various chunking methods for different document types (PlainText, HTML) and different languages (English, Chinese, Japanese, German, French, Spanish, Portuguese).

    *Character Splitting*

    Character splitting is the most basic form of splitting up the text. It is the process of simply dividing the text into N-character sized chunks regardless of their content or form.

    *Recursive Character Text Splitting*

    The problem with the Character splitter is that it does not take into account the structure of our document at all. It simply splits by a fixed number of characters.

    The Recursive Character Text Splitter helps with this. It specifies a series of separators which will be used to split the text. The separators are as follows or you can customize them:

    - "\\n\\n" - Double new line, or most commonly paragraph breaks
    - "\\n" - New lines
    - " " - Spaces
    - "" - Characters

    *Document Specific Splitting*

    This Splitting is all about making your chunking strategy fit your different data formats or languages.

    The PlainText and HTML splitters will be similar to Recursive Character, but with different separators or different languages.

    *PlainText with English*

    - "\\n\\n",
    - "\\n",
    - " ".

    *PlainText with Chinese*

    - "\\n\\n",
    - "\\n",
    - "。",
    - " ".

    Parameters
    ----------
    chunk_size : int, optional
        Maximum size of chunks to return.

        Defaults to 30.

    overlap : int, optional
        Overlap in characters between chunks.

        Defaults to 0.

    strip_whitespace : bool, optional
        Whether to strip whitespace from the start and end of every chunk.

        Defaults to False.

    keep_separator : bool, optional
        Whether to keep the separator and where to place it in each corresponding chunk.

        Defaults to False.

    thread_ratio : float, optional
        The ratio of available threads for multi-thread task:

        - 0: single thread.
        -  0–1: uses the specified percentage of available threads. PAL uses all available threads if the number is 1.0.

        Defaults to 1.0.

    split_type : str, optional
        Configuration for spliting type of all elements:

        - 'char': character splitting.
        - 'recursive': recursive splitting.
        - 'document': document splitting.

        Defaults to 'recursive'.

    doc_type : str, optional
        Configuration for document type of all elements:

        - 'plain': plain text.
        - 'html': html text.

        Only valid when the ``split_type`` is the 'document' splitter.

        Defaults to 'plain'.

    language : str, optional
        Configuration for language of all elements:

        - 'auto': auto detect.
        - 'en': English.
        - 'zh': Chinese.
        - 'ja': Japanese.
        - 'de': German.
        - 'fr': French.
        - 'es': Spanish.
        - 'pt': Portuguese.

        Only valid when the ``split_type`` is the 'document' splitter and ``doc_type`` is 'plain'.

        Defaults to 'auto'.

    separator : str, optional
        Configuration for splitting separators of all elements.

        No default value.

    Attributes
    ----------
    statistics_ : DataFrame
        Statistics.

    Examples
    --------
    >>> textsplitter = TextSplitter(chunk_size=300)
    >>> res = textsplitter.split_text(data)
    >>> print(res.collect())
    >>> print(textsplitter.statistics_.collect())
    """
    lang_map = {"auto":0, "en": 1, "zh": 2, "ja": 3, "de": 4, "fr":5, "es": 6, "pt": 7}
    split_type_map = {"character":0, "recursive": 1, "document": 2}
    doc_type_map = {"plain":0, "html": 1}
    def __init__(self,
                 chunk_size=None,
                 overlap=None,
                 strip_whitespace=None,
                 keep_separator=None,
                 thread_ratio=None,
                 split_type=None,
                 doc_type=None,
                 language=None,
                 separator=None):

        setattr(self, 'hanaml_parameters', pal_param_register())
        super(TextSplitter, self).__init__()
        self.chunk_size = self._arg('chunk_size', chunk_size, int)
        self.thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        self.overlap = self._arg('overlap', overlap, int)
        self.strip_whitespace = self._arg('strip_whitespace', strip_whitespace, bool)
        self.keep_separator = self._arg('keep_separator', keep_separator, bool)
        self.split_type = self._arg('split_type', split_type, self.split_type_map)
        self.doc_type = self._arg('doc_type', doc_type, self.doc_type_map)
        self.language = self._arg('language_type', language, self.lang_map)
        self.separator = self._arg('separator', separator, str)

    def split_text(self, data,
                   order_status=False,
                   specific_split_type=None,
                   specific_doc_type=None,
                   specific_language=None,
                   specific_separator=None):
        r"""
        Split the text into smaller chunks and return the result.

        Parameters
        ----------
        data : DataFrame
            The input data, structured as follows:

            - ID: type VARCHAR, NVARCHAR, INTEGER, the text id.
            - TEXT: type VARCHAR, NVARCHAR, NCLOB, the text content.

        order_status : bool, optional
            Specifies whether or not to order the text chunks generated by the splitter.

            Defaults to False.
        specific_split_type : dict, optional
            Specifies the split type (different from the global split type) for specific text elements in a dict,
            where keys are for document IDs and values should be valid split types.

            Defaults to None.

        specific_doc_type : dict, optional
            Specifies the doc type (different from the global doc type) for specific text elements in a dict,
            where keys are for document IDs and values should be valid doc types.

            Defaults to None.

        specific_language : dict, optional
            Specifies the language (different from the global language) for specific text elements in a dict,
            where keys are for document IDs and values should be valid language abberviations supported by the algorithm.

            Defaults to None.

        specific_separator : dict, optional
            Specifies the separators (different from the global separator) for specific text elements in a dict,
            where keys are for document IDs and values should be valid separators.

            Defaults to None.

        Returns
        -------
        DataFrame
            The result of the text split.
        """
        conn = data.connection_context
        require_pal_usable(conn)
        order_status = self._arg('order_status', order_status, bool)
        specific_split_type = self._arg('specific_split_type', specific_split_type, dict)
        specific_doc_type = self._arg('specific_doc_type', specific_doc_type, dict)
        specific_language = self._arg('specific_language', specific_language, dict)
        specific_separator = self._arg('specific_separator', specific_separator, dict)
        specific_all = {'SPECIFIC_SPLIT_TYPE' : specific_split_type,
                        'SPECIFIC_DOC_TYPE' : specific_doc_type,
                        'SPECIFIC_LANGUAGE_TYPE' : specific_language,
                        'SPECIFIC_SEPARATOR' : specific_separator}
        all_maps = {'SPECIFIC_SPLIT_TYPE' : self.split_type_map,
                    'SPECIFIC_DOC_TYPE' : self.doc_type_map,
                    'SPECIFIC_LANGUAGE_TYPE' : self.lang_map}
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        outputs = ['RESULT', 'STATS']
        outputs = ['#PAL_CHUNKING_{}_TBL_{}'.format(name, unique_id) for name in outputs]
        res_tbl, stats_tbl = outputs
        param_rows = [('CHUNK_SIZE', self.chunk_size, None, None),
                      ('OVERLAP', self.overlap, None, None),
                      ('STRIP_WHITESPACE', self.strip_whitespace, None, None),
                      ('KEEP_SEPARATOR', self.keep_separator, None, None),
                      ('GLOBAL_SPLIT_TYPE', self.split_type, None, None),
                      ('GLOBAL_DOC_TYPE', self.doc_type, None, None),
                      ('GLOBAL_LANGUAGE_TYPE', self.language, None, None),
                      ('GLOBAL_SEPARATOR', None, None, self.separator),
                      ('THREAD_RATIO', None, self.thread_ratio, None),
                      ('ORDER_STATUS', order_status, None, None)]
        for specific_param in specific_all:
            specific_val = specific_all[specific_param]
            if specific_val is not None:
                if "SEPARATOR" not in specific_param:
                    val_map = all_maps[specific_param]
                    param_rows.extend([(specific_param + '_' + val_key,
                                        val_map[specific_val[val_key]],
                                        None, None) for val_key in specific_val])
                else:
                    param_rows.extend([(specific_param + '_' + val_key,
                                        None, None,
                                        specific_val[val_key]) for val_key in specific_val])
        try:
            self._call_pal_auto(conn,
                                'PAL_TEXTSPLIT',
                                data,
                                ParameterTable().with_data(param_rows),
                                *outputs)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, outputs)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, outputs)
            raise
        self.statistics_ = conn.table(stats_tbl)
        return conn.table(res_tbl)
