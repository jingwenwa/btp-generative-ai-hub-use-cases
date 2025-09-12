#pylint: disable=invalid-name, too-many-arguments, too-many-locals, line-too-long
#pylint: disable=too-many-lines, relative-beyond-top-level, too-many-arguments, bare-except
#pylint: disable=superfluous-parens, too-many-branches, no-else-return, broad-except
#pylint: disable=consider-using-f-string, too-many-statements, too-many-instance-attributes
'''
This module provides various functions of text mining. The following functions are available:
    * :func:`tf_analysis`
    * :func:`text_classification`
    * :func:`get_related_doc`
    * :func:`get_related_term`
    * :func:`get_relevant_doc`
    * :func:`get_relevant_term`
    * :func:`get_suggested_term`
    * :class:`TFIDF`
    * :class:`TextClassificationWithModel`
'''
import os
import logging
import uuid
import time

from hdbcli import dbapi
from hana_ml.dataframe import DataFrame
from hana_ml.ml_base import try_drop, execute_logged, quotename
from hana_ml.algorithms.pal.pal_base import (
    ParameterTable,
    arg,
    PALBase,
    call_pal_auto_with_hint,
    pal_param_register,
    require_pal_usable
)

logger = logging.getLogger(__name__)

class TextClassificationWithModel(PALBase):
    r"""
    Text classification class. This class enables us to train an **RDT** classifier for TF-IDF vectorized text data
    firstly and then apply it for inference.

    Parameters
    ----------
    language : str, optional
        Specify the language type. HANA cloud instance currently supports 'EN', 'DE', 'ES', 'FR', 'RU', 'PT'.
        If None, auto detection will be applied.

        Defaults to None (auto detection).

    enable_stopwords : bool, optional
        Determine whether to turn on stopwords.

        Defaults to True.

    keep_numeric : bool, optional
        Determine whether to keep numbers.

        Valid only when ``enable_stopwords`` is True.

        Defaults to False.

    allowed_list : bool, optional
        A list of words that are retained by the stopwords logic.

        Valid only when ``enable_stopwords`` is True.

    notallowed_list : bool, optional
        A list of words, which are recognized and deleted
        by the stopwords logic.

        Valid only when ``enable_stopwords`` is True.

    n_estimators : int, optional
        Specifies the number of decision trees in the RDT model.

        Defaults to 100.

    max_depth : int, optional
        The maximum depth of a tree in RDT, where -1 means unlimited.

        Default to 56.

    split_threshold : float, optional
        Specifies the stopping condition of the tree-growing process in RDT model:
        if the improvement value of the best split is less than this value,
        the tree stops growing.

        Defaults to 1e-5.

    min_samples_leaf : int, optional
        Specifies the minimum number of records in a leaf of a tree in RDT model.

        Defaults to 1.

        .. note::

            Note that parameters ``n_estimators``, ``max_depth``, ``split_threshold``
            and ``min_samples_leaf`` are all for building the RDT model
            for text classification.

    Examples
    --------
    >>> tc = TextClassificationWithModel(enabel_stopwords=True,
    ...                                  n_estimators=50,
    ...                                  max_depth=6,
    ...                                  min_samples_leaf=2,
    ...                                  split_threshold=1e-6)
    >>> tc.fit(data=document_file_train_data)
    >>> pred_res = tc.predict(data=document_file_test_data)
    """
    lang_map = {"EN": "EN",
                "DE": "DE",
                "ES": "ES",
                "FR": "FR",
                "RU": "RU",
                "PT": "PT",
                "en": "EN",
                "de": "DE",
                "es": "ES",
                "fr": "FR",
                "ru": "RU",
                "pt": "PT"}
    def __init__(self,
                 language=None,
                 enable_stopwords=True,
                 keep_numeric=None,
                 allowed_list=None,
                 notallowed_list=None,
                 n_estimators=None,
                 max_depth=None,
                 split_threshold=None,
                 min_samples_leaf=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(TextClassificationWithModel, self).__init__()
        self.language = self._arg('language', language, self.lang_map)
        self.enable_stopwords = self._arg('enable_stopwords', enable_stopwords, bool)
        self.keep_numeric = self._arg('keep_numeric', keep_numeric, bool)
        self.allowed_list = (self._arg('allowed_list', allowed_list, list))
        self.notallowed_list = self._arg('notallowed_list', notallowed_list, list)
        if self.allowed_list:
            self.allowed_list = ", ".join(list(self.allowed_list))
        if self.notallowed_list:
            self.notallowed_list = ", ".join(list(self.notallowed_list))
        self.n_estimators = self._arg('n_estimators', n_estimators, int)
        self.max_depth = self._arg('max_depth', max_depth, int)
        self.min_samples_leaf = self._arg('min_samples_leaf', min_samples_leaf, int)
        self.split_threshold = self._arg('split_threshold', split_threshold, float)

    def fit(self, data, seed=None, thread_ratio=None):
        r"""
        Train the model.

        Parameters
        ----------
        data : DataFrame
            Input data, structured as follows:

            - 1st column, ID.
            - 2nd column, Document content.
            - 3rd column, Document category.
        seed : int, optional
            Specify the seed for random number generation.
        thread_ratio : float, optional
            The ratio of total number of threads that can be used by this function.

        Returns
        -------
        A fitted instance of class `TextClassificationWithModel`.
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        conn = data.connection_context
        param_rows = [('LANGUAGE', None, None, self.language),
                      ('ENABLE_STOPWORDS', None, None,
                       str(self.enable_stopwords) if self.enable_stopwords is not None else None),
                      ('KEEP_NUMERIC', None, None, 'True' if self.keep_numeric else 'False'),
                      ('ALLOWED_LIST', None, None, self.allowed_list),
                      ('NOTALLOWED_LIST', None, None, self.notallowed_list),
                      ('SEED', seed, None, None),
                      ('THREAD_RATIO', None, thread_ratio, None),
                      ('TREES_NUM', self.n_estimators, None, None),
                      ('MAX_DEPTH', self.max_depth, None, None),
                      ('NODE_SIZE', self.min_samples_leaf, None, None),
                      ('SPLIT_THRESHOLD', None, self.split_threshold, None)]
        unique_id = str(uuid.uuid1()).replace('-', '_')
        tables = ['TF_IDF_RES', 'DOC_TERM_FREQ', 'DOC_CATEGORY', "MODEL", "EXTRA"]
        tables = ["#PAL_TEXT_CLASSIFICATION_{}_TBL_{}_{}".format(table, self.id, unique_id)
                  for table in tables]
        try:
            self._call_pal_auto(conn,
                                'PAL_TEXT_CLASSIFICATION_TRAIN',
                                data,
                                ParameterTable().with_data(param_rows),
                                *tables)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, tables)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, tables)
            raise
        self.tf_idf_ = conn.table(tables[0])
        self.doc_term_freq_ = conn.table(tables[1])
        self.doc_category_ = conn.table(tables[2])
        self.model_ = [self.tf_idf_, self.doc_term_freq_, self.doc_category_, conn.table(tables[3])]
        return self

    def predict(self, data, rdt_top_n=None, thread_ratio=None):
        r"""
        Predict the model.

        Parameters
        ----------
        data : DataFrame
            Input data, structured as follows:

            - 1st column, ID.
            - 2nd column, Document content.
        rdt_top_n : int, optional
            Specify the number of top terms to be used for the Random Decision Tree algorithm.
        thread_ratio : float, optional
            The ratio of total number of threads that can be used by this function.

        Returns
        -------
        DataFrame

            The result.
        """
        if not hasattr(self, 'model_'):
            raise AttributeError("Model is not fitted yet.")
        conn = data.connection_context
        param_rows = [('RDT_TOP_N', rdt_top_n, None, None),
                      ('THREAD_RATIO', None, thread_ratio, None)]
        unique_id = str(uuid.uuid1()).replace('-', '_')
        tables = ['PREDICT', 'STATS']
        tables = ["#PAL_TEXT_CLASSIFICATION_{}_TBL_{}_{}".format(table, self.id, unique_id)
                  for table in tables]
        try:
            self._call_pal_auto(conn,
                                'PAL_TEXT_CLASSIFICATION_PREDICT',
                                self.model_[0],
                                self.model_[1],
                                self.model_[2],
                                data,
                                self.model_[3],
                                ParameterTable().with_data(param_rows),
                                *tables)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, tables)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, tables)
            raise
        self.stats_ = conn.table(tables[1])
        self.statistics_ = self.stats_
        return conn.table(tables[0])

def tf_analysis(data, lang=None, enable_stopwords=None, keep_numeric=None):
    """
    Perform Term Frequency(TF) analysis on the given document.
    TF is the number of occurrences of term in document.

    This function is available in HANA Cloud.


    Parameters
    ----------
    data : DataFrame
        Input data, structured as follows:

        - 1st column, ID.
        - 2nd column, Document content.
        - 3rd column, Document category.

    lang : str, optional
        Specify the language type. HANA cloud instance currently supports 'EN', 'DE', 'ES', 'FR', 'RU', 'PT'.
        If None, auto detection will be applied.

        Defaults to None (auto detection).

    enable_stopwords : bool, optional
        Determine whether to turn on stopwords.

        Defaults to True.

    keep_numeric : bool, optional
        Determine whether to keep numbers.

        Defaults to False.

    Returns
    -------
    A tuple of DataFrames
        TF-IDF result, structured as follows:

        - TM_TERM.
        - TM_TERM_FREQUENCY.
        - TM_IDF_FREQUENCY.
        - TF_VALUE.
        - IDF_VALUE.
        - TF_IDF_VALUE.

        Document term frequency table, structured as follows:

        - ID.
        - TM_TERM.
        - TM_TERM_FREQUENCY.

        Document category table, structured as follows:

        - ID.
        - Document category.

    Examples
    --------

    Input DataFrame df:

    >>> df.collect()
          ID                                                  CONTENT       CATEGORY
    0   doc1                      term1 term2 term2 term3 term3 term3     CATEGORY_1
    1   doc2                      term2 term3 term3 term4 term4 term4     CATEGORY_1
    2   doc3                      term3 term4 term4 term5 term5 term5     CATEGORY_2
    3   doc4    term3 term4 term4 term5 term5 term5 term5 term5 term5     CATEGORY_2
    4   doc5                                              term4 term6     CATEGORY_3
    5   doc6                                  term4 term6 term6 term6     CATEGORY_3

    Invoke tf_analysis function:

    >>> tfidf= tf_analysis(data=df)

    Output:

    >>> tfidf[0].head(3).collect()
      TM_TERMS TM_TERM_TF_F  TM_TERM_IDF_F  TM_TERM_TF_V  TM_TERM_IDF_V
    0    term1            1              1      0.030303       1.791759
    1    term2            3              2      0.090909       1.098612
    2    term3            7              4      0.212121       0.405465

    >>> tfidf[1].head(3).collect()
         ID TM_TERMS  TM_TERM_FREQUENCY
    0  doc1    term1                  1
    1  doc1    term2                  2
    2  doc1    term3                  3

    >>> tfidf[2].head(3).collect()
          ID    CATEGORY
    0   doc1  CATEGORY_1
    1   doc2  CATEGORY_1
    2   doc3  CATEGORY_2
    """
    conn_context = data.connection_context
    if not conn_context.is_cloud_version():
        raise AttributeError("Feature not supported for on-premise.")
    lang = arg('lang', lang, str)
    if isinstance(enable_stopwords, bool):
        if enable_stopwords:
            enable_stopwords = 'True'
        else:
            enable_stopwords = 'False'
    if isinstance(keep_numeric, bool):
        if keep_numeric:
            keep_numeric = 'True'
        else:
            keep_numeric = 'False'
    param_rows = [('LANGUAGE', None, None, lang),
                  ('ENABLE_STOPWORDS', None, None, enable_stopwords),
                  ('KEEP_NUMERIC', None, None, keep_numeric)]
    unique_id = str(uuid.uuid1()).replace('-', '_').upper()
    result_tbl = "#TM_TFIDF_RESULT_{}".format(unique_id)
    doc_term_freq = "#TM_TFIDF_DOC_TERM_FREQ_{}".format(unique_id)
    doc_category = "#TM_TFIDF_DOC_CATEGORY_{}".format(unique_id)
    output = [result_tbl, doc_term_freq, doc_category]
    try:
        call_pal_auto_with_hint(conn_context,
                                None,
                                'PAL_TF_ANALYSIS',
                                data,
                                ParameterTable().with_data(param_rows),
                                *output)
    except dbapi.Error as db_err:
        logger.exception(str(db_err))
        try_drop(conn_context, output)
        raise
    except Exception as db_err:
        logger.exception(str(db_err))
        try_drop(conn_context, output)
        raise
    return (conn_context.table(result_tbl), conn_context.table(doc_term_freq), conn_context.table(doc_category))

def search_docs_by_keywords(pred_data,
                            ref_data=None,
                            num_best_matches=None,
                            thread_number=None,
                            thread_ratio=None,
                            lang=None,
                            bm25_k1=None,
                            bm25_b=None,
                            **kwargs):
    r"""
    This function searches for the best matching documents based on the given keywords. The algorithms used for matching is BM25.

    This function supports English, German, Spanish, French, Russian and Portuguese and is available in HANA Cloud.

    Parameters
    ----------
    pred_data : DataFrame
        The prediction data for search, structured as follows:

        - 1st column, ID.
        - 2nd column, KEYWORDS or Content.
    ref_data : DataFrame or a tuple of DataFrames
        Specify the reference data.

        If ``ref_data`` is a DataFrame, then it should be structured as follows:

        - 1st column, ID.
        - 2nd column, Document content.
        - 3rd column, Document category.

        Otherwise if ``ref_data`` is a tuple of DataFrames, the it should be corresponding to the reference TF-IDF data, with DataFrames
        structured as follows:

          - 1st DataFrame

            .. only:: html

                - TM_TERM.
                - TM_TERM_FREQUENCY.
                - TM_IDF_FREQUENCY.
                - TF_VALUE.
                - IDF_VALUE.
                - TF_IDF_VALUE.

            .. only:: latex

                ============ ===========
                Column Index Content
                ============ ===========
                1            TM_TERM
                2            TM_TERM_FREQUENCY
                3            TM_IDF_FREQUENCY
                4            TF_VALUE
                5            IDF_VALUE
                ============ ===========

          - 2nd DataFrame

            .. only:: html

                - 1st column, ID.
                - 2nd column, TM_TERM.
                - 3rd column, TM_TERM_FREQUENCY.

            .. only:: latex

                ============ =================
                Column Index Content
                ============ =================
                1            ID
                2            TM_TERM
                3            TM_TERM_FREQUENCY
                ============ =================

          - 3rd DataFrame

            .. only:: html

                - 1st column, ID.
                - 2nd column, Document category.

            .. only:: latex

                ============ =================
                Column Index Content
                ============ =================
                1            ID
                2            Document category
                ============ =================
    num_best_matches : int, optional
        Controls how many results to output.

        Defaults to 1.

    thread_number : int, optional
        Specifies the number of threads that can be used by this function.

        Defaults to 1.

    thread_ratio : float, optional
        Specifies the ratio of threads that can be used by this function.
        The range of this parameter is from 0 to 1, where 0 means only using one thread,
        and 1 means using at most all the currently available threads.
        Values outside this range are ignored and this function heuristically determines the number of threads to use.

        Defaults to 0.0.

    lang : str, optional
        Specify the language type. HANA cloud instance currently supports 'EN', 'DE', 'ES', 'FR', 'RU', 'PT'.
        If None, auto detection will be applied.

        Defaults to None (auto detection).

    bm25_k1 : bool, optional
        The ``bm25_k1`` parameter in the BM25 algorithm is a tuning parameter that controls the term frequency saturation effect.
        It determines how much the term frequency (TF) component contributes to the overall BM25 score.

        Defaults to 1.2.

    bm25_b : bool, optional
        The ``bm25_b`` parameter in the BM25 algorithm is a tuning parameter that controls the degree of document length normalization.
        It adjusts the impact of document length on the BM25 score.

        Defaults to 0.75.

    Returns
    -------
    Two DataFrames:

        - Result.
        - Place holder.


    Examples
    --------

    Input DataFrame df:

    >>> df.collect()
          ID                                                  CONTENT       CATEGORY
    0   doc1                      term1 term2 term2 term3 term3 term3     CATEGORY_1
    1   doc2                      term2 term3 term3 term4 term4 term4     CATEGORY_1
    2   doc3                      term3 term4 term4 term5 term5 term5     CATEGORY_2
    3   doc4    term3 term4 term4 term5 term5 term5 term5 term5 term5     CATEGORY_2
    4   doc5                                              term4 term6     CATEGORY_3
    5   doc6                                  term4 term6 term6 term6     CATEGORY_3

    Invoke search_docs_by_keywords function:

    >>> result, _ = search_docs_by_keywords(pred_data=df)
    >>> result.collect()

    """
    conn_context = pred_data.connection_context
    if not conn_context.is_cloud_version():
        raise AttributeError("Feature not supported for on-premise.")
    tf_analysis_result = None
    doc_term_freq = None
    doc_category = None
    lang = arg('lang', lang, str)
    if isinstance(ref_data, DataFrame):
        tf_analysis_result, doc_term_freq, doc_category = tf_analysis(ref_data, lang)
    else:
        tf_analysis_result, doc_term_freq, doc_category = ref_data

    num_best_matches = arg('num_best_matches', num_best_matches, int)
    thread_number = arg('thread_number', thread_number, int)
    thread_ratio = arg('thread_ratio', thread_ratio, float)
    bm25_k1 = arg('bm25_k1', bm25_k1, float)
    bm25_b = arg('bm25_b', bm25_b, float)

    param_rows = [('THREAD_NUMBER', thread_number, None, None),
                  ('NUM_BEST_MATCHES', num_best_matches, None, None),
                  ('THREAD_RATIO', None, thread_ratio, None),
                  ('BM25_K1', None, bm25_k1, None),
                  ('BM25_B', None, bm25_b, None)]

    extra_params = dict(kwargs)
    for kkey, vvalue in extra_params.items():
        if isinstance(vvalue, float):
            param_rows.extend([(kkey.upper(), None, vvalue, None)])
        elif isinstance(vvalue, (int, bool)):
            param_rows.extend([(kkey.upper(), vvalue, None, None)])
        else:
            param_rows.extend([(kkey.upper(), None, None, vvalue)])

    unique_id = str(uuid.uuid1()).replace('-', '_').upper()
    result_tbl = "#TM_KEYWORDS_SEARCH_RESULT_{}".format(unique_id)
    extra_tbl = "#TM_KEYWORDS_EXTRA_RESULT_{}".format(unique_id)
    output = [result_tbl, extra_tbl]
    try:
        call_pal_auto_with_hint(conn_context,
                                None,
                                'PAL_SEARCH_DOCS_BY_KEYWORDS',
                                tf_analysis_result,
                                doc_term_freq,
                                doc_category,
                                pred_data,
                                ParameterTable().with_data(param_rows),
                                *output)
    except dbapi.Error as db_err:
        logger.exception(str(db_err))
        try_drop(conn_context, output)
        raise
    except Exception as db_err:
        logger.exception(str(db_err))
        try_drop(conn_context, output)
        raise
    return conn_context.table(result_tbl), conn_context.table(extra_tbl)

def text_classification(pred_data,
                        ref_data=None,
                        k_nearest_neighbours=None,
                        thread_ratio=None,
                        lang=None,
                        index_name=None,
                        created_index=None):
    r"""
    This function classifies (categorizes) an input document with respect to sets of categories (taxonomies)
    using TF-IDF text vectorizer and KNN classifier.

    Parameters
    ----------
    pred_data : DataFrame
        The prediction data for classification, structured as follows:

        - 1st column, ID.
        - 2nd column, Document content.

    ref_data : DataFrame or a tuple of DataFrames
        Specify the reference data.

        If ``ref_data`` is a DataFrame, then it should be structured as follows:

        - 1st column, ID.
        - 2nd column, Document content.
        - 3rd column, Document category.

        Otherwise if ``ref_data`` is a tuple of DataFrames, the it should be
        corresponding to the reference TF-IDF data, with DataFrames
        structured as follows:

          - 1st DataFrame

            .. only:: html

                - 1st column, TM_TERM.
                - 2nd column, TM_TERM_FREQUENCY.
                - 3rd column, TM_IDF_FREQUENCY.
                - 4th column, TF_VALUE.
                - 5th column, IDF_VALUE.

            .. only:: latex

                ============ ===========
                Column Index Content
                ============ ===========
                1            TM_TERM
                2            TM_TERM_FREQUENCY
                3            TM_IDF_FREQUENCY
                4            TF_VALUE
                5            IDF_VALUE
                ============ ===========

          - 2nd DataFrame

            .. only:: html

                - 1st column, ID.
                - 2nd column, TM_TERM.
                - 3rd column, TM_TERM_FREQUENCY.

            .. only:: latex

                ============ =================
                Column Index Content
                ============ =================
                1            ID
                2            TM_TERM
                3            TM_TERM_FREQUENCY
                ============ =================

          - 3rd DataFrame

            .. only:: html

                - 1st column, ID.
                - 2nd column, Document category.

            .. only:: latex

                ============ =================
                Column Index Content
                ============ =================
                1            ID
                2            Document category
                ============ =================


    k_nearest_neighbours : int, optional
        Number of nearest neighbors (k).

        Defaults to 1.

    thread_ratio : float, optional
        Specify the ratio of total number of threads that can be used by this function.
        The range of this parameter is from 0 to 1, where 0 means only using 1 thread,
        and 1 means using at most all the currently available threads.
        Values outside this range are ignored and this function heuristically determines the number of threads to use.

        Only valid for a HANA cloud instance.

        Defaults to 0.0.

    lang : str, optional
        Specify the language type. HANA cloud instance currently supports 'EN', 'DE', 'ES', 'FR', 'RU', 'PT'.
        If None, auto detection will be applied.

        Defaults to None (auto detection) in HANA cloud and None in HANA On-Premise (please provide the value in this case).

    index_name : str, optional
        Specify the index name that apply only to the HANA On-Premise instance.

        If None, it will be generated.

    created_index : {"index": xxx, "schema": xxx, "table": xxx}, optional
        Use the created index on the given table that apply only to the HANA On-Premise instance.

        Defaults to None.

    Returns
    -------
    DataFrames (cloud version)
        Text classification result, structured as follows:

        - Predict data ID.
        - TARGET.

        Statistics table, structured as follows:

        - Predict data ID.
        - K.
        - Training data ID.
        - Distance.

    DataFrame (on-premise version)
        Text classification result, structured as follows:

        - Predict data ID.
        - RANK.
        - CATEGORY_SCHEMA.
        - CATEGORY_TABLE.
        - CATEGORY_COLUMN.
        - CATEGORY_VALUE.
        - NEIGHBOR_COUNT.
        - SCORE.


    Examples
    --------

    Input DataFrame df:

    >>> df.collect()
          ID                                                  CONTENT       CATEGORY
    0   doc1                      term1 term2 term2 term3 term3 term3     CATEGORY_1
    1   doc2                      term2 term3 term3 term4 term4 term4     CATEGORY_1
    2   doc3                      term3 term4 term4 term5 term5 term5     CATEGORY_2
    3   doc4    term3 term4 term4 term5 term5 term5 term5 term5 term5     CATEGORY_2
    4   doc5                                              term4 term6     CATEGORY_3
    5   doc6                                  term4 term6 term6 term6     CATEGORY_3

    Invoke text_classification:

    >>> res = text_classification(pred_data=df.select(df.columns[0], df.columns[1]), ref_data=df)

    Result on a SAP HANA cloud instance:

    >>> res[0].head(1).collect()
           ID     TARGET
    0    doc1 CATEGORY_1

    Result on a SAP HANA On-Premise instance:

    >>> res[0].head(1).collect()
         ID RANK  CATEGORY_SCHEMA                   CATEGORY_TABLE    CATEGORY_COLUMN  CATEGORY_VALUE  NEIGHBOR_COUNT
    0  doc1    1       "PAL_USER" "TM_CATEGORIZE_KNN_DT_6_REF_TBL"         "CATEGORY"      CATEGORY_1               1
    ...                               SCORE
    ...0.5807794005266924131092309835366905

    """
    conn_context = pred_data.connection_context
    if conn_context.is_cloud_version():
        tf_analysis_result = None
        doc_term_freq = None
        doc_category = None
        if isinstance(ref_data, DataFrame):
            tf_analysis_result, doc_term_freq, doc_category = tf_analysis(ref_data, lang)
        else:
            tf_analysis_result, doc_term_freq, doc_category = ref_data
        k_nearest_neighbours = arg('k_nearest_neighbours', k_nearest_neighbours, int)
        thread_ratio = arg('thread_ratio', thread_ratio, float)
        param_rows = [('THREAD_RATIO', None, thread_ratio, None),
                      ('K_NEAREST_NEIGHBOURS', k_nearest_neighbours, None, None),
                      ('FEATURE_SEARCH_METHOD', 0, None, None)]
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        result_tbl = "#TM_TEXT_CLASSIFICATION_RESULT_{}".format(unique_id)
        stats_tbl = "#TM_TEXT_CLASSIFICATION_STATS_{}".format(unique_id)
        output = [result_tbl, stats_tbl]
        try:
            call_pal_auto_with_hint(conn_context,
                                    None,
                                    'PAL_TEXTCLASSIFICATION',
                                    tf_analysis_result,
                                    doc_term_freq,
                                    doc_category,
                                    pred_data,
                                    ParameterTable().with_data(param_rows),
                                    *output)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn_context, output)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn_context, output)
            raise
        return conn_context.table(result_tbl), conn_context.table(stats_tbl)
    else:
        lang = arg('lang', lang, str, required=True)
        if created_index:
            mater_tab = created_index["table"]
            if "schema" in mater_tab:
                schema = created_index["schema"]
            else:
                schema = conn_context.get_current_schema()
        else:
            mater_tab = 'TM_CATEGORIZE_KNN_{}_REF_TBL'.format(ref_data.name.replace('-', '_').upper())
            ref_data.save(mater_tab, force=True)
            logger.warning("Materized the dataframe to HANA table: %s.", mater_tab)
            schema = conn_context.get_current_schema()
            conn_context.add_primary_key(mater_tab, ref_data.columns[0], schema)
        if created_index is None:
            if index_name is None:
                index_name = "TM_CATEGORIZE_KNN_{}_INDEX".format(ref_data.name.replace('-', '_').upper())
            _try_drop_index(conn_context, index_name)
            _try_create_index(conn_context, index_name, mater_tab, ref_data.columns[1], schema, lang)
            logger.warning("Created index: %s.", index_name)
        if k_nearest_neighbours is None:
            k_nearest_neighbours = 1
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        result_df = None

        for row in pred_data.select(pred_data.columns[0]).collect().to_numpy().flatten():
            sel_statement = """
            SELECT '{6}' ID, * FROM TM_CATEGORIZE_KNN(
                DOCUMENT ({0})
                SEARCH NEAREST NEIGHBORS {1} "{4}"
                FROM "{5}"."{2}"
                RETURN TOP DEFAULT
                "{3}"
                FROM "{5}"."{2}")""".format(pred_data.filter(""""{cond}"='{rowid}'""".format(cond=pred_data.columns[0], rowid=row)).select(pred_data.columns[1]).select_statement,
                                            k_nearest_neighbours,
                                            mater_tab,
                                            ref_data.columns[2],
                                            ref_data.columns[1],
                                            schema,
                                            row)
            temp_df = conn_context.sql(sel_statement)
            if result_df:
                result_df = result_df.union(temp_df)
            else:
                result_df = temp_df
        return result_df

def _try_drop_index(conn_context, name):
    sql = "DROP FULLTEXT INDEX {}".format(quotename(name))
    try:
        with conn_context.connection.cursor() as cur:
            execute_logged(cur,
                           sql,
                           conn_context.sql_tracer,
                           conn_context)
    except:
        pass

def _try_create_index(conn_context, name, table, col, schema, lang):
    sql = """CREATE FULLTEXT INDEX {0} ON "{3}"."{1}"("{2}")
    TEXT ANALYSIS ON TEXT MINING ON LANGUAGE DETECTION ('{4}')
    TEXT MINING CONFIGURATION OVERLAY '<xml><property name="similarityFunction">COSINE</property></xml>';
    """.format(name, table, col, schema, lang)
    try:
        with conn_context.connection.cursor() as cur:
            execute_logged(cur,
                           sql,
                           conn_context.sql_tracer,
                           conn_context)
    except Exception as err:
        logger.error(err)
        pass

    ci_timeout = 7200
    if "OPCI_TIMEOUT" in os.environ:
        ci_timeout = int(os.environ["OPCI_TIMEOUT"])
    for _ in range(0, ci_timeout):
        time.sleep(1)
        is_in_queue = conn_context.sql("SELECT COUNT(*) FROM {}.{} WHERE INDEXING_STATUS({})='QUEUED';".format(quotename(schema),
                                                                                                               quotename(table),
                                                                                                               quotename(col))).collect().iat[0, 0]
        if is_in_queue == 0:
            break

def _get_basic_func(func, pred_data, ref_data, top, threshold, lang, index_name, thread_ratio, created_index, key=None):
    conn_context = pred_data.connection_context
    tf_analysis_result = None
    doc_term_freq = None
    doc_category = None
    if conn_context.is_cloud_version():
        if isinstance(ref_data, DataFrame):
            tf_analysis_result, doc_term_freq, doc_category = tf_analysis(ref_data, lang)
        else:
            tf_analysis_result, doc_term_freq, doc_category = ref_data
        top = arg('top', top, int)
        threshold = arg('threshold', threshold, float)
        param_rows = []
        if threshold is not None:
            param_rows.append(('THRESHOLD', None, threshold, None))
        if top is not None:
            param_rows.append(('TOP', top, None, None))
        if thread_ratio is not None:
            param_rows.append(('THREAD_RATIO', None, thread_ratio, None))

        cols = pred_data.columns
        # massive mode
        if (len(cols) > 1):
            key = arg('key', key, str)
            if key is None:
                key = cols[0]
            if key is not None and key not in cols:
                msg = f"Please select key from {cols}!"
                logger.error(msg)
                raise ValueError(msg)
            cols.remove(key)
            pred_data = pred_data[[key] + [cols[0]]]
            func = func + '_MULTIINPUT'

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        result_tbl = "#TM_{}_RESULT_{}".format(func, unique_id)
        try:
            call_pal_auto_with_hint(conn_context,
                                    None,
                                    func,
                                    tf_analysis_result,
                                    doc_term_freq,
                                    doc_category,
                                    pred_data,
                                    ParameterTable().with_data(param_rows),
                                    result_tbl)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn_context, result_tbl)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn_context, result_tbl)
            raise
        return conn_context.table(result_tbl)
    else:
        if created_index:
            mater_tab = created_index["table"]
            if "schema" in mater_tab:
                schema = created_index["schema"]
            else:
                schema = conn_context.get_current_schema()
        else:
            mater_tab = '{}_{}_REF_TBL'.format(func, ref_data.name.replace('-', '_').upper())
            ref_data.save(mater_tab, force=True)
            logger.warning("Materized the dataframe to HANA table: %s.", mater_tab)
            schema = conn_context.get_current_schema()
            conn_context.add_primary_key(mater_tab, ref_data.columns[0], schema)
        if created_index is None:
            if index_name is None:
                index_name = "{}_{}_INDEX".format(func, ref_data.name.replace('-', '_').upper())
            _try_drop_index(conn_context, index_name)
            _try_create_index(conn_context, index_name, mater_tab, ref_data.columns[1], schema, lang)
            logger.warning("Created index: %s.", index_name)
        if top is None:
            top = 'DEFAULT'
        sel_statement = None
        if func in ("TM_GET_RELATED_DOCUMENTS"):
            sel_statement = """
                            SELECT T.* FROM {0}(
                                DOCUMENT ({1})
                                SEARCH "{4}"
                                FROM "{5}"."{2}"
                                RETURN
                                TOP {3}
                                {6}) AS T""".format(func,
                                                    pred_data.select_statement,
                                                    mater_tab,
                                                    top,
                                                    ref_data.columns[1],
                                                    schema,
                                                    ref_data.columns[0])
        elif func in ("TM_GET_RELEVANT_TERMS"):
            sel_statement = """
                            SELECT * FROM {0}(
                                DOCUMENT ({1})
                                SEARCH "{4}"
                                FROM "{5}"."{2}"
                                RETURN
                                TOP {3})""".format(func, pred_data.select_statement, mater_tab, top, ref_data.columns[1], schema)
        elif func in ("TM_GET_RELEVANT_DOCUMENTS"):
            sel_statement = """
                            SELECT T.* FROM {0}(
                                TERM '{1}'
                                SEARCH "{4}"
                                FROM "{5}"."{2}"
                                RETURN
                                TOP {3}
                                {6}) AS T""".format(func,
                                                    pred_data.collect().iat[0, 0],
                                                    mater_tab,
                                                    top,
                                                    ref_data.columns[1],
                                                    schema,
                                                    ref_data.columns[0])
        else:
            sel_statement = """
                            SELECT * FROM {0}(
                                TERM '{1}'
                                SEARCH "{4}"
                                FROM "{5}"."{2}"
                                RETURN
                                TOP {3})""".format(func, pred_data.collect().iat[0, 0], mater_tab, top, ref_data.columns[1], schema)
        return conn_context.sql(sel_statement)

def get_related_doc(pred_data, ref_data=None, top=None, threshold=None, lang='EN', index_name=None, thread_ratio=None, created_index=None, key=None):
    """
    This function returns the top-ranked related documents for a query document / or multiple docments based on Term Frequency - Inverse Document Frequency (TF-IDF) result or reference data.

    Parameters
    ----------
    pred_data : DataFrame

        Accepts input data in two different data structures:

        Single-row mode:

            - 1st column, Document content.

        .. Note ::
            Important to note that this mode can only process one content at a time. Therefore, the input table must have a structure of one row and one column only.

        Massive mode supports multiple rows:

            - 1st column, ID.
            - 2nd column, Document content.

        .. Note ::
            Important to note that this mode can only valid in SAP HANA Cloud instance.

    key : str, optional
        Specifies the ID column. Only valid when ``pred_data`` contains multiple rows.

        Defaults to the first column of ``pred_data``.

    ref_data : DataFrame or a tuple of DataFrames
        Specify the reference data.

        If ``ref_data`` is a DataFrame, then it should be structured as follows:

            - 1st column, ID.
            - 2nd column, Document content.
            - 3rd column, Document category.

        If ``ref_data`` is a tuple of DataFrames, then it should be corresponding to
        the reference TF-IDF data, with each DataFrame structured as follows:

            - 1st DataFrame, TF-IDF Result.

              .. only:: html

                - 1st column, TM_TERM.
                - 2nd column, TF_VALUE.
                - 3rd column, IDF_VALUE.

            .. only:: latex

                ============ ===========
                Column Index Content
                ============ ===========
                1            TM_TERM
                2            TF_VALUE
                3            IDF_VALUE
                ============ ===========

            - 2nd DataFrame, Doc Term Freq Table

              .. only:: html

                - 1st column, ID.
                - 2nd column, TM_TERM.
                - 3rd column, TM_TERM_FREQUENCY.

            .. only:: latex

                ============ =================
                Column Index Content
                ============ =================
                1            ID
                2            TM_TERM
                3            TM_TERM_FREQUENCY
                ============ =================

            - 3rd DataFrame, Doc Category Table

              .. only:: html

                - 1st column, ID.
                - 2nd column, Document category.

            .. only:: latex

                ============ =================
                Column Index Content
                ============ =================
                1            ID
                2            Document category
                ============ =================

    top : int, optional
        Only show top N results. If 0, it shows all.

        Defaults to 0.

    threshold : float, optional
        Only the results which score bigger than this value will be put into the result table.

        Defaults to 0.0.

    lang : str, optional
        Specify the language type. The HANA Cloud instance currently supports 'EN', 'DE', 'ES', 'FR', 'RU', 'PT'.
        If None, auto detection will be applied.

        Defaults to 'EN'.

    index_name : str, optional
        Specify the index name that apply only to the HANA On-Premise instance.

        If None, it will be generated.

    thread_ratio : float, optional
        Specify the ratio of total number of threads that can be used by this function.
        The range of this parameter is from 0 to 1, where 0 means only using 1 thread,
        and 1 means using at most all the currently available threads.
        Values outside this range are ignored and this function heuristically determines the number of threads to use.

        Only valid for a HANA cloud instance.

        Defaults to 0.0.

    created_index : {"index": xxx, "schema": xxx, "table": xxx}, optional
        Use the created index on the given table that apply only to the HANA On-Premise instance.

        Defaults to None.

    Returns
    -------
    DataFrame


    Examples
    --------

    Assuming 'ref_df' is an existing DataFrame that contains document IDs, content, and categories.
    Below are examples of invoking the 'get_related_doc' function.

    >>> ref_df.collect()
          ID                                                  CONTENT       CATEGORY
    0   doc1                      term1 term2 term2 term3 term3 term3     CATEGORY_1
    1   doc2                      term2 term3 term3 term4 term4 term4     CATEGORY_1
    2   doc3                      term3 term4 term4 term5 term5 term5     CATEGORY_2
    3   doc4    term3 term4 term4 term5 term5 term5 term5 term5 term5     CATEGORY_2
    4   doc5                                              term4 term6     CATEGORY_3
    5   doc6                                  term4 term6 term6 term6     CATEGORY_3

    For SAP HANA cloud:

    1. Invoking the function on a SAP HANA cloud instance using a single-row input DataFrame 'pred_df':

    >>> pred_df.collect()
                       CONTENT
    0  term2 term2 term3 term3

    >>> get_related_doc(pred_data=pred_df, ref_data=tfidf).collect()
           ID       SCORE
    0    doc2    0.891550
    1    doc1    0.804670
    2    doc3    0.042024
    3    doc4    0.021225

    tfidf is a DataFrame returned by tf_analysis function, please refer to the examples section of tf_analysis for its content.

    2. Invoking the function on a SAP HANA cloud instance using a massive input DataFrame 'pred_df_massive' which contains multiple rows of data:

    >>> pred_df_massive.collect()
       ID                          CONTENT
    0   1          term2 term2 term3 term3
    1   5    term3 term5 term5 term5 term6

    >>> get_related_doc(pred_data=pred_df_massive, ref_data=ref_df).collect()
       PREDICT_ID  K DOC_ID     SCORE
    0           1  0   doc2  0.891550
    1           1  1   doc1  0.804670
    2           1  2   doc3  0.042024
    3           1  3   doc4  0.021225
    4           5  0   doc4  0.946186
    5           5  1   doc3  0.943719
    6           5  2   doc6  0.313616
    7           5  3   doc5  0.309858
    8           5  4   doc2  0.063908
    9           5  5   doc1  0.045706

    For SAP HANA On-Premise:

    Invoking the function on a SAP HANA On-Premise instance (only supports single-row mode):

    >>> res = get_related_doc(pred_data=pred_df, ref_data=ref_df)
    >>> res.collect()
       ID    RANK   TOTAL_TERM_COUNT  TERM_COUNT  CORRELATIONS  FACTORS  ROTATED_FACTORS  CLUSTER_LEVEL  CLUSTER_LEFT
    0  doc2     1                  6           3          None     None             None           None          None
    1  doc1     2                  6           3          None     None             None           None          None
    2  doc3     3                  6           3          None     None             None           None          None
    3  doc4     4                  9           3          None     None             None           None          None
    ... CLUSTER_RIGHT  HIGHLIGHTED_DOCUMENT  HIGHLIGHTED_TERMTYPES                                   SCORE
    ...          None                  None                   None    0.8915504731053067732915451415465213
    ...          None                  None                   None    0.8046698732333942283290184604993556
    ...          None                  None                   None   0.04202449735779462125506711345224176
    ...          None                  None                   None   0.02122540837399113089478674964993843

 """
    if pred_data.connection_context.is_cloud_version():
        return _get_basic_func("PAL_TMGETRELATEDDOC", pred_data, ref_data, top, threshold, lang, index_name, thread_ratio, created_index, key)
    else:
        return _get_basic_func("TM_GET_RELATED_DOCUMENTS", pred_data, ref_data, top, threshold, lang, index_name, thread_ratio, created_index)

def get_related_term(pred_data, ref_data=None, top=None, threshold=None, lang='EN', index_name=None, thread_ratio=None, created_index=None, key=None):
    """
    This function returns the top-ranked related terms for a query term / or multiple terms based on Term Frequency - Inverse Document Frequency (TF-IDF) result or reference data.

    Parameters
    ----------
    pred_data : DataFrame

        Accepts input data in two different data structures:

        Single-row mode:

            - 1st column, Document content.

        .. Note ::
            Important to note that this mode can only process one content at a time. Therefore, the input table must have a structure of one row and one column only.

        Massive mode supports multiple rows:

            - 1st column, ID.
            - 2nd column, Document content.

        .. Note ::
            Important to note that this mode can only valid in SAP HANA Cloud instance.

    key : str, optional
        Specifies the ID column. Only valid when ``pred_data`` contains multiple rows.

        Defaults to the first column of ``pred_data``.

    ref_data : DataFrame or a tuple of DataFrames
        Specifies the reference data.

        If ``ref_data`` is a DataFrame, then it should be structured as follows:

            - 1st column, ID.
            - 2nd column, Document content.
            - 3rd column, Document category.

        If ``ref_data`` is a tuple of DataFrames, the it should be corresponding to
        reference TF-IDF data, with each DataFrame structured as follows:

          - 1st DataFrame

            .. only:: html

                - 1st column, TM_TERM.
                - 2nd column, TF_VALUE.
                - 3rd column, IDF_VALUE.


            .. only:: latex

                ============ ===========
                Column Index Content
                ============ ===========
                1            TM_TERM
                2            TF_VALUE
                3            IDF_VALUE
                ============ ===========

          - 2nd DataFrame

            .. only:: html

                - 1st column, ID.
                - 2nd column, TM_TERM.
                - 3rd column, TM_TERM_FREQUENCY.

            .. only:: latex

                ============ =================
                Column Index Content
                ============ =================
                1            ID
                2            TM_TERM
                3            TM_TERM_FREQUENCY
                ============ =================

          - 3rd DataFrame

            .. only:: html

                - 1st column, ID.
                - 2nd column, Document category.

            .. only:: latex

                ============ =================
                Column Index Content
                ============ =================
                1            ID
                2            Document category
                ============ =================

    top : int, optional
        Shows top N results. If 0, it shows all.

        Defaults to 0.

    threshold : float, optional
        Only the results which score bigger than this value will be put into a result table.

        Defaults to 0.0.

    lang : str, optional
        Specifies the language type. The HANA cloud instance currently supports 'EN', 'DE', 'ES', 'FR', 'RU', 'PT'.
        If None, auto detection will be applied.

        Defaults to 'EN'.

    index_name : str, optional
        Specifies the index name that apply only to the HANA On-Premise instance.

        If None, it will be generated.

    thread_ratio : float, optional
        Specifies the ratio of total number of threads that can be used by this function.
        The range of this parameter is from 0 to 1, where 0 means only using 1 thread,
        and 1 means using at most all the currently available threads.
        Values outside this range are ignored and this function heuristically determines the number of threads to use.

        Only valid for a HANA Cloud instance.

        Defaults to 0.0.

    created_index : {"index": xxx, "schema": xxx, "table": xxx}, optional
        Use the created index on the given table that apply only to the HANA On-Premise instance.

        Defaults to None.

    Returns
    -------
    DataFrame

    Examples
    --------

    input ref_df:

    >>> ref_df.collect()
          ID                                                  CONTENT       CATEGORY
    0   doc1                      term1 term2 term2 term3 term3 term3     CATEGORY_1
    1   doc2                      term2 term3 term3 term4 term4 term4     CATEGORY_1
    2   doc3                      term3 term4 term4 term5 term5 term5     CATEGORY_2
    3   doc4    term3 term4 term4 term5 term5 term5 term5 term5 term5     CATEGORY_2
    4   doc5                                              term4 term6     CATEGORY_3
    5   doc6                                  term4 term6 term6 term6     CATEGORY_3

    Invoke the function on a SAP HANA cloud instance:

    1. pred_df in single-row mode:

    >>> pred_df.collect()
      CONTENT
    0   term3

    >>> get_related_term(pred_data=pred_df, ref_data=ref_df).collect()
            ID       SCORE
    0    term3    1.000000
    1    term2    0.923760
    2    term1    0.774597
    3    term4    0.550179
    4    term5    0.346410

    2. pred_df_massive in massive mode which supports multiple rows:

    >>> pred_df_massive.collect()
       ID    CONTENT
    0   1      term3
    1   2     term33
    2   3      term6

    >>> get_related_term(pred_data=pred_df_massive, ref_data=ref_df).collect()
       PREDICT_ID  K   TERM     SCORE
    0           2  0  term2  0.938145
    1           2  1  term3  0.346242
    2           5  0  term6  0.983396
    3           5  1  term3  0.181471

    Invoke the function on a SAP HANA On-Premise instance (only supports single-row mode):

    >>> res = get_related_term(pred_data=pred_df, ref_data=ref_df)
    >>> res.collect()
      RANK  TERM  NORMALIZED_TERM  TERM_TYPE  TERM_FREQUENCY  DOCUMENT_FREQUENCY  CORRELATIONS
    0    1 term3            term3       noun               7                   4          None
    1    2 term2            term2       noun               3                   2          None
    2    3 term1            term1       noun               1                   1          None
    3    4 term4            term4       noun               9                   5          None
    4    5 term5            term5       noun               9                   2          None
    ... FACTORS  ROTATED_FACTORS  CLUSTER_LEVEL  CLUSTER_LEFT  CLUSTER_RIGHT                                 SCORE
    ...    None             None           None          None           None  1.0000003613794823387195265240734440
    ...    None             None           None          None           None  0.9237607645314674931213971831311937
    ...    None             None           None          None           None  0.7745969491648266869177064108953346
    ...    None             None           None          None           None  0.5501794128048571597133786781341769
    ...    None             None           None          None           None  0.3464102866993003515538873671175679
    """

    if pred_data.connection_context.is_cloud_version():
        return _get_basic_func("PAL_TMGETRELATEDTERM", pred_data, ref_data, top, threshold, lang, index_name, thread_ratio, created_index, key)
    else:
        return _get_basic_func("TM_GET_RELATED_TERMS", pred_data, ref_data, top, threshold, lang, index_name, thread_ratio, created_index)

def get_relevant_doc(pred_data, ref_data=None, top=None, threshold=None, lang='EN', index_name=None, thread_ratio=None, created_index=None, key=None):
    """
    This function returns the top-ranked documents that are relevant to a term / or multiple terms based on Term Frequency - Inverse Document Frequency (TF-IDF) result or reference data.

    Parameters
    ----------
    pred_data : DataFrame

        Accepts input data in two different data structures:

        Single-row mode:

            - 1st column, Document content.

        .. Note ::
            Important to note that this mode can only process one content at a time. Therefore, the input table must have a structure of one row and one column only.

        Massive mode supports multiple rows:

            - 1st column, ID.
            - 2nd column, Document content.

        .. Note ::
            Important to note that this mode can only valid in SAP HANA Cloud instance.

    key : str, optional
        Specifies the ID column. Only valid when ``pred_data`` contains multiple rows.

        Defaults to the first column of ``pred_data``.

    ref_data : DataFrame or a tuple of DataFrames
        Specifies the reference data.

        If ``ref_data`` is a DataFrame, then it should be structured as follows:

            - 1st column, ID.
            - 2nd column, Document content.
            - 3rd column, Document category.

        If ``ref_data`` is a tuple of DataFrames, the it should be corresponding to
        reference TF-IDF data, with each DataFrame structured as follows:

          - 1st DataFrame

            .. only:: html

                - 1st column, TM_TERM.
                - 2nd column, TF_VALUE.
                - 3rd column, IDF_VALUE.


            .. only:: latex

                ============ ===========
                Column Index Content
                ============ ===========
                1            TM_TERM
                2            TF_VALUE
                3            IDF_VALUE
                ============ ===========

          - 2nd DataFrame

            .. only:: html

                - 1st column, ID.
                - 2nd column, TM_TERM.
                - 3rd column, TM_TERM_FREQUENCY.

            .. only:: latex

                ============ =================
                Column Index Content
                ============ =================
                1            ID
                2            TM_TERM
                3            TM_TERM_FREQUENCY
                ============ =================

          - 3rd DataFrame

            .. only:: html

                - 1st column, ID.
                - 2nd column, Document category.

            .. only:: latex

                ============ =================
                Column Index Content
                ============ =================
                1            ID
                2            Document category
                ============ =================

    top : int, optional
        Shows top N results. If 0, it shows all.

        Defaults to 0.

    threshold : float, optional
        Only the results which score bigger than this value will be put into a result table.

        Defaults to 0.0.

    lang : str, optional
        Specifies the language type. The HANA cloud instance currently supports 'EN', 'DE', 'ES', 'FR', 'RU', 'PT'.
        If None, auto detection will be applied.

        Defaults to 'EN'.

    index_name : str, optional
        Specifies the index name that apply only to the HANA On-Premise instance.

        If None, it will be generated.

    thread_ratio : float, optional
        Specifies the ratio of total number of threads that can be used by this function.
        The range of this parameter is from 0 to 1, where 0 means only using 1 thread,
        and 1 means using at most all the currently available threads.
        Values outside this range are ignored and this function heuristically determines the number of threads to use.

        Only valid for a HANA Cloud instance.

        Defaults to 0.0.

    created_index : {"index": xxx, "schema": xxx, "table": xxx}, optional
        Use the created index on the given table that apply only to the HANA On-Premise instance.

        Defaults to None.

    Returns
    -------
    DataFrame

    Examples
    --------

    Input DataFrame ref_df:

    >>> ref_df.collect()
          ID                                                  CONTENT       CATEGORY
    0   doc1                      term1 term2 term2 term3 term3 term3     CATEGORY_1
    1   doc2                      term2 term3 term3 term4 term4 term4     CATEGORY_1
    2   doc3                      term3 term4 term4 term5 term5 term5     CATEGORY_2
    3   doc4    term3 term4 term4 term5 term5 term5 term5 term5 term5     CATEGORY_2
    4   doc5                                              term4 term6     CATEGORY_3
    5   doc6                                  term4 term6 term6 term6     CATEGORY_3

    Invoke the function on a SAP HANA cloud instance:

    1. pred_df in single-row mode:

    >>> pred_df.collect()
      CONTENT
    0   term3

    >>> get_relevant_doc(pred_data=pred_df, ref_data=ref_df).collect()
           ID       SCORE
    0    doc1    0.774597
    1    doc2    0.516398
    2    doc3    0.258199
    3    doc4    0.258199

    2. pred_df_massive in massive mode which supports multiple rows:

    >>> pred_df_massive.collect()
       ID   CONTENT
    0   2     term2
    1   3    term33
    2   5     term5

    >>> get_relevant_doc(pred_data=pred_df_massive, ref_data=ref_df).collect()
       PREDICT_ID  K DOC_ID     SCORE
    0           2  0   doc1  0.894427
    1           2  1   doc2  0.447214
    2           5  0   doc4  0.894427
    3           5  1   doc3  0.447214

    Invoke the function on a SAP HANA On-Premise instance (only supports single-row mode):

    >>> res = get_relevant_doc(pred_data=pred_df, ref_data=ref_df, top=4)
    >>> res.collect()
         ID    RANK   TOTAL_TERM_COUNT  TERM_COUNT  CORRELATIONS  FACTORS  ROTATED_FACTORS  CLUSTER_LEVEL  CLUSTER_LEFT
    0  doc1       1                  6           3          None     None             None           None          None
    1  doc2       2                  6           3          None     None             None           None          None
    2  doc3       3                  6           3          None     None             None           None          None
    3  doc4       4                  9           3          None     None             None           None          None
    ... CLUSTER_RIGHT  HIGHLIGHTED_DOCUMENT  HIGHLIGHTED_TERMTYPES                                   SCORE
    ...          None                  None                   None    0.7745969491648266869177064108953346
    ...          None                  None                   None    0.5163979661098845319600059156073257
    ...          None                  None                   None    0.2581989830549422659800029578036629
    ...          None                  None                   None    0.2581989830549422659800029578036629

    """
    if pred_data.connection_context.is_cloud_version():
        return _get_basic_func("PAL_TMGETRELEVANTDOC", pred_data, ref_data, top, threshold, lang, index_name, thread_ratio, created_index, key)
    else:
        return _get_basic_func("TM_GET_RELEVANT_DOCUMENTS", pred_data, ref_data, top, threshold, lang, index_name, thread_ratio, created_index)

def get_relevant_term(pred_data, ref_data=None, top=None, threshold=None, lang='EN', index_name=None, thread_ratio=None, created_index=None, key=None):
    """
    This function returns the top-ranked relevant terms that describe a document / or multiple docments based on Term Frequency - Inverse Document Frequency (TF-IDF) result or reference data.

    Parameters
    ----------
    pred_data : DataFrame

        Accepts input data in two different data structures:

        Single-row mode:

            - 1st column, Document content.

        .. Note ::
            Important to note that this mode can only process one content at a time. Therefore, the input table must have a structure of one row and one column only.

        Massive mode supports multiple rows:

            - 1st column, ID.
            - 2nd column, Document content.

        .. Note ::
            Important to note that this mode can only valid in SAP HANA Cloud instance.

    key : str, optional
        Specifies the ID column. Only valid when ``pred_data`` contains multiple rows.

        Defaults to the first column of ``pred_data``.

    ref_data : DataFrame or a tuple of DataFrames
        Specifies the reference data.

        If ``ref_data`` is a DataFrame, then it should be structured as follows:

            - 1st column, ID.
            - 2nd column, Document content.
            - 3rd column, Document category.

        If ``ref_data`` is a tuple of DataFrames, the it should be corresponding to
        reference TF-IDF data, with each DataFrame structured as follows:

          - 1st DataFrame

            .. only:: html

                - 1st column, TM_TERM.
                - 2nd column, TF_VALUE.
                - 3rd column, IDF_VALUE.


            .. only:: latex

                ============ ===========
                Column Index Content
                ============ ===========
                1            TM_TERM
                2            TF_VALUE
                3            IDF_VALUE
                ============ ===========

          - 2nd DataFrame

            .. only:: html

                - 1st column, ID.
                - 2nd column, TM_TERM.
                - 3rd column, TM_TERM_FREQUENCY.

            .. only:: latex

                ============ =================
                Column Index Content
                ============ =================
                1            ID
                2            TM_TERM
                3            TM_TERM_FREQUENCY
                ============ =================

          - 3rd DataFrame

            .. only:: html

                - 1st column, ID.
                - 2nd column, Document category.

            .. only:: latex

                ============ =================
                Column Index Content
                ============ =================
                1            ID
                2            Document category
                ============ =================


    top : int, optional
        Shows top N results. If 0, it shows all.

        Defaults to 0.

    threshold : float, optional
        Only the results which score bigger than this value will be put into a result table.

        Defaults to 0.0.

    lang : str, optional
        Specifies the language type. The HANA Cloud instance currently supports 'EN', 'DE', 'ES', 'FR', 'RU', 'PT'.
        If None, auto detection will be applied.

        Defaults to 'EN'.

    index_name : str, optional
        Specifies the index name that apply only to the HANA On-Premise instance.

        If None, it will be generated.

    thread_ratio : float, optional
        Specifies the ratio of total number of threads that can be used by this function.
        The range of this parameter is from 0 to 1, where 0 means only using 1 thread,
        and 1 means using at most all the currently available threads.
        Values outside this range are ignored and this function heuristically determines the number of threads to use.

        Only valid for a HANA Cloud instance.

        Defaults to 0.0.

    created_index : {"index": xxx, "schema": xxx, "table": xxx}, optional
        Use the created index on the given table that apply only to the HANA On-Premise instance.

        Defaults to None.

    Returns
    -------
    DataFrame

    Examples
    --------

    Input DataFrame ref_df:

    >>> ref_df.collect()
          ID                                                  CONTENT       CATEGORY
    0   doc1                      term1 term2 term2 term3 term3 term3     CATEGORY_1
    1   doc2                      term2 term3 term3 term4 term4 term4     CATEGORY_1
    2   doc3                      term3 term4 term4 term5 term5 term5     CATEGORY_2
    3   doc4    term3 term4 term4 term5 term5 term5 term5 term5 term5     CATEGORY_2
    4   doc5                                              term4 term6     CATEGORY_3
    5   doc6                                  term4 term6 term6 term6     CATEGORY_3

    Invoke the function on a SAP HANA cloud instance:

    1. pred_df in single-row mode:

    >>> pred_df.collect()
      CONTENT
    0   term3

    >>> get_relevant_term(pred_data=pred_df, ref_data=ref_df).collect()
            ID   SCORE
    0    term3     1.0

    2. pred_df_massive in massive mode which supports multiple rows:

    >>> pred_df_massive.collect()
       ID                      CONTENT
    0   2      term2 term2 term3 term3
    1   5     term6 term6 term33 term3

    >>> get_relevant_term(pred_data=pred_df_massive, ref_data=ref_df).collect()
       PREDICT_ID  K   TERM     SCORE
    0           2  0  term2  0.938145
    1           2  1  term3  0.346242
    2           5  0  term6  0.983396
    3           5  1  term3  0.181471

    Invoke the function on a SAP HANA On-Premise instance (only supports single-row mode):

    >>> res = get_relevant_term(pred_data=pred_df, ref_data=ref_df)
    >>> res.collect()
      RANK  TERM  NORMALIZED_TERM  TERM_TYPE  TERM_FREQUENCY  DOCUMENT_FREQUENCY  CORRELATIONS
    0    1 term3            term3       noun               7                   4          None
    ... FACTORS  ROTATED_FACTORS  CLUSTER_LEVEL  CLUSTER_LEFT  CLUSTER_RIGHT                                 SCORE
    ...    None             None           None          None           None   1.000002901113076436701021521002986

    """
    if pred_data.connection_context.is_cloud_version():
        return _get_basic_func("PAL_TMGETRELEVANTTERM", pred_data, ref_data, top, threshold, lang, index_name, thread_ratio, created_index, key)
    else:
        return _get_basic_func("TM_GET_RELEVANT_TERMS", pred_data, ref_data, top, threshold, lang, index_name, thread_ratio, created_index)

def get_suggested_term(pred_data, ref_data=None, top=None, threshold=None, lang='EN', index_name=None, thread_ratio=None, created_index=None, key=None):
    """
    This function returns the top-ranked terms that match an initial substring / or multiple substrings based on Term Frequency - Inverse Document Frequency (TF-IDF) result or reference data.

    Parameters
    ----------
    pred_data : DataFrame

        Accepts input data in two different data structures:

        Single-row mode:

            - 1st column, Document content.

        .. Note ::
            Important to note that this mode can only process one content at a time. Therefore, the input table must have a structure of one row and one column only.

        Massive mode supports multiple rows:

            - 1st column, ID.
            - 2nd column, Document content.

        .. Note ::
            Important to note that this mode can only valid in SAP HANA Cloud instance.

    key : str, optional
        Specifies the ID column. Only valid when ``pred_data`` contains multiple rows.

        Defaults to the first column of ``pred_data``.

    ref_data : Da0taFrame or a tuple of DataFrames
        Specifies the reference data.

        If ``ref_data`` is a DataFrame, then it should be structured as follows:

            - 1st column, ID.
            - 2nd column, Document content.
            - 3rd column, Document category.

        If ``ref_data`` is a tuple of DataFrames, the it should be corresponding to
        reference TF-IDF data, with each DataFrame structured as follows:

          - 1st DataFrame

            .. only:: html

                - 1st column, TM_TERM.
                - 2nd column, TF_VALUE.
                - 3rd column, IDF_VALUE.


            .. only:: latex

                ============ ===========
                Column Index Content
                ============ ===========
                1            TM_TERM
                2            TF_VALUE
                3            IDF_VALUE
                ============ ===========

          - 2nd DataFrame

            .. only:: html

                - 1st column, ID.
                - 2nd column, TM_TERM.
                - 3rd column, TM_TERM_FREQUENCY.

            .. only:: latex

                ============ =================
                Column Index Content
                ============ =================
                1            ID
                2            TM_TERM
                3            TM_TERM_FREQUENCY
                ============ =================

          - 3rd DataFrame

            .. only:: html

                - 1st column, ID.
                - 2nd column, Document category.

            .. only:: latex

                ============ =================
                Column Index Content
                ============ =================
                1            ID
                2            Document category
                ============ =================


    top : int, optional
        Shows top N results. If 0, it shows all.

        Defaults to 0.

    threshold : float, optional
        Only the results which score bigger than this value will be put into a result table.

        Defaults to 0.0.

    lang : str, optional
        Specifies the language type. The HANA Cloud instance currently supports 'EN', 'DE', 'ES', 'FR', 'RU', 'PT'.
        If None, auto detection will be applied.

        Defaults to 'EN' in HANA Cloud and None in HANA On-Premise (please provide the value in this case).

    index_name : str, optional
        Specifies the index name that apply only to the HANA On-Premise instance.

        If None, it will be generated.

    thread_ratio : float, optional
        Specifies the ratio of total number of threads that can be used by this function.
        The range of this parameter is from 0 to 1, where 0 means only using 1 thread,
        and 1 means using at most all the currently available threads.
        Values outside this range are ignored and this function heuristically determines the number of threads to use.

        Only valid for a HANA Cloud instance.

        Defaults to 0.0.

    created_index : {"index": xxx, "schema": xxx, "table": xxx}, optional
        Use the created index on the given table that apply only to the HANA On-Premise instance.

        Defaults to None.

    Returns
    -------
    DataFrame

    Examples
    --------

    Input DataFrame ref_df:

    >>> ref_df.collect()
          ID                                                  CONTENT       CATEGORY
    0   doc1                      term1 term2 term2 term3 term3 term3     CATEGORY_1
    1   doc2                      term2 term3 term3 term4 term4 term4     CATEGORY_1
    2   doc3                      term3 term4 term4 term5 term5 term5     CATEGORY_2
    3   doc4    term3 term4 term4 term5 term5 term5 term5 term5 term5     CATEGORY_2
    4   doc5                                              term4 term6     CATEGORY_3
    5   doc6                                  term4 term6 term6 term6     CATEGORY_3

    Invoke the function on a SAP HANA Cloud instance:

    1. pred_df in single-row mode:

    >>> pred_df.collect()
      CONTENT
    0   term3

    Invoke the function on a SAP HANA Cloud instance,

    >>> get_suggested_term(pred_data=pred_df, ref_data=ref_df).collect()
            ID     SCORE
    0    term3       1.0

    2. pred_df_massive in massive mode which supports multiple rows:

    >>> pred_df_massive.collect()
       ID CONTENT
    0   2     ter
    1   3     abc

    >>> get_suggested_term(pred_data=pred_df_massive, ref_data=ref_df).collect()
       PREDICT_ID  K   TERM     SCORE
    0           2  0  term5  0.830048
    1           2  1  term6  0.368910
    2           2  2  term2  0.276683
    3           2  3  term3  0.238269
    4           2  4  term1  0.150417
    5           2  5  term4  0.137752

    Invoke the function on a SAP HANA On-Premise instance (only supports single-row mode):

    >>> res = get_suggested_term(pred_data=pred_df, ref_data=ref_df)
    >>> res.collect()
      RANK   TERM  NORMALIZED_TERM  TERM_TYPE  TERM_FREQUENCY  DOCUMENT_FREQUENCY                                SCORE
    0    1  term3            term3       noun               7                   4  0.999999999999999888977697537484346
    """
    if pred_data.connection_context.is_cloud_version():
        return _get_basic_func("PAL_TMGETSUGGESTEDTERM", pred_data, ref_data, top, threshold, lang, index_name, thread_ratio, created_index, key)
    else:
        return _get_basic_func("TM_GET_SUGGESTED_TERMS", pred_data, ref_data, top, threshold, lang, index_name, thread_ratio, created_index)

class TFIDF(PALBase):  # pylint: disable=too-many-instance-attributes, too-few-public-methods
    r"""
    Class for term frequency–inverse document frequency.

    Parameters
    ----------
    language : str, {'en', 'de', 'es', 'fr', 'ru', 'pt'}
        Specify the language type. HANA Cloud instance currently supports
        'EN', 'DE', 'ES', 'FR', 'RU', 'PT'.
        If None, auto detection will be applied.

        Defaults to None (auto detection).

    enable_stopwords : bool, optional
        Determine whether to turn on stopwords.

        Defaults to True.

    keep_numeric : bool, optional
        Determine whether to keep numbers.

        Valid only when ``enable_stopwords`` is True.

        Defaults to False.

    allowed_list : bool, optional
        A list of words that are retained by the stopwords logic.

        Valid only when ``enable_stopwords`` is True.

    notallowed_list : bool, optional
        A list of words, which are recognized and deleted
        by the stopwords logic.

        Valid only when ``enable_stopwords`` is True.

    Examples
    --------
    Input DataFrame:

    >>> df_train.collect()
        ID      CONTENT
    0   doc1    term1 term2 term2 term3 term3 term3
    1   doc2    term2 term3 term3 term4 term4 term4
    ...
    4   doc4    term4 term6
    5   doc6    term4 term6 term6 term6

    Creating a TFIDF instance:

    >>> tfidf = TFIDF()

    Performing text_collector():

    >>> idf, _ = tfidf.text_collector(data=df_train)

    >>> idf.collect()
            TM_TERMS    TM_TERM_IDF_VALUE
        0   term1       1.791759
        1   term2       1.098612
        2   term3       0.405465
        3   term4       0.182322
        4   term5       1.098612
        5   term6       1.098612

    Performing text_tfidf():

    >>> result = tfidf.text_tfidf(data=df_train)

    >>> result.collect()
            ID      TERMS   TF_VALUE    TFIDF_VALUE
        0   doc1    term1   1.0         1.791759
        1   doc1    term2   2.0         2.197225
        2   doc1    term3   3.0         1.216395
        ...
        13  doc4    term6   1.0         1.098612
        14  doc6    term4   1.0         0.182322
        15  doc6    term6   3.0         3.295837
    """
    lang_list = ['en', 'de', 'es', 'fr', 'ru', 'pt']
    def __init__(self,
                 language=None,
                 enable_stopwords=True,
                 keep_numeric=None,
                 allowed_list=None,
                 notallowed_list=None):
        super(TFIDF, self).__init__()
        self.language = self._arg('language', language, {lang:lang for lang in self.lang_list})
        self.enable_stopwords = self._arg('enable_stopwords', enable_stopwords, bool)
        self.keep_numeric = self._arg('keep_numeric', keep_numeric, bool)
        self.allowed_list = (self._arg('allowed_list', allowed_list, list))
        self.notallowed_list = self._arg('notallowed_list', notallowed_list, list)
        if self.allowed_list:
            self.allowed_list = ", ".join(list(self.allowed_list))
        if self.notallowed_list:
            self.notallowed_list = ", ".join(list(self.notallowed_list))
        self.idf = None
        self.extend = None

    def text_collector(self, data):
        """
        Its use is primarily compute inverse document frequency of documents which provided by user.

        Parameters
        ----------
        data : DataFrame
            Data to be analysis.
            The first column of the input data table is assumed to be an ID column.

        Returns
        -------
        DataFrame
            - Inverse document frequency of documents.
            - Extended table.
        """
        conn = data.connection_context
        require_pal_usable(conn)
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        tables = ['TERM-IDF', 'EXTEND_OUT']
        tables = ["#PAL_IDF_{}_TBL_{}_{}".format(
            tbl, self.id, unique_id) for tbl in tables]
        idf_tbl, extend_tbl = tables
        param_rows = [('LANGUAGE', None, None, self.language.upper() if self.language is not None else None),
                      ('ENABLE_STOPWORDS', None, None,
                       str(self.enable_stopwords) if self.enable_stopwords is not None else None),
                      ('KEEP_NUMERIC', None, None, 'True' if self.keep_numeric else 'False'),
                      ('ALLOWED_LIST', None, None, self.allowed_list),
                      ('NOTALLOWED_LIST', None, None, self.notallowed_list)]
        try:
            self._call_pal_auto(conn,
                                "PAL_TEXT_COLLECT",
                                data,
                                ParameterTable().with_data(param_rows),
                                *tables)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, tables)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, tables)
            raise
        self.idf = conn.table(idf_tbl)
        return conn.table(idf_tbl), conn.table(extend_tbl)

    def text_tfidf(self, data, idf=None):
        """
        Its use is primarily compute term frequency - inverse document frequency by document.

        Parameters
        ----------
        data : DataFrame
            Data to be analysis.

            The first column of the input data table is assumed to be an ID column.
        idf : DataFrame, optional
            Inverse document frequency of documents.

        Returns
        -------
        DataFrame
            - Term frequency - inverse document frequency by document.
        """
        if not idf:
            if not self.idf:
                msg = "text_collector() has not been excucated."
                logger.error(msg)
                raise ValueError(msg)
            idf = self.idf
        conn = data.connection_context
        require_pal_usable(conn)
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        result_tbl = "#PAL_TFIDF_RESULT_{}_{}".format(self.id, unique_id)
        param_rows = [('LANGUAGE', None, None, self.language.upper() if self.language is not None else None),
                      ('ENABLE_STOPWORDS', None, None,
                       str(self.enable_stopwords) if self.enable_stopwords is not None else None),
                      ('KEEP_NUMERIC', None, None, 'True' if self.keep_numeric else 'False'),
                      ('ALLOWED_LIST', None, None, self.allowed_list),
                      ('NOTALLOWED_LIST', None, None, self.notallowed_list)]
        try:
            self._call_pal_auto(conn,
                                "PAL_TEXT_TFIDF",
                                data,
                                idf,
                                ParameterTable().with_data(param_rows),
                                result_tbl)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, result_tbl)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, result_tbl)
            raise
        return conn.table(result_tbl)
