#pylint:disable=line-too-long, too-many-arguments, too-many-instance-attributes
#pylint:disable=invalid-name, too-few-public-methods, too-many-statements, too-many-locals
#pylint:disable=too-many-branches, c-extension-no-member, consider-using-generator
'''
This module wrapped the PAL text analysis (integeration of NER, POS tagging and sentiment analysis)
as well as its sub-functionalities.

    * :func:`text_analysis`
    * :func:`pos_tag`
    * :func:`named_entity_recognition`
    * :func:`sentiment_analysis`

'''
import logging
import uuid
from hdbcli import dbapi
from hana_ml.ml_base import arg
from hana_ml.algorithms.pal.pal_base import (
    ParameterTable,
    try_drop,
    require_pal_usable,
    call_pal_auto_with_hint
)
logger = logging.getLogger(__name__)

def text_analysis(data, thread_ratio=None, timeout=None):
    r"""
    Text analysis function, can perform the task of POS (Part-of-Speech), NER (Named-Entity-Recognition)
    and sentiment-phrase-score.

    Parameters
    ----------
    data : DataFrame
        The input data for text analysis, must be a 4-column DataFrame structured as follows:

        - 1st column : ID of input text, of type INT, VARCHAR if NVARCHAR
        - 2nd column : Text content, of type VARCHAR, NVARCHAR or NCLOB
        - 3rd column : Specifies the language of the text content, can be 'en', 'de', 'fr', 'es', 'pt' or empty
          (means automatically detected)
        - 4th column : Specifies the task, which can be 'pos', 'ner', 'sentiment-phrase-score' or a combination
          of them (separated by comma, e.g. 'pos, sentiment-phrase-score').

    thread_ratio : float, optional
        Specifies the ratio of threads that can be used by this function, with valid range from 0 to 1, where

        - 0 means only using a single thread
        - 1 means using at most all the currently available threads.

        Values outside valid range are ignored (no error thrown), and in such case
        the function heuristically determines the number of threads to use.

        Defaults to 0.0.
    timeout : int, optional
        Specifies the maximum amount of time (in seconds) the client will wait for a response from the server.

        Defaults to 10.

    Examples
    --------
    >>> sentences, pos, ner, doc_sentiment,  sentence_sentiment, phrase_sentiment, extra = text_analysis(data=df, thread_ratio=0.5, timeout=20)

    Returns
    -------
    A tuple of DataFrames:

        - DataFrame 1 : Sentences result table
        - DataFrame 2 : POS result table
        - DataFrame 3 : NER result table
        - DataFrame 4 : Documents sentiment result table
        - DataFrame 5 : Sentences sentiment result table
        - DataFrame 6 : Phrases sentiment result table
        - DataFrame 7 : Extra result table
    """
    conn = data.connection_context
    require_pal_usable(conn)
    unique_id = str(uuid.uuid1()).replace('-', '_').upper()
    outputs = ['sentences', 'pos', 'ner', 'doc_sentiment',  'sentences_sentiment',
               'phrases_sentiment', 'extra']
    outputs = [f'#PAL_TEXT_ANALYSIS_{x.upper()}_RESULT_TBL_{unique_id}' for x in outputs]
    thread_ratio = arg('thread_ratio', thread_ratio, float)
    timeout = arg('timeout', timeout, int)
    param_rows = [('THREAD_RATIO', None, thread_ratio, None),
                  ('TIMEOUT', timeout, None, None)]
    try:
        call_pal_auto_with_hint(conn,
                                None,
                                'PAL_TEXT_ANALYSIS',
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
    sentences_result_tbl = conn.table(outputs[0])
    pos_result_tbl = conn.table(outputs[1])
    pos_result_tbl = pos_result_tbl.rename_columns({'TOKEN' : 'ENTITY', 'ENTITY':'CATEGORY'})
    ner_result_tbl = conn.table(outputs[2])
    ner_result_tbl = ner_result_tbl.rename_columns({'TOKEN' : 'ENTITY', 'ENTITY':'CATEGORY'})
    doc_sentiment_result_tbl = conn.table(outputs[3])
    sentences_sentiment_result_tbl = conn.table(outputs[4])
    phrases_sentiment_result_tbl = conn.table(outputs[5])
    phrases_sentiment_result_tbl = phrases_sentiment_result_tbl.rename_columns({'TEXT':'PHRASE', 'TOKEN' : 'ENTITY', 'ENTITY':'CATEGORY'})
    extra_result_tbl = conn.table(outputs[6])
    extra_result_tbl = extra_result_tbl.rename_columns({'TEXT':'CONTENT'})
    return sentences_result_tbl, pos_result_tbl, ner_result_tbl, doc_sentiment_result_tbl, sentences_sentiment_result_tbl, phrases_sentiment_result_tbl, extra_result_tbl

def pos_tag(data, lang=None, thread_ratio=None, timeout=None):
    r"""
    Part of Speech (POS) tagging is a natural language processing technique that involves assigning specific
    grammatical categories or labels (such as nouns, verbs, adjectives, adverbs, pronouns, etc.) to individual words within a sentence.
    This process provides insights into the syntactic structure of the text, aiding in understanding word relationships,
    disambiguating word meanings, and facilitating various linguistic and computational analyses of textual data.

    data : DataFrame
        The input data for text analysis, should be a DataFrame structured as follows:

        - 1st column : ID of input text, of type INT, VARCHAR if NVARCHAR
        - 2nd column : Text content, of type VARCHAR, NVARCHAR or NCLOB
        - 3rd column (optional) : Specifies the language of the text content, can be 'en', 'de', 'fr', 'es', 'pt' or NULL
          (means automatically detected).

    lang : {'en', 'de', 'fr', 'es', 'pt'}, optional
        Specifies the language of the input texts in ``data``.

        Effective only when the language column in ``data`` is not provided  (i.e. ``data`` has two columns).

    thread_ratio : float, optional
        Specifies the ratio of threads that can be used by this function, with valid range from 0 to 1, where

        - 0 means only using a single thread.
        - 1 means using at most all the currently available threads.

        Values outside valid range are ignored (no error thrown), and in such case
        the function heuristically determines the number of threads to use.

        Defaults to 0.0.
    timeout : int, optional
        Specifies the maximum amount of time (in seconds) the client will wait for a response from the server.

        Defaults to 10.

    Examples
    --------
    >>> pos, sentences, extra = pos_tag(data=df, thread_ratio=0.5, timeout=20)

    Returns
    -------
    A tuple of DataFrames:

        - DataFrame 1 : The POS result table
        - DataFrame 2 : Sentences result table
        - DataFrame 3 : Extra result table
    """
    conn = data.connection_context
    require_pal_usable(conn)
    cols = data.columns
    lang_sets = ['en', 'de', 'fr', 'es', 'pt']
    lang = arg('lang', lang, {lng:lng for lng in lang_sets})
    if len(cols) == 2:
        lang = 'NULL' if lang is None else f'\'{lang}\''
        aug_data = conn.sql(f'SELECT *, {lang} AS TEXT_LANGUAGE, \'pos\' AS TA_TASK FROM ({data.select_statement})')
    elif len(cols) == 3:
        aug_data = conn.sql(f'SELECT *, \'pos\' AS TA_TASK FROM ({data.select_statement})')
    else:
        msg = 'Input data should have either 2 or 3 columns, while it is detected that ' +\
              f'current data has {len(cols)} columns.'
        raise ValueError(msg)
    result_dfs = text_analysis(data=aug_data, thread_ratio=thread_ratio, timeout=timeout)
    pos_result_tbl = result_dfs[1]
    sentences_result_tbl = result_dfs[0]
    extra_result_tbl = result_dfs[6]
    return pos_result_tbl, sentences_result_tbl, extra_result_tbl

def named_entity_recognition(data, lang=None, thread_ratio=None, timeout=None):
    r"""
    This is a wrapper of named entity recognition (NER) functionality for text analysis, which aims at
    facilitating users' use of text analysis targeted specially for named entity recognition.

    data : DataFrame
        The input data for text analysis, should be a DataFrame structured as follows:

        - 1st column : ID of input text, of type INT, VARCHAR if NVARCHAR
        - 2nd column : Text content, of type VARCHAR, NVARCHAR or NCLOB
        - 3rd column (optional) : Specifies the language of the text content, can be 'en', 'de', 'fr', 'es', 'pt' or NULL
          (means automatically detected).
    lang : {'en', 'de', 'fr', 'es', 'pt'}, optional
        Specifies the language of the input texts in ``data``.

        Effective only when the language column in ``data`` is not provided  (i.e. ``data`` has two columns).

    thread_ratio : float, optional
        Specifies the ratio of threads that can be used by this function, with valid range from 0 to 1, where

        - 0 means only using a single thread.
        - 1 means using at most all the currently available threads.

        Values outside valid range are ignored (no error thrown), and in such case
        the function heuristically determines the number of threads to use.

        Defaults to 0.0.
    timeout : int, optional
        Specifies the maximum amount of time (in seconds) the client will wait for a response from the server.

        Defaults to 10.

    Examples
    --------
    >>> ner, sentences, extra = named_entity_recognition(data=df, thread_ratio=0.5, timeout=20)

    Returns
    -------
    A tuple of DataFrames:

        - DataFrame 1 : The NER results for input texts
        - DataFrame 2 : Sentences result table
        - DataFrame 3 : Extra result table
    """
    conn = data.connection_context
    require_pal_usable(conn)
    cols = data.columns
    lang_sets = ['en', 'de', 'fr', 'es', 'pt']
    lang = arg('lang', lang, {lng:lng for lng in lang_sets})
    if len(cols) == 2:
        lang = 'NULL' if lang is None else f'\'{lang}\''
        aug_data = conn.sql(f'SELECT *, {lang} AS TEXT_LANGUAGE, \'ner\' AS TA_TASK FROM ({data.select_statement})')
    elif len(cols) == 3:
        aug_data = conn.sql(f'SELECT *, \'ner\' AS TA_TASK FROM ({data.select_statement})')
    else:
        msg = 'Input data should have either 2 or 3 columns, while it is detected that ' +\
              f'current data has {len(cols)} columns.'
        raise ValueError(msg)
    result_dfs = text_analysis(data=aug_data, thread_ratio=thread_ratio, timeout=timeout)
    ner_result_tbl = result_dfs[2]
    sentences_result_tbl = result_dfs[0]
    extra_result_tbl = result_dfs[6]
    return ner_result_tbl, sentences_result_tbl, extra_result_tbl

def sentiment_analysis(data, lang=None, thread_ratio=None, timeout=None):
    r"""
    A sentiment score, often referred to as a sentiment analysis score,
    is a numerical representation of the sentiment or emotion conveyed in a piece of text, be it a tweet,
    a product review, or an article. It provides insight into whether the expressed sentiment is positive, negative, or neutral.
    Understanding sentiment scores is essential for businesses, marketers, and data scientists,
    as it helps them make data-driven decisions and gain valuable insights. This task output doc, sentence, and word level sentiment.

    data : DataFrame
        The input data for text analysis, should be a DataFrame structured as follows:

        - 1st column : ID of input text, of type INT, VARCHAR if NVARCHAR
        - 2nd column : Text content, of type VARCHAR, NVARCHAR or NCLOB
        - 3rd column (optional) : Specifies the language of the text content, can be 'en', 'de', 'fr', 'es', 'pt' or NULL
          (means automatically detected).
    lang : {'en', 'de', 'fr', 'es', 'pt'}, optional
        Specifies the language of the input texts in ``data``.

        Effective only when the language column in ``data`` is not provided  (i.e. ``data`` has two columns).

    thread_ratio : float, optional
        Specifies the ratio of threads that can be used by this function, with valid range from 0 to 1, where

        - 0 means only using a single thread.
        - 1 means using at most all the currently available threads.

        Values outside valid range are ignored (no error thrown), and in such case
        the function heuristically determines the number of threads to use.

        Defaults to 0.0.
    timeout : int, optional
        Specifies the maximum amount of time (in seconds) the client will wait for a response from the server.

        Defaults to 10.

    Examples
    --------
    >>> doc_sentiment, sentence_sentiment, phrase_sentiment, sentences, extra = sentiment_analysis(data=df, thread_ratio=0.5, timeout=20)


    Returns
    -------
    A tuple of DataFrames:

        - DataFrame 1 : Documents sentiment result table
        - DataFrame 2 : Sentences sentiment result table
        - DataFrame 3 : Phrases sentiment result table
        - DataFrame 4 : Sentences result table
        - DataFrame 5 : Extra result table
    """
    conn = data.connection_context
    require_pal_usable(conn)
    cols = data.columns
    lang_sets = ['en', 'de', 'fr', 'es', 'pt']
    lang = arg('lang', lang, {lng:lng for lng in lang_sets})
    if len(cols) == 2:
        lang = 'NULL' if lang is None else f'\'{lang}\''
        aug_data = conn.sql(f'SELECT *, {lang} AS TEXT_LANGUAGE, \'sentiment-phrase-score\' AS TA_TASK FROM ({data.select_statement})')
    elif len(cols) == 3:
        aug_data = conn.sql(f'SELECT *, \'sentiment-phrase-score\' AS TA_TASK FROM ({data.select_statement})')
    else:
        msg = 'Input data should have either 2 or 3 columns, while it is detected that ' +\
              f'current data has {len(cols)} columns.'
        raise ValueError(msg)
    result_dfs = text_analysis(data=aug_data, thread_ratio=thread_ratio, timeout=timeout)
    doc_sentiment_result_tbl = result_dfs[3]
    sentences_sentiment_result_tbl = result_dfs[4]
    phrases_sentiment_result_tbl = result_dfs[5]
    sentences_result_tbl = result_dfs[0]
    extra_result_tbl = result_dfs[6]
    return doc_sentiment_result_tbl, sentences_sentiment_result_tbl, phrases_sentiment_result_tbl, sentences_result_tbl, extra_result_tbl
