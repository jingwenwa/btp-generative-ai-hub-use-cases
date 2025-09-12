#pylint:disable=too-many-lines, relative-beyond-top-level
#pylint:disable=consider-using-f-string
'''
This module contains PAL wrappers for weighted_score_table algorithm.

The following functions is available:

    * :func:`weighted_score_table`
'''
import logging
import uuid
from hdbcli import dbapi
from .pal_base import (
    ParameterTable,
    arg,
    try_drop,
    ListOfStrings,
    require_pal_usable,
    call_pal_auto_with_hint
)
logger = logging.getLogger(__name__)#pylint: disable=invalid-name

def weighted_score_table(data, maps, weights, key, features=None, thread_ratio=None):#pylint: disable=too-many-arguments
    """
    A weighted score table is a method of evaluating alternatives when the importance of each criterion differs.
    In a weighted score table, each alternative is given a score for each criterion. These scores are then weighted by the importance of each criterion.
    All of an alternative's weighted scores are then added together to calculate its total weighted score. The alternative with the highest total score should be the best alternative.
    You can use weighted score tables to make predictions about future customer behavior. You first create a model based on historical data in the data mining application,
    and then apply the model to new data to make the prediction. The prediction, which is the output of the model, is called a score.
    You can create a single score for your customers by taking into account different dimensions.

    Parameters
    ----------

    data : DataFrame
        Input data.

    maps : DataFrame
        Every attribute (except ID) in the input data table maps to two columns
        in the map Function table: Key column and Value column.

        The Value column must be of DOUBLE type.

    weights : DataFrame
        This table has three columns.

        When the data table has n attributes (except ID), the weights table will have n rows.

    key : str
        Name of the ID column.

    features : str or a list of str, optional
        Name of the feature columns.

        If not given, the feature columns should be all columns in the DataFrame
        except the ID column.

    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Default to 0.

    Returns
    -------
    DataFrame
        The result value of weight for each score.

    Examples
    --------
    Input DataFrame df_train, df_map, df_weight:

    >>> df_train.collect()
        ID   GENDER    INCOME   HEIGHT
    0   0    male      5000     1.73
    1   1    male      9000     1.80
    ...
    9   9    female    9500     1.85

    >>> df_map.collect()
        GENDER  VAL1   INCOME   VAL2   HEIGHT  VAL3
    0     male   2.0        0    0.0      1.5   0.0
    1   female   1.5     5500    1.0      1.6   1.0
    2     None   0.0     9000    2.0     1.71   2.0
    3     None   0.0    12000    3.0     1.80   3.0

    >>> df_weight.collect()
        WEIGHT  ISDIS   ROWNUM
    0      0.5      1        2
    1      2.0     -1        4
    2      1.0     -1        4

    Perform weighted_score_table():

    >>> res = weighted_score_table(data=df_train, maps=df_map,
                                   weights=df_weight,
                                   key='ID', thread_ratio=0.3)
    >>> res.collect()
       ID  SCORE
    0   0   3.00
    1   1   8.00
    ...
    9   9   7.75
    """
    conn = data.connection_context
    require_pal_usable(conn)
    key = arg('key', key, str)
    data_ = data
    if features is not None:
        if isinstance(features, str):
            features = [features]
        try:
            features = arg('features', features, ListOfStrings)#pylint: disable=undefined-variable
            data_ = data[[key] + features]
        except:
            msg = "'features' must be list of string or string."
            logger.error(msg)
            raise TypeError(msg)

    thread_ratio = arg('thread_ratio', thread_ratio, float)
    param_rows = [('THREAD_RATIO', None, thread_ratio, None)]
    unique_id = str(uuid.uuid1()).replace('-', '_').upper()
    result_tbl = "#WEIGHTED_TABLE_RESULT_{}".format(unique_id)
    try:
        call_pal_auto_with_hint(conn,
                                None,
                                'PAL_WEIGHTED_TABLE',
                                data_, maps, weights,
                                ParameterTable().with_data(param_rows),
                                result_tbl)
    except dbapi.Error as db_err:
        logger.exception(str(db_err))
        try_drop(conn, result_tbl)#pylint: disable=no-value-for-parameter
        raise
    except Exception as db_err:
        logger.exception(str(db_err))
        try_drop(conn, result_tbl)#pylint: disable=no-value-for-parameter
        raise
    return conn.table(result_tbl)
