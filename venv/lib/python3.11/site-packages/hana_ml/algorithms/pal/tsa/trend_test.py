"""
This module contains Python wrapper for PAL trend test algorithm.

The following function is available:

    * :func:`trend_test`
"""
#pylint: disable=line-too-long, too-many-arguments, too-few-public-methods
#pylint: disable=invalid-name, too-many-locals, too-many-statements
#pylint: disable=too-many-branches, c-extension-no-member
import logging
import uuid
from hdbcli import dbapi
from .utility import _convert_index_from_timestamp_to_int, _is_index_int
from ..pal_base import (
    ParameterTable,
    arg,
    try_drop,
    require_pal_usable,
    call_pal_auto_with_hint
)
logger = logging.getLogger(__name__)

def trend_test(data, key=None, endog=None, method=None, alpha=None):
    r"""
    Trend test is a statistical method used in time series analysis to determine whether there is a consistent upward or downward movement over time, and calculate the de-trended time series.

    Parameters
    ----------

    data : DataFrame

        The input data should comprise at least two columns. One is ID column, while the other is raw data.

    key : str, optional
        The ID column.

        Defaults to the first column of data if the index column of data is not provided.
        Otherwise, defaults to the index column of data.

    endog : str, optional
        The column of series to be tested.

        Defaults to the first non-ID column.

    method : {'mk', 'difference-sign'}, optional
        Specifies the method used to identify the trend:

        -'mk': Mann-Kendall test.
        -'difference-sign': Difference-sign test.

        Defaults to 'mk'.

    alpha : float, optional
        The significance value.

        Its typical value ranges between 0 and 0.5, asserting confidence in the observed trend within the series.

        Defaults to 0.05.

    Returns
    -------
    DataFrames

        DataFrame 1 : statistics, structured as follows:

        **STAT_NAME**: includes

        - TREND: -1 for downward trend, 0 for no trend, and 1 for upward trend
        - S: the number of positive pairs minus the negative pairs
        - P-VALUE: The p-value of the observed S

        **STAT_VALUE**: value of stats above.

        DataFrame 2 : a detrended table, structured as follows:

        - ID : Time stamp that is monotonically increasing sorted.
        - DETRENDED_SERIES: The corresponding de-trended time series. The first value absents if trend presents.


    Examples
    --------
    >>> stats, detrended = trend_test(data=df, key='ID', endog='SERIES', method='mk', alpha=0.05)
    >>> stats.collect()
    >>> detrended.collect()

    """
    conn = data.connection_context
    require_pal_usable(conn)
    method_map = {'mk':1, 'difference-sign':2}

    method = arg('method', method, method_map)
    alpha = arg('alpha', alpha, float)
    key = arg('key', key, str)
    endog = arg('endog', endog, str)

    cols = data.columns
    if len(cols) < 2:
        msg = ("Input data should contain at least 2 columns: " +
               "one for ID, another for raw data.")
        logger.error(msg)
        raise ValueError(msg)

    if key is not None and key not in cols:
        msg = 'Please select key from name of columns!'
        logger.error(msg)
        raise ValueError(msg)

    index = data.index
    if index is not None:
        if key is None:
            if not isinstance(index, str):
                key = cols[0]
                warn_msg = "The index of data is not a single column and key is None, so the first column of data is used as key!"
                logger.warning(warn_msg)
            else:
                key = index
        else:
            if key != index:
                warn_msg = f"Discrepancy between the designated key column '{key}' and the designated index column '{index}'"
                logger.warning(warn_msg)
    else:
        if key is None:
            key = cols[0]
    cols.remove(key)

    if endog is not None:
        if endog not in cols:
            msg = 'Please select endog from name of columns!'
            logger.error(msg)
            raise ValueError(msg)
    else:
        endog = cols[0]

    data_ = data[[key] + [endog]]

    # key column type check
    is_index_int = _is_index_int(data_, key)
    if not is_index_int:
        data_ = _convert_index_from_timestamp_to_int(data_, key)

    unique_id = str(uuid.uuid1()).replace('-', '_').upper()
    outputs = ['STATS', 'DETRENDED',]
    outputs = [f'#PAL_TREND_TEST_{name}_TBL_{unique_id}' for name in outputs]
    stats_tbl, detrended_tbl = outputs

    param_rows = [('METHOD', method, None, None),
                  ('ALPHA', None, alpha, None)]
    try:
        call_pal_auto_with_hint(conn,
                                None,
                                'PAL_TREND_TEST',
                                data_,
                                ParameterTable().with_data(param_rows),
                                *outputs)
    except dbapi.Error as db_err:
        logger.exception(str(db_err))
        try_drop(conn, stats_tbl)
        try_drop(conn, detrended_tbl)
        raise
    except Exception as db_err:
        logger.exception(str(db_err))
        try_drop(conn, stats_tbl)
        try_drop(conn, detrended_tbl)
        raise
    detrended_df = conn.table(detrended_tbl)

    if not is_index_int:
        detrended_cols = detrended_df.columns
        detrended_int = detrended_df.rename_columns({detrended_cols[0]:'ID_RESULT'})
        data_int = data.add_id('ID_DATA', ref_col=key)
        detrended_df = detrended_int.join(data_int, 'ID_RESULT=ID_DATA').select(key, detrended_cols[1])
    return conn.table(stats_tbl), detrended_df
