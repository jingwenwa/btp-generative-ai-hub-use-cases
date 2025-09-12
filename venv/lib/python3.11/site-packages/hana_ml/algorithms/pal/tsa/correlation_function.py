"""
This module contains Python wrapper for PAL correlation function algorithm.

The following function is available:

    * :func:`correlation`
"""
#pylint: disable=invalid-name, too-many-arguments, too-many-locals
#pylint: disable=too-many-lines, line-too-long, relative-beyond-top-level
#pylint: disable=consider-using-f-string, too-many-statements
import logging
import uuid
from hdbcli import dbapi
from .utility import _convert_index_from_timestamp_to_int, _is_index_int
from ..utility import check_pal_function_exist
from ..pal_base import (
    ParameterTable,
    arg,
    try_drop,
    require_pal_usable,
    call_pal_auto_with_hint
)
logger = logging.getLogger(__name__)

def correlation(data,
                key=None,
                x=None,
                y=None,
                thread_ratio=None,
                method=None,
                max_lag=None,
                calculate_pacf=None,
                calculate_confint=False,
                alpha=None,
                bartlett=None):
    r"""
    This correlation function gives the statistical correlation between random variables.

    Parameters
    ----------

    data : DataFrame
        Input data.

    key : str, optional
        Name of the ID column.

        Defaults to the index column of ``data`` (i.e. data.index) if it is set.

    x : str, optional
        The name of the first series of data columns.

    y : str, optional
        The name of the second series of data columns.

    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
         Values outside the range will be ignored and this function heuristically determines the number of threads to use. Valid only when ``method`` is set as 'brute_force'.

        Defaults to -1.
    method : {'auto', 'brute_force', 'fft'}, optional
        Indicates the method to be used to calculate the correlation function.

        Defaults to 'auto'.

    max_lag : int, optional
        Maximum lag for the correlation function.

        Defaults to sqrt(n), where n is the data number.

    calculate_pacf : bool, optional
        Controls whether to calculate Partial Autocorrelation Coefficient(PACF) or not.

        Valid only when only one series is provided.

        Defaults to True.

    calculate_confint : bool, optional
        Controls whether to calculate confidence intervals or not.

        If it is True, two additional columns of confidence intervals are shown in the result.

        Defaults to False.

    alpha : float, optional
        Confidence bound for the given level are returned. For instance if alpha=0.05, 95% confidence bound is returned.

        Valid only when only ``calculate_confint`` is True.

        Defaults to 0.05.

    bartlett : bool, optional

        - False: use standard error to calculate the confidence bound.
        - True: use Bartlett's formula to calculate the confidence bound.

        Valid only when only ``calculate_confint`` is True.

        Defaults to True.

    Returns
    -------
    DataFrame
        Result of the correlation function, structured as follows:

        - LAG: ID column.
        - CV: ACV/CCV.
        - CF: ACF/CCF.
        - PACF: PACF. Null if cross-correlation is calculated.
        - ACF_CONFIDENCE_BOUND: Confidence intervals of ACF. The result show this column when calculate_confint = True.
        - PACF_CONFIDENCE_BOUND: Confidence intervals of PACF. The result show this column when calculate_confint = True.

    Examples
    --------
    >>> res = correlation(data=df, key='ID', x='X',
                          thread_ratio=0.4, method='auto',
                          calculate_pacf=True)
    >>> res.collect()
    """
    conn = data.connection_context
    require_pal_usable(conn)
    index = data.index
    key = arg('key', key, str, not isinstance(index, str))
    if isinstance(index, str):
        if key is not None and index != key:
            warn_msg = "Discrepancy between the designated key column '{}' ".format(key) +\
            "and the designated index column '{}'.".format(index)
            logger.warning(warn_msg)
    key = index if key is None else key

    x, y = arg('x', x, str), arg('y', y, str)
    if x is None:
        msg = ("The first series must be given.")
        logger.error(msg)
        raise ValueError(msg)
    cols = [x]
    if x is not None and y is not None:
        cols.append(y)
    data_ = data[[key] + cols]
    method_map = {'auto': -1, 'brute_force': 0, 'fft': 1}
    thread_ratio = arg('thread_ratio', thread_ratio, float)
    method = arg('method', method, method_map)
    max_lag = arg('max_lag', max_lag, int)
    calculate_pacf = arg('calculate_pacf', calculate_pacf, bool)
    calculate_confint = arg('calculate_confint', calculate_confint, bool)
    alpha = arg('alpha', alpha, float)
    bartlett = arg('bartlett', bartlett, bool)

    unique_id = str(uuid.uuid1()).replace('-', '_').upper()
    table = ["#CORREALATION_RESULT_{}".format(unique_id)]
    result_tbl = table[0]
    param_array = [('THREAD_RATIO', None, thread_ratio, None),
                   ('USE_FFT', method, None, None),
                   ('MAX_LAG', max_lag, None, None),
                   ('CALCULATE_PACF', calculate_pacf, None, None)]

    if calculate_confint is True:
        param_array.extend([('ALPHA', None, alpha, None),
                            ('BARTLETT', bartlett, None, None)])

    if not _is_index_int(data_, key):
        data_ = _convert_index_from_timestamp_to_int(data_, key)
    if calculate_confint is True and not (check_pal_function_exist(conn, '%CONFIDENCE%', like=True) \
    or conn.disable_hana_execution):
        msg = 'The version of your SAP HANA does not support the calculation with confidence!' + \
        ' Please set calculate_confint = False.'
        logger.error(msg)
        raise ValueError(msg)
    try:
        if calculate_confint is True:
            sql, _ = call_pal_auto_with_hint(conn,
                                             None,
                                             'PAL_CORRELATION_FUNCTION_WITH_CONFIDENCE',
                                             data_,
                                             ParameterTable().with_data(param_array),
                                             *table)
        else:
            sql, _ = call_pal_auto_with_hint(conn,
                                             None,
                                             'PAL_CORRELATION_FUNCTION',
                                             data_,
                                             ParameterTable().with_data(param_array),
                                             *table)
        conn.execute_statement = sql
    except dbapi.Error as db_err:
        logger.exception(str(db_err))
        try_drop(conn, result_tbl)
        raise
    except Exception as db_err:
        logger.exception(str(db_err))
        try_drop(conn, result_tbl)
        raise
    return conn.table(result_tbl)
