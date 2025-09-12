'''
This module contains a SAP HANA PAL wrapper for ABC analysis algorithm.

The following function is available:

    * :func:`abc_analysis`
'''
import logging
import uuid
from hdbcli import dbapi
from hana_ml.ml_base import try_drop
from .pal_base import (
    ParameterTable,
    arg,
    call_pal_auto_with_hint
)
#pylint:disable=too-many-lines, relative-beyond-top-level, too-many-arguments
#pylint: disable=consider-using-f-string
#pylint: disable=invalid-name, too-many-arguments, too-many-locals, line-too-long
logger = logging.getLogger(__name__)

def abc_analysis(data, key=None, percent_A=None, percent_B=None, percent_C=None,
                 revenue=None, thread_ratio=None):
    """
    ABC analysis is used to classify objects (such as customers, employees, or products) based on a particular measure (such as revenue or profit).
    ABC analysis suggests that inventories of an organization are not of equal value, thus can be grouped into three categories (A, B, and C) by their estimated importance. 'A' items are very important for an organization. 'B' items are of medium importance, that is, less important than 'A' items and more important than 'C' items. 'C' items are of the least importance.
    An example of ABC classification is as follows:

    - 'A' items - 20% of the items (customers) accounts for 70% of the revenue.
    - 'B' items - 30% of the items (customers) accounts for 20% of the revenue.
    - 'C' items - 50% of the items (customers) accounts for 10% of the revenue.

    Parameters
    ----------
    data : DataFrame
        The input data.
    key : str, optional
        Name of the ID column.

        Defaults to the index column of ``data`` (i.e. data.index) if it is set.
    revenue : str, optional
        Name of column for revenue (or profits).

        If not given, the input DataFrame must only have two columns.

        Defaults to the first non-key column.
    percent_A : float
        The proportion allocated to A class.
    percent_B : float
        The proportion allocated to B class.
    percent_C : float
        The proportion allocated to C class.
    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 0.

    Returns
    -------
    DataFrame
        The result after partitioning the data into three categories.

    Examples
    --------
    Input DataFrame:

    >>> df.collect()
          ITEM    VALUE
    0    item1     15.4
    1    item2    200.4
    ...
    8    item9    96.15
    9   item10      9.4

    Perform abc_analysis():

    >>> res = abc_analysis(data=df, key='ITEM', thread_ratio=0.3,
                           percent_A=0.7, percent_B=0.2, percent_C=0.1)
    >>> res.collect()
       ABC_CLASS         ITEM
    0          A        item3
    1          A        item2
    ...
    8          C        item8
    9          C       item10
    """
    conn_context = data.connection_context
    index = data.index
    key = arg('key', key, str, not isinstance(index, str))
    if isinstance(index, str):
        if key is not None and index != key:
            msg = "Discrepancy between the designated key column '{}' ".format(key) +\
            "and the designated index column '{}'.".format(index)
            logger.warning(msg)
    key = index if key is None else key
    revenue = arg('revenue', revenue, str)
    if revenue is None:
        if len(data.columns) != 2:
            msg = ("If 'revenue' is not given, the input dataframe " +
                   "must only have two columns.")
            logger.error(msg)
            raise ValueError(msg)
        revenue = data.columns[-1]
    data_ = data[[key, revenue]]
    thread_ratio = arg('thread_ratio', thread_ratio, float)
    percent_A = arg('percent_A', percent_A, float, required=True)
    percent_B = arg('percent_B', percent_B, float, required=True)
    percent_C = arg('percent_C', percent_C, float, required=True)
    param_rows = [('THREAD_RATIO', None, thread_ratio, None),
                  ('PERCENT_A', None, percent_A, None),
                  ('PERCENT_B', None, percent_B, None),
                  ('PERCENT_C', None, percent_C, None)
                 ]
    unique_id = str(uuid.uuid1()).replace('-', '_').upper()
    result_tbl = "#ABC_ANALYSIS_RESULT_{}".format(unique_id)
    try:
        call_pal_auto_with_hint(conn_context,
                                None,
                                'PAL_ABC',
                                data_,
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
