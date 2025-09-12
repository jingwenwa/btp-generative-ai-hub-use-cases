"""
This module contains Python wrapper for PAL seasonality test algorithm.

The following function is available:

    * :func:`seasonal_decompose`
"""
#pylint:disable=line-too-long, too-many-locals, unused-argument
#pylint:disable=invalid-name, too-many-arguments, too-few-public-methods, too-many-statements
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

def seasonal_decompose(data,
                       key=None,
                       endog=None,
                       alpha=None,
                       thread_ratio=None,
                       model=None,
                       decompose_type=None,
                       extrapolation=None,
                       smooth_width=None,
                       auxiliary_normalitytest=None,
                       periods=None,
                       decompose_method=None,
                       stl_robust=None,
                       stl_seasonal_average=None,
                       smooth_method_non_seasonal=None
                       ):
    r"""
    Seasonal_decompose function tests whether a time series has a seasonality or not.
    If it does, the corresponding additive or multiplicative seasonality model is identified, and the series is decomposed into three components: seasonal, trend, and random.

    Parameters
    ----------
    data : DataFrame
        Input data. It should have at least two columns; the ID column and the raw data column.

    key : str, optional
        The ID column.

        Defaults to the first column of data if the index column of data is not provided.
        Otherwise, defaults to the index column of data.

    endog : str, optional
        The column of series to be decomposed.

        Defaults to the first non-ID column.

    alpha : float, optional
        The criterion for the autocorrelation coefficient.
        It ranges between 0 and 1. A higher value implies a more stringent requirement for seasonality.

        Defaults to 0.2.

    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to -1.

    decompose_type : {'additive', 'multiplicative', 'auto'}, optional
        Specifies the decompose type.

        - 'additive': Additive decomposition model.
        - 'multiplicative': Multiplicative decomposition model.
        - 'auto': Decomposition model automatically determined from input data.

        Defaults to 'auto'.

    extrapolation : bool, optional
       Specifies whether to extrapolate the endpoints or not.
       Set to True when there is an end-point issue.

       Defaults to False.

    smooth_width : int, optional
       Specifies the width of the moving average applied to non-seasonal data.
       0 indicates linear fitting to extract trends.
       Can not be larger than half of the data length.

       Defaults to 0.

    auxiliary_normalitytest : bool, optional
       Specifies whether to use normality test to identify model types.

       Defaults to False.

    periods : int, optional
       The length of the periods. When this parameter is specified between 2 and half of the series length, autocorrelation value is calculated for this number of periods and the result is compared to ``alpha`` parameter.

       If correlation value is equal to or higher than ``alpha``, decomposition is executed with the value of ``periods``. Otherwise, decomposition is executed with no seasonality. For other value of ``periods``, decomposition is also executed with no seasonality.

       Defaults to None.

    decompose_method : str, optional
       Specifies the decomposition method.

       - 'traditional'.
       - 'stl'.

       Defaults to 'traditional'.

    stl_robust : bool, optional

       Whether to use residual weights during STL decomposition. Residual weights can make STL decomposition more robust to outliers.

       - False : Does not use residual weights.
       - True : Uses residual weights.

       Only valid when ``decompose_method`` is 'stl'.

       Defaults to True.

    stl_seasonal_average : bool, optional

       Whether to make seasonal data equal in every seasonal cycle.

       For example, for the seasonal data of a monthly time series with a period of 12, we can get 12 subseries, corresponding to each month. "True" means we take average for each subseries and get the seasonal component for each month. "False" means we do not take the avarage.

       Only valid when ``decompose_method`` is 'stl'.

       Defaults to False.

    smooth_method_non_seasonal : str, optional

       Specifies the smoothing method for non-seasonal time series.

       - 'moving_average'.
       - 'super_smoother'.

       Defaults to 'moving_average'.

    Returns
    -------

    DataFrames

        DataFrame 1 : Statistics

        DataFrame 2 : Seasonal decomposition

        - ID column of input data.
        - SEASONAL: seasonality component.
        - TREND: trend component.
        - RANDOM: white noise component.

    Examples
    --------
    >>> stats, decompose = seasonal_decompose(data=df, key='ID', endog='SERIES', alpha=0.2)
    >>> stats.collect()
    >>> decompose.collect()

    """
    seasonal_decompose_map = {'additive': 1, 'multiplicity' : 2,
                              'multiplicative': 2,
                              'none' : None, 'auto': 0}
    decompose_method_map = {'traditional': 0, 'stl' : 1}
    smooth_map = {'moving_average': 0, 'super_smoother': 1}

    conn = data.connection_context
    require_pal_usable(conn)
    alpha = arg('alpha', alpha, float)
    thread_ratio = arg('thread_ratio', thread_ratio, float)
    decompose_type = arg('decompose_type', decompose_type, seasonal_decompose_map)
    extrapolation = arg('extrapolation', extrapolation, bool)
    smooth_width = arg('smooth_width', smooth_width, int)
    auxiliary_normalitytest = arg('auxiliary_normalitytest', auxiliary_normalitytest, bool)
    periods = arg('periods', periods, int)
    decompose_method = arg('decompose_method', decompose_method, decompose_method_map)
    stl_robust = arg('stl_robust', stl_robust, bool)
    stl_seasonal_average = arg('stl_seasonal_average', stl_seasonal_average, bool)
    smooth_method_non_seasonal = arg('smooth_method_non_seasonal', smooth_method_non_seasonal, smooth_map)

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

    unique_id = str(uuid.uuid1()).replace('-', '_').upper()
    outputs = ['STATS', 'DECOMPOSE',]
    outputs = [f'#PAL_SEASONAL_DECOMPOSE_{name}_TBL_{unique_id}' for name in outputs]
    stats_tbl, decompose_tbl = outputs
    param_rows = [('ALPHA', None, alpha, None),
                  ('THREAD_RATIO', None, thread_ratio, None),
                  ('DECOMPOSE_TYPE', decompose_type, None, None),
                  ('EXTRAPOLATION', extrapolation, None, None),
                  ('SMOOTH_WIDTH', smooth_width, None, None),
                  ('AUXILIARY_NORMALITYTEST', auxiliary_normalitytest, None, None),
                  ('PERIODS', periods, None, None),
                  ('DECOMPOSE_METHOD', decompose_method, None, None),
                  ('STL_ROBUST', stl_robust, None, None),
                  ('STL_SEASONAL_AVERAGE', stl_seasonal_average, None, None),
                  ('SMOOTH_METHOD_NON_SEASONAL', smooth_method_non_seasonal, None, None)]

    is_index_int = _is_index_int(data_, key)
    if not is_index_int:
        data_ = _convert_index_from_timestamp_to_int(data_, key)
    try:
        sql, _ = call_pal_auto_with_hint(conn,
                                         None,
                                         'PAL_SEASONALITY_TEST',
                                         data_,
                                         ParameterTable().with_data(param_rows),
                                         *outputs)
        conn.execute_statement = sql
    except dbapi.Error as db_err:
        logger.exception(str(db_err))
        try_drop(conn, stats_tbl)
        try_drop(conn, decompose_tbl)
        raise
    except Exception as db_err:
        logger.exception(str(db_err))
        try_drop(conn, stats_tbl)
        try_drop(conn, decompose_tbl)
        raise
    decompose_df = conn.table(decompose_tbl)
    if not is_index_int:
        decom_cols = decompose_df.columns
        decompose_int = decompose_df.rename_columns({decom_cols[0]:'ID_RESULT'})
        data_int = data.add_id('ID_DATA', ref_col=key)
        decompose_df = decompose_int.join(data_int, 'ID_RESULT=ID_DATA').select(key, decom_cols[1], decom_cols[2], decom_cols[3])
    return conn.table(stats_tbl), decompose_df
