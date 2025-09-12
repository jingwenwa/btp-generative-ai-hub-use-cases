"""
This module contains the Python wrapper for PAL_ITSF procedure.

The following classes are available:

    * :func:`intermittent_forecast`

"""
#pylint: disable=consider-using-f-string
import logging
import uuid
from hdbcli import dbapi
from hana_ml.ml_base import quotename
from .utility import _convert_index_from_timestamp_to_int, _is_index_int
from .utility import _get_forecast_starttime_and_timedelta
from ..pal_base import (
    ParameterTable,
    arg,
    try_drop,
    require_pal_usable,
    call_pal_auto_with_hint
)
logger = logging.getLogger(__name__)#pylint:disable=invalid-name

def intermittent_forecast(data, key=None,#pylint:disable=too-many-statements, too-many-arguments, too-many-locals, invalid-name, too-many-branches
                          endog=None, p=None,
                          q=None, forecast_num=None,
                          optimizer=None, method=None,
                          grid_size=None, optimize_step=None,
                          accuracy_measure=None, ignore_zero=None,
                          expost_flag=None, thread_ratio=None,
                          iter_count=None, random_state=None,
                          penalty=None):
    r"""

    Intermittent Time Series Forecast (ITSF) is a forecasting strategy for products with intermittent demand.

    Differences from the constant weight of the Croston method:

    - ITSF provides an exponential weight for estimation, meaning closer data points have greater weight.
    - ITSF does not require the initial value of non-zero demands or the time intervals between non-zero demands.

    Parameters
    ----------
    data : DataFrame
        Input data.

    key : str, optional
        Specifies the ID (representing time-order) column of ``data``.

        Required if a single ID column cannot be inferred from the index of ``data``.

        If there is a single column name in the index of ``data``,
        then ``key`` defaults to that column; otherwise, ``key`` is mandatory.

    endog : str, optional
        Specifies the name of the column for intermittent demand values.

        Defaults to the first non-key column of ``data``.

    p : int, optional
        The smoothing parameter for demand, where:

        - -1 : Automatically optimizes this parameter.
        - Positive integers : Specifies the value for smoothing ([1, n]) and forecasts manually.

        The specified value cannot exceed the length of the time series for analysis.

        Defaults to -1.

    q : int, optional
        The smoothing parameter for the time intervals between intermittent demands, where:

        - -1 : Automatically optimizes this parameter.
        - Non-negative values ([1, p]) : Specifies the value manually.

        Defaults to -1.

    forecast_num : int, optional
        Forecast length. When set to 1, the algorithm forecasts only one value.

        Defaults to 1.

    optimizer : {'lbfgsb', 'brute', 'sim_annealing'}, optional
        Specifies the optimization algorithm for automatically identifying
        parameters ``p`` and ``q``.

        - 'lbfgsb' : Bounded Limited-memory Broyden-Fletcher-Goldfarb-Shanno (LBFGSB) method with parameters ``p`` and ``q`` initialized by the default scheme.
        - 'brute' : Brute-force method, LBFGSB with parameters ``p`` and ``q`` initialized by grid search.
        - 'sim_annealing' : Simulated annealing method.

        Defaults to 'lbfgsb'.

    method : str, optional
        Specifies the method (or mode) for the output:

        - 'sporadic': Uses the sporadic method.
        - 'constant': Uses the constant method.

        Defaults to 'constant'.

    grid_size : int, optional
        Specifies the number of steps from the start point to the length of the data for grid search.

        Only valid when ``optimizer`` is set to 'brute'.

        Defaults to 20.

    optimize_step : float, optional
        Specifies the minimum step for each iteration of the LBFGSB method.

        Defaults to 0.001.

    accuracy_measure : str or list of str, optional
        The metric to quantify how well a model fits the input data.
        Options: 'mse', 'rmse', 'mae', 'mape', 'smape', 'mase'.

        Defaults to 'mse'.

        .. Note::
            Specify a measure name if you want the corresponding measure value to be
            reflected in the output statistics (the second DataFrame in the return).

    ignore_zero : bool, optional

        - False: Uses zero values in the input dataset when calculating 'mape'.
        - True: Ignores zero values in the input dataset when calculating 'mape'.

        Only valid when ``accuracy_measure`` is 'mape'.

        Defaults to False.

    expost_flag : bool, optional

        - False: Does not output the expost forecast and only outputs the forecast values.
        - True: Outputs both the expost forecast and the forecast values.

        Defaults to True.

    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible threads.
        Values outside the range will be ignored, and this function heuristically determines the number of threads to use.

        Defaults to 0.

    iter_count : int, optional
        A positive integer that controls the number of iterations for simulated annealing.

        Defaults to 1000.

    random_state : int, optional
        Specifies the seed for the random number generator. Valid for the simulated annealing method.

        Defaults to 1.

    penalty : float, optional
        A penalty applied to the cost function to avoid overfitting.

        Defaults to 1.0.

    Returns
    -------
    tuple of DataFrames
        - 1st DataFrame: Forecast values.
        - 2nd DataFrame: Related statistics.

    Examples
    --------
    >>> forecasts, stats = intermittent_forecast(data=df, p=3, forecast_num=3,
                                                 optimizer='lbfgsb_grid', grid_size=20,
                                                 optimize_step=0.011, expost_flag=False,
                                                 accuracy_measure='mse', ignore_zero=False,
                                                 thread_ratio=0.5)
    >>> forecasts.collect()
    >>> stats.collect()
    """
    conn = data.connection_context
    require_pal_usable(conn)
    method_map = {'constant':0, 'sporadic':1}
    optimizer_map = {'lbfgsb_default': 2, 'lbfgsb_grid': 1,
                     'brute': 1, 'sim_annealing': 0, 'lbfgsb': 2}
    measures = ['mse', 'rmse', 'mae', 'mape', 'smape', 'mase']
    cols = data.columns
    if len(cols) < 2:
        msg = ("Input data should contain at least 2 columns: " +
               "one for ID, another for raw data.")
        logger.error(msg)
        raise ValueError(msg)
    index = data.index
    key = arg('key', key, str, not isinstance(index, str))
    if key is not None:
        if key not in cols:
            msg = f'Please select key from {cols}!'
            logger.error(msg)
            raise ValueError(msg)
    if isinstance(index, str):
        if key is not None and index != key:
            msg = f"Discrepancy between the designated key column '{key}' " +\
            "and the designated index column '{index}'."
            logger.warning(msg)
    key = index if key is None else key
    cols.remove(key)
    endog = arg('endog', endog, str)
    if endog not in cols + [None]:
        msg = f'Please select endog from {cols}!'
        logger.error(msg)
        raise ValueError(msg)
    endog = cols[0] if endog is None else endog
    p = arg('p', p, int)
    if p is not None and p > data.count():
        msg = 'The value of p exceeds the length of time-series!'
        logger.error(msg)
        raise ValueError(msg)
    q = arg('q', q, int)
    if isinstance(p, int) and isinstance(q, int):
        if all(x is not None for x in [p, q]):
            if p != -1 and q > p:
                msg = 'The value of q is greater than p!'
                logger.error(msg)
                raise ValueError(msg)
    forecast_num = arg('forecast_num', forecast_num, int)
    optimizer = arg('optimizer', optimizer, optimizer_map)
    method = arg('method', method, method_map)
    grid_size = arg('grid_size', grid_size, int)
    optimize_step = arg('optimize_step', optimize_step, float)
    accuracy_measure = arg('accuracy_measure', accuracy_measure, {ms:ms.upper() for ms in measures})
    ignore_zero = arg('ignore_zero', ignore_zero, bool)
    expost_flag = arg('expost_flag', expost_flag, bool)
    thread_ratio = arg('thread_ratio', thread_ratio, float)
    iter_count = arg('iter_count', iter_count, int)
    random_state = arg('random_state', random_state, int)
    penalty = arg('penalty', penalty, float)
    data_ = data[[key] + [endog]]
    is_idx_int = _is_index_int(data_, key)
    if not is_idx_int:
        data_ = _convert_index_from_timestamp_to_int(data_, key)
    try:
        forecast_start, timedelta = _get_forecast_starttime_and_timedelta(data, key, is_idx_int)
    except:#pylint:disable=bare-except
        pass
    unique_id = str(uuid.uuid1()).replace('-', '_').upper()
    outputs = ['FORECAST', 'STATS']
    outputs = ['#PAL_ITSF_{}_TBL_{}'.format(name, unique_id) for name in outputs]
    forecast_tbl, stats_tbl = outputs

    param_rows = [('P', p, None, None),
                  ('Q', q, None, None),
                  ('FORECAST_NUM', forecast_num, None, None),
                  ('METHOD', optimizer, None, None),
                  ('ALGORITHM_TYPE', method, None, None),
                  ('BRUTE_STEP', grid_size, None, None),
                  ('OPTIMIZE_STEP', None, optimize_step, None),
                  ('MEASURE_NAME', None, None, accuracy_measure),
                  ('IGNORE_ZERO', ignore_zero, None, None),
                  ('EXPOST_FLAG', expost_flag, None, None),
                  ('THREAD_RATIO', None, thread_ratio, None),
                  ('ITER_COUNT', iter_count, None, None),
                  ('SEED', random_state, None, None),
                  ('PENALTY', None, penalty, None)]
    try:
        call_pal_auto_with_hint(conn,
                                None,
                                'PAL_ITSF',
                                data_,
                                ParameterTable().with_data(param_rows),
                                *outputs)
    except dbapi.Error as db_err:
        logger.exception(str(db_err))
        try_drop(conn, stats_tbl)
        try_drop(conn, forecast_tbl)
        raise
    except Exception as db_err:
        logger.exception(str(db_err))
        try_drop(conn, stats_tbl)
        try_drop(conn, forecast_tbl)
        raise
    fct_ = conn.table(forecast_tbl)
    if not is_idx_int:
        fct_ = conn.sql("""
                        SELECT ADD_SECONDS('{0}', ({1}-{6}) * {2}) AS {5},
                        {4} FROM ({3})
                        """.format(forecast_start,
                                   quotename(fct_.columns[0]),
                                   timedelta,
                                   fct_.select_statement,
                                   quotename(fct_.columns[1]),
                                   key,
                                   data.count() + (1 if expost_flag is False else 0)))
    return fct_, conn.table(stats_tbl)
