"""
This module contains Python wrapper for PAL Hull-White.

The following function is available:

    * :func:`hull_white_simulate`
"""
#pylint:disable=line-too-long, too-many-arguments
#pylint:disable=invalid-name, too-few-public-methods, too-many-statements, too-many-locals
#pylint:disable=too-many-branches, c-extension-no-member
import logging
import uuid
from hdbcli import dbapi
from .utility import _is_index_int, _col_index_check
from ..pal_base import (
    ParameterTable,
    arg,
    try_drop,
    require_pal_usable,
    call_pal_auto_with_hint
)
logger = logging.getLogger(__name__)

def hull_white_simulate(data,
                        key=None,
                        endog=None,
                        num_simulation_paths=None,
                        random_seed=None,
                        mean_reversion_speed=None,
                        volatility=None,
                        time_increment=None,
                        confidence_level=None,
                        initial_value=None):
    r"""
    The Hull-White model, as implemented in PAL, is a single-factor interest rate model that plays a crucial role in financial mathematics and risk management.
    The Hull-White model is particularly significant because it provides a framework for understanding how interest rates evolve over time, which is vital for pricing various financial instruments like bonds and interest rate derivatives.
    By using this formula, the Hull-White model can simulate various interest rate paths, allowing financial analysts and economists to anticipate changes in the economic landscape and make more informed decisions regarding investment and risk management strategies.

    Parameters
    ----------

    data : DataFrame
        Input data which contains two columns, one is ID column, the other is the value of the drift term.

    key : str, optional
        The ID column.

        Defaults to the first column of data if the index column of data is not provided.
        Otherwise, defaults to the index column of data.

    endog : str, optional

        The column of series to be tested.

        Defaults to the first non-key column.

    num_simulation_paths : int, optional
        Number of total simulation paths

        Defaults to 5000.

    random_seed : int, optional
        Indicates using machine time as seed.

        Defaults to 0.

    mean_reversion_speed : float, optional
        Alpha in the formula.

        Defaults to 0.1.

    volatility : float, optional
        Sigma in the formula.

        Defaults to 0.01.

    time_increment : float, optional
        dt in the formula. In daily interest rate modeling, dt might be set to 1/252 (assuming 252 business days in a year), while in monthly modeling, it could be 1/12.

        Defaults to 1/252.

    confidence_level : float, optional
        Confidence level that sets the upper and lower bounds of the simulation values.

        Defaults to 0.95.

    initial_value : float, optional
        Starting value of the simulation.

        Defaults to 0.0.

    Returns
    -------
    DataFrame
        Result, structured as follows:

            - 1st Column, ID, Time step that is monotonically increasing sorted.
            - 2nd Column, MEAN, Mean of the simulation at the corresponding time step.
            - 3rd Column, VARIANCE, Variance of the simulation at the corresponding time step.
            - 4th Column, LOWER_BOUND, Lower bound of the simulation at the corresponding time step with the given confidence level.
            - 5th Column, UPPER_BOUND, Upper bound of the simulation at the corresponding time step with the given confidence level.

    Examples
    --------

    Time series data df:

    >>> df.head(3).collect()
        TIME_STAMP  VALUE
    0            0  0.075
    1            1  0.160
    2            2  0.130
    ......
    27          27  0.600
    28          28  0.970
    29          29  0.830

    Perform hull_white_simulate():

    >>> result = hull_white_simulate(data=df,
                                     key='TIME_STAMP',
                                     endog='VALUE',
                                     num_simulation_paths=5000,
                                     random_seed=1,
                                     mean_reversion_speed=0.1,
                                     volatility=0.01,
                                     time_increment=0.083,
                                     confidence_level=0.95,
                                     initial_value=0.0)

    Outputs:

    >>> result.collect()
        ID      MEAN  VARIANCE  LOWER_BOUND  UPPER_BOUND
    0    0  0.006255  0.000008     0.000666     0.011843
    1    1  0.019503  0.000017     0.011505     0.027502
    ...
    28  28  0.919654  0.000191     0.892594     0.946713
    29  29  0.980900  0.000197     0.953388     1.008413

    """
    conn = data.connection_context
    require_pal_usable(conn)
    num_simulation_paths = arg('num_simulation_paths', num_simulation_paths, int)
    random_seed = arg('random_seed', random_seed, int)
    mean_reversion_speed = arg('mean_reversion_speed', mean_reversion_speed, float)
    volatility = arg('volatility', volatility, float)
    time_increment = arg('time_increment', time_increment, float)
    confidence_level = arg('confidence_level', confidence_level, float)
    initial_value = arg('initial_value', initial_value, float)

    unique_id = str(uuid.uuid1()).replace('-', '_').upper()
    res_tbl = f'#PAL_HULL_WHITE_RESULT_TBL_{unique_id}'

    cols = data.columns
    index = data.index
    if index is not None:
        key = _col_index_check(key, 'key', index, cols)
    else:
        if key is None:
            key = cols[0]
    if key is not None and key not in cols:
        msg = f"Please select key from {cols}!"
        logger.error(msg)
        raise ValueError(msg)
    cols.remove(key)

    if endog is not None and endog not in cols:
        msg = f"Please select endog from {cols}!"
        logger.error(msg)
        raise ValueError(msg)
    if endog is None:
        endog = cols[0]
    data_ = data[[key] + [endog]]

    # key column type check
    is_index_int = _is_index_int(data, key)
    if not is_index_int:
        data_= data_.add_id(key + '(INT)', ref_col=key, starting_point=0).deselect(key)

    param_rows = [('NUM_SIMULATION_PATHS', num_simulation_paths, None, None),
                  ('RANDOM_SEED', random_seed, None, None),
                  ('MEAN_REVERSION_SPEED', None, mean_reversion_speed, None),
                  ('VOLATILITY', None, volatility, None),
                  ('TIME_INCREMENT', None, time_increment, None),
                  ('CONFIDENCE_LEVEL', None, confidence_level, None),
                  ('INITIAL_VALUE', None, initial_value, None)]

    try:
        sql, _ = call_pal_auto_with_hint(conn,
                                         None,
                                         'PAL_HULL_WHITE_SIMULATE',
                                         data_,
                                         ParameterTable().with_data(param_rows),
                                         res_tbl)
        conn.execute_statement = sql
    except dbapi.Error as db_err:
        logger.exception(str(db_err))
        try_drop(conn, res_tbl)
        raise
    except Exception as db_err:
        logger.exception(str(db_err))
        try_drop(conn, res_tbl)
        raise
    res_df = conn.table(res_tbl)

    if not is_index_int:
        res_cols = res_df.columns
        res_int = res_df.rename_columns({res_cols[0]:'ID_RESULT'})
        data_int = data.add_id('ID_DATA', ref_col=key, starting_point=0)
        res_df = res_int.join(data_int, 'ID_RESULT=ID_DATA').select(key, res_cols[1], res_cols[2], res_cols[3], res_cols[4])

    return res_df
