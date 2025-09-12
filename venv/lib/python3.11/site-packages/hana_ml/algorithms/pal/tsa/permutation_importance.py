"""
This module contains Python wrapper for PAL permutation feature importance.

The following function is available:

    * :func:`permutation_importance`
"""
#pylint:disable=line-too-long, too-many-arguments
#pylint:disable=invalid-name, too-few-public-methods, too-many-statements, too-many-locals
#pylint:disable=too-many-branches, c-extension-no-member
import logging
import uuid
from deprecated import deprecated
from hdbcli import dbapi
from .utility import _convert_index_from_timestamp_to_int, _is_index_int
from ..utility import check_pal_function_exist
from ..pal_base import (
    ParameterTable,
    arg,
    try_drop,
    call_pal_auto_with_hint,
    ListOfStrings
)
logger = logging.getLogger(__name__)

class PermutationImportanceMixin:
    """
    Mixin class for calculating permutation importance of features for a time series model.
    """
    def _get_permutation_importance(self,
                                   permutation_data,
                                   real_data,
                                   model=None,
                                   repeat_time=None,
                                   random_state=None,
                                   thread_ratio=None,
                                   partition_ratio=None,
                                   regressor_top_k=None,
                                   accuracy_measure=None,
                                   ignore_zero=None):
        repeat_time = arg('repeat_time', repeat_time, int)
        random_state = arg('random_state', random_state, int)
        thread_ratio = arg('thread_ratio', thread_ratio, float)
        partition_ratio = arg('partition_ratio', partition_ratio, float)
        regressor_top_k = arg('regressor_top_k', regressor_top_k, int)
        ignore_zero = arg('ignore_zero', ignore_zero, bool)
        accuracy_measure_list = {"mpe":"mpe", "mse":"mse", "rmse":"rmse", "mape":"mape"}
        if accuracy_measure:
            if isinstance(accuracy_measure, str):
                accuracy_measure = [accuracy_measure]
            for acc in accuracy_measure:
                arg('accuracy_measure', acc.lower(), accuracy_measure_list)
            accuracy_measure = [acc.upper() for acc in accuracy_measure]

        conn = permutation_data.connection_context
        if model is None: # model free
            model_df = conn.sql("SELECT TOP 0 * FROM (SELECT NULL ROW_INDEX, NULL MODEL_CONTENT  FROM DUMMY) dt;").cast({"ROW_INDEX": "INTEGER", "MODEL_CONTENT ": "NVARCHAR(5000)"})
            real_data = conn.sql("SELECT TOP 0 * FROM (SELECT NULL ID, NULL Y FROM DUMMY) dt;").cast({"ID": "INTEGER", "Y": "DOUBLE"})
        else: # specific model
            model_df = model

        param_rows = [('REPEAT_TIME', repeat_time, None, None),
                      ('SEED', random_state, None, None),
                      ('THREAD_RATIO', None, thread_ratio, None),
                      ('PARTITION_RATIO', None, partition_ratio, None),
                      ('REGRESSOR_TOP_K', regressor_top_k, None, None),
                      ('IGNORE_ZERO', ignore_zero, None, None)]

        if model is None:
            param_rows.extend([('AUTO_MODEL', 0, None, None)])
            param_rows.extend([('METHOD', 30, None, None)])

        if accuracy_measure is not None:
            for acc in accuracy_measure:
                param_rows.extend([('MEASURE_NAME', None, None, acc)])

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        res_tbl = f'#PAL_PERMUTATION_IMPORTANT_TS_RESULT_TBL_{unique_id}'
        if not (check_pal_function_exist(conn, '%PERMUTATION_FEATURE%', like=True) or
        conn.disable_hana_execution):
            msg = 'The version of your SAP HANA does not support permutation_importance for time series!'
            logger.error(msg)
            raise ValueError(msg)
        try:
            sql, _ = call_pal_auto_with_hint(conn,
                                             None,
                                             'PAL_TS_PERMUTATION_FEATURE_IMPORTANCE',
                                             permutation_data,
                                             ParameterTable().with_data(param_rows),
                                             model_df,
                                             real_data,
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
        res = conn.table(res_tbl)
        setattr(self, "permutation_importance_", res)
        return res

@deprecated(version='2.21.240618', reason="Please use get_permutation_importance() function of ARIMA, AutoARIMA, AdditiveModelForecast, BSTS, or LTSF.")
def permutation_importance(data,
                           model=None,
                           key=None,
                           endog=None,
                           exog=None,
                           repeat_time=None,
                           random_state=None,
                           thread_ratio=None,
                           partition_ratio=None,
                           regressor_top_k=None,
                           accuracy_measure=None,
                           ignore_zero=None): # pragma: no cover
    r"""
    Permutation importance for time series is an exogenous regressor evaluation method that measures the increase in the model score when randomly shuffling the exogenous regressor's values.

    Based on the same permutation importance method, there are two ways to calculate the exogenous regressor importance: model-specific and model free. And they reveal how much the model relies on the exogenous regressor for forecasting by breaking the association between the exogenous regressor and the true value.

    - Model-specific means calculating the exogenous regressor importance based on a specific model. For example, ARIMAX, Bayesian structural time series(BSTS), long-term series forecasting(LTSF), and additive model time series analysis(AMTSA).

    - Model free means calculating the exogenous regressor importance by using a regression method like RDT(Random Decision Trees). For example, Exponential smoothing series.

    By using model-specific methods, you need to provide the trained model table and compared true value table to calculate the importance. However, for the model free methods, only the data table is required.

    Parameters
    ----------

    data : DataFrame
        Input data.

        - If model is provided, the predict dataset (key and exog) as well as true value (target) is required.
        - If no model is provided, please enter the data for fitting and prediction.

    model : DataFrame, optional
        If model-specific methods are used, a trained model DataFrame of time series algorithm is required.
        Currently, we support the model of ARIMA, AutoARIMA, LTSF, Additive Model Forecast and BSTS.

        Defaults to None.

    key : str, optional
        The ID column.

        Defaults to the first column of data if the index column of data is not provided.
        Otherwise, defaults to the index column of data.

    endog : str, optional

        The column of series to be tested.

        Defaults to the first non-key column.

    repeat_time : int, optional
        Indicates the number of times the exogenous regressor importance should be calculated for each column.

        Defaults to 5.

    random_state : int, optional
        Specifies the seed for random number generator.

        - 0: Uses the current time (in second) as seed.
        - Others: Uses the specified value as seed.

        Defaults to 0.

    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to -1.

    partition_ratio : float, optional
        Splits the input data into two parts: training data and compare data.

        Only valid when ``model`` is None (no model is provided).

        Defaults to 0.3.

    regressor_top_k : int, optional
        Captures the top K exogenous regressors.

        Defaults to 10.

    accuracy_measure : str or a list of str, optional

        The metric to quantify how well a model fits input data.
        Options: "mpe", "mse", "rmse", "mape".

        No default value.

    ignore_zero : bool, optional

        - False: Uses zero values in the input dataset when calculating "mpe" or "mape".
        - True: Ignores zero values in the input dataset when calculating "mpe" or "mape".

        Only valid when ``accuracy_measure`` is "mpe" or "mape".

        Defaults to False.

    Returns
    -------
    DataFrame
        The importance of the exogenous regressor, structured as follows:

            - PAIR : Measure name.
            - NAME : Exogenous regressor name.
            - VALUE : The importance of the exogenous regressor.

    Examples
    --------

    Example 1: model-specific

    >>> bsts = BSTS(burn=0.6, expected_model_size=1, niter=200, seed=1)
    >>> bsts.fit(data=df_fit, key='ID', endog='TARGET')
    >>> pires = permutation_importance(data=df_predict,
                                       accuracy_measure=['mse', 'mape'],
                                       regressor_top_k=3,
                                       model=bsts.model_,
                                       key='ID',
                                       endog='TARGET')

    Example 2: model free (no model is provided)

    >>> pires = permutation_importance(data=df,
                                       accuracy_measure=['mse', 'mape'],
                                       random_state=1,
                                       regressor_top_k=4,
                                       key='ID',
                                       endog='TARGET')

    """

    repeat_time = arg('repeat_time', repeat_time, int)
    random_state = arg('random_state', random_state, int)
    thread_ratio = arg('thread_ratio', thread_ratio, float)
    partition_ratio = arg('partition_ratio', partition_ratio, float)
    regressor_top_k = arg('regressor_top_k', regressor_top_k, int)
    ignore_zero = arg('ignore_zero', ignore_zero, bool)

    accuracy_measure_list = {"mpe":"mpe", "mse":"mse", "rmse":"rmse", "mape":"mape"}
    if accuracy_measure:
        if isinstance(accuracy_measure, str):
            accuracy_measure = [accuracy_measure]
        for acc in accuracy_measure:
            arg('accuracy_measure', acc.lower(), accuracy_measure_list)
        accuracy_measure = [acc.upper() for acc in accuracy_measure]

    conn = data.connection_context
    cols = data.columns
    index = data.index
    key = arg('key', key, str, not isinstance(index, str))
    if isinstance(index, str):
        if key is not None and index != key:
            warn_msg = f"Discrepancy between the designated key column '{key}' and the designated index column '{index}'."
            logger.warning(warn_msg)
    key = index if key is None else key
    cols.remove(key)
    endog = arg('endog', endog, str)
    if endog is None:
        endog = cols[0]
    cols.remove(endog)
    if isinstance(exog, str):
        exog = [exog]
    exog = arg('exog', exog, ListOfStrings)
    if exog is None:
        exog = cols
    if model is None: # model free
        pred_data = data[[key] + [endog] + exog]
        model_df = conn.sql("SELECT TOP 0 * FROM (SELECT NULL ROW_INDEX, NULL MODEL_CONTENT  FROM DUMMY) dt;").cast({"ROW_INDEX": "INTEGER", "MODEL_CONTENT ": "NVARCHAR(5000)"})
        real_data = conn.sql("SELECT TOP 0 * FROM (SELECT NULL ID, NULL Y FROM DUMMY) dt;").cast({"ID": "INTEGER", "Y": "DOUBLE"})
    else: # specific model
        pred_data = data[[key] + exog]
        real_data = data[[key] + [endog]]
        model_df = model

    is_index_int = _is_index_int(pred_data, key)
    if not is_index_int:
        if model and ('SEASONALITY_MODE' not in str(model.collect().iat[0,1])):
            pred_data = _convert_index_from_timestamp_to_int(pred_data, key)
            real_data = _convert_index_from_timestamp_to_int(real_data, key)
        if model is None:
            pred_data = _convert_index_from_timestamp_to_int(pred_data, key)

    param_rows = [('REPEAT_TIME', repeat_time, None, None),
                  ('SEED', random_state, None, None),
                  ('THREAD_RATIO', None, thread_ratio, None),
                  ('PARTITION_RATIO', None, partition_ratio, None),
                  ('REGRESSOR_TOP_K', regressor_top_k, None, None),
                  ('IGNORE_ZERO', ignore_zero, None, None)]

    if model is None:
        param_rows.extend([('AUTO_MODEL', 0, None, None)])
        param_rows.extend([('METHOD', 30, None, None)])

    if accuracy_measure is not None:
        for acc in accuracy_measure:
            param_rows.extend([('MEASURE_NAME', None, None, acc)])

    unique_id = str(uuid.uuid1()).replace('-', '_').upper()
    res_tbl = f'#PAL_PERMUTATION_IMPORTANT_TS_RESULT_TBL_{unique_id}'
    if not (check_pal_function_exist(conn, '%PERMUTATION_FEATURE%', like=True) or
    conn.disable_hana_execution):
        msg = 'The version of your SAP HANA does not support permutation_importance!'
        logger.error(msg)
        raise ValueError(msg)
    try:
        sql, _   = call_pal_auto_with_hint(conn,
                                           None,
                                           'PAL_TS_PERMUTATION_FEATURE_IMPORTANCE',
                                           pred_data,
                                           ParameterTable().with_data(param_rows),
                                           model_df,
                                           real_data,
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
    return conn.table(res_tbl)
