"""
This module contains Python wrapper for SAP HANA PAL Unified Exponential Smoothing.

The following classes are available:
    * :class:`UnifiedExponentialSmoothing`
"""
#pylint: disable=too-many-lines, too-many-branches, unused-argument, use-a-generator
#pylint: disable=line-too-long, too-many-statements, consider-using-dict-items
#pylint: disable=too-many-locals, too-many-instance-attributes, c-extension-no-member
#pylint: disable=too-many-arguments, invalid-name, unnecessary-pass, too-many-nested-blocks
#pylint: disable=ungrouped-imports, bare-except, super-with-arguments
#pylint: disable=consider-using-f-string, attribute-defined-outside-init
#pylint: disable=broad-except, no-member, access-member-before-definition
import logging
import uuid
from hdbcli import dbapi
from hana_ml.ml_base import try_drop, quotename
from hana_ml.visualizers.report_builder import Page
from hana_ml.visualizers.time_series_report import TimeSeriesExplainer
from hana_ml.visualizers.time_series_report_template_helper import TimeSeriesTemplateReportHelper
from .tsa.arima import _convert_index_from_timestamp_to_int, _is_index_int
from .tsa.arima import _get_forecast_starttime_and_timedelta, _col_index_check
from .utility import _map_param
from .sqlgen import trace_sql
from .pal_base import (
    arg,
    PALBase,
    ParameterTable,
    require_pal_usable,
    pal_param_register,
    ListOfTuples,
    ListOfStrings)
logger = logging.getLogger(__name__)

def _params_check(input_dict, param_map, func):
    update_params = {}
    for parm in input_dict:
        if parm in param_map.keys():
            if parm == 'delta':
                delta_val = input_dict.get('delta')
                adaptive_method_val = input_dict.get('adaptive_method')
                if delta_val is not None and adaptive_method_val is not True:
                    msg = 'delta is only valid when adaptive_method is True!'
                    logger.error(msg)
                    raise ValueError(msg)
                update_params['DELTA'] = (delta_val, float)
            elif parm == 'seasonal':
                if input_dict.get('seasonal') is not None:
                    input_dict['seasonal'] = arg('seasonal', input_dict['seasonal'], (int, str))
                    if isinstance(input_dict['seasonal'], str):
                        input_dict['seasonal'] = arg('seasonal', input_dict['seasonal'],
                                                     {"multiplicative": 0, "additive": 1})
                    if isinstance(input_dict['seasonal'], str):
                        update_params['SEASONAL'] = (input_dict.get('seasonal'), str)
                    else:
                        update_params['SEASONAL'] = (input_dict.get('seasonal'), int)
            elif parm == 'season_start':
                if input_dict.get('season_start') is not None:
                    input_dict['season_start'] = arg('season_start', input_dict['season_start'], ListOfTuples)
                    for element in input_dict['season_start']:
                        if len(element) != 2:
                            msg = 'The length of each tuple of season_start should be 2!'
                            logger.error(msg)
                            raise ValueError(msg)
                        if not isinstance(element[0], int):
                            msg = 'The type of the first element of the tuple of season_start should be int!'
                            logger.error(msg)
                            raise ValueError(msg)
                        if not isinstance(element[1], (float, int)):
                            msg = 'The type of the second element of the tuple of season_start should be float!'
                            logger.error(msg)
                            raise ValueError(msg)
                    update_params['SEASON_START'] = (input_dict['season_start'], ListOfTuples)
            elif parm == 'accuracy_measure':
                if input_dict.get('accuracy_measure') is not None:
                    ac = input_dict.get('accuracy_measure')
                    if isinstance(ac, str):
                        ac = [ac]
                    if func in ['AESM']:
                        if len(ac) != 1:
                            msg = "Please input accuracy_measure from 'mse' OR 'mape'!"
                            logger.error(msg)
                            raise ValueError(msg)
                        arg('accuracy_measure', ac[0].lower(), {'mse':'mse', 'mape':'mape'})
                    acc_list = {"mpe":"mpe", "mse":"mse", "rmse":"rmse", "et":"et",
                                "mad":"mad", "mase":"mase", "wmape":"wmape",
                                "smape":"smape", "mape":"mape"}
                    if func not in ['AESM']:
                        for acc in ac:
                            acc = acc.lower()
                            arg('accuracy_measure', acc, acc_list)
                    update_params['MEASURE_NAME'] = (ac, ListOfStrings)
            elif parm == 'trend_test_method':
                if input_dict.get('trend_test_method') is not None:
                    tt_meth = input_dict.get('trend_test_method')
                tt_meth = arg('trend_test_method', tt_meth, (int, str))
                if isinstance(tt_meth, str):
                    tt_meth = arg('trend_test_method', tt_meth,
                                  {'mk': 1, 'difference-sign': 2})
                update_params['TREND_TEST_METHOD'] = (tt_meth, int)
            else:
                parm_val = input_dict[parm]
                arg_map = param_map[parm]
                if arg_map[1] == ListOfStrings and isinstance(parm_val, str):
                    parm_val = [parm_val]
                if len(arg_map) == 2:
                    update_params[arg_map[0]] = (arg(parm, parm_val, arg_map[1]), arg_map[1])
                else:
                    update_params[arg_map[0]] = (arg(parm, parm_val, arg_map[2]), arg_map[1])
        else:
            err_msg = f"'{parm}' is not a valid parameter name for initializing a {func} model!"
            logger.error(err_msg)
            raise KeyError(err_msg)

    return update_params

class UnifiedExponentialSmoothing(PALBase):
    """
    The Python wrapper for SAP HANA PAL Unified Exponential Smoothing function.
    The Unified Exponential Smoothing algorithms include:

    - SESM (Single Exponential Smoothing)
    - DESM (Double Exponential Smoothing)
    - TESM (Triple Exponential Smoothing)
    - BESM (Brown Exponential Smoothing)
    - AESM (Auto Exponential Smoothing)

    Parameters
    ----------

    func : str

        The name of a specified exponential smoothing algorithm.

        The following algorithms are supported:

        - 'SESM' : Single Exponential Smoothing.
        - 'DESM' : Double Exponential Smoothing.
        - 'TESM' : Triple Exponential Smoothing.
        - 'BESM' : Brown Exponential Smoothing.
        - 'AESM' : Auto Exponential Smoothing.

    massive : bool, optional
        Specifies whether or not to use massive mode.

        - True : massive mode.
        - False : single mode.

        For parameter setting in massive mode, you could use both
        group_params (please see the example below) or the original parameters.
        Using original parameters will apply for all groups. However, if you define some parameters of a group,
        the value of all original parameter setting will be not applicable to such group.

        An example is as follows:

        .. only:: latex

            >>> msesm = UnifiedExponentialSmoothing(func='sesm',
                                                    massive=True,
                                                    accuracy_measure='mse',
                                                    group_params={'Group_1': {'adaptive_method':False}})

        .. raw:: html

            <iframe allowtransparency="true" style="border:1px solid #ccc; background: #eeffcb;"
                 src="../../_static/uni_exp_example.html" width="100%" height="60%" sandbox="">
            </iframe>

        In this example, as 'adaptive_method' is set in group_params for Group_1,
        'accuracy_measure' is not applicable to Group_1.

        Defaults to False.

    group_params : dict, optional
        If massive mode is activated (``massive`` is True), input data for exponential smoothing shall be divided into different
        groups with different exponential smoothing parameters applied. This parameter specifies the parameter
        values of the chosen exponential smoothing algorithm ``func`` w.r.t. different groups in a dict format,
        where keys corresponding to group ids while values should be a dict for exponential smoothing algorithm
        parameter value assignments.

        An example is as follows:

        .. only:: latex

            >>> msesm = UnifiedExponentialSmoothing(func='sesm',
                                                    massive=True,
                                                    accuracy_measure='mse',
                                                    group_params={'Group_1': {'adaptive_method':False}})

        .. raw:: html

            <iframe allowtransparency="true" style="border:1px solid #ccc; background: #eeffcb;"
                 src="../../_static/uni_exp_example.html" width="100%" height="60%" sandbox="">
            </iframe>

        Valid only when ``massive`` is True and defaults to None.

    **kwargs : keyword arguments

        Arbitrary keyword arguments and please referred to the responding algorithm for the parameters' key-value pair.

        - **'SESM'** : :class:`~hana_ml.algorithms.pal.tsa.exponential_smoothing.SingleExponentialSmoothing`

        - **'DESM'** : :class:`~hana_ml.algorithms.pal.tsa.exponential_smoothing.DoubleExponentialSmoothing`

        - **'TESM'** : :class:`~hana_ml.algorithms.pal.tsa.exponential_smoothing.TripleExponentialSmoothing`

        - **'AESM'** : :class:`~hana_ml.algorithms.pal.tsa.exponential_smoothing.AutoExponentialSmoothing`

        - **'BESM'** : :class:`~hana_ml.algorithms.pal.tsa.exponential_smoothing.BrownExponentialSmoothing`

        For more parameter mappings of hana_ml and HANA PAL, please refer to the doc page: :ref:`param_mapping`

    Attributes
    ----------

    forecast_ : DataFrame
        Forecast values.

    stats_ : DataFrame
        Statistics.

    error_msg_ : DataFrame
        Error message.
        Only valid if ``massive`` is True when initializing an 'UnifiedExponentialSmoothing' instance.


    Examples
    --------
    >>> ub = UnifiedExponentialSmoothing(func='besm',
                                         alpha=0.1,
                                         forecast_num=6,
                                         adaptive_method=False,
                                         accuracy_measure='mse',
                                         expost_flag=True)

    Perform fit_predict():

    >>> ub.fit_predict(data=df)

    Output:

    >>> ub.forecast_.collect()
    >>> ub.stats_.collect()

    """

    func_dict = {'sesm' : 'SESM',
                 'desm' : 'DESM',
                 'tesm' : 'TESM',
                 'besm' : 'BESM',
                 'aesm' : 'AESM'}

    _sesm_param = {'alpha': ('ALPHA', float),
                   'delta': ('DELTA', float),
                   'forecast_num': ('FORECAST_NUM', int),
                   'accuracy_measure': ('ACCURACY_MEASURE', str),
                   'adaptive_method': ('ADAPTIVE_METHOD', bool),
                   'ignore_zero': ('IGNORE_ZERO', bool),
                   'expost_flag': ('EXPOST_FLAG', bool),
                   'prediction_confidence_1': ('PREDICTION_CONFIDENCE_1', float),
                   'prediction_confidence_2': ('PREDICTION_CONFIDENCE_2', float),
                   'decom_state': ('DECOM_STATE', bool)
                   }

    _desm_param = {'alpha': ('ALPHA', float),
                   'beta': ('BETA', float),
                   'forecast_num': ('FORECAST_NUM', int),
                   'accuracy_measure': ('ACCURACY_MEASURE', str),
                   'phi': ('PHI', float),
                   'damped': ('DAMPED', bool),
                   'ignore_zero': ('IGNORE_ZERO', bool),
                   'expost_flag': ('EXPOST_FLAG', bool),
                   'prediction_confidence_1': ('PREDICTION_CONFIDENCE_1', float),
                   'prediction_confidence_2': ('PREDICTION_CONFIDENCE_2', float),
                   'decom_state': ('DECOM_STATE', bool)
                   }

    _tesm_param = {'alpha': ('ALPHA', float),
                   'beta': ('BETA', float),
                   'gamma': ('GAMMA', float),
                   'seasonal_period': ('CYCLE', int),
                   'forecast_num': ('FORECAST_NUM', int),
                   'accuracy_measure': ('ACCURACY_MEASURE', str),
                   'seasonal': ('SEASONAL', (int, str)),
                   'initial_method': ('INITIAL_METHOD', int),
                   'phi': ('PHI', float),
                   'damped': ('DAMPED', bool),
                   'ignore_zero': ('IGNORE_ZERO', bool),
                   'expost_flag': ('EXPOST_FLAG', bool),
                   'level_start': ('LEVEL_START', float),
                   'trend_start': ('TREND_START', float),
                   'season_start': ('SEASON_START', list),
                   'prediction_confidence_1': ('PREDICTION_CONFIDENCE_1', float),
                   'prediction_confidence_2': ('PREDICTION_CONFIDENCE_2', float),
                   'decom_state': ('DECOM_STATE', bool)
                   }

    _aesm_param = {'model_selection': ('MODELSELECTION', bool),
                   'forecast_model_name': ('FORECAST_MODEL_NAME', str),
                   'optimizer_time_budget': ('OPTIMIZER_TIME_BUDGET', int),
                   'max_iter': ('MAX_ITERATION', int),
                   'optimizer_random_seed': ('OPTIMIZER_RANDOM_SEED', int),
                   'thread_ratio': ('THREAD_RATIO', float),
                   'expost_flag': ('EXPOST_FLAG', bool),
                   'alpha': ('ALPHA', float),
                   'beta': ('BETA', float),
                   'gamma': ('GAMMA', float),
                   'phi': ('PHI', float),
                   'forecast_num': ('FORECAST_NUM', int),
                   'seasonal_period': ('CYCLE', int),
                   'seasonal': ('SEASONAL', (int, str)),
                   'initial_method': ('INITIAL_METHOD', int),
                   'training_ratio': ('TRAINING_RATIO', float),
                   'damped': ('DAMPED', bool),
                   'accuracy_measure': ('ACCURACY_MEASURE', str),
                   'seasonality_criterion': ('SEASONALITY_CRITERION', float),
                   'trend_test_method': ('TREND_TEST_METHOD', (int, str)),
                   'trend_test_alpha': ('TREND_TEST_ALPHA', float),
                   'alpha_min': ('ALPHA_MIN', float),
                   'beta_min': ('BETA_MIN', float),
                   'gamma_min': ('GAMMA_MIN', float),
                   'phi_min': ('PHI_MIN', float),
                   'alpha_max': ('ALPHA_MAX', float),
                   'beta_max': ('BETA_MAX', float),
                   'gamma_max': ('GAMMA_MAX', float),
                   'phi_max': ('PHI_MAX', float),
                   'prediction_confidence_1': ('PREDICTION_CONFIDENCE_1', float),
                   'prediction_confidence_2': ('PREDICTION_CONFIDENCE_2', float),
                   'level_start': ('LEVEL_START', float),
                   'trend_start': ('TREND_START', float),
                   'season_start': ('SEASON_START', list),
                   'decom_state': ('DECOM_STATE', bool)
                   }

    _besm_param = {'alpha': ('ALPHA', float),
                   'delta': ('DELTA', float),
                   'forecast_num': ('FORECAST_NUM', int),
                   'accuracy_measure': ('ACCURACY_MEASURE', str),
                   'adaptive_method': ('ADAPTIVE_METHOD', bool),
                   'ignore_zero': ('IGNORE_ZERO', bool),
                   'expost_flag': ('EXPOST_FLAG', bool),
                   'prediction_confidence_1': ('PREDICTION_CONFIDENCE_1', float),
                   'prediction_confidence_2': ('PREDICTION_CONFIDENCE_2', float),
                   'decom_state': ('DECOM_STATE', bool)
                   }

    map_dict = {'SESM' : _sesm_param,
                'DESM' : _desm_param,
                'TESM' : _tesm_param,
                'BESM' : _besm_param,
                'AESM' : _aesm_param}

    def __init__(self,
                 func,
                 massive=False,
                 group_params=None,
                 **kwargs):
        super(UnifiedExponentialSmoothing, self).__init__()
        setattr(self, 'hanaml_parameters', pal_param_register())
        func = func.lower()
        self.func = self._arg('Function name', func, self.func_dict)
        self.__real_func = self.func if massive is False else "M" + self.func
        self.massive = self._arg('massive', massive, bool)
        func_map = self.map_dict[self.func]
        self.__pal_params = {}
        self.params = {**kwargs}
        if self.massive is not True:
            self.__pal_params = _params_check(input_dict=self.params,
                                              param_map=func_map,
                                              func=self.func)
        else:# massive mode
            group_params = self._arg('group_params', group_params, dict)
            group_params = {} if group_params is None else group_params
            for group in group_params:
                self._arg('Parameters with group_key ' + str(group), group_params[group], dict)
            self.group_params = group_params
            for group in self.group_params:
                self.__pal_params[group] = {}
                self.__pal_params[group] = _params_check(input_dict=self.group_params[group],
                                                         param_map=func_map,
                                                         func=self.func)
            if self.params:
                special_group_name = 'PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID'
                self.__pal_params[special_group_name] = _params_check(input_dict=self.params,
                                                                      param_map=func_map,
                                                                      func=self.func)

        self.forecast_ = None
        self.stats_ = None
        self.statistics_ = self.stats_
        self.error_msg_ = None
        self.is_index_int = True
        self.forecast_start = None
        self.timedelta = None
        self.fit_data = None

    @trace_sql
    def fit_predict(self,
                    data,
                    key=None,
                    endog=None,
                    group_key=None,
                    build_report=False):
        """
        fit_prediction function for unified exponential smoothing.

        Parameters
        ----------
        data : DataFrame
            Training data.

        key : str, optional

            Name of ID column.

            Defaults to the first column of data if the index column of data is not provided.
            Otherwise, defaults to the index column of data.

        endog : str, optional
            The column of series to be fitted and predicted.

            Defaults to the first column of data after eliminating key column.

        group_key : str, optional
            The column of group_key. Data type can be INT or NVARCHAR/VARCHAR.

            If data type is INT, only parameters specified in ``group_params``
            in class instance initialization are valid.

            This parameter is only valid when massive mode is activated(i.e. ``massive``
            is set as True in class instance initialization).

            Defaults to the first column of data if the index columns of data is not provided.
            Otherwise, defaults to the first column of index columns.

        build_report : bool, optional
            Whether to build a time series report or not.

            Example:

            >>> from hana_ml.visualizers.unified_report import UnifiedReport
            >>> ub = UnifiedExponentialSmoothing(func='besm')
            >>> ub.fit_predict(data=df, build_report=True)
            >>> UnifiedReport(ub).display()

            Defaults to False.

        Returns
        -------
        DataFrame

            Forecast values.

        """
        conn = data.connection_context
        require_pal_usable(conn)
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        setattr(self, 'key', key)
        setattr(self, 'endog', endog)
        setattr(self, 'exog', None)
        cols = data.columns
        index = data.index
        self.is_index_int = True

        expect_len = 3 if self.massive is True else 2
        if len(cols) < expect_len:
            msg = ("Input data should contain at least 2 columns: " +\
                   "" if expect_len == 2 else "group_key, " +\
                   "key and endog.")
            logger.error(msg)
            raise ValueError(msg)

        if self.massive is True:
            group_key = self._arg('group_key', group_key, str)
            if index is not None:
                group_key = _col_index_check(group_key, 'group_key', index[0], cols)
            else:
                if group_key is None:
                    group_key = cols[0]
            if group_key is not None and group_key not in cols:
                msg = f"Please select group_key from {cols}!"
                logger.error(msg)
                raise ValueError(msg)
            data_groups = list(data[[group_key]].collect()[group_key].drop_duplicates())
            param_keys = list(self.group_params.keys())
            if not self._disable_hana_execution:
                gid_type = data[[group_key]].dtypes()[0]
                if not all([(int(ky) if 'INT' in gid_type[1] else ky) in data_groups for ky in param_keys]):
                    msg = 'Invalid group key identified in group parameters!'
                    logger.error(msg)
                    raise ValueError(msg)
            else:
                gid_type = {tp[0]:(tp[0], tp[1], tp[2]) for tp in data.dtypes()}[group_key]
            cols.remove(group_key)

            key = self._arg('key', key, str)
            if index is not None:
                key = _col_index_check(key, 'key', index[1], cols)
            else:
                if key is None:
                    key = cols[0]
        else: # single mode
            key = self._arg('key', key, str)
            if index is not None:
                key = _col_index_check(key, 'key', index, cols)
            else:
                if key is None:
                    key = cols[0]

        # for both modes
        key = self._arg('key', key, str)
        if key is not None and key not in cols:
            msg = f"Please select key from {cols}!"
            logger.error(msg)
            raise ValueError(msg)
        cols.remove(key)

        endog = self._arg('endog', endog, str)
        if endog is not None:
            if endog not in cols:
                msg = f"Please select endog from {cols}!"
                logger.error(msg)
                raise ValueError(msg)
        else:
            endog = cols[0]

        if self.massive is not True: # single mode
            data_ = data[[key] + [endog]]
            self.is_index_int = _is_index_int(data_, key)
            if not self.is_index_int:
                data_ = _convert_index_from_timestamp_to_int(data_, key)
            try:
                self.forecast_start, self.timedelta = _get_forecast_starttime_and_timedelta(data, key, self.is_index_int)
            except Exception as err:
                logger.warning(err)
            param_rows = [('FUNCTION', None, None, self.func)]
            for name in self.__pal_params:
                value, typ = self.__pal_params[name]
                if name == 'MEASURE_NAME':
                    if isinstance(value, str):
                        value = [value]
                    for each_ac in value:
                        param_rows.extend([('ACCURACY_MEASURE', None, None, each_ac)])
                        param_rows.extend([('MEASURE_NAME', None, None, each_ac)])
                elif name == 'SEASON_START':
                    param_rows.extend([('SEASON_START', element[0], element[1], None)
                                       for element in value])
                elif name == 'SEASONAL':
                    param_rows.extend([('SEASONAL', value, None, None)])
                else:
                    tpl = [_map_param(name, value, typ)]
                    param_rows.extend(tpl)
        else: # massive mode
            data_ = data[[group_key, key, endog]]
            self.is_index_int = _is_index_int(data_, key)
            if not self.is_index_int:# timestamp
                recomb_data = None
                self.forecast_start = {}
                self.timedelta = {}
                group_count = {}
                for group in data_groups:
                    group_val = f"'{group}'"
                    group_data = data_.filter("{}={}".format(quotename(data_.dtypes()[0][0]),
                                                             group_val)).sort(data_.dtypes()[0][0])
                    group_count[group] = group_data.count()
                    try:
                        self.forecast_start[group], self.timedelta[group] =\
                        _get_forecast_starttime_and_timedelta(group_data,
                                                              key,
                                                              self.is_index_int)
                    except Exception as err:
                        logger.warning(err)
                    group_data = _convert_index_from_timestamp_to_int(group_data, key)
                    if recomb_data is None:
                        recomb_data = group_data
                    else:
                        recomb_data = recomb_data.union(group_data)
                data_ = recomb_data[[group_key, key+'(INT)', endog]]

            param_rows = [(None, 'FUNCTION', None, None, self.__real_func)]

            for group in self.__pal_params:
                for name in self.__pal_params[group]:
                    value, typ = self.__pal_params[group][name]
                    if name == 'MEASURE_NAME':
                        if isinstance(value, str):
                            value = [value]
                        for each_ac in value:
                            param_rows.extend([(group, 'ACCURACY_MEASURE', None, None, each_ac)])
                            param_rows.extend([(group, 'MEASURE_NAME', None, None, each_ac)])
                    elif name == 'SEASON_START':
                        for element in value:
                            param_rows.extend([(group, 'SEASON_START', element[0], element[1], None)])
                    elif name == 'SEASONAL':
                        param_rows.extend([(group, 'SEASONAL', value, None, None)])
                    else:
                        tpl = [tuple([group] + list(_map_param(name, value, typ)))]
                        param_rows.extend(tpl)

        self.fit_data = data_
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        outputs = ['FORECAST', 'STATS', 'ERROR_MSG', 'PLACE_HOLDER1', 'PLACE_HOLDER2']
        outputs = ['#PAL_UNIFIED_EXPS_{}_{}_{}'.format(tbl, self.id, unique_id)
                   for tbl in outputs]
        forecast_tbl, stats_tbl, error_msg_tbl, _, _ = outputs
        try:
            self._call_pal_auto(conn,
                                'PAL_UNIFIED_EXPONENTIALSMOOTHING',
                                data_,
                                ParameterTable().with_data(param_rows),
                                *outputs)
        except dbapi.Error as db_err:
            logger.error(str(db_err))
            try_drop(conn, outputs)
            raise
        except Exception as db_err:
            logger.error(str(db_err))
            try_drop(conn, outputs)
            raise

        #pylint: disable=attribute-defined-outside-init
        self.forecast_ = conn.table(forecast_tbl)
        self.stats_ = conn.table(stats_tbl)
        self.statistics_ = self.stats_
        self.error_msg_ = conn.table(error_msg_tbl)
        if not self._disable_hana_execution:
            if not self.error_msg_.collect().empty:
                row = self.error_msg_.count()
                for i in range(1, row+1):
                    warn_msg = "For group_key '{}',".format(self.error_msg_.collect()['GROUP_ID'][i-1]) +\
                               " the error message is '{}'.".format(self.error_msg_.collect()['MESSAGE'][i-1]) +\
                               "More information could be seen in the attribute error_msg_!"
                    logger.warning(warn_msg)

        if not self.is_index_int:
            if self.massive is not True: # single mode
                fct_ = conn.sql("""
                                SELECT {0},
                                ADD_SECONDS('{1}', ({2}-{10}) * {3}) AS {12},
                                {5} AS {13},
                                {6},
                                {7},
                                {8},
                                {9},
                                {11}
                                FROM ({4})
                                """.format(quotename(self.forecast_.columns[0]),
                                           self.forecast_start,
                                           quotename(self.forecast_.columns[1]),
                                           self.timedelta,
                                           self.forecast_.select_statement,
                                           quotename(self.forecast_.columns[2]),
                                           quotename(self.forecast_.columns[3]),
                                           quotename(self.forecast_.columns[4]),
                                           quotename(self.forecast_.columns[5]),
                                           quotename(self.forecast_.columns[6]),
                                           data.count() + 1,
                                           quotename(self.forecast_.columns[7]),
                                           quotename(key),
                                           quotename(endog)))
            else: # massive mode
                comb_data = None
                fct = self.forecast_
                for group in data_groups:
                    group_val = f"'{group}'"
                    group_fct = fct.filter('GROUP_ID={}'.format(group_val)).sort('TIMESTAMP')
                    group_fct = conn.sql("""
                                         SELECT {0},
                                         ADD_SECONDS('{1}', ({2}-{10}) * {3}) AS {12},
                                         {5} AS {13},
                                         {6},
                                         {7},
                                         {8},
                                         {9},
                                         {11}
                                         FROM ({4})
                                         """.format(quotename(self.forecast_.columns[0]),
                                                    self.forecast_start[group],
                                                    quotename(self.forecast_.columns[1]),
                                                    self.timedelta[group],
                                                    group_fct.select_statement,
                                                    quotename(self.forecast_.columns[2]),
                                                    quotename(self.forecast_.columns[3]),
                                                    quotename(self.forecast_.columns[4]),
                                                    quotename(self.forecast_.columns[5]),
                                                    quotename(self.forecast_.columns[6]),
                                                    group_count[group] + 1,
                                                    quotename(self.forecast_.columns[7]),
                                                    quotename(key),
                                                    quotename(endog)))

                    if comb_data is None:
                        comb_data = group_fct
                    else:
                        comb_data = group_fct.union(comb_data)
                fct_ = comb_data.sort(['GROUP_ID', key])
            self.forecast_ = fct_
        setattr(self, "forecast_result", self.forecast_)
        if build_report:
            self.build_report()
        return self.forecast_

    def build_report(self):
        r"""
        Generate the time series report.

        Examples
        --------
        >>> from hana_ml.visualizers.unified_report import UnifiedReport
        >>> uexp = UnifiedExponentialSmoothing(func='besm')
        >>> uexp.fit_predict(data=df)
        >>> uexp.build_report()
        >>> UnifiedReport(uexp).display()
        """
        if self.key is None:
            self.key = self.training_data.columns[0]
        if self.endog is None:
            self.endog = self.training_data.columns[1]
        if len(self.training_data.columns) > 2:
            if self.exog is None:
                self.exog = self.training_data.columns
                self.exog.remove(self.key)
                self.exog.remove(self.endog)
        self.report = TimeSeriesTemplateReportHelper(self)
        pages = []
        page0 = Page("Forecast Result Analysis")
        tse = TimeSeriesExplainer(key=self.key, endog=self.endog, exog=self.exog)
        tse.add_line_to_comparison_item("Training Data", data=self.training_data, x_name=self.key, y_name=self.endog)
        if hasattr(self, 'forecast_result'):
            if self.forecast_result:
                tse.add_line_to_comparison_item("Forecast Result",
                                                data=self.forecast_result,
                                                x_name=self.forecast_result.columns[1],
                                                y_name=self.forecast_result.columns[2])
                tse.add_line_to_comparison_item('PI1', data=self.forecast_result, x_name=self.forecast_result.columns[1], confidence_interval_names=[self.forecast_result.columns[3], self.forecast_result.columns[4]],color="pink")
                tse.add_line_to_comparison_item('PI2', data=self.forecast_result, x_name=self.forecast_result.columns[1], confidence_interval_names=[self.forecast_result.columns[5], self.forecast_result.columns[6]],color="#ccc")
        page0.addItems(tse.get_comparison_item())
        pages.append(page0)
        self.report.add_pages(pages)
        self.report.build_report()

    def generate_html_report(self, filename=None):
        """
        Display function.
        """
        self.report.generate_html_report(filename)

    def generate_notebook_iframe_report(self):
        """
        Display function.
        """
        self.report.generate_notebook_iframe_report()
