"""
This module contains Python wrapper for PAL auto ARIMA algorithm.

The following class is available:

    * :class:`AutoARIMA`
"""
#pylint: disable=too-many-instance-attributes, too-few-public-methods, invalid-name, too-many-statements, too-few-public-methods
#pylint: disable=too-many-lines, line-too-long, too-many-arguments, too-many-branches, too-many-locals, bare-except, attribute-defined-outside-init
#pylint: disable=broad-except, arguments-differ, unnecessary-pass, super-with-arguments, c-extension-no-member
#pylint: disable=use-a-generator, consider-using-dict-items, no-member
import logging
import uuid
from hdbcli import dbapi
from hana_ml.ml_base import quotename
from ..pal_base import (
    arg,
    ParameterTable,
    ListOfStrings,
    pal_param_register,
    try_drop,
    require_pal_usable
)
from ..sqlgen import trace_sql
from .arima import ARIMA
from .utility import _convert_index_from_timestamp_to_int, _is_index_int, _delete_none_key_in_dict, _validate_og
from .utility import _get_forecast_starttime_and_timedelta, _categorical_variable_update, _col_index_check
from ..utility import check_pal_function_exist, _map_param
logger = logging.getLogger(__name__)#pylint: disable=invalid-name

def _params_check(input_dict, param_map):
    update_params = {}
    if not input_dict or input_dict is None:
        return update_params

    for parm in input_dict:
        if parm in ['show_explainer', 'allow_new_index']:
            val = input_dict.get(parm)
            if val is not None:
                arg('{}'.format(parm), val, bool)
        elif parm ==  'categorical_variable':
            pass
        elif parm == 'information_criterion':
            value = input_dict.get('information_criterion')
            information_criterion = arg('information_criterion', value, (int, str))
            if isinstance(information_criterion, str):
                information_criterion = arg('information_criterion',
                                            information_criterion,
                                            {'aicc': 0, 'aic': 1, 'bic': 2})
            update_params['INFORMATION_CRITERION'] = (information_criterion, int)
        elif parm == 'search_strategy':
            value = input_dict.get('search_strategy')
            search_strategy = arg('search_strategy', value, (int, str))
            if isinstance(search_strategy, str):
                search_strategy = arg('search_strategy',
                                      search_strategy,
                                      {'exhaustive': 0, 'stepwise': 1})
            update_params['SEARCH_STRATEGY'] = (search_strategy, int)
        else:
            if parm in param_map.keys():
                parm_val = input_dict[parm]
                arg_map = param_map[parm]
                if arg_map[1] == ListOfStrings and isinstance(parm_val, str):
                    parm_val = [parm_val]
                if len(arg_map) == 2:
                    update_params[arg_map[0]] = (arg(parm, parm_val, arg_map[1]), arg_map[1])
                else:
                    update_params[arg_map[0]] = (arg(parm, parm_val, arg_map[2]), arg_map[1])
            else:
                err_msg = "'{}' is not a valid parameter name for initializing an auto ARIMA model!".format(parm)
                logger.error(err_msg)
                raise KeyError(err_msg)

    return update_params

class _AutoARIMABase(ARIMA):
    __init_param_dict = {'seasonal_period' : ('SEASONAL_PERIOD', int),
                         'seasonality_criterion' : ('SEASONALITY_CRITERION', float),
                         'd' : ('D',int),
                         'kpss_significance_level'  : ('KPSS_SIGNIFICANCE_LEVEL', float),
                         'max_d'  : ('MAX_D', int),
                         'seasonal_d' : ('SEASONAL_D', int),
                         'ch_significance_level' : ('CH_SIGNIFICANCE_LEVEL', float),
                         'max_seasonal_d' : ('MAX_SEASONAL_D', int),
                         'max_p' : ('MAX_P', int),
                         'max_q' : ('MAX_Q', int),
                         'max_seasonal_p'  : ('MAX_SEASONAL_P', int),
                         'max_seasonal_q'  : ('MAX_SEASONAL_Q', int),
                         'information_criterion' : ('INFORMATION_CRITERION', (int, str)),
                         'search_strategy' : ('SEARCH_STRATEGY', (int, str)),
                         'max_order' : ('MAX_ORDER', int),
                         'initial_p' : ('INITIAL_P', int),
                         'initial_q' : ('INITIAL_Q', int),
                         'initial_seasonal_p' : ('INITIAL_SEASONAL_P', int),
                         'initial_seasonal_q'  : ('INITIAL_SEASONAL_Q', int),
                         'guess_states'  : ('GUESS_STATES', int),
                         'max_search_iterations' : ('MAX_SEARCH_ITERATIONS', int),
                         'method' : ('METHOD', int, {'css':0, 'mle':1, 'css-mle':2}),
                         'allow_linear' : ('ALLOW_LINEAR', (int, bool)),
                         'forecast_method'  : ('FORECAST_METHOD', int, {'formula_forecast':0, 'innovations_algorithm':1}),
                         'output_fitted'  : ('OUTPUT_FITTED', bool),
                         'thread_ratio' : ('THREAD_RATIO', float),
                         'background_size' : ('BACKGROUND_SIZE', int)}

    def __init__(self,
                 seasonal_period=None,
                 seasonality_criterion=None,
                 d=None,
                 kpss_significance_level=None,
                 max_d=None,
                 seasonal_d=None,
                 ch_significance_level=None,
                 max_seasonal_d=None,
                 max_p=None,
                 max_q=None,
                 max_seasonal_p=None,
                 max_seasonal_q=None,
                 information_criterion=None,
                 search_strategy=None,
                 max_order=None,
                 initial_p=None,
                 initial_q=None,
                 initial_seasonal_p=None,
                 initial_seasonal_q=None,
                 guess_states=None,
                 max_search_iterations=None,
                 method=None,
                 allow_linear=None,
                 forecast_method=None,
                 output_fitted=None,
                 thread_ratio=None,
                 background_size=None,
                 massive=False,
                 group_params=None):

        if not hasattr(self, 'hanaml_parameters'):
            setattr(self, 'hanaml_parameters', pal_param_register())
        super(_AutoARIMABase, self).__init__()

        init_params = {'seasonal_period' : seasonal_period,
                       'seasonality_criterion' : seasonality_criterion,
                       'd' : d,
                       'kpss_significance_level' : kpss_significance_level,
                       'max_d' : max_d,
                       'seasonal_d' : seasonal_d,
                       'ch_significance_level' : ch_significance_level,
                       'max_seasonal_d' : max_seasonal_d,
                       'max_p' : max_p,
                       'max_q' : max_q,
                       'max_seasonal_p' : max_seasonal_p,
                       'max_seasonal_q' : max_seasonal_q,
                       'information_criterion' : information_criterion,
                       'search_strategy' : search_strategy,
                       'max_order' : max_order,
                       'initial_p' : initial_p,
                       'initial_q' : initial_q,
                       'initial_seasonal_p' : initial_seasonal_p,
                       'initial_seasonal_q' : initial_seasonal_q,
                       'guess_states' : guess_states,
                       'max_search_iterations' : max_search_iterations,
                       'method' : method,
                       'allow_linear' : allow_linear,
                       'forecast_method' : forecast_method,
                       'output_fitted' : output_fitted,
                       'thread_ratio' : thread_ratio,
                       'background_size' : background_size}

        init_params = _delete_none_key_in_dict(init_params)
        self.init_params = init_params
        self.__pal_params = {}

        self.massive = self._arg('massive', massive, bool)
        if self.massive is not True:
            self.__pal_params = _params_check(input_dict=init_params,
                                              param_map=self.__init_param_dict)

        else: # massive mode
            group_params = arg('group_params', group_params, dict)
            group_params = {} if group_params is None else group_params
            for group in group_params:
                self._arg('Parameters with group_key ' + str(group), group_params[group], dict)
            self.group_params = group_params

            for group in self.group_params:
                self.__pal_params[group] = _params_check(input_dict=self.group_params[group],
                                                         param_map=self.__init_param_dict)
            if init_params:
                special_group_name = 'PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID'
                self.__pal_params[special_group_name] = _params_check(input_dict=init_params,
                                                                      param_map=self.__init_param_dict)

        self.forecast_start = None
        self.timedelta = None
        self.is_index_int = True
        self.data_groups = None
        self.explainer_ = None

    @trace_sql
    def _fit(self, data, endog, group_params, categorical_variable):
        conn = data.connection_context
        require_pal_usable(conn)
        self.conn_context = conn
        param_rows = []

        if self.massive is not True: # single mode
            for name in self.__pal_params:
                value, typ = self.__pal_params[name]
                tpl = [_map_param(name, value, typ)]
                param_rows.extend(tpl)

            tpl = [('DEPENDENT_VARIABLE', None, None, endog)]
            param_rows.extend(tpl)

            categorical_variable = _categorical_variable_update(categorical_variable)
            if categorical_variable:
                param_rows.extend([('CATEGORICAL_VARIABLE', None, None, var) for var in categorical_variable])

            unique_id = str(uuid.uuid1()).replace('-', '_').upper()
            outputs = ['MODEL', 'FIT']
            outputs = ['#PAL_AUTOARIMA_{}_TBL_{}_{}'.format(name, self.id, unique_id)
                       for name in outputs]
            model_tbl, fit_tbl = outputs
            try:
                self._call_pal_auto(conn,
                                    'PAL_AUTOARIMA',
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
            self.model_ = conn.table(model_tbl)
            self.fitted_ = conn.table(fit_tbl)
        else: # massive mode
            special_group_name = 'PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID'
            categorical_variable = _categorical_variable_update(categorical_variable)
            if categorical_variable is not None:
                param_rows.extend([(special_group_name, 'CATEGORICAL_VARIABLE', None, None, var) for var in categorical_variable])

            # for each group, only categorical_variable could be set in this algorithm fit()
            for group in group_params:
                if group in ['PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID']:
                    continue
                group_categorical_variable = None
                if group_params[group].get('categorical_variable') is not None:
                    group_categorical_variable = group_params[group]['categorical_variable']
                group_categorical_variable = _categorical_variable_update(group_categorical_variable)
                if group_categorical_variable:
                    param_rows.extend([(group, 'CATEGORICAL_VARIABLE', None, None, var) for var in group_categorical_variable])

            for group in self.__pal_params:
                for name in self.__pal_params[group]:
                    value, typ = self.__pal_params[group][name]
                    tpl = [tuple([group] + list(_map_param(name, value, typ)))]
                    param_rows.extend(tpl)

            unique_id = str(uuid.uuid1()).replace('-', '_').upper()
            outputs = ['MODEL', 'FIT', 'ERROR']
            outputs = ['#PAL_MASSIVE_AUTOARIMA_{}_TBL_{}_{}'.format(name, self.id, unique_id)
                       for name in outputs]
            model_tbl, fit_tbl, errormsg_tbl = outputs
            if not (check_pal_function_exist(conn, '%MASSIVE_AUTOARIMA%', like=True) or self._disable_hana_execution):
                msg = 'The version of your SAP HANA does not support massive AutoARIMA!'
                logger.error(msg)
                raise ValueError(msg)
            try:
                self._call_pal_auto(conn,
                                    'PAL_MASSIVE_AUTOARIMA',
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

            self.model_ = conn.table(model_tbl)
            self.fitted_ = conn.table(fit_tbl)
            self.error_msg_ = None

            setattr(self, 'fit_data', data)
            if self.massive is True:
                if not self._disable_hana_execution:
                    self.error_msg_ = conn.table(errormsg_tbl)
                    if not self.error_msg_.collect().empty:
                        row = self.error_msg_.count()
                        for i in range(1, row+1):
                            warn_msg = "For group_key '{}',".format(self.error_msg_.collect()['GROUP_ID'][i-1]) +\
                                       " the error message is '{}'.".format(self.error_msg_.collect()['ERROR_MESSAGE'][i-1]) +\
                                       "More information could be seen in the attribute error_msg_!"
                            logger.warning(warn_msg)

class AutoARIMA(_AutoARIMABase):
    r"""
    The ARIMA model, a potent tool in time series analysis, can be challenging due to the difficulty in selecting suitable parameters.
    AutoARIMA automates this selection process. This model includes seven parameters (p, d, q, P, D, Q, and m),
    where the seasonality (m) can be estimated using the seasonal_decompose() function, and 'd' and 'D' are usually determined first due to information criterion considerations.
    The optimal values of p, q, P, Q are obtained through two main methods: 'exhaustive search,' which tests all possible combinations but can be time-consuming, and 'stepwise search,'
    which is more efficient but may not yield the optimal result. The constant part's inclusion depends on the criterion information, mainly when d + D isn't more than 1.

    Parameters
    ----------
    seasonal_period : int, optional
        Value of the seasonal period.

        - Negative: Automatically identify seasonality by means of an auto-correlation scheme.
        - 0 or 1: Non-seasonal.
        - Others: Seasonal period.

        Defaults to -1.
    seasonality_criterion : float, optional
        The criterion of the auto-correlation coefficient for accepting seasonality,
        in the range of (0, 1).
        The larger it is, the less probable a time series is regarded to be seasonal. Valid only when ``seasonal_period`` is negative.

        Defaults to 0.2.
    D : int, optional
        Order of first-differencing.

        - Others: Uses the specified value as the first-differencing order.
        - Negative: Automatically identifies the first-differencing order with the KPSS test.

        Defaults to -1.
    kpss_significance_level : float, optional
        The significance level for the KPSS test. Supported values are 0.01, 0.025, 0.05, and 0.1.
        The smaller it is, the more probable a time series is considered as first-stationary,
        that is, the less probable it needs first-differencing.
        Valid only when ``D`` is negative.

        Defaults to 0.05.
    max_d : int, optional
        The maximum value of D when the KPSS test is applied.

        Defaults to 2.
    seasonal_d : int, optional
        Order of seasonal-differencing.

          - Negative: Automatically identifies seasonal-differencing order using the Canova-Hansen test.
          - Others: Uses the specified value as the seasonal-differencing order.

        Defaults to -1.
    ch_significance_level : float, optional

        The significance level for the Canova-Hansen test. Supported values are 0.01, 0.025,
        0.05, 0.1, and 0.2.
        The smaller it is, the more probable a time series
        is considered seasonal-stationary; that is, the less probable it needs
        seasonal-differencing.

        Valid only when ``seasonal_d`` is negative.

        Defaults to 0.05.
    max_seasonal_d : int, optional
        The maximum value of ``seasonal_d`` when the Canova-Hansen test is applied.

        Defaults to 1.
    max_p : int, optional
        The maximum value of AR order p.

        Defaults to 5.
    max_q : int, optional
        The maximum value of MA order q.

        Defaults to 5.
    max_seasonal_p : int, optional
        The maximum value of SAR order P.

        Defaults to 2.
    max_seasonal_q : int, optional
        The maximum value of SMA order Q.

        Defaults to 2.
    information_criterion : {'aicc', 'aic', 'bic'}, optional
        The information criterion for order selection.

        - 'aicc': Akaike information criterion with correction (for small sample sizes)
        - 'aic': Akaike information criterion
        - 'bic': Bayesian information criterion

        Defaults to 'aicc'.
    search_strategy : {'exhaustive', 'stepwise'}, optional
        Specifies the search strategy for the optimal ARMA model.

        - 'exhaustive': exhaustive traverse.
        - 'stepwise': stepwise traverse.

        Defaults to 'stepwise'.
    max_order : int, optional
        The maximum value of (``max_p`` + ``max_q`` + ``max_seasonal_p`` + ``max_seasonal_q``). \
        Valid only when ``search_strategy`` is 'exhaustive'.

        Defaults to 15.
    initial_p : int, optional
        Order p of the user-defined initial model.
        Valid only when ``search_strategy`` is 'stepwise'.

        Defaults to 0.
    initial_q : int, optional
        Order q of the user-defined initial model.
        Valid only when ``search_strategy`` is 'stepwise'.

        Defaults to 0.
    initial_seasonal_p : int, optional
        Order seasonal_p of the user-defined initial model.
        Valid only when ``search_strategy`` is 'stepwise'.

        Defaults to 0.
    initial_seasonal_q : int, optional
        Order seasonal_q of the user-defined initial model.
        Valid only when ``search_strategy`` is 'stepwise'.

        Defaults to 0.
    guess_states : int, optional
        If employing ACF/PACF to guess initial ARMA models, besides the user-defined model:

            - 0: No guess. Besides the user-defined model, uses states (2, 2) (1, 1)m, (1, 0) (1, 0)m,
              and (0, 1) (0, 1)m meanwhile as starting states.

            - 1: Guesses starting states taking advantage of ACF/PACF.

        Valid only when ``search_strategy`` is 'stepwise'.

        Defaults to 1.
    max_search_iterations : int, optional
        The maximum iterations for searching optimal ARMA states.

        Valid only when ``search_strategy`` is 'stepwise'.

        Defaults to (``max_p`` + 1) * (``max_q`` + 1) * (``max_seasonal_p`` + 1) * (``max_seasonal_q`` + 1).
    method : {'css', 'mle', 'css-mle'}, optional
        The objective function for numeric optimization

        - 'css': use the conditional sum of squares.
        - 'mle': use the maximized likelihood estimation.
        - 'css-mle': use css to approximate starting values first and then mle to fit.

        Defaults to 'css-mle'.
    allow_linear : bool, optional
        Controls whether to check the linear model ARMA(0,0)(0,0)m.

        Defaults to True.
    forecast_method : {'formula_forecast', 'innovations_algorithm'}, optional
        Store information for the subsequent forecast method.

        - 'formula_forecast': compute future series via formula.
        - 'innovations_algorithm': apply the innovations algorithm to compute future
          series, which requires more original information to be stored.

        Defaults to 'innovations_algorithm'.
    output_fitted : bool, optional
        Output fitted result and residuals if True.

        Defaults to True.
    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored, and this function heuristically determines the number of threads to use.

        Defaults to -1.
    background_size : int, optional
        Indicates the number of data points used in ARIMA with explanations in the predict function.
        If you want to use the ARIMA with explanations, you must set ``background_size`` to be a positive value or -1 (auto mode)
        when initializing an ARIMA instance and then set ``show_explainer=True`` in the predict function.

        Defaults to NULL (no explanations).
    massive : bool, optional
        Specifies whether or not to activate massive mode.

        - True : massive mode.
        - False : single mode.

        For parameter setting in massive mode, you could use both
        group_params (please see the example below) or the original parameters.
        Using original parameters will apply for all groups. However, if you define some parameters of a group,
        the value of all original parameter settings will not be applicable to such a group.

        An example is as follows:

        .. only:: latex

            >>> ar = AutoARIMA(order=(1, 0, 0),
                               background_size=5,
                               massive=True,
                               group_params={'Group_1': {'output_fitted':False},
                                             'Group_2': {'output_fitted':True}})
        .. raw:: html

            <iframe allowtransparency="true" style="border:1px solid #ccc; background: #eeffcb;"
                src="../../_static/autoarima_init_example1.html" width="100%" height="100%" sandbox="">
            </iframe>

        In this example, as a parameter 'output_fitted' is set in group_params for Group_1 & Group_2,
        parameter setting of 'background_size' is not applicable to Group_1 & Group_2.

        Defaults to False.
    group_params : dict, optional
        If massive mode is activated (``massive`` is True), input data is divided into different
        groups with different parameters applied.

        An example with group_params is as follows:

        .. only:: latex

            >>> ar = AutoARIMA(massive=True,
                               group_params={'Group_1':{'background_size':5},
                                             'Group_2':{'output_fitted':False}})

        .. raw:: html

            <iframe allowtransparency="true" style="border:1px solid #ccc; background: #eeffcb;"
                src="../../_static/autoarima_init_example2.html" width="100%" height="100%" sandbox="">
            </iframe>

        Valid only when ``massive`` is True and defaults to None.

    Attributes
    ----------

    model_ : DataFrame
        Model content.

    fitted_: DataFrame
        Fitted values and residuals.

    explainer_ : DataFrame
        The decomposition of trend, seasonal, transitory, irregular and reason code of exogenous variables.
        Only contains value after ``show_explainer=True`` in the predict() function.

    permutation\_importance\_ : DataFrame
        The importance of exogenous variables as determined by permutation importance analysis.
        The attribute only appears when invoking the get_permutation_importance() function after a trained model is obtained, structured as follows:

            - 1st column : PAIR, measure name.
            - 2nd column : NAME, exogenous regressor name.
            - 3rd column : VALUE, the importance of the exogenous regressor.

    Examples
    --------

    Create an AutoARIMA instance:

    >>> autoarima = AutoARIMA(search_strategy='stepwise', allow_linear=True, thread_ratio=1.0)

    Perform fit():

    >>> autoarima.fit(data=df)

    Output:

    >>> autoarima.model_.collect()
    >>> autoarima.fitted_.collect()

    Perform predict():

    >>> result = autoarima.predict(forecast_method='innovations_algorithm', forecast_length=10)
    >>> result.collect()

    If you want to see the decomposed result of the predict result, you could set ``show_explainer = True``:

    >>> result = autoarima.predict(forecast_method='innovations_algorithm',
                                   forecast_length=10,
                                   allow_new_index=False,
                                   show_explainer=True)

    Show the attribute ``explainer_`` of the AutoARIMA instance:

    >>> autoarima.explainer_.collect()

    """
    op_name = 'ARIMA'
    def fit(self,
            data,
            key=None,
            endog=None,
            exog=None,
            group_key=None,
            group_params=None,
            categorical_variable=None):
        r"""
        Fit the model to the training dataset.

        Parameters
        ----------
        data : DataFrame

            Input data which at least has two columns: key and endog.

            We also support ARIMAX which needs external data (exogenous variables).

        key : str, optional

            The timestamp column of data. The type of key column should be INTEGER,
            TIMESTAMP, DATE or SECONDDATE.

            In single mode, defaults to the first column of data if the index column of data is not provided.
            Otherwise, defaults to the index column of data.

            In massive mode, defaults to the first non-group key column of data if the index columns of data are not provided.
            Otherwise, defaults to the second of index columns of data and the first column of index columns is group_key.

        endog : str, optional

            The endogenous variable, i.e., time series. The type of endog column should be INTEGER,
            DOUBLE or DECIMAL(p,s).

            In single mode, defaults to the first non-key column.
            In massive mode, defaults to the first non-group_key, non-key column.

        exog : list of str, optional

            An optional array of exogenous variables. The type of exog column should be INTEGER,
            DOUBLE or DECIMAL(p,s).

            Valid only for Auto ARIMAX.

            Defaults to None. Please set this parameter explicitly if you have exogenous variables.

        group_key : str, optional
            The column of group_key. Data type can be INT or NVARCHAR/VARCHAR.
            If data type is INT, only parameters set in the ``group_params`` are valid.

            This parameter is valid only when massive mode is activated (i.e., parameter ``massive``
            is set as True in class instance initialization).

            Defaults to the first column of data if the index columns of data are not provided.
            Otherwise, defaults to the first column of index columns.

        group_params : dict, optional
            If massive mode is activated (``massive`` is set to True in class instance initialization),
            input data is divided into different groups with different parameters applied.

            An example with ``group_params`` is as follows:

            .. only:: latex

                >>> ar = AutoARIMA(massive=True,
                        group_params={'Group_1':{'background_size':5},
                                      'Group_2':{'output_fitted':False}})
                >>> ar.fit(data=df,
                    key='ID',
                    group_key='GROUP_ID',
                    endog='Y',
                    exog=['X1', 'X2', 'X3'],
                    group_params={'Group_1':{'categorical_variable':'X1'}})

            .. raw:: html

                <iframe allowtransparency="true" style="border:1px solid #ccc; background: #eeffcb;"
                    src="../../_static/autoarima_fit_example.html" width="100%" height="100%" sandbox="">
                </iframe>

            Valid only when ``massive`` is True.

        categorical_variable : str or list of str, optional
            Specifies INTEGER columns that should be treated as categorical.
            Other INTEGER columns will be treated as continuous.

            Defaults to None.

        Returns
        -------
        A fitted object of class "AutoARIMA".
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)

        if data is None:
            msg = 'The data for fit cannot be None!'
            logger.error(msg)
            raise ValueError(msg)

        group_params = {} if group_params is None else group_params
        if group_params:
            for group in group_params:
                self._arg('Parameters with group_key ' + str(group),
                          group_params[group], dict)
        #cols = data.columns
        #index = data.index
        key = self._arg('key', key, str)
        endog = self._arg('endog', endog, str)
        if isinstance(exog, str):
            exog = [exog]
        exog = self._arg('exog', exog, ListOfStrings)
        group_id = []
        if self.massive is True:#massive mode
            cols = data.columns
            index = data.index
            group_key = self._arg('group_key', group_key, str)
            if index is not None:
                group_key = _col_index_check(group_key, 'group_key', index[0], cols)
            else:
                if group_key is None:
                    group_key = cols[0]
            if group_key is not None and group_key not in cols:
                msg = "Please select group_key from {}!".format(cols)
                logger.error(msg)
                raise ValueError(msg)
            data_groups = list(data[[group_key]].collect()[group_key].drop_duplicates())
            self.data_groups = data_groups
            param_keys = list(group_params.keys())
            gid_type = {tp[0]:(tp[0], tp[1], tp[2]) for tp in data.dtypes()}[group_key]
            if not self._disable_hana_execution:
                if not all([(int(ky) if 'INT' in gid_type[1] else ky) in data_groups for ky in param_keys]):
                    msg = 'Invalid group key identified in group parameters!'
                    logger.error(msg)
                    raise ValueError(msg)

            group_id = [group_key]
            cols.remove(group_key)
            if index is not None:
                key = _col_index_check(key, 'key', index[1], cols)
            else:
                if key is None:
                    key = cols[0]
            endog, exog = _validate_og(key, endog, exog, cols)
            self.is_index_int = _is_index_int(data, key)
            data_ = data[group_id + [key] + [endog] + exog]#issue here
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
                    pass
                if self.is_index_int is False:
                    group_data = _convert_index_from_timestamp_to_int(group_data, key)
                if recomb_data is None:
                    recomb_data = group_data
                else:
                    recomb_data = recomb_data.union(group_data)
            if not self._disable_hana_execution and recomb_data is not None:
                if self.is_index_int is True:
                    data_ = recomb_data[group_id + [key] + [endog] + exog]
                else:
                    data_ = recomb_data[group_id + [key+'(INT)'] + [endog] + exog]
        else: # single mode
            if not self._disable_hana_execution:
                cols = data.columns
                index = data.index
                if index is not None:
                    key = _col_index_check(key, 'key', index, cols)
                else:
                    if key is None:
                        key = cols[0]
                endog, exog = _validate_og(key, endog, exog, cols)
                data_ = data[[key] + [endog] + exog]
                self.is_index_int = _is_index_int(data_, key)
                if not self.is_index_int:
                    data_= _convert_index_from_timestamp_to_int(data_, key)
                try:
                    self.forecast_start, self.timedelta = _get_forecast_starttime_and_timedelta(data, key, self.is_index_int)
                except Exception as err:
                    logger.warning(err)
            else:
                data_ = data

        setattr(self, 'key', key)
        setattr(self, 'endog', endog)
        setattr(self, 'exog', exog)

        super(AutoARIMA, self)._fit(data_, endog, group_params, categorical_variable)
        return self
