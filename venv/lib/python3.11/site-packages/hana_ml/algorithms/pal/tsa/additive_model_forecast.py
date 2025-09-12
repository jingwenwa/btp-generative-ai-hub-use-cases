"""
This module contains Python wrapper for PAL Additive Model Forecast algorithm.

The following class are available:

    * :class:`AdditiveModelForecast`
"""
#pylint: disable=too-many-instance-attributes, too-few-public-methods, too-many-nested-blocks
#pylint: disable=too-many-lines, line-too-long, invalid-name, too-many-branches, broad-except
#pylint: disable=too-many-arguments, too-many-locals, attribute-defined-outside-init， unnecessary-pass
#pylint: disable=super-with-arguments, c-extension-no-member, consider-using-dict-items
#pylint: disable=too-many-statements, use-a-generator, no-member， access-member-before-definition
import json
import logging
import uuid
from hdbcli import dbapi
from hana_ml.dataframe import quotename
from hana_ml.ml_exceptions import FitIncompleteError
from hana_ml.visualizers.report_builder import Page
from hana_ml.visualizers.time_series_report import AdditiveModelForecastExplainer
from hana_ml.visualizers.time_series_report_template_helper import TimeSeriesTemplateReportHelper
from .permutation_importance import PermutationImportanceMixin
from ..sqlgen import HolidayTable, trace_sql
from ..utility import check_pal_function_exist, _map_param
from .utility import _convert_index_from_timestamp_to_int, _is_index_int
from .utility import (
    _categorical_variable_update,
    _delete_none_key_in_dict,
    _col_index_check,
    _validate_og
)
from ..pal_base import (
    arg,
    PALBase,
    ParameterTable,
    ListOfStrings,
    pal_param_register,
    try_drop,
    require_pal_usable
)
logger = logging.getLogger(__name__)

def _convert_int_index_to_timestamp_index(df, key):
    columns = df.columns
    columns.remove(key)
    non_key_columns = ','.join(list(map(quotename, columns)))
    sql = f"SELECT TO_TIMESTAMP(\"{key}\" + 1) AS \"{key}\", {non_key_columns} FROM ({df.select_statement})"
    return df.connection_context.sql(sql)

def _convert_timestamp_index_to_int_index(df, key, df_key):
    columns = df.columns
    columns.remove(key)
    temp_id = str(uuid.uuid1()).replace('-', '_').upper()
    new_df = df.add_id(temp_id, ref_col=key).drop(key).set_index(temp_id).join(df_key.add_id(temp_id, ref_col=df_key.columns[0]).set_index(temp_id))
    return new_df[[key] + columns]

def _params_check(input_dict, param_map):
    update_params = {}
    if not input_dict or input_dict is None:
        return update_params

    for parm in input_dict:
        if parm in ['categorical_variable', 'show_explainer']:
            pass
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
                err_msg = f"'{parm}' is not a valid parameter name for initializing an AdditiveModelForecast model!"
                logger.error(err_msg)
                raise KeyError(err_msg)

        growth_val = input_dict.get('growth')
        logistic_growth_capacity_val =  input_dict.get('logistic_growth_capacity')
        if growth_val == 'logistic' and logistic_growth_capacity_val is None:
            msg = "logistic_growth_capacity is mandatory when growth is 'logistic'!"
            logger.error(msg)
            raise ValueError(msg)

    return update_params

class _AdditiveModelForecastBase(PALBase):
    __init_param_dict = {'growth' : ('GROWTH', str),
                         'logistic_growth_capacity' : ('CAP', float),
                         'seasonality_mode'  : ('SEASONALITY_MODE', str),
                         'seasonality'  : ('SEASONALITY', ListOfStrings),
                         'num_changepoints' : ('NUM_CHANGEPOINTS', int),
                         'changepoint_range' : ('CHANGEPOINT_RANGE', float),
                         'regressor' : ('REGRESSOR', ListOfStrings),
                         'changepoints' : ('CHANGE_POINT', ListOfStrings),
                         'yearly_seasonality' : ('YEARLY_SEASONALITY', int, {'auto': -1, 'false': 0, 'true': 1}),
                         'weekly_seasonality' : ('WEEKLY_SEASONALITY', int, {'auto': -1, 'false': 0, 'true': 1}),
                         'daily_seasonality' : ('DAILY_SEASONALITY', int, {'auto': -1, 'false': 0, 'true': 1}),
                         'seasonality_prior_scale' : ('SEASONALITY_PRIOR_SCALE', float),
                         'holiday_prior_scale' : ('HOLIDAYS_PRIOR_SCALE', float),
                         'changepoint_prior_scale' : ('CHANGEPOINT_PRIOR_SCALE', float)}

    __predict_param_dict = {'logistic_growth_capacity' : ('CAP', float),
                            'interval_width' : ('INTERVAL_WIDTH', float),
                            'uncertainty_samples' : ('UNCERTAINTY_SAMPLES', int),
                            'show_explainer' : ('EXPLAINER', bool),
                            'decompose_seasonality' : ('EXPLAIN_SEASONALITY', bool),
                            'decompose_holiday' : ('EXPLAIN_HOLIDAY', bool)}
    op_name = 'AMTSA'
    def __init__(self,
                 growth=None,
                 logistic_growth_capacity=None,
                 seasonality_mode=None,
                 seasonality=None,
                 num_changepoints=None,
                 changepoint_range=None,
                 regressor=None,
                 changepoints=None,
                 yearly_seasonality=None,
                 weekly_seasonality=None,
                 daily_seasonality=None,
                 seasonality_prior_scale=None,
                 holiday_prior_scale=None,
                 changepoint_prior_scale=None,
                 massive=False,
                 group_params=None):
        if not hasattr(self, 'hanaml_parameters'):
            setattr(self, 'hanaml_parameters', pal_param_register())
        super(_AdditiveModelForecastBase, self).__init__()

        init_params = {'growth' : growth,
                       'logistic_growth_capacity' : logistic_growth_capacity,
                       'seasonality_mode' : seasonality_mode,
                       'seasonality' : seasonality,
                       'num_changepoints' : num_changepoints,
                       'changepoint_range' : changepoint_range,
                       'regressor' : regressor,
                       'changepoints' : changepoints,
                       'yearly_seasonality' : yearly_seasonality,
                       'weekly_seasonality' : weekly_seasonality,
                       'daily_seasonality' : daily_seasonality,
                       'seasonality_prior_scale' : seasonality_prior_scale,
                       'holiday_prior_scale' : holiday_prior_scale,
                       'changepoint_prior_scale' : changepoint_prior_scale}

        init_params = _delete_none_key_in_dict(init_params)
        self.init_params = init_params
        self.__pal_params = {}

        self.massive = self._arg('massive', massive, bool)
        if self.massive is not True:
            self.__pal_params = _params_check(input_dict=self.init_params,
                                              param_map=self.__init_param_dict)
        else: # massive mode
            group_params = self._arg('group_params', group_params, dict)
            group_params = {} if group_params is None else group_params
            for group in group_params:
                self._arg('Parameters with group_key ' + str(group), group_params[group], dict)
            self.group_params = group_params

            for group in self.group_params:
                self.__pal_params[group] = _params_check(input_dict=self.group_params[group],
                                                         param_map=self.__init_param_dict)
            if init_params:
                special_group_name = 'PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID'
                self.__pal_params[special_group_name] = _params_check(input_dict=self.init_params,
                                                                      param_map=self.__init_param_dict)

    @trace_sql
    def _fit(self, data, holiday, group_params, categorical_variable, group_key_type):
        conn = data.connection_context
        require_pal_usable(conn)
        param_rows = []

        if self.massive is not True:
            for name in self.__pal_params:
                value, typ = self.__pal_params[name]
                if isinstance(value, (list, tuple)):
                    for val in value:
                        tpl = [_map_param(name, val, typ)]
                        param_rows.extend(tpl)
                else:
                    tpl = [_map_param(name, value, typ)]
                    param_rows.extend(tpl)

            categorical_variable = _categorical_variable_update(categorical_variable)
            if categorical_variable:
                param_rows.extend([('CATEGORICAL_VARIABLE', None, None, var) for var in categorical_variable])

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
                if self.__pal_params[group]:
                    for name in self.__pal_params[group]:
                        value, typ = self.__pal_params[group][name]
                        if isinstance(value, (list, tuple)):
                            for val in value:
                                tpl = [tuple([group] + list(_map_param(name, val, typ)))]
                                param_rows.extend(tpl)
                        else:
                            tpl = [tuple([group] + list(_map_param(name, value, typ)))]
                            param_rows.extend(tpl)

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        if holiday is None:
            holiday = HolidayTable(itype=group_key_type)
        if self.massive is not True:
            model_tbl = f'#PAL_ADDITIVE_MODEL_FORECAST_MODEL_TBL_{self.id}_{unique_id}'
            outputs = [model_tbl]
            try:
                self._call_pal_auto(conn,
                                    'PAL_ADDITIVE_MODEL_ANALYSIS',
                                    data,
                                    holiday,
                                    ParameterTable().with_data(param_rows),
                                    *outputs)
            except dbapi.Error as db_err:
                msg = str(conn.hana_version())
                logger.exception("HANA version: %s. %s", msg, str(db_err))
                try_drop(conn, outputs)
                raise
            except Exception as db_err:
                msg = str(conn.hana_version())
                logger.exception("HANA version: %s. %s", msg, str(db_err))
                try_drop(conn, outputs)
                raise
        else:
            model_tbl = f'#PAL_MASSIVE_ADDITIVE_MODEL_FORECAST_MODEL_TBL_{self.id}_{unique_id}'
            errormsg_tbl = f'#PAL_MASSIVE_ADDITIVE_MODEL_FORECAST_ERROR_TBL_{self.id}_{unique_id}'
            outputs = [model_tbl, errormsg_tbl]
            if not (check_pal_function_exist(conn, '%MASSIVE_ADDITIVE_MODEL%', like=True) or \
            self._disable_hana_execution):
                msg = 'The version of your SAP HANA does not support massive AdditiveModelForecast!'
                logger.error(msg)
                raise ValueError(msg)
            try:
                self._call_pal_auto(conn,
                                    'PAL_MASSIVE_ADDITIVE_MODEL_ANALYSIS',
                                    data,
                                    holiday,
                                    ParameterTable(itype=group_key_type).with_data(param_rows),
                                    *outputs)
            except dbapi.Error as db_err:
                msg = str(conn.hana_version())
                logger.exception("HANA version: %s. %s", msg, str(db_err))
                try_drop(conn, outputs)
                raise
            except Exception as db_err:
                msg = str(conn.hana_version())
                logger.exception("HANA version: %s. %s", msg, str(db_err))
                try_drop(conn, outputs)
                raise

        setattr(self, 'fit_data', data)
        self.model_ = conn.table(model_tbl)
        self.error_msg_ = None
        if self.massive is True:
            if not self._disable_hana_execution:
                self.error_msg_ = conn.table(errormsg_tbl)
                if not self.error_msg_.collect().empty:
                    row = self.error_msg_.count()
                    for i in range(1, row+1):
                        warn_msg = "For group_key '{}',".format(self.error_msg_.collect()['GROUP_ID'][i-1]) +\
                                   " the error message is '{}'.".format(self.error_msg_.collect()['MESSAGE'][i-1]) +\
                                   "More information could be seen in the attribute error_msg_!"
                        logger.warning(warn_msg)

    @trace_sql
    def _predict(self,
                 data,
                 group_params,
                 predict_params,
                 group_key_type):

        conn = data.connection_context

        __pal_predict_params = {}
        param_rows = []
        if self.massive is not True:
            __pal_predict_params = _params_check(input_dict=predict_params,
                                                 param_map=self.__predict_param_dict)
            for name in __pal_predict_params:
                value, typ = __pal_predict_params[name]
                if isinstance(value, (list, tuple)):
                    for val in value:
                        tpl = [_map_param(name, val, typ)]
                        param_rows.extend(tpl)
                else:
                    tpl = [_map_param(name, value, typ)]
                    param_rows.extend(tpl)
        else: # massive mode
            special_group_name = 'PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID'
            general_params = {}
            general_params = _params_check(input_dict=predict_params,
                                           param_map=self.__predict_param_dict)

            if general_params:
                __pal_predict_params[special_group_name] = general_params

            # for each group, only categorical_variable could be set in this algorithm fit()
            for group in group_params:
                if group in ['PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID']:
                    continue
                each_group_params = {}
                each_group_params = _params_check(input_dict=group_params[group],
                                                  param_map=self.__predict_param_dict)
                if each_group_params:
                    __pal_predict_params[group] = each_group_params

            for group in __pal_predict_params:
                if __pal_predict_params[group]:
                    for name in __pal_predict_params[group]:
                        value, typ = __pal_predict_params[group][name]
                        if isinstance(value, (list, tuple)):
                            for val in value:
                                tpl = [tuple([group] + list(_map_param(name, val, typ)))]
                                param_rows.extend(tpl)
                        else:
                            tpl = [tuple([group] + list(_map_param(name, value, typ)))]
                            param_rows.extend(tpl)

        show_explainer = False
        if predict_params.get('show_explainer'):
            show_explainer = predict_params['show_explainer']

        if self.massive is not True:
            unique_id = str(uuid.uuid1()).replace('-', '_').upper()
            result_tbl = f"#PAL_ADDITIVE_MODEL_FORECAST_RESULT_TBL_{self.id}_{unique_id}"
            decompose_tbl = f"#PAL_ADDITIVE_MODEL_FORECAST_DECOMPOSITION_TBL_{self.id}_{unique_id}"
            if show_explainer is True and not (check_pal_function_exist(conn, 'ADDITIVE_MODEL_EXPLAIN%', like=True) or \
            self._disable_hana_execution):
                msg = 'The version of SAP HANA does not support additive_model_forecast explainer. Please set show_explainer=False!'
                logger.error(msg)
                raise ValueError(msg)
            try:
                if show_explainer is not True:
                    self._call_pal_auto(conn,
                                        'PAL_ADDITIVE_MODEL_PREDICT',
                                        data,
                                        self.model_,
                                        ParameterTable().with_data(param_rows),
                                        result_tbl)
                else:
                    self._call_pal_auto(conn,
                                        'PAL_ADDITIVE_MODEL_EXPLAIN',
                                        data,
                                        self.model_,
                                        ParameterTable().with_data(param_rows),
                                        result_tbl,
                                        decompose_tbl)
            except dbapi.Error as db_err:
                msg = str(conn.hana_version())
                logger.exception("HANA version: %s. %s", msg, str(db_err))
                try_drop(conn, result_tbl)
                raise
            except Exception as db_err:
                msg = str(conn.hana_version())
                logger.exception("HANA version: %s. %s", msg, str(db_err))
                try_drop(conn, result_tbl)
                raise
            self.explainer_ = None
            if show_explainer is True:
                self.explainer_ = conn.table(decompose_tbl)
            return conn.table(result_tbl)

        # massive mode
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        result_tbl = f"#PAL_MASSIVE_AMF_PREDICT_RESULT_TBL_{self.id}_{unique_id}"
        decompose_tbl = f"#PAL_MASSIVE_AMF_PREDICT_DECOMPOSITION_TBL_{self.id}_{unique_id}"
        errormsg_tbl = f"#PAL_MASSIVE_AMF_PREDICT_ERROR_TBL_{self.id}_{unique_id}"
        if not (check_pal_function_exist(conn, 'MASSIVE_ADDITIVE_MODEL%', like=True) or \
        self._disable_hana_execution):
            msg = 'The version of SAP HANA does not support massive AdditiveModelForecast!'
            logger.error(msg)
            raise ValueError(msg)
        try:
            self._call_pal_auto(conn,
                                'PAL_MASSIVE_ADDITIVE_MODEL_PREDICT',
                                data,
                                self.model_,
                                ParameterTable(itype=group_key_type).with_data(param_rows),
                                result_tbl,
                                decompose_tbl,
                                errormsg_tbl)
        except dbapi.Error as db_err:
            msg = str(conn.hana_version())
            logger.exception("HANA version: %s. %s", msg, str(db_err))
            try_drop(conn, result_tbl)
            raise
        except Exception as db_err:
            msg = str(conn.hana_version())
            logger.exception("HANA version: %s. %s", msg, str(db_err))
            try_drop(conn, result_tbl)
            raise
        err_msg = None
        if not self._disable_hana_execution:
            self.explainer_ = conn.table(decompose_tbl)
            err_msg = conn.table(errormsg_tbl)
            if not err_msg.collect().empty:
                row = err_msg.count()
                for i in range(1, row+1):
                    warn_msg = "For group_key '{}',".format(err_msg.collect()['GROUP_ID'][i-1]) +\
                               " the error message is '{}'.".format(err_msg.collect()['MESSAGE'][i-1]) +\
                               "More information could be seen in the 2nd return Dataframe!"
                    logger.warning(warn_msg)
        if not hasattr(self, 'explainer_'):
            setattr(self, 'explainer_', None)
        return conn.table(result_tbl), self.explainer_, err_msg

class AdditiveModelForecast(_AdditiveModelForecastBase, PermutationImportanceMixin):
    r"""
    Additive Model Time Series Analysis (AMTSA) uses an additive model to forecast time series data. It effectively handles data with strong seasonal effects and is adaptable to shifts in historical trends.
    AMTSA uses a decomposable time series model with three main components: trend, seasonality, and holidays or events.

    Parameters
    ----------

    growth : {'linear', 'logistic'}, optional

        Specifies a trend, which could be either linear or logistic.

        Defaults to 'linear'.

    logistic_growth_capacity : float, optional

        Specifies the carrying capacity for logistic growth.
        Mandatory and valid only when ``growth`` is 'logistic'.

        No default value.

    seasonality_mode : {'additive', 'multiplicative'}, optional

        Mode for seasonality.

        Defaults to 'additive'.

    seasonality : str or a list of str, optional
        Adds seasonality to the model in a json format, include:

          - NAME
          - PERIOD
          - FOURIER_ORDER
          - PRIOR_SCALE, optional
          - MODE, optional

        Each str is in json format such as
        '{ "NAME": "MONTHLY", "PERIOD":30, "FOURIER_ORDER":5 }'.
        FOURIER_ORDER determines how quickly the seasonality can change.
        PRIOR_SCALE controls the amount of regularization.
        No seasonality will be added to the model if this parameter is not provided.

        No default value.

    num_changepoints : int, optional

        The number of potential changepoints.
        Not effective if ``changepoints`` is provided.

        Defaults to 25 if not provided.

    changepoint_range : float, optional

        Proportion of history in which trend changepoints will be estimated.
        Not effective if ``changepoints`` is provided.

        Defaults to 0.8.

    regressor : a list of str, optional
        Specifies the regressor, include:

          - NAME
          - PRIOR_SCALE
          - STANDARDIZE
          - MODE: "additive" or 'multiplicative'.

        Each str is json format such as
        '{ "NAME": "X1", "PRIOR_SCALE":4, "MODE": "additive" }'.
        PRIOR_SCALE controls for the amount of regularization;
        STANDARDIZE Specifies whether or not the regressor is standardized.

        No default value.

    changepoints : list of str, optional,

        Specifies a list of changepoints in the format of timestamp,
        such as ['2019-01-01 00:00:00, '2019-02-04 00:00:00']

        No default value.

    yearly_seasonality : {'auto', 'false', 'true'}, optional

        Specifies whether or not to fit yearly seasonality.

        'false' and 'true' simply corresponds to their logical meaning,
        while 'auto' means automatically determined from the input data.

        Defaults to 'auto'.

    weekly_seasonality : {'auto', 'false', 'true'}, optional

        Specifies whether or not to fit the weekly seasonality.

        'auto' means automatically determined from input data.

        Defaults to 'auto'.

    daily_seasonality : {'auto', 'false', 'true'}, optional

        Specifies whether or not to fit the daily seasonality.

        'auto' means automatically determined from input data.

        Defaults to 'auto'.

    seasonality_prior_scale : float, optional

        Parameter modulating the strength of the seasonality model.

        Defaults to 10.

    holiday_prior_scale : float, optional

        Parameter modulating the strength of the holiday components model.

        Defaults to 10.

    changepoint_prior_scale : float, optional

        Parameter modulating the flexibility of the automatic changepoint selection.

        Defaults to 0.05.

    massive : bool, optional
        Specifies whether or not to activate the massive mode.

        - True : massive mode.
        - False : single mode.

        For parameter setting in the massive mode, you could use both
        group_params (please see the example below) or the original parameters.
        Using original parameters will apply for all groups.
        However, if you define some parameters of a group,
        the value of all original parameter setting will be not applicable to such group.

        An example is as follows:

        .. only:: latex

            >>> amf = AdditiveModelForecast(massive=True,
                                            changepoint_prior_scale=0.06,
                                            group_params={'Group_1': {'seasonality_mode':'additive'}})

        .. raw:: html

            <iframe allowtransparency="true" style="border:1px solid #ccc; background: #eeffcb;"
                src="../../_static/amf_init_example1.html" width="100%" height="100%" sandbox="">
            </iframe>

        In this example, as ``seasonality_mode`` is set in group_params for Group_1,
        parameter setting of ``changepoint_prior_scale`` is not applicable to Group_1.

        Defaults to False.

    group_params : dict, optional
        If the massive mode is activated (``massive`` is True), input data is divided into different
        groups with different parameters applied.

        An example with group_params is as follows:

        .. only:: latex

            >>> amf = AdditiveModelForecast(massive=True,
                                            group_params={'Group_1': {'seasonality_mode':'additive'},
                                                          'Group_2': {'seasonality_mode':'multiplicative'}})

        .. raw:: html

            <iframe allowtransparency="true" style="border:1px solid #ccc; background: #eeffcb;"
                src="../../_static/amf_init_example2.html" width="100%" height="100%" sandbox="">
            </iframe>

        Valid only when ``massive`` is True and defaults to None.

    References
    ----------
    :ref:`Seasonalities in Additive Model Forecast<amf_season-label>`

    Attributes
    ----------

    model_ : DataFrame

        Model content.

    explainer_ : DataFrame

        The decomposition of trend, seasonal, holiday and exogenous variables.

        - In single mode, only contains value when ``show_explainer=True`` in the predict() function.
        - In massive mode, this attribute always contains value.

    error_msg_ : DataFrame

        Error message.
        Only valid if ``massive`` is True when initializing an 'AdditiveModelForecast' instance.

    permutation\_importance\_ : DataFrame

        The importance of exogenous variables as determined by permutation importance analysis.
        The attribute only appear when invoking get_permutation_importance() function after a trained model is obtained, structured as follows:

        - 1st column : PAIR, measure name.
        - 2nd column : NAME, exogenous regressor name.
        - 3rd column : VALUE, the importance of the exogenous regressor.

    Examples
    --------

    Input DataFrame df_fit:

    >>> df_fit.head(5).collect()
            ts         y
    2007-12-10  9.590761
    2007-12-11  8.519590
    2007-12-12  8.183677
    2007-12-13  8.072467
    2007-12-14  7.893572

    Create an Additive Model Forecast model:

    >>> amf = additive_model_forecast.AdditiveModelForecast(growth='linear')

    Perform fit():

    >>> amf.fit(data=df_fit)

    Output:

    >>> amf.model_.collect()
       ROW_INDEX                                      MODEL_CONTENT
    0          0  {"GROWTH":"linear","FLOOR":0.0,"SEASONALITY_MO...

    Perform predict():

    Input DataFrame df_predict:

    >>> df_predict.head(5).collect()
                ts    y
    0   2008-03-09  0.0
    1   2008-03-10  0.0
    2   2008-03-11  0.0
    3   2008-03-12  0.0
    4   2008-03-13  0.0

    >>> result = amf.predict(data=df_predict)

    Output:

    >>> result.collect()
                ts      YHAT  YHAT_LOWER  YHAT_UPPER
    0   2008-03-09  7.676880    6.930349    8.461546
    1   2008-03-10  8.147574    7.387315    8.969112
    2   2008-03-11  7.410452    6.630115    8.195562
    3   2008-03-12  7.198807    6.412776    7.977391
    4   2008-03-13  7.087702    6.310826    7.837083

    If you want to see the decomposed result of predict result, you could set ``show_explainer = True``:

    >>> result = amf.predict(data=df_predict,
                             show_explainer=True,
                             decompose_seasonality=False,
                             decompose_holiday=False)

    Show the attribute ``explainer_``:

    >>> amf.explainer_.head(5).collect()
                ts     TREND                                SEASONAL HOLIDAY EXOGENOUS
    0   2008-03-09  7.432172   {"seasonalities":0.24470822257259804}      {}        {}
    1   2008-03-10  7.390030     {"seasonalities":0.757544365973254}      {}        {}
    2   2008-03-11  7.347887   {"seasonalities":0.06256440574150749}      {}        {}
    3   2008-03-12  7.305745  {"seasonalities":-0.10693834906369426}      {}        {}
    4   2008-03-13  7.263603  {"seasonalities":-0.17590059499681369}      {}        {}

    """
    def fit(self,
            data,
            key=None,
            endog=None,
            exog=None,
            holiday=None,
            group_key=None,
            group_params=None,
            categorical_variable=None):
        r"""
        Fit the model to the training dataset.

        Parameters
        ----------

        data : DataFrame

            Input data. The structure is as follows.

            - The first column: index (ID), type TIMESTAMP, SECONDDATE or DATE.
            - The second column: raw data, type INTEGER or DECIMAL(p,s).
            - Other columns: external data, type INTEGER, DOUBLE or DECIMAL(p,s).

        key : str, optional

            The timestamp column of data. The type of key column is TIMESTAMP, SECONDDATE, or DATE.

            In the single mode, defaults to the first column of data if the index column of data is
            not provided; otherwise, defaults to the index column of data.

            In the massive mode, defaults to the first-non group key column of data if the index columns
            of data is not provided; otherwise, defaults to the second of index columns of data and
            the first column of index columns is group_key.

        endog : str, optional

            The endogenous variable, i.e. time series. The type of endog column is
            INTEGER, DOUBLE, or DECIMAL(p, s).

            - In single mode, defaults to the first non-key column.
            - In massive mode, defaults to the first non group_key, non key column.

        exog : str or a list of str, optional

            An optional array of exogenous variables. The type of exog column is INTEGER, DOUBLE, or DECIMAL(p, s).

            Defaults to None. Please set this parameter explicitly if you have exogenous variables.

        holiday : DataFrame, optional

            Input holiday data. The structure is as follows.

            - 1st column : timestamp/key, TIMESTAMP, SECONDDATE, DATE
            - 2nd column : holiday name, VARCHAR, NVARCHAR
            - 3rd column : lower window of holiday, less than 0, INTEGER, optional
            - 4th column : upper window of holiday, greater than 0, INTEGER, optional

            if ``massive`` is True, the structure of input holiday data is as follows:

            - 1st column: group_key, INTEGER, VRACHAR or NVARCHAR
            - 2nd column: timestamp/key, TIMESTAMP, SECONDDATE, DATE
            - 3rd column : holiday name, VARCHAR, NVARCHAR
            - 4th column : lower window of holiday, less than 0, INTEGER, optional
            - 3th column : upper window of holiday, greater than 0, INTEGER, optional

            Defaults to None.

        group_key : str, optional
            The column of group_key. Data type can be INT or NVARCHAR/VARCHAR.
            If data type is INT, only parameters set in the group_params are valid.

            This parameter is only valid when `self.massive` is True.

            Defaults to the first column of data if the index columns of data is not provided.
            Otherwise, defaults to the first column of index columns.

        group_params : dict, optional
            If massive mode is activated (``massive`` is True), input data is divided into different
            groups with different parameters applied.

            An example with group_params is as follows:

            .. raw:: html

                <iframe allowtransparency="true" style="border:1px solid #ccc; background: #eeffcb;"
                    src="../../_static/amf_fit_example.html" width="100%" height="100%" sandbox="">
                </iframe>

            Valid only when `self.massive` is True.

            Defaults to None.

        categorical_variable : str or ist of str, optional

            Specifies INTEGER columns specified that should be be treated as categorical.

            Other INTEGER columns will be treated as continuous.

            Defaults to None.

        Returns
        -------
        A fitted object of class "AdditiveModelForecast".
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        if self.massive is True:
            fit_output_signature = [{'GROUP_ID': 'NVARCHAR(100)', 'ROW_INDEX': 'INT', 'MODEL_CONTENT': 'NCLOB'}, {'GROUP_ID': 'NVARCHAR(100)', 'ERROR_TIMESTAMP': 'NVARCHAR(100)', 'ERRORCODE': 'INT', 'MESSAGE': 'NVARCHAR(200)'}]
        else:
            fit_output_signature = [{'ROW_INDEX': 'INT', 'MODEL_CONTENT': 'NCLOB'}]
        setattr(self, "fit_output_signature", fit_output_signature)
        group_params = {} if group_params is None else group_params
        if group_params:
            for group in group_params:
                self._arg('Parameters with group_key ' + str(group),
                          group_params[group], dict)
        group_key_type = None
        group_id = []
        key = self._arg('key', key, str)
        endog = self._arg('endog', endog, str)
        if isinstance(exog, str):
            exog = [exog]
        exog = self._arg('exog', exog, ListOfStrings)
        if self.massive is True:
            cols = data.columns
            group_key = self._arg('group_key', group_key, str)
            index = data.index
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
            param_keys = list(group_params.keys())
            gid_type = {tp[0]:(tp[0], tp[1], tp[2]) for tp in data.dtypes()}[group_key]
            if not self._disable_hana_execution:
                if not all([(int(ky) if 'INT' in gid_type[1] else ky) in data_groups for ky in param_keys]):
                    msg = 'Invalid group key identified in group parameters!'
                    logger.error(msg)
                    raise ValueError(msg)
            group_key_type = "VARCHAR(5000)"
            group_id = [group_key]
            cols.remove(group_key)
            if index is not None:
                key = _col_index_check(key, 'key', index[1], cols)
            else:
                if key is None:
                    key = cols[0]
            endog, exog = _validate_og(key, endog, exog, cols)
            data_ = data[group_id + [key] + [endog] + exog]
            setattr(self, "group_key", group_key)
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
            else:
                data_ = data
            if _is_index_int(data_, key):
                data_ = _convert_int_index_to_timestamp_index(data_, key)

        setattr(self, "key", key)
        setattr(self, "exog", exog)
        setattr(self, "endog", endog)

        super(AdditiveModelForecast, self)._fit(data_, holiday, group_params, categorical_variable, group_key_type)
        return self

    def make_future_dataframe(self, data=None, key=None, group_key=None, periods=1, increment_type='seconds'):
        """
        Create a new dataframe for time series prediction.

        Parameters
        ----------
        data : DataFrame, optional
            The training data contains the index.

            Defaults to the data used in the fit().

        key : str, optional
            The index defined in the training data.

            Defaults to the specified key in fit() or the value in data.index or the PAL's default key column position.

        group_key : str, optional
            Specify the group id column.

            This parameter is only valid when ``massive`` is True.

            Defaults to the specified group_key in fit() or the first column of the dataframe.

        periods : int, optional
            The number of rows created in the predict dataframe.

            Defaults to 1.

        increment_type : {'seconds', 'days', 'months', 'years'}, optional
            The increment type of the time series.

            Defaults to 'seconds'.

        Returns
        -------

        DataFrame

        """
        if data is None:
            data = self._fit_args[0]
        if self.massive:
            if group_key is None:
                if hasattr(self, "group_key"):
                    if self.group_key is None:
                        group_key = data.columns[0]
                    else:
                        group_key = self.group_key
                else:
                    group_key = data.columns[0]
            if key is None:
                if data.index is None:
                    if hasattr(self, "key"):
                        if self.key is None:
                            key = data.columns[1]
                        else:
                            key = self.key
                    else:
                        key = data.columns[1]
                else:
                    key = data.index
            group_id_type = data.get_table_structure()[group_key]
            group_list = data.select(group_key).distinct().collect()[group_key]
            timeframe = []
            for group in group_list:
                if 'INT' in group_id_type.upper():
                    m_data = data.filter(f"{group_key}={group}")
                else:
                    m_data = data.filter(f"{group_key}='{group}'")
                max_ = m_data.select(key).max()
                sec_max_ = m_data.select(key).distinct().sort_values(key, ascending=False).head(2).collect().iat[1, 0]
                delta = max_ - sec_max_
                is_int = 'INT' in m_data.get_table_structure()[key]
                if is_int:
                    forecast_start, timedelta = max_ + delta, delta
                else:
                    forecast_start, timedelta = max_ + delta, delta.total_seconds()

                if not is_int:
                    if 'day' in increment_type.lower():
                        increment_type = 'days'
                        timedelta = round(timedelta / 86400)
                        if timedelta == 0:
                            raise ValueError("The interval between the training time series is less than one day.")
                    elif 'month' in increment_type.lower():
                        increment_type = 'months'
                        timedelta = round(timedelta / 2592000)
                        if timedelta == 0:
                            raise ValueError("The interval between the training time series is less than one month.")
                    elif 'year' in increment_type.lower():
                        increment_type = 'years'
                        timedelta = round(timedelta / 31536000)
                        if timedelta == 0:
                            raise ValueError("The interval between the training time series is less than one year.")
                    else:
                        increment_type = 'seconds'

                increment_type = increment_type.upper()
                for period in range(0, periods):
                    if 'INT' in group_id_type.upper():
                        if is_int:
                            timeframe.append(f"SELECT {group} AS \"{group_key}\", TO_INT({forecast_start} + {timedelta} * {period}) AS \"{key}\" FROM DUMMY")
                        else:
                            timeframe.append(f"SELECT {group} AS \"{group_key}\", ADD_SECONDS('{forecast_start}', {timedelta} * {period}) AS \"{key}\" FROM DUMMY")
                    else:
                        if is_int:
                            timeframe.append(f"SELECT '{group}' AS \"{group_key}\", TO_INT({forecast_start} + {timedelta} * {period}) AS \"{key}\" FROM DUMMY")
                        else:
                            timeframe.append(f"SELECT '{group}' AS \"{group_key}\", ADD_SECONDS('{forecast_start}', {timedelta} * {period}) AS \"{key}\" FROM DUMMY")
            sql = ' UNION '.join(timeframe)
            return data.connection_context.sql(sql).sort_values([group_key, key]).add_constant('PLACE_HOLDER', 0)

        # single mode
        if key is None:
            if data.index is None:
                key = data.columns[0]
            else:
                key = data.index
        max_ = data.select(key).max()
        sec_max_ = data.select(key).distinct().sort_values(key, ascending=False).head(2).collect().iat[1, 0]
        delta = max_ - sec_max_
        is_int = 'INT' in data.get_table_structure()[key]
        if is_int:
            forecast_start, timedelta = max_ + delta, delta
        else:
            forecast_start, timedelta = max_ + delta, delta.total_seconds()
        timeframe = []

        if not is_int:
            if 'day' in increment_type.lower():
                increment_type = 'days'
                timedelta = round(timedelta / 86400)
                if timedelta == 0:
                    raise ValueError("The interval between the training time series is less than one day.")
            elif 'month' in increment_type.lower():
                increment_type = 'months'
                timedelta = round(timedelta / 2592000)
                if timedelta == 0:
                    raise ValueError("The interval between the training time series is less than one month.")
            elif 'year' in increment_type.lower():
                increment_type = 'years'
                timedelta = round(timedelta / 31536000)
                if timedelta == 0:
                    raise ValueError("The interval between the training time series is less than one year.")
            else:
                increment_type = 'seconds'

        for period in range(0, periods):
            if is_int:
                timeframe.append(f"SELECT TO_INT({forecast_start} + {timedelta} * {period}) AS \"{key}\" FROM DUMMY")
            else:
                timeframe.append("SELECT ADD_{}('{}', {} * {}) AS \"{}\" FROM DUMMY".format(increment_type.upper(), forecast_start, timedelta, period, key))
        sql = ' UNION ALL '.join(timeframe)
        return data.connection_context.sql(sql).sort_values(key).add_constant('PLACE_HOLDER', 0.0)

    def predict(self,
                data,
                key=None,
                exog=None,
                group_key=None,
                group_params=None,
                logistic_growth_capacity=None,
                interval_width=None,
                uncertainty_samples=None,
                show_explainer=False,
                decompose_seasonality=None,
                decompose_holiday=None,
                add_placeholder=False):
        """
        Generates time series forecasts based on the fitted model.

        Parameters
        ----------

        data : DataFrame, optional

            Index and exogenous variables for forecast.
            The structure is as follows.

              - First column: Index (ID), type TIMESTAMP, SECONDDATE or DATE.
              - Second column: Placeholder column for forecast values, type DOUBLE or DECIMAL(p,s).
              - Other columns : external data, type INTEGER, DOUBLE or DECIMAL(p,s).

            if massive is True, the structure of data is as follows:

              - First column: Group_key, type INTEGER, VRACHAR or NVARCHAR.
              - Second column: Index (ID), type TIMESTAMP, SECONDDATE or DATE.
              - Third column : Placeholder column for forecast values, type DOUBLE or DECIMAL(p,s).
              - Other columns: external data, type INTEGER, DOUBLE or DECIMAL(p,s).

        key : str, optional

            The timestamp column of data. The data type of key column should be
            TIMESTAMP, DATE or SECONDDATE.

            In single mode, defaults to the first column of data if the index column of data is not provided.
            Otherwise, defaults to the index column of data.

            In massive mode, defaults to the first-non group key column of data
            if the index columns of data is not provided; otherwise, defaults to
            the second of index columns of data and the first column of index columns is group_key.

        group_key : str, optional
            The column of group_key. Data type can be INT or NVARCHAR/VARCHAR.
            If data type is INT, only parameters set in the group_params are valid.

            This parameter is only valid when ``massive`` is True.

            Defaults to the first column of data if the index columns of data is not provided.
            Otherwise, defaults to the first column of index columns.

        group_params : dict, optional
            If massive mode is activated (``massive`` is True), input data is divided into different
            groups with different parameters applied.

            An example with ``group_params`` is as follows:

            .. only:: latex

                >>> amf = AdditiveModelForecast(massive=True)
                >>> res = amf.fit(data=train_df).predict(group_params={'Group_1': {'interval_width':0.5},
                                                                       'Group_2': {'interval_width':0.6}})

            .. raw:: html

                <iframe allowtransparency="true" style="border:1px solid #ccc; background: #eeffcb;"
                    src="../../_static/amf_predict_example.html" width="100%" height="100%" sandbox="">
                </iframe>

            Valid only when ``massive`` is True and defaults to None.

        logistic_growth_capacity: float, optional

            Specifies the carrying capacity for logistic growth.
            Mandatory and valid only when ``growth`` is 'logistic'.

            Defaults to None.
        interval_width : float, optional

            Width of the uncertainty intervals.

            Defaults to 0.8.

        uncertainty_samples : int, optional

            Number of simulated draws used to estimate uncertainty intervals.

            Defaults to 1000.

        show_explainer : bool, optional
            Indicates whether to invoke the AdditiveModelForecast with explanations function in the predict.
            If true, the contributions of trend, seasonal, holiday and exogenous variables are
            shown in a attribute called ``explainer_`` of the AdditiveModelForecast instance.

            Defaults to False.

        decompose_seasonality : bool, optional
            Specifies whether or not seasonal component will be decomposed.
            Valid only when ``show_explainer`` is True.

            Defaults to False.

        decompose_holiday : bool, optional
            Specifies whether or not holiday component will be decomposed.
            Valid only when ``show_explainer`` is True.

            Defaults to False.

        Returns
        -------

        DataFrame 1
            Forecasted values, structured as follows:

            - ID, type timestamp.
            - YHAT, type DOUBLE, forecast value.
            - YHAT_LOWER, type DOUBLE, lower bound of confidence region.
            - YHAT_UPPER, type DOUBLE, higher bound of confidence region.

        DataFrame 2
            The decomposition of trend, seasonal, holiday and exogenous variables.

        DataFrame 3 (optional)
            Error message.
            Only valid if ``massive`` is True when initializing an 'AdditiveModelForecast' instance.

        """
        if getattr(self, 'model_') is None:
            raise FitIncompleteError()
        predict_input_signature = [data.get_table_structure(), {'GROUP_ID': 'NVARCHAR(100)', 'ROW_INDEX': 'INT', 'MODEL_CONTENT': 'NCLOB'} if self.massive else {'ROW_INDEX': 'INT', 'MODEL_CONTENT': 'NCLOB'}]
        setattr(self, "predict_input_signature", predict_input_signature)
        group_params = {} if group_params is None else group_params
        if group_params:
            for group in group_params:
                self._arg('Parameters with group_key ' + str(group),
                          group_params[group], dict)

        predict_params = {'logistic_growth_capacity' : logistic_growth_capacity,
                          'interval_width' : interval_width,
                          'uncertainty_samples' : uncertainty_samples,
                          'show_explainer' : show_explainer,
                          'decompose_seasonality' : decompose_seasonality if show_explainer is True else None,
                          'decompose_holiday' : decompose_holiday if show_explainer is True else None}

        predict_params = _delete_none_key_in_dict(predict_params)

        index = data.index
        cols = data.columns
        group_key_type = None
        group_id = []
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
            param_keys = list(group_params.keys())
            gid_type = {tp[0]:(tp[0], tp[1], tp[2]) for tp in data.dtypes()}[group_key]
            if not self._disable_hana_execution:
                if not all([(int(ky) if 'INT' in gid_type[1] else ky) in data_groups for ky in param_keys]):
                    msg = 'Invalid group key identified in group parameters!'
                    logger.error(msg)
                    raise ValueError(msg)
            group_key_type = "VARCHAR(5000)"
            group_id = [group_key]
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

        if key is not None and key not in cols:
            msg = f"Please select key from {cols}!"
            logger.error(msg)
            raise ValueError(msg)
        cols.remove(key)
        exog = cols
        if add_placeholder:
            data_ = data.add_constant('PLACE_HOLDER', 0.0)[group_id + [key] + ['PLACE_HOLDER'] + exog]
        else:
            data_ = data[group_id + [key] + exog]

        setattr(self, 'predict_data', data_)
        if self.massive is True:
            predict_output_signature = [{'GROUP_ID': 'NVARCHAR(100)', key: data.get_table_structure()[key], 'YHAT': 'DOUBLE', 'YHAT_LOWER': 'DOUBLE', 'YHAT_UPPER': 'DOUBLE'}, {'GROUP_ID': 'NVARCHAR(100)', key: data.get_table_structure()[key], 'TREND': 'DOUBLE', 'SEASONAL': 'NCLOB', 'HOLIDAY': 'NCLOB', 'EXOGENOUS': 'NCLOB'}, {'GROUP_ID': 'NVARCHAR(100)', 'ERROR_TIMESTAMP': 'NVARCHAR(100)', 'ERRORCODE': 'INT', 'MESSAGE': 'NVARCHAR(200)'}]
        else:
            if show_explainer:
                predict_output_signature = [{key: data.get_table_structure()[key], 'YHAT': 'DOUBLE', 'YHAT_LOWER': 'DOUBLE', 'YHAT_UPPER': 'DOUBLE'}, {key: data.get_table_structure()[key], 'TREND': 'DOUBLE', 'SEASONAL': 'NCLOB', 'HOLIDAY': 'NCLOB', 'EXOGENOUS': 'NCLOB'}]
            else:
                predict_output_signature = [{key: data.get_table_structure()[key], 'YHAT': 'DOUBLE', 'YHAT_LOWER': 'DOUBLE', 'YHAT_UPPER': 'DOUBLE'}]
        setattr(self, "predict_output_signature", predict_output_signature)
        converted_int2timestamp = False
        data_key = None
        if self.massive is not True:
            if _is_index_int(data_, key):
                data_key = data_.select(key)
                data_ = _convert_int_index_to_timestamp_index(data_, key)
                converted_int2timestamp = True
        forecast_result = super(AdditiveModelForecast, self)._predict(data_,
                                                                      group_params,
                                                                      predict_params,
                                                                      group_key_type)
        if isinstance(forecast_result, (list, tuple)):
            if converted_int2timestamp:
                forecast_result[0] = _convert_timestamp_index_to_int_index(forecast_result[0], key, data_key)
            setattr(self, "forecast_result", forecast_result[0])
        else:
            if converted_int2timestamp:
                forecast_result = _convert_timestamp_index_to_int_index(forecast_result, key, data_key)
            setattr(self, "forecast_result", forecast_result)
        if hasattr(self, 'explainer_'):
            setattr(self, "reason_code", self.explainer_)
        return forecast_result

    def build_report(self):
        r"""
        Generate time series report.
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
        tse = AdditiveModelForecastExplainer(key=self.key, endog=self.endog, exog=self.exog)
        tse.add_line_to_comparison_item("Training Data", data=self.training_data, x_name=self.key, y_name=self.endog)
        if hasattr(self, 'forecast_result'):
            if self.forecast_result:
                tse.add_line_to_comparison_item("Forecast Result",
                                                data=self.forecast_result,
                                                x_name=self.forecast_result.columns[0],
                                                y_name=self.forecast_result.columns[1])
                tse.add_line_to_comparison_item('Predict Interval', data=self.forecast_result, x_name=self.forecast_result.columns[0], confidence_interval_names=[self.forecast_result.columns[2], self.forecast_result.columns[3]],color="#ccc")
        page0.addItems(tse.get_comparison_item())
        pages.append(page0)
        if hasattr(self, 'reason_code'):
            if self.reason_code:
                page1 = Page("Explainability")
                try:
                    if 'seasonality_mode' not in self.init_params:
                        self.init_params['seasonality_mode'] = 'additive'
                    if self.init_params['seasonality_mode']:
                        if self.init_params['seasonality_mode'] == 'additive':
                            exogenous_names_with_additive_mode = set(self.exog)
                            exogenous_names_with_multiplicative_mode = set()
                        else:
                            exogenous_names_with_additive_mode = set()
                            exogenous_names_with_multiplicative_mode = set(self.exog)
                    else:
                        exogenous_names_with_additive_mode = set(self.exog)
                        exogenous_names_with_multiplicative_mode = set()
                    if 'regressor' in self.init_params:
                        for item in self.init_params['regressor']:
                            dict_item = json.loads(item)
                            if dict_item['MODE'] == 'additive':
                                exogenous_names_with_additive_mode.add(dict_item['NAME'])
                                exogenous_names_with_multiplicative_mode.discard(dict_item['NAME'])
                            else:
                                exogenous_names_with_multiplicative_mode.add(dict_item['NAME'])
                                exogenous_names_with_additive_mode.discard(dict_item['NAME'])
                    tse.set_seasonality_mode(list(exogenous_names_with_additive_mode), list(exogenous_names_with_multiplicative_mode))
                    tse.set_forecasted_data(self.predict_data)
                    tse.set_forecasted_result_explainer(self.reason_code)
                    page1.addItems(tse.get_decomposition_items_from_forecasted_result())
                    page1.addItems(tse.get_summary_plot_items_from_forecasted_result())
                    page1.addItems(tse.get_force_plot_item_from_forecasted_result())
                except Exception as err:
                    logger.error(err)
                    pass
                pages.append(page1)
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

    def get_permutation_importance(self, data, model=None, key=None, endog=None, exog=None, repeat_time=None, random_state=None, thread_ratio=None,
                                   partition_ratio=None, regressor_top_k=None, accuracy_measure=None, ignore_zero=None):
        """
        Please see :ref:`permutation_imp_ts-label` for details.
        """
        if hasattr(self, 'massive'):
            if self.massive:
                msg = 'Permutation importance is not supported in the massive mode!'
                logger.error(msg)
                raise ValueError(msg)

        if key is None:
            key = self.key
        if endog is None:
            endog = self.endog
        if exog is None:
            exog = self.exog
        if model is None:
            if getattr(self, 'model_') is None:
                raise FitIncompleteError()
            model = self.model_
        if isinstance(model, str):
            model = model.lower()
            if model == 'rdt':
                model = None
            else:
                msg = "Permutation importance only supports 'rdt' as model free method!"
                logger.error(msg)
                raise ValueError(msg)

        if model is None:
            permutation_data = data[[key] + [endog] + exog]
            is_index_int = _is_index_int(permutation_data, key)
            if not is_index_int:
                permutation_data = _convert_index_from_timestamp_to_int(permutation_data, key)
            real_data=None
        else:
            permutation_data = data[[key] + [endog] + exog]
            real_data = data[[key] + [endog]]

        return super()._get_permutation_importance(permutation_data, real_data, model, repeat_time, random_state, thread_ratio,
                                                   partition_ratio, regressor_top_k, accuracy_measure, ignore_zero)
