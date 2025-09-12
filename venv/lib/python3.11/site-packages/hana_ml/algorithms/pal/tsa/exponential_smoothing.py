#pylint: disable=too-many-lines, line-too-long, invalid-name, relative-beyond-top-level, too-few-public-methods, no-member
#pylint: disable=too-many-arguments, too-many-instance-attributes, too-few-public-methods, too-many-branches
#pylint: disable=attribute-defined-outside-init, too-many-statements, too-many-locals, bare-except, unnecessary-pass
#pylint: disable=unused-import, super-with-arguments, c-extension-no-member, broad-except, use-a-generator, consider-using-dict-items
"""
This module contains Python wrappers for PAL exponential smoothing algorithms.

The following classes are available:

    * :class:`SingleExponentialSmoothing`
    * :class:`DoubleExponentialSmoothing`
    * :class:`TripleExponentialSmoothing`
    * :class:`AutoExponentialSmoothing`
    * :class:`BrownExponentialSmoothing`
    * :class:`Croston`
    * :class:`CrostonTSB`

"""
import logging
import uuid
from hdbcli import dbapi
from hana_ml.ml_base import quotename
from hana_ml.visualizers.report_builder import Page
from hana_ml.visualizers.time_series_report import TimeSeriesExplainer
from .utility import _convert_index_from_timestamp_to_int, _is_index_int, _col_index_check
from .utility import _get_forecast_starttime_and_timedelta, _delete_none_key_in_dict
from ..pal_base import (
    PALBase,
    pal_param_register,
    arg,
    ParameterTable,
    ListOfStrings,
    try_drop,
    require_pal_usable
)
from ..utility import check_pal_function_exist, _map_param
from ..unified_exponentialsmoothing import UnifiedExponentialSmoothing

logger = logging.getLogger(__name__)

def _check_column(col_name, col_val, cols):
    if col_val:
        if col_val not in cols:
            msg = f"Invalid '{col_name}' selection: '{col_val}'. Please select '{col_name}' from the available columns: {cols}!"
            logger.error(msg)
            raise ValueError(msg)

class _ExponentialSmoothingBase(PALBase):

    trend_test_map = {'mk': 1, 'difference-sign': 2}
    def __init__(self,
                 model_selection=None,# Auto ESM
                 forecast_model_name=None,# Auto ESM
                 optimizer_time_budget=None,# Auto ESM
                 max_iter=None,# Auto ESM
                 optimizer_random_seed=None,# Auto ESM
                 thread_ratio=None,# Auto ESM
                 alpha=None,
                 beta=None,
                 gamma=None,
                 phi=None,
                 forecast_num=None,
                 seasonal_period=None,
                 seasonal=None,
                 initial_method=None,
                 training_ratio=None,
                 damped=None,
                 accuracy_measure=None,
                 seasonality_criterion=None,# Auto ESM
                 trend_test_method=None,# Auto ESM
                 trend_test_alpha=None,# Auto ESM
                 alpha_min=None, # Auto ESM
                 beta_min=None,# Auto ESM
                 gamma_min=None,# Auto ESM
                 phi_min=None,# Auto ESM
                 alpha_max=None,# Auto ESM
                 beta_max=None,# Auto ESM
                 gamma_max=None,# Auto ESM
                 phi_max=None,# Auto ESM
                 prediction_confidence_1=None,
                 prediction_confidence_2=None,
                 level_start=None,
                 trend_start=None,
                 season_start=None,
                 delta=None,#SESM
                 adaptive_method=None,#SESM
                 ignore_zero=None,
                 expost_flag=None,
                 method=None
                ):
        if not hasattr(self, 'hanaml_parameters'):
            setattr(self, 'hanaml_parameters', pal_param_register())
        super(_ExponentialSmoothingBase, self).__init__()

        self.model_selection = self._arg('model_selection', model_selection, (int, bool))
        self.forecast_model_name = self._arg('forecast_model_name', forecast_model_name, str)
        self.optimizer_time_budget = self._arg('optimizer_time_budget', optimizer_time_budget, int)
        self.max_iter = self._arg('max_iter', max_iter, int)
        self.optimizer_random_seed = self._arg('optimizer_random_seed', optimizer_random_seed, int)
        self.thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        self.alpha = self._arg('alpha', alpha, float)
        self.beta = self._arg('beta', beta, float)
        self.gamma = self._arg('gamma', gamma, float)
        self.phi = self._arg('phi', phi, float)
        self.forecast_num = self._arg('forecast_num', forecast_num, int)
        self.seasonal_period = self._arg('seasonal_period', seasonal_period, int)
        self.seasonal = self._arg('seasonal', seasonal, (int, str))
        if isinstance(self.seasonal, str):
            self.seasonal = self._arg('seasonal', seasonal,
                                      dict(multiplicative=0, additive=1))
        self.initial_method = self._arg('initial_method', initial_method, int)
        self.training_ratio = self._arg('training_ratio', training_ratio, float)
        self.damped = self._arg('damped', damped, (int, bool))
        self.seasonality_criterion = self._arg('seasonality_criterion', seasonality_criterion, float)
        self.trend_test_method = self._arg('trend_test_method', trend_test_method, (int, str))
        if isinstance(self.trend_test_method, str):
            self.trend_test_method = self._arg('trend_test_method',
                                               trend_test_method,
                                               self.trend_test_map)
        self.trend_test_alpha = self._arg('trend_test_alpha', trend_test_alpha, float)
        self.alpha_min = self._arg('alpha_min', alpha_min, float)
        self.beta_min = self._arg('beta_min', beta_min, float)
        self.gamma_min = self._arg('gamma_min', gamma_min, float)
        self.phi_min = self._arg('phi_min', phi_min, float)
        self.alpha_max = self._arg('alpha_max', alpha_max, float)
        self.beta_max = self._arg('beta_max', beta_max, float)
        self.gamma_max = self._arg('gamma_max', gamma_max, float)
        self.phi_max = self._arg('phi_max', phi_max, float)
        self.prediction_confidence_1 = self._arg('prediction_confidence_1', prediction_confidence_1, float)
        self.prediction_confidence_2 = self._arg('prediction_confidence_2', prediction_confidence_2, float)
        self.level_start = self._arg('level_start', level_start, float)
        self.trend_start = self._arg('trend_start', trend_start, float)
        self.delta = self._arg('delta', delta, float)
        self.adaptive_method = self._arg('adaptive_method', adaptive_method, bool)
        self.ignore_zero = self._arg('ignore_zero', ignore_zero, bool)
        self.expost_flag = self._arg('expost_flag', expost_flag, bool)
        self.method = self._arg('method', method, int)

        # accuracy_measure for single/double/triple exp smooth
        accuracy_measure_list = {"mpe":"mpe", "mse":"mse", "rmse":"rmse", "et":"et",
                                 "mad":"mad", "mase":"mase", "wmape":"wmape",
                                 "smape":"smape", "mape":"mape"}
        if accuracy_measure is not None:
            if isinstance(accuracy_measure, str):
                accuracy_measure = [accuracy_measure]
            for acc in accuracy_measure:
                self._arg('accuracy_measure', acc.lower(), accuracy_measure_list)
            self.accuracy_measure = [acc.upper() for acc in accuracy_measure]
        else:
            self.accuracy_measure = None

        #check self.season_start which is a list of tuple. Each tuple has two elements and 1st element is int and 2nd is float
        self.season_start = self._arg('season_start', season_start, list)
        if self.season_start is not None:
            if all(isinstance(elm, tuple) for elm in self.season_start):
                if not all(len(elm) == 2 for elm in self.season_start):
                    msg = "If 'season_start' is a list of tuples, the each tuple " +\
                    "must be of length 2."
                    logger.error(msg)
                    raise ValueError(msg)
                for element in self.season_start:
                    if not isinstance(element[0], int):
                        msg = ('The type of the first element of the tuple of season_start should be int!')
                        logger.error(msg)
                        raise ValueError(msg)
                    if not isinstance(element[1], (float, int)):
                        msg = ('The type of the second element of the tuple of season_start should be float!')
                        logger.error(msg)
                        raise ValueError(msg)
            elif not all(isinstance(elm, (float, int)) for elm in self.season_start):
                msg = "If 'season_start' is a not a list of tuples, then it must be "+\
                "a list of numerical values."
                logger.error(msg)
                raise ValueError(msg)
        self.is_index_int = None
        self.forecast_start = None
        self.timedelta = None

    def _fit_predict(self, exp_smooth_function, data, key, endog):
        conn = data.connection_context
        require_pal_usable(conn)
        setattr(self, "training_data", data)
        setattr(self, "key", key)
        setattr(self, "endog", endog)
        setattr(self, "exog", None)
        if not self._disable_hana_execution:
            cols = data.columns
            if len(cols) < 2:
                msg = ("Input data should contain at least 2 columns: " +
                       "one for ID, another for raw data.")
                logger.error(msg)
                raise ValueError(msg)

            index = data.index
            key = self._arg('key', key, str)
            if key is not None and key not in cols:
                msg = ('Please select key from name of columns!')
                logger.error(msg)
                raise ValueError(msg)

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
                        warn_msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                        "and the designated index column '{}'.".format(index)
                        logger.warning(warn_msg)
            else:
                if key is None:
                    key = cols[0]
            cols.remove(key)

            endog = self._arg('endog', endog, str)
            if endog is not None:
                if endog not in cols:
                    msg = ('Please select endog from name of columns!')
                    logger.error(msg)
                    raise ValueError(msg)
            else:
                endog = cols[0]

            data_ = data[[key] + [endog]]

            self.is_index_int = _is_index_int(data_, key)
            if not self.is_index_int:
                data_ = _convert_index_from_timestamp_to_int(data_, key)
            try:
                self.forecast_start, self.timedelta = _get_forecast_starttime_and_timedelta(data, key, self.is_index_int)
            except Exception as err:
                logger.warning(err)
                pass
        else:
            data_ = data
        function_map = {1:'PAL_SINGLE_EXPSMOOTH',
                        2:'PAL_DOUBLE_EXPSMOOTH',
                        3:'PAL_TRIPLE_EXPSMOOTH',
                        4:'PAL_AUTO_EXPSMOOTH',
                        5:'PAL_BROWN_EXPSMOOTH',
                        6:'PAL_CROSTON'}
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        outputs = ['FORECAST', 'STATISTICS']
        outputs = ['#PAL_EXP_SMOOTHING_{}_TBL_{}_{}'.format(name, self.id, unique_id)
                   for name in outputs]
        forecast_tbl, stats_tbl = outputs
        param_rows = [
            ('MODELSELECTION', self.model_selection, None, None),
            ('FORECAST_MODEL_NAME', None, None, self.forecast_model_name),
            ('OPTIMIZER_TIME_BUDGET', self.optimizer_time_budget, None, None),
            ('MAX_ITERATION', self.max_iter, None, None),
            ('OPTIMIZER_RANDOM_SEED', self.optimizer_random_seed, None, None),
            ('THREAD_RATIO', None, self.thread_ratio, None),
            ('ALPHA', None, self.alpha, None),
            ('BETA', None, self.beta, None),
            ('GAMMA', None, self.gamma, None),
            ('PHI', None, self.phi, None),
            ('FORECAST_NUM', self.forecast_num, None, None),
            ('CYCLE', self.seasonal_period, None, None),
            ('SEASONAL', self.seasonal, None, None),
            ('INITIAL_METHOD', self.initial_method, None, None),
            ('TRAINING_RATIO', None, self.training_ratio, None),
            ('DAMPED', self.damped, None, None),
            ('SEASONALITY_CRITERION', None, self.seasonality_criterion, None),
            ('TREND_TEST_METHOD', self.trend_test_method, None, None),
            ('TREND_TEST_ALPHA', None, self.trend_test_alpha, None),
            ('ALPHA_MIN', None, self.alpha_min, None),
            ('BETA_MIN', None, self.beta_min, None),
            ('GAMMA_MIN', None, self.gamma_min, None),
            ('PHI_MIN', None, self.phi_min, None),
            ('ALPHA_MAX', None, self.alpha_max, None),
            ('BETA_MAX', None, self.beta_max, None),
            ('GAMMA_MAX', None, self.gamma_max, None),
            ('PHI_MAX', None, self.phi_max, None),
            ('PREDICTION_CONFIDENCE_1', None, self.prediction_confidence_1, None),
            ('PREDICTION_CONFIDENCE_2', None, self.prediction_confidence_2, None),
            ('LEVEL_START', None, self.level_start, None),
            ('TREND_START', None, self.trend_start, None),
            ('DELTA', None, self.delta, None),#SESM
            ('ADAPTIVE_METHOD', self.adaptive_method, None, None),#SESM
            ('IGNORE_ZERO', self.ignore_zero, None, None),
            ('EXPOST_FLAG', self.expost_flag, None, None),
            ('METHOD', self.method, None, None)
        ]
        if self.accuracy_measure is not None:
            if isinstance(self.accuracy_measure, str):
                self.accuracy_measure = [self.accuracy_measure]
            for acc_measure in self.accuracy_measure:
                param_rows.extend([('ACCURACY_MEASURE', None, None, acc_measure)])
                param_rows.extend([('MEASURE_NAME', None, None, acc_measure)])
        if self.season_start is not None:
            if isinstance(self.season_start, tuple):
                param_rows.extend([('SEASON_START', element[0], element[1], None)
                                   for element in self.season_start])
            else:
                param_rows.extend([('SEASON_START', idx + 1, val, None)
                                   for idx, val in enumerate(self.season_start)])

        pal_function = function_map[exp_smooth_function]

        if exp_smooth_function == 5:
            if check_pal_function_exist(conn, 'BROWN%INTERVAL%', like=True):
                pal_function = 'PAL_BROWN_EXPSMOOTH_INTERVAL'

        try:
            self._call_pal_auto(conn,
                                pal_function,
                                data_,
                                ParameterTable().with_data(param_rows),
                                forecast_tbl,
                                stats_tbl)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, forecast_tbl)
            try_drop(conn, stats_tbl)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, forecast_tbl)
            try_drop(conn, stats_tbl)
            raise
        self.stats_ = conn.table(stats_tbl)
        self.statistics_ = self.stats_
        self.forecast_ = conn.table(forecast_tbl)
        if not (self.is_index_int or self._disable_hana_execution):
            if exp_smooth_function < 5:
                fct_ = conn.sql("""
                                SELECT ADD_SECONDS('{0}', ({1}-{9}) * {2}) AS {10},
                                {4},
                                {5},
                                {6},
                                {7},
                                {8}
                                FROM ({3})
                                """.format(self.forecast_start,
                                           quotename(self.forecast_.columns[0]),
                                           self.timedelta,
                                           self.forecast_.select_statement,
                                           quotename(self.forecast_.columns[1]),
                                           quotename(self.forecast_.columns[2]),
                                           quotename(self.forecast_.columns[3]),
                                           quotename(self.forecast_.columns[4]),
                                           quotename(self.forecast_.columns[5]),
                                           data.count() + 1,
                                           quotename(key)))
            if exp_smooth_function == 5:
                if pal_function == 'PAL_BROWN_EXPSMOOTH_INTERVAL':
                    fct_ = conn.sql("""
                                    SELECT ADD_SECONDS('{0}', ({1}-{9}) * {2}) AS {10},
                                    {4},
                                    {5},
                                    {6},
                                    {7},
                                    {8}
                                    FROM ({3})
                                    """.format(self.forecast_start,
                                               quotename(self.forecast_.columns[0]),
                                               self.timedelta,
                                               self.forecast_.select_statement,
                                               quotename(self.forecast_.columns[1]),
                                               quotename(self.forecast_.columns[2]),
                                               quotename(self.forecast_.columns[3]),
                                               quotename(self.forecast_.columns[4]),
                                               quotename(self.forecast_.columns[5]),
                                               data.count() + 1,
                                               quotename(key)))
                else:
                    fct_ = conn.sql("""
                                    SELECT ADD_SECONDS('{0}', ({1}-{5}) * {2}) AS {6},
                                    {4} FROM ({3})
                                    """.format(self.forecast_start,
                                               quotename(self.forecast_.columns[0]),
                                               self.timedelta,
                                               self.forecast_.select_statement,
                                               quotename(self.forecast_.columns[1]),
                                               data.count() + 1,
                                               quotename(key)))
            if exp_smooth_function == 6:
                fct_ = conn.sql("""
                                SELECT ADD_SECONDS('{0}', ({1}-{6}) * {2}) AS {5},
                                {4} FROM ({3})
                                """.format(self.forecast_start,
                                           quotename(self.forecast_.columns[0]),
                                           self.timedelta,
                                           self.forecast_.select_statement,
                                           quotename(self.forecast_.columns[1]),
                                           quotename(key),
                                           data.count() + 1))
            self.forecast_ = fct_
        setattr(self, "forecast_result", self.forecast_)
        return self.forecast_

    def build_report(self):
        r"""
        Generate time series report.
        """
        from hana_ml.visualizers.time_series_report_template_helper import TimeSeriesTemplateReportHelper #pylint: disable=cylic-import
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
        tse.add_line_to_comparison_item("Forecast Data", data=self.forecast_result, x_name=self.forecast_result.columns[0], y_name=self.forecast_result.columns[1])
        tse.add_line_to_comparison_item('PI1', data=self.forecast_result, x_name=self.forecast_result.columns[0], confidence_interval_names=[self.forecast_result.columns[2], self.forecast_result.columns[3]],color="pink")
        tse.add_line_to_comparison_item('PI2', data=self.forecast_result, x_name=self.forecast_result.columns[0], confidence_interval_names=[self.forecast_result.columns[4], self.forecast_result.columns[5]],color="#ccc")
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

class SingleExponentialSmoothing(_ExponentialSmoothingBase):
    r"""
    Single exponential smoothing is suitable to model the time series without trend and seasonality.
    In the model, the smoothed value is the weighted sum of previous smoothed value and previous observed value.
    PAL provides two simple exponential smoothing algorithms: single exponential smoothing and adaptive-response-rate simple exponential smoothing.
    The adaptive-response-rate single exponential smoothing algorithm may have an advantage over single exponential smoothing in that it allows the value of alpha to be modified.

    Parameters
    ----------

    alpha : float, optional
        The smoothing constant alpha for single exponential smoothing,
        or the initialization value for adaptive-response-rate single exponential smoothing.

        Valid range is (0, 1).

        Defaults to 0.1 for single exponential smoothing, and 0.2 for adaptive-response-rate single exponential smoothing.

    delta : float, optional
        Value of weighted for At and Mt(relative for the computation of adaptive smoothing parameter).

        The definitions of At and Mt are stated in
        `SAP HANA PAL Single Exponential Smoothing <https://help.sap.com/viewer/2cfbc5cf2bc14f028cfbe2a2bba60a50/2.0.06/en-US/ba4bb85a74d84c2b994aa7192cac3b1b.html>`_

        Only valid when ``adaptive_method`` is True.

        Defaults to 0.2.

    forecast_num : int, optional
        Number of values to be forecast.

        Defaults to 0.

    adaptive_method : bool, optional

        - False: Single exponential smoothing.
        - True: Adaptive-response-rate single exponential smoothing.

        Defaults to False.

    accuracy_measure : str or a list of str, optional

        The metric to quantify how well a model fits input data.
        Options: "mpe", "mse", "rmse", "et", "mad", "mase", "wmape", "smape", "mape".

        No default value.

        .. Note::
            Specify a measure name if you want the corresponding measure value to be
            reflected in the output statistics self.stats\_.

    ignore_zero : bool, optional

        - False: Uses zero values in the input dataset when calculating "mpe" or "mape".
        - True: Ignores zero values in the input dataset when calculating "mpe" or "mape".

        Only valid when ``accuracy_measure`` is "mpe" or "mape".

        Defaults to False.

    expost_flag : bool, optional

        - False: Does not output the expost forecast, and just outputs the forecast values.
        - True: Outputs the expost forecast and the forecast values.

        Defaults to True.

    prediction_confidence_1 : float, optional
        Prediction confidence for interval 1.

        Only valid when the upper and lower columns are provided in the result table.

        Defaults to 0.8.

    prediction_confidence_2 : float, optional
        Prediction confidence for interval 2.

        Only valid when the upper and lower columns are provided in the result table.

        Defaults to 0.95.

    Attributes
    ----------
    forecast_ : DataFrame
        Forecast values.

    stats_ : DataFrame
        Statistics.

    Examples
    --------
    >>> sesm = SingleExponentialSmoothing(adaptive_method=False,
                                          accuracy_measure='mse',
                                          alpha=0.1,
                                          delta=0.2,
                                          forecast_num=12,
                                          expost_flag=True)

    Perform fit_predict():

    >>> sesm.fit_predict(data=df)

    Output:

    >>> sesm.forecast_.collect()
    >>> sesm.stats_.collect()

    """
    op_name = 'SingleExpSm'
    def __init__(self,
                 alpha=None,
                 delta=None,
                 forecast_num=None,
                 adaptive_method=None,
                 accuracy_measure=None,
                 ignore_zero=None,
                 expost_flag=None,
                 prediction_confidence_1=None,
                 prediction_confidence_2=None
                ):
        setattr(self, 'hanaml_parameters', pal_param_register())
        if delta is not None and adaptive_method is False:
            msg = ('delta is only valid when adaptive_method is True!')
            logger.error(msg)
            raise ValueError(msg)

        super(SingleExponentialSmoothing, self).__init__(
            alpha=alpha,
            delta=delta,
            forecast_num=forecast_num,
            adaptive_method=adaptive_method,
            accuracy_measure=accuracy_measure,
            ignore_zero=ignore_zero,
            expost_flag=expost_flag,
            prediction_confidence_1=prediction_confidence_1,
            prediction_confidence_2=prediction_confidence_2)

    def fit_predict(self, data, key=None, endog=None):
        """
        Fit and predict based on the given time series.

        Parameters
        ----------
        data : DataFrame
            Input data. At least two columns, one is ID column, the other is raw data.

        key : str, optional
            The ID column.

            Defaults to the first column of data if the index column of data is not provided.
            Otherwise, defaults to the index column of data.

        endog : str, optional
            The column of series to be fitted and predicted.

            Defaults to the first non-ID column.

        Returns
        -------
        DataFrame

            Forecast values.
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        return super(SingleExponentialSmoothing, self)._fit_predict(exp_smooth_function=1,
                                                                    data=data,
                                                                    key=key,
                                                                    endog=endog)

class DoubleExponentialSmoothing(_ExponentialSmoothingBase):
    r"""
    Double exponential smoothing is suitable to model the time series with trend but without seasonality.
    In the model there are two kinds of smoothed quantities: smoothed signal and smoothed trend.

    Parameters
    ----------

    alpha : float, optional
        Weight for smoothing. Value range: 0 < alpha < 1.

        Defaults to 0.1.

    beta : float, optional
        Weight for the trend component. Value range: 0 < beta < 1.

        Defaults to 0.1.

    forecast_num : int, optional
        Number of values to be forecast.

        Defaults to 0.

    phi : float, optional
        Value of the damped smoothing constant phi (0 < phi < 1).

        Defaults to 0.1.

    damped : bool, optional
        Specifies whether or not to use damped trend method.

        - False: No, uses the Holt's linear trend method.
        - True: Yes, use damped trend method.

        Defaults to False.

    accuracy_measure : str or a list of str, optional

        The metric to quantify how well a model fits input data.
        Options: "mpe", "mse", "rmse", "et", "mad", "mase", "wmape", "smape", "mape".

        No default value.

        .. Note::
            Specify a measure name if you want the corresponding measure value to be
            reflected in the output statistics self.stats\_.

    ignore_zero : bool, optional

        - False: Uses zero values in the input dataset when calculating "mpe" or "mape".
        - True: Ignores zero values in the input dataset when calculating "mpe" or "mape".

        Only valid when ``accuracy_measure`` is "mpe" or "mape".

        Defaults to False.

    expost_flag : bool, optional

        - False: Does not output the expost forecast, and just outputs the forecast values.
        - True: Outputs the expost forecast and the forecast values.

        Defaults to True.

    prediction_confidence_1 : float, optional
        Prediction confidence for interval 1.

        Only valid when the upper and lower columns are provided in the result table.

        Defaults to 0.8.

    prediction_confidence_2 : float, optional
        Prediction confidence for interval 2.

        Only valid when the upper and lower columns are provided in the result table.

        Defaults to 0.95.

    Attributes
    ----------
    forecast_ : DataFrame
        Forecast values.

    stats_ : DataFrame
        Statistics.

    Examples
    --------
    >>> desm = DoubleExponentialSmoothing(alpha=0.501,
                                          beta=0.072,
                                          forecast_num=6)

    Perform fit_predict():

    >>> desm.fit_predict(data=df)

    Output:

    >>> desm.forecast_.collect()
    >>> desm.stats_.collect()

    """
    op_name = 'DoubleExpSm'
    def __init__(self,
                 alpha=None,
                 beta=None,
                 forecast_num=None,
                 phi=None,
                 damped=None,
                 accuracy_measure=None,
                 ignore_zero=None,
                 expost_flag=None,
                 prediction_confidence_1=None,
                 prediction_confidence_2=None
                ):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(DoubleExponentialSmoothing, self).__init__(
            alpha=alpha,
            beta=beta,
            forecast_num=forecast_num,
            phi=phi,
            damped=damped,
            accuracy_measure=accuracy_measure,
            ignore_zero=ignore_zero,
            expost_flag=expost_flag,
            prediction_confidence_1=prediction_confidence_1,
            prediction_confidence_2=prediction_confidence_2)

    def fit_predict(self, data, key=None, endog=None):
        """
        Fit and predict based on the given time series.

        Parameters
        ----------
        data : DataFrame
            Input data. At least two columns, one is ID column, the other is raw data.

        key : str, optional
            The ID column.

            Defaults to the first column of data if the index column of data is not provided.
            Otherwise, defaults to the index column of data.

        endog : str, optional
            The column of series to be fitted and predicted.

            Defaults to the first non-ID column.

        Returns
        -------
        DataFrame

            Forecast values.
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        return super(DoubleExponentialSmoothing, self)._fit_predict(exp_smooth_function=2,
                                                                    data=data,
                                                                    key=key,
                                                                    endog=endog)

class TripleExponentialSmoothing(_ExponentialSmoothingBase):
    r"""
    Triple exponential smoothing is used to handle the time series data containing a seasonal component.

    Parameters
    ----------
    alpha : float, optional
        Weight for smoothing. Value range: 0 < alpha < 1.

        Defaults to 0.1.

    beta : float, optional
        Weight for the trend component. Value range: 0 <= beta < 1.

        Defaults to 0.1.

    gamma : float, optional
        Weight for the seasonal component. Value range: 0 < gamma < 1.

        Defaults to 0.1.

    seasonal_period : int, optional
        Length of a seasonal_period(should be greater than 1).

        For example, the ``seasonal_period`` of quarterly data is 4,
        and the ``seasonal_period`` of monthly data is 12.

        Defaults to 2.

    forecast_num : int, optional
        Number of values to be forecast.

        Defaults to 0.

    seasonal : {'multiplicative', 'additive'}, optional
        Specifies the type of model for triple exponential smoothing.

            - 'multiplicative': Multiplicative triple exponential smoothing.
            - 'additive': Additive triple exponential smoothing.

        When ``seasonal`` is set to 'additive', the default value of initial_method is 1;
        When ``seasonal`` is set to 'multiplicative', the default value of initial_method is 0.

        Defaults to 'multiplicative'.

    initial_method : int, optional
        Initialization method for the trend and seasonal components.

        Defaults to 0 or 1, depending the setting of ``seasonal``.

    phi : float, optional
        Value of the damped smoothing constant phi (0 < phi < 1).

        Defaults to 0.1.

    damped : bool, optional
        Specifies whether or not to use damped trend method.

        - False: No, uses the Holt's linear trend method.
        - True: Yes, use damped trend method.

        Defaults to False.

    accuracy_measure : str or a list of str, optional

        The metric to quantify how well a model fits input data.
        Options: "mpe", "mse", "rmse", "et", "mad", "mase", "wmape", "smape", "mape".

        No default value.

        .. Note::
            Specify a measure name if you want the corresponding measure value to be
            reflected in the output statistics self.stats\_.

    ignore_zero : bool, optional

        - False: Uses zero values in the input dataset when calculating "mpe" or "mape".
        - True: Ignores zero values in the input dataset when calculating "mpe" or "mape".

        Only valid when ``accuracy_measure`` is "mpe" or "mape".

        Defaults to False.

    expost_flag : bool, optional

        - False: Does not output the expost forecast, and just outputs the forecast values.
        - True: Outputs the expost forecast and the forecast values.

        Defaults to True.

    level_start : float, optional
        The initial value for level component S.

        If this value is not provided, it will be calculated in the way as described in Triple Exponential Smoothing.

        ``level_start`` cannot be zero. If it is set to zero, 0.0000000001 will be used instead.

    trend_start : float, optional
        The initial value for trend component B.

    season_start : list of tuple/float, optional
        A list of initial values for seasonal component C. If specified, the list
        must be of the length specified in ``seasonal_period``, i.e. start values
        must be provided for a whole seasonal period.

        We can simply give out the start values in a list, where the cycle index of each value is determined by
        its index in the list; or we can give out the start values together with their cycle indices in a list of tuples.

        For example, suppose the seasonal period is 4, with starting values :math:`x_i, 1 \leq i \leq 4` indexed by their cycle ID.
        Then the four season start values can be specified in a list as :math:`[x_1, x_2, x_3, x_4]`,
        or equivalently in a list of tuples as :math:`[(1, x_1), (2, x_2), (3, x_3), (4, x_4)]`.

        If not provided, start values shall be computed by a default scheme.

    prediction_confidence_1 : float, optional
        Prediction confidence for interval 1.

        Only valid when the upper and lower columns are provided in the result table.

        Defaults to 0.8.

    prediction_confidence_2 : float, optional
        Prediction confidence for interval 2.

        Only valid when the upper and lower columns are provided in the result table.

        Defaults to 0.95.

    Attributes
    ----------
    forecast_ : DataFrame
        Forecast values.

    stats\_ : DataFrame
        Statistics analysis content.

    Examples
    --------
    Input DataFrame df:

    >>> df.collect()
    ID    RAW_DATA
     1       362.0
     2       385.0
    ...
    23       854.0
    24       661.0

    Create a TripleExponentialSmoothing instance:

    >>> tesm = TripleExponentialSmoothing(alpha=0.822,
                                          beta=0.055,
                                          gamma=0.055)

    Perform fit_predict():

    >>> tesm.fit_predict(data=df)

    Output:

    >>> tesm.forecast_.collect().set_index('TIMESTAMP').head(3)
    TIMESTAMP           VALUE   PI1_LOWER    PI1_UPPER   PI2_LOWER    PI2_UPPER
           5       371.288158         NaN          NaN         NaN          NaN
           6       414.636207         NaN          NaN         NaN          NaN
           7       471.431808         NaN          NaN         NaN          NaN

    >>> tesm.stats_.collect()
    STAT_NAME        STAT_VALUE
          MSE        616.541542
    """
    op_name = 'TripleExpSm'
    def __init__(self,
                 alpha=None,
                 beta=None,
                 gamma=None,
                 seasonal_period=None,
                 forecast_num=None,
                 seasonal=None,
                 initial_method=None,
                 phi=None,
                 damped=None,
                 accuracy_measure=None,
                 ignore_zero=None,
                 expost_flag=None,
                 level_start=None,
                 trend_start=None,
                 season_start=None,
                 prediction_confidence_1=None,
                 prediction_confidence_2=None
                ):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(TripleExponentialSmoothing, self).__init__(
            alpha=alpha,
            beta=beta,
            gamma=gamma,
            seasonal_period=seasonal_period,
            forecast_num=forecast_num,
            seasonal=seasonal,
            initial_method=initial_method,
            phi=phi,
            damped=damped,
            accuracy_measure=accuracy_measure,
            ignore_zero=ignore_zero,
            expost_flag=expost_flag,
            level_start=level_start,
            trend_start=trend_start,
            season_start=season_start,
            prediction_confidence_1=prediction_confidence_1,
            prediction_confidence_2=prediction_confidence_2)

    def fit_predict(self, data, key=None, endog=None):
        """
        Fit and predict based on the given time series.

        Parameters
        ----------
        data : DataFrame
            Input data. At least two columns, one is ID column, the other is raw data.

        key : str, optional
            The ID column.

            Defaults to the first column of data if the index column of data is not provided.
            Otherwise, defaults to the index column of data.

        endog : str, optional
            The column of series to be fitted and predicted.

            Defaults to the first non-ID column.

        Returns
        -------
        DataFrame

            Forecast values.
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        return super(TripleExponentialSmoothing, self)._fit_predict(exp_smooth_function=3,
                                                                    data=data,
                                                                    key=key,
                                                                    endog=endog)

class AutoExponentialSmoothing(_ExponentialSmoothingBase):
    r"""
    Auto exponential smoothing is used to calculate optimal parameters of a set of smoothing functions including Single Exponential Smoothing, Double Exponential Smoothing, and Triple Exponential Smoothing.

    Parameters
    ----------

    model_selection : bool, optional
        Specifies whether the algorithms will perform model selection or not.

            - True: the algorithm will select the best model among Single/Double/Triple/
              Damped Double/Damped Triple Exponential Smoothing models.
            - False: the algorithm will not perform the model selection.

        If ``forecast_model_name`` is set, the model defined by ``forecast_model_name`` will be used.

        Defaults to False.

    forecast_model_name : str, optional
        Name of the statistical model used for calculating the forecast.

        - 'SESM': Single Exponential Smoothing.
        - 'DESM': Double Exponential Smoothing.
        - 'TESM': Triple Exponential Smoothing.

        This parameter must be set unless ``model_selection`` is set to True.
    optimizer_time_budget : int, optional
        Time budget for Nelder-Mead optimization process.
        The time unit is second and the value should be larger than zero.

        Defaults to 1.

    max_iter : int, optional
        Maximum number of iterations for simulated annealing.

        Defaults to 100.
    optimizer_random_seed : int, optional
        Random seed for simulated annealing.
        The value should be larger than zero.

        Defaults to system time.
    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 1.0.
    alpha : float, optional
        Weight for smoothing. Value range: 0 < alpha < 1.

        Default value is computed automatically.

    beta : float, optional
        Weight for the trend component. Value range: 0 <= beta < 1.
        If it is not set, the optimized value will be computed automatically.
        Only valid when the model is set by user or identified by the algorithm as 'DESM' or 'TESM'.
        Value 0 is allowed under Triple Exponential Smoothing (TESM) model only.

        Defaults value is computed automatically.
    gamma : float, optional
        Weight for the seasonal component. Value range: 0 < gamma < 1.
        Only valid when the model is set by user or identified by the algorithm as as 'TESM'.

        Default value is computed automatically.
    phi : float, optional
        Value of the damped smoothing constant phi (0 < phi < 1).
        Only valid when the model is set by user or identified by the algorithm as a damped model.

        Default value is computed automatically.
    forecast_num : int, optional
        Number of values to be forecast.

        Defaults to 0.
    seasonal_period : int, optional
        Length of a seasonal_period (L > 1).

        For example, the ``seasonal_period`` of quarterly data is 4,
        and the ``seasonal_period`` of monthly data is 12.

        Only valid when the model is set by user or identified by the algorithm as 'TESM'.

        Default value is computed automatically.
    seasonal : {'multiplicative', 'additive'}, optional
        Specifies the type of model for triple exponential smoothing.

            - 'multiplicative': Multiplicative triple exponential smoothing.
            - 'additive': Additive triple exponential smoothing.

        When ``seasonal`` is set to 'additive', the default value of ``initial_method`` is 1;
        When ``seasonal`` is set to 'multiplicative', the default value of ``initial_method`` is 0.

        Defaults to 'multiplicative'.
    initial_method : int, optional
        Initialization method for the trend and seasonal components.

        Refer to :class:`~hana_ml.algorithms.pal.tsa.exponential_smoothing.TripleExponentialSmoothing` for detailed information on initialization method.

        Only valid when the model is set by user or identified by the algorithm as 'TESM'.

        Defaults to 0 or 1.
    training_ratio : float, optional
        The ratio of training data to the whole time series.
        Assuming the size of time series is N, and the training ratio is r,
        the first N*r time series is used to train, whereas only the latter N*(1-r) one
        is used to test.
        If this parameter is set to 0.0 or 1.0, or the resulting training data
        (N*r) is less than 1 or equal to the size of time series, no train-and-test procedure is
        carried out.

        Defaults to 1.0.

    damped : bool, optional
        For Double Exponential Smoothing:

          - False: Uses the Holt's linear method.
          - True: Uses the additive damped trend Holt's linear method.

        For Triple Exponential Smoothing :

          - False: Uses the Holt Winter method.
          - True: Uses the additive damped seasonal Holt Winter method.

        If ``model_selection`` is set to True, the default value will be computed automatically.
        Otherwise, the default value is False.

    accuracy_measure : str, {'mse', 'mape'}, optional
        The criterion used for the optimization.

        Defaults to 'mse'.
    seasonality_criterion : float, optional
        The criterion of the auto-correlation coefficient for accepting seasonality,
        in the range of (0, 1).

        The larger it is, the less probable a time series is
        regarded to be seasonal.

        Only valid when ``forecast_model_name`` is 'TESM' or ``model_selection``
        is set to True, and ``seasonal_period`` is not defined.

        Defaults to 0.5.
    trend_test_method : {'mk', 'difference-sign'}, optional

        - 'mk': Mann-Kendall test.
        - 'difference-sign': Difference-sign test.

        Defaults to 'mk'.
    trend_test_alpha : float, optional
        Tolerance probability for trend test. The value range is (0, 0.5).

        Only valid when ``model_selection`` is set to True.

        Defaults to 0.05.
    alpha_min : float, optional
        Sets the minimum value of alpha.
        Only valid when ``alpha`` is not defined.

        Defaults to 0.0000000001.
    beta_min : float, optional
        Sets the minimum value of beta.
        Only valid when ``beta`` is not defined.

        Defaults to 0.0000000001.
    gamma_min : float, optional
        Sets the minimum value of gamma.
        Only valid when ``gamma`` is not defined.

        Defaults to 0.0000000001.
    phi_min : float, optional
        Sets the minimum value of phi.
        Only valid when ``phi`` is not defined.

        Defaults to 0.0000000001.
    alpha_max : float, optional
        Sets the maximum value of alpha.
        Only valid when ``alpha`` is not defined.

        Defaults to 1.0.

    beta_max : float, optional
        Sets the maximum value of beta.
        Only valid when ``beta`` is not defined.

        Defaults to 1.0.
    gamma_max : float, optional
        Sets the maximum value of gamma.
        Only valid when ``gamma`` is not defined.

        Defaults to 1.0.
    phi_max : float, optional
        Sets the maximum value of phi.
        Only valid when ``phi`` is not defined.

        Defaults to 1.0.
    prediction_confidence_1 : float, optional
        Prediction confidence for interval 1.
        Only valid when the upper and lower columns are provided in the result table.

        Defaults to 0.8.
    prediction_confidence_2 : float, optional
        Prediction confidence for interval 2.
        Only valid when the upper and lower columns are provided in the result table.

        Defaults to is 0.95.
    level_start : float, optional
        The initial value for level component S.
        If this value is not provided, it will be calculated in the way as described in :class:`~hana_ml.algorithms.pal.tsa.exponential_smoothing.TripleExponentialSmoothing`.
        Notice that ``level_start`` cannot be zero.

        If it is set to zero, 0.0000000001 will be used instead.
    trend_start : float, optional
        The initial value for trend component B.

        If this value is not provided, it will be calculated in the way as described in :class:`~hana_ml.algorithms.pal.tsa.exponential_smoothing.TripleExponentialSmoothing`.
    season_start : list of tuple/float, optional
        A list of initial values for seasonal component C. If specified, the list
        must be of the length specified in ``seasonal_period``, i.e. start values
        must be provided for a whole seasonal period.

        We can simply give out the start values in a list, where the cycle index of each value is determined by
        its index in the list; or we can give out the start values together with their cycle indices in a list of tuples.

        For example, suppose the seasonal period is 4, with starting values :math:`x_i, 1 \leq i \leq 4` indexed by their cycle IDs.
        Then the four season start values can be specified in a list as :math:`[x_1, x_2, x_3, x_4]`,
        or equivalently in a list of tuples as :math:`[(1, x_1), (2, x_2), (3, x_3), (4, x_4)]`.

        If not provided, start values shall be computed by a default scheme.

    expost_flag : bool, optional

        - False: Does not output the expost forecast, and just outputs the forecast values.
        - True: Outputs the expost forecast and the forecast values.

        Defaults to True.

    Attributes
    ----------
    forecast_ : DataFrame
        Forecast values.

    stats_ : DataFrame
        Statistics.

    Examples
    --------
    >>> autoExp = time_series.AutoExponentialSmoothing(forecast_model_name='TESM',
                                                       alpha=0.4,
                                                       beta=0.4,
                                                       gamma=0.4,
                                                       seasonal_period=4,
                                                       forecast_num=3,
                                                       seasonal='multiplicative',
                                                       initial_method=1,
                                                       training_ratio=0.75)

    Perform fit_predict():

    >>> autoExp.fit_predict(data=df)

    Output:

    >>> autoExp.forecast_.collect()
    >>> autoExp.stats_.collect()

    """

    def __init__(self,
                 model_selection=None,# Auto ESM
                 forecast_model_name=None,# Auto ESM
                 optimizer_time_budget=None,# Auto ESM
                 max_iter=None,# Auto ESM
                 optimizer_random_seed=None,# Auto ESM
                 thread_ratio=None,# Auto ESM
                 alpha=None,
                 beta=None,
                 gamma=None,
                 phi=None,
                 forecast_num=None,
                 seasonal_period=None,
                 seasonal=None,
                 initial_method=None,
                 training_ratio=None,
                 damped=None,
                 accuracy_measure=None,
                 seasonality_criterion=None,# Auto ESM
                 trend_test_method=None,# Auto ESM
                 trend_test_alpha=None,# Auto ESM
                 alpha_min=None, # Auto ESM
                 beta_min=None,# Auto ESM
                 gamma_min=None,# Auto ESM
                 phi_min=None,# Auto ESM
                 alpha_max=None,# Auto ESM
                 beta_max=None,# Auto ESM
                 gamma_max=None,# Auto ESM
                 phi_max=None,# Auto ESM
                 prediction_confidence_1=None,
                 prediction_confidence_2=None,
                 level_start=None,
                 trend_start=None,
                 season_start=None,
                 expost_flag=None
                ):
        setattr(self, 'hanaml_parameters', pal_param_register())
        if accuracy_measure is not None:
            if isinstance(accuracy_measure, str):
                accuracy_measure = [accuracy_measure]
            if len(accuracy_measure) != 1:
                msg = "Please select accuracy_measure from 'mse' OR 'mape'!"
                logger.error(msg)
                raise ValueError(msg)
            self._arg('accuracy_measure', accuracy_measure[0].lower(), {'mse':'mse', 'mape':'mape'})
            accuracy_measure = accuracy_measure[0].lower()

        super(AutoExponentialSmoothing, self).__init__(
            model_selection=model_selection,
            forecast_model_name=forecast_model_name,
            optimizer_time_budget=optimizer_time_budget,
            max_iter=max_iter,
            optimizer_random_seed=optimizer_random_seed,
            thread_ratio=thread_ratio,
            alpha=alpha,
            beta=beta,
            gamma=gamma,
            phi=phi,
            forecast_num=forecast_num,
            seasonal_period=seasonal_period,
            seasonal=seasonal,
            initial_method=initial_method,
            training_ratio=training_ratio,
            damped=damped,
            accuracy_measure=accuracy_measure,
            seasonality_criterion=seasonality_criterion,
            trend_test_method=trend_test_method,
            trend_test_alpha=trend_test_alpha,
            alpha_min=alpha_min,
            beta_min=beta_min,
            gamma_min=gamma_min,
            phi_min=phi_min,
            alpha_max=alpha_max,
            beta_max=beta_max,
            gamma_max=gamma_max,
            phi_max=phi_max,
            prediction_confidence_1=prediction_confidence_1,
            prediction_confidence_2=prediction_confidence_2,
            level_start=level_start,
            trend_start=trend_start,
            season_start=season_start,
            expost_flag=expost_flag)

    def fit_predict(self, data, key=None, endog=None):
        """
        Fit and predict based on the given time series.

        Parameters
        ----------
        data : DataFrame
            Input data. At least two columns, one is ID column, the other is raw data.
        key : str, optional
            The ID column.

            Defaults to the first column of data if the index column of data is not provided.
            Otherwise, defaults to the index column of data.
        endog : str, optional
            The column of series to be fitted and predicted.

            Defaults to the first non-ID column.

        Returns
        -------
        DataFrame

            Forecast values.
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        if self.training_ratio is None:
            self.training_ratio = 1.0
        if not self._disable_hana_execution:
            rows = data.count() * self.training_ratio
            half_row = rows/2

            if self.seasonal_period is not None and self.seasonal_period > half_row:
                msg = ('seasonal_period should be smaller than' +
                       ' 1/2(row number * training_ratio) of data!')
                logger.error(msg)
                raise ValueError(msg)

        return super(AutoExponentialSmoothing, self)._fit_predict(exp_smooth_function=4, data=data,
                                                                  key=key, endog=endog)

class BrownExponentialSmoothing(_ExponentialSmoothingBase):
    r"""
    Brown exponential smoothing is suitable to model the time series with trend but without seasonality.
    Both non-adaptive and adaptive brown linear exponential smoothing are provided in PAL.

    Parameters
    ----------

    alpha : float, optional
        The smoothing constant alpha for brown exponential smoothing or
        the initialization value for adaptive brown exponential smoothing (0 < alpha < 1).

          - Defaults to 0.1 when Brown exponential smoothing
          - Defaults to 0.2 when Adaptive brown exponential smoothing

    delta : float, optional
        Value of weighted for At and Mt.

        Only valid when ``adaptive_method`` is True.

        Defaults to 0.2

    forecast_num : int, optional
        Number of values to be forecast.

        Defaults to 0.

    adaptive_method : bool, optional

        - False: Brown exponential smoothing.
        - True: Adaptive brown exponential smoothing.

        Defaults to False.

    accuracy_measure : str or a list of str, optional

        The metric to quantify how well a model fits input data.
        Options: "mpe", "mse", "rmse", "et", "mad", "mase", "wmape", "smape", "mape".

        No default value.

        .. Note::
            Specify a measure name if you want the corresponding measure value to be
            reflected in the output statistics self.stats\_.

    ignore_zero : bool, optional

        - False: Uses zero values in the input dataset when calculating "mpe" or "mape".
        - True: Ignores zero values in the input dataset when calculating "mpe" or "mape".

        Only valid when ``accuracy_measure`` is "mpe" or "mape".

        Defaults to False.

    expost_flag : bool, optional

        - False: Does not output the expost forecast, and just outputs the forecast values.
        - True: Outputs the expost forecast and the forecast values.

        Defaults to True.

    prediction_confidence_1 : float, optional
        Prediction confidence for interval 1.

        Only valid when the upper and lower columns are provided in the result table.

        Defaults to 0.8.

    prediction_confidence_2 : float, optional
        Prediction confidence for interval 2.

        Only valid when the upper and lower columns are provided in the result table.

        Defaults to 0.95.

    Attributes
    ----------
    forecast_ : DateFrame
        Forecast values.

    stats_ : DataFrame
        Statistics.

    Examples
    --------
    >>> brown_exp_smooth = BrownExponentialSmoothing(alpha=0.1,
                                                     delta=0.2,
                                                     forecast_num=6,
                                                     adaptive_method=False,
                                                     accuracy_measure='mse',
                                                     ignore_zero=0,
                                                     expost_flag=1)

    Perform fit_predict():

    >>> brown_exp_smooth.fit_predict(data=df)

    Output:

    >>> brown_exp_smooth.forecast_.collect()
    >>> brown_exp_smooth.stats_.collect()

    """
    op_name = 'BrownExpSm'
    def __init__(self,
                 alpha=None,
                 delta=None,
                 forecast_num=None,
                 adaptive_method=None,
                 accuracy_measure=None,
                 ignore_zero=None,
                 expost_flag=None,
                 prediction_confidence_1=None,
                 prediction_confidence_2=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        if delta is not None and adaptive_method is False:
            msg = 'delta is only valid when adaptive_method is True!'
            logger.error(msg)
            raise ValueError(msg)

        super(BrownExponentialSmoothing, self).__init__(alpha=alpha,
                                                        delta=delta,
                                                        forecast_num=forecast_num,
                                                        adaptive_method=adaptive_method,
                                                        accuracy_measure=accuracy_measure,
                                                        ignore_zero=ignore_zero,
                                                        expost_flag=expost_flag,
                                                        prediction_confidence_1=prediction_confidence_1,
                                                        prediction_confidence_2=prediction_confidence_2)

    def fit_predict(self, data, key=None, endog=None):
        """
        Fit and predict based on the given time series.

        Parameters
        ----------
        data : DataFrame
            Input data. At least two columns, one is ID column, the other is raw data.

        key : str, optional
            The ID column.

            Defaults to the first column of data if the index column of data is not provided.
            Otherwise, defaults to the index column of data.

        endog : str, optional
            The column of series to be fitted and predicted.

            Defaults to the first non-ID column.

        Returns
        -------
        DataFrame

            Forecast values.
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        return super(BrownExponentialSmoothing, self)._fit_predict(exp_smooth_function=5,
                                                                   data=data,
                                                                   key=key,
                                                                   endog=endog)

class Croston(_ExponentialSmoothingBase):
    r"""
    Croston method is a forecast strategy for products with intermittent demand.
    Croston method consists of two steps. First, separate exponential smoothing estimates are made of the average size of a demand.
    Second, the average interval between demands is calculated. This is then used in a form of the constant model to predict the future demand.

    Parameters
    ----------

    alpha : float, optional
        Value of the smoothing constant alpha (0 < alpha < 1).

        Defaults to 0.1.

    forecast_num : int, optional
        Number of values to be forecast.

        When it is set to 1, the algorithm only forecasts one value.

        Defaults to 0.

    method : str, optional

        - 'sporadic': Use the sporadic method.
        - 'constant': Use the constant method.

        Defaults to 'sporadic'.

    accuracy_measure : str or a list of str, optional

        The metric to quantify how well a model fits input data.
        Options: "mpe", "mse", "rmse", "et", "mad", "mase", "wmape", "smape", "mape".

        No default value.

        .. Note::
            Specify a measure name if you want the corresponding measure value to be
            reflected in the output statistics self.stats\_.

    ignore_zero : bool, optional

        - False: Uses zero values in the input dataset when calculating "mpe" or "mape".
        - True: Ignores zero values in the input dataset when calculating "mpe" or "mape".

        Only valid when ``accuracy_measure`` is "mpe" or "mape".

        Defaults to False.

    expost_flag : bool, optional

        - False: Does not output the expost forecast, and just outputs the forecast values.
        - True: Outputs the expost forecast and the forecast values.

        Defaults to True.

    Attributes
    ----------
    forecast_ : DateFrame
        Forecast values.

    stats_ : DataFrame
        Statistics.

    Examples
    --------
    >>> croston = Croston(alpha=0.1,
                          forecast_num=1,
                          method='sporadic',
                          accuracy_measure='mape')

    Perform fit_predict():

    >>> croston.fit_predict(data=df)

    Output:

    >>> croston.forecast_.collect()
    >>> croston.stats_.collect()

    """
    def __init__(self,
                 alpha=None,
                 forecast_num=None,
                 method=None,
                 accuracy_measure=None,
                 ignore_zero=None,
                 expost_flag=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        method = self._arg('method', method,
                           {'sporadic': 0, 'constant': 1})
        if alpha is None:
            alpha = 0.1
        super(Croston, self).__init__(alpha=alpha,
                                      forecast_num=forecast_num,
                                      accuracy_measure=accuracy_measure,
                                      ignore_zero=ignore_zero,
                                      expost_flag=expost_flag,
                                      method=method)

    def fit_predict(self, data, key=None, endog=None):
        """
        Fit and predict based on the given time series.

        Parameters
        ----------
        data : DataFrame
            Input data. At least two columns, one is ID column, the other is raw data.

        key : str, optional
            The ID column.

            Defaults to the first column of data if the index column of data is not provided.
            Otherwise, defaults to the index column of data.

        endog : str, optional
            The column of series to be fitted and predicted.

            Defaults to the first non-ID column.

        Returns
        -------
        DataFrame

            Forecast values.
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        return super(Croston, self)._fit_predict(exp_smooth_function=6, data=data, key=key,
                                                 endog=endog)

    def build_report(self):
        r"""
        Generate time series report.
        """
        from hana_ml.visualizers.time_series_report_template_helper import TimeSeriesTemplateReportHelper #pylint: disable=cylic-import
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
        tse.add_line_to_comparison_item("Forecast Data", data=self.forecast_result, x_name=self.forecast_result.columns[0], y_name=self.forecast_result.columns[1])
        page0.addItems(tse.get_comparison_item())
        pages.append(page0)
        self.report.add_pages(pages)
        self.report.build_report()

def _params_check(input_dict, param_map):
    update_params = {}
    if not input_dict or input_dict is None:
        return update_params

    for parm in input_dict:
        if parm in param_map.keys():
            if parm == 'accuracy_measure':
                if input_dict.get('accuracy_measure') is not None:
                    ac_values = input_dict.get('accuracy_measure')
                    if isinstance(ac_values, str):
                        ac_values = [ac_values]
                    accuracy_measure_list = {"mpe":"mpe", "mse":"mse", "rmse":"rmse", "et":"et",
                                "mad":"mad", "mase":"mase", "wmape":"wmape",
                                "smape":"smape", "mape":"mape"}
                    ac_lower = [acc.lower() for acc in ac_values]
                    for acc in ac_lower:
                        arg('accuracy_measure', acc, accuracy_measure_list)
                    update_params['accuracy_measure'] = (ac_lower, ListOfStrings)
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
            err_msg = "'{}' is not a valid parameter name for initializing a Croston TSB model!".format(parm)
            logger.error(err_msg)
            raise KeyError(err_msg)

    return update_params

class CrostonTSB(PALBase):
    r"""
    Croston TSB method (for Teunter, Syntetos & Babai) is a forecast strategy for products with intermittent demand. It is a modification of Croston's method.

    It replaces the demand interval in Croston's method with demand probability, which is updated every period. Compared to Croston's method, the forecast
    of the TSB method is unbiased, and its probability forecast can be used to estimate the risk of obsolescence.

    Parameters
    ----------

    alpha : float, optional
        Smoothing parameter for demand.

        Defaults to 0.1.

    beta : float, optional
        Smoothing parameter for probability.

        Defaults to 0.1.

    forecast_num : int, optional
        Number of values to be forecast.

        When it is set to 1, the algorithm only forecasts one value.

        Defaults to 0.

    method : str, optional

        - 'sporadic': Use the sporadic method.
        - 'constant': Use the constant method.

        Defaults to 'sporadic'.

    accuracy_measure : str or a list of str, optional

        The metric to quantify how well a model fits input data.
        Options: "mpe", "mse", "rmse", "et", "mad", "mase", "wmape", "smape", "mape".

        No default value.

        .. Note::
            Specify a measure name if you want the corresponding measure value to be
            reflected in the output statistics ``self.stats_``.

    expost_flag : bool, optional

        - False: Does not output the expost forecast, and just outputs the forecast values.
        - True: Outputs both the expost forecast and the forecast values.

        Defaults to True.

    ignore_zero : bool, optional

        - False: Uses zero values in the input dataset when calculating "mpe" or "mape".
        - True: Ignores zero values in the input dataset when calculating "mpe" or "mape".

        Only valid when ``accuracy_measure`` is "mpe" or "mape".

        Defaults to False.

    remove_leading_zeros : bool, optional

        - False: Uses leading zero values in the input dataset when smoothing the probability.
        - True: Ignores leading zero values in the input dataset when smoothing the probability.

        Defaults to False.

    massive : bool, optional
        Specifies whether or not to use the massive mode of Croston TSB.

        - True: Massive mode.
        - False: Single mode.

        For parameter settings in massive mode, you can use both
        ``group_params`` (see the example below) or the original parameters.
        Using original parameters will apply them to all groups. However, if you define some parameters for a group,
        the value of all original parameter settings will not be applicable to that group.

        An example is as follows:

        .. only:: latex

            >>> mcr = CrostonTSB(massive=True,
                                 expost_flag=False,
                                 group_params={'Group_1': {'accuracy_measure':'MAPE'}})
            >>> res = mcr.fit_predict(data=df,
                                      key='ID',
                                      endog='y',
                                      group_key='GROUP_ID')

        .. raw:: html

            <iframe allowtransparency="true" style="border:1px solid #ccc; background: #eeffcb;"
                 src="../../_static/croston_tsb_example.html" width="100%" height="60%" sandbox="">
            </iframe>

        In this example, as 'accuracy_measure' is set in group_params for Group_1,
        parameter setting of 'expost_flag' is not applicable to Group_1.

        Defaults to False.

    group_params : dict, optional
        If massive mode is activated (`massive` is True),
        input data for Croston TSB will be divided into different
        groups with different parameters applied.

        An example is as follows:

        .. only:: latex

            >>> mcr = CrostonTSB(massive=True,
                                 expost_flag=False,
                                 group_params={'Group_1': {'accuracy_measure':'MAPE'}})
            >>> res = mcr.fit_predict(data=df,
                                      key='ID',
                                      endog='y',
                                      group_key='GROUP_ID')

        .. raw:: html

            <iframe allowtransparency="true" style="border:1px solid #ccc; background: #eeffcb;"
                 src="../../_static/croston_tsb_example.html" width="100%" height="60%" sandbox="">
            </iframe>

        Valid only when ``massive`` is True and defaults to None.


    Attributes
    ----------

    forecast_ : DataFrame
        Forecast values.

    stats_ : DataFrame
        Statistics analysis.

    metrics_ : DataFrame
        Metrics values.

    error_msg_ : DataFrame
        Error messages.
        Only valid if `massive` is True when initializing a `CrostonTSB` instance.

    Examples
    --------
    Single mode example:

    >>> cr = CrostonTSB(alpha=0.3, beta=0.1, forecast_num=10,
                        method='constant', accuracy_measure=['mape'],
                        expost_flag=True, ignore_zero=False, remove_leading_zeros=False)

    Perform fit_predict():

    >>> forecast = cr.fit_predict(data=df, key='ID')

    Output:

    >>> forecast.collect()
    >>> cr.stats_.collect()
    >>> cr.metrics_.collect()

    Massive mode example:

    >>> cr_massive = CrostonTSB(massive=True, group_params={
                                'Group_1': {forecase_num=5, 'accuracy_measure': ['mape']},
                                'Group_2': {'alpha': 0.3, 'beta': 0.15, forecase_num=5, 'accuracy_measure': ['mse']}})

    Perform fit_predict():

    >>> forecast_massive = cr_massive.fit_predict(data=df_massive, group_key='GROUP_ID', key='ID', endog='y')

    Output:

    >>> forecast_massive.collect()
    >>> cr_massive.stats_.collect()
    >>> cr_massive.metrics_.collect()
    >>> cr_massive.error_msg_.collect()
    """
    __init_param_dict = {'alpha' : ('ALPHA', float),
                         'beta' : ('BETA', float),
                         'forecast_num'  : ('FORECAST_NUM', int),
                         'method'  : ('METHOD', int, {'sporadic': 0, 'constant': 1}),
                         'accuracy_measure' : ('MEASURE_NAME', ListOfStrings),
                         'ignore_zero' : ('IGNORE_ZERO', bool),
                         'expost_flag' : ('EXPOST_FLAG', bool),
                         'remove_leading_zeros' : ('REMOVE_LEADING_ZEROS', bool)}

    def __init__(self,
                 alpha=None,
                 beta=None,
                 forecast_num=None,
                 method=None,
                 accuracy_measure=None,
                 ignore_zero=None,
                 expost_flag=None,
                 remove_leading_zeros=None,
                 massive=False,
                 group_params=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(CrostonTSB, self).__init__()

        init_params = {'alpha' : alpha,
                       'beta' : beta,
                       'forecast_num' : forecast_num,
                       'method' : method,
                       'accuracy_measure' : accuracy_measure,
                       'ignore_zero' : ignore_zero,
                       'expost_flag' : expost_flag,
                       'remove_leading_zeros' : remove_leading_zeros}
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
                self._arg('Parameters with GROUP ID ' + str(group), group_params[group], dict)
            self.group_params = group_params

            for group in self.group_params:
                self.__pal_params[group] = _params_check(input_dict=self.group_params[group],
                                                         param_map=self.__init_param_dict)
            if self.init_params:
                special_group_name = 'PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID'
                self.__pal_params[special_group_name] = _params_check(input_dict=self.init_params,
                                                                      param_map=self.__init_param_dict)

        self.forecast_ = None
        self.stats_ = None
        self.statistics_ = self.stats_
        self.metrics_ = None
        self.err_msg_ = None
        self.is_index_int = None
        self.forecast_start = None
        self.timedelta = None

    def fit_predict(self, data, key=None, endog=None, group_key=None):
        """
        Fit and predict based on the given time series.

        Parameters
        ----------
        data : DataFrame
            Input data. At least two columns, one is ID column, the other is raw data.

        key : str, optional
            The ID column.

            In single mode, defaults to the first column of data if the index column of data is not provided.
            Otherwise, defaults to the index column of data.

            In massive mode, defaults to the first-non group key column of data if the index columns of data is not provided.
            Otherwise, defaults to the second of index columns of data and the first column of index columns is group_key.

        endog : str, optional
            The column of series to be fitted and predicted.

            In single mode, defaults to the first non-ID column.
            In massive mode, defaults to the first non group_key, non key column.

        group_key : str, optional
            The column of group_key. The data type can be INT or NVARCHAR/VARCHAR.
            This parameter is only valid when ``massive`` is True.

            Defaults to the first column of data if the index columns of data is not provided.
            Otherwise, defaults to the first column of index columns.


        Returns
        -------
        DataFrame

            Forecast values.

        """
        conn = data.connection_context
        require_pal_usable(conn)
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        setattr(self, "key", key)
        setattr(self, "endog", endog)
        setattr(self, "exog", None)
        param_rows = []

        if data is None:
            msg = 'The data cannot be None!'
            logger.error(msg)
            raise ValueError(msg)

        cols = data.columns
        index = data.index
        self.is_index_int = True
        if self.massive is True:
            group_key = self._arg('group_key', group_key, str)
            if index is not None:
                group_key = _col_index_check(group_key, 'group_key', index[0], cols)
            else:
                if group_key is None:
                    group_key = cols[0]

            _check_column('group_key', group_key, cols)
            data_groups = list(data[[group_key]].collect()[group_key].drop_duplicates()) if not \
            self._disable_hana_execution else []
            param_keys = list(self.group_params.keys())
            gid_type = {tp[0]:(tp[0], tp[1], tp[2]) for tp in data.dtypes()}[group_key]
            if not self._disable_hana_execution:
                if not all([(int(ky) if 'INT' in gid_type[1] else ky) in data_groups for ky in param_keys]):
                    msg = 'Invalid group key identified in group parameters!'
                    logger.error(msg)
                    raise ValueError(msg)
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

        # common check
        _check_column('key', key, cols)
        cols.remove(key)
        endog = self._arg('endog', endog, str)
        _check_column('endog', endog, cols)
        endog = cols[0] if endog is None else endog

        if self.massive is not True: # single mode
            data_ = data[[key] + [endog]]
            self.is_index_int = _is_index_int(data_, key)
            if not self.is_index_int:
                data_ = _convert_index_from_timestamp_to_int(data_, key)
            try:
                self.forecast_start, self.timedelta = _get_forecast_starttime_and_timedelta(data, key, self.is_index_int)
            except Exception as err:
                logger.warning(err)

            for name in self.__pal_params:
                value, typ = self.__pal_params[name]
                if name == 'accuracy_measure':
                    if isinstance(value, str):
                        value = [value]
                    for each_ac in value:
                        param_rows.extend([('MEASURE_NAME', None, None, each_ac)])
                else:
                    tpl = [_map_param(name, value, typ)]
                    param_rows.extend(tpl)

            unique_id = str(uuid.uuid1()).replace('-', '_').upper()
            outputs = ['FORECAST', 'STATS', 'METRICS']
            outputs = ['#PAL_CROSTON_TSB_{}_{}_{}'.format(tbl, self.id, unique_id)
                       for tbl in outputs]
            forecast_tbl, stats_tbl, metrics_tbl = outputs
            if not (check_pal_function_exist(conn, '%CROSTONTSB%', like=True) or \
            self._disable_hana_execution):
                msg = 'The version of SAP HANA does not support Croston TSB!'
                logger.error(msg)
                raise ValueError(msg)
            try:
                self._call_pal_auto(conn,
                                    'PAL_CROSTONTSB',
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

            self.forecast_ = conn.table(forecast_tbl)
            if not self.is_index_int:
                single_sql = """
                            SELECT
                            ADD_SECONDS('{2}', ({0}-{7}) *{3}) AS {5},
                            {1} AS {6}
                            FROM ({4})
                            """.format(quotename(self.forecast_.columns[0]),
                                       quotename(self.forecast_.columns[1]),
                                       self.forecast_start,
                                       self.timedelta,
                                       self.forecast_.select_statement,
                                       quotename(key),
                                       quotename(endog),
                                       data.count() + 1)
                self.forecast_ = conn.sql(single_sql)
            self.stats_ = conn.table(stats_tbl)
            self.statistics_ = self.stats_
            self.metrics_ = conn.table(metrics_tbl)
            setattr(self, "forecast_result", self.forecast_)
            return self.forecast_

        # massive mode
        data_ = data[[group_key, key, endog]]
        self.is_index_int = _is_index_int(data_, key)
        if not self.is_index_int:# key is timestamp
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

        for group in self.__pal_params:
            for name in self.__pal_params[group]:
                value, typ = self.__pal_params[group][name]
                if name == 'accuracy_measure':
                    if isinstance(value, str):
                        value = [value]
                    for each_ac in value:
                        param_rows.extend([(group, 'MEASURE_NAME', None, None, each_ac)])
                else:
                    tpl = [tuple([group] + list(_map_param(name, value, typ)))]
                    param_rows.extend(tpl)

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        outputs = ['FORECAST', 'STATS', 'METRICS', 'ERROR_MSG']
        outputs = ['#PAL_MASSIVE_CROSTON_TSB{}_{}_{}'.format(tbl, self.id, unique_id)
                   for tbl in outputs]
        forecast_tbl, stats_tbl, metrics_tbl, error_msg_tbl = outputs
        if not (check_pal_function_exist(conn, '%MASSIVE_CROSTONTSB%', like=True) or \
        self._disable_hana_execution):
            msg = 'The version of your SAP HANA does not support massive CrostonTSB!'
            logger.error(msg)
            raise ValueError(msg)
        try:
            self._call_pal_auto(conn,
                                'PAL_MASSIVE_CROSTONTSB',
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

        self.forecast_ = conn.table(forecast_tbl)
        if not self._disable_hana_execution:
            if not self.is_index_int:
                comb_data = None
                for group in data_groups:
                    group_val = f"'{group}'"
                    group_fct = self.forecast_.filter('GROUP_ID={}'.format(group_val)).sort(key+'(INT)')# GROUP_ID is predefined group_key column name in PAL output table
                    massive_sql = """
                                  SELECT {0},
                                  ADD_SECONDS('{3}', ({1}-{6}) * {4}) AS {7},
                                  {2} AS {8}
                                  FROM ({5})
                                  """.format(quotename(self.forecast_.columns[0]), #0 GROUP_ID
                                             quotename(self.forecast_.columns[1]), #1 key -> timestamp
                                             quotename(self.forecast_.columns[2]), #2 predicted value
                                             self.forecast_start[group], #3 forecast_start
                                             self.timedelta[group], #4 timedelta 86400
                                             group_fct.select_statement,#5 select statement
                                             group_count[group] + 1, #6 convert the int to the start timestamp, irrelavant to the expost_flag
                                             quotename(key),#7 key
                                             quotename(endog))#8 endog
                    group_fct = conn.sql(massive_sql)
                    if comb_data is None:
                        comb_data = group_fct
                    else:
                        comb_data = group_fct.union(comb_data)
                self.forecast_ = comb_data.sort(['GROUP_ID', key]) #GROUP_ID is predefined group_key column name in PAL output table

            self.stats_ = conn.table(stats_tbl)
            self.statistics_ = self.stats_
            self.metrics_ = conn.table(metrics_tbl)
            self.error_msg_ = conn.table(error_msg_tbl)

            if not self.error_msg_.collect().empty:
                row = self.error_msg_.count()
                for i in range(1, row+1):
                    warn_msg = "For group_key '{}',".format(self.error_msg_.collect()['GROUP_ID'][i-1]) +\
                               " the error message is '{}'.".format(self.error_msg_.collect()['MESSAGE'][i-1]) +\
                               "More information could be seen in the attribute error_msg_!"
                    logger.warning(warn_msg)
        setattr(self, "forecast_result", self.forecast_)
        return self.forecast_

    def build_report(self):
        r"""
        Generates time series report.
        """
        from hana_ml.visualizers.time_series_report_template_helper import TimeSeriesTemplateReportHelper #pylint: disable=cylic-import
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
        tse.add_line_to_comparison_item("Forecast Data", data=self.forecast_result, x_name=self.forecast_result.columns[0], y_name=self.forecast_result.columns[1])
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
