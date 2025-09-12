"""
This module contains Python wrapper for PAL time series outlier detection algorithm.

The following function is available:

    * :class:`OutlierDetectionTS`
"""
#pylint:disable=line-too-long, too-many-arguments, too-few-public-methods
#pylint: disable=invalid-name, unused-argument, too-many-locals, too-many-statements
#pylint: disable=attribute-defined-outside-init, unused-variable
#pylint: disable=too-many-branches, c-extension-no-member, too-many-nested-blocks
import logging
import uuid
from hdbcli import dbapi
from hana_ml.ml_base import quotename
from hana_ml.algorithms.pal.utility import check_pal_function_exist, _map_param
from ..pal_base import (
    arg,
    PALBase,
    ParameterTable,
    require_pal_usable,
    pal_param_register,
    try_drop
)
from .utility import _convert_index_from_timestamp_to_int, _is_index_int, _col_index_check
from .utility import _delete_none_key_in_dict
from ..sqlgen import trace_sql
logger = logging.getLogger(__name__)

def _params_check(input_dict, param_map):
    update_params = {}
    if not input_dict:
        return {}
    for parm in input_dict:
        #print('parm', parm)
        if parm in param_map.keys():
            if parm == 'voting_config':
                continue
            parm_val = input_dict[parm]
            arg_map = param_map[parm]
            if len(arg_map) == 2:
                update_params[arg_map[0]] = (arg(parm, parm_val, arg_map[1]), arg_map[1])
            else:
                update_params[arg_map[0]] = (arg(parm, parm_val, arg_map[2]), arg_map[1])
        else:
            err_msg = f"'{parm}' is not a valid parameter name for initializing a outlier detection model!"
            logger.error(err_msg)
            raise KeyError(err_msg)

        auto_param = 'false'
        if input_dict.get("outlier_method") == 'dbscan':
            auto_param = 'true' if any(param is None for param in (input_dict.get('minpts'), input_dict.get('eps'))) else 'false'
            update_params['AUTO_PARAM'] = (auto_param, str)

    # voting config check
    voting_config = input_dict.get("voting_config")
    if voting_config is not None:
        for name, param_dict in voting_config.items():
            out_method_num = arg('voting outlier method', name, {'z1':0, 'z2':1, 'iqr':2, 'mad':3, 'isolationforest':4, 'dbscan':5})
            update_params[f'VOTING_OUTLIER_METHOD_{out_method_num}'] = (out_method_num, int)
            min_val, eps_val = None, None
            for par_name, par_val in param_dict.items():
                if par_name == 'threshold':
                    thres_val = arg('threshold in voting outlier method {name}', par_val, float)
                    update_params[f"THRESHOLD_OUTLIER_METHOD_{out_method_num}"] = (thres_val, float)
                elif par_name == 'contamination':
                    con_val = arg('contamination in voting outlier method {name}', par_val, float)
                    update_params[f"CONTAMINATION_OUTLIER_METHOD_{out_method_num}"] = (con_val, float)
                elif par_name == 'minpts':
                    min_val = arg('minpts in voting outlier method', par_val, int)
                    update_params['MINPTS'] = (min_val, int)
                elif par_name == 'eps':
                    eps_val = arg('eps in voting outlier method', par_val, float)
                    update_params['RADIUS'] = (eps_val, float)
                else:
                    if par_name in param_map.keys():
                        voting_arg_map = param_map[par_name]
                        if len(voting_arg_map) == 2:
                            update_params[voting_arg_map[0]] = (arg(par_name, par_val, voting_arg_map[1]), voting_arg_map[1])
                        else:
                            update_params[voting_arg_map[0]] = (arg(par_name, par_val, voting_arg_map[2]), voting_arg_map[1])
                    else:
                        err_msg = f"'{par_name}' is not a valid parameter name in voting_config!"
                        logger.error(err_msg)
                        raise KeyError(err_msg)
            if out_method_num == 5: #DBSCAN
                auto_param = 'true' if any(param is None for param in (min_val, eps_val)) else 'false'
                update_params['AUTO_PARAM'] = (auto_param, str)

    return update_params

class OutlierDetectionTS(PALBase):#pylint:disable=too-many-instance-attributes
    r"""
    Outlier detection for time-series. In time series, an outlier is a data point that is different from the general behavior of remaining data points.
    In this algorithm, the outlier detection procedure is divided into two steps. In step 1, we get the residual from the original series. In step 2, we detect the outliers from the residual.

    Parameters
    ----------

    auto : bool, optional

        - True : automatic method to get residual.
        - False : manual method to get residual.

        Defaults to True.

    detect_intermittent_ts : bool, optional

        - True : detects whether the time series is intermittent.
        - False : does not detect whether the time series is intermittent.

        only valid when ``auto`` is True. If input data is intermittent time series, it will not do outlier detection

        Defaults to False.

    smooth_method : str, optional
        the method to get the residual.

        - 'no' : no smoothing method is used.
        - 'median' : median filter.
        - 'loess' : LOESS (locally estimated scatterplot smoothing) or LOWESS (locally weighted scatterplot smoothing) is a locally weighted linear regression method. This method is applicable to the time series which is non-seasonal. This method is also suitable for non-smooth time series.
        - 'super' : super smoother. This method combines a set of LOESS methods. Like LOESS, this method is applicable to non-seasonal time series. This method is also suitable for non-smooth time series.

        only valid when ``auto`` is False.

        Defaults to 'median'.

    window_size : int, optional
        Odd number, the window size for median filter, not less than 3.

        The value 1 means median filter is not applied. Only valid when ``auto`` is False and ``smooth_method`` is 'median'.

        Defaults to 3.

    loess_lag : int, optional
        Odd number, the lag for LOESS, not less than 3.

        Only valid when ``auto`` is False and ``smooth_method`` is 'loess'.

        Defaults to 7.

    current_value_flag : bool, optional
        Whether to take the current data point when using LOESS smoothing method.

        - True : takes the current data point.
        - False : does not take the current data point.

        For example, to estimate the value at time t with the window [t-3, t-2, t-1, t, t+1, t+2, t+3], taking the current data point means estimating the value at t with the real data points at [t-3, t-2, t-1, t, t+1, t+2, t+3], while not taking the current data point means estimating the value at t with the real data points at [t-3, t-2, t-1, t+1, t+2, t+3], without the real data point at t.

        Only valid when ``auto`` is False and ``smooth_method`` is 'median'.

        Defaults to False.

    outlier_method : str, optional

        The method for calculate the outlier score from residual.

        - 'z1' : Z1 score.
        - 'z2' : Z2 score.
        - 'iqr' : IQR score.
        - 'mad' : MAD score.
        - 'isolationforest' : isolation forest score.
        - 'dbscan' : DBSCAN.

        Defaults to 'z1'.

    threshold : float, optional
        The threshold for outlier score. If the absolute value of outlier score is beyond the
        threshold, we consider the corresponding data point as an outlier.

        Only valid when ``outlier_method`` = 'iqr', 'isolationforest', 'mad', 'z1', 'z2'. For ``outlier_method`` = 'isolationforest', when ``contamination`` is provided, ``threshold`` is not valid and outliers are decided by ``contamination``.

        Defaults to 3 when ``outlier_method`` is 'mad', 'z1' and 'z2'.
        Defaults to 1.5 when ``outlier_method`` is 'iqr'.
        Defaults to 0.7 when ``outlier_method`` is 'isolationforest'.

    detect_seasonality : bool, optional
        When calculating the residual,

        - False: Does not consider the seasonal decomposition.
        - True: Considers the seasonal decomposition.

        Only valid when ``auto`` is False and ``smooth_method`` is 'median'.

        Defaults to False.

    alpha : float, optional
        The criterion for the autocorrelation coefficient. The value range is (0, 1).

        A larger value indicates a stricter requirement for seasonality.

        Only valid when ``detect_seasonality`` is True.

        Defaults to 0.2 if ``auto`` is False and defaults to 0.4 if `auto`` is True.

    extrapolation : bool, optional
        Specifies whether to extrapolate the endpoints.
        Set to True when there is an end-point issue.

        Only valid when ``detect_seasonality`` is True.

        Defaults to False if ``auto`` is False and defaults to True if `auto`` is True.

    periods : int, optional
        When this parameter is not specified, the algorithm will search the seasonal period.
        When this parameter is specified between 2 and half of the series length, autocorrelation value
        is calculated for this number of periods and the result is compared to ``alpha`` parameter.
        If correlation value is equal to or higher than ``alpha``, decomposition is executed with the value of ``periods``.
        Otherwise, the residual is calculated without decomposition. For other value of parameter ``periods``, the residual is also calculated without decomposition.

        Only valid when ``detect_seasonality`` is True. If the user knows the seasonal period, specifying ``periods`` can speed up the calculation, especially when the time series is long.

        No Default value.

    random_state : int, optional
        Specifies the seed for random number generator.

        - 0: Uses the current time (in second) as seed.
        - Others: Uses the specified value as seed.

        Only valid when ``outlier_method`` is 'isolationforest'.

        Default to 0.

    n_estimators : int, optional
        Specifies the number of trees to grow.

        Only valid when ``outlier_method`` is 'isolationforest'.

        Default to 100.

    max_samples : int, optional
        Specifies the number of samples to draw from input to train each tree.
        If ``max_samples`` is larger than the number of samples provided,
        all samples will be used for all trees.

        Only valid when ``outlier_method`` is 'isolationforest'.

        Default to 256.

    bootstrap : bool, optional
        Specifies sampling method.

        - False: Sampling without replacement.
        - True: Sampling with replacement.

        Only valid when ``outlier_method`` is 'isolationforest'.

        Default to False.

    contamination : float, optional
        The proportion of outliers in the dataset. Should be in the range (0, 0.5].

        Only valid when ``outlier_method`` is 'isolationforest'. When ``outlier_method`` is 'isolationforest' and ``contamination`` is specified, ``threshold`` is not valid.

        No Default value.

    minpts : int, optional
        Specifies the minimum number of points required to form a cluster. The point itself is not included in ``minpts``.

        Only valid when ``outlier_method`` is 'dbscan'.

        Defaults to 1.

    eps : float, optional
        Specifies the scan radius.

        Only valid when ``outlier_method`` is 'dbscan'.

        Defaults to 0.5.

    distance_method : {'manhattan', 'euclidean', 'minkowski', 'chebyshev', 'standardized_euclidean', 'cosine'}, optional
        Specifies the method to compute the distance between two points.

        Only valid when ``outlier_method`` is 'dbscan' or when ``voting_config`` includes 'dbscan' as a voting outlier detection method.

        Defaults to 'euclidean'.

    dbscan_normalization : bool, optional
        Specifies whether to take normalization of data before applying it to DBSCAN method.

        - False: Does not take normalization.
        - True: Takes normalization.

        Only valid when ``outlier_method`` is 'dbscan' or when ``voting_config`` includes 'dbscan' as a voting outlier detection method.

        Defaults to False.

    dbscan_outlier_from_cluster : bool, optional
        Specifies how to take outliers from DBSCAN result.

        - False: Takes the largest cluster as normal points and others as outliers.
        - True: Takes the points with CLUSTER_ID = -1 as outliers.

        Only valid when ``outlier_method`` is 'dbscan' or when ``voting_config`` includes 'dbscan' as a voting outlier detection method.

        Defaults to False.

    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use. Only valid when ``detect_seasonality`` is True or ``outlier_method`` is 'isolationforest' or 'dbscan' or ``auto`` is True.

        Defaults to -1.

    residual_usage : {"outlier_detection", "outlier_correction"}, optional
        Specifies which residual to output.

        - 'outlier_detection': Residual for outlier detection.
        - 'outlier_correction': Residual for outlier correction.

        Defaults to 'outlier_detection'.

    voting_config : dict, optional
        Specifies the outlier detection method used in the voting and their conrresponding parameters and values.
        For each method, the options of parameters are as below:

        - 'z1': ``threshold``.
        - 'z2': ``threshold``.
        - 'mad':``threshold``.
        - 'iqr': ``threshold``.
        - 'isolationforest': ``random_state``, ``n_estimators``, ``max_samples``, ``bootstrap``, ``threshold``, ``contamination``.
        - 'dbscan': ``eps``, ``minpts``, ``distance_method``, ``dbscan_normalization``, ``dbscan_outlier_from_cluster``.

        An example is :

        >>> od = OutlierDetectionTS(
                        voting_config={"z1": {"threshold":10}, "z2": {"threshold":1},
                                       "mad":{"threshold":3}, "iqr": {"threshold":2},
                                       "isolationforest": {"contamination":0.2},
                                       "dbscan": {'minpts':1,
                                                  "eps":0.5,
                                                  "distance_method":"euclidean",
                                                  "dbscan_normalization":True,
                                                  "dbscan_outlier_from_cluster":False}},
                       residual_usage="outlier_correction")

        No default value.

    voting_outlier_method_criterion : float, optional
        The criterion for outlier voting. Suppose the number of voters is N. If more than int(criterion * N) voters detect the point as an outlier,
        the point will be treated as an outlier.

        Only valid when ``voting_config`` is not None.

        Defaults to 0.5.

    massive : bool, optional
        Specifies whether or not to use massive mode.

        - True : massive mode.
        - False : single mode.

        For parameter setting in massive mode, you could use both
        group_params (please see the example below) or the original parameters.
        Using original parameters will apply for all groups. However, if you define some parameters of a group,
        the value of all original parameter setting will be not applicable to such group.

        An example is as follows:

        >>> od = OutlierDetectionTS((massive=True)
        >>> od.fit_predict(data=df,
                           key='ID',
                           endog='Y',
                           group_key="GROUP_ID")

        Defaults to False.

    group_params : dict, optional
        If massive mode is activated (``massive`` is True), input data shall be divided into different
        groups with different parameters applied.

        An example is as follows:

        >>> od = OutlierDetectionTS(massive=True,
                                    group_params={'Group_1' : {'auto' : False}, 'Group_2' : {'auto' : True}})
        >>> od.fit_predict(data=df,
                           key='ID',
                           endog='Y',
                           group_key="GROUP_ID")

        Valid only when ``massive`` is True and defaults to None.

    References
    ----------
    Outlier detection methods implemented in this class are commonly consisted of two steps:

        #. :ref:`Residual Extraction<residual_extraction-label>`
        #. :ref:`Outlier Detection from Residual<odt_residual-label>`

    Please refer to the above links for detailed description of all methods as well as related parameters.

    Attributes
    ----------
    stats_ : DataFrame
        Statistics, structured as follows:

        - STAT_NAME : Name of statistics.
        - STAT_VALUE : Value of statistics.

    error_msg_ : DataFrame

        Error message.
        Only valid if ``massive`` is True when initializing an 'OutlierDetectionTS' instance.

    Examples
    --------
    >>> tsod = OutlierDetectionTS(detect_seasonality=False,
                                  outlier_method='z1',
                                  window_size=3,
                                  threshold=3.0)
    >>> res = tsod.fit_predict(data=df,
                               key='ID',
                               endog='Y')

    Outputs:

    >>> res.collect()
    >>> tsod.stats_.collect()

    """
    __init_param_dict = {'auto': ('AUTO', bool),
                         'detect_intermittent_ts': ('DETECT_INTERMITTENT_TS', bool),
                         'smooth_method': ('SMOOTH_METHOD', int, {'no': -1, 'median': 0, 'loess': 1, 'super': 2}),
                         'window_size': ('WINDOW_SIZE', int),
                         'loess_lag': ('LOESS_LAG', int),
                         'current_value_flag': ('CURRENT_VALUE_FLAG', bool),
                         'outlier_method': ('OUTLIER_METHOD', int, {'z1': 0, 'z2': 1, 'iqr': 2, 'mad': 3, 'isolationforest': 4, 'dbscan': 5}),
                         'threshold': ('THRESHOLD', float),
                         'detect_seasonality': ('DETECT_SEASONALITY', bool),
                         'alpha': ('ALPHA', float),
                         'extrapolation': ('EXTRAPOLATION', bool),
                         'periods': ('PERIODS', int),
                         'random_state': ('SEED', int),
                         'n_estimators': ('N_ESTIMATORS', int),
                         'max_samples': ('MAX_SAMPLES', int),
                         'bootstrap': ('BOOTSTRAP', bool),
                         'contamination': ('CONTAMINATION', float),
                         'minpts': ('MINPTS', int),
                         'eps': ('RADIUS', float),
                         'distance_method': ('DISTANCE_METHOD', int, {'manhattan': 1, 'euclidean': 2, 'minkowski': 3, 'chebyshev': 4, 'standardized_euclidean': 5, 'cosine': 6}),
                         'dbscan_normalization': ('DBSCAN_NORMALIZATION', bool),
                         'dbscan_outlier_from_cluster': ('DBSCAN_OUTLIER_FROM_CLUSTER', bool),
                         'residual_usage': ('RESIDUAL_USAGE', int, {"outlier_detection": 0, "outlier_correction": 1}),
                         'voting_config': ('XXXX', dict),
                         'voting_outlier_method_criterion': ('VOTING_OUTLIER_METHOD_CRITERION', float),
                         'thread_ratio': ('THREAD_RATIO', float)}

    def __init__(self,
                 auto=None,
                 detect_intermittent_ts=None,
                 smooth_method=None,
                 window_size=None,
                 loess_lag=None,
                 current_value_flag=None,
                 outlier_method=None,
                 threshold=None,
                 detect_seasonality=None,
                 alpha=None,
                 extrapolation=None,
                 periods=None,
                 random_state=None,
                 n_estimators=None,
                 max_samples=None,
                 bootstrap=None,
                 contamination=None,
                 minpts=None,
                 eps=None,
                 distance_method=None,
                 dbscan_normalization=None,
                 dbscan_outlier_from_cluster=None,
                 residual_usage=None,
                 voting_config=None,
                 voting_outlier_method_criterion=None,
                 thread_ratio=None,
                 massive=False,
                 group_params=None
                 ):
        if not hasattr(self, 'hanaml_parameters'):
            setattr(self, 'hanaml_parameters', pal_param_register())
        super().__init__()

        init_params = {'auto' : auto,
                       'detect_intermittent_ts' : detect_intermittent_ts,
                       'smooth_method' : smooth_method,
                       'window_size' : window_size,
                       'loess_lag' : loess_lag,
                       'current_value_flag' : current_value_flag,
                       'outlier_method' : outlier_method,
                       'threshold' : threshold,
                       'detect_seasonality' : detect_seasonality,
                       'alpha' : alpha,
                       'extrapolation' : extrapolation,
                       'periods' : periods,
                       'random_state' : random_state,
                       'n_estimators' : n_estimators,
                       'max_samples' : max_samples,
                       'bootstrap' : bootstrap,
                       'contamination' : contamination,
                       'minpts' : minpts,
                       'eps' : eps,
                       'distance_method' : distance_method,
                       'dbscan_normalization' : dbscan_normalization,
                       'dbscan_outlier_from_cluster' : dbscan_outlier_from_cluster,
                       'residual_usage' : residual_usage,
                       'voting_config' : voting_config,
                       'voting_outlier_method_criterion' : voting_outlier_method_criterion,
                       'thread_ratio' : thread_ratio}

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

        self.op_name = 'Outlier'
        self.stats_ = None
        self.statistics_ = self.stats_
        self.metrics_ = None
        self.is_index_int = None
        self.error_msg_ = None

    @trace_sql
    def fit_predict(self, data, key=None, endog=None,
                    group_key=None, group_params=None):
        r"""
        Detection of outliers in time-series data.

        Parameters
        ----------

        data : DataFrame

            Input data containing the target time-series.

            ``data`` should have at least two columns: one is the ID column,
            the other is the raw data.

        key : str, optional
            Specifies the ID column, which indicates the order of the time-series.

            It is recommended to always specify this column manually.

            Defaults to the first column of data if the index column of data is not provided.
            Otherwise, defaults to the index column of data.

        endog : str, optional
            Specifies the column that contains the values of the time-series to be tested.

            Defaults to the first non-key column.

        group_key : str, optional
            The column of group_key. Data type can be INT or NVARCHAR/VARCHAR.
            If the data type is INT, only parameters set in the group_params are valid.

            This parameter is only valid when ``massive`` is True in the class instance initialization.

            Defaults to the first column of data if the index columns of data are not provided.
            Otherwise, defaults to the first column of index columns.

        Returns
        -------

        DataFrame
            Outlier detection result.

        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        setattr(self, "key", key)
        setattr(self, "endog", endog)
        conn = data.connection_context
        require_pal_usable(conn)
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

            if group_key is not None and group_key not in cols:
                msg = f"Please select group_key from {cols}!"
                logger.error(msg)
                raise ValueError(msg)
            data_groups = list(data[[group_key]].collect()[group_key].drop_duplicates())
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
            cols.remove(key)

            endog = self._arg('endog', endog, str)
            if endog is None:
                endog = cols[0]

            data_ = data[[group_key, key, endog]]
            self.is_index_int = _is_index_int(data_, key)
            if not self.is_index_int:# timestamp
                recomb_data = None
                self.forecast_start = {}
                self.timedelta = {}
                group_count = {}
                for group in data_groups:
                    group_val = f"'{group}'"
                    group_data = data_.filter(f"{quotename(data_.dtypes()[0][0])}={group_val}").sort(data_.dtypes()[0][0])
                    group_count[group] = group_data.count()
                    group_data = _convert_index_from_timestamp_to_int(group_data, key)
                    if recomb_data is None:
                        recomb_data = group_data
                    else:
                        recomb_data = recomb_data.union(group_data)
                data_ = recomb_data[[group_key, key + '(INT)', endog]]

            for group in self.__pal_params:
                for name, value_type in self.__pal_params[group].items():
                    if "VOTING_OUTLIER_METHOD" in name and name != 'VOTING_OUTLIER_METHOD_CRITERION':
                        tpl = [(group, "VOTING_OUTLIER_METHOD", value_type[0], None, None)]
                    else:
                        value, typ = self.__pal_params[group][name]
                        tpl = [tuple([group] + list(_map_param(name, value, typ)))]
                    param_rows.extend(tpl)

            unique_id = str(uuid.uuid1()).replace('-', '_').upper()
            outputs = ['RESTULT', 'STATS', 'METRICS', 'ERROR_MSG']
            outputs = [f'#PAL_MASSIVE_TS_OUTLIER_{name}_TBL_{self.id}_{unique_id}' for name in outputs]
            res_tbl, stats_tbl, metrics_tbl, msg_tbl = outputs
            if not (check_pal_function_exist(conn, '%MASSIVE_OUTLIERDETECTIONFORTIMESERIES%', like=True) or \
            self._disable_hana_execution):
                msg = 'The version of your SAP HANA does not support massive outlier detection for time series!'
                logger.error(msg)
                raise ValueError(msg)
            try:
                self._call_pal_auto(conn,
                                    "PAL_MASSIVE_OUTLIER_DETECTION_FOR_TIME_SERIES",
                                    data_,
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
            self.stats_ = conn.table(stats_tbl)
            self.statistics_ = self.stats_
            self.metrics_ = conn.table(metrics_tbl)
            self.error_msg_ = conn.table(msg_tbl)
            res_df = conn.table(res_tbl)

            if not self._disable_hana_execution:
                # ID is timestamp
                if not self.is_index_int:
                    res_cols = res_df.columns
                    res_df = res_df.rename_columns({'TIMESTAMP':'ID_RESULT'})
                    res_groups = list(res_df[["GROUP_ID"]].collect()["GROUP_ID"].drop_duplicates())
                    selected_data = None
                    data_new = data.select(group_key, key)
                    for group_name in res_groups:
                        group_data = data_new.filter(f"{quotename(data_new.dtypes()[0][0])}={group_name}").sort(data_new.dtypes()[0][0]).add_id("ID_INT", ref_col=key)
                        individual = group_data.alias('L').join(res_df.alias('R'), condition=f'L.ID_INT = R.ID_RESULT and L.{quotename(group_key)}=R.GROUP_ID')
                        if selected_data is None:
                            selected_data = individual
                        else:
                            selected_data = selected_data.union(individual)
                    res_df = selected_data.select(group_key, key, res_cols[2], res_cols[3], res_cols[4], res_cols[5])
            return res_df

        # single mode
        key = self._arg('key', key, str)
        if index is not None:
            key = _col_index_check(key, 'key', index, cols)
        else:
            if key is None:
                key = cols[0]

        cols.remove(key)

        endog = self._arg('endog', endog, str)
        if endog is None:
            endog = cols[0]

        used_cols = [key] + [endog]
        if any(col not in data.columns for col in used_cols):
            msg = "'key' or 'endog' parameter contains unrecognized column name!"
            raise ValueError(msg)
        data_ = data[used_cols]

        self.is_index_int = _is_index_int(data_, key)
        if not self.is_index_int:
            data_ = _convert_index_from_timestamp_to_int(data_, key)

        for name, value_type in self.__pal_params.items():
            if "VOTING_OUTLIER_METHOD" in name and name != 'VOTING_OUTLIER_METHOD_CRITERION':
                tpl = [("VOTING_OUTLIER_METHOD", value_type[0], None, None)]
            else:
                value, typ = value_type[0], value_type[1]
                tpl = [_map_param(name, value, typ)]
            param_rows.extend(tpl)

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        outputs = ['RESTULT', 'STATS', 'METRICS']
        outputs = [f'#PAL_TS_OUTLIER_{name}_TBL_{self.id}_{unique_id}' for name in outputs]
        res_tbl, stats_tbl, metrics_tbl = outputs
        if not (check_pal_function_exist(conn, '%OUTLIERDETECTIONFORTIMESERIES%', like=True) or \
        self._disable_hana_execution):
            msg = 'The version of your SAP HANA does not support the outlier detection for time series!'
            logger.error(msg)
            raise ValueError(msg)
        try:
            self._call_pal_auto(conn,
                                'PAL_OUTLIER_DETECTION_FOR_TIME_SERIES',
                                data_,
                                ParameterTable().with_data(param_rows),
                                *outputs)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, outputs)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, outputs)
        self.stats_ = conn.table(stats_tbl)
        self.statistics_ = self.stats_
        self.metrics_ = conn.table(metrics_tbl)
        res_df = conn.table(res_tbl)

        if not self._disable_hana_execution:
            # ID is timestamp
            if not self.is_index_int:
                res_cols = res_df.columns
                res_int = res_df.rename_columns({res_cols[0]:'ID_RESULT'})
                data_int = data.add_id('ID_DATA', ref_col=key)
                res_df = res_int.join(data_int, 'ID_RESULT=ID_DATA').select(key, res_cols[1], res_cols[2], res_cols[3], res_cols[4])
        return res_df
