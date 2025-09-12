"""
This module contains Python wrapper for PAL change-point detection algorithm.

The following class is available:

    * :class:`CPD`
    * :class:`BCPD`
    * :class:`OnlineBCPD`
"""
#pylint:disable=too-many-lines, line-too-long, too-many-arguments, too-few-public-methods, too-many-instance-attributes
#pylint:disable=too-many-locals, no-else-return, attribute-defined-outside-init, too-many-branches, too-many-statements, use-a-generator
#pylint: disable=c-extension-no-member, super-with-arguments, invalid-name, consider-using-dict-items
import logging
import uuid
from hdbcli import dbapi
from hana_ml.ml_base import quotename
from ..utility import check_pal_function_exist, _map_param
from .utility import _delete_none_key_in_dict
from .utility import _convert_index_from_timestamp_to_int, _is_index_int, _col_index_check
from ..pal_base import (
    PALBase,
    arg,
    ParameterTable,
    ListOfStrings,
    try_drop,
    require_pal_usable,
    pal_param_register
)
logger = logging.getLogger(__name__)#pylint:disable=invalid-name

class CPD(PALBase):
    r"""
    Change-point detection (CPDetection) methods aim at detecting multiple abrupt changes such as change in mean,
    variance or distribution in an observed time-series data.

    Parameters
    ----------
    cost : {'normal_mse', 'normal_rbf', 'normal_mhlb', 'normal_mv', 'linear', 'gamma', 'poisson', 'exponential', 'normal_m', 'negbinomial'}, optional
        The cost function for change-point detection.

        Defaults to 'normal_mse'.
    penalty : {'aic', 'bic', 'mbic', 'oracle', 'custom'}, optional
        The penalty function for change-point detection.

        Defaults to
            (1)'aic' if ``solver`` is 'pruneddp', 'pelt' or 'opt',
            (2)'custom' if ``solver`` is 'adppelt'.
    solver : {'pelt', 'opt', 'adppelt', 'pruneddp'}, optional
        Method for finding change-points of given data, cost and penalty.
        Each solver supports different cost and penalty functions.

        - 1.  For cost functions, 'pelt', 'opt' and 'adpelt' support the following eight:
              'normal_mse', 'normal_rbf', 'normal_mhlb', 'normal_mv', 'linear', 'gamma', 'poisson', 'exponential';
              while 'pruneddp' supports the following four cost functions: 'poisson', 'exponential', 'normal_m', 'negbinomial'.
        - 2.  For penalty functions, 'pruneddp' supports all penalties, 'pelt', 'opt' and 'adppelt' support the following three:
              'aic','bic','custom', while 'adppelt' only supports 'custom' cost.

        Defaults to 'pelt'.
    lamb : float, optional
        Assigned weight of the penalty w.r.t. the cost function, i.e. penalization factor.
        It can be seen as trade-off between speed and accuracy of running the detection algorithm.
        A small values (usually less than 0.1) will dramatically improve the efficiency.

        Defaults to 0.02, and valid only when ``solver`` is 'pelt' or 'adppelt'.
    min_size : int, optional
        The minimal length from the very beginning within which change would not happen.
        Valid only when ``solver`` is 'opt', 'pelt' or 'adppelt'.

        Defaults to 2.
    min_sep : int, optional
        The minimal length of separation between consecutive change-points.
        Defaults to 1, valid only when ``solver`` is 'opt', 'pelt' or 'adppelt'.

    max_k : int, optional
        The maximum number of change-points to be detected.
        If the given value is less than 1, this number would be determined automatically from the input data.

        Defaults to 0, valid only when ``solver`` is 'pruneddp'.
    dispersion : float, optinal
        Dispersion coefficient for Gamma and negative binomial distribution.
        Valid only when `cost` is 'gamma' or 'negbinomial'.

        Defaults to 1.0.
    lamb_range : list of two numerical(float and int) values, optional(deprecated)
        User-defined range of penalty.
        Only valid when ``solver`` is 'adppelt'.

        Deprecated, please use ``range_penalty`` instead.
    max_iter : int, optional
        Maximum number of iterations for searching the best penalty.
        Valid only when ``solver`` is 'adppelt'.

        Defaults to 40.
    range_penalty : list of two numerical values, optional
        User-defined range of penalty.
        Valid only when ``solver`` is 'adppelt' and ``value_penalty`` is not provided.

        Defaults to [0.01, 100].
    value_penalty : float, optional
        Value of user-defined penalty.
        Valid when ``penalty`` is 'custom' or ``solver`` is 'adppelt'.

        No default value.

    Attributes
    ----------
    stats_ : DataFrame
        Statistics.

    Examples
    --------
    >>> cpd = CPD(solver='pelt',
    ...           cost='normal_mse',
    ...           penalty='aic',
    ...           lamb=0.02)

    Perform fit_predict() and check the results:

    >>> cp = cpd.fit_predict(data=df)
    >>> cp.collect()
    >>> cpd.stats_.collect()
    """

    solver_map = {'pelt':'Pelt', 'opt':'Opt', 'adppelt':'AdpPelt', 'pruneddp':'PrunedDP'}
    penalty_map = {'aic':'AIC', 'bic':'BIC', 'mbic':'mBIC', 'oracle':'Oracle', 'custom':'Custom'}
    cost_map = {'normal_mse':'Normal_MSE', 'normal_rbf':'Normal_RBF',
                'normal_mhlb':'Normal_MHLB', 'normal_mv':'Normal_MV',
                'linear':'Linear', 'gamma':'Gamma', 'poisson':'Poisson',
                'exponential':'Exponential', 'normal_m':'Normal_M',
                'negbinomial':'NegBinomial'}
    def __init__(self,#pylint: disable=too-many-positional-arguments, too-many-locals, too-many-branches
                 cost=None,
                 penalty=None,
                 solver=None,
                 lamb=None,
                 min_size=None,
                 min_sep=None,
                 max_k=None,
                 dispersion=None,
                 lamb_range=None,
                 max_iter=None,
                 range_penalty=None,
                 value_penalty=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(CPD, self).__init__()
        self.cost = self._arg('cost', cost, self.cost_map)
        self.penalty = self._arg('penalty', penalty, self.penalty_map)
        self.solver = self._arg('solver', solver, self.solver_map)
        if self.solver in ('Pelt', 'Opt', None) and self.penalty not in ('AIC', 'BIC', 'Custom', None):
            msg = ("When 'solver' is 'pelt' or 'opt', "+
                   "only 'aic', 'bic' and 'custom' are valid penalty functions.")
            raise ValueError(msg)
        if self.solver == 'AdpPelt' and self.penalty not in ('Custom', None):
            msg = "When 'solver' is 'adppelt', penalty function must be 'custom'."
            raise ValueError(msg)
        cost_list_one = ['Normal_MSE', 'Normal_RBF', 'Normal_MHLB', 'Normal_MV',
                         'Linear', 'Gamma', 'Poisson', 'Exponential']
        cost_list_two = ['Poisson', 'Exponential', 'Normal_M', 'NegBinomial']
        if self.solver in ('Pelt', 'Opt', 'AdpPelt', None):
            if  self.cost is not None and self.cost not in cost_list_one:
                msg = ("'solver' is currently one of the following: pelt, opt and adppelt, "+
                       "in this case cost function must be one of the following: normal_mse, normal_rbf, "+
                       "normal_mhlb, normal_mv, linear, gamma, poisson, exponential.")
                raise ValueError(msg)
        elif self.cost is not None and self.cost not in cost_list_two:
            msg = "'solver' is currently PrunedDP, in this case 'cost' must be assigned a valid value listed as follows: poisson, exponential, normal_m, negbinomial"
            raise ValueError(msg)
        self.lamb = self._arg('lamb', lamb, float)
        self.min_size = self._arg('min_size', min_size, int)
        self.min_sep = self._arg('min_sep', min_sep, int)
        self.max_k = self._arg('max_k', max_k, int)
        self.dispersion = self._arg('dispersion', dispersion, float)
        if lamb_range is not None:
            if isinstance(lamb_range, list) and len(lamb_range) == 2 and all(isinstance(val, (int, float)) for val in lamb_range):#pylint:disable=line-too-long
                self.lamb_range = lamb_range
            else:
                msg = ("Wrong setting for parameter 'lamb_range', correct setting "+
                       "should be a list of two numerical values that corresponds to "+
                       "lower- and upper-limit of the penelty weight.")
                raise ValueError(msg)
        else:
            self.lamb_range = None
        self.max_iter = self._arg('max_iter', max_iter, int)
        if range_penalty is not None:
            if isinstance(range_penalty, (list, tuple)) and len(range_penalty) == 2 and all(isinstance(val, (int, float)) for val in range_penalty):#pylint:disable=line-too-long
                self.lamb_range = list(range_penalty)
            else:
                msg = ("Wrong setting for parameter 'range_penalty', correct setting "+
                       "should be a list of two numerical values that corresponds to "+
                       "lower- and upper-limit of the penelty value.")
                raise ValueError(msg)
        else:
            self.lamb_range = None
        self.value_penalty = self._arg('value_penalty', value_penalty, float)

    def fit_predict(self, data, key=None, features=None):
        """
        Detecting change-points of the input data.

        Parameters
        ----------

        data : DataFrame

            Input time-series data for change-point detection.

        key : str, optional

            Column name for time-stamp of the input time-series data.

            If the index column of data is not provided or not a single column, and the key of fit_predict function is not provided,
            the default value is the first column of data.

            If the index of data is set as a single column, the default value of key is index column of data.

        features : str or a list of str, optional

            Column name(s) for the value(s) of the input time-series data.

        Returns
        -------

        DataFrame

            Detected the change-points of the input time-series data.
        """
        conn = data.connection_context
        require_pal_usable(conn)
        setattr(self, 'hanaml_fit_params', pal_param_register())
        cols = data.columns

        index = data.index
        key = self._arg('key', key, str)
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

        if features is not None:
            if isinstance(features, str):
                features = [features]
            features = self._arg('features', features, ListOfStrings)
        else:
            cols.remove(key)
            features = cols
        used_cols = [key] + features
        if any(col not in data.columns for col in used_cols):
            msg = "'key' or 'features' parameter contains unrecognized column name."
            raise ValueError(msg)
        data_ = data[used_cols]
        param_rows = [
            ('COSTFUNCTION', None, None, self.cost),
            ('SOLVER', None, None, self.solver),
            ('PENALIZATION_FACTOR', None, self.lamb, None),
            ('MIN_SIZE', self.min_size, None, None),
            ('MIN_SEP', self.min_sep, None, None),
            ('MaxK', self.max_k, None, None),
            ('DISPERSION', None, self.dispersion, None),
            ('MAX_ITERATION', self.max_iter, None, None)]
        if (self.penalty == 'Custom' or self.solver == 'AdpPelt') and self.value_penalty is not None:
            param_rows.extend([('PENALTY', None, self.value_penalty, 'Custom')])
        elif self.penalty not in ['Custom', None]:
            param_rows.extend([('PENALTY', None, None, self.penalty)])
        if self.lamb_range is not None:
            param_rows.extend([('RANGE_PENALTIES', None, None, str(self.lamb_range))])
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        tables = ['RESULT', 'STATS']
        tables = ["#PAL_CPDETECTION_{}_TBL_{}_{}".format(tbl, self.id, unique_id) for tbl in tables]
        result_tbl, stats_tbl = tables
        try:
            self._call_pal_auto(conn,
                                "PAL_CPDETECTION",
                                data_,
                                ParameterTable().with_data(param_rows),
                                *tables)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, tables)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, tables)
            raise
        self.stats_ = conn.table(stats_tbl)
        self.statistics_ = self.stats_
        return conn.table(result_tbl)

class BCPD(PALBase):
    r"""
    Bayesian  Change-point detection (BCPD) detects abrupt changes in the time series. It, to some extent, can been assumed as an enhanced version of seasonality test in additive mode.
    Similarly, it decomposes a time series into three components: trend, season and random, but with a remarkable difference that it is capable of detecting change points within both trend and season parts, using a quasi RJ-MCMC method.

    Parameters
    ----------
    max_tcp : int
        Maximum number of trend change points to be detected.
    max_scp : int
        Maximum number of season change points to be detected.
    trend_order : int, optional
        Order of trend segments that used for decomposition

        Defaults to 1.
    max_harmonic_order : int, optional
        Maximum order of harmonic waves within seasonal segments.

        Defaults to 10.
    min_period : int, optional
        Minimum possible period within seasonal segments.

        Defaults to 1.
    max_period : int, optional
        Maximum possible period within seasonal segments.

        Defaults to half of the data length.
    random_seed : int, optional
        Indicates the seed used to initialize the random number generator:

        - 0: Uses the system time.
        - Not 0: Uses the provided value.

        Defaults to 0.
    max_iter : int, optional
        BCPD is iterative, the more iterations, the more precise will the result be rendered.

        Defaults to 5000.
    interval_ratio : float, optional
        Regulates the interval between change points, which should be larger than the corresponding portion of total length.

        Defaults to 0.1.

    Examples
    --------
    >>> bcpd = BCPD(max_tcp=5, max_scp=5)
    >>> tcp, scp, period, components = bcpd.fit_predict(data=df)
    >>> tcp.collect()
    >>> scp.collect()
    >>> period.collect()
    >>> components.collect()

    """

    def __init__(self,#pylint: disable=too-many-positional-arguments, too-many-locals, too-many-branches
                 max_tcp,
                 max_scp,
                 trend_order=None,
                 max_harmonic_order=None,
                 min_period=None,
                 max_period=None,
                 random_seed=None,
                 max_iter=None,
                 interval_ratio=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        if max_scp > 0 and max_harmonic_order is None:
            warn_msg = "Please enter a positive value of max_harmonic_order when max_scp is larger than 0!"
            logger.warning(warn_msg)
        super(BCPD, self).__init__()
        self.trend_order = self._arg('trend_order', trend_order, int)
        self.max_tcp = self._arg('max_tcp', max_tcp, int)
        self.max_scp = self._arg('max_scp', max_scp, int)
        self.max_harmonic_order = self._arg('max_harmonic_order', max_harmonic_order, int)
        self.min_period = self._arg('min_period', min_period, int)
        self.max_period = self._arg('max_period', max_period, int)
        self.random_seed = self._arg('random_seed', random_seed, int)
        self.max_iter = self._arg('max_iter', max_iter, int)
        self.interval_ratio = self._arg('interval_ratio', interval_ratio, float)

        self.is_index_int = None

    def fit_predict(self, data, key=None, endog=None, features=None):
        """
        Detects change-points of the input data.

        Parameters
        ----------

        data : DataFrame

            Input time-series data for change-point detection.

        key : str, optional

            Column name for time-stamp of the input time-series data.

            If the index column of data is not provided or not a single column, and the key of fit_predict function is not provided,
            the default value is the first column of data.

            If the index of data is set as a single column, the default value of key is index column of data.

        endog : str, optional

            Column name for the value of the input time-series data.
            Defaults to the first non-key column.

        features : str or a list of str, optional (*deprecated*)
            Column name(s) for the value(s) of the input time-series data.

        Returns
        -------

        DataFrame 1

            The detected the trend change-points of the input time-series data.

        DataFrame 2

            The detected the season change-points of the input time-series data.

        DataFrame 3

            The detected the period within each season segment of the input time-series data.

        DataFrame 4

            The decomposed components.

        """
        conn = data.connection_context
        require_pal_usable(conn)
        setattr(self, 'hanaml_fit_params', pal_param_register())
        cols = data.columns
        index = data.index
        key = self._arg('key', key, str)
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

        if endog is not None:
            features = endog
        if features is not None:
            if not isinstance(features, str):
                msg = "BCPD currently only supports one column of endog!"
                raise ValueError(msg)
        else:
            cols.remove(key)
            features = cols[0]
        used_cols = [key] + [features]
        for idx, col in enumerate(used_cols):
            param_name = 'key' if idx == 0 else 'endog'
            if col not in data.columns:
                msg = f"Column `{col}` specified by {param_name} cannot be found in input data."
                raise ValueError(msg)
        data_ = data[used_cols]

        self.is_index_int = _is_index_int(data_, key)
        if not self.is_index_int:
            data_ = _convert_index_from_timestamp_to_int(data_, key)

        param_rows = [
            ('TREND_ORDER', self.trend_order, None, None),
            ('MAX_TCP_NUM', self.max_tcp, None, None),
            ('MAX_SCP_NUM', self.max_scp, None, None),
            ('MAX_HARMONIC_ORDER', self.max_harmonic_order, None, None),
            ('MIN_PERIOD', self.min_period, None, None),
            ('MAX_PERIOD', self.max_period, None, None),
            ('RANDOM_SEED', self.random_seed, None, None),
            ('MAX_ITER', self.max_iter, None, None),
            ('INTERVAL_RATIO', None, self.interval_ratio, None)]
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        outputs = ['TREND_CHANGE_POINT', 'SEASON_CHANGE_POINT', 'PERIOD_LIST', 'DECOMPOSED']
        outputs = ["#PAL_BCPD_{}_TBL_{}_{}".format(tbl, self.id, unique_id) for tbl in outputs]
        tcp_tbl, scp_tbl, period_tbl, decompose_tbl = outputs
        try:
            self._call_pal_auto(conn,
                                "PAL_BCPD",
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
        decompose_df = conn.table(decompose_tbl)
        tcp_df = conn.table(tcp_tbl)
        scp_df = conn.table(scp_tbl)
        period_df = conn.table(period_tbl)
        if not self._disable_hana_execution:
            decom_cols = decompose_df.columns
            # ID is timestamp
            if not self.is_index_int:
                if tcp_df.shape[0] > 0:
                    tcp_cols = tcp_df.columns
                    tcp_int = tcp_df.sort('TREND_CP')
                    data_int = data[used_cols].sort(key).add_id(f'{key}_INT')
                    tcp_timestamp = tcp_int.join(data_int, f'"TREND_CP"="{key}_INT"').select(tcp_cols[0], key).sort(tcp_cols[0]).rename_columns({key:'TREND_CP'})
                else:
                    tcp_timestamp = tcp_df

                if scp_df.shape[0] > 0:
                    scp_cols = scp_df.columns
                    scp_int = scp_df.sort('SEASON_CP')
                    data_int = data[used_cols].sort(key).add_id(f'{key}_INT')
                    scp_timestamp = scp_int.join(data_int, f'"SEASON_CP"="{key}_INT"').select(scp_cols[0], key).sort(scp_cols[0]).rename_columns({key:'SEASON_CP'})
                else:
                    scp_timestamp = scp_df
                decompose_int = decompose_df.rename_columns({'ID':'ID_RESULT'})
                data_int = data[used_cols].add_id(f'{key}_INT', ref_col=key)
                decompose_timestamp = decompose_int.join(data_int, f'"ID_RESULT"="{key}_INT"').select(key, decom_cols[1], decom_cols[2], decom_cols[3])
                return(tcp_timestamp,
                       scp_timestamp,
                       period_df,
                       decompose_timestamp)

            # ID is INT
            decompose_int_sql = """
                            SELECT {0} AS {5},
                            {1},
                            {2},
                            {3}
                            FROM {4}
                            """.format(quotename(decom_cols[0]),
                                       quotename(decom_cols[1]),
                                       quotename(decom_cols[2]),
                                       quotename(decom_cols[3]),
                                       decompose_tbl,
                                       quotename(key))
            decompose_int = conn.sql(decompose_int_sql)
            return (tcp_df,
                    scp_df,
                    period_df,
                    decompose_int)
        return None

def _params_check(input_dict, param_map):
    update_params = {}
    if not input_dict or input_dict is None:
        return update_params

    for parm in input_dict:
        if parm in param_map.keys():
            parm_val = input_dict[parm]
            arg_map = param_map[parm]
            update_params[arg_map[0]] = (arg(parm, parm_val, arg_map[1]), arg_map[1])
        else:
            err_msg = f"'{parm}' is not a valid parameter name for initializing a Online BCPD model!"
            logger.error(err_msg)
            raise KeyError(err_msg)

    return update_params

class OnlineBCPD(PALBase):
    r"""
    Online Bayesian Change-point detection.

    Parameters
    ----------

    alpha : float, optional
        Parameter of t-distribution.

        Defaults to 0.1.

    beta : float, optional
        Parameter of t-distribution.

        Defaults to 0.01.

    kappa : float, optional
        Parameter of t-distribution.

        Defaults to 1.0.

    mu : float, optional
        Parameter of t-distribution.

        Defaults to 0.0.

    lamb : float, optional
        Parameter of constant hazard function.

        Defaults to 250.0.

    threshold : float, optional
        Threshold to determine a change point:

        - 0: Return the probability of change point for every time step.
        - 0~1: Only return the time step of which the probability is above the threshold.

        Defaults to 0.0.

    delay : int, optional
        Number of incoming time steps to determine whether the current time step is a change point.

        Defaults to 3.

    prune : bool, optional
        Reduce the size of the model table after every run:

        - False: Do not prune.
        - True: Prune.

        Defaults to False.

    massive : bool, optional
        Specifies whether or not to use massive mode of OnlineBCPD.

        - True: Massive mode.
        - False: Single mode.

        For parameter setting in massive mode, you can use both
        ``group_params`` (please see the example below) or the original parameters.
        Using original parameters will apply to all groups. However, if you define some parameters for a group,
        the value of all original parameter settings will not be applicable to such a group.

        An example is as follows:

        >>> obcpd = OnlineBCPD(massive=True,
                               threshold=2,
                               group_params={'Group_1': {'threshold': 10, 'prune': False}})
        >>> res = obcpd.fit_predict(data=df,
                                    key='ID',
                                    endog='y',
                                    group_key='GROUP_ID')

        In this example, as 'threshold' is set in `group_params` for Group_1, it is not applicable to Group_1.

        Defaults to False.

    group_params : dict, optional
        If massive mode is activated (`massive` is True),
        input data shall be divided into different groups with different parameters applied.

        An example is as follows:

        >>> obcpd = OnlineBCPD(massive=True,
                               group_params={'Group_1': {'threshold': 10, 'prune': False},
                                             'Group_2': {'threshold': 10, 'prune': True}})
        >>> res = obcpd.fit_predict(data=df,
                                    key='ID',
                                    endog='y',
                                    group_key='GROUP_ID')

        Valid only when `massive` is True and defaults to None.

    Attributes
    ----------

    model_ : DataFrame
        Model.

    error_msg_ : DataFrame
        Error message.
        Only valid if `massive` is True when initializing an `OnlineBCPD` instance.

    Examples
    --------
    Input Data:

    >>> df.collect()
       ID        VAL
    0   0   9.926943
    1   1   9.262971
    2   2   9.715766
    3   3   9.944334
    4   4   9.577682
    5   5  10.036977
    6   6   9.513112
    7   7  10.233246
    8   8  10.159134
    9   9   9.759518
    .......

    Create an OnlineBCPD instance:

    >>> obcpd = OnlineBCPD(alpha=0.1,
                           beta=0.01,
                           kappa=1.0,
                           mu=0.0,
                           delay=5,
                           threshold=0.5,
                           prune=True)

    Invoke fit_predict():

    >>> model, cp = obcpd.fit_predict(data=df, model=None)

    Output:

    >>> print(model.head(5).collect())
       ID  ALPHA        BETA  KAPPA         MU          PROB
    0   0    0.1    0.010000    1.0   0.000000  4.000000e-03
    1   1    0.6   71.013179    2.0   8.426338  6.478577e-05
    2   2    1.1   86.966340    3.0  10.732357  7.634862e-06
    3   3    1.6  100.514641    4.0  12.235038  1.540977e-06
    4   4    2.1  107.197565    5.0  13.052529  3.733699e-07
    >>> print(cp.collect())
       ID  POSITION  PROBABILITY
    0   0        58     0.989308
    1   1       249     0.991023
    2   2       402     0.994154
    3   3       539     0.981004
    4   4       668     0.994708

    """

    __init_param_dict = {'alpha'     : ('ALPHA',     float),
                         'beta'      : ('BETA',      float),
                         'kappa'     : ('KAPPA',     float),
                         'mu'        : ('MU',        float),
                         'lamb'      : ('LAMBDA',    float),
                         'threshold' : ('THRESHOLD', float),
                         'delay'     : ('DELAY',     int  ),
                         'prune'     : ('PRUNE',     bool )}

    def __init__(self,#pylint:disable=too-many-positional-arguments
                 alpha=None,
                 beta=None,
                 kappa=None,
                 mu=None,
                 lamb=None,
                 threshold=None,
                 delay=None,
                 prune=None,
                 massive=False,
                 group_params=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(OnlineBCPD, self).__init__()

        init_params = {'alpha'     :  alpha,
                       'beta'      :  beta,
                       'kappa'     :  kappa,
                       'mu'        :  mu,
                       'lamb'      :  lamb,
                       'threshold' :  threshold,
                       'delay'     :  delay,
                       'prune'     :  prune}

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
            self.error_msg_ = None

        self.model_ = None
        self.model_tbl_name = None
        self.conn = None

    def fit_predict(self, data, key=None, endog=None, model=None, group_key=None):#pylint:disable=too-many-positional-arguments
        r"""
        Detects change-points of the input data.

        Parameters
        ----------

        data : DataFrame

            Input time-series data for change-point detection.

        key : str, optional

            Column name for time-stamp of the input time-series data.

            If the index column of data is not provided or not a single column, and the key of fit_predict function is not provided,
            the default value is the first column of data.

            If the index of data is set as a single column, the default value of key is index column of data.

        endog : str, optional

            Column name for the value of the input time-series data.
            Defaults to the first non-key column.

        model : DataFrame, optional

            The model for change point detection.

            Defaults to self.model\_ (the default value of self.model\_ is None).

        group_key : str, optional
            The column of group_key. The data type can be INT or NVARCHAR/VARCHAR.
            This parameter is only valid when ``massive`` is True.

            Defaults to the first column of data if the index columns of data is not provided.
            Otherwise, defaults to the first column of index columns.

        Returns
        -------
        A tuple of DataFrames:
            DataFrame 1

                Model.

            DataFrame 2

                The detected change points.

        """
        conn = data.connection_context
        require_pal_usable(conn)
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        setattr(self, "key", key)
        setattr(self, "endog", endog)
        param_rows = []

        if data is None:
            msg = 'The data cannot be None!'
            logger.error(msg)
            raise ValueError(msg)

        self.conn = conn
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
                for name in self.__pal_params[group]:
                    value, typ = self.__pal_params[group][name]
                    tpl = [tuple([group] + list(_map_param(name, value, typ)))]
                    param_rows.extend(tpl)

            unique_id = str(uuid.uuid1()).replace('-', '_').upper()
            model_tbl = f'#PAL_MASSIVE_ONLINE_BCPD_MODEL_TBL_{self.id}_{unique_id}'
            cp_tbl    = f'#PAL_MASSIVE_ONLINE_BCPD_CHANGE_POINT_TBL_{self.id}_{unique_id}'
            msg_tbl   = f'#PAL_MASSIVE_ONLINE_BCPD_ERROR_MSG_TBL_{self.id}_{unique_id}'
            self.model_tbl_name = model_tbl
            outputs = [model_tbl, cp_tbl, msg_tbl]
            input_model = None
            if not (check_pal_function_exist(conn, '%MASSIVE_ONLINE_BCPD%', like=True) or \
            self._disable_hana_execution):
                msg = 'The version of your SAP HANA does not support massive online BCPD!'
                logger.error(msg)
                raise ValueError(msg)
            try:
                if model:
                    input_model = model
                elif self.model_:
                    input_model = self.model_
                else:
                    input_model = conn.sql("SELECT TOP 0 * FROM (SELECT 1 GROUP_ID, 1 ID, 1.0 ALPHA, 1.0 BETA, 1.0 KAPPA, 1.0 MU, 1.0 PROB FROM DUMMY) dt;")
                    input_model = input_model.cast({"ID":"INTEGER", "ALPHA":"DOUBLE", "BETA":"DOUBLE", "KAPPA":"DOUBLE", "MU":"DOUBLE", "PROB":"DOUBLE"})

                self._call_pal_auto(conn,
                                    "PAL_MASSIVE_ONLINE_BCPD",
                                    data_,
                                    input_model,
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
            self.error_msg_ = conn.table(msg_tbl)
            cp_df = conn.table(cp_tbl)

            # key is timestamp
            if not self.is_index_int:
                cp_timestamp = cp_df
                if cp_df.shape[0] > 0:
                    new_id_name = key + "_TEMP"
                    cp_df = cp_df.rename_columns({'ID': new_id_name})
                    cp_cols = cp_df.columns
                    cp_groups = list(cp_df[["GROUP_ID"]].collect()["GROUP_ID"].drop_duplicates())
                    # selected Input DataFrame based on result group id
                    selected_data = None
                    data_new = data.select(group_key, key)
                    for group_name in cp_groups:
                        group_data = data_new.filter(f"{quotename(data_new.dtypes()[0][0])}={group_name}").sort(data_new.dtypes()[0][0]).add_id("GROUP_INDEX", ref_col=key)
                        if selected_data is None:
                            selected_data = group_data
                        else:
                            selected_data = selected_data.union(group_data)
                    cp_timestamp = cp_df.alias('L').join(selected_data.alias('R'), condition=f'L.POSITION = R.GROUP_INDEX and L.GROUP_ID=R.{quotename(group_key)}')
                    cp_timestamp = cp_timestamp.select(cp_cols[0], new_id_name, key, cp_cols[3])
                    cp_timestamp = cp_timestamp.rename_columns({key:'POSITION', new_id_name:'ID'})
                return (self.model_, cp_timestamp)
            # key is INT
            return (self.model_, cp_df)

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
            msg = "'key' or 'endog' parameter contains unrecognized column name."
            raise ValueError(msg)
        data_ = data[used_cols]

        self.is_index_int = _is_index_int(data_, key)
        if not self.is_index_int:
            data_ = _convert_index_from_timestamp_to_int(data_, key)

        for name in self.__pal_params:
            value, typ = self.__pal_params[name]
            tpl = [_map_param(name, value, typ)]
            param_rows.extend(tpl)

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        model_tbl = f'#PAL_ONLINE_BCPD_MODEL_TBL_{self.id}_{unique_id}'
        self.model_tbl_name = model_tbl
        cp_tbl    = f'#PAL_ONLINE_BCPD_CHANGE_POINT_TBL_{self.id}_{unique_id}'
        outputs = [model_tbl, cp_tbl]
        input_model = None
        if not (check_pal_function_exist(conn, '%ONLINE_BCPD%', like=True) or \
        self._disable_hana_execution):
            msg = 'The version of your SAP HANA does not support online BCPD!'
            logger.error(msg)
            raise ValueError(msg)
        try:
            if model:
                input_model = model
            elif self.model_:
                input_model = self.model_
            else:
                input_model = conn.sql("SELECT TOP 0 * FROM (SELECT 1 ID, 1.0 ALPHA, 1.0 BETA, 1.0 KAPPA, 1.0 MU, 1.0 PROB FROM DUMMY) dt;")
                input_model = input_model.cast({"ID":"INTEGER", "ALPHA":"DOUBLE", "BETA":"DOUBLE", "KAPPA":"DOUBLE", "MU":"DOUBLE", "PROB":"DOUBLE"})
            self._call_pal_auto(conn,
                                "PAL_ONLINE_BCPD",
                                data_,
                                input_model,
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
        cp_df = conn.table(cp_tbl)

        # ID is timestamp
        if not self.is_index_int:
            cp_timestamp = cp_df
            if cp_df.shape[0] > 0:
                data_int = data.add_id('ID_DATA', ref_col=key)
                new_id_name = key + "_TEMP"
                cp_df = cp_df.rename_columns({'ID': new_id_name})
                cp_cols = cp_df.columns
                cp_timestamp = cp_df.join(data_int, 'POSITION=ID_DATA').select(cp_cols[0], key, cp_cols[2])
                cp_timestamp = cp_timestamp.rename_columns({key:'POSITION', new_id_name:'ID'})
            return (self.model_, cp_timestamp)

        # ID is INT
        return (self.model_, cp_df)

    def get_stats(self):
        r"""
        Gets the statistics.

        Returns
        -------

        DataFrame

            Statistics.
        """
        if self.model_:
            return self.model_
        return None
