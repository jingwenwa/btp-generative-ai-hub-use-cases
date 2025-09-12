#pylint: disable=too-many-locals, too-many-statements, too-many-branches, too-many-arguments
#pylint:disable=too-many-lines, line-too-long, invalid-name, undefined-variable
#pylint:disable=too-few-public-methods, too-many-instance-attributes, attribute-defined-outside-init
"""
This module contains Python API of PAL recommender system algorithms.
The following classes are available:

* :class:`ALS`
* :class:`FRM`
* :class:`FFMClassifier`
* :class:`FFMRegressor`
* :class:`FFMRanker`
* :class:`MLPRecommender`

"""
import logging
import uuid
from hdbcli import dbapi
from hana_ml.ml_exceptions import FitIncompleteError
from .utility import check_pal_function_exist
from .pal_base import (
    PALBase,
    _INT_TYPES,
    ParameterTable,
    ListOfTuples,
    ListOfStrings,
    pal_param_register,
    try_drop,
    require_pal_usable
)
from .sqlgen import trace_sql
logger = logging.getLogger(__name__)
#_INTEGER_TYPES = (int, long) if(sys.version_info.major == 2) else(int,)
#_STRING_TYPES = (str, unicode) if(sys.version_info.major == 2) else(str,)

class ALS(PALBase):
    r"""
    Alternating least squares (ALS) is a powerful matrix factorization algorithm
    for building both explicit and implicit feedback based recommender systems.

    Parameters
    ----------
    factor_num : int, optional
        Length of factor vectors in the model.

        Default to 8.
    random_state : int, optional
        Specifies the seed for random number generator.

          - 0: Uses the current time as the seed.
          - Others: Uses the specified value as the seed.

        Default to 0.
    lamb : float, optional
        Specifies the L2 regularization of the factors.

        Default to 1e-2
    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 0.
    max_iter : int, optional
        Specifies the maximum number of iterations for the ALS algorithm.

        Default to 20.
    tol : float, optional
        Specifies the tolerance for exiting the iterative algorithm.

        The algorithm exits if the value of cost function is not decreased more
        than this value since the last check.

        If ``tol`` is set to 0, there is no check, and the algorithm only exits on reaching the maximum number of iterations.

        Note that evaluations of cost function require additional calculations, and you can set this parameter to 0 to avoid it.

        Default to 0.
    exit_interval : int, optional
        Specifies the number of iterations between consecutive convergence checkings.

        Basically, the algorithm calculates cost function and checks every ``exit_interval`` iterations
        to see if the tolerance has been reached.

        Note that evaluations of cost function require additional calculations.

        Only valid when ``tol`` is not 0.

        Default to 5.
    implicit : bool, optional
        Specifies implicit/explicit ALS.

        Default to False.
    linear_solver : {'cholesky', 'cg'}, optional
        Specifies the linear system solver.

        Default to 'cholesky'.
    cg_max_iter : int, optional
        Specifies maximum number of iteration of cg solver.

        Only valid when ``linear_solver`` is specified.

        Default to 3.
    alpha : float, optional
        Used when computing the confidence level in implicit ALS.

        Only valid when ``implicit`` is set to True.

        Default to 1.0.
    resampling_method : str, optional
        Specifies the resampling method for model evaluation or parameter selection.

        Valid resampling methods include: 'cv', 'bootstrap', 'cv_sha', 'bootstrap_sha',
        'cv_hyperband', 'bootstrap_hyperband'. It should be emphasized that the later four
        methods are designed for parameter selection only, not for model evaluation.

        If not specified, neither model evaluation nor parameters selection is activated.

        No default value.
    evaluation_metric : {'rmse'}, optional
        Specifies the evaluation metric for model evaluation or parameter selection.

        If not specified, neither model evaluation nor parameter selection is activated.

        No default value.
    fold_num : int, optional
        Specifies the fold number for the cross validation method.

        Mandatory and valid only when ``resampling_method`` is set as 'cv'.

        Default to 1.
    repeat_times : int, optional
        Specifies the number of repeat times for resampling.

        Default to 1.
    search_strategy : {'grid', 'random'}, optional
        Specifies the method to activate parameter selection.

        Mandatory when ``resampling_method`` is set as 'cv_sha' or 'bootstrap_sha'.

        Defaults to 'random' and cannot be changed if ``resampling_method`` is set as 'cv_hyperband'
        or 'bootstrap_hyperband', otherwise no default value.
    random_search_times : int, optional
        Specifies the number of times to randomly select candidate parameters for selection.

        Mandatory and valid when ``search_strategy`` is set as 'random'.

        No default value.
    timeout : int, optional
        Specifies maximum running time for model evaluation or parameter selection, in seconds.

        No timeout when 0 is specified.

        Default to 0.
    progress_indicator_id : str, optional
        Sets an ID of progress indicator for model evaluation or parameter selection.

        No progress indicator is active if no value is provided.

        No default value.
    param_values : dict or ListOfTuples, optional

        Specifies values of parameters to be selected.

        Input should be a dict or list of size-two tuples, with key/1st element of each tuple being the target parameter name,
        while value/2nd element being the a list of valued for selection.

        Valid only when ``resampling_method`` and ``search_strategy`` are both specified.

        Valid parameter names include : ``alpha``, ``factor_num``, ``lamb``.

        No default value.

    param_range : dict or ListOfTuples, optional

        Specifies ranges of parameters to be selected.

        Input should be a dict or list of size-two tuples, with key/1st element of each tuple being the name of the target parameter,
        and value/2nd element being a list that specifies the range of parameters with the following format:

          [start, step, end] or [start, end].

        Valid only Only when `resampling_method` and `search_strategy` are both specified.

        Valid parameter names include : ``alpha``, ``factor_num``, ``lamb``.

        No default value.

    reduction_rate : float, optional
        Specifies reduction rate in SHA or Hyperband method.

        For each round, the available parameter candidate size will be divided by value of this parameter.
        Thus valid value for this parameter must be greater than 1.0

        Valid only when ``resampling_method`` takes one of the following values:
        'cv_sha', 'bootstrap_sha', 'cv_hyperband', 'bootstrap_hyperband'.

        Defaults to 3.0.

    min_resource_rate : float, optional
        Specifies the minimum resource rate that should be used in SHA or Hyperband iteration.

        Valid only when ``resampling_method`` takes one of the following values: 'cv_sha', 'cv_hyperband',
        'bootstrap_sha', 'bootstrap_hyperband'.

        Defaults to 0.0.

    aggressive_elimination : bool, optional
        Specifies whether to apply aggressive elimination while using SHA method.

        Aggressive elimination happens when the data size and parameters size to be searched does not match
        and there are still bunch of parameters to be searched while data size reaches its upper limits.
        If aggressive elimination is applied, lower bound of limit of data size will be used multiple times
        first to reduce number of parameters.

        Valid only when ``resampling_method`` is 'cv_sha' or 'bootstrap_sha'.

        Defaults to False.


    Attributes
    ----------
    metadata_ : DataFrame
        Model metadata content.
    map_ : DataFrame
        Map info.
    factors_ : DataFrame
        Decomposed factors.
    optim_param_ : DataFrame
        Optimal parameters selected.
    stats_ : DataFrame
        Statistics.
    iter_info_ : DataFrame
        Cost function value and RMSE of corresponding iterations.

    Examples
    --------
    Input DataFrame df_train:

    >>> df_train.collect()
      USER       MOVIE FEEDBACK
    0    A      Movie1      4.8
    1    A      Movie2      4.0
    ...
    35   E      Movie7      3.5
    36   E      Movie8      3.5

    Create an ALS instance:

    >>> als = ALS(factor_num=2, lamb=1e-2, max_iter=20, tol=1e-6,
                  exit_interval=5, linear_solver='cholesky', thread_ratio=0, random_state=1)

    Perform fit():

    >>> als.fit(data=df_train)

    >>> als.factors_.collect().head(10)
        FACTOR_ID    FACTOR
    0           0  1.108775
    1           1 -0.582392
    ...
    8           8  1.151257
    9           9  0.315342

    Perform predict():

    >>> res = als.predict(data=df_predict,
                          thread_ratio=1,
                          key='ID')

    Output:

    >>> res.collect()
       ID   USER      MOVIE     PREDICTION
    0   1      A     Movie3       3.868747
    1   2      A     Movie7       2.870243
    ...
    6   7      D     Movie5       4.325851
    7   8      E  Bad_Movie       2.545807
    """
    resampling_method_list = ['cv', 'cv_sha', 'cv_hyperband',
                              'bootstrap', 'bootstrap_sha',
                              'bootstrap_hyperband']
    evaluation_metric_list = ['RMSE']
    search_strat_list = ['grid', 'random']
    range_params_map = {'factor_num' : 'FACTOR_NUMBER',
                        'lamb' : 'REGULARIZATION',
                        'alpha' : 'ALPHA'}
    linear_solver_map = {'choleskey': 0, 'cg': 1, 'cholesky': 0}
    resource_map = {'data_size': None, 'max_iter': 'MAX_ITERATION'}
    pal_funcname = 'PAL_ALS'
    def __init__(self,#pylint: disable=too-many-arguments, too-many-locals, too-many-branches, too-many-statements
                 random_state=None,
                 max_iter=None,
                 tol=None,
                 exit_interval=None,
                 implicit=None,
                 linear_solver=None,
                 cg_max_iter=None,
                 thread_ratio=None,
                 resampling_method=None,
                 evaluation_metric=None,
                 fold_num=None,
                 repeat_times=None,
                 search_strategy=None,
                 random_search_times=None,
                 timeout=None,
                 progress_indicator_id=None,
                 param_values=None,
                 param_range=None,
                 factor_num=None,
                 lamb=None,
                 alpha=None,
                 reduction_rate=None,
                 min_resource_rate=None,
                 aggressive_elimination=None
                 ):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(ALS, self).__init__()
        self.factor_num = self._arg('factor_num', factor_num, int)
        self.random_state = self._arg('random_state', random_state, int)
        self.lamb = self._arg('lamb', lamb, float)
        self.max_iter = self._arg('max_iter', max_iter, int)
        self.tol = self._arg('tol', tol, float)
        self.exit_interval = self._arg('exit_interval', exit_interval, int)
        #if self.tol is not None and tol == 0:
        #    if exit_interval is not None:
        #        msg = ("`exit_interval` should only be valid if tolerance is not set to 0.")
        #        raise ValueError(msg)
        self.implicit = self._arg('implicit', implicit, int)
        self.linear_solver = self._arg('linear_solver', linear_solver, self.linear_solver_map)
        #if self.linear is not None:
        #    if self.linear not in :
        #        msg = ("Linear solver '{}' is not available in ALS.".format(self.linear))
        #        logger.error(msg)
        #        raise ValueError(msg)
        self.cg_max_iter = self._arg('cg_max_iter', cg_max_iter, int)
        #if self.linear_solver != 1:
        #    if cg_max_iter is not None:
        #        msg = ("`cg_max_iter` should only be valid if `linear_solver` is set as 'cg'.")
        #        raise ValueError(msg)
        self.alpha = self._arg('alpha', alpha, float)
        #if self.implicit is not True:
        #    if alpha is not None:
        #        msg = ("`alpha` should only be valid if `implicit` is set as True.")
        #        raise ValueError(msg)
        self.thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        self.resampling_method = self._arg('resampling_method', resampling_method, str)
        if self.resampling_method is not None:
            self.resampling_method = self.resampling_method.lower()
            if self.resampling_method not in self.resampling_method_list:#pylint:disable=line-too-long, bad-option-value
                msg = ("Resampling method '{}' is not available ".format(self.resampling_method)+
                       "for model evaluation/parameter selection in ALS.")
                logger.error(msg)
                raise ValueError(msg)
        self.evaluation_metric = self._arg('evaluation_metric', evaluation_metric, str)
        if self.evaluation_metric is not None:
            self.evaluation_metric = self.evaluation_metric.upper()
            if self.evaluation_metric not in self.evaluation_metric_list:
                msg = ("Evaluation metric '{}' is not available.".format(self.evaluation_metric))
                logger.error(msg)
                raise ValueError(msg)
        self.fold_num = self._arg('fold_num', fold_num, int)
        if 'cv' not in str(self.resampling_method):
            if self.fold_num is not None:
                msg = ("Fold number should only be valid if "+
                       "`resampling_method` is set as 'cv', 'cv_sha' or 'cv_hyperband'.")
                raise ValueError(msg)
        self.repeat_times = self._arg('repeat_times', repeat_times, int)
        if 'cv' in str(self.resampling_method) and self.fold_num is None:
            msg = ("`fold_num` cannot be None when `resampling_method` is set to 'cv', 'cv_sha' or 'cv_hyperband'.")
            logger.error(msg)
            raise ValueError(msg)
        self.search_strategy = self._arg('search_strategy', search_strategy, str)
        if 'hyperband' in str(self.resampling_method):
            self.search_strategy = 'random'
        elif 'sha' in str(self.resampling_method) and self.search_strategy is None:
            msg = ("Parameter `search_strategy` must be specified when `resampling_method` is set to "+
                   "'cv_sha' or 'bootstrap_sha'.")
            logger.error(msg)
            raise ValueError(msg)
        if self.search_strategy is not None:
            self.seach_strategy = search_strategy.lower()
            if self.search_strategy not in self.search_strategy:
                msg = ("Search strategy `{}` is invalid ".format(self.search_strategy)+
                       "for parameter selection.")
                logger.error(msg)
                raise ValueError(msg)
        self.random_search_times = self._arg('random_search_times', random_search_times, int)
        if self.search_strategy == 'random' and self.random_search_times is None:
            msg = ("`random_search_times` cannot be None when"+
                   " `search_strategy` is set to 'random'.")
            logger.error(msg)
            raise ValueError(msg)
        if self.search_strategy != 'random' and self.random_search_times is not None:
            msg = ("`random_search_times` should only be valid when `search_strategy` is set as 'random'.")
            raise ValueError(msg)
        self.timeout = self._arg('timeout', timeout, int)
        self.progress_indicator_id = self._arg('progress_indicator_id', progress_indicator_id, str)
        if isinstance(param_range, dict):
            param_range = [(x, param_range[x]) for x in param_range]
        if isinstance(param_values, dict):
            param_values = [(x, param_values[x]) for x in param_values]
        self.param_values = self._arg('param_values', param_values, ListOfTuples)
        self.param_range = self._arg('param_range', param_range, ListOfTuples)
        self.model_ = None
        if self.search_strategy is None:
            if self.param_values is not None:
                msg = ("Specifying the values of `{}` ".format(self.param_values[0][0])+
                       "for non-parameter-search-strategy"+
                       " parameter selection is invalid.")
                logger.error(msg)
                raise ValueError(msg)
            if self.param_range is not None:
                msg = ("Specifying the range of `{}` for ".format(self.param_range[0][0])+
                       "non-parameter-search-strategy parameter selection is invalid.")
                logger.error(msg)
                raise ValueError(msg)
        else:
            value_list = []
            if self.alpha is not None:
                value_list.append("alpha")
            if self.factor_num is not None:
                value_list.append("factor_num")
            if self.lamb is not None:
                value_list.append("lamb")
            if self.param_values is not None:
                for x in self.param_values:
                    if len(x) != 2:#pylint:disable=bad-option-value
                        msg = ("Each tuple that specifies the values of a parameter should"+
                               " contain exactly 2 elements: 1st is parameter name,"+
                               " 2nd is a list of valid values.")
                        logger.error(msg)
                        raise ValueError(msg)
                    if x[0] not in self.range_params_map:
                        msg = ("Specifying the values of `{}` for ".format(x[0])+
                               "parameter selection is invalid.")
                        logger.error(msg)
                        raise ValueError(msg)
                    if x[0] in value_list:
                        msg = ("Parameter `{}` is invalid for ".format(x[0])+
                               "re-specification in parameter selection.")
                        logger.error(msg)
                        raise ValueError(msg)
                    if x[0] == 'alpha' and self.implicit is not True:
                        msg = ("`alpha` should only be valid if `implicit` is set as True.")
                        raise ValueError(msg)
                    if (x[0] == 'factor_num') and not (isinstance(x[1], list) and all(isinstance(t, _INT_TYPES) for t in x[1])):
                        msg = "Valid values of `{}` must be a list of int.".format(x[0])
                        logger.error(msg)
                        raise TypeError(msg)
                    if (x[0] in ('alpha', 'lamb')) and not (isinstance(x[1], list) and all(isinstance(t, (int, float)) for t in x[1])):
                        msg = "Valid values of `{}` must be a list of numerical values.".format(x[0])
                        logger.error(msg)
                        raise TypeError(msg)
                    value_list.append(x[0])

            if self.param_range is not None:
                rsz = [3] if self.search_strategy == 'grid'else [2, 3]
                for x in self.param_range:
                    if len(x) != 2:#pylint:disable=bad-option-value
                        msg = ("Each tuple that specifies the range of a parameter should contain"+
                               " exactly 2 elements: 1st is parameter name, 2nd is value range.")
                        logger.error(msg)
                        raise ValueError(msg)
                    if x[0] not in self.range_params_map:
                        msg = ("Parameter `{}` is invalid for ".format(x[0])+
                               "range specification in parameter selection.")
                        logger.error(msg)
                        raise ValueError(msg)
                    if x[0] in value_list:
                        msg = ("Parameter `{}` is invalid for ".format(x[0])+
                               "re-specification in parameter selection.")
                        logger.error(msg)
                        raise ValueError(msg)
                    if x[0] == 'alpha' and self.implicit is not True:
                        msg = ("`alpha` should only be valid if `implicit` is set as True.")
                        raise ValueError(msg)
                    if (x[0] == 'factor_num') and not (isinstance(x[1], list) and len(x[1]) in rsz and all(isinstance(t, _INT_TYPES) for t in x[1])):
                        msg = ("The provided range of `{}` is either not ".format(x[0])+
                               "a list of int, or it contains wrong number of values.")
                        logger.error(msg)
                        raise TypeError(msg)
                    if (x[0] in ('alpha', 'lamb')) and not (isinstance(x[1], list) and len(x[1]) in rsz and all(isinstance(t, (int, float)) for t in x[1])):
                        msg = ("The provided range of `{}` is either not ".format(x[0])+
                               "a list of numerical values, or it contains wrong number of values.")
                        logger.error(msg)
                        raise TypeError(msg)
        self.reduction_rate = self._arg('reduction_rate', reduction_rate, float)
        if self.reduction_rate is not None and self.reduction_rate <= 1.0:
            msg = '`reduction_rate` must be greater than 1'
            logger.error(msg)
            raise ValueError(msg)
        self.aggressive_elimination = self._arg('aggressive_elimination', aggressive_elimination,
                                                bool)
        self.min_resource_rate = self._arg('min_resource_rate', min_resource_rate, float)

    def fit(self, data, key=None, usr=None, item=None, feedback=None):
        """
        Fit the model to the training dataset.

        Parameters
        ----------
        data : DataFrame
            Data to be fitted for ALS model.

            It provides the observed feedback of users for different items, thus should contain at least
            the following three columns:

                - the column for user names/IDs
                - the column for item names/IDs
                - the column for users' feedback values w.r.t. items

        key : str, optional
            Name of the ID column.

            If ``key`` is not provided, then:

                - if ``data`` is indexed by a single column, then ``key`` defaults
                  to that index column;
                - otherwise, it is assumed that ``data`` contains no ID column.

        usr : str, optional
            Name of the user column.

            Defaults to the first non-key column of the input data.

        item : str, optional
            Name of the item column.

            Defaults to the first non-key and non-usr column of the input data.

        feedback : str, optional
            Name of the feedback column, where each value reflects the feedback(scoring) value
            of a user w.r.t. an item.

            Defaults to the last column of the input data.

        Returns
        -------
        A fitted object of class "ALS".
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        conn = data.connection_context
        require_pal_usable(conn)
        cols = data.columns
        key = self._arg('key', key, str)
        index = data.index
        if isinstance(index, str):
            if key is not None and index != key:
                msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(msg)
        key = index if key is None else key
        usr = self._arg('usr', usr, str)
        item = self._arg('item', item, str)
        feedback = self._arg('feedback', feedback, str)
        if key is not None:
            cols.remove(key)
        if usr is None:
            usr = cols[0]
        cols.remove(usr)
        if item is None:
            item = cols[0]
        cols.remove(item)
        if feedback is None:
            feedback = cols[-1]
        cols_left = [usr, item, feedback]
        param_rows = [('FACTOR_NUMBER', self.factor_num, None, None),
                      ('SEED', self.random_state, None, None),
                      ('REGULARIZATION', None, self.lamb, None),
                      ('MAX_ITERATION', self.max_iter, None, None),
                      ('EXIT_THRESHOLD', None, self.tol, None),
                      ('IMPLICIT_TRAIN', self.implicit, None, None),
                      ('LINEAR_SYSTEM_SOLVER', self.linear_solver, None, None),
                      ('THREAD_RATIO', None, self.thread_ratio, None),
                      ('RESAMPLING_METHOD', None, None, self.resampling_method),
                      ('EVALUATION_METRIC', None, None,
                       self.evaluation_metric.upper() if self.evaluation_metric is not None else None),
                      ('REPEAT_TIMES', self.repeat_times, None, None),
                      ('PARAM_SEARCH_STRATEGY', None, None, self.search_strategy),
                      ('TIMEOUT', self.timeout, None, None),
                      ('PROGRESS_INDICATOR_ID', None, None, self.progress_indicator_id),
                      ('MIN_RESOURCE_RATE', None, self.min_resource_rate, None),
                      ('REDUCTION_RATE', None, self.reduction_rate, None),
                      ('AGGRESSIVE_ELIMINATION', self.aggressive_elimination,
                       None, None)]
        if self.tol is not None:
            param_rows.extend([('EXIT_INTERVAL', self.exit_interval, None, None)])
        if self.linear_solver is not None:
            param_rows.extend([('CG_MAX_ITERATION', self.cg_max_iter, None, None)])
        if self.implicit is not None:
            param_rows.extend([('ALPHA', None, self.alpha, None)])
        if self.resampling_method is not None:
            param_rows.extend([('FOLD_NUM', self.fold_num, None, None)])
        if self.search_strategy is not None:
            param_rows.extend([('RANDOM_SEARCH_TIMES', self.random_search_times, None, None)])
        if self.param_values is not None:
            for x in self.param_values:
                values = str(x[1]).replace('[', '{').replace(']', '}')
                param_rows.extend([(self.range_params_map[x[0]]+'_VALUES',
                                    None, None, values)])
        if self.param_range is not None:
            for x in self.param_range:
                range_ = str(x[1])
                if len(x[1]) == 2 and self.search_strategy == 'random':
                    range_ = range_.replace(',', ',,')
                param_rows.extend([(self.range_params_map[x[0]]+'_RANGE',
                                    None, None, range_)])

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        tables = ['MODEL_METADATA', 'MODEL_MAP', 'MODEL_FACTORS', 'ITERATION_INFORMATION', 'STATISTICS', 'OPTIMAL_PARAMETER']
        tables = ["#PAL_ALS_{}_TBL_{}_{}".format(tbl, self.id, unique_id) for tbl in tables]
        metadata_tbl, map_tbl, factors_tbl, iter_info_tbl, stat_tbl, optim_param_tbl = tables
        try:
            self._call_pal_auto(conn,
                                "PAL_ALS",
                                data[cols_left],
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
        self.metadata_ = conn.table(metadata_tbl)
        self.map_ = conn.table(map_tbl)
        self.factors_ = conn.table(factors_tbl)
        self.optim_param_ = conn.table(optim_param_tbl)
        self.stats_ = conn.table(stat_tbl)
        self.statistics_ = self.stats_
        #pylint:disable=attribute-defined-outside-init
        self.iter_info_ = conn.table(iter_info_tbl)
        self.model_ = [self.metadata_, self.map_, self.factors_]
        return self

    def predict(self, data, key=None, usr=None, item=None, thread_ratio=None):
        """
        Prediction for the input data with the trained ALS model.

        Parameters
        ----------
        data : DataFrame
            Data to be predicted, structured similarly as the input data for fit but only
            without the feedback column.

        key : str, optional
            Name of the ID column.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        usr : str, optional
            Name of the user column.

            Defaults to the first non-key column of the input data.

        item : str, optional
            Name of the item column.

            Defaults to the first non-key and non-usr column of the input data.

        thread_ratio : float, optional
            Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
            Values outside the range will be ignored and this function heuristically determines the number of threads to use.

            Default to 0.

        Returns
        -------
        DataFrame
            Prediction result of the missing values(e.g. user feedback) in the input data, structured as follows:

              - 1st column : Data ID
              - 2nd column : User name/ID
              - 3rd column : Item name/ID
              - 4th column : Predicted feedback values
        """
        conn = data.connection_context

        if getattr(self, 'model_') is None:
            raise FitIncompleteError()
        cols = data.columns
        usr = self._arg('usr', usr, str)
        item = self._arg('item', item, str)
        index = data.index
        key = self._arg('key', key, str, required=not isinstance(index, str))
        if isinstance(index, str):
            if key is not None and index != key:
                msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(msg)
        key = index if key is None else key
        cols.remove(key)
        if usr is None:
            usr = cols[0]
        cols.remove(usr)
        if item is None:
            item = cols[-1]
        cols_left = [key, usr, item]
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        result_tbl = '#PAL_ALS_RESULT_TBL_{}_{}'.format(self.id, unique_id)
        param_rows = [('THREAD_RATIO', None, thread_ratio, None)]
        try:
            self._call_pal_auto(conn,
                                'PAL_ALS_PREDICT',
                                data[cols_left],
                                self.model_[0],
                                self.model_[1],
                                self.model_[2],
                                ParameterTable().with_data(param_rows),
                                result_tbl)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, result_tbl)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, result_tbl)
            raise
        return conn.table(result_tbl)

    def create_model_state(self, model=None, function=None,
                           pal_funcname='PAL_ALS',
                           state_description=None, force=False):
        r"""
        Create PAL model state.

        Parameters
        ----------
        model : DataFrame, optional
            Specify the model for AFL state.

            Defaults to self.model\_.

        function : str, optional
            Specify the function in the unified API.

            A placeholder parameter, not effective for ALS.

        pal_funcname : int or str, optional
            PAL function name.

            Defaults to 'PAL_ALS'.

        state_description : str, optional
            Description of the state as model container.

            Defaults to None.

        force : bool, optional
            If True it will delete the existing state.

            Defaults to False.
        """
        super()._create_model_state(model, function, pal_funcname, state_description, force)

    def set_model_state(self, state):
        """
        Set the model state by state information.

        Parameters
        ----------
        state: DataFrame or dict
            If state is DataFrame, it has the following structure:

                - NAME: VARCHAR(100), it mush have STATE_ID, HINT, HOST and PORT.
                - VALUE: VARCHAR(1000), the values according to NAME.

            If state is dict, the key must have STATE_ID, HINT, HOST and PORT.
        """
        super()._set_model_state(state)

    def delete_model_state(self, state=None):
        """
        Delete PAL model state.

        Parameters
        ----------
        state : DataFrame, optional
            Specified the state.

            Defaults to self.state.
        """
        super()._delete_model_state(state)

class FRM(PALBase):
    r"""
    Factorized Polynomial Regression Models or Factorization Machines approach.

    Factorization approach, for example matrix factorization, provides high accuracy in several important prediction problems, including recommender systems. In its basic form, matrix factorization characterizes both users and items by vectors of latent factors inferred from user-item rating patterns and high correspondence between user and item factors will lead to a recommendation. Factorization machines approach for recommender system is more general than common factorization approaches as it can characterize latent factors not only for users and items themselves, but also for side features related to them by making use of additional information, and therefore, makes the predictions more accurate.

    Parameters
    ----------

    solver : {'sgd', 'momentum', 'nag', 'adagrad'}, optional
        Specifies the method for solving the objective minimization problem.

        Default to 'sgd'.
    factor_num : int, optional
        Length of factor vectors in the model.

        Default to 8.
    init : float, optional
        Variance of the normal distribution used to initialize the model parameters.

        Default to 1e-2.
    random_state : int, optional
        Specifies the seed for random number generator.

            -  0: Uses the current time as the seed.
            -  Others: Uses the specified value as the seed.

        Note that due to the inherently randomicity of parallel sgc, models of different
        trainings might be different even with the same seed of random number generator.

        Default to 0.
    lamb : float, optional
        L2 regularization of the factors.

        Default to 1e-8.
    linear_lamb : float, optional
        L2 regularization of the factors.

        Default to 1e-10.
    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 0.
    max_iter : int, optional
        Specifies the maximum number of iterations for the ALS algorithm.

        Default value is 50.
    sgd_tol : float, optional
        Exit threshold.

        The algorithm exits when the cost function has not decreased
        more than this threshold in ``sgd_exit_interval`` steps.

        Default to 1e-5
    sgd_exit_interval : int, optional
        The algorithm exits when the cost function has not decreased
        more than ``sgd_tol`` in ``sgd_exit_interval`` steps.

        Default to 5.
    momentum : float, optional
        The momentum factor in method 'momentum' or 'nag'.

        Valid only when `method` is 'momentum' or 'nag'.

        Default to 0.9.
    resampling_method : {'cv', 'bootstrap'}, optional
        Specifies the resampling method for model evaluation or parameter selection.

        If not specified, neither model evaluation nor parameter selection is activated.

        No default value.
    evaluation_metric : {'rmse'}, optional
        Specifies the evaluation metric for model evaluation or parameter selection.

        If not specified, neither model evaluation nor parameter selection is activated.

        No default value.
    fold_num : int, optional
        Specifies the fold number for the cross validation method.

        Mandatory and valid only when ``resampling_method`` is set to 'cv'.

        Default to 1.
    repeat_times : int, optional
        Specifies the number of repeat times for resampling.

        Default to 1.
    search_strategy : {'grid', 'random'}, optional
        Specifies the method to activate parameter selection.

        No default value.
    random_search_times : int, optional
        Specifies the number of times to randomly select candidate parameters for selection.

        Mandatory and valid when PARAM_SEARCH_STRATEGY is set to random.

        No default value.
    timeout : int, optional
        Specifies maximum running time for model evaluation or parameter selection, in seconds.

        No timeout when 0 is specified.

        Default to 0.
    progress_indicator_id : str, optional
        Sets an ID of progress indicator for model evaluation or parameter selection.

        No progress indicator is active if no value is provided.

        No default value.
    param_values : dict or ListOfTuples, optional
        Specifies values of parameters to be selected.

        Input should be a dict or list of tuple of two elements, with the key/1st element being the parameter name,
        and value/2nd element being a list of values for selection.

        Valid only when ``resampling_method`` and ``search_strategy`` are both specified.

        Valid parameter names include : 'factor_num', 'lamb', 'linear_lamb', 'momentum'.

        No default value.
    param_range : dict or ListOfTuples, optional
        Specifies ranges of param to be selected.

        Input should be a dict or list of tuple of two elements , with key/1st element being the parameter name,
        and value/2nd element being a list of numerical values indicating the range for selection.

        Valid only when ``resampling_method`` and ``search_strategy`` are both specified.

        Valid parameter names include:'factor_num', 'lamb', 'linear_lamb', 'momentum'.

        No default value.

    reduction_rate : float, optional
        Specifies reduction rate in SHA or Hyperband method.

        For each round, the available parameter candidate size will be divided by value of this parameter.
        Thus valid value for this parameter must be greater than 1.0

        Valid only when ``resampling_method`` takes one of the following values:
        'cv_sha', 'bootstrap_sha', 'cv_hyperband', 'bootstrap_hyperband'.

        Defaults to 3.0.

    min_resource_rate : float, optional
        Specifies the minimum resource rate that should be used in SHA or Hyperband iteration.

        Valid only when ``resampling_method`` takes one of the following values: 'cv_sha', 'cv_hyperband',
        'bootstrap_sha', 'bootstrap_hyperband'.

        Defaults to 0.0.

    aggressive_elimination : bool, optional
        Specifies whether to apply aggressive elimination while using SHA method.

        Aggressive elimination happens when the data size and parameters size to be searched does not match
        and there are still bunch of parameters to be searched while data size reaches its upper limits.
        If aggressive elimination is applied, lower bound of limit of data size will be used multiple times
        first to reduce number of parameters.

        Valid only when ``resampling_method`` is 'cv_sha' or 'bootstrap_sha'.

        Defaults to False.


    Attributes
    ----------
    metadata_ : DataFrame
        Model metadata content.
    model_ : DataFrame
        Model (Map, Weight)
    factors_ : DataFrame
        Decomposed factors.
    optim_param_ : DataFrame
        Optimal parameters selected.
    stats_ : DataFrame
        Statistics.
    iter_info_ : DataFrame.
        Cost function value and RMSE of corresponding iteration.

    Examples
    --------
    Input DataFrame:

    >>> df_train.collect()
      USER       MOVIE  FEEDBACK
    0    A      Movie1       4.8
    1    A      Movie2       4.0
    ...
    35   E      Movie7       3.5
    36   E      Movie8       3.5

    Input user dataframe for training:

    >>> usr_info.collect()
        USER            USER_SIDE_FEATURE
        -- There is no side information for user provided. --

    Input item dataframe for training:

    >>> item_info.collect()
    0     MOVIE              GENRES
    1    Movie1              Sci-Fi
    ...
    8    Movie8     Sci-Fi,Thriller
    9 Bad_Movie    Romance,Thriller


    Create a FRM instance:

    >>> frm = FRM(factor_num=2, solver='adagrad',
                  learning_rate=0, max_iter=100,
                  thread_ratio=0.5, random_state=1)

    Perform fit():

    >>> frm.fit(data=df_train, usr_info, item_info, categorical_variable='TIMESTAMP')

    >>> frm.factors_.collect().head(10)
       FACTOR_ID      FACTOR
    0          0   -0.083550
    1          1   -0.083654
    ...
    8          8   -0.056534
    9          9   -0.342042

    Perform predict():

    >>> res = frm.predict(data=df_predict,
                          usr_info=usr_info,
                          item_info=item_info,
                          thread_ratio=0.5,
                          key='ID')
    >>> res.collect()
       ID USER  ITEM  PREDICTION
    0   1    A  None    3.486804
    1   2    A     4    3.490246
    ...
    6   7    D     3    4.097683
    7   8    E     2    2.317224
    """
    solver_map = {'sgd': 0, 'momentum': 1, 'nag': 2, 'adagrad': 3}
    resampling_method_list = ['cv', 'cv_sha', 'cv_hyperband',
                              'bootstrap', 'bootstrap_sha',
                              'bootstrap_hyperband']
    evaluation_metric_list = ['rmse']
    resource_map = {'data_size': None, 'max_iter': 'MAX_ITERATION'}
    search_strat_list = {'grid': 'grid', 'random': 'random'}
    range_params_map = {'factor_num' : 'FACTOR_NUMBER',
                        'lamb' : 'REGULARIZATION',
                        'linear_lamb' : 'LINEAR_REGULARIZATION',
                        'momentum' : 'MOMENTUM'}
    pal_funcname = 'PAL_FRM'
    def __init__(self,
                 solver=None,
                 factor_num=None,
                 init=None,
                 random_state=None,
                 learning_rate=None,
                 linear_lamb=None,
                 lamb=None,
                 max_iter=None,
                 sgd_tol=None,
                 sgd_exit_interval=None,
                 thread_ratio=None,
                 momentum=None,
                 resampling_method=None,
                 evaluation_metric=None,
                 fold_num=None,
                 repeat_times=None,
                 search_strategy=None,
                 random_search_times=None,
                 timeout=None,
                 progress_indicator_id=None,
                 param_values=None,
                 param_range=None,
                 reduction_rate=None,
                 min_resource_rate=None,
                 aggressive_elimination=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(FRM, self).__init__()
        self.solver = self._arg('solver', solver, self.solver_map)
        self.factor_num = self._arg('factor_num', factor_num, int)
        self.init = self._arg('init', init, float)
        self.random_state = self._arg('random_state', random_state, int)
        self.learning_rate = self._arg('learning_rate', learning_rate, float)
        self.linear_lamb = self._arg('linear_lamb', linear_lamb, float)
        self.lamb = self._arg('lamb', lamb, float)
        self.max_iter = self._arg('max_iter', max_iter, int)
        self.sgd_tol = self._arg('sgd_tol', sgd_tol, float)
        self.sgd_exit_interval = self._arg('sgd_exit_interval', sgd_exit_interval, int)
        self.thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        self.momentum = self._arg('momentum', momentum, float)
        self.resampling_method = self._arg('resampling_method', resampling_method, str)
        if self.resampling_method is not None:
            self.resampling_method = self.resampling_method.lower()
            if self.resampling_method not in self.resampling_method_list:#pylint:disable=line-too-long, bad-option-value
                msg = ("Resampling method '{}' is not available ".format(self.resampling_method)+
                       "for model evaluation/parameter selection in FRM.")
                logger.error(msg)
                raise ValueError(msg)
        self.evaluation_metric = self._arg('evaluation_metric', evaluation_metric, str)
        if self.evaluation_metric is not None:
            self.evaluation_metric  = self.evaluation_metric.lower()
            if self.evaluation_metric not in self.evaluation_metric_list:
                msg = ("Evaluation metric '{}' is not available.".format(self.evaluation_metric))
                logger.error(msg)
                raise ValueError(msg)
        self.fold_num = self._arg('fold_num', fold_num, int)
        if 'cv' in str(self.resampling_method) and self.fold_num is None:
            msg = ("`fold_num` cannot be None when `resampling_method` is set to 'cv', 'cv_sha' or 'cv_hyperband'.")
            logger.error(msg)
            raise ValueError(msg)
        self.repeat_times = self._arg('repeat_times', repeat_times, int)
        self.search_strategy = self._arg('search_strategy', search_strategy, self.search_strat_list)
        if 'hyperband' in str(self.resampling_method):
            self.search_strategy = 'random'
        elif 'sha' in str(self.resampling_method) and self.search_strategy is None:
            msg = ("Parameter `search_strategy` must be specified when `resampling_method` is set to "+
                   "'cv_sha' or 'bootstrap_sha'.")
            logger.error(msg)
            raise ValueError(msg)
        self.random_search_times = self._arg('random_search_times', random_search_times, int)
        if self.search_strategy == 'random' and self.random_search_times is None:
            msg = ("`random_search_times` cannot be None when `search_strategy` is set to 'random'.")
            logger.error(msg)
            raise ValueError(msg)
        if self.search_strategy != 'random' and self.random_search_times is not None:
            msg = ("`random_search_times` should only be valid when `search_strategy` is set as 'random'.")
            raise ValueError(msg)
        self.timeout = self._arg('timeout', timeout, int)
        self.progress_indicator_id = self._arg('progress_indicator_id', progress_indicator_id, str)
        if isinstance(param_range, dict):
            param_range = [(x, param_range[x]) for x in param_range]
        if isinstance(param_values, dict):
            param_values = [(x, param_values[x]) for x in param_values]
        self.param_values = self._arg('param_values', param_values, ListOfTuples)
        self.param_range = self._arg('param_range', param_range, ListOfTuples)
        self.reduction_rate = self._arg('reduction_rate', reduction_rate, float)
        if self.reduction_rate is not None and self.reduction_rate <= 1.0:
            msg = '`reduction_rate` must be greater than 1'
            logger.error(msg)
            raise ValueError(msg)
        self.aggressive_elimination = self._arg('aggressive_elimination', aggressive_elimination,
                                                bool)
        self.min_resource_rate = self._arg('min_resource_rate', min_resource_rate, float)
        self.model_ = None
        if self.search_strategy is None:
            if self.param_values is not None:
                msg = ("Specifying the values of `{}` ".format(self.param_values[0][0])+
                       "for non-parameter-search-strategy parameter selection is invalid.")
                logger.error(msg)
                raise ValueError(msg)
            if self.param_range is not None:
                msg = ("Specifying the range of `{}` for ".format(self.param_range[0][0])+
                       "non-parameter-search-strategy parameter selection is invalid.")
                logger.error(msg)
                raise ValueError(msg)
        else:
            value_list = []
            if self.factor_num is not None:
                value_list.append("factor_num")
            if self.linear_lamb is not None:
                value_list.append("linear_lamb")
            if self.lamb is not None:
                value_list.append("lamb")
            if self.momentum is not None:
                value_list.append("momentum")
            if self.param_values is not None:
                for x in self.param_values:
                    if len(x) != 2:#pylint:disable=bad-option-value
                        msg = ("Each tuple that specifies the values of a parameter should"+
                               " contain exactly 2 elements: 1st is parameter name,"+
                               " 2nd is a list of valid values.")
                        logger.error(msg)
                        raise ValueError(msg)
                    if x[0] not in self.range_params_map:
                        msg = ("Specifying the values of `{}` for ".format(x[0])+
                               "parameter selection is invalid.")
                        logger.error(msg)
                        raise ValueError(msg)
                    if x[0] in value_list:
                        msg = ("Parameter `{}` is invalid for ".format(x[0])+
                               "re-specification in parameter selection.")
                        logger.error(msg)
                        raise ValueError(msg)
                    if x[0] == 'momentum' and self.solver not in (2, 1):
                        msg = ("`momentum` should only be valid if `solver` is set as 'momentum' or 'nag'.")
                        raise ValueError(msg)
                    if (x[0] == 'factor_num') and not (isinstance(x[1], list) and all(isinstance(t, _INT_TYPES) for t in x[1])):
                        msg = "Valid values of `{}` must be a list of int.".format(x[0])
                        logger.error(msg)
                        raise TypeError(msg)
                    if (x[0] in ('linear_lamb', 'lamb', 'momentum')) and not (isinstance(x[1], list) and all(isinstance(t, (int, float)) for t in x[1])):
                        msg = "Valid values of `{}` must be a list of numerical values.".format(x[0])
                        logger.error(msg)
                        raise TypeError(msg)
                    value_list.append(x[0])

            if self.search_strategy is not None:
                rsz = [3] if self.search_strategy == 'grid'else [2, 3]
                for x in self.param_range:
                    if len(x) != 2:#pylint:disable=bad-option-value
                        msg = ("Each tuple that specifies the range of a parameter should contain"+
                               " exactly 2 elements: 1st is parameter name, 2nd is value range.")
                        logger.error(msg)
                        raise ValueError(msg)
                    if x[0] not in self.range_params_map:
                        msg = ("Parameter `{}` is invalid for ".format(x[0])+
                               "range specification in parameter selection.")
                        logger.error(msg)
                        raise ValueError(msg)
                    if x[0] in value_list:
                        msg = ("Parameter `{}` is invalid for ".format(x[0])+
                               "re-specification in parameter selection.")
                        logger.error(msg)
                        raise ValueError(msg)
                    if x[0] == 'momentum' and self.solver not in (1, 2):
                        msg = ("`momentum` should only be valid if method is set as 'momentum' or 'nag'.")
                        raise ValueError(msg)
                    if (x[0] == 'factor_num') and not (isinstance(x[1], list) and len(x[1]) in rsz and all(isinstance(t, _INT_TYPES) for t in x[1])):
                        msg = ("The provided range of `{}` is either not ".format(x[0])+
                               "a list of int, or it contains wrong number of values.")
                        logger.error(msg)
                        raise TypeError(msg)
                    if (x[0] in ('linear_lamb', 'lamb', 'momentum')) and not (isinstance(x[1], list) and len(x[1]) in rsz and all(isinstance(t, (int, float)) for t in x[1])):
                        msg = ("The provided range of `{}` is either not ".format(x[0])+
                               "a list of numerical values, or it contains wrong number of values.")
                        logger.error(msg)
                        raise TypeError(msg)

    def fit(self, data, usr_info, item_info, key=None, usr=None, item=None, feedback=None,
            features=None, usr_features=None, item_features=None,
            usr_key=None, item_key=None,
            categorical_variable=None, usr_categorical_variable=None, item_categorical_variable=None):
        """
        Fit the model to the training dataset.

        Parameters
        ----------
        data : DataFrame
            Data to be fit.
        usr_info : DataFrame
            DataFrame containing user side features.
        item_info : DataFrame
            DataFrame containing item side features.
        key : str, optional
            Name of the ID column.

            If ``key`` is not provided, then:

                - if ``data`` is indexed by a single column, then ``key`` defaults
                  to that index column;
                - otherwise, it is assumed that ``data`` contains no ID column.

        usr : str, optional
            Name of the user column.

            Defaults to the first non-key column of ``data``.
        item : str, optional
            Name of the item column.

            Defaults to the first non-key and non-usr column of the input data.
        feedback : str, optional
            Name of the feedback column.

            Defaults to the last column of the input data.
        features : str or a list of str, optional
            Global side features column name in the training dataframe.

            Defaults to the rest of input data removing key, usr, item and feedback columns.
        usr_features : str or a list of str, optional
            User side features column name in the training dataframe.

            Defaults to all columns in ``usr_info`` exclusive of the one specified by ``usr_key``.
        item_features : str or a list of str, optional
            Item side features column name in the training dataframe.

            Defaults to all columns in ``item_info`` exclusive of the one specified by ``item_key``.
        user_key : str, optional
            Specifies the column in ``usr_info`` that contains user names or IDs.

            Defaults to the 1st column of ``usr_info``.
        item_key : str, optional
            Specifies the column in ``item_info`` that contains item names or IDs.

            Defaults to the 1st column of ``item_info``
        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.
        usr_categorical_variable : str or a list of str, optional
            Name of user side feature columns of INTEGER type that should be treated as categorical.
        item_categorical_variable : str or a list of str, optional
            Name of item side feature columns of INTEGER type that should be treated as categorical.

        Returns
        -------
        A fitted object of class "FRM".
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        conn = data.connection_context
        require_pal_usable(conn)
        if categorical_variable is not None:
            if isinstance(categorical_variable, str):
                categorical_variable = [categorical_variable]
            try:
                categorical_variable = self._arg('categorical_variable', categorical_variable, ListOfStrings)#pylint: disable=undefined-variable
            except:
                msg = ("'categorical_variable' must be list of string or string.")
                logger.error(msg)
                raise TypeError(msg)
        if usr_categorical_variable is not None:
            if isinstance(usr_categorical_variable, str):
                usr_categorical_variable = [usr_categorical_variable]
            try:
                usr_categorical_variable = self._arg('usr_categorical_variable', usr_categorical_variable, ListOfStrings)#pylint: disable=undefined-variable
            except:
                msg = ("'usr_categorical_variable' must be list of string or string.")
                logger.error(msg)
                raise TypeError(msg)
        if item_categorical_variable is not None:
            if isinstance(item_categorical_variable, str):
                item_categorical_variable = [item_categorical_variable]
            try:
                item_categorical_variable = self._arg('item_categorical_variable', item_categorical_variable, ListOfStrings)#pylint: disable=undefined-variable
            except:
                msg = ("'item_categorical_variable' must be list of string or string.")
                logger.error(msg)
                raise TypeError(msg)
        cols = data.columns
        key = self._arg('key', key, str)
        index = data.index
        if isinstance(index, str):
            if key is not None and index != key:
                msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(msg)
        key = index if key is None else key
        if key is not None:
            cols.remove(key)
        if usr is None:
            usr = cols[0]
        cols.remove(usr)
        if item is None:
            item = cols[0]
        cols.remove(item)
        if feedback is None:
            feedback = cols[-1]
        cols.remove(feedback)
        if features is not None:
            if isinstance(features, str):
                features = [features]
            try:
                features = self._arg('features', features, ListOfStrings)#pylint: disable=undefined-variable
            except:
                msg = ("'features' must be list of string or string.")
                logger.error(msg)
                raise TypeError(msg)
        else:
            features = cols
        data_ = data[[usr] + [item] + features + [feedback]]
        if usr_features is not None:
            if isinstance(usr_features, str):
                usr_features = [usr_features]
            try:
                usr_features = self._arg('usr_features', usr_features, ListOfStrings)#pylint: disable=undefined-variable
            except:
                msg = ("'usr_features' must be list of string or string.")
                logger.error(msg)
                raise TypeError(msg)
        usr_cols = usr_info.columns
        usr_key = self._arg('usr_key', usr_key, str)
        if usr_key is not None:
            usr_cols.remove(usr_key)
        elif len(usr_cols) > 0:#pylint:disable=len-as-condition
            usr_cols.remove(usr_cols[0])
        if usr_features is not None:
            for var in usr_features:
                usr_cols.remove(var)

        if item_features is not None:
            if isinstance(item_features, str):
                item_features = [item_features]
            try:
                item_features = self._arg('item_features', item_features, ListOfStrings)#pylint: disable=attribute-defined-outside-init, undefined-variable
            except:
                msg = ("'item_features' must be list of string or string.")
                logger.error(msg)
                raise TypeError(msg)
        item_cols = item_info.columns
        item_key = self._arg('item_key', item_key, str)
        if item_key is not None:
            item_cols.remove(item_key)
        elif len(item_cols) > 0:#pylint:disable=len-as-condition
            item_cols.remove(item_cols[0])
        if item_features is not None:
            for var in item_features:
                item_cols.remove(var)
        param_rows = [('FACTOR_NUMBER', self.factor_num, None, None),
                      ('SEED', self.random_state, None, None),
                      ('REGULARIZATION', None, self.lamb, None),
                      ('LINEAR_REGULARIZATION', None, self.linear_lamb, None),
                      ('MAX_ITERATION', self.max_iter, None, None),
                      ('SGD_EXIT_THRESHOLD', None, self.sgd_tol, None),
                      ('SGD_EXIT_INTERVAL', None, self.sgd_exit_interval, None),
                      ('TIMEOUT', self.timeout, None, None),
                      ('PARAM_SEARCH_STRATEGY', None, None, self.search_strategy),
                      ('RESAMPLING_METHOD', None, None, self.resampling_method),
                      ('EVALUATION_METRIC', None, None,
                       self.evaluation_metric.upper() if self.evaluation_metric is not None else None),
                      ('PROGRESS_INDICATOR_ID', None, None, self.progress_indicator_id),
                      ('LEARNING_RATE', None, self.learning_rate, None),
                      ('THREAD_RATIO', None, self.thread_ratio, None),
                      ('REPEAT_TIMES', self.repeat_times, None, None),
                      ('INITIALIZATION', None, self.init, None),
                      ('METHOD', self.solver, None, None),
                      ('FOLD_NUM', self.fold_num, None, None),
                      ('HAS_ID', key is not None, None, None),
                      ('MIN_RESOURCE_RATE', None, self.min_resource_rate, None),
                      ('REDUCTION_RATE', None, self.reduction_rate, None),
                      ('AGGRESSIVE_ELIMINATION', self.aggressive_elimination, None, None)]
        if categorical_variable is not None:
            param_rows.extend(('CATEGORICAL_VARIABLE', None, None, variable)
                              for variable in categorical_variable)
        if usr_categorical_variable is not None:
            param_rows.extend(('USER_CATEGORICAL_VARIABLE', None, None, variable)
                              for variable in usr_categorical_variable)
        if item_categorical_variable is not None:
            param_rows.extend(('ITEM_CATEGORICAL_VARIABLE', None, None, variable)
                              for variable in item_categorical_variable)
        if self.solver in (1, 2):
            param_rows.extend([('MOMENTUM', None, self.momentum, None)])
        if self.search_strategy is not None:
            param_rows.extend([('RANDOM_SEARCH_TIMES', self.random_search_times, None, None)])
        if usr_cols is not None and usr_features is not None:
            param_rows.extend(('USER_EXCLUDED_FEATURE', None, None, exc)
                              for exc in usr_cols)
        if item_cols is not None and item_features is not None:
            param_rows.extend(('ITEM_EXCLUDED_FEATURE', None, None, exc)
                              for exc in item_cols)
        if self.param_values is not None:
            for x in self.param_values:
                values = str(x[1]).replace('[', '{').replace(']', '}')
                param_rows.extend([(self.range_params_map[x[0]]+'_VALUES',
                                    None, None, values)])
        if self.param_range is not None:
            for x in self.param_range:
                range_ = str(x[1])
                if len(x[1]) == 2 and self.search_strategy == 'random':
                    range_ = range_.replace(',', ',,')
                param_rows.extend([(self.range_params_map[x[0]]+'_RANGE',
                                    None, None, range_)])

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        tables = ['MODEL_METADATA', 'MODEL', 'MODEL_FACTORS', 'ITERATION_INFORMATION', 'STATISTICS', 'OPTIMAL_PARAMETER']
        tables = ["#PAL_FRM_{}_TBL_{}_{}".format(tbl, self.id, unique_id) for tbl in tables]
        metadata_tbl, model_tbl, factors_tbl, iter_info_tbl, stat_tbl, optim_param_tbl = tables
        try:
            self._call_pal_auto(conn,
                                "PAL_FRM",
                                data_,
                                usr_info,
                                item_info,
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
        self.metadata_ = conn.table(metadata_tbl)
        self.model_ = [conn.table(model_tbl)]
        self.factors_ = conn.table(factors_tbl)
        self.optim_param_ = conn.table(optim_param_tbl)
        self.stats_ = conn.table(stat_tbl)
        self.statistics_ = self.stats_
        #pylint:disable=attribute-defined-outside-init
        self.iter_info_ = conn.table(iter_info_tbl)

        # for model storage, need to add self.metadata_ and self.factors for predict
        self.model_.append(self.metadata_)
        self.model_.append(self.factors_)
        return self

    def predict(self, data, usr_info, item_info, key=None, usr=None, item=None, features=None, thread_ratio=None):
        """
        Prediction for the input data with the trained FRM model.

        Parameters
        ----------
        data : DataFrame
            Data to be fit.
        usr_info : DataFrame
            User side features.
        item_info : DataFrame
            Item side features.

        key : str, optional
            Name of the ID column.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        usr : list of str, optional
            Name of the column containing user name or user ID.
            If not provided, it defaults to 1st non-ID column of ``data``.

        item : str, optional
            Name of the column containing item name or item ID.

            If not provided, it defaults to the 1st non-ID, non-usr column of ``data``.

        features : str or a list of str, optional
            Global side features column name in the training dataframe.

            Defaults to all non key, usr and item columns of ``data``.
        thread_ratio : float, optional
            Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
            Values outside the range will be ignored and this function heuristically determines the number of threads to use.

            Default to 0.

        Returns
        -------
        DataFrame
            Prediction result of FRM algorithm, structured as follows:

                - 1st column : Data ID
                - 2nd column : User name/ID
                - 3rd column : Item name/Id
                - 4th column : Predicted rating
        """
        conn = data.connection_context

        if getattr(self, 'model_') is None:
            raise FitIncompleteError()
        cols = data.columns
        index = data.index
        key = self._arg('key', key, str, required=not isinstance(index, str))
        if isinstance(index, str):
            if key is not None and index != key:
                msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(msg)
        key = index if key is None else key
        cols.remove(key)
        if usr is None:
            usr = cols[0]
        cols.remove(usr)
        if item is None:
            item = cols[-1]
        cols.remove(item)
        if features is not None:
            if isinstance(features, str):
                features = [features]
            try:
                features = self._arg('features', features, ListOfStrings)
            except:
                msg = ("'features' must be list of string or string.")
                logger.error(msg)
                raise TypeError(msg)
        else:
            features = cols
        data_ = data[[key] + [usr] + [item] + features]
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        result_tbl = '#PAL_FRM_RESULT_TBL_{}_{}'.format(self.id, unique_id)
        param_rows = [('THREAD_RATIO', None, thread_ratio, None)]

        self.metadata_ = self.model_[1]
        self.factors_ = self.model_[2]
        try:
            self._call_pal_auto(conn,
                                'PAL_FRM_PREDICT', data_, usr_info, item_info,
                                self.metadata_, self.model_[0], self.factors_,
                                ParameterTable().with_data(param_rows), result_tbl)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, result_tbl)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, result_tbl)
            raise
        return conn.table(result_tbl)

    def create_model_state(self, model=None, function=None,
                           pal_funcname='PAL_FRM',
                           state_description=None, force=False):
        r"""
        Create PAL model state.

        Parameters
        ----------
        model : DataFrame, optional
            Specify the model for AFL state.

            Defaults to self.model\_.

        function : str, optional
            Specify the function in the unified API.

            A placeholder parameter, not effective for FRM.

        pal_funcname : int or str, optional
            PAL function name.

            Defaults to 'PAL_FRM'.

        state_description : str, optional
            Description of the state as model container.

            Defaults to None.

        force : bool, optional
            If True it will delete the existing state.

            Defaults to False.
        """
        super()._create_model_state(model, function, pal_funcname, state_description, force)

    def set_model_state(self, state):
        """
        Set the model state by state information.

        Parameters
        ----------
        state: DataFrame or dict
            If state is DataFrame, it has the following structure:

                - NAME: VARCHAR(100), it mush have STATE_ID, HINT, HOST and PORT.
                - VALUE: VARCHAR(1000), the values according to NAME.

            If state is dict, the key must have STATE_ID, HINT, HOST and PORT.
        """
        super()._set_model_state(state)

    def delete_model_state(self, state=None):
        """
        Delete PAL model state.

        Parameters
        ----------
        state : DataFrame, optional
            Specified the state.

            Defaults to self.state.
        """
        super()._delete_model_state(state)

class _FFMBase(PALBase):
    """
    Base class for Field-Aware Factorization Machine of recommender system algorithms.
    """
    handle_missing_map = {'remove': 1, 'skip' : 1, 'replace' : 2, 'fill_zero' : 2}
    def __init__(self,
                 functionality=None,
                 ordering=None,
                 normalise=None,
                 include_linear=None,
                 include_constant=None,
                 early_stop=None,
                 random_state=None,
                 factor_num=None,
                 max_iter=None,
                 train_size=None,
                 learning_rate=None,
                 linear_lamb=None,
                 poly2_lamb=None,
                 tol=None,
                 exit_interval=None,
                 handle_missing=None):
        if not hasattr(self, 'hanaml_parameters'):
            setattr(self, 'hanaml_parameters', pal_param_register())
        super(_FFMBase, self).__init__()
        self.handle_missing = self._arg('handle_missing', handle_missing, self.handle_missing_map)
        if self.handle_missing is None:
            self.handle_missing = self.handle_missing_map['fill_zero']
        self.exit_interval = self._arg('exit_interval', exit_interval, int)
        self.tol = self._arg('tol', tol, float)
        self.learning_rate = self._arg('learning_rate', learning_rate, float)
        self.linear_lamb = self._arg('linear_lamb', linear_lamb, float)
        self.poly2_lamb = self._arg('poly2_lamb', poly2_lamb, float)
        self.random_state = self._arg('random_state', random_state, int)
        self.max_iter = self._arg('max_iter', max_iter, int)
        self.train_size = self._arg('train_size', train_size, float)
        self.factor_num = self._arg('factor_num', factor_num, int)
        self.normalise = self._arg('normalise', normalise, bool)
        self.ordering = self._arg('ordering', ordering, ListOfStrings)
        if ordering is not None:
            self.ordering = (', ').join(self.ordering)
        self.include_linear = self._arg('include_linear', include_linear, bool)
        self.early_stop = self._arg('early_stop', early_stop, bool)
        self.include_constant = self._arg('include_constant', include_constant, bool)
        self.functionality = self._arg('functionality', functionality, str)
        self.model_ = None

    def _fit(self, data, key=None, features=None, label=None, categorical_variable=None, delimiter=None):
        conn = data.connection_context
        require_pal_usable(conn)
        delimiter = self._arg('delimiter', delimiter, str)
        label = self._arg('label', label, str)
        if categorical_variable is not None:
            if isinstance(categorical_variable, str):
                categorical_variable = [categorical_variable]
            try:
                categorical_variable = self._arg('categorical_variable', categorical_variable, ListOfStrings)
            except:
                msg = ("`categorical_variable` must be list of string or string.")
                logger.error(msg)
                raise TypeError(msg)
        cols = data.columns
        key = self._arg('key', key, str)
        index = data.index
        if isinstance(index, str):
            if key is not None and index != key:
                msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(msg)
        key = index if key is None else key
        if key is not None:
            cols.remove(key)
        de_col = label if label is not None else cols[-1]
        de_col_dtype = {tp[0]:(tp[0:3]) for tp in data.dtypes()}[de_col][1]
        de_col_is_categorical = False
        if categorical_variable is not None:
            if de_col in categorical_variable:
                de_col_is_categorical = True

        if not(de_col_dtype in ('INT', 'FLOAT', 'DOUBLE') and not de_col_is_categorical) and self.functionality == 'regression':
            msg = ("Cannot do regression when response is not numeric.")
            logger.error(msg)
            raise ValueError(msg)
        if de_col_dtype in ('DOUBLE', 'FLOAT') and self.functionality == 'ranking':
            msg = ("Cannot do ranking when response is of double type.")
            logger.error(msg)
            raise ValueError(msg)

        cols.remove(de_col)
        if features is not None:
            if isinstance(features, str):
                features = [features]
            try:
                features = self._arg('features', features, ListOfStrings)
            except:
                msg = ("`features` must be list of string or string.")
                logger.error(msg)
                raise TypeError(msg)
        else:
            features = cols
        key_col = [] if key is None else [key]
        data_ = data[key_col + features + [de_col]]
        param_rows = [('HAS_ID', key is not None, None, None),
                      ('DELIMITER', None, None, delimiter),
                      ('TASK', None, None, self.functionality),
                      ('DEPENDENT_VARIABLE', None, None, label),
                      ('NORMALISE', self.normalise, None, None),
                      ('ORDERING', None, None, self.ordering),
                      ('INCLUDE_CONSTANT', self.include_constant, None, None),
                      ('INCLUDE_LINEAR', self.include_linear, None, None),
                      ('EARLY_STOP', self.early_stop, None, None),
                      ('SEED', self.random_state, None, None),
                      ('K_NUM', self.factor_num, None, None),
                      ('MAX_ITERATION', self.max_iter, None, None),
                      ('TRAIN_RATIO', None, self.train_size, None),
                      ('LEARNING_RATE', None, self.learning_rate, None),
                      ('LINEAR_LAMBDA', None, self.linear_lamb, None),
                      ('POLY2_LAMBDA', None, self.poly2_lamb, None),
                      ('CONVERGENCE_CRITERION', None, self.tol, None),
                      ('CONVERGENCE_INTERVAL', self.exit_interval, None, None),
                      ('HANDLE_MISSING', self.handle_missing, None, None)]
        if categorical_variable is not None:
            param_rows.extend(('CATEGORICAL_VARIABLE', None, None, variable)
                              for variable in categorical_variable)
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        tables = ['META', 'COEFFICIENT', 'STATISTICS', 'CROSS_VALIDATION']
        tables = ["#PAL_FFM_{}_TBL_{}_{}".format(tbl, self.id, unique_id) for tbl in tables]
        meta_tbl, coef_tbl, stat_tbl, cross_valid_tbl = tables
        try:
            self._call_pal_auto(conn,
                                "PAL_FFM",
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
        self.meta_ = conn.table(meta_tbl)
        self.coef_ = conn.table(coef_tbl)
        self.stats_ = conn.table(stat_tbl)
        self.statistics_ = self.stats_
        #pylint:disable=attribute-defined-outside-init
        self.cross_valid_ = conn.table(cross_valid_tbl)
        self.model_ = [self.meta_, self.coef_]

    def _predict(self, data, key=None, features=None, thread_ratio=None, handle_missing=None):
        conn = data.connection_context

        if getattr(self, 'model_') is None:
            raise FitIncompleteError()
        self.handle_missing = self._arg('handle_missing', handle_missing, self.handle_missing_map)
        if self.handle_missing is None:
            self.handle_missing = self.handle_missing_map['fill_zero']
        cols = data.columns
        index = data.index
        key = self._arg('key', key, str, required=not isinstance(index, str))
        if isinstance(index, str):
            if key is not None and index != key:
                msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(msg)
        key = index if key is None else key
        cols.remove(key)
        if features is not None:
            if isinstance(features, str):
                features = [features]
            try:
                features = self._arg('features', features, ListOfStrings)
            except:
                msg = ("`features` must be list of string or string.")
                logger.error(msg)
                raise TypeError(msg)
        else:
            features = cols
        data_ = data[[key] + features]
        param_rows = [('THREAD_RATIO', None, thread_ratio, None),
                      ('HANDLE_MISSING', self.handle_missing, None, None)]
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        res_tbl = "#PAL_FFM_RESULT_TBL_{}_{}".format(self.id, unique_id)
        try:
            self._call_pal_auto(conn,
                                'PAL_FFM_PREDICT',
                                data_,
                                self.model_[0],
                                self.model_[1],
                                ParameterTable().with_data(param_rows),
                                res_tbl)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, res_tbl)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, res_tbl)
            raise
        return conn.table(res_tbl)

class FFMClassifier(_FFMBase):
    r"""
    Field-Aware Factorization Machine with the task of classification.

    Parameters
    ----------
    factor_num : int, optional
        The factorization dimensionality.
        Default to 4.
    random_state : int, optional
        Specifies the seed for random number generator.

          - 0: Uses the current time as the seed.
          - Others: Uses the specified value as the seed.

        Default to 0.
    train_size : float, optional
        The proportion of dataset used for training, and the remaining dataset for validation.

        For example, 0.8 indicates that 80% for training, and the remaining 20% for validation.

        Default to 0.8 if number of instances not less than 40, 1.0 otherwise.
    max_iter : int, optional
        Specifies the maximum number of iterations for the alternative least square algorithm.

        Default to 20
    ordering : a list of str, optional(deprecated)
        Specifies the categories orders for ranking.

        This parameter is meaningless for classification problems and will be
        removed in future release.

        No default value.
    normalise : bool, optional
        Specifies whether to normalize each instance so that its L1 norm is 1.

        Default to True.
    include_constant : bool, optional
        Specifies whether to include the w0 constant part.

        Default to True.
    include_linear : bool, optional
        Specifies whether to include the linear part of regression model.

        Default to True.
    early_stop : bool, optional
        Specifies whether to early stop the SGD optimization.

        Valid only if the value of ``thread_ratio`` is less than 1.

        Default to True.
    learning_rate : float, optional
        The learning rate for SGD iteration.

        Default to 0.2.
    linear_lamb : float, optional
        The L2 regularization parameter for the linear coefficient vector.

        Default to 1e-5.
    poly2_lamb : float, optional
        The L2 regularization parameter for factorized coefficient matrix of the quadratic term.

        Default to 1e-5.
    tol : float, optional
        The criterion to determine the convergence of SGD.

        Default to 1e-5.
    exit_interval : int, optional
        The interval of two iterations for comparison to determine the convergence.

        Default to 5.
    handle_missing : str, optional
        Specifies how to handle missing feature:

            - 'skip': skip rows with missing values.
            - 'fill_zero': replace missing values with 0.

        Default to 'fill_zero'.

    Attributes
    ----------
    meta_ : DataFrame
        Model metadata content.
    coef_ : DataFrame
        DataFrame that provides the following information:
            - Feature name,
            - Field name,
            - The factorization number,
            - The parameter value.
    stats_ : DataFrame
        Statistics.
    cross_valid_ : DataFrame
        Cross validation content.

    Examples
    --------
    Input DataFrame:

    >>> df_train_classification.collect()
       USER                   MOVIE  TIMESTAMP        CTR
    0     A                  Movie1        3.0      Click
    1     A                  Movie2        3.0      Click
    ...
    35    E                  Movie7        4.0  Not click
    36    E                  Movie8        3.0  Not click

    Create a FFMClassifier instance:

    >>> ffm = FFMClassifier(linear_lamb=1e-5,
                            poly2_lamb=1e-6,
                            random_state=1,
                            factor_num=4,
                            early_stop=1,
                            learning_rate=0.2,
                            max_iter=20,
                            train_size=0.8)

    Perform fit():

    >>> ffm.fit(data=df_train_classification,
                categorical_variable='TIMESTAMP')
    >>> ffm.stats_.collect()
         STAT_NAME          STAT_VALUE
    0         task      classification
    1  feature_num                  18
    2    field_num                   3
    3        k_num                   4
    4     category    Click, Not click
    5         iter                   3
    6      tr-loss  0.6409316561278655
    7      va-loss  0.7452354780967997

    Perform predict():

    >>> res = ffm.predict(data=df_predict,
                          key='ID',
                          thread_ratio=1)
    >>> res.collect()
       ID      SCORE  CONFIDENCE
    0   1  Not click    0.543537
    1   2  Not click    0.545470
    ...
    7   8  Not click    0.536781
    8   9  Not click    0.635412
    """
    def __init__(self, ordering=None, normalise=None, include_linear=None, include_constant=None,
                 early_stop=None, random_state=None, factor_num=None, max_iter=None, train_size=None, learning_rate=None,
                 linear_lamb=None, poly2_lamb=None, tol=None, exit_interval=None, handle_missing=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(FFMClassifier, self).__init__(functionality='classification',
                                            ordering=ordering,
                                            normalise=normalise,
                                            include_linear=include_linear,
                                            include_constant=include_constant,
                                            early_stop=early_stop,
                                            random_state=random_state,
                                            factor_num=factor_num,
                                            max_iter=max_iter,
                                            train_size=train_size,
                                            learning_rate=learning_rate,
                                            linear_lamb=linear_lamb,
                                            poly2_lamb=poly2_lamb,
                                            tol=tol,
                                            exit_interval=exit_interval,
                                            handle_missing=handle_missing)

    def fit(self, data, key=None, features=None, label=None, categorical_variable=None, delimiter=None):
        """
        Fit the model to the training dataset.

        Parameters
        ----------
        data : DataFrame
            Data to be fit.
        key : str, optional
            Name of the ID column.

            If ``key`` is not provided, then:

                - if ``data`` is indexed by a single column, then ``key`` defaults
                  to that index column;
                - otherwise, it is assumed that ``data`` contains no ID column.

        features : str or a list of str optional
            Name of the feature columns.
        delimiter : str, optional
            The delimiter to separate string features.

            For example, "China, USA" indicates two feature values "China" and "USA".

            Default to ','.
        label : str, optional
            Specifies the dependent variable.

            For classification, the label column can be any kind of data type.

            Default to last column name.
        categorical_variable : str or a list of str optional
            Indicates whether or not a column data is actually corresponding
            to a category variable even the data type of this column is INTEGER.

            By default, 'VARCHAR' or 'NVARCHAR' is category variable, and 'INTEGER'
            or 'DOUBLE' is continuous variable.

        Returns
        -------
        A fitted object of class "FFMClassifier".

        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        self._fit(data, key, features, label, categorical_variable, delimiter)
        return self

    def predict(self, data, key=None, features=None, thread_ratio=None, handle_missing=None):
        """
        Prediction for the input data with the trained FFMClassifier model.

        Parameters
        ----------
        data : DataFrame
            Data to be fit.
        key : str, optional
            Name of the ID column.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        features : str or a list of str optional
            Global side features column name in the training dataframe.
        thread_ratio : float, optional
            Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
            Values outside the range will be ignored and this function heuristically determines the number of threads to use.

            Default to -1.
        handle_missing : str, optional
            Specifies how to handle missing feature:

                - 'skip': skip rows with missing values.
                - 'fill_zero': replace missing values with 0.

            Default to 'fill_zero'.

        Returns
        -------
        DataFrame
            Prediction result, structured as follows:

              - 1st column : ID
              - 2nd column : SCORE, i.e. predicted class labels
              - 3rd column : CONFIDENCE, the confidence for assigning class labels.
        """
        pred_res = super(FFMClassifier, self)._predict(data, key, features, thread_ratio, handle_missing)
        return pred_res

class FFMRegressor(_FFMBase):
    r"""
    Field-Aware Factorization Machine with the task of Regression.

    Parameters
    ----------

    factor_num : int, optional
        The factorization dimensionality.
        Default to 4.
    random_state : int, optional
        Specifies the seed for random number generator.

        -   0: Uses the current time as the seed.
        -   Others: Uses the specified value as the seed.

        Default to 0.
    train_size : float, optional
        The proportion of data used for training, and the remaining dataset for validation.

        For example, 0.8 indicates that 80% for training, and the remaining 20% for validation.

        Default to 0.8 if number of instances not less than 40, 1.0 otherwise.
    max_iter : int, optional
        Specifies the maximum number of iterations for the ALS algorithm.

        Default to 20
    ordering : a list of str, optional(deprecated)
        Specifies the categories orders for ranking.

        This parameter is meaningless for regression problems and will be
        removed in future release.

        No default value.
    normalise : bool, optional
        Specifies whether to normalize each instance so that its L1 norm is 1.

        Default to True.
    include_constant : bool, optional
        Specifies whether to include the constant part.

        Default to True.
    include_linear : bool, optional
        Specifies whether to include the linear part of the model.

        Default to True.
    early_stop : bool, optional
        Specifies whether to early stop the SGD optimization.

        Valid only if the value of ``train_size`` is less than 1.

        Default to True.
    learning_rate : float, optional
        The learning rate for SGD iteration.

        Default to 0.2.
    linear_lamb : float, optional
        The L2 regularization parameter for the linear coefficient vector.

        Default to 1e-5.
    poly2_lamb : float, optional
        The L2 regularization parameter for factorized coefficient matrix of the quadratic term.

        Default to 1e-5.

    tol : float, optional
        The criterion to determine the convergence of SGD.

        Default to 1e-5.
    exit_interval : int, optional
        The interval of two iterations for comparison to determine the convergence.

        Default to 5.
    handle_missing : str, optional
        Specifies how to handle missing feature:

            -   'skip': remove rows with missing values.
            -   'fill_zero': replace missing values with 0.

        Default to 'fill_zero'.

    Attributes
    ----------
    meta_ : DataFrame
        Model metadata content.
    coef_ : DataFrame
        The DataFrame inclusive of the following information:
            - Feature name,
            - Field name,
            - The factorization number,
            - The parameter value.
    stats_ : DataFrame
        Statistics.
    cross_valid_ : DataFrame
        Cross validation content.

    Examples
    --------
    Input DataFrame:

    >>> df_train_regression.collect()
       USER                   MOVIE  TIMESTAMP  CTR
    0     A                  Movie1        3.0    0
    1     A                  Movie2        3.0    5
    ...
    35    E                  Movie7        4.0    2
    36    E                  Movie8        3.0    0

    Create a FFMRegressor instance:

    >>> ffm = FFMRegressor(factor_num=4, early_stop=True, learning_rate=0.2, max_iter=20, train_size=0.8,
                            linear_lamb=1e-5, poly2_lamb=1e-6, random_state=1)

    Perform fit():

    >>> ffm.fit(data=df_train_regression, categorical_variable='TIMESTAMP')

    >>> ffm.stats_.collect
         STAT_NAME          STAT_VALUE
    0         task          regression
    1  feature_num                  18
    2    field_num                   3
    3        k_num                   4
    4         iter                  15
    5      tr-loss  0.4503367758101421
    6      va-loss  1.6896813062750056

    Perform predict():

    >>> res = ffm.predict(data=df_predict, key='ID', thread_ratio=1)

    >>> res.collect()
       ID                SCORE CONFIDENCE
    0   1    2.978197866860172       None
    1   2  0.43883354766746385       None
    ...
    7   8   0.8713338730015039       None
    8   9    2.347070689885986       None
    """
    def __init__(self, ordering=None, normalise=None, include_linear=None, include_constant=None,
                 early_stop=None, random_state=None, factor_num=None, max_iter=None, train_size=None, learning_rate=None,
                 linear_lamb=None, poly2_lamb=None, tol=None, exit_interval=None, handle_missing=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(FFMRegressor, self).__init__(functionality='regression',
                                           ordering=ordering,
                                           normalise=normalise,
                                           include_linear=include_linear,
                                           include_constant=include_constant,
                                           early_stop=early_stop,
                                           random_state=random_state,
                                           factor_num=factor_num,
                                           max_iter=max_iter,
                                           train_size=train_size,
                                           learning_rate=learning_rate,
                                           linear_lamb=linear_lamb,
                                           poly2_lamb=poly2_lamb,
                                           tol=tol,
                                           exit_interval=exit_interval,
                                           handle_missing=handle_missing)

    def fit(self, data, key=None, features=None, label=None, categorical_variable=None, delimiter=None):
        """
        Fit the model to the training dataset.

        Parameters
        ----------
        data : DataFrame
            Data to be fit.
        key : str, optional
            Name of the ID column.

            If ``key`` is not provided, then:

                - if ``data`` is indexed by a single column, then ``key`` defaults
                  to that index column;
                - otherwise, it is assumed that ``data`` contains no ID column.

        features : str or a list of str optional
            Name of the feature columns.
        delimiter : str, optional
            The delimiter to separate string features.

            For example, "China, USA" indicates two feature values "China" and "USA".

            Default to ','.
        label : str, optional
            Specifies the dependent variable.

            For regression, the label column must have numerical data type.

            Default to last column name.
        categorical_variable : str or a list of str optional
            Indicates whether or not a column data is actually corresponding
            to a category variable even the data type of this column is INTEGER.

            By default, 'VARCHAR' or 'NVARCHAR' is category variable, and 'INTEGER'
            or 'DOUBLE' is continuous variable.

        Returns
        -------
        A fitted object of class "FFMRegressor".
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        self._fit(data, key, features, label, categorical_variable, delimiter)
        return self

    def predict(self, data, key=None, features=None, thread_ratio=None, handle_missing=None):
        """
        Prediction for the input data with the trained FFMRegressor model.

        Parameters
        ----------
        data : DataFrame
            Data to be fit.
        key : str, optional
            Name of the ID column.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        features : str or a list of str optional
            Global side features column name in the training dataframe.
        thread_ratio : float, optional
            Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
            Values outside the range will be ignored and this function heuristically determines the number of threads to use.

            Default to -1.
        handle_missing : {'skip', 'fill_zero'}, optional
            Specifies how to handle missing feature:

              - 'skip': remove rows with missing values.
              - 'fill_zero': replace missing values with 0.

            Default to 'fill_zero'.

        Returns
        -------
        DataFrame
            Prediction result, structured as follows:

              - 1st column : ID
              - 2nd column : SCORE, i.e. predicted values
              - 3rd column : CONFIDENCE, all NULLs.
        """
        pred_res = super(FFMRegressor, self)._predict(data, key, features, thread_ratio, handle_missing)
        return pred_res

class FFMRanker(_FFMBase):
    r"""
    Field-Aware Factorization Machine with the task of ranking using ordinal regression.

    Parameters
    ----------

    factor_num : int, optional
        The factorization dimensionality.

        Default to 4.
    random_state : int, optional
        Specifies the seed for random number generator.

          - 0: Uses the current time as the seed.
          - Others: Uses the specified value as the seed.

        Default to 0.
    train_size : float, optional
        The proportion of data used for training, and the remaining dataset for validation.

        For example, 0.8 indicates that 80% for training, and the remaining 20% for validation.

        Default to 0.8 if number of instances not less than 40, 1.0 otherwise.
    max_iter : int, optional
        Specifies the maximum number of iterations for the ALS algorithm.

        Default to 20.
    ordering : a list of str, optional
        Specifies the categories orders(in ascending) for ranking.

        No default value.
    normalise : bool, optional
        Specifies whether to normalize each instance so that its L1 norm is 1.

        Default to True.
    include_linear : bool, optional
        Specifies whether to include the the linear part of the model.

        Default to True.
    early_stop : bool, optional
        Specifies whether to early stop the SGD optimization.

        Valid only if the value of ``train_size`` is less than 1.

        Default to True.
    learning_rate : float, optional
        The learning rate for SGD iteration.

        Default to 0.2.
    linear_lamb : float, optional
        The L2 regularization parameter for the linear coefficient vector.

        Default to 1e-5.
    poly2_lamb : float, optional
        The L2 regularization parameter for factorized coefficient matrix of the quadratic term.

        Default to 1e-5.
    tol : float, optional
        The criterion to determine the convergence of SGD.

        Default to 1e-5.
    exit_interval : int, optional
        The interval of two iterations for comparison to determine the convergence.

        Default to 5.
    handle_missing : {'skip', 'fill_zero'}, optional
        Specifies how to handle missing feature:

          - 'skip': remove rows with missing values.
          - 'fill_zero': replace missing values with 0.

        Default to 'fill_zero'.

    Attributes
    ----------
    meta_ : DataFrame
        Model metadata content.
    coef_ : DataFrame
        The DataFrame inclusive of the following information:
            - Feature name,
            - Field name,
            - The factorization number,
            - The parameter value.
    stats_ : DataFrame
        Statistics.
    cross_valid_ : DataFrame
        Cross validation content.

    Examples
    --------
    Input DataFrame df_train_ranker:

    >>> df_train_ranker.collect()
       USER                   MOVIE  TIMESTAMP       CTR
    0     A                  Movie1        3.0    medium
    1     A                  Movie2        3.0  too high
    ...
    35    E                  Movie7        4.0       low
    36    E                  Movie8        3.0   too low

    Create a FFMRanker instance:

    >>> ffm = FFMRanker(ordering=['too low', 'low', 'medium', 'high', 'too high'],
                        factor_num=4, early_stop=True, learning_rate=0.2, max_iter=20, train_size=0.8,
                        linear_lamb=1e-5, poly2_lamb=1e-6, random_state=1)

    Perform fit():

    >>> ffm.fit(data=df_train_rank, categorical_variable='TIMESTAMP')
    >>> ffm.stats_.collect()
         STAT_NAME                            STAT_VALUE
    0         task                               ranking
    ...
    6      tr-loss                    1.3432013591533276
    7      va-loss                    1.5509792122994928

    Perform predict():

    >>> res = ffm.predict(data=df_predict, key='ID', thread_ratio=1)

    >>> res.collect()
       ID     SCORE  CONFIDENCE
    0   1      high    0.294206
    1   2    medium    0.209893
    ...
    8   9      high    0.282633
    """
    def __init__(self, ordering=None, normalise=None, include_linear=None,
                 early_stop=None, random_state=None, factor_num=None, max_iter=None, train_size=None, learning_rate=None,
                 linear_lamb=None, poly2_lamb=None, tol=None, exit_interval=None, handle_missing=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(FFMRanker, self).__init__(functionality='ranking',
                                        ordering=ordering,
                                        normalise=normalise,
                                        include_linear=include_linear,
                                        include_constant=True,
                                        early_stop=early_stop,
                                        random_state=random_state,
                                        factor_num=factor_num,
                                        max_iter=max_iter,
                                        train_size=train_size,
                                        learning_rate=learning_rate,
                                        linear_lamb=linear_lamb,
                                        poly2_lamb=poly2_lamb,
                                        tol=tol,
                                        exit_interval=exit_interval,
                                        handle_missing=handle_missing)

    def fit(self, data, key=None, features=None, label=None, categorical_variable=None, delimiter=None):
        """
        Fit the model to the training dataset.

        Parameters
        ----------
        data : DataFrame
            Data to be fit.
        key : str, optional
            Name of the ID column.

            If ``key`` is not provided, then:

                - if ``data`` is indexed by a single column, then ``key`` defaults
                  to that index column;
                - otherwise, it is assumed that ``data`` contains no ID column.

        features : str or a list of str optional
            Name of the feature columns.
        delimiter : str, optional
            The delimiter to separate string features.

            For example, "China, USA" indicates two feature values "China" and "USA".

            Default to ','.
        label : str, optional
            Specifies the dependent variable.

            For ranking, the label column must have categorical data type.

            Default to last column name.
        categorical_variable : str or a list of str optional
            Indicates whether or not a column data is actually corresponding
            to a category variable even the data type of this column is INTEGER.

            By default, 'VARCHAR' or 'NVARCHAR' is category variable, and 'INTEGER'
            or 'DOUBLE' is continuous variable.

        Returns
        -------
        A fitted object of class "FFMRanker".
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        self._fit(data, key, features, label, categorical_variable, delimiter)
        return self

    def predict(self, data, key=None, features=None, thread_ratio=None, handle_missing=None):
        """
        Prediction for the input data with the trained FFMRanker model.

        Parameters
        ----------
        data : DataFrame
            Data to be fit.
        key : str, optional
            Name of the ID column.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        features : str or a list of str optional
            Global side features column name in the training dataframe.
        thread_ratio : float, optional
            Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
            Values outside the range will be ignored and this function heuristically determines the number of threads to use.

            Default to -1.
        handle_missing : str, optional
            Specifies how to handle missing feature:

              - 'skip': remove rows with missing values.
              - 'fill_zero': replace missing values with 0.

            Default to 'fill_zero'.

        Returns
        -------
        DataFrame
            Prediction result, structured as follows:

               - 1st column : ID
               - 2nd column : SCORE, i.e. predicted ranking
               - 3rd column : CONFIDENCE, the confidence for ranking.
        """
        pred_res = super(FFMRanker, self)._predict(data, key, features, thread_ratio, handle_missing)
        return pred_res

class MLPRecommender(PALBase):
    r"""
    The python interface for an MLP-based recommender system method in PAL.
    Current implementation only supports binary-classification task.

    Parameters
    ----------
    batch_size : int, optional
        Specifies the number of training samples in a batch.
        Defaults to 16.

    num_epochs : int, optional
        Specifies the number of training epochs.

        Defaults to 1.

    num_heads : int, optional
        Specifies the number of heads used in bilinear interaction aggregation layer.

        When the hidden dimension of the MLP(s) is large, we can use multiple heads to
        reduce the matrix computation time

        Defaults to 1(i.e. single head).

    embedding_dim : int, optional
        Specifies the embedding size of each feature vector.

        Defaults to 10.

    use_feature_selection : int, optional
        Specifies whether or not to use feature selection.

        - 0: Not use
        - 1: Use

        Defaults to 1

    learning_rate : int, optional
        Specifies the learning rate for batch training.

        Defaults to 0.0005.

    mlp1_dropout_prob : float, optional
        Specifies the dropout probability for MLP1.

        Defaults to 0.0.

    mlp2_dropout_prob : float, optional
        Specifies the dropout probability for MLP2.

        Defaults to 0.0.

    mlp1_hidden_dim : list of int, optional
        Specifies the sizes of hidden-layers for MLP1.

        Defaults to [64, 32].

    mlp2_hidden_dim : list of int, optional
        Specifis the sizes of hidden-layers for MLP2.

        Defaults to [64, 32].
    fs_hidden_dim : list of int, optional
        Specifies the size of the hidden-layers for the feature selection module.

        Defaults to [64].

    random_state : int, optional
        Specifies the seed for (pseudo-)random number generation.

        - 0 : current system time.
        - others : the real seed

        Defaults to 0.
    task : {'classification', 'regression'}, optional
        Specifies the task for the recommender system.

        It can be either 'classification' or 'regression'.

        Defaults to 'classification'.

    Attributes
    ----------
    model_ : DataFrame
        Model content.

    train_log_ : DataFrame
        Logs of the training process.

    Examples
    --------
    >>> data.head(5).collect()
        ID   user   item  daytime  weekday  isweekend  homework  cost  weather  country  city  label
    0    0    451   4149     5041     5046       5053      5055  5058     5060     5069  5149      0
    1    1     91   3503     5041     5047       5053      5056  5058     5065     5095  5149      0
    2    2    168    983     5040     5050       5054      5055  5058     5060     5069  5207      1
    3    3    620   1743     5045     5051       5054      5055  5058     5061     5073  5149      0
    4    4     46   2692     5040     5049       5054      5055  5058     5060     5086  5211      0

    Set up the basic structure of the MLPs and interaction/merging layers, then train an MLP recommender model
    using the input data illustrated as above:

    >>> mr = MLPRecommender(batch_size=10,
                            num_epochs=20,
                            random_seed=2023,
                            num_heads=5,
                            embedding_dim=10,
                            use_feature_selection=True,
                            learning_rate=0.01,
                            mlp1_dropout_prob=0.1,
                            mlp2_dropout_prob=0.1,
                            mlp1_hidden_dim=[256, 128, 96],
                            mlp2_hidden_dim=[256, 128, 96],
                            fs_hidden_dim=[256, 128, 96])
    >>> mr.fit(data, key='ID', label='label',
               selected_feature_set1=['user', 'homework'],
               selected_feature_set2=['item', 'daytime'])
    >>> mr.train_log_.filter('BATCH=\'epoch average loss\'').head(5).collect()
       EPOCH               BATCH      LOSS
    0      0  epoch average loss  0.882001
    1      1  epoch average loss  0.676835
    2      2  epoch average loss  0.827260
    3      3  epoch average loss  0.634558
    4      4  epoch average loss  0.228178
    """
    def __init__(self,
                 batch_size=None,
                 num_epochs=None,
                 num_heads=None,
                 embedding_dim=None,
                 use_feature_selection=None,
                 learning_rate=None,
                 mlp1_dropout_prob=None,
                 mlp2_dropout_prob=None,
                 mlp1_hidden_dim=None,
                 mlp2_hidden_dim=None,
                 fs_hidden_dim=None,
                 random_state=None,
                 task=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(MLPRecommender, self).__init__()
        self.batch_size = self._arg('batch_size', batch_size, int)
        self.num_epochs = self._arg('num_epochs', num_epochs, int)
        self.num_heads  = self._arg('num_heads', num_heads, int)
        self.embedding_dim = self._arg('embedding_dim', embedding_dim, int)
        self.use_feature_selection = self._arg('use_feature_selection',
                                               use_feature_selection, int)
        self.learning_rate = self._arg('learning_rate', learning_rate, float)
        self.mlp1_dropout_prob = self._arg('mlp1_dropout_prob',
                                           mlp1_dropout_prob, float)
        self.mlp2_dropout_prob = self._arg('mlp2_dropout_prob',
                                           mlp2_dropout_prob, float)
        self.random_state = self._arg('random_state', random_state, int)
        if isinstance(mlp1_hidden_dim, int):
            mlp1_hidden_dim = [mlp1_hidden_dim]
        self.mlp1_hidden_dim = self._arg('mlp1_hidden_dim',
                                         mlp1_hidden_dim, list)
        if self.mlp1_hidden_dim is not None:
            for idx, val in enumerate(self.mlp1_hidden_dim):
                self.mlp1_hidden_dim[idx] = self._arg(f'The element of mlp1_hidden_dim with index {idx}',
                                                      val, int)
        if isinstance(mlp2_hidden_dim, int):
            mlp2_hidden_dim = [mlp2_hidden_dim]
        self.mlp2_hidden_dim = self._arg('mlp2_hidden_dim',
                                         mlp2_hidden_dim, list)
        if self.mlp2_hidden_dim is not None:
            for idx, val in enumerate(self.mlp2_hidden_dim):
                self.mlp2_hidden_dim[idx] = self._arg(f'The element of mlp2_hidden_dim with index {idx}',
                                                      val, int)
        if isinstance(fs_hidden_dim, int):
            fs_hidden_dim = [fs_hidden_dim]
        self.fs_hidden_dim = self._arg('fs_hidden_dim', fs_hidden_dim, list)
        if self.fs_hidden_dim is not None:
            for idx, val in enumerate(self.fs_hidden_dim):
                self.fs_hidden_dim[idx] = self._arg(f'The element of fs_hidden_dim with index {idx}',
                                                    val, int)
        self.task = self._arg('task', task, {'classification':0, 'regression':1})

    @trace_sql
    def fit(self, data, key=None, label=None,
            selected_feature_set1=None,
            selected_feature_set2=None):
        r"""
        Given the training dataset, train a MLP recommender model that is mainly consisted of
        two MLPs , namely MLP1 and MLP2, with biliear fusion and (possibly) feature selection modules.

        Parameters
        ----------
        data : DataFrame
            Training data.
        key : str, optional
            Name of the ID column.

            If ``key`` is not provided, then:

                - if ``data`` is indexed by a single column, then ``key`` defaults
                  to that index column;

                - otherwise, it defaults to the 1st column of ``data``.

        label : str, optional
            Name of the dependent variable.

            Defaults to the last column of ``data``(excluding the ID column specified by ``key``).

        selected_feature_set1 : list of str, optional
            Selected features of ``data`` that are fed into MLP1.

            Defaults to None(equivalent to []).

        selected_feature_set2 : list of str, optional
            Selected features of ``data`` that are fed into MLP2.

            Defaults to None(equivalent to []).

        Returns
        -------
        A fitted object of class "MLPRecommender".
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        conn = data.connection_context
        require_pal_usable(conn)
        selected_feature_set1 = self._arg('selected_feature_set1',
                                          selected_feature_set1,
                                          ListOfStrings)
        selected_feature_set2 = self._arg('selected_feature_set2',
                                          selected_feature_set2,
                                          ListOfStrings)
        key = self._arg('key', key, str)
        label = self._arg('label', label, str)
        cols = data.columns
        cols_default = True#data columns with default setting or not
        if label is not None:
            cols.remove(label)
            cols_default = False
        if key is not None:
            cols.remove(key)
            cols_default = False
        data_ = data if cols_default else data[([key] if key is not None else []) +\
        cols + ([label] if label is not None else [])]

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        train_log_tbl, model_tbl = [ '#MLP_RECOMMENDER_{}_TBL_{}_{}'.format(tab,
                                                                            self.id,
                                                                            unique_id) \
        for tab in ['TRAIN_LOG', 'MODEL']]
        outputs = [train_log_tbl, model_tbl]
        mlp1_hidden_dim = None if self.mlp1_hidden_dim is None else ', '.join(str(x) for x in self.mlp1_hidden_dim)#pylint:disable=line-too-long
        mlp2_hidden_dim = None if self.mlp2_hidden_dim is None else ', '.join(str(x) for x in self.mlp2_hidden_dim)#pylint:disable=line-too-long
        fs_hidden_dim = None if self.fs_hidden_dim is None else ', '.join(str(x) for x in self.fs_hidden_dim)
        param_rows = [('BATCH_SIZE', self.batch_size, None, None),
                      ('NUM_EPOCHS', self.num_epochs, None, None),
                      ('NUM_HEADS', self.num_heads, None, None),
                      ('EMBEDDING_DIM', self.embedding_dim, None, None),
                      ('USE_FEATURE_SELECTION', self.use_feature_selection,
                       None, None),
                      ('LEARNING_RATE', None, self.learning_rate, None),
                      ('MLP1_DROPOUT_PROB', None, self.mlp1_dropout_prob, None),
                      ('MLP2_DROPOUT_PROB', None, self.mlp2_dropout_prob, None),
                      ('RANDOM_SEED', self.random_state, None, None),
                      ('MLP1_HIDDEN_DIM', None, None, mlp1_hidden_dim),
                      ('MLP2_HIDDEN_DIM', None, None, mlp2_hidden_dim),
                      ('FS_HIDDEN_DIM', None, None, fs_hidden_dim),
                      ('SELECTED_FEATURE_SET1', None, None,
                       '' if selected_feature_set1 is None else \
                       ', '.join(selected_feature_set1)),
                      ('SELECTED_FEATURE_SET2', None, None,
                       '' if selected_feature_set2 is None else \
                       ', '.join(selected_feature_set2)),
                      ('TASK', self.task, None, None)]
        if not (check_pal_function_exist(conn, '%MLP_RECOMMENDER%', like=True) or \
        self._disable_hana_execution):
            msg = 'The version of your SAP HANA does not support MLP Recommender!'
            logger.error(msg)
            raise ValueError(msg)
        try:
            self._call_pal_auto(conn,
                                'PAL_MLP_RECOMMENDER_TRAIN',
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
        # pylint: disable=attribute-defined-outside-init
        if not self._disable_hana_execution:
            self.train_log_ = conn.table(train_log_tbl)
            self.model_ = conn.table(model_tbl)
        return self

    @trace_sql
    def predict(self, data, key=None):
        r"""
        Predict target values using a trained model.

        Parameters
        ----------
        data : DataFrame
            Input data for prediction.

        key : str, optional
            Specifies name of the ID column in ``data``.

            Defaults to the index of ``data`` if ``data`` is indexed by a single column,
            otherwise it must be provided.

        Returns
        -------
        DataFrame

            The prediction result, structured as follows:

            - 1st column: IDs
            - 2nd column: Predicted values
        """
        conn = data.connection_context
        if getattr(self, 'model_') is None:
            raise FitIncompleteError()
        # SQLTRACE
        conn.sql_tracer.set_sql_trace(self, self.__class__.__name__, 'Predict')
        index = data.index
        key = self._arg('key', key, str, required=not isinstance(index, str))
        if isinstance(index, str):
            if key is not None and index != key:
                msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(msg)
        key = index if key is None else key
        cols = data.columns
        cols.remove(key)
        data_ = data[[key] + cols]
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        result_tbl = '#PAL_MLP_RECOMMENDER_RESULT_TBL_{}_{}'.format(self.id, unique_id)
        if not (check_pal_function_exist(conn, '%MLP_RECOMMENDER%', like=True) or \
        self._disable_hana_execution):
            msg = 'The version of your SAP HANA does not support MLP Recommender!'
            logger.error(msg)
            raise ValueError(msg)

        try:
            self._call_pal_auto(conn,
                                'PAL_MLP_RECOMMENDER_PREDICT',
                                data_,
                                self.model_,
                                ParameterTable().with_data([]),
                                result_tbl)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, result_tbl)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, result_tbl)
            raise
        return conn.table(result_tbl)
