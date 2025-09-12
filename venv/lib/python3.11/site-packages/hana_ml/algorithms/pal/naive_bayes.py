"""
This module contains wrappers for PAL naive bayes classification.

The following classes are available:

    * :class:`NaiveBayes`
"""

#pylint: disable=relative-beyond-top-level,line-too-long
#pylint: disable=consider-using-f-string
import logging
import uuid
from hdbcli import dbapi
from hana_ml.ml_exceptions import FitIncompleteError
from hana_ml.ml_base import try_drop
from . import metrics
from .sqlgen import trace_sql
from .pal_base import (
    PALBase,
    ParameterTable,
    ListOfStrings,
    pal_param_register,
    require_pal_usable
)
logger = logging.getLogger(__name__) #pylint: disable=invalid-name

class NaiveBayes(PALBase):
    """
    Naive Bayes is a classification algorithm based on Bayes theorem. It estimates the class-conditional probability by assuming that the attributes are conditionally independent of one another.

    Parameters
    ----------

    alpha : float, optional
        Laplace smoothing value. Set a positive value to enable Laplace smoothing
        for categorical variables and use that value as the smoothing parameter.

        Set value 0 to disable Laplace smoothing.

        Defaults to 0.

    discretization : {'no', 'supervised'}, optional
        Discretize continuous attributes. Case-insensitive.

          - 'no' or not provided: disable discretization.
          - 'supervised': use supervised discretization on all the continuous attributes.

        Defaults to 'no'.

    model_format : {'json', 'pmml'}, optional
        Controls whether to output the model in JSON format or PMML format.
        Case-insensitive.

          - 'json' or not provided: JSON format.
          - 'pmml': PMML format.

        Defaults to 'json'.

    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 0.

    resampling_method : str, optional
        Specifies the resampling method for model evaluation or parameter selection.

        Valid options include: 'cv', 'stratified_cv', 'bootstrap', 'stratified_bootstrap',
        'cv_sha', 'stratified_cv_sha', 'bootstrap_sha', 'stratified_bootstrap_sha',
        'cv_hyperband', 'stratified_cv_hyperband', 'bootstrap_hyperband',
        'stratified_bootstrap_hyperband'.

        If no value is specified for this parameter, neither model evaluation nor parameter selection is activated.

        No default value.

        .. note::
            Resampling method with suffix 'sha' or 'hyperband' is used for
            parameter selection only, not for model evaluation.

    evaluation_metric : {'accuracy', 'f1_score', 'auc'}, optional
        Specifies the evaluation metric for model evaluation or parameter selection.

        Mandatory if model evaluation or parameter selection is expected.

        No default value.
    fold_num : int, optional
        Specifies the fold number for the cross validation method.

        Mandatory and valid only when ``resampling_method`` is set to 'cv' or 'stratified_cv'.

        No default value.

    repeat_times : int, optional
        Specifies the number of repeat times for resampling.

        Default to 1.

    search_strategy : {'grid', 'random'}
        Specifies the parameter search method.

        No default value.

    random_search_times : int, optional
        Specifies the number of times to randomly select candidate parameters for selection.

        Mandatory and valid when ``search_strategy`` is set to 'random'.

        No default value.

    random_state : int, optional
        Specifies the seed for random generation.

        Use system time when 0 is specified.

        Default to 0.

    timeout : int, optional
        Specifies maximum running time for model evaluation or parameter selection, in seconds.

        No timeout when 0 is specified.

        Default to 0.

    progress_indicator_id : str, optional
        Sets an ID of progress indicator for model evaluation or parameter selection.

        No progress indicator is active if no value is provided.

        No default value.

    alpha_range : list of numeric values, optional

        Specifies the range for candidate ``alpha`` values for parameter selection.

        Only valid when ``search_strategy`` is specified.

        No default value.

    alpha_values : list of numeric values, optional
        Specifies candidate ``alpha`` values for parameter selection.

        Only valid when ``search_strategy`` is specified.

        No default value.

    reduction_rate : float, optional
        Specifies reduction rate in SHA or Hyperband method.

        For each round, the available parameter candidate size will be divided by value of this parameter.
        Thus valid value for this parameter must be greater than 1.0

        Valid only when ``resampling_method`` is specified with suffix 'sha' or 'hyperband'(e.g. 'cv_sha',
        'stratified_bootstrap_hyperband').

        Defaults to 3.0.

    aggressive_elimination : bool, optional
        Specifies whether to apply aggressive elimination while using SHA method.

        Aggressive elimination happens when the data size and parameters size to be searched does not match
        and there are still bunch of parameters to be searched while data size reaches its upper limits.
        If aggressive elimination is applied, lower bound of limit of data size will be used multiple times
        first to reduce number of parameters.

        Valid only when ``resampling_method`` is specified with suffix 'sha'.

        Defaults to False.

    Attributes
    ----------
    model_ : DataFrame
        Model content.

        .. note::
            The Laplace value (alpha) is only stored by JSON format models.
            If the PMML format is chosen, you may need to set the Laplace value (alpha)
            again in predict() and score().

    stats_ : DataFrame
        Statistics.

    optim_param_ : DataFrame
        Selected optimal parameters content.

    Examples
    --------
    >>> nb = NaiveBayes(alpha=1.0, model_format='pmml')
    >>> nb.fit(data=df_train)
    >>> nb.predict(data=df_predict, key='ID', alpha=1.0, verbose=True).collect()
    """
    # pylint: disable=too-many-arguments,too-many-instance-attributes
    model_format_map = {'json': 0, 'pmml': 1}
    discretization_map = {'no':0, 'supervised': 1}
    resampling_list = ['cv', 'stratified_cv', 'bootstrap', 'stratified_bootstrap']
    evaluation_metric_map = {'accuracy': 'ACCURACY', 'f1_score': 'F1_SCORE', 'auc': 'AUC'}
    search_strategy_map = {'grid': 'grid', 'random': 'random'}
    def __init__(self,#pylint:disable=too-many-locals,too-many-branches,too-many-statements
                 alpha=None,
                 discretization=None,
                 model_format=None,
                 thread_ratio=None,
                 resampling_method=None,
                 evaluation_metric=None,
                 fold_num=None,
                 repeat_times=None,
                 search_strategy=None,
                 random_search_times=None,
                 random_state=None,
                 timeout=None,
                 progress_indicator_id=None,
                 alpha_range=None,
                 alpha_values=None,
                 reduction_rate=None,
                 aggressive_elimination=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(NaiveBayes, self).__init__()
        self.pal_funcname = 'PAL_NAIVE_BAYES'
        self.op_name = 'NB_Classifier'
        self.alpha = self._arg('alpha', alpha, float)
        self.discretization = self._arg('discretization', discretization, self.discretization_map)
        self.model_format = self._arg('model_format', model_format, self.model_format_map)
        self.thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        self.progress_indicator_id = self._arg('progress_indicator_id', progress_indicator_id, str)
        resampling_long_list = self.resampling_list + [x + '_sha' for x in self.resampling_list] + \
        [x + '_hyperband' for x in self.resampling_list]
        self.resampling_map = {x : x for x in resampling_long_list}
        self.resampling_method = self._arg('resampling_method', resampling_method, self.resampling_map)
        self.evaluation_metric = self._arg('evaluation_metric', evaluation_metric, self.evaluation_metric_map)#pylint:disable=line-too-long
        self.fold_num = self._arg('fold_num', fold_num, int)
        self.repeat_times = self._arg('repeat_times', repeat_times, int)
        self.search_strategy = self._arg('search_strategy', search_strategy, self.search_strategy_map)
        if 'hyperband' in str(self.resampling_method):
            self.search_strategy = 'random'
        self.random_search_times = self._arg('random_search_times', random_search_times, int)
        self.random_state = self._arg('random_state', random_state, int)
        self.timeout = self._arg('timeout', timeout, int)
        self.alpha_values = self._arg('alpha_values', alpha_values, list)
        self.alpha_range = self._arg('alpha_range', alpha_range, list)
        self.reduction_rate = self._arg('reduction_rate', reduction_rate, float)
        self.aggressive_elimination = self._arg('aggressive_elimination', aggressive_elimination, bool)
        param_sel_count = 0
        for param in (self.search_strategy, self.evaluation_metric, self.resampling_method):
            if param is not None:
                param_sel_count += 1
        if param_sel_count not in (0, 3):
            msg = ("'resampling_method', 'evaluation_metric', and"+
                   " 'search_strategy' must be set together.")
            logger.error(msg)
            raise ValueError(msg)
        if self.search_strategy == 'random' and self.random_search_times is None:
            msg = ("'random_search_times' must be set when "+
                   "'search_strategy' is set as random.")
            logger.error(msg)
            raise ValueError(msg)
        if self.search_strategy in (None, 'grid') and self.random_search_times is not None:
            msg = ("'random_search_times' is not valid " +
                   "when parameter selection is not enabled"+
                   ", or 'search_strategy' is not set as 'random'.")
            logger.error(msg)
            raise ValueError(msg)
        if 'cv' in str(self.resampling_method) and self.fold_num is None:
            msg = ("'fold_num' must be set when 'resampling_method' gets 'cv' involved.")
            logger.error(msg)
            raise ValueError(msg)
        if 'cv' not in str(self.resampling_method) and self.fold_num is not None:
            msg = ("'fold_num' is not valid when parameter " +
                   "selection is not enabled, or 'resampling_method'"+
                   " does not get 'cv' involved.")
            logger.error(msg)
            raise ValueError(msg)
        if self.resampling_method is None:
            if self.alpha_values is not None:
                msg = ("'alpha_values' can only be specified "+
                       "when parameter selection is enabled.")
                logger.error(msg)
                raise ValueError(msg)
            if self.alpha_range is not None:
                msg = ("'alpha_range' can only be specified "+
                       "when parameter selection is enabled.")
                logger.error(msg)
                raise ValueError(msg)
        if self.resampling_method is not None:
            alpha_set_count = 0
            for alpha_set in (self.alpha, self.alpha_range, self.alpha_values):
                if alpha_set is not None:
                    alpha_set_count += 1
            if alpha_set_count > 1:
                msg = ("The following parameters cannot be specified together: " +
                       "'alpha', 'alpha_values', 'alpha_range'.")
                logger.error(msg)
                raise ValueError(msg)
            if self.alpha_values is not None:
                if not all(isinstance(t, (int, float)) for t in self.alpha_values):
                    msg = "Valid values of `alpha_values` must be a list of numerical values."
                    logger.error(msg)
                    raise TypeError(msg)

            if self.alpha_range is not None:
                rsz = [3] if self.search_strategy == 'grid'else [2, 3]
                if not len(self.alpha_range) in rsz or not all(isinstance(t, (int, float)) for t in self.alpha_range):#pylint:disable=line-too-long
                    msg = ("The provided `alpha_range` is either not "+
                           "a list of numerical values, or it contains"+
                           " wrong number of values.")
                    logger.error(msg)
                    raise TypeError(msg)

    @trace_sql
    def fit(self, data, key=None, features=None, label=None, categorical_variable=None):#pylint:disable=too-many-branches,too-many-statements
        """
        Fit the model to the training dataset.

        Parameters
        ----------
        data : DataFrame
            Training data.

        key : str, optional

            Name of the ID column.

            If ``key`` is not provided, then:

                - if ``data`` is indexed by a single column, then ``key`` defaults
                  to that index column;

                - otherwise, it is assumed that ``data`` contains no ID column.

        features : a list of str, optional
            Names of the feature columns.

            If ``features`` is not provided, it defaults to all non-ID, non-label columns.

        label : str, optional
            Name of the dependent variable.

            Defaults to the last column.

        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.

        Returns
        -------
        A fitted object of class "NaiveBayes".
        """
        # pylint: disable=too-many-locals
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        conn = data.connection_context
        require_pal_usable(conn)
        key = self._arg('key', key, str)
        features = self._arg('features', features, ListOfStrings)
        label = self._arg('label', label, str)
        if not self._disable_hana_execution:
            data_ = data.rearrange(key=key, features=features, label=label)
            key = data.index if key is None else key
        else:
            data_ = data
        if categorical_variable is not None:
            if isinstance(categorical_variable, str):
                categorical_variable = [categorical_variable]
        self.categorical_variable = self._arg('categorical_variable', categorical_variable, ListOfStrings)#pylint: disable=attribute-defined-outside-init
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        tables = ['MODEL', 'STATS', 'OPTIMAL_PARAM']
        tables = ['#PAL_NAIVE_BAYES_{}_TBL_{}_{}'.format(tbl, self.id, unique_id)
                  for tbl in tables]
        model_tbl, stats_tbl, optim_param_tbl = tables#pylint:disable=unused-variable
        param_rows = [('LAPLACE', None, self.alpha, None),
                      ('DISCRETIZATION', self.discretization, None, None),
                      ('MODEL_FORMAT', self.model_format, None, None),
                      ('THREAD_RATIO', None, self.thread_ratio, None),
                      ('HAS_ID', key is not None, None, None)]
        if self.categorical_variable is not None:
            param_rows.extend(('CATEGORICAL_VARIABLE', None, None, var)
                              for var in self.categorical_variable)
        if self.resampling_method is not None:
            param_rows.extend([('RESAMPLING_METHOD', None, None, self.resampling_method)])
            param_rows.extend([('EVALUATION_METRIC', None, None, self.evaluation_metric)])
            param_rows.extend([('SEED', self.random_state, None, None)])
            param_rows.extend([('REPEAT_TIMES', self.repeat_times, None, None)])
            param_rows.extend([('PARAM_SEARCH_STRATEGY', None, None, self.search_strategy)])

            param_rows.extend([('FOLD_NUM', self.fold_num, None, None)])
            param_rows.extend([('RANDOM_SEARCH_TIMES', self.random_search_times, None, None)])
            param_rows.extend([('TIMEOUT', self.timeout, None, None)])
            param_rows.extend([('PROGRESS_INDICATOR_ID', None, None, self.progress_indicator_id)])
            param_rows.extend([('REDUCTION_RATE', None, self.reduction_rate, None)])
            param_rows.extend([('AGGRESSIVE_ELIMINATION', self.aggressive_elimination,
                                None, None)])
            if self.alpha_range is not None:
                val = str(self.alpha_range)
                param_rows.extend([('LAPLACE_RANGE', None, None, val)])
            if self.alpha_values is not None:
                val = str(self.alpha_values).replace('[', '{').replace(']', '}')
                param_rows.extend([('LAPLACE_VALUES', None, None, val)])

        try:
            self._call_pal_auto(conn,
                                'PAL_NAIVE_BAYES',
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
        self.model_ = conn.table(model_tbl)#pylint: disable=attribute-defined-outside-init
        self.stats_ = conn.table(stats_tbl)#pylint: disable=attribute-defined-outside-init
        self.statistics_ = self.stats_
        #pylint:disable=attribute-defined-outside-init
        self.optim_param_ = conn.table(optim_param_tbl)#pylint: disable=attribute-defined-outside-init
        return self

    @trace_sql
    def predict(self, data, key=None, features=None, alpha=None,#pylint:disable=too-many-locals
                verbose=None, verbose_top_n=None):
        """
        Predict dependent variable values based on a fitted model.

        Parameters
        ----------
        data : DataFrame
            Independent variable values to predict for.

        key : str, optional

            Name of the ID column.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        features : a list of str, optional
            Names of the feature columns.

            If ``features`` is not provided, it defaults to all non-ID columns.

        alpha : float, optional
            Laplace smoothing value.

            Set a positive value to enable Laplace smoothing for categorical variables and use that value as the smoothing parameter.

            Set value 0 to disable Laplace smoothing.

            Defaults to the alpha value in the JSON model, if there is one, or 0 otherwise.

        verbose : bool, optional
            If true, output all classes and the corresponding confidences for each data point.

            Defaults to False.

        verbose_top_n : int, optional

            Specifies the number of top n classes to present after sorting with confidences.
            It cannot exceed the number of classes in label of the training data, and it can be 0,
            which means to output the confidences of `all` classes.

            Effective only when ``verbose`` is set as True.

            Defaults to 0.

        Returns
        -------
        DataFrame
            Predicted result.

        .. note::
            A non-zero Laplace value (alpha) is required if there exist discrete
            category values that only occur in the test set. It can be read from
            JSON models or from the parameter ``alpha`` in predict().
            The Laplace value you set here takes precedence over the values
            read from JSON models.
        """
        conn = data.connection_context
        if getattr(self, 'model_') is None:
            raise FitIncompleteError()
        index = data.index
        key = self._arg('key', key, str, required=not isinstance(index, str))
        if isinstance(index, str):
            if key is not None and index != key:
                msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(msg)
        key = index if key is None else key
        features = self._arg('features', features, ListOfStrings)
        alpha = self._arg('alpha', alpha, float)
        verbose = self._arg('verbose', verbose, bool)
        verbose_top_n = self._arg('verbose_top_n', verbose_top_n, int)

        cols = data.columns
        cols.remove(key)
        if features is None:
            features = cols

        data_ = data[[key] + features]

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        result_tbl = '#PAL_NAIVE_BAYES_RESULT_TBL_{}_{}'.format(self.id, unique_id)

        param_rows = [('THREAD_RATIO', None, self.thread_ratio, None),
                      ('VERBOSE_OUTPUT', verbose, None, None),
                      ('VERBOSE_TOP_N', verbose_top_n, None, None),
                      ('LAPLACE', None, alpha, None)]
        try:
            self._call_pal_auto(conn,
                                "PAL_NAIVE_BAYES_PREDICT",
                                data_,
                                self.model_,
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

    def score(self, data, key=None, features=None, label=None, alpha=None):#pylint:disable= too-many-locals
        """
        Returns the mean accuracy on the given test data and labels.

        Parameters
        ----------
        data : DataFrame
            Data on which to assess model performance.

        key : str, optional

            Name of the ID column.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        features : str or a list of str, optional
            Names of the feature columns.

            If `features` is not provided, it defaults to all non-ID, non-label columns.

        label : str, optional
            Name of the dependent variable.

            Defaults to the last column.

        alpha : float, optional
            Laplace smoothing value.

            Set a positive value to enable Laplace smoothing for categorical variables and use that value as the smoothing parameter.

            Set value 0 to disable Laplace smoothing.

            Defaults to the alpha value in the JSON model, if there is one, or 0 otherwise.

        Returns
        -------
        float
            Mean accuracy on the given test data and labels.

        """
        setattr(self, 'hanaml_score_params', pal_param_register())
        setattr(self, 'testing_data', data)
        if getattr(self, 'model_') is None:
            raise FitIncompleteError()
        index = data.index
        key = self._arg('key', key, str, required=not isinstance(index, str))
        if isinstance(index, str):
            if key is not None and index != key:
                msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(msg)
        key = index if key is None else key
        features = self._arg('features', features, ListOfStrings)
        label = self._arg('label', label, str)
        cols = data.columns
        cols.remove(key)
        if label is None:
            label = cols[-1]
        cols.remove(label)
        if features is None:
            features = cols
        prediction = self.predict(data=data, key=key, alpha=alpha,
                                  features=features, verbose=False)
        prediction = prediction.select(key, 'CLASS').rename_columns(['ID_P', 'PREDICTION'])
        actual = data.select(key, label).rename_columns(['ID_A', 'ACTUAL'])
        joined = actual.join(prediction, 'ID_P=ID_A').select('ACTUAL', 'PREDICTION')
        accuracy =  metrics.accuracy_score(joined,
                                           label_true='ACTUAL',
                                           label_pred='PREDICTION')
        setattr(self, 'score_metrics_', {'ACCURACY': accuracy})
        return accuracy

    def create_model_state(self, model=None, function=None,
                           pal_funcname='PAL_NAIVE_BAYES',
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

            A placeholder parameter, not effective for Naive Bayes.

        pal_funcname : int or str, optional
            PAL function name.

            Defaults to 'PAL_NAIVE_BAYES'.

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
