"""
This module contains PAL wrapper for Support Vector Machine algorithms.

The following classes are available:
    * :class:`SVC`
    * :class:`SVR`
    * :class:`SVRanking`
    * :class:`OneClassSVM`
"""
#pylint: disable=too-many-lines, line-too-long, invalid-name, relative-beyond-top-level
#pylint: disable=consider-iterating-dictionary
import logging
import uuid
import itertools
from hdbcli import dbapi
from hana_ml.ml_exceptions import FitIncompleteError
from .sqlgen import trace_sql
from .pal_base import (
    PALBase,
    ParameterTable,
    ListOfStrings,
    pal_param_register,
    ListOfTuples,
    try_drop,
    require_pal_usable
)
from . import metrics
logger = logging.getLogger(__name__) #pylint: disable=invalid-name

class _SVMBase(PALBase):#pylint:disable=too-many-instance-attributes, too-few-public-methods
    """Base class for Support Vector Machine algorithms."""
    type_map = {1:'SVC', 2:'SVR', 3:'SVRANKING', 4:'ONECLASSSVM'}
    kernel_map = {'linear':0, 'poly':1, 'rbf':2, 'sigmoid':3}
    scale_info_map = {'no':0, 'standardization':1, 'rescale':2}
    search_strategy_list = ['grid', 'random']
    range_params_map = {'c' : 'SVM_C',
                        'nu' : 'NU',
                        'gamma' : 'RBF_GAMMA',
                        'degree' : 'POLY_DEGREE',
                        'coef_lin' : 'COEF_LIN',
                        'coef_const' : 'COEF_CONST'}
    linear_solver_map = {'choleskey': 0, 'cg': 1}
    #pylint:disable=too-many-arguments, too-many-branches, too-many-statements
    def  __init__(self, svm_type, c=None, kernel=None,
                  degree=None, gamma=None, coef_lin=None, coef_const=None,
                  probability=None, shrink=None, error_tol=None, evaluation_seed=None,
                  thread_ratio=None, nu=None, scale_info=None, scale_label=None, handle_missing=True,
                  categorical_variable=None, category_weight=None, regression_eps=None,
                  compression=None, max_bits=None, max_quantization_iter=None,
                  resampling_method=None, evaluation_metric=None, fold_num=None,
                  repeat_times=None, search_strategy=None, random_search_times=None, random_state=None,
                  timeout=None, progress_indicator_id=None, param_values=None, param_range=None,
                  reduction_rate=None, aggressive_elimination=None,
                  onehot_min_frequency=None, onehot_max_categories=None):
        #pylint:disable=too-many-locals
        if not hasattr(self, 'hanaml_parameters'):
            setattr(self, 'hanaml_parameters', pal_param_register())
        super(_SVMBase, self).__init__()
        self.pal_funcname = 'PAL_SVM'
        self.type = svm_type
        self.svm_c = self._arg('c', c, float)
        self.kernel_type = self._arg('kernel', kernel, self.kernel_map)
        self.poly_degree = self._arg('degree', degree, int)
        self.rbf_gamma = self._arg('gamma', gamma, float)
        self.coef_lin = self._arg('coef_lin', coef_lin, float)
        self.coef_const = self._arg('coef_const', coef_const, float)
        self.probability = self._arg('probability', probability, bool)
        self.shrink = self._arg('shrink', shrink, bool)
        self.error_tol = self._arg('error_tol', error_tol, float)
        self.evaluation_seed = self._arg('evaluation_seed', evaluation_seed, int)
        self.thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        self.nu = self._arg('nu', nu, float)#pylint: disable=invalid-name
        self.scale_info = self._arg('scale_info', scale_info, self.scale_info_map)
        self.scale_label = self._arg('scale_label', scale_label, bool)
        self.handle_missing = self._arg('handle_missing', handle_missing, bool)
        if isinstance(categorical_variable, str):
            categorical_variable = [categorical_variable]
        self.categorical_variable = self._arg('categorical_variable', categorical_variable,
                                              ListOfStrings)
        self.category_weight = self._arg('category_weight', category_weight, float)
        self.regression_eps = self._arg('regression_eps', regression_eps, float)
        self.compression = self._arg('compression', compression, bool)
        self.max_bits = self._arg('max_bits', max_bits, int)
        self.max_quantization_iter = self._arg('max_quantization_iter', max_quantization_iter,
                                               int)

        self.model_type = _SVMBase.type_map[self.type]
        if isinstance(resampling_method, str):
            resampling_method = resampling_method.lower()
        self.resampling_method = self._arg('resampling_method', resampling_method,
                                           {x.lower():x for x in self.resampling_method_list})#pylint:disable=no-member
        if isinstance(evaluation_metric, str):
            evaluation_metric = evaluation_metric.lower()
        self.evaluation_metric = self._arg('evaluation_metric', evaluation_metric,
                                           {x.lower():x for x in self.evaluation_metric_list},#pylint:disable=no-member
                                           required = self.resampling_method is not None and \
                                           isinstance(self, SVC))
        self.fold_num = self._arg('fold_num', fold_num, int,
                                  required = 'cv' in str(self.resampling_method))
        self.repeat_times = self._arg('repeat_times', repeat_times, int)
        #need some check work here
        self.search_strategy = self._arg('search_strategy', search_strategy,
                                         {x:x for x in self.search_strategy_list},
                                         required = 'sha' in str(self.resampling_method))
        if 'hyper' in str(self.resampling_method):
            self.search_strategy = 'random'
        self.random_search_times = self._arg('random_search_times', random_search_times, int,
                                             required = str(self.search_strategy) == 'random')
        self.random_state = self._arg('random_state', random_state, int)
        self.timeout = self._arg('timeout', timeout, int)
        self.progress_indicator_id = self._arg('progress_indicator_id', progress_indicator_id, str)
        self.reduction_rate = self._arg('reduction_rate', reduction_rate, (float, int))
        self.aggressive_elimination = self._arg('aggressive_elimination', aggressive_elimination,
                                                bool)
        if isinstance(param_range, dict):
            param_range = [(x, param_range[x]) for x in param_range]
        if isinstance(param_values, dict):
            param_values = [(x, param_values[x]) for x in param_values]
        self.param_values = self._arg('param_values', param_values, ListOfTuples)
        self.param_range = self._arg('param_range', param_range, ListOfTuples)
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
            if self.param_values is not None:
                for x in self.param_values:#pylint:disable=invalid-name
                    if len(x) != 2:#pylint:disable=bad-option-value
                        msg = ("Each tuple that specifies the values of a parameter should"+
                               " contain exactly 2 elements: 1st is parameter name,"+
                               " 2nd is a list of valid values.")
                        logger.error(msg)
                        raise ValueError(msg)
                    if x[0] not in list(self.range_params_map.keys()):
                        msg = ("Specifying the values of `{}` for ".format(x[0])+
                               "parameter selection is invalid.")
                        logger.error(msg)
                        raise ValueError(msg)
                    if x[0] in value_list:
                        msg = ("Parameter `{}` is invalid for ".format(x[0])+
                               "re-specification in parameter selection.")
                        logger.error(msg)
                        raise ValueError(msg)
                    value_list.append(x[0])

            if self.param_range is not None:
                for x in self.param_range:#pylint:disable=invalid-name
                    if len(x) != 2:#pylint:disable=bad-option-value
                        msg = ("Each tuple that specifies the range of a parameter should contain"+
                               " exactly 2 elements: 1st is parameter name, 2nd is value range.")
                        logger.error(msg)
                        raise ValueError(msg)
                    if x[0] not in list(self.range_params_map.keys()):
                        msg = ("Parameter `{}` is invalid for ".format(x[0])+
                               "range specification in parameter selection.")
                        logger.error(msg)
                        raise ValueError(msg)
                    if x[0] in value_list:
                        msg = ("Parameter `{}` is invalid for ".format(x[0])+
                               "re-specification in parameter selection.")
                        logger.error(msg)
                        raise ValueError(msg)
                    value_list.append(x[0])
        self.onehot_min_frequency = self._arg('onehot_min_frequency', onehot_min_frequency, int)
        self.onehot_max_categories = self._arg('onehot_max_categories', onehot_max_categories, int)

    #pylint:disable=too-many-statements, too-many-locals, too-many-branches
    @trace_sql
    def _fit(self, data, key=None, features=None, label=None, qid=None, categorical_variable=None):
        conn = data.connection_context

        key = self._arg('key', key, str)
        features = self._arg('features', features, ListOfStrings)
        label = self._arg('label', label, str)
        qid = self._arg('qid', qid, str, self.type == 3)
        if isinstance(categorical_variable, str):
            categorical_variable = [categorical_variable]
        categorical_variable = self._arg('categorical_variable', categorical_variable, ListOfStrings)
        model_type = self.model_type
        if not self._disable_hana_execution:
            require_pal_usable(conn)
            index = data.index
            if isinstance(index, str):
                if key is not None and index != key:
                    msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                    "and the designated index column '{}'.".format(index)
                    logger.warning(msg)
            key = index if key is None else key
            cols_left = data.columns
            if key is not None:
                cols_left.remove(key)
            if self.type == 3:
                cols_left.remove(qid)
            if self.type != 4:
                #all algorithms except OneClassSVM need label
                if label is None:
                    label = cols_left[-1]
                cols_left.remove(label)
            if features is None:
                features = cols_left
            used_cols = [col for col in itertools.chain([key], features, [qid], [label])
                        if col is not None]
            ##Output warning messages if qid is set when type is not svranking
            if self.type != 3 and qid is not None:
                error_msg = "Qid will only be valid when type is SVRanking."
                logger.error(error_msg)
                raise ValueError(error_msg)
            training_data = data[used_cols]
        else:
            training_data = data
        #data_tbl = "#PAL_{}_DATA_TBL_{}".format(model_type, self.id)
        ## param table manipulation
        param_array = [('TYPE', self.type, None, None),
                       ('SVM_C', None, self.svm_c, None),
                       ('KERNEL_TYPE', self.kernel_type, None, None),
                       ('POLY_DEGREE', self.poly_degree, None, None),
                       ('RBF_GAMMA', None, self.rbf_gamma, None),
                       ('THREAD_RATIO', None, self.thread_ratio, None),
                       ('NU', None, self.nu, None),
                       ('REGRESSION_EPS', None, self.regression_eps, None),
                       ('COEF_LIN', None, self.coef_lin, None),
                       ('COEF_CONST', None, self.coef_const, None),
                       ('PROBABILITY', self.probability, None, None),
                       ('HAS_ID', key is not None, None, None),
                       ('SHRINK', self.shrink, None, None),
                       ('ERROR_TOL', None, self.error_tol, None),
                       ('EVALUATION_SEED', self.evaluation_seed, None, None),
                       ('SCALE_INFO', self.scale_info, None, None),
                       ('SCALE_LABEL', self.scale_label, None, None),
                       ('HANDLE_MISSING', self.handle_missing, None, None),
                       ('CATEGORY_WEIGHT', None, self.category_weight, None),
                       ('COMPRESSION', self.compression, None, None),
                       ('MAX_BITS', self.max_bits, None, None),
                       ('MAX_QUANTIZATION_ITER', self.max_quantization_iter, None, None),
                       ('RESAMPLING_METHOD', None, None, self.resampling_method),
                       ('EVALUATION_METRIC', None, None, self.evaluation_metric),
                       ('FOLD_NUM', self.fold_num, None, None),
                       ('REPEAT_TIMES', self.repeat_times, None, None),
                       ('PARAM_SEARCH_STRATEGY', None, None, self.search_strategy),
                       ('RANDOM_SEARCH_TIMES', self.random_search_times, None, None),
                       ('SEED', self.random_state, None, None),
                       ('TIMEOUT', self.timeout, None, None),
                       ('PROGRESS_INDICATOR_ID', None, None, self.progress_indicator_id),
                       ('REDUCTION_RATE', None, self.reduction_rate, None),
                       ('AGGRESSIVE_ELIMINATION', self.aggressive_elimination,
                        None, None),
                       ('ONEHOT_MIN_FREQUENCY', self.onehot_min_frequency, None, None),
                       ('ONEHOT_MAX_CATEGORIES', self.onehot_max_categories, None, None)]

        if self.param_values is not None:
            for x in self.param_values:#pylint:disable=invalid-name
                values = str(x[1]).replace('[', '{').replace(']', '}')
                param_array.extend([(self.range_params_map[x[0]]+'_VALUES',
                                     None, None, values)])
        if self.param_range is not None:
            for x in self.param_range:#pylint:disable=invalid-name
                range_ = str(x[1])
                if len(x[1]) == 2 and self.search_strategy == 'random':
                    range_ = range_.replace(',', ',,')
                param_array.extend([(self.range_params_map[x[0]]+'_RANGE',
                                     None, None, range_)])

        #for categorical variable
        if categorical_variable is not None:
            param_array.extend([('CATEGORICAL_VARIABLE', None, None, variable)
                                for variable in categorical_variable])
        elif self.categorical_variable is not None:
            param_array.extend([('CATEGORICAL_VARIABLE', None, None, variable)
                                for variable in self.categorical_variable])
        if self.type == 1:
            param_array.extend([('CATEGORICAL_VARIABLE', None, None, label)])
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        model_tbl = "#PAL_{}_MODEL_TBL_{}_{}".format(model_type, self.id, unique_id)
        stat_tbl = "#PAL_{}_STATISTIC_TBL_{}_{}".format(model_type, self.id, unique_id)
        opt_param_tbl = "#PAL_{}_OPT_PARAM_TBL_{}_{}".format(model_type, self.id, unique_id)
        tables = [model_tbl, stat_tbl, opt_param_tbl]
        try:
            self._call_pal_auto(conn,
                                'PAL_SVM',
                                training_data,
                                ParameterTable().with_data(param_array),
                                *tables)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, tables)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, tables)
            raise
        self.model_ = conn.table(model_tbl)#pylint:disable=attribute-defined-outside-init
        self.stat_ = conn.table(stat_tbl)#pylint:disable=attribute-defined-outside-init
        self.optim_param_ = conn.table(opt_param_tbl)##pylint:disable=attribute-defined-outside-init

    @trace_sql
    def _predict(self, data, key=None, features=None, qid=None, verbose=False):#pylint:disable=too-many-locals
        #check for fit table existence
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
        qid = self._arg('qid', qid, str, self.type == 3)
        verbose = self._arg('verbose', verbose, bool)
        unique_id = str(uuid.uuid1())
        unique_id = unique_id.replace('-', '_').upper()
        cols_left = data.columns
        cols_left.remove(key)
        if self.type == 3:
            cols_left.remove(qid)
        index_col = key
        model_type = self.model_type
        if features is None:
            features = cols_left
        used_cols = [col for col in itertools.chain([index_col], features, [qid])
                     if col is not None]
        predict_set = data[used_cols]
        thread_ratio = 0.0 if self.thread_ratio is None else self.thread_ratio
        param_array = [('THREAD_RATIO', None, thread_ratio, None)]
        param_array.append(('VERBOSE_OUTPUT', verbose, None, None))
        result_tbl = "#{}_PREDICT_RESULT_TBL_{}_{}".format(model_type, self.id, unique_id)
        try:
            self._call_pal_auto(conn,
                                "PAL_SVM_PREDICT",
                                predict_set,
                                self.model_,
                                ParameterTable().with_data(param_array),
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

    def create_model_state(self, model=None, function=None, pal_funcname='PAL_SVM', state_description=None, force=False):
        r"""
        Create PAL model state.

        Parameters
        ----------
        model : DataFrame, optional
            Specify the model for AFL state.

            Defaults to self.model\_.

        function : str, optional
            Specify the function in the unified API.

            A placeholder parameter, not effective for SVM.

        pal_funcname : int or str, optional
            PAL function name.

            Defaults to 'PAL_SVM'.

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
        super()._create_model_state(state)

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

class SVC(_SVMBase):
    r"""
    Support Vector Machines (SVMs) refer to a family of supervised learning models using the concept of support vector.

    Compared with many other supervised learning models, SVMs have the advantages in that the models produced by SVMs can be either linear or non-linear, where the latter is realized by a technique called Kernel Trick.

    Like most supervised models, there are training phase and testing phase for SVMs. In the training phase, a function f(x):->y where f(∙) is a function (can be non-linear) mapping a sample onto a TARGET, is learnt. The training set consists of pairs denoted by {xi, yi}, where x denotes a sample represented by several attributes, and y denotes a TARGET (supervised information). In the testing phase, the learnt f(∙) is further used to map a sample with unknown TARGET onto its predicted TARGET.

    Classification is one of the most frequent tasks in many fields including machine learning, data mining, computer vision, and business data analysis. Compared with linear classifiers like logistic regression, **SVC** is able to produce non-linear decision boundary, which leads to better accuracy on some real world dataset. In classification scenario, f(∙) refers to decision function, and a TARGET refers to a "label" represented by a real number.

    Parameters
    ----------

    c : float, optional
        Trade-off between training error and margin.
        Value range > 0.

        Defaults to 100.0.

    kernel : {'linear', 'poly', 'rbf', 'sigmoid'}, optional
        Specifies the kernel type to be used in the algorithm.

        Defaults to 'rbf'.

    degree : int, optional
        Coefficient for the 'poly' kernel type. Value range >= 1.

        Defaults to 3.

    gamma : float, optional
        Coefficient for the 'rbf' kernel type.

        Defaults to 1.0/number of features in the dataset.
        Only valid for when ``kernel`` is 'rbf'.

    coef_lin : float, optional
        Coefficient for the poly/sigmoid kernel type.

        Defaults to 0.

    coef_const : float, optional
        Coefficient for the poly/sigmoid kernel type.

        Defaults to 0.

    probability : bool, optional
        If True, output probability during prediction.

        Defaults to False.

    shrink : bool, optional
        If True, use shrink strategy.

        Defaults to True.

    tol : float, optional
        Specifies the error tolerance in the training process. Value range > 0.

        Defaults to 0.001.

    evaluation_seed : int, optional
        The random seed in parameter selection. Value range >= 0.

        Defaults to 0.

    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 0.0.

    scale_info : {'no', 'standardization', 'rescale'}, optional
        Options:

          - 'no' : No scale.
          - 'standardization' : Transforms the data to have zero mean
            and unit variance.
          - 'rescale' : Rescales the range of the features to scale the
            range in [-1,1].

        Defaults to 'standardization'.

    handle_missing : bool, optional
        Whether to handle missing values:

            - False: No,

            - True: Yes.

        Defaults to True.

    categorical_variable : str or a list of str, optional
        Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

        No default value.

    category_weight : float, optional
        Represents the weight of category attributes.
        Value range > 0.

        Defaults to 0.707.

    compression : bool, optional
        Specifies if the model is stored in compressed format.

        Default value depends on the SAP HANA Version.
        Please refer to the corresponding documentation of SAP HANA PAL.

    max_bits : int, optional
        The maximum number of bits to quantize continuous features, equivalent to
        use :math:`2^{max\_bits}` bins.

        Must be less than 31.

        Valid only when the value of ``compression`` is True.

        Defaults to 12.

    max_quantization_iter : int, optional
        The maximum iteration steps for quantization.

        Valid only when the value of compression is True.

        Defaults to 1000.

    resampling_method : str, optional
        Specifies the resampling method for model evaluation or parameter selection.

          - 'cv'
          - 'cv_sha'
          - 'cv_hyperband'
          - 'stratified_cv'
          - 'stratified_cv_sha'
          - 'stratified_cv_hyperband'
          - 'bootstrap'
          - 'bootstrap_sha'
          - 'bootstrap_hyperband'
          - 'stratified_bootstrap'
          - 'stratified_bootstrap_sha'
          - 'stratified_bootstrap_hyperband'

        If no value is specified for this parameter, neither model evaluation
        nor parameter selection is activated.

        No default value.

        .. note::
            Resampling methods that end with 'sha' or 'hyperband' are used for
            parameter selection only, not for model evaluation.

    evaluation_metric : {'ACCURACY', 'F1_SCORE', 'AUC'}, optional
        Specifies the evaluation metric for model evaluation or parameter selection.

        No default value.

    fold_num : int, optional
        Specifies the fold number for the cross validation method.
        Mandatory and valid only when ``resampling_method`` is one of the following:
        'cv', 'cv_sha', 'cv_hyperband', 'stratified_cv', 'stratified_cv_sha',
        'stratified_cv_hyperband'.

        No default value.

    repeat_times : int, optional
        Specifies the number of repeat times for resampling.

        Default to 1.

    search_strategy : str, optional
        Specify the parameter search method:

          - 'grid'
          - 'random'

        Mandatory when ``resampling`` method is one of the following: 'cv_sha',
        'stratified_cv_sha', 'bootstrap_sha', 'stratified_bootstrap_sha'.

        Defaults to ``random`` and cannot be changed if ``resampling_method`` is 'cv_hyperband',
        'stratified_cv_hyperband', 'bootstrap_hyperband' or 'stratified_bootstrap_hyperband';
        otherwise no default value, and parameter selection cannot be activated
        if not specified.

    random_search_times : int, optional
        Specifies the number of times to randomly select candidate parameters for selection.

        Mandatory when ``search_strategy`` is set to 'random', or when ``resampling_method``
        is set to one of the following: 'cv_hyperband', 'stratified_cv_hyperband',
        'bootstrap_hyperband', 'stratified_bootstrap_hyperband'.

        No default value.

    random_state : int, optional
        Specifies the seed for random generation. Use system time when 0 is specified.

        Default to 0.

    timeout : int, optional
        Specifies maximum running time for model evaluation or parameter selection, in seconds.
        No timeout when 0 is specified.

        Default to 0.

    progress_indicator_id : str, optional
        Sets an ID of progress indicator for model evaluation or parameter selection.
        No progress indicator is active if no value is provided.

        No default value.

    param_values : dict or list of tuple, optional
        Sets the values of following parameters for model parameter selection:

            ``c``, ``degree``, ``coef_lin``, ``coef_const``.

        If input is list of tuple, then each tuple should contain exactly two elements:

            - 1st element is the parameter name(str type),
            - 2nd element is a list of valid values for that parameter.

        Otherwise, if input is dict, then the key of each elements must specify a parameter name,
        while the value specifies a list of valid values for that parameter.

        A simple example for illustration:

            [('c', [0.1, 0.2, 0.5]), ('degree', [0.2, 0.6])],

        or

            dict(c=[0.1, 0.2, 0.5], degree = [0.2, 0.6])

        Valid only when ``resampling_method`` and ``search_strategy`` are both specified.

        No default value.

    param_range : dict or list of tuple, optional
        Sets the range of the following parameters for model parameter selection:

            ``c``, ``degree``, ``coef_lin``, ``coef_const``.

        If input is list of tuple, then each tuple should contain exactly two elements:

            - 1st element is the parameter name(str type),
            - 2nd element is a list that specifies the range of that parameter as [start, step, end],
              while step is ignored if ``search_strategy`` is 'random'.

        Valid only when ``resampling_method`` and ``search_strategy`` are both specified.

        No default value.

    reduction_rate : float, optional
        Specifies reduction rate in SHA or Hyperband method.

        For each round, the available parameter candidate size will be divided by value of this parameter.
        Thus valid value for this parameter must be greater than 1.0

        Valid only when ``resampling_method`` is set to one of the following values:
        'cv_sha', 'stratified_cv_sha', 'bootstrap_sha', 'stratified_bootstrap_sha',
        'cv_hyperband', 'stratified_cv_hyperband', 'bootstrap_hyperband',
        'stratified_bootstrap_hyperband'.

        Defaults to 3.0.

    aggressive_elimination : bool, optional
        Specifies whether to apply aggressive elimination while using SHA method.

        Aggressive elimination happens when the data size and parameters size to be searched does not match
        and there are still bunch of parameters to be searched while data size reaches its upper limits.
        If aggressive elimination is applied, lower bound of limit of data size will be used multiple times
        first to reduce number of parameters.

        Valid only when ``resampling_method`` is set to one of the following:
        'cv_sha', 'stratified_cv_sha', 'bootstrap_sha', 'stratified_bootstrap_sha'.

        Defaults to False.

    onehot_min_frequency :  int, optional
        Specifies the minimum frequency below which a category will be considered infrequent.

        Defaults to 1.

    onehot_max_categories : int, optional
        Specifies an upper limit to the number of output features for each input feature. It includes the feature that combines infrequent categories.

        Defaults to 0.

    References
    ----------
    Three key functionalities are enabled in support vector classification(SVC), listed as follows:

        - :ref:`Model Evaluation and Parameter Selection<param_select-label>`
        - :ref:`Successive Halving and Hyperband Method for Parameter Selection<sha_hyperband-label>`
        - :ref:`Model Compression<model_compression_svm-label>`

    Please refer to the links above for detailed description about each functionality together with
    relevant parameters.

    Attributes
    ----------
    model_ : DataFrame
        Model content.

    stat_ : DataFrame
        Statistics content.

    Examples
    --------
    >>> svc = svm.SVC(gamma=0.005, handle_missing=False)
    >>> svc.fit(data=df_fit, key='ID', features=['F1', 'F2'])
    >>> res = svc.predict(data=df_predict, key='ID', features=['F1', 'F2'])
    >>> res.collect()

    """
    #pylint:disable=too-many-arguments
    resampling_method_list = ['cv', 'stratified_cv', 'bootstrap', 'stratified_bootstrap',
                              'cv_sha', 'stratified_cv_sha', 'bootstrap_sha', 'stratified_bootstrap_sha',
                              'cv_hyperband', 'stratified_cv_hyperband', 'bootstrap_hyperband',
                              'stratified_bootstrap_hyperband']
    evaluation_metric_list = ['ACCURACY', 'F1_SCORE', 'AUC']
    def __init__(self, c=None, kernel='rbf', degree=None,
                 gamma=None, coef_lin=None, coef_const=None, probability=False, shrink=True,
                 tol=None, evaluation_seed=None, thread_ratio=None, scale_info=None, handle_missing=True,
                 categorical_variable=None, category_weight=None,
                 compression=None, max_bits=None, max_quantization_iter=None,
                 resampling_method=None, evaluation_metric=None,
                 fold_num=None, repeat_times=None, search_strategy=None, random_search_times=None,
                 random_state=None, timeout=None, progress_indicator_id=None, param_values=None, param_range=None,
                 reduction_rate=None, aggressive_elimination=None,
                 onehot_min_frequency=None, onehot_max_categories=None):
        #pylint:disable=too-many-locals
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(SVC, self).__init__(1, c, kernel, degree, gamma, coef_lin,
                                  coef_const, probability, shrink, tol, evaluation_seed,
                                  thread_ratio, None, scale_info, None, handle_missing, categorical_variable,
                                  category_weight, None,
                                  compression, max_bits, max_quantization_iter,
                                  resampling_method=resampling_method, evaluation_metric=evaluation_metric,
                                  fold_num=fold_num, repeat_times=repeat_times, search_strategy=search_strategy,
                                  random_search_times=random_search_times, random_state=random_state, timeout=timeout,
                                  progress_indicator_id=progress_indicator_id, param_values=param_values,
                                  param_range=param_range, reduction_rate=reduction_rate,
                                  aggressive_elimination=aggressive_elimination,
                                  onehot_min_frequency=onehot_min_frequency,
                                  onehot_max_categories=onehot_max_categories)
        self.op_name = 'SVM_Classifier'

    def fit(self, data, key=None, features=None, label=None, categorical_variable=None):
        """
        Fit the model to the training dataset.

        Parameters
        ----------
        data : DataFrame
            DataFrame containing the data.
        key : str, optional
            Name of the ID column.

            If ``key`` is not provided, then:

                - if ``data`` is indexed by a single column, then ``key`` defaults
                  to that index column;

                - otherwise, it is assumed that ``data`` contains no ID column.

        features : a list of str, optional
            Names of the feature columns.

            If ``features`` is not provided, it defaults to
            all the non-ID, non-label columns.

        label : str, optional
            Name of the label column.

            If ``label`` is not provided, it defaults to the last non-ID column.

        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.

        Returns
        -------
        A fitted object of class "SVC".
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        self._fit(data=data, key=key, features=features, label=label, categorical_variable=categorical_variable)
        return self

    def predict(self, data, key=None, features=None, verbose=False):#pylint:disable=too-many-locals
        """
        Predict dependent variable values based on a fitted model.

        Parameters
        ----------
        data : DataFrame
            DataFrame containing the data.
        key : str, optional
            Name of the ID column.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        features : a list of str, optional
            Names of the feature columns.
            If ``features`` is not provided, it defaults to all the
            non-ID, non-label columns.
        verbose : bool, optional
            If True, output scoring probabilities for each class.
            It is only applicable when probability is true during instance
            creation.

            Defaults to False.

        Returns
        -------
        DataFrame
            Predict result, structured as follows:

              - ID column, with the same name and type as ``data`` 's ID column.
              - SCORE, type NVARCHAR(100), prediction value.
              - PROBABILITY, type DOUBLE, prediction probability.
                It is NULL when ``probability`` is False during
                instance creation.
        """
        return self._predict(data=data, key=key, features=features, qid=None, verbose=verbose)

    def score(self, data, key=None, features=None, label=None):
        """
        Returns the accuracy on the given test data and labels.

        Parameters
        ----------
        data : DataFrame
            DataFrame containing the data.
        key : str, optional
            Name of the ID column.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        features : a list of str, optional
            Names of the feature columns.

            If ``features`` is not provided, it defaults to all the non-ID,
            non-label columns.
        label : str, optional
            Name of the label column.

            If ``label`` is not provided, it defaults to the last non-ID column.

        Returns
        -------
        float
            Scalar accuracy value comparing the predicted result and
            original label.
        """
        setattr(self, 'hanaml_score_params', pal_param_register())
        setattr(self, 'testing_data', data)
        conn = data.connection_context
        require_pal_usable(conn)
        if getattr(self, 'model_') is None:
            raise FitIncompleteError()

        # SQLTRACE
        conn.sql_tracer.set_sql_trace(self, self.__class__.__name__, 'Score')

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
        #return scalar value of accuracy after calling predict
        unique_id = str(uuid.uuid1())
        unique_id = unique_id.replace('-', '_').upper()
        cols = data.columns
        cols.remove(key)
        if label is None:
            label = data.columns[-1]
        cols.remove(label)
        if features is None:
            features = cols

        prediction = self.predict(data=data, key=key,
                                  features=features)
        prediction = prediction.select(key, 'SCORE').rename_columns(['ID_P', 'PREDICTION'])

        actual = data.select(key, label).rename_columns(['ID_A', 'ACTUAL'])
        joined = actual.join(prediction, 'ID_P=ID_A').select('ACTUAL', 'PREDICTION')

        accuracy = metrics.accuracy_score(joined,
                                          label_true='ACTUAL',
                                          label_pred='PREDICTION')
        setattr(self, 'score_metrics_', {"ACCURACY": accuracy})
        return accuracy

class SVR(_SVMBase):
    r"""
    Support Vector Machines (SVMs) refer to a family of supervised learning models using the concept of support vector.

    Compared with many other supervised learning models, SVMs have the advantages in that the models produced by SVMs can be either linear or non-linear, where the latter is realized by a technique called Kernel Trick.

    Like most supervised models, there are training phase and testing phase for SVMs. In the training phase, a function f(x):->y where f(∙) is a function (can be non-linear) mapping a sample onto a TARGET, is learnt. The training set consists of pairs denoted by {xi, yi}, where x denotes a sample represented by several attributes, and y denotes a TARGET (supervised information). In the testing phase, the learnt f(∙) is further used to map a sample with unknown TARGET onto its predicted TARGET.

    **SVR** is another method for regression analysis. Compared with classical linear regression methods like least square regression, the regression function in SVR can be non-linear. In regression scenario, f(∙) refers to regression function, and TARGET refers to "response" represented by a real number.

    Parameters
    ----------

    c : float, optional
        Trade-off between training error and margin.
        Value range > 0.

        Defaults to 100.0.

    kernel : {'linear', 'poly', 'rbf', 'sigmoid'}, optional
        Specifies the kernel type to be used in the algorithm.

        Defaults to 'rbf'.

    degree : int, optional
        Coefficient for the 'poly' kernel type. Value range >= 1.

        Defaults to 3.

    gamma : float, optional
        Coefficient for the 'rbf' kernel type.

        Defaults to 1.0/number of features in the dataset. Only valid when ``kernel`` is 'rbf'.

    coef_lin : float, optional
        Coefficient for the poly/sigmoid kernel type.

        Defaults to 0.

    coef_const : float, optional
        Coefficient for the poly/sigmoid kernel type.

        Defaults to 0.

    shrink : bool, optional
        If True, use shrink strategy.

        Defaults to True.

    tol : float, optional
        Specifies the error tolerance in the training process. Value range > 0.

        Defaults to 0.001.

    evaluation_seed : int, optional
        The random seed in parameter selection. Value range >= 0.

        Defaults to 0.

    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 0.0.

    scale_info : {'no', 'standardization', 'rescale'}, optional
        Options:

          - 'no' : No scale.
          - 'standardization' : Transforms the data to have zero mean
            and unit variance.
          - 'rescale' : Rescales the range of the features to scale the
            range in [-1,1].

        Defaults to 'standardization'.

    scale_label : bool, optional
        If True, standardize the label for SVR.

        It is only applicable when the ``scale_info`` is
        'standardization'.

        Defaults to True.

    handle_missing : bool, optional
        Whether to handle missing values:

            - False: No,

            - True: Yes.

        Defaults to True.

    categorical_variable : str or a list of str, optional
        Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

        No default value.

    category_weight : float, optional
        Represents the weight of category attributes. Value range > 0.

        Defaults to 0.707.

    regression_eps : float, optional
        Epsilon width of tube for regression.

        Defaults to 0.1.

    compression : bool, optional
       Specifies if the model is stored in compressed format.

        Default value depends on the SAP HANA Version. Please refer to the corresponding documentation of SAP HANA PAL.

    max_bits : int, optional
        The maximum number of bits to quantize continuous features, equivalent to
        use :math:`2^{max\_bits}` bins.

        Must be less than 31.

        Valid only when the value of compression is True.

        Defaults to 12.

    max_quantization_iter : int, optional
        The maximum iteration steps for quantization.

        Valid only when the value of compression is True.

        Defaults to 1000.

    resampling_method : str, optional
        Specifies the resampling method for model evaluation or parameter selection.

          - 'cv'
          - 'cv_sha'
          - 'cv_hyperband'
          - 'bootstrap'
          - 'bootstrap_sha'
          - 'bootstrap_hyperband'

        If no value is specified for this parameter, neither model evaluation nor parameter selection is activated.

        No default value.

        .. note::
            Resampling methods that end with 'sha' or 'hyperband' are used for
            parameter selection only, not for model evaluation.

    fold_num : int, optional
        Specifies the fold number for the cross validation method.

        Mandatory and valid only when ``resampling_method`` is set to 'cv', 'cv_sha' or 'cv_hyperband'.

        No default value.

    repeat_times : int, optional
        Specifies the number of repeat times for resampling.

        Default to 1.

    search_strategy : str, optional
        Specify the parameter search method:

          - 'grid'
          - 'random'

        Mandatory when ``resampling`` method is set to 'cv_sha' or 'bootstrap_sha'.

        Defaults to ``random`` and cannot be changed if ``resampling_method`` is set to 'cv_hyperband' or
        'bootstrap_hyperband'; otherwise no default value, and parameter selection cannot be activated
        if not specified.

    random_search_times : int, optional
        Specifies the number of times to randomly select candidate parameters for selection.

        Mandatory when ``search_strategy`` is set to 'random', or when ``resampling_method``
        is set to 'cv_hyperband' or 'bootstrap_hyperband'.

        No default value.

    random_state : int, optional
        Specifies the seed for random generation. Use system time when 0 is specified.

        Default to 0.

    timeout : int, optional
        Specifies maximum running time for model evaluation or parameter selection, in seconds.
        No timeout when 0 is specified.

        Default to 0.

    progress_indicator_id : str, optional
        Sets an ID of progress indicator for model evaluation or parameter selection.
        No progress indicator is active if no value is provided.

        No default value.

    param_values : dict or list of tuple, optional
        Sets the values of following parameters for model parameter selection:

            ``gamma``, ``c``.

        If input is list of tuple, then each tuple should contain exactly two elements:

            - 1st element is the parameter name(str type),
            - 2nd element is a list of valid values for that parameter.

        Otherwise, if input is dict, then the key of each element should specify a parameter name,
        while the corresponding value specifies a list of values for that parameter.

        A simple example for illustration:

            [('c', [0.1, 0.2, 0.5]), ('gamma', [0.2, 0.6])]

        or

            {'c':[0.1, 0.2, 0.5], 'gamma':[0.2,0.6]}

        Valid only when ``resampling_method`` and ``search_strategy`` are both specified.

        No default value.

    param_range : dict or list of tuple, optional
        Sets the range of the following parameters for model parameter selection:

            ``gamma``, ``c``.

        If input is list of tuple, then each tuple should contain exactly two elements:

            - 1st element is the parameter name(str type),
            - 2nd element is a list that specifies the range of that parameter as [start, step, end].

        Otherwise, if input is dict, then the key of each element must specify a parameter name,
        while the corresponding value specifies the range of that parameter.

        Valid only when ``resampling_method`` and ``search_strategy`` are both specified.

        No default value.

    reduction_rate : float, optional
        Specifies reduction rate in SHA or Hyperband method.

        For each round, the available parameter candidate size will be divided by value of this parameter.
        Thus valid value for this parameter must be greater than 1.0

        Valid only when ``resampling_method`` is set to one of the following values:
        'cv_sha', 'bootstrap_sha', 'cv_hyperband', 'bootstrap_hyperband'.

        Defaults to 3.0.

    aggressive_elimination : bool, optional
        Specifies whether to apply aggressive elimination while using SHA method.

        Aggressive elimination happens when the data size and parameters size to be searched does not match
        and there are still bunch of parameters to be searched while data size reaches its upper limits.
        If aggressive elimination is applied, lower bound of limit of data size will be used multiple times
        first to reduce number of parameters.

        Valid only when ``resampling_method`` is set to 'cv_sha' or 'bootstrap_sha'.

        Defaults to False.

    onehot_min_frequency :  int, optional
        Specifies the minimum frequency below which a category will be considered infrequent.

        Defaults to 1.

    onehot_max_categories : int, optional
        Specifies an upper limit to the number of output features for each input feature. It includes the feature that combines infrequent categories.

        Defaults to 0.

    References
    ----------
    Three key functionalities are enabled in support vector regression(SVR), listed as follows:

        - :ref:`Model Evaluation and Parameter Selection<param_select-label>`
        - :ref:`Successive Halving and Hyperband Method for Parameter Selection<sha_hyperband-label>`
        - :ref:`Model Compression<model_compression_svm-label>`

    Please refer to the links above for detailed description about each functionality together with
    relevant parameters.

    Attributes
    ----------
    model_ : DataFrame
        Model content.

    stat_ : DataFrame
        Statistics content.

    Examples
    --------
    Create a SVR instance and call the fit():

    >>> svr = SVR(kernel='linear', scale_info='standardization', scale_label=True, handle_missing=False)
    >>> svr.fit(data=df_fit, key='ID', features=['F1', 'F2'])
    >>> res = svr.predict(data=df_predict, key='ID', features=['F1', 'F2'])
    >>> res.collect()

    """
    #pylint:disable=too-many-arguments
    resampling_method_list = ['cv', 'bootstrap', 'cv_sha', 'bootstrap_sha',
                              'cv_hyperband', 'bootstrap_hyperband']
    evaluation_metric_list = ['RMSE']
    def __init__(self, c=None, kernel='rbf', degree=None,
                 gamma=None, coef_lin=None, coef_const=None, shrink=True,
                 tol=None, evaluation_seed=None, thread_ratio=None, scale_info=None,
                 scale_label=None, handle_missing=True, categorical_variable=None,
                 category_weight=None, regression_eps=None,
                 compression=None, max_bits=None, max_quantization_iter=None,
                 resampling_method=None, fold_num=None,
                 repeat_times=None, search_strategy=None, random_search_times=None, random_state=None,
                 timeout=None, progress_indicator_id=None, param_values=None, param_range=None,
                 reduction_rate=None, aggressive_elimination=None,
                 onehot_min_frequency=None, onehot_max_categories=None):
        #pylint:disable=too-many-locals
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(SVR, self).__init__(2, c, kernel, degree, gamma,
                                  coef_lin, coef_const, False, shrink, tol, evaluation_seed,
                                  thread_ratio, None, scale_info, scale_label,
                                  handle_missing, categorical_variable, category_weight,
                                  regression_eps,
                                  compression, max_bits, max_quantization_iter,
                                  resampling_method=resampling_method, evaluation_metric='RMSE',
                                  fold_num=fold_num, repeat_times=repeat_times, search_strategy=search_strategy,
                                  random_search_times=random_search_times, random_state=random_state, timeout=timeout,
                                  progress_indicator_id=progress_indicator_id, param_values=param_values,
                                  param_range=param_range, reduction_rate=reduction_rate,
                                  aggressive_elimination=aggressive_elimination,
                                  onehot_min_frequency=onehot_min_frequency,
                                  onehot_max_categories=onehot_max_categories)
        self.op_name = 'SVM_Regressor'

    def fit(self, data, key=None, features=None, label=None, categorical_variable=None):
        """
        Fit the model to the training dataset.

        Parameters
        ----------
        data : DataFrame
            DataFrame containing the data.
        key : str, optional
            Name of the ID column.

            If ``key`` is not provided, then:

                - if ``data`` is indexed by a single column, then ``key`` defaults
                  to that index column;

                - otherwise, it is assumed that ``data`` contains no ID column.

        features : a list of str, optional
            Names of the feature columns.

            If ``features`` is not provided, it defaults to all the
            non-ID, non-label columns.
        label : str, optional
            Name of the label column.

            If ``label`` is not provided, it defaults to the
            last non-ID column.
        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.

        Returns
        -------
        A fitted object of class "SVR".
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        self._fit(data=data, key=key, features=features, label=label, categorical_variable=categorical_variable)
        return self

    def predict(self, data, key=None, features=None):#pylint:disable=too-many-locals
        """
        Predict dependent variable values based on a fitted model.

        Parameters
        ----------
        data : DataFrame
            DataFrame containing the data.
        key : str, optional
            Name of the ID column.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        features : a list of str, optional
            Names of the feature columns.

            If ``features`` is not provided, it defaults to
            all the non-ID columns.

        Returns
        -------

        DataFrame
            Predict result, structured as follows:

              - ID column, with the same name and type as ``data1`` 's ID column.
              - SCORE, type NVARCHAR(100), prediction value.
              - PROBABILITY, type DOUBLE, prediction probability.
                Always NULL. This column is only used for SVC and SVRanking.
        """
        return self._predict(data=data, key=key, features=features, qid=None)

    def score(self, data, key=None, features=None, label=None):
        """
        Returns the coefficient of determination R2 of the prediction.

        Parameters
        ----------
        data : DataFrame
            DataFrame containing the data.
        key : str, optional
            Name of the ID column.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        features : a list of str, optional
            Names of the feature columns.

            If ``features`` is not provided, it defaults to all the non-ID
            and non-label columns.

        label : str, optional
            Name of the label column.

            If ``label`` is not provided, it defaults to the last non-ID column.

        Returns
        -------
        float
            Returns the coefficient of determination R2 of the prediction.
        """
        setattr(self, 'hanaml_score_params', pal_param_register())
        setattr(self, 'testing_data', data)
        conn = data.connection_context
        require_pal_usable(conn)
        if getattr(self, 'model_') is None:
            raise FitIncompleteError()

        # SQLTRACE
        conn.sql_tracer.set_sql_trace(self, self.__class__.__name__, 'Score')

        #return scalar value of accuracy after calling predict
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
        unique_id = str(uuid.uuid1())
        unique_id = unique_id.replace('-', '_').upper()
        cols = data.columns
        cols.remove(key)
        if label is None:
            label = data.columns[-1]
        cols.remove(label)
        if features is None:
            features = cols
        prediction = self.predict(data, key, features)
        prediction = prediction.select([key, 'SCORE']).rename_columns(['ID_P', 'PREDICTION'])
        original = data[[key, label]].rename_columns(['ID_A', 'ACTUAL'])
        joined = original.join(prediction, 'ID_P=ID_A').select('ACTUAL', 'PREDICTION')
        r2 = metrics.r2_score(joined,
                              label_true='ACTUAL',
                              label_pred='PREDICTION')
        setattr(self, 'score_metrics_', {"R2": r2})
        return r2

class SVRanking(_SVMBase):
    r"""
    Support Vector Machines (SVMs) refer to a family of supervised learning models using the concept of support vector.

    Compared with many other supervised learning models, SVMs have the advantages in that the models produced by SVMs can be either linear or non-linear, where the latter is realized by a technique called Kernel Trick.

    Like most supervised models, there are training phase and testing phase for SVMs. In the training phase, a function f(x):->y where f(∙) is a function (can be non-linear) mapping a sample onto a TARGET, is learnt. The training set consists of pairs denoted by {xi, yi}, where x denotes a sample represented by several attributes, and y denotes a TARGET (supervised information). In the testing phase, the learnt f(∙) is further used to map a sample with unknown TARGET onto its predicted TARGET.

    SVRanking implements a pairwise "learning to rank" algorithm which learns a ranking function from several sets (distinguished by Query ID) of ranked samples. In the scenario of ranking, f(∙) refers to ranking function, and TARGET refers to score, according to which the final ranking is made. For pairwise ranking, f(∙) is learnt so that the pairwise relationship expressing the rank of the samples within each set is considered.

    Parameters
    ----------

    c : float, optional
        Trade-off between training error and margin.
        Value range > 0.

        Defaults to 100.

    kernel : {'linear', 'poly', 'rbf', 'sigmoid'}, optional
        Specifies the kernel type to be used in the algorithm.

        Defaults to 'rbf'.

    degree : int, optional
        Coefficient for the 'poly' kernel type.
        Value range >= 1.

        Defaults to 3.

    gamma : float, optional
        Coefficient for the 'rbf' kernel type.

        Defaults to to 1.0/number of features in the dataset.

        Only valid when ``kernel`` is 'rbf'.

    coef_lin : float, optional
        Coefficient for the 'poly'/'sigmoid' kernel type.

        Defaults to 0.

    coef_const : float, optional
        Coefficient for the 'poly'/'sigmoid' kernel type.

        Defaults to 0.

    probability : bool, optional
        If True, output probability during prediction.

        Defaults to False.

    shrink : bool, optional
        If True, use shrink strategy.

        Defaults to True.

    tol : float, optional
        Specifies the error tolerance in the training process.
        Value range > 0.

        Defaults to 0.001.

    evaluation_seed : int, optional
        The random seed in parameter selection.
        Value range >= 0.

        Defaults to 0.

    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 0.0.

    scale_info : {'no', 'standardization', 'rescale'}, optional
        Options:

          - 'no' : No scale.
          - 'standardization' : Transforms the data to have zero mean
            and unit variance.
          - 'rescale' : Rescales the range of the features to scale the
            range in [-1,1].

        Defaults to 'standardization'.

    handle_missing : bool, optional
        Whether to handle missing values:
            * False: No,

            * True: Yes.

        Defaults to True.

    categorical_variable : str or a list of str, optional
        Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

        No default value.

    category_weight : float, optional
        Represents the weight of category attributes.
        Value range > 0.

        Defaults to 0.707.

    compression : bool, optional

        Specifies if the model is stored in compressed format.

        Default value depends on the SAP HANA Version. Please refer to the corresponding documentation of SAP HANA PAL.

    max_bits : int, optional
        The maximum number of bits to quantize continuous features, equivalent to
        use :math:`2^{max\_bits}` bins.

        Must be less than 31.

        Valid only when the value of compression is True.

        Defaults to 12.

    max_quantization_iter : int, optional
        The maximum iteration steps for quantization.

        Valid only when the value of compression is True.

        Defaults to 1000.

    resampling_method : str, optional
        Specifies the resampling method for model evaluation or parameter selection.

          - 'cv'
          - 'cv_sha'
          - 'cv_hyperband'
          - 'bootstrap'
          - 'bootstrap_sha'
          - 'bootstrap_hyperband'

        If no value is specified for this parameter, neither model evaluation nor parameter selection is activated.

        No default value.

        .. note::

            Resampling methods that end with 'sha' or 'hyperband' are used for
            parameter selection only, not for model evaluation.
    fold_num : int, optional
        Specifies the fold number for the cross validation method.

        Mandatory and valid only when ``resampling_method`` is set to 'cv', 'cv_sha' or 'cv_hyperband'.

        No default value.

    repeat_times : int, optional
        Specifies the number of repeat times for resampling.

        Default to 1.

    search_strategy : str, optional
        Specify the parameter search method:

          - 'grid'
          - 'random'

        Mandatory when ``resampling`` method is set to 'cv_sha' or 'bootstrap_sha'.

        Defaults to ``random`` and cannot be changed if ``resampling_method`` is set to 'cv_hyperband' or
        'bootstrap_hyperband'; otherwise no default value, and parameter selection cannot be activated
        if not specified.

    random_search_times : int, optional
        Specifies the number of times to randomly select candidate parameters for selection.

        Mandatory when ``search_strategy`` is set to 'random', or when ``resampling_method``
        is set to 'cv_hyperband' or 'bootstrap_hyperband'.

        No default value.

    random_state : int, optional
        Specifies the seed for random generation. Use system time when 0 is specified.
        Default to 0.

    timeout : int, optional
        Specifies maximum running time for model evaluation or parameter selection, in seconds.
        No timeout when 0 is specified.

        Default to 0.

    progress_indicator_id : str, optional
        Sets an ID of progress indicator for model evaluation or parameter selection.
        No progress indicator is active if no value is provided.

        No default value.

    param_values : dict or list of tuple, optional
        Sets the values of following parameters for model parameter selection:

            ``coef_lin``, ``coef_const``, ``c``.

        If input is list of tuple, then each tuple should contain exactly two elements:

            - 1st element is the parameter name(str type),
            - 2nd element is a list of valid values for that parameter.

        Otherwise, if input is dict, then the key of each element must specify a parameter name,
        while the corresponding value specifies a list of values for that parameter.

        A simple example for illustration:

            [('c', [0.1, 0.2, 0.5]), ('coef_const', [0.2, 0.6])],

        or

            {'c' : [0.1, 0.2, 0.5], 'coef_const' : [0.2, 0.6]}

        Valid only when ``resampling_method`` and ``search_strategy`` are both specified.

        No default value.

    param_range : dict or list of tuple, optional
        Sets the range of the following parameters for model parameter selection:

            ``coef_lin``, ``coef_const``, ``c``.

        If input is list of tuple, then each tuple should contain exactly two elements:

            - 1st element is the parameter name(str type),
            - 2nd element is a list that specifies the range of that parameter as [start, step, end].

        Otherwise, if input is dict, then the key of each element must specify a parameter name,
        while the corresponding value specifies the range of that parameter.

        Valid only when ``resampling_method`` and ``search_strategy`` are both specified.

        No default value.

    reduction_rate : float, optional
        Specifies reduction rate in SHA or Hyperband method.

        For each round, the available parameter candidate size will be divided by value of this parameter.
        Thus valid value for this parameter must be greater than 1.0

        Valid only when ``resampling_method`` is set to one of the following values:
        'cv_sha', 'bootstrap_sha', 'cv_hyperband', 'bootstrap_hyperband'.

        Defaults to 3.0.

    aggressive_elimination : bool, optional
        Specifies whether to apply aggressive elimination while using SHA method.

        Aggressive elimination happens when the data size and parameters size to be searched does not match
        and there are still bunch of parameters to be searched while data size reaches its upper limits.
        If aggressive elimination is applied, lower bound of limit of data size will be used multiple times
        first to reduce number of parameters.

        Valid only when ``resampling_method`` is set to 'cv_sha' or 'bootstrap_sha'.

        Defaults to False.

    onehot_min_frequency :  int, optional
        Specifies the minimum frequency below which a category will be considered infrequent.

        Defaults to 1.

    onehot_max_categories : int, optional
        Specifies an upper limit to the number of output features for each input feature. It includes the feature that combines infrequent categories.

        Defaults to 0.

    References
    ----------
    Three key functionalities are enabled in support vector ranking(SVRanking), listed as follows:

        - :ref:`Model Evaluation and Parameter Selection<param_select-label>`
        - :ref:`Successive Halving and Hyperband Method for Parameter Selection<sha_hyperband-label>`
        - :ref:`Model Compression<model_compression_svm-label>`

    Please refer to the links above for detailed description about each functionality together with
    relevant parameters.

    Attributes
    ----------
    model_ : DataFrame
        Model content.

    stat_ : DataFrame
        Statistics content.

    Examples
    --------
    >>> svranking = SVRanking(gamma=0.005)
    >>> svranking.fit(data=df_fit, key='ID', qid='QID', features=['F1', 'F2'], label='LABEL')
    >>> res = svranking.predict(data=df_predict, key='ID', features=['F1', 'F2'], qid='QID')
    >>> res.collect()

    """
    #pylint:disable=too-many-arguments
    resampling_method_list = ['cv', 'bootstrap', 'cv_sha', 'bootstrap_sha',
                              'cv_hyperband', 'bootstrap_hyperband']
    evaluation_metric_list = ['ERROR_RATE']
    def __init__(self, c=None, kernel='rbf', degree=None,
                 gamma=None, coef_lin=None, coef_const=None, probability=False, shrink=True,
                 tol=None, evaluation_seed=None, thread_ratio=None, scale_info=None, handle_missing=True,
                 categorical_variable=None, category_weight=None,
                 compression=None, max_bits=None, max_quantization_iter=None,
                 resampling_method=None,
                 fold_num=None, repeat_times=None, search_strategy=None, random_search_times=None,
                 random_state=None, timeout=None, progress_indicator_id=None,
                 param_values=None, param_range=None, reduction_rate=None,
                 aggressive_elimination=None,
                 onehot_min_frequency=None, onehot_max_categories=None):
        #pylint:disable=too-many-locals
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(SVRanking, self).__init__(3, c, kernel, degree, gamma, coef_lin,
                                        coef_const, probability, shrink, tol, evaluation_seed,
                                        thread_ratio, None, scale_info, None, handle_missing, categorical_variable,
                                        category_weight, None,
                                        compression, max_bits, max_quantization_iter,
                                        resampling_method=resampling_method, evaluation_metric='ERROR_RATE',
                                        fold_num=fold_num, repeat_times=repeat_times, search_strategy=search_strategy,
                                        random_search_times=random_search_times, random_state=random_state, timeout=timeout,
                                        progress_indicator_id=progress_indicator_id, param_values=param_values,
                                        param_range=param_range, reduction_rate=reduction_rate,
                                        aggressive_elimination=aggressive_elimination,
                                        onehot_min_frequency=onehot_min_frequency,
                                        onehot_max_categories=onehot_max_categories)

    def fit(self, data, key=None, qid=None, features=None, label=None, categorical_variable=None):
        """
        Fit the model to the training dataset.

        Parameters
        ----------
        data : DataFrame
            DataFrame containing the data.
        key : str, optional
            Name of the ID column.

            If ``key`` is not provided, then:

                - if ``data`` is indexed by a single column, then ``key`` defaults
                  to that index column;

                - otherwise, it is assumed that ``data`` contains no ID column.

        qid : str
            Name of the qid column.
        features : a list of str, optional
            Names of the feature columns.

            If ``features`` is not provided, it defaults to all
            the non-ID, non-label, non-qid columns.
        label : str, optional
            Name of the label column.

            If ``label`` is not provided, it defaults to the last non-ID, non-qid column.
        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.

        Returns
        -------
        A fitted object of class "SVRanking".
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        self._fit(data=data, key=key, features=features, label=label, qid=qid, categorical_variable=categorical_variable)
        return self

    def predict(self, data, key=None, qid=None, features=None, verbose=False):#pylint:disable=too-many-locals
        """
        Predict dependent variable values based on a fitted model.

        Parameters
        ----------
        data : DataFrame
            DataFrame containing the data.
        key : str, optional
            Name of the ID column.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        qid : str
            Name of the qid column.
        features : a list of str, optional
            Names of the feature columns.

            If ``features`` is not provided, it defaults to all
            the non-ID, non-qid columns.
        verbose : bool, optional
            If True, output scoring probabilities for each class.

            Defaults to False.

        Returns
        -------
        DataFrame
            Predict result, structured as follows:

                - ID column, with the same name and type as ``data``'s ID column.
                - SCORE, type NVARCHAR(100), prediction value.
                - PROBABILITY, type DOUBLE, prediction probability.
                  It is NULL when ``probability`` is False during
                  instance creation.

        .. note::

            PAL will throw an error if probability=True in the
            constructor and verbose=True is not provided to predict().
            This is a known bug.
        """
        return self._predict(data=data, key=key, features=features, verbose=verbose, qid=qid)

class OneClassSVM(_SVMBase):
    r"""
    Support Vector Machines (SVMs) refer to a family of supervised learning models using the concept of support vector.

    Compared with many other supervised learning models, SVMs have the advantages in that the models produced by SVMs can be either linear or non-linear, where the latter is realized by a technique called Kernel Trick.

    Like most supervised models, there are training phase and testing phase for SVMs. In the training phase, a function f(x):->y where f(∙) is a function (can be non-linear) mapping a sample onto a TARGET, is learnt. The training set consists of pairs denoted by {xi, yi}, where x denotes a sample represented by several attributes, and y denotes a TARGET (supervised information). In the testing phase, the learnt f(∙) is further used to map a sample with unknown TARGET onto its predicted TARGET.

    One class SVM is an unsupervised algorithm that learns a decision function for outlier detection: classifying new data as similar or different to the training set. In One class SVM scenario, f(∙) refers to decision function, and there is no TARGET needed since it is unsupervised.

    Parameters
    ----------

    c : float, optional
        Trade-off between training error and margin.
        Value range > 0.

        Defaults to 100.0.

    kernel : {'linear', 'poly', 'rbf', 'sigmoid'}, optional
        Specifies the kernel type to be used in the algorithm.

        Defaults to 'rbf'.

    degree : int, optional
        Coefficient for the poly kernel type.
        Value range >= 1.

        Defaults to 3.

    gamma : float, optional
        Coefficient for the 'rbf' kernel type.

        Defaults to to 1.0/number of features in the dataset.

        Only valid when ``kernel`` is 'rbf'.

    coef_lin : float, optional
        Coefficient for the 'poly'/'sigmoid' kernel type.

        Defaults to 0.

    coef_const : float, optional
        Coefficient for the 'poly'/'sigmoid' kernel type.

        Defaults to 0.

    shrink : bool, optional
        If True, use shrink strategy.

        Defaults to True.

    tol : float, optional
        Specifies the error tolerance in the training process. Value range > 0.

        Defaults to 0.001.

    evaluation_seed : int, optional
        The random seed in parameter selection. Value range >= 0.

        Defaults to 0.

    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 0.0.

    nu : float, optional
        The value for both the upper bound of the fraction of training errors
        and the lower bound of the fraction of support vectors.

        Defaults to 0.5.

    scale_info : {'no', 'standardization', 'rescale'}, optional
        Options:

          - 'no' : No scale.
          - 'standardization' : Transforms the data to have zero mean
            and unit variance.
          - 'rescale' : Rescales the range of the features to scale the
            range in [-1,1].

        Defaults to 'standardization'.

    handle_missing : bool, optional
        Whether to handle missing values:

            False: No,
            True: Yes.

        Defaults to True.

    categorical_variable : str or a list of str, optional
        Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

        No default value.

    category_weight : float, optional
        Represents the weight of category attributes.
        Value range > 0.

        Defaults to 0.707.

    compression : bool, optional
       Specifies if the model is stored in compressed format.

        Default value depends on the SAP HANA Version. Please refer to the corresponding documentation of SAP HANA PAL.

    max_bits : int, optional
        The maximum number of bits to quantize continuous features, equivalent to
        use :math:`2^{max\_bits}` bins.


        Must be less than 31.

        Valid only when the value of compression is True.

        Defaults to 12.

    max_quantization_iter : int, optional
        The maximum iteration steps for quantization.

        Valid only when the value of compression is True.

        Defaults to 1000.

    resampling_method : str, optional
        Specifies the resampling method for model evaluation or parameter selection.

          - 'cv'
          - 'cv_sha'
          - 'cv_hyperband'
          - 'bootstrap'
          - 'bootstrap_sha'
          - 'bootstrap_hyperband'

        If no value is specified for this parameter, neither model evaluation nor parameter selection is activated.

        No default value.

        .. note::
            Resampling methods that end with 'sha' or 'hyperband' are used for
            parameter selection only, not for model evaluation.
    fold_num : int, optional
        Specifies the fold number for the cross validation method.

        Mandatory and valid only when ``resampling_method`` is set to 'cv', 'cv_sha' or 'cv_hyperband'.

        No default value.

    repeat_times : int, optional
        Specifies the number of repeat times for resampling.

        Default to 1.

    search_strategy : str, optional
        Specify the parameter search method:

          - 'grid'
          - 'random'

        Mandatory when ``resampling`` method is set to 'cv_sha' or 'bootstrap_sha'.

        Defaults to ``random`` and cannot be changed if ``resampling_method`` is set to 'cv_hyperband' or
        'bootstrap_hyperband'; otherwise no default value, and parameter selection cannot be activated
        if not specified.

    random_search_times : int, optional
        Specifies the number of times to randomly select candidate parameters for selection.

        Mandatory when ``search_strategy`` is set to 'random', or when ``resampling_method``
        is set to 'cv_hyperband' or 'bootstrap_hyperband'.

        No default value.

    random_state : int, optional
        Specifies the seed for random generation. Use system time when 0 is specified.

        Default to 0.

    timeout : int, optional
        Specifies maximum running time for model evaluation or parameter selection, in seconds.

        No timeout when 0 is specified.

        Default to 0.

    progress_indicator_id : str, optional
        Sets an ID of progress indicator for model evaluation or parameter selection.
        No progress indicator is active if no value is provided.

        No default value.

    param_values : dict or list of tuple, optional
        Sets the values of following parameters for model parameter selection:

            ``nu``, ``c``.

        If input is list of tuple, then each tuple should contain exactly two elements:

            - 1st element is the parameter name(str type),
            - 2nd element is a list of valid values for that parameter.

        Otherwise, if input is dict, then the key of each element must specify a parameter name,
        while the corresponding value specifies a list of values for that parameter.

        A simple example for illustration:

            [('c', [0.1, 0.2, 0.5]), ('nu', [0.2, 0.6])]

        or

            {'c' : [0.1, 0.2, 0.5], 'nu' : [0.2, 0.6]}

        Valid only when ``resampling_method`` and ``search_strategy`` are both specified.

        No default value.

    param_range : dict or list of tuple, optional
        Sets the range of the following parameters for model parameter selection:

            ``nu``, ``c``.

        If input is list of tuple, then each tuple should contain exactly two elements:

            - 1st element is the parameter name(str type),
            - 2nd element is a list that specifies the range of that parameter as [start, step, end].

        Otherwise, if input is dict, then the key of each element must specify a parameter name,
        while the corresponding value specifies the range of that parameter.

        Valid only when ``resampling_method`` and ``search_strategy`` are both specified.

        No default value.

    reduction_rate : float, optional
        Specifies reduction rate in SHA or Hyperband method.

        For each round, the available parameter candidate size will be divided by value of this parameter.
        Thus valid value for this parameter must be greater than 1.0

        Valid only when ``resampling_method`` is set to one of the following values:
        'cv_sha', 'bootstrap_sha', 'cv_hyperband', 'bootstrap_hyperband'.

        Defaults to 3.0.

    aggressive_elimination : bool, optional
        Specifies whether to apply aggressive elimination while using SHA method.

        Aggressive elimination happens when the data size and parameters size to be searched does not match
        and there are still bunch of parameters to be searched while data size reaches its upper limits.
        If aggressive elimination is applied, lower bound of limit of data size will be used multiple times
        first to reduce number of parameters.

        Valid only when ``resampling_method`` is set to 'cv_sha' or 'bootstrap_sha'.

        Defaults to False.

    onehot_min_frequency :  int, optional
        Specifies the minimum frequency below which a category will be considered infrequent.

        Defaults to 1.

    onehot_max_categories : int, optional
        Specifies an upper limit to the number of output features for each input feature. It includes the feature that combines infrequent categories.

        Defaults to 0.

    References
    ----------
    Three key functionalities are enabled in one-class support vector machine(OneClassSVM), listed as follows:

        - :ref:`Model Evaluation and Parameter Selection<param_select-label>`
        - :ref:`Successive Halving and Hyperband Method for Parameter Selection<sha_hyperband-label>`
        - :ref:`Model Compression<model_compression_svm-label>`

    Please refer to the links above for detailed description about each functionality together with
    relevant parameters.

    Attributes
    ----------
    model_ : DataFrame
        Model content.

    stat_ : DataFrame
        Statistics content.

    Examples
    --------
    >>> svc_one = OneClassSVM(scale_info='no', category_weight=1)
    >>> svc_one.fit(data=df_fit, key='ID', features=['F1', 'F2'])
    >>> res = svc_one.predict(data=df_predict, key='ID', features=['F1', 'F2'])
    >>> res.collect()

    """
    #pylint:disable=too-many-arguments
    resampling_method_list = ['cv', 'bootstrap', 'cv_sha', 'bootstrap_sha',
                              'cv_hyperband', 'bootstrap_hyperband']
    evaluation_metric_list = ['ACCURACY']
    def  __init__(self, c=None, kernel='rbf', degree=None, gamma=None,
                  coef_lin=None, coef_const=None, shrink=True, tol=None,
                  evaluation_seed=None, thread_ratio=None, nu=None, scale_info=None,
                  handle_missing=True, categorical_variable=None, category_weight=None,
                  compression=None, max_bits=None, max_quantization_iter=None,
                  resampling_method=None,
                  fold_num=None, repeat_times=None, search_strategy=None, random_search_times=None,
                  random_state=None, timeout=None, progress_indicator_id=None,
                  param_values=None, param_range=None, reduction_rate=None,
                  aggressive_elimination=None,
                  onehot_min_frequency=None, onehot_max_categories=None):
        #pylint:disable=too-many-locals
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(OneClassSVM, self).__init__(4, c, kernel, degree, gamma, coef_lin,
                                          coef_const, False, shrink, tol, evaluation_seed,
                                          thread_ratio, nu, scale_info, None, handle_missing, categorical_variable,
                                          category_weight, None,
                                          compression, max_bits, max_quantization_iter,
                                          resampling_method=resampling_method, evaluation_metric='ACCURACY',
                                          fold_num=fold_num, repeat_times=repeat_times, search_strategy=search_strategy,
                                          random_search_times=random_search_times, random_state=random_state, timeout=timeout,
                                          progress_indicator_id=progress_indicator_id, param_values=param_values,
                                          param_range=param_range, reduction_rate=reduction_rate,
                                          aggressive_elimination=aggressive_elimination,
                                          onehot_min_frequency=onehot_min_frequency,
                                          onehot_max_categories=onehot_max_categories)

    def fit(self, data, key=None, features=None, categorical_variable=None):
        """
        Fit the model to the training dataset.

        Parameters
        ----------
        data : DataFrame
            DataFrame containing the data.
        key : str, optional
            Name of the ID column.

            If ``key`` is not provided, then:

            - if ``data`` is indexed by a single column, then ``key`` defaults
              to that index column;

            - otherwise, it is assumed that ``data`` contains no ID column.

        features : a list of str, optional
            Names of the feature columns.

            If ``features`` is not provided, it defaults to all
            the non-ID columns.
        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.

        Returns
        -------
        A fitted object of class "OneClassSVM".
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        self._fit(data=data, key=key, features=features, label=None, categorical_variable=categorical_variable)
        return self

    def predict(self, data, key=None, features=None):#pylint:disable=too-many-locals
        """
        Predict dependent variable values based on a fitted model.

        Parameters
        ----------
        data : DataFrame
            DataFrame containing the data.
        key : str, optional
            Name of the ID column.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        features : a list of str, optional
            Names of the feature columns.

            If ``features`` is not provided, it defaults to all
            the non-ID columns.

        Returns
        -------
        DataFrame
            Predict result.
        """
        return self._predict(data=data, key=key, features=features)
