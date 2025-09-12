"""
This module contains Python wrappers for PAL Multi-layer Perceptron algorithm.

The following classes are available:

    * :class:`MLPClassifier`
    * :class:`MLPRegressor`
    * :class:`MLPMultiTaskClassifier`
    * :class:`MLPMultiTaskRegressor`
"""
#pylint: disable=too-many-arguments, too-many-lines
#pylint: disable=relative-beyond-top-level
#pylint: disable=line-too-long, unused-variable
#pylint: disable=consider-using-f-string, consider-iterating-dictionary
#pylint: disable=invalid-name
import logging
import uuid
import itertools
from hdbcli import dbapi
from hana_ml.dataframe import DataFrame
from hana_ml.ml_exceptions import FitIncompleteError
from hana_ml.ml_base import try_drop, quotename
from .utility import check_pal_function_exist
from .pal_base import (
    PALBase,
    ParameterTable,
    execute_logged,
    _TEXT_TYPES,
    _INT_TYPES,
    ListOfTuples,
    ListOfStrings,
    pal_param_register,
    require_pal_usable
)
from .sqlgen import trace_sql
from . import metrics
logger = logging.getLogger(__name__) #pylint: disable=invalid-name


class _MLPBase(PALBase):#pylint: disable=too-many-instance-attributes, too-few-public-methods
    """
    Base class for Multi-layer Perceptron.
    """
    activation_map = {
        'tanh' : 1,
        'linear' : 2,
        'sigmoid_asymmetric' : 3,
        'sigmoid_symmetric' : 4,
        'gaussian_asymmetric' : 5,
        'gaussian_symmetric' : 6,
        'elliot_asymmetric' : 7,
        'elliot_symmetric' : 8,
        'sin_asymmetric' : 9,
        'sin_symmetric' : 10,
        'cos_asymmetric' : 11,
        'cos_symmetric' : 12,
        'relu' : 13
    }
    #functionality_map = {'classification': 0, 'regression': 1}
    style_map = {'batch' : 0, 'stochastic' : 1}
    norm_map = {'no': 0, 'z-transform' : 1, 'scalar' : 2}
    weight_map = {'all-zeros' : 0, 'normal' : 1, 'uniform' : 2,
                  'variance-scale-normal' : 3, 'variance-scale-uniform' : 4}
    resampling_method_reg = ['cv', 'bootstrap', 'cv_sha', 'bootstrap_sha',
                             'cv_hyperband', 'bootstrap_hyperband']
    resampling_method_cls_add = ['stratified_cv', 'stratified_bootstrap',
                                 'stratified_cv_sha', 'stratified_cv_bootstrap',
                                 'stratified_bootstrap_sha', 'stratified_bootstrap_hyperband']
    search_strategy_list = ('grid', 'random')
    range_params_map = {'learning_rate' : 'LEARNING_RATE',
                        'momentum' : 'MOMENTUM_FACTOR',
                        'batch_size' : 'MINI_BATCH_SIZE'}
    pal_funcname = 'PAL_MULTILAYER_PERCEPTRON'
    #range_params = ('learning_rate', 'momentum', 'batch_size')
    def __init__(self,#pylint: disable=too-many-arguments, too-many-branches, too-many-statements, too-many-locals
                 functionality,
                 activation=None,
                 activation_options=None,
                 output_activation=None,
                 output_activation_options=None,
                 hidden_layer_size=None,
                 hidden_layer_size_options=None,
                 max_iter=None,
                 training_style=None,
                 learning_rate=None,
                 momentum=None,
                 batch_size=None,
                 normalization=None,
                 weight_init=None,
                 categorical_variable=None,
                 resampling_method=None,
                 evaluation_metric=None,
                 fold_num=None,
                 repeat_times=None,
                 search_strategy=None,
                 random_search_times=None,
                 random_state=None,
                 timeout=None,
                 progress_indicator_id=None,
                 param_values=None,
                 param_range=None,
                 thread_ratio=None,
                 reduction_rate=None,
                 aggressive_elimination=None):
        if not hasattr(self, 'hanaml_parameters'):
            setattr(self, 'hanaml_parameters', pal_param_register())
        #type checking
        super(_MLPBase, self).__init__()
        self.activation = self._arg('activation', activation,
                                    self.activation_map)
        act_options = self._arg('activation_options', activation_options,
                                ListOfStrings)
        if self.activation is None and act_options is None:
            msg = ("'activation' and 'activation_options' cannot both be None.")
            logger.error(msg)
            raise ValueError(msg)
        if act_options is not None:
            self.activation_options = []
            for act in act_options:
                if act not in list(self.activation_map.keys()):#pylint:disable=bad-option-value
                    msg = ("'{}' is an invalid activation function.".format(act))
                    logger.error(msg)
                    raise ValueError(msg)
                self.activation_options.append(self.activation_map[act])
            self.activation_options = str(self.activation_options).replace('[', '{').replace(']', '}')#pylint:disable=line-too-long
        else:
            self.activation_options = None
        self.output_activation = self._arg('output_activation', output_activation,
                                           self.activation_map)
        out_act_options = self._arg('output_activation_options',
                                    output_activation_options,
                                    ListOfStrings)
        if self.output_activation is None and out_act_options is None:
            msg = ("'output_activation' and 'output_activation_options' "+
                   "cannot both be None.")
            logger.error(msg)
            raise ValueError(msg)
        if out_act_options is not None:
            self.output_activation_options = []
            for act_out in out_act_options:
                if act_out not in list(self.activation_map.keys()):#pylint:disable=bad-option-value
                    msg = ("'{}' is an invalid activation function".format(act_out)+
                           " for output layer.")
                    logger.error(msg)
                    raise ValueError(msg)
                self.output_activation_options.append(self.activation_map[act_out])
            self.output_activation_options = str(self.output_activation_options).replace('[', '{').replace(']', '}')#pylint:disable=line-too-long
        else:
            self.output_activation_options = None
        if hidden_layer_size is not None:
            if isinstance(hidden_layer_size, (tuple, list)) and all(isinstance(x, _INT_TYPES) for x in hidden_layer_size):#pylint:disable=line-too-long
                self.hidden_layer_size = ','.join(str(x) for x in hidden_layer_size)
            else:
                msg = "Parameter 'hidden_layer_size' must be type of tuple/list of int."
                logger.error(msg)
                raise TypeError(msg)
        else:
            self.hidden_layer_size = None
        hls_options = self._arg('hidden_layer_size_options', hidden_layer_size_options,
                                ListOfTuples)
        if hls_options is not None:
            #self.hidden_layer_size_options = []
            for hls_option in hls_options:
                if not all(isinstance(x, _INT_TYPES) for x in hls_option):
                    msg = ("Valid option of 'hidden_layer_size' must be "+
                           "tuple of int, while provided options contain"+
                           " values of invalid types.")
                    logger.error(msg)
                    raise TypeError(msg)
            hls_options = str(hls_options).replace('[', '{').replace(']', '}')
            self.hidden_layer_size_options = hls_options.replace('(', '"').replace(')', '"')
        else:
            self.hidden_layer_size_options = None
        if self.hidden_layer_size is None and self.hidden_layer_size_options is None:
            msg = ("'hidden_layer_size' and 'hidden_layer_size_options' "+
                   "cannot both be None.")
            logger.error(msg)
            raise ValueError(msg)
        self.max_iter = self._arg('max_iter', max_iter, int)
        self.functionality = functionality
        self.training_style = self._arg('training_style', training_style,
                                        self.style_map)
        if isinstance(param_range, dict):
            param_range = [(x, param_range[x]) for x in param_range]
        if isinstance(param_values, dict):
            param_values = [(x, param_values[x]) for x in param_values]
        self.param_values = self._arg('param_values', param_values, ListOfTuples)
        self.param_range = self._arg('param_range', param_range, ListOfTuples)
        par_names = []
        if self.param_range is not None:
            par_names = par_names + [x[0] for x in self.param_range]
        if self.param_values is not None:
            par_names = par_names + [x[0] for x in self.param_values]
        self.learning_rate = self._arg('learning_rate', learning_rate, float,
            required=self.training_style in [None, 1] and 'learning_rate' not in par_names)
        self.momentum = self._arg('momentum', momentum, float,
            required=self.training_style in [None, 1] and 'momentum' not in par_names)
        self.batch_size = self._arg('batch_size', batch_size, int)
        self.normalization = self._arg('normalization', normalization, self.norm_map)
        self.weight_init = self._arg('weight_init', weight_init, self.weight_map)
        if categorical_variable is not None:
            if isinstance(categorical_variable, _TEXT_TYPES):
                categorical_variable = [categorical_variable]
            if isinstance(categorical_variable, list) and all(isinstance(x, _TEXT_TYPES)for x in categorical_variable):#pylint:disable=line-too-long
                self.categorical_variable = categorical_variable
            else:
                msg = "Parameter 'categorical_variable' must be type of str or list of str."
                logger.error(msg)
                raise TypeError(msg)
        else:
            self.categorical_variable = None
        self.thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        self.resampling_method = self._arg('resampling_method', resampling_method, str)
        resampling_method_list = self.resampling_method_reg + self.resampling_method_cls_add if functionality == 0 else self.resampling_method_reg#pylint:disable=line-too-long
        self.resampling_method = self._arg('resampling_method', resampling_method, {mtd:mtd for mtd in resampling_method_list})
        self.evaluation_metric = self._arg('evaluation_metric', evaluation_metric, self.evaluation_metric_map)#pylint:disable=no-member
        self.fold_num = self._arg('fold_num', fold_num, int, required='cv' in str(resampling_method))
        self.repeat_times = self._arg('repeat_times', repeat_times, int)
        #need some check work here
        self.search_strategy = self._arg('search_strategy', search_strategy, str,
                                         required='sha' in str(self.resampling_method))
        if 'hyperband' in str(self.resampling_method):
            self.search_strategy = 'random'
        if self.search_strategy is not None:
            if self.search_strategy not in self.search_strategy_list:
                msg = ("Search strategy '{}' is invalid ".format(self.search_strategy)+
                       "for parameter selection.")
                logger.error(msg)
                raise ValueError(msg)
        self.random_search_times = self._arg('random_search_times', random_search_times, int,
                                             required=self.search_strategy == 'random')
        self.random_state = self._arg('random_state', random_state, int)
        self.timeout = self._arg('timeout', timeout, int)
        self.progress_indicator_id = self._arg('progress_indicator_id', progress_indicator_id, str)
        self.reduction_rate = self._arg('reduction_rate', reduction_rate, float)
        self.aggressive_elimination = self._arg('aggressive_elimination', aggressive_elimination, bool)

        #Validating the input values for parameter selection
        #param values and range valid only when search strategy being specified
        #print("Current training style is {}......".format(self.training_style))
        if self.param_values is not None and self.search_strategy is not None:
            for x in self.param_values:#pylint:disable=invalid-name
                if len(x) != 2:#pylint:disable=bad-option-value
                    msg = ("Each tuple that specifies the values of a parameter should"+
                           " contain exactly 2 elements: 1st is parameter name,"+
                           " 2nd is a list of valid values.")
                    logger.error(msg)
                    raise ValueError(msg)
                if x[0] not in list(self.range_params_map.keys()):
                    msg = ("Specifying the values of '{}' for ".format(x[0])+
                           "parameter selection is invalid.")
                    logger.error(msg)
                    raise ValueError(msg)
                if self.training_style == 1 and  x[0] == 'batch_size':
                    if not (isinstance(x[1], list) and all(isinstance(t, _INT_TYPES) for t in x[1])):#pylint:disable=line-too-long
                        msg = "Valid values of 'batch_size' must be a list of int."
                        logger.error(msg)
                        raise TypeError(msg)
                if self.training_style == 1:
                    if not (isinstance(x[1], list) and all(isinstance(t, (float, int)) for t in x[1])):#pylint:disable=line-too-long
                        msg = ("Valid values of '{}' ".format(x[0])+
                               "must be a list of numerical values.")
                        logger.error(msg)
                        raise TypeError(msg)
                #else:
                #    print("Verified {} values......".format(x[0]))
        rsz = []
        if self.search_strategy is not None:
            if self.search_strategy == 'grid':
                rsz = [3]
            else:
                rsz = [2, 3]
        if self.param_range is not None and self.search_strategy is not None:
            for x in self.param_range:#pylint:disable=invalid-name
                if len(x) != 2:#pylint:disable=bad-option-value
                    msg = ("Each tuple that specifies the range of a parameter should contain"+
                           " exactly 2 elements: 1st is parameter name, 2nd is value range.")
                    logger.error(msg)
                    raise ValueError(msg)
                if x[0] not in list(self.range_params_map.keys()):
                    msg = ("Parameter '{}' is invalid for ".format(x[0])+
                           "range specification in parameter selection.")
                    logger.error(msg)
                    raise ValueError(msg)
                if x[0] == 'batch_size':
                    if not(isinstance(x[1], list) and len(x[1]) in rsz and all(isinstance(t, int) for t in x[1])):#pylint:disable=line-too-long
                        msg = ("The provided range of 'batch_size' is either not "+
                               "a list of int, or it contains wrong number of values.")
                        logger.error(msg)
                        raise TypeError(msg)
                if not (isinstance(x[1], list) and len(x[1]) in rsz and all(isinstance(t, (float, int)) for t in x[1])):#pylint:disable=line-too-long
                    msg = ("The provided range of '{}' is either not ".format(x[0])+
                           "a list of numerical values, or it contains the wrong number of values.")
                    logger.error(msg)
                    raise TypeError(msg)

    @trace_sql
    def _fit(self, data, key=None, features=None, label=None, categorical_variable=None):#pylint: disable=too-many-locals, too-many-statements, too-many-branches
        conn = data.connection_context

        has_id = False
        index_col = None
        #Do we need type check for key column and also check its existence in df?
        key = self._arg('key', key, str)
        if not self._disable_hana_execution:
            require_pal_usable(conn)
            index = data.index
            if isinstance(index, str):
                if key is not None and index != key:
                    msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                    "and the designated index column '{}'.".format(index)
                    logger.warning(msg)
            key = index if key is None else key
            if key is not None:
                has_id = True
                index_col = key
            cols_left = data.columns
            if label is None:
                label = data.columns[-1]
            if isinstance(label, _TEXT_TYPES):
                label = [label]
            cols_left = [x for x in cols_left if x not in label]
            if has_id:
                cols_left.remove(index_col)
            if features is None:
                features = cols_left
            used_cols = [col for col in itertools.chain([index_col], features, label)
                        if col is not None]
            training_df = data[used_cols]
        else:
            training_df = data
        if categorical_variable is not None:
            if isinstance(categorical_variable, _TEXT_TYPES):
                categorical_variable = [categorical_variable]
            if not (isinstance(categorical_variable, list) and all(isinstance(x, _TEXT_TYPES)for x in categorical_variable)):#pylint:disable=line-too-long
                msg = "Parameter 'categorial_variable' must be type of str or list of str."
                logger.error(msg)
                raise TypeError(msg)

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        outputs = model_tbl, log_tbl, stat_tbl, optimal_tbl = [
            '#MLP_{}_TBL_{}_{}'.format(name, self.id, unique_id) for name in ['MODEL',
                                                                              'TRAINING_LOG',
                                                                              'STATISTICS',
                                                                              'OPTIMAL_PARAM']
            ]

        param_rows = [('HIDDEN_LAYER_ACTIVE_FUNC', self.activation, None, None),
                      ('HIDDEN_LAYER_ACTIVE_FUNC_VALUES', None, None, self.activation_options),
                      ('OUTPUT_LAYER_ACTIVE_FUNC', self.output_activation, None, None),
                      ('OUTPUT_LAYER_ACTIVE_FUNC_VALUES', None, None, self.output_activation_options),
                      ('HIDDEN_LAYER_SIZE', None, None, self.hidden_layer_size),
                      ('HIDDEN_LAYER_SIZE_VALUES', None, None, self.hidden_layer_size_options),
                      ('HAS_ID', int(has_id), None, None),
                      ('MAX_ITERATION', self.max_iter, None, None),
                      ('FUNCTIONALITY', self.functionality, None, None),
                      ('TRAINING_STYLE', self.training_style, None, None),
                      ('LEARNING_RATE', None, self.learning_rate, None),
                      ('MOMENTUM_FACTOR', None, self.momentum, None),
                      ('MINI_BATCH_SIZE', self.batch_size, None, None),
                      ('NORMALIZATION', self.normalization, None, None),
                      ('WEIGHT_INIT', self.weight_init, None, None),
                      ('THREAD_RATIO', None, self.thread_ratio, None),
                      ('RESAMPLING_METHOD', None, None, self.resampling_method),
                      ('EVALUATION_METRIC', None, None, self.evaluation_metric),
                      ('FOLD_NUM', self.fold_num, None, None),
                      ('REPEAT_TIMES', self.repeat_times, None, None),
                      ('PARAM_SEARCH_STRATEGY', None, None, self.search_strategy),
                      ('RANDOM_SEARCH_TIMES', self.random_search_times, None, None),
                      ('SEED', self.random_state, None, None),
                      ('TIMEOUT', self.timeout, None, None),
                      ('REDUCTION_RATE', None, self.reduction_rate, None),
                      ('AGGRESSIVE_ELIMINATION', self.aggressive_elimination,
                       None, None)]

        if categorical_variable is not None:
            param_rows.extend([('CATEGORICAL_VARIABLE', None, None, variable)
                               for variable in categorical_variable])
        elif self.categorical_variable is not None:
            param_rows.extend([('CATEGORICAL_VARIABLE', None, None, variable)
                               for variable in self.categorical_variable])
        if not self._disable_hana_execution:
            if self.functionality == 0:
                param_rows.extend([('CATEGORICAL_VARIABLE', None, None, label[0])])
            param_rows.extend([('DEPENDENT_VARIABLE', None, None, name)
                              for name in label])
        if self.param_values is not None and self.search_strategy is not None:
            for x in self.param_values:#pylint:disable=invalid-name
                values = str(x[1]).replace('[', '{').replace(']', '}')
                param_rows.extend([(self.range_params_map[x[0]]+'_VALUES',
                                    None, None, values)])
        if self.param_range is not None and self.search_strategy is not None:
            for x in self.param_range:#pylint:disable=invalid-name
                range_ = str(x[1])
                if len(x[1]) == 2 and self.training_style == 'stochastic':
                    range_ = range_.replace(',', ',,')
                param_rows.extend([(self.range_params_map[x[0]]+'_RANGE',
                                    None, None, range_)])
        try:
            self._call_pal_auto(conn,
                                'PAL_MULTILAYER_PERCEPTRON',
                                training_df,
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
        self.model_ = conn.table(model_tbl)#pylint:disable=attribute-defined-outside-init
        self.stats_ = conn.table(stat_tbl)
        #pylint:disable=attribute-defined-outside-init
        self.statistics_ = self.stats_
        #pylint:disable=attribute-defined-outside-init
        self.train_log_ = conn.table(log_tbl)#pylint:disable=attribute-defined-outside-init
        self.optim_param_ = conn.table(optimal_tbl)#pylint:disable=attribute-defined-outside-init

    @trace_sql
    def _predict(self, data, key, features=None, thread_ratio=None, verbose_top_n=None):#pylint: disable=too-many-locals
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
        thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        verbose_top_n = self._arg('verbose_top_n', verbose_top_n, int)

        unique_id = str(uuid.uuid1())
        unique_id = unique_id.replace('-', '_').upper()
        mlp_type = 'MLPCLASSIFIER' if self.functionality == 0 else 'MLPREGRESSOR'
        # 'key' is necessary for prediction data(PAL default: key, Feature)
        cols = data.columns
        cols.remove(key)
        if features is None:
            features = cols
        data_ = data[[key] + features]
        param_tbl, result_tbl, soft_max_tbl = [
            '#{}_{}_TBL_{}_{}'.format(mlp_type, name, self.id, unique_id)
            for name in ['PREDICT_CONTROL',
                         'PREDICT_RESULT',
                         'SOFT_MAX'
                        ]
            ]
        out_tables = [result_tbl, soft_max_tbl]
        param_rows = [('THREAD_RATIO', None, thread_ratio, None),
                      ('VERBOSE_TOP_N', verbose_top_n, None, None)]

        try:
            self._call_pal_auto(conn,
                                'PAL_MULTILAYER_PERCEPTRON_PREDICT',
                                data_,
                                self.model_,
                                ParameterTable(param_tbl).with_data(param_rows),
                                *out_tables)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, out_tables)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, out_tables)
            raise
        return (conn.table(result_tbl),
                conn.table(soft_max_tbl))

    def create_model_state(self, model=None, function=None,
                           pal_funcname='PAL_MULTILAYER_PERCEPTRON',
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

            A placeholder parameter, not effective for Multilayer Perceptron.

        pal_funcname : int or str, optional
            PAL function name.

            Defaults to 'PAL_MULTILAYER_PERCEPTRON'.

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

class MLPClassifier(_MLPBase):
    """
    Multi-layer perceptron (MLP) Classifier.

    Parameters
    ----------

    activation : str

        Specifies the activation function for the hidden layer.

        Valid activation functions include:
          - 'tanh',
          - 'linear',
          - 'sigmoid_asymmetric',
          - 'sigmoid_symmetric',
          - 'gaussian_asymmetric',
          - 'gaussian_symmetric',
          - 'elliot_asymmetric',
          - 'elliot_symmetric',
          - 'sin_asymmetric',
          - 'sin_symmetric',
          - 'cos_asymmetric',
          - 'cos_symmetric',
          - 'relu'

        Should not be specified only if ``activation_options`` is provided.

    activation_options : list of str, optional

        A list of activation functions for parameter selection.

        See ``activation`` for the full set of valid activation functions.

    output_activation : str

        Specifies the activation function for the output layer.

        Valid activation functions same as those in ``activation``.

        Should not be specified only if ``output_activation_options`` is provided.

    output_activation_options : list of str, optional

        A list of activation functions for the output layer for parameter selection.

        See ``activation`` for the full set of activation functions.

    hidden_layer_size : list of int or tuple of int

        Sizes of all hidden layers.

        Should not be specified only if ``hidden_layer_size_options`` is provided.

    hidden_layer_size_options : list of tuples, optional

        A list of optional sizes of all hidden layers for parameter selection.

    max_iter : int, optional

        Maximum number of iterations.

        Defaults to 100.

    training_style : {'batch', 'stochastic'}, optional

        Specifies the training style.

        Defaults to 'stochastic'.

    learning_rate : float, optional

        Specifies the learning rate.
        Mandatory and valid only when ``training_style`` is 'stochastic'.

    momentum : float, optional

        Specifies the momentum for gradient descent update.
        Mandatory and valid only when ``training_style`` is 'stochastic'.

    batch_size : int, optional

        Specifies the size of mini batch.
        Valid only when ``training_style`` is 'stochastic'.

        Defaults to 1.

    normalization : {'no', 'z-transform', 'scalar'}, optional

        Defaults to 'no'.

    weight_init : {'all-zeros', 'normal', 'uniform', 'variance-scale-normal', 'variance-scale-uniform'}, optional

        Specifies the weight initial value.

        Defaults to 'all-zeros'.

    categorical_variable : str or a list of str, optional
        Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

        No default value.
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
    evaluation_metric : {'accuracy','f1_score', 'auc_1vsrest', 'auc_pairwise'}, optional

        Specifies the evaluation metric for model evaluation or parameter selection.

        Must be specified together with ``resampling_method`` to activate model evaluation or
        parameter selection.

        No default value.

    fold_num : int, optional

        Specifies the fold number for the cross-validation.

        Mandatory and valid only when ``resampling_method`` is specified to be one of the following:
        'cv', 'stratified_cv', 'cv_sha', 'stratified_cv_sha', 'cv_hyperband', 'stratified_cv_hyperband'.

    repeat_times : int, optional

        Specifies the number of repeat times for resampling.

        Defaults to 1.

    search_strategy : {'grid', 'random'}, optional

        Specifies the method for parameter selection.

        - mandatory if ``resampling_method`` is specified with suffix 'sha'
        - defaults to 'random' and cannot be changed if ``resampling_method`` is specified with suffix 'hyperband'
        - otherwise no default value, and parameter selection will not be activated if not specified

    random_search_times : int, optional

        Specifies the number of times to randomly select candidate parameters.

        Mandatory and valid only when ``search_strategy`` is set to 'random'.

    random_state : int, optional

        Specifies the seed for random generation.

        When 0 is specified, system time is used.

        Defaults to 0.

    timeout : int, optional

        Specifies maximum running time for model evaluation/parameter selection,
        in seconds.

        No timeout when 0 is specified.

        Defaults to 0.

    progress_id : str, optional

        Sets an ID of progress indicator for model evaluation/parameter selection.

        If not provided, no progress indicator is activated.

    param_values : dict or list of tuples, optional

        Specifies the values of following parameters for model parameter selection:

            ``learning_rate``, ``momentum``, ``batch_size``.

        If input is list of tuples, then each tuple must contain exactly two elements:

            - 1st element is the parameter name(str type),
            - 2nd element is a list of valid values for that parameter.

        If input is dict, then for each element, the key must be parameter name, while value
        be a list of valid values for the corresponding parameter.

        A simple example for illustration:

            [('learning_rate', [0.1, 0.2, 0.5]), ('momentum', [0.2, 0.6])],

        or

            dict(learning_rate=[0.1, 0.2, 0.5], momentum=[0.2, 0.6]).

        Valid only when ``resampling_method`` and ``search_strategy`` are both specified,
        and ``training_style`` is 'stochastic'.

    param_range : list of tuple, optional

        Specifies the range of the following parameters for model parameter selection:

            ``learning_rate``, ``momentum``, ``batch_size``.

        If input is a list of tuples, the each tuple should contain exactly two elements:

            - 1st element is the parameter name(str type),
            - 2nd element is a list that specifies the range of that parameter as follows:
              first value is the start value, second value is the step, and third value is the end value.
              The step value can be omitted, and will be ignored, if ``search_strategy`` is set to 'random'.

        Otherwise, if input is a dict, then for each element the key should be parameter name, while value
        specifies the range of that parameter.

        Valid only when ``resampling_method`` and ``search_strategy`` are both specified,
        and ``training_style`` is 'stochastic'.

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

    train_log_ : DataFrame
        Provides mean squared error between predicted values and target
        values for each iteration.

    stats_ : DataFrame
        Statistics.

    optim_param_ : DataFrame
        Provides optimal parameters selected. Available only when parameter selection is triggered.


    Examples
    --------

    Training data df:

    >>> df.collect()
       V000  V001 V002  V003 LABEL
    0     1  1.71   AC     0    AA
    1    10  1.78   CA     5    AB
    ...
    8    12  2.13   AC     4     C
    9    18  1.87   AC     6    AA

    Create a MLPClassifier instance and call fit():

    >>> mlpc = MLPClassifier(hidden_layer_size=(10,10),
    ...                      activation='tanh', output_activation='tanh',
    ...                      learning_rate=0.001, momentum=0.0001,
    ...                      training_style='stochastic',max_iter=100,
    ...                      normalization='z-transform', weight_init='normal',
    ...                      thread_ratio=0.3, categorical_variable='V003')
    >>> mlpc.fit(data=df)

    Training result may look different from the following results due
    to model randomness.

    >>> mlpc.model_.collect()
       ROW_INDEX                                      MODEL_CONTENT
    0          1  {"CurrentVersion":"1.0","DataDictionary":[{"da...
    1          2  t":0.2700182926188939},{"from":13,"weight":0.0...
    2          3  ht":0.2414416413305134},{"from":21,"weight":0....
    >>> mlpc.train_log_.collect()
        ITERATION     ERROR
    0           1  1.080261
    1           2  1.008358
    ...
    98         99  0.309770
    99        100  0.308704

    Perform predcit():

    >>> res, stat = mlpc.predict(data=pred_df, key='ID')

    Output:

    >>> res.collect()
       ID TARGET     VALUE
    0   1      C  0.472751
    1   2      C  0.417681
    2   3      C  0.543967
    >>> stat.collect()
       ID CLASS  SOFT_MAX
    0   1    AA  0.371996
    1   1    AB  0.155253
    ...
    7   3    AB  0.106220
    8   3     C  0.543967

    Model Evaluation:

    >>> mlpc = MLPClassifier(activation='tanh',
    ...                      output_activation='tanh',
    ...                      hidden_layer_size=(10,10),
    ...                      learning_rate=0.001,
    ...                      momentum=0.0001,
    ...                      training_style='stochastic',
    ...                      max_iter=100,
    ...                      normalization='z-transform',
    ...                      weight_init='normal',
    ...                      resampling_method='cv',
    ...                      evaluation_metric='f1_score',
    ...                      fold_num=10,
    ...                      repeat_times=2,
    ...                      random_state=1,
    ...                      progress_indicator_id='TEST',
    ...                      thread_ratio=0.3)
    >>> mlpc.fit(data=df, label='LABEL', categorical_variable='V003')

    Model evaluation result may look different from the following result due to randomness.

    >>> mlpc.stats_.collect()
                STAT_NAME                                         STAT_VALUE
    0             timeout                                              FALSE
    1     TEST_1_F1_SCORE                       1, 0, 1, 1, 0, 1, 0, 1, 1, 0
    2     TEST_2_F1_SCORE                       0, 0, 1, 1, 0, 1, 0, 1, 1, 1
    3  TEST_F1_SCORE.MEAN                                                0.6
    4   TEST_F1_SCORE.VAR                                           0.252631
    5      EVAL_RESULTS_1  {"candidates":[{"TEST_F1_SCORE":[[1.0,0.0,1.0,...
    6     solution status  Convergence not reached after maximum number o...
    7               ERROR                                 0.2951168443145714

    Parameter selection:

    >>> act_opts=['tanh', 'linear', 'sigmoid_asymmetric']
    >>> out_act_opts = ['sigmoid_symmetric', 'gaussian_asymmetric', 'gaussian_symmetric']
    >>> layer_size_opts = [(10, 10), (5, 5, 5)]
    >>> mlpc = MLPClassifier(activation_options=act_opts,
    ...                      output_activation_options=out_act_opts,
    ...                      hidden_layer_size_options=layer_size_opts,
    ...                      learning_rate=0.001,
    ...                      batch_size=2,
    ...                      momentum=0.0001,
    ...                      training_style='stochastic',
    ...                      max_iter=100,
    ...                      normalization='z-transform',
    ...                      weight_init='normal',
    ...                      resampling_method='stratified_bootstrap',
    ...                      evaluation_metric='accuracy',
    ...                      search_strategy='grid',
    ...                      fold_num=10,
    ...                      repeat_times=2,
    ...                      random_state=1,
    ...                      progress_indicator_id='TEST',
    ...                      thread_ratio=0.3)
    >>> mlpc.fit(data=df, label='LABEL', categorical_variable='V003')

    Parameter selection result may look different from the following result due to randomness.

    >>> mlpc.stats_.collect()
                STAT_NAME                                         STAT_VALUE
    0             timeout                                              FALSE
    1     TEST_1_ACCURACY                                               0.25
    ...
    8      EVAL_RESULTS_4  rs":"HIDDEN_LAYER_SIZE=10, 10;OUTPUT_LAYER_ACT...
    9               ERROR                                  0.684842661926971
    >>> mlpc.optim_param_.collect()
                     PARAM_NAME  INT_VALUE DOUBLE_VALUE STRING_VALUE
    0         HIDDEN_LAYER_SIZE        NaN         None      5, 5, 5
    1  OUTPUT_LAYER_ACTIVE_FUNC        4.0         None         None
    2  HIDDEN_LAYER_ACTIVE_FUNC        3.0         None         None
    """
    evaluation_metric_map = {'accuracy' : 'ACCURACY', 'f1_score' : 'F1_SCORE',
                             'auc_onevsrest' : 'AUC_1VsRest',
                             'auc_1vsrest' : 'AUC_1VsRest',
                             'auc_pairwise': 'AUC_pairwise'}
    #pylint: disable=too-many-arguments, too-many-locals
    def __init__(self, activation=None, activation_options=None,
                 output_activation=None, output_activation_options=None,
                 hidden_layer_size=None, hidden_layer_size_options=None,
                 max_iter=None, training_style=None, learning_rate=None, momentum=None,
                 batch_size=None, normalization=None, weight_init=None, categorical_variable=None,
                 resampling_method=None, evaluation_metric=None, fold_num=None,
                 repeat_times=None, search_strategy=None, random_search_times=None,
                 random_state=None, timeout=None, progress_indicator_id=None,
                 param_values=None, param_range=None, thread_ratio=None,
                 reduction_rate=None, aggressive_elimination=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(MLPClassifier, self).__init__(activation=activation,
                                            activation_options=activation_options,
                                            output_activation=output_activation,
                                            output_activation_options=output_activation_options,
                                            hidden_layer_size=hidden_layer_size,
                                            hidden_layer_size_options=hidden_layer_size_options,
                                            max_iter=max_iter,
                                            functionality=0,
                                            training_style=training_style,
                                            learning_rate=learning_rate,
                                            momentum=momentum,
                                            batch_size=batch_size,
                                            normalization=normalization,
                                            weight_init=weight_init,
                                            categorical_variable=categorical_variable,
                                            resampling_method=resampling_method,
                                            evaluation_metric=evaluation_metric,
                                            fold_num=fold_num,
                                            repeat_times=repeat_times,
                                            search_strategy=search_strategy,
                                            random_search_times=random_search_times,
                                            random_state=random_state,
                                            timeout=timeout,
                                            progress_indicator_id=progress_indicator_id,
                                            param_values=param_values,
                                            param_range=param_range,
                                            thread_ratio=thread_ratio,
                                            reduction_rate=reduction_rate,
                                            aggressive_elimination=aggressive_elimination)
        self.op_name = 'MLP_Classifier'

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
                  to that index column
                - otherwise, it is assumed that ``data`` contains no ID column

        features : a list of str, optional

            Names of the feature columns.

            If ``features`` is not provided, it defaults to all
            the non-ID and non-label columns.

        label : str, optional

            Name of the label column.
            If ``label`` is not provided, it defaults to the last column.

        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.

        Returns
        -------
        A fitted object of class "MLPClassifier".

        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        key = self._arg('key', key, str)
        features = self._arg('features', features, ListOfStrings)
        label = self._arg('label', label, str)
        self._fit(data, key, features, label, categorical_variable)
        return self

    def predict(self, data, key=None, features=None, thread_ratio=None, verbose_top_n=None):
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

        thread_ratio : float, optional
            Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
            Values outside the range will be ignored and this function heuristically determines the number of threads to use.

            Defaults to 0.

        verbose_top_n : int, optional

            Specifies the number of top n classes to present after sorting with softmax values. The result is shown in the second returned DataFrame.
            It cannot exceed the number of classes in label of the training data, and it can be 0, which means to output softmax values of all classes.

            Defaults to 0.

        Returns
        -------

        DataFrames

            Predicted classes, structured as follows:

              - ID column, with the same name and type as ``data`` 's ID column.
              - TARGET, type NVARCHAR, predicted class name.
              - VALUE, type DOUBLE, softmax value for the predicted class.

            Softmax values for all classes or top n classes (if ``verbose_top_n`` is set), structured as follows:

              - ID column, with the same name and type as ``data`` 's ID column.
              - CLASS, type NVARCHAR, class name.
              - VALUE, type DOUBLE, softmax value for that class.
        """
        return super(MLPClassifier, self)._predict(data=data, key=key, features=features, thread_ratio=thread_ratio, verbose_top_n=verbose_top_n)

    def score(self, data, key=None, features=None, label=None, thread_ratio=None):
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

            If ``features`` is not provided, it defaults to all
            the non-ID and non-label columns.

        label : str, optional

            Name of the label column.

            If ``label`` is not provided, it defaults to the last column.

        Returns
        -------

        float

            Scalar value of accuracy after comparing the predicted result
            and original label.
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
        unique_id = str(uuid.uuid1())
        unique_id = unique_id.replace('-', '_').upper()
        cols = data.columns
        cols.remove(key)
        if label is None:
            label = cols[-1]
        cols.remove(label)
        if features is None:
            features = cols

        prediction, _ = self.predict(data=data, key=key,
                                     features=features, thread_ratio=thread_ratio)
        prediction = prediction.select(key, 'TARGET').rename_columns(['ID_P', 'PREDICTION'])

        actual = data.select(key, label).rename_columns(['ID_A', 'ACTUAL'])
        joined = actual.join(prediction, 'ID_P=ID_A').select('ACTUAL', 'PREDICTION')

        accuracy = metrics.accuracy_score(joined,
                                          label_true='ACTUAL',
                                          label_pred='PREDICTION')
        setattr(self, 'score_metrics_', {"ACCURACY": accuracy})
        return accuracy

class MLPRegressor(_MLPBase):
    r"""
    Multi-layer perceptron (MLP) Regressor.

    Parameters
    ----------
    activation : str

        Specifies the activation function for the hidden layer.

        Valid activation functions include:
          - 'tanh',
          - 'linear',
          - 'sigmoid_asymmetric',
          - 'sigmoid_symmetric',
          - 'gaussian_asymmetric',
          - 'gaussian_symmetric',
          - 'elliot_asymmetric',
          - 'elliot_symmetric',
          - 'sin_asymmetric',
          - 'sin_symmetric',
          - 'cos_asymmetric',
          - 'cos_symmetric',
          - 'relu'

        Should not be specified only if ``activation_options`` is provided.

    activation_options : list of str, optional

        A list of activation functions for parameter selection.

        See ``activation`` for the full set of valid activation functions.

    output_activation : str

        Specifies the activation function for the output layer.

        Valid choices of activation function same as  those in ``activation``.

        Should not be specified only if ``output_activation_options`` is provided.

    output_activation_options : list of str, conditionally mandatory

        A list of activation functions for the output layer for parameter selection.

        See ``activation`` for the full set of activation functions for output layer.

    hidden_layer_size : list of int or tuple of int

        Sizes of all hidden layers.

        Should not be specified only if ``hidden_layer_size_options`` is provided.

    hidden_layer_size_options : list of tuples, optional

        A list of optional sizes of all hidden layers for parameter selection.

    max_iter : int, optional

        Maximum number of iterations.

        Defaults to 100.

    training_style :  {'batch', 'stochastic'}, optional

        Specifies the training style.

        Defaults to 'stochastic'.

    learning_rate : float, optional

        Specifies the learning rate.

        Mandatory and valid only when ``training_style`` is 'stochastic'.

    momentum : float, optional

        Specifies the momentum for gradient descent update.

        Mandatory and valid only when ``training_style`` is 'stochastic'.

    batch_size : int, optional

        Specifies the size of mini batch.

        Valid only when ``training_style`` is 'stochastic'.

        Defaults to 1.

    normalization : {'no', 'z-transform', 'scalar'}, optional

        Defaults to 'no'.

    weight_init : {'all-zeros', 'normal', 'uniform', 'variance-scale-normal', 'variance-scale-uniform'}, optional

        Specifies the weight initial value.

        Defaults to 'all-zeros'.

    categorical_variable : str or a list of str, optional

        Specifies column name(s) in the data table used as category variable.

        Valid only when column is of INTEGER type.

    thread_ratio : float, optional

        Controls the proportion of available threads to use for training.

        The value range is from 0 to 1, where 0 indicates a single thread,
        and 1 indicates up to all available threads.

        Values between 0 and 1 will use that percentage of available threads.

        Values outside this range tell PAL to heuristically determine the number of threads to use.

        Defaults to 0.

    resampling_method : str, optional

        Specifies the resampling method for model evaluation or parameter selection.
        Valid options are listed as follows: 'cv', 'bootstrap', 'cv_sha', 'bootstrap_sha',
        'cv_hyperband', 'bootstrap_hyperband'.

        If not specified, neither model evaluation or parameter selection shall
        be triggered.

        .. note::
            Resampling methods with suffix 'sha' or 'hyperband' are for parameter selection only,
            not for model evaluation.

    evaluation_metric : {'rmse'}, optional

        Specifies the evaluation metric for model evaluation or parameter selection.
        Must be specified together with ``resampling_method`` to activate model evaluation
        or parameter selection.

        No default value.

    fold_num : int, optional

        Specifies the fold number for the cross-validation.

        Mandatory and valid only when ``resampling_method`` is specified as one of the following:
        'cv', 'cv_sha', 'cv_hyperband'.

    repeat_times : int, optional

        Specifies the number of repeat times for resampling.

        Defaults to 1.

    search_strategy : {'grid', 'random'}, optional

        Specifies the method for parameter selection.

        - if ``resampling_method`` is specified as 'cv_sha' or 'bootstrap_sha',
          then this parameter is mandatory.
        - if ``resampling_method`` is specified as 'cv_hyperband' or 'bootstrap_hyperband',
          then this parameter defaults to 'random' and cannot be changed.
        - otherwise this parameter has no default value, and parameter selection will not
          be activated if it is not specified.

    random_searhc_times : int, optional

        Specifies the number of times to randomly select candidate parameters.

        Mandatory and valid only when ``search_strategy`` is set to 'random'.

    random_state : int, optional

        Specifies the seed for random generation.

        When 0 is specified, system time is used.

        Defaults to 0.

    timeout : int, optional

        Specifies maximum running time for model evaluation/parameter selection,
        in seconds.

        No timeout when 0 is specified.

        Defaults to 0.

    progress_id : str, optional

        Sets an ID of progress indicator for model evaluation/parameter selection.

        If not provided, no progress indicator is activated.

    param_values : dict or list of tuples, optional

        Specifies the values of following parameters for model parameter selection:

            ``learning_rate``, ``momentum``, ``batch_size``.

        If input is list of tuples, then each tuple must contain exactly two elements:

            - 1st element is the parameter name(str type),
            - 2nd element is a list of valid values for that parameter.

        Otherwise, if input is dict, then for each element, the key must be a parameter name, while value
        be a list of valid values for that parameter.

        A simple example for illustration:

            [('learning_rate', [0.1, 0.2, 0.5]), ('momentum', [0.2, 0.6])],

        or

            dict(learning_rate=[0.1, 0.2, 0.5], momentum=[0.2, 0.6]).

        Valid only when ``resampling_method`` and ``search_strategy`` are both specified,
        and ``training_style`` is 'stochastic'.

    param_range : dict or list of tuple, optional

        Sets the range of the following parameters for model parameter selection:

            ``learning_rate``, ``momentum``, ``batch_size``.

        If input is a list of tuples, the each tuple should contain exactly two elements:

            - 1st element is the parameter name(str type),
            - 2nd element is a list that specifies the range of that parameter as follows:
              first value is the start value, second value is the step, and third value is the end value.
              The step value can be omitted, and will be ignored, if ``search_strategy`` is set to 'random'.

        Otherwise, if input is a dict, then for each element the key should be parameter name, while value
        specifies the range of that parameter.

        Valid only when ``resampling_method`` and ``search_strategy`` are both specified,
        and ``training_style`` is 'stochastic'.

    reduction_rate : float, optional
        Specifies reduction rate in SHA or Hyperband method.

        For each round, the available parameter candidate size will be divided by value of this parameter.
        Thus valid value for this parameter must be greater than 1.0

        Valid only when ``resampling_method`` takes one of the following values:
        'cv_sha', 'bootstrap_sha', 'cv_hyperband', 'bootstrap_hyperband'.

        Defaults to 3.0.

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

    model_ : DataFrame
        Model content.

    train_log_ : DataFrame
        Provides mean squared error between predicted values and target
        values for each iteration.

    stats_ : DataFrame
        Statistics.

    optim_param_ : DataFrame
        Provides optimal parameters selected.

        Available only when parameter selection is triggered.

    Examples
    --------

    Training data df:

    >>> df.collect()
       V000  V001 V002  V003  T001  T002  T003
    0     1  1.71   AC     0  12.7   2.8  3.06
    1    10  1.78   CA     5  12.1   8.0  2.65
    ...
    8    12  2.13   AC     4  13.2   1.9  1.34
    9    18  1.87   AC     6  25.5   3.6  2.14

    Create a MLPRegressor instance and call fit():

    >>> mlpr = MLPRegressor(hidden_layer_size=(10,5),
    ...                     activation='sin_asymmetric',
    ...                     output_activation='sin_asymmetric',
    ...                     learning_rate=0.001, momentum=0.00001,
    ...                     training_style='batch',
    ...                     max_iter=10000, normalization='z-transform',
    ...                     weight_init='normal', thread_ratio=0.3)
    >>> mlpr.fit(data=df, label=['T001', 'T002', 'T003'])

    Training result may look different from the following results due
    to model randomness.

    >>> mlpr.model_.collect()
       ROW_INDEX                                      MODEL_CONTENT
    0          1  {"CurrentVersion":"1.0","DataDictionary":[{"da...
    1          2  3782583596893},{"from":10,"weight":-0.16532599...
    >>> mlpr.train_log_.collect()
         ITERATION       ERROR
    0            1   34.525655
    1            2   82.656301
    ...
    733        734   11.891081
    734        735   11.891081

    [735 rows x 2 columns]

    >>> pred_df.collect()
       ID  V000  V001 V002  V003
    0   1     1  1.71   AC     0
    1   2    10  1.78   CA     5
    2   3    17  2.36   AA     6

    Invoke predict():

    >>> res  = mlpr.predict(data=pred_df, key='ID')

    Result may look different from the following results due to model randomness.

    >>> res.collect()
       ID TARGET      VALUE
    0   1   T001  12.700012
    1   1   T002   2.799133
    ...
    7   3   T002   2.799659
    8   3   T003   2.190000
    """
    evaluation_metric_map = {'rmse' : 'RMSE'}
    #pylint:disable=too-many-arguments, too-many-locals
    def __init__(self, activation=None, activation_options=None,
                 output_activation=None, output_activation_options=None,
                 hidden_layer_size=None, hidden_layer_size_options=None,
                 max_iter=None, training_style='stochastic', learning_rate=None, momentum=None,
                 batch_size=None, normalization=None, weight_init=None, categorical_variable=None,
                 resampling_method=None, evaluation_metric=None, fold_num=None,
                 repeat_times=None, search_strategy=None, random_search_times=None,
                 random_state=None, timeout=None, progress_indicator_id=None,
                 param_values=None, param_range=None, thread_ratio=None,
                 reduction_rate=None, aggressive_elimination=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(MLPRegressor, self).__init__(activation=activation,
                                           activation_options=activation_options,
                                           output_activation=output_activation,
                                           output_activation_options=output_activation_options,
                                           hidden_layer_size=hidden_layer_size,
                                           hidden_layer_size_options=hidden_layer_size_options,
                                           max_iter=max_iter,
                                           functionality=1,
                                           training_style=training_style,
                                           learning_rate=learning_rate,
                                           momentum=momentum,
                                           batch_size=batch_size,
                                           normalization=normalization,
                                           weight_init=weight_init,
                                           categorical_variable=categorical_variable,
                                           resampling_method=resampling_method,
                                           evaluation_metric=evaluation_metric,
                                           fold_num=fold_num,
                                           repeat_times=repeat_times,
                                           search_strategy=search_strategy,
                                           random_search_times=random_search_times,
                                           random_state=random_state,
                                           timeout=timeout,
                                           progress_indicator_id=progress_indicator_id,
                                           param_values=param_values,
                                           param_range=param_range,
                                           thread_ratio=thread_ratio,
                                           reduction_rate=reduction_rate,
                                           aggressive_elimination=aggressive_elimination)
        self.op_name = 'MLP_Regressor'

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
                  to that index column
                - otherwise, it is assumed that ``data`` contains no ID column

        features : a list of str, optional

            Names of the feature columns.

            If ``features`` is not provided, it defaults to all
            the non-ID and non-label columns.

        label : str or a list of str, optional

            Name of the label column, or list of names of multiple label
            columns.

            If ``label`` is not provided, it defaults to the last column.

        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.

        Returns
        -------
        A fitted object of class "MLPRegressor".

        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        index = data.index
        key = self._arg('key', key, str)
        if isinstance(index, str):
            if key is not None and index != key:
                msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(msg)
        key = index if key is None else key
        features = self._arg('features', features, ListOfStrings)
        #check for str or list of str:
        if label is not None:
            msg = "label should be a string or list of strings."
            if isinstance(label, list):
                if not all(isinstance(x, _TEXT_TYPES) for x in label):
                    logger.error(msg)
                    raise TypeError(msg)
            else:
                if not isinstance(label, _TEXT_TYPES):
                    logger.error(msg)
                    raise ValueError(msg)
        self._fit(data=data, key=key, features=features, label=label, categorical_variable=categorical_variable)
        return self

    def predict(self, data, key=None, features=None, thread_ratio=None):
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

            If ``features`` is not provided, it defaults to all the non-ID columns.

        thread_ratio : float, optional
            Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
            Values outside the range will be ignored and this function heuristically determines the number of threads to use.

            Defaults to 0.


        Returns
        -------

        DataFrame

            Predicted results, structured as follows:

                - ID column, with the same name and type as ``data`` 's ID column.
                - TARGET, type NVARCHAR, target name.
                - VALUE, type DOUBLE, regression value.
        """
        pred_res, _ = super(MLPRegressor, self)._predict(data=data, key=key, features=features, thread_ratio=thread_ratio, verbose_top_n=None)
        return pred_res

    def score(self, data, key, features=None, label=None, thread_ratio=None):#pylint: disable=too-many-locals
        #check for fit table existence
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

            If ``features`` is not provided, it defaults to all
            the non-ID and non-label columns.

        label : str or a list of str, optional

            Name of the label column, or list of names of multiple label
            columns.

            If ``label`` is not provided, it defaults to the last column.

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
        key = self._arg('key', key, str, True)
        features = self._arg('features', features, ListOfStrings)
        #check for str or list of str:
        if label is not None:
            msg = "label should be a string or list of strings."
            if isinstance(label, list):
                if not all(isinstance(x, _TEXT_TYPES) for x in label):
                    logger.error(msg)
                    raise ValueError(msg)
            else:
                if not isinstance(label, _TEXT_TYPES):
                    logger.error(msg)
                    raise ValueError(msg)
        #return scalar value of accuracy after calling predict
        unique_id = str(uuid.uuid1())
        unique_id = unique_id.replace('-', '_').upper()
        cols = data.columns
        cols.remove(key)
        mlp_type = 'MLPREGRESSOR'
        if label is None:
            label = cols[-1]
        if isinstance(label, _TEXT_TYPES):
            label = [label]
        if features is None:
            features = [x for x in cols if x not in label]
        val_df = data.select([key] + label)
        pred_res = self.predict(data, key, features, thread_ratio)
        ##create compare table with ID, original val, predicted_val
        comp_tbl = '#{}_COMPARE_TBL_{}_{}'.format(mlp_type, self.id, unique_id)
        try:
            with conn.connection.cursor() as cur:
                #reorganize pred_res like (ID, COL1, COL2...) instead of ID, COL_NAME, COL_VALUE
                temp_sql = [('MAX(CASE WHEN ("TARGET" = {0})' +
                             'THEN "VALUE" ELSE NULL END) ' +
                             'AS {1}').format("N'{}'".format(name.replace("'", "''")),
                                              quotename(name))
                            for name in label]
                temp_sql = ", ".join(temp_sql)
                pred_res_new_sql = ('SELECT {0}, {1} FROM ({2}) ' +
                                    'GROUP BY {0} ORDER BY {0}').format(quotename(key),
                                                                        temp_sql,
                                                                        pred_res.select_statement)
                comp_cols = ['ori.{0} as {1}, pred.{0} as {2}'.format(quotename(name),
                                                                      quotename('ORI_' + name),
                                                                      quotename('PRED_' + name))
                             for name in label]
                comp_cols = ', '.join(comp_cols)
                comp_tbl_sql = ('CREATE LOCAL TEMPORARY COLUMN TABLE {0} ' +
                                'AS (SELECT ori.{1}, {2} FROM'
                                ' ({3}) ori,' +
                                ' ({4}) AS pred WHERE ' +
                                'ori.{1} = ' +
                                'pred.{1});').format(quotename(comp_tbl),
                                                     quotename(key),
                                                     comp_cols,
                                                     val_df.select_statement,
                                                     pred_res_new_sql)
                execute_logged(cur, comp_tbl_sql, conn.sql_tracer, conn)
                #construct sql for calculating U => ((y_true - y_pred) ** 2).sum()
                u_sql = ['SUM(POWER({0} - {1}, 2))'.format(quotename('ORI_' + name),
                                                           quotename('PRED_' + name))
                         for name in label]
                u_sql = ' + '.join(u_sql)
                #construct sql for calculating V => ((y_true - y_true.mean()) ** 2).sum()
                v_sql = [('SUM(POWER({0} - (SELECT AVG({0}) FROM {1}),' +
                          '2))').format(quotename('ORI_' + name),
                                        quotename(comp_tbl))
                         for name in label]
                v_sql = ' + '.join(v_sql)
                #construct sql for calculating R2 => 1 - U/V
                res_sql = ('SELECT 1- ({0}) / ({1}) FROM {2};').format(u_sql,
                                                                       v_sql,
                                                                       quotename(comp_tbl))
                execute_logged(cur, res_sql, conn.sql_tracer, conn)
                r_2 = cur.fetchall()
                r2 = r_2[0][0]
                setattr(self, 'score_metrics_', {"R2": r2})
                return r2
        except dbapi.Error as db_err:
            #logger.error('HANA error during MLPRegressor score.', exc_info=True)
            logger.error(str(db_err))
            raise
        except Exception as db_err:
            #logger.error('HANA error during MLPRegressor score.', exc_info=True)
            logger.error(str(db_err))
            raise

class _MLPMultiTaskBase(PALBase):#pylint:disable=too-many-instance-attributes
    r"""
    Base class for MLP Multi Task.
    """
    activation_map = {
        'sigmoid' : 0,
        'tanh' : 1,
        'relu' : 2,
        'leaky-relu' : 3,
        'elu' : 4,
        'gelu' : 5
    }
    normalization_map = {None: 0, 'no': 0, 'z-transform': 1, 'scalar': 2}
    optimizer_map = {'sgd' : 0, 'rmsprop' : 1,  'adam': 2, 'adagrad': 3}
    network_type_map = {'basic' : 0, 'resnet' : 1}
    training_style_map = {'batch' : 0, 'stochastic' : 1}
    param_search_strategy_list = ['grid', 'random']
    pal_funcname = 'PAL_MLP_MULTI_TASK'
    def __init__(self,#pylint: disable=too-many-arguments, too-many-branches, too-many-statements, too-many-locals, too-many-positional-arguments
                 functionality,
                 hidden_layer_size=None,
                 activation=None,
                 batch_size=None,
                 num_epochs=None,
                 random_state=None,
                 use_batchnorm=None,
                 learning_rate=None,
                 optimizer=None,
                 dropout_prob=None,
                 training_percentage=None,
                 early_stop=None,
                 normalization=None,
                 warmup_epochs=None,
                 patience=None,
                 save_best_model=None,
                 training_style=None,
                 network_type=None,
                 embedded_num=None,
                 residual_num=None,
                 finetune=None,
                 resampling_method=None,
                 evaluation_metric=None,
                 fold_num=None,
                 repeat_times=None,
                 param_search_strategy=None,
                 random_search_times=None,
                 timeout=None,
                 progress_indicator_id=None,
                 reduction_rate=None,
                 aggressive_elimination=None,
                 param_range=None,
                 param_values=None):
        if not hasattr(self, 'hanaml_parameters'):
            setattr(self, 'hanaml_parameters', pal_param_register())
        super(_MLPMultiTaskBase, self).__init__()
        param_tune_flag = all(x is not None for x in [resampling_method, evaluation_metric, param_search_strategy])
        self.param_range = self._arg('param_range', param_range, (dict, ListOfTuples))
        self.param_values = self._arg('param_values', param_values, (dict, ListOfTuples))
        if isinstance(self.param_range, dict):
            self.param_range = [(param, self.param_range[param]) for param in self.param_range]
        if isinstance(self.param_values, dict):
            self.param_values = [(param, self.param_values[param]) for param in self.param_values]
        range_value_params = []
        if self.param_values is not None:
            for parm in self.param_values:
                range_value_params.append(parm[0])
        if self.param_range is not None:
            for parm in self.param_range:
                range_value_params.append(parm[0])
        self.functionality = functionality
        self.finetune = self._arg('finetune', finetune, bool)
        self.network_type = self._arg('network_type', network_type, self.network_type_map)
        self.training_style = self._arg('training_style', training_style, self.training_style_map)
        self.hidden_layer_size = self._arg('hidden_layer_size',
                                           hidden_layer_size,
                                           (list, tuple),
                                           required=finetune is not True and self.network_type in [0, None] \
                                           and not param_tune_flag and 'hidden_layer_size' not in range_value_params)
        if self.hidden_layer_size is not None:
            self.hidden_layer_size = [self._arg('Every element in "hidden_layer_size"',
                                                elm, int) for elm in list(self.hidden_layer_size)]
            self.hidden_layer_size = ', '.join([str(ls) for ls in self.hidden_layer_size])
        self.activation = self._arg('activation', activation, self.activation_map)
        self.batch_size = self._arg('batch_size', batch_size, int)
        self.num_epochs = self._arg('num_epochs', num_epochs, int)
        self.random_state = self._arg('random_state', random_state, int)
        self.use_batchnorm = self._arg('use_batchnorm', use_batchnorm, bool)
        self.optimizer = self._arg('optimizer', optimizer, self.optimizer_map)
        self.learning_rate = self._arg('learning_rate', learning_rate, float)
        self.dropout_prob = self._arg('dropout_prob', dropout_prob, float)
        self.training_percentage = self._arg('training_percentage', training_percentage, float)
        self.early_stop = self._arg('early_stop', early_stop, bool)
        self.normalization = self._arg('normalization', normalization, self.normalization_map)
        self.warmup_epochs = self._arg('warmup_epochs', warmup_epochs, int)
        self.patience = self._arg('patience', patience, int)
        self.save_best_model = self._arg('save_best_model', save_best_model, bool)
        self.embedded_num = self._arg('embedded_num', embedded_num, int,
                                      required=self.network_type == 1 and finetune is not True \
                                      and not param_tune_flag and 'embeded_num' not in range_value_params)
        self.residual_num = self._arg('residual_num', residual_num, int,
                                      required=self.network_type == 1 and finetune is not True \
                                      and not param_tune_flag and 'residual_num' not in range_value_params)
        self.resampling_method = self._arg('resampling_method', resampling_method, str)

        self.resampling_method = self._arg('resampling_method', resampling_method, {mtd:mtd for mtd in self.resampling_method_list})#pylint:disable=no-member
        self.evaluation_metric = self._arg('evaluation_metric', evaluation_metric, self.evaluation_metric_map)#pylint:disable=no-member
        self.fold_num = self._arg('fold_num', fold_num, int, required='cv' in str(resampling_method))
        self.repeat_times = self._arg('repeat_times', repeat_times, int)
        #need some check work here
        self.param_search_strategy = self._arg('param_search_strategy', param_search_strategy, str,
                                               required='sha' in str(self.resampling_method))
        if 'hyperband' in str(self.resampling_method):
            self.search_strategy = 'random'
        if self.param_search_strategy is not None:
            if self.param_search_strategy not in self.param_search_strategy_list:
                msg = ("Param search strategy '{}' is invalid ".format(self.param_search_strategy)+
                       "for parameter selection.")
                logger.error(msg)
                raise ValueError(msg)
        self.random_search_times = self._arg('random_search_times', random_search_times, int,
                                             required=self.param_search_strategy == 'random')
        self.timeout = self._arg('timeout', timeout, int)
        self.progress_indicator_id = self._arg('progress_indicator_id', progress_indicator_id, str)
        self.reduction_rate = self._arg('reduction_rate', reduction_rate, float)
        self.aggressive_elimination = self._arg('aggressive_elimination', aggressive_elimination, bool)

    def _fit(self, data=None, key=None, features=None,#pylint:disable=too-many-positional-arguments
             label=None,categorical_variable=None,
             pre_model=None,
             model_table_name=None):
        conn = data.connection_context
        has_id = False
        index_col = None
        #Do we need type check for key column and also check its existence in df?
        key = self._arg('key', key, str)

        if not self._disable_hana_execution:
            require_pal_usable(conn)
            index = data.index
            if isinstance(index, str):
                if key is not None and index != key:
                    msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                    "and the designated index column '{}'.".format(index)
                    logger.warning(msg)
            key = index if key is None else key
            if key is not None:
                has_id = True
                index_col = key
            cols_left = data.columns
            if label is None:
                label = data.columns[-1]
            if isinstance(label, _TEXT_TYPES):
                label = [label]
            cols_left = [x for x in cols_left if x not in label]
            if has_id:
                cols_left.remove(index_col)
            if features is None:
                features = cols_left
            used_cols = [col for col in itertools.chain([index_col], features, label)
                        if col is not None]
            training_df = data[used_cols]
        else:
            training_df = data
        if categorical_variable is not None:
            if isinstance(categorical_variable, _TEXT_TYPES):
                categorical_variable = [categorical_variable]
            if not (isinstance(categorical_variable, list) and all(isinstance(x, _TEXT_TYPES)for x in categorical_variable)):#pylint:disable=line-too-long
                msg = "Parameter 'categorial_variable' must be type of str or list of str."
                logger.error(msg)
                raise TypeError(msg)
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        outputs = [
            '#MLP_MULTI_TASK_{}_TBL_{}_{}'.format(name, self.id, unique_id) for name in ['MODEL',
                                                                                  'TRAINING_LOG',
                                                                                  'STATISTICS',
                                                                                  'OPTIMAL_PARAM',
                                                                                  'PLACE_HOLDER']]
        model_tbl, log_tbl, stat_tbl, optimal_tbl, _ = outputs
        if model_table_name:
            model_tbl = model_table_name
            outputs[0] = model_table_name
        param_rows = [('HIDDEN_LAYER_ACTIVE_FUNC', self.activation, None, None),
                      ('HIDDEN_LAYER_SIZE', None, None, self.hidden_layer_size),
                      ('HAS_ID', int(has_id), None, None),
                      ('BATCH_SIZE', self.batch_size, None, None),
                      ('FUNCTIONALITY', self.functionality, None, None),
                      ('OPTIMIZER', self.optimizer, None, None),
                      ('LEARNING_RATE', None, self.learning_rate, None),
                      ('USE_BATCHNORM', self.use_batchnorm, None, None),
                      ('MAX_ITERATION', self.num_epochs, None, None),
                      ('DROPOUT_PROB', None, self.dropout_prob, None),
                      ('NORMALIZATION', self.normalization, None, None),
                      ('TRAINING_PERCENTAGE', None, self.training_percentage, None),
                      ('SEED', self.random_state, None, None),
                      ('EARLY_STOP', self.early_stop, None, None),
                      ('PATIENCE', self.patience, None, None),
                      ('SAVE_BEST_MODEL', self.save_best_model, None, None),
                      ('WARMUP_EPOCHS', self.warmup_epochs, None, None),
                      ('TRAINING_STYLE', self.training_style, None, None),
                      ('FINETUNE', self.finetune, None, None),
                      ('NETWORK_TYPE', self.network_type, None, None),
                      ('EMBEDDED_NUM', self.embedded_num, None, None),
                      ('RESIDUAL_NUM', self.residual_num, None, None),
                      ('RESAMPLING_METHOD', None, None, self.resampling_method),
                      ('EVALUATION_METRIC', None, None, self.evaluation_metric),
                      ('FOLD_NUM', self.fold_num, None, None),
                      ('REPEAT_TIMES', self.repeat_times, None, None),
                      ('PARAM_SEARCH_STRATEGY', None, None, self.param_search_strategy),
                      ('RANDOM_SEARCH_TIMES', self.random_search_times, None, None),
                      ('TIMEOUT', self.timeout, None, None),
                      ('REDUCTION_RATE', None, self.reduction_rate, None),
                      ('AGGRESSIVE_ELIMINATION', self.aggressive_elimination,
                       None, None)]
        if self.param_range is not None:
            for idx in range(len(self.param_range)):
                param = self.param_range[idx]
                pal_range = str(param[1]).replace(',', ',,' if len(param[1]) == 2 else ',')
                param_rows.extend([(param[0].upper() + '_RANGE', None, None, pal_range)])
        if self.param_values is not None:
            for idx in range(len(self.param_values)):
                param = self.param_values[idx]
                if param[0] == 'hidden_layer_size':
                    param_value = ', '.join([str(x).replace('[', '"').replace(']', '"') for x in param[1]])
                    param_value = '{' + param_value + '}'
                elif param[0] == 'activation':
                    param_value = str({self.activation_map[x] for x in param[1]})
                elif param[0] == 'optimizer':
                    param_value = str({self.optimizer_map[x] for x in param[1]})
                else:
                    param_value = str(set(param[1]))
                param_rows.extend([(param[0].upper() + '_VALUES' if param[0] != 'activation' else 'HIDDEN_LAYER_ACTIVE_FUNC_VALUES',
                                    None, None, param_value)])
        pre_model = self._arg('pre_model', pre_model, DataFrame,
                              required=self.finetune is True)
        if pre_model is None:
            pre_model_name = f'#MLP_MULTI_TASK_PRE_MODEL_TBL _{self.id}_{unique_id}'
            table_structure = {"ROW_INDEX" : "INTEGER", "PART_INDEX" : "INTEGER",
                               "MODEL_CONTENT" : "NVARCHAR(5000)"}
            try_drop(conn, pre_model_name)
            conn.create_table(pre_model_name, table_structure)
            pre_model = conn.table(pre_model_name)
        if categorical_variable is not None:
            param_rows.extend([('CATEGORICAL_VARIABLE', None, None, variable)
                               for variable in categorical_variable])
        if label is not None:
            param_rows.extend([('DEPENDENT_VARIABLE', None, None, variable)
                               for variable in label])
        if self.functionality == 0 and label is None:
            param_rows.extend([('CATEGORICAL_VARIABLE', None, None, data.columns[-1])])
        if not (check_pal_function_exist(conn, '%MLP_MULTI_TASK%', like=True) or \
        self._disable_hana_execution):
            msg = 'The version of your SAP HANA does not support Multi-task MLP!'
            logger.error(msg)
            raise ValueError(msg)
        try:
            self._call_pal_auto(conn,
                                'PAL_MLP_MULTI_TASK',
                                training_df,
                                ParameterTable().with_data(param_rows),
                                pre_model,
                                *outputs)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, outputs)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, outputs)
            raise
        if pre_model is None:
            try_drop(conn, pre_model_name)
        self.model_ = conn.table(model_tbl)#pylint:disable=attribute-defined-outside-init
        self.stats_ = conn.table(stat_tbl)#pylint:disable=attribute-defined-outside-init
        self.statistics_ = self.stats_
        #pylint:disable=attribute-defined-outside-init
        self.train_log_ = conn.table(log_tbl)#pylint:disable=attribute-defined-outside-init
        self.optim_param_ = conn.table(optimal_tbl)#pylint:disable=attribute-defined-outside-init

    def _predict(self, data=None, key=None, features=None, verbose=None, model=None):#pylint:disable=too-many-positional-arguments
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
        #thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        verbose = self._arg('verbose', verbose, bool)

        unique_id = str(uuid.uuid1())
        unique_id = unique_id.replace('-', '_').upper()
        mlp_type = 'MLPMultiTaskClassifier' if self.functionality == 0 else 'MLPMultiTaskRegressor'
        # 'key' is necessary for prediction data(PAL default: key, Feature)
        cols = data.columns
        cols.remove(key)
        if features is None:
            features = cols
        data_ = data[[key] + features]
        param_tbl, result_tbl, ph_tbl = [
            '#{}_{}_TBL_{}_{}'.format(mlp_type.upper(), name, self.id, unique_id)
            for name in ['PREDICT_CONTROL',
                         'PREDICT_RESULT',
                         'PLACE_HOLDER']
        ]
        outputs = [result_tbl, ph_tbl]
        param_rows = [('VERBOSE', verbose, None, None)]
        if not (check_pal_function_exist(conn, '%MLP_MULTI_TASK%', like=True) or \
        self._disable_hana_execution):
            msg = 'The version of your SAP HANA does not support Multi-task MLP!'
            logger.error(msg)
            raise ValueError(msg)
        try:
            self._call_pal_auto(conn,
                                'PAL_MLP_MULTI_TASK_PREDICT',
                                data_,
                                ParameterTable(param_tbl).with_data(param_rows),
                                self.model_ if model is None else model,
                                *outputs)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, outputs)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, outputs)
            raise
        return conn.table(result_tbl)

    def create_model_state(self, model=None, function=None,#pylint:disable=too-many-positional-arguments
                           pal_funcname='PAL_MLP_MULTI_TASK',
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

            A placeholder parameter, not effective for MultiTask MLP.

        pal_funcname : int or str, optional
            PAL function name.

            Defaults to 'PAL_MLP_MULTI_TASK'.

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

class MLPMultiTaskClassifier(_MLPMultiTaskBase):
    r"""
    Multi Task MLP Classifier.

    Parameters
    ----------
    hidden_layer_size : list (tuple) of int, optional

        Specifies the sizes of all hidden layers in the neural network.

        Mandatory and valid only when ``network_type`` is 'basic' and ``finetune`` is not True.

    activation : str, optional
        Specifies the activation function for the hidden layer.

        Valid activation functions include:

        - 'sigmoid'
        - 'tanh'
        - 'relu'
        - 'leaky-relu'
        - 'elu'
        - 'gelu'

        Defaults to 'relu'.

    batch_size : int, optional

        Specifies the number of training samples in a batch.

        Defaults to 16 (if the input data contains less than 16 samples,
        the size of input dat is used).

    num_epochs : int, optional

        Specifies the maximum number of training epochs.

        Defaults to 100.

    random_state : int, optional
        Specifies the seed for random generation.
        Use system time when 0 is specified.

        Defaults to 0.
    use_batchnorm : bool, optional
        Specifies whether to use batch-normalization in each hidden layer or not.

        Defaults to True (i.e. use batch-normalization).

    learning_rate : float, optional

        Specifies the learning rate for gradient based optimizers.

        Defaults to 0.001.

    optimizer : str, optional
        Specifies the optimizer for training the neural network.

        - 'sgd'
        - 'rmsprop'
        - 'adam'
        - 'adagrad'

        Defaults to 'adam'.
    dropout_prob : float, optional
        Specifies the dropout probability applied when training the neural network.

        Defaults to 0.0 (i.e. no dropout).

    training_percentage : float, optional
        Specifies the percentage of input data used for training (with the rest of input data used for valiation).

        Defaults to 0.9.

    early_stop : bool, optional
        Specifies whether to use the automatic early stopping method or not.

        Defaults to True (i.e. use automatic early stopping)

    normalization : str, optional
        Specifies the normalization type for input data.

        - 'no' (no normalization)
        - 'z-transform'
        - 'scalar'

        Defaults to 'no'.

    warmup_epochs : int, optional
        Specifies the least number of epochs to wait before executing the auto early stopping method.

        Defaults to 5.

    patience : int, optional
        Specifies the uumber of epochs to wait before terminating the training if no improvement is shown.

        Defaults to 5.
    save_best_model : bool, optional
        Specifies whether to save the best model (regarding to the minimum loss on the validation set).

        Defaults to False (i.e. save the model from the last training epoch, not the best one).
    training_style : {'batch', 'stochastic'}, optional
        Specifies the training style of the learning algorithm, either in batch mode or in stochastic mode.

        - 'batch' : This approach uses the entire training dataset to update model parameters,
          where LBFGS-B optimizer is adopted. This approach can be stable but memory-intensive.
        - 'stochastic' : This approach updates parameters with individual samples.
           While potentially less stable, it often leads to better generalization.

        Defaults to 'stochastic'.
    network_type : {'basic', 'resnet'}, optional
        Specifies the structure of the underlying neural-network to train. It can be a basic neural-network,
        or a neural-network comprising of residual blocks, i.e. ResNet.

        Defaults to 'basic' (corresponding to basic neural-network).

    embedded_num : int, optional
        Specifies the embedding dimension of ResNet for the input data, which equals to the dimension of the 1st linear
        in ResNet.

        Mandatory and valid when ``network_type`` is 'resnet' and ``finetune`` is not True.

    residual_num : int, optional
        Specifies the number of residual blocks in ResNet.

        Mandatory and valid when ``network_type`` is 'resnet' and ``finetune`` is not True.

    finetune : bool, optional
        Specifies the task type of the initialized class, i.e. whether it is used to finetune an existing pre-trained model,
        or trian a new model from scratch given the input data.

        Defaults to False.

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
    evaluation_metric : {'accuracy','f1_score', 'auc_1vsrest', 'auc_pairwise'}, optional

        Specifies the evaluation metric for model evaluation or parameter selection.

        Must be specified together with ``resampling_method`` to activate model evaluation or
        parameter selection.

        No default value.

    fold_num : int, optional

        Specifies the fold number for the cross-validation.

        Mandatory and valid only when ``resampling_method`` is specified to be one of the following:
        'cv', 'stratified_cv', 'cv_sha', 'stratified_cv_sha', 'cv_hyperband', 'stratified_cv_hyperband'.

    repeat_times : int, optional

        Specifies the number of repeat times for resampling.

        Defaults to 1.

    param_search_strategy : {'grid', 'random'}, optional

        Specifies the method for parameter selection.

        - mandatory if ``resampling_method`` is specified with suffix 'sha'
        - defaults to 'random' and cannot be changed if ``resampling_method`` is specified with suffix 'hyperband'
        - otherwise no default value, and parameter selection will not be activated if not specified

    random_search_times : int, optional

        Specifies the number of times to randomly select candidate parameters.

        Mandatory and valid only when ``param_search_strategy`` is set to 'random'.

    timeout : int, optional

        Specifies maximum running time for model evaluation/parameter selection,
        in seconds.

        No timeout when 0 is specified.

        Defaults to 0.

    progress_indicator_id : str, optional

        Sets an ID of progress indicator for model evaluation/parameter selection.

        If not provided, no progress indicator is activated.

    param_values : dict or list of tuples, optional

        Specifies the values of following parameters for model parameter selection:

            ``hidden_layer_size``, ``residual_num``, ``embedded_dim``,
            ``activation``, ``learning_rate``, ``optimizer``, ``dropout_prob``.

        If input is list of tuples, then each tuple must contain exactly two elements:

            - 1st element is the parameter name(str type),
            - 2nd element is a list of valid values for that parameter.

        Otherwise, if input is dict, then for each element, the key must be a parameter name, while value
        be a list of valid values for that parameter.

        A simple example for illustration:

            [('learning_rate', [0.1, 0.2, 0.5]), ('hidden_layer_size', [[10, 10], [100]])],

        or

            dict(learning_rate=[0.1, 0.2, 0.5], hidden_layer_size=[[10, 10], [100]]).

        Valid only when ``resampling_method`` and ``param_search_strategy`` are both specified,
        and ``training_style`` is 'stochastic'.

    param_range : dict or list of tuple, optional

        Sets the range of the following parameters for model parameter selection:

            ``residual_num``, ``embedded_dim``, ``learning_rate``, ``dropout_prob``

        If input is a list of tuples, the each tuple should contain exactly two elements:

            - 1st element is the parameter name(str type),
            - 2nd element is a list that specifies the range of that parameter as follows:
              first value is the start value, second value is the step, and third value is the end value.
              The step value can be omitted, and will be ignored, if ``param_search_strategy`` is set to 'random'.

        Otherwise, if input is a dict, then for each element the key should be parameter name, while value
        specifies the range of that parameter.

        Valid only when ``resampling_method`` and ``param_search_strategy`` are both specified and
        ``training_style`` is 'stochastic'.

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

        The MLP model.

    train_log_ : DataFrame

        Provides training errors among iterations.

    stats_ : DataFrame

        Names and values of statistics.

    optim_param_ : DataFrame

        Provides optimal parameters selected.

    Examples
    --------
    >>> train_data.collect()
       ID          X1          X2          X3         Y1     Y2
    0   0         1.0        10.0       100.0          A      1
    1   1         1.1        10.1       100.0          A      1
    2   2         2.2        20.2        11.0          B      2
    3   3         2.3        20.4        12.0          B      2
    4   4         2.2        20.3        25.0          B      1
    .   .         ...         ...         ...          .      .

    >>> mlp = MLPMultiTaskClassifier(hidden_layer_size=[5,5,5],
                                     activation='tanh')
    >>> mlp.fit(data=train_data, key='ID',
    ...         label=['Y1', 'Y2'])
    """

    resampling_method_list = ['cv', 'cv_sha', 'cv_hyperband', 'bootstrap',
                              'bootstrap', 'bootstrap_sha', 'bootstrap_hyperband',
                              'stratified_bootstrap', 'stratified_bootstrap_sha',
                              'stratified_bootstrap_hyperband', 'stratified_cv',
                              'stratified_cv_sha', 'stratified_cv_hyperband']
    evaluation_metric_map = {'accuracy' : 'ACCURACY', 'f1_score': 'F1_SCORE',
                             'auc_1vsrest': 'AUC_1VsRest', 'auc_pairwise': 'AUC_pairwise'}
    def __init__(self,#pylint: disable=too-many-arguments, too-many-branches, too-many-statements, too-many-locals, too-many-positional-arguments
                 hidden_layer_size=None,
                 activation=None,
                 batch_size=None,
                 num_epochs=None,
                 random_state=None,
                 use_batchnorm=None,
                 learning_rate=None,
                 optimizer=None,
                 dropout_prob=None,
                 training_percentage=None,
                 early_stop=None,
                 normalization=None,
                 warmup_epochs=None,
                 patience=None,
                 save_best_model=None,
                 training_style=None,
                 network_type=None,
                 embedded_num=None,
                 residual_num=None,
                 finetune=None,
                 resampling_method=None,
                 evaluation_metric=None,
                 fold_num=None,
                 repeat_times=None,
                 param_search_strategy=None,
                 random_search_times=None,
                 timeout=None,
                 progress_indicator_id=None,
                 reduction_rate=None,
                 aggressive_elimination=None,
                 param_range=None,
                 param_values=None):
        super(MLPMultiTaskClassifier, self).__init__(functionality=0,
                                                     hidden_layer_size=hidden_layer_size,
                                                     activation=activation,
                                                     batch_size=batch_size,
                                                     num_epochs=num_epochs,
                                                     random_state=random_state,
                                                     use_batchnorm=use_batchnorm,
                                                     learning_rate=learning_rate,
                                                     optimizer=optimizer,
                                                     dropout_prob=dropout_prob,
                                                     training_percentage=training_percentage,
                                                     early_stop=early_stop,
                                                     normalization=normalization,
                                                     warmup_epochs=warmup_epochs,
                                                     patience=patience,
                                                     save_best_model=save_best_model,
                                                     training_style=training_style,
                                                     network_type=network_type,
                                                     embedded_num=embedded_num,
                                                     residual_num=residual_num,
                                                     finetune=finetune,
                                                     resampling_method=resampling_method,
                                                     evaluation_metric=evaluation_metric,
                                                     fold_num=fold_num,
                                                     repeat_times=repeat_times,
                                                     param_search_strategy=param_search_strategy,
                                                     random_search_times=random_search_times,
                                                     timeout=timeout,
                                                     progress_indicator_id=progress_indicator_id,
                                                     reduction_rate=reduction_rate,
                                                     aggressive_elimination=aggressive_elimination,
                                                     param_range=param_range,
                                                     param_values=param_values)
        self.op_name = 'MLP_M_TASK_Classifier'

    def fit(self, data=None, key=None,#pylint:disable=too-many-positional-arguments
            features=None, label=None,
            categorical_variable=None,
            pre_model=None,
            model_table_name=None):
        r"""
        Fit function for Multi Task MLP (for classifiation).

        Parameters
        ----------
        data : DataFrame
            DataFrame containing the data.

            Note that if ``finetune`` is set as True when class is initialized,
            then ``data`` must be structured the same as the one
            use for training the model stored in ``pre_model``.

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

        label : str or a list of str, optional
            Name of the target columns.

            If not provided, it defaults to the last non-ID column.

        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.
        pre_model : DataFrame, optional
            Specifies the pre-model for online/continued training.

            Mandatory and valid only if ``finetune`` is set as True when class is initialized.

        model_table_name : str, optional
            Specifies the name of the model table.

            Defaults to None.

        Returns
        -------
        A fitted object of class "MLPMultiTaskClassifier".
        """
        self._fit(data=data, key=key, features=features, label=label,
                  categorical_variable=categorical_variable,
                  pre_model=pre_model, model_table_name=model_table_name)
        return self

    def predict(self, data=None, key=None, features=None, verbose=None, model=None):#pylint:disable=too-many-positional-arguments
        r"""
        Predict method for Multi Task MLP (for classification).

        Parameters
        ----------
        data : DataFrame
            DataFrame containing the data for prediction purpose.
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

            Defaults to False.

        Returns
        -------
        DataFrame
            Predict result.
        """
        return self._predict(data=data, key=key, features=features, verbose=verbose, model=model)

class MLPMultiTaskRegressor(_MLPMultiTaskBase):
    r"""
    MLP Multi Task Regressor.

    Parameters
    ----------
    hidden_layer_size : list (tuple) of int, optional

        Specifies the sizes of all hidden layers in the neural network.

        Mandatory and valid only when ``network_type`` is 'basic' and ``finetune`` is not True.

    activation : str, optional
        Specifies the activation function for the hidden layer.

        Valid activation functions include:

        - 'sigmoid'
        - 'tanh'
        - 'relu'
        - 'leaky-relu'
        - 'elu'
        - 'gelu'

        Defaults to 'relu'.

    batch_size : int, optional

        Specifies the number of training samples in a batch.

        Defaults to 16 (if the input data contains less than 16 samples,
        the size of input dat is used).

    num_epochs : int, optional

        Specifies the maximum number of training epochs.

        Defaults to 100.

    random_state : int, optional
        Specifies the seed for random generation.
        Use system time when 0 is specified.

        Defaults to 0.
    use_batchnorm : bool, optional
        Specifies whether to use batch-normalization in each hidden layer or not.

        Defaults to True (i.e. use batch-normalization).

    learning_rate : float, optional

        Specifies the learning rate for gradient based optimizers.

        Defaults to 0.001.

    optimizer : str, optional
        Specifies the optimizer for training the neural network.

        - 'sgd'
        - 'rmsprop'
        - 'adam'
        - 'adagrad'

        Defaults to 'adam'.
    dropout_prob : float, optional
        Specifies the dropout probability applied when training the neural network.

        Defaults to 0.0 (i.e. no dropout).

    training_percentage : float, optional
        Specifies the percentage of input data used for training (with the rest of input data used for valiation).

        Defaults to 0.9.

    early_stop : bool, optional
        Specifies whether to use the automatic early stopping method or not.

        Defaults to True (i.e. use automatic early stopping)

    normalization : str, optional
        Specifies the normalization type for input data.

        - 'no' (no normalization)
        - 'z-transform'
        - 'scalar'

        Defaults to 'no'.

    warmup_epochs : int, optional
        Specifies the least number of epochs to wait before executing the auto early stopping method.

        Defaults to 5.

    patience : int, optional
        Specifies the uumber of epochs to wait before terminating the training if no improvement is shown.

        Defaults to 5.
    save_best_model : bool, optional
        Specifies whether to save the best model (regarding to the minimum loss on the validation set).

        Defaults to False (i.e. save the model from the last training epoch, not the best one).
    training_style : {'batch', 'stochastic'}, optional
        Specifies the training style of the learning algorithm, either in batch mode or in stochastic mode.

        - 'batch' : This approach uses the entire training dataset to update model parameters,
          where LBFGS-B optimizer is adopted. It can be stable but memory-intensive.
        - 'stochastic' : This approach updates parameters with individual samples based on gradient descent.
           While potentially less stable, it often leads to better generalization.

        Defaults to 'stochastic'.
    network_type : {'basic', 'resnet'}, optional
        Specifies the structure of the underlying neural-network to train. It can be a basic neural-network,
        or a neural-network comprising of residual blocks, i.e. ResNet.

        Defaults to 'basic' (corresponding to basic neural-network).

    embedded_num : int, optional
        Specifies the embedding dimension of ResNet for the input data, which equals to the dimension of the 1st linear
        in ResNet.

        Mandatory and valid when ``network_type`` is 'resnet' and ``finetune`` is not True.

    residual_num : int, optional
        Specifies the number of residual blocks in ResNet.

        Mandatory and valid when ``network_type`` is 'resnet' and ``finetune`` is not True.

    finetune : bool, optional
        Specifies the task type of the initialized class, i.e. whether it is used to finetune an existing pre-trained model,
        or trian a new model from scratch given the input data.

        Defaults to False.

    resampling_method : str, optional

        Specifies the resampling method for model evaluation or parameter selection.
        Valid options are listed as follows: 'cv', 'bootstrap', 'cv_sha', 'bootstrap_sha',
        'cv_hyperband', 'bootstrap_hyperband'.

        If not specified, neither model evaluation or parameter selection shall
        be triggered.

        .. note::
            Resampling methods with suffix 'sha' or 'hyperband' are for parameter selection only,
            not for model evaluation.

    evaluation_metric : {'rmse'}, optional

        Specifies the evaluation metric for model evaluation or parameter selection.
        Must be specified together with ``resampling_method`` to activate model evaluation
        or parameter selection.

        No default value.

    fold_num : int, optional

        Specifies the fold number for the cross-validation.

        Mandatory and valid only when ``resampling_method`` is specified as one of the following:
        'cv', 'cv_sha', 'cv_hyperband'.

    repeat_times : int, optional

        Specifies the number of repeat times for resampling.

        Defaults to 1.

    param_search_strategy : {'grid', 'random'}, optional

        Specifies the method for parameter selection.

        - if ``resampling_method`` is specified as 'cv_sha' or 'bootstrap_sha',
          then this parameter is mandatory.
        - if ``resampling_method`` is specified as 'cv_hyperband' or 'bootstrap_hyperband',
          then this parameter defaults to 'random' and cannot be changed.
        - otherwise this parameter has no default value, and parameter selection will not
          be activated if it is not specified.

    random_searhc_times : int, optional

        Specifies the number of times to randomly select candidate parameters.

        Mandatory and valid only when ``param_search_strategy`` is set to 'random'.

    random_state : int, optional

        Specifies the seed for random generation.

        When 0 is specified, system time is used.

        Defaults to 0.

    timeout : int, optional

        Specifies maximum running time for model evaluation/parameter selection,
        in seconds.

        No timeout when 0 is specified.

        Defaults to 0.

    progress_indicator_id : str, optional

        Sets an ID of progress indicator for model evaluation/parameter selection.

        If not provided, no progress indicator is activated.

    param_values : dict or list of tuples, optional

        Specifies the values of following parameters for model parameter selection:

            ``hidden_layer_size``, ``residual_num``, ``embedded_dim``,
            ``activation``, ``learning_rate``, ``optimizer``, ``dropout_prob``.

        If input is list of tuples, then each tuple must contain exactly two elements:

            - 1st element is the parameter name(str type),
            - 2nd element is a list of valid values for that parameter.

        Otherwise, if input is dict, then for each element, the key must be a parameter name, while value
        be a list of valid values for that parameter.

        A simple example for illustration:

            [('learning_rate', [0.1, 0.2, 0.5]), ('hidden_layer_size', [[10, 10], [100]])],

        or

            dict(learning_rate=[0.1, 0.2, 0.5], hidden_layer_size=[[10, 10], [100]]).

        Valid only when ``resampling_method`` and ``param_search_strategy`` are both specified,
        and ``training_style`` is 'stochastic'.

    param_range : dict or list of tuple, optional

        Sets the range of the following parameters for model parameter selection:

            ``residual_num``, ``embedded_dim``, ``learning_rate``, ``dropout_prob``

        If input is a list of tuples, the each tuple should contain exactly two elements:

            - 1st element is the parameter name(str type),
            - 2nd element is a list that specifies the range of that parameter as follows:
              first value is the start value, second value is the step, and third value is the end value.
              The step value can be omitted, and will be ignored, if ``param_search_strategy`` is set to 'random'.

        Otherwise, if input is a dict, then for each element the key should be parameter name, while value
        specifies the range of that parameter.

        Valid only when ``resampling_method`` and ``param_search_strategy`` are both specified and
        ``training_style`` is 'stochastic'.

    reduction_rate : float, optional
        Specifies reduction rate in SHA or Hyperband method.

        For each round, the available parameter candidate size will be divided by value of this parameter.
        Thus valid value for this parameter must be greater than 1.0

        Valid only when ``resampling_method`` takes one of the following values:
        'cv_sha', 'bootstrap_sha', 'cv_hyperband', 'bootstrap_hyperband'.

        Defaults to 3.0.

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
    model_ : DataFrame

        The MLP model.

    train_log_ : DataFrame

        Provides training errors among iterations.

    stats_ : DataFrame

        Names and values of statistics.

    optim_param_ : DataFrame

        Provides optimal parameters selected.

    Examples
    --------
    >>> train_data.collect()
       ID          X1          X2          X3         Y1     Y2
    0   0         1.0        10.0       100.0         1.0     1
    1   1         1.1        10.1       100.0         1.1     1
    2   2         2.2        20.2        11.0        10.0     2
    3   3         2.3        20.4        12.0        10.1     2
    4   4         2.2        20.3        25.0        10.2     1
    .   .         ...         ...         ...          .      .

    >>> mlp = MLPMultiTaskRegressor(hidden_layer_size=[5,5,5],
    ...                              activation='leaky-relu')
    >>> mlp.fit(data=train_data, key='ID',
    ...         label=['Y1', 'Y2'])
    """
    resampling_method_list = ['cv', 'cv_sha', 'cv_hyperband', 'bootstrap',
                              'bootstrap', 'bootstrap_sha', 'bootstrap_hyperband']
    evaluation_metric_map = {'rmse':'RMSE'}
    def __init__(self,#pylint: disable=too-many-arguments, too-many-branches, too-many-statements, too-many-locals, too-many-positional-arguments
                 hidden_layer_size=None,
                 activation=None,
                 batch_size=None,
                 num_epochs=None,
                 random_state=None,
                 use_batchnorm=None,
                 learning_rate=None,
                 optimizer=None,
                 dropout_prob=None,
                 training_percentage=None,
                 early_stop=None,
                 normalization=None,
                 warmup_epochs=None,
                 patience=None,
                 save_best_model=None,
                 training_style=None,
                 network_type=None,
                 embedded_num=None,
                 residual_num=None,
                 finetune=None,
                 resampling_method=None,
                 evaluation_metric=None,
                 fold_num=None,
                 repeat_times=None,
                 param_search_strategy=None,
                 random_search_times=None,
                 timeout=None,
                 progress_indicator_id=None,
                 reduction_rate=None,
                 aggressive_elimination=None,
                 param_range=None,
                 param_values=None):
        super(MLPMultiTaskRegressor, self).__init__(functionality=1,
                                              hidden_layer_size=hidden_layer_size,
                                              activation=activation,
                                              batch_size=batch_size,
                                              num_epochs=num_epochs,
                                              random_state=random_state,
                                              use_batchnorm=use_batchnorm,
                                              learning_rate=learning_rate,
                                              optimizer=optimizer,
                                              dropout_prob=dropout_prob,
                                              training_percentage=training_percentage,
                                              early_stop=early_stop,
                                              normalization=normalization,
                                              warmup_epochs=warmup_epochs,
                                              patience=patience,
                                              save_best_model=save_best_model,
                                              training_style=training_style,
                                              network_type=network_type,
                                              embedded_num=embedded_num,
                                              residual_num=residual_num,
                                              finetune=finetune,
                                              resampling_method=resampling_method,
                                              evaluation_metric=evaluation_metric,
                                              fold_num=fold_num,
                                              repeat_times=repeat_times,
                                              param_search_strategy=param_search_strategy,
                                              random_search_times=random_search_times,
                                              timeout=timeout,
                                              progress_indicator_id=progress_indicator_id,
                                              reduction_rate=reduction_rate,
                                              aggressive_elimination=aggressive_elimination,
                                              param_range=param_range,
                                              param_values=param_values)
        self.op_name = 'MLP_M_TASK_Regressor'

    def fit(self, data=None, key=None,#pylint:disable=too-many-positional-arguments
            features=None, label=None,
            categorical_variable=None,
            pre_model=None):
        r"""
        Fit function for Multi Task MLP (for regression).

        Parameters
        ----------
        data : DataFrame
            DataFrame containing the data.

            Note that if ``finetune`` is set as True when class is initialized,
            then ``data`` must be structured the same as the one used for training the model
            stored in ``pre_model``.

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

        label : str or a list of str, optional
            Name of the target columns.

            If not provided, it defaults to the last non-ID column.

        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.
        pre_model : DataFrame, optional
            Specifies the pre-model for online/continued training.

            Mandatory and valid only if ``finetune`` is set as True when class is initialized.

        Returns
        -------
        A fitted object of class "MLPMultiTaskRegressor".
        """
        self._fit(data=data, key=key, features=features, label=label,
                  categorical_variable=categorical_variable,
                  pre_model=pre_model)
        return self

    def predict(self, data=None, key=None, features=None, model=None):
        r"""
        Predict metho for Multi Task MLP (for regression).

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
            non-ID, non-label columns..

        Returns
        -------
        DataFrame
            Predict result.
        """
        return self._predict(data=data, key=key, features=features, model=model)
