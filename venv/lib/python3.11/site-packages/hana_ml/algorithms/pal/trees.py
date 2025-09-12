"""
This module includes decision tree-based models for classification and regression.

The following classes are available:

    * :class:`DecisionTreeClassifier`
    * :class:`DecisionTreeRegressor`
    * :class:`RDTClassifier`
    * :class:`RDTRegressor`
    * :class:`GradientBoostingClassifier`
    * :class:`GradientBoostingRegressor`
    * :class:`HybridGradientBoostingClassifier`
    * :class:`HybridGradientBoostingRegressor`
"""
#pylint: disable=attribute-defined-outside-init,too-many-locals,too-many-branches,too-many-statements
#pylint: disable=too-many-lines,line-too-long,too-many-arguments,relative-beyond-top-level
#pylint: disable=super-with-arguments, c-extension-no-member, useless-super-delegation
#pylint: disable=too-many-instance-attributes
#pylint: disable=invalid-name
import logging
import uuid
import numpy as np
from deprecated import deprecated
from hdbcli import dbapi
from hana_ml.ml_exceptions import FitIncompleteError
from .utility import check_pal_function_exist
from .sqlgen import trace_sql
from .pal_base import (
    PALBase,
    ParameterTable,
    ListOfStrings,
    ListOfTuples,
    try_drop,
    pal_param_register,
    require_pal_usable
)
from . import metrics
logger = logging.getLogger(__name__) #pylint: disable=invalid-name

class _RDTBase(PALBase):#pylint: disable=too-many-instance-attributes, too-few-public-methods
    """
    Base Random forest model for classification and regression.
    """
    model_format_map = {'json':1, 'pmml':2}

    def __init__(self,
                 n_estimators=100,
                 max_features=None,
                 max_depth=None,
                 min_samples_leaf=None,
                 split_threshold=None,
                 calculate_oob=True,
                 random_state=None,
                 thread_ratio=None,
                 allow_missing_dependent=True,
                 categorical_variable=None, #move to fit()
                 sample_fraction=None,
                 compression=None,
                 max_bits=None,
                 quantize_rate=None,
                 fittings_quantization=None,
                 model_format=None):

        if not hasattr(self, 'hanaml_parameters'):
            setattr(self, 'hanaml_parameters', pal_param_register())
        super(_RDTBase, self).__init__()
        self.pal_funcname = 'PAL_RANDOM_DECISION_TREES'
        self.n_estimators = self._arg('n_estimators', n_estimators, int)
        self.max_features = self._arg('max_features', max_features, int)
        self.max_depth = self._arg('max_depth', max_depth, int)
        self.min_samples_leaf = self._arg('min_samples_leaf', min_samples_leaf, int)
        self.split_threshold = self._arg('split_threshold', split_threshold, float)
        if isinstance(self, RDTRegressor) and self.split_threshold is None:
            self.split_threshold = 1e-9
        self.calculate_oob = self._arg('calculate_oob', calculate_oob, bool)
        self.random_state = self._arg('random_state', random_state, int)
        self.thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        self.allow_missing_dependent = self._arg('allow_missing_dependent',
                                                 allow_missing_dependent, bool)
        if isinstance(categorical_variable, str):
            categorical_variable = [categorical_variable]
        self.categorical_variable = self._arg('categorical_variable',
                                              categorical_variable, ListOfStrings)
        self.sample_fraction = self._arg('sample_fraction', sample_fraction, float)
        self.strata = None
        self.priors = None
        self.compression = self._arg('compression', compression, bool)
        self.max_bits = self._arg('max_bits', max_bits, int)
        self.quantize_rate = self._arg('quantize_rate', quantize_rate, float)
        self.fittings_quantization = self._arg('fittings_quantization',
                                               fittings_quantization, bool)
        self.model_format = self._arg('model_format', model_format, self.model_format_map)
    #has_id default value is inconsistent with document
    @trace_sql
    def _fit(self, data, key=None, features=None, label=None, categorical_variable=None):
        conn = data.connection_context

        key = self._arg('key', key, str)
        features = self._arg('features', features, ListOfStrings)
        label = self._arg('label', label, str)
        if isinstance(categorical_variable, str):
            categorical_variable = [categorical_variable]
        categorical_variable = self._arg('categorical_variable', categorical_variable, ListOfStrings)
        if not self._disable_hana_execution:
            require_pal_usable(conn)
            data_ = data.rearrange(key=key, features=features, label=label)
            key = data.index if key is None else key
            if features is None:
                n_features = len(data.columns)
                if key or data.index:
                    n_features = n_features - 1
                if label:
                    n_features = n_features - 1
                else:
                    label = data_.columns[-1]
            else:
                n_features = len(features)

            if self.max_features is not None and self.max_features > n_features:
                msg = 'max_features should not be larger than the number of features in the input.'
                logger.error(msg)
                raise ValueError(msg)
        else:
            data_ = data

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        tables = ['MODEL', 'VAR_IMPORTANCE', 'OOB_ERR', 'CM']
        tables = ['#PAL_RDT_{}_TBL_{}_{}'.format(name, self.id, unique_id)
                  for name in tables]
        model_tbl, var_importance_tbl, oob_err_tbl, cm_tbl = tables

        param_rows = [
            ('TREES_NUM', self.n_estimators, None, None),
            ('TRY_NUM', self.max_features, None, None),
            ('MAX_DEPTH', self.max_depth, None, None),
            ('NODE_SIZE', self.min_samples_leaf, None, None),
            ('SPLIT_THRESHOLD', None, self.split_threshold, None),
            ('CALCULATE_OOB', self.calculate_oob, None, None),
            ('SEED', self.random_state, None, None),
            ('THREAD_RATIO', None, self.thread_ratio, None),
            ('ALLOW_MISSING_DEPENDENT', self.allow_missing_dependent, None, None),
            ('SAMPLE_FRACTION', None, self.sample_fraction, None),
            ('HAS_ID', key is not None, None, None),
            ('COMPRESSION', self.compression, None, None),
            ('MAX_BITS', self.max_bits, None, None),
            ('QUANTIZE_RATE', None, self.quantize_rate, None),
            ('FITTINGS_QUANTIZATION', self.fittings_quantization, None, None),
            ('MODEL_FORMAT', self.model_format, None, None)
            ]

        if categorical_variable is not None:
            param_rows.extend(('CATEGORICAL_VARIABLE', None, None, variable)
                              for variable in categorical_variable)
        elif self.categorical_variable is not None:
            param_rows.extend(('CATEGORICAL_VARIABLE', None, None, variable)
                              for variable in self.categorical_variable)
        if isinstance(self, RDTClassifier):
            param_rows.extend([('CATEGORICAL_VARIABLE', None, None, label)])
        if not self._disable_hana_execution:
            if self.strata is not None:
                label_t = data_.dtypes([label])[0][1]
                if label_t == 'INT':
                    param_rows.extend(('STRATA', class_type, fraction, None)
                                    for class_type, fraction in self.strata)
                elif label_t in {'VARCHAR', 'NVARCHAR'}:
                    param_rows.extend(('STRATA', None, fraction, class_type)
                                    for class_type, fraction in self.strata)
            if self.priors is not None:
                label_t = data_.dtypes([label])[0][1]
                if label_t == 'INT':
                    param_rows.extend(('PRIORS', class_type, prior_prob, None)
                                    for class_type, prior_prob in self.priors)
                elif label_t in {'VARCHAR', 'NVARCHAR'}:
                    param_rows.extend(('PRIORS', None, prior_prob, class_type,)
                                    for class_type, prior_prob in self.priors)
        else:
            if self.strata:
                param_rows.extend(('STRATA', None, fraction, class_type)
                                    for class_type, fraction in self.strata)
            if self.priors:
                param_rows.extend(('PRIORS', None, prior_prob, class_type,)
                                    for class_type, prior_prob in self.priors)

        try:
            self._call_pal_auto(conn,
                                "PAL_RANDOM_DECISION_TREES",
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
        if not self._disable_hana_execution:
            self.model_ = conn.table(model_tbl)
            self.feature_importances_ = conn.table(var_importance_tbl)
            if self.calculate_oob:
                self.oob_error_ = conn.table(oob_err_tbl)
            else:
                conn.table(oob_err_tbl)  # table() has to be called to enable correct sql tracing
            self._confusion_matrix_ = conn.table(cm_tbl)
        #cm_tbl is empty when calculate_oob is False in PAL

    missing_replacement_map = {'feature_marginalized':1, 'instance_marginalized':2}
    @trace_sql
    def _predict(self, data, key=None, features=None, verbose=None,
                 block_size=None, missing_replacement=None,
                 verbose_top_n=None):
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
        verbose = self._arg('verbose', verbose, bool)
        block_size = self._arg('block_size', block_size, int)
        missing_replacement = self._arg('missing_replacement',
                                        missing_replacement,
                                        self.missing_replacement_map)
        verbose_top_n = self._arg('verbose_top_n', verbose_top_n, int)
        cols = data.columns
        cols.remove(key)
        if features is None:
            features = cols

        data_ = data[[key] + features]

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()

        #tables = data_tbl, model_tbl, param_tbl, result_tbl = [
        result_tbl = '#PAL_RDT_RESULT_TBL_{}_{}'.format(self.id, unique_id)
        #    for name in ['DATA', 'MODEL', 'PARAM', 'FITTED']]

        param_rows = [
            ('THREAD_RATIO', None, self.thread_ratio, None),
            ('VERBOSE', verbose, None, None),
            ('BLOCK_SIZE', block_size, None, None),
            ('MISSING_REPLACEMENT', missing_replacement, None, None),
            ('VERBOSE_TOP_N', verbose_top_n, None, None)]

        #result_spec = [
        #    (parse_one_dtype(data.dtypes([data.columns[0]])[0])),
        #    ("SCORE", NVARCHAR(100)),
        #    ("CONFIDENCE", DOUBLE)
        #]
        try:
            #self._materialize(data_tbl, data)
            #self._materialize(model_tbl, self.model_)
            #self._create(ParameterTable(param_tbl).with_data(param_rows))
            #self._create(Table(result_tbl, result_spec))
            self._call_pal_auto(conn,
                                "PAL_RANDOM_DECISION_TREES_PREDICT",
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

    def create_model_state(self, model=None, function=None,
                           pal_funcname='PAL_RANDOM_DECISION_TREES',
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

            A placeholder parameter, not effective for RDT.

        pal_funcname : int or str, optional
            PAL function name.

            Defaults to 'PAL_RANDOM_DECISION_TREES'.

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

class RDTClassifier(_RDTBase):#pylint: disable=too-many-instance-attributes
    r"""
    The random decision trees algorithm is an ensemble learning method for classification and regression. It grows many classification and regression trees, and outputs the class (classification) that is voted by majority or mean prediction (regression) of the individual trees.

    The algorithm uses both bagging and random feature selection techniques. Each new training set is drawn with replacement from the original training set, and then a tree is grown on the new training set using random feature selection. Considering that the number of rows of the training data is n originally, two sampling methods for classification are available:

    Bagging: The sampling size is n, and each one is drawn from the original dataset with replacement.

    Stratified sampling: For class j, nj data is drawn from it with replacement. And n1+n2+… might not be exactly equal to n, but in PAL, the summation should not be larger than n, for the sake of out-of-bag error estimation. This method is used usually when imbalanced data presents.

    The random decision trees algorithm generates an internal unbiased estimate (out-of-bag error) of the generalization error as the trees building processes, which avoids cross-validation. It gives estimates of what variables are important from nodes’ splitting process. It also has an effective method for estimating missing data:

    1. Training data: If the mth variable is numerical, the method computes the median of all values of this variable in class j or computes the most frequent non-missing value in class j, and then it uses this value to replace all missing values of the mth variable in class j.

    2. Test data: The class label is absent, therefore one missing value is replicated n times, each filled with the corresponding class’ most frequent item or median.

    Parameters
    ----------
    n_estimators : int, optional
        Specifies the number of decision trees in the model.

        Defaults to 100.
    max_features : int, optional
        Specifies the number of randomly selected splitting variables.

        Should not be larger than the number of input features.
        Defaults to sqrt(p), where p is the number of input features.
    max_depth : int, optional
        The maximum depth of a tree, where -1 means unlimited.

        Default to 56.
    min_samples_leaf : int, optional
        Specifies the minimum number of records in a leaf.

        Defaults to 1 for classification.
    split_threshold : float, optional
        Specifies the stop condition: if the improvement value of the best
        split is less than this value, the tree stops growing.

        Defaults to 1e-5.
    calculate_oob : bool, optional
        If true, calculate the out-of-bag error.

        Defaults to True.
    random_state : int, optional
        Specifies the seed for random number generator.

            - 0: Uses the current time (in seconds) as the seed.
            - Others: Uses the specified value as the seed.

        Defaults to 0.
    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to -1.
    allow_missing_dependent : bool, optional
        Specifies if a missing target value is allowed.

            - False: Not allowed. An error occurs if a missing target is present.
            - True: Allowed. The datum with the missing target is removed.

        Defaults to True.
    categorical_variable : str or a list of str, optional
        Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

        Default value detected from input data.
    sample_fraction : float, optional
        The fraction of data used for training.

        Assume there are n pieces of data, sample fraction is r, then n*r
        data is selected for training.

        Defaults to 1.0.
    compression : bool, optional
        Specifies if the model is stored in compressed format.

        Default value depends on the SAP HANA Version. Please refer to the corresponding documentation of SAP HANA PAL.

    max_bits : int, optional
        The maximum number of bits to quantize continuous features.

        Equivalent to use :math:`2^{max\_bits}` bins.

        Must be less than 31.

        Valid only when the value of ``compression`` is True.

        Defaults to 12.

    quantize_rate : float, optional
        Quantizes a categorical feature if the largest class frequency of the feature is less than quantize_rate.

        Valid only when ``compression`` is True.

        Defaults to 0.005.

    strata : List of tuples: (class, fraction), optional
        Strata proportions for stratified sampling.

        A (class, fraction) tuple specifies that rows with that class should make up the specified
        fraction of each sample.

        If the given fractions do not add up to 1, the remaining portion is divided equally between classes with
        no entry in ``strata``, or between all classes if all classes have
        an entry in ``strata``.

        If ``strata`` is not provided, bagging is used instead of stratified
        sampling.
    priors : List of tuples: (class, prior_prob), optional
        Prior probabilities for classes.

        A (class, prior_prob) tuple specifies the prior probability of this class.

        If the given priors do not add up to 1, the remaining portion is divided equally
        between classes with no entry in ``priors``, or between all classes
        if all classes have an entry in 'priors'.

        If ``priors`` is not provided, it is determined by the proportion of
        every class in the training data.
    model_format : {'json', 'pmml'}, optional
        Specifies the model format to store, case-insensitive.

        - 'json': export model in json format.
        - 'pmml': export model in pmml format.

        Not effective if ``compression`` is True, in which case
        the model is stored in neither json nor pmml, but compressed format.

        Defaults to 'pmml'.

    References
    ----------
    Parameters ``compression``, ``max_bits`` and ``quantize_rate`` are for compressing Random Decision Trees
    classification model, please see :ref:`Model Compression<model_compression_rdt-label>` for more details
    about this topic.

    Attributes
    ----------
    model_ : DataFrame
        Model content.

    feature_importances_ : DataFrame

        The feature importance (the higher, the more important the feature).

    oob_error_ : DataFrame

        Out-of-bag error rate or mean squared error for random decision trees up
        to indexed tree.
        Set to None if ``calculate_oob`` is False.

    confusion_matrix_ : DataFrame

        Confusion matrix used to evaluate the performance of classification
        algorithms.

    Examples
    --------
    >>> rfc = RDTClassifier(n_estimators=3, max_features=3,  split_threshold=0.00001,
                            calculate_oob=True, min_samples_leaf=1, thread_ratio=1.0)

    Perform fit():

    >>> rfc.fit(data=df_train, key='ID', features=['F1', 'F2'], label='LABEL')
    >>> rfc.feature_importances_.collect()

    Perform predict():

    >>> res = rfc.predict(data=df_predict, key='ID', verbose=False)
    >>> res.collect()

    Perform score():

    >>> rfc.score(data=df_score, key='ID')
    """

    #pylint:disable=too-many-arguments
    def __init__(self,
                 n_estimators=100,
                 max_features=None,
                 max_depth=None,
                 min_samples_leaf=1,
                 split_threshold=None,
                 calculate_oob=True,
                 random_state=None,
                 thread_ratio=None,
                 allow_missing_dependent=True,
                 categorical_variable=None,
                 sample_fraction=None,
                 compression=None,
                 max_bits=None,
                 quantize_rate=None,
                 strata=None,
                 priors=None,
                 model_format=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(RDTClassifier, self).__init__(n_estimators,
                                            max_features, max_depth,
                                            min_samples_leaf, split_threshold,
                                            calculate_oob, random_state, thread_ratio,
                                            allow_missing_dependent, categorical_variable,
                                            sample_fraction, compression, max_bits, quantize_rate, None, model_format)
        self.strata = self._arg('strata', strata, ListOfTuples)
        self.priors = self._arg('priors', priors, ListOfTuples)
        self.op_name = 'RDT_Classifier'

    def fit(self, data, key=None, features=None, label=None, categorical_variable=None):
        """
        Fit the model to the training dataset.

        Parameters
        ----------
        data : DataFrame
            Training data.

        key : str, optional
            Name of the ID column in ``data``.

            If ``key`` is not provided, then:

            - if ``data`` is indexed by a single column, then ``key`` defaults
              to that index column;
            - otherwise, it is assumed that ``data`` contains no ID column.

        features : a list of str, optional
            Names of the feature columns.

            If ``features`` is not provided, it defaults to all non-ID,
            non-label columns.

        label : str, optional
            Name of the dependent variable.

            Defaults to the last non-ID column.

        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.

        Returns
        -------
        A fitted object of class "RDTClassifier".
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        super(RDTClassifier, self)._fit(data, key, features, label, categorical_variable)
        # pylint: disable=attribute-defined-outside-init
        if not self._disable_hana_execution:
            self.confusion_matrix_ = self._confusion_matrix_
        return self

    def predict(self, data, key=None, features=None, verbose=None,
                block_size=None, missing_replacement=None,
                verbose_top_n=None):
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

        block_size : int, optional

            The number of rows loaded per time during prediction.
            0 indicates load all data at once.

            Defaults to 0.

        missing_replacement : str, optional

            The missing replacement strategy:

            - 'feature_marginalized': marginalise each missing feature out independently.
            - 'instance_marginalized': marginalise all missing features in an instance as a whole corresponding to each category.

            Defaults to feature_marginalized.

        verbose : bool, optional

            If true, output all classes and the corresponding confidences
            for each data point.

        verbose_top_n : int, optional

            Specifies the number of top n classes to present after sorting with confidences.
            It cannot exceed the number of classes in label of the training data, and it can be 0,
            which means to output the confidences of `all` classes.

            Effective only when ``verbose`` is set as True.

            Defaults to 0.

        Returns
        -------

        DataFrame
            DataFrame of score and confidence, structured as follows:

              - ID column, with same name and type as ``data`` 's ID column.
              - SCORE, type DOUBLE, representing the predicted classes.
              - CONFIDENCE, type DOUBLE, representing the confidence of a class.

        """
        return super(RDTClassifier, self)._predict(data=data, key=key,
                                                   features=features,
                                                   verbose=verbose,
                                                   block_size=block_size,
                                                   missing_replacement=missing_replacement,
                                                   verbose_top_n=verbose_top_n)

    def score(self, data, key=None, features=None, label=None,
              block_size=None, missing_replacement=None):
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

        features : a list of str, optional

            Names of the feature columns.
            If ``features`` is not provided, it defaults to all non-ID,
            non-label columns.

        label : str, optional

            Name of the dependent variable.

            Defaults to the last non-ID column.

        block_size : int, optional

            The number of rows loaded per time during prediction.
            0 indicates load all data at once.

            Defaults to 0.

        missing_replacement : str, optional

            The missing replacement strategy:

              - 'feature_marginalized': marginalise each missing feature out independently.
              - 'instance_marginalized': marginalise all missing features in an instance as a whole corresponding to each category.

            Defaults to 'feature_marginalized'.

        Returns
        -------

        float

            Mean accuracy on the given test data and labels.
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
        #input check for block_size and missing_replacement is done in predict()

        cols = data.columns
        cols.remove(key)
        if label is None:
            label = cols[-1]
        cols.remove(label)
        if features is None:
            features = cols

        prediction = self.predict(data=data, key=key, features=features,
                                  block_size=block_size,
                                  missing_replacement=missing_replacement)
        prediction = prediction.select(key, 'SCORE').rename_columns(['ID_P', 'PREDICTION'])

        actual = data.select(key, label).rename_columns(['ID_A', 'ACTUAL'])
        joined = actual.join(prediction, 'ID_P=ID_A').select('ACTUAL', 'PREDICTION')

        accuracy = metrics.accuracy_score(joined,
                                          label_true='ACTUAL',
                                          label_pred='PREDICTION')
        setattr(self, 'score_metrics_', {"ACCURACY": accuracy})
        return accuracy

class RDTRegressor(_RDTBase):#pylint: disable=too-many-instance-attributes
    r"""
    The random decision trees algorithm is an ensemble learning method for classification and regression. It grows many classification and regression trees, and outputs the class (classification) that is voted by majority or mean prediction (regression) of the individual trees.

    The algorithm uses both bagging and random feature selection techniques. Each new training set is drawn with replacement from the original training set, and then a tree is grown on the new training set using random feature selection. Considering that the number of rows of the training data is n originally, two sampling methods for classification are available:

    Bagging: The sampling size is n, and each one is drawn from the original dataset with replacement.

    Stratified sampling: For class j, nj data is drawn from it with replacement. And n1+n2+… might not be exactly equal to n, but in PAL, the summation should not be larger than n, for the sake of out-of-bag error estimation. This method is used usually when imbalanced data presents.

    The random decision trees algorithm generates an internal unbiased estimate (out-of-bag error) of the generalization error as the trees building processes, which avoids cross-validation. It gives estimates of what variables are important from nodes’ splitting process. It also has an effective method for estimating missing data:

    1. Training data: If the mth variable is numerical, the method computes the median of all values of this variable in class j or computes the most frequent non-missing value in class j, and then it uses this value to replace all missing values of the mth variable in class j.

    2. Test data: The class label is absent, therefore one missing value is replicated n times, each filled with the corresponding class’ most frequent item or median.

    Parameters
    ----------
    n_estimators : int, optional
        Specifies the number of decision trees in the model.

        Defaults to 100.
    max_features : int, optional
        Specifies the number of randomly selected splitting variables.

        Should not be larger than the number of input features.

        Defaults to p/3, where p is the number of input features.

    max_depth : int, optional
        The maximum depth of a tree, where -1 means unlimited.

        Default to 56.
    min_samples_leaf : int, optional
        Specifies the minimum number of records in a leaf.

        Defaults to 5.
    split_threshold : float, optional
        Specifies the stop condition: if the improvement value of the best
        split is less than this value, the tree stops growing.

        Defaults to 1e-9.
    calculate_oob : bool, optional
        If True, calculate the out-of-bag error.

        Defaults to True.
    random_state : int, optional
        Specifies the seed for random number generator.

          - 0: Uses the current time (in seconds) as the seed.
          - Others: Uses the specified value as the seed.

        Defaults to 0.
    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to -1.
    allow_missing_dependent : bool, optional
        Specifies if a missing target value is allowed.

        - False: Not allowed. An error occurs if a missing target is present.
        - True: Allowed. The datum with a missing target is removed.

        Defaults to True.
    categorical_variable : str or a list of str, optional
        Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

        Default value detected from input data.
    sample_fraction : float, optional
        The fraction of data used for training.

        Assume there are n pieces of data, sample fraction is r, then n*r
        data is selected for training.

        Defaults to 1.0.
    compression : bool, optional
        Specifies if the model is stored in compressed format.

        Default value depends on the SAP HANA Version. Please refer to the corresponding documentation of SAP HANA PAL.

    max_bits : int, optional
        The maximum number of bits to quantize continuous features,
        equivalent to use :math:`2^{max\_bits}` bins.

        Must be less than 31.

        Valid only when the value of ``compression`` is True.

        Defaults to 12.
    quantize_rate : float, optional
        Quantizes a categorical feature if the largest class frequency of the feature is less than ``quantize_rate``.

        Valid only when ``compression`` is True.

        Defaults to 0.005.
    fittings_quantization : int, optional
        Indicates whether to quantize fitting values.

        Valid only for regression when ``compression`` is True.

        Defaults to False.
    model_format : {'json', 'pmml'}, optional
        Specifies the tree model format for store. Case-insensitive.

        - 'json': export model in json format.
        - 'pmml': export model in pmml format.

        Not effective if ``compression`` is True, in which case
        the model is stored in neither json nor pmml, but compressed format.

        Defaults to 'pmml'.

    References
    ----------
    Parameters ``compression``, ``max_bits``, ``quantize_rate`` and ``fittings_quantization`` are for compressing
    Random Decision Trees regression model, please see :ref:`Model Compression<model_compression_rdt-label>` for
    more details about this topic.

    Attributes
    ----------
    model_ : DataFrame
        Model content.
    feature_importances_ : DataFrame
        The feature importance (the higher, the more important the feature).
    oob_error_ : DataFrame
        Out-of-bag error rate or mean squared error for random decision trees
        up to indexed tree.
        Set to None if ``calculate_oob`` is False.

    Examples
    --------
    >>> rfr = RDTRegressor()

    Perform fit():

    >>> rfr.fit(data=df_train, key='ID')
    >>> rfr.feature_importances_.collect()

    Perform predict():

    >>> res = rfr.predict(data=df_predict, key='ID')
    >>> res.collect()

    Perform score():

    >>> rfr.score(data=df_score, key='ID')

    """
    op_name = 'RDT_Regressor'
    def fit(self, data, key=None, features=None, label=None, categorical_variable=None):
        """
        Fit the model to the training dataset.

        Parameters
        ----------
        data : DataFrame
            Training data.

        key : str, optional
            Name of the ID column in ``data``.

            If ``key`` is not provided, then:

            - if ``data`` is indexed by a single column, then ``key`` defaults
              to that index column;
            - otherwise, it is assumed that ``data`` contains no ID column.

        features : a list of str, optional
            Names of the feature columns.

            If ``features`` is not provided, it defaults to all non-ID,
            non-label columns.

        label : str, optional
            Name of the dependent variable.

            Defaults to the last non-ID column.

        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.

        Returns
        -------
        A fitted object of class "RDTRegressor".
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        super(RDTRegressor, self)._fit(data, key, features, label, categorical_variable)
        return self

    def predict(self, data, key=None, features=None, verbose=None, block_size=None, missing_replacement=None):#pylint:disable=too-many-arguments
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

        verbose : bool, optional(deprecated)

            If true, output all classes and the corresponding confidences
            for each data point. Only valid for classification.

            Not valid for regression problem and will be removed in future release.

        block_size : int, optional

            The number of rows loaded per time during prediction.

            0 indicates load all data at once.

            Defaults to 0.

        missing_replacement : str, optional(deprecated)
            The missing replacement strategy:

            - 'feature_marginalized': marginalise each missing feature out independently.
            - 'instance_marginalized': marginalise all missing features in an instance as a whole corresponding to each category.

            No valid for regression problem and will be removed in future release.

        Returns
        -------

        DataFrame

            DataFrame of score and confidence, structured as follows:

              - ID column, with same name and type as ``data``'s ID column.
              - SCORE, type DOUBLE, representing the predicted values.
              - CONFIDENCE, all 0s. It is included due to the fact PAL uses the same table for classification.
        """
        return super(RDTRegressor, self)._predict(data=data, key=key,
                                                  features=features,
                                                  verbose=verbose,
                                                  block_size=block_size,
                                                  missing_replacement=missing_replacement)

    def score(self, data, key=None, features=None, label=None,
              block_size=None, missing_replacement=None):
        """
        Returns the coefficient of determination R2 of the prediction.

        Parameters
        ----------

        data : DataFrame

            Data on which to assess model performance.

        key : str, optional

            Name of the ID column.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        features : a list of str, optional

            Names of the feature columns.

            If ``features`` is not provided, it defaults to all non-ID,
            non-label columns.

        label : str, optional

            Name of the dependent variable.

            Defaults to the last non-ID column.

        block_size : int, optional

            The number of rows loaded per time during prediction.

            0 indicates load all data at once.

            Defaults to 0.

        missing_replacement : str, optional(deprecated)

            The missing replacement strategy:

              - 'feature_marginalized': marginalise each missing feature out independently.

              - 'instance_marginalized': marginalise all missing features in an instance as a whole corresponding to each category.

            No valid for regression problem and will be removed in future release.

        Returns
        -------

        float

            The coefficient of determination R^2 of the prediction on the
            given data.
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
        #input check for block_size and missing_replacement is done in predict()

        cols = data.columns
        cols.remove(key)
        if label is None:
            label = cols[-1]
        cols.remove(label)
        if features is None:
            features = cols

        prediction = self.predict(data=data, key=key, features=features,
                                  block_size=block_size,
                                  missing_replacement=missing_replacement)
        original = data[[key, label]]
        prediction = prediction.select([key, 'SCORE']).rename_columns(['ID_P', 'PREDICTION'])
        original = data[[key, label]].rename_columns(['ID_A', 'ACTUAL'])
        joined = original.join(prediction, 'ID_P=ID_A').select('ACTUAL', 'PREDICTION')
        r2 = metrics.r2_score(joined,
                              label_true='ACTUAL',
                              label_pred='PREDICTION')
        setattr(self, 'score_metrics_', {"R2": r2})
        return r2

class RandomForestClassifier(RDTClassifier):
    """
    Alias of Random Decision Tree model for classification.
    """

class RandomForestRegressor(RDTRegressor):
    """
    Alias of Random Decision Tree model for regression.
    """
#pylint: disable=no-member
class _DecisionTreeBase(PALBase):#pylint: disable=too-many-instance-attributes
    """
    Base Decision tree model for classification and regression.
    """
    #map_map = {"discretization_type": discretization_type_map}
    func_map = {'classification': 'classification', 'regression': 'regression'}
    model_format_map = {'json':1, 'pmml':2}
    discretization_type_map = {'mdlpc':0, 'equal_freq':1}
    evaluation_map = {'classification': {'error_rate': 'ERROR_RATE', 'nll': 'NLL', 'auc': 'AUC'},
                      'regression': {'mae': 'MAE', 'rmse': 'RMSE'}}
    search_strategy_map = {'grid': 'grid', 'random': 'random'}
    values_list = ['discretization_type', 'min_records_of_leaf', 'min_records_of_parent',
                   'max_depth', 'split_threshold', 'max_branch', 'merge_threshold']

    def __init__(self,
                 algorithm='cart',
                 thread_ratio=None,
                 allow_missing_dependent=True,
                 percentage=None,
                 min_records_of_parent=None,
                 min_records_of_leaf=None,
                 max_depth=None,
                 categorical_variable=None,
                 split_threshold=None,
                 use_surrogate=None,
                 model_format=None,
                 output_rules=True,
                 resampling_method=None,
                 fold_num=None,
                 repeat_times=None,
                 evaluation_metric=None,
                 timeout=None,
                 search_strategy=None,
                 random_search_times=None,
                 progress_indicator_id=None,
                 param_values=None,
                 param_range=None,
                 functionality=None
                ):

        if not hasattr(self, 'hanaml_parameters'):
            setattr(self, 'hanaml_parameters', pal_param_register())
        super(_DecisionTreeBase, self).__init__()
        self.pal_funcname = 'PAL_DECISION_TREE'
        self.functionality = self._arg('functionality', functionality, self.func_map)
        self.algorithm = self._arg('algorithm', algorithm, self.algorithm_map, required=True)
        self.thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        self.allow_missing_dependent = self._arg('allow_missing_dependent', allow_missing_dependent, bool)#pylint:disable=line-too-long
        self.percentage = self._arg('percentage', percentage, float)
        self.min_records_of_parent = self._arg('min_records_of_parent', min_records_of_parent, int)
        self.min_records_of_leaf = self._arg('min_records_of_leaf', min_records_of_leaf, int)
        self.max_depth = self._arg('max_depth', max_depth, int)
        if isinstance(categorical_variable, str):
            categorical_variable = [categorical_variable]
        self.categorical_variable = self._arg('categorical_variable', categorical_variable, ListOfStrings)#pylint:disable=line-too-long
        self.split_threshold = self._arg('split_threshold', split_threshold, float)
        self.priors = None
        self.discretization_type = None
        self.bins = None
        self.max_branch = None
        self.merge_threshold = None
        self.use_surrogate = self._arg('use_surrogate', use_surrogate, bool)
        if use_surrogate is not None:
            if self.algorithm != self.algorithm_map['cart']:
                msg = ("use_surrogate is inapplicable. " +
                       "It is only applicable when algorithm is cart.")
                logger.error(msg)
                raise ValueError(msg)
        self.model_format = self._arg('model_format', model_format, self.model_format_map)
        self.output_rules = self._arg('output_rules', output_rules, bool)
        self.output_confusion_matrix = None
        self.resampling_method = self._arg('resampling_method', resampling_method, self.resampling_map)#pylint:disable=line-too-long
        self.evaluation_metric = self._arg('evaluation_metric', evaluation_metric, self.evaluation_map[functionality])#pylint:disable=line-too-long
        self.fold_num = self._arg('fold_num', fold_num, int)
        self.repeat_times = self._arg('repeat_times', repeat_times, int)
        self.search_strategy = self._arg('search_strategy', search_strategy, self.search_strategy_map)#pylint:disable=line-too-long
        self.random_search_times = self._arg('random_search_times', random_search_times, int)
        self.timeout = self._arg('timeout', timeout, int)
        self.progress_indicator_id = self._arg('progress_indicator_id', progress_indicator_id, str)
        if isinstance(param_range, dict):
            param_range = [(x, param_range[x]) for x in param_range]
        if isinstance(param_values, dict):
            param_values = [(x, param_values[x]) for x in param_values]
        self.param_values = self._arg('param_values', param_values, ListOfTuples)
        self.param_range = self._arg('param_range', param_range, ListOfTuples)
        if param_values is not None:
            for i, par_val in enumerate(param_values):
                if par_val[0] == 'discretizaition_type':
                    self.param_values[i] = ('discretizaition_type', [0, 1])
        search_param_count = 0
        for param in (self.resampling_method, self.evaluation_metric):
            if param is not None:
                search_param_count += 1
        if search_param_count not in (0, 2):
            msg = ("'resampling_method', and 'evaluation_metric' must be set together.")
            logger.error(msg)
            raise ValueError(msg)
        if self.search_strategy is not None and self.resampling_method is None:
            msg = ("'search_strategy' cannot be set if 'resampling_method' is not specified.")
            logger.error(msg)
            raise ValueError(msg)
        if self.resampling_method in ('cv', 'stratified_cv') and self.fold_num is None:
            msg = ("'fold_num' must be set when "+
                   "'resampling_method' is set as 'cv' or 'stratified_cv'.")
            logger.error(msg)
            raise ValueError(msg)
        if self.resampling_method not in ('cv', 'stratified_cv') and self.fold_num is not None:
            msg = ("'fold_num' is not valid when parameter " +
                   "selection is not enabled, or 'resampling_method'"+
                   " is not set as 'cv' or 'stratified_cv'.")
            logger.error(msg)
            raise ValueError(msg)
        if self.search_strategy == 'random' and self.random_search_times is None:
            msg = ("'random_search_times' must be set when "+
                   "'search_strategy' is set as random.")
            logger.error(msg)
            raise ValueError(msg)
        if self.search_strategy != 'random' and self.random_search_times is not None:
            msg = ("'random_search_times' is not valid " +
                   "when parameter selection is not enabled"+
                   ", or 'search_strategy' is not set as 'random'.")
            logger.error(msg)
            raise ValueError(msg)
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

    #has_id default value is inconsistent with document
    @trace_sql
    def _fit(self, data, key=None, features=None, label=None, categorical_variable=None):
        conn = data.connection_context
        require_pal_usable(conn)
        key = self._arg('key', key, str)
        features = self._arg('features', features, ListOfStrings)
        label = self._arg('label', label, str)
        if isinstance(categorical_variable, str):
            categorical_variable = [categorical_variable]
        categorical_variable = self._arg('categorical_variable', categorical_variable, ListOfStrings)
        if not self._disable_hana_execution:
            data_ = data.rearrange(key=key, features=features, label=label)
            key = data.index if key is None else key
        else:
            data_ = data
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        tables = ['MODEL', 'RULES', 'CM', 'STATS', 'CV']
        tables = ['#PAL_DECISION_TREE_{}_TBL_{}_{}'.format(name, self.id, unique_id)
                  for name in tables]
        model_tbl, rules_tbl, cm_tbl, stats_tbl, cv_tbl = tables#pylint:disable=unused-variable
        param_rows = [
            ('ALGORITHM', self.algorithm, None, None),
            ('THREAD_RATIO', None, self.thread_ratio, None),
            ('ALLOW_MISSING_DEPENDENT', self.allow_missing_dependent, None, None),
            ('PERCENTAGE', None, self.percentage, None),
            ('MIN_RECORDS_OF_PARENT', self.min_records_of_parent, None, None),
            ('MIN_RECORDS_OF_LEAF', self.min_records_of_leaf, None, None),
            ('MAX_DEPTH', self.max_depth, None, None),
            ('SPLIT_THRESHOLD', None, self.split_threshold, None),
            ('DISCRETIZATION_TYPE', self.discretization_type, None, None),
            ('MAX_BRANCH', self.max_branch, None, None),
            ('MERGE_THRESHOLD', None, self.merge_threshold, None),
            ('USE_SURROGATE', self.use_surrogate, None, None),
            ('MODEL_FORMAT', self.model_format, None, None),
            ('IS_OUTPUT_RULES', self.output_rules, None, None),
            ('IS_OUTPUT_CONFUSION_MATRIX', self.output_confusion_matrix, None, None),
            ('HAS_ID', key is not None, None, None)]
        if categorical_variable is not None:
            param_rows.extend(('CATEGORICAL_VARIABLE', None, None, variable)
                              for variable in categorical_variable)
        elif self.categorical_variable is not None:
            param_rows.extend(('CATEGORICAL_VARIABLE', None, None, variable)
                              for variable in self.categorical_variable)
        if self.functionality == 'classification':
            param_rows.extend([('CATEGORICAL_VARIABLE', None, None, label)])
        if self.priors is not None:
            param_rows.extend(('{}_PRIOR_'.format(class_type), None, prior_prob, None)
                              for class_type, prior_prob in self.priors)
        if self.bins is not None:
            param_rows.extend(('{}_BIN_'.format(col_name), num_bins, None, None)
                              for col_name, num_bins in self.bins)
        if self.resampling_method is not None:
            param_rows.extend([('RESAMPLING_METHOD', None, None, self.resampling_method)])
            param_rows.extend([('FOLD_NUM', self.fold_num, None, None)])
            param_rows.extend([('REPEAT_TIMES', self.repeat_times, None, None)])
            param_rows.extend([('EVALUATION_METRIC', None, None, self.evaluation_metric)])
            param_rows.extend([('TIMEOUT', self.timeout, None, None)])
            param_rows.extend([('PARAM_SEARCH_STRATEGY', None, None, self.search_strategy)])
            param_rows.extend([('RANDOM_SEARCH_TIMES', self.random_search_times, None, None)])
            param_rows.extend([('PROGRESS_INDICATOR_ID', None, None, self.progress_indicator_id)])
            if self.param_values is not None:
                for x in self.param_values:#pylint:disable=invalid-name
                    value_str = x[1]
                    #if isinstance(x[1][0], str):
                    #    value_str = [self.map_map[x[0]][val] for val in x[1]]
                    values = str(value_str).replace('[', '{').replace(']', '}')
                    param_rows.extend([(x[0].upper() + "_VALUES",
                                        None, None, values)])
            if self.param_range is not None:
                for x in self.param_range:#pylint:disable=invalid-name
                    range_ = str(x[1])
                    if len(x[1]) == 2 and self.search_strategy == 'random':
                        range_ = range_.replace(',', ',,')
                    param_rows.extend([(x[0].upper() + "_RANGE",
                                        None, None, range_)])
        try:
            self._call_pal_auto(conn,
                                "PAL_DECISION_TREE",
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
        # pylint: disable=attribute-defined-outside-init
        if not self._disable_hana_execution:
            self.model_ = conn.table(model_tbl)
            self.decision_rules_ = conn.table(rules_tbl) if self.output_rules else None
            self._confusion_matrix_ = conn.table(cm_tbl)
            self.stats_ = conn.table(stats_tbl)
            self.statistics_ = self.stats_
            self.cv_ = conn.table(cv_tbl)

    @trace_sql
    def _predict(self, data, key=None, features=None, verbose=False, verbose_top_n=None):
        '''
        Prediction for the fit model.
        '''
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
        verbose = self._arg('verbose', verbose, bool)
        verbose_top_n = self._arg('verbose_top_n', verbose_top_n, int)
        cols = data.columns
        cols.remove(key)
        if features is None:
            features = cols
        data_ = data[[key] + features]
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        result_tbl = '#PAL_DECISION_TREE_RESULT_TBL_{}_{}'.format(self.id, unique_id)
        param_rows = [('THREAD_RATIO', None, self.thread_ratio, None),
                      ('VERBOSE', verbose, None, None),
                      ('VERBOSE_TOP_N', verbose_top_n, None, None)]
        try:
            self._call_pal_auto(conn,
                                "PAL_DECISION_TREE_PREDICT",
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

    def create_model_state(self, model=None, function=None,
                           pal_funcname='PAL_DECISION_TREE',
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

            A placeholder parameter, not effective for Decision Tree.

        pal_funcname : int or str, optional
            PAL function name.
            Should be a valid PAL procedure name that supports model state.

            Defaults to 'PAL_DECISION_TREE'.

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

class DecisionTreeClassifier(_DecisionTreeBase):
    """
    A decision tree is used as a classifier for determining an appropriate action or decision among a predetermined set of actions for a given case.

    A decision tree helps effectively identify the factors to consider and how each factor has historically been associated with different outcomes of the decision. A decision tree uses a tree-like structure of conditions and their possible consequences. Each node of a decision tree can be a leaf node or a decision node.

    Leaf node: indicates the value of the dependent (target) variable.

    Decision node: contains one condition that specifies some test on an attribute value. The outcome of the condition is further divided into branches with sub-trees or leaf nodes.

    The PAL_DECISION_TREE function in PAL integrates the three most popular decision trees, including C45, CHAID, and CART. Some distinctions between the implements and usages of these 3 algorithms are listed as below.

    1. C45 and CHAID can generate non-binary trees, besides binary tree, while CART is restricted to binary tree.

    2. Unlike C45 and CHAID, CART is able to not only classify, but also do regression.

    3. C45 and CHAID treat missing independent variable as a special value, whereas CART applies surrogate split to handle it.

    4. As for ordered independent variable, C45 and CHAID firstly discretise it, whereas CART uses predicate {is xm ≤ c?} instead of {is xm ∈ s?} to handle it. For large dataset with a number of ordered independent variables, consequently, CART is more efficient than the other two.

    5. C45 uses information gain ratio, CHAID uses chi-square statistics, and CART uses Gini index (classification) or least square (regression), for split.

    In this function, the dependent variable, known as the class label or response, can have missing values, but datum of such kind is discarded before growing a tree. Meanwhile, independent variables consisting of identical value or only missing values are removed beforehand.

    Parameters
    ----------

    algorithm : {'c45', 'chaid', 'cart'}, optional
        Algorithm used to grow a decision tree. Case-insensitive.

        - 'c45': C4.5 algorithm.
        - 'chaid': Chi-square automatic interaction detection.
        - 'cart': Classification and regression tree.

        Defaults to 'cart'.
    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to -1.
    allow_missing_dependent : bool, optional
        Specifies if a missing target value is allowed.

        - False: Not allowed. An error occurs if a missing target is present.
        - True: Allowed. The datum with the missing target is removed.

        Defaults to True.
    percentage : float, optional
        Specifies the percentage of the input data that will be used to build
        the tree model.

        The rest of the data will be used for pruning.

        Defaults to 1.0.
    min_records_of_parent : int, optional
        Specifies the stop condition: if the number of records in one node is
        less than the specified value, the algorithm stops splitting.

        Defaults to 2.
    min_records_of_leaf : int, optional
        Promises the minimum number of records in a leaf.

        Defaults to 1.
    max_depth : int, optional
        The maximum depth of a tree.

        By default the value is unlimited.
    categorical_variable : str or a list of str, optional
        Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

        Default value detected from input data.
    split_threshold : float, optional
        Specifies the stop condition for a node:

        - C45: The information gain ratio of the best split is less than this value.
        - CHAID: The p-value of the best split is greater than or equal to this value.
        - CART: The reduction of Gini index or relative MSE of the best split is less than this value.

        The smaller the ``split_threshold`` value is, the larger a C45 or CART tree grows.

        On the contrary, CHAID will grow a larger tree with larger ``split_threshold`` value.

        Defaults to 1e-5 for C45 and CART, 0.05 for CHAID.
    discretization_type : {'mdlpc', 'equal_freq'}, optional
        Strategy for discretizing continuous attributes. Case-insensitive.

        - 'mdlpc': Minimum description length principle criterion.
        - 'equal_freq': Equal frequency discretization.

        Valid only when ``algorithm`` is 'c45' or 'chaid'.

        Defaults to 'mdlpc'.
    bins : List of tuples: (column name, number of bins), optional
        Specifies the number of bins for discretization.

        Only valid when ``discretizaition_type`` is 'equal_freq'.

        Defaults to 10 for each column.
    max_branch : int, optional
        Specifies the maximum number of branches.

        Valid only when ``algorithm`` is 'chaid'.

        Defaults to 10.
    merge_threshold : float, optional
        Specifies the merge condition for CHAID: if the metric value is
        greater than or equal to the specified value, the algorithm will
        merge two branches.

        Only valid when ``algorithm`` is 'chaid'.

        Defaults to 0.05.
    use_surrogate : bool, optional
        If true, use surrogate split when NULL values are encountered.

        Only valid when ``algorithm`` is 'cart'.

        Defaults to True.
    model_format : {'json', 'pmml'}, optional
        Specifies the tree model format for store. Case-insensitive.

        - 'json': export model in json format.
        - 'pmml': export model in pmml format.

        Defaults to 'json'.
    output_rules : bool, optional
        If true, output decision rules.

        Defaults to True.
    priors : List of tuples: (class, prior_prob), optional
        Specifies the prior probability of every class label.

        Default value detected from data.
    output_confusion_matrix : bool, optional
        If true, output the confusion matrix.

        Defaults to True.
    resampling_method : {'cv', 'stratified_cv', 'bootstrap', 'stratified_bootstrap'}, optional
        The resampling method for model evaluation or parameter search.

        Once set, model evaluation or parameter search is enabled.

        No default value.
    evaluation_metric : {'error_rate', 'nll', 'auc'}, optional
        The evaluation metric. Once ``resampling_method`` is set,
        this parameter must be set.

        No default value.
    fold_num : int, optional
        The fold number for cross validation.
        Valid only and mandatory when ``resampling_method`` is set
        as 'cv' or 'stratified_cv'.

        No default value.
    repeat_times : int, optional
        The number of repeated times for model evaluation or parameter selection.

        Defaults to 1.
    timeout : int, optional
        The time allocated (in seconds) for program running, where 0 indicates unlimited.

        Defaults to 0.

    random_search_times : int, optional
        Specifies the number of search times for random search.

        Only valid and mandatory when ``search_strategy`` is set as 'random'.

        No default value.
    progress_indicator_id : str, optional
        Sets an ID of progress indicator for model evaluation or parameter selection.

        No progress indicator is active if no value is provided.

        No default value.
    param_values : dict or ListOfTuples, optional

        Specifies values of parameters to be selected.

        Input should be a dict or a list of size-two tuples, with key/1st element
        being the target parameter name, while value/2nd element being the a list of valued for selection.

        Only valid when ``resampling_method`` and ``search_strategy`` are both specified.

        Valid Parameter names include: 'discretization_type', 'min_records_of_leaf',
        'min_records_of_parent', 'max_depth', 'split_threshold', 'max_branch', 'merge_threshold'.

        No default value.
    param_range : dict or ListOfTuples, optional

        Specifies ranges of parameters to be selected.

        Input should be dict or list of size-two tuples, with key/1st element being
        the name of the target parameter(in string format), while value/2nd element specifies the range of
        that parameter with [start, step, end] or [start, end].

        Valid only when ``resampling_method`` and ``search_strategy`` are both specified.

        Valid Parameter names include: 'discretization_type', 'min_records_of_leaf',
        'min_records_of_parent', 'max_depth', 'split_threshold', 'max_branch', 'merge_threshold'.

        No default value.

    Attributes
    ----------
    model_ : DataFrame
        Model content.
    decision_rules_ : DataFrame
        Rules for decision tree to make decisions.
        Set to None if ``output_rules`` is False.
    confusion_matrix_ : DataFrame
        Confusion matrix used to evaluate the performance of classification
        algorithms.
        Set to None if ``output_confusion_matrix`` is False.
    stats_ : DataFrame
        Statistics.
    cv_ : DataFrame
        Cross validation information.
        Only has output when parameter selection is enabled.

    Examples
    --------
    >>> dtc = DecisionTreeClassifier(algorithm='c45',
    ...                              min_records_of_parent=2,
    ...                              min_records_of_leaf=1,
    ...                              thread_ratio=0.4, split_threshold=1e-5,
    ...                              model_format='json', output_rules=True)

    Perform fit():

    >>> dtc.fit(data=df_train, features=['F1', 'F2'],
    ...         label='LABEL')
    >>> dtc.decision_rules_.collect()

    Perform predict():

    >>> res = dtc.predict(data=df_predict, key='ID', verbose=False)
    >>> res.collect()

    Perform score():

    >>> rfc.score(data=df_score, key='ID')

    """
    resampling_map = {'cv': 'cv', 'stratified_cv': 'stratified_cv', 'bootstrap': 'bootstrap',
                      'stratified_bootstrap': 'stratified_bootstrap'}
    algorithm_map = {'c45': 1, 'chaid': 2, 'cart': 3}
    def __init__(self,
                 algorithm='cart',
                 thread_ratio=None,
                 allow_missing_dependent=True,
                 percentage=None,
                 min_records_of_parent=None,
                 min_records_of_leaf=None,
                 max_depth=None,
                 categorical_variable=None,
                 split_threshold=None,
                 discretization_type=None,
                 bins=None,
                 max_branch=None,
                 merge_threshold=None,
                 use_surrogate=None,
                 model_format=None,
                 output_rules=True,
                 priors=None,
                 output_confusion_matrix=True,
                 resampling_method=None,
                 fold_num=None,
                 repeat_times=None,
                 evaluation_metric=None,
                 timeout=None,
                 search_strategy=None,
                 random_search_times=None,
                 progress_indicator_id=None,
                 param_values=None,
                 param_range=None
                ):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(DecisionTreeClassifier, self).__init__(algorithm,
                                                     thread_ratio,
                                                     allow_missing_dependent,
                                                     percentage,
                                                     min_records_of_parent,
                                                     min_records_of_leaf,
                                                     max_depth,
                                                     categorical_variable,
                                                     split_threshold,
                                                     use_surrogate,
                                                     model_format,
                                                     output_rules,
                                                     resampling_method,
                                                     fold_num,
                                                     repeat_times,
                                                     evaluation_metric,
                                                     timeout,
                                                     search_strategy,
                                                     random_search_times,
                                                     progress_indicator_id,
                                                     param_values,
                                                     param_range,
                                                     functionality='classification')
        self.op_name = 'DT_Classifier'
        self.discretization_type = self._arg('discretization_type',
                                             discretization_type,
                                             self.discretization_type_map)
        self.bins = self._arg('bins', bins, ListOfTuples)
        self.priors = self._arg('priors', priors, ListOfTuples)
        self.output_confusion_matrix = self._arg('output_confusion_matrix', output_confusion_matrix, bool)#pylint:disable=line-too-long
        self.max_branch = self._arg('max_branch', max_branch, int)
        self.merge_threshold = self._arg('merge_threshold', merge_threshold, float)
        if self.algorithm not in (self.algorithm_map['c45'], self.algorithm_map['chaid']) and self.discretization_type is not None:
            msg = ("discretization_type is inapplicable, " +
                   "when algorithm is not set as c45 or chaid.")
            logger.error(msg)
            raise ValueError(msg)
        if self.bins is not None and self.discretization_type != self.discretization_type_map['equal_freq']:
            msg = ("bins is inapplicable when discretization_type is not set as equal_freq.")
            logger.error(msg)
            raise ValueError(msg)
        if self.max_branch is not None and self.algorithm != self.algorithm_map['chaid']:
            msg = ("max_branch is inapplicable when algorithm is not set as chaid.")
            logger.error(msg)
            raise ValueError(msg)
        if self.merge_threshold is not None and self.algorithm != self.algorithm_map['chaid']:
            msg = ("merge_threshold is inapplicable when algorithm is not set as chaid.")
            logger.error(msg)
            raise ValueError(msg)
        if self.search_strategy is not None:
            set_param_list = []
            if self.max_branch is not None:
                set_param_list.append("max_branch")
            if self.merge_threshold is not None:
                set_param_list.append("merge_threshold")
            if self.split_threshold is not None:
                set_param_list.append("split_threshold")
            if self.max_depth is not None:
                set_param_list.append("max_depth")
            if self.min_records_of_parent is not None:
                set_param_list.append("min_records_of_parent")
            if self.min_records_of_leaf is not None:
                set_param_list.append("min_records_of_leaf")
            if self.discretization_type is not None:
                set_param_list.append("discretization_type")
            if self.param_values is not None:
                for x in self.param_values:#pylint:disable=invalid-name
                    if len(x) != 2:#pylint:disable=bad-option-value
                        msg = ("Each tuple that specifies the values of a parameter should"+
                               " contain exactly 2 elements: 1st is parameter name,"+
                               " 2nd is a list of valid values.")
                        logger.error(msg)
                        raise ValueError(msg)
                    if x[0] not in self.values_list:
                        msg = ("Specifying the values of `{}` for ".format(x[0])+
                               "parameter selection is invalid.")
                        logger.error(msg)
                        raise ValueError(msg)
                    if x[0] in set_param_list:
                        msg = ("Parameter `{}` is invalid for ".format(x[0])+
                               "re-specification in parameter selection.")
                        logger.error(msg)
                        raise ValueError(msg)
                    if x[0] == 'discretization_type' and self.algorithm not in (1, 2):
                        msg = ("discretization_type is inapplicable, " +
                               "since algorithm is {} instead of c45 or chaid.".format(algorithm))
                        logger.error(msg)
                        raise ValueError(msg)
                    if x[0] in ('max_branch', 'merge_threshold') and self.algorithm != 2:
                        msg = ("'{}' is inapplicable when algorithm is not set as chaid.".format(x[0]))
                        logger.error(msg)
                        raise ValueError(msg)
                    if x[0] in ('min_records_of_parent', 'min_records_of_leaf', 'max_depth', 'max_branch') and not (isinstance(x[1], list) and all(isinstance(t, int) for t in x[1])):#pylint:disable=line-too-long
                        msg = "Valid values of `{}` must be a list of int.".format(x[0])
                        logger.error(msg)
                        raise TypeError(msg)
                    if (x[0] in ('split_threshold', 'merge_threshold')) and not (isinstance(x[1], list) and all(isinstance(t, (int, float)) for t in x[1])):#pylint:disable=line-too-long
                        msg = "Valid values of `{}` must be a list of numerical values.".format(x[0])
                        logger.error(msg)
                        raise TypeError(msg)
                    set_param_list.append(x[0])

            if self.param_range is not None:
                rsz = [3] if self.search_strategy == 'grid' else [2, 3]
                for x in self.param_range:#pylint:disable=invalid-name
                    if len(x) != 2:#pylint:disable=bad-option-value
                        msg = ("Each tuple that specifies the range of a parameter should contain"+
                               " exactly 2 elements: 1st is parameter name, 2nd is value range.")
                        logger.error(msg)
                        raise ValueError(msg)
                    if x[0] not in self.values_list:
                        msg = ("Specifying the values of `{}` for ".format(x[0])+
                               "parameter selection is invalid.")
                        logger.error(msg)
                        raise ValueError(msg)
                    if x[0] in set_param_list:
                        msg = ("Parameter `{}` is invalid for ".format(x[0])+
                               "re-specification in parameter selection.")
                        logger.error(msg)
                        raise ValueError(msg)
                    if x[0] == 'discretization_type' and self.algorithm not in (1, 2):
                        msg = ("discretization_type is inapplicable, " +
                               "since algorithm is {} instead of c45 or chaid.".format(algorithm))
                        logger.error(msg)
                        raise ValueError(msg)
                    if x[0] in ('max_branch', 'merge_threshold') and self.algorithm != 2:
                        msg = ("'{}' is inapplicable when algorithm is not set as chaid.".format(x[0]))
                        logger.error(msg)
                        raise ValueError(msg)
                    if x[0] in ('discretization_type', 'min_records_of_parent', 'min_records_of_leaf', 'max_depth', 'max_branch') and not (isinstance(x[1], list) and len(x[1]) in rsz and all(isinstance(t, int) for t in x[1])):#pylint:disable=line-too-long
                        msg = "Valid values of `{}` must be a list of int.".format(x[0])
                        logger.error(msg)
                        raise TypeError(msg)
                    if (x[0] in ('split_threshold', 'merge_threshold')) and not (isinstance(x[1], list) and len(x[1]) in rsz and all(isinstance(t, (int, float)) for t in x[1])):#pylint:disable=line-too-long
                        msg = "Valid values of `{}` must be a list of numerical values.".format(x[0])
                        logger.error(msg)
                        raise TypeError(msg)
                    set_param_list.append(x[0])

    def fit(self, data, key=None, features=None, label=None, categorical_variable=None):
        """
        Fit the model to the training dataset.

        Parameters
        ----------
        data : DataFrame
            Training data.
        key : str, optional
            Name of the ID column in ``data``.

            If ``key`` is not provided, then:

            - if ``data`` is indexed by a single column, then ``key`` defaults
              to that index column;
            - otherwise, it is assumed that ``data`` contains no ID column.

        features : a list of str, optional
            Names of the feature columns.

            If ``features`` is not provided, it defaults to all non-ID,
            non-label columns.
        label : str, optional
            Name of the dependent variable.

            Defaults to the name of the last non-ID column.
        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.


        Returns
        -------
        A fitted object of class "DecisionTreeClassifier".
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        super(DecisionTreeClassifier, self)._fit(data, key, features, label, categorical_variable)
        # pylint: disable=attribute-defined-outside-init
        if not self._disable_hana_execution:
            self.confusion_matrix_ = self._confusion_matrix_ if self.output_confusion_matrix else None
        return self

    def predict(self, data, key=None, features=None, verbose=False, verbose_top_n=None):
        r"""
        Predict dependent variable values based on a fitted model.

        Parameters
        ----------
        data : DataFrame
            DataFrame containing the data.

        key : str, optional
            Name of the ID column in ``data``.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        features : a list of str, optional
            Names of the feature columns.

            If ``features`` is not provided, it defaults to
            all the non-ID columns.

        verbose : bool, optional
            Specifies whether to output all classes and the corresponding confidences
            for each data record in ``data``.

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
            Predict result, structured as follows:

              - ID column, with the same name and type as the ID column in ``data``.
              - SCORE, type NVARCHAR(100), prediction class labels.
              - CONFIDENCE, type DOUBLE, confidence values w.r.t. the corresponding assigned class labels.
        """
        return super(DecisionTreeClassifier, self)._predict(data=data,
                                                            key=key,
                                                            features=features,
                                                            verbose=verbose,
                                                            verbose_top_n=verbose_top_n)

    def score(self, data, key=None, features=None, label=None):
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

        features : a list of str, optional
            Names of the feature columns.

            If ``features`` is not provided, it defaults to all non-ID,
            non-label columns.
        label : str, optional
            Name of the dependent variable.

            Defaults to the name of the last non-ID column.

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
        prediction = self.predict(data=data, key=key, features=features)
        prediction = prediction.select(key, 'SCORE').rename_columns(['ID_P', 'PREDICTION'])
        actual = data.select(key, label).rename_columns(['ID_A', 'ACTUAL'])
        joined = actual.join(prediction, 'ID_P=ID_A').select('ACTUAL', 'PREDICTION')
        accuracy = metrics.accuracy_score(joined,
                                          label_true='ACTUAL',
                                          label_pred='PREDICTION')
        setattr(self, 'score_metrics_', {"ACCURACY": accuracy})
        return accuracy

class DecisionTreeRegressor(_DecisionTreeBase):
    """
    DecisionTreeRegressor is a decision tree-based machine learning model used for regression tasks,
    which predicts continuous output values by learning simple decision rules inferred from the data features.

    Parameters
    ----------

    algorithm : {'cart'}, optional
        Algorithm used to grow a decision tree.

            - 'cart': Classification and Regression tree.

        If not specified, defaults to 'cart'.
    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to -1.
    allow_missing_dependent : bool, optional
        Specifies if a missing target value is allowed.

          - False: Not allowed. An error occurs if a missing target is present.
          - True: Allowed. The datum with the missing target is removed.

        Defaults to True.
    percentage : float, optional
        Specifies the percentage of the input data that will be used to build
        the tree model.

        The rest of the data will be used for pruning.

        Defaults to 1.0.
    min_records_of_parent : int, optional
        Specifies the stop condition: if the number of records in one node
        is less than the specified value, the algorithm stops splitting.

        Defaults to 2.
    min_records_of_leaf : int, optional
        Promises the minimum number of records in a leaf.

        Defaults to 1.
    max_depth : int, optional
        The maximum depth of a tree.

        By default it is unlimited.
    categorical_variable : str or a list of str, optional
        Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

        Default value detected from input data.
    split_threshold : float, optional
        Specifies the stop condition for a node:

          - CART: The reduction of Gini index or relative MSE of the best
            split is less than this value.

        The smaller the split_threshold value is, the larger a CART tree grows.

        Defaults to 1e-5 for CART.
    use_surrogate : bool, optional
        If true, use surrogate split when NULL values are encountered.

        Defaults to True.
    model_format : {'json', 'pmml'}, optional
        Specifies the tree model format for store. Case-insensitive.

          - 'json': export model in json format.
          - 'pmml': export model in pmml format.

        Defaults to json.
    output_rules : bool, optional
        If true, output decision rules.

        Defaults to True.
    resampling_method : {'cv', 'bootstrap'}, optional
        The resampling method for model evaluation or parameter search.
        Once set, model evaluation or parameter search is enabled.

        No default value.
    evaluation_metric : {'mae', 'rmse'}, optional
        The evaluation metric. Once ``resampling_method`` is set,
        this parameter must be set.

        No default value.
    fold_num : int, optional
        The fold number for cross validation.

        Valid only and mandatory when ``resampling_method`` is set
        as 'cv'.

        No default value.
    repeat_times : int, optional
        The number of repeated times for model evaluation or parameter search.

        Defaults to 1.
    timeout : int, optional
        The time allocated (in seconds) for program running.

        0 indicates unlimited.

        Defaults to 0.
    search_strategy : {'random', 'grid'}, optional
        The search strategy for parameters.

        If not specified, parameter selection cannot be carried out.

        No default value.
    random_search_times : int, optional
        Specifies the number of search times for random search.

        Only valid and mandatory when ``search_strategy`` is set as 'random'.

        No default value.
    progress_indicator_id : str, optional
        Sets an ID of progress indicator for model evaluation or parameter selection.

        No progress indicator is active if no value is provided.

        No default value.
    param_values : dict or ListOfTuples, optional

        Specifies values of parameters to be selected.

        Input should be a dict or a list of size-two tuples, with key/1st element
        being the target parameter name, while value/2nd element being the a list of valued for selection.

        Only valid when ``resampling_method`` and ``search_strategy`` are both specified.

        Valid Parameters for values specification include :

        ``split_threshold``, ``max_depth``, ``min_records_of_leaf``, ``min_records_of_parent``.

        No default value.

    param_range : dict or ListOfTuples, optional

        Specifies ranges of parameters to be selected.

        Input should be dict or list of size-two tuples, with key/1st element being
        the name of the target parameter(in string format), while value/2nd element specifies the range of
        that parameter with [start, step, end] or [start, end].

        Valid only when ``resampling_method`` and ``search_strategy`` are both specified.

        Valid Parameters for range specification include:

        ``split_threshold``, ``max_depth``, ``min_records_of_leaf``, ``min_records_of_parent``.

        No default value.


    Attributes
    ----------
    model_ : DataFrame
        Model content.
    decision_rules_ : DataFrame
        Rules for decision tree to make decisions.
        Set to None if ``output_rules`` is False.
    stats_ : DataFrame
        Statistics.
    cv_ : DataFrame
        Cross validation information.
        Only has content when parameter selection is enabled.

    Examples
    --------
    >>>  dtr = DecisionTreeRegressor(algorithm='cart',
    ...                              min_records_of_parent=2, min_records_of_leaf=1,
    ...                              thread_ratio=0.4, split_threshold=1e-5,
    ...                              model_format='pmml', output_rules=True)

    Perform fit():

    >>> dtr.fit(data=df_train, key='ID')
    >>> dtr.decision_rules_.collect()

    Perform predict():

    >>> res = dtr.predict(data=df_predict, key='ID')
    >>> res.collect()

    Perform score():

    >>> dtr.score(data=df_score, key='ID')

    """
    resampling_map = {'cv': 'cv', 'bootstrap': 'bootstrap'}
    algorithm_map = {'cart': 3}
    def __init__(self,
                 algorithm='cart',
                 thread_ratio=None,
                 allow_missing_dependent=True,
                 percentage=None,
                 min_records_of_parent=None,
                 min_records_of_leaf=None,
                 max_depth=None,
                 categorical_variable=None,
                 split_threshold=None,
                 use_surrogate=None,
                 model_format=None,
                 output_rules=True,
                 output_confusion_matrix=True,
                 resampling_method=None,
                 fold_num=None,
                 repeat_times=None,
                 evaluation_metric=None,
                 timeout=None,
                 search_strategy=None,
                 random_search_times=None,
                 progress_indicator_id=None,
                 param_values=None,
                 param_range=None
                ):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(DecisionTreeRegressor, self).__init__(algorithm=algorithm,
                                                    thread_ratio=thread_ratio,
                                                    allow_missing_dependent=allow_missing_dependent,
                                                    percentage=percentage,
                                                    min_records_of_parent=min_records_of_parent,
                                                    min_records_of_leaf=min_records_of_leaf,
                                                    max_depth=max_depth,
                                                    categorical_variable=categorical_variable,
                                                    split_threshold=split_threshold,
                                                    use_surrogate=use_surrogate,
                                                    model_format=model_format,
                                                    output_rules=output_rules,
                                                    resampling_method=resampling_method,
                                                    fold_num=fold_num,
                                                    repeat_times=repeat_times,
                                                    evaluation_metric=evaluation_metric,
                                                    timeout=timeout,
                                                    search_strategy=search_strategy,
                                                    random_search_times=random_search_times,
                                                    progress_indicator_id=progress_indicator_id,
                                                    param_values=param_values,
                                                    param_range=param_range,
                                                    functionality='regression')
        self.op_name = 'DT_Regressor'
        self.output_confusion_matrix = self._arg("output_confusion_matrix", output_confusion_matrix, bool)
        if self.algorithm != self.algorithm_map['cart']:
            msg = ("'algorithm' must be set to cart when doing regression.")
            logger.error(msg)
            raise ValueError(msg)
        #discretization_type, max_branch, merge_threshold,
        self.values_list = ['split_threshold', 'max_depth', 'min_records_of_leaf', 'min_records_of_parent']
        if self.search_strategy is not None:
            set_param_list = []
            if self.split_threshold is not None:
                set_param_list.append("split_threshold")
            if self.max_depth is not None:
                set_param_list.append("max_depth")
            if self.min_records_of_parent is not None:
                set_param_list.append("min_records_of_parent")
            if self.min_records_of_leaf is not None:
                set_param_list.append("min_records_of_leaf")
            if self.param_values is not None:
                for x in self.param_values:#pylint:disable=invalid-name
                    if len(x) != 2:#pylint:disable=bad-option-value
                        msg = ("Each tuple that specifies the values of a parameter should"+
                               " contain exactly 2 elements: 1st is parameter name,"+
                               " 2nd is a list of valid values.")
                        logger.error(msg)
                        raise ValueError(msg)
                    if x[0] not in self.values_list:
                        msg = ("Specifying the values of `{}` for ".format(x[0])+
                               "parameter selection is invalid.")
                        logger.error(msg)
                        raise ValueError(msg)
                    if x[0] in set_param_list:
                        msg = ("Parameter `{}` is invalid for ".format(x[0])+
                               "re-specification in parameter selection.")
                        logger.error(msg)
                        raise ValueError(msg)
                    if x[0] in ('max_branch', 'merge_threshold') and self.algorithm != 2:
                        msg = ("'{}' is inapplicable when algorithm is not set as chaid.".format(x[0]))
                        logger.error(msg)
                        raise ValueError(msg)
                    if x[0] in ('min_records_of_parent', 'min_records_of_leaf', 'max_depth') and not (isinstance(x[1], list) and all(isinstance(t, int) for t in x[1])):#pylint:disable=line-too-long
                        msg = "Valid values of `{}` must be a list of int.".format(x[0])
                        logger.error(msg)
                        raise TypeError(msg)
                    if (x[0] == 'split_threshold') and not (isinstance(x[1], list) and all(isinstance(t, (int, float)) for t in x[1])):#pylint:disable=line-too-long
                        msg = "Valid values of `{}` must be a list of numerical values.".format(x[0])
                        logger.error(msg)
                        raise TypeError(msg)
                    set_param_list.append(x[0])

            if self.param_range is not None:
                rsz = [3] if self.search_strategy == 'grid' else [2, 3]
                for x in self.param_range:#pylint:disable=invalid-name
                    if len(x) != 2:#pylint:disable=bad-option-value
                        msg = ("Each tuple that specifies the range of a parameter should contain"+
                               " exactly 2 elements: 1st is parameter name, 2nd is value range.")
                        logger.error(msg)
                        raise ValueError(msg)
                    if x[0] not in self.values_list:
                        msg = ("Specifying the values of `{}` for ".format(x[0])+
                               "parameter selection is invalid.")
                        logger.error(msg)
                        raise ValueError(msg)
                    if x[0] in set_param_list:
                        msg = ("Parameter `{}` is invalid for ".format(x[0])+
                               "re-specification in parameter selection.")
                        logger.error(msg)
                        raise ValueError(msg)
                    if x[0] in ('max_branch', 'merge_threshold') and self.algorithm != 2:
                        msg = ("'{}' is inapplicable when algorithm is not set as chaid.".format(x[0]))
                        logger.error(msg)
                        raise ValueError(msg)
                    if x[0] in ('min_records_of_parent', 'min_records_of_leaf', 'max_depth', 'max_branch') and not (isinstance(x[1], list) and len(x[1]) in rsz and all(isinstance(t, int) for t in x[1])):#pylint:disable=line-too-long
                        msg = "Valid values of `{}` must be a list of int.".format(x[0])
                        logger.error(msg)
                        raise TypeError(msg)
                    if (x[0] in ('split_threshold', 'merge_threshold')) and not (isinstance(x[1], list) and len(x[1]) in rsz and all(isinstance(t, (int, float)) for t in x[1])):#pylint:disable=line-too-long
                        msg = "Valid values of `{}` must be a list of numerical values.".format(x[0])
                        logger.error(msg)
                        raise TypeError(msg)
                    set_param_list.append(x[0])

    def fit(self, data, key=None, features=None, label=None, categorical_variable=None):
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
            If ``features`` is not provided, it defaults to all non-ID,
            non-label columns.
        label : str, optional
            Name of the dependent variable.
            Defaults to the name of last non-ID column.
        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.


        Returns
        -------
        A fitted object of class "DecisionTreeRegressor".
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        super(DecisionTreeRegressor, self)._fit(data, key, features, label, categorical_variable)
        # pylint: disable=attribute-defined-outside-init
        self.confusion_matix_ = None
        return self

    def predict(self, data, key=None, features=None, verbose=None):
        r"""
        Predict dependent variable values based on a fitted model.

        Parameters
        ----------
        data : DataFrame
            DataFrame containing the data.

        key : str, optional
            Name of the ID column in ``data``.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        features : a list of str, optional
            Names of the feature columns.

            If ``features`` is not provided, it defaults to
            all the non-ID columns.

        verbose : bool, optional(deprecated)
            Specifies whether to output all classes and the corresponding confidences
            for each data record in ``data``.

            Non-effective, reserved only for forward compatibility.

        Returns
        -------
        DataFrame
            Predict result, structured as follows:

              - ID column, with the same name and type as the ID column in ``data``.
              - SCORE, type NVARCHAR(100), predicted values.
              - CONFIDENCE, type DOUBLE, all 0s.
        """
        return super(DecisionTreeRegressor, self)._predict(data, key, features, verbose)

    def score(self, data, key=None, features=None, label=None):
        """
        Returns the coefficient of determination R2 of the prediction.

        Parameters
        ----------
        data : DataFrame
            Data on which to assess model performance.
        key : str, optional
            Name of the ID column.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        features : a list of str, optional
            Names of the feature columns.
            If ``features`` is not provided, it defaults to all non-ID,
            non-label columns.
        label : str, optional
            Name of the dependent variable.

            Defaults to the name of the last non-ID column.

        Returns
        -------
        float
            The coefficient of determination R2 of the prediction on the
            given data.
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
        prediction = self.predict(data, key, features).select([key, 'SCORE'])
        prediction = prediction.rename_columns(['ID_P', 'PREDICTION'])
        original = data[[key, label]].rename_columns(['ID_A', 'ACTUAL'])
        joined = original.join(prediction, 'ID_P=ID_A').select('ACTUAL', 'PREDICTION')
        r2 = metrics.r2_score(joined,
                              label_true='ACTUAL',
                              label_pred='PREDICTION')
        setattr(self, 'score_metrics_', {"R2": r2})
        return r2

#Wrapper for GBDT
GBDTALIAS = {'n_estimators':'iter_num',
             'max_tree_depth':'max_depth',
             'loss':'regression_type',
             'split_threshold':'min_split_loss',
             'subsample':'row_sample_rate',
             'lamb':'lambda',
             'learning_rate':'learning_rate',
             'min_sample_weight_leaf':'min_leaf_sample_weight',
             'max_w_in_split':'max_w_in_split',
             'col_subsample_split':'col_sample_rate_split',
             'col_subsample_tree':'col_sample_rate_tree',
             'alpha':'alpha', 'scale_pos_w':'scale_pos_w',
             'base_score':'base_score'}


#pylint:disable=too-few-public-methods, too-many-instance-attributes
class _GradientBoostingBase(PALBase):
    """
    Base Gradient Boosting tree model for classification and regression.
    """
    rangeparm = ('n_estimators', 'max_depth', 'learning_rate',
                 'min_sample_weight_leaf', 'max_w_in_split',
                 'col_subsample_split', 'col_subsample_tree',
                 'lamb', 'alpha', 'scale_pos_w', 'base_score',
                 'split_threshold', 'subsample')

    valid_loss = {'linear':'LINEAR', 'logistic':'LOGISTIC'}

    def __init__(self,
                 n_estimators=10,
                 subsample=None,
                 max_depth=None,
                 loss=None,
                 split_threshold=None,
                 learning_rate=None,
                 fold_num=None,
                 default_split_dir=None,
                 min_sample_weight_leaf=None,
                 max_w_in_split=None,
                 col_subsample_split=None,
                 col_subsample_tree=None,
                 lamb=None,
                 alpha=None,
                 scale_pos_w=None,
                 base_score=None,
                 cv_metric=None,
                 ref_metric=None,
                 categorical_variable=None,
                 allow_missing_label=None,
                 thread_ratio=None,
                 cross_validation_range=None
                ):

        if not hasattr(self, 'hanaml_parameters'):
            setattr(self, 'hanaml_parameters', pal_param_register())
        super(_GradientBoostingBase, self).__init__()
        self.n_estimators = self._arg('n_estimators', n_estimators, int)
        #self.random_state = self._arg('random_state', random_state, int)
        self.subsample = self._arg('subsample', subsample, float)
        self.max_depth = self._arg('max_depth', max_depth, int)
        self.split_threshold = self._arg('split_threshold', split_threshold, float)
        self.loss = self._arg('loss', loss, self.valid_loss)
        self.learning_rate = self._arg('learning_rate', learning_rate, float)
        self.default_split_dir = self._arg('default_split_dir', default_split_dir, int)
        self.fold_num = self._arg('fold_num', fold_num, int)
        self.min_sample_weight_leaf = self._arg('min_sample_weight_leaf',
                                                min_sample_weight_leaf, float)
        #self.min_samples_leaf = self._arg('min_samples_leaf', min_samples_leaf, int)
        self.max_w_in_split = self._arg('max_w_in_split', max_w_in_split, float)
        self.col_subsample_split = self._arg('col_subsample_split', col_subsample_split, float)
        self.col_subsample_tree = self._arg('col_subsample_tree', col_subsample_tree, float)
        self.lamb = self._arg('lamb', lamb, float)
        self.alpha = self._arg('alpha', alpha, float)
        self.scale_pos_w = self._arg('scale_pos_w', scale_pos_w, float)
        self.base_score = self._arg('base_score', base_score, float)
        self.cv_metric = self._arg('cv_metric', cv_metric, str)
        if isinstance(ref_metric, str):
            ref_metric = [ref_metric]
        self.ref_metric = self._arg('ref_metric', ref_metric, ListOfStrings)
        if isinstance(categorical_variable, str):
            categorical_variable = [categorical_variable]
        self.categorical_variable = self._arg('categorical_variable',
                                              categorical_variable, ListOfStrings)
        self.allow_missing_label = self._arg('allow_missing_label', allow_missing_label, bool)
        self.thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        self.cross_validation_range = self._arg('cross_validation_range',
                                                cross_validation_range, ListOfTuples)
        if self.cross_validation_range is not None:
            if self.cross_validation_range:
                for prm in self.cross_validation_range:
                    if prm[0] not in self.rangeparm:
                        msg = ("Parameter name '{}' not supported ".format(prm[0]) +
                               "for cross-validation.")
                        logger.error(msg)
                        raise ValueError(msg)
        self.label_type = 'unknown'

    @trace_sql
    def _fit(self, data, key=None, features=None, label=None, categorical_variable=None):

        """
        Train the tree-ensemble model on input data:

        Parameters
        ----------

        data : DataFrame

            Training data.

        key : str, optional

            Name of the ID column.

            If ``key`` is not provideed, it is assumed that the input data has no ID column.

        features : a list of str, optional

            Names of the feature columns.

            If ``features`` is not provided, it defaults to all non-ID,
            non-label columns.

        label : str, optional

            Name of the dependent variable.

            Defaults to the name of the last non-ID column.

        categorical_variable : str or a list of str, optional

            Specifies INTEGER columns that should be treated as categorical.

            Other INTEGER columns will be treated as continuous.
        """
        conn = data.connection_context
        require_pal_usable(conn)
        key = self._arg('key', key, str)
        index = data.index
        if isinstance(index, str):
            if key is not None and index != key:
                msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(msg)
        key = index if key is None else key
        features = self._arg('features', features, ListOfStrings)
        label = self._arg('label', label, str)
        categorical_variable = self._arg('categorical_variable', categorical_variable, ListOfStrings)
        cols = data.columns
        #has_id input is process here
        if key is not None:
            id_col = [key]
            cols.remove(key)
        else:
            id_col = []
        if label is None:
            label = cols[-1]
        cols.remove(label)
        if features is None:
            features = cols
        #n_features = len(features)
        #Generate a temp view of the data so that label is always in the final column
        data_ = data[id_col + features + [label]]
        ##okay, now label is in the final column
        tables = ['MODEL', 'VAR_IMPORTANCE', 'CM', 'STATISTICS', 'CV']
        tables = ['#PAL_GRADIENT_BOOSTING_{}_TBL_{}'.format(name, self.id) for name in tables]
        model_tbl, var_importance_tbl, cm_tbl, stats_tbl, cv_tbl = tables
        param_rows = [
            ('ITER_NUM', self.n_estimators, None, None),
            ('ROW_SAMPLE_RATE', None, self.subsample, None),
            ('MAX_TREE_DEPTH', self.max_depth, None, None),
            ('MIN_SPLIT_LOSS', None, self.split_threshold, None),
            ('REGRESSION_TYPE', None, None, self.loss if self.loss is not None else None),
            ('FOLD_NUM', self.fold_num, None, None),
            ('DEFAULT_SPLIT_DIR', self.default_split_dir, None, None),
            ('LEARNING_RATE', None, self.learning_rate, None),
            ('MIN_LEAF_SAMPLE_WEIGHT', None, self.min_sample_weight_leaf, None),
            ('MAX_W_IN_SPLIT', None, self.max_w_in_split, None),
            ('COL_SAMPLE_RATE_SPLIT', None, self.col_subsample_split, None),
            ('COL_SAMPLE_RATE_TREE', None, self.col_subsample_tree, None),
            ('LAMBDA', None, self.lamb, None),
            ('ALPHA', None, self.alpha, None),
            ('SCALE_POS_W', None, self.scale_pos_w, None),
            ('BASE_SCORE', None, self.base_score, None),
            ('CV_METRIC', None, None,
             self.cv_metric.upper() if self.cv_metric is not None else None),
            #This line is left as intended
            ('ALLOW_MISSING_LABEL', self.allow_missing_label, None, None),
            ('THREAD_RATIO', None, self.thread_ratio, None),
            ('HAS_ID', key is not None, None, None)]
        #If categorical variable exists, do row extension
        if self.categorical_variable is not None:
            param_rows.extend(('CATEGORICAL_VARIABLE', None, None, variable)
                              for variable in self.categorical_variable)
        if categorical_variable is not None:
            param_rows.extend(('CATEGORICAL_VARIABLE', None, None, variable)
                              for variable in categorical_variable)
        if self.ref_metric is not None:
            param_rows.extend(('REF_METRIC', None, None, metric.upper())
                              for metric in self.ref_metric)
        #if cross-validation is triggered, do row extension
        if self.fold_num is not None:
            if self.fold_num > 1 and self.cross_validation_range is not None:
                param_rows.extend(('RANGE_{}'.format(GBDTALIAS[cvparam].upper()),
                                   None,
                                   None,
                                   '{}'.format(range_))
                                  for cvparam, range_ in self.cross_validation_range)

        #model_spec = [
        #    ("ROW_INDEX", INTEGER),
        #    ("KEY", NVARCHAR(1000)),
        #    ("VALUE", NVARCHAR(1000))]

        #var_importance_spec = [
        #    ("VARIABLE_NAME", NVARCHAR(256)),
        #    ("IMPORTANCE", DOUBLE)]
        #cm_spec = [
        #    ("ACTUAL_CLASS", NVARCHAR(1000)),
        #    ("PREDICT_CLASS", NVARCHAR(1000)),
        #    ("COUNT", INTEGER)]
        #stats_spec = [
        #    ("STAT_NAME", NVARCHAR(1000)),
        #    ("STAT_VALUE", NVARCHAR(1000))]
        #cv_spec = [
        #    ("PARM_NAME", NVARCHAR(1000)),
        #    ("INT_VALUE", INTEGER),
        #    ("DOUBLE_VALUE", DOUBLE),
        #    ("STRING_VALUE", NVARCHAR(1000))]
        try:
            #self._materialize(data_tbl, data)
            #self._create(ParameterTable(param_tbl).with_data(param_rows))
            #self._create(Table(model_tbl, model_spec))
            #self._create(Table(var_importance_tbl, var_importance_spec))
            #self._create(Table(cm_tbl, cm_spec))
            #self._create(Table(stats_tbl, stats_spec))
            #self._create(Table(cv_tbl, cv_spec))
            self._call_pal_auto(conn,
                                "PAL_GBDT",
                                data_,
                                ParameterTable().with_data(param_rows),
                                *tables)
        except dbapi.Error as db_err:
            #msg = ('HANA error while attempting to fit '+
            #       'gradient boosting tree model.')
            logger.exception(str(db_err))
            try_drop(conn, tables)
            raise
        except Exception as err:
            msg = str(err)
            logger.exception(msg)
            try_drop(conn, tables)
            raise
        #pylint: disable=attribute-defined-outside-init
        #self.param_ = conn.table(param_tbl)
        self.model_ = conn.table(model_tbl)
        self.feature_importances_ = conn.table(var_importance_tbl)
        self._confusion_matrix_ = conn.table(cm_tbl)
        self.stats_ = conn.table(stats_tbl)
        self.statistics_ = self.stats_
        if self.cross_validation_range is not None and self.fold_num > 1:
            self.cv_ = conn.table(cv_tbl)
        else:
            self.cv_ = None

    @trace_sql
    def _predict(self, key, data, features=None, verbose=None):
        """
        Predict dependent variable values based on fitted moel.

        Parameters
        ----------

        data : DataFrame

            Independent variable values to predict for.

        key : str
            Name of the ID column.

        features : a list of str, optional

            Names of the feature columns.

            If ``features`` is not provided, it defaults to all non-ID columns.

        verbose : bool, optional

            If true, output all classes and the corresponding confidences
            for each data point. Only valid classification.

            Default to False if not provided.

        Returns
        -------

        DataFrame

            DataFrame of score and confidence, structured as follows:
            1st column - ID column, with same as and type as ``data``'s
                         ID column.
            2nd column - SCORE, type NVARCHAR(1000), representing the predicted
                         classes/values.
            3rd column - CONFIDENCE, type DOUBLE, representing the confidence of
                         of a class. All Zero's for regression.
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
        verbose = self._arg('verbose', verbose, bool)
        cols = data.columns
        cols.remove(key)
        if features is None:
            features = cols
        data_ = data[[key] + features]
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        #tables = data_tbl, model_tbl, param_tbl, result_tbl = [
        result_tbl = '#PAL_GRADIENT_BOOSTING_RESULT_TBL_{}_{}'.format(self.id, unique_id)
            #for name in ['DATA', 'MODEL', 'PARAM', 'FITTED']]
        param_rows = [
            ('THREAD_RATIO', None, self.thread_ratio, None),
            ('VERBOSE', verbose, None, None)]
        #result_spec = [
        #    (parse_one_dtype(data.dtypes([data.columns[0]])[0])),
        #    ("SCORE", NVARCHAR(100)),
        #    ("CONFIDENCE", DOUBLE)]

        try:
            #self._materialize(data_tbl, data)
            #self._materialize(model_tbl, self.model_)
            #self._create(ParameterTable(param_tbl).with_data(param_rows))
            #self._create(Table(result_tbl, result_spec))
            self._call_pal_auto(conn,
                                "PAL_GBDT_PREDICT",
                                data_,
                                self.model_,
                                ParameterTable().with_data(param_rows),
                                result_tbl)
        except dbapi.Error as db_err:
            #msg = 'HANA error during gradient boosting prediction.'
            logger.exception(str(db_err))
            try_drop(conn, result_tbl)
            raise
        except Exception as db_err:
            #msg = 'HANA error during gradient boosting prediction.'
            logger.exception(str(db_err))
            try_drop(conn, result_tbl)
            raise

        return conn.table(result_tbl)


@deprecated("This method is deprecated. Please use HybridGradientBoostingClassifier instead.")
class GradientBoostingClassifier(_GradientBoostingBase): # pragma: no cover
#pylint: disable=too-many-instance-attributes, line-too-long
    """
    Gradient Boosting model for classification.

    Parameters
    ----------

    n_estimators : int, optional

        Specifies the number of trees in Gradient Boosting.

        Defaults to 10.

    loss : str, optional

        Type of loss function to be optimized.
        Supported values are 'linear' and 'logistic'.

        Defaults to 'linear'.

    max_depth : int, optional

        The maximum depth of a tree.

        Defaults to 6.

    split_threshold : float, optional

        Specifies the stopping condition: if the improvement value of the best
        split is less than this value, then the tree stops growing.

    learning_rate : float, optional.

        Learning rate of each iteration, must be within the range (0, 1].

        Defaults to 0.3.

    subsample : float, optional

        The fraction of samples to be used for fitting each base learner.

        Defaults to 1.0.

    fold_num : int, optional

        The k-value for k-fold cross-validation.
        Effective only when ``cross_validation_range`` is not None nor empty.

    default_split_dir : int, optional.

        Default split direction for missing values.
        Valid input values are 0, 1 and 2, where:

          - 0 : Automatically determined,
          - 1 : Left,
          - 2 : Right.

        Defaults to 0.

    min_sample_weight_leaf : float, optional

        The minimum sample weights in leaf node.

        Defaults to 1.0.

    max_w_in_split : float, optional

        The maximum weight constraint assigned to each tree node.

        Defaults to 0 (i.e. no constraint).

    col_subsample_split : float, optional

        The fraction of features used for each split, should be within range (0, 1].

        Defaults to 1.0.

    col_subsample_tree : float, optional

        The fraction of features used for each tree growth, should be within range (0, 1]

        Defaults to 1.0.

    lamb : float, optional

        L2 regularization weight for the target loss function.
        Should be within range (0, 1].

        Defaults to 1.0.

    alpha : float, optional

        Weight of L1 regularization for the target loss function.

        Defaults to 1.0.

    scale_pos_w : float, optional

        The weight scaled to positive samples in regression.

        Defaults to 1.0.

    base_score : float, optional

        Initial prediction score for all instances. Global bias for sufficient number
        of iterations(changing this value will not have too much effect).

    cv_metric : { 'rmse', 'mae', 'log_likelihood', 'multi_log_likelihood',\
    'error_rate', 'multi_error_rate', 'auc'}, optional

        The metric used for cross-validation.

        If multiple lines of metrics are provided, then only the first one is valid.
        If not set, it takes the first value (in alphabetical order) of the parameter
        'ref_metric' when the latter is set, otherwise it goes to default values.

        Defaults to

          - 1)'error_rate' for binary classification,

          - 2)'multi_error_rate' for multi-class classification.

    ref_metric : str or a list of str, optional

        Specifies a reference metric or a list of reference metrics.
        Supported metrics same as cv_metric.
        If not provided, defaults to

        - 1)['error_rate'] for binary classification,

        - 2)['multi_error_rate'] for multi-class classification.

    categorical_variable : str or a list of str, optional
        Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

        No default value.

    allow_missing_label :  bool, optional

        Specifies whether missing label value is allowed.

          - False : not allowed. In missing values presents in the input data, an error shall be thrown.


          - True : allowed. The datum with missing label will be removed automatically.

    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to -1.

    cross_validation_range : list of tuples, optional
        Indicates the set of parameters involved for cross-validation.
        Cross-validation is triggered only when this param is not None and the list
        is not tempty, and fold_num is greater than 1.\
        Each tuple is a pair, with the first being parameter name of str type, and
        the second being the a list of number of the following form:
        [<begin-value>, <test-numbers>, <end-value>].

        Supported parameters for cross-validation: ``n_estimators``, ``max_depth``, ``learning_rate``,
        ``min_sample_weight_leaf``, ``max_w_in_split``, ``col_subsample_split``, ``col_subsample_tree``,
        ``lamb``, ``alpha``, ``scale_pos_w``, ``base_score``.

        A simple example for illustration:

        [('n_estimators', [4, 3, 10]),

        ('learning_rate', [0.1, 3, 1.0]),

        ('split_threshold', [0.1, 3, 1.0])]


    Attributes
    ----------

    model_ : DataFrame
        Model content.

    feature_importances_ : DataFrame

        The feature importance (the higher, the more import the feature)

    confusion_matrix_ : DataFrame

        Confusion matrix used to evaluate the performance of classification algorithm.

    stats_ : DataFrame

        Statistics info for cross-validation.

    cv_ : DataFrame

        Best choice of parameter produced by cross-validation.

    Examples
    --------
    >>> gbc = GradientBoostingClassifier(
    ...     n_estimators=4, split_threshold=0,
    ...     learning_rate=0.5, fold_num=5, max_depth=6,
    ...     cv_metric='error_rate', ref_metric=['auc'],
    ...     cross_validation_range=[('learning_rate',[0.1,1.0,3]), ('n_estimators', [4,10,3]), ('split_threshold', [0.1,1.0,3])])

    Perform fit():

    >>> gbc.fit(data=df, key='ID', features=['F1', 'F2'], label='LABEL')
    >>> gbc.stats_.collect()

    Perform predict():

    >>> res = gbc.predict(data=df_predict)
    >>> result.collect()
    """

    def fit(self, data, key=None, features=None, label=None, categorical_variable=None):
        """
        Fit the model to the training dataset.

        Parameters
        ----------

        data : DataFrame
            Training data.

        key : str, optional

            Name of the ID column. If ``key`` is not provided, it is assumed
            that the input has no ID column.

        features : a list of str, optional

            Names of the feature columns.
            If ``features`` is not provided, it defaults to all non-ID,
            non-label columns.

        label : str, optional

            Name of the dependent variable.

            Defaults to the name of the last non-ID column.

        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.

        Returns
        -------
        A fitted object of class "GradientBoostingClassifier".
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        if label is None:
            label = data.columns[-1]
        self.label_type = data.dtypes([str(label)])[0][1]
        if self.categorical_variable is None:
            categorical_variable = []
        else:
            categorical_variable = self.categorical_variable
        if self.label_type not in ('VARCHAR', 'NVARCHAR'):
            if label not in categorical_variable or self.label_type != 'INT':
                msg = ("Label column data type'{}\' is ".format(self.label_type) +
                       "not supported for classification.")
                logger.error(msg)
                raise ValueError(msg)

        super(GradientBoostingClassifier, self)._fit(data, key, features, label, categorical_variable)
        #pylint: disable=attribute-defined-outside-init
        self.confusion_matrix_ = self._confusion_matrix_
        return self

    def predict(self, data, key=None, features=None, verbose=None):
        """
        Predict dependent variable values based on a fitted model.

        Parameters
        ----------

        data : DataFrame

            Input DataFrame.

        key : str, optional

            Name of the ID column.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        features : a list of str, optional

            Names of the feature columns.
            If ``features`` is not provided, it defaults to all non-ID columns.

        verbose : bool, optional

            If true, output all classes and the corresponding confidences
            for each data point.

        Returns
        -------

        DataFrame

            DataFrame of score and confidence.
        """
        return super(GradientBoostingClassifier, self)._predict(data=data,
                                                                key=key,
                                                                features=features,
                                                                verbose=verbose)

    def score(self, data, key=None, features=None, label=None):
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

        features : a list of str, optional

            Names of the feature columns.
            If ``features`` is not provided, it defaults to all non-ID,
            non-label columns.

        label : str, optional

            Name of the dependent variable.

            Defaults to the name of the last non-ID column.

        Returns
        -------

        float

            Mean accuracy on the given test data and labels.
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
        #input check for block_size and missing_replacement is done in predict()

        cols = data.columns
        cols.remove(key)
        if label is None:
            label = cols[-1]
        cols.remove(label)
        if features is None:
            features = cols

        prediction = self.predict(data=data, key=key, features=features)
        prediction = prediction.select(key, 'SCORE').rename_columns(['ID_P', 'PREDICTION'])

        actual = data.select(key, label).rename_columns(['ID_A', 'ACTUAL'])
        joined = actual.join(prediction, 'ID_P=ID_A').select('ACTUAL', 'PREDICTION')

        accuracy = metrics.accuracy_score(joined,
                                          label_true='ACTUAL',
                                          label_pred='PREDICTION')
        setattr(self, 'score_metrics_', {"ACCURACY": accuracy})
        return accuracy

@deprecated("This method is deprecated. Please use HybridGradientBoostingRegressor instead.")
class GradientBoostingRegressor(_GradientBoostingBase): # pragma: no cover
#pylint: disable=too-many-instance-attributes
    """
    Gradient Boosting Tree model for regression.

    Parameters
    ----------

    n_estimators : int, optional

        Specifies the number of trees in Gradient Boosting.

        Defaults to 10.

    loss : str, optional

        Type of loss function to be optimized. Supported values are 'linear' and 'logistic'.

        Defaults to 'linear'.

    max_depth : int, optional

        The maximum depth of a tree.

        Defaults to 6.

    split_threshold : float, optional

        Specifies the stopping condition: if the improvement value of the best split is less than this value, then the tree stops growing.

    learning_rate : float, optional.

        Learning rate of each iteration, must be within the range (0, 1].

        Defaults to 0.3.

    subsample : float, optional

        The fraction of samples to be used for fitting each base learner.

        Defaults to 1.0.

    fold_num : int, optional

        The k-value for k-fold cross-validation.

    default_split_dir : int, optional.

        Default split direction for missing values.
        Valid input values are 0, 1 and 2, where:

          - 0: Automatically determined,
          - 1: Left,
          - 2: Right.

        Defaults to 0.

    min_sample_weight_leaf : float, optional

        The minimum sample weights in leaf node.

        Defaults to 1.0.

    max_w_in_split : float, optional

        The maximum weight constraint assigned to each tree node.

        Defaults to 0 (i.e. no constraint).

    col_subsample_split : float, optional

        The fraction of features used for each split, should be within range (0, 1].

        Defaults to 1.0.

    col_subsample_tree : float, optional

        The fraction of features used for each tree growth, should be within range (0, 1]

        Defaults to 1.0.

    lamb : float, optional

        L2 regularization weight for the target loss function.
        Should be within range (0, 1].

        Defaults to 1.0.

    alpha : float, optional

        Weight of L1 regularization for the target loss function.

        Defaults to 1.0.

    scale_pos_w : float, optional

        The weight scaled to positive samples in regression.

        Defaults to 1.0.

    base_score : float, optional

        Initial prediction score for all instances. Global bias for sufficient number
        of iterations(changing this value will not have too much effect).

    cv_metric : str, optional

        The metric used for cross-validation.

        Supported metrics include: 'rmse', 'mae', 'log_likelihood', 'multi_log_likelihood',
        'error_rate', 'multi_error_rate' and 'auc'.

        If multiple lines of metrics are provided, then only the first one is valid.

        If not set, it takes the first value (in alphabetical order) of the parameter
        'ref_metric' when the latter is set, otherwise it goes to default values.

        Defaults to

          - 1)'error_rate' for binary classification,
          - 2)'multi_error_rate' for multi-class classification.

    ref_metric : str or a list of str, optional

        Specifies a reference metric or a list of reference metrics.
        Supported metrics same as cv_metric.
        If not provided, defaults to

          - 1)['error_rate'] for binary classification,
          - 2)['multi_error_rate'] for multi-class classification.

    categorical_variable : str, optional

        Indicates which variables should be treated as categorical. Otherwise default behavior is followed:

        1) STRING - categorical,
        2) INTEGER and DOUBLE - continous.

        Only valid for INTEGER variables, omitted otherwise.

    allow_missing_label : bool, optional

        Specifies whether missing label value is allowed.

          - False: not allowed. In missing values presents in the input data, an error shall be thrown.
          - True: allowed. The datum with missing label will be removed automatically.

    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to -1.

    cross_validation_range : list of tuples, optional

        Indicates the set of parameters involved for cross-validation.
        Each tuple is a pair, with the first being parameter name of str type, and
        the second being the a list of number of the following form:

        [<begin-value>, <test-numbers>, <end-value>].

        Supported parameters for cross-validation: ``n_estimators``, ``max_depth``, ``learning_rate``,
        ``min_sample_weight_leaf``, ``max_w_in_split``, ``col_subsample_split``, ``col_subsample_tree``,
        ``lamb``, ``alpha``, ``scale_pos_w``, ``base_score``.

        A simple example for illustration:

        [('n_estimators', [4, 3, 10]), ('learning_rate', [0.1, 3, 1.0]),\
        ('split_threshold', [0.1, 3, 1.0])]


    Attributes
    ----------

    model_ : DataFrame
        Model content.

    feature_importances_ : DataFrame

        The feature importance (the higher, the more import the feature)

    stats_ : DataFrame

        Statistics info for cross-validation.

    cv_ : DataFrame

        Best choice of parameter produced by cross-validation.

    Examples
    --------

    >>> gbr = GradientBoostingRegressor(
    ...     n_estimators=20, split_threshold=0.75,
    ...     learning_rate=0.75, fold_num=5, max_depth=6,
    ...     cv_metric='rmse', ref_metric=['mae'],
    ...     cross_validation_range=[('learning_rate',[0.0,5,1.0]), ('n_estimators', [10, 11, 20]), ('split_threshold', [0.0, 5, 1.0])])

    Perform fit():

    >>> gbr.fit(data=df, key='ID', features=['F1', 'F2'], label='LABEL')
    >>> gbr.stats_.collect()

    Perform predict():

    >>> res = gbr.predict(data=df_predict)
    >>> result.collect()

    """

    def fit(self, data, key=None, features=None, label=None, categorical_variable=None):
        """
        Fit the model to the training dataset.

        Parameters
        ----------

        data : DataFrame

            Training data.

        key : str, optional

            Name of the ID column in ``data``.

            If ``key`` is not provided, then:

            - if ``data`` is indexed by a single column, then ``key`` defaults
              to that index column;
            - otherwise, it is assumed that ``data`` contains no ID column.

        features : a list of str, optional

            Names of the feature columns.
            If ``features`` is not provided, it defaults to all non-ID, non-label columns.

        label : str, optional

            Name of the dependent variable.

            Defaults to the name of the last non-ID column.

        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.

        Returns
        -------
        A fitted object of class "GradientBoostingRegressor".
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        if label is None:
            label = data.columns[-1]
        self.label_type = data.dtypes([label])[0][1]
        if self.categorical_variable is None:
            categorical_variable = []
        else:
            categorical_variable = self.categorical_variable
        if self.label_type in ('VARCHAR', 'NVARCHAR') \
            or (self.label_type == 'INT' and label in categorical_variable):
            msg = "Label column is treated as categorical, not supported for regression."
            logger.error(msg)
            raise ValueError(msg)
        super(GradientBoostingRegressor, self)._fit(data, key=key, features=features, label=label, categorical_variable=categorical_variable)
        return self

    def predict(self, data, key=None, features=None, verbose=None):
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

        verbose : bool, optional
            If true, output all classes and the corresponding confidences for each data point.

            Invalid for regression problems and will be removed in future release

        Returns
        -------
        DataFrame

            DataFrame of score and confidence, structured as follows:

                - ID column, with same name and type as ``data``'s ID column.
                - SCORE, type DOUBLE, representing the predicted value.
                - CONFIDENCE, all None's for regression.
        """
        return super(GradientBoostingRegressor, self)._predict(data=data,
                                                               key=key,
                                                               features=features,
                                                               verbose=verbose)

    def score(self, data, key=None, features=None, label=None):
        """
        Returns the coefficient of determination R2 of the prediction.

        Parameters
        ----------

        data : DataFrame

            Data on which to assess model performance.

        key : str, optional

            Name of the ID column.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        features : a list of str, optional

            Names of the feature columns.
            If ``features`` is not provided, it defaults to all non-ID,
            non-label columns.

        label : str, optional

            Name of the dependent variable.

            Defaults to the name of the last non-ID column.

        Returns
        -------
        float
            The coefficient of determination R2 of the prediction on the given data.
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
        #input check for block_size and missing_replacement is done in predict()

        cols = data.columns
        cols.remove(key)
        if label is None:
            label = cols[-1]
        cols.remove(label)
        if features is None:
            features = cols

        prediction = self.predict(data=data, key=key, features=features)
        original = data[[key, label]]
        prediction = prediction.select([key, 'SCORE']).rename_columns(['ID_P', 'PREDICTION'])
        original = data[[key, label]].rename_columns(['ID_A', 'ACTUAL'])
        joined = original.join(prediction, 'ID_P=ID_A').select('ACTUAL', 'PREDICTION')
        r2 = metrics.r2_score(joined,
                              label_true='ACTUAL',
                              label_pred='PREDICTION')
        setattr(self, 'score_metrics_', {"R2": r2})
        return r2

#pylint:disable=too-few-public-methods, too-many-instance-attributes, no-member
class _HybridGradientBoostingBase(PALBase):
    """
    Hybrid Gradient Boosting Tree model for classification and regression.
    """
    rangeparm = ('n_estimators', 'max_depth', 'learning_rate',
                 'min_sample_weight_leaf', 'max_w_in_split',
                 'col_subsample_split', 'col_subsample_tree',
                 'lamb', 'alpha', 'scale_pos_w', 'base_score',
                 'split_threshold', 'subsample')

    hgbt_name_map = {
        'n_estimators':'ITER_NUM', 'random_state':'SEED',
        'subsample':'ROW_SAMPLE_RATE', 'max_depth':'MAX_DEPTH',
        'split_threshold':'GAMMA', 'learning_rate':'ETA',
        'sketch_eps':'SKETCH_EPS', 'fold_num':'FOLD_NUM',
        'min_sample_weight_leaf':'MIN_CHILD_HESSIAN',
        'min_samples_leaf':'NODE_SIZE',
        'max_w_in_split':'NODE_WEIGHT_CONSTRAINT',
        'col_subsample_split':'COL_SAMPLE_RATE_BYSPLIT',
        'col_subsample_tree':'COL_SAMPLE_RATE_BYTREE',
        'lamb':'LAMBDA', 'alpha':'ALPHA',
        'base_score':'BASE_SCORE',
        'evaluation_metric':'EVALUATION_METRIC',
        'ref_metric':'REF_METRIC',
        'calculate_importance':'CALCULATE_IMPORTANCE',
        'calculate_cm':'CALCULATE_CONFUSION_MATRIX'}

    split_method_map = {'exact':'exact', 'sketch':'sketch', 'sampling':'sampling', 'histogram':'histogram'}
    param_search_map = {'grid':'grid', 'random':'random'}
    missing_replace_map = {'feature_marginalized':1, 'instance_marginalized':2}
    objfun_map = {'se': 0, 'sle': 1, 'pseudo-huber': 2, 'gamma': 3, 'tweedie': 4, 'logistic': 5, 'hinge': 6,'softmax': 7}
    direction_map = {'left': 0, 'right': 1}
    resource_map = {'n_estimators': 'ITER_NUM', 'data_size': None}
    model_tree_map = {'constant': 0, 'linear': 1}
    def __init__(self,
                 n_estimators=None,
                 random_state=None,
                 subsample=None,
                 max_depth=None,
                 split_threshold=None,
                 learning_rate=None,
                 split_method=None,
                 sketch_eps=None,
                 fold_num=None,
                 min_sample_weight_leaf=None,
                 min_samples_leaf=None,
                 max_w_in_split=None,
                 col_subsample_split=None,
                 col_subsample_tree=None,
                 lamb=None,
                 alpha=None,
                 adopt_prior=None,
                 evaluation_metric=None,
                 cv_metric=None,
                 ref_metric=None,
                 calculate_importance=None,
                 thread_ratio=None,
                 resampling_method=None,
                 param_search_strategy=None,
                 repeat_times=None,
                 timeout=None,
                 progress_indicator_id=None,
                 random_search_times=None,
                 param_range=None,
                 cross_validation_range=None,
                 param_values=None,
                 replace_missing=None,
                 default_missing_direction=None,
                 feature_grouping=None,
                 tol_rate=None,
                 compression=None,
                 max_bits=None,
                 max_bin_num=None,
                 resource=None,
                 max_resource=None,
                 reduction_rate=None,
                 min_resource_rate=None,
                 aggressive_elimination=None,
                 validation_set_rate=None,
                 stratified_validation_set=None,
                 tolerant_iter_num=None,
                 fg_min_zero_rate=None,
                 base_score=None,
                 validation_set_metric=None,
                 model_tree=None,
                 linear_lambda=None):
        if not hasattr(self, 'hanaml_parameters'):
            setattr(self, 'hanaml_parameters', pal_param_register())
        super(_HybridGradientBoostingBase, self).__init__()
        self.pal_funcname = 'PAL_HGBT'
        self.n_estimators = self._arg('n_estimators', n_estimators, int)
        self.random_state = self._arg('random_state', random_state, int)
        self.subsample = self._arg('subsample', subsample, float)
        self.max_depth = self._arg('max_depth', max_depth, int)
        self.split_threshold = self._arg('split_threshold', split_threshold, float)
        #self.loss = self._arg('loss', loss, str)
        self.learning_rate = self._arg('learning_rate', learning_rate, float)
        self.split_method = self._arg('split_method', split_method,
                                      self.split_method_map)
        self.sketch_eps = self._arg('sketch_eps', sketch_eps, float)
        self.fold_num = self._arg('fold_num', fold_num, int)
        self.min_sample_weight_leaf = self._arg('min_sample_weight_leaf',
                                                min_sample_weight_leaf, float)
        self.min_samples_leaf = self._arg('min_samples_leaf', min_samples_leaf, int)
        self.max_w_in_split = self._arg('max_w_in_split', max_w_in_split, float)
        self.col_subsample_split = self._arg('col_subsample_split',
                                             col_subsample_split, float)
        self.col_subsample_tree = self._arg('col_subsample_tree',
                                            col_subsample_tree, float)
        self.lamb = self._arg('lamb', lamb, float)
        self.alpha = self._arg('alpha', alpha, float)
        self.adopt_prior = self._arg('adopt_prior', adopt_prior, bool)
        #self.scale_pos_w = self._arg('scale_pos_w', scale_pos_w, float)
        #self.base_score = self._arg('base_score', base_score, float)
        self.replace_missing = self._arg('replace_missing', replace_missing, bool)
        self.default_missing_direction = self._arg('default_missing_direction',
                                                   default_missing_direction,
                                                   self.direction_map)
        self.feature_grouping = self._arg('feature_grouping', feature_grouping, bool)
        self.tol_rate = self._arg('tol_rate', tol_rate, float)
        self.compression = self._arg('compression', compression, bool)
        self.max_bits = self._arg('max_bits', max_bits, int)
        self.max_bin_num = self._arg('max_bin_num', max_bin_num, int)
        #self.categorical_variable = self._arg('categorical_variable',
        #                                      categorical_variable, ListOfStrings)
        self.thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        self.model_tree = self._arg('model_tree', model_tree, self.model_tree_map)
        self.linear_lambda = self._arg('linear_lambda', linear_lambda, float)
        self.resampling_method = self._arg('resampling_method', resampling_method,
                                           {x:x for x in self.resampling_methods})
        if isinstance(evaluation_metric, str) and 'f1_score' in evaluation_metric.lower():
            evaluation_metric = 'f1-score' + evaluation_metric[8:]
        self.evaluation_metric = self._arg('evaluation_metric',
                                           evaluation_metric.split('_')[0] if \
                                           any(x in str(evaluation_metric) for \
                                           x in ['f1-score', 'recall', 'precision']) \
                                           else evaluation_metric,
                                           {x:x for x in self.valid_metric_list},
                                           required=resampling_method is not None)
        if any(x in str(self.evaluation_metric) for x in ['f1-score', 'recall', 'precision']):
            self.evaluation_metric = evaluation_metric
        if self.evaluation_metric is None:
            self.evaluation_metric = self._arg('cv_metric', cv_metric, {x:x for x in self.valid_metric_list})
        if isinstance(ref_metric, str):
            ref_metric = [ref_metric]
        self.ref_metric = self._arg('ref_metric', ref_metric, ListOfStrings)
        if self.ref_metric is not None:
            self.ref_metric = ['f1-score' + '_' + x.split('_')[2] if 'f1_score' in x.lower() \
            else x for x in self.ref_metric]
        self.param_search_strategy = self._arg('param_search_strategy', param_search_strategy,
                                               self.param_search_map,
                                               required='sha' in str(self.resampling_method))
        if 'hyperband' in str(self.resampling_method):
            self.param_search_strategy = 'random'
        self.confusion_matrix_ = None
        if isinstance(param_range, dict):
            param_range = [(x, param_range[x]) for x in param_range]
        if isinstance(param_values, dict):
            param_values = [(x, param_values[x]) for x in param_values]
        self.param_range = self._arg('param_range',
                                     param_range, ListOfTuples)
        if self.param_range is None:
            self.param_range = self._arg('cross_validation_range',
                                         cross_validation_range,
                                         ListOfTuples)
        self.param_values = self._arg('param_values',
                                      param_values,
                                      ListOfTuples)
        self.calculate_importance = self._arg('calculate_importance',
                                              calculate_importance, bool)
        if self.param_range is not None:
            if self.param_range:
                for prm in self.param_range:
                    if prm[0] not in self.rangeparm:
                        msg = ('Parameter name {} not supported '.format(prm[0]) +
                               'for parameter selection.')
                        logger.error(msg)
                        raise ValueError(msg)

        if self.param_values is not None:
            if self.param_values:
                for prm in self.param_values:
                    if prm[0] not in self.rangeparm:
                        msg = ('Parameter name {} not supported '.format(prm[0]) +
                               'for parameter selection.')
                        logger.error(msg)
                        raise ValueError(msg)
        self.repeat_times = self._arg('repeat_times', repeat_times, int)
        self.random_search_times = self._arg('random_search_times', random_search_times, int,
                                             required=self.param_search_strategy == 'random')
        self.timeout = self._arg('timeout', timeout, int)
        self.progress_indicator_id = self._arg('progress_indicator_id', progress_indicator_id, str)
        self.resource = self._arg('resource', resource, self.resource_map)
        self.max_resource = self._arg('max_resource', max_resource, int,
                                      required=self.resource is not None)
        self.reduction_rate = self._arg('reduction_rate', reduction_rate, float)
        if self.reduction_rate is not None and self.reduction_rate <= 1.0:
            msg = '`reduction_rate` must be greater than 1'
            logger.error(msg)
            raise ValueError(msg)
        self.aggressive_elimination = self._arg('aggressive_elimination', aggressive_elimination,
                                                bool)
        self.min_resource_rate = self._arg('min_resource_rate', min_resource_rate, float)
        self.validation_set_rate = self._arg('validation_set_rate', validation_set_rate, float)
        self.stratified_validation_set = self._arg('stratified_validation_set',
                                                   stratified_validation_set, bool)
        self.tolerant_iter_num = self._arg('tolerant_iter_num', tolerant_iter_num, int)
        self.fg_min_zero_rate = self._arg('fg_min_zero_rate', fg_min_zero_rate, float)
        self.base_score = self._arg('base_score', base_score, float)
        self.validation_set_metric = self._arg('validation_set_metric', validation_set_metric, str)
        self.label_type = 'unknown'
        self.calculate_cm = None
        self.obj_func = None
        self.tweedie_power = None
        self.huber_slope = None
        self.scale_weight = None
        self.scale_weight_target = None
        self.use_vec_leaf = None

    @trace_sql
    def _fit(self,
             data,
             key=None,
             features=None,
             label=None,
             categorical_variable=None,
             warm_start=None,
             scale_weight=None,
             scale_weight_target=None):
        conn = data.connection_context
        require_pal_usable(conn)
        if isinstance(categorical_variable, str):
            categorical_variable = [categorical_variable]
        self.categorical_variable = self._arg('categorical_variable',
                                            categorical_variable,
                                            ListOfStrings)
        if warm_start is True and self.model_ is None:
            msg = 'warm_start mode requires the model of previous fit and self.model_ should not be None!'
            logger.error(msg)
            raise ValueError(msg)
        self.scale_weight = self._arg('scale_weight', scale_weight, float)
        self.scale_weight_target = self._arg('scale_weight_target', scale_weight_target, (str, int),
                                             required = scale_weight is not None)
        key = self._arg('key', key, str)
        features = self._arg('features', features, ListOfStrings)
        label = self._arg('label', label, str)
        if not self._disable_hana_execution:
            data_ = data.rearrange(key=key, features=features, label=label)
            key = data.index if key is None else key
            if label is None:
                label = data_.columns[-1]
        else:
            data_ = data
        ##okay, now label is in the final column
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        tables = ['OPTIMAL_PARAM', 'MODEL', 'VAR_IMPORTANCE', 'CM', 'STATS', 'CV']
        tables = ['#PAL_HGBT_{}_TBL_{}_{}'.format(name, self.id, unique_id) for name in tables]
        param_tbl, model_tbl, var_importance_tbl, cm_tbl, stats_tbl, cv_tbl = tables
        out_tables = [model_tbl, var_importance_tbl, cm_tbl, stats_tbl, cv_tbl]
        param_rows = [
            ('ITER_NUM', self.n_estimators, None, None),
            ('SEED', self.random_state, None, None),
            ('SKETCH_EPS', None, self.sketch_eps, None),
            ('ROW_SAMPLE_RATE', None, self.subsample, None),
            ('MAX_DEPTH', self.max_depth, None, None),
            ('GAMMA', None, self.split_threshold, None),
            ('FOLD_NUM', self.fold_num, None, None),
            ('ETA', None, self.learning_rate, None),
            ('SPLIT_METHOD', None, None, self.split_method),
            ('MIN_CHILD_HESSIAN', None, self.min_sample_weight_leaf, None),
            ('NODE_SIZE', self.min_samples_leaf, None, None),
            ('NODE_WEIGHT_CONSTRAINT', None, self.max_w_in_split, None),
            ('COL_SAMPLE_RATE_BYSPLIT', None, self.col_subsample_split, None),
            ('COL_SAMPLE_RATE_BYTREE', None, self.col_subsample_tree, None),
            ('LAMBDA', None, self.lamb, None),
            ('ALPHA', None, self.alpha, None),
            ('BASE_SCORE', None, self.base_score, None),
            ('START_FROM_AVERAGE', self.adopt_prior, None, None),
            ('CALCULATE_IMPORTANCE', self.calculate_importance, None, None),
            ('CALCULATE_CONFUSION_MATRIX', self.calculate_cm, None, None),
            ('THREAD_RATIO', None, self.thread_ratio, None),
            ('RESAMPLING_METHOD', None, None, self.resampling_method),
            ('PARAM_SEARCH_STRATEGY', None, None, self.param_search_strategy),
            ('HAS_ID', key is not None, None, None),
            ('REPEAT_TIMES', self.repeat_times, None, None),
            ('PROGRESS_INDICATOR_ID', None, None, self.progress_indicator_id),
            ('TIMEOUT', self.timeout, None, None),
            ('OBJ_FUNC', self.obj_func, None, None),
            ('TWEEDIE_POWER', None, self.tweedie_power, None),
            ('REPLACE_MISSING', self.replace_missing, None, None),
            ('DEFAULT_MISSING_DIRECTION', self.default_missing_direction, None, None),
            ('FEATURE_GROUPING', self.feature_grouping, None, None),
            ('TOLERANT_RATE', None, self.tol_rate, None),
            ('COMPRESSION', self.compression, None, None),
            ('MAX_BITS', self.max_bits, None, None),
            ('MAX_BIN_NUM', self.max_bin_num, None, None),
            ('RANDOM_SEARCH_TIMES', self.random_search_times, None, None),
            ('RESOURCE', None, None, self.resource),
            ('MAX_RESOURCE', self.max_resource, None, None),
            ('MIN_RESOURCE_RATE', None, self.min_resource_rate, None),
            ('REDUCTION_RATE', None, self.reduction_rate, None),
            ('AGGRESSIVE_ELIMINATION', self.aggressive_elimination, None, None),
            ('VALIDATION_SET_RATE', None, self.validation_set_rate, None),
            ('STRATIFIED_VALIDATION_SET', self.stratified_validation_set, None, None),
            ('TOLERANT_ITER_NUM', self.tolerant_iter_num, None, None),
            ('FG_MIN_ZERO_RATE', None, self.fg_min_zero_rate, None),
            ('HUBER_SLOPE', None, self.huber_slope, None),
            ('SCALE_WEIGHT', None, self.scale_weight, None),
            ('SCALE_WEIGHT_TARGET', None, None,
             str(self.scale_weight_target) if self.scale_weight_target is not None else None),
            ('MODEL_TREE', self.model_tree, None, None),
            ('LINEAR_LAMBDA', None, self.linear_lambda, None),
            ('USE_VEC_LEAF', self.use_vec_leaf, None, None)]
        if self.evaluation_metric is not None:
            input_metric = self.evaluation_metric.upper()
            if isinstance(self, HybridGradientBoostingClassifier):
                if self.evaluation_metric.lower().startswith('f1'):
                    input_metric = 'F1_SCORE_' + self.evaluation_metric.split('_')[-1]
                elif any(self.evaluation_metric.lower().startswith(x) for x in ['recall', 'precision']):
                    input_metric =  input_metric.split('_')[0] + '_' + self.evaluation_metric.split('_')[-1]
            param_rows.extend([('EVALUATION_METRIC', None, None, input_metric)])
        if self.validation_set_metric is not None:
            metric_name = self.validation_set_metric.upper()
            if isinstance(self, HybridGradientBoostingClassifier):
                if metric_name.startswith('F1'):
                    metric_name = 'F1_SCORE_' + self.validation_set_metric.split('_')[-1]
                elif any(metric_name.startswith(x) for x in ['PRECISION_', 'RECALL_']):
                    metric_name = metric_name.split('_')[0] + '_' + \
                    self.validation_set_metric.split('_')[1]
            param_rows.extend([('VALIDATION_SET_METRIC', None, None, metric_name)])
        #If categorical variable exists,
        #extend param rows to include the claim statement of categorical variables
        if self.categorical_variable is not None:
            param_rows.extend([('CATEGORICAL_VARIABLE', None, None, variable)
                               for variable in self.categorical_variable])
        if isinstance(self, HybridGradientBoostingClassifier):
            param_rows.extend([('CATEGORICAL_VARIABLE', None, None, label)])
        if self.ref_metric is not None:
            param_rows.extend([('REF_METRIC', None, None,
                                'F1_SCORE_{}'.format(metric.split('_')[-1]) if metric.lower().startswith('f1') else
                                (metric.split('_')[0].upper() + '_' + metric.split('_')[1] if \
                                 any(metric.lower().startswith(x) for x in ['recall', 'precision']) \
                                 else metric.upper())) for metric in self.ref_metric])
        #if cross-validation is triggered,
        # extend param rows to include the statement of ranges for cross-validation
        if self.param_range is not None:
            param_rows.extend([(
                '{}_RANGE'.format(self.hgbt_name_map[cvparam]),
                None,
                None,
                str(range_) if len(range_) == 3 else str(range_).replace(",", ",,")) for cvparam, range_ in self.param_range])
            param_rows.extend([(
                'RANGE_{}'.format(self.hgbt_name_map[cvparam]),
                None,
                None,
                str([range_[0], range_[2], int(np.floor((range_[2] - range_[0])/range_[1]))])) for cvparam, range_ in self.param_range])#pylint:disable=line-too-long
                #'[' + (to_string(range_) if len(range_) == 3 else ',,'.join(to_string(range_).split(','))) +']') for cvparam, range_ in self.param_range])
        if self.param_values is not None:
            param_rows.extend([(
                '{}_VALUES'.format(self.hgbt_name_map[cvparam]),
                None,
                None,
                str(values_).replace('[', '{').replace(']', '}')) for cvparam, values_ in self.param_values])
        try:
            if warm_start is not True:
                self._call_pal_auto(conn,
                                    "PAL_HGBT",
                                    data_,
                                    ParameterTable(param_tbl).with_data(param_rows),
                                    *out_tables)
            else:
                if check_pal_function_exist(conn, 'HGBT_CONTINUE%', like=True):
                    self._call_pal_auto(conn,
                                        "PAL_HGBT_CONTINUE",
                                        data_,
                                        self.model_,
                                        ParameterTable().with_data(param_rows),
                                        *out_tables)

        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, out_tables)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, out_tables)
            raise
        if not self._disable_hana_execution:
            self.model_ = conn.table(model_tbl)
            self.feature_importances_ = conn.table(var_importance_tbl)
            self._confusion_matrix_ = conn.table(cm_tbl)
            self.stats_ = conn.table(stats_tbl)
            self.statistics_ = self.stats_
            if self.resampling_method is not None and self.param_search_strategy is not None:
                self.selected_param_ = conn.table(cv_tbl)
            else:
                conn.table(cv_tbl)  # table() has to be called to enable correct sql tracing
                self.selected_param_ = None

    @trace_sql
    def _predict(self, key, data,
                 features=None,
                 verbose=None,
                 missing_replacement=None,
                 thread_ratio=None,
                 verbose_top_n=None):
        conn = data.connection_context
        if getattr(self, 'model_') is None:
            raise FitIncompleteError()
        self.thread_ratio = thread_ratio
        index = data.index
        key = self._arg('key', key, str, required=not isinstance(index, str))
        if isinstance(index, str):
            if key is not None and index != key:
                msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(msg)
        key = index if key is None else key
        features = self._arg('features', features, ListOfStrings)
        missing_replacement = self._arg('missing_replacement',
                                        missing_replacement,
                                        self.missing_replace_map)
        verbose = self._arg('verbose', verbose, bool)
        verbose_top_n = self._arg('verbose_top_n', verbose_top_n, int)
        cols = data.columns
        cols.remove(key)
        if features is None:
            features = cols
        data_ = data[[key] + features]
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        param_tbl, result_tbl = [
            '#PAL_HGBT_PREDICT_{}_TBL_{}_{}'.format(name, self.id, unique_id)
            for name in ['PARAM', 'RESULT']]
        param_rows = [
            ('THREAD_RATIO', None, self.thread_ratio, None),
            ('VERBOSE', verbose, None, None),
            ('MISSING_REPLACEMENT', missing_replacement, None, None),
            ('VERBOSE_TOP_N', verbose_top_n, None, None)]

        try:
            self._call_pal_auto(conn,
                                "PAL_HGBT_PREDICT",
                                data_,
                                self.model_,
                                ParameterTable(param_tbl).with_data(param_rows),
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
                           pal_funcname='PAL_HGBT',
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

            A placeholder parameter, not effective for HGBT.

        pal_funcname : int or str, optional
            PAL function name.

            Defaults to 'PAL_HGBT'.

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

class HybridGradientBoostingClassifier(_HybridGradientBoostingBase):
#pylint: disable=too-many-instance-attributes
    r"""
    Hybrid Gradient Boosting trees model for classification.

    Parameters
    ----------

    n_estimators : int, optional
        Specifies the number of trees in Gradient Boosting.

        Defaults to 10.

    split_method : {'exact', 'sketch', 'sampling', 'histogram'}, optional
        The method to finding split point for numerical features.

        - 'exact': the exact method, trying all possible points.
        - 'sketch': the sketch method, accounting for the distribution of the sum of hessian.
        - 'sampling': samples the split point randomly.
        - 'histogram': builds histogram upon data and uses it as split point.

        Defaults to 'exact'.

    random_state : int, optional
        The seed for random number generating.

        - 0: current time as seed.
        - Others : the seed.

    max_depth : int, optional
        The maximum depth of a tree.

        Defaults to 6.

    split_threshold : float, optional
        Specifies the stopping condition: if the improvement value of the best
        split is less than this value, then the tree stops growing.

    learning_rate : float, optional.
        Learning rate of each iteration, must be within the range (0, 1].

        Defaults to 0.3.

    subsample : float, optional
        The fraction of samples to be used for fitting each base learner.

        Defaults to 1.0.

    fold_num : int, optional
        The k value for k-fold cross-validation.

        Mandatory and valid only when ``resampling_method`` is set as of one the following:
        'cv', 'cv_sha', 'cv_hyperband', 'stratified_cv', 'stratified_cv_sha',
        'stratified_cv_hyperband'.

        No default value.

    sketch_eps : float, optional
        The value of the sketch method which sets up an upper limit for the sum of
        sample weights between two split points.

        Basically, the less this value is, the more number of split points are tried.

        Defaults to 0.1.

    min_sample_weight_leaf : float, optional
        The minimum summation of ample weights in a leaf node.

        Defaults to 1.0.

    min_samples_leaf : int, optional
        The minimum number of data in a leaf node.

        Defaults to 1.

    max_w_in_split : float, optional
        The maximum weight constraint assigned to each tree node.

        Defaults to 0 (i.e. no constraint).

    col_subsample_split : float, optional
        The fraction of features used for each split, should be within range (0, 1].

        Defaults to 1.0.

    col_subsample_tree : float, optional
        The fraction of features used for each tree growth, should be within range (0, 1]

        Defaults to 1.0.

    lamb : float, optional
        L2 regularization weight for the target loss function.
        Should be within range (0, 1].

        Defaults to 1.0.

    alpha : float, optional
        Weight of L1 regularization for the target loss function.

        Defaults to 1.0.

    base_score : float, optional
        Initial prediction score for all instances. Global bias for sufficient number
        of iterations(changing this value will not have too much effect).

        Defaults to 0.5.

    adopt_prior : bool, optional
        Indicates whether to adopt the prior distribution as the initial point.

        Frequencies of class labels are used for classification problems.

        ``base_score`` is ignored if this parameter is set to True.

        Defaults to False.

    evaluation_metric : {'rmse', 'mae', 'nll', 'error_rate', 'auc', \
    'recall_<class name>', 'precision_<class name>, 'f1-score_<class name>'}, optional
        Specifies the metric used for model evaluation or parameter selection.

        Mandatory if ``resampling_method`` is set.

    cv_metric : {'rmse', 'mae', 'nll', 'error_rate', 'auc'}, optional (deprecated)
        Same as ``evaluation_metric``.

        Will be deprecated in future release.

    ref_metric : str or a list of str, optional
        Specifies a reference metric or a list of reference metrics.
        Any reference metric must be a valid option of ``evaluation_metric``.

        Defaults to ['error_rate'].

    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to -1.

    calculate_importance : bool, optional
        Determines whether to calculate variable importance.

        Defaults to True.

    calculate_cm : bool, optional
        Determines whether to calculate confusion matrix.

        Defaults to True.

    resampling_method : str, optional
        Specifies the resampling method for model evaluation or parameter selection.

        Valid options include: 'cv', 'stratified_cv', 'bootstrap', 'stratified_bootstrap',
        'cv_sha', 'stratified_cv_sha', 'bootstrap_sha', 'stratified_bootstrap_sha',
        'cv_hyperband', 'stratified_cv_hyperband', 'bootstrap_hyperband',
        'stratified_bootstrap_hyperband'.

        If no value is specified for this parameter, neither model evaluation nor parameter selection is activated.

        No default value.

        .. note::
           Resampling methods that end with 'sha' or 'hyperband' are used for
           parameter selection only, not for model evaluation.

    param_search_strategy: {'grid', 'random'}, optional
        The search strategy for parameter selection.

        Mandatory if ``resampling_method`` is specified and ends with 'sha'.

        Defaults to 'random' and cannot be changed if ``resampling_method`` is specified and
        ends with 'hyperband'; otherwise no default value, and parameter selection
        cannot be carried out if not specified.

    repeat_times : int, optional
        Specifies the repeat times for resampling.

        Defaults to 1.

    random_search_times : int, optional
        Specify number of times to randomly select candidate parameters in parameter selection.

        Mandatory and valid only when ``param_search_strategy`` is set to 'random'.

        No default value.

    timeout : int, optional
        Specify maximum running time for model evaluation/parameter selection in seconds.

        Defaults to 0, which means no timeout.

    progress_indicator_id : str, optional
        Set an ID of progress indicator for model evaluation or parameter selection.

        No progress indicator will be active if no value is provided.

        No default value.

    param_range : dict or ListOfTuples, optional
        Specifies the range of parameters involved for parameter selection.

        Valid only when ``resampling_method`` and ``param_search_strategy`` are both specified.

        If input is list of tuples, then each tuple must be a pair, with the first being parameter name of str type, and
        the second being the a list of numbers with the following structure:

            [<begin-value>, <step-size>, <end-value>].

            <step-size> can be omitted if ``param_search_strategy`` is 'random'.

        Otherwise, if input is dict, then the key of each element must specify a parameter name, while
        the value of each element specifies the range of that parameter.

        Supported parameters for range specification: ``n_estimators``, ``max_depth``, ``learning_rate``,
        ``min_sample_weight_leaf``, ``max_w_in_split``, ``col_subsample_split``, ``col_subsample_tree``,
        ``lamb``, ``alpha``, ``base_score``.

        A simple example for illustration:

        [('n_estimators', [4, 3, 10]), ('learning_rate', [0.1, 0.3, 1.0])],

        or

        {'n_estimators': [4, 3, 10], 'learning_rate' : [0.1, 0.3, 1.0]}.

        No default value.

    cross_validation_range : list of tuples, optional(deprecated)
        Same as ``param_range``.

        Will be deprecated in future release.

    param_values : dict or ListOfTuples, optional
        Specifies the values of parameters involved for parameter selection.

        Valid only when ``resampling_method`` and ``param_search_strategy`` are both specified.

        If input is list of tuple, then each tuple must be a pair, with the first being parameter name of str type, and
        the second be a list values for that parameter.

        Otherwise, if input is dict, then the key of each element must specify a parameter name, while
        the value of each element specifies list of values of that parameter.

        Supported parameters for values specification are same as those valid for range specification, see ``param_range``.

        A simple example for illustration:

        [('n_estimators', [4, 7, 10]), ('learning_rate', [0.1, 0.4, 0.7, 1.0])],

        or

        {'n_estimators' : [4, 7, 10], 'learning_rate' : [0.1, 0.4, 0.7, 1.0]}.

        No default value.
    obj_func : str, optional
        Specifies the objective function to optimize, with valid options listed as follows:

        - 'logistic' : The objective function for logistic regression(for binary classification)
        - 'hinge' : The Hinge loss function(for binary classification)
        - 'softmax' : The softmax function for multi-class classification

        Defaults to 'logistic' for binary classification, and 'softmax' for multi-class classification.
    replace_missing : bool, optional
        Specifies whether or not to replace missing value by another value in the feature.

        If True,  the replacement value is the mean value for a continuous feature,
        and the mode(i.e. most frequent value) for a categorical feature.

        Defaults to True.
    default_missing_direction : {'left', 'right'}, optional
        Define the default direction where missing value will go to while tree splitting.

        Defaults to 'right'.
    feature_grouping : bool, optional
        Specifies whether or not to group sparse features that contains only one significant value
        in each row.

        Defaults to False.
    tol_rate : float, optional
        While feature grouping is enabled, still merging features when there are rows containing more than one significant value.
        This parameter specifies the rate of such rows allowed.

        Valid only when ``feature_grouping`` is set as True.

        Defaults to 0.0001.
    compression : bool, optional
        Indicates whether or not the trained model should be compressed.

        Ineffective if ``model_tree`` is 'linear'.

        Defaults to False.
    max_bits : int, optional
        Specifies the maximum number of bits to quantize continuous features, which is equivalent to
        use :math:`2^{max\_bits}` bins.

        Valid only when ``compression`` is set as True, and must be less than 31.

        Defaults to 12.

    max_bin_num : int, optional
        Specifies the maximum bin number for histogram method.

        Decreasing this number gains better performance in terms of running time at a cost of accuracy degradation.

        Only valid when ``split_method`` is set to 'histogram'.

        Defaults to 256.

    resource : str, optional
        Specifies the resource type used in successive-halving(SHA) and hyperband algorithm for parameter selection.

        Currently there are two valid options: 'n_estimators' and 'data_size'.

        Mandatory and valid only when ``resampling_method`` is set as one of the following values:
        'cv_sha', 'stratified_cv_sha', 'bootstrap_sha', 'stratified_bootstrap_sha',
        'cv_hyperband', 'stratified_cv_hyperband', 'bootstrap_hyperband', 'stratified_bootstrap_hyperband'.

        Defaults to 'data_size'.

    max_resource : int, optional
        Specifies the maximum number of estimators that should be used in SHA or Hyperband method.

        Mandatory when ``resource`` is set as 'n_estimators', and invalid if ``resampling_method`` does not take one
        of the following values: 'cv_sha', 'stratified_cv_sha', 'bootstrap_sha', 'stratified_bootstrap_sha',
        'cv_hyperband', 'stratified_cv_hyperband', 'bootstrap_hyperband', 'stratified_bootstrap_hyperband'.

    reduction_rate : float, optional
        Specifies reduction rate in SHA or Hyperband method.

        For each round, the available parameter candidate size will be divided by value of this parameter.
        Thus valid value for this parameter must be greater than 1.0

        Valid only when ``resampling_method`` takes one of the following values:
        'cv_sha', 'stratified_cv_sha', 'bootstrap_sha', 'stratified_bootstrap_sha',
        'cv_hyperband', 'stratified_cv_hyperband', 'bootstrap_hyperband', 'stratified_bootstrap_hyperband'.

        Defaults to 3.0.

    min_resource_rate : float, optional
        Specifies the minimum resource rate that should be used in SHA or Hyperband iteration.

        Valid only when ``resampling_method`` takes one of the following values:
        'cv_sha', 'stratified_cv_sha', 'bootstrap_sha', 'stratified_bootstrap_sha',
        'cv_hyperband', 'stratified_cv_hyperband', 'bootstrap_hyperband', 'stratified_bootstrap_hyperband'.

        Defaults to:

        - 0.0 if ``resource`` is set as 'data_size'(i.e. the default value)
        - 1/``max_resource`` if ``resource`` is set as 'n_estimators'.

    aggressive_elimination : bool, optional
        Specifies whether to apply aggressive elimination while using SHA method.

        Aggressive elimination happens when the data size and parameters size to be searched does not match
        and there are still bunch of parameters to be searched while data size reaches its upper limits.
        If aggressive elimination is applied, lower bound of limit of data size will be used multiple times
        first to reduce number of parameters.

        Valid only when ``resampling_method`` is set as one of the following:
        'cv_sha', 'stratified_cv_sha', 'bootstrap_sha', 'stratified_bootstrap_sha'.

        Defaults to False.

    validation_set_rate : float, optional
        Specifies the sampling rate of validation set for model evaluation in early stopping.

        Valid range is [0, 1).

        Need to specify a positive value to activate early stopping.

        Defaults to 0.

    stratified_validation_set : bool, optional
        Specifies whether or not to apply stratified sampling for getting the validation set for early stopping.

        Valid only when ``validation_set_rate`` is specified with a positive value.

        Defaults to False.

    tolerant_iter_num : int, optional
        Specifies the number of successive deteriorating iterations before early stopping.

        Valid only when ``validation_set_rate`` is specified with a positive value.

        Defaults to 10.

    fg_min_zero_rate : float, optional
        Specifies the minimum zero rate that is used to indicate sparse columns for feature grouping.

        Valid only when ``feature_grouping`` is True.

        Defaults to 0.5.

    validation_set_metric : str, optional
        Specifies the metric used to evaluate the validation dataset for early-stop. Valid options are listed as follows:

        - 'error_rate'
        - 'rmse'
        - 'mae'
        - 'auc'
        - 'nll'
        - 'f1_score_<class name>'
        - 'recall_<class name>'
        - 'precision_<class name>'

        If not specified, the value of `obj_func` will be used.

    model_tree : {'constant', 'linear'}, optional
        Specifies the tree model type for leaf nodes:

        - 'constant' : assigning a constant value for each leaf node
        - 'linear' : fitting a linear regression model for all instances falling into a leaf node

        Defaults to 'constant'.

    linear_lambda : float, optional
        Specifies the value of L2 regularization weight applied when fitting linear models for all leaf nodes.

        Valid only when ``model_tree`` is 'linear'.

        Defaults to 1.0.

    use_vec_leaf : bool, optional
        Specifies whether or not to use vector leaf method to let every leaf of tree have a vector prediction.

        Valid only when ``obj_func`` is 'softmax'.

        Defaults to False.

    References
    ----------
    - :ref:`Early Stop<early_stop-label>`
    - :ref:`Feature Grouping<feature_grouping-label>`
    - :ref:`Histogram Split<histogram_split-label>`
    - :ref:`Successive Halving and Hyperband for Parameter Selection<sha_hyperband-label>`

    Attributes
    ----------
    model_ : DataFrame
        Model content.

    feature_importances_ : DataFrame

        The feature importance (the higher, the more import the feature)

    confusion_matrix_ : DataFrame

        Confusion matrix used to evaluate the performance of classification algorithm.

    stats_ : DataFrame
        Statistics.

    selected_param_ : DataFrame

        Best choice of parameter selected.

    Examples
    --------
    >>> hgbc = HybridGradientBoostingClassifier(
    ...           n_estimators=4, split_threshold=0,
    ...           learning_rate=0.5, fold_num=5, max_depth=6,
    ...           evaluation_metric='error_rate', ref_metric=['auc'],
    ...           param_range=[('learning_rate',[0.1, 0.45, 1.0]),
    ...                        ('n_estimators', [4, 3, 10]),
    ...                        ('split_threshold', [0.1, 0.45, 1.0])])

    Perform fit():

    >>> hgbc.fit(data=df_train, features=['F1', 'F2'], label='LABEL')

    Perform predict():

    >>> res = hgbc.fit(data=df_predict, key='ID', verbose=False)
    >>> res.collect()

    """
    valid_metric_list = ['rmse', 'mae', 'nll', 'error_rate', 'auc',
                         'f1-score', 'recall', 'precision', 'f1_score']
    resampling_methods = {'cv', 'cv_sha', 'cv_hyperband',
                          'stratified_cv', 'stratified_cv_sha',
                          'stratified_cv_hyperband',
                          'bootstrap', 'bootstrap_sha', 'bootstrap_hyperband',
                          'stratified_bootstrap', 'stratified_bootstrap_sha',
                          'stratified_bootstrap_hyperband'}
    objfun_map = {'logistic': 5, 'hinge': 6, 'softmax': 7}
    validation_metrics = ['error_rate', 'rmse', 'mae', 'auc', 'nll',
                          'f1-score_', 'recall_', 'precision_', 'f1_score_']
    def __init__(self,
                 n_estimators=None,
                 random_state=None,
                 subsample=None,
                 max_depth=None,
                 split_threshold=None,
                 learning_rate=None,
                 split_method=None,
                 sketch_eps=None,
                 fold_num=None,
                 min_sample_weight_leaf=None,
                 min_samples_leaf=None,
                 max_w_in_split=None,
                 col_subsample_split=None,
                 col_subsample_tree=None,
                 lamb=None,
                 alpha=None,
                 base_score=None,
                 adopt_prior=None,
                 evaluation_metric=None,
                 cv_metric=None,
                 ref_metric=None,
                 calculate_importance=None,
                 calculate_cm=None,
                 thread_ratio=None,
                 resampling_method=None,
                 param_search_strategy=None,
                 repeat_times=None,
                 timeout=None,
                 progress_indicator_id=None,
                 random_search_times=None,
                 param_range=None,
                 cross_validation_range=None,
                 param_values=None,
                 obj_func=None,
                 replace_missing=None,
                 default_missing_direction=None,
                 feature_grouping=None,
                 tol_rate=None,
                 compression=None,
                 max_bits=None,
                 max_bin_num=None,
                 resource=None,
                 max_resource=None,
                 reduction_rate=None,
                 min_resource_rate=None,
                 aggressive_elimination=None,
                 validation_set_rate=None,
                 stratified_validation_set=None,
                 tolerant_iter_num=None,
                 fg_min_zero_rate=None,
                 validation_set_metric=None,
                 model_tree=None,
                 linear_lambda=None,
                 use_vec_leaf=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(HybridGradientBoostingClassifier, self).__init__(n_estimators,
                                                               random_state,
                                                               subsample,
                                                               max_depth,
                                                               split_threshold,
                                                               learning_rate,
                                                               split_method,
                                                               sketch_eps,
                                                               fold_num,
                                                               min_sample_weight_leaf,
                                                               min_samples_leaf,
                                                               max_w_in_split,
                                                               col_subsample_split,
                                                               col_subsample_tree,
                                                               lamb,
                                                               alpha,
                                                               adopt_prior,
                                                               evaluation_metric,
                                                               cv_metric,
                                                               ref_metric,
                                                               calculate_importance,
                                                               thread_ratio,
                                                               resampling_method,
                                                               param_search_strategy,
                                                               repeat_times,
                                                               timeout,
                                                               progress_indicator_id,
                                                               random_search_times,
                                                               param_range,
                                                               cross_validation_range,
                                                               param_values,
                                                               replace_missing,
                                                               default_missing_direction,
                                                               feature_grouping,
                                                               tol_rate,
                                                               compression,
                                                               max_bits,
                                                               max_bin_num,
                                                               resource,
                                                               max_resource,
                                                               reduction_rate,
                                                               min_resource_rate,
                                                               aggressive_elimination,
                                                               validation_set_rate,
                                                               stratified_validation_set,
                                                               tolerant_iter_num,
                                                               fg_min_zero_rate,
                                                               base_score,
                                                               validation_set_metric,
                                                               model_tree,
                                                               linear_lambda)
        self.obj_func = self._arg('obj_func', obj_func, self.objfun_map)
        self.use_vec_leaf = self._arg('use_vec_leaf', use_vec_leaf, bool)
        if self.base_score is not None and self.obj_func in (5, 6):
            if self.base_score <= 0 or self.base_score >= 1:
                msg = 'Parameter `base_score` must be in range (0, 1) for binary classification.'
                logger.error(msg)
                raise ValueError(msg)
        self.calculate_cm = self._arg('calculate_cm', calculate_cm, bool)
        if self.ref_metric is not None:
            for metric in self.ref_metric:
                if metric not in self.valid_metric_list and \
                all(x not in metric for x in ['recall', 'precision', 'f1-score', 'f1_score']):
                    msg = ("'{}' is not a valid reference metric ".format(metric)+
                           "for model evaluation in HGBT classification.")
                    logger.error(msg)
                    raise ValueError(msg)
        if self.validation_set_metric is not None:
            if not any(self.validation_set_metric.lower().startswith(x) for x in self.validation_metrics):
                msg = f'The input validation_set_metric "{validation_set_metric}" is not supported!'
                raise ValueError(msg)
        self.op_name = "HGBT_Classifier"
    #confusion matrix becomes non-empty when fit finishes
    def fit(self, data,
            key=None,
            features=None,
            label=None,
            categorical_variable=None,
            warm_start=None,
            scale_weight=None,
            scale_weight_target=None):
        """
        Fit the model to the training dataset.

        Parameters
        ----------

        data : DataFrame

            Input DataFrame.

        key : str, optional

            Name of the ID column.

            If ``key`` is not provided, then:

            - if ``data`` is indexed by a single column, then ``key`` defaults
              to that index column;
            - otherwise, it is assumed that ``data`` contains no ID column.

        features : a list of str, optional

            Names of the feature columns.

            If ``features`` is not provided, it defaults to all non-ID,
            non-label columns.

        label : str, optional

            Name of the dependent variable.

            Defaults to the name of the last non-ID column.

        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.
        warm_start : bool, optional
            When set to True, reuse the ``model_`` of current object to fit and add more trees to the existing model.
            Otherwise, just fit a new model.

            Defaults to False.

        scale_weight : bool, optional
            Specifies the scaled weight of instances with class labels to be specified by ``scale_weight_target``.

            This parameter is mainly designed for handling imbalanced binary classification problems, i.e. increasing the weight for minority class
            for decreasing the weight for majority class.

            Defaults to 1.0(i.e. do not scale).

        scale_weight_target : str, optional
            Specifies the class name of instances to be scaled in weights,
            which is valid and mandatory only when ``scale_weight`` is specified.

        Returns
        -------
        A fitted object of class "HybridGradientBoostingClassifier".
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        if not self._disable_hana_execution:
            if label is None:
                label = data.columns[-1]
            self.label_type = {x[0]:x for x in data.dtypes()}[label][1]
            if all(x not in self.label_type for x in ['INT', 'VARCHAR']):
                msg = ("Label column data type {} ".format(self.label_type) +
                    "is not supported for classification.")
                logger.error(msg)
                raise ValueError(msg)
        if categorical_variable is None:
            cat_var = []
        else:
            cat_var = categorical_variable
        super(HybridGradientBoostingClassifier, self)._fit(data=data,
                                                           key=key,
                                                           features=features,
                                                           label=label,
                                                           categorical_variable=cat_var,
                                                           warm_start=warm_start,
                                                           scale_weight=scale_weight,
                                                           scale_weight_target=scale_weight_target)
        #pylint: disable=attribute-defined-outside-init
        if not self._disable_hana_execution:
            self.confusion_matrix_ = self._confusion_matrix_
        return self

    def predict(self,
                data,
                key=None,
                features=None,
                verbose=None,
                thread_ratio=None,
                missing_replacement=None,
                verbose_top_n=None):
        """
        Predict labels based on the trained HGBT classifier.

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

        missing_replacement : str, optional

            The missing replacement strategy:

            - 'feature_marginalized': marginalise each missing feature out \
              independently.
            - 'instance_marginalized': marginalise all missing features \
              in an instance as a whole corr

        verbose : bool, optional

            If True, output all classes and the corresponding confidences \
            for each data point.

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

            DataFrame of score and confidence.
        """
        return super(HybridGradientBoostingClassifier, self)._predict(
            data=data, key=key, features=features, verbose=verbose,
            thread_ratio=thread_ratio, missing_replacement=missing_replacement,
            verbose_top_n=verbose_top_n)

    def score(self, data, key=None, features=None, label=None,
              missing_replacement=None):
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

        features : a list of str, optional

            Names of the feature columns.

            If ``features`` is not provided, it defaults to all non-ID,
            non-label columns.

        label : str, optional

            Name of the dependent variable.

            Defaults to the name of the last non-ID column.

        missing_replacement : str, optional

            The missing replacement strategy:

            - 'feature_marginalized': marginalise each missing feature out
              independently.
            - 'instance_marginalized': marginalise all missing features
              in an instance as a whole corresponding to each category.

            Defaults to 'feature_marginalized'.

        Returns
        -------
        float
            Mean accuracy on the given test data and labels.
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
        #input check for block_size and missing_replacement is done in predict()

        cols = data.columns
        cols.remove(key)
        if label is None:
            label = cols[-1]
        cols.remove(label)
        if features is None:
            features = cols

        prediction = self.predict(data=data, key=key, features=features,
                                  missing_replacement=missing_replacement)
        prediction = prediction.select(key, 'SCORE').rename_columns(['ID_P', 'PREDICTION'])

        actual = data.select(key, label).rename_columns(['ID_A', 'ACTUAL'])
        joined = actual.join(prediction, 'ID_P=ID_A').select('ACTUAL', 'PREDICTION')

        accuracy = metrics.accuracy_score(joined,
                                          label_true='ACTUAL',
                                          label_pred='PREDICTION')
        setattr(self, 'score_metrics_', {"ACCURACY": accuracy})
        return accuracy

    def load_model(self, model):
        """
        Assign a model to a HybridGradientBoostingClassifier instance.
        For example, you could assign a stored HybridGradientBoostingClassifier model to a new instance for enabling warm_start.

        Parameters
        ----------
        model : DataFrame
            The model DataFrame to be loaded.

        """
        return super(HybridGradientBoostingClassifier, self).load_model(model)

class HybridGradientBoostingRegressor(_HybridGradientBoostingBase):
    r"""
    Hybrid Gradient Boosting model for regression.

    Parameters
    ----------

    n_estimators : int, optional
        Specifies the number of trees in Gradient Boosting.

        Defaults to 10.

    split_method : {'exact', 'sketch', 'sampling', 'histogram'}, optional
        The method to find split point for numeric features.

        - 'exact': the exact method, trying all possible points.
        - 'sketch': the sketch method, accounting for the distribution of the sum of hessian.
        - 'sampling': samples the split point randomly.
        - 'histogram': builds histogram upon data and uses it as split point.

        Defaults to 'exact'.

    random_state : int, optional
        The seed for random number generating.

        - 0: current time as seed.
        - Others : the seed.

        Defaults to 0.
    max_depth : int, optional
        The maximum depth of a tree.

        Defaults to 6.

    split_threshold : float, optional
        Specifies the stopping condition: if the improvement value of the best
        split is less than this value, then the tree stops growing.

        Defaults to 0.
    learning_rate : float, optional.
        Learning rate of each iteration, must be within the range (0, 1].

        Defaults to 0.3.

    subsample : float, optional
        The fraction of samples to be used for fitting each base learner.

        Defaults to 1.0.

    fold_num : int, optional
        The k value for k-fold cross-validation.

        Mandatory and valid only when ``resampling_method`` is set as
        'cv', 'cv_sha' or 'cv_hyperband'.

    sketch_eps : float, optional
        The value of the sketch method which sets up an upper limit for the sum of
        sample weights between two split points.

        Basically, the less this value is, the more number of split points are tried.

        Defaults to 0.1.

    min_sample_weight_leaf : float, optional
        The minimum summation of ample weights in a leaf node.

        Defaults to 1.0.

    min_sample_leaf : int, optional
        The minimum number of data in a leaf node.

        Defaults to 1.

    max_w_in_split : float, optional
        The maximum weight constraint assigned to each tree node.

        Defaults to 0 (i.e. no constraint).

    col_subsample_split : float, optional
        The fraction of features used for each split, should be within range (0, 1].

        Defaults to 1.0.

    col_subsample_tree : float, optional
        The fraction of features used for each tree growth, should be within range (0, 1].

        Defaults to 1.0.

    lamb : float, optional
        Weight of L2 regularization for the target loss function.

        Should be within range (0, 1].

        Defaults to 1.0.

    alpha : float, optional
        Weight of L1 regularization for the target loss function.

        Defaults to 1.0.

    adopt_prior : bool, optional
        For regression problems, this parameter specifies whether or not to use
        the average value of the training data as the initial prediction score.

        ``base_score`` is ignored if this parameter is set to True.

        Defaults to False.

    evaluation_metric : {'rmse', 'mae'}, optional
        The evaluation metric used for model evaluation or parameter selection.

        Mandatory if ``resampling_method`` is set.

    cv_metric : {'rmse', 'mae'}, optional(deprecated)
        Same as ``evaluation_metric``.

        Will be deprecated in future release.

    ref_metric : str or a list of str, optional
        Specifies a reference metric or a list of reference metrics.

        Any reference metric must be a valid option of ``evaluation_metric``.

        No default value.

    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to -1.

    calculate_importance : bool, optional
        Determines whether to calculate variable importance.

        Defaults to True.

    calculate_cm : bool, optional
        Determines whether to calculate confusion matrix.

        Defaults to True.

    resampling_method : {'cv', 'cv_sha', 'cv_hyperband', 'bootstrap', 'bootstrap_sha', 'bootstrap_hyperband'}, optional
        Specifies the resampling method for model evaluation or parameter selection.

        If no value is specified for this parameter, neither model evaluation nor parameter selection is activated.

        No default value.

        .. note::
            Resampling methods that end with 'sha' or 'hyperband' are used for
            parameter selection only, not for model evaluation.

    param_search_strategy: {'grid', 'random'}, optional
        The search strategy for parameters.

        Mandatory if ``resampling_method`` is specified and ends with 'sha'.

        Defaults to 'random' and cannot be changed if ``resampling_method`` is specified and
        ends with 'hyperband'; otherwise no default value, and parameter selection
        cannot be carried out if not specified.
    repeat_times : int, optional
        Specifies the repeat times for resampling.

        Defaults to 1.

    random_search_times : int, optional
        Specify number of times to randomly select candidate parameters in parameter selection.

        Mandatory and valid only when ``param_search_strategy`` is 'random'.

        No default value.

    timeout : int, optional
        Specify maximum running time for model evaluation/parameter selection in seconds.

        Defaults to 0, which means no timeout.

    progress_indicator_id : str, optional
        Set an ID of progress indicator for model evaluation or parameter selection.

        No progress indicator will be active if no value is provided.

        No default value.

    param_range : dict or ListOfTuples, optional
        Specifies the range of parameters involved for parameter selection.

        Valid only when ``resampling_method`` and ``param_search_strategy`` are both specified.

        If input is list of tuples, then each tuple must be a pair, with the first being parameter name of str type, and
        the second being the a list of numbers with the following structure:

            [<begin-value>, <step-size>, <end-value>].

            <step-size> can be omitted if ``param_search_strategy`` is 'random'.

        Otherwise, if input is dict, then the key of each element must specify a parameter name, while
        the value of each element specifies the range of that parameter.

        Supported parameters for range specification: ``n_estimators``, ``max_depth``, ``learning_rate``, \
        ``min_sample_weight_leaf``, ``max_w_in_split``, ``col_subsample_split``, ``col_subsample_tree``, \
        ``lamb``, ``alpha``, ``base_score``.

        A simple example for illustration:

            [('n_estimators', [4, 3, 10]), ('learning_rate', [0.1, 0.3, 1.0])],

        or

            {'n_estimators': [4, 3, 10], 'learning_rate' : [0.1, 0.3, 1.0]}.

        No default value.

    cross_validation_range : list of tuples, optional(deprecated)
          Same as ``param_range``.

          Will be deprecated in future release.

    param_values : dict or ListOfTuples, optional
        Specifies the values of parameters involved for parameter selection.

        Valid only when ``resampling_method`` and ``param_search_strategy`` are both specified.

        If input is list of tuple, then each tuple must be a pair, with the first being parameter name of str type, and
        the second be a list values for that parameter.

        Otherwise, if input is dict, then the key of each element must specify a parameter name, while
        the value of each element specifies list of values of that parameter.

        Supported parameters for values specification are same as those valid for range specification, see ``param_range``.

        A simple example for illustration:

            [('n_estimators', [4, 7, 10]), ('learning_rate', [0.1, 0.4, 0.7, 1.0])],

        or

            {'n_estimators' : [4, 7, 10], 'learning_rate' : [0.1, 0.4, 0.7, 1.0]}.

        No default value.
    obj_func : str, optional
        Specifies the objective function to optimize, with valid options listed as follows:

        - 'se' : Squared error
        - 'ae' : Absolute error(with iterative *reweighted-least-square* solver)
        - 'sle' : Squared-log error
        - 'huber' : Huber loss function
        - 'pseudo-huber' : Pseudo Huber loss function
        - 'gamma' : Gamma objective function
        - 'tweedie' : Tweedie objective function

        Defaults to 'se'.
    tweedie_power : float, optional
        Specifies the power for tweedie objective function, with valid range [1.0, 2.0].

        Only valid when ``obj_func`` is 'tweedie'.

        Defaults to 1.5.
    replace_missing : bool, optional
        Specifies whether or not to replace missing value by another value in the feature.

        If True,  the replacement value is the mean value for a continuous feature,
        and the mode(i.e. most frequent value) for a categorical feature.

        Defaults to True.
    default_missing_direction : {'left', 'right'}, optional
        Define the default direction where missing value will go to while tree splitting.

        Defaults to 'right'.
    feature_grouping : bool, optional
        Specifies whether or not to group sparse features that contains only one significant value
        in each row.

        Defaults to False.
    tol_rate : float, optional
        While feature grouping is enabled, still merging features when there are rows containing more than one significant value.
        This parameter specifies the rate of such rows allowed.

        Valid only when ``feature_grouping`` is set as True.

        Defaults to 0.0001.
    compression : bool, optional
        Indicates whether or not the trained model should be compressed.

        Ineffective if ``model_tree`` is 'linear'.

        Defaults to False.

    max_bits : int, optional
        Specifies the maximum number of bits to quantize continuous features, which is equivalent to
        use :math:`2^{max\_bits}` bins.

        Valid only when ``compression`` is set as True, and must be less than 31.

        Defaults to 12.

    max_bin_num : int, optional
        Specifies the maximum bin number for histogram method.

        Decreasing this number gains better performance in terms of running time at a cost of accuracy degradation.

        Only valid when ``split_method`` is set to 'histogram'.

        Defaults to 256.

    resource : str, optional
        Specifies the resource type used in successive-halving(SHA) and hyperband algorithm for parameter selection.

        Currently there are two valid options: 'n_estimators' and 'data_size'.

        Mandatory and valid only when ``resampling_method`` is set as one of the following:
        'cv_sha', 'bootstrap_sha', 'cv_hyperband', 'bootstrap_hyperband'.

        Defaults to 'data_size'.

    max_resource : int, optional
        Specifies the maximum number of estimators that should be used in SHA or Hyperband method.

        Mandatory when ``resource`` is set as 'n_estimators', and invalid if ``resampling_method`` does not take one
        of the following values: 'cv_sha', 'bootstrap_sha', 'cv_hyperband', 'bootstrap_hyperband'.

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

        Defaults to:

            - 0.0 if ``resource`` is set as 'data_size'(the default value)
            - 1/``max_resource`` if ``resource`` is set as 'n_estimators'.

    aggressive_elimination : bool, optional
        Specifies whether to apply aggressive elimination while using SHA method.

        Aggressive elimination happens when the data size and parameters size to be searched does not match
        and there are still bunch of parameters to be searched while data size reaches its upper limits.
        If aggressive elimination is applied, lower bound of limit of data size will be used multiple times
        first to reduce number of parameters.

        Valid only when ``resampling_method`` is 'cv_sha' or 'bootstrap_sha'.

        Defaults to False.

    validation_set_rate : float, optional
        Specifies the sampling rate of validation set for model evaluation in early stopping.

        Valid range is [0, 1).

        Need to specify a positive value to activate early stopping.

        Defaults to 0.

    stratified_validation_set : bool, optional
        Specifies whether or not to apply stratified sampling for getting the validation set for early stopping.

        Valid only when ``validation_set_rate`` is specified with a positive value.

        Defaults to False.

    tolerant_iter_num : int, optional
        Specifies the number of successive deteriorating iterations before early stopping.

        Valid only when ``validation_set_rate`` is specified with a positive value.

        Defaults to 10.

    fg_min_zero_rate : float, optional
        Specifies the minimum zero rate that is used to indicate sparse columns for feature grouping.

        Valid only when ``feature_grouping`` is True.

        Defaults to 0.5.

    huber_slope : float, optional
        Specifies the slope parameter in Huber loss function or pseudo-Huber loss function.

        The value must be greater than 0.

        Valid only when ``obj_func`` is set as 'huber' or 'pseudo-huber'.

        Defaults to 1.0.
    base_score : float, optional
        Specifies the initial prediction score of the training data.

        Ignored if ``adopt_prior`` is set as True.

        Default value dependents on the choice of ``obj_func`` specified.

    validation_set_metric : str, optional
        Specifies the metric used to evaluate the validation dataset for early-stop. Valid options are listed as follows:

        - 'rmse'
        - 'mae'

        If not specified, the value of `obj_func` will be used.

    model_tree : {'constant', 'linear'}, optional
        Specifies the tree model type for leaf nodes:

        - 'constant' : assigning a constant value for each leaf node
        - 'linear' : fitting a linear regression model for all instances falling into a leaf node

        Defaults to 'constant'.

    linear_lambda : float, optional
        Specifies the value of L2 regularization weight applied when fitting linear models for all leaf nodes.

        Valid only when ``model_tree`` is 'linear'.

        Defaults to 1.0.

    References
    ----------
    - :ref:`Early Stop<early_stop-label>`
    - :ref:`Feature Grouping<feature_grouping-label>`
    - :ref:`Histogram Split<histogram_split-label>`
    - :ref:`Successive Halving and Hyperband for Parameter Selection<sha_hyperband-label>`

    Attributes
    ----------

    model_ : DataFrame
        Model content.

    feature_importances_ : DataFrame

        The feature importance (the higher, the more import the feature).

    stats_ : DataFrame
        Statistics.

    selected_param_ : DataFrame

        Best parameters obtained from parameter selection.

    Examples
    --------
    >>> hgbr = HybridGradientBoostingRegressor(
    ...           n_estimators=20, split_threshold=0.75,
    ...           split_method='exact', learning_rate=0.75,
    ...           fold_num=5, max_depth=6,
    ...           resampling_method='cv',
    ...           param_search_strategy='grid',
    ...           evaluation_metric = 'rmse', ref_metric=['mae'],
    ...           param_range=[('learning_rate',[0.01, 0.25, 1.0]),
    ...                        ('n_estimators', [10, 1, 20]),
    ...                        ('split_threshold', [0.01, 0.25, 1.0])])

    Preform fit():

    >>> hgbr.fit(data=df_train, features=['F1','F2'], label='TARGET')

    Preform predict():

    >>> res = hgbr.predict(data=df_predict, key='ID', verbose=False)
    >>> res.collect()

    Preform score():

    >>> res = hgbr.score(data=df_score)
    >>> res.collect()

    """
    valid_metric_list = ['rmse', 'mae']
    resampling_methods = {'cv', 'cv_sha', 'cv_hyperband',
                          'bootstrap', 'bootstrap_sha', 'bootstrap_hyperband'}
    objfun_map = {'se': 0, 'sle': 1, 'pseudo-huber': 2, 'gamma': 3, 'tweedie': 4, 'huber': 9,
                  'ae': 10}
    validation_metrics = ['rmse', 'mae']
    def __init__(self,
                 n_estimators=None,
                 random_state=None,
                 subsample=None,
                 max_depth=None,
                 split_threshold=None,
                 learning_rate=None,
                 split_method=None,
                 sketch_eps=None,
                 fold_num=None,
                 min_sample_weight_leaf=None,
                 min_samples_leaf=None,
                 max_w_in_split=None,
                 col_subsample_split=None,
                 col_subsample_tree=None,
                 lamb=None,
                 alpha=None,
                 adopt_prior=None,
                 evaluation_metric=None,
                 cv_metric=None,
                 ref_metric=None,
                 calculate_importance=None,
                 thread_ratio=None,
                 resampling_method=None,
                 param_search_strategy=None,
                 repeat_times=None,
                 timeout=None,
                 progress_indicator_id=None,
                 random_search_times=None,
                 param_range=None,
                 cross_validation_range=None,
                 param_values=None,
                 obj_func=None,
                 tweedie_power=None,
                 replace_missing=None,
                 default_missing_direction=None,
                 feature_grouping=None,
                 tol_rate=None,
                 compression=None,
                 max_bits=None,
                 max_bin_num=None,
                 resource=None,
                 max_resource=None,
                 reduction_rate=None,
                 min_resource_rate=None,
                 aggressive_elimination=None,
                 validation_set_rate=None,
                 stratified_validation_set=None,
                 tolerant_iter_num=None,
                 fg_min_zero_rate=None,
                 huber_slope=None,
                 base_score=None,
                 validation_set_metric=None,
                 model_tree=None,
                 linear_lambda=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(HybridGradientBoostingRegressor, self).__init__(n_estimators,
                                                              random_state,
                                                              subsample,
                                                              max_depth,
                                                              split_threshold,
                                                              learning_rate,
                                                              split_method,
                                                              sketch_eps,
                                                              fold_num,
                                                              min_sample_weight_leaf,
                                                              min_samples_leaf,
                                                              max_w_in_split,
                                                              col_subsample_split,
                                                              col_subsample_tree,
                                                              lamb,
                                                              alpha,
                                                              adopt_prior,
                                                              evaluation_metric,
                                                              cv_metric,
                                                              ref_metric,
                                                              calculate_importance,
                                                              thread_ratio,
                                                              resampling_method,
                                                              param_search_strategy,
                                                              repeat_times,
                                                              timeout,
                                                              progress_indicator_id,
                                                              random_search_times,
                                                              param_range,
                                                              cross_validation_range,
                                                              param_values,
                                                              replace_missing,
                                                              default_missing_direction,
                                                              feature_grouping,
                                                              tol_rate,
                                                              compression,
                                                              max_bits,
                                                              max_bin_num,
                                                              resource,
                                                              max_resource,
                                                              reduction_rate,
                                                              min_resource_rate,
                                                              aggressive_elimination,
                                                              validation_set_rate,
                                                              stratified_validation_set,
                                                              tolerant_iter_num,
                                                              fg_min_zero_rate,
                                                              base_score,
                                                              validation_set_metric,
                                                              model_tree,
                                                              linear_lambda)
        self.obj_func = self._arg('obj_func', obj_func, self.objfun_map)
        self.op_name = 'HGBT_Regressor'
        self.tweedie_power = self._arg('tweedie_power', tweedie_power, float)
        self.huber_slope = self._arg('huber_slope', huber_slope, float)
        self.validation_set_metric = self._arg('validation_set_metric', validation_set_metric,
                                               {x : x for x in self.validation_metrics})
        if self.ref_metric is not None:
            for metric in self.ref_metric:
                if metric not in self.valid_metric_list:
                    msg = ("'{}' is not a valid reference metric ".format(metric)+
                           "for model evaluation in HGBT regression.")
                    logger.error(msg)
                    raise ValueError(msg)

    #@override
    def fit(self, data,
            key=None,
            features=None,
            label=None,
            categorical_variable=None,
            warm_start=None):
        r"""
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

            If ``features`` is not provided, it defaults to all non-ID,
            non-label columns.

        label : str, optional

            Name of the dependent variable.

            Defaults to the name of the last non-ID column.

        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.
        warm_start : bool, optional
            When set to True, reuse the ``model_`` of current object to fit and add more trees to the existing model.
            Otherwise, just fit a new model.

            Defaults to False.


        Returns
        -------
        A fitted object of class "HybridGradientBoostingRegressor".
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        if label is None:
            label = data.columns[-1]
        self.label_type = data.dtypes([label])[0][1]
        if categorical_variable is None:
            cat_var = []
        else:
            cat_var = categorical_variable
        if self.label_type in ('VARCHAR', 'NVARCHAR') or \
            (self.label_type == 'INT' and label in cat_var):
            msg = "Label column is treated as categorical, not supported for regression."
            logger.error(msg)
            raise ValueError(msg)
        super(HybridGradientBoostingRegressor, self)._fit(data,
                                                          key=key,
                                                          features=features,
                                                          label=label,
                                                          categorical_variable=categorical_variable,
                                                          warm_start=warm_start)
        return self

    def predict(self, data,
                key=None,
                features=None,
                verbose=None,
                thread_ratio=None,
                missing_replacement=None):
        r"""
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

            Names of the feature columns. \
            If not provided, it defaults to all non-ID columns.

        missing_replacement : str, optional
            The missing replacement strategy:

            - 'feature_marginalized': marginalise each missing feature out
              independently.
            - 'instance_marginalized': marginalise all missing features
              in an instance as a whole corresponding to each category.

            Defaults to 'feature_marginalized'.

        verbose : bool, optional(deprecated)

            If true, output all classes and the corresponding confidences
            for each data point.

            Invalid for regression problem and will be removed in future release.

        Returns
        -------

        DataFrame

            DataFrame of score and confidence, structured as follows:

            - ID column, with same name and type as ``data`` 's ID column.
            - SCORE, type DOUBLE, representing the predicted classes.
            - CONFIDENCE, type DOUBLE, all None for regression prediction.
        """
        return super(HybridGradientBoostingRegressor, self)._predict(
            data=data, key=key, features=features, verbose=verbose,
            thread_ratio=thread_ratio, missing_replacement=missing_replacement)

    def score(self, data, key=None, features=None, label=None, missing_replacement=None,
              score_type=None, tweedie_power=None):
        """
        Returns the regression score based on specified score type.

        Parameters
        ----------

        data : DataFrame

            Data on which to assess model performance.

        key : str, optional

            Name of the ID column.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.


        features : a list of str, optional

            Names of the feature columns.

            If ``features`` is not provided, it defaults to all non-ID, non-label columns.

        label : str, optional

            Name of the dependent variable.

            Defaults to the name of the last non-ID column.

        missing_replacement : str, optional

            The missing replacement strategy:

            - 'feature_marginalized': marginalise each missing feature out
              independently.
            - 'instance_marginalized': marginalise all missing features
              in an instance as a whole corresponding to each category.

            Defaults to feature_marginalized.

        score_type : {'r2', 'r2-log', 'ae', 'gamma', 'tweedie'}, optional
            Specifies the type of regression score to be computed.

            - 'r2' : r2 score
            - 'r2-log' : r2 log score
            - 'ae' : absolute error score
            - 'gamma' : gamma score
            - 'tweedie' : tweedie score

            Default value depends on the value of ``obj_func`` specified when training the model:

            - ``obj_func`` = 'se' : defaults to 'r2'.
            - ``obj_func`` = 'sle' : defaults to 'r2-log'.
            - ``obj_func`` = 'huber' or 'pseudo-huber' : defaults to 'ae'.
            - ``obj_func`` = 'gamma' : defaults to 'gamma'.
            - ``obj_func`` = 'tweedie' : defaults to 'tweedie'.

        tweedie_power : float, optional
            Specifies the power parameter for Tweedie regression, with valid range (1.0, 2.0).

            Valid only when ``score_type`` is 'tweedie'.

            Defaults to 1.5 if `self.tweedie_power` is None,
            else defaults to `self.tweedie_power`.

        Returns
        -------
        float
            The regression score calculated base on the given data.
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
        obj_score_map = {'se':'r2', 'huber':'ae',
                         'pseudo-huber': 'ae',
                         'sle':'r2-log', 'gamma':'gamma',
                         'tweedie':'tweedie'}
        score_type = self._arg('score_type', score_type, str)
        if score_type is None:
            obj_f = 'se' if self.obj_func is None else \
            list(self.objfun_map.keys())[list(self.objfun_map.values()).index(self.obj_func)]
            score_type = obj_score_map[obj_f]
        #input check for block_size and missing_replacement is done in predict()
        cols = data.columns
        cols.remove(key)
        if label is None:
            label = cols[-1]
        cols.remove(label)
        if features is None:
            features = cols
        prediction = self.predict(data=data, key=key, features=features,
                                  missing_replacement=missing_replacement)
        prediction = prediction.cast('SCORE', 'DOUBLE')#For prediction, cast SCORE column to DOUBLE
        original = data[[key, label]]
        prediction = prediction.select([key, 'SCORE']).rename_columns(['ID_P', 'PREDICTION'])
        original = data[[key, label]].rename_columns(['ID_A', 'ACTUAL'])
        joined = original.join(prediction, 'ID_P=ID_A').select('ACTUAL', 'PREDICTION')
        if tweedie_power is None:
            tweedie_power = 1.5 if self.tweedie_power is None else self.tweedie_power
        scr = -1.0
        try:#logic: try to calculate target score, and if the calculation fails, return the r2 score instead.
            scr = metrics.regression_score(joined,
                                           label_true='ACTUAL',
                                           label_pred='PREDICTION',
                                           score_type = score_type,
                                           tweedie_power = tweedie_power)
        except ValueError:
            msg = 'Calculation of target score failed, r2 score is returned instead!'
            logger.warning(msg)
            scr = metrics.r2_score(joined,
                                   label_true='ACTUAL',
                                   label_pred='PREDICTION')
        setattr(self, 'score_metrics_', {"R2": scr})
        return scr

    def load_model(self, model):
        """
        Assign a model to a HybridGradientBoostingRegressor instance.
        For example, you could assign a stored HybridGradientBoostingRegressor model to a new instance for enabling warm_start.

        Parameters
        ----------
        model : DataFrame
            The model DataFrame to be loaded.

        """
        return super(HybridGradientBoostingRegressor, self).load_model(model)
