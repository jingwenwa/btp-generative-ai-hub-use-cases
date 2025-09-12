"""
This module contains Python wrapper for SAP HANA PAL unified-classification.

The following classes are available:
    * :class:`UnifiedClassification`
"""
#pylint: disable=too-many-lines, unused-private-member, use-a-generator
#pylint: disable=line-too-long, c-extension-no-member
#pylint: disable=too-many-locals, consider-using-f-string
#pylint: disable=too-many-arguments, unused-variable
#pylint: disable=ungrouped-imports, consider-using-dict-items
#pylint: disable=relative-beyond-top-level
#pylint: disable=no-member, too-many-nested-blocks
#pylint: disable=consider-iterating-dictionary
#pylint:disable=line-too-long, attribute-defined-outside-init
#pylint:disable=invalid-name, too-many-statements, too-many-branches
import logging
import uuid
import pandas as pd
from hdbcli import dbapi
from hana_ml.visualizers.model_report import (
    _UnifiedClassificationReportBuilder
)
from hana_ml.ml_base import try_drop, quotename
from hana_ml.ml_exceptions import FitIncompleteError
from .preprocessing import Sampling
from .utility import AMDPHelper, mlflow_autologging
from .sqlgen import trace_sql
from .utility import check_pal_function_exist
from .tsa.arima import _delete_none_key_in_dict, _col_index_check
from .pal_base import (
    arg,
    PALBase,
    ParameterTable,
    require_pal_usable,
    pal_param_register,
    ListOfStrings,
    ListOfTuples)
logger = logging.getLogger(__name__) #pylint: disable=invalid-name

def json2tab_for_reason_code(data, key="ID", reason_code_col="REASON_CODE"):
    """
    Transform json formatted reason code to table formatted one.

    parameters
    ----------
    data : DataFrame
        DataFrame contains the reason code.

    """
    tab_struct = data.get_table_structure()
    if key not in tab_struct:
        raise ValueError("{} doesn't exist!".format(key))
    if reason_code_col not in tab_struct:
        raise ValueError("{} doesn't exist!".format(reason_code_col))
    key_type = tab_struct[key]
    var_id_quote = ''
    if not 'INT' in key_type.upper():
        var_id_quote = '"'
    unique_id = str(uuid.uuid1()).replace('-', '_').upper()
    temp_tab_name = "#REASON_CODE_" + unique_id
    data.connection_context.sql("""
        SELECT '{' || '""" +
        "{0}:{1}".format(quotename(key), var_id_quote) +
        """' || {0} || '{1},""".format(quotename(key), var_id_quote) +
        """{0}:' || {0} ||""".format(quotename(reason_code_col)) +
        """ '}' AS REASON_CODE FROM""" +
        "{0}.{1}".format(quotename(data.source_table['SCHEMA_NAME']),
                                   quotename(data.source_table['TABLE_NAME']))).save(temp_tab_name, force=True)
    return data.connection_context.sql("""
        SELECT JT.*
        FROM JSON_TABLE({0}.REASON_CODE, '$'
        COLUMNS
            (
                {1} {2} PATH '$.{1}',
                NESTED PATH '$.REASON_CODE[*]'
                COLUMNS
                    (
                        "attr" VARCHAR(255) PATH '$.attr',
                        "pct" DOUBLE PATH '$.pct',
                        "val" DOUBLE PATH '$.val'
                    )
            )
            ) AS JT""".format(temp_tab_name, key, key_type))

def _key_index_check(key, param_name, index_value):
    if key is not None:
        if isinstance(index_value, str) and key != index_value:
            warn_msg = "Discrepancy between the designated {} column '{}' ".format(param_name, key) +\
                       "and the designated index {} column which is '{}'.".format(param_name, index_value)
            logger.warning(warn_msg)
    elif isinstance(index_value, str):
        key = index_value
    return key

def _params_check(input_dict, param_map, func):
    update_params = {}
    if not input_dict:
        return {}
    for parm in input_dict:
        if parm not in ['categorical_variable', 'strategy_by_col']:
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
                err_msg = "'{}' is not a valid parameter name for initializing a {} model".format(parm, func)
                logger.error(err_msg)
                raise KeyError(err_msg)
        else:
            continue
        par_val = input_dict.get('partition_method')
        stra_val = input_dict.get('stratified_column')
        if par_val == "stratified" and stra_val is None:
            msg = "Please select stratified_column when you use stratified partition method!"
            logger.error(msg)
            raise ValueError(msg)
    return update_params


def _mlogr_solver_number_update(input_dict, func):
    if not input_dict:
        return {}
    update_params = input_dict
    if func == 'M_LOGR' and 'METHOD' in update_params:
        if update_params['METHOD'][0] in (None, -1):
            update_params['METHOD'] = (None, int)
        elif update_params['METHOD'][0] == 3:
            update_params['METHOD'] = (0, int)
        elif update_params['METHOD'][0] == 2:
            update_params['METHOD'] = (1, int)
        else:
            msg = "Solver not supported for multi-class logistic regression."
            logger.error(msg)
            raise ValueError(msg)
    return update_params


def _categorical_variable_update(input_cate_var, label_t, label):
    if isinstance(input_cate_var, str):
        input_cate_var = [input_cate_var]
    if label_t is not None:
        if 'INT' in label_t:
            if input_cate_var is None:
                input_cate_var = [label]
            elif label not in input_cate_var:
                input_cate_var.append(label)
    input_cate_var = arg('categorical_variable', input_cate_var, ListOfStrings)
    return input_cate_var


class UnifiedClassification(PALBase, _UnifiedClassificationReportBuilder, AMDPHelper):#pylint: disable=too-many-instance-attributes
    """
    The Python wrapper for SAP HANA PAL Unified Classification function.

    Compared with the original classification interfaces,
    new features supported are listed below:

    - Classification algorithms easily switch
    - Dataset automatic partition
    - Model evaluation procedure provided
    - More metrics supported

    Parameters
    ----------

    func : str

        The name of a specified classification algorithm.
        The following algorithms are supported:

        - 'DecisionTree'
        - 'HybridGradientBoostingTree'
        - 'LogisticRegression'
        - 'MLP'
        - 'NaiveBayes'
        - 'RandomDecisionTree'
        - 'SVM'
        - 'MLP_MultiTask'

        .. Note ::
            'LogisticRegression' contains both binary-class logistic-regression as well as multi-class logistic-regression functionalities. \
            By default the functionality is assumed to be binary-class. If you want to shift to multi-class logistic-regression, \
            please set ``func`` to be 'LogisticRegression' and specify ``multi-class = True``.

    multi_class : bool, optional
        Specifies whether or not to use multiclass-logisticregression.

        Only valid when ``func`` is 'LogisticRegression'. Defaults to None.

    massive : bool, optional
        Specifies whether or not to use massive mode.

        - True : massive mode.
        - False : single mode.

        For parameter setting in massive mode, you could use both
        group_params (please see the example below) or the original parameters.
        Using original parameters will apply for all groups. However, if you define some parameters of a group,
        the value of all original parameter setting will be not applicable to such group.

        An example is as follows:

        .. only:: latex

            >>> uc = UnifiedClassification(func='logisticregression',
                                           multi_class=True,
                                           massive=True,
                                           max_iter=10,
                                           group_params={'Group_1': {'solver': 'auto'}})
            >>> uc.fit(data=df,
                       key='ID',
                       features=["OUTLOOK" ,"TEMP", "HUMIDITY","WINDY"],
                       label="CLASS",
                       group_key="GROUP_ID",
                       background_size=4,
                       group_params={'Group_1':{'background_random_state':2}})

        .. raw:: html

            <iframe allowtransparency="true" style="border:1px solid #ccc; background: #eeffcb;"
                src="../../_static/uc_init_example1.html" width="100%" height="100%" sandbox="">
            </iframe>

        In the first line of code, as 'solver' is set in group_params for Group_1,
        parameter setting of 'max_iter' is not applicable to Group_1.

        Defaults to False.

    group_params : dict, optional
        If massive mode is activated (``massive`` is True), input data for classification shall be divided into different
        groups with different classification parameters applied. This parameter specifies the parameter
        values of the chosen classification algorithm ``func`` w.r.t. different groups in a dict format,
        where keys corresponding to ``group_key`` while values should be a dict for classification algorithm
        parameter value assignments.

        An example is as follows:

        .. only:: latex

            >>> uc = UnifiedClassification(func='logisticregression',
                                           multi_class=True,
                                           massive=True,
                                           max_iter=10,
                                           group_params={'Group_1' : {'solver' : 'auto'}})
            >>> uc.fit(data=df,
                       key='ID',
                       features=["OUTLOOK" ,"TEMP", "HUMIDITY","WINDY"],
                       label="CLASS",
                       group_key="GROUP_ID",
                       background_size=4,
                       group_params={'Group_1' : {'background_random_state' : 2}})

        .. raw:: html

            <iframe allowtransparency="true" style="border:1px solid #ccc; background: #eeffcb;"
                src="../../_static/uc_init_example2.html" width="100%" height="100%" sandbox="">
            </iframe>

        Valid only when ``massive`` is True and defaults to None.

    pivoted : bool, optional
        If True, it will enable PAL unified classification function for pivoted data. In this case,
        meta data must be provided in the fit function.

        Defaults to False.

    **kwargs : keyword arguments

        Arbitrary keyword arguments and please referred to the responding algorithm for the parameters' key-value pair.

        **Note that some parameters are disabled in the classification algorithm!**

        - **'DecisionTree'** : :class:`~hana_ml.algorithms.pal.trees.DecisionTreeClassifier`

          - Disabled parameters: output_rules, output_confusion_matrix.
          - Parameters removed from initialization but can be specified in fit(): categorical_variable, bins, priors.

        - **'HybridGradientBoostingTree'** : :class:`~hana_ml.algorithms.pal.trees.HybridGradientBoostingClassifier`

          - Disabled parameters: calculate_importance, calculate_cm.
          - Parameters removed from initialization but can be specified in fit(): categorical_variable.

        - **'LogisticRegression'** :class:`~hana_ml.algorithms.pal.linear_model.LogisticRegression`

          - Disabled parameters : pmml_export.
          - Parameters removed from initialization but can be specified in fit(): categorical_variable, class_map0, class_map1.
          - Parameters with changed meaning : ``json_export``, where False value now means
            'Exports multi-class logistic regression model in PMML'.

        - **'MLP'** : :class:`~hana_ml.algorithms.pal.neural_network.MLPClassifier`

          - Disabled parameters: functionality.
          - Parameters removed from initialization but can be specified in fit(): categorical_variable.

        - **'NaiveBayes'** : :class:`~hana_ml.algorithms.pal.naive_bayes.NaiveBayes`

          - Parameters removed from initialization but can be specified in fit(): categorical_variable.

        - **'RandomDecisionTree'** : :class:`~hana_ml.algorithms.pal.trees.RDTClassifier`

          - Disabled parameters: calculate_oob.
          - Parameters removed from initialization but can be specified in fit(): categorical_variable, strata, priors.

        - **'SVM'** : :class:`~hana_ml.algorithms.pal.svm.SVC`

          - Parameters removed from initialization but can be specified in fit(): categorical_variable.

        - **'MLP_MultiTask'** :class:`~hana_ml.algorithms.pal.neural_network.MLPMultiTaskClassifier`

          - Disabled parameters : ``finetune``.

        For more parameter mappings of hana_ml and HANA PAL, please refer to the doc page: :ref:`param_mapping`

        An example for decision tree algorithm is shown below:

        You could create a dictionary to pass the arguments:

        .. only:: latex

            >>> dt_params = dict(algorithm='c45',
                                 model_format='json',
                                 min_records_of_parent=2,
                                 min_records_of_leaf=1,
                                 thread_ratio=0.4,
                                 resampling_method='cv',
                                 evaluation_metric='auc',
                                 fold_num=5,
                                 progress_indicator_id='CV',
                                 param_search_strategy='grid',
                                 param_values=dict(split_threshold=[1e-3 , 1e-4, 1e-5]))
            >>> uni_dt = UnifiedClassification(func='DecisionTree', **dt_params)

        .. raw:: html

            <iframe allowtransparency="true" style="border:1px solid #ccc; background: #eeffcb;"
                src="../../_static/uc_init_example3.html" width="100%" height="100%" sandbox="">
            </iframe>

        or use the following line instead as a whole:

        .. only:: latex

            >>> uni_dt = UnifiedClassification(func='DecisionTree',
                                               algorithm='c45',
                                               model_format='json',
                                               min_records_of_parent=2,
                                               min_records_of_leaf=1,
                                               thread_ratio=0.4,
                                               resampling_method='cv',
                                               evaluation_metric='auc',
                                               fold_num=5,
                                               progress_indicator_id='CV',
                                               param_search_strategy='grid',
                                               param_values=dict(split_threshold=[1e-3 , 1e-4, 1e-5]))

        .. raw:: html

            <iframe allowtransparency="true" style="border:1px solid #ccc; background: #eeffcb;"
                src="../../_static/uc_init_example4.html" width="100%" height="100%" sandbox="">
            </iframe>

    Attributes
    ----------

    model_ : list of DataFrames.

        Model content.

    importance_ : DataFrame

        The feature importance (the higher, the more important the feature).

    statistics_ : DataFrame

        Names and values of statistics.

    optimal_param_ : DataFrame

        Provides optimal parameters selected.

        Available only when parameter selection is triggered.

    confusion_matrix_ : DataFrame

        Confusion matrix used to evaluate the performance of classification
        algorithms.

    metrics_ : DataFrame

        Value of metrics.

    partition_ : DataFrame

        Type of partition.

    error_msg_ : DataFrame

        Error message.
        Only valid if ``massive`` is True when initializing an 'UnifiedClassification' instance.

    Examples
    --------
    Case 1: Assume the training DataFrame is df_fit, data for prediction is df_predict and for score is df_score.

    Train the model:

    >>> rdt_params = dict(random_state=2,
                          split_threshold=1e-7,
                          min_samples_leaf=1,
                          n_estimators=10,
                          max_depth=55)

    >>> uc_rdt = UnifiedClassification(func = 'RandomDecisionTree', **rdt_params)

    >>> uc_rdt.fit(data=df_fit,
                   partition_method='stratified',
                   stratified_column='CLASS',
                   partition_random_state=2,
                   training_percent=0.7,
                   ntiles=2)

    Output:

    >>> uc_rdt.importance_.collect().set_index('VARIABLE_NAME')
       VARIABLE_NAME  IMPORTANCE
    0        OUTLOOK    0.203566
    1           TEMP    0.479270
    2       HUMIDITY    0.317164
    3          WINDY    0.000000

    Prediction:

    >>> res = uc_rdt.predict(data=df_predict, key = "ID")[['ID', 'SCORE', 'CONFIDENCE']].collect()
      ID  SCORE  CONFIDENCE
    0  0   Play         1.0
    1  1   Play         0.8
    2  2   Play         0.7
    3  3   Play         0.9
    4  4   Play         0.8
    5  5   Play         0.8
    6  6   Play         0.9

    Score:

    >>> score_res = uc_rdt.score(data=df_score,
                                 key='ID',
                                 max_result_num=2,
                                 ntiles=2)[1]
    >>> score_res.head(4).collect()
       STAT_NAME         STAT_VALUE   CLASS_NAME
    0        AUC  0.673469387755102         None
    1     RECALL                  0  Do not Play
    2  PRECISION                  0  Do not Play
    3   F1_SCORE                  0  Do not Play


    Case 2: UnifiedReport for UnifiedClassification is shown as follows:

    >>> from hana_ml.algorithms.pal.model_selection import GridSearchCV
    >>> hgc = UnifiedClassification('HybridGradientBoostingTree')
    >>> gscv = GridSearchCV(estimator=hgc,
                            param_grid={'learning_rate': [0.1, 0.4, 0.7, 1],
                                        'n_estimators': [4, 6, 8, 10],
                                        'split_threshold': [0.1, 0.4, 0.7, 1]},
                            train_control=dict(fold_num=5,
                                               resampling_method='cv',
                                               random_state=1,
                                               ref_metric=['auc']),
                            scoring='error_rate')
    >>> gscv.fit(data=df_train,
                 key= 'ID',
                 label='CLASS',
                 partition_method='stratified',
                 partition_random_state=1,
                 stratified_column='CLASS',
                 build_report=True)

    To look at the dataset report:

    >>> UnifiedReport(data=df_train).build().display()

     .. image:: ../../image/unified_report_dataset_report.png

    To see the model report:

    >>> UnifiedReport(gscv.estimator).display()

     .. image:: ../../image/unified_report_model_report_classification.png

    To see the Optimal Parameter page:

     .. image:: ../../image/unified_report_model_report_classification2.png

    Case 3: Local interpretability of models - tree SHAP

    >>> uhgc = UnifiedClassification(func='HybridGradientBoostingTree')# HGBT model
    >>> uhgc.fit(data=df_train) # do not need any background data for tree models
    >>> res = uhgc.predict(data=df_predict,
    ...                    ...,
    ...                    attribution_method='tree-shap',# specify the attribution method to activate local interpretability
    ...                    top_k_attributions=5)

    Case 4: Local interpretability of models - kernel SHAP

    >>> unb = UnifiedClassification(func='NaiveBayes')# Naive Bayes model
    >>> unb.fit(data=df_train,
    ...         background_size=10,# specify non-zero background data size to activate local intepretability
    ...         background_random_state=2022)
    >>> res = unb.predict(data=df_predict,
    ...                   ...,
    ...                   top_k_attributions=4,
    ...                   sample_size=0,
    ...                   random_state=2022)
    """
    func_dict = {'decisiontree' : 'DT',
                 'logisticregression' : 'LOGR',
                 'multiclass-logisticregression' : 'M_LOGR',
                 'hybridgradientboostingtree' : 'HGBT',
                 'mlp' : 'MLP',
                 'naivebayes' : 'NB',
                 'randomforest' : 'RDT',
                 'randomdecisiontree' : 'RDT',
                 'svm' : 'SVM',
                 'mlp_multitask' : 'MLP_M_TASK'}
    __base_resampling_methods = ['cv', 'bootstrap', 'stratified_cv', 'stratified_bootstrap']
    __all_resampling_methods = __base_resampling_methods + [x + '_sha' for x in __base_resampling_methods] +\
    [x + '_hyperband' for x in __base_resampling_methods]
    __cv_dict = {'resampling_method' : ('RESAMPLING_METHOD', str, {x : x for x in __all_resampling_methods}),
                 'evaluation_metric' : ('EVALUATION_METRIC', str),
                 'metric' : ('EVALUATION_METRIC', str),
                 'random_state' : ('SEED', int),
                 'fold_num' : ('FOLD_NUM', int),
                 'repeat_times' : ('REPEAT_TIMES', int),
                 'search_strategy' : ('PARAM_SEARCH_STRATEGY', str, {'random' : 'random', 'grid' : 'grid'}),
                 'param_search_strategy' : ('PARAM_SEARCH_STRATEGY', str, {'random' : 'random', 'grid' : 'grid'}),
                 'random_search_times' : ('RANDOM_SEARCH_TIMES', int),
                 'timeout' : ('TIMEOUT', int),
                 'progress_indicator_id' : ('PROGRESS_INDICATOR_ID', str),
                 'param_values' : ('_VALUES', dict),
                 'param_range' : ('_RANGE', dict)}
    __activation_map = {'tanh' : 1,
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
                        'relu' : 13}
    __fit_add_dict = {
        'DT' : {'bins' : ('_BIN_', ListOfTuples),
                'priors' : ('_PRIOR_', ListOfTuples)},
        'RDT' : {'strata' : ('STRATA', ListOfTuples),
                 'priors' : ('PRIOR', ListOfTuples)},
        'HGBT' : {'scale_weight' : ('SCALE_WEIGHT', float),
                  'scale_weight_target' : ('SCALE_WEIGHT_TARGET', (str, int))},
        'LOGR' : {'class_map0' : ('CLASS_MAP0', str),
                  'class_map1' : ('CLASS_MAP1', str)},
        'MLP' : {},
        'NB' : {},
        'SVM' : {},
        'MLP_M_TASK' : {}}
    __fit_param_dict = {
        'partition_method' : ('PARTITION_METHOD', int, {'no' : 0,
                                                        'user_defined' : 1,
                                                        'predefined' : 1,
                                                        'stratified' : 2}),
        'partition_random_state' : ('PARTITION_RANDOM_SEED', int),
        'stratified_column' : ('PARTITION_STRATIFIED_VARIABLE', str),
        'training_percent' : ('PARTITION_TRAINING_PERCENT', float),
        'training_size' : ('PARTITION_TRAINING_SIZE', int),
        'output_partition_result' : ('OUTPUT_PARTITION_RESULT', bool),
        'background_size' : ('BACKGROUND_SIZE', int),
        'background_random_state' : ('BACKGROUND_SAMPLING_SEED', int),
        'ntiles' : ('NTILES', int)}
    __impute_dict = {
        'impute' : ('HANDLE_MISSING_VALUE', bool),
        'strategy' : ('IMPUTATION_TYPE', int, {'non' : 0, 'most_frequent-mean' : 1, 'most_frequent-median' : 2, 'most_frequent-zero' : 3, 'most_frequent-als' : 4, 'delete' : 5}),
        'als_factors' : ('ALS_FACTOR_NUMBER', int),
        'als_lambda' : ('ALS_REGULARIZATION', float),
        'als_maxit' : ('ALS_MAX_ITERATION', int),
        'als_randomstate' : ('ALS_SEED', int),
        'als_exit_threshold' : ('ALS_EXIT_THRESHOLD', float),
        'als_exit_interval' : ('ALS_EXIT_INTERVAL', int),
        'als_linsolver' : ('ALS_LINEAR_SYSTEM_SOLVER', int, {'cholsky' : 0, 'cg' : 1, 'cholesky' : 0}),
        'als_cg_maxit' : ('ALS_CG_MAX_ITERATION', int),
        'als_centering' : ('ALS_CENTERING', bool),
        'als_scaling' : ('ALS_SCALING', bool)}
    __permutation_imp_dict = {
        'permutation_importance' : ('PERMUTATION_IMPORTANCE', bool),
        'permutation_evaluation_metric' : ('PERMUTATION_EVALUATION_METRIC', str,
                                           {x:x.upper() for x in ['accuracy', 'auc', 'kappa', 'mcc']}),
        'permutation_n_repeats' : ('PERMUTATION_N_REPEATS', int),
        'permutation_seed' : ('PERMUTATION_SEED', int),
        'permutation_n_samples' : ('PERMUTATION_N_SAMPLES', int)}
    __predict_score_param_dict = {
        'thread_ratio' : ('THREAD_RATIO', float),
        'verbose' : ('VERBOSE', bool),
        'verbose_top_n' : ('VERBOSE_TOP_N', int),
        'class_map0' : ('CLASS_MAP0', str),
        'class_map1' : ('CLASS_MAP1', str),
        'alpha' : ('LAPLACE', float),
        'block_size' : ('BLOCK_SIZE', int),
        'missing_replacement' : ('MISSING_REPLACEMENT', int, {'feature_marginalized' : 1, 'instance_marginalized' : 2}),
        'top_k_attributions' : ('TOP_K_ATTRIBUTIONS', int),
        'attribution_method' : ('FEATURE_ATTRIBUTION_METHOD', int, {'no' : 0, 'saabas' : 1, 'tree-shap' : 2}),
        'sample_size' : ('SAMPLESIZE', int),
        'random_state' : ('SEED', int),
        'max_result_num' : ('MAX_RESULT_NUM', int),
        'ntiles' : ('NTILES', int),
        'ignore_unknown_category' : ('IGNORE_UNKNOWN_CATEGORY', bool),
        'positive_label' : ('POSITIVE_LABEL', str),
        'shap_value_for_positive_only' : ('SHAP_VALUE_FOR_POSITIVE_ONLY', bool)}

    map_dict = {
        'DT' : {
            'algorithm' : ('ALGORITHM', int, {'c45' : 1, 'chaid' : 2, 'cart' : 3}),
            'allow_missing_dependent' : ('ALLOW_MISSING_LABEL', bool),
            'percentage' : ('PERCENTAGE', float),
            'min_records_of_parent' : ('MIN_RECORDS_PARENT', int),
            'min_records_of_leaf' : ('MIN_RECORDS_LEAF', int),
            'max_depth' : ('MAX_DEPTH', int),
            'split_threshold' : ('SPLIT_THRESHOLD', float),
            'discretization_type' : ('DISCRETIZATION_TYPE', int, {'mdlpc' : 0, 'equal_freq' : 1}),
            #'bins' : ('_BIN_', ListOfTuples),
            'max_branch' : ('MAX_BRANCH', int),
            'merge_threshold' : ('MERGE_THRESHOLD', float),
            'use_surrogate' : ('USE_SURROGATE', bool),
            'model_format' : ('MODEL_FORMAT', int, {'json' : 1, 'pmml' : 2}),
            #'prior' : ('_PRIOR_', ListOfTuples),
            'thread_ratio' : ('THREAD_RATIO', float)},
        'RDT' : {
            'n_estimators' : ('N_ESTIMATORS', int),
            'max_features' : ('MAX_FEATURES', int),
            'min_samples_leaf' : ('MIN_SAMPLES_LEAF', int),
            'max_depth' : ('MAX_DEPTH', int),
            'split_threshold' : ('SPLIT_THRESHOLD', float),
            'random_state' : ('SEED', int),
            'thread_ratio' : ('THREAD_RATIO', float),
            'allow_missing_dependent' : ('ALLOW_MISSING_LABEL', bool),
            'sample_fraction' : ('SAMPLE_FRACTION', float),
            'compression' : ('COMPRESSION', bool),
            'max_bits' : ('MAX_BITS', int),
            'quantize_rate' : ('QUANTIZE_RATE', float),
            'model_format' : ('MODEL_FORMAT', int, {'json' : 1, 'pmml' : 2})},
        'HGBT' : {
            'n_estimators' : ('N_ESTIMATORS', int),
            'random_state' : ('SEED', int),
            'subsample' : ('SUBSAMPLE', float),
            'max_depth' : ('MAX_DEPTH', int),
            'split_threshold' : ('SPLIT_THRESHOLD', float),
            'learning_rate' : ('LEARNING_RATE', float),
            'split_method' : ('SPLIT_METHOD', str, {'exact' : 'exact',
                                                    'sketch' : 'sketch',
                                                    'sampling' : 'sampling',
                                                    'histogram' : 'histogram'}),
            'sketch_eps' : ('SKETCH_ESP', float),
            'min_sample_weight_leaf' : ('MIN_SAMPLES_WEIGHT_LEAF', float),
            'ref_metric' : ('REF_METRIC', ListOfStrings),
            'min_samples_leaf' : ('MIN_SAMPLES_LEAF', int),
            'max_w_in_split' : ('MAX_W_IN_SPLIT', float),
            'col_subsample_split' : ('COL_SUBSAMPLE_SPLIT', float),
            'col_subsample_tree' : ('COL_SUBSAMPLE_TREE', float),
            'lamb' : ('LAMB', float),
            'alpha' : ('ALPHA', float),
            'base_score' : ('BASE_SCORE', float),
            'adopt_prior' : ('START_FROM_AVERAGE', bool),
            'replace_missing' : ('REPLACE_MISSING', bool),
            'obj_func' : ('OBJ_FUNC', int, {'logistic': 5, 'hinge': 6, 'softmax': 7}),
            'default_missing_direction' : ('DEFAULT_MISSING_DIRECTION', int, {'left' : 0, 'right' : 1}),
            'feature_grouping' : ('FEATURE_GROUPING', bool),
            'tol_rate' : ('TOLERANT_RATE', float),
            'compression' : ('COMPRESSION', bool),
            'max_bits' : ('MAX_BITS', int),
            'max_bin_num' : ('MAX_BIN_NUM', int),
            'thread_ratio' : ('THREAD_RATIO', float),
            'reduction_rate' : ('REDUCTION_RATE', float),
            'resource' : ('RESOURCE', str, {'data_size' : None, 'n_estimators' : 'ITER_NUM'}),
            'min_resource_rate' : ('MIN_RESOURCE_RATE', float),
            'max_resource' : ('MAX_RESOURCE', int),
            'aggressive_elimination' : ('AGGRESSIVE_ELIMINATION', bool),
            'validation_set_rate' : ('VALIDATION_SET_RATE', float),
            'stratified_validation_set' : ('STRATIFIED_VALIDATION_SET', bool),
            'tolerant_iter_num' : ('TOLERANT_ITER_NUM', int),
            'fg_min_zero_rate' : ('FG_MIN_ZERO_RATE', float),
            'validation_set_metric' : ('VALIDATION_SET_METRIC', str),
            'model_tree' : ('MODEL_TREE', int, {'constant' : 0, 'linear' : 1}),
            'linear_lambda' : ('LINEAR_LAMBDA', float),
            'use_vec_leaf' : ('USE_VEC_LEAF', bool)},
        'LOGR' : {
            'multi_class' : ('M_', bool),
            'max_iter' : ('MAX_ITER', int),
            'enet_alpha' : ('ALPHA', float),
            'enet_lambda' : ('LAMB', float),
            'tol' : ('TOL', float),
            'solver' : ('METHOD', int, {'auto' : -1, 'newton' : 0, 'cyclical' : 2,
                                        'lbfgs' : 3, 'stochastic' : 4, 'proximal' : 6}),
            'epsilon' : ('EPSILON', float),
            'standardize' : ('STANDARDIZE', bool),
            'max_pass_number' : ('MAX_PASS_NUMBER', int),
            'sgd_batch_number' : ('SGD_BATCH_NUMBER', int),
            'precompute' : ('PRECOMPUTE', bool),
            'handle_missing' : ('HANDLE_MISSING', bool),
            'lbfgs_m' : ('LBFGS_M', int),
            'stat_inf': ('STAT_INF', bool),
            'json_export' : ('JSON_EXPORT', bool),
            'resource': ('RESOURCE', str,
                         {'max_iter': 'MAX_ITERATION',
                          'max_pass_number': 'MAX_PASS_NUMBER',
                          'automatic': 'MAX_ITERATION'}),
            'max_resource': ('MAX_RESOURCE', int),
            'min_resource_rate': ('MIN_RESOURCE_RATE', float),
            'reduction_rate': ('REDUCTION_RATE', float),
            'aggressive_elimination': ('AGGRESSIVE_ELIMINATION', bool),
            'ps_verbose': ('PS_VERBOSE', bool)},
        'MLP' : {
            'activation' : ('ACTIVATION',
                            int,
                            __activation_map),
            'activation_options' : ('ACTIVATION_OPTIONS', ListOfStrings),
            'output_activation' : ('OUTPUT_ACTIVATION',
                                   int,
                                   __activation_map),
            'output_activation_options' : ('OUTPUT_ACTIVATION_OPTIONS', ListOfStrings),
            'hidden_layer_size' : ('HIDDEN_LAYER_SIZE', (tuple, list)),
            'hidden_layer_size_options' : ('HIDDEN_LAYER_SIZE_OPTIONS', ListOfTuples),
            'max_iter' : ('MAX_ITER', int),
            'training_style' : ('TRAINING_STYLE', int, {'batch' : 0, 'stochastic' : 1}),
            'learning_rate' : ('LEARNING_RATE', float),
            'momentum' : ('MOMENTUM', float),
            'batch_size' : ('BATCH_SIZE', int),
            'normalization' : ('NORMALIZATION', int, {'no' : 0, 'z-transform' : 1, 'scalar' : 2}),
            'weight_init' : ('WEIGHT_INIT',
                             int,
                             {'all-zeros' : 0,
                              'normal' : 1,
                              'uniform' : 2,
                              'variance-scale-normal' : 3,
                              'variance-scale-uniform' : 4}),
            'thread_ratio' : ('THREAD_RATIO', float),
            'reduction_rate' : ('REDUCTION_RATE', float),
            'aggressive_elimination' : ('AGGRESSIVE_ELIMINATION', bool)},
        'NB' : {
            'alpha' : ('ALPHA', float),
            'discretization' : ('DISCRETIZATION', int, {'no' : 0, 'supervised' : 1}),
            'model_format' : ('MODEL_FORMAT', int, {'json' : 0, 'pmml' : 1}),
            'thread_ratio' : ('THREAD_RATIO', float)},
        'SVM' : {
            'c' : ('SVM_C', float),
            'kernel' : ('KERNEL_TYPE', int, {'linear' : 0, 'poly' : 1, 'rbf' : 2, 'sigmoid' : 3}),
            'degree' : ('POLY_DEGREE', int),
            'gamma' : ('RBF_GAMMA', float),
            'coef_lin' : ('COEF_LIN', float),
            'coef_const' : ('COEF_CONST', float),
            'probability' : ('PROBABILITY', bool),
            'shrink' : ('SHRINK', bool),
            'tol' : ('TOL', float),
            'evaluation_seed' : ('EVALUATION_SEED', int),
            'scale_info' : ('SCALE_INFO', int, {'no' : 0, 'standardization' : 1, 'rescale' : 2}),
            'handle_missing' : ('HANDLE_MISSING', bool),
            'category_weight' : ('CATEGORY_WEIGHT', float),
            'compression' : ('COMPRESSION', bool),
            'max_bits' : ('MAX_BITS', int),
            'max_quantization_iter' : ('MAX_QUANTIZATION_ITER', int),
            'reduction_rate' : ('REDUCTION_RATE', float),
            'aggressive_elimination' : ('AGGRESSIVE_ELIMINATION', bool)},
        'MLP_M_TASK' : {'hidden_layer_size' : ('HIDDEN_LAYER_SIZE', (list, tuple)),
                        'activation' : ('ACTIVATION', int, {'sigmoid' : 0, 'tanh' : 1, 'relu' : 2,
                                                            'leaky-relu' : 3, 'elu' : 4,
                                                            'gelu' : 5}),

                        'batch_size' : ('BATCH_SIZE', int),
                        'num_epochs' : ('MAX_ITER', int),
                        'random_state' : ('SEED', int),
                        'use_batchnorm'  : ('USE_BATCHNORM', bool),
                        'learning_rate' : ('LEARNING_RATE', float),
                        'optimizer' : ('OPTIMIZER', int, {'sgd' : 0, 'rmsprop' : 1,  'adam': 2, 'adagrad': 3}),
                        'dropout_prob' : ('DROPOUT_PROB', float),
                        'training_percentage' : ('TRAINING_PERCENTAGE', float),
                        'early_stop'  : ('EARLY_STOP', bool),
                        'normalization' : ('NORMALIZATION',  int, {None: 0, 'no': 0, 'z-transform': 1, 'scalar': 2}),
                        'warmup_epochs'  : ('WARMUP_EPOCHS', int),
                        'patience' : ('PATIENCE', int),
                        'save_best_model' : ('SAVE_BEST_MODEL', bool),
                        'training_style' : ('TRAINING_STYLE', int, {'batch' : 0, 'stochastic' : 1}),
                        'network_type'  : ('NETWORK_TYPE', int, {'basic' : 0, 'resnet' : 1}),
                        'embedded_num'  : ('EMBEDDED_NUM', int),
                        'residual_num' :  ('RESIDUAL_NUM', int)}
    }
    __overall_imputation_map = {'non' : 0, 'delete' : 5,
                                'most_frequent-mean' : 1, 'mean' : 1,
                                'most_frequent-median' : 2, 'median' : 2,
                                'most_frequent-zero' : 3, 'zero' : 3,
                                'most_frequent-als' : 4, 'als' : 4}
    __column_imputation_map = {'non' : 0, 'delete' : 1,
                               'most_frequent' : 100,
                               'categorical_const' : 101,
                               'mean' : 200, 'median' : 201,
                               'numerical_const' : 203,
                               'als' : 204}
    pal_funcname = 'PAL_UNIFIED_CLASSIFICATION'

    def __init__(self,#pylint:disable=too-many-positional-arguments
                 func,
                 multi_class=None,
                 massive=False,
                 group_params=None,
                 pivoted=False,
                 **kwargs):
        setattr(self, 'hanaml_parameters', pal_param_register())
        PALBase.__init__(self)
        _UnifiedClassificationReportBuilder.__init__(self, ["KEY", "VALUE"], ["KEY", "VALUE"])
        AMDPHelper.__init__(self)
        self.func = self._arg('Function name', func, self.func_dict)
        self.params = {**kwargs}
        if self.func == 'LOGR' and multi_class is True:
            self.real_func = 'M_LOGR'
        else:
            self.real_func = self.func

        # for massive mode
        self.massive = self._arg('massive', massive, bool)
        group_params = self._arg('group_params', group_params, dict)
        group_params = {} if group_params is None else group_params
        if group_params:
            for group in group_params:
                self._arg(self.func + ' Parameters with group_key ' + str(group),
                          group_params[group], dict)

        self.__pal_params = {}
        func_map = dict(self.map_dict[self.func], **self.__cv_dict)

        if self.massive is not True:
            self.__pal_params = _params_check(input_dict=self.params, param_map=func_map, func=func)
            self.__pal_params = _mlogr_solver_number_update(input_dict=self.__pal_params, func=self.real_func)
        else: # massive mode
            self.group_params = group_params
            if self.group_params:
                for group in self.group_params:
                    self.__pal_params[group] = {}
                    self.__pal_params[group] = _params_check(input_dict=self.group_params[group],
                                                             param_map=func_map,
                                                             func=func)
                    self.__pal_params[group] = _mlogr_solver_number_update(input_dict=self.__pal_params[group],
                                                                           func=self.real_func)
            if self.params:
                special_group_name = 'PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID'
                self.__pal_params[special_group_name] = _params_check(input_dict=self.params,
                                                                      param_map=func_map,
                                                                      func=func)
                self.__pal_params[special_group_name] = _mlogr_solver_number_update(input_dict=self.__pal_params[special_group_name],
                                                                                    func=self.real_func)
        self.pivoted = self._arg('pivoted', pivoted, bool)
        self.model_ = None
        self.scoring_list_ = None
        self.importance_ = None
        self.statistics_ = None
        self.optimal_param_ = None
        self.confusion_matrix_ = None
        self.matrics_ = None
        self.partition_ = None
        self.__param_rows = None
        self._is_autologging = False
        self._autologging_model_storage_schema = None
        self._autologging_model_storage_meta = None
        self.auto_metric_sampling = False
        self.is_exported = False

    def disable_mlflow_autologging(self):
        """
        It will disable mlflow autologging.
        """
        self._is_autologging = False

    def enable_mlflow_autologging(self, schema=None, meta=None, is_exported=False):
        """
        It will enable mlflow autologging.

        Parameters
        ----------
        schema : str, optional
            Define the model storage schema for mlflow autologging.

            Defaults to the current schema.
        meta : str, optional
            Define the model storage meta table for mlflow autologging.

            Defaults to 'HANAML_MLFLOW_MODEL_STORAGE'.
        is_exported : bool, optional
            Determine whether export the HANA model to mlflow.

            Defaults to False.
        """
        self._is_autologging = True
        self._autologging_model_storage_schema = schema
        self._autologging_model_storage_meta = meta
        self.is_exported = is_exported

    def update_cv_params(self, name, value, typ):
        """
        Update parameters for model-evaluation/parameter-selection.
        """
        if name in self.__cv_dict.keys():
            self.__pal_params[self.__cv_dict[name][0]] = (value, typ)

    def __map_param(self, name, value, typ,
                    label_type='NVARCHAR'):
        tpl = ()
        if typ in [int, bool]:
            tpl = (name, value, None, None)
        elif typ == float:
            tpl = (name, None, value, None)
        elif typ in [str, ListOfStrings]:
            if '_METRIC' in name:#evaluation_metric, ref_metric, validation_set_metric
                if value.lower().startswith('f1'):
                    value = 'F1_SCORE' + (value[8:] if len(value) > 8 else '')# for compatibility of HGBT and LogR
                elif any(mtc in value.lower() for mtc in ['recall', 'precision']):
                    value_split = value.split('_')
                    value = value_split[0].upper() + '_' + value_split[1]
                else:
                    value = value.upper()
            tpl = (name, None, None, value)
        else:
            if self.func == 'RDT':
                if label_type in ['VARCHAR', 'NVARCHAR']:
                    tpl = (name, None, value[1], value[0])
                else:
                    tpl = (name, value[0], value[1], None)
            elif self.func == 'DT':
                if name == '_BIN_':
                    tpl = (str(value[0])+name, value[1], None, None)
                else:
                    tpl = (str(value[0])+name, None, value[1], None)
        return tpl

    @mlflow_autologging(logtype='pal_fit')
    @trace_sql
    def fit(self,#pylint: disable=too-many-branches, too-many-statements, too-many-positional-arguments
            data,
            key=None,
            features=None,
            label=None,
            group_key=None,
            group_params=None,
            purpose=None,
            partition_method=None,
            stratified_column=None,
            partition_random_state=None,
            training_percent=None,
            training_size=None,
            ntiles=None,
            categorical_variable=None,
            output_partition_result=None,
            background_size=None,
            background_random_state=None,
            build_report=False,
            impute=False,
            strategy=None,
            strategy_by_col=None,
            als_factors=None,
            als_lambda=None,
            als_maxit=None,
            als_randomstate=None,
            als_exit_threshold=None,
            als_exit_interval=None,
            als_linsolver=None,
            als_cg_maxit=None,
            als_centering=None,
            als_scaling=None,
            meta_data=None,
            permutation_importance=None,
            permutation_evaluation_metric=None,
            permutation_n_repeats=None,
            permutation_seed=None,
            permutation_n_samples=None,
            **kwargs):
        r"""
        Fit function for unified classification.

        Parameters
        ----------

        data : DataFrame
            DataFrame that contains the training data.

            If the corresponding UnifiedClassification instance is for pivoted input data(i.e.
            setting ``pivoted`` = True in initialization), then ``data`` must be pivoted such that:

            - in `massive` mode, ``data`` must be structured as follows:

              - 1st column: Group ID, type INTEGER, VARCHAR or NVARCHAR
              - 2nd column: Record ID, type INTEGER, VARCHAR or NVARCHAR
              - 3rd column: Variable Name, type VARCHAR or NVARCHAR
              - 4th column: Variable Value, type VARCHAR or NVARCHAR
              - 5th column: Self-defined Data Partition, type INTEGER, 1 for training and 2 for validation.


            - in `non-massive` mode, ``data`` must be structured as follows:

              - 1st column: Record ID, type INTEGER, VARCHAR or NVARCHAR
              - 2nd column: Variable Name, type VARCHAR or NVARCHAR
              - 3rd column: Variable Value, type VARCHAR or NVARCHAR
              - 4th column: Self-defined Data Partition, type INTEGER, 1 for training and 2 for validation.

            .. note::

              If ``data`` is pivoted, then the following parameters become ineffective: ``key``, ``features``,
              ``label``, ``group_key`` and ``purpose``.

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
            If ``label`` is not provided, it defaults to the last non-ID column.

        group_key : str, optional
            The column of group_key. Data type can be INT or NVARCHAR/VARCHAR.
            If data type is INT, only parameters set in the group_params are valid.

            This parameter is only valid when ``massive`` is True in class instance initialization.

            Defaults to the first column of data if the index columns of data is not provided.
            Otherwise, defaults to the first column of index columns.

        group_params : dict, optional
            If massive mode is activated (``massive`` is set as True in class instance initialization),
            input data for classification shall be divided into different
            groups with different classification parameters applied. This parameter specifies the parameter
            values of the chosen classification algorithm ``func`` in fit() w.r.t. different groups in a dict format,
            where keys corresponding to ``group_key`` while values should be a dict for classification algorithm
            parameter value assignments.

            An example is as follows:

            .. only:: latex

                >>> uc = UnifiedClassification(func='logisticregression',
                                               multi_class=True,
                                               massive=True,
                                               max_iter=10,
                                               group_params={'Group_1': {'solver': 'auto'}})
                >>> uc.fit(data=df,
                           key='ID',
                           features=["OUTLOOK" ,"TEMP", "HUMIDITY","WINDY"],
                           label="CLASS",
                           group_key="GROUP_ID",
                           background_size=4,
                           group_params={'Group_1':{'background_random_state':2}})

            .. raw:: html

                <iframe allowtransparency="true" style="border:1px solid #ccc; background: #eeffcb;"
                    src="../../_static/uc_fit_example.html" width="100%" height="100%" sandbox="">
                </iframe>

            Valid only when ``massive`` is set as True in class instance initialization.

            Defaults to None.

        purpose : str, optional
            Indicates the name of purpose column which is used for user self-defined data partition.

            The meaning of value in the column for each data instance is shown below:

            - 1 : training
            - 2 : validation

            Valid and mandatory only when ``partition_method`` is 'predefined'(or equivalently, 'user_defined').

            No default value.

        partition_method : {'no', 'predefined', 'stratified'}, optional
            Defines the way to divide the dataset.

            - 'no' : no partition.
            - 'predefined'/'user_defined' : predefined partition.
            - 'stratified' : stratified partition.

            Defaults to 'no'.
        stratified_column : str, optional
            Indicates which column is used for stratification.

            Valid only when ``partition_method`` is set to 'stratified'.

            No default value.
        partition_random_state : int, optional
            Indicates the seed used to initialize the random number generator.

            Valid only when ``partition_method`` is set to 'stratified'.

            - 0 : Uses the system time.
            - Not 0 : Uses the specified seed.

            Defaults to 0.
        training_percent : float, optional
            The percentage of data used for training.
            Value range: 0 <= value <= 1.

            Defaults to 0.8.
        training_size : int, optional
            Row size of data used for training. Value range >= 0.

            If both ``training_percent`` and ``training_size`` are specified, ``training_percent`` takes precedence.

            No default value.
        ntiles : int, optional
            Used to control the population tiles in metrics output.
            The validation value should be at least 1 and no larger than the row size of the validation data.
            For AUC, this parameter means the maximum tiles.

            The value should be at least 1 and no larger than the row size of the input data

            If the row size of data for metrics evaluation is less than 20,
            the default value is 1; otherwise it is 20.

        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.
        output_partition_result : bool, optional
            Specifies whether or not to output the partition result.

            Valid only when ``partition_method`` is not 'no', and ``key`` is not None.

            Defaults to False.

        background_size : int, optional
            Specifies the size of background data used for SHapley Additive exPlanations(SHAP) values calculation.

            Should not larger than the size of training data.

            Valid only for Naive Bayes, Support Vector Machine, or Multilayer Perceptron and Multi-class
            Logistic Regression models. For such models, users should specify a non-zero value to activate
            SHAP explanations in model scoring(i.e. predict() or score()) phase.

            Defaults to 0(no background data, in which case the calculation of SHAP values shall be disabled).

            .. note::
                SHAP is a method for local interpretability of models, please see :ref:`local_interpretability-label`
                for more details.

        background_random_state : int, optional
            Specifies the seed for random number generator in the background data sampling.

            - 0 : Uses current time as seed
            - Others : The specified seed value

            Valid only for Naive Bayes, Support Vector Machine, or Multilayer Perceptron and Multi-class
            Logistic Regression models.

            Defaults to 0.

        build_report : bool, optional
            Whether to build a model report or not.

            Example:

            >>> from hana_ml.visualizers.unified_report import UnifiedReport
            >>> hgc = UnifiedClassification('HybridGradientBoostingTree')
            >>> hgc.fit(data=diabetes_train, key= 'ID', label='CLASS',
                        partition_method='stratified', partition_random_state=1,
                        stratified_column='CLASS', build_report=True)
            >>> UnifiedReport(hgc).display()

            Defaults to False.

        impute : bool, optional
            Specifies whether or not to handle missing values in the data for training.

            Defaults to False.

        strategy, strategy_by_col, als_* : parameters for missing value handling, optional
            All these parameters mentioned above are for handling missing values
            in data, please see :ref:`impute_params-label` for more details.

            All parameters are valid only when ``impute`` is set as True.

        meta_data : DataFrame, optional
            Specifies the meta data for pivoted input data. Mandatory if ``pivoted`` is specified as True
            in initializing the class instance.

            If provided, then ``meta_data`` should be structured as follows:

            - 1st column: NAME, type VRACHAR or NVARCHAR. The name of the variable.
            - 2nd column: TYPE, VRACHAR or NVARCHAR. The type of the variable, can be CONTINUOUS, CATEGORICAL or TARGET.

        permutation_* : parameter for permutation feature importance, optional
            All parameters with prefix 'permutation\_' are for the calculation of permutation
            feature importance.

            They are valid only when ``partition_method`` is specified as 'predefined' or 'stratified',
            since permuation feature importance is calculated on the validation set.

            Please see :ref:`permutation_imp-label` for more details.

        **kwargs : keyword arguments
            Additional keyword arguments of model fitting for different classification algorithms.

            Please referred to the fit function of each algorithm as follows:

            - **'DecisionTree'** : :class:`~hana_ml.algorithms.pal.trees.DecisionTreeClassifier`
            - **'HybridGradientBoostingTree'** : :class:`~hana_ml.algorithms.pal.trees.HybridGradientBoostingClassifier`
            - **'LogisticRegression'** :class:`~hana_ml.algorithms.pal.linear_model.LogisticRegression`
            - **'MLP'** : :class:`~hana_ml.algorithms.pal.neural_network.MLPClassifier`
            - **'NaiveBayes'** : :class:`~hana_ml.algorithms.pal.naive_bayes.NaiveBayes`
            - **'RandomDecisionTree'** : :class:`~hana_ml.algorithms.pal.trees.RDTClassifier`
            - **'SVM'** : :class:`~hana_ml.algorithms.pal.svm.SVC`

        Returns
        -------
        A fitted object of class "UnifiedClassification".
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        conn = data.connection_context
        require_pal_usable(conn)
        if self.pivoted and meta_data is None:
            msg = "meta_data must be given when `pivoted`=True."
            logger.error(msg)
            raise ValueError(msg)
        group_params = self._arg('group_params', group_params, dict)
        if group_params:
            for group in group_params:
                self._arg(self.func + ' Parameters with group_key ' + str(group),
                          group_params[group], dict)

        if partition_method == "stratified" and stratified_column is None:
            msg = "Please select stratified_column when you use stratified partition method!"
            logger.error(msg)
            raise ValueError(msg)

        cols = data.columns
        index = data.index
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
                msg = "Please select group_key from {}!".format(cols)
                logger.error(msg)
                raise ValueError(msg)
            data_groups = list(data[[group_key]].collect()[group_key].drop_duplicates())
            param_keys = list(self.group_params.keys())
            gid_type = {tp[0]:tp for tp in data.dtypes()}[group_key]
            if not self._disable_hana_execution:
                if data_groups and not all([(int(ky) if 'INT' in gid_type[1] else ky) in data_groups for ky in param_keys]):
                    msg = 'Invalid group key identified in group parameters!'
                    logger.error(msg)
                    raise ValueError(msg)
            group_key_type = "VARCHAR(5000)"
            group_id = [group_key]
            cols.remove(group_key)

        key = self._arg('key', key, str)
        if index is not None:
            key = _key_index_check(key, 'key', index[1] if self.massive else index)
        if key is not None and key not in cols:
            msg = "Please select key from {}!".format(cols)
            logger.error(msg)
            raise ValueError(msg)
        has_id = False
        if key is not None:
            id_col = [key]
            has_id = True
            cols.remove(key)
        else:
            id_col = []

        check = False
        if partition_method is not None:
            if partition_method in ['user_defined', 'predefined']:
                check = True
        if check is False:
            purpose = None
        purpose = self._arg('purpose', purpose, str, check)
        if purpose is not None:
            cols.remove(purpose)
            purpose = [purpose]
        else:
            purpose = []

        label = self._arg('label', label, str)
        if label is None:
            label = cols[-1]
        cols.remove(label)
        if not self._disable_hana_execution:
            label_t = data.dtypes([label])[0][1]
        else:
            label_t = {tp[0]:tp for tp in data.dtypes()}[label]

        if isinstance(features, str):
            features = [features]
        features = self._arg('features', features, ListOfStrings)
        if features is None:
            features = cols

        if strategy_by_col is not None:
            for col_strategy in strategy_by_col:
                if col_strategy[0] not in features:
                    msg = ('{} is not a valid column name'.format(col_strategy[0]) +
                           ' of the input dataframe for column imputation.')
                    logger.error(msg)
                    raise ValueError(msg)

        data_ = data[group_id + id_col + features + [label] + purpose]

        self.fit_params = {"partition_method" : partition_method,
                           "stratified_column" : stratified_column,
                           "partition_random_state" : partition_random_state,
                           "training_percent" : training_percent,
                           "training_size" : training_size,
                           "ntiles" : ntiles,
                           "output_partition_result" : output_partition_result,
                           "background_size" : background_size,
                           "background_random_state" : background_random_state}

        impute_params = {'impute' : impute,
                         'strategy' : strategy,
                         'strategy_by_col' : strategy_by_col,
                         'als_factors' : als_factors,
                         'als_lambda' : als_lambda,
                         'als_maxit' : als_maxit,
                         'als_randomstate' : als_randomstate,
                         'als_exit_threshold' : als_exit_threshold,
                         'als_exit_interval' : als_exit_interval,
                         'als_linsolver' : als_linsolver,
                         'als_cg_maxit' : als_cg_maxit,
                         'als_centering' : als_centering,
                         'als_scaling' : als_scaling}
        permutation_imp_params = {'permutation_importance' : permutation_importance,
                                  'permutation_evaluation_metric' : permutation_evaluation_metric,
                                  'permutation_n_repeats' : permutation_n_repeats,
                                  'permutation_seed' : permutation_seed,
                                  'permutation_n_samples' : permutation_n_samples}
        self.fit_params = _delete_none_key_in_dict(self.fit_params)
        impute_params = _delete_none_key_in_dict(impute_params)
        permutation_imp_params = _delete_none_key_in_dict(permutation_imp_params)
        if self.massive is not True:
            fit_map = {**self.__fit_param_dict, **self.__impute_dict,
                       **self.__permutation_imp_dict}
            pal_fit_params = {}
            pal_fit_params = _params_check(input_dict={**self.fit_params,
                                                       **impute_params,
                                                       **permutation_imp_params},
                                           param_map=fit_map,
                                           func=self.real_func)
            self.__pal_params.update(pal_fit_params)

            param_rows = [('FUNCTION', None, None, self.real_func),
                          #('KEY', has_id, None, None)]
                          ('KEY', 1 if self.pivoted else has_id, None, None)]

            for name in self.__pal_params:
                value, typ = self.__pal_params[name]
                if isinstance(value, (list, tuple)):
                    if name == 'HIDDEN_LAYER_SIZE':
                        value = ', '.join([str(v) for v in value])
                        param_rows.extend([(name, None, None, value)])
                    elif name == 'HIDDEN_LAYER_SIZE_OPTIONS':
                        value = ', '.join([str(v) for v in value])
                        value = value.replace('(', '"').replace(')', '"')
                        value = value.replace('[', '"').replace(']', '"')
                        value = '{' + value + '}'
                        param_rows.extend([(name, None, None, value)])
                    elif name in ['ACTIVATION_OPTIONS', 'OUTPUT_ACTIVATION_OPTIONS']:
                        value = ', '.join([str(self.__activation_map[v]) for v in value])
                        value = '{' + value +'}'
                        param_rows.extend([(name, None, None, value)])
                    else:
                        for val in value:
                            tpl = [self.__map_param(name, val, typ, label_t)]
                            param_rows.extend(tpl)
                elif typ == dict:
                    if name == '_RANGE':
                        for var in value:
                            rge = [str(v) for v in value[var]]
                            rge_str = '[' + ((',' if len(rge) == 3 else ',,'). join(rge)) + ']'
                            tpl = [(self.map_dict[self.func][var][0] + name, None, None, rge_str)]
                            param_rows.extend(tpl)
                    elif name == '_VALUES':
                        for var in value:
                            vvr = [str(v) for v in value[var]]
                            vvr_str = '{' + ','.join(vvr) + '}'
                            tpl = [(self.map_dict[self.func][var][0] + name, None, None, vvr_str)]
                            param_rows.extend(tpl)
                else:
                    tpl = [self.__map_param(name, value, typ, label_t)]
                    param_rows.extend(tpl)

            categorical_variable = _categorical_variable_update(categorical_variable, label_t, label)
            if categorical_variable is not None:
                param_rows.extend([('CATEGORICAL_VARIABLE', None, None, var) for var in categorical_variable])

            add_param_map = self.__fit_add_dict[self.func]
            if kwargs:
                for kye in kwargs:
                    if kye in add_param_map.keys():
                        par_val = kwargs[kye]
                        var_type = add_param_map[kye][1]
                        if var_type == ListOfStrings and isinstance(par_val, str):
                            par_val = [par_val]
                        value = self._arg(kye, par_val, var_type)
                        if isinstance(value, (list, tuple)):
                            for val in value:
                                tpl = [self.__map_param(add_param_map[kye][0],
                                                        val,
                                                        add_param_map[kye][1])]
                                param_rows.extend(tpl)
                        elif kye in ['class_map0', 'class_map1', 'scale_weight_target']:
                            param_rows.extend([(kye.upper(), None, None, str(kwargs[kye]))])
                        else:
                            param_rows.extend([(add_param_map[kye][0],
                                                par_val if var_type in (int, bool) else None,
                                                par_val if var_type == float else None,
                                                par_val if var_type == str else None)])
                    else:
                        err_msg = "'{}' is not a valid parameter for fitting the classification '{}' model.".format(kye, self.func)
                        logger.error(err_msg)
                        raise KeyError(err_msg)

            if self.real_func == 'LOGR' and any([var not in kwargs for var in ['class_map0', 'class_map1']]):
                if label_t in ['VARCHAR', 'NVARCHAR']:
                    err_msg = 'Values of class_map0 and class_map1 must be specified when fitting a LOGR model!'
                    logger.error(err_msg)
                    raise ValueError(err_msg)
            if impute is True and strategy_by_col is not None:
                for col_imp_type in strategy_by_col:
                    imp_type = self._arg('Imputation type', col_imp_type[1], self.__column_imputation_map)
                    if len(col_imp_type) == 2:
                        param_rows.extend([('{}_IMPUTATION_TYPE'.format(col_imp_type[0]),
                                            imp_type, None, None)])
                    elif len(col_imp_type) == 3:
                        if imp_type == 101:
                            param_rows.extend([('{}_IMPUTATION_TYPE'.format(col_imp_type[0]),
                                                imp_type, None, str(col_imp_type[2]))])
                        else:
                            param_rows.extend([('{}_IMPUTATION_TYPE'.format(col_imp_type[0]),
                                                imp_type, col_imp_type[2], None)])
                    else:
                        continue

        else: # massive mode
            group_params = self._arg('group_params', group_params, dict)
            special_group_name = 'PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID'
            param_rows = []
            fit_map = {**self.__fit_param_dict,
                       **self.__impute_dict,
                       **self.__permutation_imp_dict}
            general_params = {}
            general_params = _params_check(input_dict={**self.fit_params,
                                                       **impute_params,
                                                       **permutation_imp_params},
                                           param_map=fit_map,
                                           func=self.real_func)

            if general_params:
                if not self.__pal_params.get(special_group_name):
                    self.__pal_params[special_group_name] = general_params
                else:
                    self.__pal_params[special_group_name].update(general_params)

            if 'INT' not in group_key_type:
                categorical_variable = _categorical_variable_update(categorical_variable, label_t, label)
                if categorical_variable is not None:
                    param_rows.extend([(special_group_name, 'CATEGORICAL_VARIABLE', None, None, var) for var in categorical_variable])
                param_rows.extend([(special_group_name, 'HANDLE_MISSING_VALUE', impute if impute else None, None, None)])
                if impute is True and strategy_by_col is not None:
                    for col_imp_type in strategy_by_col:
                        imp_type = self._arg('Imputation type', col_imp_type[1], self.__column_imputation_map)
                        if len(col_imp_type) == 2:
                            param_rows.extend([(special_group_name, '{}_IMPUTATION_TYPE'.format(col_imp_type[0]),
                                                imp_type, None, None)])
                        elif len(col_imp_type) == 3:
                            if imp_type == 101:
                                param_rows.extend([(special_group_name, '{}_IMPUTATION_TYPE'.format(col_imp_type[0]),
                                                    imp_type, None, str(col_imp_type[2]))])
                            else:
                                param_rows.extend([(special_group_name, '{}_IMPUTATION_TYPE'.format(col_imp_type[0]),
                                                    imp_type, col_imp_type[2], None)])
                        else:
                            continue

                if kwargs:
                    additional_param_map = self.__fit_add_dict[self.func]
                    for kye in kwargs:
                        if kye in additional_param_map.keys():
                            par_val = kwargs[kye]
                            var_type = additional_param_map[kye][1]
                            if var_type == ListOfStrings and isinstance(par_val, str):
                                par_val = [par_val]
                            value = self._arg(kye, par_val, var_type)
                            if (kye in ['class_map0', 'class_map1'] and self.real_func != 'M_LOGR') or \
                            kye == 'scale_weight_target':
                                value = self._arg(kye, par_val, (str, int))
                                tpl = [tuple([special_group_name] + list(self.__map_param(additional_param_map[kye][0],
                                                                                          value,
                                                                                          additional_param_map[kye][1])))]
                                param_rows.extend(tpl)
                            elif isinstance(value, (list, tuple)):
                                for val in value:
                                    tpl = [tuple([special_group_name] + list(self.__map_param(additional_param_map[kye][0],
                                                                                              val,
                                                                                              var_type)))]
                                    param_rows.extend(tpl)
                            else:
                                param_rows.extend([(additional_param_map[kye][0],
                                                    par_val if var_type in (int, bool) else None,
                                                    par_val if var_type == float else None,
                                                    par_val if var_type == str else None)])
                        else:
                            err_msg = "'{}' is not a valid parameter for fitting the classification '{}' model.".format(kye, self.func)
                            logger.error(err_msg)
                            raise KeyError(err_msg)

            # if group_key is INTEGER, no general parameter is allowed to use
            g_params = general_params
            hmv_val = g_params.get('HANDLE_MISSING_VALUE')
            if hmv_val is None or hmv_val[0] is not True:
                del g_params['HANDLE_MISSING_VALUE']
            if 'INT' in group_key_type and (g_params or impute or strategy_by_col or kwargs):
                warn_msg = "If the type of group_key is INTEGER, only parameters in group_params are valid!"
                logger.warning(warn_msg)

            # for each group
            if group_params is not None:
                for group in group_params:
                    if group in ['PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID']:
                        continue
                    group_val = int(group) if 'INT' in group_key_type else group
                    each_group_params = {}
                    each_group_map = {**self.__fit_param_dict,
                                      **self.__impute_dict,
                                      **self.__fit_add_dict[self.func],
                                      **self.__permutation_imp_dict}
                    each_group_params = _params_check(input_dict=group_params[group],
                                                      param_map=each_group_map,
                                                      func=self.real_func)
                    if 'INT' not in group_key_type:
                        param_rows.extend([(group_val, 'KEY', has_id, None, None)])
                        param_rows.extend([(group_val, 'FUNCTION', None, None, self.real_func)])
                    if each_group_params:
                        if group in self.__pal_params.keys():
                            if self.__pal_params[group]:
                                self.__pal_params[group].update(each_group_params)
                            else:
                                self.__pal_params[group] = each_group_params
                        else:
                            self.__pal_params[group] = each_group_params

                    impute_val = group_params[group].get('impute')
                    strategy_by_col_val = group_params[group].get('strategy_by_col')
                    param_rows.extend([(group_val, 'HANDLE_MISSING_VALUE', impute_val,
                                        None, None)])
                    if impute_val is True and strategy_by_col_val is not None:
                        for col_imp_type in strategy_by_col_val:
                            imp_type = self._arg('Imputation type', col_imp_type[1], self.__column_imputation_map)
                            if len(col_imp_type) == 2:
                                param_rows.extend([(group_val, '{}_IMPUTATION_TYPE'.format(col_imp_type[0]),
                                                    imp_type, None, None)])
                            elif len(col_imp_type) == 3:
                                if imp_type == 101:
                                    param_rows.extend([(group_val, '{}_IMPUTATION_TYPE'.format(col_imp_type[0]),
                                                        imp_type, None, str(col_imp_type[2]))])
                                else:
                                    param_rows.extend([(group_val, '{}_IMPUTATION_TYPE'.format(col_imp_type[0]),
                                                        imp_type, col_imp_type[2], None)])
                            else:
                                continue

                    group_categorical_variable = None
                    if group_params[group].get('categorical_variable') is not None:
                        group_categorical_variable = group_params[group]['categorical_variable']
                    group_categorical_variable = _categorical_variable_update(group_categorical_variable, label_t, label)
                    if group_categorical_variable:
                        param_rows.extend([(group_val, 'CATEGORICAL_VARIABLE', None, None, var) for var in group_categorical_variable])

            # if group_key is INT, need to specific key for all group is key or not
            if 'INT' in group_key_type:
                for each in data_groups:
                    each_val = int(each)
                    param_rows.extend([(each_val, 'KEY', has_id, None, None)])
                    param_rows.extend([(each_val, 'FUNCTION', None, None, self.real_func)])
            else:
                param_rows.extend([('PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID', 'KEY', has_id, None, None)])
                param_rows.extend([('PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID', 'FUNCTION', None, None, self.real_func)])

            for group in self.__pal_params:
                is_special_group = False
                if group in ['PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID']:
                    group_val = 'PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID'
                    is_special_group = True
                else:
                    group_val = int(group) if 'INT' in group_key_type else group
                if 'INT' in group_key_type and is_special_group is True:
                    continue
                if 'INT' not in group_key_type:
                    param_rows.extend([(group_val, 'KEY', has_id, None, None)])
                    param_rows.extend([(group_val, 'FUNCTION', None, None, self.real_func)])
                if self.__pal_params[group]:
                    for name in self.__pal_params[group]:
                        value, typ = self.__pal_params[group][name]
                        if isinstance(value, (list, tuple)):
                            if name == 'HIDDEN_LAYER_SIZE':
                                value = ', '.join([str(v) for v in value])
                                param_rows.extend([(group_val, name, None, None, value)])
                            elif name == 'HIDDEN_LAYER_SIZE_OPTIONS':
                                value = ', '.join([str(v) for v in value])
                                value = value.replace('(', '"').replace(')', '"')
                                value = value.replace('[', '"').replace(']', '"')
                                value = '{' + value + '}'
                                param_rows.extend([(group_val, name, None, None, value)])
                            elif name in ['ACTIVATION_OPTIONS', 'OUTPUT_ACTIVATION_OPTIONS']:
                                value = ', '.join([str(self.__activation_map[v]) for v in value])
                                value = '{' + value +'}'
                                param_rows.extend([(group_val, name, None, None, value)])
                            elif name in ['_BIN_', '_PRIOR_', 'STRATA', 'PRIOR']:
                                for val in value:
                                    tpl = [tuple([group_val] + list(self.__map_param(name, val, typ)))]
                                    param_rows.extend(tpl)
                            else:
                                for val in value:
                                    tpl = [tuple([group_val] + list(self.__map_param(name, val, typ, label_t)))]
                                    param_rows.extend(tpl)
                        elif typ == dict:
                            if name == '_RANGE':
                                for var in value:
                                    rge = [str(v) for v in value[var]]
                                    rge_str = '[' + ((',' if len(rge) == 3 else ',,'). join(rge)) + ']'
                                    tpl = [(group_val, self.map_dict[self.func][var][0] + name, None, None, rge_str)]
                                    param_rows.extend(tpl)
                            elif name == '_VALUES':
                                for var in value:
                                    vvr = [str(v) for v in value[var]]
                                    vvr_str = '{' + ','.join(vvr) + '}'
                                    tpl = [(group_val, self.map_dict[self.func][var][0] + name, None, None, vvr_str)]
                                    param_rows.extend(tpl)
                        else:
                            tpl = [tuple([group_val] + list(self.__map_param(name, value, typ)))]
                            param_rows.extend(tpl)

                # consider the case that when the number of groups in init() is more than that of fit()
                if 'INT' in label_t:
                    param_rows.extend([(group_val, 'CATEGORICAL_VARIABLE', None, None, label)])

        self.label = label
        if self.label is None:
            self.label = data_.columns[-1]
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        if self.massive is True:
            outputs_massive = ['MODEL', 'IMPORTANCE', 'STATS', 'OPT_PARAM', 'CONFUSION_MATRIX',
                               'METRICS', 'PARTITION_TYPE', 'ERRORMSG', 'PLACE_HOLDER1']
            outputs_massive = ['#PAL_MASSIVE_UNIFIED_CLASSIFICATION_{}_{}_{}'.format(tbl, self.id, unique_id)
                               for tbl in outputs_massive]
            model_tbl, imp_tbl, stats_tbl, opt_param_tbl, cm_tbl, metrics_tbl, partition_tbl, errormsg_tbl, _ = outputs_massive
            fit_output_signature = [
                    {"GROUP_ID": "NVARCHAR(256)", "ROW_INDEX": "INTEGER", "PART_INDEX": "INTEGER", "MODEL_CONTENT": "NCLOB"},
                    {"GROUP_ID": "NVARCHAR(256)", "VARIABLE_NAME": "NVARCHAR(256)", "IMPORTANCE": "DOUBLE"},
                    {"GROUP_ID": "NVARCHAR(256)", "STAT_NAME": "NVARCHAR(256)", "STAT_VALUE": "NVARCHAR(1000)", "CLASS_NAME": "NVARCHAR(256)"},
                    {"GROUP_ID": "NVARCHAR(256)", "PARAM_NAME": "NVARCHAR(256)", "INT_VALUE": "INTEGER", "DOUBLE_VALUE": "DOUBLE", "STRING_VALUE": "NVARCHAR(1000)"},
                    {"GROUP_ID": "NVARCHAR(256)", "ACTUAL_CLASS": "NVARCHAR(1000)", "PREDICTED_CLASS": "NVARCHAR(1000)", "COUNT": "INTEGER"},
                    {"GROUP_ID": "NVARCHAR(256)", "NAME": "NVARCHAR(256)", "X": "DOUBLE", "Y": "DOUBLE"},
                    {"GROUP_ID": "NVARCHAR(256)", "ID": "INTEGER", "TYPE": "INTEGER"},
                    {"GROUP_ID": "NVARCHAR(256)", "ERROR_TIMESTAMP": "NVARCHAR(256)", "ERRORCODE": "INTEGER", "MASSAGE":"NVARCHAR(1000)"},
                    {"GROUP_ID": "NVARCHAR(256)", "OBJECT": "NVARCHAR(10)", "KEY": "NVARCHAR(10)", "VALUE": "NVARCHAR(10)"}
                    ]
            setattr(self, "fit_output_signature", fit_output_signature)
            if not (check_pal_function_exist(conn, '%UNIFIED_MASSIVE%', like=True) or self._disable_hana_execution):
                msg = 'The version of your SAP HANA does not support unified massive classification!'
                logger.error(msg)
                raise ValueError(msg)
            try:
                if self.pivoted:
                    setattr(self, 'fit_data', data)
                    self._call_pal_auto(conn,
                                        'PAL_UNIFIED_MASSIVE_CLASSIFICATION_PIVOT',
                                        meta_data,
                                        data,
                                        ParameterTable(itype=group_key_type).with_data(param_rows),
                                        *outputs_massive)
                else:
                    setattr(self, 'fit_data', data_)
                    self._call_pal_auto(conn,
                                        'PAL_UNIFIED_MASSIVE_CLASSIFICATION',
                                        data_,
                                        ParameterTable(itype=group_key_type).with_data(param_rows),
                                        *outputs_massive)
            except dbapi.Error as db_err:
                logger.error(str(db_err))
                try_drop(conn, outputs_massive)
                raise
            except Exception as db_err:
                logger.error(str(db_err))
                try_drop(conn, outputs_massive)
                raise
        else: # single mode
            outputs = ['MODEL', 'IMPORTANCE', 'STATS', 'OPT_PARAM', 'CONFUSION_MATRIX',
                       'METRICS', 'PARTITION_TYPE', 'PLACE_HOLDER1']
            outputs = ['#PAL_UNIFIED_CLASSIFICATION_{}_{}_{}'.format(tbl, self.id, unique_id)
                       for tbl in outputs]
            model_tbl, imp_tbl, stats_tbl, opt_param_tbl, cm_tbl, metrics_tbl, partition_tbl, _ = outputs
            fit_output_signature = [
                    {"ROW_INDEX": "INTEGER", "PART_INDEX": "INTEGER", "MODEL_CONTENT": "NCLOB"},
                    {"VARIABLE_NAME": "NVARCHAR(256)", "IMPORTANCE": "DOUBLE"},
                    {"STAT_NAME": "NVARCHAR(256)", "STAT_VALUE": "NVARCHAR(1000)", "CLASS_NAME": "NVARCHAR(256)"},
                    {"PARAM_NAME": "NVARCHAR(256)", "INT_VALUE": "INTEGER", "DOUBLE_VALUE": "DOUBLE", "STRING_VALUE": "NVARCHAR(1000)"},
                    {"ACTUAL_CLASS": "NVARCHAR(1000)", "PREDICTED_CLASS": "NVARCHAR(1000)", "COUNT": "INTEGER"},
                    {"NAME": "NVARCHAR(256)", "X": "DOUBLE", "Y": "DOUBLE"},
                    {"ID": "INTEGER", "TYPE": "INTEGER"},
                    {"OBJECT": "NVARCHAR(10)", "KEY": "NVARCHAR(10)", "VALUE": "NVARCHAR(10)"}
                    ]
            setattr(self, "fit_output_signature", fit_output_signature)
            try:
                if self.pivoted:
                    setattr(self, 'fit_data', data)
                    self._call_pal_auto(conn,
                                        'PAL_UNIFIED_CLASSIFICATION_PIVOT',
                                        meta_data,
                                        data,
                                        ParameterTable().with_data(param_rows),
                                        *outputs)
                else:
                    setattr(self, 'fit_data', data_)
                    self._call_pal_auto(conn,
                                        'PAL_UNIFIED_CLASSIFICATION',
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

        self.scoring_list_ = None
        self.importance_ = conn.table(imp_tbl)
        self.statistics_ = conn.table(stats_tbl)
        self.optimal_param_ = conn.table(opt_param_tbl)
        self.confusion_matrix_ = conn.table(cm_tbl)
        self.metrics_ = conn.table(metrics_tbl)
        self.partition_ = conn.table(partition_tbl)
        self.error_msg_ = None
        if self.massive is True:
            self.error_msg_ = conn.table(errormsg_tbl)
            if not self._disable_hana_execution:
                if not self.error_msg_.collect().empty:
                    row = self.error_msg_.count()
                    for i in range(1, row+1):
                        warn_msg = "For group_key '{}',".format(self.error_msg_.collect()[group_key][i-1]) +\
                                   " the error message is '{}'.".format(self.error_msg_.collect()['MESSAGE'][i-1]) +\
                                   "More information could be seen in the attribute error_msg_!"
                        logger.warning(warn_msg)
        model_df = conn.table(model_tbl)
        if self.func == 'DT':
            setattr(model_df, '_is_uni_dt', True)
        self.model_ = [model_df,
                       self.statistics_,
                       self.optimal_param_,
                       self.confusion_matrix_,
                       self.importance_,
                       self.metrics_]
        self.__param_rows = param_rows
        if build_report:
            self.build_report()
        return self

    def get_optimal_parameters(self):
        """
        Return the optimal parameters.
        """
        return self.model_[2].collect()

    def get_confusion_matrix(self):
        """
        Return the confusion matrix.
        """
        return self.model_[3].collect()

    def get_feature_importances(self):
        """
        Return the feature importance.
        """
        imp = self.model_[4].collect()
        return {row.VARIABLE_NAME:float(row.IMPORTANCE) for idx, row in imp.iterrows()}

    def get_performance_metrics(self):
        """
        Return the performance metrics.
        """
        mtrc = self.model_[1].collect()
        return {row.STAT_NAME:float(row.STAT_VALUE) for idx, row in mtrc.iterrows()}

    @trace_sql
    def predict(self,#pylint:disable=too-many-positional-arguments
                data,
                key=None,
                features=None,
                group_key=None,
                group_params=None,
                model=None,
                thread_ratio=None,
                verbose=None,
                class_map1=None,
                class_map0=None,
                alpha=None,
                block_size=None,
                missing_replacement=None,
                categorical_variable=None,
                top_k_attributions=None,
                attribution_method=None,
                sample_size=None,
                random_state=None,
                impute=False,
                strategy=None,
                strategy_by_col=None,
                als_factors=None,
                als_lambda=None,
                als_maxit=None,
                als_randomstate=None,
                als_exit_threshold=None,
                als_exit_interval=None,
                als_linsolver=None,
                als_cg_maxit=None,
                als_centering=None,
                als_scaling=None,
                ignore_unknown_category=None,
                verbose_top_n=None,
                positive_label=None,
                shap_value_for_positive_only=None,
                **kwargs):
        r"""
        Predict dependent variable values based on a fitted model.

        Parameters
        ----------
        data :  DataFrame
            Data to be predicted.

            If `self.pivoted` is True, then ``data`` must be pivoted, indicating that it should be structured
            the same as the pivoted data used for training(exclusive of the last data partition column)
            and contains no target values. In this case, the following parameters become ineffective:
            ``key``, ``features``, ``group_key``.


        key : str, optional
            Name of the ID column.

            In single mode, mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.
            Defaults to the single index column of ``data`` if not provided.

            In massive mode, defaults to the first-non group key column of data if the index columns of data is not provided.
            Otherwise, defaults to the second of index columns of data and the first column of index columns is group_key.

        features : a list of str, optional
            Names of feature columns in data for prediction.

            Defaults all non-key columns in `data` if not provided.

        group_key : str, optional
            The column of group_key. Data type can be INT or NVARCHAR/VARCHAR.
            If data type is INT, only parameters set in the group_params are valid.

            This parameter is only valid when ``massive`` is set as True in
            class instance initialization.

            Defaults to the first column of data if the index columns of data is not provided.
            Otherwise, defaults to the first column of index columns.

        group_params : dict, optional
            If massive mode is activated (``massive`` is set as True in class instance initialization), input data
            for classification shall be divided into different groups with different classification parameters applied.
            This parameter specifies the parameter values of the chosen classification algorithm ``func``
            in predict() w.r.t. different groups in a dict format, where keys corresponding to ``group_key``
            while values should be a dict for classification algorithm parameter value assignments.

            An example is as follows:

            .. only:: latex

                >>> uc = UnifiedClassification(func='logisticregression',
                                               multi_class=False,
                                               massive=True,
                                               max_iter=10,
                                               group_params={'Group_1': {'solver': 'auto'}})
                >>> uc.fit(data=df,
                           key='ID',
                           features=["OUTLOOK" ,"TEMP", "HUMIDITY","WINDY"],
                           label="CLASS",
                           group_key="GROUP_ID",
                           background_size=4,
                           group_params={'Group_1':{'background_random_state':2}})
                >>> res = uc.predict(data=pred_df,
                                     key='ID',
                                     group_key='GROUP_ID',
                                     group_params={'Group_1':{'class_map0':'A', 'class_map1':'B'},
                                                   'Group_2':{'class_map0':'C', 'class_map1':'D'}})

            .. raw:: html

                <iframe allowtransparency="true" style="border:1px solid #ccc; background: #eeffcb;"
                    src="../../_static/uc_predict_example.html" width="100%" height="100%" sandbox="">
                </iframe>

            Valid only when ``massive`` is set as True in class instance initialization.

            Defaults to None.

        model : DataFrame, optional
            A fitted classification model.

            Defaults to self.model\_.

        thread_ratio : float, optional
            Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
            Values outside the range will be ignored and this function heuristically determines the number of threads to use.

            Defaults to the PAL's default value.

        verbose : bool, optional
            Specifies whether to output all classes and the corresponding confidences for each data.

            Defaults to False.

        class_map0 : str, optional
            Specifies the label value which will be mapped to 0 in logistic regression.

            Valid only for logistic regression models when label variable is of VARCHAR or NVARCHAR type.

            No default value.

        class_map1 : str, optional
            Specifies the label value which will be mapped to 1 in logistic regression.

            Valid only for logistic regression models when label variable is of VARCHAR or NVARCHAR type.

            No default value.

        alpha : float, optional
            Specifies the value for laplace smoothing.

            - 0: Disables Laplace smoothing.
            - Other positive values: Enables Laplace smoothing for discrete values.

            Valid only for Naive Bayes models.

            Defaults to 0.

        block_size : int, optional
            Specifies the number of data loaded per time during scoring.

            - 0: load all data once
            - Other positive Values: the specified number

            Valid only for RandomDecisionTree and HybridGradientBoostingTree model

            Defaults to 0.

        missing_replacement : str, optional
            Specifies the strategy for replacement of missing values in prediction data.

                - 'feature_marginalized': marginalises each missing feature out independently
                - 'instance_marginalized': marginalises all missing features in an instance as a
                  whole corresponding to each category

            Valid only when ``impute`` is False, and
            only for RandomDecisionTree and HybridGradientBoostingTree models.

            Defaults to 'feature_marginalized'.

        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.
        top_k_attributions : int, optional
            Specifies the number of features with highest attributions to output.

            Defaults to 10.

        attribution_method : {'no', 'saabas', 'tree-shap'}, optional

            Specifies which method to use for tree-based model reasoning.

            - 'no' : No reasoning.
            - 'saabas' : Saabas method.
            - 'tree-shap' : Tree SHAP method.

            Valid only for tree-based models, i.e. DecisionTree, RandomDecisionTree and HybridGradientBoostingTree models.
            For such models, users should explicitly specify either 'saabas' or 'tree-shap' as the attribution method
            in order to activate SHAP explanations in the prediction result.

            Defaults to 'tree-shap'.

        sample_size : int, optional
            Specifies the number of sampled combinations of features.

            - 0 : Heuristically determined by algorithm.
            - Others : The specified sample size.

            Valid only for Naive Bayes, Support Vector Machine, Multilayer Perceptron and
            Multi-class Logistic Regression models.

            Defaults to 0.

            .. note::
                ``top_k_attributions``, ``attribution_method`` and ``sample_size`` are core parameters
                for `local interpretability of models` in hana_ml.algorithms.pal package, please see
                :ref:`local_interpretability-label` for more details.

        random_state : int, optional
            Specifies the seed for random number generator when sampling the combination of features.

            - 0 : User current time as seed.
            - Others : The actual seed.

            Valid only for Naive Bayes, Support Vector Machine, Multilayer Perceptron and
            Multi-class Logistic Regression models.

            Defaults to 0.

        impute : bool, optional
            Specifies whether or not to handle missing values in the data for prediction.

            Defaults to False.

        strategy, strategy_by_col, als_* : parameters for missing value handling, optional
            All these parameters mentioned above are for handling missing values
            in data, please see :ref:`impute_params-label` for more details.

            All parameters are valid only when ``impute`` is set as True.

        ignore_unknown_category : bool, optional
            Specifies whether or not to ignore unknown category value.

            - False : Report error if unknown category value is found.
            - True : Ignore unknown category value if there is any.

            Valid only when the model for prediction is multi-class logistic regression.

            Defaults to True.

        verbose_top_n : int, optional
            Sorted and presents top N verboses. The range is [0, count of labels].

            Effective only when ``verbose`` is set as True.

            Defaults to 0.

        positive_label : str, optional
            Signifies the name of the positive label in binary classification.

            Only valid when ``func`` is "decisiontree", "hybridgradientboostingtree" and "randomdecisiontree".

            This is a new parameter availible in HANA PAL 2024 QRC2.

            Defaults to 0.

        shap_value_for_positive_only : bool, optional
            Specifies if SHAP value always refers to probability of positive or refers to probability of predicted label.

            Only valid when ``func`` is "hybridgradientboostingtree".

            This is a new parameter availible in HANA PAL 2024 QRC2.

            Defaults to True.

        **kwargs : keyword arguments
            Additional keyword arguments w.r.t. different classification algorithms within UnifiedClassification.

        Returns
        -------

        DataFrame 1
            Prediction result.

        DataFrame 2 (optional)
            Error message.
            Only valid if ``massive`` is True when initializing an 'UnifiedClassification' instance.

        """
        if model is None and getattr(self, 'model_') is None:
            raise FitIncompleteError()
        conn = data.connection_context

        group_params = self._arg('group_params', group_params, dict)
        if group_params:
            for group in group_params:
                self._arg(self.func + ' Parameters with group_key ' + str(group),
                          group_params[group], dict)

        cols = data.columns
        index = data.index
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
                msg = "Please select group_key from {}!".format(cols)
                logger.error(msg)
                raise ValueError(msg)
            data_groups = list(data[[group_key]].collect()[group_key].drop_duplicates())
            param_keys = list(self.group_params.keys())
            gid_type = {tp[0]:tp for tp in data.dtypes()}[group_key]
            if not self._disable_hana_execution:
                if data_groups and not all([(int(ky) if 'INT' in gid_type[1] else ky) in data_groups for ky in param_keys]):
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
                key = self._arg('key', key, str, required=True)
        else: # single mode
            key = self._arg('key', key, str)
            if index is not None:
                key = _col_index_check(key, 'key', index, cols)
            else:
                key = self._arg('key', key, str, required=True)

        if key is not None and key not in cols:
            msg = "Please select key from {}!".format(cols)
            logger.error(msg)
            raise ValueError(msg)
        cols.remove(key)

        if isinstance(features, str):
            features = [features]
        features = self._arg('features', features, ListOfStrings)
        if features is None:
            features = cols
        data_ = data[group_id + [key] + features]

        predict_params = {'thread_ratio' : thread_ratio,
                          'verbose' : verbose,
                          'class_map1' : class_map1,
                          'class_map0' : class_map0,
                          'alpha' : alpha,
                          'block_size' : block_size,
                          'missing_replacement' : missing_replacement,
                          'top_k_attributions' : top_k_attributions,
                          'attribution_method' : attribution_method,
                          'sample_size' : sample_size,
                          'random_state' : random_state,
                          'ignore_unknown_category' : ignore_unknown_category,
                          'verbose_top_n' : verbose_top_n,
                          'positive_label' : positive_label,
                          'shap_value_for_positive_only': shap_value_for_positive_only}
        predict_impute_params = {'impute' : impute,
                                 'strategy' : strategy,
                                 'strategy_by_col' : strategy_by_col,
                                 'als_factors' : als_factors,
                                 'als_lambda' : als_lambda,
                                 'als_maxit' : als_maxit,
                                 'als_randomstate' : als_randomstate,
                                 'als_exit_threshold' : als_exit_threshold,
                                 'als_exit_interval' : als_exit_interval,
                                 'als_linsolver' : als_linsolver,
                                 'als_cg_maxit' : als_cg_maxit,
                                 'als_centering' : als_centering,
                                 'als_scaling' : als_scaling}
        predict_params = _delete_none_key_in_dict(predict_params)
        predict_impute_params = _delete_none_key_in_dict(predict_impute_params)

        self.__pal_predict_params = {}
        if self.massive is not True:
            predict_map = {**self.__predict_score_param_dict}
            self.__pal_predict_params = _params_check(input_dict=predict_params,
                                                      param_map=predict_map,
                                                      func=self.real_func)
            param_rows = [('FUNCTION', None, None, self.real_func)]

            update_impute_params = predict_impute_params
            impute_map = {**self.__impute_dict}
            pal_impute_params = _params_check(input_dict=update_impute_params,
                                              param_map=impute_map,
                                              func=self.real_func)
            self.__pal_predict_params.update(pal_impute_params)
            for name in self.__pal_predict_params:
                value, typ = self.__pal_predict_params[name]
                tpl = [self.__map_param(name, value, typ)]
                param_rows.extend(tpl)

            categorical_variable = _categorical_variable_update(categorical_variable, None, None)
            if categorical_variable is not None:
                param_rows.extend([('CATEGORICAL_VARIABLE', None, None, var) for var in categorical_variable])

            param = {**kwargs}
            if param:
                func_map = predict_map
                for name, value in param.items():
                    name_type = func_map[name]
                    name_ = name_type[0]
                    typ = name_type[1]
                    if len(name_type) == 3:
                        value = name_type[2][value]
                    if isinstance(value, (list, tuple)):
                        for val in value:
                            param_rows.extend([self.__map_param(name_, val, typ)])
                    else:
                        param_rows.extend([self.__map_param(name_, value, typ)])

            if update_impute_params:
                impute_val = update_impute_params.get('impute')
                strategy_by_col_val = update_impute_params.get('strategy_by_col')
                if impute_val is True and strategy_by_col_val is not None:
                    for col_imp_type in strategy_by_col_val:
                        imp_type = self._arg('Imputation type', col_imp_type[1], self.__column_imputation_map)
                        if len(col_imp_type) == 2:
                            param_rows.extend([('{}_IMPUTATION_TYPE'.format(col_imp_type[0]),
                                                imp_type, None, None)])
                        elif len(col_imp_type) == 3:
                            if imp_type == 101:
                                param_rows.extend([('{}_IMPUTATION_TYPE'.format(col_imp_type[0]),
                                                    imp_type, None, str(col_imp_type[2]))])
                            else:
                                param_rows.extend([('{}_IMPUTATION_TYPE'.format(col_imp_type[0]),
                                                    imp_type, col_imp_type[2], None)])
                        else:
                            continue
        else: # massive mode
            special_group_name = 'PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID'
            param_rows = []
            predict_map = {**self.__predict_score_param_dict, **self.__impute_dict}
            general_params = {}
            general_params = _params_check(input_dict={**predict_params, **predict_impute_params},
                                           param_map=predict_map,
                                           func=self.real_func)
            g_params = general_params
            hmv_val = g_params.get('HANDLE_MISSING_VALUE')
            if hmv_val is None or hmv_val[0] is not True:
                del g_params['HANDLE_MISSING_VALUE']
            if 'INT' in group_key_type and (g_params or impute or strategy_by_col):
                warn_msg = "If the type of group_key is INTEGER, only parameters in group_params are valid!"
                logger.warning(warn_msg)

            if general_params:
                self.__pal_predict_params[special_group_name] = general_params

            if 'INT' not in group_key_type:
                categorical_variable = _categorical_variable_update(categorical_variable, None, None)
                if categorical_variable is not None:
                    param_rows.extend([(special_group_name, 'CATEGORICAL_VARIABLE', None, None, var) for var in categorical_variable])
                if impute is True and strategy_by_col is not None:
                    for col_imp_type in strategy_by_col:
                        imp_type = self._arg('Imputation type', col_imp_type[1], self.__column_imputation_map)
                        if len(col_imp_type) == 2:
                            param_rows.extend([(special_group_name, '{}_IMPUTATION_TYPE'.format(col_imp_type[0]),
                                                imp_type, None, None)])
                        elif len(col_imp_type) == 3:
                            if imp_type == 101:
                                param_rows.extend([(special_group_name, '{}_IMPUTATION_TYPE'.format(col_imp_type[0]),
                                                    imp_type, None, str(col_imp_type[2]))])
                            else:
                                param_rows.extend([(special_group_name, '{}_IMPUTATION_TYPE'.format(col_imp_type[0]),
                                                    imp_type, col_imp_type[2], None)])
                        else:
                            continue

            # for each group
            if group_params:
                for group in group_params:
                    if group in ['PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID']:
                        continue
                    group_val = int(group) if 'INT' in group_key_type else group
                    param_rows.extend([(group_val, 'FUNCTION', None, None, self.real_func)])
                    each_group_params = {}
                    each_group_params = _params_check(input_dict=group_params[group],
                                                      param_map=predict_map,
                                                      func=self.real_func)
                    if each_group_params:
                        self.__pal_predict_params[group] = each_group_params

                    group_categorical_variable = None
                    if group_params[group].get('categorical_variable') is not None:
                        group_categorical_variable = group_params[group]['categorical_variable']
                    group_categorical_variable = _categorical_variable_update(group_categorical_variable, None, None)
                    if group_categorical_variable is not None:
                        param_rows.extend([(group_val, 'CATEGORICAL_VARIABLE', None, None, var) for var in group_categorical_variable])

                    impute_val = group_params[group].get('impute')
                    strategy_by_col_val = group_params[group].get('strategy_by_col')
                    if impute_val is True and strategy_by_col_val is not None:
                        for col_imp_type in strategy_by_col_val:
                            imp_type = self._arg('Imputation type', col_imp_type[1], self.__column_imputation_map)
                            if len(col_imp_type) == 2:
                                param_rows.extend([(group_val, '{}_IMPUTATION_TYPE'.format(col_imp_type[0]),
                                                    imp_type, None, None)])
                            elif len(col_imp_type) == 3:
                                if imp_type == 101:
                                    param_rows.extend([(group_val, '{}_IMPUTATION_TYPE'.format(col_imp_type[0]),
                                                        imp_type, None, str(col_imp_type[2]))])
                                else:
                                    param_rows.extend([(group_val, '{}_IMPUTATION_TYPE'.format(col_imp_type[0]),
                                                        imp_type, col_imp_type[2], None)])
                            else:
                                continue

            # if group_key is INT, need to specific key for all group is key or not
            if 'INT' in group_key_type:
                for each in data_groups:
                    each_val = int(each)
                    param_rows.extend([(each_val, 'FUNCTION', None, None, self.real_func)])

            for group in self.__pal_predict_params:
                is_special_group = False
                if group in ['PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID']:
                    group_val = 'PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID'
                    is_special_group = True
                else:
                    group_val = int(group) if 'INT' in group_key_type else group
                if 'INT' in group_key_type and is_special_group is True:
                    continue
                if self.__pal_predict_params[group]:
                    if 'INT' not in group_key_type:
                        param_rows.extend([(group_val, 'FUNCTION', None, None, self.real_func)])
                    for name in self.__pal_predict_params[group]:
                        value, typ = self.__pal_predict_params[group][name]
                        tpl = [tuple([group_val] + list(self.__map_param(name, value, typ)))]
                        param_rows.extend(tpl)

            if 'INT' not in group_key_type:
                param_rows.extend([('PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID', 'FUNCTION', None, None, self.real_func)])

        if model is None:
            if isinstance(self.model_, (list, tuple)):
                model = self.model_[0]
            else:
                model = self.model_

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        if self.massive is True:
            outputs = ['RESULT', 'ERROR_MSG', 'PH']
            outputs_massive = ['#PAL_UNIFIED_MASSIVE_CLASSIFICATION_PREDICT_{}_TBL_{}_{}'.format(name, self.id, unique_id)
                               for name in outputs]
            result_tbl, errmsg_tbl, ph_tbl = outputs_massive
            predict_output_signature = [
                        {"GROUP_ID": "NVARCHAR(256)", "ID": "NVARCHAR(256)", "SCORE": "NVARCHAR(256)", "CONFIDENCE": "DOUBLE", "REASON_CODE": "NCLOB"},
                        {"GROUP_ID": "NVARCHAR(256)", "ERROR_TIMESTAMP": "NVARCHAR(256)", "ERRORCODE": "INTEGER", "MASSAGE":"NVARCHAR(1000)"},
                        {"GROUP_ID": "NVARCHAR(256)", "OBJECT": "NVARCHAR(10)", "KEY": "NVARCHAR(10)", "VALUE": "NVARCHAR(10)"}
                        ]
            setattr(self, "predict_output_signature", predict_output_signature)
            if not (check_pal_function_exist(conn, '%UNIFIED_MASSIVE%', like=True) or self._disable_hana_execution):
                msg = 'The version of your SAP HANA does not support unified massive classification!'
                logger.error(msg)
                raise ValueError(msg)
            try:
                if self.pivoted:
                    setattr(self, 'predict_data', data)
                    self._call_pal_auto(conn,
                                        'PAL_UNIFIED_MASSIVE_CLASSIFICATION_PREDICT_PIVOT',
                                        data,
                                        model,
                                        ParameterTable(itype=group_key_type).with_data(param_rows),
                                        *outputs_massive)
                else:
                    setattr(self, 'predict_data', data_)
                    self._call_pal_auto(conn,
                                        'PAL_UNIFIED_MASSIVE_CLASSIFICATION_PREDICT',
                                        data_,
                                        model,
                                        ParameterTable(itype=group_key_type).with_data(param_rows),
                                        *outputs_massive)
            except dbapi.Error as db_err:
                logger.exception(str(db_err))
                try_drop(conn, outputs_massive)
                raise
            except Exception as db_err:
                logger.exception(str(db_err))
                try_drop(conn, outputs_massive)
                raise
            err_msg = conn.table(errmsg_tbl)
            if not self._disable_hana_execution:
                if not err_msg.collect().empty:
                    row = err_msg.count()
                    for i in range(1, row+1):
                        warn_msg = "For group_key '{}',".format(err_msg.collect()[group_key][i-1]) +\
                                   " the error message is '{}'.".format(err_msg.collect()['MESSAGE'][i-1]) +\
                                   "More information could be seen in the 2nd return Dataframe!"
                        logger.warning(warn_msg)
            return conn.table(result_tbl), err_msg

        outputs = ['RESULT', 'PH']
        outputs = ['#PAL_UNIFIED_CLASSIF_PREDICT_{}_TBL_{}_{}'.format(name, self.id, unique_id) for name in outputs]
        result_tbl, ph_tbl = outputs
        predict_output_signature = [
                    {"ID": "NVARCHAR(256)", "SCORE": "NVARCHAR(256)", "CONFIDENCE": "DOUBLE", "REASON_CODE": "NCLOB"},
                    {"OBJECT": "NVARCHAR(10)", "KEY": "NVARCHAR(10)", "VALUE": "NVARCHAR(10)"}
                    ]
        setattr(self, "predict_output_signature", predict_output_signature)
        try:
            if self.pivoted:
                self._call_pal_auto(conn,
                                    'PAL_UNIFIED_CLASSIFICATION_PREDICT_PIVOT',
                                    data,
                                    model,
                                    ParameterTable().with_data(param_rows),
                                    *outputs)
            else:
                self._call_pal_auto(conn,
                                    'PAL_UNIFIED_CLASSIFICATION_PREDICT',
                                    data_,
                                    model,
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
        return conn.table(result_tbl)

    def score(self,#pylint:disable=too-many-positional-arguments
              data,
              key=None,
              features=None,
              label=None,
              group_key=None,
              group_params=None,
              model=None,
              thread_ratio=None,
              max_result_num=None,
              ntiles=None,
              class_map1=None,
              class_map0=None,
              alpha=None,
              block_size=None,
              missing_replacement=None,
              categorical_variable=None,
              top_k_attributions=None,
              attribution_method=None,
              sample_size=None,
              random_state=None,
              impute=False,
              strategy=None,
              strategy_by_col=None,
              als_factors=None,
              als_lambda=None,
              als_maxit=None,
              als_randomstate=None,
              als_exit_threshold=None,
              als_exit_interval=None,
              als_linsolver=None,
              als_cg_maxit=None,
              als_centering=None,
              als_scaling=None,
              ignore_unknown_category=None,
              positive_label=None,
              shap_value_for_positive_only=None):
        r"""
        Evaluates the model quality.
        In the Unified Classification, statistics and metrics are provided to show the model quality.
        Currently the following metrics are supported:

        - AUC and ROC
        - RECALL, PRECISION, F1-SCORE, SUPPORT
        - ACCURACY
        - KAPPA
        - MCC
        - CUMULATIVE GAINS
        - CULMULATIVE LIFT
        - LIFT

        Parameters
        ----------
        data :  DataFrame
            Data for scoring.

            If `self.pivoted` is True, then ``data`` must be pivoted, indicating that it should be structured
            the same as the pivoted data used for training(exclusive of the last data partition column).
            In this case, the following parameters become ineffective:``key``, ``features``, ``group_key``.

        key : str, optional
            Name of the ID column.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        features : ListOfString or str, optional
            Names of feature columns.

            Defaults to all non-ID, non-label columns if not provided.
        label : str, optional
            Name of the label column.

            Defaults to the last non-ID column if not provided.
        group_key : str, optional
            The column of group_key. Data type can be INT or NVARCHAR/VARCHAR.
            If data type is INT, only parameters set in the group_params are valid.

            This parameter is only valid when ``massive`` is set as True in class instance initialization.

            Defaults to the first column of data if the index columns of data is not provided.
            Otherwise, defaults to the first column of index columns.

        group_params : dict, optional
            If massive mode is activated (``massive`` is True in class instance initialization), input data
            for classification shall be divided into different groups with different
            classification parameters applied. This parameter specifies the parameter
            values of the chosen classification algorithm ``func`` in score() w.r.t. different groups in a dict format,
            where keys corresponding to ``group_key`` while values should be a dict for classification algorithm
            parameter value assignments.

            An example is as follows:

            .. only:: latex

                >>> uc = UnifiedClassification(func='logisticregression',
                                               multi_class=False,
                                               massive=True,
                                               max_iter=10,
                                               group_params={'Group_1': {'solver': 'auto'}})
                >>> uc.fit(data=df,
                           key='ID',
                           features=["OUTLOOK" ,"TEMP", "HUMIDITY","WINDY"],
                           label="CLASS",
                           group_key="GROUP_ID",
                           background_size=4,
                           group_params={'Group_1':{'background_random_state':2}})
                >>> res = uc.score(data=score_df,
                                   key='ID',
                                   group_key='GROUP_ID',
                                   group_params={'Group_1':{'class_map0':'A', 'class_map1':'B'},
                                                 'Group_2':{'class_map0':'C', 'class_map1':'D'}})

            .. raw:: html

                <iframe allowtransparency="true" style="border:1px solid #ccc; background: #eeffcb;"
                    src="../../_static/uc_score_example.html" width="100%" height="100%" sandbox="">
                </iframe>

            Valid only when ``massive`` is set as True in class instance initialization.

            Defaults to None.

        model : DataFrame, optional
            A fitted classification model.

            Defaults to the self.model\_.

        thread_ratio : float, optional
            Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
            Values outside the range will be ignored and this function heuristically determines the number of threads to use.

            Defaults to the PAL's default value.
        max_result_num : int, optional
            Specifies the output number of prediction results.

        label : str, optional
            The setting of the parameter should be same with the one in train.
        ntiles : int, optional
            Used to control the population tiles in metrics output.

            The value should be at least 1 and no larger than the row size of the input data
        class_map0 : str, optional
            Specifies the label value which will be mapped to 0 in logistic regression.

            Valid only for logistic regression models when label variable is of VARCHAR or NVARCHAR type.

            No default value.
        class_map1 : str, optional
            Specifies the label value which will be mapped to 1 in logistic regression.

            Valid only for logistic regression models when label variable is of VARCHAR or NVARCHAR type.

            No default value.
        alpha : float, optional
            Specifies the value for laplace smoothing.

            - 0: Disables Laplace smoothing.
            - Other positive values: Enables Laplace smoothing for discrete values.

            Valid only for Naive Bayes models.

            Defaults to 0.
        block_size : int, optional
            Specifies the number of data loaded per time during scoring.

            - 0: load all data once.
            - Other positive Values: the specified number.

            Valid only for RandomDecisionTree and HybridGradientBoostingTree models.

            Defaults to 0.
        missing_replacement : str, optional
            Specifies the strategy for replacement of missing values in prediction data.

            - 'feature_marginalized': marginalises each missing feature out independently.
            - 'instance_marginalized': marginalises all missing features in an instance as a whole corresponding to each category.

            Valid only when ``impute`` is False, and only for RandomDecisionTree and HybridGradientBoostingTree models.

            Defaults to 'feature_marginalized'.

        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.
        top_k_attributions : int, optional
            Specifies the number of features with highest attributions to output.

            Defaults to 10.

        attribution_method : {'no', 'saabas', 'tree-shap'}, optional
            Specifies which method to use for tree-based model reasoning.

            - 'no' : No reasoning.
            - 'saabas' : Saabas method.
            - 'tree-shap' : Tree SHAP method.

            Valid only for tree-based models, i.e. DecisionTree, RandomDecisionTree and HybridGradientBoostingTree models.
            For such models, users should explicitly specify either 'saabas' or 'tree-shap' as the attribution method
            in order to activate SHAP explanations in the scoring result.

            Defaults to 'tree-shap'.

        sample_size : int, optional
            Specifies the number of sampled combinations of features.

            - 0 : Heuristically determined by algorithm.
            - Others : The specified sample size.

            Valid only for Naive Bayes, Support Vector Machine, Multilayer Perceptron and
            Multi-class Logistic Regression models.

            Defaults to 0.

            .. note::
                ``top_k_attributions``, ``attribution_method`` and ``sample_size`` are core parameters
                for `local interpretability of models` in hana_ml.algorithms.pal package, please see
                :ref:`local_interpretability-label` for more details.

        random_state : int, optional
            Specifies the seed for random number generator when sampling the combination of features.

            - 0 : User current time as seed.
            - Others : The actual seed.

            Valid only for Naive Bayes, Support Vector Machine, Multilayer Perceptron and
            Multi-class Logistic Regression models.

            Defaults to 0.

        impute : bool, optional
            Specifies whether or not to handle missing values in the data for scoring.

            Defaults to False.

        strategy, strategy_by_col, als_* : parameters for missing value handling, optional
            All these parameters mentioned above are for handling missing values
            in data, please see :ref:`impute_params-label` for more details.

            All parameters are valid only when ``impute`` is set as True.

        ignore_unknown_category : bool, optional
            Specifies whether or not to ignore unknown category value.

            - False : Report error if unknown category value is found.
            - True : Ignore unknown category value if there is any.

            Valid only when the model for scoring is multi-class logistic regression.

            Defaults to True.

        positive_label : str, optional
            Signifies the name of the positive label in binary classification.

            Only valid when ``func`` is "decisiontree", "hybridgradientboostingtree" and "randomdecisiontree".

            This is a new parameter availible in HANA PAL 2024 QRC2.

            Defaults to 0

        shap_value_for_positive_only : bool, optional
            Specifies if SHAP value always refers to probability of positive or refers to probability of predicted label.

            Only valid when ``func`` is "hybridgradientboostingtree".

            This is a new parameter availible in HANA PAL 2024 QRC2.

            Defaults to True.

        Returns
        -------
        A list of DataFrames

        - Prediction result by ignoring the true labels of the input data,
          structured the same as the result table of predict() function.
        - Statistics
        - Confusion matrix
        - Metrics
        - Error message (optional). Only valid if ``massive`` is True when initializing an 'UnifiedClassification' instance.
        """
        setattr(self, 'hanaml_score_params', pal_param_register())
        setattr(self, 'testing_data', data)
        if model is None and getattr(self, 'model_') is None:
            raise FitIncompleteError()
        conn = data.connection_context
        require_pal_usable(conn)
        #method_map = {'no': 0, 'saabas': 1, 'tree-shap': 2}
        group_params = self._arg('group_params', group_params, dict)
        if group_params:
            for group in group_params:
                self._arg(self.func + ' Parameters with group_key ' + str(group),
                          group_params[group], dict)

        cols = data.columns
        index = data.index
        group_key_type = None
        group_id = []
        if not self.pivoted:
            if self.massive is True:
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
                param_keys = list(self.group_params.keys())
                if not self._disable_hana_execution:
                    gid_type = data[[group_key]].dtypes()[0]
                    if not all([(int(ky) if 'INT' in gid_type[1] else ky) in data_groups for ky in param_keys]):
                        msg = 'Invalid group key identified in group parameters!'
                        logger.error(msg)
                        raise ValueError(msg)
                else:
                    gid_type = {tp[0]:tp for tp in data.dtypes()}[group_key]
                group_key_type = "VARCHAR(5000)"
                group_id = [group_key]
                cols.remove(group_key)

                key = self._arg('key', key, str)
                if index is not None:
                    key = _col_index_check(key, 'key', index[1], cols)
                else:
                    key = self._arg('key', key, str, required=True)
            else: # single mode
                key = self._arg('key', key, str)
                if index is not None:
                    key = _col_index_check(key, 'key', index, cols)
                else:
                    key = self._arg('key', key, str, required=True)

                if key is not None and key not in cols:
                    msg = "Please select key from {}!".format(cols)
                    logger.error(msg)
                    raise ValueError(msg)
                cols.remove(key)

            if isinstance(features, str):
                features = [features]
            features = self._arg('features', features, ListOfStrings)
            label = self._arg('label', label, str)
            if label is None:
                label = cols[-1]
            cols.remove(label)
            label_t = data.dtypes([label])[0][1]
            if features is None:
                features = cols
            data_ = data[group_id + [key] + features + [label]]
        else:
            data_ = data

        score_params = {"thread_ratio" : thread_ratio,
                        "max_result_num" : max_result_num,
                        "ntiles" : ntiles,
                        "class_map1" : class_map1,
                        "class_map0" : class_map0,
                        "alpha" : alpha,
                        "block_size" : block_size,
                        "missing_replacement" : missing_replacement,
                        "top_k_attributions" : top_k_attributions,
                        "attribution_method" : attribution_method,
                        "sample_size" : sample_size,
                        "random_state" : random_state,
                        "ignore_unknown_category" : ignore_unknown_category,
                        'positive_label' : positive_label,
                        'shap_value_for_positive_only': shap_value_for_positive_only}
        score_impute_params = {"impute" : impute,
                               "strategy" : strategy,
                               'strategy_by_col' : strategy_by_col,
                               "als_factors" : als_factors,
                               "als_lambda" : als_lambda,
                               "als_maxit" : als_maxit,
                               "als_randomstate" : als_randomstate,
                               "als_exit_threshold" : als_exit_threshold,
                               "als_exit_interval" : als_exit_interval,
                               "als_linsolver" : als_linsolver,
                               "als_cg_maxit" : als_cg_maxit,
                               "als_centering" : als_centering,
                               "als_scaling" : als_scaling}

        score_params = _delete_none_key_in_dict(score_params)
        score_impute_params = _delete_none_key_in_dict(score_impute_params)

        self.__pal_score_params = {}
        if self.massive is not True:
            score_map = {**self.__predict_score_param_dict}
            self.__pal_score_params = _params_check(input_dict=score_params,
                                                    param_map=score_map,
                                                    func=self.real_func)
            param_rows = [('FUNCTION', None, None, self.real_func)]
            update_impute_params = score_impute_params
            impute_map = {**self.__impute_dict}
            pal_impute_params = _params_check(input_dict=update_impute_params,
                                              param_map=impute_map,
                                              func=self.real_func)
            self.__pal_score_params.update(pal_impute_params)

            for name in self.__pal_score_params:
                value, typ = self.__pal_score_params[name]
                tpl = [self.__map_param(name, value, typ)]
                param_rows.extend(tpl)
            if not self.pivoted:
                categorical_variable = _categorical_variable_update(categorical_variable, label_t, label)
            if categorical_variable is not None:
                param_rows.extend([('CATEGORICAL_VARIABLE', None, None, var) for var in categorical_variable])

            impute_val = update_impute_params.get('impute')
            strategy_by_col_val = update_impute_params.get('strategy_by_col')
            if impute_val is True and strategy_by_col_val is not None:
                for col_imp_type in strategy_by_col_val:
                    imp_type = self._arg('Imputation type', col_imp_type[1], self.__column_imputation_map)
                    if len(col_imp_type) == 2:
                        param_rows.extend([('{}_IMPUTATION_TYPE'.format(col_imp_type[0]),
                                            imp_type, None, None)])
                    elif len(col_imp_type) == 3:
                        if imp_type == 101:
                            param_rows.extend([('{}_IMPUTATION_TYPE'.format(col_imp_type[0]),
                                                imp_type, None, str(col_imp_type[2]))])
                        else:
                            param_rows.extend([('{}_IMPUTATION_TYPE'.format(col_imp_type[0]),
                                                imp_type, col_imp_type[2], None)])
                    else:
                        continue
        else: # massive mode
            group_params = self._arg('group_params', group_params, dict)
            self.__pal_score_params = {}

            # for general params
            special_group_name = 'PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID'
            param_rows = []
            score_map = {**self.__predict_score_param_dict, **self.__impute_dict}
            general_params = {}
            general_params = _params_check(input_dict={**score_params, **score_impute_params},
                                           param_map=score_map,
                                           func=self.real_func)
            g_params = general_params
            hmv_val = g_params.get('HANDLE_MISSING_VALUE')
            if hmv_val is None or hmv_val[0] is not True:
                del g_params['HANDLE_MISSING_VALUE']

            if 'INT' in group_key_type and (g_params or impute or strategy_by_col):
                warn_msg = "If the type of group_key is INTEGER, only parameters in group_params are valid!"
                logger.warning(warn_msg)

            if general_params:
                self.__pal_score_params[special_group_name] = general_params

            if 'INT' not in group_key_type and not self.pivoted:
                categorical_variable = _categorical_variable_update(categorical_variable, label_t, label)
                if categorical_variable is not None:
                    param_rows.extend([(special_group_name, 'CATEGORICAL_VARIABLE', None, None, var) for var in categorical_variable])
                if impute is True and strategy_by_col is not None:
                    for col_imp_type in strategy_by_col:
                        imp_type = self._arg('Imputation type', col_imp_type[1], self.__column_imputation_map)
                        if len(col_imp_type) == 2:
                            param_rows.extend([(special_group_name, '{}_IMPUTATION_TYPE'.format(col_imp_type[0]),
                                                imp_type, None, None)])
                        elif len(col_imp_type) == 3:
                            if imp_type == 101:
                                param_rows.extend([(special_group_name, '{}_IMPUTATION_TYPE'.format(col_imp_type[0]),
                                                    imp_type, None, str(col_imp_type[2]))])
                            else:
                                param_rows.extend([(special_group_name, '{}_IMPUTATION_TYPE'.format(col_imp_type[0]),
                                                    imp_type, col_imp_type[2], None)])
                        else:
                            continue

            # for each group
            if group_params:
                for group in group_params:
                    if group in ['PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID']:
                        continue
                    group_val = int(group) if 'INT' in group_key_type else group
                    if 'INT' not in group_key_type:
                        param_rows.extend([(group_val, 'FUNCTION', None, None, self.real_func)])
                    each_group_params = {}
                    each_group_params = _params_check(input_dict=group_params[group],
                                                      param_map=score_map,
                                                      func=self.real_func)
                    if each_group_params:
                        self.__pal_score_params[group] = each_group_params

                    group_categorical_variable = None
                    if group_params[group].get('categorical_variable') is not None:
                        group_categorical_variable = group_params[group]['categorical_variable']
                    if not self.pivoted:
                        group_categorical_variable = _categorical_variable_update(group_categorical_variable, label_t, label)
                    if group_categorical_variable:
                        param_rows.extend([(group_val, 'CATEGORICAL_VARIABLE', None, None, var) for var in group_categorical_variable])

                    impute_val = group_params[group].get('impute')
                    strategy_by_col_val = group_params[group].get('strategy_by_col')
                    if impute_val is True and strategy_by_col_val is not None:
                        for col_imp_type in strategy_by_col_val:
                            imp_type = self._arg('Imputation type', col_imp_type[1], self.__column_imputation_map)
                            if len(col_imp_type) == 2:
                                param_rows.extend([(group_val, '{}_IMPUTATION_TYPE'.format(col_imp_type[0]),
                                                    imp_type, None, None)])
                            elif len(col_imp_type) == 3:
                                if imp_type == 101:
                                    param_rows.extend([(group_val, '{}_IMPUTATION_TYPE'.format(col_imp_type[0]),
                                                        imp_type, None, str(col_imp_type[2]))])
                                else:
                                    param_rows.extend([(group_val, '{}_IMPUTATION_TYPE'.format(col_imp_type[0]),
                                                        imp_type, col_imp_type[2], None)])
                            else:
                                continue

            for group in self.__pal_score_params:
                is_special_group = False
                if group in ['PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID']:
                    group_val = 'PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID'
                    is_special_group = True
                else:
                    group_val = int(group) if 'INT' in group_key_type else group
                if 'INT' in group_key_type and is_special_group is True:
                    continue
                if self.__pal_score_params[group]:
                    if 'INT' not in group_key_type:
                        param_rows.extend([(group_val, 'FUNCTION', None, None, self.real_func)])
                    for name in self.__pal_score_params[group]:
                        value, typ = self.__pal_score_params[group][name]
                        tpl = [tuple([group_val] + list(self.__map_param(name, value, typ)))]
                        param_rows.extend(tpl)

            # if group_key is INT, need to specific key for all group is key or not
            if 'INT' in group_key_type:
                for each in data_groups:
                    each_val = int(each)
                    param_rows.extend([(each_val, 'FUNCTION', None, None, self.real_func)])
            if 'INT' not in group_key_type:
                param_rows.extend([('PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID', 'FUNCTION', None, None, self.real_func)])

        if model is None:
            if isinstance(self.model_, (list, tuple)):
                model = self.model_[0]
            else:
                model = self.model_

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        if self.massive is True:
            outputs = ['RESULT', 'STATS', 'CONFUSION_MATRIX', 'METRICS', 'ERROR_MSG']
            outputs_massive = ['#PAL_UNIFIED_MASSIVE_CLASSIFICATION_SCORE_{}_TBL_{}_{}'.format(name, self.id, unique_id)
                               for name in outputs]
            result_tbl, stats_tbl, cm_tbl, metrics_tbl, errmsg_tbl = outputs_massive
            score_output_signature = [
                    {"GROUP_ID": "NVARCHAR(256)", "ID": "NVARCHAR(256)", "SCORE": "NVARCHAR(256)", "CONFIDENCE": "DOUBLE", "REASON CODE": "NCLOB"},
                    {"GROUP_ID": "NVARCHAR(256)", "STAT_NAME": "NVARCHAR(256)", "STAT_VALUE": "NVARCHAR(1000)", "CLASS_NAME": "NVARCHAR(256)"},
                    {"GROUP_ID": "NVARCHAR(256)", "ACTUAL_CLASS": "NVARCHAR(1000)", "PREDICTED_CLASS": "NVARCHAR(1000)", "COUNT": "INTEGER"},
                    {"GROUP_ID": "NVARCHAR(256)", "NAME": "NVARCHAR(256)", "X": "DOUBLE", "Y": "DOUBLE"},
                    {"GROUP_ID": "NVARCHAR(256)", "ERROR_TIMESTAMP": "NVARCHAR(256)", "ERRORCODE": "INTEGER", "MASSAGE":"NVARCHAR(1000)"}
                    ]
            setattr(self, "score_output_signature", score_output_signature)
            # SQLTRACE
            conn.sql_tracer.set_sql_trace(self, self.__class__.__name__, 'Score')
            if not (check_pal_function_exist(conn, '%UNIFIED_MASSIVE%', like=True) or self._disable_hana_execution):
                msg = 'The version of your SAP HANA does not support unified massive classification!'
                logger.error(msg)
                raise ValueError(msg)
            try:
                if self.pivoted:
                    self._call_pal_auto(conn,
                                        'PAL_UNIFIED_MASSIVE_CLASSIFICATION_SCORE_PIVOT',
                                        data,
                                        model,
                                        ParameterTable(itype=group_key_type).with_data(param_rows),
                                        *outputs_massive)
                else:
                    self._call_pal_auto(conn,
                                        'PAL_UNIFIED_MASSIVE_CLASSIFICATION_SCORE',
                                        data_,
                                        model,
                                        ParameterTable(itype=group_key_type).with_data(param_rows),
                                        *outputs_massive)
            except dbapi.Error as db_err:
                logger.exception(str(db_err))
                try_drop(conn, outputs_massive)
                raise
            except Exception as db_err:
                logger.exception(str(db_err))
                try_drop(conn, outputs_massive)
                raise
            err_msg = conn.table(errmsg_tbl)
            if not self._disable_hana_execution:
                if not err_msg.collect().empty:
                    row = err_msg.count()
                    for i in range(1, row+1):
                        warn_msg = "For group_key '{}',".format(err_msg.collect()[group_key][i-1]) +\
                                   " the error message is '{}'.".format(err_msg.collect()['MESSAGE'][i-1]) +\
                                   "More information could be seen in the 5th return Dataframe!"
                        logger.warning(warn_msg)
            result_df = conn.table(result_tbl)
            stats_df = conn.table(stats_tbl)
            cm_df = conn.table(cm_tbl)
            metrics_df = conn.table(metrics_tbl)
            self.scoring_list_ = [result_df, stats_df, cm_df, metrics_df]
            return result_df, stats_df, cm_df, metrics_df, err_msg

        outputs = ['RESULT', 'STATS', 'CONFUSION_MATRIX', 'METRICS']
        outputs = ['#PAL_UNIFIED_CLASSIFICATION_SCORE_{}_TBL_{}_{}'.format(name, self.id, unique_id)
                   for name in outputs]
        result_tbl, stats_tbl, cm_tbl, metrics_tbl = outputs
        score_output_signature = [
                    {"ID": "NVARCHAR(256)", "SCORE": "NVARCHAR(256)", "CONFIDENCE": "DOUBLE", "REASON CODE": "NCLOB"},
                    {"STAT_NAME": "NVARCHAR(256)", "STAT_VALUE": "NVARCHAR(1000)", "CLASS_NAME": "NVARCHAR(256)"},
                    {"ACTUAL_CLASS": "NVARCHAR(1000)", "PREDICTED_CLASS": "NVARCHAR(1000)", "COUNT": "INTEGER"},
                    {"NAME": "NVARCHAR(256)", "X": "DOUBLE", "Y": "DOUBLE"}
                    ]
        setattr(self, "score_output_signature", score_output_signature)
        # SQLTRACE
        conn.sql_tracer.set_sql_trace(self, self.__class__.__name__, 'Score')
        try:
            if self.pivoted:
                self._call_pal_auto(conn,
                                    'PAL_UNIFIED_CLASSIFICATION_SCORE_PIVOT',
                                    data,
                                    model,
                                    ParameterTable().with_data(param_rows),
                                    *outputs)
            else:
                self._call_pal_auto(conn,
                                    'PAL_UNIFIED_CLASSIFICATION_SCORE',
                                    data_,
                                    model,
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
        result_df = conn.table(result_tbl)
        stats_df = conn.table(stats_tbl)
        cm_df = conn.table(cm_tbl)
        metrics_df = conn.table(metrics_tbl)
        self.scoring_list_ = [result_df, stats_df, cm_df, metrics_df]
        return result_df, stats_df, cm_df, metrics_df

    def build_report(self):
        """
        Build the model report.

        Examples
        --------
        >>> from hana_ml.visualizers.unified_report import UnifiedReport
        >>> hgc = UnifiedClassification('HybridGradientBoostingTree')
        >>> hgc.fit(data=diabetes_train, key= 'ID', label='CLASS',
                    partition_method='stratified', partition_random_state=1,
                    stratified_column='CLASS')
        >>> hgc.build_report()
        >>> UnifiedReport(hgc).display()
        """
        try:
            rowlist = []
            for key, val in self.hanaml_parameters["kwargs"].items():
                rowlist.append({"KEY": key, "VALUE": str(val)})
            rowlist.append({"KEY": "func", "VALUE": self.func})
            parameter_df = pd.DataFrame(rowlist)
            # pylint: disable=protected-access
            if self.model_[5].filter("NAME='ROC_FPR'").count() > 200:
                if self.roc_sampling is None and self.other_samplings is None:
                    roc_sampling = Sampling('simple_random_without_replacement', sampling_size=200)
                    other_samplings = {'CUMGAINS':Sampling('simple_random_without_replacement', sampling_size=200),
                                       'RANDOM_CUMGAINS':Sampling('simple_random_without_replacement', sampling_size=200),
                                       'PERF_CUMGAINS':Sampling('simple_random_without_replacement', sampling_size=200),
                                       'LIFT':Sampling('simple_random_without_replacement', sampling_size=200),
                                       'RANDOM_LIFT':Sampling('simple_random_without_replacement', sampling_size=200),
                                       'PERF_LIFT':Sampling('simple_random_without_replacement', sampling_size=200),
                                       'CUMLIFT':Sampling('simple_random_without_replacement', sampling_size=200),
                                       'RANDOM_CUMLIFT':Sampling('simple_random_without_replacement', sampling_size=200),
                                       'PERF_CUMLIFT':Sampling('simple_random_without_replacement', sampling_size=200)}
                    self.set_metric_samplings(roc_sampling=roc_sampling, other_samplings=other_samplings)
                    self.auto_metric_sampling = True
            if self.scoring_list_ and len(self.scoring_list_) == 4:
                self._set_scoring_statistic_table(self.scoring_list_[1]) \
                    ._set_scoring_confusion_matrix_table(self.scoring_list_[2]) \
                    ._set_scoring_metric_table(self.scoring_list_[3])
            self._set_statistic_table(self.model_[1]) \
                ._set_parameter_table(parameter_df) \
                ._set_optimal_parameter_table(self.model_[2].collect()) \
                ._set_confusion_matrix_table(self.model_[3]) \
                ._set_variable_importance_table(self.model_[4]) \
                ._set_metric_table(self.model_[5]) \
                ._render_report()
        except Exception as err:
            logger.error(str(err))
            raise
        return self

    def create_amdp_class(self,#pylint:disable=too-many-branches
                          amdp_name,
                          training_dataset='',
                          apply_dataset='',
                          num_reason_features=3):
        """
        Create AMDP class file. Then build_amdp_class can be called to generate amdp class.

        Parameters
        ----------
        training_dataset : str, optional
            Name of training dataset.

            Defaults to ''.
        apply_dataset : str, optional
            Name of apply dataset.

            Defaults to ''.
        num_reason_features : int, optional
            The number of features that contribute to the classification decision the most.
            This reason code information is to be displayed during the prediction phase.

            Defaults to 3.
        """
        self.add_amdp_template("tmp_hemi_unified_classification_func.abap")
        self.add_amdp_name(amdp_name)
        self.load_abap_class_mapping()
        fit_data_struct = ''
        fit_data_st = {}
        if hasattr(self, "fit_data_struct"):
            fit_data_st = self.fit_data_struct
        if hasattr(self, "fit_data"):
            if self.fit_data:
                fit_data_st = self.fit_data.get_table_structure()
        if fit_data_st.keys():
            for key, val in fit_data_st.items():
                fit_data_struct = fit_data_struct + " " * 8 + "{} TYPE {},\n".format(key.lower(),
                                                                                     self.abap_class_mapping(val))
            self.add_amdp_item("<<TRAIN_INPUT_STRUCTURE>>",
                               fit_data_struct[:-1])
        self.add_amdp_item("<<CAST_TARGET_OUTPUT>>", '')
        self.add_amdp_item("<<RESULT_OUTPUT_STRUCTURE>>",
                           " " * 8 + "id TYPE int4,\n" +\
                           " " * 8 + "score TYPE int4,\n" +\
                           " " * 8 + "confidence TYPE f,")
        reasoncode_struct = ''
        for num in range(0, num_reason_features):
            reasoncode_struct = reasoncode_struct + " " * 8 +\
                        "reason_code_feature_{} TYPE shemi_reason_code_feature_name,\n".format(num + 1) +\
                        " " * 8 +"reason_code_percentage_{} TYPE shemi_reason_code_feature_pct,\n".format(num + 1)
        self.add_amdp_item("<<REASON_CODE_STRUCTURE>>",
                           reasoncode_struct[:-1])
        self.add_amdp_item("<<TRAINING_DATASET>>",
                           training_dataset)
        self.add_amdp_item("<<APPLY_DATASET>>",
                           apply_dataset)
        param_meta = []
        param_default_meta = []
        for fit_param in self.get_fit_parameters():
            param_meta.append("( name = '{}' type = cl_hemi_constants=>cs_param_type-string role = cl_hemi_constants=>cs_param_role-train configurable = abap_true has_context = abap_false )".format(fit_param[0]))
            param_default_meta.append("( name = '{}' value = '{}' )".format(fit_param[0], fit_param[1]))
        if self.get_predict_parameters():
            for predict_param in self.get_predict_parameters():
                param_meta.append("name = '{}' type = cl_hemi_constants=>cs_param_type-string role = cl_hemi_constants=>cs_param_role-apply configurable = abap_true has_context = abap_false )".format(predict_param[0]))
                param_default_meta.append("( name = '{}' value = '{}' )".format(predict_param[0], predict_param[1]))
        self.add_amdp_item("<<PARAMETER>>",
                           "( {} )".format("\n".join(param_meta)))
        self.add_amdp_item("<<PARAMETER_DEFAULT>>",
                           "( {} )".format("\n".join(param_default_meta)))
        self.add_amdp_item("<<TARGET_COLUMN>>",
                           self.label)
        self.add_amdp_item("<<KEY_FIELD_DESCRIPTION>>",
                           '')
        predict_data_cols = ''
        predict_data_st = {}
        if hasattr(self, "predict_data_struct"):
            predict_data_st = self.predict_data_struct
        if hasattr(self, "predict_data"):
            if self.predict_data:
                predict_data_st = self.predict_data.get_table_structure()
        if predict_data_st.keys():
            for key, val in predict_data_st.items():
                predict_data_cols = predict_data_cols + " " * 16 + "{},\n".format(key.lower())
            self.add_amdp_item("<<PREDICT_DATA_COLS>>",
                               predict_data_cols[:-2])
        result_field = []
        result_field.append('cast(result.ID as "$ABAP.type( INT4 )") as ID,')
        result_field.append(" " * 23 + 'cast(result.SCORE as "$ABAP.type( INT4 )") as SCORE,')
        result_field.append(" " * 23 + 'cast(result.CONFIDENCE as "$ABAP.type( FLTP )") as CONFIDENCE,')
        self.add_amdp_item("<<RESULT_FIELDS>>",
                           "\n".join(result_field)[:-1])
        reasoncode_result = ''
        for num in range(0, num_reason_features):
            reasoncode_result = reasoncode_result + " " * 23 +\
                        "trim(both '\"' from json_query(result.reason_code, '$[{}].attr')) as reason_code_feature_{},\n".format(num, num + 1) +\
                        " " * 23 +"json_query(result.reason_code, '$[{}].pct' ) as reason_code_percentage_{},\n".format(num, num + 1)
        self.add_amdp_item("<<RESULT_REASON_CODE_FIELDS>>",
                           reasoncode_result[:-2])
        return self

    def create_model_state(self, model=None, function=None,#pylint:disable=too-many-positional-arguments
                           pal_funcname='PAL_UNIFIED_CLASSIFICATION',
                           state_description=None, force=False):
        r"""
        Create PAL model state.

        Parameters
        ----------
        model : DataFrame, optional
            Specify the model for AFL state.

            Defaults to self.model\_.

        function : str, optional
            Specify the function name of the classification algorithm..

            Defaults to self.real_func

        pal_funcname : int or str, optional
            PAL function name.

            Defaults to 'PAL_UNIFIED_CLASSIFICATION'.

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
