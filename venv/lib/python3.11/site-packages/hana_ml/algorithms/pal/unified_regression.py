"""
This module contains Python wrapper for SAP HANA PAL unified-regression.

The following classes are available:
    * :class:`UnifiedRegression`
"""
#pylint: disable=too-many-lines
#pylint: disable=line-too-long, too-many-nested-blocks
#pylint: disable=too-many-locals, consider-using-dict-items
#pylint: disable=too-many-arguments, too-many-function-args
#pylint: disable=ungrouped-imports, unused-private-member
#pylint: disable=relative-beyond-top-level, invalid-name
#pylint: disable=no-member
#pylint: disable=consider-iterating-dictionary
#pylint: disable=too-many-instance-attributes, c-extension-no-member
#pylint: disable=too-many-branches, too-many-statements, unused-argument
import logging
import uuid
import json
import pandas as pd
from hdbcli import dbapi
from hana_ml.visualizers.model_report import _UnifiedRegressionReportBuilder
from hana_ml.ml_base import try_drop
from hana_ml.ml_exceptions import FitIncompleteError
from .utility import mlflow_autologging, check_pal_function_exist
from .tsa.arima import _delete_none_key_in_dict, _col_index_check
from .sqlgen import trace_sql
from .pal_base import (
    arg,
    PALBase,
    ParameterTable,
    require_pal_usable,
    pal_param_register,
    ListOfStrings,
    ListOfTuples
)
from .unified_classification import _key_index_check
logger = logging.getLogger(__name__)


def _params_check(input_dict, param_map, func):
    update_params = {}
    if not input_dict:
        return {}

    for parm in input_dict:
        if parm in ['categorical_variable', 'strategy_by_col']:
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
                err_msg = "'{}' is not a valid parameter name for initializing a {} model".format(parm, func)
                logger.error(err_msg)
                raise KeyError(err_msg)

        par_val = input_dict.get('partition_method')
        stra_val = input_dict.get('stratified_column')
        if par_val == "stratified" and stra_val is None:
            msg = "Please select stratified_column when you use stratified partition method!"
            logger.error(msg)
            raise ValueError(msg)

    return update_params


def _impute_dict_update(old_input_dict, new_input_dict):
    if old_input_dict is None:
        return new_input_dict
    if new_input_dict is None:
        return old_input_dict
    for key, value in new_input_dict.items():
        if value is not None:
            old_input_dict[key] = value
    return old_input_dict


class UnifiedRegression(PALBase, _UnifiedRegressionReportBuilder):
    """
    The Python wrapper for SAP HANA PAL Unified Regression function.

    Compared with the original regression interfaces,
    new features supported are listed below:

        - Regression algorithms easily switch
        - Dataset automatic partition
        - Model evaluation procedure provided
        - More metrics supported

    Parameters
    ----------

    func : str

        The name of a specified regression algorithm.

        The following algorithms(case-insensitive) are supported:

        - 'DecisionTree'
        - 'HybridGradientBoostingTree'
        - 'LinearRegression'
        - 'RandomDecisionTree'
        - 'MLP'
        - 'SVM'
        - 'GLM'
        - 'GeometricRegression'
        - 'PolynomialRegression'
        - 'ExponentialRegression'
        - 'LogarithmicRegression'
        - 'MLP_MultiTask'

    massive : bool, optional
        Specifies whether or not to use massive mode of unified regression.

        - True : massive mode.
        - False : single mode.

        For parameter setting in massive mode, you could use both
        group_params (please see the example below) or the original parameters.
        Using original parameters will apply for all groups. However, if you define some parameters of a group,
        the value of all original parameter setting will be not applicable to such group.

        An example is as follows:

        .. only:: latex

            >>> ur = UnifiedRegression(func='DecisionTree',
                                       multi_class=True,
                                       massive=True,
                                       thread_ratio=0.5,
                                       group_params={{'Group_1': {'percentage':0.6}})
            >>> ur.fit(data=df,
                       key='ID',
                       features=["OUTLOOK" ,"TEMP", "HUMIDITY","WINDY"],
                       label="CLASS",
                       group_key="GROUP_ID",
                       background_size=4,
                       group_params={'Group_1': {'background_random_state':2}})

       .. raw:: html

            <iframe allowtransparency="true" style="border:1px solid #ccc; background: #eeffcb;"
                src="../../_static/ur_init_example1.html" width="100%" height="100%" sandbox="">
            </iframe>

        In this example, as 'percentage' is set in group_params for Group_1,
        parameter setting of 'thread_ratio' is not applicable to Group_1.

        Defaults to False.

    group_params : dict, optional
        If massive mode is activated (``massive`` is True), input data for regression shall be divided into different
        groups with different regression parameters applied. This parameter specifies the parameter
        values of the chosen regression algorithm ``func`` w.r.t. different groups in a dict format,
        where keys corresponding to ``group_key`` while values should be a dict for regression algorithm
        parameter value assignments.

        An example is as follows:

        .. only:: latex

            >>> ur = UnifiedRegression(func='DecisionTree',
                                       multi_class=True,
                                       massive=True,
                                       thread_ratio=0.5,
                                       group_params={{'Group_1': {'percentage':0.6},
                                                      'Group_2': {'percentage':0.8}})
            >>> ur.fit(data=df,
                       key='ID',
                       features=["OUTLOOK" ,"TEMP", "HUMIDITY","WINDY"],
                       label="CLASS",
                       group_key="GROUP_ID",
                       background_size=4,
                       group_params={'Group_1': {'background_random_state':2}})

        .. raw:: html

            <iframe allowtransparency="true" style="border:1px solid #ccc; background: #eeffcb;"
                src="../../_static/ur_init_example2.html" width="100%" height="100%" sandbox="">
            </iframe>

        Valid only when ``massive`` is True and defaults to None.

    pivoted : bool, optional
        If True, it will enable PAL unified regression for pivoted data. In this case,
        meta data must be provided in the fit function.

        Defaults to False.

    **kwargs : keyword arguments

        Arbitrary keyword arguments and please referred to the responding algorithm for the parameters' key-value pair.

        **Note that some parameters are disabled/modified in the regression algorithm!**

            - **'DecisionTree'** : :class:`~hana_ml.algorithms.pal.trees.DecisionTreeRegressor`

              - Disabled parameters: ``output_rules``
              - Parameters removed from initialization but can be specified in fit(): ``categorical_variable``

            - **'HybridGradientBoostingTree'** : :class:`~hana_ml.algorithms.pal.trees.HybridGradientBoostingRegressor`

              - Disabled parameters: ``calculate_importance``
              - Parameters removed from initialization but can be specified in fit(): ``categorical_variable``
              - Modified parameters: ``obj_func`` added 'quantile' as a new choice. This is for quantile regression.
                In particular, only under 'quantile' loss can interval prediction be made for HGBT model
                in predict/score phase.

            - **'LinearRegression'** : :class:`~hana_ml.algorithms.pal.linear_model.LinearRegression`

              - Disabled parameters: pmml_export
              - Parameters removed from initialization but can be specified in fit(): categorical_variable
              - Parameters with changed meaning : ``json_export``, where False value now means
                'Exports multiple linear regression model in PMML'.

            - **'RandomDecisionTree'** : :class:`~hana_ml.algorithms.pal.trees.RDTRegressor`

              - Disabled parameters: ``calculate_oob``
              - Parameters removed from initialization but can be specified in fit(): ``categorical_variable``

            - **'MLP'** : :class:`~hana_ml.algorithms.pal.neural_network.MLPRegressor`

              - Disabled parameters: ``functionality``
              - Parameters removed from initialization but can be specified in fit(): ``categorical_variable``

            - **'SVM'** : :class:`~hana_ml.algorithms.pal.svm.SVR`

              - Parameters removed from initialization but can be specified in fit(): ``categorical_variable``

            - **'GLM'** : :class:`~hana_ml.algorithms.pal.regression.GLM`

              - Disabled parameters: ``output_fitted``
              - Parameters removed from initialization but can be specified in fit(): ``categorical_variable``

            - **'GeometricRegression'** : :class:`~hana_ml.algorithms.pal.regression.BiVariateGeometricRegression`

              - Disabled parameters: ``pmml_export``

            - **'PolynomialRegression'** : :class:`~hana_ml.algorithms.pal.regression.PolynomialRegression`

              - Disabled parameters: ``pmml_export``

            - **'ExponentialRegression'** : :class:`~hana_ml.algorithms.pal.regression.ExponentialRegression`

              - Disabled parameters: ``pmml_export``

            - **'LogarithmicRegression'** : :class:`~hana_ml.algorithms.pal.regression.BiVariateNaturalLogarithmicRegression`

              - Disabled parameters: ``pmml_export``

            - **'MLP_MultiTask'** :class:`~hana_ml.algorithms.pal.neural_network.MLPMultiTaskRegressor`

                - Disabled parameters : ``finetune``.


        For more parameter mappings of hana_ml and HANA PAL, please refer to the doc page: :ref:`param_mapping`.

    Attributes
    ----------

    model_ : DataFrame
        Model content.

    statistics_ : DataFrame

        Names and values of statistics.

    optimal_param_ : DataFrame

        Provides optimal parameters selected.

        Available only when parameter selection is triggered.

    partition_ : DataFrame

        Partition result of training data.

        Available only when training data has an ID column and random partition is applied.

    error_msg_ : DataFrame

        Error message.
        Only valid if ``massive`` is True when initializing an 'UnifiedRegression' instance.

    Examples
    --------
    Case 1: Training data for regression:

    >>> df.collect()
      ID    X1 X2  X3       Y
    0  0  0.00  A   1  -6.879
    1  1  0.50  A   1  -3.449
    2  2  0.54  B   1   6.635
    3  3  1.04  B   1  11.844
    4  4  1.50  A   1   2.786
    5  5  0.04  B   2   2.389
    6  6  2.00  A   2  -0.011
    7  7  2.04  B   2   8.839
    8  8  1.54  B   1   4.689
    9  9  1.00  A   2  -5.507

    Create an UnifiedRegression instance for linear regression problem:

    >>> mlr_params = dict(solver = 'qr',
                          adjusted_r2=False,
                          thread_ratio=0.5)

    >>> umlr = UnifiedRegression(func='LinearRegression', **mlr_params)

    Fit the UnifiedRegression instance with the aforementioned training data:

    >>> par_params = dict(partition_method='random',
                          training_percent=0.7,
                          partition_random_state=2,
                          output_partition_result=True)

    >>> umlr.fit(data=df,
                 key='ID',
                 label='Y',
                 **par_params)

    Check the resulting statistics on testing data:

    >>> umlr.statistics_.collect()
            STAT_NAME          STAT_VALUE
    0       TEST_EVAR   0.871459247598903
    1        TEST_MAE  2.0088082000000003
    2       TEST_MAPE  12.260003987804756
    3  TEST_MAX_ERROR   5.329849599999999
    4        TEST_MSE   9.551661310681718
    5         TEST_R2  0.7774293644548433
    6       TEST_RMSE    3.09057621013974
    7      TEST_WMAPE  0.7188006440839695

    Data for prediction:

    >>> df_pred.collect()
      ID       X1 X2  X3
    0  0    1.690  B   1
    1  1    0.054  B   2
    2  2  980.123  A   2
    3  3    1.000  A   1
    4  4    0.563  A   1

    Perform predict():

    >>> pred_res = mlr.predict(data=df_pred, key='ID')
    >>> pred_res.collect()
       ID        SCORE UPPER_BOUND LOWER_BOUND REASON
    0   0     8.719607        None        None   None
    1   1     1.416343        None        None   None
    2   2  3318.371440        None        None   None
    3   3    -2.050390        None        None   None
    4   4    -3.533135        None        None   None

    Data for scoring:

    >>> df_score.collect()
       ID       X1 X2  X3    Y
    0   0    1.690  B   1  1.2
    1   1    0.054  B   2  2.1
    2   2  980.123  A   2  2.4
    3   3    1.000  A   1  1.8
    4   4    0.563  A   1  1.0

    Perform scoring:

    >>> score_res = umlr.score(data=df_score, key="ID", label='Y')

    Check the statistics on scoring data:

    >>> score_res[1].collect()
       STAT_NAME         STAT_VALUE
    0       EVAR  -6284768.906191169
    1        MAE   666.5116459919999
    2       MAPE   278.9837795885635
    3  MAX_ERROR  3315.9714402299996
    4        MSE   2199151.795823181
    5         R2   -7854112.55651136
    6       RMSE  1482.9537402842952
    7      WMAPE   392.0656741129411

    Case 2: UnifiedReport for UnifiedRegression is shown as follows:

    >>> hgr = UnifiedRegression(func='HybridGradientBoostingTree')
    >>> gscv = GridSearchCV(estimator=hgr,
                            param_grid={'learning_rate': [0.1, 0.4, 0.7, 1],
                                        'n_estimators': [4, 6, 8, 10],
                                        'split_threshold': [0.1, 0.4, 0.7, 1]},
                            train_control=dict(fold_num=5,
                                               resampling_method='cv',
                                               random_state=1),
                            scoring='rmse')
    >>> gscv.fit(data=diabetes_train, key= 'ID',
             label='CLASS',
             partition_method='random',
             partition_random_state=1,
             build_report=True)

    To see the model report:

    >>> UnifiedReport(gscv.estimator).display()

     .. image:: ../../image/unified_report_model_report_regression.png
        :align: center

    Case 3: Local interpretability of models - linear SHAP

    >>> umlr = UnifiedRegression(func='LinearRegression')
    >>> umlr.fit(data=df_train, background_size=4)#specify positive background data size to activate local interpretability
    >>> res = umlr.predict(data=df_predict,
    ...                    ...,
    ...                    top_k_attributions=5,
    ...                    sample_size=0,
    ...                    random_state=2022,
    ...                    ignore_correlation=False)#consider correlations between features, only for linear SHAP

    Case 4: Local interpretability of models - tree SHAP for tree model

    >>> udtr = UnifiedRegression(func='DecisionTree')
    >>> udtr.fit(data=df_train)
    >>> res = udtr.predict(data=df_predict,
    ...                    ...,
    ...                    top_k_attributions=8,
    ...                    attribution_method='tree-shap',#specify attribution method to activate local interpretability
    ...                    random_state=2022)

    Case 5: Local interpretability of models - kernel SHAP for non-linear/non-tree models

    >>> usvr = UnifiedRegression(func='SVM')# SVM model
    >>> usvr.fit(data=df_train, background_size=8)#specify positive background data size to activate local interpretability
    >>> res = usvr.predict(data=df_predict,
    ...                    ...,
    ...                    top_k_attributions=6,
    ...                    sample_size=6,
    ...                    random_state=2022)
    """
    func_dict = {
        'decisiontree' : 'DT',
        'hybridgradientboostingtree' : 'HGBT',
        'linearregression' : 'MLR',
        'randomforest' : 'RDT',
        'randomdecisiontree' : 'RDT',
        'svm' : 'SVM',
        'mlp' : 'MLP',
        'glm' : 'GLM',
        'geometricregression': 'GEO',
        'polynomialregression' : 'POL',
        'exponentialregression' : 'EXP',
        'logarithmicregression' : 'LOG',
        'mlp_multitask' : 'MLP_M_TASK'}
    __cv_dict = {'resampling_method' : ('RESAMPLING_METHOD',
                                        str, {'cv' : 'cv',
                                              'bootstrap' : 'bootstrap',
                                              'cv_sha' : 'cv_sha',
                                              'bootstrap_sha' : 'bootstrap_sha',
                                              'cv_hyperband' : 'cv_hyperband',
                                              'bootstrap_hyperband' : 'bootstrap_hyperband'}),
                 'random_state' : ('SEED', int),
                 'fold_num' : ('FOLD_NUM', int),
                 'repeat_times' : ('REPEAT_TIMES', int),
                 'search_strategy' : ('PARAM_SEARCH_STRATEGY', str,
                                      {'random' : 'random', 'grid' : 'grid'}),
                 'param_search_strategy' : ('PARAM_SEARCH_STRATEGY', str,
                                            {'random' : 'random', 'grid' : 'grid'}),
                 'random_search_times' : ('RANDOM_SEARCH_TIMES', int),
                 'timeout' : ('TIMEOUT', int),
                 'progress_indicator_id' : ('PROGRESS_INDICATOR_ID', str)}
    __param_grid_dict = {'param_values' : ('_VALUES', dict),
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
    __param_dict = {
        'adjusted_r2' : ('ADJUSTED_R2', bool),
        'decomposition' : ('DECOMPOSITION', int, {'lu' : 0, 'qr' : 1, 'svd' : 2, 'cholesky' : 5}),
        'thread_ratio' : ('THREAD_RATIO', float)}
    map_dict = {
        'GEO' : __param_dict,
        'LOG' : __param_dict,
        'EXP' : __param_dict,
        'POL' : dict({'degree' : ('DEGREE', int),
                      'degree_values' : ('DEGREE_VALUES', list),
                      'degree_range' : ('DEGREE_RANGE', list),
                      'evaluation_metric' : ('EVALUATION_METRIC', str, {'rmse' : 'RMSE'})},
                     **__param_dict),
        'GLM' : {
            'family' : ('FAMILY', str, {'gaussian' : 'gaussian',
                                        'normal' : 'gaussian',
                                        'poisson' : 'poisson',
                                        'binomial' : 'binomial',
                                        'gamma' : 'gamma',
                                        'inversegaussin' : 'inversegaussian',
                                        'negativebinomial' : 'negativebinomial',
                                        'ordinal' : 'ordinal'}),
            'link' : ('LINK', str, {'identity' : 'identity',
                                    'log' : 'log',
                                    'logit' : 'logit',
                                    'probit' : 'probit',
                                    'comploglog' : 'comploglog',
                                    'reciprocal' : 'inverse',
                                    'inverse' : 'inverse',
                                    'inversesquare' : 'inversesquare',
                                    'sqrt' : 'sqrt'}),
            'solver' : ('SOLVER', str, {'irls' : 'irls', 'nr' : 'nr', 'cd' : 'cd'}),
            'significance_level' : ('SIGNIFICANCE_LEVEL', float),
            'quasilikelihood' : ('QUASILIKELIHOOD', bool),
            'group_response' : ('GROUP_RESPONSE', bool),
            'handle_missing_fit' : ('HANDLE_MISSING_FIT', int, {'skip' : 1, 'abort' : 0, 'fill_zero' : 2}),
            'max_iter' : ('MAX_ITER', int),
            'tol' : ('TOL', float),
            'enet_alpha' : ('ALPHA', float),
            'enet_lambda' : ('LAMBDA', float),
            'alpha' : ('ALPHA', float),
            'lamb' : ('LAMBDA', float),
            'num_lambda' : ('NUM_LAMBDA', int),
            'ordering' : ('ORDERING', ListOfStrings),
            'thread_ratio' : ('THREAD_RATIO', float),
            'lambda_min_ratio' : ('LAMBDA_MIN_RATIO', float),
            'evaluation_metric' : ('EVALUATION_METRIC', str,
                                   {'rmse' : 'RMSE', 'mae' : 'MAE', 'error_rate' : 'ERROR_RATE'})
        },
        'MLR' : {
            'adjusted_r2' : ('ADJUSTED_R2', bool),
            'solver' : ('SOLVER', int, {'qr' : 1, 'svd' : 2,
                                        'cd' : 4, 'cyclical' : 4,
                                        'cholesky' : 5, 'admm' : 6}),
            'alpha_to_enter' : ('ALPHA_TO_ENTER', float),
            'alpha_to_remove' : ('ALPHA_TO_REMOVE', float),
            'bp_test' : ('BP_TEST', bool),
            'dw_test' : ('DW_TEST', bool),
            'enet_alpha' : ('ALPHA', float),
            'enet_lambda' : ('LAMBDA', float),
            'handle_missing' : ('HANDLE_MISSING', bool),
            'mandatory_feature' : ('MANDATORY_FEATURE', ListOfStrings),
            'ks_test' : ('KS_TEST', bool),
            'max_iter' : ('MAX_ITER', int),
            'intercept' : ('INTERCEPT', bool),
            'pho' : ('PHO', float),
            'reset_test' : ('RESET_TEST', int),
            'stat_inf' : ('STAT_INF', bool),
            'tol' : ('TOL', float),
            'thread_ratio' : ('THREAD_RATIO', float),
            'var_select' : ('VAR_SELECT', int, {'no' : 0, 'forward' : 1, 'backward' : 2, 'stepwise' : 3}),
            'evaluation_metric' : ('EVALUATION_METRIC', str, {'rmse' : 'RMSE'}),
            'json_export' : ('JSON_EXPORT', bool),
            'precompute_lms_sketch' : ('PRECOMPUTE_LMS_SKETCH', bool),
            'stable_sketch_alg' : ('STABLE_SKETCH_ALG', bool),
            'sparse_sketch_alg' : ('SPARSE_SKETCH_ALG', bool),
            'resource' : ('RESOURCE', str, {'max_iter': 'MAX_ITER'}),
            'max_resource' : ('MAX_RESOURCE', int),
            'reduction_rate' : ('REDUCTION_RATE', float),
            'aggressive_elimination' : ('AGGRESSIVE_ELIMINATION', bool),
            'ps_verbose' : ('PS_VERBOSE', bool)
            },
        'MLP' : {
            'activation' : ('ACTIVATION',
                            int,
                            __activation_map),
            'activation_options' : ('ACTIVATION_OPTIONS', ListOfStrings),
            'output_activation' : ('OUTPUT_ACTIVATION',
                                   int,
                                   __activation_map),
            'output_activation_options' : ('OUTPUT_ACTIVATION_OPTIONS', ListOfStrings),
            'hidden_layer_size' : ('HIDDEN_LAYER_SIZE', (list, tuple)),
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
            'evaluation_metric' : ('EVALUATION_METRIC', str, {'rmse' : 'RMSE'}),
            'reduction_rate' : ('REDUCTION_RATE', float),
            'aggressive_elimination' : ('AGGRESSIVE_ELIMINATION', bool)},
        'DT' : {
            'allow_missing_dependent' : ('ALLOW_MISSING_LABEL', bool),
            'percentage' : ('PERCENTAGE', float),
            'min_records_of_parent' : ('MIN_RECORDS_PARENT', int),
            'min_records_of_leaf' : ('MIN_RECORDS_LEAF', int),
            'max_depth' : ('MAX_DEPTH', int),
            'split_threshold' : ('SPLIT_THRESHOLD', float),
            'discretization_type' : ('DISCRETIZATION_TYPE', int, {'mdlpc' : 0, 'equal_freq' : 1}),
            'max_branch' : ('MAX_BRANCH', int),
            'merge_threshold' : ('MERGE_THRESHOLD', float),
            'use_surrogate' : ('USE_SURROGATE', bool),
            'model_format' : ('MODEL_FORMAT', int, {'json' : 1, 'pmml' : 2}),
            'thread_ratio' : ('THREAD_RATIO', float),
            'evaluation_metric' : ('EVALUATION_METRIC', str,
                                   {'rmse' : 'RMSE', 'mae' : 'MAE'})},
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
            'fittings_quantization' : ('FITTINGS_QUANTIZATION', bool),
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
            'thread_ratio' : ('THREAD_RATIO', float),
            'evaluation_metric' : ('EVALUATION_METRIC', str,
                                   {'rmse':'RMSE', 'mae':'MAE'}),
            'compression' : ('COMPRESSION', bool),
            'max_bits' : ('MAX_BITS', int),
            'obj_func' : ('OBJ_FUNC', int, {'se': 0, 'sle': 1,
                                            'ae': 10,
                                            'huber': 9,
                                            'pseudo-huber': 2,
                                            'gamma': 3, 'tweedie': 4,
                                            'quantile': 8}),
            'tweedie_power' : ('TWEEDIE_POWER', float),
            'huber_slope' : ('HUBER_SLOPE', float),
            'replace_missing' : ('REPLACE_MISSING', bool),
            'default_missing_direction' : ('DEFAULT_MISSING_DIRECTION', int,
                                           {'left' : 0, 'right' : 1}),
            'feature_grouping' : ('FEATURE_GROUPING', bool),
            'tol_rate' : ('TOLERANT_RATE', float),
            'max_bin_num' : ('MAX_BIN_NUM', int),
            'reduction_rate' : ('REDUCTION_RATE', float),
            'resource' : ('RESOURCE', {'data_size' : None, 'n_estimators' : 'N_ESTIMATORS'}),
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
            'use_vec_leaf' : ('USE_VEC_LEAF', bool),
            'pinball_delta' : ('PINBALL_DELTA', float)},
        'SVM' : {
            'c' : ('SVM_C', float),
            'kernel' : ('KERNEL_TYPE', int, {'linear' : 0, 'poly' : 1, 'rbf' : 2, 'sigmoid' : 3}),
            'degree' : ('POLY_DEGREE', int),
            'gamma' : ('RBF_GAMMA', float),
            'coef_lin' : ('COEF_LIN', float),
            'coef_const' : ('COEF_CONST', float),
            'shrink' : ('SHRINK', bool),
            'regression_eps' : ('REGRESSION_EPS', float),
            'compression' : ('COMPRESSION', bool),
            'max_bits' : ('MAX_BITS', int),
            'max_quantization_iter' : ('MAX_QUANTIZATION_ITER', int),
            'tol' : ('TOL', float),
            'evaluation_seed' : ('EVALUATION_SEED', int),
            'scale_label' : ('SCALE_LABEL', bool),
            'scale_info' : ('SCALE_INFO', int, {'no' : 0, 'standardization' : 1, 'rescale' : 2}),
            'handle_missing' : ('HANDLE_MISSING', bool),
            'category_weight' : ('CATEGORY_WEIGHT', float),
            'thread_ratio' : ('THREAD_RATIO', float),
            'evaluation_metric' : ('EVALUATION_METRIC', str, {'rmse' : 'RMSE'}),
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

    __column_imputation_map = {'non' : 0, 'delete' : 1,
                               'most_frequent' : 100,
                               'categorical_const' : 101,
                               'mean' : 200, 'median' : 201,
                               'numerical_const' : 203,
                               'als' : 204}
    __fit_param_dict = {
        'partition_method' : ('PARTITION_METHOD', int, {'no' : 0, 'predefined' : 1, 'random' : 2}),
        'partition_random_state' : ('PARTITION_RANDOM_SEED', int),
        'training_percent' : ('PARTITION_TRAINING_PERCENT', float),
        'output_partition_result' : ('OUTPUT_PARTITION_RESULT', bool),
        'background_size' : ('BACKGROUND_SIZE', int),
        'background_random_state' : ('BACKGROUND_SAMPLING_SEED', int),
        'output_coefcov' : ('OUTPUT_COEFCOV', bool),
        'output_leaf_values' : ('OUTPUT_LEAF_VALUES', bool),
        'significance_level' : ('SIGNIFICANCE_LEVEL', float),
        'ignore_zero' : ('IGNORE_ZERO', bool)
        }

    __permutation_imp_dict = {
        'permutation_importance' : ('PERMUTATION_IMPORTANCE', bool),
        'permutation_evaluation_metric' : ('PERMUTATION_EVALUATION_METRIC', str,
                                           {x:x.upper() for x in ['rmse', 'mae', 'mape']}),
        'permutation_n_repeats' : ('PERMUTATION_N_REPEATS', int),
        'permutation_seed' : ('PERMUTATION_SEED', int),
        'permutation_n_samples' : ('PERMUTATION_N_SAMPLES', int)}

    __impute_dict = {
        'impute' : ('HANDLE_MISSING_VALUE', bool),
        'strategy' : ('IMPUTATION_TYPE', int, {'non' : 0, 'delete' : 5,
                                               'most_frequent-mean' : 1, 'mean' : 1,
                                               'most_frequent-median' : 2, 'median' : 2,
                                               'most_frequent-zero' : 3, 'zero' : 3,
                                               'most_frequent-als' : 4, 'als' : 4}),
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

    __predict_score_param_dict = {
        'thread_ratio' : ('THREAD_RATIO', float),
        'prediction_type' : ('TYPE', str, {'response' : 'response', 'link' : 'link'}),
        'significance_level' : ('SIGNIFICANCE_LEVEL', float),
        'handle_missing' : ('HANDLE_MISSING', int, {'skip' : 1, 'fill_zero' : 2}),
        'block_size' : ('BLOCK_SIZE', int),
        'top_k_attributions' : ('TOP_K_ATTRIBUTIONS', int),
        'attribution_method' : ('FEATURE_ATTRIBUTION_METHOD', int, {'no' : 0, 'saabas' : 1, 'tree-shap' : 2}),
        'sample_size' : ('SAMPLESIZE', int),
        'random_state' : ('SEED', int),
        'ignore_correlation' : ('IGNORE_CORRELATION', bool),
        'interval_type' : ('INTERVAL', {'no': 0, 'confidence': 1, 'prediction': 2})}

    pal_funcname = 'PAL_MASSIVE_UNIFIED_REGRESSION'

    def __init__(self,
                 func,
                 massive=False,
                 group_params=None,
                 pivoted=False,
                 **kwargs):
        setattr(self, 'hanaml_parameters', pal_param_register())
        PALBase.__init__(self)
        _UnifiedRegressionReportBuilder.__init__(self, ["KEY", "VALUE"], ["KEY", "VALUE"])
        self.func = self._arg('Function name', func, self.func_dict)
        self.real_func = self.func
        self.params = {**kwargs}
        if self.func == 'RDT' and 'split_threshold' not in self.params:
            self.params['split_threshold'] = 1e-9
        # for massive mode
        self.massive = self._arg('massive', massive, bool)
        group_params = self._arg('group_params', group_params, dict)
        group_params = {} if group_params is None else group_params
        if group_params:
            for group in group_params:
                self._arg(self.func + ' Parameters with group_key ' + str(group),
                          group_params[group], dict)

        self.__pal_params = {}
        if self.func in ['DT', 'HGBT', 'SVM', 'MLR', 'GLM', 'MLP']:
            func_map = {**self.map_dict[self.func], **self.__cv_dict,
                        **self.__param_grid_dict}
        elif self.func == 'POL':
            func_map = {**self.map_dict[self.func], **self.__cv_dict}
        else:
            func_map = self.map_dict[self.func]

        if self.massive is not True:
            self.__pal_params = _params_check(input_dict=self.params, param_map=func_map, func=func)
        else: # massive mode
            self.group_params = group_params
            if self.group_params:
                for group in self.group_params:
                    self.__pal_params[group] = {}
                    self.__pal_params[group] = _params_check(input_dict=self.group_params[group],
                                                             param_map=func_map,
                                                             func=func)
            if self.params:
                special_group_name = 'PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID'
                self.__pal_params[special_group_name] = _params_check(input_dict=self.params,
                                                                      param_map=func_map,
                                                                      func=func)
        self.pivoted = self._arg('pivoted', pivoted, bool)
        self.model_ = None
        self.scoring_list_ = None
        self.statistics_ = None
        self.optimal_param_ = None
        self.partition_ = None
        self.error_msg_ = None
        self.__param_rows = None
        self._is_autologging = False
        self._autologging_model_storage_schema = None
        self._autologging_model_storage_meta = None
        self.fit_data = None
        self.predict_data = None
        self.label = None
        self.fit_params = None
        self.__pal_predict_params = None
        self.is_exported = False
        self.registered_model_name = None
        self.scoring_data_ = None

    def disable_mlflow_autologging(self):
        """
        It will disable mlflow autologging.
        """
        self._is_autologging = False

    def enable_mlflow_autologging(self, schema=None, meta=None, is_exported=False, registered_model_name=None):
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
        registered_model_name : str, optional
            MLFlow registered_model_name.
        """
        self._is_autologging = True
        self._autologging_model_storage_schema = schema
        self._autologging_model_storage_meta = meta
        self.is_exported = is_exported
        self.registered_model_name = registered_model_name

    def update_cv_params(self, name, value, typ):
        """
        Update parameters for model-evaluation/parameter-selection.
        """
        if name in self.__cv_dict.keys():
            self.__pal_params[self.__cv_dict[name][0]] = (value, typ)

    def __map_param(self, name, value, typ):#pylint:disable=no-self-use
        tpl = ()
        if typ in [int, bool]:
            if name == 'INTERCEPT':
                value = False if value is None else not value
            tpl = (name, value, None, None)
        elif typ == float:
            tpl = (name, None, value, None)
        elif typ in [str, ListOfStrings]:
            tpl = (name, None, None,
                   value.upper() if '_METRIC' in name else value)
        elif isinstance(typ, dict):
            val = value
            if isinstance(val, (int, float)):
                tpl = (name, val, None, None)
            else:
                tpl = (name, None, None, val)
        return tpl

    def __forecast(self,#pylint:disable=too-many-branches, too-many-statements
                   data,
                   predict=True,
                   key=None,
                   features=None,
                   label=None,
                   model=None,
                   thread_ratio=None,
                   prediction_type=None,
                   significance_level=None,
                   handle_missing=None,
                   block_size=None,
                   top_k_attributions=None,
                   attribution_method=None,
                   sample_size=None,
                   random_state=None,
                   ignore_correlation=None,
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
                   group_key=None,
                   group_params=None,
                   interval_type=None):
        predict_params = {'thread_ratio' : thread_ratio,
                          'prediction_type' : prediction_type,
                          'significance_level' : significance_level,
                          'handle_missing' : handle_missing,
                          'block_size' : block_size,
                          'top_k_attributions' : top_k_attributions,
                          'attribution_method' : attribution_method,
                          'sample_size' : sample_size,
                          'random_state' : random_state,
                          'ignore_correlation' : ignore_correlation,
                          'interval_type' : interval_type}
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

        if model is None and getattr(self, 'model_') is None:
            raise FitIncompleteError()
        conn = data.connection_context

        param_rows = []
        if not self.pivoted:
            cols = data.columns
            index = data.index
            group_id = []
            group_key_type = None
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
                    if key is None:
                        key = cols[0]
            else: # single mode
                index = data.index
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
            if not predict:
                if label is None:
                    label = cols[-1]
                cols.remove(label)
            if features is None:
                features = cols

            data_ = data[group_id + [key] + features + ([] if predict else [label])]
        else:
            data_ = data
        if model is None:
            if isinstance(self.model_, (list, tuple)):
                model = self.model_[0]
            else:
                model = self.model_

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

        # SQLTRACE
        conn.sql_tracer.set_sql_trace(self, self.__class__.__name__,
                                      'Predict' if predict else 'Score')
        fcn = "PREDICT" if predict else "SCORE"
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()

        if self.massive is not True:
            outputs = ['RESULT', 'PH' if predict else 'STATS']
            outputs = ['#PAL_UNIFIED_REGRESSION_{}_{}_TBL_{}_{}'.format(fcn, name, self.id, unique_id)
                       for name in outputs]
            result_tbl, stats_tbl = outputs
            self.real_func = self.func
            if fcn == 'PREDICT':
                predict_output_signature = [
                        {"ID": "NVARCHAR(1000)", "SCORE": "DOUBLE", "LOWER_BOUND": "DOUBLE", "UPPER_BOUND": "DOUBLE", "REASON": "NCLOB"},
                        {"OBJECT": "NVARCHAR(10)", "KEY": "NVARCHAR(10)", "VALUE": "NVARCHAR(10)"}
                        ]
                setattr(self, "predict_output_signature", predict_output_signature)
            else: # score
                score_output_signature = [
                        {"ID": "NVARCHAR(1000)", "SCORE": "DOUBLE", "LOWER_BOUND": "DOUBLE", "UPPER_BOUND": "DOUBLE", "REASON": "NCLOB"},
                        {"STAT_NAME": "NVARCHAR(100)", "STAT_VALUE": "NVARCHAR(1000)"}
                        ]
                setattr(self, "score_output_signature", score_output_signature)
            try:
                if self.pivoted:
                    setattr(self, 'predict_data', data)
                    self._call_pal_auto(conn,
                                        'PAL_UNIFIED_REGRESSION_{}_PIVOT'.format(fcn),
                                        data,
                                        model,
                                        ParameterTable().with_data(param_rows),
                                        *outputs)
                else:
                    setattr(self, 'predict_data', data_)
                    self._call_pal_auto(conn,
                                        'PAL_UNIFIED_REGRESSION_{}'.format(fcn),
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
            if predict:
                return conn.table(result_tbl)
            return conn.table(result_tbl), conn.table(stats_tbl)
        # massive mode
        if fcn == 'PREDICT':
            outputs_predict = ['RESULT', 'ERROR_MSG', 'PH']
            outputs_predict = ['#PAL_UNIFIED_REGRESSION_{}_{}_TBL_{}_{}'.format(fcn, name, self.id, unique_id)
                               for name in outputs_predict]
            result_tbl, errmsg_tbl, _ = outputs_predict
            predict_output_signature = [
                        {"GROUP_ID": "NVARCHAR(256)", "ID": "NVARCHAR(256)", "SCORE": "DOUBLE", "UPPER_BOUND": "DOUBLE", "LOWER_BOUND": "DOUBLE", "REASON": "NCLOB"},
                        {"GROUP_ID": "NVARCHAR(256)", "ERROR_TIMESTAMP": "NVARCHAR(256)", "ERRORCODE": "INTEGER", "MASSAGE":"NVARCHAR(1000)"},
                        {"GROUP_ID": "NVARCHAR(256)", "OBJECT": "NVARCHAR(10)", "KEY": "NVARCHAR(10)", "VALUE": "NVARCHAR(10)"}
                        ]
            setattr(self, "predict_output_signature", predict_output_signature)
        if fcn == 'SCORE':
            outputs_score = ['RESULT', 'STATS', 'ERROR_MSG']
            outputs_score = ['#PAL_UNIFIED_REGRESSION_{}_{}_TBL_{}_{}'.format(fcn, name, self.id, unique_id)
                             for name in outputs_score]
            result_tbl, stats_tbl, errmsg_tbl = outputs_score
            score_output_signature = [
                        {"GROUP_ID": "NVARCHAR(256)", "ID": "NVARCHAR(256)", "SCORE": "DOUBLE", "UPPER_BOUND": "DOUBLE", "LOWER_BOUND": "DOUBLE", "REASON": "NCLOB"},
                        {"GROUP_ID": "NVARCHAR(256)", "STAT_NAME": "NVARCHAR(100)", "STAT_VALUE": "NVARCHAR(1000)"},
                        {"GROUP_ID": "NVARCHAR(256)", "ERROR_TIMESTAMP": "NVARCHAR(256)", "ERRORCODE": "INTEGER", "MASSAGE":"NVARCHAR(1000)"}
                        ]
            setattr(self, "score_output_signature", score_output_signature)
        self.real_func = self.func

        try:
            if check_pal_function_exist(conn, '%UNIFIED_MASSIVE%', like=True) or self._disable_hana_execution:
                if self.pivoted:
                    if fcn == 'PREDICT':
                        self._call_pal_auto(conn,
                                            'PAL_UNIFIED_MASSIVE_REGRESSION_PREDICT_PIVOT',
                                            data,
                                            model,
                                            ParameterTable(itype=group_key_type).with_data(param_rows),
                                            *outputs_predict)
                    if fcn == 'SCORE':
                        self._call_pal_auto(conn,
                                            'PAL_UNIFIED_MASSIVE_REGRESSION_SCORE_PIVOT',
                                            data,
                                            model,
                                            ParameterTable(itype=group_key_type).with_data(param_rows),
                                            *outputs_score)
                else:
                    if fcn == 'PREDICT':
                        self._call_pal_auto(conn,
                                            'PAL_UNIFIED_MASSIVE_REGRESSION_PREDICT',
                                            data_,
                                            model,
                                            ParameterTable(itype=group_key_type).with_data(param_rows),
                                            *outputs_predict)
                    if fcn == 'SCORE':
                        self._call_pal_auto(conn,
                                            'PAL_UNIFIED_MASSIVE_REGRESSION_SCORE',
                                            data_,
                                            model,
                                            ParameterTable(itype=group_key_type).with_data(param_rows),
                                            *outputs_score)
            else:
                msg = 'The version of your SAP HANA does not support unified massive regression!'
                logger.error(msg)
                raise ValueError(msg)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            if fcn == 'PREDICT':
                try_drop(conn, outputs_predict)
            else:
                try_drop(conn, outputs_score)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            if fcn == 'PREDICT':
                try_drop(conn, outputs_predict)
            else:
                try_drop(conn, outputs_score)
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
        if predict:
            return conn.table(result_tbl), err_msg
        return conn.table(result_tbl), conn.table(stats_tbl), err_msg

    @mlflow_autologging(logtype='pal_fit')
    @trace_sql
    def fit(self,
            data,
            key=None,
            features=None,
            label=None,
            purpose=None,
            partition_method=None,
            partition_random_state=None,
            training_percent=None,
            output_partition_result=None,
            categorical_variable=None,
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
            group_key=None,
            group_params=None,
            output_coefcov=None,
            output_leaf_values=None,
            meta_data=None,
            significance_level=None,
            ignore_zero=None,
            permutation_importance=None,
            permutation_evaluation_metric=None,
            permutation_n_repeats=None,
            permutation_seed=None,
            permutation_n_samples=None):
        r"""
        Fit function for unified regression.

        Parameters
        ----------

        data : DataFrame
            DataFrame containing the training data.

            If the corresponding UnifiedRegression instance is for pivoted input data(i.e.
            setting ``pivoted = True`` in initialization), then ``data`` must be pivoted
            such that:

            - in `massive` mode, ``data`` must be `exactly` structured as follows:

              - 1st column: Group ID, type INTEGER, VARCHAR or NVARCHAR
              - 2nd column: Record ID, type INTEGER, VARCHAR or NVARCHAR
              - 3rd column: Variable Name, type VARCHAR or NVARCHAR
              - 4th column: Variable Value, type VARCHAR or NVARCHAR
              - 5th column: Self-defined Data Partition, type INTEGER, 1 for training and 2 for validation.


            - in `non-massive` mode, ``data`` must be `exactly` structured as follows:

              - 1st column: Record ID, type INTEGER, VARCHAR or NVARCHAR
              - 2nd column: Variable Name, type VARCHAR or NVARCHAR
              - 3rd column: Variable Value, type VARCHAR or NVARCHAR
              - 4th column: Self-defined Data Partition, type INTEGER, 1 for training and 2 for validation.

            .. note::

              If ``data`` is pivoted, then the following parameters become ineffective: ``key``, ``features``,
              ``label``, ``group_key`` and ``purpose``.

        key : str, optional
            Name of ID column.

            In single mode, if ``key`` is not provided, then: if ``data`` is indexed by a single column, then ``key`` defaults
            to that index column; Otherwise, it is assumed that ``data`` contains no ID column.

            In massive mode, defaults to the first-non group key column of data if the index columns of data is not provided.
            Otherwise, defaults to the second of index columns of data and the first column of index columns is group_key.

        features : a list of str, optional
            Names of the feature columns.

            If ``features`` is not provided, it defaults to all non-key, non-label columns.

        label : str or a list of str, optional
            Name of the dependent variable.

            Should be a list of two strings for GLM models with ``family`` being 'binomial'.

            If ``label`` is not provided, it defaults to:

            - the first non-key column of ``data``, when ``func`` parameter from initialization function
              takes the following values:\
              'GeometricRegression', 'PolynomialRegression', 'LinearRegression',
              'ExponentialRegression', 'GLM' (except when ``family`` is 'binomial')
            - the first two non-key columns of ``data``, when ``func`` parameter in initialization function
              takes the value of 'GLM' and ``familly`` is specified as 'binomial'.

        purpose : str, optional
            Indicates the name of purpose column which is used for predefined data partition.

            The meaning of value in the column for each data instance is shown below:

            - 1 : training.
            - 2 : testing.

            Mandatory and valid only when ``partition_method`` is 'predefined'..

        partition_method : {'no', 'predefined', 'random'}, optional
            Defines the way to divide the dataset.

            - 'no' : no partition.
            - 'predefined' : predefined partition.
            - 'random' : random partition.

            Defaults to 'no'.

        partition_random_state : int, optional
            Indicates the seed used to initialize the random number generator for data partition.

            Valid only when ``partition_method`` is set to 'random'.

            - 0 : Uses the system time.
            - Not 0 : Uses the specified seed.

            Defaults to 0.

        training_percent : float, optional
            The percentage of data used for training.

            Value range: 0 <= value <= 1.

            Defaults to 0.8.

        output_partition_result : bool, optional
            Specifies whether or not to output the partition result of ``data`` in data partition table.

            Valid only when ``key`` is provided and ``partition_method`` is set to 'random'.

            Defaults to False.

        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.
        background_size : int, optional
            Specifies the size of background data used for Shapley Additive Explanations (SHAP) values calculation.

            Should not larger than the size of training data.

            Valid only for Exponential Regression, Generalized Linear Models(GLM), Linear Regression,
            Multi-layer Perceptron and Support Vector Regression.

            Defaults to 0(no background data, in which case the calculation of SHAP values shall be disabled).

        background_random_state : int, optional
            Specifies the seed for random number generator in the background data sampling.

            - 0 : Uses current time as seed
            - Others : The specified seed value

            Valid only for Exponential Regression, Generalized Linear Models(GLM), Linear Regression,
            Multi-layer Perceptron and Support Vector Regression(SVR).

            Defaults to 0.

        build_report : bool, optional
            Whether to build a model report or not.

            Example:

            >>> from hana_ml.visualizers.unified_report import UnifiedReport
            >>> hgr = UnifiedRegression(func='HybridGradientBoostingTree')
            >>> hgr.fit(data=df_boston, key= 'ID', label='MEDV', build_report=True)
            >>> UnifiedReport(hgr).display()

            Defaults to False.

        impute : bool, optional
            Specifies whether or not to impute missing values in the training data.

            Defaults to False.

        strategy, strategy_by_col, als_* : parameters for missing value handling, optional
            All these parameters mentioned above are for handling missing values
            in data, please see :ref:`impute_params-label` for more details.

            All parameters are valid only when ``impute`` is set as True.

        group_key : str, optional
            The column of group_key. Data type can be INT or NVARCHAR/VARCHAR.
            If data type INT, only parameters set in the group_params are valid.

            This parameter is only valid when ``massive`` is set as True in class instance
            initialization.

            Defaults to the first column of data if the index columns of data is not provided.
            Otherwise, defaults to the first column of index columns.

        group_params : dict, optional
            If massive mode is activated (``massive`` is set as True in class instance initialization),
            input data for regression shall be divided into different
            groups with different regression parameters applied. This parameter specifies the parameter
            values of the chosen regression algorithm ``func`` w.r.t. different groups in a dict format,
            where keys corresponding to ``group_key`` while values should be a dict for regression algorithm
            parameter value assignments.

            An example is as follows:

            .. only:: latex

                >>> ur = UnifiedRegression(func='DecisionTree',
                                           massive=True,
                                           group_params={{'Group_1': {'percentage':0.6},
                                                          'Group_2':{'percentage':0.8}})
                >>> ur.fit(data=df,
                           key='ID',
                           features=['OUTLOOK' ,'TEMP', 'HUMIDITY','WINDY'],
                           label='CLASS',
                           group_key='GROUP_ID',
                           background_size=4,
                           group_params={'Group_1': {'background_random_state':2}})

            .. raw:: html

                <iframe allowtransparency="true" style="border:1px solid #ccc; background: #eeffcb;"
                    src="../../_static/ur_fit_example.html" width="100%" height="100%" sandbox="">
                </iframe>

            Valid only when ``massive`` is set as  True in class instance initialization.

            Defaults to None.

        output_coefcov : bool, optional
            Specifies whether or not to output coefficient covariance information for Linear Regression.

            Valid only if ``func`` is specified as 'LinearRegression' and ``json_export`` as True.

            Defaults to False.

            .. note::

              To enable output of confidence/prediction interval for Linear Regression model in UnifiedRegression
              during predicting/scoring phase, we need to set ``output_coefcov`` as 1.

        output_leaf_values : bool, optional
            Specifies whether or not save the target target values in each leaf node in the training phase for Random Decision Trees
            model(otherwise only mean of the target values is saved in the model). Setting the value of this parameter as True
            to enable the output of prediction interval for Random Decision Trees model in UnifiedRegression during
            predicting/scoring phase

            Valid only for fitting Random Decision Trees model(i.e. setting ``func`` as 'RandomDecisionTree') when
            ``model_format`` is 'json'  or ``compression`` is True during class instance initialization.

            Defaults to False.

        meta_data : DataFrame, optional

            Specifies the meta data for pivoted input data. Mandatory if ``pivoted`` is specified as True
            in initializing the class instance.

            If provided, then ``meta_data`` should be structured as follows:

            - 1st column: NAME, type VRACHAR or NVARCHAR. The name of the variable.
            - 2nd column: TYPE, VRACHAR or NVARCHAR. The type of the variable, can be CONTINUOUS, CATEGORICAL or TARGET.

        significance_level : float, optional
            Specifies the significance level of the prediction interval for Hybrid Gradient Boosting Tree(HGBT) model.

            Valid only when ``func`` is specified as 'HybridGradientBoostingTree', and ``obj_func`` as 'quantile' during class instance initialization.

            Defaults to 0.05.

        ignore_zero : bool, optional
            Specifies whether or not to ignore zero values in ``data`` when calculating MPE or MAPE.

            Defaults to False, i.e. use the zero values in ``data`` when calculating MPE or MAPE.

        permutation_* : parameter for permutation feature importance, optional
            All parameters with prefix 'permutation\_' are for the calculation of permutation
            feature importance.

            They are valid only when ``partition_method`` is specified as 'predefined' or 'random',
            since permuation feature importance is calculated on the validation set.

            Please see :ref:`permutation_imp-label` for more details.

        Returns
        -------
        A fitted object of class "UnifiedRegression".
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        conn = data.connection_context
        require_pal_usable(conn)
        self.real_func = self.func
        if self.pivoted and meta_data is None:
            msg = "meta_data must be given when `pivoted`=True."
            logger.error(msg)
            raise ValueError(msg)
        self.fit_params = {"partition_method" : partition_method,
                           "partition_random_state" : partition_random_state,
                           "training_percent" : training_percent,
                           "output_partition_result" : output_partition_result,
                           "background_size" : background_size,
                           "background_random_state" : background_random_state,
                           "output_coefcov" : output_coefcov,
                           "output_leaf_values" : output_leaf_values,
                           "significance_level" : significance_level,
                           "ignore_zero" : ignore_zero}

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
            key = _key_index_check(key, 'key',
                                   index[1] if self.massive else index)
        if key is not None and key not in cols:
            msg = "Please select key from {}!".format(cols)
            logger.error(msg)
            raise ValueError(msg)

        has_id = False
        purpose = self._arg('purpose', purpose, str, partition_method == "predefined")
        if partition_method != 1:#purpose ineffective when partition method is not 1
            purpose = None
        if isinstance(features, str):
            features = [features]
        features = self._arg('features', features, ListOfStrings)
        label = self._arg('label', label, str)

        if key is not None:
            id_col = [key]
            has_id = True
            cols.remove(key)
        else:
            id_col = []
        if purpose is not None:
            cols.remove(purpose)
            purpose = [purpose]
        else:
            purpose = []
        if label is None:
            if self.func == 'GLM' and 'group_response' in self.params.keys() and self.params['group_response'] is True:#pylint:disable=line-too-long
                label = cols[-2:]
            else:
                label = cols[-1]
        if isinstance(label, (list, tuple)):
            for lab in label:
                cols.remove(lab)
        else:
            cols.remove(label)
        if isinstance(label, str):
            label = [label]
        if features is None:
            features = cols
        if strategy_by_col is not None:
            for col_strategy in strategy_by_col:
                if col_strategy[0] not in features:
                    msg = ('{} is not a valid column name'.format(col_strategy[0]) +
                           ' of the input dataframe for column imputation.')
                    logger.error(msg)
                    raise ValueError(msg)

        data_ = data[group_id + id_col + features + label + purpose]

        self.label = label
        if isinstance(self.label, (list, tuple)):
            self.label = self.label[0]
        if self.label is None:
            self.label = data_.columns[-1]

        if self.massive is not True:
            fit_map = {**self.__fit_param_dict, **self.__impute_dict, **self.__permutation_imp_dict}
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

            if self.func == 'SVM':
                param_rows.extend([('EVALUATION_METRIC', None, None, 'RMSE')])
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
                        value = '{' + value + '}'
                        param_rows.extend([(name, None, None, value)])
                    elif name == 'ORDERING':
                        tpl = [('ORDERING', None, None, ', '.join(value))]
                        param_rows.extend(tpl)
                    elif name == 'DEGREE_RANGE':
                        tpl = [('DEGREE_RANGE', None, None, str(value))]
                        param_rows.extend(tpl)
                    elif name == 'DEGREE_VALUES':
                        tpl = [('DEGREE_VALUES', None, None,
                                '{' + ','.join([str(x) for x in value]) + '}')]
                        param_rows.extend(tpl)
                    else:
                        for val in value:
                            tpl = [self.__map_param(name, val, typ)]
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
                    tpl = [self.__map_param(name, value, typ)]
                    param_rows.extend(tpl)

            if isinstance(categorical_variable, str):
                categorical_variable = [categorical_variable]
            categorical_variable = arg('categorical_variable', categorical_variable, ListOfStrings)
            if categorical_variable is not None:
                param_rows.extend([('CATEGORICAL_VARIABLE', None, None, var) for var in categorical_variable])

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

            unique_id = str(uuid.uuid1()).replace('-', '_').upper()
            outputs = ['MODEL', 'STATS', 'OPT_PARAM', 'PARTITION', 'PLACE_HOLDER1', 'PLACE_HOLDER2']
            outputs = ['#PAL_UNIFIED_REGRESSION_{}_{}_{}'.format(tbl, self.id, unique_id)
                       for tbl in outputs]
            model_tbl, stats_tbl, opt_param_tbl, partition_tbl, _, _ = outputs
            fit_output_signature = [
                {"ROW_INDEX": "INTEGER", "PART_INDEX": "INTEGER", "MODEL_CONTENT": "NCLOB"},
                {"STAT_NAME": "NVARCHAR(256)", "STAT_VALUE": "NVARCHAR(1000)"},
                {"PARAM_NAME": "NVARCHAR(256)", "INT_VALUE": "INTEGER", "DOUBLE_VALUE": "DOUBLE", "STRING_VALUE": "NVARCHAR(1000)"},
                {"ID": "INTEGER", "TYPE": "INTEGER"},
                {"OBJECT": "NVARCHAR(10)", "KEY": "NVARCHAR(10)", "VALUE": "NVARCHAR(10)"},
                {"OBJECT": "NVARCHAR(10)", "KEY": "NVARCHAR(10)", "VALUE": "NVARCHAR(10)"}
            ]
            setattr(self, "fit_output_signature", fit_output_signature)
            try:
                if self.pivoted:
                    setattr(self, 'fit_data', data)
                    self._call_pal_auto(conn,
                                        'PAL_UNIFIED_REGRESSION_PIVOT',
                                        meta_data,
                                        data,
                                        ParameterTable().with_data(param_rows),
                                        *outputs)
                else:
                    setattr(self, 'fit_data', data_)
                    self._call_pal_auto(conn,
                                        'PAL_UNIFIED_REGRESSION',
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

        else: # massive mode
            group_params = self._arg('group_params', group_params, dict)
            special_group_name = 'PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID'
            param_rows = []
            fit_map = {**self.__fit_param_dict, **self.__impute_dict}
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

            if 'INT' in group_key_type and general_params:
                warn_msg = "If the type of group_key is INTEGER, only parameters in group_params are valid!"
                logger.warning(warn_msg)

            if 'INT' not in group_key_type:
                # categorical_variable
                if isinstance(categorical_variable, str):
                    categorical_variable = [categorical_variable]
                categorical_variable = arg('categorical_variable', categorical_variable, ListOfStrings)
                if categorical_variable is not None:
                    param_rows.extend([(special_group_name, 'CATEGORICAL_VARIABLE', None, None, var) for var in categorical_variable])
                # strategy_by_col
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
            if group_params is not None:
                for group in group_params:
                    if group in ['PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID']:
                        continue
                    group_val = int(group) if 'INT' in group_key_type else group
                    each_group_params = {}
                    each_group_map = {**self.__fit_param_dict, **self.__impute_dict}
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
                    if isinstance(group_categorical_variable, str):
                        group_categorical_variable = [group_categorical_variable]
                    group_categorical_variable = arg('group_categorical_variable', group_categorical_variable, ListOfStrings)
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
                            elif name == 'ORDERING':
                                tpl = [(group_val, 'ORDERING', None, None, ', '.join(value))]
                                param_rows.extend(tpl)
                            elif name == 'DEGREE_RANGE':
                                tpl = [(group_val, 'DEGREE_RANGE', None, None, str(value))]
                                param_rows.extend(tpl)
                            elif name == 'DEGREE_VALUES':
                                tpl = [(group_val, 'DEGREE_VALUES', None, None,
                                        '{' + ','.join([str(x) for x in value]) + '}')]
                                param_rows.extend(tpl)
                            else:
                                for val in value:
                                    tpl = [tuple([group_val] + list(self.__map_param(name, val, typ)))]
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

            unique_id = str(uuid.uuid1()).replace('-', '_').upper()
            outputs_massive = ['MODEL', 'STATS', 'OPT_PARAM', 'PARTITION_TYPE', 'ERRORMSG', 'PLACE_HOLDER1', 'PLACE_HOLDER2']
            outputs_massive = ['#PAL_MASSIVE_UNIFIED_REGRESSION_{}_{}_{}'.format(tbl, self.id, unique_id)
                               for tbl in outputs_massive]
            model_tbl, stats_tbl, opt_param_tbl, partition_tbl, errormsg_tbl, _, _ = outputs_massive
            fit_output_signature = [
                {"GROUP_ID": "NVARCHAR(100)", "ROW_INDEX": "INTEGER", "PART_INDEX": "INTEGER", "MODEL_CONTENT": "NCLOB"},
                {"GROUP_ID": "NVARCHAR(100)", "STAT_NAME": "NVARCHAR(1000)", "STAT_VALUE": "NVARCHAR(256)"},
                {"GROUP_ID": "NVARCHAR(100)", "PARAM_NAME": "NVARCHAR(256)", "INT_VALUE": "INTEGER", "DOUBLE_VALUE": "DOUBLE", "STRING_VALUE": "NVARCHAR(1000)"},
                {"GROUP_ID": "NVARCHAR(100)", "ID": "INTEGER", "TYPE": "INTEGER"},
                {"GROUP_ID": "NVARCHAR(256)", "ERROR_TIMESTAMP": "NVARCHAR(256)", "ERRORCODE": "INTEGER", "MASSAGE":"NVARCHAR(1000)"},
                {"OBJECT": "NVARCHAR(10)", "KEY": "NVARCHAR(10)", "VALUE": "NVARCHAR(10)"},
                {"OBJECT": "NVARCHAR(10)", "KEY": "NVARCHAR(10)", "VALUE": "NVARCHAR(10)"}
            ]
            setattr(self, "fit_output_signature", fit_output_signature)
            try:
                if check_pal_function_exist(conn, '%UNIFIED_MASSIVE%', like=True) or self._disable_hana_execution:
                    if self.pivoted:
                        setattr(self, 'fit_data', data)
                        self._call_pal_auto(conn,
                                            'PAL_UNIFIED_MASSIVE_REGRESSION_PIVOT',
                                            meta_data,
                                            data,
                                            ParameterTable(itype=group_key_type).with_data(param_rows),
                                            *outputs_massive)
                    else:
                        setattr(self, 'fit_data', data_)
                        self._call_pal_auto(conn,
                                            'PAL_UNIFIED_MASSIVE_REGRESSION',
                                            data_,
                                            ParameterTable(itype=group_key_type).with_data(param_rows),
                                            *outputs_massive)
                else:
                    msg = 'The version of your SAP HANA does not support unified massive regression!'
                    logger.error(msg)
                    raise ValueError(msg)
            except dbapi.Error as db_err:
                logger.error(str(db_err))
                try_drop(conn, outputs_massive)
                raise
            except Exception as db_err:
                logger.error(str(db_err))
                try_drop(conn, outputs_massive)
                raise

        #pylint: disable=attribute-defined-outside-init
        self.scoring_list_ = None
        self.statistics_ = conn.table(stats_tbl)
        self.optimal_param_ = conn.table(opt_param_tbl)
        self.partition_ = conn.table(partition_tbl)
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
                       self.optimal_param_]
        self.__param_rows = param_rows
        if build_report:
            self.build_report()
        return self

    def get_optimal_parameters(self):
        """
        Returns the optimal parameters.
        """
        return self.model_[2].collect()

    def get_performance_metrics(self):
        """
        Returns the performance metrics.
        """
        mtrc = self.model_[1].filter("STAT_NAME NOT IN ('IMP', 'OUT_OF_BAG')").collect()
        return {row.STAT_NAME[5:]:float(row.STAT_VALUE) for idx, row in mtrc.iterrows()}

    def get_feature_importances(self):
        """
        Returns the feature importances
        """
        imp_dict = json.loads(self.model_[1].filter("STAT_NAME='IMP'").collect()['STAT_VALUE'][0])
        return({x:float(imp_dict[x]) for x in imp_dict})

    @trace_sql
    def predict(self,
                data,
                key=None,
                features=None,
                model=None,
                thread_ratio=None,
                prediction_type=None,
                significance_level=None,
                handle_missing=None,
                block_size=None,
                top_k_attributions=None,
                attribution_method=None,
                sample_size=None,
                random_state=None,
                ignore_correlation=None,
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
                group_key=None,
                group_params=None,
                interval_type=None):#pylint:disable=unused-argument
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
            Name of ID column.

            In single mode, mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.
            Defaults to the single index column of ``data`` if not provided.

            In massive mode, defaults to the first-non group key column of data if the index columns of data is not provided.
            Otherwise, defaults to the second of index columns of data and the first column of index columns is group_key.


        features : a list of str, optional
            Names of feature columns in data for prediction.

            Defaults all non-ID columns in `data` if not provided.

        model : DataFrame, optional
            A fitted regression model.

            Defaults to self.model\_.

        thread_ratio : float, optional
            Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
            Values outside the range will be ignored and this function heuristically determines the number of threads to use.

            Defaults to the PAL's default value.

        prediction_type : str, optional
            Specifies the type of prediction. Valid options include:

            - 'response' : direct response (with link)
            - 'link' : linear response (without link)

            Valid only for GLM models.

            Defaults to 'response'.

        significance_level : float, optional
            Specifies significance level for the confidence/prediction interval.

            Valid only for the following 3 cases:

            - GLM model with IRLS solver applied(i.e. ``func`` is specified as 'GLM' and ``solver``
              as 'irls' during class instance initialization).
            - Linear Regression model with json model imported(i.e. ``func`` is specified as
              'LinearRegression' and ``json_export`` as True during class instance initialization).

            Defaults to 0.05.

        handle_missing : str, optional
            Specifies the way to handle missing values. Valid options include:

            - 'skip' : skip(i.e. remove) rows with missing values
            - 'fill_zero' : replace missing values with 0.

            Valid only for GLM models.

            Defaults to 'fill_zero'.

        block_size : int, optional
            Specifies the number of data loaded per time during scoring.

            - 0: load all data once
            - Others: the specified number

            This parameter is for reducing memory consumption, especially as the predict data is huge,
            or it consists of a large number of missing independent variables. However, you might lose some efficiency.

            Valid only for RandomDecisionTree(RDT) models.

            Defaults to 0.

        top_k_attributions : int, optional
            Specifies the number of features with highest attributions to output.

            Defaults to 10.

        attribution_method : {'no', 'saabas', 'tree-shap'}, optional
            Specifies which method to use for model reasoning.

            - 'no' : No reasoning
            - 'saabas' : Saabas method
            - 'tree-shap' : Tree SHAP method

            Valid only for tree-based models, i.e. DecisionTree, RandomDecisionTree and HybridGradientBoostingTree models.

            Defaults to 'tree-shap'.

        sample_size : int, optional
            Specifies the number of sampled combinations of features.

            - 0 : Heuristically determined by algorithm
            - Others : The specified sample size

            Valid only for Exponential Regression, GLM, Linear Regression, MLP and Support Vector Regression.

            Defaults to 0.

        random_state : int, optional
            Specifies the seed for random number generator when sampling the combination of features.

            - 0 : User current time as seed
            - Others : The actual seed

            Valid only for Exponential Regression, GLM, Linear Regression, MLP and Support Vector Regression.

            Defaults to 0.

        ignore_correlation : bool, optional
            Specifies whether or not to ignore the correlation between the features.

            Valid only for Exponential Regression, GLM and Linear Regression that adopt
            `linear SHAP` for local interpretability of models.

            Defaults to False.

        impute : bool, optional
            Specifies whether or not to impute missing values in ``data``.

            Defaults to False.

        strategy, strategy_by_col, als_* : parameters for missing value handling, optional
            All these parameters mentioned above are for handling missing values
            in data, please see :ref:`impute_params-label` for more details.

            All parameters are valid only when ``impute`` is set as True.

        group_key : str, optional
            The column of group_key. Data type can be INT or NVARCHAR/VARCHAR.
            If data type is INT, only parameters set in the group_params are valid.

            This parameter is only valid when ``massive`` is set as True in class instance
            initialization.

            Defaults to the first column of data if the index columns of data is not provided.
            Otherwise, defaults to the first column of index columns.

        group_params : dict, optional
            If massive mode is activated (``massive`` is set as True in class instance initialization),
            input data for regression shall be divided into different
            groups with different regression parameters applied. This parameter specifies the parameter
            values of the chosen regression algorithm ``func`` w.r.t. different groups in a dict format,
            where keys corresponding to ``group_key`` while values should be a dict for regression algorithm
            parameter value assignments.

            An example is as follows:

            .. only:: latex

                >>> ur = UnifiedRegression(func='DecisionTree',
                                           massive=True,
                                           group_params={{'Group_1': {'percentage':0.6},
                                                          'Group_2':{'percentage':0.8}})
                >>> ur.fit(data=df,
                           key='ID',
                           group_key='GROUP_ID',
                           features=['OUTLOOK' ,'TEMP', 'HUMIDITY','WINDY'],
                           label='CLASS')
                >>> res = ur.predict(data=pred_df,
                                     key='ID',
                                     group_key='GROUP_ID',
                                     group_params={'Group_1':{'attribution_method':'saabas'}})

            .. raw:: html

                <iframe allowtransparency="true" style="border:1px solid #ccc; background: #eeffcb;"
                    src="../../_static/ur_predict_example.html" width="100%" height="100%" sandbox="">
                </iframe>

            Valid only when ``massive`` is set as True in class instance initialization.

            Defaults to None.

        interval_type : {'no', 'confidence', 'prediction'}, optional
            Specifies the type of interval to output:

            - 'no': do not calculate and output any interval
            - 'confidence': calculate and output the confidence interval
            - 'prediction': calculate and output the prediction interval

            Valid only for one of the following 4 cases:

            - GLM model with IRLS solver applied(i.e. ``func`` is specified as 'GLM' and ``solver``
              as 'irls' during class instance initialization).
            - Linear Regression model with json model imported and coefficient covariance information computed
              (i.e. ``func`` is specified as 'LinearRegression', ``json_export`` specified as True
              during class instance initialization, and ``output_coefcov`` specified as True during
              the training phase).
            - Random Decision Trees model with all leaf values retained(i.e. ``func`` is 'RandomDecisionTree' and
              ``output_leaf_values`` is True). In this case, ``interval_type`` could be specified as either 'no' or
              'prediction'.
            - Hybrid Gradient Boosting Tree model with quantile objective function(i.e. ``func`` is
              'HybridGradientBoostingTree', and ``obj_func`` is 'quantile' for class instance initialization).
              In this case, ``interval_type`` can be specified as either 'no' or 'prediction'.

            Defaults to 'no'.

        Returns
        -------
        DataFrame
            A collection of DataFrames listed as follows:

                - Prediction result by ignoring the true labels of the input data,
                  structured the same as the result table of predict() function.
                - Error message (optional). Only valid if ``massive`` is True when initializing an 'UnifiedRegression' instance.

        Examples
        --------
        Example 1 - Linear Regression predict with confidence interval:

        >>> bsh_train = conn.table('BOSTON_HOUSING_TRAIN_DATA')
        >>> bsh_test = conn.table('BOSTON_HOUSING_TEST_DATA')
        >>> ulr = UnifiedRegression(func='LinearRegression',
        ...                         json_export=True)# prediction/confidence interval only available for json model
        >>> ulr.fit(data=bsh_df,
        ...         key='ID',
        ...         label='MEDV',
        ...         output_coefcov=True)# set as True to output coefficient interval
        >>> ulr.predict(data=bsh_test.deselect('MEDV'),
        ...             key='ID',
        ...             significance_level=0.05,
        ...             interval_type='confidence')# specifies the interval type as confidence

        Example 2 - GLM model predict of response with prediction interval:

        >>> bsh_train = conn.table('BOSTON_HOUSING_TRAIN_DATA')
        >>> bsh_test = conn.table('BOSTON_HOUSING_TEST_DATA')
        >>> uglm = UnifiedRegression(func='GLM', family='gaussian', link='identity')
        >>> uglm.fit(data=bsh_df, key='ID', label='MEDV')
        >>> ulr.predict(data=bsh_test.deselect('MEDV'),
        ...             key='ID',
        ...             significance_level=0.05,
        ...             prediction_type='response',# set to 'response' for direct response
        ...             interval_type='prediction')# specifies the interval type as prediction
        """
        res = self.__forecast(data=data,
                              predict=True,
                              key=key,
                              features=features,
                              label=None,
                              model=model,
                              thread_ratio=thread_ratio,
                              prediction_type=prediction_type,
                              significance_level=significance_level,
                              handle_missing=handle_missing,
                              block_size=block_size,
                              top_k_attributions=top_k_attributions,
                              attribution_method=attribution_method,
                              sample_size=sample_size,
                              random_state=random_state,
                              ignore_correlation=ignore_correlation,
                              impute=impute,
                              strategy=strategy,
                              strategy_by_col=strategy_by_col,
                              als_factors=als_factors,
                              als_lambda=als_lambda,
                              als_maxit=als_maxit,
                              als_randomstate=als_randomstate,
                              als_exit_threshold=als_exit_threshold,
                              als_exit_interval=als_exit_interval,
                              als_linsolver=als_linsolver,
                              als_cg_maxit=als_cg_maxit,
                              als_centering=als_centering,
                              als_scaling=als_scaling,
                              group_key=group_key,
                              group_params=group_params,
                              interval_type=interval_type)
        return res

    @trace_sql
    def score(self,
              data,
              key=None,
              features=None,
              label=None,
              model=None,
              prediction_type=None,
              significance_level=None,
              handle_missing=None,
              thread_ratio=None,
              block_size=None,
              top_k_attributions=None,
              attribution_method=None,
              sample_size=None,
              random_state=None,
              ignore_correlation=None,
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
              group_key=None,
              group_params=None,
              interval_type=None):
        r"""
        Evaluate the model quality.
        In the Unified regression, statistics and metrics are provided to show the model quality.
        Currently the following metrics are supported:

        - EVAR
        - MAE
        - MAPE
        - MAX_ERROR
        - MSE
        - R2
        - RMSE
        - WMAPE

        Parameters
        ----------
        data :  DataFrame
            Data for scoring.

            If `self.pivoted` is True, then ``data`` must be pivoted, indicating that it should be structured
            the same as the pivoted data used for training(exclusive of the last data partition column).
            In this case, the following parameters become ineffective:``key``, ``features``, ``label``, ``group_key``.

        key : str, optional
            Name of the ID column.

            In single mode, mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.
            Defaults to the single index column of ``data`` if not provided.

            In massive mode, defaults to the first-non group key column of data if the index columns of data is not provided.
            Otherwise, defaults to the second of index columns of data and the first column of index columns is group_key.

        features : ListOfString or str, optional
            Names of feature columns.

            Defaults to all non-ID, non-label columns if not provided.

        label : str, optional
            Name of the label column.

            Defaults to the last non-ID column if not provided.

        model : DataFrame, optional
            A fitted regression model.

            Defaults to self.model\_.

        thread_ratio : float, optional
            Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
            Values outside the range will be ignored and this function heuristically determines the number of threads to use.

            Defaults to the PAL's default value.

        prediction_type : str, optional
            Specifies the type of prediction. Valid options include:

            - 'response' : direct response (with link).
            - 'link' : linear response (without link).

            Valid only for GLM models.

            Defaults to 'response'.

        significance_level : float, optional
            Specifies significance level for the confidence interval and prediction interval.

            Valid only for the following 2 cases:

            - GLM model with IRLS solver applied(i.e. ``func`` is specified as 'GLM' and ``solver``
              as 'irls' during class instance initialization).
            - Linear Regression model with json model imported(i.e. ``func`` is specified as
              'LinearRegression' and ``json_export`` as True during class instance initialization).

            Defaults to 0.05.

        handle_missing : str, optional
            Specifies the way to handle missing values. Valid options include:

            - 'skip' : skip rows with missing values.
            - 'fill_zero' : replace missing values with 0.

            Valid only for GLM models.

            Defaults to 'fill_zero'.

        block_size : int, optional
            Specifies the number of data loaded per time during scoring.

            - 0: load all data once.
            - Others: the specified number.

            This parameter is for reducing memory consumption, especially as the predict data is huge,
            or it consists of a large number of missing independent variables. However, you might lose some efficiency.

            Valid only for RandomDecisionTree models.

            Defaults to 0.

        top_k_attributions : int, optional
            Specifies the number of features with highest attributions to output.

            Defaults to 10.

        attribution_method : {'no', 'saabas', 'tree-shap'}, optional
            Specifies which method to use for model reasoning.

            - 'no' : No reasoning.
            - 'saabas' : Saabas method.
            - 'tree-shap' : Tree SHAP method.

            Valid only for tree-based models, i.e. DecisionTree, RandomDecisionTree and HybridGradientBoostingTree models.

            Defaults to 'tree-shap'.

        sample_size : int, optional
            Specifies the number of sampled combinations of features.

            - 0 : Heuristically determined by algorithm.
            - Others : The specified sample size.

            Valid only for Exponential Regression, GLM, Linear Regression, MLP and Support Vector Regression.

            Defaults to 0.

        random_state : int, optional
            Specifies the seed for random number generator when sampling the combination of features.

            - 0 : User current time as seed.
            - Others : The actual seed.

            Valid only for Exponential Regression, GLM, Linear Regression, MLP and Support Vector Regression.

            Defaults to 0.

        ignore_correlation : bool, optional
            Specifies whether or not to ignore the correlation between the features.

            Valid only for Exponential Regression, GLM and Linear Regression.

            Defaults to False.

        impute : bool, optional
            Specifies whether or not to impute missing values in ``data``.

            Defaults to False.

        strategy, strategy_by_col, als_* : parameters for missing value handling, optional
            All these parameters mentioned above are for handling missing values
            in data, please see :ref:`impute_params-label` for more details.

            All parameters are valid only when ``impute`` is set as True.

        group_key : str, optional
            The column of group_key. Data type can be INT or NVARCHAR/VARCHAR.
            If data type is INT, only parameters set in the group_params are valid.

            This parameter is only valid when ``massive`` is set as True in class instance initialization.

            Defaults to the first column of data if the index columns of data is not provided.
            Otherwise, defaults to the first column of index columns.

        group_params : dict, optional
            If massive mode is activated (``massive`` is set as True in class instance initialization),
            input data for regression shall be divided into different
            groups with different regression parameters applied. This parameter specifies the parameter
            values of the chosen regression algorithm ``func`` w.r.t. different groups in a dict format,
            where keys corresponding to ``group_key`` while values should be a dict for regression algorithm
            parameter value assignments.

            An example is as follows:

            .. only:: latex

                >>> ur = UnifiedRegression(func='DecisionTree',
                                           massive=True,
                                           group_params={{'Group_1': {'percentage':0.6},
                                                          'Group_2':{'percentage':0.8}})
                >>> ur.fit(data=df,
                           key='ID',
                           group_key='GROUP_ID',
                           features=['OUTLOOK' ,'TEMP', 'HUMIDITY','WINDY'],
                           label='CLASS')
                >>> res = ur.score(data=score_df,
                                   key='ID',
                                   group_key='GROUP_ID',
                                   group_params={'Group_1':{'attribution_method':'saabas'}})

            .. raw:: html

                <iframe allowtransparency="true" style="border:1px solid #ccc; background: #eeffcb;"
                    src="../../_static/ur_score_example.html" width="100%" height="100%" sandbox="">
                </iframe>

            Valid only when ``massive`` is set True in class instance initialization.

            Defaults to None.
        interval_type : {'no', 'confidence', 'prediction'}, optional
            Specifies the type of interval to output:

            - 'no': do not calculate and output any interval.
            - 'confidence': calculate and output the confidence interval.
            - 'prediction': calculate and output the prediction interval.

            Valid only for one of the following 4 cases:

            - GLM model with IRLS solver applied(i.e. ``func`` is specified as 'GLM' and ``solver``
              as 'irls' during class instance initialization).
            - Linear Regression model with json model imported and coefficient covariance information computed
              (i.e. ``func`` is specified as 'LinearRegression', ``json_export`` specified as True
              during class instance initialization, and ``output_coefcov`` specified as True during
              the training phase).
            - Random Decision Trees model with all leaf values retained(i.e. ``func`` is 'RandomDecisionTree' and
              ``output_leaf_values`` is True). In this case, ``interval_type`` could be specified as either 'no' or
              'prediction'.
            - Hybrid Gradient Boosting Tree model with quantile objective function(i.e. ``func`` is
              'HybridGradientBoostingTree', and ``obj_func`` is 'quantile' for class instance initialization).
              In this case, ``interval_type`` can be specified as either 'no' or 'prediction'.

            Defaults to 'no'.

        Returns
        -------
        DataFrame
            A collection of DataFrames listed as follows:

            - Prediction result by ignoring the true labels of the input data,
              structured the same as the result table of predict() function.
            - Statistics results
            - Error message (optional). Only valid if ``massive`` is True when initializing an 'UnifiedRegression' instance.

        Examples
        --------
        Example 1 - Linear Regression scoring with prediction interval:

        >>> bsh_train = conn.table('BOSTON_HOUSING_TRAIN_DATA')
        >>> bsh_test = conn.table('BOSTON_HOUSING_TEST_DATA')
        >>> ulr = UnifiedRegression(func='LinearRegression',
        ...                         json_export=True)# prediction/confidence interval only available for json model
        >>> ulr.fit(data=bsh_df,
        ...         key='ID',
        ...         label='MEDV',
        ...         output_coefcov=True) # set as True to output interval
        >>> ulr.predict(data=bsh_test.deselect('MEDV'),
        ...             key='ID',
        ...             significance_level=0.05,
        ...             interval_type='prediction')# specifies the interval type as prediction

        Example 2 - GLM model predict of linear response with confidence interval:

        >>> bsh_train = conn.table('BOSTON_HOUSING_TRAIN_DATA')
        >>> bsh_test = conn.table('BOSTON_HOUSING_TEST_DATA')
        >>> uglm = UnifiedRegression(func='GLM', family='gaussian', link='identity')
        >>> uglm.fit(data=bsh_df, key='ID', label='MEDV')
        >>> ulr.predict(data=bsh_test.deselect('MEDV'),
        ...             key='ID',
        ...             significance_level=0.05,
        ...             prediction_type='link', # set as 'link' for linear response
        ...             interval_type='confidence')# specifies the interval type as confidence
        """
        setattr(self, 'hanaml_score_params', pal_param_register())
        setattr(self, 'testing_data', data)
        self.scoring_data_ = data
        res = self.__forecast(data=data,
                              predict=False,
                              key=key,
                              features=features,
                              label=label,
                              model=model,
                              thread_ratio=thread_ratio,
                              prediction_type=prediction_type,
                              significance_level=significance_level,
                              handle_missing=handle_missing,
                              block_size=block_size,
                              top_k_attributions=top_k_attributions,
                              attribution_method=attribution_method,
                              sample_size=sample_size,
                              random_state=random_state,
                              ignore_correlation=ignore_correlation,
                              impute=impute,
                              strategy=strategy,
                              strategy_by_col=strategy_by_col,
                              als_factors=als_factors,
                              als_lambda=als_lambda,
                              als_maxit=als_maxit,
                              als_randomstate=als_randomstate,
                              als_exit_threshold=als_exit_threshold,
                              als_exit_interval=als_exit_interval,
                              als_linsolver=als_linsolver,
                              als_cg_maxit=als_cg_maxit,
                              als_centering=als_centering,
                              als_scaling=als_scaling,
                              group_key=group_key,
                              group_params=group_params,
                              interval_type=interval_type)
        self.scoring_list_ = res
        return res

    def build_report(self):
        """
        Build the model report.

        Examples
        --------
        >>> from hana_ml.visualizers.unified_report import UnifiedReport
        >>> hgr = UnifiedRegression(func='HybridGradientBoostingTree')
        >>> hgr.fit(data=df_boston, key= 'ID', label='MEDV')
        >>> hgr.build_report()
        >>> UnifiedReport(hgr).display()

        """
        try:
            rowlist = []
            for key, val in self.hanaml_parameters["kwargs"].items():
                rowlist.append({"KEY": key, "VALUE": str(val)})
            rowlist.append({"KEY": "func", "VALUE": self.func})
            parameter_df = pd.DataFrame(rowlist)
            imp = self.model_[1].filter(""""STAT_NAME"='IMP'""").collect()

            # pylint: disable=protected-access
            if self.scoring_list_ and len(self.scoring_list_) == 2:
                # input_table, result_table
                self._set_scoring_result_table(self.scoring_data_.select([self.scoring_data_.columns[0], self.label]), self.scoring_list_[0])
                self._set_scoring_statistic_table(self.scoring_list_[1])
            if len(imp) > 0:
                imp_dict = json.loads(imp.iat[0, 1])
                list_of_keys = []
                list_of_values = []
                for key, val in imp_dict.items():
                    list_of_keys.append(key)
                    list_of_values.append(val)
                var_imp_tbl = pd.DataFrame({"VARIABLE_NAME": list_of_keys, "IMPORTANCE": list_of_values})
                self._set_statistic_table(self.model_[1]) \
                    ._set_parameter_table(parameter_df) \
                    ._set_optimal_parameter_table(self.model_[2].collect()) \
                    ._set_variable_importance_table(var_imp_tbl) \
                    ._render_report()
            else:
                self._set_statistic_table(self.model_[1]) \
                    ._set_parameter_table(parameter_df) \
                    ._set_optimal_parameter_table(self.model_[2].collect()) \
                    ._render_report()
        except Exception as err:
            logger.error(str(err))
            raise
        return self

    def create_model_state(self, model=None, function=None,
                           pal_funcname='PAL_UNIFIED_REGRESSION',
                           state_description=None, force=False):
        r"""
        Create PAL model state.

        Parameters
        ----------
        model : DataFrame, optional
            Specify the model for AFL state.

            Defaults to self.model\_.

        function : str, optional
            Specify the function name of the regression algorithm.

            Valid options include:

            - 'SVM' : Support Vector Regression
            - 'MLP' : Multilayer Perceptron Regression
            - 'DT' :  Decision Tree Regression
            - 'HGBT' : Hybrid Gradient Boosting Tree Regression
            - 'MLR' : Multiple Linear Regression
            - 'RDT' : Random Decision Trees Regression

            Defaults to `self.real_func`.

            .. note::
                The default value could be invalid. In such case,
                a ValueError shall be thrown.

        pal_funcname : int or str, optional
            PAL function name.

            Defaults to 'PAL_UNIFIED_REGRESSION'.

        state_description : str, optional
            Description of the state as model container.

            Defaults to None.

        force : bool, optional
            If True it will delete the existing state.

            Defaults to False.
        """
        func = self.func if function is None else function
        if self.func != func:
            msg = 'Inconsistency between the specified function for model state creation and class initialization.'
            logger.warning(msg)
        if func in ['GLM', 'GEO', 'EXP', 'POL', 'LOG']:
            msg = '{} Regression does not support state-enabled scoring.'.format(self.func)
            logger.error(msg)
            raise ValueError(msg)
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
