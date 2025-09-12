"""
This module supports to run PAL functions in a pipeline manner.
"""

#pylint: disable=invalid-name, too-many-lines
#pylint: disable=eval-used, too-many-statements, too-many-arguments
#pylint: disable=unused-variable, attribute-defined-outside-init
#pylint: disable=line-too-long, no-self-use
#pylint: disable=too-many-locals
#pylint: disable=too-many-branches, too-many-nested-blocks
#pylint: disable=consider-using-f-string
#pylint: disable=protected-access
#pylint: disable=consider-iterating-dictionary
#pylint: disable=too-many-instance-attributes
#pylint: disable=unused-import
#pylint: disable=super-init-not-called
#pylint: disable=too-many-function-args
import json
import logging
import re
import uuid
from hdbcli import dbapi
from hana_ml.ml_base import ListOfStrings
from hana_ml.ml_exceptions import FitIncompleteError
from hana_ml.visualizers.digraph import Digraph
from .pal_base import (
    PALBase,
    ParameterTable,
    pal_param_register,
    try_drop,
    execute_logged
)
from .sqlgen import trace_sql
from .utility import AMDPHelper, mlflow_autologging, check_pal_function_exist
from .neural_network import MLPClassifier, MLPRegressor
from .svm import SVC, SVR
from .tsa.auto_arima import AutoARIMA
from .tsa.exponential_smoothing import (
    AutoExponentialSmoothing,
    SingleExponentialSmoothing,
    DoubleExponentialSmoothing,
    TripleExponentialSmoothing,
    BrownExponentialSmoothing
)
from .tsa.additive_model_forecast import AdditiveModelForecast
from .tsa.bsts import BSTS
from .tsa.outlier_detection import OutlierDetectionTS
from .auto_ml import AutomaticClassification
logger = logging.getLogger(__name__)#pylint: disable=invalid-name

class BuiltInOp(PALBase):
    r"""
    Pipeline built-in operators.

    Parameters
    ----------
    op_name : {'OneHotEncoder', 'LabelEncoder', 'CBEncoder', 'PolynomialFeatures'}

    **kwargs : keywords arguments
        Arbitrary keywords arguments passed to the specified built-in operator specified by ``op_name``.

        Valid operator-parameter setting is displayed as follows:

        - **OneHotEncoder**

          - ``ignore_unknown`` : type int, with valid values {0, 1} and default value 1, where 0
            means does not ignore the any unknown categorical value in predict data(i.e. throw-error if encountered),
            and 1 means encoding the unknown categories from -n to -1(n is the number of unknown
            categories in predict data).
          - ``minimum_fraction`` : type float with default value 0.0, which specifies the minimum fraction of uniques values in a feature for
            it to be considered categorical.

        - **LabelEncoder**

          - ``ignore_unknown`` : type int with valid values {0, 1} and default value 1, where 0
            means does not ignore the any unknown categorical value in predict data(i.e. throw-error if encountered),
            and 1 means encoding the unknown categories from -n to -1(n is the number of unknown
            categories in predict data).

        - **PolynomialFeatures**

          - ``min_degree`` : type int with default value 1. It specifies the minimum polynomial degree of generated features.
          - ``max_degree`` : type int with default value 2. It specifies the maximum polynomial degree of generated features.
          - ``interaction_only`` : type bool with default value False. If set as true, only interaction features are produced.
          - ``include_bias`` : type bool with default value True. It specifies whether or not to include a column of constant 1
            in the generated polynomial features.

        - **CBEncoder**

          None


    """
    __param_type_map = {'OneHotEncoder': {'ignore_unknown': 'integer'},
                        'LabelEncoder': {'minimum_fraction': 'float',
                                         'ignore_unknown': 'integer'},
                        'PolynomialFeatures': {'min_degree': 'integer',
                                               'max_degree': 'integer',
                                               'interaction_only': 'bool',
                                               'include_bias': 'bool'},
                        'CBEncoder': {}}

    def __init__(self, op_name, **kwargs):
        self.op_name = op_name
        self.hanaml_parameters = {'op_name': op_name}
        if op_name not in ['OneHotEncoder', 'CBEncoder', 'LabelEncoder', 'PolynomialFeatures']:
            msg = f'"{op_name}" is not a valid built-in operator.'
            logger.error(msg)
            raise ValueError(msg)
        self._fit_param = None
        kargs = {**kwargs}
        if kargs:
            try:
                self._fit_param = [(key.upper(), val, self.__param_type_map[op_name][key]) for key, val in kargs.items()]
            except KeyError as err:
                msg = str(err) + f' is not a valid argument for operator "{op_name}".'
                logger.error(msg)
                raise KeyError(msg)
        self._predict_param = None
        self._score_param = None

    def fit(self, data):
        """
        Dummy function.
        """
        if not hasattr(self, 'fit_data'):
            setattr(self, 'fit_data', data)
        return self

    def fit_transform(self, data):
        """
        Dummy function.
        """
        return data

def _merge_meta_pivoted_tables(meta_df, pivoted_df):
    """
    Obtain a HANA DataFrame from PIPELINE_EXECUTE results of two tables after applying "ToPivoted" operator
    """
    df1 = meta_df.rename_columns({'COLUMN_ID': "COLUMN_ID_META"})
    df2 = pivoted_df.rename_columns({'COLUMN_ID': "COLUMN_ID_VALUES"})
    # join and pivot two output tables
    joined = df1.join(df2, 'COLUMN_ID_META=COLUMN_ID_VALUES').select("COLUMN_NAME", "ROW_ID", "VALUE").cast({"VALUE":"DOUBLE"}).pivot_table(values='VALUE', index='ROW_ID', columns='COLUMN_NAME')
    # order the columns based on meta table - COLUMN_ID/COLUMN_NAME
    cols_name_list = meta_df.sort("COLUMN_ID").select("COLUMN_NAME").collect()['COLUMN_NAME'].tolist()
    col_type_dict = meta_df.collect().set_index('COLUMN_NAME')['DATA_TYPE'].to_dict()
    # cast the data type of each column
    result_df = joined[cols_name_list].cast(col_type_dict)

    return result_df

class Pipeline(PALBase, AMDPHelper): #pylint: disable=useless-object-inheritance
    """
    Pipeline construction to run transformers and estimators sequentially.

    Parameters
    ----------

    steps : list
        List of (name, transform) tuples that are chained. The last object should be an estimator.
    """

    __estimator_list = {'NB_Classifier',
                        'M_LOGR_Classifier',
                        'SVM_Classifier',
                        'RDT_Classifier',
                        'DT_Classifier',
                        'HGBT_Classifier',
                        'MLP_Classifier',
                        'MLP_M_TASK_Classifier',
                        'POL_Regressor',
                        'LOG_Regressor',
                        'HGBT_Regressor',
                        'GLM_Regressor',
                        'GEO_Regressor',
                        'RDT_Regressor',
                        'EXP_Regressor',
                        'MLP_Regressor',
                        'MLP_M_TASK_Regressor',
                        'DT_Regressor',
                        'MLR_Regressor',
                        'SVM_Regressor',
                        'SingleExpSm',
                        'TripleExpSm',
                        'DoubleExpSm',
                        'BrownExpSm',
                        'BSTS',
                        'ARIMA',
                        'AMTSA',
                        'HGBT_TimeSeries',
                        'MLR_TimeSeries'}
    def __init__(self, steps=None, pipeline=None):
        super(Pipeline, self).__init__()
        AMDPHelper.__init__(self)
        if steps is None:
            steps = []
        if isinstance(steps, str):
            steps_str = steps
            self.steps = eval(steps)
        else:
            self.steps = steps
            temp_steps = []
            for step in steps:
                nested_parameters = []
                for kkey, vval in step[1].hanaml_parameters.items():
                    if isinstance(vval, str):
                        nested_parameters.append("{}='{}'".format(kkey, vval))
                    else:
                        nested_parameters.append("{}={}".format(kkey, vval))

                temp_steps.append("(\"{}\", {}({}))".format(step[0],
                                                          step[1].__module__ + '.' + type(step[1]).__name__,
                                                          ", ".join(nested_parameters)))
            steps_str = "[{}]".format(", ".join(temp_steps))
        self.hanaml_parameters = {"steps": steps_str}
        self.nodes = []
        self.pipeline = None
        self.pipeline_only = pipeline
        self.predict_info_ = None
        self.info_ = None
        self.use_pal_pipeline_fit = None
        if self.pipeline_only:
            if isinstance(self.pipeline_only, dict):
                self.pipeline_only = json.dumps(pipeline)
            else:
                self.pipeline_only = pipeline
            self.pipeline = self.pipeline_only
            self.use_pal_pipeline_fit = True
        self._is_autologging = False
        self._autologging_model_storage_schema = None
        self._autologging_model_storage_meta = None
        self.is_exported = False
        self.registered_model_name = None
        self.report = None

    def enable_mlflow_autologging(self, schema=None, meta=None, is_exported=False, registered_model_name=None):
        """
        Enables mlflow autologging. Only works for fit function.

        Parameters
        ----------
        schema : str, optional
            Defines the model storage schema for mlflow autologging.

            Defaults to the current schema.
        meta : str, optional
            Defines the model storage meta table for mlflow autologging.

            Defaults to 'HANAML_MLFLOW_MODEL_STORAGE'.
        is_exported : bool, optional
            Determines whether export the HANA model to mlflow.

            Defaults to False.
        registered_model_name : str, optional
            mlflow registered_model_name.

            Defaults to None.
        """
        self._is_autologging = True
        self._autologging_model_storage_schema = schema
        self._autologging_model_storage_meta = meta
        self.is_exported = is_exported
        self.registered_model_name = registered_model_name

    def disable_mlflow_autologging(self):
        """
        It will disable mlflow autologging.
        """
        self._is_autologging = False

    def _check_if_type_ts(self):
        expsm_estimators = [SingleExponentialSmoothing, DoubleExponentialSmoothing,
                            TripleExponentialSmoothing, BrownExponentialSmoothing,
                            AutoExponentialSmoothing]
        ts_estimators = [AutoARIMA, AdditiveModelForecast, BSTS] + expsm_estimators
        if self.pipeline_only:
            type_ts = False
            expsm_flag = False
            if 'ExpSm' in str(self.pipeline).upper():
                expsm_flag = True
                type_ts = True
            if 'BSTS' in str(self.pipeline).upper():
                type_ts = True
            if 'ARIMA' in str(self.pipeline).upper():
                type_ts = True
            if 'AMTSA' in str(self.pipeline).upper():
                type_ts = True
            if 'TimeSeries' in str(self.pipeline).upper():
                type_ts = True
            return False, False
        else:
            type_ts = any(isinstance(self.steps[-1][1], ts_est) for ts_est in ts_estimators)
            expsm_flag = isinstance(self.steps[-1][1], tuple(expsm_estimators))
            return type_ts, expsm_flag

    def fit_transform(self, data, fit_params=None):
        """
        Fit all the transforms one after the other and transform the data.

        Parameters
        ----------

        data : DataFrame
            SAP HANA DataFrame to be transformed in the pipeline.

        fit_params : dict, optional
            The parameters corresponding to the transformer's name
            where each parameter name is prefixed such that parameter p for step s has key s__p.

            Defaults to None.

        Returns
        -------

        DataFrame
            The transformed SAP HANA DataFrame.

        Examples
        --------

        >>> my_pipeline = Pipeline([
                ('PCA', PCA(scaling=True, scores=True)),
                ('imputer', Imputer(strategy='mean'))
                ])
        >>> fit_params = {'PCA__key': 'ID', 'PCA__label': 'CLASS'}
        >>> my_pipeline.fit_transform(data=train_df, fit_params=fit_params)

        """
        data_ = data
        count = 0
        if fit_params is None:
            fit_params = {}
        for step in self.steps:
            fit_param_str = ''
            m_fit_params = {}
            for param_key, param_val in fit_params.items():
                if "__" not in param_key:
                    raise ValueError("The parameter name format incorrect. The parameter name is prefixed such that parameter p for step s has key s__p.")
                step_marker, param_name = param_key.split("__")
                if step[0] in step_marker:
                    m_fit_params[param_name] = param_val
                    fit_param_str = fit_param_str + ",\n" + param_name + "="
                    if isinstance(param_val, str):
                        fit_param_str = fit_param_str + "'{}'".format(param_val)
                    else:
                        fit_param_str = fit_param_str + str(param_val)
            data_ = step[1].fit_transform(data_, **m_fit_params)
            self.nodes.append((step[0],
                               "{}.fit_transform(data={}{})".format(_get_obj(step[1]),
                                                                    repr(data_),
                                                                    fit_param_str),
                               [str(count)],
                               [str(count + 1)]))
            count = count + 1
        return data_

    @mlflow_autologging(logtype='pal_fit')
    @trace_sql
    def fit(self, data,
            key=None,
            features=None,
            label=None,
            fit_params=None,
            categorical_variable=None,
            generate_json_pipeline=False,
            use_pal_pipeline_fit=True,
            endog=None,
            exog=None,
            model_table_name=None,
            use_explain=None,
            explain_method=None,
            background_size=None,
            background_sampling_seed=None):
        """
        Fit function for a pipeline.

        Parameters
        ----------
        data : DataFrame
            SAP HANA DataFrame.

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

            Defaults to the name of the last non-ID column.
        fit_params : dict, optional
            Parameters corresponding to the transformers/estimator name
            where each parameter name is prefixed such that parameter p for step s has key s__p.

            Defaults to None.
        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.
        generate_json_pipeline : bool, optional
            Help generate json formatted pipeline.

            Defaults to False.
        use_pal_pipeline_fit : bool, optional
            Use PAL's pipeline fit function instead of the original chain execution.

            Defaults to True.
        endog : str, optional
            Specifies the endogenous variable in time-series data.
            Please use ``endog`` instead of ``label`` if ``data`` is time-series data.

            Defaults to the name of 1st non-key column in ``data``.
        exog : str or a list of str, optional
            Specifies the exogenous variables in time-series data.
            Please use ``exog`` instead of ``features`` if ``data`` is time-series data.

            Defaults to

              - the list of names of all non-key, non-endog columns in ``data`` if final estimator
                is not ExponentialSmoothing based
              - [] otherwise.
        model_table_name : str, optional
            Specifies the HANA model table name instead of the generated temporary table.

            Defaults to None.
        use_explain : bool, optional
            Specifies whether to store information for pipeline explaination. Please note that this option is applicable only when the estimator in the pipeline is either a Classifier/Regressor/Timeseries.

            Defaults to False.
        explain_method : str, optional
            Specifies the explaination method. Only valid when `use_explain` is set to True. Only valid when the estimator in the pipeline is either a Classifier/Regressor.

            Options are:

            - 'kernelshap' : This method makes explainations by utilizing the Kernel SHAP. For this option to be functional, the ``background_size`` parameter should be greater than 0.
            - 'globalsurrogate' : This method makes explainations by utilizing the Global Surrogate method.

            Defaults to 'globalsurrogate'.
        background_size : int, optional
            The number of background data used in Kernel SHAP. Only valid ``explain_method`` is 'kernelshap'. It should not be larger than the row size of train data.

            Dependencies:

            - Classifier/Regressor: This option is only valid when ``use_explain`` is set to True, and ``explain_method`` is 'kernelshap'.
            - Timeseries: This option is only valid when ``use_explain`` is True.

            Defaults to None.
        background_sampling_seed : int, optional
            Specifies the seed for random number generator in the background sampling.

            - 0: Uses the current time (in second) as seed
            - Others: Uses the specified value as seed

            Dependencies:

            - Classifier/Regressor: This option is only valid when ``use_explain`` is set to True, and ``explain_method`` is 'kernelshap'.
            - Timeseries: This option is only valid when ``use_explain`` is True.

            Defaults to 0.

        Examples
        --------

        >>> my_pipeline = Pipeline([
            ('pca', PCA(scaling=True, scores=True)),
            ('imputer', Imputer(strategy='mean')),
            ('hgbt', HybridGradientBoostingClassifier(
            n_estimators=4, split_threshold=0, learning_rate=0.5, fold_num=5,
            max_depth=6, cross_validation_range=cv_range))
            ])
        >>> fit_params = {'pca__key': 'ID',
                          'pca__label': 'CLASS',
                          'hgbt__key': 'ID',
                          'hgbt__label': 'CLASS',
                          'hgbt__categorical_variable': 'CLASS'}
        >>> hgbt_model = my_pipeline.fit(data=train_data, fit_params=fit_params)
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        self.use_pal_pipeline_fit = use_pal_pipeline_fit
        type_ts, expsm_flag = self._check_if_type_ts()
        if type_ts:
            features, label = exog, endog
        features = [] if expsm_flag else features
        if not use_pal_pipeline_fit:
            data_ = data.rearrange(key=key, features=features, label=label,
                                   type_ts=type_ts)
        else:
            data_ = data
        setattr(self, 'fit_data', data_)
        self.label = label
        conn = data_.connection_context
        count = 0
        if fit_params is None:
            fit_params = {}
        obj = None
        if not self.pipeline_only:
            for step in self.steps:
                fit_param_str = ''
                m_fit_params = {}
                for param_key, param_val in fit_params.items():
                    if "__" not in param_key:
                        raise ValueError("The parameter name format incorrect. The parameter name is prefixed such that parameter p for step s has key s__p.")
                    step_marker, param_name = param_key.split("__")
                    if step[0] in step_marker:
                        m_fit_params[param_name] = param_val
                        fit_param_str = fit_param_str + ",\n" + param_name + "="
                        if isinstance(param_val, str):
                            fit_param_str = fit_param_str + "'{}'".format(param_val)
                        else:
                            fit_param_str = fit_param_str + str(param_val)
                obj = step[1]
                if count < len(self.steps) - 1:
                    if use_pal_pipeline_fit:
                        if not hasattr(step[1], 'op_name'):
                            obj_cls_name = step[1].__class__.__name__
                            msg = f'Operator {obj_cls_name} not supported in PAL Pipeline.'
                            raise ValueError(msg)
                        obj.disable_hana_execution()
                        if hasattr(obj, 'thread_ratio') and (getattr(obj, 'thread_ratio') is not None):
                            setattr(obj, 'thread_ratio', None)#thread_ratio must be removed if using pal pipeline fit
                    if not isinstance(obj, OutlierDetectionTS):
                        if use_pal_pipeline_fit:
                            data_ = self.fit_data
                        data_ = obj.fit_transform(data_, **m_fit_params)
                        self.nodes.append((step[0],
                                        "{}.fit_transform(data={}{})".format(_get_obj(obj),
                                                                                repr(data_),
                                                                                fit_param_str),
                                        [str(count)],
                                        [str(count + 1)]))
                    else:
                        data_ = obj.fit_predict(data_, **m_fit_params)
                        self.nodes.append((step[0],
                                        "{}.fit_predict(data={}{})".format(_get_obj(obj),
                                                                                repr(data_),
                                                                                fit_param_str),
                                        [str(count)],
                                        [str(count + 1)]))
                else:
                    if use_pal_pipeline_fit:
                        if not hasattr(step[1], 'op_name'):
                            obj_cls_name = step[1].__class__.__name__
                            msg = f'Operator {obj_cls_name} not supported in PAL Pipeline.'
                            raise ValueError(msg)
                        obj.disable_hana_execution()
                        if hasattr(obj, 'thread_ratio') and (getattr(obj, 'thread_ratio') is not None):
                            setattr(obj, 'thread_ratio', None)#thread_ratio must be removed if using pal pipeline fit
                    if expsm_flag:
                        obj.fit_predict(data_, **m_fit_params)
                    else:
                        obj.fit(data_, **m_fit_params)
                    fit_func = 'fit_predict' if expsm_flag else 'fit'
                    self.nodes.append((step[0],
                                    "{}.{}(data={}{})".format(_get_obj(obj),
                                                                fit_func,
                                                                repr(data_),
                                                                fit_param_str),
                                    [str(count)],
                                    [str(count + 1)]))
                count = count + 1
            if generate_json_pipeline and not use_pal_pipeline_fit:
                self.generate_json_pipeline()
        if use_pal_pipeline_fit:
            if not self.pipeline_only:
                self.pipeline = self.generate_json_pipeline()
            op_dict = json.loads(self.pipeline)
            if isinstance(op_dict, (list, tuple)):
                op_dict = op_dict[0]
            last_operator = list(op_dict.keys())[0]
            if last_operator in self.__estimator_list:
                if isinstance(categorical_variable, str):
                    categorical_variable = [categorical_variable]
                categorical_variable = self._arg('categorical_variable',
                                                 categorical_variable,
                                                 ListOfStrings)
                pipeline_param = [('HAS_ID', key is not None, None, None),
                                  ('DEPENDENT_VARIABLE', None, None, label)]
                if categorical_variable is not None:
                    pipeline_param.extend([('CATEGORICAL_VARIABLE', None, None, var) for var in categorical_variable])
                use_explain = self._arg('use_explain', use_explain, bool)
                if use_explain:
                    explain_method = self._arg('explain_method', explain_method, {'kernelshap' : 0, 'globalsurrogate' : 1})
                    background_size = self._arg('background_size', background_size, int)
                    background_sampling_seed = self._arg('background_sampling_seed', background_sampling_seed, int)
                    pipeline_param.extend([('BACKGROUND_SIZE', background_size, None, None),
                                           ('BACKGROUND_SAMPLING_SEED', background_sampling_seed, None, None),
                                           ('USE_EXPLAIN', use_explain, None, None),
                                           ('EXPLAIN_METHOD', explain_method, None, None)])
                if isinstance(self.pipeline, dict):
                    pipeline = json.dumps(self.pipeline)
                else:
                    pipeline = self.pipeline
                pipeline_param.extend([('PIPELINE', None, None, pipeline)])
                unique_id = str(uuid.uuid1()).replace('-', '_').upper()
                outputs = ['MODEL', 'INFO']
                outputs = ['#PAL_PIPELINE_{}_TBL_{}_{}'.format(name, self.id, unique_id) for name in outputs]
                if model_table_name:
                    outputs[0] = model_table_name
                model_tbl, info_tbl = outputs
                try:
                    self._call_pal_auto(conn,
                                        'PAL_PIPELINE_FIT',
                                        data.rearrange(key=key, features=features,
                                                       label=label, type_ts=type_ts),
                                        ParameterTable().with_data(pipeline_param),
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
                self.info_ = conn.table(info_tbl)
            else: # No estimator, use PAL_PIPELINE_EXECUTE
                if not self.pipeline_only:
                    self.pipeline = self.generate_json_pipeline(pivot=True)
                pipeline_param=[('EXECUTE', None, None, 'fit')]
                unique_id = str(uuid.uuid1()).replace('-', '_').upper()
                outputs = ['OUTPUT_MODEL', 'OUTPUT_1', 'OUTPUT_2', 'OUTPUT_3']
                outputs = ['#PAL_PIPELINE_EXECUTE_FIT_{}_TBL_{}_{}'.format(name, self.id, unique_id) for name in outputs]
                output_model, output_tbl_1, output_tbl_2, output_tbl_3 = outputs
                if model_table_name: # update model name
                    outputs[0] = model_table_name
                model_df = conn.sql("SELECT 0 INDEX, {} CONTENT FROM DUMMY;".format("'" + self.pipeline + "'"))
                dummy_df = conn.sql("SELECT TOP 0 * FROM (SELECT 1 NAME, 2 VALUE FROM DUMMY) dt;")
                meta_tbl_df = conn.sql("SELECT TOP 0 * FROM (SELECT 1 TABLE_ID, 'aaa' TABLE_NAME, 1 COLUMN_ID, 'aaa' COLUMN_NAME, 'aaa' DATA_TYPE, 1 MAX_LENGTH FROM DUMMY) dt;")
                meta_tbl_df = meta_tbl_df.cast({"TABLE_ID":"INTEGER", "TABLE_NAME":"VARCHAR(256)", "COLUMN_ID":"INTEGER", "COLUMN_NAME":"VARCHAR(256)", "DATA_TYPE":"VARCHAR(256)", "MAX_LENGTH":"INTEGER"})
                pivoted_result_df = conn.sql("SELECT TOP 0 * FROM (SELECT 1 TABLE_ID, 1 COLUMN_ID, 1 ROW_ID, 'aaa' VALUE FROM DUMMY) dt;")
                pivoted_result_df = pivoted_result_df.cast({"TABLE_ID":"INTEGER", "COLUMN_ID":"INTEGER", "ROW_ID":"INTEGER", "VALUE":"VARCHAR(256)"})
                try:
                    self._call_pal_auto(conn,
                                        'PAL_PIPELINE_EXECUTE',
                                        model_df,
                                        ParameterTable().with_data(pipeline_param),
                                        data.rearrange(key=key, features=features, label=label, type_ts=type_ts),
                                        dummy_df, # input data table 2
                                        dummy_df, # input data table 3
                                        meta_tbl_df, # meta tbl
                                        pivoted_result_df, # pivoted result
                                        dummy_df, # dummy output template table 3
                                        *outputs)
                except dbapi.Error as db_err:
                    logger.exception(str(db_err))
                    try_drop(conn, outputs)
                    raise
                except Exception as db_err:
                    logger.exception(str(db_err))
                    try_drop(conn, outputs)
                    raise
                if not self._disable_hana_execution:
                    self.model_ = conn.table(outputs[0])
                    meta_tbl = conn.table(outputs[1])
                    pivoted_tbl = conn.table(outputs[2])
                    self.transformed_data = _merge_meta_pivoted_tables(meta_tbl, pivoted_tbl)
        return self

    def predict(self, data, key=None, features=None, model=None,
                exog=None, predict_args=None,
                show_explainer=False,
                top_k_attributions=None,
                random_state=None,
                sample_size=None,
                verbose_output=None,
                ):
        r"""
        Predict function for a pipeline.

        Parameters
        ----------
        data :  DataFrame
            SAP HANA DataFrame.

        key : str, optional

            Name of the ID column. Mandatory if ``data`` is not indexed, or is indexed by multiple columns.

            Defaults to the index of ``data`` if ``data`` is indexed by a single column.

        features : a list of str, optional

            Names of the feature columns.

            If ``features`` is not provided, it defaults to all non-ID columns.

        model : DataFrame, optional
            The model to be used for prediction.

            Defaults to the fitted model (model\_).

        exog : list of str, optional
            Names of exogenous variables.

            Please use ``exog`` instead of ``features`` when the final estimator of the Pipeline object
            is for TimeSeries.

            Defaults to all non-key columns if not provided.

        predict_args : dict, optional
            Specifies the parameters for the predict method of the estimator
            of the target pipeline, with keys being parameter names and values
            being parameter values.

            For example, suppose the input pipeline is
            [('PCA', PCA()), ('RDT', RDTClassifier(algorithms='cart')],
            and the estimator **RDTClassifier** can
            take the following parameters when making predictions:
            ``block_size``, ``missing_replacement``. Then, we can specify the values
            of these two parameters as follows:

            predict_args = {'block_size':5, 'missing_replacement':'instance_marginalized'}

            Defaults to None.

        show_explainer : bool, optional
            If True, the reason code of the pipelie will be returned. Please note that this option is applicable only when the estimator in the pipeline is either a Classifier or a Regressor.

            Defaults to False

        top_k_attributions : int, optional
            Displays the top k attributions in reason code. Only valid when ``show_explainer`` is set to True.

            Effective only when ``model`` contains background data from the training phase.

            Defaults to PAL's default value.

        random_state : DataFrame, optional
            Specifies the random seed.  Only valid when ``show_explainer`` is set to True.

            Defaults to 0(system time).

        sample_size : int, optional
            Specifies the number of sampled combinations of features.  Only valid when ``show_explainer`` is set to True.

            It is better to use a number that is greater than the number of features
            in ``data``.

            If set as 0, it is determined by algorithm heuristically.

            Defaults to 0.

        verbose_output : bool, optional

            - True: Outputs the probability of all label categories.
            - False: Outputs the category of the highest probability only.

            Only valid when ``show_explainer`` is set to True.

            Defaults to True.


        Attributes
        ----------
        predict_info_ : DataFrame
            Structured as follows:

            - 1st column: STAT_NAME.
            - 2nd column: STAT_VALUE.

        Returns
        -------
        DataFrame
            Predicted result, structured as follows:

            - 1st column: Data type and name same as the 1st column of ``data``.
            - 2nd column: SCORE, predicted values(for regression) or class labels(for classification).
            - 3rd column: CONFIDENCE, confidence of a class (available only if ``show_explainer`` is True).
            - 4th column: REASON CODE, attributions of features (available only if ``show_explainer`` is True).
            - 5th & 6th columns: placeholder columns for future implementations(available only if ``show_explainer`` is True).
        """
        conn = data.connection_context
        if model is None:
            if getattr(self, 'model_') is None:
                raise FitIncompleteError()
            if isinstance(self.model_, (list, tuple)):
                model = self.model_[0]
            else:
                model = self.model_
        type_ts, _ = self._check_if_type_ts()
        if type_ts:
            features = exog
        predict_args_json, no_valid_args = AutomaticClassification()._gen_predict_args_json(predict_args)
        param_rows = []
        calling_function = 'PAL_PIPELINE_PREDICT'
        if no_valid_args:
            param_rows.extend([('PREDICT_ARGS', None, None, predict_args_json)])
        if show_explainer:
            calling_function = 'PAL_PIPELINE_EXPLAIN' #not implemented for time-series data
            param_rows.extend([('TOP_K_ATTRIBUTIONS', top_k_attributions, None, None),
                               ('SEED', random_state, None, None),
                               ('SAMPLESIZE', sample_size, None, None),
                               ('VERBOSE_OUTPUT', verbose_output, None, None)])

        data_ = data.rearrange(key=key, features=features, for_predict=True)
        setattr(self, 'predict_data', data_)
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        outputs = ['RESULT', 'INFO']
        outputs = ['#PAL_PIPELINE_{}_RESULT_TBL_{}_{}'.format(name, self.id, unique_id)
                   for name in outputs]
        result_tbl, info_tbl = outputs

        try:
            self._call_pal_auto(conn,
                                calling_function,
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
        self.predict_info_ = conn.table(info_tbl)
        return result_df

    @trace_sql
    def score(self, data, key=None, features=None,#pylint:disable=too-many-arguments
              label=None, model=None,
              random_state=None,
              top_k_attributions=None,
              sample_size=None,
              verbose_output=None,
              predict_args=None,
              endog=None,
              exog=None):
        r"""
        Score function for a fitted pipeline model.

        Parameters
        ----------
        data : DataFrame
            SAP HANA DataFrame.

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

            Defaults to the name of the last non-ID column.

        model : str, optional
            The trained model.

            Defaults to self.model\_.

        random_state : int, optional
            Specifies the seed for random number generator.

            - 0: Uses the current time (in seconds) as the seed.
            - Others: Uses the specified value as the seed.

            Valid only when model table has background data information.

            Defaults to 0.

        top_k_attributions : str, optional
            Outputs the attributions of top k features which contribute the most.
            Valid only when model table has background data information.

            Defaults to 10.

        sample_size : int, optional
            Specifies the number of sampled combinations of features.

            It is better to use a number that is greater than the number of features
            in ``data``.

            If set as 0, it is determined by algorithm heuristically.

            Defaults to 0.

        verbose_output : bool, optional
            Specifies whether to output all classes and the corresponding confidences for each data.

            - True: Outputs the probability of all label categories.
            - False: Outputs the category of the highest probability only.

            Valid only for classification.

            Defaults to False.

        predict_args : dict, optional
            Specifies the parameters for the predict method of the estimator
            of the target pipeline, with keys being parameter names and values
            being parameter values.

            For example, suppose the input pipeline is
            [('PCA', PCA()), ('RDT', RDTClassifier(algorithms='cart')],
            and the estimator **RDTClassifier** can
            take the following parameters when making predictions:
            ``block_size``, ``missing_replacement``. Then, we can specify the values
            of these two parameters as follows:

            predict_args = {'block_size':5, 'missing_replacement':'instance_marginalized'}

            Defaults to None.

        endog : str, optional
            Specifies the endogenous variable in time-series data.

            Please use ``endog`` instead of ``label`` if ``data`` is time-series.

            Defaults to the name of 1st non-key column in ``data``.

        exog : str or a list of str, optional
            Specifies the exogenous variables in time-series data.

            Please use ``exog`` instead of ``features`` if ``data`` is time-series.

            Defaults to

              - the list of names of all non-key, non-endog columns in ``data`` if final estimator
                is not ExponentialSmoothing based
              - [] otherwise.


        Returns
        -------
        DataFrame 1

            Prediction result, structured as follows:

            - 1st column, ID of input data.
            - 2nd column, SCORE, class assignment.
            - 3rd column, REASON CODE, attribution of features.
            - 4th & 5th column, placeholder columns for future implementations.

        DataFrame 2

            Statistics, structured as follows:

            - 1st column, STAT_NAME
            - 2nd column, STAT_VALUE

        """
        setattr(self, 'hanaml_score_params', pal_param_register())
        setattr(self, 'testing_data', data)
        conn = data.connection_context
        random_state = self._arg('random_state', random_state, int)
        sample_size = self._arg('sample_size', sample_size, int)
        top_k_attributions = self._arg('top_k_attributions',
                                       top_k_attributions, int)
        verbose_output = self._arg('verbose_output', verbose_output, bool)
        type_ts, expsm_flag = self._check_if_type_ts()
        if model is None:
            if getattr(self, 'model_') is None:
                raise FitIncompleteError()
            if isinstance(self.model_, (list, tuple)):
                model = self.model_[0]
            else:
                model = self.model_
        if type_ts:
            features, label = exog, endog
        if expsm_flag:
            features = []
        predict_args_json, no_valid_args = AutomaticClassification()._gen_predict_args_json(predict_args)
        param_rows = []
        if no_valid_args:
            param_rows.extend([('PREDICT_ARGS', None, None, predict_args_json)])
        data_ = data.rearrange(key=key, features=features, label=label, type_ts=type_ts)
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        outputs = ['RESULT', 'STATS', 'PH1', 'PH2']
        output_tbls = [f'#PAL_PIPELINE_{name}_RESULT_TBL_{self.id}_{unique_id}' for name in outputs]
        param_rows.extend([
            ('SEED', random_state, None, None),
            ('TOP_K_ATTRIBUTIONS', top_k_attributions, None, None),
            ('SAMPLESIZE', sample_size, None, None),
            ('VERBOSE_OUTPUT', verbose_output, None, None)])
        setattr(self, 'score_data', data_)
        if not (check_pal_function_exist(conn, '%PIPELINE_SCORE%', like=True) or self._disable_hana_execution):
            msg = 'The version of your SAP HANA does not support pipeline score function!'
            logger.error(msg)
            raise ValueError(msg)
        try:
            self._call_pal_auto(conn,
                                'PAL_PIPELINE_SCORE',
                                data_,
                                model,
                                ParameterTable().with_data(param_rows),
                                *output_tbls)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, output_tbls)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, output_tbls)
            raise
        setattr(self, 'score_metrics_', conn.table(output_tbls[1]))
        return tuple(conn.table(tbl) for tbl in output_tbls[:2])

    def fit_predict(self, data, apply_data=None, fit_params=None, predict_params=None):
        """
        Fit all the transformers one after another and transform the
        data, then fit_predict the transformed data using the final estimator.

        Parameters
        ----------
        data : DataFrame
            SAP HANA DataFrame to be transformed in the pipeline.

        apply_data : DataFrame
            SAP HANA DataFrame to be predicted in the pipeline.

            Defaults to None.
        fit_params : dict, optional
            Parameters corresponding to the transformers/estimator name
            where each parameter name is prefixed such that parameter p for step s has key s__p.

            Defaults to None.
        predict_params : dict, optional
            Parameters corresponding to the predictor name
            where each parameter name is prefixed such that parameter p for step s has key s__p.

        Returns
        -------
        DataFrame
            A SAP HANA DataFrame.

        Examples
        --------

        >>> my_pipeline = Pipeline([
            ('pca', PCA(scaling=True, scores=True)),
            ('imputer', Imputer(strategy='mean')),
            ('hgbt', HybridGradientBoostingClassifier(
            n_estimators=4, split_threshold=0, learning_rate=0.5, fold_num=5,
            max_depth=6, cross_validation_range=cv_range))
            ])
        >>> fit_params = {'pca__key': 'ID',
                          'pca__label': 'CLASS',
                          'hgbt__key': 'ID',
                          'hgbt__label': 'CLASS',
                          'hgbt__categorical_variable': 'CLASS'}
        >>> hgbt_model = my_pipeline.fit_predict(data=train_data, apply_data=test_data, fit_params=fit_params)
        """
        data_ = data
        if apply_data:
            apply_data_ = apply_data
        count = 0
        if fit_params is None:
            fit_params = {}
        if predict_params is None:
            predict_params = {}
        for step in self.steps:
            fit_param_str = ''
            m_fit_params = {}
            m_predict_params = {}
            for param_key, param_val in fit_params.items():
                if "__" not in param_key:
                    raise ValueError("The parameter name format incorrect. The parameter name is prefixed such that parameter p for step s has key s__p.")
                step_marker, param_name = param_key.split("__")
                if step[0] in step_marker:
                    m_fit_params[param_name] = param_val
                    fit_param_str = fit_param_str + ",\n" + param_name + "="
                    if isinstance(param_val, str):
                        fit_param_str = fit_param_str + "'{}'".format(param_val)
                    else:
                        fit_param_str = fit_param_str + str(param_val)
            predit_param_str = ''
            for param_key, param_val in predict_params.items():
                if "__" not in param_key:
                    raise ValueError("The parameter name format incorrect. The parameter name is prefixed such that parameter p for step s has key s__p.")
                step_marker, param_name = param_key.split("__")
                if step[0] in step_marker:
                    m_predict_params[param_name] = param_val
                    predit_param_str = predit_param_str + ",\n" + param_name + "="
                    if isinstance(param_val, str):
                        predit_param_str = predit_param_str + "'{}'".format(param_val)
                    else:
                        predit_param_str = predit_param_str + str(param_val)
            if count < len(self.steps) - 1:
                data_ = step[1].fit_transform(data_, **m_fit_params)
                apply_data_ = step[1].fit_transform(apply_data_, **m_predict_params)
                self.nodes.append((step[0],
                                   "{}\n.fit_transform(data={}{})".format(_get_obj(step[1]),
                                                                          repr(data_),
                                                                          fit_param_str),
                                   [str(count)],
                                   [str(count + 1)]))
            else:
                expsm_estimators = [SingleExponentialSmoothing, DoubleExponentialSmoothing,
                                    TripleExponentialSmoothing, BrownExponentialSmoothing,
                                    AutoExponentialSmoothing]
                expsm_flag = isinstance(step[1], tuple(expsm_estimators))
                if expsm_flag:
                    m_fit_predict_params = m_fit_params
                    m_fit_predict_params.update(m_predict_params)
                    data_ = step[1].fit_predict(data_, **m_fit_predict_params)
                    fit_predict_param_str = fit_param_str + ", " + predit_param_str[2:]
                    self.nodes.append((step[0],
                                      "{}\n.fit_predict(data={}{})".format(_get_obj(step[1]),
                                                                           repr(data_),
                                                                           fit_predict_param_str),
                                      [str(count)],
                                      [str(count + 1)]))
                else:
                    if apply_data:
                        data_ = step[1].fit(data_, **m_fit_params).predict(apply_data_, **m_predict_params)
                    else:
                        data_ = step[1].fit(data_, **m_fit_params).predict(**m_predict_params)
                    if apply_data:
                        apply_param_str = repr(apply_data_) + ", " + predit_param_str[2:]
                    else:
                        apply_param_str = predit_param_str[2:]
                    self.nodes.append((step[0],
                                      "{}\n.fit(data={}{})\n.predict({})".format(_get_obj(step[1]),
                                                                                 repr(data_),
                                                                                 fit_param_str,
                                                                                 apply_param_str),
                                      [str(count)],
                                      [str(count + 1)]))
            count = count + 1
        return data_

    def plot(self, name="my_pipeline", iframe_height=450):
        """
        Plot a pipeline.

        Parameters
        ----------
        name : str, optional
            Pipeline Name.

            Defaults to "my_pipeline".
        iframe_height : int, optional
            Height of iframe.

            Defaults to 450.
        """
        digraph = Digraph(name)
        node = []
        for elem in self.nodes:
            node.append(digraph.add_python_node(elem[0],
                                                elem[1],
                                                in_ports=elem[2],
                                                out_ports=elem[3]))
        for node_x in range(0, len(node) - 1):
            digraph.add_edge(node[node_x].out_ports[0],
                             node[node_x + 1].in_ports[0])
        digraph.build()
        digraph.generate_notebook_iframe(iframe_height)

    def generate_json_pipeline(self, pivot=False):
        """
        Generate the json formatted pipeline for pipeline fit function.
        """
        inputs = "ROWDATA"
        uni_mlp_mapping = {"HIDDEN_LAYER_ACTIVE_FUNC": "ACTIVATION",
                           "HIDDEN_LAYER_ACTIVE_FUNC_VALUES": "ACTIVATION_OPTIONS",
                           "HIDDEN_LAYER_SIZE_VALUES": "HIDDEN_LAYER_SIZE_OPTIONS",
                           "MAX_ITERATION": "MAX_ITER",
                           "MINI_BATCH_SIZE": "BATCH_SIZE",
                           "MINI_BATCH_SIZE_VALUES": "BATCH_SIZE_VALUES",
                           "MINI_BATCH_SIZE_RANGE": "BATCH_SIZE_RANGE",
                           "MOMENTUM_FACTOR": "MOMENTUM",
                           "MOMENTUM_FACTOR_VALUES": "MOMENTUM_VALUES",
                           "MOMENTUM_FACTOR_RANGE": "MOMENTUM_RANGE",
                           "OUTPUT_LAYER_ACTIVE_FUNC": "OUTPUT_ACTIVATION",
                           "OUTPUT_LAYER_ACTIVE_FUNC_VALUES": "OUTPUT_ACTIVATION_OPTIONS"}
        uni_svm_mapping = { "ERROR_TOL": "TOL"}
        if pivot is True:
            pipeline_list = []
            for step in self.steps:
                new_op = {}
                arg_dict = {}
                params = {}
                try:
                    fit_args = step[1].get_parameters()["fit"]
                except KeyError:
                    fit_args = {}
                for args in fit_args:
                    arg_key = args[0]
                    if self.use_pal_pipeline_fit:
                        if args[0] in ('HAS_ID', 'CATEGORICAL_VARIABLE',
                                       'KEY', 'DEPENDENT_VARIABLE', 'LABEL'):
                            continue
                        if isinstance(step[1], (MLPClassifier, MLPRegressor)):
                            if args[0] in uni_mlp_mapping.keys():
                                arg_key = uni_mlp_mapping[args[0]]
                        if isinstance(step[1], (SVC, SVR)):
                            if args[0] in uni_svm_mapping.keys():
                                arg_key = uni_svm_mapping[args[0]]
                    params[arg_key] = args[1]
                arg_dict["args"] = params
                new_op[step[1].op_name] = arg_dict
                pipeline_list.append(new_op)
            pipeline_list.append("ToPivoted")
            self.pipeline = json.dumps(pipeline_list)
        else:
            for step in self.steps:
                new_inputs = {}
                params = {}
                try:
                    fit_args = step[1].get_parameters()["fit"]
                except KeyError:
                    fit_args = {}
                for args in fit_args:
                    arg_key = args[0]
                    if self.use_pal_pipeline_fit:
                        if args[0] in ('HAS_ID', 'CATEGORICAL_VARIABLE',
                                       'KEY', 'DEPENDENT_VARIABLE', 'LABEL'):
                            continue
                        if isinstance(step[1], (MLPClassifier, MLPRegressor)):
                            if args[0] in uni_mlp_mapping.keys():
                                arg_key = uni_mlp_mapping[args[0]]
                        if isinstance(step[1], (SVC, SVR)):
                            if args[0] in uni_svm_mapping.keys():
                                arg_key = uni_svm_mapping[args[0]]
                    params[arg_key] = args[1]
                new_inputs["args"] = params
                new_inputs["inputs"] = {"data": inputs}
                inputs = {}
                if self.use_pal_pipeline_fit:
                    inputs[step[1].op_name] = new_inputs
                else:
                    inputs[step[0]] = new_inputs
            self.pipeline = json.dumps(inputs)
        return self.pipeline

    def create_amdp_class(self,
                          amdp_name,
                          training_dataset,
                          apply_dataset):
        """
        Create AMDP class file. Then build_amdp_class can be called to generate amdp class.

        Parameters
        ----------
        amdp_name : str
            Name of amdp.

        training_dataset : str
            Name of training dataset.

        apply_dataset : str
            Name of apply dataset.
        """
        self.add_amdp_template("tmp_hemi_pipeline_func.abap")
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
        self.add_amdp_item("<<TRAINING_DATASET>>",
                           training_dataset)
        self.add_amdp_item("<<APPLY_DATASET>>",
                           apply_dataset)
        param_meta = []
        param_default_meata = []
        for fit_param in self.get_fit_parameters():
            param_meta.append("( name = '{}' type = cl_hemi_constants=>cs_param_type-string role = cl_hemi_constants=>cs_param_role-train configurable = abap_true has_context = abap_false )".format(fit_param[0]))
            param_default_meata.append("( name = '{}' value = '{}' )".format(fit_param[0], fit_param[1]))
        if self.get_predict_parameters():
            for predict_param in self.get_predict_parameters():
                param_meta.append("name = '{}' type = cl_hemi_constants=>cs_param_type-string role = cl_hemi_constants=>cs_param_role-apply configurable = abap_true has_context = abap_false )".format(predict_param[0]))
                param_default_meata.append("( name = '{}' value = '{}' )".format(predict_param[0], predict_param[1]))
        self.add_amdp_item("<<PARAMETER>>",
                           "( {} )".format("\n".join(param_meta)))
        self.add_amdp_item("<<PARAMETER_DEFAULT>>",
                           "( {} )".format("\n".join(param_default_meata)))
        self.add_amdp_item("<<TARGET_COLUMN>>",
                           self.label)
        self.add_amdp_item("<<KEY_FIELD_DESCRIPTION>>",
                           '')
        self.add_amdp_item("<<RESULT_OUTPUT_STRUCTURE>>",
                           " " * 8 + "id TYPE string,\n" +\
                           " " * 8 + "score TYPE string,")
        predict_data_cols = ''
        predict_data_st = {}
        if hasattr(self, "predict_data_struct"):
            predict_data_st = self.predict_data_struct
        if hasattr(self, "predict_data"):
            if self.predict_data:
                predict_data_st = self.predict_data.get_table_structure()
        if predict_data_st.keys():
            for key in list(predict_data_st.keys())[:-1]:
                predict_data_cols = predict_data_cols + " " * 16 + "{},\n".format(key.lower())
            self.add_amdp_item("<<PREDICT_DATA_COLS>>",
                               predict_data_cols[:-2])
        return self

    def evaluate(self,
                 data,
                 key=None,
                 features=None,
                 label=None,
                 categorical_variable=None,
                 resampling_method=None,
                 fold_num=None,
                 random_state=None,
                 endog=None,
                 exog=None,
                 gap_num=None,
                 percentage=None):
        """
        Evaluation function for a pipeline.

        Parameters
        ----------

        data : DataFrame
            SAP HANA DataFrame.

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

        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.
        resampling_method : character, optional
            The resampling method for pipeline model evaluation.
            For different pipeline, the options are different.

            - regressor: {'cv', 'stratified_cv'}
            - classifier: {'cv'}
            - timeseries: {'rocv', 'block', 'simple_split'}

            Defaults to 'stratified_cv' if the estimator in ``pipeline`` is a classifier,
            and defaults to(and can only be) 'cv' if the estimator in ``pipeline`` is a regressor,
            and defaults to 'rocv' if the estimator in ``pipeline`` is a timeseries.

        fold_num : int, optional
            The fold number for cross validation.

            Defaults to 5.

        random_state : int, optional
            Specifies the seed for random number generator.

            - 0: Uses the current time (in seconds) as the seed.
            - Others: Uses the specified value as the seed.

            Defaults to 0.

        endog : str, optional
            Specifies the endogenous variable in time-series data.
            Please use ``endog`` instead of ``label`` if ``data`` is time-series.

            Defaults to the name of 1st non-key column in ``data``.
        exog : str or a list of str, optional
            Specifies the exogenous variables in time-series data.
            Please use ``exog`` instead of ``features`` if ``data`` is time-series.

            Defaults to

              - the list of names of all non-key, non-endog columns in ``data`` if final estimator
                is not ExponentialSmoothing based
              - [] otherwise.


        gap_num : int, optional
            Number of samples to exclude from the end of each train set before the test set.
            Valid only if the final estimator of the target `pipeline` is for time-series.

            Defaults to 0.

        percentage : float, optional
            Percentage between training data and test data. Only applicable when
            the final estimator of the target `pipeline` is for time-series, and
            ``resampling_method`` is set as 'block'.

            Defaults to 0.7.

        Returns
        -------
        DataFrame

          - 1st column, NAME, Score name
          - 2nd column, VALUE, Score value

        """
        pipeline = self.pipeline
        is_timeseries = None
        is_classification = None
        is_regression = None
        is_expsm = False
        if 'Classifier' in pipeline:
            is_classification = True
        elif 'Regressor' in pipeline:
            is_regression = True
        else:
            is_timeseries = True
            if 'ExpSm' in pipeline:
                is_expsm = True
        conn = data.connection_context
        key = self._arg('key', key, str)
        index = data.index
        if isinstance(index, str):
            if key is not None and index != key:
                msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(msg)
        key = index if key is None else key
        if key is None and is_timeseries:
            err_msg = "Parameter 'key' must be specified for the evaluate function of AutomaticTimeSeries."
            logger.error(err_msg)
            raise ValueError(err_msg)
        features = self._arg('features', features, ListOfStrings)
        label = self._arg('label', label, str)
        endog = self._arg('endog', endog, str)
        exog = self._arg('exog', exog, ListOfStrings)
        if is_timeseries:
            features, label = exog, endog
        if isinstance(categorical_variable, str):
            categorical_variable = [categorical_variable]
        categorical_variable = self._arg('categorical_variable', categorical_variable, ListOfStrings)
        cols = data.columns
        if key is not None:
            id_col = [key]
            cols.remove(key)
        else:
            id_col = []
        if label is None:
            label = cols[0] if is_timeseries else cols[-1]
        cols.remove(label)
        if features is None:
            features = [] if is_expsm else cols
        if is_timeseries:
            data_ = data[id_col + [label] + features]
        else:
            data_ = data[id_col + features + [label]]
        #data_ = data[id_col + features + [label]]
        if isinstance(pipeline, dict):
            pipeline = json.dumps(pipeline)
        if is_classification:
            resampling_map = {'cv': 'cv', 'stratified_cv': 'stratified_cv'}
        elif is_regression:
            resampling_map = {'cv':'cv'}
        else:
            resampling_map = {'rocv': 1, 'block': 2, 'simple_split': 3}
        resampling_method = self._arg('resampling_method', resampling_method, resampling_map)
        fold_num = self._arg('fold_num', fold_num, int)
        random_state = self._arg('random_state', random_state, int)
        gap_num = self._arg('gap_num', gap_num, int)
        percentage = self._arg('percentage', percentage, float)
        if is_regression:
            ptype = 'regressor'
        elif is_classification:
            ptype = 'classifier'
        else:
            ptype = 'timeseries'
        param_rows = [
            ('HAS_ID', key is not None, None, None),
            ('FOLD_NUM', fold_num, None, None),
            ('RANDOM_SEED', random_state, None, None),
            ('PIPELINE', None, None, pipeline),
            ('PIPELINE_TYPE', None, None, ptype),
            ('GAP_NUM', gap_num, None, None),
            ('PERCENTAGE', None, percentage, None)]

        if categorical_variable is not None:
            param_rows.extend(('CATEGORICAL_VARIABLE', None, None, variable)
                              for variable in categorical_variable)

        if resampling_method is not None:
            if is_timeseries:
                param_rows.extend([('SPLIT_METHOD', resampling_method, None, None)])
            else :
                param_rows.extend([('RESAMPLING_METHOD', None, None, resampling_method)])

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        result_tbl = '#PAL_AUTOML_EVALUATED_RESULT_TBL_{}_{}'.format(self.id, unique_id)
        try:
            self._call_pal_auto(conn,
                                'PAL_PIPELINE_EVALUATE',
                                data_,
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
        result_df = conn.table(result_tbl)
        return result_df

    def transform(self,
                  data=None,
                  key=None,
                  features=None,
                  label=None):
        """
        Transform function for a pipeline.

        Parameters
        ----------

        data : DataFrame, optional
            SAP HANA DataFrame.

            Defaults to None.

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

        Returns
        -------

        The transformed DataFrame.

        """
        if data is None:
            if self.transformed_data is not None:
                return self.transformed_data
        data_ = data
        transformed_data = None
        conn = data.connection_context
        if not self._disable_hana_execution:
            key = self._arg('key', key, str)
            index = data.index
            if isinstance(index, str):
                if key is not None and index != key:
                    msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                    "and the designated index column '{}'.".format(index)
                    logger.warning(msg)
            key = index if key is None else key
            if key is None:
                err_msg = "Parameter 'key' must be specified if the index of data is not provided!"
                logger.error(err_msg)
                raise ValueError(err_msg)
            features = self._arg('features', features, ListOfStrings)
            label = self._arg('label', label, str)
            cols = data.columns
            id_col = [key]
            cols.remove(key)
            if label is not None:
                cols.remove(label)
            if features is None:
                features = cols
            if label is not None:
                id_label_df = data.select(key, label).rename_columns({key:"RAW_ID"})
            data_ = data[id_col + features]

        pipeline_param=[('EXECUTE', None, None, 'predict')]

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        outputs = ['OUTPUT_MODEL', 'OUTPUT_1', 'OUTPUT_2', 'OUTPUT_3']
        outputs = ['#PAL_PIPELINE_EXECUTE_PREDICT_{}_TBL_{}_{}'.format(name, self.id, unique_id) for name in outputs]
        output_model, output_tbl_1, output_tbl_2, output_tbl_3 = outputs

        model_df = self.model_
        dummy_df = conn.sql("SELECT TOP 0 * FROM (SELECT 1 NAME, 2 VALUE FROM DUMMY) dt;")
        meta_tbl_df = conn.sql("SELECT TOP 0 * FROM (SELECT 1 TABLE_ID, 'aaa' TABLE_NAME, 1 COLUMN_ID, 'aaa' COLUMN_NAME, 'aaa' DATA_TYPE, 1 MAX_LENGTH FROM DUMMY) dt;")
        meta_tbl_df = meta_tbl_df.cast({"TABLE_ID":"INTEGER", "TABLE_NAME":"VARCHAR(256)", "COLUMN_ID":"INTEGER", "COLUMN_NAME":"VARCHAR(256)", "DATA_TYPE":"VARCHAR(256)", "MAX_LENGTH":"INTEGER"})
        pivoted_result_df = conn.sql("SELECT TOP 0 * FROM (SELECT 1 TABLE_ID, 1 COLUMN_ID, 1 ROW_ID, 'aaa' VALUE FROM DUMMY) dt;")
        pivoted_result_df = pivoted_result_df.cast({"TABLE_ID":"INTEGER", "COLUMN_ID":"INTEGER", "ROW_ID":"INTEGER", "VALUE":"VARCHAR(256)"})
        try:
            self._call_pal_auto(conn,
                                'PAL_PIPELINE_EXECUTE',
                                model_df,
                                ParameterTable().with_data(pipeline_param),
                                data_,
                                dummy_df, # input data table 2
                                dummy_df, # input data table 3
                                meta_tbl_df, # meta tbl
                                pivoted_result_df, # pivoted result
                                dummy_df, # dummy output template table 3
                                *outputs)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, outputs)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, outputs)
            raise
        if not self._disable_hana_execution:
            meta_tbl = conn.table(outputs[1])
            pivoted_tbl = conn.table(outputs[2])
            transformed_data = _merge_meta_pivoted_tables(meta_tbl, pivoted_tbl)
            if label is not None:
                transformed_data = transformed_data.join(id_label_df, "RAW_ID=ID").deselect("RAW_ID")
        return transformed_data

def _get_obj(obj):
    tmp_mem = []
    for key, val in obj.hanaml_parameters.items():
        if val is not None:
            tmp_mem.append("{}={}".format(key, val))
    return "{}({})".format(re.findall('\'([^$]*)\'', str(obj.__class__))[0], ",\n".join(tmp_mem))
