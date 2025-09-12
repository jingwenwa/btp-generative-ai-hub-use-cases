"""
This module contains auto machine learning API.

The following classes are available:

    * :class:`AutomaticClassification`
    * :class:`AutomaticRegression`
    * :class:`AutomaticTimeSeries`
    * :class:`Preprocessing`

"""
#pylint: disable=too-many-branches, super-with-arguments, c-extension-no-member
#pylint: disable=too-many-statements, too-many-arguments, too-many-lines
#pylint: disable=too-many-locals
#pylint: disable=line-too-long
#pylint: disable=too-few-public-methods
#pylint: disable=too-many-instance-attributes
#pylint: disable=attribute-defined-outside-init
#pylint: disable=protected-access
#pylint: disable=no-self-use
#pylint: disable=consider-iterating-dictionary
#pylint: disable=anomalous-backslash-in-string
#pylint: disable=no-member
#pylint: disable=too-many-nested-blocks
#pylint: disable=too-many-public-methods
#pylint: disable=too-many-function-args
import json
import logging
import os
import uuid
import pandas as pd
from hdbcli import dbapi
from hana_ml.dataframe import DataFrame, quotename
from hana_ml.ml_exceptions import FitIncompleteError
from hana_ml.ml_base import ListOfStrings
from hana_ml.visualizers.automl_config import AutoMLConfig
from hana_ml.visualizers.report_builder import Page
from hana_ml.visualizers.time_series_report import TimeSeriesExplainer
from .utility import mlflow_autologging
from .preprocessing import FeatureNormalizer, KBinsDiscretizer, Imputer, Discretize, MDS, SMOTE, SMOTETomek, TomekLinks, Sampling, FeatureSelection
from .decomposition import PCA
from .sqlgen import trace_sql
from .utility import check_pal_function_exist
from .pal_base import (
    PALBase,
    ParameterTable,
    call_pal_auto_with_hint,
    pal_param_register,
    try_drop,
    require_pal_usable
)

logger = logging.getLogger(__name__)#pylint: disable=invalid-name

def _is_unique_key(data, key):
    return data.select(key).distinct().count() == data.select(key).count()

def _check_scorings(scorings, ptype):
    if ptype == 'classifier':
        valid_set = set(['ACCURACY', 'AUC', 'KAPPA', 'MCC', 'RECALL', 'PRECISION', 'F1_SCORE', 'SUPPORT', 'LAYERS', 'TIME'])
    elif ptype == 'regressor':
        valid_set = set(['EVAR', 'MAE', 'MAPE', 'MAX_ERROR', 'MSE', 'RMSE', 'R2', 'WMAPE', 'LAYERS', 'TIME'])
    else:
        valid_set = set(['EVAR', 'MAE', 'MAPE', 'MAX_ERROR', 'MSE', 'RMSE', 'R2', 'WMAPE', 'SPEC', 'LAYERS', 'TIME'])

    if scorings:
        upper_scorings = {key.upper() : value for key, value in scorings.items() if not('F1_SCORE' in key or 'RECALL' in key or 'PRECISION' in key or 'SUPPORT' in key)}
        scorings_set = set(upper_scorings.keys())
        if not valid_set.issuperset(scorings_set):
            msg = 'The name of optimization target in scorings is not valid!'
            logger.error(msg)
            raise KeyError(msg)

class _AutoMLBase(PALBase):
    DEFAULT_AUTO_SQL_CONTENT_SCHEMA = "PAL_CONTENT"
    _category_map = {'NB_Classifier': 'Classifier',
                     'M_LOGR_Classifier': 'Classifier',
                     'SVM_Classifier': 'Classifier',
                     'RDT_Classifier': 'Classifier',
                     'DT_Classifier': 'Classifier',
                     'HGBT_Classifier': 'Classifier',
                     'MLP_Classifier': 'Classifier',
                     'MLP_M_TASK_Classifier': 'Classifier',
                     'POL_Regressor': 'Regressor',
                     'LOG_Regressor': 'Regressor',
                     'HGBT_Regressor': 'Regressor',
                     'GLM_Regressor': 'Regressor',
                     'GEO_Regressor': 'Regressor',
                     'RDT_Regressor': 'Regressor',
                     'EXP_Regressor': 'Regressor',
                     'MLP_Regressor': 'Regressor',
                     'DT_Regressor': 'Regressor',
                     'MLR_Regressor': 'Regressor',
                     'SVM_Regressor': 'Regressor',
                     'MLP_M_TASK_Regressor' : 'Regressor',
                     'Outlier': 'Resampler',
                     'SAMPLING': 'Resampler',
                     'SMOTE': 'Resampler',
                     'SMOTETomek': 'Resampler',
                     'TomekLinks': 'Resampler',
                     'SingleExpSm': 'TimeSeries',
                     'TripleExpSm': 'TimeSeries',
                     'DoubleExpSm': 'TimeSeries',
                     'BrownExpSm': 'TimeSeries',
                     'BSTS': 'TimeSeries',
                     'ARIMA': 'TimeSeries',
                     'AMTSA': 'TimeSeries',
                     'OneHotEncoder': 'Transformer',
                     'LabelEncoder': 'Transformer',
                     'ImputeTS': 'Transformer',
                     'FS_unsupervised': 'Transformer',
                     'FS_supervised': 'Transformer',
                     'SCALE': 'Transformer',
                     'CBEncoder': 'Transformer',
                     'CATPCA': 'Transformer',
                     'PolynomialFeatures': 'Transformer',
                     'HGBT_TimeSeries': 'TimeSeries',
                     'MLR_TimeSeries': 'TimeSeries',
                     'Imputer': 'Transformer'}
    _predict_kwargs_map = {'NB_Classifier': {'laplace': ('LAPLACE', float)},
                           'M_LOGR_Classifier':
                           {'ignore_unknown_category': ('IGNORE_UNKNOWN_CATEGORY', bool)},
                           'SVM_Classifier':{},
                           'DT_Classifier': {},
                           'HGBT_Classifier': {
                           'missing_replacement': ('MISSING_REPLACEMENT',
                                                   int, {'feature_marginalized':1,
                                                         'instance_marginalized':2})},
                           'MLP_Classifier':{},
                           'MLP_M_TASK_Classifier':{},
                           'MLP_M_TASK_Regressor':{},
                           'RDT_Classifier':
                           {'block_size':('BLOCK_SIZE', int),
                            'missing_replacement': ('MISSING_REPLACEMENT',
                                                    int, {'feature_marginalized':1,
                                                          'instance_marginalized':2})},
                           'GLM_Regressor':
                           {'prediction_type': ('TYPE', str, {x:x for x in ['response', 'link']}),
                            'handle_missing': ('HANDLE_MISSING', int, {'skip': 1, 'fill_zero': 2,
                                                                       'remove': 1, 'replace': 2})},
                           'POL_Regressor': {},
                           'LOG_Regressor': {},
                           'HGBT_Regressor': {},
                           'GEO_Regressor': {},
                           'RDT_Regressor': {'block_size':('BLOCK_SIZE', int)},
                           'EXP_Regressor': {},
                           'MLP_Regressor': {},
                           'DT_Regressor': {},
                           'MLR_Regressor': {},
                           'SVM_Regressor': {},
                           'SingleExpSm': {},
                           'DoubleExpSm': {},
                           'TripleExpSm': {},
                           'BrownExpSm': {},
                           'BSTS': {},
                           'ARIMA': {'forecast_method':('FORECAST_METHOD', int,
                                                        {'formula_forecast':0,
                                                         'innovations_algorithm':1,
                                                         'truncation_algorithm':2})},
                           'AMTSA': {'logistic_growth_capacity':('CAP', float)},
                           'HGBT_TimeSeries': {},
                           'MLR_TimeSeries': {}}

    def __init__(self,
                 scorings=None,
                 generations=None,
                 population_size=None,
                 offspring_size=None,
                 elite_number=None,
                 min_layer=None,
                 max_layer=None,
                 mutation_rate=None,
                 crossover_rate=None,
                 random_seed=None,
                 config_dict=None,
                 progress_indicator_id=None,
                 fold_num=None,
                 resampling_method=None,
                 max_eval_time_mins=None,
                 early_stop=None,
                 successive_halving=None,
                 min_budget=None,
                 max_budget=None,
                 min_individuals=None,
                 connections=None,
                 alpha=None,
                 delta=None,
                 top_k_connections=None,
                 top_k_pipelines=None,
                 search_method=None,
                 fine_tune_pipeline=None,
                 fine_tune_resource=None,
                 outlier=None,
                 outlier_thresholds=None,
                 outlier_pipeline_num=None,
                 outlier_tune_elite_number=None,
                 with_hyperband=None,
                 reduction_rate=None,
                 min_resource=None,
                 max_resource=None):
        if not hasattr(self, 'hanaml_parameters'):
            setattr(self, 'hanaml_parameters', pal_param_register())
        super(_AutoMLBase, self).__init__()
        self.scorings = self._arg('scorings', scorings, dict)
        if self.scorings:
            if isinstance(self, AutomaticClassification):
                ptype = 'classifier'
            elif isinstance(self, AutomaticRegression):
                ptype = 'regressor'
            else:
                ptype = 'timeseries'
            _check_scorings(self.scorings, ptype)
            self.scorings = json.dumps(self.scorings)
        self.generations = self._arg('generations', generations, int)
        self.population_size = self._arg('population_size', population_size, int)
        self.offspring_size = self._arg('offspring_size', offspring_size, int)
        self.elite_number = self._arg('elite_number', elite_number, int)
        self.min_layer = self._arg('min_layer', min_layer, int)
        self.max_layer = self._arg('max_layer', max_layer, int)
        self.mutation_rate = self._arg('mutation_rate', mutation_rate, float)
        self.crossover_rate = self._arg('crossover_rate', crossover_rate, float)
        self.random_seed = self._arg('random_seed', random_seed, int)
        self.config_dict = self._arg('config_dict', config_dict, (str, dict, AutoMLConfig))

        self.__automl_config_viz = None
        if isinstance(self.config_dict, dict):
            self.config_dict = json.dumps(self.config_dict)
        if isinstance(self.config_dict, AutoMLConfig):
            self.__automl_config_viz = self.config_dict
            self.config_dict = json.dumps(self.config_dict.get_config_dict())
        self.progress_indicator_id = self._arg('progress_indicator_id', progress_indicator_id, str)
        if self.progress_indicator_id:
            if len(self.progress_indicator_id) > 64:
                raise ValueError("The length of progress id should be less than or equal to 64!")
        self.fold_num = self._arg('fold_num', fold_num, int)
        if isinstance(self, AutomaticClassification):
            resampling_map = {'cv': 'cv', 'stratified_cv': 'stratified_cv'}
        elif isinstance(self, AutomaticRegression):
            resampling_map = {'cv':'cv'}
        else:
            resampling_map = {'rocv': 1, 'block': 2, 'simple_split': 3}
        self.resampling_method = self._arg('resampling_method', resampling_method, resampling_map)
        self.max_eval_time_mins = self._arg('max_eval_time_mins', max_eval_time_mins, float)
        self.early_stop = self._arg('early_stop', early_stop, int)
        self.successive_halving = self._arg('successive_halving', successive_halving, bool)
        self.min_budget = self._arg('min_budget', min_budget, int)
        self.max_budget = self._arg('max_budget', max_budget, int)
        self.min_individuals = self._arg('min_individuals', min_individuals, int)
        self.connections = self._arg('connections', connections, (str, dict))
        if isinstance(self.connections, dict):
            self.connections = json.dumps(self.connections)
        self.alpha = self._arg('alpha', alpha, float)
        self.delta = self._arg('delta', delta, float)
        self.top_k_connections = self._arg('top_k_connections', top_k_connections, int)
        self.top_k_pipelines = self._arg('top_k_pipelines', top_k_pipelines, int)
        self.search_method = self._arg('search_method', search_method, str)
        self.fine_tune_pipeline = self._arg('fine_tune_pipeline',
                                            fine_tune_pipeline, bool)
        self.fine_tune_resource = self._arg('fine_tune_resource',
                                            fine_tune_resource, int)
        self._status = 0
        self.__enable_workload_class = False
        self.used_workload_class_name = None
        self._is_autologging = False
        self._autologging_model_storage_schema = None
        self._autologging_model_storage_meta = None
        self._fetch_category_map = False
        self.is_exported = False
        self.registered_model_name = None
        self.report = None
        self.percentage = None
        self.gap_num = None
        self.progress_indicator_cleanup = None
        self.auto_sql_content_schema = self.DEFAULT_AUTO_SQL_CONTENT_SCHEMA
        self._use_auto_sql_content = True
        self._not_exist_auto_sql_content = None
        self._progress_log_level = None
        self.__disable_log_cleanup = False
        self.__retention_period = 365

        self.outlier = self._arg('outlier', outlier, bool)
        if isinstance(outlier_thresholds, float):
            outlier_thresholds = [outlier_thresholds]
        self.outlier_thresholds = self._arg('outlier_thresholds', outlier_thresholds, list)
        self.outlier_pipeline_num = self._arg('outlier_pipeline_num', outlier_pipeline_num, int)
        self.outlier_tune_elite_number = self._arg('outlier_tune_elite_number', outlier_tune_elite_number, int)
        self.with_hyperband = self._arg('with_hyperband', with_hyperband, bool)
        self.reduction_rate = self._arg('reduction_rate', reduction_rate, float)
        self.min_resource = self._arg('min_resource', min_resource, int)
        self.max_resource = self._arg('max_resource', max_resource, int)

    def _set_retention_period(self, retention_period):
        self.__retention_period = retention_period

    def _set_auto_sql_content_schema(self, schema):
        self.auto_sql_content_schema = schema

    def _exist_auto_sql_content_log(self, connection_context=None):
        if self._use_auto_sql_content is False:
            return False
        if self._not_exist_auto_sql_content:
            return False
        exist = []
        if not self._disable_hana_execution:
            exist = connection_context.sql('SELECT * FROM SYS.PROCEDURES WHERE SCHEMA_NAME = \'{}\' and PROCEDURE_NAME = \'{}\';'.format(self.auto_sql_content_schema, "REMOVE_AUTOML_LOG")).collect()
        if len(exist) > 0:
            return True
        self._not_exist_auto_sql_content = True
        self._use_auto_sql_content = False
        return False

    def _gen_predict_args_json(self, predict_args):
        predict_args_json, no_valid_args = None, 0
        estimators_all = ('_Classifier', '_Regressor', '_TimeSeries',
                          'ExpSm', 'ARIMA', 'AMTSA', 'BSTS')#Enumerate all estimators provided
        if predict_args:
            if all(any(x in key for x in estimators_all) for key in list(predict_args.keys())):#if estimator name provided
                predict_args_json = '{'
                for p_estimator, args in predict_args.items():
                    if not args:#skip empty predict args
                        continue
                    args = self._arg('Predict args', args, dict)
                    predict_args_json += '"' + p_estimator + '":{'
                    predict_args_map = self._predict_kwargs_map[p_estimator]
                    for var_key, var_val in args.items():
                        if var_key not in predict_args_map:
                            msg = "Variable '{}' is not supported, ".format(var_key) +\
                            "it is simply ignored."
                            logger.warning(msg)
                            continue
                        var_pal = predict_args_map[var_key]
                        map_var = self._arg(var_key, var_val, var_pal[1] if len(var_pal) == 2 else var_pal[2])
                        predict_args_json += '"{}":{},'.format(var_pal[0],
                                                               int(map_var) if var_pal[1] in (int, bool) else \
                                                               (float(map_var) if var_pal[1] == float \
                                                               else '"{}"'.format(map_var)))
                        no_valid_args += 1
                    if predict_args_json[-1] == ',':
                        predict_args_json = predict_args_json[0:-1] + '},'
                    else:
                        predict_args_json = predict_args_json + '},'
                predict_args_json = predict_args_json[0:-1] + '}'#removal of superfluous ',' and enclosing the entire json string.
            else:
                msg = 'Wrong format for predict_args!'
                raise ValueError(msg)
        return predict_args_json, no_valid_args

    def disable_auto_sql_content(self, disable=True):
        """
        Disable auto SQL content logging. Use AFL's default progress logging.
        """
        self._use_auto_sql_content = not disable

    def persist_progress_log(self):
        """
        Persist the progress log.
        """
        self.progress_indicator_cleanup = 0

    def display_progress_table(self, connection_context):
        """
        Return the progress table.

        Parameters
        ----------
        connection_context : ConnectionContext
            The connection object to a SAP HANA database.

        Returns
        -------
        DataFrame
            Progress table.
        """
        log_table = None
        self._exist_auto_sql_content_log(connection_context)
        if self._use_auto_sql_content:
            log_table = "{}.AUTOML_LOG".format(self.auto_sql_content_schema)
        else:
            log_table = "_SYS_AFL.FUNCTION_PROGRESS_IN_AFLPAL"
        return connection_context.sql("SELECT * FROM {} WHERE EXECUTION_ID='{}'".format(log_table, self.progress_indicator_id))

    def _auto_sql_content_cleanup(self, connection_context, execution_id, is_force):
        has_log = connection_context.sql("SELECT COUNT(*) FROM {}.AUTOML_LOG WHERE EXECUTION_ID = '{}'".format(self.auto_sql_content_schema, execution_id)).collect().iat[0, 0]
        if has_log > 0:
            connection_context.execute_sql("DO BEGIN\nCALL {}.REMOVE_AUTOML_LOG('{}', TO_BOOLEAN({}), info);\nEND;".format(self.auto_sql_content_schema, execution_id, 1 if is_force else 0))

    def cleanup_progress_log(self, connection_context):
        """
        Clean up the progress log.

        Parameters
        ----------
        connection_context : ConnectionContext
            The connection object to a SAP HANA database.
        """
        if not self.__disable_log_cleanup:
            self._exist_auto_sql_content_log(connection_context)
            if self._use_auto_sql_content:
                try:
                    self._auto_sql_content_cleanup(connection_context, self.progress_indicator_id, True)
                    self.progress_indicator_cleanup = None
                except dbapi.Error as db_err:
                    msg = str(connection_context.hana_version())
                    logger.exception("HANA version: %s. %s", msg, str(db_err))
                    raise
                except Exception as db_err:
                    msg = str(connection_context.hana_version())
                    logger.exception("HANA version: %s. %s", msg, str(db_err))
                    raise
            else:
                if check_pal_function_exist(connection_context, "%PROGRESS_INDICATOR_CLEANUP%", like=True):
                    has_log = connection_context.sql("SELECT COUNT(*) FROM _SYS_AFL.FUNCTION_PROGRESS_IN_AFLPAL WHERE EXECUTION_ID = '{}'".format(self.progress_indicator_id)).collect().iat[0, 0]
                    if has_log > 0:
                        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
                        info_tbl  = f'#PAL_CLEANUP_PROGRESS_LOG_INFO_TBL_{self.id}_{unique_id}'
                        param_rows = [('PROGRESS_INDICATOR_ID', None, None, self.progress_indicator_id)]
                        try:
                            call_pal_auto_with_hint(connection_context,
                                None,
                                'PAL_PROGRESS_INDICATOR_CLEANUP',
                                ParameterTable().with_data(param_rows),
                                info_tbl)
                            self.progress_indicator_cleanup = None
                        except dbapi.Error as db_err:
                            msg = str(connection_context.hana_version())
                            logger.exception("HANA version: %s. %s", msg, str(db_err))
                            raise
                        except Exception as db_err:
                            msg = str(connection_context.hana_version())
                            logger.exception("HANA version: %s. %s", msg, str(db_err))
                            raise
                        finally:
                            try_drop(connection_context, info_tbl)

    def get_best_pipeline(self):
        """
        Return the best pipeline.
        """
        if hasattr(self, 'best_pipeline_'):
            if self.best_pipeline_:
                return self.best_pipeline_.collect().iat[0, 1]
        return self.model_[1].collect().iat[0, 1]

    def get_config_dict(self):
        """
        Return the config_dict.
        """
        if isinstance(self.config_dict, str):
            return json.loads(self.config_dict)
        else:
            return self.config_dict

    def get_optimal_config_dict(self):
        """
        Return the optimal config_dict. Only available when connections is used.
        """
        if self.connections:
            if hasattr(self, 'info_'):
                if self.info_:
                    return json.loads(self.info_.filter("STAT_NAME='optimal_config'").collect().iat[0, 1])
        return None

    def get_optimal_connections(self):
        """
        Return the optimal connections. Only available when connections is used.
        """
        if self.connections:
            if hasattr(self, 'info_'):
                if self.info_:
                    return json.loads(self.info_.filter("STAT_NAME='optimal_connections'").collect().iat[0, 1])
        return None

    def make_future_dataframe(self, data=None, key=None, periods=1, increment_type='seconds'):
        """
        Create a new dataframe for time series prediction.

        Parameters
        ----------
        data : DataFrame, optional
            The training data contains the index.

            Defaults to the data used in the fit().

        key : str, optional
            The index defined in the training data.

            Defaults to the specified key in fit function or the data.index or the first column of the data.

        periods : int, optional
            The number of rows created in the predict dataframe.

            Defaults to 1.

        increment_type : {'seconds', 'days', 'months', 'years'}, optional
            The increment type of the time series.

            Defaults to 'seconds'.

        Returns
        -------
        DataFrame

        """
        if data is None:
            data = self._fit_args[0]
        if key is None:
            if data.index is None:
                if hasattr(self, "key"):
                    if self.key is None:
                        key = data.columns[0]
                    else:
                        key = self.key
                else:
                    key = data.columns[0]
            else:
                key = data.index
        max_ = data.select(key).max()
        sec_max_ = data.select(key).distinct().sort_values(key, ascending=False).head(2).collect().iat[1, 0]
        delta = (max_ - sec_max_)
        is_int = 'INT' in data.get_table_structure()[key]
        if is_int:
            forecast_start, timedelta = max_ + delta, delta
        else:
            forecast_start, timedelta = max_ + delta, delta.total_seconds()
        timeframe = []
        if not is_int:
            if 'day' in increment_type.lower():
                increment_type = 'days'
                timedelta = round(timedelta / 86400)
                if timedelta == 0:
                    raise ValueError("The interval between the training time series is less than one day.")
            elif 'month' in increment_type.lower():
                increment_type = 'months'
                timedelta = round(timedelta / 2592000)
                if timedelta == 0:
                    raise ValueError("The interval between the training time series is less than one month.")
            elif 'year' in increment_type.lower():
                increment_type = 'years'
                timedelta = round(timedelta / 31536000)
                if timedelta == 0:
                    raise ValueError("The interval between the training time series is less than one year.")
            else:
                increment_type = 'seconds'
        for period in range(0, periods):
            if is_int:
                timeframe.append("SELECT TO_INT({} + {} * {}) AS \"{}\" FROM DUMMY".format(forecast_start, timedelta, period, key))
            else:
                timeframe.append("SELECT ADD_{}('{}', {} * {}) AS \"{}\" FROM DUMMY".format(increment_type.upper(), forecast_start, timedelta, period, key))
        sql = ' UNION ALL '.join(timeframe)
        return data.connection_context.sql(sql).sort_values(key)

    def enable_mlflow_autologging(self, schema=None, meta=None, is_exported=False, registered_model_name=None):
        """
        Enable the mlflow autologging.

        Parameters
        ----------
        schema : str, optional
            Defines the model storage schema for mlflow autologging.

            Defaults to the current schema.
        meta : str, optional
            Defines the model storage meta table for mlflow autologging.

            Defaults to 'HANAML_MLFLOW_MODEL_STORAGE'.
        is_exported : bool, optional
            Determines whether export a HANA model to mlflow.

            Defaults to False.

        registered_model_name : str, optional
            MLFlow registered_model_name.

            Defaults to None.
        """
        self._is_autologging = True
        self._autologging_model_storage_schema = schema
        self._autologging_model_storage_meta = meta
        self.is_exported = is_exported
        self.registered_model_name = registered_model_name

    def enable_workload_class(self, workload_class_name):
        """
        HANA WORKLOAD CLASS is recommended to set when the AutomaticClassification/AutomaticRegression/AutomaticTimeSeries is called.

        Parameters
        ----------
        workload_class_name : str
            The name of HANA WORKLOAD CLASS.

        """
        self.apply_with_hint('WORKLOAD_CLASS("{}")'.format(workload_class_name), True)
        self.__enable_workload_class = True
        self.used_workload_class_name = workload_class_name

    def disable_mlflow_autologging(self):
        """
        Disable the mlflow autologging.

        """
        self._is_autologging = False

    def disable_workload_class_check(self):
        """
        Disable the workload class check. Please note that the AutomaticClassification/AutomaticRegression/AutomaticTimeSeries may cause large resource.
        Without setting workload class, there's no resource restriction on the training process.

        """
        self.__enable_workload_class = True
        self.used_workload_class_name = None

    def disable_log_cleanup(self, disable=True):
        """
        Disable the log clean up.
        """
        self.__disable_log_cleanup = disable

    def get_workload_classes(self, connection_context):
        """
        Return the available workload classes information.

        Parameters
        ----------
        connection_context : str, optional
            The connection to a SAP HANA instance.

        """
        return connection_context.sql("SELECT * FROM WORKLOAD_CLASSES").collect()

    def set_progress_log_level(self, log_level):
        """
        Set progress log level to output scorings.

        Parameters
        ----------
        log_level: {'full', 'full_best', 'specified'}
            'full' prints all scores. 'full_best' prints all scores only for the 'current_best' of each generation; other pipelines print only the scores specified by SCORINGS. 'Specified' means all pipelines print only the specified scores.

        """
        self._progress_log_level = log_level

    def _fit(self,
             data,
             key=None,
             features=None,
             label=None,
             pipeline=None,
             categorical_variable=None,
             background_size=None,
             background_sampling_seed=None,
             model_table_name=None,
             use_explain=None,
             explain_method=None):
        if pipeline is None:
            if not self.__enable_workload_class:
                self._status = -1
                raise FitIncompleteError("Please define the workload class and call the enable_workload_class() method first!")
        if not isinstance(data, DataFrame):
            self._status = -1
            raise TypeError("The type of data should be HANA DataFrame.")
#        if self.progress_indicator_id is None:
#            self.progress_indicator_id = "AutoML-" + str(self.gen_id)
        conn = data.connection_context
        if self.progress_indicator_cleanup == 0:
            if not check_pal_function_exist(conn, "%PROGRESS_INDICATOR_CLEANUP%", like=True):
                self.progress_indicator_cleanup = None
                logger.warning("No PAL_PROGRESS_INDICATOR_CLEANUP procedure find! Can't persist progress log!")
            else:
                if self.display_progress_table(conn).count() > 0:
                    self.cleanup_progress_log(conn)
        if not self._disable_hana_execution:
            data.connection_context.connection_id = data.connection_context.get_connection_id()
            require_pal_usable(conn)
            conn.sql_tracer.set_sql_trace(self, self.__class__.__name__, 'Fit')
            self._exist_auto_sql_content_log(connection_context=conn)
        index = data.index
        if isinstance(index, str):
            if key is not None and index != key:
                msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(msg)
        key = index if key is None else key
        if key is None:
            if isinstance(self, AutomaticTimeSeries):
                msg = "Parameter 'key' must be set for time-series data."
                logger.error(msg)
                raise ValueError(msg)
            warn_msg = "Parameter 'key' has not been set." +\
            " The dataset will be considered without ID column."
            logger.warning(warn_msg)
        else:
            if not self._disable_hana_execution:
                if not self._disable_arg_check:
                    if not _is_unique_key(data, key):
                        msg = "The provided key is not unique."
                        raise ValueError(msg)
        #pylint:disable=attribute-defined-outside-init
        use_explain = self._arg('use_explain', use_explain, bool)
        explain_method = self._arg('explain_method', explain_method,
                                   {'kernelshap' : 0, 'globalsurrogate' : 1})
        cols = data.columns
        #has_id input is process here
        if key is not None:
            id_col = [key]
            cols.remove(key)
        else:
            id_col = []
        if label is None:
            if isinstance(self, AutomaticTimeSeries):
                label = cols[0]
            else:
                label = cols[-1]
        cols.remove(label)
        #retrieve data type for  the label column
        #crucial for distinguish between regression and classification problems
        #and related error handling
        if features is None:
            features = cols
        if isinstance(categorical_variable, str):
            categorical_variable = [categorical_variable]
        categorical_variable = self._arg('categorical_variable',
                                         categorical_variable,
                                         ListOfStrings)
        #n_features = len(features)
        #Generate a temp view of the data so that label is always in the final column
        data_ = None
        if isinstance(self, AutomaticTimeSeries):
            data_ = data[id_col + [label] + features]
        else:
            data_ = data[id_col + features + [label]]
        if pipeline is None:
            if self.__enable_workload_class:
                if  self.used_workload_class_name:
                    if not self._disable_hana_execution:
                        if self.used_workload_class_name not in self.get_workload_classes(conn)["WORKLOAD_CLASS_NAME"].to_list():
                            logger.warning("The workload class name cannot be found in the HANA settings.")
        if self.__automl_config_viz:
            self.config_dict = json.dumps(self.__automl_config_viz.get_config_dict())
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        outputs = ['BEST_PIPELINE', 'MODEL', 'INFO']
        outputs = ['#PAL_AUTOML_{}_TBL_{}_{}'.format(name, self.id, unique_id)
                   for name in outputs]
        if model_table_name:
            outputs[1] = model_table_name
        best_pipeline_tbl, model_tbl, info_tbl = outputs
        ptype = None
        percentage = None
        gap_num = None
        if isinstance(self, AutomaticRegression):
            ptype = 'regressor'
        elif isinstance(self, AutomaticClassification):
            ptype = 'classifier'
        else:
            ptype = 'timeseries'
            percentage = self.percentage
            gap_num = self.gap_num
        pipeline_param = [('HAS_ID', key is not None, None, None)]
        if categorical_variable is not None:
            pipeline_param.extend([('CATEGORICAL_VARIABLE', None,
                                    None, var) for var in categorical_variable])

        param_rows = [('SCORINGS', None, None, self.scorings),
                      ('GENERATIONS', self.generations, None, None),
                      ('POPULATION_SIZE', self.population_size, None, None),
                      ('OFFSPRING_SIZE', self.offspring_size, None, None),
                      ('ELITE_NUMBER', self.elite_number, None, None),
                      ('MIN_LAYER', self.min_layer, None, None),
                      ('MAX_LAYER', self.max_layer, None, None),
                      ('MUTATION_RATE', None, self.mutation_rate, None),
                      ('CROSSOVER_RATE', None, self.crossover_rate, None),
                      ('RANDOM_SEED', self.random_seed, None, None),
                      ('CONFIG_DICT', None, None, self.config_dict),
                      ('FOLD_NUM', self.fold_num, None, None),
                      ('PIPELINE_TYPE', None, None, ptype),
                      ('MAX_EVAL_TIME_MINS', None, self.max_eval_time_mins, None),
                      ('EARLY_STOP', self.early_stop, None, None),
                      ('PERCENTAGE', None, percentage, None),
                      ('GAP_NUM', gap_num, None, None),
                      ('SUCCESSIVE_HALVING', 0 if ptype=='timeseries' else self.successive_halving, None, None),
                      ('MIN_BUDGET', self.min_budget, None, None),
                      ('MAX_BUDGET', self.max_budget, None, None),
                      ('MIN_INDIVIDUALS', self.min_individuals, None, None),
                      ('BACKGROUND_SIZE', background_size, None, None),
                      ('BACKGROUND_SAMPLING_SEED', background_sampling_seed, None, None),
                      ('PROGRESS_INDICATOR_CLEANUP', self.progress_indicator_cleanup, None, None),
                      ('CONNECTIONS', None, None, self.connections),
                      ('ALPHA', None, self.alpha, None),
                      ('DELTA', None, self.delta, None),
                      ('TOP_K_CONNECTIONS', self.top_k_connections, None, None),
                      ('TOP_K_PIPELINES', self.top_k_pipelines, None, None),
                      ('SEARCH_METHOD', None, None, self.search_method),
                      ('SCORINGS_LOG_LEVEL', None, None, self._progress_log_level),
                      ('WITH_HYPERBAND', self.with_hyperband, None, None),
                      ('REDUCTION_RATE', None, self.reduction_rate, None),
                      ('MIN_RESOURCE', self.min_resource, None, None),
                      ('MAX_RESOURCE', self.max_resource, None, None)]

        if self._use_auto_sql_content:
            param_rows.extend([('RETENTION_PERIOD', self.__retention_period if self.progress_indicator_cleanup == 0 else 0, None, None)])
            param_rows.extend([('EXECUTION_ID', None, None, self.progress_indicator_id)])
        else:
            param_rows.extend([('PROGRESS_INDICATOR_ID', None, None, self.progress_indicator_id)])
        param_rows.extend(pipeline_param)
        if self.resampling_method is not None:
            if isinstance(self, AutomaticTimeSeries):
                param_rows.extend([('SPLIT_METHOD', self.resampling_method, None, None)])
            else :
                param_rows.extend([('RESAMPLING_METHOD', None, None, self.resampling_method)])
        if self.outlier is True:
            param_rows.extend([('REGRESSION_OUTLIER', self.outlier, None, None),
                               ('REGRESSION_OUTLIER_PIPELINE_NUM', self.outlier_pipeline_num, None, None),
                               ('REGRESSION_OUTLIER_TUNE_ELITE_NUMBER', self.outlier_tune_elite_number, None, None)])
            if self.outlier_thresholds is not None:
                param_rows.extend([('REGRESSION_OUTLIER_THRESHOLDS', None, thres, None) for thres in self.outlier_thresholds])

        setattr(self, 'fit_data', data_)
        try:
            if pipeline:
                if isinstance(pipeline, dict):
                    pipeline = json.dumps(pipeline)
                pipeline_param.extend([('PIPELINE', None, None, pipeline),
                                       ('BACKGROUND_SIZE', background_size, None, None),
                                       ('BACKGROUND_SAMPLING_SEED', background_sampling_seed, None, None),
                                       ('USE_EXPLAIN', use_explain, None, None),
                                       ('EXPLAIN_METHOD', explain_method, None, None)])
                pipeline_outputs = outputs[1:]
                self._call_pal_auto(conn,
                                    'PAL_PIPELINE_FIT',
                                    data_,
                                    ParameterTable().with_data(pipeline_param),
                                    *pipeline_outputs)
                fit_output_signature = [{'ROW_INDEX': 'INT', 'MODEL_CONTENT': 'VARCHAR(5000)'}, {"STAT_NAME": "VARCHAR(5000)", "STAT_VALUE": "VARCHAR(5000)"}]
                setattr(self, "fit_output_signature", fit_output_signature)
            else:
                if str(self.search_method).lower() in ['none', 'ga']:
                    param_rows.extend([('FINE_TUNE_PIPELINE', self.fine_tune_pipeline,
                                        None, None),
                                       ('FINE_TUNE_RESOURCE', self.fine_tune_resource,
                                        None, None)])
                self._call_pal_auto(conn,
                                    'PAL_AUTOML_FIT',
                                    data_,
                                    ParameterTable().with_data(param_rows),
                                    *outputs)
                fit_output_signature = [{"ID": "INT", "PIPELINE": "VARCHAR(5000)", "SCORES": "VARCHAR(5000)"}, {'ROW_INDEX': 'INT', 'MODEL_CONTENT': 'VARCHAR(5000)'}, {"STAT_NAME": "VARCHAR(5000)", "STAT_VALUE": "VARCHAR(5000)"}]
                setattr(self, "fit_output_signature", fit_output_signature)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            if pipeline:
                try_drop(conn, pipeline_outputs)
            else:
                try_drop(conn, outputs)
                if self.progress_indicator_cleanup == 0:
                    self.cleanup_progress_log(conn)
            self._status = -1
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            if pipeline:
                try_drop(conn, pipeline_outputs)
            else:
                try_drop(conn, outputs)
                if self.progress_indicator_cleanup == 0:
                    self.cleanup_progress_log(conn)
            self._status = -1
            raise
        # pylint: disable=attribute-defined-outside-init
        if pipeline is None:
            self._status = 1
            self.best_pipeline_ = conn.table(best_pipeline_tbl)
            self.model_ = [conn.table(model_tbl),
                           self.best_pipeline_]
        else:
            self.model_ = conn.table(model_tbl)
        self.info_ = conn.table(info_tbl)

    @mlflow_autologging(logtype='pal_fit')
    @trace_sql
    def fit(self,
            data,
            key=None,
            features=None,
            label=None,
            pipeline=None,
            categorical_variable=None,
            background_size=None,
            background_sampling_seed=None,
            model_table_name=None,
            use_explain=None,
            explain_method=None):
        r"""
        Fit function of AutomaticClassification/AutomaticRegression.

        Parameters
        ----------
        data : DataFrame
            The input data.

        key : str, optional
            Name of the ID column.

            If ``key`` is not provided, then:

            - if ``data`` is indexed by a single column, then ``key`` defaults to that index column;
            - otherwise, it is assumed that ``data`` contains no ID column.

        features : a list of str, optional
            Names of the feature columns.

            If ``features`` is not provided, it defaults to all non-ID, non-label columns.

        label : str, optional
            Name of the dependent variable.

            Defaults to the name of the last non-ID column.

        pipeline : str or dict, optional
            Directly uses the input pipeline to fit.

            Defaults to None.

        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.
        background_size : int, optional
            This represents the amount of background data in Kernel SHAP. Its value should not exceed the number of rows in the training data.
            Only valid when ``use_explain`` is True and ``explain_method`` is 'kernelshap'.

            Defaults to None.
        background_sampling_seed : int, optional
            Specifies the seed for random number generator in the background sampling. Only valid when ``use_explain`` is True.

            - 0: Uses the current time (in second) as seed
            - Others: Uses the specified value as seed

            Defaults to 0.
        model_table_name : str, optional
            Specifies the HANA model table name instead of the generated temporary table.

            Defaults to None.
        use_explain : bool, optional
            Specifies whether to store information for explaination.

            Defaults to False.
        explain_method : str, optional
            Specifies the explaination method. Only valid when ``use_explain`` is True.

            Options are:

            - 'kernelshap' : to make explaination by Kernel SHAP, ``background_size`` should be larger than 0.
            - 'globalsurrogate'

            Defaults to 'globalsurrogate'.

        Returns
        -------
        A fitted object of class "AutomaticClassification" or "AutomaticRegression".

        """
        if not hasattr(self, 'hanaml_fit_params'):
            setattr(self, 'hanaml_fit_params', pal_param_register())
        if not hasattr(self, 'training_data'):
            setattr(self, 'training_data', data)
        if pipeline is None and (use_explain or background_size is not None):
            #call automl first
            self._fit(data=data,
                      key=key,
                      features=features,
                      label=label,
                      pipeline=pipeline,
                      categorical_variable=categorical_variable,
                      background_size=background_size,
                      background_sampling_seed=background_sampling_seed)
            #call pipeline fit
            self._fit(data=data,
                      key=key,
                      features=features,
                      label=label,
                      pipeline=self.get_best_pipeline(),
                      categorical_variable=categorical_variable,
                      background_size=background_size,
                      background_sampling_seed=background_sampling_seed,
                      model_table_name=model_table_name,
                      explain_method=explain_method,
                      use_explain=use_explain)
        else:
            self._fit(data=data,
                      key=key,
                      features=features,
                      label=label,
                      pipeline=pipeline,
                      categorical_variable=categorical_variable,
                      background_size=background_size,
                      background_sampling_seed=background_sampling_seed,
                      model_table_name=model_table_name,
                      use_explain=use_explain,
                      explain_method=explain_method)
        return self

    @trace_sql
    def _predict(self, data, key=None, features=None, model=None,
                 show_explainer=False, top_k_attributions=None,
                 random_state=None, sample_size=None,
                 verbose_output=None,
                 predict_args=None):
        conn = data.connection_context
        if model is None:
            if getattr(self, 'model_') is None:
                raise FitIncompleteError()
            model = self.model_ if not isinstance(self.model_, list) else self.model_[0]
        predict_input_signature = [data.get_table_structure(), {'ROW_INDEX': 'INT', 'MODEL_CONTENT': 'VARCHAR(5000)'}]
        setattr(self, "predict_input_signature", predict_input_signature)
        index = data.index
        if isinstance(index, str):
            if key is not None and index != key:
                msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(msg)
        key = index if key is None else key
        key = self._arg('key', key, str, required=not isinstance(index, str))
        cols = data.columns
        cols.remove(key)
        if features is None:
            features = cols
        random_state = self._arg('random_state', random_state, int)
        top_k_attributions = self._arg('top_k_attributions',
                                       top_k_attributions, int)
        sample_size = self._arg('sample_size', sample_size, int)
        verbose_output = self._arg('verbose_output', verbose_output, bool)
        data_ = data[[key] + features]
        if show_explainer:
            predict_output_signature = [{key: data.get_table_structure()[key], 'SCORES': 'NVARCHAR(5000)', 'CONFIDENCE': 'DOUBLE', 'REASON_CODE': 'NCLOB', 'PLACEHOLDER_1': 'NVARCHAR(5000)', 'PLACEHOLDER_2': 'NVARCHAR(5000)'}, {'STAT_NAME': 'NVARCHAR(5000)', 'STAT_VALUE': 'NVARCHAR(5000)'}]
        else:
            predict_output_signature = [{key: data.get_table_structure()[key], 'SCORES': 'VARCHAR(5000)'}, {'STAT_NAME': 'VARCHAR(5000)', 'STAT_VALUE': 'VARCHAR(5000)'}]
        setattr(self, "predict_output_signature", predict_output_signature)
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        outputs = ['RESULT', 'INFO']
        outputs = ['#PAL_AUTOML_{}_RESULT_TBL_{}_{}'.format(name, self.id, unique_id)
                   for name in outputs]
        result_tbl, info_tbl = outputs
        param_rows = []
        calling_function = 'PAL_PIPELINE_PREDICT'
        predict_args = self._arg('predict_args', predict_args, dict)
        predict_args_json, no_valid_args = self._gen_predict_args_json(predict_args)
        if no_valid_args > 0:
            param_rows.extend([('PREDICT_ARGS', None, None, predict_args_json)])
        if show_explainer:
            calling_function = 'PAL_PIPELINE_EXPLAIN'
            param_rows.extend([('TOP_K_ATTRIBUTIONS', top_k_attributions, None, None),
                               ('SEED', random_state, None, None),
                               ('SAMPLESIZE', sample_size, None, None),
                               ('VERBOSE_OUTPUT', verbose_output, None, None)])
        setattr(self, 'predict_data', data_)
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
            self._status = -1
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, outputs)
            self._status = -1
            raise
        self._status = 1
        result_df = conn.table(result_tbl)
        self.predict_info_ = conn.table(info_tbl)
        return result_df

    def _operator_in_config(self, operator_name, config_dict): #pylint: disable=inconsistent-return-statements
        if operator_name in config_dict:
            return config_dict[operator_name]

    def evaluate(self,
                 data,
                 pipeline=None,
                 key=None,
                 features=None,
                 label=None,
                 categorical_variable=None,
                 resampling_method=None,
                 fold_num=None,
                 random_state=None):
        """
        Evaluates a pipeline.

        Parameters
        ----------

        data : DataFrame
            Data for pipeline evaluation.

        pipeline : json str or dict
            Pipeline to be evaluated.

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
            - timeseries: {'rocv', 'block'}

            Defaults to 'stratified_cv' if the estimator in ``pipeline`` is a classifier,
            and defaults to(and can only be) 'cv' if the estimator in ``pipeline`` is a regressor,
            and defaults to 'rocv' if if the estimator in ``pipeline`` is a timeseries.

        fold_num : int, optional
            The fold number for cross validation. If the value is 0, the function will automatically determine the fold number.

            Defaults to 5.

        random_state : int, optional
            Specifies the seed for random number generator.

            - 0: Uses the current time (in seconds) as the seed.
            - Others: Uses the specified value as the seed.

        Returns
        -------
        DataFrame
            Scores.
        """
        if pipeline is None:
            pipeline = self.best_pipeline_.filter("ID=0").select("PIPELINE").collect().iat[0, 0]
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
        if key is None and isinstance(self, AutomaticTimeSeries):
            err_msg = "Parameter 'key' must be specified for the evaluate function of AutomaticTimeSeries."
            logger.error(err_msg)
            raise ValueError(err_msg)
        features = self._arg('features', features, ListOfStrings)
        label = self._arg('label', label, str)
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
            label = cols[0] if isinstance(self, AutomaticTimeSeries) else cols[-1]
        cols.remove(label)
        if features is None:
            features = cols
        if isinstance(self, AutomaticTimeSeries):
            data_ = data[id_col + [label] + features]
        else:
            data_ = data[id_col + features + [label]]
        #data_ = data[id_col + features + [label]]
        if isinstance(pipeline, dict):
            pipeline = json.dumps(pipeline)
        if isinstance(self, AutomaticClassification):
            resampling_map = {'cv': 'cv', 'stratified_cv': 'stratified_cv'}
        elif isinstance(self, AutomaticRegression):
            resampling_map = {'cv':'cv'}
        else:
            resampling_map = {'rocv': 1, 'block': 2, 'simple_split': 3}
        resampling_method = self._arg('resampling_method', resampling_method, resampling_map)
        fold_num = self._arg('fold_num', fold_num, int)
        random_state = self._arg('random_state', random_state, int)
        if isinstance(self, AutomaticRegression):
            ptype = "regressor"
        elif isinstance(self, AutomaticClassification):
            ptype = 'classifier'
        else:
            ptype = 'timeseries'
        param_rows = [
            ('HAS_ID', key is not None, None, None),
            ('FOLD_NUM', fold_num, None, None),
            ('RANDOM_SEED', random_state, None, None),
            ('PIPELINE', None, None, pipeline),
            ('PIPELINE_TYPE', None, None, ptype),
            ('PERCENTAGE', None, self.percentage, None),
            ('GAP_NUM', self.gap_num, None, None)]

        if categorical_variable is not None:
            param_rows.extend(('CATEGORICAL_VARIABLE', None, None, variable)
                              for variable in categorical_variable)

        if resampling_method is not None:
            if isinstance(self, AutomaticTimeSeries):
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

    @trace_sql
    def _score(self,
               data,
               key=None,
               features=None,
               label=None,
               model=None,
               random_state=None,
               top_k_attributions=None,
               sample_size=None,
               verbose_output=None,
               predict_args=None):
        conn = data.connection_context
        require_pal_usable(conn)
        if model is None:
            if getattr(self, 'model_') is None:
                raise FitIncompleteError()
            if isinstance(self.model_, list):
                model = self.model_[0]
            else:
                model = self.model_
        key = self._arg('key', key, str, required=not isinstance(data.index, str))
        index = data.index
        if isinstance(index, str):
            if key is not None and index != key:
                msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(msg)
        key = index if key is None else key
        features = self._arg('features', features, ListOfStrings)
        label = self._arg('label', label, str)
        cols = data.columns
        if key is not None:
            id_col = [key]
            cols.remove(key)
        else:
            id_col = []
        if label is None:
            label = cols[0] if isinstance(self, AutomaticTimeSeries) else cols[-1]
        cols.remove(label)
        if features is None:
            features = cols
        if isinstance(self, AutomaticTimeSeries):
            data_ = data[id_col + [label] + features]
        else:
            data_ = data[id_col + features + [label]]
        random_state = self._arg('random_state', random_state, int)
        top_k_attributions = self._arg('top_k_attributions',
                                       top_k_attributions, int)
        sample_size = self._arg('sample_size', sample_size, int)
        verbose_output = self._arg('verbose_output', verbose_output, bool)
        predict_args = self._arg('predict_args', predict_args, dict)
        if predict_args:
            if ('GLM_Regressor' in predict_args) and ('prediction_type' in predict_args['GLM_Regressor']):
                del predict_args['GLM_Regressor']['prediction_type']
            elif 'prediction_type' in predict_args:
                del predict_args['prediction_type']
        predict_args_json, no_valid_args = self._gen_predict_args_json(predict_args)
        param_rows = [
            ('SEED', random_state, None, None),
            ('TOP_K_ATTRIBUTIONS', top_k_attributions, None, None),
            ('SAMPLESIZE', sample_size, None, None),
            ('VERBOSE_OUTPUT', verbose_output, None, None)]
        if no_valid_args > 0:
            param_rows.extend([('PREDICT_ARGS', None, None, predict_args_json)])
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        tbls = ['RESULT', 'STATS', 'PH1', 'PH2']
        result_tbls = [f'#PAL_PIPELINE_SCORE_{tb}_TBL_{self.id}_{unique_id}' for tb in tbls]
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
                                *result_tbls)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, result_tbls)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, result_tbls)
            raise
        setattr(self, 'score_metrics_', conn.table(result_tbls[1]))
        return tuple(conn.table(tb) for tb in result_tbls[:2])

    def update_config_dict(self, operator_name, param_name=None, param_config=None):
        """
        Updates the config dict.

        Parameters
        ----------
        operator_name : str
            The name of operator.

        param_name : str, optional
            The parameter name to be updated.
            If the parameter name doesn't exist in the config dict, it will create a new one.

            Defaults to None.

        param_config : any, optional
            The parameter config value.

            Defaults to None.
        """
        config_dict = json.loads(self.config_dict)
        get_operator_dict = self._operator_in_config(operator_name, config_dict)
        if get_operator_dict:
            if param_name == "LAG_FEATURES" :
                config_dict[operator_name][param_name] = [param_config]
            else :
                config_dict[operator_name][param_name] = param_config
        else:
            if param_name:
                if param_name == "LAG_FEATURES" :
                    config_dict[operator_name] = {param_name: [param_config]}
                else :
                    config_dict[operator_name] = {param_name: param_config}
            else:
                config_dict.update({operator_name: {}})
        self.config_dict = json.dumps(config_dict)

    def delete_config_dict(self, operator_name=None, category=None, param_name=None):
        """
        Deletes the content of the config dict.

        Parameters
        ----------
        operator_name : str, optional
            Deletes the operator based on the given name in the config dict.

            Defaults to None.

        category : str, optional
            Deletes the whole given category in the config dict.

            Defaults to None.

        param_name : str, optional
            Deletes the parameter based on the given name once the operator name is provided.

            Defaults to None.
        """
        config_dict = json.loads(self.config_dict)
        result_dict = {}
        if category:
            for op_name in config_dict:
                if op_name in self._category_map:
                    if self._category_map[op_name] != category:
                        result_dict[op_name] = config_dict[op_name]
                else:
                    result_dict[op_name] = config_dict[op_name]
        else:
            for op_name in config_dict:
                if operator_name != op_name:
                    result_dict[op_name] = config_dict[op_name]
                elif param_name is not None:
                    operator_dict = config_dict[op_name]
                    operator_dict.pop(param_name)
                    result_dict[op_name] = operator_dict
        self.config_dict = json.dumps(result_dict)

    def display_config_dict(self, operator_name=None, category=None):
        """
        Displays the config dict.

        Parameters
        ----------
        operator_name : str, optional
            Only displays the information on the given operator name.

            Defaults to None.

        category : str, optional
            Only displays the information on the given category.

            Defaults to None.
        """
        if operator_name:
            operator_dict = self._operator_in_config(operator_name, json.loads(self.config_dict))
            if operator_dict:
                print(operator_name)
                print(len(operator_name)  * "." + "\n")
                param = []
                config = []
                for key, value in operator_dict.items():
                    param.append(key)
                    config.append(value)
                print(pd.DataFrame({"Param": param, "Config": config}))
                print("\n")
            else:
                msg = f"Operator - '{operator_name}' does not exist in the config_dict."
                logger.warning(msg)
        elif category:
            exist_flag = False
            for op_name in json.loads(self.config_dict):
                if op_name in self._category_map:
                    if self._category_map[op_name] == category:
                        exist_flag = True
                        print(op_name)
                        print(len(op_name)  * "." + "\n")
                        param = []
                        config = []
                        operator_dict = self._operator_in_config(op_name, json.loads(self.config_dict))
                        for key, value in operator_dict.items():
                            param.append(key)
                            config.append(value)
                        print(pd.DataFrame({"Param": param, "Config": config}))
                        print("-" * 60 + "\n")
            if exist_flag is False:
                msg = f"Category - '{category}' does not exist in the config_dict."
                logger.warning(msg)
        else:
            category = []
            used_op = []
            for op_key in json.loads(self.config_dict):
                #op_key = list(operator_dict.keys())[0]
                used_op.append(op_key)
                if op_key in self._category_map:
                    category.append(self._category_map[op_key])
                else:
                    category.append('undefined')
            print(pd.DataFrame({"Used Operators": used_op, "Category": category}))
            print("\n" + "-" * 60 + "\n")
            for op_name in json.loads(self.config_dict):
                #op_name = list(operator_dict.keys())[0]
                print(op_name)
                print(len(op_name)  * "." + "\n")
                param = []
                config = []
                for key, value in json.loads(self.config_dict)[op_name].items():
                    param.append(key)
                    config.append(value)
                print(pd.DataFrame({"Param": param, "Config": config}))
                print("-" * 60 + "\n")

    def pipeline_plot(self, name="my_pipeline", iframe_height=450):
        """
        Pipeline plot.

        Parameters
        ----------
        name : str, optional
            The name of the pipeline plot.

            Defaults to 'my_pipeline'.

        iframe_height : int, optional
            The display height.

            Defaults to 450.
        """
        from hana_ml.visualizers.digraph import Digraph
        try:
            digraph = Digraph(name)
            p_content = []
            p_args = []
            pipeline = json.loads(self.best_pipeline_.collect()["PIPELINE"].iat[0])
            pipe = _PipelineWalk(pipeline)
            for i in range(1, 100): #pylint: disable=unused-variable
                p_content.append(pipe.current_content)
                p_args.append(pipe.current_args)
                pipe._next()
                if pipe.end:
                    p_content.append(pipe.current_content)
                    p_args.append(pipe.current_args)
                    break
            p_content.reverse()
            p_args.reverse()
            count = 0
            nodes = []
            for p_1, p_2 in zip(p_content, p_args):
                nodes.append((str(p_1), str(p_2), [str(count)], [str(count + 1)]))
                count = count + 1
            node = []
            for elem in nodes:
                node.append(digraph.add_python_node(elem[0],
                                                    elem[1],
                                                    in_ports=elem[2],
                                                    out_ports=elem[3]))
            for node_x in range(0, len(node) - 1):
                digraph.add_edge(node[node_x].out_ports[0],
                                 node[node_x + 1].in_ports[0])
            digraph.build()
            digraph.generate_notebook_iframe(iframe_height)
        except Exception as err:
            logger.error(err)
            raise

    def update_category_map(self, connection_context):
        """
        Updates the list of operators.

        Parameters
        ----------
        connection_context : str, optional
            The connection to a SAP HANA instance.

        """
        if connection_context:
            update_map = _fetch_default_category_map(connection_context)
            if update_map:
                self._category_map = update_map
                self._fetch_category_map = True

def _fetch_default_config_dict(connection_context, pipeline_type, template_type='default'):
    unique_id = str(uuid.uuid1()).replace('-', '_').upper()
    outputs = ['#PAL_DEFAULT_CONFIG_DICT_{}_{}'.format(tbl, unique_id) for tbl in range(0, 2)]
    param_rows = []
    param_rows.extend([('PIPELINE_TYPE', None, None, pipeline_type)])
    param_rows.extend([('CONFIG_DICT', None, None, template_type)])
    try:
        PALBase()._call_pal_auto(connection_context,
                                 'PAL_AUTOML_CONFIG',
                                 ParameterTable().with_data(param_rows),
                                 *outputs)
    except dbapi.Error as db_err:
        logger.error(str(db_err))
        try_drop(connection_context, outputs)
        raise
    except Exception as db_err:
        logger.error(str(db_err))
        try_drop(connection_context, outputs)
        raise
    result = connection_context.table(outputs[0]).collect().iat[0, 1]
    try_drop(connection_context, outputs)
    return result

def get_pipeline_info(connection_context):
    """
    Returns the information of the supported operators.

    Parameters
    ----------
    connection_context : str, optional
        The connection to a SAP HANA instance.

    """
    output = '#PAL_PIPELINE_INFO_TBL'
    param_rows = []
    try:
        if check_pal_function_exist(connection_context, '%PIPELINE_INFO%', like=True):
            if not connection_context.has_table(output):
                PALBase()._call_pal_auto(connection_context,
                                        'PAL_PIPELINE_INFO',
                                        ParameterTable().with_data(param_rows),
                                        output)
        else:
            return False
    except dbapi.Error as db_err:
        logger.error(str(db_err))
        try_drop(connection_context, output)
        raise
    except Exception as db_err:
        logger.error(str(db_err))
        try_drop(connection_context, output)
        raise
    return connection_context.table(output)

def _fetch_default_category_map(connection_context):
    result = get_pipeline_info(connection_context)
    if result:
        info_df = result.select(['NAME', 'CATEGORY']).collect()
        info_dict = {}
        for i in range(0, len(info_df)):
            key = info_df.iat[i,0]
            value = info_df.iat[i,1]
            info_dict[key] = value
        return info_dict

    return False

class AutomaticClassification(_AutoMLBase):
    """
    AutomaticClassification offers an intelligent search amongst machine learning pipelines for supervised classification tasks.
    Each machine learning pipeline contains several operators such as preprocessors, supervised classification models and transformer
    that follows API of hana-ml algorithms.

    For AutomaticClassification parameter mappings of hana_ml and HANA PAL, please refer to the doc page: :ref:`param_mapping`.

    In addition, in order to better demonstrate the process, we also provide a series of visualizers such as PipelineProgressStatusMonitor, SimplePipelineProgressStatusMonitor, and BestPipelineReport, as well as a set of log management methods.

    Parameters
    ----------
    scorings : dict, optional
        AutomaticClassification supports multi-objective optimization with specified weights for each target.
        The goal is to maximize the target. Therefore, if you want to minimize the target, the target weight needs to be negative.

        The available target options are as follows:

        - ACCURACY : Represents the percentage of correctly classified samples. Higher values indicate better performance. It is recommended to assign a positive weight to this metric.
        - AUC: Stands for Area Under Curve. Higher values indicate better performance. It is recommended to assign a positive weight to this metric.
        - KAPPA : Cohen's kappa coefficient measures the agreement between predicted and actual classifications. Higher values indicate better performance. It is recommended to assign a positive weight to this metric.
        - MCC: Matthews Correlation Coefficient measures the quality of binary classifications. Higher values indicate better performance. It is recommended to assign a positive weight to this metric.
        - RECALL_<CLASS> : Recall represents the ability of a model to identify instances of a specific class. Higher values indicate better performance. It is recommended to assign a positive weight to this metric.
        - PRECISION_<CLASS> : Precision represents the ability of a model to accurately classify instances for a specific class. Higher values indicate better performance. It is recommended to assign a positive weight to this metric.
        - F1_SCORE_<CLASS> : The F1 score measures the balance between precision and recall for a specific class. Higher values indicate better performance. It is recommended to assign a positive weight to this metric.
        - SUPPORT_<CLASS> : The support metric represents the number of instances of a specific class. Higher values indicate better performance. It is recommended to assign a positive weight to this metric.
        - LAYERS: Represents the number of operators used. Lower values indicate better performance. It is recommended to assign a negative weight to this metric.
        - TIME: Represents the computational time in seconds used. Lower values indicate better performance. It is recommended to assign a negative weight to this metric.

        Defaults to {"ACCURACY": 1.0, "AUC": 1.0} (maximize ACCURACY and AUC).

    generations : int, optional
        The number of iterations of the pipeline optimization.

        Defaults to 5.

    population_size : int, optional

        - When ``search_method`` takes the value of 'GA', ``population_size`` is the number of individuals in each generation in genetic programming algorithm. Having too few individuals can limit the possibilities of crossover and exploration of the search space to only a small portion. Conversely, if there are too many individuals, the performance of the genetic algorithm may slow down.
        - When ``search_method`` takes the value of 'random', ``population_size`` is the number of pipelines randomly generated and evaluated in random search.

        Defaults to 20.

    offspring_size : int, optional
        The number of offsprings to produce in each generation.

        It controls the number of new individuals generated in each iteration by genetic operations, from population.

        Defaults to the size of ``population_size``.

    elite_number : int, optional
        The number of elite to produce in each generation.

        Defaults to 1/4 of ``population_size``.

    min_layer : int, optional
        The minimum number of operators in the pipeline.

        Defaults to 1.

    max_layer : int, optional
        The maximum number of operators in a pipeline.

        Defaults to 5.

    mutation_rate : float, optional
        The mutation rate for the genetic programming algorithm.

        Represents the random search ability. A suitable value can prevent the GA from falling into a local optimum.

        The sum of ``mutation_rate`` and ``crossover_rate`` cannot be greater than 1.0. When the sum is less than 1.0, the remaining probability will be used to regenerate.

        Defaults to 0.9.

    crossover_rate : float, optional
        The crossover rate for the genetic programming algorithm.

        Represents the local search ability. A larger crossover rate will cause GA to converge to a local optimum faster.

        The sum of ``mutation_rate`` and ``crossover_rate`` cannot be greater than 1.0. When the sum is less than 1.0, the remaining probability will be used to regenerate.

        Defaults to 0.1.

    random_seed : int, optional
        Specifies the seed for random number generator. Use system time if not provided.

        No default value.

    config_dict : str or dict, optional
        The customized configuration for the searching space.

        - {'light', 'default'}: use provided config_dict templates.
        - JSON format config_dict. It could be JSON string or dict.

        If it is None, the default config_dict will be used.

        Defaults to None.

    progress_indicator_id : str, optional
        Set the ID used to output monitoring information of the optimization progress.

        No default value.

    fold_num : int, optional
        The number of fold in the cross validation process.

        Defaults to 5.

    resampling_method : {'cv', 'stratified_cv'}, optional
        Specifies the resampling method for pipeline evaluation.

        Defaults to 'stratified_cv'.

    max_eval_time_mins : float, optional
        Time limit to evaluate a single pipeline. The unit is minute.

        Defaults to 0.0 (there is no time limit).

    early_stop : int, optional
        Stop optimization progress when best pipeline is not updated for the give consecutive generations.
        0 means there is no early stop.

        Defaults to 5.

    successive_halving : bool, optional
        Specifies whether uses successive_halving in the evaluation phase.

        Defaults to True.

    min_budget : int, optional
        Specifies the minimum budget (the minimum evaluation dataset size) when successive halving has been applied.

        Defaults to 1/5 of dataset size.

    max_budget : int, optional
        Specifies the maximum budget (the maximum evaluation dataset size) when successive halving has been applied.

        Defaults to the whole dataset size.

    min_individuals : int, optional
        Specifies the minimum individuals in the evaluation phase when successive halving has been applied.

        Defaults to 3.

    connections : str or dict, optional

        Specifies the connections in the Connection constrained Optimization. The options are:

        - 'default'
        - customized connections json string or a dict.

        Defaults to None. If ``connections`` is not provided, connection constrained optimization is not applied.

    alpha : float, optional
        Adjusts rejection probability in connection optimization.

        Valid only when ``connections`` is set.

        Defaults to 0.1.

    delta : float, optional
        Controls the increase rate of connection weights.

        Valid only when ``connections`` is set.

        Defaults to 1.0.

    top_k_connections : int, optional
        The number of top connections used to generate optimal connections.

        Valid only when ``connections`` is set.

        Defaults to 1/2 of (connection size in ``connections``).

    top_k_pipelines : int, optional
        The number of pipelines used to update connections in each iteration.

        Valid only when ``connections`` is set.

        Defaults to 1/2 of ``offspring_size``.

    search_method : str, optional

        Optimization algorithm used in AutoML.

        - 'GA': Genetic Algorithm
        - 'random': Random Search

        Defaults to 'GA'.

    fine_tune_pipeline : bool, optional
        Specifies whether or not to fine-tune the pipelines generated by the genetic algorithm.

        Valid only when ``search_method`` takes the value of 'GA'.

        Defaults to False.
    fine_tune_resource : int, optional
        Specifies the resource limit to use for fine-tuning the pipelines generated by the genetic algorithm.

        Valid only when ``fine_tune_pipeline`` is set as True.

        Defaults to the value of ``population_size``.

    with_hyperband : bool, optional
        Indicates whether to use Hyperband

        Only valid when ``search_method`` is "random".

        Defaults to False.

    reduction_rate : float, optional
        Specifies the reduction rate in the Hyperband method.

        Only valid when ``with_hyperband`` is True.

        Defaults to 1.

    min_resource : int, optional
        The minimum number of resources allocated in each iteration of Hyperband.

        Only valid when ``with_hyperband`` is True.

        Defaults to max(5, data.count()/10).

    max_resource : int, optional
        The maximum number of resources allocated in each iteration of Hyperband.

        Only valid when ``with_hyperband`` is True.

        Defaults to data.count().

    References
    ----------
    Under the given ``config_dict`` and ``scoring``, `AutomaticClassification` uses genetic programming to
    to search for the best valid pipeline. Please see :ref:`Genetic Optimization in AutoML<genetic_automl-label>`
    for more details.

    Attributes
    ----------
    best_pipeline_: DataFrame
        Best pipelines selected, structured as follows:

        - 1st column: ID, type INTEGER, pipeline IDs.
        - 2nd column: PIPELINE, type NVARCHAR, pipeline contents.
        - 3rd column: SCORES, type NVARCHAR, scoring metrics for pipeline.

        Available only when the ``pipeline`` parameter is not specified during the fitting process.

    model_ : DataFrame or a list of DataFrames
        If pipeline is not None, structured as follows

        - 1st column: ROW_INDEX.
        - 2nd column: MODEL_CONTENT.

        If auto-ml is enabled, structured as follows

            - 1st DataFrame:

              .. only:: html

                 - 1st column: ROW_INDEX.
                 - 2nd column: MODEL_CONTENT.

              .. only: latex

                ============ ==============
                Column Index Column Name
                ============ ==============
                1            ROW_INDEX
                2            MODEL_CONTENT
                ============ ==============

            - 2nd DataFrame: best_pipeline\_

    info_ : DataFrame
        Related info/statistics for AutomaticClassification pipeline fitting, structured as follows:

        - 1st column: STAT_NAME.
        - 2nd column: STAT_VALUE.

    Examples
    --------

    Create an AutomaticClassification instance:

    >>> progress_id = "automl_{}".format(uuid.uuid1())
    >>> auto_c = AutomaticClassification(generations=2,
                                         population_size=5,
                                         offspring_size=5,
                                         progress_indicator_id=progress_id)
    >>> auto_c.enable_workload_class("MY_WORKLOAD_CLASS")

    Invoke a PipelineProgressStatusMonitor instance:

    >>> progress_status_monitor = PipelineProgressStatusMonitor(connection_context=dataframe.ConnectionContext(url, port, user, pwd),
                                                                automatic_obj=auto_c)
    >>> progress_status_monitor.start()
    >>> auto_c.fit(data=df_train)

    Output:

    .. image:: ../../image/progress_classification.png

    Show the best pipeline:

    >>> print(auto_c.best_pipeline_.collect())
    ID                                           PIPELINE  \
     0  {"HGBT_Classifier":{"args":{"ITER_NUM":100,"MA...
                                               SCORES
    {"ACCURACY":0.6726642676262828,"AUC":0.7516449...

    Plot the best pipeline:

    >>> BestPipelineReport(auto_c).generate_notebook_iframe()

    .. image:: ../../image/best_pipeline_classification.png

    Perform predict():

    >>> res = auto_c.predict(data=df_test)
    >>> print(res.collect())
     ID SCORES
    702      1
    502      0
    ...    ...
    282      1
    581      0

    If you want to use an existing pipeline to fit and predict:

    >>> pipeline = auto_c.best_pipeline_.collect().iat[0, 1]
    >>> auto_c.fit(data=df_train, pipeline=pipeline)
    >>> res = auto_c.predict(data=df_test)
    """
    def __init__(self,
                 scorings=None,
                 generations=None,
                 population_size=None,
                 offspring_size=None,
                 elite_number=None,
                 min_layer=None,
                 max_layer=None,
                 mutation_rate=None,
                 crossover_rate=None,
                 random_seed=None,
                 config_dict=None,
                 progress_indicator_id=None,
                 fold_num=None,
                 resampling_method=None,
                 max_eval_time_mins=None,
                 early_stop=None,
                 successive_halving=None,
                 min_budget=None,
                 max_budget=None,
                 min_individuals=None,
                 connections=None,
                 alpha=None,
                 delta=None,
                 top_k_connections=None,
                 top_k_pipelines=None,
                 search_method=None,
                 fine_tune_pipeline=None,
                 fine_tune_resource=None,
                 with_hyperband=None,
                 reduction_rate=None,
                 min_resource=None,
                 max_resource=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(AutomaticClassification, self).__init__(scorings=scorings,
                                                      generations=generations,
                                                      population_size=population_size,
                                                      offspring_size=offspring_size,
                                                      elite_number=elite_number,
                                                      min_layer=min_layer,
                                                      max_layer=max_layer,
                                                      mutation_rate=mutation_rate,
                                                      crossover_rate=crossover_rate,
                                                      random_seed=random_seed,
                                                      config_dict=config_dict,
                                                      progress_indicator_id=progress_indicator_id,
                                                      fold_num=fold_num,
                                                      resampling_method=resampling_method,
                                                      max_eval_time_mins=max_eval_time_mins,
                                                      early_stop=early_stop,
                                                      successive_halving=successive_halving,
                                                      min_budget=min_budget,
                                                      max_budget=max_budget,
                                                      min_individuals=min_individuals,
                                                      connections=connections,
                                                      alpha=alpha,
                                                      delta=delta,
                                                      top_k_connections=top_k_connections,
                                                      top_k_pipelines=top_k_pipelines,
                                                      search_method=search_method,
                                                      fine_tune_pipeline=fine_tune_pipeline,
                                                      fine_tune_resource=fine_tune_resource,
                                                      with_hyperband=with_hyperband,
                                                      reduction_rate=reduction_rate,
                                                      min_resource=min_resource,
                                                      max_resource=max_resource)
        if (self.config_dict is None) or (self.config_dict =='default'):
            config_dict_file =os.path.join(os.path.dirname(__file__), "templates",
                                            "config_dict_classification_default.json")
            with open(config_dict_file) as input_file:
                self.config_dict = json.dumps(json.load(input_file))
        if self.config_dict == 'light':
            config_dict_file = os.path.join(os.path.dirname(__file__), "templates",
                                            "light_config_dict_classification.json")
            with open(config_dict_file) as input_file:
                self.config_dict = json.dumps(json.load(input_file))

    def predict(self, data, key=None, features=None, model=None,
                show_explainer=False, top_k_attributions=None,
                random_state=None, sample_size=None,
                verbose_output=None,
                predict_args=None):
        r"""
        Predict function for AutomaticClassification.

        Parameters
        ----------
        data :  DataFrame
            Data to be predicted.

        key : str, optional

            Name of the ID column.

            Mandatory if ``data`` is not indexed, or is indexed by multiple columns.

            Defaults to the index of ``data`` if ``data`` is indexed by a single column.

        features : a list of str, optional

            Names of the feature columns.

            If ``features`` is not provided, it defaults to all non-ID columns.

        model : DataFrame, optional
            The model to be used for prediction.

            Defaults to the fitted model (model\_).

        show_explainer : bool, optional
            If True, the reason code will be returned. Only valid when ``background_size`` is provided during the fit() function.

            Defaults to False

        top_k_attributions : int, optional
            Display the top k attributions in reason code.

            Effective only when ``model`` contains background data from the training phase.

            Defaults to PAL's default value.

        random_state : DataFrame, optional
            Specifies the random seed.

            Defaults to 0(system time).

        sample_size : int, optional
            Specifies the number of sampled combinations of features.

            It is better to use a number that is greater than the number of features
            in ``data``.

            If set as 0, it is determined by algorithm heuristically.

            Defaults to 0.

        verbose_output : bool, optional

            - True: Outputs the probability of all label categories.
            - False: Outputs the category of the highest probability only.

            Defaults to True.

        predict_args : dict, optional
            Specifies estimator-specific parameters passed to the predict method.

            If not None, it must be specified as a dict with one of the following format

            - `key` for estimator name, and `value` for estimator-specific parameter setting in a dict.
              For example `{'RDT_Classifier':{'block_size': 5}, 'NB_Classifier':{'laplace':1.0}}`.

            Defaults to None(i.e. no estimator-specific predict parameter provided).

        Returns
        -------
        DataFrame

            Predicted result, structured as follows:

            - 1st column: Data type and name same as the 1st column of ``data``.
            - 2nd column: SCORE, class labels.
            - 3rd column: CONFIDENCE, confidence of a class(available only if ``show_explainer`` is True).
            - 4th column: REASON CODE, attributions of features(available only if ``show_explainer`` is True).
            - 5th & 6th columns: placeholder columns for future implementations(available only if ``show_explainer`` is True).
        """
        return self._predict(data=data,
                             key=key,
                             features=features,
                             model=model,
                             show_explainer=show_explainer,
                             top_k_attributions=top_k_attributions,
                             random_state=random_state,
                             sample_size=sample_size,
                             verbose_output=verbose_output,
                             predict_args=predict_args)

    def _get_highlight_metric(self):
        if self.scorings:
            return list(json.loads(self.scorings).keys())[0]
        return 'AUC'

    def reset_config_dict(self, connection_context=None, template_type='default', config_dict=None):
        """
        Reset config dict.

        Parameters
        ----------
        connection_context : ConnectionContext, optional
            If it is set, the default config dict will use the one stored in a SAP HANA instance.

            Defaults to None.
        template_type : {'default', 'light'}, optional
            HANA config dict type.

            Defaults to 'default'.
        config_dict : str or dict, optional
            Manually set the custom config_dict.

            Defaults to None.
        """
        if connection_context:
            self.config_dict = _fetch_default_config_dict(connection_context, 'classifier', template_type)
        else:
            if config_dict is None:
                if template_type == 'default':
                    template = 'config_dict_classification_default.json'
                else:
                    template = 'light_config_dict_classification.json'
                config_dict_file = os.path.join(os.path.dirname(__file__), "templates",
                                                template)
                with open(config_dict_file) as input_file:
                    self.config_dict = json.dumps(json.load(input_file))
            else:
                if isinstance(config_dict, dict):
                    self.config_dict = json.dumps(config_dict)
                else:
                    self.config_dict = json.dumps(json.loads(config_dict))

    def score(self, data, key=None, features=None, label=None, model=None,
              random_state=None, top_k_attributions=None,
              sample_size=None, verbose_output=None,
              predict_args=None):
        r"""

        Pipeline model score function, with final estimator being a classifier.

        Parameters
        ----------

        data : DataFrame
            Data for pipeline model scoring.

        key : str, optional

            Name of the ID column.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        features : a list of str, optional

            Names of the feature columns.
            Should be same as those provided in the training data.

            If ``features`` is not provided, it defaults to all non-ID, non-label columns.

        label : str, optional

            Name of the dependent variable.

            Defaults to the name of the last non-ID column.

        model : DataFrame, optional
            DataFrame that contains the pipeline model for scoring.

            Defaults to the fitted pipeline model of the current class instance.

        random_state : DataFrame, optional
            Specifies the random seed.

            Defaults to -1(system time).

        top_k_attributions : int, optional
            Display the top k attributions in reason code.

            Effective only when ``model`` contains background data from the training phase.

            Defaults to PAL's default value.

        sample_size : int, optional
            Specifies the number of sampled combinations of features.

            It is better to use a number that is greater than the number of features
            in ``data``.

            If set as 0, it is determined by algorithm heuristically.

            Defaults to 0.
        verbose_output : bool, optional

            - True: Outputs the probability of all label categories.
            - False: Outputs the category of the highest probability only.

            Defaults to True.
        predict_args : dict, optional
            Specifies estimator-specific parameters passed to the predict phase of the score method.

            If not None, it must be specified as a dict with one of the following format

            - `key` for estimator name, and `value` for estimator-specific parameter setting in a dict.
              For example `{'RDT_Classifier':{'block_size': 5}, 'NB_Classifier':{'laplace':1.0}}`.
            - `key` for parameter name, value for parameter value. For example, if the pipeline model for
              prediction is associated with estimator 'RDT_Classifier', then we can specify predict parameters
              of this estimator as `{'block_size': 5}`, by simply omitting the estimator name. This applies
              to the case when we known exactly the estimator info of the pipeline.

            Defaults to None(i.e. no estimator-specific predict parameter provided).

        Returns
        -------
        DataFrames

            DataFrame 1 : Prediction result for the input data, structured as follows:

            - 1st column, ID of input data.
            - 2nd column, SCORE, class assignment.
            - 3rd column, REASON CODE, attribution of features.
            - 4th & 5th column, placeholder columns for future implementations.

            DataFrame 2 : Statistics.
        """
        setattr(self, 'hanaml_score_params', pal_param_register())
        setattr(self, 'testing_data', data)
        return self._score(data=data, key=key,
                           features=features,
                           label=label,
                           model=model,
                           random_state=random_state,
                           top_k_attributions=top_k_attributions,
                           sample_size=sample_size,
                           verbose_output=verbose_output,
                           predict_args=predict_args)

class AutomaticRegression(_AutoMLBase):
    """

    AutomaticRegression offers an intelligent search amongst machine learning pipelines for supervised regression tasks.
    Each machine learning pipeline contains several operators such as preprocessors, supervised regression models and transformer
    that follows API of hana-ml algorithms.

    For AutomaticRegression parameter mappings of hana_ml and HANA PAL, please refer to the doc page: :ref:`param_mapping`

    In addition, in order to better demonstrate the process, we also provide a series of visualizers such as PipelineProgressStatusMonitor, SimplePipelineProgressStatusMonitor, and BestPipelineReport, as well as a set of log management methods.

    Parameters
    ----------
    scorings : dict, optional
        AutomaticRegression supports multi-objective optimization with specified weights for each target.
        The goal is to maximize the target. Therefore, if you want to minimize the target, the weight of the target needs to be negative.

        The available target options are as follows:

        - EVAR: Explained Variance. Higher values indicate better performance. It is recommended to assign a positive weight to this metric.
        - MAE: Mean Absolute Error. Lower values indicate better performance. It is recommended to assign a negative weight to this metric.
        - MAPE: Mean Absolute Percentage Error. Lower values indicate better performance. It is recommended to assign a negative weight to this metric.
        - MAX_ERROR: The maximum absolute difference between the observed value and the expected value. Lower values indicate better performance. It is recommended to assign a negative weight to this metric.
        - MSE: Mean Squared Error. Lower values indicate better performance. It is recommended to assign a negative weight to this metric.
        - RMSE: Root Mean Squared Error. Lower values indicate better performance. It is recommended to assign a negative weight to this metric.
        - R2: R-squared. Higher values indicate better performance. It is recommended to assign a positive weight to this metric.
        - WMAPE: Weighted Mean Absolute Percentage Error. Lower values indicate better performance. It is recommended to assign a negative weight to this metric.
        - LAYERS: The number of operators. Lower values indicate better performance. It is recommended to assign a negative weight to this metric.
        - TIME: Represents the computational time in seconds used. Lower values indicate better performance. It is recommended to assign a negative weight to this metric.

        The defaults to {"MAE": -1.0, "EVAR": 1.0} (minimize MAE and maximize EVAR).

    generations : int, optional
        The number of iterations of the pipeline optimization.

        Defaults to 5.

    population_size : int, optional

        - When ``search_method`` takes the value of 'GA', ``population_size`` is the number of individuals in each generation in genetic programming algorithm. Having too few individuals can limit the possibilities of crossover and exploration of the search space to only a small portion. Conversely, if there are too many individuals, the performance of the genetic algorithm may slow down.
        - When ``search_method`` takes the value of 'random', ``population_size`` is the number of pipelines randomly generated and evaluated in random search.

        Defaults to 20.

    offspring_size : int, optional
        The number of offsprings to produce in each generation.

        It controls the number of new individuals generated in each iteration by genetic operations, from population.

        Defaults to the number of ``population_size``.

    elite_number : int, optional
        The number of elite to output into result table.

        Defaults to 1/4 of ``population_size``.

    min_layer : int, optional
        The minimum number of operators in a pipeline.

        Defaults to 1.

    max_layer : int, optional
        The maximum number of operators in a pipeline.

        Defaults to 5.

    mutation_rate : float, optional
        The mutation rate for the genetic programming algorithm.

        Represents the random search ability. A suitable value can prevent the GA from falling into a local optimum.

        The sum of ``mutation_rate`` and ``crossover_rate`` cannot be greater than 1.0. When the sum is less than 1.0, the remaining probability will be used to regenerate.

        Defaults to 0.9.

    crossover_rate : float, optional
        The crossover rate for the genetic programming algorithm.

        Represents the local search ability. A larger crossover rate will cause GA to converge to a local optimum faster.

        The sum of ``mutation_rate`` and ``crossover_rate`` cannot be greater than 1.0. When the sum is less than 1.0, the remaining probability will be used to regenerate.

        Defaults to 0.1.

    random_seed : int, optional
        Specifies the seed for random number generator. Use system time if not provided.

        No default value.

    config_dict : str or dict, optional
        The customized configuration for the searching space.

        - {'light', 'default'}: use provided config_dict templates.
        - JSON format config_dict. It could be JSON string or dict.

        If it is None, the default config_dict will be used.

        Defaults to None.

    progress_indicator_id : str, optional
        Set the ID used to output monitoring information of the optimization progress.

        No default value.

    fold_num : int, optional
        The number of fold in the cross validation process. If the value is 0, the function will automatically determine the fold number.

        Defaults to 5.

    resampling_method : {'cv'}, optional
        Specifies the resampling method for pipeline evaluation.

        Defaults to 'cv'.

    max_eval_time_mins : float, optional
        Time limit to evaluate a single pipeline. The unit is minute.

        Defaults to 0.0 (there is no time limit).

    early_stop : int, optional
        Stop optimization progress when best pipeline is not updated for the give consecutive generations.
        0 means there is no early stop.

        Defaults to 5.

    successive_halving : bool, optional
        Specifies whether uses successive_halving in the evaluation phase.

        Defaults to True.

    min_budget : int, optional
        The minimum data size used in successive halving iteration when successive halving has been applied.

        Defaults to 1/5 of dataset size.

    max_budget : int, optional
        The maximum data size used in successive halving iteration when successive halving has been applied.

        Defaults to the whole dataset size.

    min_individuals : int, optional
        The minimum population size in successive halving iteration.

        Defaults to 3.

    connections : str or dict, optional

        Specifies the connections in the Connection constrained Optimization. The options are:

        - 'default'
        - customized connections json string or a dict.

        Defaults to None. If ``connections`` is not provided, connection constrained optimization is not applied.

    alpha : float, optional
        Adjusts rejection probability in connection optimization.

        Valid only when ``connections`` is set.

        Defaults to 0.1.

    delta : float, optional
        Controls the increase rate of connection weights.

        Valid only when ``connections`` is set.

        Defaults to 1.0.

    top_k_connections : int, optional
        The number of top connections used to generate optimal connections.

        Valid only when ``connections`` is set.

        Defaults to 1/2 of (connection size in ``connections``).

    top_k_pipelines : int, optional
        The number of pipelines used to update connections in each iteration.

        Valid only when ``connections`` is set.

        Defaults to 1/2 of ``offspring_size``.

    search_method : str, optional

        Optimization algorithm used in AutoML.

        - 'GA': Genetic Algorithm
        - 'random': Random Search

        Defaults to 'GA'.

    fine_tune_pipeline : bool, optional
        Specifies whether or not to fine-tune the pipelines generated by the genetic algorithm.

        Valid only when ``search_method`` takes the value of 'GA'.

        Defaults to False.
    fine_tune_resource : int, optional
        Specifies the resource limit to use for fine-tuning the pipelines generated by the genetic algorithm.

        Valid only when ``fine_tune_pipeline`` is set as True.

        Defaults to the value of ``population_size``.

    outlier : bool, optional
        Indicates whether to perform regression outlier tuning on the best pipeline.

        - False: No
        - True: Yes

        Defaults to False.

    outlier_thresholds : float of list of float, optional
        The thresholds for regression outlier detection in regression outlier tuning.
        This parameter supports multiple input values.
        Refer to the SQL example in the Regression Outlier Tuning section of the Optimization documentation for value assignment.

        Valid only when ``outlier`` is set as True.

        Defaults to [3.0, 4.0, 5.0].

    outlier_pipeline_num : int, optional
        Number of pipelines with regression outlier detection in regression outlier tuning process.
        If set to 0, PAL evaluates all possible pipelines with regression outlier detection.
        If set to a non-zero value less than the number of input values for parameter ``outlier_thresholds`` PAL adjusts it to match the number of input values for ``outlier_thresholds``.

        Valid only when ``outlier`` is set as True.

        Defaults to the value of ``population_size``.

    outlier_tune_elite_number : int, optional
        Number of elite pipelines in the regression outlier tuning process to output into the result table.
        The ``elite_number`` parameter applies to pipelines before the regression outlier tuning process.

        Valid only when ``outlier`` is set as True.

        Defaults to the value of ``population_size`` divided by 4.

    with_hyperband : bool, optional
        Indicates whether to use Hyperband

        Only valid when ``search_method`` is "random".

        Defaults to False.

    reduction_rate : float, optional
        Specifies the reduction rate in the Hyperband method.

        Only valid when ``with_hyperband`` is True.

        Defaults to 1.

    min_resource : int, optional
        The minimum number of resources allocated in each iteration of Hyperband.

        Only valid when ``with_hyperband`` is True.

        Defaults to max(5, data.count()/10).

    max_resource : int, optional
        The maximum number of resources allocated in each iteration of Hyperband.

        Only valid when ``with_hyperband`` is True.

        Defaults to data.count().

    References
    ----------
    Under the given ``config_dict`` and ``scoring``, `AutomaticRegression` uses genetic programming to
    to search for the best valid pipeline. Please see :ref:`Genetic Optimization in AutoML<genetic_automl-label>`
    for more details.

    Attributes
    ----------
    best_pipeline_: DataFrame
        Best pipelines selected, structured as follows:

        - 1st column: ID, type INTEGER, pipeline IDs.
        - 2nd column: PIPELINE, type NVARCHAR, pipeline contents.
        - 3rd column: SCORES, type NVARCHAR, scoring metrics for pipeline.

        Available only when the ``pipeline`` parameter is not specified during the fitting process.

    model_ : DataFrame or a list of DataFrames
        If pipeline is not None, structured as follows

        - 1st column: ROW_INDEX.
        - 2nd column: MODEL_CONTENT.

        If auto-ml is enabled, structured as follows

            - 1st DataFrame:

              .. only:: html

                 - 1st column: ROW_INDEX.
                 - 2nd column: MODEL_CONTENT.

              .. only: latex

                ============ ==============
                Column Index Column Name
                ============ ==============
                1            ROW_INDEX
                2            MODEL_CONTENT
                ============ ==============

            - 2nd DataFrame: best_pipeline\_

    info_ : DataFrame
        Related info/statistics for AutomaticRegression pipeline fitting, structured as follows:

        - 1st column: STAT_NAME.
        - 2nd column: STAT_VALUE.

    Examples
    --------

    Create an AutomaticRegression instance:

    >>> progress_id = "automl_{}".format(uuid.uuid1())
    >>> auto_r = AutomaticRegression(generations=5,
                                     population_size=5,
                                     offspring_size=5,
                                     scorings={'MSE':-1.0, 'RMSE':-1.0},
                                     progress_indicator_id=progress_id)
    >>> auto_r.enable_workload_class("MY_WORKLOAD_CLASS")

    Invoke a PipelineProgressStatusMonitor instance:

    >>> progress_status_monitor = PipelineProgressStatusMonitor(connection_context=dataframe.ConnectionContext(url, port, user, pwd),
                                                                automatic_obj=auto_r)
    >>> progress_status_monitor.start()
    >>> auto_r.fit(data=df_train)

    Output:

    .. image:: ../../image/progress_regression.png

    Show the best pipeline:

    >>> print(auto_r.best_pipeline_.collect())

    Plot the best pipeline:

    .. image:: ../../image/best_pipeline_regression.png

    >>> BestPipelineReport(auto_r).generate_notebook_iframe()

    Make prediction:

    >>> res = auto_r.predict(data=df_test)

    If you want to use an existing pipeline to fit and predict:

    >>> pipeline = auto_r.best_pipeline_.collect().iat[0, 1]
    >>> auto_r.fit(data=df_train, pipeline=pipeline)
    >>> res = auto_r.predict(data=df_test)
    """
    def __init__(self,
                 scorings=None,
                 generations=None,
                 population_size=None,
                 offspring_size=None,
                 elite_number=None,
                 min_layer=None,
                 max_layer=None,
                 mutation_rate=None,
                 crossover_rate=None,
                 random_seed=None,
                 config_dict=None,
                 progress_indicator_id=None,
                 fold_num=None,
                 resampling_method=None,
                 max_eval_time_mins=None,
                 early_stop=None,
                 successive_halving=None,
                 min_budget=None,
                 max_budget=None,
                 min_individuals=None,
                 connections=None,
                 alpha=None,
                 delta=None,
                 top_k_connections=None,
                 top_k_pipelines=None,
                 search_method=None,
                 fine_tune_pipeline=None,
                 fine_tune_resource=None,
                 outlier=None,
                 outlier_thresholds=None,
                 outlier_pipeline_num=None,
                 outlier_tune_elite_number=None,
                 with_hyperband=None,
                 reduction_rate=None,
                 min_resource=None,
                 max_resource=None
                 ):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(AutomaticRegression, self).__init__(scorings=scorings,
                                                  generations=generations,
                                                  population_size=population_size,
                                                  offspring_size=offspring_size,
                                                  elite_number=elite_number,
                                                  min_layer=min_layer,
                                                  max_layer=max_layer,
                                                  mutation_rate=mutation_rate,
                                                  crossover_rate=crossover_rate,
                                                  random_seed=random_seed,
                                                  config_dict=config_dict,
                                                  progress_indicator_id=progress_indicator_id,
                                                  fold_num=fold_num,
                                                  resampling_method=resampling_method,
                                                  max_eval_time_mins=max_eval_time_mins,
                                                  early_stop=early_stop,
                                                  successive_halving=successive_halving,
                                                  min_budget=min_budget,
                                                  max_budget=max_budget,
                                                  min_individuals=min_individuals,
                                                  connections=connections,
                                                  alpha=alpha,
                                                  delta=delta,
                                                  top_k_connections=top_k_connections,
                                                  top_k_pipelines=top_k_pipelines,
                                                  search_method=search_method,
                                                  fine_tune_pipeline=fine_tune_pipeline,
                                                  fine_tune_resource=fine_tune_resource,
                                                  outlier=outlier,
                                                  outlier_thresholds=outlier_thresholds,
                                                  outlier_pipeline_num=outlier_pipeline_num,
                                                  outlier_tune_elite_number=outlier_tune_elite_number,
                                                  with_hyperband=with_hyperband,
                                                  reduction_rate=reduction_rate,
                                                  min_resource=min_resource,
                                                  max_resource=max_resource)
        if (self.config_dict is None) or (self.config_dict == 'default'):
            config_dict_file = os.path.join(os.path.dirname(__file__), "templates",
                                            "config_dict_regression_default.json")
            with open(config_dict_file) as input_file:
                self.config_dict = json.dumps(json.load(input_file))
        if self.config_dict == 'light':
            config_dict_file = os.path.join(os.path.dirname(__file__), "templates",
                                            "light_config_dict_regression.json")
            with open(config_dict_file) as input_file:
                self.config_dict = json.dumps(json.load(input_file))

    def predict(self, data, key=None, features=None, model=None,
                show_explainer=False, top_k_attributions=None,
                random_state=None, sample_size=None,
                predict_args=None):
        r"""
        Predict function for AutomaticClassification.

        Parameters
        ----------
        data :  DataFrame
            Data to be predicted.

        key : str, optional

            Name of the ID column.

            Mandatory if ``data`` is not indexed, or is indexed by multiple columns.

            Defaults to the index of ``data`` if ``data`` is indexed by a single column.

        features : a list of str, optional

            Names of the feature columns.

            If ``features`` is not provided, it defaults to all non-ID columns.

        model : DataFrame, optional
            The model to be used for prediction.

            Defaults to the fitted model (model\_).

        show_explainer : bool, optional
            If True, the reason code will be returned. Only valid when ``background_size`` is provided during the fit() function.

            Defaults to False
        top_k_attributions : int, optional
            Display the top k attributions in reason code.

            Effective only when ``model`` contains background data from the training phase.

            Defaults to PAL's default value.

        random_state : DataFrame, optional
            Specifies the random seed.

            Defaults to 0(system time).

        sample_size : int, optional
            Specifies the number of sampled combinations of features.

            It is better to use a number that is greater than the number of features
            in ``data``.

            If set as 0, it is determined by algorithm heuristically.

            Defaults to 0.

        predict_args : dict, optional
            Specifies estimator-specific parameters passed to the predict method.

            If not None, it must be specified as a dict with one of the following formats:

            - `key` for estimator name, and `value` for estimator-specific parameter setting in a dict.
              For example `{'RDT_Classifier':{'block_size': 5}, 'NB_Classifier':{'laplace':1.0}}`.

            Defaults to None(i.e. no estimator-specific predict parameter provided).

        Returns
        -------
        DataFrame

            Predicted result, structured as follows:

            - 1st column: Data type and name same as the 1st column of ``data``.
            - 2nd column: SCORE, target regression values.
            - 3rd column: CONFIDENCE, all NULLs for regression(available only if ``show_explainer`` is True).
            - 4th column: REASON CODE, attributions of features(available only if ``show_explainer`` is True).
            - 5th & 6th columns: placeholder columns for future implementations(available only if ``show_explainer`` is True).
        """
        return self._predict(data=data, key=key, features=features,
                             model=model,
                             show_explainer=show_explainer,
                             top_k_attributions=top_k_attributions,
                             random_state=random_state,
                             sample_size=sample_size,
                             verbose_output=None,
                             predict_args=predict_args)

    def _get_highlight_metric(self):
        if self.scorings:
            return list(json.loads(self.scorings).keys())[0]
        return 'MAE'

    def reset_config_dict(self, connection_context=None, template_type='default'):
        """
        Reset config dict.

        Parameters
        ----------
        connection_context : ConnectionContext, optional
            If it is set, the default config dict will use the one stored in a SAP HANA instance.

            Defaults to None.
        template_type : {'default', 'light'}, optional
            HANA config dict type.

            Defaults to 'default'.
        """
        if connection_context:
            self.config_dict = _fetch_default_config_dict(connection_context, 'regressor', template_type)
        else:
            if template_type == 'default':
                template = "config_dict_regression_default.json"
            else:
                template = "light_config_dict_regression.json"
            config_dict_file = os.path.join(os.path.dirname(__file__), "templates",
                                            template)
            with open(config_dict_file) as input_file:
                self.config_dict = json.dumps(json.load(input_file))

    def score(self, data, key=None, features=None, label=None,
              model=None, random_state=None, top_k_attributions=None,
              sample_size=None, predict_args=None):
        r"""
        Pipeline model score function, with final estimator being a regressor.

        Parameters
        ----------
        data : DataFrame
            Data for pipeline model scoring.

        key : str, optional

            Name of the ID column.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        features : a list of str, optional

            Names of the feature columns.
            Should be same as those provided in the training data.

            If ``features`` is not provided, it defaults to all non-ID, non-label columns.

        label : str, optional

            Name of the dependent variable.

            Defaults to the name of the last non-ID column.

        model : DataFrame, optional
            DataFrame that contains the pipeline model for scoring.

            Defaults to the fitted pipeline model of the current class instance.

        random_state : DataFrame, optional
            Specifies the random seed.

            Defaults to 0(system time).

        top_k_attributions : int, optional
            Display the top k attributions in reason code.

            Effective only when ``model`` contains background data from the training phase.

            Defaults to PAL's default value.

        sample_size : int, optional
            Specifies the number of sampled combinations of features.

            It is better to use a number that is greater than the number of features
            in ``data``.

            If set as 0, it is determined by algorithm heuristically.

            Defaults to 0.

        predict_args : dict, optional
            Specifies estimator-specific parameters passed to the predict phase of the score method.

            If not None, it must be specified as a dict with one of the following format

            - `key` for estimator name, and `value` for estimator-specific parameter setting in a dict.
              For example `{'RDT_Classifier':{'block_size': 5}, 'NB_Classifier':{'laplace':1.0}}`.
            - `key` for parameter name, value for parameter value. For example, if the pipeline model for
              prediction is associated with estimator 'RDT_Classifier', then we can specify predict parameters
              of this estimator as `{'block_size': 5}`, by simply omitting the estimator name. This applies
              to the case when we known exactly the estimator info of the pipeline.

            Defaults to None(i.e. no estimator-specific predict parameter provided).

        Returns
        -------
        DataFrames

            DataFrame 1 : Prediction result for the input data, structured as follows:

            - 1st column, ID of input data.
            - 2nd column, SCORE, predicted target value.
            - 3rd column, REASON CODE, attribution of features.
            - 4th & 5th column, placeholder columns for future implementations.

            DataFrame 2 : Statistics.
        """
        setattr(self, 'hanaml_score_params', pal_param_register())
        setattr(self, 'testing_data', data)
        return self._score(data=data,
                           key=key,
                           features=features,
                           label=label,
                           random_state=random_state,
                           model=model,
                           top_k_attributions=top_k_attributions,
                           sample_size=sample_size,
                           predict_args=predict_args)

class AutomaticTimeSeries(_AutoMLBase):
    """

    AutomaticTimeSeries offers an intelligent search amongst machine learning pipelines for time series tasks.
    Each machine learning pipeline contains several operators such as preprocessors, time series models and transformer
    that follows API of hana-ml algorithms.

    For AutomaticTimeSeries parameter mappings of hana_ml and HANA PAL, please refer to the doc page: :ref:`param_mapping`

    In addition, in order to better demonstrate the process, we also provide a series of visualizers such as PipelineProgressStatusMonitor, SimplePipelineProgressStatusMonitor, and BestPipelineReport, as well as a set of log management methods.

    Parameters
    ----------
    scorings : dict, optional
        AutomaticTimeSeries supports multi-objective optimization with specified weights of each target.
        The goal is to maximize the target. Therefore,
        if you want to minimize the target, the weight of target needs to be negative.

        The available target options are as follows:

        - EVAR: Explained Variance. Higher values indicate better performance. It is recommended to assign a positive weight to this metric.
        - MAE: Mean Absolute Error. Lower values indicate better performance. It is recommended to assign a negative weight to this metric.
        - MAPE: Mean Absolute Percentage Error. Lower values indicate better performance. It is recommended to assign a negative weight to this metric.
        - MAX_ERROR: The maximum absolute difference between the observed value and the expected value. Lower values indicate better performance. It is recommended to assign a negative weight to this metric.
        - MSE: Mean Squared Error. Lower values indicate better performance. It is recommended to assign a negative weight to this metric.
        - R2: R-squared. Higher values indicate better performance. It is recommended to assign a positive weight to this metric.
        - RMSE: Root Mean Squared Error. Lower values indicate better performance. It is recommended to assign a negative weight to this metric.
        - WMAPE: Weighted Mean Absolute Percentage Error. Lower values indicate better performance. It is recommended to assign a negative weight to this metric.
        - LAYERS: The number of operators. Lower values indicate better performance. It is recommended to assign a negative weight to this metric.
        - SPEC: Stock keeping oriented Prediction Error Costs. Lower values indicate better performance. It is recommended to assign a negative weight to this metric.
        - TIME: Represents the computational time in seconds used. Lower values indicate better performance. It is recommended to assign a negative weight to this metric.

        Defaults to {MAE":-1.0, "EVAR":1.0} (minimize MAE and maximize EVAR).

    generations : int, optional
        The number of iterations of the pipeline optimization.

        Defaults to 5.

    population_size : int, optional

        - When ``search_method`` takes the value of 'GA', ``population_size`` is the number of individuals in each generation in genetic programming algorithm. Having too few individuals can limit the possibilities of crossover and exploration of the search space to only a small portion. Conversely, if there are too many individuals, the performance of the genetic algorithm may slow down.
        - When ``search_method`` takes the value of 'random', ``population_size`` is the number of pipelines randomly generated and evaluated in random search.

        Defaults to 20.

    offspring_size : int, optional
        The number of offsprings to produce in each generation.

        It controls the number of new individuals generated in each iteration by genetic operations, from population.

        Defaults to the number of ``population_size``.

    elite_number : int, optional
        The number of elite to output into result table.

        Defaults to 1/4 of ``population_size``.

    min_layer : int, optional
        The minimum number of operators in a pipeline.

        Defaults to 1.

    max_layer : int, optional
        The maximum number of operators in a pipeline.

        Defaults to 5.

    mutation_rate : float, optional
        The mutation rate for the genetic programming algorithm.

        Represents the random search ability. A suitable value can prevent the GA from falling into a local optimum.

        The sum of ``mutation_rate`` and ``crossover_rate`` cannot be greater than 1.0. When the sum is less than 1.0, the remaining probability will be used to regenerate.

        Defaults to 0.9.

    crossover_rate : float, optional
        The crossover rate for the genetic programming algorithm.

        Represents the local search ability. A larger crossover rate will cause GA to converge to a local optimum faster.

        The sum of ``mutation_rate`` and ``crossover_rate`` cannot be greater than 1.0. When the sum is less than 1.0, the remaining probability will be used to regenerate.

        Defaults to 0.1.

    random_seed : int, optional
        Specifies the seed for random number generator. Use system time if not provided.

        No default value.

    config_dict : str or dict, optional
        The customized configuration for the searching space.

        - {'light', 'default'}: use provided config_dict templates.
        - JSON format config_dict. It could be JSON string or dict.

        If it is None, the default config_dict will be used.

        Defaults to None.

    progress_indicator_id : str, optional
        Set the ID used to output monitoring information of the optimization progress.

        No default value.

    fold_num : int, optional
        The number of fold in the cross validation process. If the value is 0, the function will automatically determine the fold number.

        Defaults to 5.

    resampling_method : {'rocv', 'block'}, optional
        Specifies the resampling method for pipeline evaluation.

        Defaults to 'rocv'.

    max_eval_time_mins : float, optional
        Time limit to evaluate a single pipeline. The unit is minute.

        Defaults to 0.0 (there is no time limit).

    early_stop : int, optional
        Stop optimization progress when best pipeline is not updated for the give consecutive generations.
        0 means there is no early stop.

        Defaults to 5.

    percentage : float, optional
        Percentage between training data and test data. Only applicable when resampling_method is 'block'.

        Defaults to 0.7.

    gap_num : int, optional

        Number of samples to exclude from the end of each train set before the test set.

        Defaults to 0.

    connections : str or dict, optional

        Specifies the connections in the Connection constrained Optimization. The options are:

        - 'default'
        - customized connections json string or a dict.

        Defaults to None. If ``connections`` is not provided, connection constrained optimization is not applied.

    alpha : float, optional
        Adjusts rejection probability in connection optimization.

        Valid only when ``connections`` is set.

        Defaults to 0.1.

    delta : float, optional
        Controls the increase rate of connection weights.

        Valid only when ``connections`` is set.

        Defaults to 1.0.

    top_k_connections : int, optional
        The number of top connections used to generate optimal connections.

        Valid only when ``connections`` is set.

        Defaults to 1/2 of (connection size in ``connections``).

    top_k_pipelines : int, optional
        The number of pipelines used to update connections in each iteration.

        Valid only when ``connections`` is set.

    search_method : str, optional

        Optimization algorithm used in AutoML.

        - 'GA': Genetic Algorithm
        - 'random': Random Search

        Defaults to 'GA'.

        Defaults to 1/2 of ``offspring_size``.
    fine_tune_pipeline : bool, optional
        Specifies whether or not to fine-tune the pipelines generated by the genetic algorithm.

        Valid only when ``search_method`` takes the value of 'GA'.

        Defaults to False.
    fine_tune_resource : int, optional
        Specifies the resource limit to use for fine-tuning the pipelines generated by the genetic algorithm.

        Valid only when ``fine_tune_pipeline`` is set as True.

        Defaults to the value of ``population_size``.

    with_hyperband : bool, optional
        Indicates whether to use Hyperband

        Only valid when ``search_method`` is "random".

        Defaults to False.

    reduction_rate : float, optional
        Specifies the reduction rate in the Hyperband method.

        Only valid when ``with_hyperband`` is True.

        Defaults to 1.

    min_resource : int, optional
        The minimum number of resources allocated in each iteration of Hyperband.

        Only valid when ``with_hyperband`` is True.

        Defaults to max(5, data.count()/10).

    max_resource : int, optional
        The maximum number of resources allocated in each iteration of Hyperband.

        Only valid when ``with_hyperband`` is True.

        Defaults to data.count().

    References
    ----------
    Under the given ``config_dict`` and ``scoring``, `AutomaticTimeSeries` uses genetic programming to
    to search for the best valid pipeline. Please see :ref:`Genetic Optimization in AutoML<genetic_automl-label>`
    for more details.

    Attributes
    ----------
    best_pipeline_: DataFrame
        Best pipelines selected, structured as follows:

        - 1st column: ID, type INTEGER, pipeline IDs.
        - 2nd column: PIPELINE, type NVARCHAR, pipeline contents.
        - 3rd column: SCORES, type NVARCHAR, scoring metrics for pipeline.

        Available only when the ``pipeline`` parameter is not specified during the fitting process.

    model_ : DataFrame or a list of DataFrames
        If pipeline is not None, structured as follows

        - 1st column: ROW_INDEX
        - 2nd column: MODEL_CONTENT

        If auto-ml is enabled, structured as follows

        - 1st DataFrame:

          .. only:: html

             - 1st column: ROW_INDEX
             - 2nd column: MODEL_CONTENT

          .. only: latex

            ============ ==============
            Column Index Column Name
            ============ ==============
            1            ROW_INDEX
            2            MODEL_CONTENT
            ============ ==============

        - 2nd DataFrame: best_pipeline\_

    info_ : DataFrame
        Related info/statistics for AutomaticTimeSeries pipeline fitting, structured as follows:

        - 1st column: STAT_NAME
        - 2nd column: STAT_VALUE

    Examples
    --------

    Create an AutomaticTimeSeries instance:

    >>> progress_id = "automl_{}".format(uuid.uuid1())
    >>> auto_ts = AutomaticTimeSeries(generations=2,
                                      population_size=5,
                                      offspring_size=5,
                                      progress_indicator_id=progress_id)
    >>> auto_ts.enable_workload_class("MY_WORKLOAD_CLASS")

    Invoke a PipelineProgressStatusMonitor instance:

    >>> progress_status_monitor = PipelineProgressStatusMonitor(connection_context=dataframe.ConnectionContext(url, port, user, pwd),
                                                                automatic_obj=auto_ts)
    >>> progress_status_monitor.start()
    >>> auto_ts.fit(data=df_ts, key='ID', endog="SERIES")

    Output:

    .. image:: ../../image/progress_ts.png

    Show the best pipeline:

    >>> print(auto_ts.best_pipeline_.collect())
       ID                                           PIPELINE  \
    0   0  {"SingleExpSm":{"args":{"ALPHA":0.6,"PHI":0.3}...

    Plot the best pipeline:

    >>> BestPipelineReport(auto_ts).generate_notebook_iframe()

    .. image:: ../../image/best_pipeline_ts.png

    Make prediction:

    >>> res = auto_ts.predict(data=df_predict)

    If you want to use an existing pipeline to fit and predict:

    >>> pipeline = auto_ts.best_pipeline_.collect().iat[0, 1]
    >>> auto_ts.fit(data=df_ts, pipeline=pipeline)
    >>> res = auto_ts.predict(data=df_predict)

    """
    def __init__(self,
                 scorings=None,
                 generations=None,
                 population_size=None,
                 offspring_size=None,
                 elite_number=None,
                 min_layer=None,
                 max_layer=None,
                 mutation_rate=None,
                 crossover_rate=None,
                 random_seed=None,
                 config_dict=None,
                 progress_indicator_id=None,
                 fold_num=None,
                 resampling_method=None,
                 max_eval_time_mins=None,
                 early_stop=None,
                 percentage=None,
                 gap_num=None,
                 connections=None,
                 alpha=None,
                 delta=None,
                 top_k_connections=None,
                 top_k_pipelines=None,
                 search_method=None,
                 fine_tune_pipeline=None,
                 fine_tune_resource=None,
                 with_hyperband=None,
                 reduction_rate=None,
                 min_resource=None,
                 max_resource=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(AutomaticTimeSeries, self).__init__(scorings=scorings,
                                                  generations=generations,
                                                  population_size=population_size,
                                                  offspring_size=offspring_size,
                                                  elite_number=elite_number,
                                                  min_layer=min_layer,
                                                  max_layer=max_layer,
                                                  mutation_rate=mutation_rate,
                                                  crossover_rate=crossover_rate,
                                                  random_seed=random_seed,
                                                  config_dict=config_dict,
                                                  progress_indicator_id=progress_indicator_id,
                                                  fold_num=fold_num,
                                                  resampling_method=resampling_method,
                                                  max_eval_time_mins=max_eval_time_mins,
                                                  early_stop=early_stop,
                                                  connections=connections,
                                                  alpha=alpha,
                                                  delta=delta,
                                                  top_k_connections=top_k_connections,
                                                  top_k_pipelines=top_k_pipelines,
                                                  search_method=search_method,
                                                  fine_tune_pipeline=fine_tune_pipeline,
                                                  fine_tune_resource=fine_tune_resource,
                                                  with_hyperband=with_hyperband,
                                                  reduction_rate=reduction_rate,
                                                  min_resource=min_resource,
                                                  max_resource=max_resource)
        if (self.config_dict is None) or (self.config_dict == 'default'):
            config_dict_file = os.path.join(os.path.dirname(__file__), "templates",
                                            "config_dict_timeseries_default.json")
            with open(config_dict_file) as input_file:
                self.config_dict = json.dumps(json.load(input_file))
        if self.config_dict == 'light':
            config_dict_file = os.path.join(os.path.dirname(__file__), "templates",
                                            "light_config_dict_timeseries.json")
            with open(config_dict_file) as input_file:
                self.config_dict = json.dumps(json.load(input_file))

        self.percentage = self._arg('percentage', percentage, float)
        self.gap_num = self._arg('gap_num', gap_num, int)

    @mlflow_autologging(logtype='pal_fit')
    @trace_sql
    def fit(self, data,#pylint:disable=arguments-differ, arguments-renamed
            key=None,
            endog=None,
            exog=None,
            pipeline=None,
            categorical_variable=None,
            background_size=None,
            background_sampling_seed=None,
            model_table_name=None,
            lag=None,
            lag_features=None,
            use_explain=None):
        r"""
        The fit function for AutomaticTimeSeries.

        Parameters
        ----------
        data : DataFrame
            The input time-series data for training.
        key : str, optional
            Specifies the column that represents the ordering of time-series data.

            If ``data`` is indexed by a single column, then ``key`` defaults
            to that index column; otherwise ``key`` must be specified(i.e. is mandatory).
        endog : str, optional
            Specifies the endogenous variable for time-series data.

            Defaults to the 1st non-key column of  ``data``
        exog : str, optional
            Specifies the exogenous variables for time-series data.

            Defaults to all non-key, non-endog columns in ``data``.
        pipeline : str or dict, optional
            Directly use the input pipeline to fit.

            Defaults to None.
        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.
        background_size : int, optional
            This represents the amount of background data in Kernel SHAP. Its value should not exceed the number of rows in the training data. Only valid when ``use_explain`` is True.

            Defaults to None.
        background_sampling_seed : int, optional
            Specifies the seed for random number generator in the background sampling. Only valid when ``use_explain`` is True.

            - 0: Uses the current time (in second) as seed.
            - Others: Uses the specified value as seed.

            Defaults to 0.
        model_table_name : str, optional
            Specifies the HANA model table name instead of the generated temporary table.

            Defaults to None.
        lag : int, a list of int or dict, optional

            The number of previous time stamp data used for generating features in current time stamp.
            Only valid when operator is HGBT_TimeSeries or MLR_TimeSeries.

            If ``lag`` is a integer or a list of integer, both content of operators 'HGBT_TimeSeries' and 'MLR_TimeSeries' will be updated.

            If ``lag`` is a dictionary, the key of this dictionary is the name of operator and value could be a integer, a list of integer or a dictionary of range (start, step, stop).
            Example : {"HGBT_TimeSeries" : 5}, or {"HGBT_TimeSeries" : [5, 7, 9]} or {"HGBT_TimeSeries" : {"range":[1,3,10]}}.

            Defaults to minimum of 100 and (data size)/10.
        lag_features : str, a list of strings or dict, optional

            The name of features in time series data used for generating new data features. The name of target column should not be contained.
            Only valid when operator is HGBT_TimeSeries or MLR_TimeSeries.

            If ``lag_features`` is a string or a list of strings, both content of operators 'HGBT_TimeSeries' and 'MLR_TimeSeries' will be updated.

            If ``lag_features`` is a dictionary, the key of this dictionary is the name of operator and value could be a string or a list of strings.
            Example : {"MLR_TimeSeries" : "FEATURE_A"}, or {"MLR_TimeSeries" : ["FEATURE_A", "FEATURE_B", "FEATURE_C"]}.

            Defaults to None.
        use_explain : bool, optional
            Specifies whether to store information for explaination.

            Defaults to False.

        Returns
        -------
        A fitted object of class "AutomaticTimeSeries".
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        setattr(self, "key", key)
        setattr(self, "exog", exog)
        setattr(self, "endog", endog)

        if lag:
            if isinstance(lag, (int, list)):
                param_config = lag
                if isinstance(lag, int):
                    param_config = [lag]
                self.update_config_dict(operator_name='HGBT_TimeSeries',
                                        param_name='LAG',
                                        param_config=param_config)
                self.update_config_dict(operator_name='MLR_TimeSeries',
                                        param_name='LAG',
                                        param_config=param_config)
            if isinstance(lag, dict):
                operator_name = next(iter(lag))
                param_config = lag[operator_name]
                if isinstance(param_config, int):
                    param_config = [param_config]
                self.update_config_dict(operator_name=operator_name,
                                        param_name='LAG',
                                        param_config=param_config)
        if lag_features:
            if isinstance(lag_features, (str, list)):
                param_config = lag_features
                if isinstance(lag_features, str):
                    param_config = [lag_features]
                self.update_config_dict(operator_name='HGBT_TimeSeries',
                                        param_name='LAG_FEATURES',
                                        param_config=param_config)
                self.update_config_dict(operator_name='MLR_TimeSeries',
                                        param_name='LAG_FEATURES',
                                        param_config=param_config)
            if isinstance(lag_features, dict):
                operator_name = next(iter(lag_features))
                param_config = lag_features[operator_name]
                if isinstance(param_config, str):
                    param_config = [param_config]
                self.update_config_dict(operator_name=operator_name,
                                        param_name='LAG_FEATURES',
                                        param_config=param_config)

        return super(AutomaticTimeSeries, self).fit(data, key,
                                                    exog, endog,
                                                    pipeline,
                                                    categorical_variable,
                                                    background_size,
                                                    background_sampling_seed,
                                                    model_table_name,
                                                    use_explain)

    def predict(self, data,#pylint:disable=arguments-differ, arguments-renamed
                key=None,
                exog=None,
                model=None,
                show_explainer=False,
                predict_args=None):
        r"""
        Predict function for AutomaticTimeSeries.

        Parameters
        ----------
        data :  DataFrame
            The input time-series data to be predicted.

        key : str, optional
            Specifies the column that represents the ordering of the input time-series data.

            If ``data`` is indexed by a single column, then ``key`` defaults
            to that index column; otherwise ``key`` must be specified(i.e. is mandatory).

        exog : str or a list of str, optional

            Names of the exogenous variables in ``data``.

            Defaults to all non-key columns if not provided.

        model : DataFrame, optional
            The model to be used for prediction.

            Defaults to the fitted model(i.e. self.model\_).

        show_explainer : bool, optional
            If True, the reason code will be returned. Only valid when ``background_size`` is provided during the fit() function.

            Defaults to False
        predict_args : dict, optional
            Specifies estimator-specific parameters passed to the predict method.

            If not None, it must be specified as a dict with one of the following format:

            - `key` for estimator name, and `value` for estimator-specific parameter setting in a dict.
              For example `{'RDT_Classifier':{'block_size': 5}, 'NB_Classifier':{'laplace':1.0}}`.

            Defaults to None(i.e. no estimator-specific predict parameter provided).

        Returns
        -------
        DataFrame
            Predicted result.
        """
        predict_result = self._predict(data=data,
                                       key=key,
                                       features=exog,
                                       model=model,
                                       show_explainer=show_explainer,
                                       predict_args=predict_args)
        setattr(self, "forecast_result", predict_result)
        return predict_result

    def evaluate(self, data,#pylint:disable=arguments-differ, arguments-renamed
                 pipeline=None,
                 key=None,
                 endog=None,
                 exog=None,
                 categorical_variable=None,
                 resampling_method=None,
                 fold_num=None,
                 random_state=None,
                 percentage=None,
                 gap_num=None):
        r"""
        This function is to evaluate the pipeline.

        Parameters
        ----------

        data : DataFrame
            Data for pipeline evaluation.

        pipeline : json str or dict
            Pipeline to be evaluated.

        key : str, optional
            Specifies the column that represents the ordering of the input time-series data.

            If ``data`` is indexed by a single column, then ``key`` defaults
            to that index column; otherwise ``key`` must be specified(i.e. is mandatory).

        endog : str, optional
            Specifies the endogenous variable for time-series data.

            Defaults to the 1st non-key column of  ``data`` .

        exog : str, optional
            Specifies the exogenous variables for time-series data.

            Defaults to all non-key, non-endog columns in ``data``.

        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.
        resampling_method : {'rocv', 'block'}, optional
            The resampling method for pipeline model evaluation.

            Defaults to 'rocv'.

        fold_num : int, optional
            The fold number for cross validation. If the value is 0, the function will automatically determine the fold number.

            Defaults to 5.

        random_state : int, optional
            Specifies the seed for random number generator.

            - 0: Uses the current time (in seconds) as the seed.
            - Others: Uses the specified value as the seed.

        percentage : float, optional
            Percentage between training data and test data. Only applicable when resampling_method is 'block'.

            Defaults to 0.7.

        gap_num : int, optional

            Number of samples to exclude from the end of each train set before the test set.

            Defaults to 0.

        Returns
        -------
        DataFrame
            Scores.
        """
        if pipeline is None:
            pipeline = self.best_pipeline_.filter("ID=0").select("PIPELINE").collect().iat[0, 0]
        self.percentage = self._arg('percentage', percentage, float)
        self.gap_num = self._arg('gap_num', gap_num, int)
        return super(AutomaticTimeSeries, self).evaluate(data, pipeline, key,
                                                         exog, endog,
                                                         categorical_variable,
                                                         resampling_method,
                                                         fold_num,
                                                         random_state)

    def score(self, data, key=None,
              endog=None, exog=None,
              model=None, predict_args=None):
        r"""
        Pipeline model score function.

        Parameters
        ----------
        data : DataFrame
            Data for pipeline model scoring.

        key : str, optional
            Specifies the column that represents the ordering of the input time-series data.

            If ``data`` is indexed by a single column, then ``key`` defaults
            to that index column; otherwise ``key`` must be specified(i.e. is mandatory).

        endog : str, optional
            Specifies the endogenous variable for time-series data.

            Defaults to the 1st non-key column of  ``data`` .

        exog : str, optional
            Specifies the exogenous variables for time-series data.

            Defaults to all non-key, non-endog columns in ``data``.

        model : DataFrame, optional
            The pipeline model used to make predictions.

            Defaults to the fitted pipeline model(i.e. self.model\_).

        predict_args : dict, optional
            Specifies estimator-specific parameters passed to the predict phase of the score method.

            If not None, it must be specified as a dict with one of the following format

            - `key` for estimator name, and `value` for estimator-specific parameter setting in a dict.
              For example `{'RDT_Classifier':{'block_size': 5}, 'NB_Classifier':{'laplace':1.0}}`.

            Defaults to None(i.e. no estimator-specific predict parameter provided).

        Returns
        -------
        DataFrames

            DataFrame 1 : Prediction result for the input data, structured as follows:
            - 1st column, ID of input data.
            - 2nd column, SCORE, forecast value.
            - 3rd column, REASON CODE, feature attributions(currently All NULLs).
            - 4th & 5th column, placeholder columns for future implementations.

            DataFrame 2 : Statistics.
        """
        setattr(self, 'hanaml_score_params', pal_param_register())
        setattr(self, 'testing_data', data)
        return self._score(data=data, key=key,
                           features=exog,
                           label=endog,
                           model=model,
                           random_state=None,
                           top_k_attributions=None,
                           sample_size=None,
                           verbose_output=False,
                           predict_args=predict_args)

    def _get_highlight_metric(self):
        if self.scorings:
            return list(json.loads(self.scorings).keys())[0]
        return 'MAE'

    def reset_config_dict(self, connection_context=None, template_type='default'):
        """
        Reset config dict.

        Parameters
        ----------
        connection_context : ConnectionContext, optional
            If it is set, the default config dict will use the one stored in a SAP HANA instance.

            Defaults to None.
        template_type : {'default', 'light'}, optional
            HANA config dict type.

            Defaults to 'default'.
        """
        if connection_context:
            self.config_dict = _fetch_default_config_dict(connection_context, 'timeseries', template_type)
        else:
            if template_type == 'default':
                template = "config_dict_timeseries_default.json"
            else:
                template = "light_config_dict_timeseries.json"
            config_dict_file = os.path.join(os.path.dirname(__file__), "templates",
                                            template)
            with open(config_dict_file) as input_file:
                self.config_dict = json.dumps(json.load(input_file))

    def build_report(self):
        r"""
        Generate a time series report.
        """
        from hana_ml.visualizers.time_series_report_template_helper import TimeSeriesTemplateReportHelper  #pylint: disable=cylic-import
        if not hasattr(self, 'key') or self.key is None:
            setattr(self, 'key', self.training_data.columns[0])
        if not hasattr(self, 'endog') or self.endog is None:
            setattr(self, 'endog', self.training_data.columns[1])
        if len(self.training_data.columns) > 2:
            if not hasattr(self, 'exog') or self.exog is None:
                setattr(self, 'exog', self.training_data.columns)
                self.exog.remove(self.key)
                self.exog.remove(self.endog)
        self.report = TimeSeriesTemplateReportHelper(self)
        pages = []
        page0 = Page("Best Alternative Pipelines")
        page0.addItems(TimeSeriesExplainer.get_items_from_best_pipeline(self.best_pipeline_, self._get_highlight_metric()))
        pages.append(page0)
        page1 = Page("Forecast Result Analysis")
        tse = TimeSeriesExplainer(key=self.key, endog=self.endog, exog=self.exog)
        tse.add_line_to_comparison_item("Training Data", data=self.training_data, x_name=self.key, y_name=self.endog)
        if hasattr(self, 'forecast_result'):
            if self.forecast_result:
                tse.add_line_to_comparison_item("Forecast Result",
                                                data=self.forecast_result,
                                                x_name=self.forecast_result.columns[0],
                                                y_name=self.forecast_result.columns[-1])
        page1.addItems(tse.get_comparison_item())
        pages.append(page1)
        self.report.add_pages(pages)
        self.report.build_report()

    def generate_html_report(self, filename=None):
        """
        Generate a time series report in the form of an HTML file.
        """
        self.report.generate_html_report(filename)

    def generate_notebook_iframe_report(self):
        """
        Generate a time series report embedded within an iframe.
        """
        self.report.generate_notebook_iframe_report()

class _PipelineWalk:

    def __init__(self, pipeline):
        self.current_walk = pipeline
        self.end = False
        if isinstance(self.current_walk, dict):
            self.current_content = list(self.current_walk.keys())[0]
            self.current_args = list(self.current_walk.values())[0]['args']
        else:
            self.current_content = self.current_walk
            self.current_args = ''
            self.end = True

    def _next(self):
        if 'inputs' in self.current_walk[self.current_content]:
            if 'data' in self.current_walk[self.current_content]['inputs']:
                self.current_walk = self.current_walk[self.current_content]['inputs']['data']
                if isinstance(self.current_walk, dict):
                    self.current_content = list(self.current_walk.keys())[0]
                    self.current_args = list(self.current_walk.values())[0]['args']
                else:
                    self.current_content = self.current_walk
                    self.current_args = ''
                    self.end = True

class Preprocessing(PALBase):
    """
    Preprocessing class. Similar to the function preprocessing.

    Parameters
    ----------
    name : str
        The preprocessing algorithm name. The options are:

        - "OneHotEncoder"
        - "FeatureNormalizer"
        - "KBinsDiscretizer"
        - "Imputer"
        - "Discretize"
        - "MDS"
        - "SMOTE"
        - "SMOTETomek"
        - "TomekLinks"
        - "Sampling"

    **kwargs: dict
        A dict of the keyword args passed to the object.
        Please refer to the documentation of the specific preprocessing algorithm for parameter information.

        - "OneHotEncoder": no additional parameter is required.
        - :class:`~hana_ml.algorithms.pal.preprocessing.FeatureNormalizer`
        - :class:`~hana_ml.algorithms.pal.preprocessing.KBinsDiscretizer`
        - :class:`~hana_ml.algorithms.pal.preprocessing.Imputer`
        - :class:`~hana_ml.algorithms.pal.preprocessing.Discretize`
        - :class:`~hana_ml.algorithms.pal.preprocessing.MDS`
        - :class:`~hana_ml.algorithms.pal.preprocessing.SMOTE`
        - :class:`~hana_ml.algorithms.pal.preprocessing.SMOTETomek`
        - :class:`~hana_ml.algorithms.pal.preprocessing.TomekLinks`
        - :class:`~hana_ml.algorithms.pal.preprocessing.Sampling`

    Examples
    --------

    >>> result = Preprocessing(name="FeatureNormalizer").fit_transform(data=data, key="ID", features=["BMI"])

    """
    def __init__(self, name, **kwargs):
        super(Preprocessing, self).__init__()
        self.name = name
        self.kwargs = {**kwargs}

    def fit_transform(self, data, key=None, features=None, **kwargs):
        """
        Execute the preprocessing algorithm and return the transformed dataset.

        Parameters
        ----------
        data : DataFrame
            Input data.

        key : str, optional
            Name of the ID column.

            Defaults to the index column of ``data`` (i.e. data.index) if it is set.

        features : list, optional
            The columns to be preprocessed.

            Defaults to None.

        **kwargs: dict
            A dict of the keyword args passed to the fit_transform function.
            Please refer to the documentation of the specific preprocessing for parameter information.
        """
        args = {**kwargs}
        if data.index is not None:
            key = data.index
        key_is_none = False
        if key is None:
            key_is_none = True
        if features is None:
            features = data.columns
            if self.name == 'OneHotEncoder':
                features = []
                for col, val in data.get_table_structure().items():
                    if 'VARCHAR' in val.upper():
                        features.append(col)
            if key is not None:
                if key in features:
                    features.remove(key)
        if isinstance(features, str):
            features = [features]
        if self.name != "OneHotEncoder":
            if key is None:
                key = "ID"
                data = data.add_id(key)
            data = data.select([key] + features)
        other = data.deselect(features)
        if self.name == 'FeatureNormalizer':
            if 'method' not in self.kwargs.keys():
                self.kwargs['method'] = "min-max"
                self.kwargs['new_max'] = 1.0
                self.kwargs['new_min'] = 0.0
            transformer = FeatureNormalizer(**self.kwargs)
            result = transformer.fit_transform(data, key, **args)
            self.execute_statement = transformer.execute_statement
        elif self.name == 'KBinsDiscretizer':
            if 'strategy' not in self.kwargs.keys():
                self.kwargs['strategy'] = "uniform_size"
                self.kwargs['smoothing'] = "means"
            transformer = KBinsDiscretizer(**self.kwargs)
            result = transformer.fit_transform(data, key, **args).deselect(["BIN_INDEX"])
            self.execute_statement = transformer.execute_statement
        elif self.name == 'Imputer':
            transformer = Imputer(**self.kwargs)
            result = transformer.fit_transform(data,
                                               key,
                                               **args)
            self.execute_statement = transformer.execute_statement
        elif self.name == 'Discretize':
            if 'strategy' not in self.kwargs.keys():
                self.kwargs['strategy'] = "uniform_size"
                self.kwargs['smoothing'] = "bin_means"
            transformer = Discretize(**self.kwargs)
            if 'binning_variable' not in args.keys():
                args['binning_variable'] = features
            result = transformer.fit_transform(data,
                                               **args)[0]
            self.execute_statement = transformer.execute_statement
        elif self.name == 'MDS':
            if 'matrix_type' not in self.kwargs.keys():
                self.kwargs['matrix_type'] = "observation_feature"
            transformer = MDS(**self.kwargs)
            result = transformer.fit_transform(data, key, **args)
            result = result[0].pivot_table(values='VALUE', index=key, columns='DIMENSION', aggfunc='avg')
            columns = result.columns
            rename_cols = {}
            for col in columns:
                if col != key:
                    rename_cols[col] = "X_" + str(col)
            result = result.rename_columns(rename_cols)
            self.execute_statement = transformer.execute_statement
        elif self.name == 'SMOTE':
            transformer = SMOTE(**self.kwargs)
            result = transformer.fit_transform(data, **args)
            self.execute_statement = transformer.execute_statement
        elif self.name == 'SMOTETomek':
            transformer = SMOTETomek(**self.kwargs)
            result = transformer.fit_transform(data,
                                               **args)
            self.execute_statement = transformer.execute_statement
        elif self.name == 'TomekLinks':
            transformer = TomekLinks(**self.kwargs)
            result = transformer.fit_transform(data,
                                               key=key,
                                               **args)
            self.execute_statement = transformer.execute_statement
        elif self.name == 'Sampling':
            transformer = Sampling(**self.kwargs)
            result = transformer.fit_transform(data, **args)
            self.execute_statement = transformer.execute_statement
        elif self.name == 'PCA':
            transformer = PCA(**self.kwargs)
            result = transformer.fit_transform(data,
                                               key=key,
                                               **args)
            self.execute_statement = transformer.execute_statement
        elif self.name == 'OneHotEncoder':
            others = list(set(data.columns) - set(features))
            query = "SELECT {}".format(", ".join(list(map(quotename, others))))
            if others:
                query = query + ", "
            for feature in features:
                categoricals = data.distinct(feature).collect()[feature].to_list()
                for cat in categoricals:
                    query = query + "CASE WHEN \"{0}\" = '{1}' THEN 1 ELSE 0 END \"{1}_{0}_OneHot\", ".format(feature, cat)
            query = query[:-2] + " FROM ({})".format(data.select_statement)
            self.execute_statement = query
            return data.connection_context.sql(query)
        elif self.name == 'FeatureSelection':
            transformer = FeatureSelection(**self.kwargs)
            result = transformer.fit_transform(data,
                                               key,
                                               **args)
            self.execute_statement = transformer.execute_statement
        else:
            pass
        if features is not None:
            if key is not None:
                result = other.set_index(key).join(result.set_index(key))
        if key_is_none:
            result = result.deselect(key)
        return result
