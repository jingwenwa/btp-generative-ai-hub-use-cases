"""
This module contains auto machine learning API.

The following classes are available:

    * :class:`MassiveAutomaticClassification`
    * :class:`MassiveAutomaticRegression`
    * :class:`MassiveAutomaticTimeSeries`

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
import json
import logging
import os
import uuid
from hdbcli import dbapi
from hana_ml.algorithms.pal.utility import check_pal_function_exist
from hana_ml.ml_exceptions import FitIncompleteError
from hana_ml.ml_base import ListOfStrings
from hana_ml.algorithms.pal.auto_ml import _AutoMLBase
from .pal_base import (
    ParameterTable,
    call_pal_auto_with_hint,
    pal_param_register,
    try_drop
)

logger = logging.getLogger(__name__)#pylint: disable=invalid-name

def _get_group_key(data, group_key):
    return data.select(group_key).distinct().collect()[group_key].tolist()

def _deep_update_for_dict(base, update):
    if update is None:
        return base
    result = base.copy()
    for kkey, vval in update.items():
        if kkey in base:
            result[kkey].update(vval)
        else:
            result.update({kkey: vval})
    return result

def _format_group_pipelines(group_pipelines):
    group_pipelines_formatted = {}
    for group_id, group_pipeline in group_pipelines.items():
        if isinstance(group_pipeline, dict):
            group_pipelines_formatted = _deep_update_for_dict(group_pipelines_formatted, {group_id: {"pipeline": json.dumps(group_pipeline)}})
        else:
            group_pipelines_formatted = _deep_update_for_dict(group_pipelines_formatted, {group_id: {"pipeline": group_pipeline}})
    return group_pipelines_formatted

def _update_config_dict_with_lag(config_dict, lag=None, lag_features=None):
    result_config_dict = config_dict
    if lag:
        if isinstance(lag, (int, list)):
            param_config = lag
            if isinstance(lag, int):
                param_config = [lag]
            result_config_dict = _update_config_dict(
                result_config_dict,
                operator_name='HGBT_TimeSeries',
                param_name='LAG',
                param_config=param_config)
            result_config_dict = _update_config_dict(
                result_config_dict,
                operator_name='MLR_TimeSeries',
                param_name='LAG',
                param_config=param_config)
        if isinstance(lag, dict):
            operator_name = next(iter(lag))
            param_config = lag[operator_name]
            if isinstance(param_config, int):
                param_config = [param_config]
            result_config_dict = _update_config_dict(
                result_config_dict,
                operator_name=operator_name,
                param_name='LAG',
                param_config=param_config)
    if lag_features:
        if isinstance(lag_features, (str, list)):
            param_config = lag_features
            if isinstance(lag_features, str):
                param_config = [lag_features]
            result_config_dict = _update_config_dict(
                result_config_dict,
                operator_name='HGBT_TimeSeries',
                param_name='LAG_FEATURES',
                param_config=param_config)
            result_config_dict = _update_config_dict(
                result_config_dict,
                operator_name='MLR_TimeSeries',
                param_name='LAG_FEATURES',
                param_config=param_config)
        if isinstance(lag_features, dict):
            operator_name = next(iter(lag_features))
            param_config = lag_features[operator_name]
            if isinstance(param_config, str):
                param_config = [param_config]
            result_config_dict = _update_config_dict(
                result_config_dict,
                operator_name=operator_name,
                param_name='LAG_FEATURES',
                param_config=param_config)
    return result_config_dict

def _update_config_dict(config_dict, operator_name, param_name=None, param_config=None):
    def _operator_in_config(operator_name, config_dict): #pylint: disable=inconsistent-return-statements
        if operator_name in config_dict:
            return config_dict[operator_name]
    config_dict = json.loads(config_dict)
    get_operator_dict = _operator_in_config(operator_name, config_dict)
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
    return json.dumps(config_dict)

class _MassiveAutoMLBase(_AutoMLBase):
    def __init__(self,
                 pipeline_type,
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
                 fold_num=None,
                 resampling_method=None,
                 max_eval_time_mins=None,
                 percentage=None,
                 gap_num=None,
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
                 max_resource=None,
                 special_group_id="PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID2",
                 progress_indicator_id=None):
        super(_MassiveAutoMLBase, self).__init__()

        self._special_group_id = special_group_id
        self._group_params = {}

        self.pipeline_type = self._arg('pipeline_type', pipeline_type, str)
        self._group_params = _deep_update_for_dict(self._group_params, {self._special_group_id: {'pipeline_type': self.pipeline_type}})

        self.scorings = self._arg('scorings', scorings, dict)
        if self.scorings:
            self.scorings = json.dumps(self.scorings)
            self._group_params = _deep_update_for_dict(self._group_params, {self._special_group_id: {'scorings': self.scorings}})

        self.generations = self._arg('generations', generations, int)
        if self.generations:
            self._group_params = _deep_update_for_dict(self._group_params, {self._special_group_id: {'generations': self.generations}})

        self.population_size = self._arg('population_size', population_size, int)
        if self.population_size:
            self._group_params = _deep_update_for_dict(self._group_params, {self._special_group_id: {'population_size': self.population_size}})

        self.offspring_size = self._arg('offspring_size', offspring_size, int)
        if self.offspring_size:
            self._group_params = _deep_update_for_dict(self._group_params, {self._special_group_id: {'offspring_size': self.offspring_size}})

        self.elite_number = self._arg('elite_number', elite_number, int)
        if self.elite_number:
            self._group_params = _deep_update_for_dict(self._group_params, {self._special_group_id: {'elite_number': self.elite_number}})

        self.min_layer = self._arg('min_layer', min_layer, int)
        if self.min_layer:
            self._group_params = _deep_update_for_dict(self._group_params, {self._special_group_id: {'min_layer': self.min_layer}})

        self.max_layer = self._arg('max_layer', max_layer, int)
        if self.max_layer:
            self._group_params = _deep_update_for_dict(self._group_params, {self._special_group_id: {'max_layer': self.max_layer}})

        self.mutation_rate = self._arg('mutation_rate', mutation_rate, float)
        if self.mutation_rate:
            self._group_params = _deep_update_for_dict(self._group_params, {self._special_group_id: {'mutation_rate': self.mutation_rate}})

        self.crossover_rate = self._arg('crossover_rate', crossover_rate, float)
        if self.crossover_rate:
            self._group_params = _deep_update_for_dict(self._group_params, {self._special_group_id: {'crossover_rate': self.crossover_rate}})

        self.random_seed = self._arg('random_seed', random_seed, int)
        if self.random_seed:
            self._group_params = _deep_update_for_dict(self._group_params, {self._special_group_id: {'random_seed': self.random_seed}})

        self.config_dict = self._arg('config_dict', config_dict, (str, dict))
        if isinstance(self.config_dict, dict):
            self.config_dict = json.dumps(self.config_dict)
        if (self.config_dict is None) or (self.config_dict =='default'):
            if pipeline_type == 'classifier':
                temp_file_name = "config_dict_classification_default.json"
            elif pipeline_type == 'regressor':
                temp_file_name = "config_dict_regression_default.json"
            elif pipeline_type == 'timeseries':
                temp_file_name = "config_dict_timeseries_default.json"
            config_dict_file =os.path.join(os.path.dirname(__file__), "templates",
                                            temp_file_name)
            with open(config_dict_file) as input_file:
                self.config_dict = json.dumps(json.load(input_file))
        if self.config_dict == 'light':
            if pipeline_type == 'classifier':
                temp_file_name = "light_config_dict_classification.json"
            elif pipeline_type == 'regressor':
                temp_file_name = "light_config_dict_regression.json"
            elif pipeline_type == 'timeseries':
                temp_file_name = "light_config_dict_timeseries.json"
            config_dict_file = os.path.join(os.path.dirname(__file__), "templates",
                                            temp_file_name)
            with open(config_dict_file) as input_file:
                self.config_dict = json.dumps(json.load(input_file))
        if self.config_dict:
            self._group_params = _deep_update_for_dict(self._group_params, {self._special_group_id: {'config_dict': self.config_dict}})

        self.fold_num = self._arg('fold_num', fold_num, int)
        if self.fold_num:
            self._group_params = _deep_update_for_dict(self._group_params, {self._special_group_id: {'fold_num': self.fold_num}})

        resampling_map = {'rocv': 1, 'block': 2, 'simple_split': 3, 'cv': 'cv', 'stratified_cv': 'stratified_cv'}
        self.resampling_method = self._arg('resampling_method', resampling_method, resampling_map)
        if self.resampling_method:
            self._group_params = _deep_update_for_dict(self._group_params, {self._special_group_id: {'resampling_method': self.resampling_method}})

        self.max_eval_time_mins = self._arg('max_eval_time_mins', max_eval_time_mins, float)
        if self.max_eval_time_mins:
            self._group_params = _deep_update_for_dict(self._group_params, {self._special_group_id: {'max_eval_time_mins': float(self.max_eval_time_mins)}})

        self.early_stop = self._arg('early_stop', early_stop, int)
        if self.early_stop:
            self._group_params = _deep_update_for_dict(self._group_params, {self._special_group_id: {'early_stop': self.early_stop}})

        self.percentage = self._arg('percentage', percentage, float)
        if self.percentage:
            self._group_params = _deep_update_for_dict(self._group_params, {self._special_group_id: {'percentage': self.percentage}})

        self.gap_num = self._arg('gap_num', gap_num, int)
        if self.gap_num:
            self._group_params = _deep_update_for_dict(self._group_params, {self._special_group_id: {'gap_num': self.gap_num}})

        self.successive_halving = self._arg('successive_halving', successive_halving, bool)
        if self.successive_halving:
            self._group_params = _deep_update_for_dict(self._group_params, {self._special_group_id: {'successive_halving': self.successive_halving}})

        self.min_budget = self._arg('min_budget', min_budget, int)
        if self.min_budget:
            self._group_params = _deep_update_for_dict(self._group_params, {self._special_group_id: {'min_budget': self.min_budget}})

        self.max_budget = self._arg('max_budget', max_budget, int)
        if self.max_budget:
            self._group_params = _deep_update_for_dict(self._group_params, {self._special_group_id: {'max_budget': self.max_budget}})

        self.min_individuals = self._arg('min_individuals', min_individuals, int)
        if self.min_individuals:
            self._group_params = _deep_update_for_dict(self._group_params, {self._special_group_id: {'min_individuals': self.min_individuals}})

        self.connections = self._arg('connections', connections, (str, dict))
        if isinstance(self.connections, dict):
            self.connections = json.dumps(self.connections)
        if self.connections:
            self._group_params = _deep_update_for_dict(self._group_params, {self._special_group_id: {'connections': self.connections}})

        self.alpha = self._arg('alpha', alpha, float)
        if self.alpha:
            self._group_params = _deep_update_for_dict(self._group_params, {self._special_group_id: {'alpha': self.alpha}})

        self.delta = self._arg('delta', delta, float)
        if self.delta:
            self._group_params = _deep_update_for_dict(self._group_params, {self._special_group_id: {'delta': self.delta}})

        self.top_k_connections = self._arg('top_k_connections', top_k_connections, int)
        if self.top_k_connections:
            self._group_params = _deep_update_for_dict(self._group_params, {self._special_group_id: {'top_k_connections': self.top_k_connections}})

        self.top_k_pipelines = self._arg('top_k_pipelines', top_k_pipelines, int)
        if self.top_k_pipelines:
            self._group_params = _deep_update_for_dict(self._group_params, {self._special_group_id: {'top_k_pipelines': self.top_k_pipelines}})

        self.search_method = self._arg('search_method', search_method, str)
        if self.search_method:
            self._group_params = _deep_update_for_dict(self._group_params, {self._special_group_id: {'search_method': self.search_method}})

        self.fine_tune_pipeline = self._arg('fine_tune_pipeline',
                                            fine_tune_pipeline, bool)
        if self.fine_tune_pipeline:
            self._group_params = _deep_update_for_dict(self._group_params, {self._special_group_id: {'fine_tune_pipeline': self.fine_tune_pipeline}})

        self.fine_tune_resource = self._arg('fine_tune_resource',
                                            fine_tune_resource, int)
        if self.fine_tune_resource:
            self._group_params = _deep_update_for_dict(self._group_params, {self._special_group_id: {'fine_tune_resource': self.fine_tune_resource}})
        self.with_hyperband = self._arg('with_hyperband', with_hyperband, bool)
        if self.with_hyperband:
            self._group_params = _deep_update_for_dict(self._group_params, {self._special_group_id: {'with_hyperband': self.with_hyperband}})
        self.reduction_rate = self._arg('reduction_rate', reduction_rate, float)
        if self.reduction_rate:
            self._group_params = _deep_update_for_dict(self._group_params, {self._special_group_id: {'reduction_rate': self.reduction_rate}})
        self.min_resource = self._arg('min_resource', min_resource, int)
        if self.min_resource:
            self._group_params = _deep_update_for_dict(self._group_params, {self._special_group_id: {'min_resource': min_resource}})
        self.max_resource = self._arg('max_resource', max_resource, int)
        if self.max_resource:
            self._group_params = _deep_update_for_dict(self._group_params, {self._special_group_id: {'max_resource': max_resource}})
        self.progress_indicator_id = self._arg('progress_indicator_id', progress_indicator_id, str)

        self.__enable_workload_class = False
        self.used_workload_class_name = None

        self.progress_indicator_cleanup = None
        self.__retention_period = 365
        self._status = 0
        self.execution_ids = []
        self._group_keys_for_execution_ids = []
        self.__disable_log_cleanup = False
        self._highlight_metric = None

    def persist_progress_log(self):
        """
        Persist the progress log.
        """
        self.progress_indicator_cleanup = 0

    def _auto_sql_content_cleanup(self, connection_context, execution_id, is_force):
        has_log = connection_context.sql("SELECT COUNT(*) FROM {}.AUTOML_LOG WHERE EXECUTION_ID = '{}'".format(self.auto_sql_content_schema, execution_id)).collect().iat[0, 0]
        if has_log > 0:
            connection_context.execute_sql("DO BEGIN\nCALL {}.REMOVE_AUTOML_LOG('{}', TO_BOOLEAN({}), info);\nEND;".format(self.auto_sql_content_schema, execution_id, 1 if is_force else 0))

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

    def disable_log_cleanup(self, disable=True):
        """
        Disable the log clean up.
        """
        self.__disable_log_cleanup = disable

    def _progress_table_cleanup(self, connection_context, execution_id):
        if not self.__disable_log_cleanup:
            self._exist_auto_sql_content_log(connection_context)
            if self._use_auto_sql_content:
                try:
                    self._auto_sql_content_cleanup(connection_context, execution_id, True)
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
                    has_log = connection_context.sql("SELECT COUNT(*) FROM _SYS_AFL.FUNCTION_PROGRESS_IN_AFLPAL WHERE EXECUTION_ID = '{}'".format(execution_id)).collect().iat[0, 0]
                    if has_log > 0:
                        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
                        info_tbl  = f'#PAL_CLEANUP_PROGRESS_LOG_INFO_TBL_{self.id}_{unique_id}'
                        param_rows = [('PROGRESS_INDICATOR_ID', None, None, execution_id)]
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

    def _format_group_params(self, group_params):
        def _ts_param_map(param_name):
            if param_name.lower() == 'resampling_method':
                return 'SPLIT_METHOD'
            return param_name.upper()
        def _param_map(param_name):
            return param_name.upper()
        param_rows = []
        for group_id, group_item in group_params.items():
            for param_name, param_value in group_item.items():
                if self.pipeline_type == 'timeseries':
                    if param_name.upper() == 'SUCCESSIVE_HALVING':
                        param_value = 0
                if isinstance(param_value, dict):
                    param_value = json.dumps(param_value)
                if isinstance(param_value, (int, bool)):
                    if self.pipeline_type == 'timeseries':
                        param_rows.extend([(group_id, _ts_param_map(param_name), param_value , None, None)])
                    else:
                        param_rows.extend([(group_id, _param_map(param_name), param_value, None, None)])
                elif isinstance(param_value, float):
                    if self.pipeline_type == 'timeseries':
                        param_rows.extend([(group_id, _ts_param_map(param_name), None, param_value, None)])
                    else:
                        param_rows.extend([(group_id, _param_map(param_name), None, param_value, None)])
                elif isinstance(param_value, (list, tuple)):
                    if self.pipeline_type == 'timeseries':
                        param_rows.extend([(group_id, _ts_param_map(param_name), None, None, var) for var in param_value])
                    else:
                        param_rows.extend([(group_id, _param_map(param_name), None, None, var) for var in param_value])
                else:
                    if self.pipeline_type == 'timeseries':
                        param_rows.extend([(group_id, _ts_param_map(param_name), None, None, param_value)])
                    else:
                        param_rows.extend([(group_id, _param_map(param_name), None, None, param_value)])
        return param_rows

    def get_best_pipelines(self):
        """
        Return the best pipeline.
        """
        best_pipeline_pf = None
        group_best_pipeline = {}
        if hasattr(self, 'best_pipeline_'):
            if self.best_pipeline_:
                best_pipeline_pf = self.best_pipeline_.collect()
            else:
                best_pipeline_pf = self.model_[1].collect()
        else:
            best_pipeline_pf = self.model_[1].collect()
        for row in best_pipeline_pf.itertuples(index=False):
            if row[1] == 0:
                group_best_pipeline[row[0]] = row[2]
        return group_best_pipeline

    def get_best_pipeline(self):
        """
        Return the best pipeline.
        """
        return self.get_best_pipelines()

    def get_config_dict(self):
        """
        Return the config_dict.
        """
        return self.config_dict

    def get_optimal_config_dict(self):
        """
        Return the optimal config_dict. Only available when connections is used.
        """
        if self.connections:
            if hasattr(self, 'info_'):
                if self.info_:
                    result = {}
                    for row in self.info_.filter("STAT_NAME='optimal_config'").collect()[[self.info_.columns[0], self.info_.columns[2]]].itertuples(index=False):
                        result[row[0]] = row[1]
                    return result
        return None

    def get_optimal_connections(self):
        """
        Return the optimal connections. Only available when connections is used.
        """
        if self.connections:
            if hasattr(self, 'info_'):
                if self.info_:
                    result = {}
                    for row in self.info_.filter("STAT_NAME='optimal_connections'").collect()[[self.info_.columns[0], self.info_.columns[2]]].itertuples(index=False):
                        result[row[0]] = row[1]
                    return result
        return None

    def make_future_dataframe(self, data=None, key=None, group_key=None, periods=1, increment_type='seconds'):
        """
        Create a new dataframe for time series prediction.

        Parameters
        ----------
        data : DataFrame, optional
            The training data contains the index.

            Defaults to the data used in the fit().

        key : str, optional
            The index defined in the training data.

            Defaults to the specified key in fit() or the value in data.index or the PAL's default key column position.

        group_key : str, optional
            Specify the group id column.

            This parameter is only valid when ``massive`` is True.

            Defaults to the specified group_key in fit() or the first column of the dataframe.

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
        if group_key is None:
            if hasattr(self, "group_key"):
                if self.group_key is None:
                    group_key = data.columns[0]
                else:
                    group_key = self.group_key
            else:
                group_key = data.columns[0]
        if key is None:
            if data.index is None:
                if hasattr(self, "key"):
                    if self.key is None:
                        key = data.columns[1]
                    else:
                        key = self.key
                else:
                    key = data.columns[1]
            else:
                key = data.index
        group_id_type = data.get_table_structure()[group_key]
        group_list = data.select(group_key).distinct().collect()[group_key]
        timeframe = []
        for group in group_list:
            if 'INT' in group_id_type.upper():
                m_data = data.filter(f"{group_key}={group}")
            else:
                m_data = data.filter(f"{group_key}='{group}'")
            max_ = m_data.select(key).max()
            sec_max_ = m_data.select(key).distinct().sort_values(key, ascending=False).head(2).collect().iat[1, 0]
            delta = max_ - sec_max_
            is_int = 'INT' in m_data.get_table_structure()[key]
            if is_int:
                forecast_start, timedelta = max_ + delta, delta
            else:
                forecast_start, timedelta = max_ + delta, delta.total_seconds()

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

            increment_type = increment_type.upper()
            for period in range(0, periods):
                if 'INT' in group_id_type.upper():
                    if is_int:
                        timeframe.append(f"SELECT {group} AS \"{group_key}\", TO_INT({forecast_start} + {timedelta} * {period}) AS \"{key}\" FROM DUMMY")
                    else:
                        timeframe.append(f"SELECT {group} AS \"{group_key}\", ADD_{increment_type}('{forecast_start}', {timedelta} * {period}) AS \"{key}\" FROM DUMMY")
                else:
                    if is_int:
                        timeframe.append(f"SELECT '{group}' AS \"{group_key}\", TO_INT({forecast_start} + {timedelta} * {period}) AS \"{key}\" FROM DUMMY")
                    else:
                        timeframe.append(f"SELECT '{group}' AS \"{group_key}\", ADD_{increment_type}('{forecast_start}', {timedelta} * {period}) AS \"{key}\" FROM DUMMY")
        sql = ' UNION ALL '.join(timeframe)

        return data.connection_context.sql(sql).sort_values([group_key, key])

    def enable_workload_class(self, workload_class_name):
        """
        HANA WORKLOAD CLASS is recommended to set when the MassiveAutomaticClassification/MassiveAutomaticRegression/MassiveAutomaticTimeSeries is called.

        Parameters
        ----------
        workload_class_name : str
            The name of HANA WORKLOAD CLASS.

        """
        self.apply_with_hint('WORKLOAD_CLASS("{}")'.format(workload_class_name), True)
        self.__enable_workload_class = True
        self.used_workload_class_name = workload_class_name

    def disable_workload_class_check(self):
        """
        Disable the workload class check. Please note that the MassiveAutomaticClassification/MassiveAutomaticRegression/MassiveAutomaticTimeSeries may cause large resource.
        Without setting workload class, there's no resource restriction on the training process.

        """
        self.__enable_workload_class = True
        self.used_workload_class_name = None

    def get_workload_classes(self, connection_context):
        """
        Return the available workload classes information.

        Parameters
        ----------
        connection_context : str, optional
            The connection to a SAP HANA instance.

        """
        return connection_context.sql("SELECT * FROM WORKLOAD_CLASSES").collect()

    def _fit(self,
             data,
             group_key,
             key=None,
             features=None,
             label=None,
             group_pipelines=None,
             categorical_variable=None,
             background_size=None,
             background_sampling_seed=None,
             model_table_name=None,
             use_explain=None,
             explain_method=None,
             group_params=None):
        fit_group_params = {}
        if group_pipelines is None:
            if not self.__enable_workload_class:
                raise FitIncompleteError("Please define the workload class and call the enable_workload_class() method first!")
        conn = data.connection_context
        self._group_keys_for_execution_ids = []
        self.execution_ids = []
        if self.progress_indicator_id:
            self._group_keys_for_execution_ids = _get_group_key(data, group_key)
            self.execution_ids = list(map(lambda x: x + '_' + self.progress_indicator_id, self._group_keys_for_execution_ids))
            grouped_execution_ids = {}
            for g_key, execution_id in zip(self._group_keys_for_execution_ids, self.execution_ids):
                grouped_execution_ids[g_key] = {"execution_id": execution_id}
            fit_group_params = _deep_update_for_dict(fit_group_params, grouped_execution_ids)
        else:
            if group_params:
                for group_id, group_item in group_params.items():
                    if 'execution_id' in group_item:
                        self._group_keys_for_execution_ids.append(group_id)
                        self.execution_ids.append(group_item['execution_id'])
                    if 'progress_indicator_id' in group_item:
                        self._group_keys_for_execution_ids.append(group_id)
                        self.execution_ids.append(group_item['progress_indicator_id'])
                        group_params[group_id]["execution_id"] = group_item['progress_indicator_id']
                        group_params[group_id].pop('progress_indicator_id')
        highlight_metric = {}
        if self.scorings:
            for g_k in self._group_keys_for_execution_ids:
                highlight_metric[g_k] = list(json.loads(self.scorings).keys())[0]
        else:
            default_score = None
            if isinstance(self, MassiveAutomaticClassification):
                default_score = 'AUC'
            elif isinstance(self, MassiveAutomaticRegression):
                default_score = 'MAE'
            else:
                default_score = 'MAE'
            for g_k in self._group_keys_for_execution_ids:
                if g_k in group_params:
                    if 'scorings' in group_params[g_k]:
                        highlight_metric[g_k] = list(json.loads(group_params[g_k]['scorings']).keys())[0]
                    else:
                        highlight_metric[g_k] = default_score
                else:
                    highlight_metric[g_k] = default_score
        setattr(self, '_highlight_metric', highlight_metric)
        if hasattr(self, "progress_monitor_config") and self.execution_ids:
            self.progress_monitor_config.set_execution_ids(self.execution_ids, self._group_keys_for_execution_ids)
            self.progress_monitor_config.set_executed_connection_id(conn.get_connection_id())
            self.progress_monitor_config.set_highlight_metric(highlight_metric)
            self.progress_monitor_config.start_progress_status_monitor()
        group_key_type = "VARCHAR(5000)"
        #pylint:disable=attribute-defined-outside-init
        use_explain = self._arg('use_explain', use_explain, bool)
        if use_explain:
            fit_group_params = _deep_update_for_dict(fit_group_params, {self._special_group_id: {'use_explain': use_explain}})

        explain_method = self._arg('explain_method', explain_method,
                                   {'kernelshap' : 0, 'globalsurrogate' : 1})
        if explain_method:
            fit_group_params = _deep_update_for_dict(fit_group_params, {self._special_group_id: {'explain_method': explain_method}})

        cols = data.columns
        cols.remove(group_key)
        #has_id input is process here
        if key is not None:
            id_col = [key]
            cols.remove(key)
        else:
            id_col = []
        if label is None:
            if self.pipeline_type == 'timeseries':
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
        if categorical_variable:
            fit_group_params = _deep_update_for_dict(fit_group_params, {self._special_group_id: {'categorical_variable': categorical_variable}})

        #n_features = len(features)
        #Generate a temp view of the data so that label is always in the final column
        data_ = None
        if self.pipeline_type == 'timeseries':
            data_ = data[[group_key] + id_col + [label] + features]
        else:
            data_ = data[[group_key] + id_col + features + [label]]
        if group_pipelines is None:
            if self.__enable_workload_class:
                if  self.used_workload_class_name:
                    if not self._disable_hana_execution:
                        if self.used_workload_class_name not in self.get_workload_classes(conn)["WORKLOAD_CLASS_NAME"].to_list():
                            logger.warning("The workload class name cannot be found in the HANA settings.")
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        outputs = ['BEST_PIPELINE', 'MODEL', 'INFO', 'ERROR']
        outputs = ['#PAL_MASSIVE_AUTOML_{}_TBL_{}_{}'.format(name, self.id, unique_id)
                   for name in outputs]
        if model_table_name:
            outputs[1] = model_table_name
        best_pipeline_tbl, model_tbl, info_tbl, error_tbl = outputs
        if key:
            fit_group_params = _deep_update_for_dict(fit_group_params, {self._special_group_id: {'HAS_ID': 1}})

        if self.progress_indicator_cleanup == 0:
            fit_group_params = _deep_update_for_dict(fit_group_params, {self._special_group_id: {'RETENTION_PERIOD': self.__retention_period}})
            fit_group_params = _deep_update_for_dict(fit_group_params, {self._special_group_id: {'PROGRESS_INDICATOR_CLEANUP': self.progress_indicator_cleanup}})

        setattr(self, 'fit_data', data_)
        try:
            if group_pipelines:
                group_pipelines_formatted = _format_group_pipelines(group_pipelines)
                fit_group_params = _deep_update_for_dict(fit_group_params, group_pipelines_formatted)
                if background_size:
                    fit_group_params = _deep_update_for_dict(fit_group_params, {self._special_group_id: {'background_size': background_size}})
                if background_sampling_seed:
                    fit_group_params = _deep_update_for_dict(fit_group_params, {self._special_group_id: {'background_sampling_seed': background_sampling_seed}})
                if use_explain:
                    fit_group_params = _deep_update_for_dict(fit_group_params, {self._special_group_id: {'use_explain': use_explain}})
                if explain_method:
                    fit_group_params = _deep_update_for_dict(fit_group_params, {self._special_group_id: {'explain_method': explain_method}})

                pipeline_outputs = outputs[1:]
                fit_group_params = _deep_update_for_dict(fit_group_params, self._group_params)
                fit_group_params = _deep_update_for_dict(fit_group_params, group_params)
                self._call_pal_auto(conn,
                                    'PAL_MASSIVE_PIPELINE_FIT',
                                    data_,
                                    ParameterTable(itype=group_key_type).with_data(self._format_group_params(fit_group_params)),
                                    *pipeline_outputs)
            else:
                fit_group_params = _deep_update_for_dict(fit_group_params, self._group_params)
                fit_group_params = _deep_update_for_dict(fit_group_params, group_params)
                self._call_pal_auto(conn,
                                    'PAL_MASSIVE_AUTOML_FIT',
                                    data_,
                                    ParameterTable(itype=group_key_type).with_data(self._format_group_params(fit_group_params)),
                                    *outputs)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            self._status = -1
            if group_pipelines:
                try_drop(conn, pipeline_outputs)
            else:
                try_drop(conn, outputs)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            self._status = -1
            if group_pipelines:
                try_drop(conn, pipeline_outputs)
            else:
                try_drop(conn, outputs)
            raise
        # pylint: disable=attribute-defined-outside-init
        if group_pipelines is None:
            self.best_pipeline_ = conn.table(best_pipeline_tbl)
            self.model_ = [conn.table(model_tbl),
                           self.best_pipeline_]
        else:
            self.model_ = conn.table(model_tbl)
        self.info_ = conn.table(info_tbl)
        self.error_ = conn.table(error_tbl)
        self._status = 1
        if not self._disable_hana_execution:
            if self.error_.count() > 0:
                logger.warning("There are errors in the fit process. Please check the error table: error_ for more information.")

    def _predict(self,
                 data,
                 group_key,
                 key=None,
                 features=None,
                 model=None,
                 show_explainer=False,
                 top_k_attributions=None,
                 random_state=None,
                 sample_size=None,
                 verbose_output=None,
                 predict_args=None,
                 group_params=None):
        conn = data.connection_context
        group_key_type = "VARCHAR(5000)"
        predict_group_params = {}
        if model is None:
            if getattr(self, 'model_') is None:
                raise FitIncompleteError()
            model = self.model_ if not isinstance(self.model_, list) else self.model_[0]
        index = data.index
        if isinstance(index, str):
            if key is not None and index != key:
                msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(msg)
        key = self._arg('key', key, str, required=not isinstance(index, str))
        key = index if key is None else key
        cols = data.columns
        cols.remove(group_key)
        cols.remove(key)
        if features is None:
            features = cols

        random_state = self._arg('random_state', random_state, int)
        if random_state:
            predict_group_params = _deep_update_for_dict(predict_group_params, {self._special_group_id: {'SEED': random_state}})

        top_k_attributions = self._arg('top_k_attributions',
                                       top_k_attributions, int)
        if top_k_attributions:
            predict_group_params = _deep_update_for_dict(predict_group_params, {self._special_group_id: {'TOP_K_ATTRIBUTIONS': top_k_attributions}})

        sample_size = self._arg('sample_size', sample_size, int)
        if sample_size:
            predict_group_params = _deep_update_for_dict(predict_group_params, {self._special_group_id: {'SAMPLESIZE': sample_size}})

        verbose_output = self._arg('verbose_output', verbose_output, bool)
        if verbose_output:
            predict_group_params = _deep_update_for_dict(predict_group_params, {self._special_group_id: {'VERBOSE_OUTPUT': verbose_output}})

        predict_args = self._arg('predict_args', predict_args, dict)
        predict_args_json, no_valid_args = self._gen_predict_args_json(predict_args)
        if no_valid_args > 0:
            predict_group_params = _deep_update_for_dict(predict_group_params, {self._special_group_id: {'PREDICT_ARGS': predict_args_json}})

        data_ = data[[group_key] + [key] + features]
        setattr(self, 'predict_data', data_)
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        outputs = ['RESULT', 'INFO', 'ERROR']
        outputs = ['#PAL_MASSIVE_AUTOML_{}_RESULT_TBL_{}_{}'.format(name, self.id, unique_id)
                   for name in outputs]
        result_tbl, info_tbl, error_tbl = outputs
        calling_function = 'PAL_MASSIVE_PIPELINE_PREDICT'

        if show_explainer and self.pipeline_type != 'timeseries':
            calling_function = 'PAL_MASSIVE_PIPELINE_EXPLAIN'#not implemented for time-series data
            if top_k_attributions:
                predict_group_params = _deep_update_for_dict(predict_group_params, {self._special_group_id: {'TOP_K_ATTRIBUTIONS': top_k_attributions}})
            if random_state:
                predict_group_params = _deep_update_for_dict(predict_group_params, {self._special_group_id: {'SEED': random_state}})
            if sample_size:
                predict_group_params = _deep_update_for_dict(predict_group_params, {self._special_group_id: {'SAMPLESIZE': sample_size}})
            if verbose_output:
                predict_group_params = _deep_update_for_dict(predict_group_params, {self._special_group_id: {'VERBOSE_OUTPUT': verbose_output}})
        predict_group_params = _deep_update_for_dict(predict_group_params, group_params)
        try:
            self._call_pal_auto(conn,
                                calling_function,
                                data_,
                                model,
                                ParameterTable(itype=group_key_type).with_data(self._format_group_params(predict_group_params)),
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
        self.error_ = conn.table(error_tbl)
        if not self._disable_hana_execution:
            if self.error_.count() > 0:
                logger.warning("There are errors in the fit process. Please check the error table: error_ for more information.")
        return result_df

    def _score(self,
               data,
               group_key,
               key=None,
               features=None,
               label=None,
               model=None,
               random_state=None,
               top_k_attributions=None,
               sample_size=None,
               verbose_output=None,
               predict_args=None,
               group_params=None):
        group_key_type = "VARCHAR(5000)"
        conn = data.connection_context
        score_group_params = {}
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
        cols.remove(group_key)
        if key is not None:
            id_col = [key]
            cols.remove(key)
        else:
            id_col = []
        if label is None:
            label = cols[0] if self.pipeline_type=='timeseries' else cols[-1]
        cols.remove(label)
        if features is None:
            features = cols
        if self.pipeline_type=='timeseries':
            data_ = data[[group_key] + id_col + [label] + features]
        else:
            data_ = data[[group_key] + id_col + features + [label]]

        random_state = self._arg('random_state', random_state, int)
        if random_state:
            score_group_params = _deep_update_for_dict(score_group_params, {self._special_group_id: {'SEED': random_state}})

        top_k_attributions = self._arg('top_k_attributions',
                                       top_k_attributions, int)
        if top_k_attributions:
            score_group_params = _deep_update_for_dict(score_group_params, {self._special_group_id: {'TOP_K_ATTRIBUTIONS': top_k_attributions}})

        sample_size = self._arg('sample_size', sample_size, int)
        if sample_size:
            score_group_params = _deep_update_for_dict(score_group_params, {self._special_group_id: {'SAMPLESIZE': sample_size}})

        verbose_output = self._arg('verbose_output', verbose_output, bool)
        if verbose_output:
            score_group_params = _deep_update_for_dict(score_group_params, {self._special_group_id: {'VERBOSE_OUTPUT': verbose_output}})

        predict_args = self._arg('predict_args', predict_args, dict)
        predict_args_json, no_valid_args = self._gen_predict_args_json(predict_args)
        if no_valid_args > 0:
            score_group_params = _deep_update_for_dict(score_group_params, {self._special_group_id: {'PREDICT_ARGS': predict_args_json}})

        score_group_params = _deep_update_for_dict(score_group_params, group_params)
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        tbls = ['RESULT', 'STATS', 'PH1', 'PH2', 'ERROR']
        result_tbls = [f'#PAL_PIPELINE_SCORE_{tb}_TBL_{self.id}_{unique_id}' for tb in tbls]
        try:
            self._call_pal_auto(conn,
                                'PAL_MASSIVE_PIPELINE_SCORE',
                                data_,
                                model,
                                ParameterTable(itype=group_key_type).with_data(self._format_group_params(score_group_params)),
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
        setattr(self, 'error_', conn.table(result_tbls[-1]))
        return tuple(conn.table(tb) for tb in result_tbls[:2])


class MassiveAutomaticClassification(_MassiveAutoMLBase):
    """
    MassiveAutomaticClassification offers an intelligent search amongst machine learning pipelines for supervised classification tasks in a massive mode.
    Each machine learning pipeline contains several operators such as preprocessors, supervised classification models, and transformers
    that follow the API of hana-ml algorithms.

    For MassiveAutomaticClassification parameter mappings of hana_ml and HANA PAL, please refer to the doc page: :ref:`param_mapping`.

    Parameters
    ----------
    scorings : dict, optional
        MassiveAutomaticClassification supports multi-objective optimization with specified weights for each target.
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

        - When ``search_method`` takes the value of 'GA', ``population_size`` is the number of individuals in each generation in the genetic programming algorithm. Having too few individuals can limit the possibilities of crossover and exploration of the search space to only a small portion. Conversely, if there are too many individuals, the performance of the genetic algorithm may slow down.
        - When ``search_method`` takes the value of 'random', ``population_size`` is the number of pipelines randomly generated and evaluated in random search.

        Defaults to 20.

    offspring_size : int, optional
        The number of offsprings to produce in each generation.

        It controls the number of new individuals generated in each iteration by genetic operations, from the population.

        Defaults to the size of ``population_size``.

    elite_number : int, optional
        The number of elites to produce in each generation.

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
        Specifies the seed for the random number generator. Use system time if not provided.

        No default value.

    config_dict : str or dict, optional
        The customized configuration for the searching space.

        - {'light', 'default'}: use provided config_dict templates.
        - JSON format config_dict. It could be a JSON string or dict.

        If it is None, the default config_dict will be used.

        Defaults to None.

    progress_indicator_id : str, optional
        Set the ID used to output monitoring information of the optimization progress.

        No default value.

    fold_num : int, optional
        The number of folds in the cross-validation process.

        Defaults to 5.

    resampling_method : {'cv', 'stratified_cv'}, optional
        Specifies the resampling method for pipeline evaluation.

        Defaults to 'stratified_cv'.

    max_eval_time_mins : float, optional
        Time limit to evaluate a single pipeline. The unit is minutes.

        Defaults to 0.0 (there is no time limit).

    early_stop : int, optional
        Stop optimization progress when the best pipeline is not updated for the given consecutive generations.
        0 means there is no early stop.

        Defaults to 5.

    successive_halving : bool, optional
        Specifies whether to use successive_halving in the evaluation phase.

        Defaults to True.

    min_budget : int, optional
        Specifies the minimum budget (the minimum evaluation dataset size) when successive halving has been applied.

        Defaults to 1/5 of the dataset size.

    max_budget : int, optional
        Specifies the maximum budget (the maximum evaluation dataset size) when successive halving has been applied.

        Defaults to the whole dataset size.

    min_individuals : int, optional
        Specifies the minimum individuals in the evaluation phase when successive halving has been applied.

        Defaults to 3.

    connections : str or dict, optional

        Specifies the connections in the Connection Constrained Optimization. The options are:

        - 'default'
        - customized connections JSON string or a dict.

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

    special_group_id : {'PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID', 'PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID2'}, optional
        The special group ID used in the pipeline processing.

        Defaults to 'PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID2'.

    References
    ----------
    Under the given ``config_dict`` and ``scoring``, `MassiveAutomaticTimeSeries` uses genetic programming to
    to search for the best valid pipeline. Please see :ref:`Genetic Optimization in AutoML<genetic_automl-label>`
    for more details.

    Attributes
    ----------
    best_pipeline_: DataFrame
        Best pipelines selected, structured as follows:

        - 1st column: GROUP_ID, type INTEGER or NVARCHAR, pipeline GROUP IDs.
        - 2nd column: ID, type INTEGER, pipeline IDs.
        - 3rd column: PIPELINE, type NVARCHAR, pipeline contents.
        - 4th column: SCORES, type NVARCHAR, scoring metrics for pipeline.

        Available only when the ``pipeline`` parameter is not specified during the fitting process.

    model_ : DataFrame or a list of DataFrames
        If pipeline is not None, structured as follows:

        - 1st column: GROUP_ID.
        - 2nd column: ROW_INDEX.
        - 3rd column: MODEL_CONTENT.

        If auto-ml is enabled, structured as follows:

            - 1st DataFrame:

              .. only:: html

                 - 1st column: GROUP_ID.
                 - 2nd column: ROW_INDEX.
                 - 3rd column: MODEL_CONTENT.

              .. only: latex

                ============ ==============
                Column Index Column Name
                ============ ==============
                1            GROUP_ID
                2            ROW_INDEX
                3            MODEL_CONTENT
                ============ ==============

            - 2nd DataFrame: best_pipeline\_

    info_ : DataFrame
        Related info/statistics for MassiveAutomaticClassification pipeline fitting, structured as follows:

        - 1st column: GROUP_ID.
        - 2nd column: STAT_NAME.
        - 3rd column: STAT_VALUE.

    error_: DataFrame
        Error information for the pipeline fitting process.

    Examples
    --------

    Create a MassiveAutomaticClassification instance:

    >>> auto_c = MassiveAutomaticClassification(generations=2,
                                                population_size=5,
                                                offspring_size=5)
    >>> auto_c.enable_workload_class("MY_WORKLOAD_CLASS")
    >>> auto_c.fit(data=df_train, group_key="GROUP_ID", label="LABEL")
    >>> pipelines = auto_c.get_best_pipelines()  # get the best pipelines
    >>> auto_c.fit(data=df_train, group_key="GROUP_ID", pipeline=pipelines)  # refit with the best pipelines
    >>> res = auto_c.predict(data=df_test, group_key="GROUP_ID")
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
                 max_resource=None,
                 special_group_id="PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID2",
                 progress_indicator_id=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(MassiveAutomaticClassification, self).__init__(
                 pipeline_type="classifier",
                 scorings=scorings,
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
                 max_resource=max_resource,
                 special_group_id=special_group_id,
                 progress_indicator_id=progress_indicator_id)

    def fit(self,
            data,
            group_key,
            key=None,
            features=None,
            label=None,
            group_pipelines=None,
            categorical_variable=None,
            background_size=None,
            background_sampling_seed=None,
            model_table_name=None,
            use_explain=None,
            explain_method=None,
            group_params=None):
        r"""
        Fit function of MassiveAutomaticClassification.

        Parameters
        ----------
        data : DataFrame
            The input data.

        group_key : str
            Name of the group column.

        key : str, optional
            Name of the ID column.

            If ``key`` is not provided, then:

            - If ``data`` is indexed by a single column, then ``key`` defaults to that index column.
            - Otherwise, it is assumed that ``data`` contains no ID column.

        features : list of str, optional
            Names of the feature columns.

            If ``features`` is not provided, it defaults to all non-group_key, non-key, non-label columns.

        label : str, optional
            Name of the dependent variable.

            Defaults to the name of the last non-group_key, non-key column.

        group_pipelines : dict of str or nested dict, optional
            Directly uses the group of pipelines to fit. Keys are group IDs and values are pipelines.

            Defaults to None.

        categorical_variable : str or list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.

        background_size : int, optional
            If set, the reason code procedure will be enabled. Only valid when `group_pipelines` is provided and
            ``explain_method`` is 'kernelshap'. It should not be larger than the row size of the training data.

            Defaults to None.

        background_sampling_seed : int, optional
            Specifies the seed for the random number generator in the background sampling. Only valid when `group_pipelines` is provided and ``explain_method`` is 'kernelshap'.

            - 0: Uses the current time (in seconds) as the seed.
            - Others: Uses the specified value as the seed.

            Defaults to 0.

        model_table_name : str, optional
            Specifies the HANA model table name instead of the generated temporary table.

            Defaults to None.

        use_explain : bool, optional
            Specifies whether to store information for pipeline explanation.

            Defaults to False.

        explain_method : str, optional
            Specifies the explanation method. Only valid when `use_explain` is True.

            Options are:

            - 'kernelshap': To make explanations using Kernel SHAP, ``background_size`` should be larger than 0.
            - 'globalsurrogate'

            Defaults to 'globalsurrogate'.

        Returns
        -------
        A fitted object of class "MassiveAutomaticClassification".
        """
        if not hasattr(self, 'hanaml_fit_params'):
            setattr(self, 'hanaml_fit_params', pal_param_register())
        if not hasattr(self, 'training_data'):
            setattr(self, 'training_data', data)
        if group_pipelines is None and (use_explain or background_size is not None):
            #call automl first
            self._fit(data,
                      group_key,
                      key=key,
                      features=features,
                      label=label,
                      group_pipelines=None,
                      categorical_variable=categorical_variable,
                      background_size=background_size,
                      background_sampling_seed=background_sampling_seed,
                      use_explain=use_explain,
                      explain_method=explain_method,
                      group_params=group_params)
            #call pipeline fit
            self._fit(data=data,
                      group_key=group_key,
                      key=key,
                      features=features,
                      label=label,
                      group_pipelines=self.get_best_pipelines(),
                      categorical_variable=categorical_variable,
                      background_size=background_size,
                      background_sampling_seed=background_sampling_seed,
                      model_table_name=model_table_name,
                      use_explain=use_explain,
                      explain_method=explain_method,
                      group_params=group_params)
        else:
            self._fit(data,
                      group_key,
                      key=key,
                      features=features,
                      label=label,
                      group_pipelines=group_pipelines,
                      categorical_variable=categorical_variable,
                      background_size=background_size,
                      background_sampling_seed=background_sampling_seed,
                      model_table_name=model_table_name,
                      use_explain=use_explain,
                      explain_method=explain_method,
                      group_params=group_params)
        return self

    def predict(self,
                data,
                group_key,
                key=None,
                features=None,
                model=None,
                show_explainer=False,
                top_k_attributions=None,
                random_state=None,
                sample_size=None,
                verbose_output=None,
                predict_args=None,
                group_params=None):
        r"""
        Predict function for MassiveAutomaticClassification.

        Parameters
        ----------
        data :  DataFrame
            Data to be predicted.

        group_key : str
            Name of the group column.

        key : str, optional

            Name of the ID column.

            Mandatory if ``data`` is not indexed, or is indexed by multiple columns.

            Defaults to the index of ``data`` if ``data`` is indexed by a single column.

        features : a list of str, optional

            Names of the feature columns.

            If ``features`` is not provided, it defaults to all non-group_key, non-key columns.

        model : DataFrame, optional
            The model to be used for prediction.

            Defaults to the fitted model (model\_).

        show_explainer : bool, optional
            If True, the reason code will be returned. Only valid when background_size is provided during the fit process.

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

        group_params : dict, optional
            Specifies the group parameters for the prediction.

            Defaults to None.

        Returns
        -------
        DataFrame

            Predicted result, structured as follows:

            - 1st column: GROUP_ID, group IDs.
            - 2nd column: Data type and name same as the 1st column of ``data``.
            - 3rd column: SCORE, class labels.
            - 4th column: CONFIDENCE, confidence of a class(available only if ``show_explainer`` is True).
            - 5th column: REASON CODE, attributions of features(available only if ``show_explainer`` is True).
            - 6th & 7th columns: placeholder columns for future implementations(available only if ``show_explainer`` is True).
        """
        return self._predict(data,
                             group_key,
                             key=key,
                             features=features,
                             model=model,
                             show_explainer=show_explainer,
                             top_k_attributions=top_k_attributions,
                             random_state=random_state,
                             sample_size=sample_size,
                             verbose_output=verbose_output,
                             predict_args=predict_args,
                             group_params=group_params)

    def score(self,
              data,
              group_key,
              key=None,
              features=None,
              label=None,
              model=None,
              random_state=None,
              top_k_attributions=None,
              sample_size=None,
              verbose_output=None,
              predict_args=None,
              group_params=None):
        r"""

        Pipeline model score function, with final estimator being a classifier.

        Parameters
        ----------

        data : DataFrame
            Data for pipeline model scoring.

        group_key : str
            Name of the group column.

        key : str, optional

            Name of the ID column.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        features : a list of str, optional

            Names of the feature columns.
            Should be same as those provided in the training data.

            If ``features`` is not provided, it defaults to all non-group_key, non-key, non-label columns.

        label : str, optional

            Name of the dependent variable.

            Defaults to the name of the last non-group_key, non-key column.

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

        group_params : dict, optional
            Specifies the group parameters for the prediction.

            Defaults to None.

        Returns
        -------
        DataFrames

            - DataFrame 1 : Prediction result for the input data.
            - DataFrame 2 : Statistics.
        """
        setattr(self, 'hanaml_score_params', pal_param_register())
        setattr(self, 'testing_data', data)
        return self._score(data,
                           group_key,
                           key=key,
                           features=features,
                           label=label,
                           model=model,
                           random_state=random_state,
                           top_k_attributions=top_k_attributions,
                           sample_size=sample_size,
                           verbose_output=verbose_output,
                           predict_args=predict_args,
                           group_params=group_params)

class MassiveAutomaticRegression(_MassiveAutoMLBase):
    """
    MassiveAutomaticRegression offers an intelligent search amongst machine learning pipelines for supervised regression tasks in a massive mode.
    Each machine learning pipeline contains several operators such as preprocessors, supervised regression models, and transformers that follow the API of hana-ml algorithms.

    For MassiveAutomaticRegression parameter mappings of hana_ml and HANA PAL, please refer to the doc page: :ref:`param_mapping`.

    Parameters
    ----------
    scorings : dict, optional
        MassiveAutomaticRegression supports multi-objective optimization with specified weights for each target.
        The goal is to minimize the target. Therefore, if you want to maximize the target, the target weight needs to be negative.

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

        Defaults to {"MAE": -1.0, "EVAR": 1.0} (minimize MAE and maximize EVAR).

    generations : int, optional
        The number of iterations of the pipeline optimization.

        Defaults to 5.

    population_size : int, optional

    population_size : int, optional


        - When ``search_method`` takes the value of 'GA', ``population_size`` is the number of individuals in each generation in the genetic programming algorithm. Having too few individuals can limit the possibilities of crossover and exploration of the search space to only a small portion. Conversely, if there are too many individuals, the performance of the genetic algorithm may slow down.
        - When ``search_method`` takes the value of 'random', ``population_size`` is the number of pipelines randomly generated and evaluated in random search.

        Defaults to 20.

    offspring_size : int, optional
        The number of offsprings to produce in each generation.

        It controls the number of new individuals generated in each iteration by genetic operations, from the population.

        Defaults to the size of ``population_size``.

    elite_number : int, optional
        The number of elites to produce in each generation.

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
        Specifies the seed for the random number generator. Use system time if not provided.

        No default value.

    config_dict : str or dict, optional
        The customized configuration for the searching space.

        - {'light', 'default'}: use provided config_dict templates.
        - JSON format config_dict. It could be a JSON string or dict.

        If it is None, the default config_dict will be used.

        Defaults to None.

    progress_indicator_id : str, optional
        Set the ID used to output monitoring information of the optimization progress.

        No default value.

    fold_num : int, optional
        The number of folds in the cross-validation process.

        Defaults to 5.

    resampling_method : {'cv', 'stratified_cv'}, optional
        Specifies the resampling method for pipeline evaluation.

        Defaults to 'stratified_cv'.

    max_eval_time_mins : float, optional
        Time limit to evaluate a single pipeline. The unit is minutes.

        Defaults to 0.0 (there is no time limit).

    early_stop : int, optional
        Stop optimization progress when the best pipeline is not updated for the given consecutive generations.
        0 means there is no early stop.

        Defaults to 5.

    successive_halving : bool, optional
        Specifies whether to use successive_halving in the evaluation phase.

        Defaults to True.

    min_budget : int, optional
        Specifies the minimum budget (the minimum evaluation dataset size) when successive halving has been applied.

        Defaults to 1/5 of the dataset size.

    max_budget : int, optional
        Specifies the maximum budget (the maximum evaluation dataset size) when successive halving has been applied.

        Defaults to the whole dataset size.

    min_individuals : int, optional
        Specifies the minimum individuals in the evaluation phase when successive halving has been applied.

        Defaults to 3.

    connections : str or dict, optional

        Specifies the connections in the Connection Constrained Optimization. The options are:

        - 'default'
        - customized connections JSON string or a dict.

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

    special_group_id : {'PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID', 'PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID2'}, optional
        The special group ID used in the pipeline processing.

        Defaults to 'PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID2'.

    References
    ----------
    Under the given ``config_dict`` and ``scoring``, `MassiveAutomaticTimeSeries` uses genetic programming to
    to search for the best valid pipeline. Please see :ref:`Genetic Optimization in AutoML<genetic_automl-label>`
    for more details.

    Attributes
    ----------
    best_pipeline_: DataFrame
        Best pipelines selected, structured as follows:

        - 1st column: GROUP_ID, type INTEGER or NVARCHAR, pipeline GROUP IDs.
        - 2nd column: ID, type INTEGER, pipeline IDs.
        - 3rd column: PIPELINE, type NVARCHAR, pipeline contents.
        - 4th column: SCORES, type NVARCHAR, scoring metrics for pipeline.

        Available only when the ``pipeline`` parameter is not specified during the fitting process.

    model_ : DataFrame or a list of DataFrames
        If pipeline is not None, structured as follows:

        - 1st column: GROUP_ID.
        - 2nd column: ROW_INDEX.
        - 3rd column: MODEL_CONTENT.

        If auto-ml is enabled, structured as follows:

            - 1st DataFrame:

              .. only:: html

                 - 1st column: GROUP_ID.
                 - 2nd column: ROW_INDEX.
                 - 3rd column: MODEL_CONTENT.

              .. only: latex

                ============ ==============
                Column Index Column Name
                ============ ==============
                1            GROUP_ID
                2            ROW_INDEX
                3            MODEL_CONTENT
                ============ ==============

            - 2nd DataFrame: best_pipeline\_

    info_ : DataFrame
        Related info/statistics for MassiveAutomaticRegression pipeline fitting, structured as follows:

        - 1st column: GROUP_ID.
        - 2nd column: STAT_NAME.
        - 3rd column: STAT_VALUE.

    error_: DataFrame
        Error information for the pipeline fitting process.

    Examples
    --------
    Create a MassiveAutomaticRegression instance:

    >>> auto_r = MassiveAutomaticRegression(generations=2,
                                            population_size=5,
                                            offspring_size=5)
    >>> auto_r.enable_workload_class("MY_WORKLOAD_CLASS")
    >>> auto_r.fit(data=df_train, group_key="GROUP_ID", label="LABEL")
    >>> pipelines = auto_r.get_best_pipelines()  # get the best pipelines
    >>> auto_r.fit(data=df_train, group_key="GROUP_ID", pipeline=pipelines)  # refit with the best pipelines
    >>> res = auto_r.predict(data=df_test, group_key="GROUP_ID")
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
                 max_resource=None,
                 special_group_id="PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID2",
                 progress_indicator_id=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(MassiveAutomaticRegression, self).__init__(
                 pipeline_type="regressor",
                 scorings=scorings,
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
                 max_resource=max_resource,
                 special_group_id=special_group_id,
                 progress_indicator_id=progress_indicator_id)

    def fit(self,
            data,
            group_key,
            key=None,
            features=None,
            label=None,
            group_pipelines=None,
            categorical_variable=None,
            background_size=None,
            background_sampling_seed=None,
            model_table_name=None,
            use_explain=None,
            explain_method=None,
            group_params=None):
        r"""
        Fit function of MassiveAutomaticRegression.

        Parameters
        ----------
        data : DataFrame
            The input data.

        group_key : str
            Name of the group column.

        key : str, optional
            Name of the ID column.

            If ``key`` is not provided, then:

            - if ``data`` is indexed by a single column, then ``key`` defaults to that index column;
            - otherwise, it is assumed that ``data`` contains no ID column.

        features : a list of str, optional
            Names of the feature columns.

            If ``features`` is not provided, it defaults to all non-group_key, non-key, non-label columns.

        label : str, optional
            Name of the dependent variable.

            Defaults to the name of the last non-group_key, non-key column.

        group_pipelines : dict of str or nested dict, optional
            Directly uses the group of pipelines to fit. Keys are group IDs and values are pipelines.

            Defaults to None.

        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.
        background_size : int, optional
            If set, the reason code procedure will be enabled. Only valid when `pipeline` is provided and
             ``explain_method`` is 'kernelshap'. It should not be larger than the row size of train data.

            Defaults to None.

        background_sampling_seed : int, optional
            Specifies the seed for random number generator in the background sampling. Only valid when `pipeline` is provided and ``explain_method`` is 'kernelshap'.

            - 0: Uses the current time (in second) as seed
            - Others: Uses the specified value as seed

            Defaults to 0.

        model_table_name : str, optional
            Specifies the HANA model table name instead of the generated temporary table.

            Defaults to None.

        use_explain : bool, optional
            Specifies whether to store information for pipeline explanation.

            Defaults to False.

        explain_method : str, optional
            Specifies the explanation method. Only valid when `use_explain` is True.

            Options are:

            - 'kernelshap' : To make explanation by Kernel SHAP, ``background_size`` should be larger than 0.
            - 'globalsurrogate'

            Defaults to 'globalsurrogate'.

        Returns
        -------
        A fitted object of class "MassiveAutomaticRegression"

        """
        if not hasattr(self, 'hanaml_fit_params'):
            setattr(self, 'hanaml_fit_params', pal_param_register())
        if not hasattr(self, 'training_data'):
            setattr(self, 'training_data', data)
        if group_pipelines is None and (use_explain or background_size is not None):
            #call automl first
            self._fit(data,
                      group_key,
                      key=key,
                      features=features,
                      label=label,
                      group_pipelines=None,
                      categorical_variable=categorical_variable,
                      background_size=background_size,
                      background_sampling_seed=background_sampling_seed,
                      use_explain=use_explain,
                      explain_method=explain_method,
                      group_params=group_params)
            #call pipeline fit
            self._fit(data=data,
                      group_key=group_key,
                      key=key,
                      features=features,
                      label=label,
                      group_pipelines=self.get_best_pipelines(),
                      categorical_variable=categorical_variable,
                      background_size=background_size,
                      background_sampling_seed=background_sampling_seed,
                      model_table_name=model_table_name,
                      use_explain=use_explain,
                      explain_method=explain_method,
                      group_params=group_params)
        else:
            self._fit(data,
                      group_key,
                      key=key,
                      features=features,
                      label=label,
                      group_pipelines=group_pipelines,
                      categorical_variable=categorical_variable,
                      background_size=background_size,
                      background_sampling_seed=background_sampling_seed,
                      model_table_name=model_table_name,
                      use_explain=use_explain,
                      explain_method=explain_method,
                      group_params=group_params)
        return self

    def predict(self,
                data,
                group_key,
                key=None,
                features=None,
                model=None,
                show_explainer=False,
                top_k_attributions=None,
                random_state=None,
                sample_size=None,
                verbose_output=None,
                predict_args=None,
                group_params=None):
        r"""
        Predict function for MassiveAutomaticRegression.

        Parameters
        ----------
        data :  DataFrame
            Data to be predicted.

        group_key : str
            Name of the group column.

        key : str, optional

            Name of the ID column.

            Mandatory if ``data`` is not indexed, or is indexed by multiple columns.

            Defaults to the index of ``data`` if ``data`` is indexed by a single column.

        features : a list of str, optional

            Names of the feature columns.

            If ``features`` is not provided, it defaults to all non-group_key, non-key columns.

        model : DataFrame, optional
            The model to be used for prediction.

            Defaults to the fitted model (model\_).

        show_explainer : bool, optional
            If True, the reason code will be returned. Only valid when background_size is provided during the fit process.

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

        group_params : dict, optional
            Specifies the group parameters for the prediction.

            Defaults to None.

        Returns
        -------
        DataFrame

            Predicted result, structured as follows:

            - 1st column: GROUP_ID, group IDs.
            - 2nd column: Data type and name same as the 1st column of ``data``.
            - 3rd column: SCORE, class labels.
            - 4th column: CONFIDENCE, confidence of a class(available only if ``show_explainer`` is True).
            - 5th column: REASON CODE, attributions of features(available only if ``show_explainer`` is True).
            - 6th & 7th columns: placeholder columns for future implementations(available only if ``show_explainer`` is True).
        """
        return self._predict(data,
                             group_key,
                             key=key,
                             features=features,
                             model=model,
                             show_explainer=show_explainer,
                             top_k_attributions=top_k_attributions,
                             random_state=random_state,
                             sample_size=sample_size,
                             verbose_output=verbose_output,
                             predict_args=predict_args,
                             group_params=group_params)

    def score(self,
              data,
              group_key,
              key=None,
              features=None,
              label=None,
              model=None,
              random_state=None,
              sample_size=None,
              verbose_output=None,
              predict_args=None,
              group_params=None):
        r"""

        Pipeline model score function, with final estimator being a regressor.

        Parameters
        ----------

        data : DataFrame
            Data for pipeline model scoring.

        group_key : str
            Name of the group column.

        key : str, optional

            Name of the ID column.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        features : a list of str, optional

            Names of the feature columns.
            Should be same as those provided in the training data.

            If ``features`` is not provided, it defaults to allnon-group_key, non-key, non-label columns.

        label : str, optional

            Name of the dependent variable.

            Defaults to the name of the last non-group_key, non-key column.

        model : DataFrame, optional
            DataFrame that contains the pipeline model for scoring.

            Defaults to the fitted pipeline model of the current class instance.

        random_state : DataFrame, optional
            Specifies the random seed.

            Defaults to -1(system time).

        sample_size : int, optional
            Specifies the number of sampled combinations of features.

            It is better to use a number that is greater than the number of features
            in ``data``.

            If set as 0, it is determined by algorithm heuristically.

            Defaults to 0.
        verbose_output : bool, optional

            - True: Outputs the predicted value.
            - False: Outputs the predicted value only.

            Defaults to True.
        predict_args : dict, optional
            Specifies estimator-specific parameters passed to the predict phase of the score method.

            If not None, it must be specified as a dict with one of the following format

            - `key` for estimator name, and `value` for estimator-specific parameter setting in a dict.
              For example `{'RDT_Regressor':{'block_size': 5}, 'NB_Regressor':{'laplace':1.0}}`.

        group_params : dict, optional
            Specifies the group parameters for the prediction.

            Defaults to None.

        Returns
        -------
        DataFrames

            - DataFrame 1 : Prediction result for the input data.
            - DataFrame 2 : Statistics.
        """
        setattr(self, 'hanaml_score_params', pal_param_register())
        setattr(self, 'testing_data', data)
        return self._score(data,
                           group_key,
                           key=key,
                           features=features,
                           label=label,
                           model=model,
                           random_state=random_state,
                           sample_size=sample_size,
                           verbose_output=verbose_output,
                           predict_args=predict_args,
                           group_params=group_params)

class MassiveAutomaticTimeSeries(_MassiveAutoMLBase):
    """

    MassiveAutomaticTimeSeries offers an intelligent search amongst machine learning pipelines for time series tasks.
    Each machine learning pipeline contains several operators such as preprocessors, time series models and transformer
    that follows API of hana-ml algorithms.

    For MassiveAutomaticTimeSeries parameter mappings of hana_ml and HANA PAL, please refer to the doc page: :ref:`param_mapping`

    Parameters
    ----------
    scorings : dict, optional
        MassiveAutomaticTimeSeries supports multi-objective optimization with specified weights of each target.
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
        - LAYERS: The number of operators. Lower values indicate better performance. It is recommended to assign a negative weight to this metric.
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
        The number of fold in the cross validation process.

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

        Specifies the connections in the Connection Contrained Optimization. The options are:

        - 'default'
        - customized connections json string or a dict.

        Defaults to None. If ``connections`` is not provided, connection contrained optimization is not applied.

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
    Under the given ``config_dict`` and ``scoring``, `MassiveAutomaticTimeSeries` uses genetic programming to
    to search for the best valid pipeline. Please see :ref:`Genetic Optimization in AutoML<genetic_automl-label>`
    for more details.

    Attributes
    ----------
    best_pipeline_: DataFrame
        Best pipelines selected, structured as follows:

        - 1st column: GROUP_ID, type INTEGER or NVARCHAR, pipeline GROUP IDs.
        - 2nd column: ID, type INTEGER, pipeline IDs.
        - 3rd column: PIPELINE, type NVARCHAR, pipeline contents.
        - 4th column: SCORES, type NVARCHAR, scoring metrics for pipeline.

        Available only when the ``pipeline`` parameter is not specified during the fitting process.

    model_ : DataFrame or a list of DataFrames
        If pipeline is not None, structured as follows

        - 1st column: GROUP_ID
        - 2nd column: ROW_INDEX
        - 3rd column: MODEL_CONTENT

        If auto-ml is enabled, structured as follows

        - 1st DataFrame:

          .. only:: html

             - 1st column: GROUP_ID
             - 2nd column: ROW_INDEX
             - 3rd column: MODEL_CONTENT

          .. only: latex

            ============ ==============
            Column Index Column Name
            ============ ==============
            1            GROUP_ID
            2            ROW_INDEX
            3            MODEL_CONTENT
            ============ ==============

        - 2nd DataFrame: best_pipeline\_

    info_ : DataFrame
        Related info/statistics for MassiveAutomaticTimeSeries pipeline fitting, structured as follows:

        - 1st column: GROUP_ID
        - 2nd column: STAT_NAME
        - 3rd column: STAT_VALUE

    error_: DataFrame
        Error information for the pipeline fitting process.

    Examples
    --------

    Create a MassiveAutomaticTimeSeries instance:

    >>> progress_id = "automl_{}".format(uuid.uuid1())
    >>> auto_ts = MassiveAutomaticTimeSeries(generations=2,
                                             population_size=5,
                                             offspring_size=5)
    >>> auto_ts.enable_workload_class("MY_WORKLOAD_CLASS")
    >>> auto_ts.fit(data=df_ts, group_key='GROUP_ID', key='ID', endog="SERIES")
    >>> pipeline = auto_ts.get_best_pipelines()
    >>> auto_ts.fit(data=df_ts, group_key='GROUP_ID', key='ID', pipeline=pipeline)
    >>> res = auto_ts.predict(data=df_predict, group_key='GROUP_ID', key='ID')

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
                 max_resource=None,
                 special_group_id="PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID2",
                 progress_indicator_id=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(MassiveAutomaticTimeSeries, self).__init__(
                 pipeline_type="timeseries",
                 scorings=scorings,
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
                 fold_num=fold_num,
                 resampling_method=resampling_method,
                 max_eval_time_mins=max_eval_time_mins,
                 early_stop=early_stop,
                 percentage=percentage,
                 gap_num=gap_num,
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
                 max_resource=max_resource,
                 special_group_id=special_group_id,
                 progress_indicator_id=progress_indicator_id)

    def fit(self,
            data,
            group_key,
            key=None,
            endog=None,
            exog=None,
            group_pipelines=None,
            categorical_variable=None,
            background_size=None,
            background_sampling_seed=None,
            model_table_name=None,
            use_explain=None,
            explain_method=None,
            lag=None,
            lag_features=None,
            group_params=None):
        r"""
        The fit function for MassiveAutomaticTimeSeries.

        Parameters
        ----------
        data : DataFrame
            The input time-series data for training.

        group_key : str
            Name of the group column.

        key : str, optional
            Specifies the column that represents the ordering of time-series data.

            If ``data`` is indexed by a single column, then ``key`` defaults
            to that index column; otherwise ``key`` must be specified(i.e. is mandatory).

        endog : str, optional
            Specifies the endogenous variable for time-series data.

            Defaults to the 1st non-group_key, non-key column of  ``data``

        exog : str, optional
            Specifies the exogenous variables for time-series data.

            Defaults to all non-group_key, non-key, non-endog columns in ``data``.


        group_pipelines : str or dict, optional
            Directly use the input pipeline to fit.

            Defaults to None.

        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.
        model_table_name : str, optional
            Specifies the HANA model table name instead of the generated temporary table.

            Defaults to None.

        use_explain : bool, optional
            Specifies whether to store information for pipeline explanation.

            Defaults to False.

        explain_method : str, optional
            Specifies the explanation method. Only valid when `use_explain` is True.

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

        group_params : dict, optional
            Specifies the group parameters for the prediction.

            Defaults to None.

        Returns
        -------
        A fitted object of class "MassiveAutomaticTimeSeries".
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        setattr(self, "key", key)
        setattr(self, "exog", exog)
        setattr(self, "endog", endog)

        self.config_dict = _update_config_dict_with_lag(self.config_dict, lag=lag, lag_features=lag_features)

        if group_params is not None:
            for kkey, vval in group_params.items():
                if 'lag' in vval or 'lag_features' in vval:
                    if 'config_dict' in vval:
                        group_params[kkey]['config_dict'] = _update_config_dict_with_lag(config_dict=vval.get('config_dict'), lag=vval.get('lag'), lag_features=vval.get('lag_features'))
                    else:
                        group_params[kkey]['config_dict'] = _update_config_dict_with_lag(config_dict=self.config_dict, lag=vval.get('lag'), lag_features=vval.get('lag_features'))

        if group_pipelines is None and (use_explain or background_size is not None):
            #call automl first
            self._fit(data,
                      group_key,
                      key=key,
                      features=exog,
                      label=endog,
                      group_pipelines=None,
                      categorical_variable=categorical_variable,
                      background_size=background_size,
                      background_sampling_seed=background_sampling_seed,
                      use_explain=use_explain,
                      explain_method=explain_method,
                      group_params=group_params)
            #call pipeline fit
            self._fit(data=data,
                      group_key=group_key,
                      key=key,
                      features=exog,
                      label=endog,
                      group_pipelines=self.get_best_pipelines(),
                      categorical_variable=categorical_variable,
                      background_size=background_size,
                      background_sampling_seed=background_sampling_seed,
                      model_table_name=model_table_name,
                      use_explain=use_explain,
                      explain_method=explain_method,
                      group_params=group_params)
        else:
            self._fit(data,
                      group_key,
                      key=key,
                      features=exog,
                      label=endog,
                      group_pipelines=group_pipelines,
                      categorical_variable=categorical_variable,
                      background_size=background_size,
                      background_sampling_seed=background_sampling_seed,
                      model_table_name=model_table_name,
                      use_explain=use_explain,
                      explain_method=explain_method,
                      group_params=group_params)
        return self

    def predict(self,
                data,#pylint:disable=arguments-differ, arguments-renamed
                group_key,
                key=None,
                exog=None,
                model=None,
                show_explainer=False,
                predict_args=None,
                group_params=None):
        r"""
        Predict function for MassiveAutomaticTimeSeries.

        Parameters
        ----------
        data :  DataFrame
            The input time-series data to be predicted.

        group_key : str
            Name of the group column.

        key : str, optional
            Specifies the column that represents the ordering of the input time-series data.

            If ``data`` is indexed by a single column, then ``key`` defaults
            to that index column; otherwise ``key`` must be specified(i.e. is mandatory).

        exog : str or a list of str, optional

            Names of the exogenous variables in ``data``.

            Defaults to all non-group_key, non-key columns if not provided.

        model : DataFrame, optional
            The model to be used for prediction.

            Defaults to the fitted model(i.e. self.model\_).

        show_explainer : bool, optional
            Reserved paramter for future implementation of SHAP Explainer.

            Currently ineffective.

        predict_args : dict, optional
            Specifies estimator-specific parameters passed to the predict method.

            If not None, it must be specified as a dict with one of the following format:

            - `key` for estimator name, and `value` for estimator-specific parameter setting in a dict.
              For example `{'RDT_Classifier':{'block_size': 5}, 'NB_Classifier':{'laplace':1.0}}`.

            Defaults to None(i.e. no estimator-specific predict parameter provided).

        group_params : dict, optional
            Specifies the group parameters for the prediction.

            Defaults to None.

        Returns
        -------
        DataFrame
            Predicted result.
        """
        predict_result = self._predict(data=data,
                                       group_key=group_key,
                                       key=key,
                                       features=exog,
                                       model=model,
                                       show_explainer=show_explainer,
                                       predict_args=predict_args,
                                       group_params=group_params)
        setattr(self, "forecast_result", predict_result)
        return predict_result

    def score(self,
              data,
              group_key,
              key=None,
              endog=None,
              exog=None,
              model=None,
              predict_args=None,
              group_params=None):
        r"""
        Pipeline model score function.

        Parameters
        ----------
        data : DataFrame
            Data for pipeline model scoring.

        group_key : str
            Name of the group column.

        key : str, optional
            Specifies the column that represents the ordering of the input time-series data.

            If ``data`` is indexed by a single column, then ``key`` defaults
            to that index column; otherwise, ``key`` must be specified (i.e., it is mandatory).

        endog : str, optional
            Specifies the endogenous variable for time-series data.

            Defaults to the 1st non-group_key, non-key column of ``data``.

        exog : str, optional
            Specifies the exogenous variables for time-series data.

            Defaults to all non-group_key, non-key, non-endog columns in ``data``.

        model : DataFrame, optional
            The pipeline model used to make predictions.

            Defaults to the fitted pipeline model (i.e., self.model\_).

        predict_args : dict, optional
            Specifies estimator-specific parameters passed to the predict phase of the score method.

            If not None, it must be specified as a dict with one of the following formats:

            - `key` for estimator name, and `value` for estimator-specific parameter settings in a dict.
              For example, `{'RDT_Classifier': {'block_size': 5}, 'NB_Classifier': {'laplace': 1.0}}`.

            Defaults to None (i.e., no estimator-specific predict parameter provided).

        group_params : dict, optional
            Specifies the group parameters for the prediction.

            Defaults to None.

        Returns
        -------
        DataFrames

            - DataFrame 1 : Prediction result for the input data.
            - DataFrame 2 : Statistics.
        """
        setattr(self, 'hanaml_score_params', pal_param_register())
        setattr(self, 'testing_data', data)
        return self._score(data=data,
                           group_key=group_key,
                           key=key,
                           features=exog,
                           label=endog,
                           model=model,
                           random_state=None,
                           top_k_attributions=None,
                           sample_size=None,
                           verbose_output=False,
                           predict_args=predict_args,
                           group_params=group_params)
