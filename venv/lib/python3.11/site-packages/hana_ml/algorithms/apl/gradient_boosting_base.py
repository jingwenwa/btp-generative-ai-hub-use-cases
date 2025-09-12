"""
Module defining a common abstract class for SAP HANA APL Gradient Boosting algorithms.
"""
import logging
import re
from collections import OrderedDict, defaultdict
import numpy as np
import pandas as pd
from hana_ml.ml_exceptions import FitIncompleteError
from hana_ml.algorithms.apl.apl_base import APLBase
from hana_ml.visualizers.report_builder import Page, ChartItem

logger = logging.getLogger(__name__) #pylint: disable=invalid-name


class GradientBoostingBase(APLBase):
    #pylint: disable=too-many-instance-attributes
    """
    Common abstract class for classification and regression.
    """

    def __init__(self,
                 early_stopping_patience,
                 eval_metric,
                 learning_rate,
                 max_depth,
                 max_iterations,
                 conn_context=None,
                 number_of_jobs=None,
                 variable_storages=None,
                 variable_value_types=None,
                 variable_missing_strings=None,
                 extra_applyout_settings=None,
                 ** other_params): #pylint: disable=too-many-arguments
        # --- model params
        self.early_stopping_patience = None
        self.eval_metric = None
        self.learning_rate = None
        self.max_depth = None
        self.max_iterations = None
        self.number_of_jobs = None
        self.label = None
        if early_stopping_patience:
            self.set_params(early_stopping_patience=early_stopping_patience)
        if eval_metric:
            self.set_params(eval_metric=eval_metric)
        if learning_rate:
            self.set_params(learning_rate=learning_rate)
        if max_depth:
            self.set_params(max_depth=max_depth)
        if max_iterations:
            self.set_params(max_iterations=max_iterations)
        if number_of_jobs:
            self.set_params(number_of_jobs=number_of_jobs)

        super(GradientBoostingBase, self).__init__(
            conn_context,
            variable_storages,
            variable_value_types,
            variable_missing_strings,
            extra_applyout_settings,
            ** other_params)

        self._model_type = None  # to be set by concret subclass

    def set_params(self, **parameters):
        """
        Sets attributes of the current model.

        Parameters
        ----------
        params : dict
            The attribute names and values
        """
        if 'early_stopping_patience' in parameters:
            self.early_stopping_patience = self._arg('early_stopping_patience',
                                                     parameters.pop('early_stopping_patience'),
                                                     int)
        if 'learning_rate' in parameters:
            self.learning_rate = self._arg('learning_rate',
                                           parameters.pop('learning_rate'),
                                           float)
        if 'max_depth' in parameters:
            self.max_depth = self._arg('max_depth',
                                       parameters.pop('max_depth'),
                                       int)
        if 'max_iterations' in parameters:
            self.max_iterations = self._arg('max_iterations',
                                            parameters.pop('max_iterations'),
                                            int)
        if 'number_of_jobs' in parameters:
            self.number_of_jobs = self._arg('number_of_jobs',
                                            parameters.pop('number_of_jobs'),
                                            int)
        if 'label' in parameters:
            self.label = parameters.pop('label')
        return super(GradientBoostingBase, self).set_params(**parameters)

    # pylint: disable=too-many-arguments
    def fit(self, data,
            key=None,
            features=None,
            label=None,
            weight=None,
            build_report=False):
        """
        Fits the model.

        Parameters
        ----------
        data : DataFrame
            The training dataset
        key : str, optional
            The name of the ID column.
            This column will not be used as feature in the model.
            It will be output as row-id when prediction is made with the model.
            If `key` is not provided, an internal key is created. But this is not the recommended
            usage. See notes below.
        features : list of str, optional
            The names of the features to be used in the model.
            If `features` is not provided, all non-ID and non-label columns will be taken.
        label : str, optional
            The name of the label column. Default is the last column.
        weight : str, optional
            The name of the weight variable.
            A weight variable allows one to assign a relative weight to each of the observations.
        build_report : bool, optional
            Whether to build report or not.
            Defaults to False.

        Returns
        -------
        self : object

        Notes
        -----
        It is highly recommended to specify a key column in the training dataset.
        If not, once the model is trained, it won't be possible anymore to have a key defined in
        any input dataset. That is particularly inconvenient to join the predictions output to
        the input dataset.
        """
        if label is None:
            label = data.columns[-1]
        self.label = label
        self._fit(data=data,
                  key=key,
                  features=features,
                  label=label,
                  weight=weight)
        if build_report:
            self.build_report()
        return self

    def get_feature_importances(self): #pylint: disable=unused-argument
        """
        Returns the feature importances.

        Returns
        -------

        Dictionary or pandas DataFrame
            If no segment column is given, a dictionary:
            { <importance_metric> : OrderedDictionary({ <feature_name> : <value> }) }.

            If a segment column is given, a pandas DataFrame which contains the feature importances
            for each segment.
        """
        if not hasattr(self, 'indicators_'):
            raise FitIncompleteError(
                "The INDICATORS table was not found. Please fit the model.")
        cond = "KEY = 'VariableContribution'"
        df_ind = self.indicators_.filter(cond).collect()
        segment_column = getattr(self, 'segment_column_name', None)
        if segment_column is None:
            df_ind['VALUE'] = df_ind['VALUE'].astype(np.float32)
            df_ind['DETAIL'] = df_ind['DETAIL'].astype(str)  # method name for importances
            # sort by method, value
            df_ind = df_ind.sort_values(['DETAIL', 'VALUE'], ascending=[True, False])
            ret = {}
            for _, row in df_ind.iterrows():
                imp_method = row.loc['DETAIL']
                var_name = row.loc['VARIABLE']
                imp_val = row.loc['VALUE']
                sub_dict = ret.get(imp_method, OrderedDict())
                sub_dict[var_name] = imp_val
                ret[imp_method] = sub_dict
            return ret
        else:
            df_ind = df_ind[['OID', 'DETAIL', 'VARIABLE', 'VALUE']]
            df_ind.columns = [segment_column, 'Metric', 'Feature', 'Importance']
            return df_ind

    def get_performance_metrics(self):
        """
        Returns the performance metrics of the last trained model.

        Returns
        -------

        Dictionary or pandas DataFrame
            If no segment column is given, a dictionary with metric name as key and metric value as
            value.

            If a segment column is given, a pandas DataFrame which contains the performance metrics
            for each segment.

        Examples
        --------

        >>> data = DataFrame(conn, 'SELECT * from APL_SAMPLES.CENSUS')
        >>> model = GradientBoostingBinaryClassifier(conn)
        >>> model.fit(data=data, key='id', label='class')
        >>> model.get_performance_metrics()
        {'AUC': 0.9385, 'PredictivePower': 0.8529, 'PredictionConfidence': 0.9759,...}
        """
        if not hasattr(self, 'indicators_'):
            raise FitIncompleteError(
                "The indicators table was not found. Please fit the model.")
        cond = "VARIABLE like 'gb_%' and (DETAIL is null or to_varchar(DETAIL)='?')"
        df_ind = self.indicators_.filter(cond).collect()
        df_ind = df_ind[df_ind['KEY'] != 'PredictionConfidence'].reset_index(drop=True)
        prediction_confidence = self.get_debrief_report('ClassificationRegression_Performance') \
            .filter('"Partition" = \'Estimation\' and "Indicator" = \'Prediction Confidence\'') \
            .collect()
        segment_column = getattr(self, 'segment_column_name', None)
        if segment_column is None:
            # Create the dictionary to be returned
            # converts df_ind.VALUE str to float if possible
            ret = {}
            for _, row in df_ind.iterrows():
                key = row.loc['KEY']
                old_v = row.loc['VALUE']
                try:
                    new_v = float(old_v)
                except ValueError:
                    new_v = old_v
                ret[key] = new_v
            if not prediction_confidence.empty:
                ret['PredictionConfidence'] = prediction_confidence['Value'].iloc[0]
            ret.update({'perf_per_iteration': self.get_evalmetrics()})
            ret.update({'BestIteration': self.get_best_iteration()})
            return ret
        else:
            df_ind = df_ind[['OID', 'KEY', 'VALUE']]
            df_ind.columns = [segment_column, 'Metric', 'Value']

            if not prediction_confidence.empty:
                prediction_confidence = prediction_confidence[['Oid', 'Indicator', 'Value']]
                prediction_confidence.columns = [segment_column, 'Metric', 'Value']
                prediction_confidence.replace('Prediction Confidence', 'PredictionConfidence', inplace=True)
                df_ind = pd.concat([df_ind, prediction_confidence])

            best_iterations = self.get_best_iteration()
            df_ind = pd.concat([df_ind, best_iterations])
            df_ind.sort_values(by=[segment_column, 'Metric'], inplace=True, ignore_index=True)

            return df_ind


    def get_best_iteration(self):
        """
        Returns the iteration that has provided the best performance on the validation dataset
        during the model training.

        Returns
        -------

        int or pandas DataFrame
            If no segment column is given, the best iteration.

            If a segment column is given, a pandas DataFrame which contains the best iteration
            for each segment.
        """
        if not hasattr(self, 'summary_'):
            raise FitIncompleteError(
                "The SUMMARY table was not found. Please fit the model.")
        cond = "KEY = 'BestIteration'"
        df_s = self.summary_.filter(cond).collect()

        segment_column = getattr(self, 'segment_column_name', None)
        if segment_column is None:
            val = df_s['VALUE'].astype(np.int64).iloc[0]
            return val
        else:
            df_s.columns = [segment_column, 'Metric', 'Value']
            return df_s

    def get_evalmetrics(self):
        """
        Returns the values of the evaluation metric at each iteration.
        These values are based on the validation dataset.

        Returns
        -------

        Dictionary or pandas DataFrame
            If no segment column is given, a dictionary:
            {'<MetricName>': <List of values>}.

            If a segment column is given, a pandas DataFrame which contains the evaluation metrics
            for each segment.
        """
        if not hasattr(self, 'indicators_'):
            raise FitIncompleteError(
                "The indicators table was not found. Please fit the model.")
        cond = "KEY like 'EvalMetric%'"
        df_ind = self.indicators_.filter(cond).collect()
        segment_column = getattr(self, 'segment_column_name', None)
        if segment_column is None:
            df_ind['DETAIL'] = df_ind['DETAIL'].fillna(0).astype(np.int64)
            df_ind.sort_values(['DETAIL'], inplace=True)
            ret = defaultdict(list)
            early_stopping_metric = ''
            for _, row in df_ind.iterrows():
                old_v = row.loc['VALUE']
                detail = row.loc['DETAIL']
                if detail == 0:
                    early_stopping_metric = old_v
                else:
                    try:
                        new_v = float(old_v)
                    except ValueError:
                        new_v = old_v
                    metric_search = re.search(r'EvalMetricValue\(([^\)]+)\)', row.loc['KEY'])
                    if metric_search is None:
                        ret[early_stopping_metric].append(new_v)
                    else:
                        metric = metric_search.group(1)
                        ret[metric].append(new_v)
            return dict(ret)
        else:
            early_stopping_metric = df_ind[df_ind['KEY'] == 'EvalMetric']['VALUE'].iloc[0]
            df_ind = df_ind[df_ind['VALUE'] != early_stopping_metric]

            for i, row in df_ind.iterrows():
                metric_search = re.search(r'EvalMetricValue\(([^\)]+)\)', row.loc['KEY'])
                if metric_search is None:
                    df_ind.at[i, 'KEY'] = early_stopping_metric
                else:
                    metric = metric_search.group(1)
                    df_ind.at[i, 'KEY'] = metric

            df_ind = df_ind[['OID', 'KEY', 'DETAIL', 'VALUE']]
            df_ind.columns = [segment_column, 'Metric', 'Iteration', 'Value']
            df_ind.sort_values(by=[segment_column, 'Metric', 'Iteration'], inplace=True,
                               ignore_index=True)
            return df_ind

    def _get_common_report_tables(self):
        importance_table = \
            self.get_debrief_report('ClassificationRegression_VariablesContribution') \
            .select('Variable', 'Contribution').rename_columns(['VARIABLE_NAME', 'IMPORTANCE']) \
            .collect().sort_values(by=['IMPORTANCE'])

        if not hasattr(self, 'summary_') or not hasattr(self, 'indicators_'):
            self.get_model_info()

        params = []
        for k in self.APL_ALIAS_KEYS:
            if k in ['early_stopping_patience',
                     'eval_metric',
                     'learning_rate',
                     'max_depth',
                     'max_iterations']:
                continue
            param_val = getattr(self, k, None)
            if param_val:
                params.append((k, param_val))
        if getattr(self, 'other_train_apl_aliases', None):
            params.append(('other_train_apl_aliases', self.other_train_apl_aliases))

        cond = ("KEY = 'EarlyStoppingPatience'"
                "or KEY = 'EvalMetric'"
                "or KEY = 'LearningRate'"
                "or KEY = 'MaxDepth'"
                "or KEY = 'MaxIterations'")
        # for these parameters, the default values are given in the summary table
        param_table = self.summary_.filter(cond).collect()
        param_table = pd.concat([param_table, pd.DataFrame(params, columns=['KEY', 'VALUE'])],
                                ignore_index=True).drop('OID', axis=1)

        optimal_param_table = pd.DataFrame({"PARAM_NAME": ["BestIteration"],
                                            "INT_VALUE": [self.get_best_iteration()],
                                            "DOUBLE_VALUE": [np.nan],
                                            "STRING_VALUE": [None]})

        return param_table, optimal_param_table, importance_table

    def _add_interaction_matrix_to_report(self, report_builder):
        if self._get_apl_version() >= 2311:
            interaction_df = self.get_debrief_report('ClassificationRegression_InteractionMatrix').collect() \
                .sort_values(by=['Oid', 'Target', 'First Variable', 'Second Variable'])
            if not interaction_df.empty:
                interaction_page = Page('Interaction Matrix')
                interaction_df['First Variable Index'] = interaction_df['First Variable'].astype('category').cat.codes
                interaction_df['Second Variable Index'] = interaction_df['Second Variable'].astype('category').cat.codes
                variable_names = interaction_df['First Variable'].unique().tolist()
                max_effect = interaction_df['Effect'].max()
                x_y_z_list = interaction_df[['First Variable Index', 'Second Variable Index', 'Effect']].values.tolist()
                interaction_config = {
                    'xAxis': {
                        'type': 'category',
                        'data': variable_names,
                        'axisLabel': { 'rotate': 30 }
                    },
                    'yAxis': {
                        'type': 'category',
                        'data': variable_names
                    },
                    'visualMap': [{
                        'min': 0,
                        'max': max_effect,
                        'orient': 'horizontal',
                        'left': 'center',
                        'bottom': '-%',
                        'calculable': 'true'
                    }],
                    'series': [
                        {
                            'type': 'heatmap',
                            'data': x_y_z_list,
                            'label': { 'show': 'true' },
                            'emphasis': {
                                'itemStyle': {
                                    'shadowBlur': 10,
                                    'shadowColor': 'rgba(0, 0, 0, 0.5)'
                                }
                            }
                        }
                    ]
                }
                interaction_matrix = ChartItem('Interaction Matrix', interaction_config)
                interaction_page.addItem(interaction_matrix)
                report_builder.addPage(interaction_page)
                report_builder.build()

    def build_report(self):
        """
        Build model report.
        """
        segment_column_name = getattr(self, 'segment_column_name', None)
        if segment_column_name is not None:
            raise NotImplementedError("Not implemented for segmented models.")
