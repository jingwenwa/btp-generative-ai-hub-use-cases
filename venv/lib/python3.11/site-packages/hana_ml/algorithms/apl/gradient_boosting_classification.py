# pylint:disable=too-many-lines
"""
This module provides the SAP HANA APL gradient boosting classification algorithm.

The following classes are available:

    * :class:`GradientBoostingBinaryClassifier`
    * :class:`GradientBoostingClassifier`
"""

import logging
import pandas as pd
from hana_ml.dataframe import DataFrame, create_dataframe_from_pandas, quotename
from hana_ml.ml_base import ListOfStrings
from hana_ml.ml_exceptions import FitIncompleteError
from hana_ml.algorithms.apl.gradient_boosting_base import GradientBoostingBase
from hana_ml.visualizers.model_report import _UnifiedClassificationReportBuilder
from hana_ml.visualizers.report_builder import Page, ChartItem

logger = logging.getLogger(__name__) #pylint: disable=invalid-name


class _GradientBoostingClassifierBase(GradientBoostingBase, _UnifiedClassificationReportBuilder):
    """
    Abstract class for SAP HANA APL Gradient Boosting Classifier algorithm
    """

    def __init__(self,
                 conn_context=None,
                 early_stopping_patience=None,
                 eval_metric=None,
                 learning_rate=None,
                 max_depth=None,
                 max_iterations=None,
                 number_of_jobs=None,
                 variable_storages=None,
                 variable_value_types=None,
                 variable_missing_strings=None,
                 extra_applyout_settings=None,
                 ** other_params): #pylint: disable=too-many-arguments
        _UnifiedClassificationReportBuilder.__init__(self, ["KEY", "VALUE"], ["KEY", "VALUE"])
        self.auto_metric_sampling = False

        GradientBoostingBase.__init__(
            self,
            conn_context=conn_context,
            early_stopping_patience=early_stopping_patience,
            eval_metric=eval_metric,
            learning_rate=learning_rate,
            max_depth=max_depth,
            max_iterations=max_iterations,
            number_of_jobs=number_of_jobs,
            variable_storages=variable_storages,
            variable_value_types=variable_value_types,
            variable_missing_strings=variable_missing_strings,
            extra_applyout_settings=extra_applyout_settings,
            ** other_params)

        # For classification, the target variable must be nominal
        self._force_target_var_type = 'nominal'

    def set_params(self, **parameters):
        """
        Sets attributes of the current model.

        Parameters
        ----------
        parameters: dict
            The names and values of the attributes to change
        """
        if 'extra_applyout_settings' in parameters:
            param = self._arg(
                'extra_applyout_settings',
                parameters.pop('extra_applyout_settings'),
                dict)
            self.extra_applyout_settings = param
        return super(_GradientBoostingClassifierBase, self).set_params(**parameters)

    def predict(self, data, prediction_type=None):
        """
        Makes predictions with the fitted model.
        It is possible to add special outputs, such as variable individual contributions,
        through the 'prediction_type' parameter.

        Parameters
        ----------
        data: hana_ml DataFrame
            The input dataset used for prediction
        prediction_type: string, optional
            Can be:
            - 'BestProbabilityAndDecision': return the probability value associated with the classification decision (default)
            - 'Decision': return the classification decision
            - 'Probability': return the probability that the row is a positive target (in binary classification) or the probabilities of all classes (in multiclass classification)
            - 'Score': return raw prediction scores
            - 'Individual Contributions': return SHAP values
            - 'Explanations': return strength indicators based on SHAP values

        Returns
        -------
        Prediction output: hana_ml DataFrame
        """
        if not self.model_:
            raise FitIncompleteError("Model not initialized. Perform a fit first.")
        if not self.label:
            raise ValueError('Label is unknown. Please specify the label parameter of the model.')
        # APPLY_CONFIG
        extra_applyout_settings = None
        if prediction_type is not None:
            extra_applyout_settings = {'APL/ApplyExtraMode': prediction_type}
        elif self.extra_applyout_settings:
            extra_applyout_settings = self.extra_applyout_settings.copy()
        apply_config_data_df = None
        if extra_applyout_settings is not None:
            if extra_applyout_settings.get('APL/ApplyExtraMode', False):
                extra_mode = extra_applyout_settings.get('APL/ApplyExtraMode')
                if extra_mode in ('Probability', 'AllProbabilities'):
                    # decision and probabilities of all classes
                    # There is no true APL Alias yet for this feature
                    apply_config_data_df = pd.DataFrame([
                        ('APL/ApplyExtraMode', 'Advanced Apply Settings', None),
                        ('APL/ApplyDecision', 'true', None),
                        ('APL/ApplyProbability', 'true', None),
                        ('APL/ApplyPredictedValue', 'false', None)
                    ])
                elif extra_mode == 'Individual Contributions':
                    apply_config_data_df = self._get_indiv_contrib_applyconf()
                elif extra_mode == 'Explanations':
                    apply_config_data_df = pd.DataFrame([
                        ('APL/ApplyExtraMode', 'Advanced Apply Settings', None),
                        ('APL/ApplyDecision', 'true', None),
                        ('APL/UnpivotedExplanations', 'true', None),
                        ('APL/ApplyPredictedValue', 'false', None)
                    ])
                # Free advanced settings
                elif extra_mode == 'Advanced Apply Settings':
                    cfg_vals = [('APL/ApplyExtraMode', 'Advanced Apply Settings', None)]
                    for alias, param_val in extra_applyout_settings.items():
                        if alias != 'APL/ApplyExtraMode':
                            cfg_vals.append((alias, param_val, None))
                    apply_config_data_df = pd.DataFrame(cfg_vals)
                if apply_config_data_df is None:
                    # any other APL/ApplyExtraMode
                    apply_config_data_df = pd.DataFrame([('APL/ApplyExtraMode', extra_mode, None)])
        if apply_config_data_df is None:
            # Default config: decision + best_proba
            apply_config_data_df = pd.DataFrame([('APL/ApplyExtraMode', 'BestProbabilityAndDecision', None)])
        applyout_df = self._predict(data=data, apply_config_data_df=apply_config_data_df)
        return self._rewrite_applyout_df(data=data, applyout_df=applyout_df)

    def _get_indiv_contrib_applyconf(self):
        """
        Gets the apply configuration for 'Individual Contributions' output.
        Returns
        -------
        A pandas dataframe for operation_config table
        """
        raise NotImplementedError()  # to be implemented by subclass

    def _score_with_applyout(self, data):
        applyout_df = self.predict(data)

        # Find the name of the label column in applyout table
        true_label_col = 'TRUE_LABEL'
        pred_label_col = 'PREDICTED'

        # Check if the label column is given in input dataset (true label)
        # If it is not present, score can't be calculated
        if true_label_col not in applyout_df.columns:
            raise ValueError("Cannot find true label column in dataset")
        if pred_label_col not in applyout_df.columns:
            raise ValueError('Cannot find the PREDICTED column in the output of predict().'
                             ' Please check the "extra_applyout_settings" parameter.')

        correct_predictions = applyout_df.filter(f'{true_label_col}={pred_label_col}')
        return correct_predictions.count() / float(applyout_df.count())  # (TP + TN) / Total

    def _rewrite_applyout_df(self, data, applyout_df): #pylint:disable=too-many-branches
        """
        Rewrites the applyout dataframe so it outputs standardized column names.
        """

        # Determines the mapping old columns to new columns
        # Stores the mapping into different list of tuples [(old_column, new_columns)]
        cols_map = []
        # indices of the starting columns. That will be used to well order the output columns
        i_id = None
        i_true_label = None
        i_predicted = None
        for i, old_col in enumerate(applyout_df.columns):
            if i == 0:
                # key column
                new_col = old_col
                cols_map.append((old_col, new_col))
                i_id = i
            elif old_col == self.label:
                new_col = 'TRUE_LABEL'
                cols_map.append((old_col, new_col))
                i_true_label = i
            elif old_col == 'gb_decision_{LABEL}'.format(LABEL=self.label):
                new_col = 'PREDICTED'
                cols_map.append((old_col, new_col))
                i_predicted = i
            else:
                # Proba of the predicted
                found = self._get_new_column_name(
                    old_col_re=r'gb_best_proba_{LABEL}'.format(LABEL=self.label),
                    old_col=old_col,
                    new_col_re=r'PROBABILITY')
                if found:
                    new_col = found
                    cols_map.append((old_col, new_col))
                else:
                    # If vector of proba
                    found = self._get_new_column_name(
                        old_col_re=r'gb_proba_{LABEL}_(.+)'.format(LABEL=self.label),
                        old_col=old_col,
                        new_col_re=r'PROBA_\1')
                    if found:
                        new_col = found
                        cols_map.append((old_col, new_col))
                    else:
                        new_col = old_col
                        cols_map.append((old_col, new_col))
        # Indices of the columns to be displayed as first columns, in a certain order
        first_cols_ix = [i for i in [i_id, i_true_label, i_predicted] if i is not None]
        # Writes the select SQL by renaming the columns
        sql = ''
        # Starting columns
        for i in first_cols_ix:
            old_col, new_col = cols_map[i]
            # If the target var is not given in applyin, do not output it
            if old_col == self.label and old_col not in data.columns:
                continue
            if sql:
                sql = sql + ', '
            sql = (sql + '{old_col} {new_col}'.format(
                old_col=quotename(old_col),
                new_col=quotename(new_col)))
        # Remaining columns
        for i, (old_col, new_col) in enumerate(cols_map):
            if i not in first_cols_ix:
                sql = sql + ', '
                sql = (sql + '{old_col} {new_col}'.format(
                    old_col=quotename(old_col),
                    new_col=quotename(new_col)))
        sql = 'SELECT ' +  sql + ' FROM ' + self.applyout_table_.name
        applyout_df_new = DataFrame(connection_context=self.conn_context,
                                    select_statement=sql)
        logger.info('DataFrame for predict ouput: %s', sql)
        return applyout_df_new

    def build_report(self):
        """
        Build model report.
        """
        GradientBoostingBase.build_report(self)

        try:
            param_table, optimal_param_table, importance_table = self._get_common_report_tables()

            confusion_matrix = \
                self.get_debrief_report('Classification_MultiClass_ConfusionMatrix') \
                .filter('"Partition" = \'Validation\'') \
                .select('Actual Class', 'Predicted Class', 'Weight') \
                .rename_columns(['ACTUAL_CLASS', 'PREDICTED_CLASS', 'COUNT']).collect()
            classes = set(confusion_matrix['ACTUAL_CLASS'].unique()) \
                .union(set(confusion_matrix['PREDICTED_CLASS'].unique()))
            zeros = []
            for class_1 in classes:
                for class_2 in classes:
                    cond = ((confusion_matrix['ACTUAL_CLASS'] == class_1)
                            & (confusion_matrix['PREDICTED_CLASS'] == class_2))
                    if confusion_matrix[cond].shape[0] == 0:
                        zeros.append((class_1, class_2, 0))
            zeros_df = pd.DataFrame(zeros, columns=['ACTUAL_CLASS', 'PREDICTED_CLASS', 'COUNT'])
            confusion_matrix = pd.concat([confusion_matrix, zeros_df], ignore_index=True)
            confusion_matrix.sort_values(['ACTUAL_CLASS', 'PREDICTED_CLASS'], inplace=True)

            # pylint: disable=protected-access
            self._set_parameter_table(param_table) \
                ._set_optimal_parameter_table(optimal_param_table) \
                ._set_variable_importance_table(importance_table) \
                ._set_confusion_matrix_table(confusion_matrix)
        except Exception as err:
            logger.error(str(err))
            raise

    def _add_local_explanations_to_report(self, max_local_explanations):
        decision_column = 'gb_decision_' + self.label
        if self.applyout_ is not None and 'Explanation_Rank' in self.applyout_.columns and decision_column in self.applyout_.columns:
            def get_group(row):
                if row['Explanation_Strength'] > 3:
                    return 0
                elif row['Explanation_Strength'] > 1:
                    return 1
                elif row['Explanation_Strength'] > 0:
                    return 2
                elif row['Explanation_Strength'] >= -1:
                    return 3
                elif row['Explanation_Strength'] >= -3:
                    return 4
                else:
                    return 5

            def get_strength(row, group):
                return row['Explanation_Strength'] if row['Explanation_Group'] == group else '-'

            apply_out = self.applyout_.collect().round(decimals=3)
            explanations_page = Page('Local Explanations')
            key_column = apply_out.columns[0]
            keys = apply_out[key_column].unique()

            for i, key in enumerate(keys):
                if i == max_local_explanations:
                    logger.warning("The number of local explanations displayed in the report is "
                                   "limited to %s. Please use the 'max_local_explanations' "
                                   "parameter of the 'build_report' method to modify this value.",
                                   str(max_local_explanations))
                    break

                explanation_df = apply_out[apply_out[key_column] == key]
                predicted_value = explanation_df[explanation_df[decision_column].notna()][decision_column].iloc[0]
                explanation_df = explanation_df[explanation_df['Explanation_Influencer'].notna()].sort_values(by='Explanation_Strength', ascending=True, ignore_index=True)
                explanation_df['Influencer Values'] = explanation_df[['Explanation_Influencer', 'Explanation_Influencer_Value']].stack().groupby(level=0).agg(' = '.join) \
                    .replace({'Negative Others = Negative Others': 'Negative Others',
                              'Positive Others = Positive Others': 'Positive Others'})

                explanation_df['Explanation_Group'] = explanation_df.apply(get_group, axis=1)

                explanation_df['Strong positive'] = explanation_df.apply(lambda row: get_strength(row, group=0), axis=1)
                explanation_df['Meaningful positive'] = explanation_df.apply(lambda row: get_strength(row, group=1), axis=1)
                explanation_df['Weak positive'] = explanation_df.apply(lambda row: get_strength(row, group=2), axis=1)
                explanation_df['Weak negative'] = explanation_df.apply(lambda row: get_strength(row, group=3), axis=1)
                explanation_df['Meaningful negative'] = explanation_df.apply(lambda row: get_strength(row, group=4), axis=1)
                explanation_df['Strong negative'] = explanation_df.apply(lambda row: get_strength(row, group=5), axis=1)

                explanations_config = {
                    'customFn': ['tooltip.formatter'],
                    'tooltip': {
                        'trigger': 'axis',
                        'axisPointer': {'type': 'none'},
                        'formatter': {
                            'params': ['params'],
                            'body': 'return params[0].axisValueLabel;'
                        }
                    },
                    'grid': {'show': 'true', 'containLabel': 'true'},
                    'legend': {'show': 'true'},
                    'xAxis': {
                        'name': 'Strength',
                        'type': 'value',
                        'axisLine': {'show': 'true'},
                        'axisTick': {'show': 'true'}
                    },
                    'yAxis': {
                        'name': 'Influencer Value',
                        'type': 'category',
                        'data': explanation_df['Influencer Values'].tolist(),
                        'axisLabel': {
                            'interval': 0
                        }
                    },
                    'series': [
                        {
                            'name': 'Strong positive',
                            'type': 'bar',
                            'stack': 's',
                            'data': explanation_df['Strong positive'].tolist(),
                            'itemStyle': {
                                'color': '#03A50E'
                            },
                            'label': {
                                'show': 'true',
                                'position': 'right',
                                'fontWeight': 'bold',
                                'color': '#60594F'
                            },
                        },
                        {
                            'name': 'Meaningful positive',
                            'type': 'bar',
                            'stack': 's',
                            'data': explanation_df['Meaningful positive'].tolist(),
                            'itemStyle': {
                                'color': '#C3E5C0'
                            },
                            'label': {
                                'show': 'true',
                                'position': 'right',
                                'fontWeight': 'bold',
                                'color': '#60594F'
                            },
                        },
                        {
                            'name': 'Weak positive',
                            'type': 'bar',
                            'stack': 's',
                            'data': explanation_df['Weak positive'].tolist(),
                            'itemStyle': {
                                'color': '#E0DEDF'
                            },
                            'label': {
                                'show': 'true',
                                'position': 'right',
                                'fontWeight': 'bold',
                                'color': '#60594F'
                            },
                        },
                        {
                            'name': 'Weak negative',
                            'type': 'bar',
                            'stack': 's',
                            'data': explanation_df['Weak negative'].tolist(),
                            'itemStyle': {
                                'color': '#E0DEDF'
                            },
                            'label': {
                                'show': 'true',
                                'position': 'left',
                                'fontWeight': 'bold',
                                'color': '#60594F'
                            },
                        },
                        {
                            'name': 'Meaningful negative',
                            'type': 'bar',
                            'stack': 's',
                            'data': explanation_df['Meaningful negative'].tolist(),
                            'itemStyle': {
                                'color': '#F4C784'
                            },
                            'label': {
                                'show': 'true',
                                'position': 'left',
                                'fontWeight': 'bold',
                                'color': '#60594F'
                            },
                        },
                        {
                            'name': 'Strong negative',
                            'type': 'bar',
                            'stack': 's',
                            'data': explanation_df['Strong negative'].tolist(),
                            'itemStyle': {
                                'color': '#F70D04'
                            },
                            'label': {
                                'show': 'true',
                                'position': 'left',
                                'fontWeight': 'bold',
                                'color': '#60594F'
                            },
                        }
                    ],
                    'toolbox': {
                        'feature': {
                            'saveAsImage': {
                                'name': 'explanations_' + str(key)
                            }
                        }
                    },
                }
                chart = ChartItem(key_column + ' = ' + str(key) + ' - Prediction = ' + str(predicted_value), explanations_config)
                explanations_page.addItem(chart)

            self._report_builder.addPage(explanations_page)
            self._report_builder.build()

class GradientBoostingClassifier(_GradientBoostingClassifierBase):
    """
    SAP HANA APL Gradient Boosting Multiclass Classifier algorithm.

    Parameters
    ----------
    conn_context :  ConnectionContext, optional
        The connection object to an SAP HANA database.
        This parameter is not needed anymore.
        It will be set automatically when a dataset is used in fit() or predict().
    early_stopping_patience: int, optional
        If the performance does not improve after **early_stopping_patience iterations**,
        the model training will stop before reaching **max_iterations**.
        Please refer to APL documentation for default value.
    eval_metric: str, optional
        The name of the metric used to evaluate the model performance on validation dataset along
        the boosting iterations.
        The possible values are 'MultiClassClassificationError' and 'MultiClassLogLoss'.
        Please refer to APL documentation for default value..
    learning_rate: float, optional
        The weight parameter controlling the model regularization to avoid overfitting risk.
        A small value improves the model generalization to unseen dataset at the expense of the
        computational cost.
        Please refer to APL documentation for default value.
    max_depth: int, optional
        The maximum depth of the decision trees added as a base learner to the model at each
        boosting iteration.
        Please refer to APL documentation for default value.
    max_iterations: int, optional
        The maximum number of boosting iterations to fit the model.
        The default value is 1000.
    number_of_jobs: int, optional
        Deprecated.
    variable_storages: dict, optional
        Specifies the variable data types (string, integer, number).
        For example, {'VAR1': 'string', 'VAR2': 'number'}.
        See notes below for more details.
    variable_value_types: dict, optional
        Specifies the variable value type (continuous, nominal, ordinal).
        For example, {'VAR1': 'continuous', 'VAR2': 'nominal'}.
        See notes below for more details.
    variable_missing_strings: dict, optional
        Specifies the variable values that will be taken as missing.
        For example, {'VAR1': '???'} means anytime the variable value equals to '???',
        it will be taken as missing.
    extra_applyout_settings: dict, optional
        Determines the output of the predict() method.
        The possible values are:

        - By default (None value): the default output.

            - <KEY>: the key column if it provided in the dataset
            - TRUE_LABEL: the class label if provided in the dataset
            - PREDICTED: the predicted label
            - PROBABILITY: the probability of the prediction (confidence)

        - {'APL/ApplyExtraMode': 'AllProbabilities'}: the probabilities for each class.

            - <KEY>: the key column if provided in the dataset
            - TRUE_LABEL: the class label if given in the dataset
            - PREDICTED: the predicted label
            - PROBA_<label_value1>: the probability for the class <label_value1>
            - ...
            - PROBA_<label_valueN>: the probability for the class <label_valueN>

        - {'APL/ApplyExtraMode': 'Individual Contributions'}: the feature importance for every
          sample

            - <KEY>: the key column if provided in the dataset
            - TRUE_LABEL: the class label when if provided in the dataset
            - PREDICTED: the predicted label
            - gb_contrib_<VAR1>: the contribution of the variable VAR1 to the score
            - ...
            - gb_contrib_<VARN>: the contribution of the variable VARN to the score
            - gb_contrib_constant_bias: the constant bias contribution to the score

    other_params: dict optional
        Corresponds to advanced settings.
        The dictionary contains {<parameter_name>: <parameter_value>}.
        The possible parameters are:

        - 'max_tasks'
        - 'segment_column_name'
        - 'cutting_strategy'
        - 'interactions'
        - 'interactions_max_kept'
        - 'variable_auto_selection'
        - 'variable_selection_max_nb_of_final_variables'
        - 'variable_selection_max_iterations'
        - 'variable_selection_percentage_of_contribution_kept_by_step'
        - 'variable_selection_quality_bar'

        See `Common APL Aliases for Model Training
        <https://help.sap.com/viewer/7223667230cb471ea916200712a9c682/latest/en-US/de2e28eaef79418799b9f4e588b04b53.html>`_
        in the SAP HANA APL Developer Guide.

        For 'max_tasks', see `FUNC_HEADER
        <https://help.sap.com/viewer/7223667230cb471ea916200712a9c682/latest/en-US/d8faaa27841341cbac41353d862484af.html>`_.
    other_train_apl_aliases: dict, optional
        Users can provide APL aliases as advanced settings to the model.
        Users are free to input any possible value.

        See `Common APL Aliases for Model Training
        <https://help.sap.com/viewer/7223667230cb471ea916200712a9c682/latest/en-US/de2e28eaef79418799b9f4e588b04b53.html>`_
        in the SAP HANA APL Developer Guide.


    Attributes
    ----------
    label: str
      The target column name. This attribute is set when the fit() method is called.
    model_: hana_ml DataFrame
        The trained model content
    summary_: APLArtifactTable
        The reference to the "SUMMARY" table generated by the model training.
        This table contains the content of the model training summary.
    indicators_: APLArtifactTable
        The reference to the "INDICATORS" table generated by the model training.
        This table contains various metrics related to the model and model variables.
    fit_operation_logs_: APLArtifactTable
        The reference to the "OPERATION_LOG" table generated by the model training
    var_desc_: APLArtifactTable
        The reference to the "VARIABLE_DESCRIPTION" table that was built during the model training
    applyout_: hana_ml DataFrame
        The predictions generated the last time the model was applied
    predict_operation_logs_: APLArtifactTable
        The reference to the "OPERATION_LOG" table when a prediction was made

    Examples
    --------
    >>> from hana_ml.algorithms.apl.gradient_boosting_classification \\
    ...     import GradientBoostingClassifier
    >>> from hana_ml.dataframe import ConnectionContext, DataFrame

    Connecting to SAP HANA

    >>> CONN = ConnectionContext('HDB_HOST', HDB_PORT, 'HDB_USER', 'HDB_PASS')
    >>> # -- Creates hana_ml DataFrame
    >>> hana_df = DataFrame(CONN,
                            'SELECT "id", "class", "capital-gain", '
                            '"native-country" from APL_SAMPLES.CENSUS')

    Creating and fitting the model

    >>> model = GradientBoostingClassifier()
    >>> model.fit(hana_df, label='native-country', key='id')

    Getting variable interactions

    >>> model.set_params(other_train_apl_aliases={
    ...     'APL/Interactions': 'true',
    ...     'APL/InteractionsMaxKept': '3'
    ... })
    >>> model.fit(data=self._df_train, key=self._key, label=self._label)
    >>> # Checks interaction info in INDICATORS table
    >>> output = model.get_indicators().filter("KEY LIKE 'Interaction%'").collect()

    Debriefing

    >>> # Global performance metrics of the model
    >>> model.get_performance_metrics()
    {'BalancedErrorRate': 0.9761904761904762, 'BalancedClassificationRate': 0.023809523809523808,
    ...

    >>> # Performance metrics of the model for each class
    >>> model.get_metrics_per_class()
    {'Precision': {'Cambodia': 0.0, 'Canada': 0.0, 'China': 0.0, 'Columbia': 0.0...

    >>> model.get_feature_importances()
    {'Gain': OrderedDict([('class', 0.7713800668716431), ('capital-gain', 0.22861991822719574)])}

    Generating the model report

    >>> from hana_ml.visualizers.unified_report import UnifiedReport
    >>> UnifiedReport(model).build().display()

    Making predictions

    >>> # Default output
    >>> applyout_df = model.predict(hana_df)
    >>> applyout_df.collect().head(3) # returns the output as a pandas DataFrame
        id     TRUE_LABEL      PREDICTED  PROBABILITY
    0   30  United-States  United-States     0.89051
    1   63  United-States  United-States     0.89051
    2   66  United-States  United-States     0.89051
    >>> # All probabilities
    >>> model.set_params(extra_applyout_settings={'APL/ApplyExtraMode': 'AllProbabilities'})
    >>> applyout_df = model.predict(hana_df)
    >>> applyout_df.collect().head(3) # returns the output as a pandas DataFrame
              id     TRUE_LABEL      PREDICTED      PROBA_?     PROBA_Cambodia  ...
    35194  19272  United-States  United-States    0.016803            0.000595  ...
    20186  39624  United-States  United-States    0.017564            0.001063  ...
    43892  38759  United-States  United-States    0.019812            0.000353  ...
    >>> # Individual Contributions
    >>> model.set_params(extra_applyout_settings={'APL/ApplyExtraMode': 'Individual Contributions'})
    >>> applyout_df = model.predict(hana_df)
    >>> applyout_df.collect().head(3) # returns the output as a pandas DataFrame
       id     TRUE_LABEL      PREDICTED  gb_contrib_class  gb_contrib_capital-gain  ...
    0  30  United-States  United-States         -0.025366                -0.014416  ...
    1  63  United-States  United-States         -0.025366                -0.014416  ...
    2  66  United-States  United-States         -0.025366                -0.014416  ...

    Saving the model in the schema named 'MODEL_STORAGE'

    >>> from hana_ml.model_storage import ModelStorage
    >>> model_storage = ModelStorage(connection_context=CONN, schema='MODEL_STORAGE')
    >>> model.name = 'My model name'
    >>> model_storage.save_model(model=model, if_exists='replace')

    Reloading the model for new predictions

    >>> model2 = model_storage.load_model(name='My model name')
    >>> out2 = model2.predict(data=hana_df)

    Please see model_storage class for further features of model storage

    Exporting the model in JSON format

    >>> json_export = model.export_apply_code('JSON')

    APL provides a JavaScript runtime in which you can make predictions based on any model that
    has been exported in JSON format. See *JavaScript Runtime* in the `SAP HANA APL Developer
    Guide <https://help.sap.com/viewer/p/apl>`_.

    Notes
    -----
    It is highly recommended to specify a key column in the training dataset.
    If not, once the model is trained, it won't be possible anymore to have a key defined in
    any input dataset. The key is particularly useful to join the predictions output to
    the input dataset.

    By default, if not provided, SAP HANA APL guesses the variable description by reading
    the first 100 rows. But, the results may be incorrect.
    The user can overwrite the guessed description by explicitly setting the variable_storages,
    variable_value_types and variable_missing_strings parameters. For example:
    ::

        model.set_params(variable_storages={
            'ID': 'integer',
            'sepal length (cm)': 'number'
        })
        model.set_params(variable_value_types={
            'sepal length (cm)': 'continuous'
        })
        model.set_params(variable_missing_strings={
            'sepal length (cm)': '-1'
        })
    """

    APL_ALIAS_KEYS = {
        'cutting_strategy': 'APL/CuttingStrategy',
        'early_stopping_patience': 'APL/EarlyStoppingPatience',
        'eval_metric': 'APL/EvalMetric',
        'learning_rate': 'APL/LearningRate',
        'max_depth': 'APL/MaxDepth',
        'max_iterations': 'APL/MaxIterations',
        'number_of_jobs': 'APL/NumberOfJobs',
        'interactions': 'APL/Interactions',
        'interactions_max_kept': 'APL/InteractionsMaxKept',
        'variable_auto_selection': 'APL/VariableAutoSelection',
        'variable_selection_max_nb_of_final_variables':
            'APL/VariableSelectionMaxNbOfFinalVariables',
        'variable_selection_max_iterations': 'APL/VariableSelectionMaxIterations',
        'variable_selection_percentage_of_contribution_kept_by_step':
            'APL/VariableSelectionPercentageOfContributionKeptByStep',
        'variable_selection_quality_bar': 'APL/VariableSelectionQualityBar',
        'segment_column_name': 'APL/SegmentColumnName'
    }
    # pylint: disable=too-many-arguments
    def __init__(self,
                 conn_context=None,
                 early_stopping_patience=None,
                 eval_metric=None,
                 learning_rate=None,
                 max_depth=None,
                 max_iterations=None,
                 number_of_jobs=None,
                 variable_storages=None,
                 variable_value_types=None,
                 variable_missing_strings=None,
                 extra_applyout_settings=None,
                 ** other_params):
        super(GradientBoostingClassifier, self).__init__(
            conn_context=conn_context,
            early_stopping_patience=early_stopping_patience,
            eval_metric=eval_metric,
            learning_rate=learning_rate,
            max_depth=max_depth,
            max_iterations=max_iterations,
            number_of_jobs=number_of_jobs,
            variable_storages=variable_storages,
            variable_value_types=variable_value_types,
            variable_missing_strings=variable_missing_strings,
            extra_applyout_settings=extra_applyout_settings,
            ** other_params)
        self._model_type = 'multiclass'

    def set_params(self, **parameters):
        """
        Sets attributes of the current model.

        Parameters
        ----------
        parameters: dict
            The names and values of the attributes to change
        """
        if 'eval_metric' in parameters:
            val = parameters.pop('eval_metric')
            valid_vals = ['MultiClassLogLoss', 'MultiClassClassificationError']
            if val not in valid_vals:
                raise ValueError("Invalid eval_metric. The value must be among " + str(valid_vals))
            self.eval_metric = self._arg('eval_metric', val, str)
        return super(GradientBoostingClassifier, self).set_params(**parameters)

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

        key = self._arg('key', key, str)
        features = self._arg('features', features, ListOfStrings)
        label = self._arg('label', label, str)
        weight = self._arg('weight', weight, str)
        self._check_valid(data, key, features, label, weight)

        n_classes = len(data.distinct(cols=label).collect())
        if n_classes == 2:
            logger.warning("The label column has exactly two distinct values. "
                           "Please consider using the 'GradientBoostingBinaryClassifier' class.")

        return super(GradientBoostingClassifier, self).fit(data, key, features, label, weight, build_report)

    def score(self, data):
        """
        Returns the accuracy score on the provided test dataset.

        Parameters
        ----------
        data: hana_ml DataFrame
            The test dataset used to compute the score.
            The labels must be provided in the dataset.

        Returns
        -------
            Float or pandas DataFrame
                If no segment column is given, the accuracy score.

                If a segment column is given, a pandas DataFrame which contains the accuracy score
                for each segment.
        """
        if self._get_apl_version() >= 2303:
            self._score(data)
            confusion_matrix = self.get_debrief_report('Classification_MultiClass_ConfusionMatrix').collect()
            confusion_matrix = confusion_matrix[confusion_matrix['Partition'] == 'ApplyIn']
            segment_column_name = getattr(self, 'segment_column_name', None)
            if segment_column_name is not None:
                accuracy_by_segment = []
                for segment in confusion_matrix['Oid'].unique():
                    segment_matrix = confusion_matrix[confusion_matrix['Oid'] == segment]
                    weight_sum = segment_matrix['Weight'].sum()
                    correct_weight_sum = segment_matrix[segment_matrix['Actual Class'] == segment_matrix['Predicted Class']]['Weight'].sum()
                    accuracy_by_segment.append((segment, correct_weight_sum / weight_sum))
                return pd.DataFrame(accuracy_by_segment, columns=[segment_column_name, 'Accuracy'])
            else:
                weight_sum = confusion_matrix['Weight'].sum()
                correct_weight_sum = confusion_matrix[confusion_matrix['Actual Class'] == confusion_matrix['Predicted Class']]['Weight'].sum()
                return correct_weight_sum / weight_sum
        else:
            return self._score_with_applyout(data)

    def get_metrics_per_class(self):
        """
        Returns the performance for each class.

        Returns
        -------

        Dictionary or pandas DataFrame
            If no segment column is given, a dictionary.

            If a segment column is given, a pandas DataFrame.

        Examples
        --------

        >>> data = DataFrame(conn, 'SELECT * from IRIS_MULTICLASSES')
        >>> model = GradientBoostingClassifier(conn)
        >>> model.fit(data=data, key='ID', label='LABEL')
        >>> model.get_metrics_per_class()
        {
        'Precision': {
            'setosa': 1.0,
            'versicolor': 1.0,
            'virginica': 0.9743589743589743
        },
        'Recall': {
            'setosa': 1.0,
            'versicolor': 0.9714285714285714,
            'virginica': 1.0
        },
        'F1Score': {
            'setosa': 1.0,
            'versicolor': 0.9855072463768115,
            'virginica': 0.9870129870129869
        }
        """
        if not hasattr(self, 'indicators_'):
            raise FitIncompleteError(
                "The indicators table was not found. Please fit the model.")
        cond = "VARIABLE like 'gb_%' and DETAIL is not null and to_varchar(DETAIL)!='?'"
        df_ind = self.indicators_.filter(cond).collect()
        segment_column = getattr(self, 'segment_column_name', None)
        if segment_column is None:
            # Create the dictionary to be returned
            # converts df_ind.VALUE str to float if possible
            ret2 = {}  # 2 levels dictionary {metrics/classes: value}
            for _, row in df_ind.iterrows():
                metric_name = row.loc['KEY']
                old_v = row.loc['VALUE']
                class_v = row.loc['DETAIL']
                try:
                    new_v = float(old_v)
                except ValueError:
                    new_v = old_v
                class_v = str(class_v)
                per_class = ret2.get(metric_name, {})
                per_class[class_v] = new_v
                ret2[metric_name] = per_class
            return ret2
        else:
            df_ind = df_ind[['OID', 'KEY', 'DETAIL', 'VALUE']]
            df_ind.columns = [segment_column, 'Metric', 'Class', 'Value']
            df_ind.sort_values(by=[segment_column, 'Metric', 'Class'], inplace=True,
                               ignore_index=True)
            return df_ind

    def _get_indiv_contrib_applyconf(self):
        """
        Gets the apply configuration for 'Individual Contributions' output.
        Returns
        -------
        A pandas dataframe for operation_config table
        """

        return pd.DataFrame([('APL/ApplyExtraMode', 'Individual Contributions', None)])

    def build_report(self, max_local_explanations=100):
        """
        Build model report.

        Parameters
        ----------
        max_local_explanations: int, optional
            The maximum number of local explanations displayed in the report.
        """
        _GradientBoostingClassifierBase.build_report(self)

        try:
            perf_report = self.get_debrief_report('ClassificationRegression_Performance')
            stat_per_class_report = self.get_debrief_report('Classification_MultiClass_Performance_by_Class')
            target_stat_report = self.get_debrief_report('MultiClassTarget_Statistics')

            stat_tables = {}
            for partition in ['Validation', 'ApplyIn']:
                if partition in perf_report.select('Partition').collect().values:
                    stat_table = perf_report \
                        .filter('"Partition" = \'{}\' and "Indicator" != \'Prediction Confidence\''.format(partition)) \
                        .select('Indicator', 'Value').rename_columns(['STAT_NAME', 'STAT_VALUE']).collect()
                    stat_table['CLASS_NAME'] = None

                    stat_per_class_table = stat_per_class_report \
                        .filter('"Partition" = \'{}\''.format(partition)).collect()
                    stat_per_class_table = stat_per_class_table[['Indicator', 'Value', 'Class']]
                    stat_per_class_table.rename(columns={'Indicator': 'STAT_NAME',
                                                         'Value': 'STAT_VALUE',
                                                         'Class': 'CLASS_NAME'}, inplace=True)
                    stat_per_class_table.replace(to_replace={'Precision': 'PRECISION',
                                                             'Recall': 'RECALL',
                                                             'F1 Score': 'F1_SCORE'}, inplace=True)

                    target_stats = target_stat_report \
                        .filter('"Partition" = \'{}\''.format(partition)).select('% Weight', 'Category') \
                        .rename_columns(['STAT_VALUE', 'CLASS_NAME']).collect()
                    target_stats['STAT_VALUE'] = \
                        target_stats['STAT_VALUE'].apply(lambda x: float("{:.3f}".format(float(x) / 100.)))
                    target_stats['STAT_NAME'] = 'SUPPORT'
                    target_stats = target_stats[target_stats.STAT_VALUE != 0]

                    stat_table = pd.concat([stat_table, stat_per_class_table, target_stats],
                                           ignore_index=True)
                    stat_tables[partition] = stat_table

            # pylint: disable=protected-access
            self._set_statistic_table(stat_tables['Validation'])
            if 'ApplyIn' in stat_tables:
                self._set_scoring_statistic_table(stat_tables['ApplyIn'])
            self._render_report()
            self._add_interaction_matrix_to_report(self._report_builder)
            self._add_local_explanations_to_report(max_local_explanations)
        except Exception as err:
            logger.error(str(err))
            raise

    def set_metric_samplings(self, roc_sampling=None, other_samplings: dict = None):

        raise NotImplementedError('Not implemented for multiclass classification.')

class GradientBoostingBinaryClassifier(_GradientBoostingClassifierBase):
    """
    SAP HANA APL Gradient Boosting Binary Classifier algorithm.
    It is very similar to GradientBoostingClassifier, the multiclass classifier.
    Its particularity lies in the provided metrics which are specific to binary classification.

    Parameters
    ----------
    conn_context :  ConnectionContext, optional
        The connection object to an SAP HANA database.
        This parameter is not needed anymore.
        It will be set automatically when a dataset is used in fit() or predict().
    early_stopping_patience: int, optional
        If the performance does not improve after **early_stopping_patience iterations**,
        the model training will stop before reaching **max_iterations**.
        Please refer to APL documentation for default value.
    eval_metric: str, optional
        The name of the metric used to evaluate the model performance on validation dataset along
        the boosting iterations.
        The possible values are 'LogLoss','AUC' and 'ClassificationError'.
        Please refer to APL documentation for default value.
    learning_rate: float, optional
        The weight parameter controlling the model regularization to avoid overfitting risk.
        A small value improves the model generalization to unseen dataset at the expense of the
        computational cost.
        Please refer to APL documentation for default value.
    max_depth: int, optional
        The maximum depth of the decision trees added as a base learner to the model at each
        boosting iteration.
        The default value is 4.
    max_iterations: int, optional
        The maximum number of boosting iterations to fit the model.
        Please refer to APL documentation for default value.
    number_of_jobs: int, optional
        Deprecated.
    variable_storages: dict, optional
        Specifies the variable data types (string, integer, number).
        For example, {'VAR1': 'string', 'VAR2': 'number'}.
        See notes below for more details.
    variable_value_types: dict, optional
        Specifies the variable value type (continuous, nominal, ordinal).
        For example, {'VAR1': 'continuous', 'VAR2': 'nominal'}.
        See notes below for more details.
    variable_missing_strings: dict, optional
        Specifies the variable values that will be taken as missing.
        For example, {'VAR1': '???'} means anytime the variable value equals to '???',
        it will be taken as missing.
    extra_applyout_settings: dict, optional
        Determines the output of the predict() method.
        The possible values are:

        - By default (None value): the default output.

            - <KEY>: the key column if provided in the dataset
            - TRUE_LABEL: the class label if provided in the dataset
            - PREDICTED: the predicted label
            - PROBABILITY: the probability of the prediction (confidence)

        - {'APL/ApplyExtraMode': 'Individual Contributions'}: the individual contributions of each
          variable to the score. The output is:

            - <KEY>: the key column if provided in the dataset
            - TRUE_LABEL: the class label if provided in the dataset
            - gb_contrib_<VAR1>: the contribution of the variable VAR1 to the score
            - ...
            - gb_contrib_<VARN>: the contribution of the variable VARN to the score
            - gb_contrib_constant_bias: the constant bias contribution to the score

    other_params: dict optional
        Corresponds to advanced settings.
        The dictionary contains {<parameter_name>: <parameter_value>}.
        The possible parameters are:

        - 'max_tasks'
        - 'segment_column_name'
        - 'correlations_lower_bound'
        - 'correlations_max_kept'
        - 'cutting_strategy'
        - 'target_key'
        - 'interactions'
        - 'interactions_max_kept'
        - 'variable_auto_selection'
        - 'variable_selection_max_nb_of_final_variables'
        - 'variable_selection_max_iterations'
        - 'variable_selection_percentage_of_contribution_kept_by_step'
        - 'variable_selection_quality_bar'

        See `Common APL Aliases for Model Training
        <https://help.sap.com/viewer/7223667230cb471ea916200712a9c682/latest/en-US/de2e28eaef79418799b9f4e588b04b53.html>`_
        in the SAP HANA APL Developer Guide.

        For 'max_tasks', see `FUNC_HEADER
        <https://help.sap.com/viewer/7223667230cb471ea916200712a9c682/latest/en-US/d8faaa27841341cbac41353d862484af.html>`_.
    other_train_apl_aliases: dict, optional
        Contains the APL alias for model training.
        The list of possible aliases depends on the APL version.

        See `Common APL Aliases for Model Training
        <https://help.sap.com/viewer/7223667230cb471ea916200712a9c682/latest/en-US/de2e28eaef79418799b9f4e588b04b53.html>`_
        in the SAP HANA APL Developer Guide.

    Attributes
    ----------
    label: str
      The target column name. This attribute is set when the fit() method is called.
    model_: hana_ml DataFrame
        The trained model content
    summary_: APLArtifactTable
        The reference to the "SUMMARY" table generated by the model training.
        This table contains the content of the model training summary.
    indicators_: APLArtifactTable
        The reference to the "INDICATORS" table generated by the model training.
        This table contains various metrics related to the model and model variables.
    fit_operation_logs_: APLArtifactTable
        The reference to the "OPERATION_LOG" table generated by the model training
    var_desc_: APLArtifactTable
        The reference to the "VARIABLE_DESCRIPTION" table that was built during the model training
    applyout_: hana_ml DataFrame
        The predictions generated the last time the model was applied
    predict_operation_logs_: APLArtifactTable
        The reference to the "OPERATION_LOG" table when a prediction was made

    Examples
    --------
    >>> from hana_ml.algorithms.apl.gradient_boosting_classification \\
    ...     import GradientBoostingBinaryClassifier
    >>> from hana_ml.dataframe import ConnectionContext, DataFrame

    Connecting to SAP HANA

    >>> CONN = ConnectionContext(HDB_HOST, HDB_PORT, HDB_USER, HDB_PASS)
    >>> # -- Creates hana_ml DataFrame
    >>> hana_df = DataFrame(CONN, 'SELECT * from APL_SAMPLES.CENSUS')

    Creating and fitting the model

    >>> model = GradientBoostingBinaryClassifier()
    >>> model.fit(hana_df, label='class', key='id')

    Getting variable interactions

    >>> model.set_params(other_train_apl_aliases={
    ...     'APL/Interactions': 'true',
    ...     'APL/InteractionsMaxKept': '3'
    ... })
    >>> model.fit(data=self._df_train, key=self._key, label=self._label)
    >>> # Checks interaction info in INDICATORS table
    >>> output = model.get_indicators().filter("KEY LIKE 'Interaction%'").collect()

    Debriefing

    >>> # Global performance metrics of the model
    >>> model.get_performance_metrics()
    {'LogLoss': 0.2567069689038737, 'PredictivePower': 0.8529, 'PredictionConfidence': 0.9759, ...}

    >>> model.get_feature_importances()
    {'Gain': OrderedDict([('relationship', 0.3866586685180664),
                          ('education-num', 0.1502334326505661)...

    Generating the model report

    >>> from hana_ml.visualizers.unified_report import UnifiedReport
    >>> UnifiedReport(model).build().display()

    Making predictions

    >>> # Default output
    >>> applyout_df = model.predict(hana_df)
    >>> applyout_df.collect().sample(3) # returns the output as a pandas DataFrame
              id  TRUE_LABEL  PREDICTED  PROBABILITY
    44903  41211           0          0    0.871326
    47878  36020           1          1    0.993455
    17549   6601           0          1    0.673872

    >>> # Individual Contributions
    >>> model.set_params(extra_applyout_settings={'APL/ApplyExtraMode': 'Individual Contributions'})
    >>> applyout_df = model.predict(hana_df)
    >>> applyout_df.collect().sample(3) # returns the output as a pandas DataFrame
          id  TRUE_LABEL  gb_contrib_age  gb_contrib_workclass  gb_contrib_fnlwgt  ...
    0  18448           0       -1.098452             -0.001238           0.060850  ...
    1  18457           0       -0.731512             -0.000448           0.020060  ...
    2  18540           0       -0.024523              0.027065           0.158083  ...

    Saving the model in the schema named 'MODEL_STORAGE'.
    Please see model_storage class for further features of model storage.

    >>> from hana_ml.model_storage import ModelStorage
    >>> model_storage = ModelStorage(connection_context=CONN, schema='MODEL_STORAGE')
    >>> model.name = 'My model name'
    >>> model_storage.save_model(model=model, if_exists='replace')

    Reloading the model for new predictions

    >>> model2 = model_storage.load_model(name='My model name')
    >>> out2 = model2.predict(data=hana_df)

    Exporting the model in JSON format

    >>> json_export = model.export_apply_code('JSON')

    APL provides a JavaScript runtime in which you can make predictions based on any model that
    has been exported in JSON format. See *JavaScript Runtime* in the `SAP HANA APL Developer
    Guide <https://help.sap.com/viewer/p/apl>`_.

    """
    APL_ALIAS_KEYS = {
        'cutting_strategy': 'APL/CuttingStrategy',
        'early_stopping_patience': 'APL/EarlyStoppingPatience',
        'eval_metric': 'APL/EvalMetric',
        'learning_rate': 'APL/LearningRate',
        'max_depth': 'APL/MaxDepth',
        'max_iterations': 'APL/MaxIterations',
        'number_of_jobs': 'APL/NumberOfJobs',
        'correlations_lower_bound': 'APL/CorrelationsLowerBound',
        'correlations_max_kept': 'APL/CorrelationsMaxKept',
        'target_key': 'APL/TargetKey',
        'interactions': 'APL/Interactions',
        'interactions_max_kept': 'APL/InteractionsMaxKept',
        'variable_auto_selection': 'APL/VariableAutoSelection',
        'variable_selection_max_nb_of_final_variables':
            'APL/VariableSelectionMaxNbOfFinalVariables',
        'variable_selection_max_iterations': 'APL/VariableSelectionMaxIterations',
        'variable_selection_percentage_of_contribution_kept_by_step':
            'APL/VariableSelectionPercentageOfContributionKeptByStep',
        'variable_selection_quality_bar': 'APL/VariableSelectionQualityBar',
        'segment_column_name': 'APL/SegmentColumnName'
    }

    # pylint: disable=too-many-arguments
    def __init__(self,
                 conn_context=None,
                 early_stopping_patience=None,
                 eval_metric=None,
                 learning_rate=None,
                 max_depth=None,
                 max_iterations=None,
                 number_of_jobs=None,
                 variable_storages=None,
                 variable_value_types=None,
                 variable_missing_strings=None,
                 extra_applyout_settings=None,
                 ** other_params):
        super(GradientBoostingBinaryClassifier, self).__init__(
            conn_context=conn_context,
            early_stopping_patience=early_stopping_patience,
            eval_metric=eval_metric,
            learning_rate=learning_rate,
            max_depth=max_depth,
            max_iterations=max_iterations,
            number_of_jobs=number_of_jobs,
            variable_storages=variable_storages,
            variable_value_types=variable_value_types,
            variable_missing_strings=variable_missing_strings,
            extra_applyout_settings=extra_applyout_settings,
            ** other_params)
        self._model_type = 'binary classification'

    def set_params(self, **parameters):
        """
        Sets attributes of the current model.

        Parameters
        ----------
        parameters: dict
            The attribute names and values
        """
        if 'eval_metric' in parameters:
            val = parameters.pop('eval_metric')
            valid_vals = ['LogLoss', 'AUC', 'ClassificationError']
            if val not in valid_vals:
                raise ValueError("Invalid eval_metric. The value must be among " + str(valid_vals))
            self.eval_metric = self._arg('eval_metric', val, str)
        return super(GradientBoostingBinaryClassifier, self).set_params(**parameters)

    def _get_indiv_contrib_applyconf(self):
        """
        Gets the apply configuration for 'Individual Contributions' output.
        Settings are different depending on the subclass (multinomial classification, binary
        classification, or regression).
        Returns
        -------
        A pandas dataframe for operation_config table
        """
        return pd.DataFrame([
            ('APL/ApplyExtraMode', 'Advanced Apply Settings', None),
            ('APL/ApplyDecision', 'true', None),
            ('APL/ApplyContribution', 'all', None),
        ])

    def score(self, data):
        """
        Returns the accuracy score on the provided test dataset.

        Parameters
        ----------
        data: hana_ml DataFrame
            The test dataset used to compute the score.
            The labels must be provided in the dataset.

        Returns
        -------
            Float or pandas DataFrame
                If no segment column is given, the accuracy score.

                If a segment column is given, a pandas DataFrame which contains the accuracy score
                for each segment.
        """
        if self._get_apl_version() >= 2303:
            self._score(data)
            perfs = self.get_debrief_report('ClassificationRegression_Performance').collect()
            perfs = perfs[(perfs['Partition'] == 'ApplyIn') & (perfs['Indicator'] == 'Classification Rate')]
            segment_column_name = getattr(self, 'segment_column_name', None)
            if segment_column_name is not None:
                accuracy_by_segment = perfs[['Oid', 'Value']].reset_index(drop=True)
                accuracy_by_segment.columns = [segment_column_name, 'Accuracy']
                return accuracy_by_segment
            else:
                return perfs['Value'].iloc[0]
        else:
            return self._score_with_applyout(data)

    def build_report(self, max_local_explanations=100):
        """
        Build model report.

        Parameters
        ----------
        max_local_explanations: int, optional
            The maximum number of local explanations displayed in the report.
        """
        _GradientBoostingClassifierBase.build_report(self)

        try:
            perf_report = self.get_debrief_report('ClassificationRegression_Performance')
            target_stat_report = self.get_debrief_report('BinaryTarget_Statistics')
            roc_curve_report = self.get_debrief_report('BinaryTarget_CurveRoc')
            cumlift_curve_report = self.get_debrief_report('BinaryTarget_CurveLift')
            cumgains_curve_report = self.get_debrief_report('BinaryTarget_CurveGain')

            stat_tables = {}
            metric_tables = {}
            for partition in ['Validation', 'ApplyIn']:
                if partition in perf_report.select('Partition').collect().values:
                    stat_table = perf_report \
                        .filter('"Partition" = \'{}\' and "Indicator" != \'Prediction Confidence\''.format(partition)) \
                        .select('Indicator', 'Value').rename_columns(['STAT_NAME', 'STAT_VALUE']) \
                        .collect() \
                        .replace('Precision', 'PRECISION') \
                        .replace('Recall', 'RECALL') \
                        .replace('F1 Score', 'F1_SCORE')
                    target_stats = target_stat_report \
                        .filter('"Partition" = \'{}\''.format(partition)).collect().iloc[0]
                    target_key = target_stats['Target Key']
                    stat_table['CLASS_NAME'] = None
                    stat_table.loc[stat_table['STAT_NAME']
                                   .isin(['PRECISION', 'RECALL', 'F1_SCORE']), 'CLASS_NAME'] = target_key
                    support = int(round(target_stats['% Positive Weight'] * target_stats['Weight'] / 100.))
                    support_df = pd.DataFrame({'STAT_NAME': ['SUPPORT'],
                                               'STAT_VALUE': [support],
                                               'CLASS_NAME': [target_key]})
                    stat_table = pd.concat([stat_table, support_df], ignore_index=True)
                    stat_tables[partition] = stat_table

                    roc_curve = roc_curve_report \
                        .select('False Positive Rate', 'True Positive Rate ({})'.format(partition)) \
                        .rename_columns(['ROC_FPR', 'ROC_TPR']).collect()
                    metric_table = pd.DataFrame(({"NAME": name, "X": i, "Y": row[name]}
                                                 for i, row in roc_curve.iterrows()
                                                 for name in ['ROC_TPR', 'ROC_FPR']))
                    cumlift_curves_names = {'Lift ({})'.format(partition): 'CUMLIFT',
                                            'Random': 'RANDOM_CUMLIFT',
                                            'Wizard': 'PERF_CUMLIFT'}
                    for curve_name, curve_new_name in cumlift_curves_names.items():
                        curve = cumlift_curve_report.select('% Population', curve_name) \
                            .rename_columns(['X', 'Y']).collect()
                        curve['X'] = curve['X'].apply(lambda x: float(x) / 100.)
                        curve.insert(loc=0, column='NAME', value=curve_new_name)
                        metric_table = pd.concat([metric_table, curve], ignore_index=True)
                    # The "Cumulative Gains" SQL report was added in APL 2123
                    if self._get_apl_version() >= 2123:
                        cumgains_curves_names = {'Gain ({})'.format(partition): 'CUMGAINS',
                                                 'Random': 'RANDOM_CUMGAINS',
                                                 'Wizard': 'PERF_CUMGAINS'}
                        for curve_name, curve_new_name in cumgains_curves_names.items():
                            curve = cumgains_curve_report.select('% Population', curve_name) \
                                .rename_columns(['X', 'Y']).collect()
                            curve['X'] = curve['X'].apply(lambda x: float(x) / 100.)
                            curve.insert(loc=0, column='NAME', value=curve_new_name)
                            metric_table = pd.concat([metric_table, curve], ignore_index=True)
                    metric_table = create_dataframe_from_pandas(self.conn_context, metric_table,
                                                                '#METRICS_TABLE_{}'.format(self.id),
                                                                force=True, disable_progressbar=True)
                    metric_tables[partition] = metric_table

            # pylint: disable=protected-access
            self._set_statistic_table(stat_tables['Validation']) \
                ._set_metric_table(metric_tables['Validation'])
            if 'ApplyIn' in stat_tables:
                self._set_scoring_statistic_table(stat_tables['ApplyIn'])
                self._set_scoring_metric_table(metric_tables['ApplyIn'])
            self._render_report()
            self._add_interaction_matrix_to_report(self._report_builder)
            self._add_local_explanations_to_report(max_local_explanations)
        except Exception as err:
            logger.error(str(err))
            raise
