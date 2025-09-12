# pylint:disable=too-many-lines
"""
This module provides the SAP HANA APL gradient boosting regression algorithm.

The following classes are available:

    * :class:`GradientBoostingRegressor`
"""
import logging
import pandas as pd
from hdbcli import dbapi
from hana_ml.dataframe import DataFrame
from hana_ml.dataframe import quotename
from hana_ml.ml_exceptions import FitIncompleteError
from hana_ml.algorithms.apl.gradient_boosting_base import GradientBoostingBase
from hana_ml.ml_base import execute_logged
from hana_ml.visualizers.model_report import _UnifiedRegressionReportBuilder
from hana_ml.visualizers.report_builder import Page, ChartItem


logger = logging.getLogger(__name__) #pylint: disable=invalid-name


class GradientBoostingRegressor(GradientBoostingBase, _UnifiedRegressionReportBuilder):
    """
    SAP HANA APL Gradient Boosting Regression algorithm.

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
        The possible values are 'MAE' and 'RMSE'.
        Please refer to APL documentation for default value.
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
        Please refer to APL documentation for default value.
    number_of_jobs: int, optional
        Deprecated.
    variable_storages: dict, optional
        Specifies the variable data types (string, integer, number).
        For example, {'VAR1': 'string', 'VAR2': 'number'}.
        See notes below for more details.
    variable_value_types: dict, optional
        Specifies the variable value types (continuous, nominal, ordinal).
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
            - TRUE_LABEL: the actual value if provided
            - PREDICTED: the predicted value

        - {'APL/ApplyExtraMode': 'Individual Contributions'}: the feature importance for every
          sample

            - <KEY>: the key column if provided
            - TRUE_LABEL: the actual value if provided
            - PREDICTED: the predicted value
            - gb_contrib_<VAR1>: the contribution of the VAR1 variable to the score
            - ...
            - gb_contrib_<VARN>: the contribution of the VARN variable to the score
            - gb_contrib_constant_bias: the constant bias contribution

    other_params: dict optional
        Corresponds to advanced settings.
        The dictionary contains {<parameter_name>: <parameter_value>}.
        The possible parameters are:

        - 'max_tasks'
        - 'segment_column_name'
        - 'correlations_lower_bound'
        - 'correlations_max_kept'
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
        Users don't need to set it explicitly, except if the model is loaded from a table.
        In this case, this attribute must be set before calling predict().
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
    >>> from hana_ml.algorithms.apl.gradient_boosting_regression import GradientBoostingRegressor
    >>> from hana_ml.dataframe import ConnectionContext, DataFrame

    Connecting to SAP HANA

    >>> CONN = ConnectionContext('HDB_HOST', HDB_PORT, 'HDB_USER', 'HDB_PASS')
    >>> # -- Creates hana_ml DataFrame
    >>> hana_df = DataFrame(CONN,
    ...                     'SELECT "id", "class", "capital-gain", '
    ...                     '"native-country", "age" from APL_SAMPLES.CENSUS')

    Creating and fitting the model

    >>> model = GradientBoostingRegressor()
    >>> model.fit(hana_df, label='age', key='id')

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
    {'L1': 7.31774, 'MeanAbsoluteError': 7.31774, 'L2': 9.42497, 'RootMeanSquareError': 9.42497, ...

    >>> model.get_feature_importances()
    {'Gain': OrderedDict([('class', 0.8728259801864624), ('capital-gain', 0.10493823140859604), ...

    Generating the model report

    >>> from hana_ml.visualizers.unified_report import UnifiedReport
    >>> UnifiedReport(model).build().display()

    Making predictions

    >>> # Default output
    >>> applyout_df = model.predict(hana_df)
    >>> applyout_df.collect().head(3) # returns the output as a pandas DataFrame
              id  TRUE_LABEL  PREDICTED
    39184  21772          27         25
    16537   7331          33         43
    7908   35226          65         42
    >>> # Individual Contributions
    >>> model.set_params(extra_applyout_settings={'APL/ApplyExtraMode': 'Individual Contributions'})
    >>> applyout_df = model.predict(hana_df)
    >>> applyout_df.collect().head(3) # returns the output as a pandas DataFrame
         id  TRUE_LABEL  gb_contrib_workclass  gb_contrib_fnlwgt  gb_contrib_education  ...
    0  6241          21             -1.330736          -0.385088              0.373539  ...
    1  6248          18             -0.784536          -2.191791             -1.788672  ...
    2  6253          26             -0.773891           0.358133             -0.185864  ...

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
        'early_stopping_patience': 'APL/EarlyStoppingPatience',
        'eval_metric': 'APL/EvalMetric',
        'learning_rate': 'APL/LearningRate',
        'max_depth': 'APL/MaxDepth',
        'max_iterations': 'APL/MaxIterations',
        'number_of_jobs': 'APL/NumberOfJobs',
        'correlations_lower_bound': 'APL/CorrelationsLowerBound',
        'correlations_max_kept': 'APL/CorrelationsMaxKept',
        'cutting_strategy': 'APL/CuttingStrategy',
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
        _UnifiedRegressionReportBuilder.__init__(self, ["KEY", "VALUE"], ["KEY", "VALUE"])

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

        self._model_type = 'regression'
        self._force_target_var_type = 'continuous'

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
            valid_vals = ['RMSE', 'MAE']
            if val not in valid_vals:
                raise ValueError("Invalid eval_metric. The value must be among " + str(valid_vals))
            self.eval_metric = self._arg('eval_metric', val, str)
        return super(GradientBoostingRegressor, self).set_params(**parameters)

    def predict(self, data, prediction_type=None):
        """
        Generates predictions with the fitted model.
        It is possible to add special outputs, such as variable individual contributions,
        through the 'prediction_type' parameter.

        Parameters
        ----------
        data: hana_ml DataFrame
            The input dataset used for prediction
        prediction_type: string, optional
            Can be:
            - 'Score': return predicted value (default)
            - 'Individual Contributions': return SHAP values
            - 'Explanations': return strength indicators based on SHAP values

        Returns
        -------
        Prediction output: hana_ml DataFrame

        """
        if not self.model_:
            raise FitIncompleteError("Model not initialized. Perform a fit first.")
        # APPLY_CONFIG
        extra_applyout_settings = None
        if prediction_type is not None:
            extra_applyout_settings = {'APL/ApplyExtraMode': prediction_type}
        elif self.extra_applyout_settings:
            extra_applyout_settings = self.extra_applyout_settings.copy()
        apply_config_data_df = None
        if extra_applyout_settings is not None:
            extra_mode = extra_applyout_settings.get('APL/ApplyExtraMode', None)
            if extra_mode:
                if extra_mode == 'Individual Contributions':
                    apply_config_data_df = self._get_indiv_contrib_applyconf()
                elif extra_mode == 'Explanations':
                    apply_config_data_df = pd.DataFrame([
                        ('APL/ApplyExtraMode', 'Advanced Apply Settings', None),
                        ('APL/UnpivotedExplanations', 'true', None),
                        ('APL/ApplyPredictedValue', 'true', None)
                    ])
                # Free advanced settings
                elif extra_mode == 'Advanced Apply Settings':
                    cfg_vals = [('APL/ApplyExtraMode', 'Advanced Apply Settings', None)]
                    for alias, param_val in extra_applyout_settings.items():
                        if alias != 'APL/ApplyExtraMode':
                            cfg_vals.append((alias, param_val, None))
                    apply_config_data_df = pd.DataFrame(cfg_vals)
                else:
                    # User provides an ExtraMode explicitly
                    apply_config_data_df = pd.DataFrame([('APL/ApplyExtraMode', extra_mode, None)])
        if apply_config_data_df is None:
            # Default
            apply_config_data_df = pd.DataFrame([])
        applyout_df = self._predict(data=data, apply_config_data_df=apply_config_data_df)
        return self._rewrite_applyout_df(data=data, applyout_df=applyout_df)

    def score(self, data):
        """
        Returns the R2 score (coefficient of determination) on the provided test dataset.

        Parameters
        ----------
        data: hana_ml DataFrame
            The test dataset used to compute the score.
            The labels must be provided in the dataset.

        Returns
        -------
            Float or pandas DataFrame
                If no segment column is given, the R2 score.

                If a segment column is given, a pandas DataFrame which contains the R2 score
                for each segment.
        """
        if self._get_apl_version() >= 2303:
            self._score(data)
            perfs = self.get_debrief_report('ClassificationRegression_Performance').collect()
            perfs = perfs[(perfs['Partition'] == 'ApplyIn') & (perfs['Indicator'] == 'R2')]
            segment_column_name = getattr(self, 'segment_column_name', None)
            if segment_column_name is not None:
                r2_by_segment = perfs[['Oid', 'Value']].reset_index(drop=True)
                r2_by_segment.columns = [segment_column_name, 'R2']
                return r2_by_segment
            else:
                return perfs['Value'].iloc[0]
        else:
            applyout_df = self.predict(data)

            # Find the name of the label column in applyout table
            true_y_col = 'TRUE_LABEL'
            pred_y_col = 'PREDICTED'

            # Check if the label column is given in input dataset (true label)
            # If it is not present, score can't be calculated
            if true_y_col not in applyout_df.columns:
                raise ValueError("Cannot find true label column in the output of predict()")
            if pred_y_col not in applyout_df.columns:
                raise ValueError('Cannot find PREDICTED column in the output of predict().'
                                 ' Please check the extra_applyout_settings')
            try:
                with self.conn_context.connection.cursor() as cur:
                    sql = (
                        'SELECT 1- (SUM(POWER((applyout.{true_y_col} - applyout.{pred_y_col}), 2)))/'
                        + '(SUM(POWER((applyout.{true_y_col} - gdt.av), 2)))'
                        + ' FROM ({applyout_df}) applyout, '
                        + '  (select avg({true_y_col}) as av from ({applyout_df}) ) as gdt'
                        )
                    sql = sql.format(applyout_df=applyout_df.select_statement,
                                     true_y_col=true_y_col,
                                     pred_y_col=pred_y_col)
                    execute_logged(cur, sql)
                    ret = cur.fetchone()
                    return float(ret[0])
            except dbapi.Error as db_er:
                logger.error(
                    "Failed to calculate the score, the error message: %s",
                    db_er,
                    exc_info=True)
                raise

    def _rewrite_applyout_df(self, data, applyout_df):
        """
        Rewrites the applyout dataframe so it outputs standardized column names.
        """

        # Determines the mapping old columns to new columns
        # Stores the mapping into different list of tuples [(old_column, new_columns)]
        cols_map = []      # starting columns: ID, TRUE_LABEL
        for i, old_col in enumerate(applyout_df.columns):
            if i == 0:
                # key column
                new_col = old_col
                cols_map.append((old_col, new_col))
            elif old_col == self.label:
                new_col = 'TRUE_LABEL'
                cols_map.append((old_col, new_col))
            elif old_col == 'gb_score_{LABEL}'.format(LABEL=self.label):
                new_col = 'PREDICTED'
                cols_map.append((old_col, new_col))
            else:
                cols_map.append((old_col, old_col))

        # Writes the select SQL by renaming the columns
        sql = ''
        # Starting columns
        for old_col, new_col in cols_map:
            # If the target var is not given in applyin, do not output it
            if old_col == self.label and old_col not in data.columns:
                continue
            if sql:
                sql = sql + ', '
            sql = (sql + '{old_col} {new_col}'.format(
                old_col=quotename(old_col),
                new_col=quotename(new_col)))
        sql = 'SELECT ' + sql + ' FROM ' + self.applyout_table_.name
        applyout_df_new = DataFrame(connection_context=self.conn_context,
                                    select_statement=sql)
        logger.info('DataFrame for predict ouput: %s', sql)
        return applyout_df_new

    def _get_indiv_contrib_applyconf(self): #pylint: disable=no-self-use
        """
        Gets the apply configuration for 'Individual Contributions' output.
        Returns
        -------
        A pandas dataframe for operation_config table
        """
        return pd.DataFrame([
            ('APL/ApplyExtraMode', 'Advanced Apply Settings', None),
            ('APL/ApplyPredictedValue', 'true', None),
            ('APL/ApplyContribution', 'all', None),
        ])

    def build_report(self, max_local_explanations=100):
        """
        Build model report.

        Parameters
        ----------
        max_local_explanations: int, optional
            The maximum number of local explanations displayed in the report.
        """
        GradientBoostingBase.build_report(self)

        try:
            param_table, optimal_param_table, importance_table = self._get_common_report_tables()

            perf_report = self.get_debrief_report('ClassificationRegression_Performance')

            stat_tables = {}
            for partition in ['Validation', 'ApplyIn']:
                if partition in perf_report.select('Partition').collect().values:
                    stat_tables[partition] = perf_report \
                        .filter('"Partition" = \'{}\''.format(partition)) \
                        .select('Indicator', 'Value')

            # pylint: disable=protected-access
            self._set_parameter_table(param_table) \
                ._set_optimal_parameter_table(optimal_param_table) \
                ._set_variable_importance_table(importance_table) \
                ._set_statistic_table(stat_tables['Validation'])
            if 'ApplyIn' in stat_tables:
                self._set_scoring_statistic_table(stat_tables['ApplyIn'])
            self._render_report()
            self._add_interaction_matrix_to_report(self._report_builder)

            if self.applyout_ is not None and 'Explanation_Rank' in self.applyout_.columns:
                apply_out = self.applyout_.collect()
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

                    explanation_df = apply_out[apply_out[key_column] == key].sort_values(by='Explanation_Rank', ascending=True, ignore_index=True)
                    explanation_df['Influencer Values'] = explanation_df[['Explanation_Influencer', 'Explanation_Influencer_Value']].stack().groupby(level=0).agg(' = '.join) \
                        .replace({'Negative Others = Negative Others': 'Negative Others',
                                  'Positive Others = Positive Others': 'Positive Others',
                                  'Baseline = Baseline': 'Baseline'})

                    start_values = []
                    end_values = []

                    sum_contribs = 0

                    for contribution in explanation_df['Explanation_Contribution'].tolist():
                        start_values.append(sum_contribs)
                        sum_contribs = sum_contribs + contribution
                        end_values.append(sum_contribs)

                    explanation_df['Start Values'] = start_values
                    explanation_df['End Values'] = end_values

                    waterfall_data_df = explanation_df.sort_values(by='Explanation_Rank', ascending=False, ignore_index=True)[['Influencer Values', 'Start Values', 'End Values']]
                    waterfall_data = [waterfall_data_df.columns.values.tolist()] + [['Predicted Value', 0, sum_contribs]] + waterfall_data_df.values.tolist()

                    explanations_config = {
                        'customFn': ['series[0].label.formatter', 'series[0].renderItem', 'tooltip.formatter'],
                        'tooltip': {
                            'trigger': 'axis',
                            'axisPointer': {'type': 'none'},
                            'formatter': {
                                'params': ['params'],
                                'body': 'return params[0].axisValueLabel;'
                            }
                        },
                        'grid': {'show': 'true', 'containLabel': 'true'},
                        'dataset': {
                            'source': waterfall_data,
                        },
                        'xAxis': {
                            'name': 'Contribution',
                            'type': 'value',
                            'axisLine': {'show': 'true'},
                            'axisTick': {'show': 'true'},
                        },
                        'yAxis': {
                            'name': 'Influencer Value',
                            'type': 'category',
                            'axisLabel': {
                                'interval': 0
                            }
                        },
                        'series': [
                            {
                                'type': 'custom',
                                'label': {
                                    'show': 'true',
                                    'position': 'right',
                                    'color': '#60594F',
                                    'fontWeight': 'bold',
                                    'formatter': {
                                        'params': ['params'],
                                        'body': ''.join([
                                            "const labelData = params.data[2] - params.data[1];",
                                            "return labelData.toFixed(3);"
                                        ])
                                    }
                                },
                                'datasetIndex': 0,
                                'encode': {
                                    'y': 0,
                                },
                                'renderItem': {
                                    'params': ['params', 'api'],
                                    'body': ''.join([
                                        "const dataIndex = api.value(0);",
                                        "const barStartValue = api.value(1);",
                                        "const barEndValue = api.value(2);",
                                        "const startCoord = api.coord([barStartValue, dataIndex]);",
                                        "const endCoord = api.coord([barEndValue, dataIndex]);",
                                        "const rectHeight = 10;",
                                        "const rectMinWidth = 1;",
                                        "let rectWidth = startCoord[0] - endCoord[0];",
                                        "const style = api.style();",
                                        "if (dataIndex === 0) {",
                                        "style.fill = '#404040';",
                                        "} else if (rectWidth > 0) {",
                                        "style.fill = '#F24269';"
                                        "} else if (rectWidth === 0) {",
                                        "style.fill = '#ededed';",
                                        "} else {",
                                        "style.fill = '#77D36F';"
                                        "}",
                                        "rectWidth = rectWidth === 0 ? rectMinWidth : rectWidth;",
                                        "const rectItem = {",
                                        "type: 'rect',",
                                        "shape: {",
                                        "x: endCoord[0],",
                                        "y: endCoord[1] - rectHeight / 2,",
                                        "width: rectWidth,",
                                        "height: rectHeight,",
                                        "},",
                                        "style: style,",
                                        "};",
                                        "return rectItem;"
                                    ])
                                }
                            },
                        ],
                        'toolbox': {
                            'feature': {
                                'saveAsImage': {
                                    'name': 'local_explanations_' + str(key)
                                }
                            }
                        },
                    }

                    chart = ChartItem(key_column + ' = ' + str(key), explanations_config)
                    explanations_page.addItem(chart)

                self._report_builder.addPage(explanations_page)
                self._report_builder.build()

        except Exception as err:
            logger.error(str(err))
            raise
