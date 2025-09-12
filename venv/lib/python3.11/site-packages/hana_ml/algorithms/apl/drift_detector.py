"""
This module provides the DriftDetector class for detecting data drift between a reference dataset and new data.
"""

import logging
from hdbcli import dbapi
from hana_ml.ml_base import ListOfStrings
from hana_ml.algorithms.apl.apl_base import APLBase
from hana_ml.visualizers.report_builder import ReportBuilder, Page, ChartItem, DescriptionItem, AlertItem

logger = logging.getLogger(__name__) #pylint: disable=invalid-name

class DriftDetector(APLBase): #pylint: disable=too-many-instance-attributes
    """
    A class to detect data drift between a reference dataset and new data.

    Data drift refers to changes in the statistical properties of the data over time, which can affect the performance of machine learning models.

    Parameters
    ----------
    variable_storages : dict, optional
        Specifies the variable data types (``string``, ``integer``, ``number``).
        For example, ``{'VAR1': 'string', 'VAR2': 'number'}``.
    variable_value_types : dict, optional
        Specifies the variable value types (``continuous``, ``nominal``, ``ordinal``).
        For example, ``{'VAR1': 'continuous', 'VAR2': 'nominal'}``.
    variable_missing_strings : dict, optional
        Specifies the variable values that will be taken as missing.
        For example, ``{'VAR1': '???'}`` means anytime the variable value equals ``'???'``,
        it will be taken as missing.
    other_params : dict, optional
        Corresponds to the advanced settings.
        The dictionary contains ``{<parameter_name>: <parameter_value>}``.
        The possible parameters are:

        - ``max_tasks``
        - ``segment_column_name``
        - ``cutting_strategy``

        See `Common APL Aliases for Model Training
        <https://help.sap.com/viewer/7223667230cb471ea916200712a9c682/latest/en-US/de2e28eaef79418799b9f4e588b04b53.html>`_
        in the SAP HANA APL Developer Guide.

        For ``max_tasks``, see `FUNC_HEADER
        <https://help.sap.com/viewer/7223667230cb471ea916200712a9c682/latest/en-US/d8faaa27841341cbac41353d862484af.html>`_.
    other_train_apl_aliases : dict, optional
        Users can provide APL aliases as advanced settings to the model.
        Unlike ``other_params`` described above, users are free to input any possible value.
        There is no control in python.

    Examples
    --------
    >>> from hana_ml.dataframe import ConnectionContext, DataFrame
    >>> from hana_ml.algorithms.apl.drift_detector import DriftDetector

    >>> # Create a connection context
    >>> conn = ConnectionContext('host', port, 'user', 'password')

    >>> # Load reference data and new data
    >>> reference_data = DataFrame(conn, 'SELECT * FROM REFERENCE_DATA_TABLE')
    >>> new_data = DataFrame(conn, 'SELECT * FROM NEW_DATA_TABLE')

    >>> # Initialize the DriftDetector
    >>> drift_detector = DriftDetector()

    >>> # Fit the drift detector with reference data
    >>> drift_detector.fit(reference_data, features=['feature1', 'feature2'], label='target')

    >>> # Detect drift in new data
    >>> results = drift_detector.detect(new_data, threshold=0.9, build_report=True)
    >>> print(results.collect())

    >>> # Fit and detect drift in one step
    >>> results = drift_detector.fit_detect(reference_data, new_data, features=['feature1', 'feature2'], label='target', threshold=0.9, build_report=True)
    >>> print(results.collect())

    >>> # Generate an HTML report
    >>> drift_detector.generate_html_report('drift_report')

    >>> # Generate a notebook iframe report
    >>> drift_detector.generate_notebook_iframe_report()

    Notes
    -----
    When calling the ``fit_detect`` method, the model is generated on-the-fly but is not returned.
    If a model must be saved, please consider using the ``fit`` method instead.
    """

    MIN_APL_VERSION = 2504

    APL_ALIAS_KEYS = {
        'cutting_strategy': 'APL/CuttingStrategy',
        'segment_column_name': 'APL/SegmentColumnName'
    }

    def __init__(self,
                 variable_storages=None,
                 variable_value_types=None,
                 variable_missing_strings=None,
                 **other_params):
        super(DriftDetector, self).__init__(
            variable_storages=variable_storages,
            variable_value_types=variable_value_types,
            variable_missing_strings=variable_missing_strings,
            **other_params)

        self.label = None
        self._report_builder = None

    def _check_apl_version(self):
        if self._get_apl_version() < self.MIN_APL_VERSION:
            raise ValueError(f'APL version must be {self.MIN_APL_VERSION} or higher.')

    def _set_model_type_from_label(self, label):
        if label is None:
            self._model_type = 'statbuilder'
            self._indicator_dataset = 'Estimation' # no Validation set
        else:
            self._model_type = 'variable-encoder'
            self._indicator_dataset = 'Validation'
            self.label = label

    def _get_results(self, threshold):
        results = self.get_debrief_report('Deviation_ByVariable', deviation_threshold=threshold)

        segment_column = getattr(self, 'segment_column_name', None)
        if segment_column is None:
            results = results[['Variable', 'Deviation Indicator']] \
                .sort('Deviation Indicator', desc=True)
        else:
            results = results.rename_columns({'Oid': 'Segment'}) \
                .select(['Segment', 'Variable', 'Deviation Indicator',
                         ('-1 * "Deviation Indicator"', 'NegDeviation')]) \
                .sort(['Segment', 'NegDeviation']) \
                .drop('NegDeviation')

        return results

    def fit(self, reference_data, features=None, label=None, weight=None):
        """
        Fits the drift detector using the provided reference data.

        Parameters
        ----------
        reference_data : DataFrame
            The reference dataset used to fit the drift detector.
        features : list of str, optional
            The list of feature column names to be used for drift detection.
        label : str, optional
            The name of the label column.
        weight : str, optional
            The name of the weight column.

        Returns
        -------
        self
            The fitted drift detector.
        """
        self._set_conn_context(reference_data.connection_context)
        self._check_apl_version()

        self._set_model_type_from_label(label)

        return self._fit(data=reference_data, features=features, label=label, weight=weight)

    def detect(self, new_data, threshold=None, build_report=False):
        """
        Detects drift in the provided data.

        Parameters
        ----------
        new_data : DataFrame
            The dataset to compare against the reference dataset.
        threshold : float, optional
            The threshold for drift detection. If None, a default threshold is used. The default threshold is specified in APL's documentation.
        build_report : bool, optional
            If True, a detailed report of the drift detection will be built. Defaults to False.

        Returns
        -------
        results : DataFrame
            A DataFrame containing the variables and their deviation indicators, sorted by the deviation indicator in descending order.
        """
        threshold = self._arg('threshold', threshold, float)
        build_report = self._arg('build_report', build_report, bool)

        self._score(new_data)

        if build_report:
            self.build_report(threshold=threshold)

        return self._get_results(threshold)

    def fit_detect(self, reference_data, new_data, features=None, label=None, weight=None, threshold=None, build_report=False):
        """
        Detects drift between reference data and new data.

        When calling the ``fit_detect`` method, the model is generated on-the-fly but is not returned.
        If a model must be saved, please consider using the ``fit`` method instead.

        Parameters
        ----------
        reference_data : DataFrame
            The reference dataset.
        new_data : DataFrame
            The dataset to compare against the reference dataset.
        features : list of str, optional
            The feature columns to consider for drift detection.
        label : str, optional
            The label column in the datasets.
        weight : str, optional
            The weight column in the datasets.
        threshold : float, optional
            The threshold for drift detection. If None, a default threshold is used. The default threshold is specified in APL's documentation.
        build_report : bool, optional
            If True, a detailed report of the drift detection will be built. Defaults to False.

        Returns
        -------
        results : DataFrame
            A DataFrame containing the variables and their deviation indicators, sorted by the deviation indicator in descending order.
        """
        if reference_data.connection_context != new_data.connection_context:
            raise ValueError('reference_data and new_data must have the same connection context.')

        self._set_conn_context(reference_data.connection_context)
        self._check_apl_version()

        features = self._arg('features', features, ListOfStrings)
        label = self._arg('label', label, str)
        weight = self._arg('weight', weight, str)
        threshold = self._arg('threshold', threshold, float)
        build_report = self._arg('build_report', build_report, bool)

        self._check_valid(reference_data, features=features, label=label, weight=weight)
        self._check_valid(new_data, features=features, label=label, weight=weight)

        self._set_model_type_from_label(label)

        try:
            func_header_table = self._create_func_header_table()
            config_table = self._create_train_config_table(has_output_indicator_table=False)
            var_roles_table = self._create_var_roles_table(
                data=reference_data,
                key=None,
                label=label,
                features=features,
                weight=weight)

            try:
                ref_data_view_name = 'REF_DATA_VIEW_{}'.format(self.id)
                ref_data_view = self._create_view(view_name=ref_data_view_name, data=reference_data)
            except dbapi.Error: # fall back if view can't be created (because the original dataset is a temp table)
                ref_data_view_name = '#REF_DATA_VIEW_{}'.format(self.id)
                ref_data_view = self._materialize_w_type_conv(name=ref_data_view_name, data=reference_data)

            try:
                new_data_view_name = 'NEW_DATA_VIEW_{}'.format(self.id)
                new_data_view = self._create_view(view_name=new_data_view_name, data=new_data)
            except dbapi.Error: # fall back if view can't be created (because the original dataset is a temp table)
                new_data_view_name = '#NEW_DATA_VIEW_{}'.format(self.id)
                new_data_view = self._materialize_w_type_conv(name=new_data_view_name, data=new_data)

            var_desc_table = self._create_var_desc_table(key=None, label=label, data_view_name=ref_data_view_name)

            log_table = self._create_operation_log_table()
            summary_table = self._create_summary_table()
            debrief_metric_table = self._create_debrief_metric_table()
            debrief_property_table = self._create_debrief_property_table()

            self._create_artifact_table(func_header_table)
            self._create_artifact_table(config_table)
            self._create_artifact_table(var_desc_table)
            self._create_artifact_table(var_roles_table)
            self._create_artifact_table(log_table)
            self._create_artifact_table(summary_table)
            self._create_artifact_table(debrief_metric_table)
            self._create_artifact_table(debrief_property_table)

            if not self._disable_hana_execution:
                self._call_apl(
                    'APL_COMPARE_DATA',
                    input_tables=[
                        func_header_table,
                        config_table,
                        var_desc_table,
                        var_roles_table,
                        ref_data_view,
                        new_data_view
                    ],
                    output_tables=[
                        log_table,
                        summary_table,
                        debrief_metric_table,
                        debrief_property_table
                    ]
                )
        except dbapi.Error as db_er:
            logger.error('fit_detect failed with the following error: %s', db_er, exc_info=True)
            self._drop_artifact_tables()
            raise
        finally:
            self._try_drop_view([ref_data_view.name, new_data_view.name])

        self.fit_operation_log_ = self.conn_context.table(log_table.name) #pylint:disable=attribute-defined-outside-init
        self.test_operation_log_ = self.fit_operation_log_
        self.summary_ = self.conn_context.table(summary_table.name) #pylint:disable=attribute-defined-outside-init
        self.debrief_metric_table_ = debrief_metric_table #pylint:disable=attribute-defined-outside-init
        self.debrief_property_table_ = debrief_property_table #pylint:disable=attribute-defined-outside-init

        if build_report:
            self.build_report(threshold=threshold)

        return self._get_results(threshold)

    def build_report(self, threshold=None, segment_name=None):
        """
        Builds a comprehensive data drift report.

        This method generates a report that includes various types of data drift analyses:

        - Variable Drift
        - Category Drift
        - Category Frequencies
        - Target-Based Category Drift
        - Group Drift
        - Group Frequencies
        - Target-Based Group Drift

        Each section of the report is built based on the data collected from the debrief reports.

        Use ``generate_html_report`` to generate an HTML file for the report, or ``generate_notebook_iframe_report`` to
        display the report in a Jupyter notebook.

        Parameters
        ----------
        threshold : float, optional
            The threshold for drift detection. If None, a default threshold is used. The default threshold is specified in APL's documentation.
        segment_name : str, optional
            If the model is segmented, the segment name for which the report will be built.
        """
        list_partitions_df = self.get_debrief_report('Statistics_Partition').collect()

        if 'ApplyIn' not in list_partitions_df['Partition'].values:
            raise Exception('No detection results found. Please ensure that the detect or fit_detect method has been called before building the report.')

        by_variable_df = self.get_debrief_report('Deviation_ByVariable', deviation_threshold=threshold).collect()

        by_category_df = self.get_debrief_report('Deviation_ByCategory', deviation_threshold=threshold).collect()
        category_frequencies_df = self.get_debrief_report('Deviation_CategoryFrequencies').collect()
        target_based_by_category_df = self.get_debrief_report('Deviation_TargetBasedByCategory', deviation_threshold=threshold).collect()

        by_group_df = self.get_debrief_report('Deviation_ByGroup', deviation_threshold=threshold).collect()
        group_frequencies_df = self.get_debrief_report('Deviation_GroupFrequencies').collect()
        target_based_by_group_df = self.get_debrief_report('Deviation_TargetBasedByGroup', deviation_threshold=threshold).collect()

        segment_column = getattr(self, 'segment_column_name', None)
        segments = by_variable_df['Oid'].unique().tolist()

        if len(segments) > 1 and segment_name is None:
            raise ValueError(
                'The model is segmented. Please provide a value for the parameter "segment_name".')
        if segment_name is not None and segment_name not in segments:
            raise ValueError('Unknown segment name.')

        if segment_name is not None:
            list_partitions_df = list_partitions_df[list_partitions_df['Oid'] == segment_name]
            by_variable_df = by_variable_df[by_variable_df['Oid'] == segment_name]
            by_category_df = by_category_df[by_category_df['Oid'] == segment_name]
            category_frequencies_df = category_frequencies_df[category_frequencies_df['Oid'] == segment_name]
            target_based_by_category_df = target_based_by_category_df[target_based_by_category_df['Oid'] == segment_name]
            by_group_df = by_group_df[by_group_df['Oid'] == segment_name]
            group_frequencies_df = group_frequencies_df[group_frequencies_df['Oid'] == segment_name]
            target_based_by_group_df = target_based_by_group_df[target_based_by_group_df['Oid'] == segment_name]

        def get_chart_height(n_axis_labels):
            return 450 if n_axis_labels > 10 else 300

        self._report_builder = ReportBuilder(title='Data Drift Report')

        overview_page = Page('Overview')
        model_summary = DescriptionItem('Detection Summary')
        model_summary.add('Reference Dataset Weight', list_partitions_df[list_partitions_df['Partition'] == 'Estimation']['Weight'].iloc[0])
        model_summary.add('New Dataset Weight', list_partitions_df[list_partitions_df['Partition'] == 'ApplyIn']['Weight'].iloc[0])
        if segment_name is not None:
            model_summary.add('Segment Variable', segment_column)
            model_summary.add('Segment', segment_name)
        model_summary.add('Date', list_partitions_df['Build Date'].astype(str).iloc[0])
        if self.label is not None:
            model_summary.add('Target Variable', self.label)
        if threshold is not None:
            model_summary.add('Threshold', threshold)
        overview_page.addItem(model_summary)
        alert_item = AlertItem('Detection Result')
        if by_variable_df.empty and by_category_df.empty and target_based_by_category_df.empty and by_group_df.empty and target_based_by_group_df.empty:
            if threshold is None:
                alert_item.add_info_msg('No drift has been detected.')
            else:
                alert_item.add_info_msg(f'No drift has been detected with the threshold of {threshold}.')
        else:
            if not by_variable_df.empty:
                if threshold is None:
                    alert_item.add_warning_msg('Variable drift has been detected. Please check the "Variable Drift" tab for detailed results.')
                else:
                    alert_item.add_warning_msg(f'Variable drift has been detected with the threshold of {threshold}. Please check the "Variable Drift" tab for detailed results.')
            else:
                if threshold is None:
                    alert_item.add_warning_msg('No variable drift detected, but drift at the category or group level has been detected. Please check the respective tabs for detailed results.')
                else:
                    alert_item.add_warning_msg(f'No variable drift detected with the threshold of {threshold}, but drift at the category or group level has been detected. Please check the respective tabs for detailed results.')
        overview_page.addItem(alert_item)
        self._report_builder.addPage(overview_page)

        if not by_variable_df.empty:
            variable_drift_page = Page('Variable Drift')
            by_variable_df = by_variable_df.sort_values(by='Deviation Indicator', ascending=True).tail(20)
            variable_indicators = by_variable_df['Deviation Indicator'].tolist()
            variable_labels = by_variable_df['Variable'].tolist()
            variable_drift_data = []
            for i, _ in enumerate(variable_indicators):
                variable_drift_data.append({'value': variable_indicators[i]})
            variable_drift_config = {
                'tips': [
                    'This chart shows the top 20 variables with the highest deviation indicators.',
                ],
                'tooltip': {
                    'trigger': 'axis',
                    'axisPointer': {'type': 'shadow'}
                },
                'legend': {},
                'grid': {'show': 'true', 'containLabel': 'true'},
                'xAxis': {
                    'name': 'Deviation Indicator',
                    'type': 'value',
                    'axisLine': {'show': 'true'},
                    'axisTick': {'show': 'true'},
                },
                'yAxis': {
                    'name': 'Variable',
                    'type': 'category',
                    'data': variable_labels,
                    'axisLabel': {
                        'interval': 0
                    }
                },
                'series': [
                    {
                        'type': 'bar',
                        'data': variable_drift_data
                    }
                ],
                'toolbox': {
                    'feature': {
                        'saveAsImage': {
                            'name': 'variable_drift'
                        }
                    }
                }
            }
            variable_drift_chart = ChartItem('Top 20 Variables by Deviation Indicator', variable_drift_config,
                                             height=get_chart_height(len(variable_labels)))
            variable_drift_page.addItem(variable_drift_chart)
            self._report_builder.addPage(variable_drift_page)

        if not by_category_df.empty:
            category_drift_page = Page('Category Drift')
            for variable in by_category_df['Variable'].unique():
                variable_df = by_category_df[by_category_df['Variable'] == variable]
                variable_df = variable_df.sort_values(by='Deviation Indicator', ascending=True).tail(20)
                category_indicators = variable_df['Deviation Indicator'].tolist()
                category_labels = variable_df['Category'].tolist()
                category_drift_data = []
                for i, _ in enumerate(category_indicators):
                    category_drift_data.append({'value': category_indicators[i]})
                category_drift_config = {
                    'tips': [
                        'This chart shows the top 20 categories with the highest deviation indicators.',
                    ],
                    'tooltip': {
                        'trigger': 'axis',
                        'axisPointer': {'type': 'shadow'}
                    },
                    'legend': {},
                    'grid': {'show': 'true', 'containLabel': 'true'},
                    'xAxis': {
                        'name': 'Deviation Indicator',
                        'type': 'value',
                        'axisLine': {'show': 'true'},
                        'axisTick': {'show': 'true'},
                    },
                    'yAxis': {
                        'name': 'Category',
                        'type': 'category',
                        'data': category_labels,
                        'axisLabel': {
                            'interval': 0
                        }
                    },
                    'series': [
                        {
                            'type': 'bar',
                            'data': category_drift_data
                        }
                    ],
                    'toolbox': {
                        'feature': {
                            'saveAsImage': {
                                'name': 'category_drift_' + variable
                            }
                        }
                    }
                }
                category_drift_chart = ChartItem(f'{variable} - Top 20 Categories by Deviation Indicator', category_drift_config,
                                                 height=get_chart_height(len(category_labels)))
                category_drift_page.addItem(category_drift_chart)
            self._report_builder.addPage(category_drift_page)

        if not category_frequencies_df.empty:
            category_frequencies_page = Page('Category Frequencies')
            for variable in sorted(category_frequencies_df['Variable'].unique()):
                variable_df = category_frequencies_df[category_frequencies_df['Variable'] == variable]
                variable_df = variable_df.sort_values(by='Abs % Change', ascending=True).tail(20)
                ref_weights = variable_df['Ref % Weight'].tolist()
                new_weights = variable_df['New % Weight'].tolist()
                category_labels = variable_df['Category'].tolist()
                category_frequencies_data_ref = []
                category_frequencies_data_new = []
                for i, _ in enumerate(ref_weights):
                    category_frequencies_data_ref.append({
                        'value': ref_weights[i],
                        'name': 'Ref % Weight'
                    })
                    category_frequencies_data_new.append({
                        'value': new_weights[i],
                        'name': 'New % Weight'
                    })
                category_frequencies_config = {
                    'tips': [
                        'This chart shows the top 20 categories with the highest absolute percentage change in weights.',
                    ],
                    'tooltip': {
                        'trigger': 'axis',
                        'axisPointer': {'type': 'shadow'}
                    },
                    'legend': {},
                    'grid': {'show': 'true', 'containLabel': 'true'},
                    'xAxis': {
                        'name': '% Weight',
                        'type': 'value',
                        'axisLine': {'show': 'true'},
                        'axisTick': {'show': 'true'},
                    },
                    'yAxis': {
                        'name': 'Category',
                        'type': 'category',
                        'data': category_labels,
                        'axisLabel': {
                            'interval': 0
                        }
                    },
                    'series': [
                        {
                            'type': 'bar',
                            'data': category_frequencies_data_ref,
                            'name': 'Ref % Weight'
                        },
                        {
                            'type': 'bar',
                            'data': category_frequencies_data_new,
                            'name': 'New % Weight'
                        }
                    ],
                    'toolbox': {
                        'feature': {
                            'saveAsImage': {
                                'name': 'category_frequencies_' + variable
                            }
                        }
                    }
                }
                category_frequencies_chart = ChartItem(f'{variable} - Top 20 Categories by Absolute % Change', category_frequencies_config,
                                                       height=get_chart_height(len(category_labels)))
                category_frequencies_page.addItem(category_frequencies_chart)
            self._report_builder.addPage(category_frequencies_page)

        if not target_based_by_category_df.empty:
            target_based_category_drift_page = Page('Target-Based Category Drift')
            for variable in target_based_by_category_df['Variable'].unique():
                variable_df = target_based_by_category_df[target_based_by_category_df['Variable'] == variable]
                variable_df = variable_df.sort_values(by='Deviation Indicator', ascending=True).tail(20)
                category_indicators = variable_df['Deviation Indicator'].tolist()
                category_labels = variable_df['Category'].tolist()
                category_drift_data = []
                for i, _ in enumerate(category_indicators):
                    category_drift_data.append({'value': category_indicators[i]})
                category_drift_config = {
                    'tips': [
                        'This chart shows the top 20 categories with the highest deviation indicators.',
                    ],
                    'tooltip': {
                        'trigger': 'axis',
                        'axisPointer': {'type': 'shadow'}
                    },
                    'legend': {},
                    'grid': {'show': 'true', 'containLabel': 'true'},
                    'xAxis': {
                        'name': 'Deviation Indicator',
                        'type': 'value',
                        'axisLine': {'show': 'true'},
                        'axisTick': {'show': 'true'},
                    },
                    'yAxis': {
                        'name': 'Category',
                        'type': 'category',
                        'data': category_labels,
                        'axisLabel': {
                            'interval': 0
                        }
                    },
                    'series': [
                        {
                            'type': 'bar',
                            'data': category_drift_data
                        }
                    ],
                    'toolbox': {
                        'feature': {
                            'saveAsImage': {
                                'name': 'target_based_category_drift_' + variable
                            }
                        }
                    }
                }
                category_drift_chart = ChartItem(f'{variable} - Top 20 Categories by Deviation Indicator', category_drift_config,
                                                 height=get_chart_height(len(category_labels)))
                target_based_category_drift_page.addItem(category_drift_chart)
            self._report_builder.addPage(target_based_category_drift_page)

        if not by_group_df.empty:
            group_drift_page = Page('Group Drift')
            for variable in by_group_df['Variable'].unique():
                variable_df = by_group_df[by_group_df['Variable'] == variable]
                variable_df = variable_df.sort_values(by='Deviation Indicator', ascending=True).tail(20)
                group_indicators = variable_df['Deviation Indicator'].tolist()
                group_labels = variable_df['Group'].tolist()
                group_drift_data = []
                for i, _ in enumerate(group_indicators):
                    group_drift_data.append({'value': group_indicators[i]})
                group_drift_config = {
                    'tips': [
                        'This chart shows the top 20 groups with the highest deviation indicators.',
                    ],
                    'tooltip': {
                        'trigger': 'axis',
                        'axisPointer': {'type': 'shadow'}
                    },
                    'legend': {},
                    'grid': {'show': 'true', 'containLabel': 'true'},
                    'xAxis': {
                        'name': 'Deviation Indicator',
                        'type': 'value',
                        'axisLine': {'show': 'true'},
                        'axisTick': {'show': 'true'},
                    },
                    'yAxis': {
                        'name': 'Group',
                        'type': 'category',
                        'data': group_labels,
                        'axisLabel': {
                            'interval': 0
                        }
                    },
                    'series': [
                        {
                            'type': 'bar',
                            'data': group_drift_data
                        }
                    ],
                    'toolbox': {
                        'feature': {
                            'saveAsImage': {
                                'name': 'group_drift_' + variable
                            }
                        }
                    }
                }
                group_drift_chart = ChartItem(f'{variable} - Top 20 Groups by Deviation Indicator', group_drift_config,
                                              height=get_chart_height(len(group_labels)))
                group_drift_page.addItem(group_drift_chart)
            self._report_builder.addPage(group_drift_page)

        if not group_frequencies_df.empty:
            group_frequencies_page = Page('Group Frequencies')
            for variable in sorted(group_frequencies_df['Variable'].unique()):
                variable_df = group_frequencies_df[group_frequencies_df['Variable'] == variable]
                variable_df = variable_df.sort_values(by='Abs % Change', ascending=True).tail(20)
                ref_weights = variable_df['Ref % Weight'].tolist()
                new_weights = variable_df['New % Weight'].tolist()
                group_labels = variable_df['Group'].tolist()
                group_frequencies_data_ref = []
                group_frequencies_data_new = []
                for i, _ in enumerate(ref_weights):
                    group_frequencies_data_ref.append({
                        'value': ref_weights[i],
                        'name': 'Ref % Weight'
                    })
                    group_frequencies_data_new.append({
                        'value': new_weights[i],
                        'name': 'New % Weight'
                    })
                group_frequencies_config = {
                    'tips': [
                        'This chart shows the top 20 groups with the highest absolute percentage change in weights.',
                    ],
                    'tooltip': {
                        'trigger': 'axis',
                        'axisPointer': {'type': 'shadow'}
                    },
                    'legend': {},
                    'grid': {'show': 'true', 'containLabel': 'true'},
                    'xAxis': {
                        'name': '% Weight',
                        'type': 'value',
                        'axisLine': {'show': 'true'},
                        'axisTick': {'show': 'true'},
                    },
                    'yAxis': {
                        'name': 'Group',
                        'type': 'category',
                        'data': group_labels,
                        'axisLabel': {
                            'interval': 0
                        }
                    },
                    'series': [
                        {
                            'type': 'bar',
                            'data': group_frequencies_data_ref,
                            'name': 'Ref % Weight'
                        },
                        {
                            'type': 'bar',
                            'data': group_frequencies_data_new,
                            'name': 'New % Weight'
                        }
                    ],
                    'toolbox': {
                        'feature': {
                            'saveAsImage': {
                                'name': 'group_frequencies_' + variable
                            }
                        }
                    }
                }
                group_frequencies_chart = ChartItem(f'{variable} - Top 20 Groups by Absolute % Change', group_frequencies_config,
                                                    height=get_chart_height(len(group_labels)))
                group_frequencies_page.addItem(group_frequencies_chart)
            self._report_builder.addPage(group_frequencies_page)

        if not target_based_by_group_df.empty:
            target_based_group_drift_page = Page('Target-Based Group Drift')
            for variable in target_based_by_group_df['Variable'].unique():
                variable_df = target_based_by_group_df[target_based_by_group_df['Variable'] == variable]
                variable_df = variable_df.sort_values(by='Deviation Indicator', ascending=True).tail(20)
                group_indicators = variable_df['Deviation Indicator'].tolist()
                group_labels = variable_df['Group'].tolist()
                group_drift_data = []
                for i, _ in enumerate(group_indicators):
                    group_drift_data.append({'value': group_indicators[i]})
                group_drift_config = {
                    'tips': [
                        'This chart shows the top 20 groups with the highest deviation indicators.',
                    ],
                    'tooltip': {
                        'trigger': 'axis',
                        'axisPointer': {'type': 'shadow'}
                    },
                    'legend': {},
                    'grid': {'show': 'true', 'containLabel': 'true'},
                    'xAxis': {
                        'name': 'Deviation Indicator',
                        'type': 'value',
                        'axisLine': {'show': 'true'},
                        'axisTick': {'show': 'true'},
                    },
                    'yAxis': {
                        'name': 'Group',
                        'type': 'category',
                        'data': group_labels,
                        'axisLabel': {
                            'interval': 0
                        }
                    },
                    'series': [
                        {
                            'type': 'bar',
                            'data': group_drift_data
                        }
                    ],
                    'toolbox': {
                        'feature': {
                            'saveAsImage': {
                                'name': 'target_based_group_drift_' + variable
                            }
                        }
                    }
                }
                group_drift_chart = ChartItem(f'{variable} - Top 20 Groups by Deviation Indicator', group_drift_config,
                                              height=get_chart_height(len(group_labels)))
                target_based_group_drift_page.addItem(group_drift_chart)
            self._report_builder.addPage(target_based_group_drift_page)

        self._report_builder.build()

    def generate_html_report(self, filename):
        """
        Generates an HTML report and saves it to the specified file.

        It requires that the ``build_report`` method has been called beforehand
        to initialize the report builder.

        Parameters
        ----------
        filename : str
            The name of the file where the HTML report will be saved.
        """
        if self._report_builder is None:
            raise Exception('Please call the build_report method before generating a report.')

        self._report_builder.generate_html(filename)

    def generate_notebook_iframe_report(self):
        """
        Generates a notebook iframe report for a Jupyter notebook.

        It requires that the ``build_report`` method has been called beforehand
        to initialize the report builder.
        """
        if self._report_builder is None:
            raise Exception('Please call the build_report method before generating a report.')

        self._report_builder.generate_notebook_iframe(iframe_height=630)

    def get_detect_operation_log(self):
        """
        Returns the operation log table from the detect operation.

        Returns
        -------
        DataFrame
            The operation log table.
        """
        if not hasattr(self, 'test_operation_log_'):
            raise AttributeError('Detect operation log not found. Run the detect method first.')
        return self.test_operation_log_
