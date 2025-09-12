"""
This module is to build report for PAL/APL models.

The following class is available:

    * :class:`UnifiedReport`
"""
#pylint: disable=line-too-long
#pylint: disable=consider-using-f-string
#pylint: disable=too-many-function-args
import logging
from deprecated import deprecated
from hana_ml.algorithms.apl.time_series import AutoTimeSeries
from hana_ml.algorithms.pal.auto_ml import AutomaticClassification, AutomaticRegression, AutomaticTimeSeries
from hana_ml.algorithms.pal.tsa.additive_model_forecast import AdditiveModelForecast
from hana_ml.algorithms.pal.tsa.arima import ARIMA
from hana_ml.algorithms.pal.tsa.auto_arima import AutoARIMA
from hana_ml.algorithms.pal.tsa.bsts import BSTS
from hana_ml.algorithms.pal.tsa.exponential_smoothing import _ExponentialSmoothingBase, CrostonTSB
from hana_ml.algorithms.pal.tsa.lstm import LSTM
from hana_ml.algorithms.pal.tsa.rnn import GRUAttention
from hana_ml.algorithms.pal.unified_exponentialsmoothing import UnifiedExponentialSmoothing
from hana_ml.dataframe import DataFrame
from hana_ml.ml_exceptions import FitIncompleteError
from hana_ml.visualizers.automl_report import BestPipelineReport
from hana_ml.visualizers.dataset_report import DatasetReportBuilder
from hana_ml.visualizers.model_debriefing import TreeModelDebriefing
from hana_ml.visualizers.model_report import _UnifiedClassificationReportBuilder, _UnifiedRegressionReportBuilder
from hana_ml.algorithms.pal.preprocessing import Sampling
from hana_ml.visualizers.time_series_report_template_helper import TimeSeriesDatasetReportHelper
#from hana_ml.algorithms.pal.utility import version_compare

logger = logging.getLogger(__name__) #pylint: disable=invalid-name

PAL_TIME_SERIES_LIST = (AutomaticTimeSeries,
                        AdditiveModelForecast,
                        BSTS,
                        ARIMA,
                        AutoARIMA,
                        _ExponentialSmoothingBase,
                        GRUAttention,
                        LSTM,
                        UnifiedExponentialSmoothing,
                        CrostonTSB)

class UnifiedReport(object):
    """
    The report generator for PAL/APL models. Currently, it only supports UnifiedClassification and UnifiedRegression.

    Examples
    --------
    Data used is called diabetes_train.

    Case 1: UnifiedReport for UnifiedClassification is shown as follows, please set build_report=True in the fit() function:

    >>> from hana_ml.algorithms.pal.model_selection import GridSearchCV
    >>> from hana_ml.algorithms.pal.model_selection import RandomSearchCV
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
    >>> gscv.fit(data=diabetes_train, key= 'ID',
                 label='CLASS',
                 partition_method='stratified',
                 partition_random_state=1,
                 stratified_column='CLASS',
                 build_report=True)

    To look at the dataset report:

    >>> UnifiedReport(diabetes_train).build().display()

     .. image:: image/unified_report_dataset_report.png

    To see the model report:

    >>> UnifiedReport(gscv.estimator).display()

     .. image:: image/unified_report_model_report_classification.png

    We could also see the Optimal Parameter page:

     .. image:: image/unified_report_model_report_classification2.png

    Case 2: UnifiedReport for UnifiedRegression is shown as follows, please set build_report=True in the fit() function:

    >>> hgr = UnifiedRegression(func = 'HybridGradientBoostingTree')
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

     .. image:: image/unified_report_model_report_regression.png

    """
    def __init__(self, obj):
        self.obj = obj
        self.dataset_report = None
        self.is_time_series = False

    def set_model_report_style(self, version):
        """
        Switch different style of model report

        Parameters
        ----------
        version : {'v2', 'v1'}, optional
            new: using report builder framework.
            old: using pure html template.

            Defaults to 'v2'.
        """
        if hasattr(self.obj, 'framework_version'):
            setattr(self.obj, 'framework_version', version)

    def set_dataset_report_style(self, version):
        """
        Switch different style of dataset report

        Parameters
        ----------
        version : {'v2', 'v1'}, optional
            new: using report builder framework.
            old: using pure html template.

            Defaults to 'v2'.
        """
        setattr(self, 'dataset_framework_version', version)

    def build(self, key=None, scatter_matrix_sampling: Sampling = None,
              ignore_scatter_matrix: bool = False, ignore_correlation: bool = False, subset_bins = None, endog=None, time_series_report_template=None):
        """
        Build the report.

        Parameters
        ----------
        key : str, valid only for DataFrame
            Name of ID column.

            Defaults to the first column.
        scatter_matrix_sampling : :class:`~hana_ml.algorithms.pal.preprocessing.Sampling`, valid only for DataFrame
            Scatter matrix sampling.
        ignore_scatter_matrix : bool, optional
            Ignore the plotting of scatter matrix if True.

            Defaults to False.
        ignore_correlation : bool, optional
            Ignore the correlation computation if True.

            Defaults to False.
        subset_bins : dict, optional
            Define the bin number in distribution chart for each column, e.g. {"col_A": 20}.

            Defaults to 20 for all.
        endog : str, optional, valid only for time series
            Name of endogenous column.

            Defaults to None.
        """
        if endog is not None:
            self.is_time_series = True
        if isinstance(self.obj, _UnifiedClassificationReportBuilder):
            self._unified_classification_build()
        elif isinstance(self.obj, _UnifiedRegressionReportBuilder):
            self._unified_regression_build()
        elif isinstance(self.obj, DataFrame):
            if key is None:
                key = self.obj.columns[0]
                logger.warning("key has not been set. The first column has been treated as the index column.")
            if self.is_time_series:
                self._time_series_dataset_report_build(key, endog, time_series_report_template)
            else:
                self._dataset_report_build(key, scatter_matrix_sampling, ignore_scatter_matrix, ignore_correlation, subset_bins)
        elif isinstance(self.obj, (AutomaticClassification, AutomaticRegression)):
            pass
        elif isinstance(self.obj, PAL_TIME_SERIES_LIST):
            self.obj.build_report()
        elif isinstance(self.obj, AutoTimeSeries):
            self.obj.build_report()
        else:
            raise NotImplementedError
        return self

    @deprecated("This method is deprecated. AUC points can be controled by the parameter `ntiles`.")
    def set_metric_samplings(self, roc_sampling: Sampling = None, other_samplings: dict = None): # pragma: no cover
        """
        Set metric samplings to report builder.

        Parameters
        ----------
        roc_sampling : :class:`~hana_ml.algorithms.pal.preprocessing.Sampling`, optional
            ROC sampling.

        other_samplings : dict, optional
            Key is column name of metric table.

                - CUMGAINS
                - RANDOM_CUMGAINS
                - PERF_CUMGAINS
                - LIFT
                - RANDOM_LIFT
                - PERF_LIFT
                - CUMLIFT
                - RANDOM_CUMLIFT
                - PERF_CUMLIFT

            Value is sampling.

        Examples
        --------
        Creating the metric samplings:

        >>> roc_sampling = Sampling(method='every_nth', interval=2)

        >>> other_samplings = dict(CUMGAINS=Sampling(method='every_nth', interval=2),
                              LIFT=Sampling(method='every_nth', interval=2),
                              CUMLIFT=Sampling(method='every_nth', interval=2))
        >>> unified_report.set_metric_samplings(roc_sampling, other_samplings)
        """
        self.obj.roc_sampling = roc_sampling
        self.obj.other_samplings = other_samplings

        return self

    def tree_debrief(self, save_html=None, digraph=True, **kwargs):
        """
        Visualize tree model.

        Parameters
        ----------
        save_html : str, optional
            If it is not None, the function will generate a html report and stored in the given name.

            Defaults to None.

        digraph : bool, optional
            If True, it will output the digraph tree structure.

            Defaults to False.
        """
        model = self.obj.model_[0] if isinstance(self.obj.model_, (list, tuple)) else self.obj.model_
        if not digraph:
            if save_html:
                TreeModelDebriefing.tree_export(model, filename=save_html, **kwargs)
            else:
                TreeModelDebriefing.tree_debrief(model, **kwargs)
        else:
            if save_html:
                TreeModelDebriefing.tree_export_with_dot(model, filename=save_html, **kwargs)
            else:
                TreeModelDebriefing.tree_debrief_with_dot(model, **kwargs)

    def display(self, save_html=None, metric_sampling=False):
        """
        Display the report.

        Parameters
        ----------
        save_html : str, optional
            If it is not None, the function will generate a html report and stored in the given name.

            Defaults to None.
        metric_sampling : bool, optional (deprecated)
            Whether the metric table needs to be sampled. It is only valid for UnifiedClassification and used together with set_metric_samplings.
            Since version 2.14, the metric_sampling is no need to specify and replaced by ntiles in unified API parameter settings.

            Defaults to False.
        """
        if metric_sampling:
            logger.warning("metric_sampling is not used any more. It is replaced by ntiles paramter in unified API.")
        elif isinstance(self.obj, _UnifiedClassificationReportBuilder):
            self._unified_classification_display(save_html)
        elif isinstance(self.obj, _UnifiedRegressionReportBuilder):
            self._unified_regression_display(save_html)
        elif isinstance(self.obj, DataFrame):
            if self.is_time_series:
                self._time_series_dataset_report_display(save_html)
            else:
                self._dataset_report_display(save_html)
        elif isinstance(self.obj, (AutomaticClassification, AutomaticRegression)):
            self._pipeline_report_display(save_html)
        elif isinstance(self.obj, PAL_TIME_SERIES_LIST):
            self._time_series_report_display(save_html)
        elif isinstance(self.obj, AutoTimeSeries):
            self._time_series_report_display(save_html)
        else:
            raise NotImplementedError

    def _time_series_dataset_report_build(self, key, endog, params=None):
        report_helper = TimeSeriesDatasetReportHelper(params=params)
        self.dataset_report = report_helper.build(self.obj, key, endog)

    def _dataset_report_build(self, key, scatter_matrix_sampling, ignore_scatter_matrix, ignore_correlation, subset_bins):
        self.dataset_report = DatasetReportBuilder()
        self.dataset_report.build(self.obj, key, scatter_matrix_sampling, ignore_scatter_matrix, ignore_correlation, subset_bins)

    def _unified_classification_build(self):
        self.obj.build_report()

    def _unified_regression_build(self):
        self.obj.build_report()

    def _time_series_dataset_report_display(self, save_html):
        if save_html is None:
            self.dataset_report.generate_notebook_iframe()
        else:
            self.dataset_report.generate_html(save_html)

    def _dataset_report_display(self, save_html):
        if hasattr(self, 'dataset_framework_version'):
            self.dataset_report.set_framework_version(self.dataset_framework_version)
        if save_html is None:
            self.dataset_report.generate_notebook_iframe_report()
        else:
            self.dataset_report.generate_html_report(save_html)

    def _unified_classification_display(self, save_html):
        if save_html is None:
            self.obj.generate_notebook_iframe_report()
        else:
            self.obj.generate_html_report(filename=save_html)

    def _unified_regression_display(self, save_html):
        if save_html is None:
            self.obj.generate_notebook_iframe_report()
        else:
            self.obj.generate_html_report(save_html)

    def _time_series_report_display(self, save_html):
        if save_html is None:
            self.obj.generate_notebook_iframe_report()
        else:
            self.obj.generate_html_report(save_html)

    def _pipeline_report_display(self, save_html):
        if save_html is None:
            BestPipelineReport(self.obj).generate_notebook_iframe()
        else:
            BestPipelineReport(self.obj).generate_html(filename=save_html)

    def get_iframe_report(self):
        """
        Return iframe report without display.
        """
        if isinstance(self.obj, DataFrame):
            if self.dataset_report is None:
                raise FitIncompleteError("Unified report has not been built.")
            return self.dataset_report.get_iframe_report_html()
        else:
            return self.obj.report
