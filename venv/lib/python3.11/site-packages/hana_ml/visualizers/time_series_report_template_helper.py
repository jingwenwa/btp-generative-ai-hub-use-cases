"""
This module contains utility function to build time series report from template.
"""
# pylint: disable=too-many-instance-attributes, unused-import, eval-used, too-many-nested-blocks, broad-except
#pylint: disable=too-few-public-methods
import logging
import os
import json
from tqdm import tqdm

from hana_ml.algorithms.pal.tsa.changepoint import BCPD
from hana_ml.visualizers.time_series_report import DatasetAnalysis, TimeSeriesReport
from hana_ml.visualizers.report_builder import Page, AlertItem

logger = logging.getLogger(__name__) #pylint: disable=invalid-name

def _load_ts_report_template(is_timestamp=True):
    if is_timestamp:
        report_template = os.path.join(os.path.dirname(__file__), "templates",
                                       "dataset_analysis_timestamp_template.json")
    else:
        report_template = os.path.join(os.path.dirname(__file__), "templates",
                                       "dataset_analysis_int_template.json")
    with open(report_template) as input_file:
        return json.load(input_file)

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

def _is_datetime(data, key):
    if key not in data.columns:
        return False
    if 'TIME' in data.get_table_structure()[key]:
        return True
    if 'DATE' in data.get_table_structure()[key]:
        return True
    return False


class TimeSeriesDatasetReportHelper:
    """
    Utility function to generate time series report from dataset.

    Parameters
    ----------
    data : DataFrame
        The data to be analyzed.
    endog : str
        The endogenous variable.
    params : dict, optional
        The parameters for the analysis.
    """
    _default_params = {
        "stationarity_item": {
            "method": None,
            "mode": None,
            "lag": None,
            "probability": None
        },
        "pacf_item": {
            "thread_ratio": None,
            "method": None,
            "max_lag": None,
            "calculate_confint": True,
            "alpha": None,
            "bartlett": None
        },
        "moving_average_item": {
            "rolling_window": -3
        },
        "rolling_stddev_item": {
            "rolling_window": 10
        },
        "real_item": {},
        "seasonal_item": {},
        "seasonal_decompose_items": {
            "alpha": None,
            "thread_ratio": None,
            "decompose_type": None,
            "extrapolation": None,
            "smooth_width": None
        },
        "timeseries_box_item": [{
            "cycle": "YEAR"
        },
        {
            "cycle": "QUARTER"
        },
        {
            "cycle": "MONTH"
        },
        {
            "cycle": "WEEK"
        }],
        "quarter_item": {},
        "outlier_item": {
            "window_size": None,
            "detect_seasonality": None,
            "alpha": None,
            "periods": None,
            "outlier_method": None,
            "threshold": None
        },
        "change_points_item": {
            "cp_object": BCPD(max_tcp=2, max_scp=1, max_harmonic_order=10, random_seed=1, max_iter=10000)
        }
    }
    def __init__(self,
                 title="Dataset Analysis",
                 params=None):
        self.title = title
        self.params = self._default_params
        if params:
            self.params = _deep_update_for_dict(self.params, params)
        self.report = None

    def build(self,
              data,
              key,
              endog):
        """
        Build function.

        Parameters
        ----------
        data : DataFrame
            The data to be analyzed.
        endog : str
            The endogenous variable.
        """
        is_datetime = _is_datetime(data, key)
        self.report = TimeSeriesReport(self.title)
        dataset_analysis = DatasetAnalysis(data=data, endog=endog, key=key)
        pages = []
        progress = tqdm(total=8, position=0)
        stationary_params = self.params.get("stationarity_item", None)
        try:
            if stationary_params is not None:
                page0 = Page('Stationarity')
                page0.addItem(dataset_analysis.stationarity_item(
                    method=stationary_params.get('method', None),
                    mode=stationary_params.get('mode', None),
                    lag=stationary_params.get('lag', None),
                    probability=stationary_params.get('probability', None)))
                pages.append(page0)
        except Exception as err:
            logger.error(err)
            pass
        progress.update(1)

        try:
            pacf_params = self.params.get("pacf_item", None)
            if pacf_params is not None:
                page1 = Page('Partial Autocorrelation')
                page1.addItem(dataset_analysis.pacf_item(
                    thread_ratio=pacf_params.get("thread_ratio", None),
                    method=pacf_params.get("method", None),
                    max_lag=pacf_params.get("max_lag", None),
                    calculate_confint=pacf_params.get("calculate_confint", True),
                    alpha=pacf_params.get("alpha", None),
                    bartlett=pacf_params.get("bartlett", None)))
                pages.append(page1)
        except Exception as err:
            logger.error(err)
            pass
        progress.update(1)

        try:
            moving_average_params = self.params.get("moving_average_item", None)
            rolling_stddev_params = self.params.get("rolling_stddev_item", None)
            if moving_average_params is not None or rolling_stddev_params is not None:
                page2 = Page('Rolling Mean and Standard Deviation')

                page2.addItems([dataset_analysis.moving_average_item
                (
                    rolling_window=moving_average_params.get("rolling_window", -3)),
                    dataset_analysis.rolling_stddev_item(rolling_window=rolling_stddev_params.get("rolling_window", 10))])
                pages.append(page2)
        except Exception as err:
            logger.error(err)
            pass
        progress.update(1)

        try:
            page3 = Page('Real and Seasonal')
            page3_count = 0
            real_params = self.params.get("real_item", None)
            if real_params is not None:
                page3.addItem(dataset_analysis.real_item())
                page3_count = page3_count + 1
            if is_datetime:
                seasonal_params = self.params.get("seasonal_item", None)
                if seasonal_params is not None:
                    page3_count = page3_count + 1
                    page3.addItem(dataset_analysis.seasonal_item()) #index to be datetime
            seasonal_decompose_params = self.params.get("seasonal_decompose_items", None)
            if seasonal_decompose_params:
                page3_count = page3_count + 1
                page3.addItems(dataset_analysis.seasonal_decompose_items(
                    alpha=seasonal_decompose_params.get('alpha', None),
                    thread_ratio=seasonal_decompose_params.get('thread_ratio', None),
                    decompose_type=seasonal_decompose_params.get('decompose_type', None),
                    extrapolation=seasonal_decompose_params.get('extrapolation', None),
                    smooth_width=seasonal_decompose_params.get('smooth_width', None)))
            if page3_count > 0:
                pages.append(page3)
        except Exception as err:
            logger.error(err)
            pass
        progress.update(1)

        try:
            if is_datetime:
                timeseries_box_params = self.params.get("timeseries_box_item", None)
                if timeseries_box_params is not None:
                    page4 = Page('Box')
                    if isinstance(timeseries_box_params, (list, tuple)):
                        _cycle_list = ['YEAR', 'QUARTER', 'MONTH', 'WEEK']
                        for idx, item in enumerate(timeseries_box_params):
                            if idx > 3:
                                break
                            page4.addItem(dataset_analysis.timeseries_box_item
                            (cycle=item.get('cycle', _cycle_list[idx])))
                    else:
                        page4.addItem(dataset_analysis.timeseries_box_item(cycle=timeseries_box_params.get('cycle', None)))
                    pages.append(page4)
        except Exception as err:
            logger.error(err)
            pass
        progress.update(1)

        try:
            if is_datetime:
                page5 = Page('Quarter')
                page5.addItem(dataset_analysis.quarter_item()) #index to be datetime
                pages.append(page5)
        except Exception as err:
            logger.error(err)
            pass
        progress.update(1)

        try:
            outlier_params = self.params.get("outlier_item", None)
            if outlier_params is not None:
                page6 = Page('Outlier')
                page6.addItem(dataset_analysis.outlier_item(
                    window_size=outlier_params.get('window_size', None),
                    detect_seasonality=outlier_params.get('detect_seasonality', None),
                    alpha=outlier_params.get('alpha', None),
                    periods=outlier_params.get('periods', None),
                    outlier_method=outlier_params.get('outlier_method', None),
                    threshold=outlier_params.get('threshold', None)))
                pages.append(page6)
        except Exception as err:
            logger.error(err)
            pass
        progress.update(1)

        try:
            change_points_params = self.params.get("change_points_item", None)
            if change_points_params is not None:
                cp_object = change_points_params.get('cp_object', None)
                if cp_object:
                    page7 = Page('Change Points')
                    change_points_params = self.params["change_points_item"]
                    page7.addItem(dataset_analysis.change_points_item(cp_object=cp_object))
                    pages.append(page7)
        except Exception as err:
            logger.error(err)
            pass
        progress.update(1)
        progress.close()

        self.report.addPages(pages)
        self.report.build()
        return self.report

class TimeSeriesTemplateReportHelper:
    """
    Utility function to generate time series report from JSON template.
    """
    def __init__(self,
                 obj,
                 fit_data=None,
                 template=None,
                 name="HANA ML Timeseries Report"):
        if hasattr(obj, 'massive'):
            if obj.massive:
                raise NotImplementedError("Currently time series report doesn't support PAL massive mode.")
        if fit_data:
            self.fit_data = fit_data
        else:
            if hasattr(obj, "training_data"):
                self.fit_data = obj.training_data
            else:
                self.fit_data = obj.fit_args_[0]
        if hasattr(obj, "endog"):
            self.endog = obj.endog
        else:
            self.endog = self.fit_data.columns[1]
        if hasattr(obj, "exog"):
            self.exog = obj.exog
        else:
            self.exog = self.fit_data.columns[2:]
        if hasattr(obj, "key"):
            self.key = obj.key
        else:
            self.key = self.fit_data.columns[0]
        is_timestamp = True
        if 'INT' in self.fit_data.get_table_structure()[self.key]:
            is_timestamp = False
        if template is None:
            self.template = _load_ts_report_template(is_timestamp)
        self.pages = []
        self.name = name

    def build_report(self):
        """
        Build function.
        """
        if self.fit_data.hasna():
            self.fit_data = self.fit_data.fillna(0)
            logger.warning("Missing value has been replaced by 0.")
        pages = []
        dataset_analysis = DatasetAnalysis(data=self.fit_data, endog=self.endog, key=self.key)
        for page in tqdm(self.template["DatasetAnalysis"]):
            temp = Page(page["name"])
            for item in page["items"]:
                if list(item.keys())[0] == 'change_points_item':
                    if self.fit_data.count() > 5000:
                        # logger.warning("Too long data for BCPD! Ignore the calculation.")
                        continue
                item_params = []
                for kkey, vval in item[list(item.keys())[0]].items():
                    if isinstance(vval, str):
                        if list(item.keys())[0] != 'change_points_item':
                            item_params.append("{}='{}'".format(kkey, vval))
                        else:
                            item_params.append("{}={}".format(kkey, vval))
                    else:
                        item_params.append("{}={}".format(kkey, vval))
                execute_str = "temp.addItems(dataset_analysis.{}({}))".format(list(item.keys())[0], ','.join(item_params))
                logger.info(execute_str)
                try:
                    eval(execute_str)
                except Exception as err:
                    logger.error(err)
                    pass
            pages.append(temp)

        target_page: Page = None
        include_change_point = False
        for page in pages:
            for item in page.items:
                if item.title == 'Outlier':
                    target_page = page
                elif item.title == 'Change Points':
                    include_change_point = True
                    break
        if include_change_point is False and target_page is not None:
            alert_item = AlertItem('Change Points')
            alert_item.add_warning_msg('Too long data for BCPD! Ignore the calculation.')
            target_page.addItem(alert_item)

        self.pages = self.pages + pages
        return self

    def generate_html_report(self, filename=None):
        """
        Display function.
        """
        report = TimeSeriesReport(self.name)
        report.addPages(self.pages)
        report.build()
        report.generate_html(filename)

    def generate_notebook_iframe_report(self):
        """
        Display function.
        """
        report = TimeSeriesReport(self.name)
        report.addPages(self.pages)
        report.build()
        report.generate_notebook_iframe()

    def add_pages(self, pages):
        """
        Add pages to the existing report.
        """
        if isinstance(pages, (list, tuple)):
            self.pages = self.pages + pages
        else:
            self.pages.append(pages)
