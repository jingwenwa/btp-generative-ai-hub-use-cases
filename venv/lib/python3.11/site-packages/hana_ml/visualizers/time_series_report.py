"""
This module represents the whole time series report.
A report can contain many pages, and each page can contain many items.
You can use the class 'DatasetAnalysis' to generate all the items and combine them into different pages at will.

The following classes are available:
    * :class:`TimeSeriesReport`
    * :class:`DatasetAnalysis`
"""

# pylint: disable=missing-function-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=too-many-instance-attributes
# pylint: disable=dangerous-default-value
# pylint: disable=too-many-nested-blocks
# pylint: disable=invalid-name
# pylint: disable=bad-continuation
# pylint: disable=invalid-name
# pylint: disable=no-else-continue
# pylint: disable=protected-access
# pylint: disable=too-few-public-methods
# pylint: disable=not-an-iterable
# pylint: disable=unsubscriptable-object
import logging
import uuid
import json
import math
import numpy as np
from hana_ml import dataframe
from hana_ml.visualizers.report_builder import ReportBuilder, ChartItem, TableItem, ForcePlotItem
from hana_ml.visualizers.automl_report import BestPipelineReport


logger = logging.getLogger(__name__)


def convert_value_to_target_type(value, base_value=None):
    if value is None:
        return None
    else:
        try:
            value = str(value)
            new_value = float(value)
            if np.isnan(new_value):
                new_value = None
            if base_value is not None:
                try:
                    base_value = float(base_value)
                    new_value = new_value + base_value
                    if np.isnan(new_value):
                        new_value = None
                except (TypeError, ValueError):
                    pass
            return new_value
        except (TypeError, ValueError):
            return value


def push_valid_value(data, value, skip_none_value=False):
    if data is None:
        return
    if value is None:
        if skip_none_value:
            pass
        else:
            data.append('-')
    else:
        data.append(value)


class TimeSeriesExplainer(object):
    def __init__(self, key, endog, exog):
        self.key = key
        self.endog = endog
        self.exog = exog

        self.line_configs = []
        self.confidence_interval_configs = []

        self.training_data: dataframe.DataFrame = None
        self.training_data_pd_df = None

        self.fitted_result: dataframe.DataFrame = None
        self.fitted_result_pd_df = None

        self.forecasted_data: dataframe.DataFrame = None
        self.forecasted_data_pd_df = None
        self.forecasted_feature_values_dict = None

        self.forecasted_result: dataframe.DataFrame = None
        self.forecasted_result_pd_df = None

        self.forecasted_result_explainer: dataframe.DataFrame = None
        self.forecasted_result_explainer_pd_df = None
        self.forecasted_result_explainer_key_data = None
        self.forecasted_feature_effects_dict = None
        self.get_cell_feature_effect_dict_func = None
        self.reason_code_name = None
        self.decomposition_item_configs = None
        self.classified_feature_names = None

    def set_training_data(self, training_data: dataframe.DataFrame):
        self.training_data = training_data
        self.training_data_pd_df = self.training_data.collect()

    def set_fitted_result(self, fitted_result: dataframe.DataFrame):
        self.fitted_result = fitted_result
        self.fitted_result_pd_df = self.fitted_result.collect()

    def set_forecasted_data(self, forecasted_data: dataframe.DataFrame):
        self.forecasted_data = forecasted_data
        self.forecasted_data_pd_df = forecasted_data.collect()

        if self.exog is not None and len(self.exog) > 0:
            self.forecasted_feature_values_dict = {}
            for feature_name in self.exog:
                self.forecasted_feature_values_dict[feature_name] = list(self.forecasted_data_pd_df[feature_name])

    def set_forecasted_result(self, forecasted_result: dataframe.DataFrame):
        self.forecasted_result = forecasted_result
        self.forecasted_result_pd_df = forecasted_result.collect()

    def set_forecasted_result_explainer(self, forecasted_result_explainer: dataframe.DataFrame, reason_code_name='EXOGENOUS'):
        self.forecasted_result_explainer = forecasted_result_explainer
        self.forecasted_result_explainer_pd_df = forecasted_result_explainer.collect()
        self.forecasted_result_explainer_key_data = []
        for d in list(self.forecasted_result_explainer_pd_df[self.key].astype(str)):
            d = convert_value_to_target_type(d)
            push_valid_value(self.forecasted_result_explainer_key_data, d)

        if self.exog is not None and len(self.exog) > 0:
            if reason_code_name is None or reason_code_name == '':
                pass
            else:
                if self.get_cell_feature_effect_dict_func is None:
                    raise ValueError('Please set get_cell_feature_effect_dict_func attribute of instance.')
                self.reason_code_name = reason_code_name

                def get_feature_effects_dict(feature_names, json_str_data, get_cell_feature_effect_dict_func):
                    feature_effects_dict = {}
                    for feature_name in feature_names:
                        feature_effects_dict[feature_name] = []
                    for json_str_cell in json_str_data:
                        cell_feature_effect_dict = {}
                        if json_str_cell is not None:
                            cell_feature_effect_dict = get_cell_feature_effect_dict_func(json_str_cell)
                        for feature_name in feature_names:
                            res = cell_feature_effect_dict.get(feature_name)
                            if res is None:
                                res = 'undefined'
                            feature_effects_dict[feature_name].append(res)
                    return feature_effects_dict

                reason_code_data = list(self.forecasted_result_explainer_pd_df[self.reason_code_name])
                self.forecasted_feature_effects_dict = get_feature_effects_dict(self.exog, reason_code_data, self.get_cell_feature_effect_dict_func)

    def add_line_to_comparison_item(self, title, data, x_name, y_name=None, confidence_interval_names=None, color=None):
        if y_name is not None:
            self.line_configs.append((title, data, x_name, y_name, 'line', color))
        if confidence_interval_names is not None and len(confidence_interval_names) == 2:
            lower_line_name = confidence_interval_names[0]
            upper_line_name = confidence_interval_names[1]
            if lower_line_name is not None and lower_line_name != '' and upper_line_name is not None and upper_line_name != '':
                if color is None or color == '':
                    color = '#ccc'
                self.confidence_interval_configs.append((title, data, x_name, confidence_interval_names, 'line', color))
            else:
                logger.error('The parameter confidence_interval_names of add_line_to_comparison_item method is not correct.')
        return self

    def get_comparison_item(self, title="Comparison"):
        if len(self.line_configs) == 0 and len(self.confidence_interval_configs) == 0:
            logger.error('Please add line by calling add_line_to_comparison_item method.')

        y_base = 0
        pair_confidence_names = []
        if self.confidence_interval_configs is not None:
            for config in self.confidence_interval_configs:
                pair_confidence_names.append(['{}({})'.format(config[0], config[3][0]), '{}({})'.format(config[0], config[3][1])])
                d_data = []
                for d in list(config[1].collect()[config[3][0]]):  # lower_data
                    d = convert_value_to_target_type(d)
                    push_valid_value(d_data, d, skip_none_value=True)
                if len(d_data) > 0:
                    y_base = math.floor(min(y_base, min(d_data)))
            y_base = -y_base

        line_titles = []
        lower_line_series_names = []
        upper_line_series_names = []
        dataset_index = -1
        datasets = []
        series = []
        x_data = []
        for config in self.line_configs:
            if config[0] in line_titles:
                raise ValueError('Multiple lines use the same title {}.'.format(config[0]))
            else:
                line_titles.append(config[0])
            result = config[1]
            try:
                result = result.collect().sort_values(config[2], ascending=True)
                x = []
                y = []
                for xd in list(result[config[2]].astype(str)):
                    xd = convert_value_to_target_type(xd)
                    push_valid_value(x, xd)
                for yd in list(result[config[3]]):
                    yd = convert_value_to_target_type(yd, y_base)
                    push_valid_value(y, yd)
                x_data.extend(x)
                dataset_index = dataset_index + 1
                datasets.append({
                    'source': [x, y]
                })
                temp_series = {
                    'name': config[0],
                    'type': config[4],
                    'datasetIndex': dataset_index,
                    'seriesLayoutBy': 'row',
                    'symbol': 'none',
                    'smooth': 'true',
                    'emphasis': {
                        'focus': 'series'
                    }
                }
                if config[5] is not None:
                    temp_series['color'] = config[5]
                series.append(temp_series)
            except KeyError:
                pass
        k = -1
        for config in self.confidence_interval_configs:
            if config[0] in line_titles:
                raise ValueError('Multiple lines use the same title {}.'.format(config[0]))
            else:
                line_titles.append(config[0])
            k = k + 1
            if config[1] is not None:
                result = config[1]
                try:
                    result = result.collect().sort_values(config[2], ascending=True)
                    x = []
                    for d in list(result[config[2]].astype(str)):
                        d = convert_value_to_target_type(d)
                        x.append(d)
                    x_data.extend(x)
                    lower = []
                    upper = []
                    temp_lower = list(result[config[3][0]])
                    temp_upper = list(result[config[3][1]])
                    for i in [0, 1]:
                        dataset_index = dataset_index + 1
                        temp_series = {
                            'color': config[5],
                            'type': config[4],
                            'lineStyle': {
                                'opacity': 0
                            },
                            'stack': 'confidence_interval_{}'.format(k),
                            'datasetIndex': dataset_index,
                            'seriesLayoutBy': 'row',
                            'symbol': 'none',
                            'smooth': 'true',
                        }
                        if i == 0:
                            for d in temp_lower:
                                d = convert_value_to_target_type(d, y_base)
                                push_valid_value(lower, d)
                            datasets.append({
                                'source': [x, lower]
                            })
                            lower_line_series_names.append('{}({})'.format(config[0], config[3][i]))
                        elif i == 1:
                            j = -1
                            for d in temp_upper:
                                j = j + 1
                                if temp_lower[j]:
                                    d = convert_value_to_target_type(d, -float(temp_lower[j]))
                                push_valid_value(upper, d)
                            datasets.append({
                                'source': [x, upper]
                            })
                            temp_series['areaStyle'] = {
                                'color': config[5]
                            }
                            upper_line_series_names.append('{}({})'.format(config[0], config[3][i]))
                        temp_series['name'] = '{}({})'.format(config[0], config[3][i])
                        series.append(temp_series)
                except KeyError:
                    pass

        x_data = list(set(x_data))

        def convert_data(d):
            try:
                return int(d)
            except ValueError:
                return d
        x_data.sort(key=convert_data)

        option = {
            'customFalseFlag': ['xAxis.axisTick.show'],
            'customFn': ['yAxis.axisLabel.formatter', 'tooltip.formatter'],
            'dataset': datasets,
            'grid': {
                'show': 'true',
                'containLabel': 'true'
            },
            'legend': {
                'type': 'scroll'
            },
            'xAxis': {
                'type': 'category',
                'data': x_data,
                'name': self.key,
                'boundaryGap': 'true',
                'axisTick': {
                    'alignWithLabel': 'true',
                    'show': 'false'
                },
                'axisLabel': {
                    'showMinLabel': 'true',
                    'showMaxLabel': 'true',
                    'hideOverlap': 'true',
                    'rotate': 90,
                    'fontSize': 9
                }
            },
            'yAxis': {
                'type': 'value',
                'name': title,
                'axisLine': {
                    'show': 'true',
                },
                'axisTick': {
                    'show': 'true'
                },
                'axisLabel': {
                    'formatter': {
                        'params': ['val'],
                        'body': 'return (val - {});'.format(y_base)
                    }
                }
            },
            'tooltip': {
                'trigger': 'axis',
                'formatter': {
                    'params': ['params'],
                    'body': ''.join([
                        "var br='<br />';",
                        "var str_sequence=params[0].name+br;",
                        "var i=-1;",
                        "var upper_line_series_names=" + str(upper_line_series_names) + ";",
                        "var lower_line_series_names=" + str(lower_line_series_names) + ";",
                        "function connectStrSequence(marker,name,value){str_sequence=str_sequence+marker+name+' ---> '+value+br;}",
                        "params.forEach(function(item){",
                        "i=i+1;",
                        "var value=0;",
                        "var y_base=" + str(y_base) + ";",
                        "if(upper_line_series_names.includes(item.seriesName)){if(lower_line_series_names.includes(params[i-1].seriesName)){value=item.value[1]+(params[i-1].value[1]-y_base);connectStrSequence(item.marker,item.seriesName,value);}}",
                        "else{value=(item.value[1]-y_base);connectStrSequence(item.marker,item.seriesName,value);}",
                        "});",
                        "return str_sequence;"
                    ])
                }
            },
            'series': series
        }
        if len(pair_confidence_names) > 0:
            option['customPairEffectOnLegendToggle'] = pair_confidence_names
        return ChartItem(title, option)

    def get_decomposition_items_from_forecasted_result(self):
        if self.forecasted_result_explainer is None:
            raise ValueError('Please set explainer of forecasted result by calling set_forecasted_result_explainer method.')
        if self.decomposition_item_configs is None:
            raise ValueError('Please set decomposition_item_configs attribute of instance.')

        min_max_y_data = [0]
        decomposition_options = []
        decomposition_items = []
        for decomposition_item_config in self.decomposition_item_configs:
            datasets = []
            series = []
            decomposition_name = decomposition_item_config[0]
            decomposition_format = decomposition_item_config[1]  # data format of decomposition: plain | json
            decomposition_type = decomposition_item_config[2]
            x = self.forecasted_result_explainer_key_data
            if decomposition_format == 'plain':
                y = []
                valid_y = []
                for d in list(self.forecasted_result_explainer_pd_df[decomposition_name]):
                    d = convert_value_to_target_type(d)
                    push_valid_value(y, d)
                    push_valid_value(valid_y, d, skip_none_value=True)
                if len(valid_y) > 0:
                    min_max_y_data.append(min(valid_y))
                    min_max_y_data.append(max(valid_y))
                datasets.append({
                    'source': [x, y]
                })
                series.append({
                    'name': decomposition_name,
                    'type': decomposition_type,
                    'datasetIndex': 0,
                    'seriesLayoutBy': 'row',
                    'symbol': 'none',
                    'smooth': 'true',
                    'emphasis': {
                        'focus': 'series'
                    }
                })
            elif decomposition_format == 'json':
                temp_dict = {}
                for cell in list(self.forecasted_result_explainer_pd_df[decomposition_name]):
                    cell_dict = json.loads(cell)
                    for k in cell_dict:
                        if temp_dict.get(k) is None:
                            temp_dict[k] = []
                        temp_dict[k].append(cell_dict[k])
                source = [x]
                for k in temp_dict:
                    min_max_y_data.append(min(temp_dict[k]))
                    min_max_y_data.append(max(temp_dict[k]))
                    source.append(temp_dict[k])
                    temp_series = {
                        'name': k,
                        'type': decomposition_type,
                        'datasetIndex': 0,
                        'seriesLayoutBy': 'row',
                        'symbol': 'none',
                        'smooth': 'true',
                        'emphasis': {
                            'focus': 'series'
                        }
                    }
                    if decomposition_type == 'bar':
                        temp_series['stack'] = decomposition_name
                    series.append(temp_series)
                datasets.append({
                    'source': source
                })
            decomposition_option = {
                'customFalseFlag': ['xAxis.axisTick.show'],
                'dataset': datasets,
                'grid': {
                    'show': 'true',
                    'containLabel': 'true'
                },
                'xAxis': {
                    'type': 'category',
                    'name': self.key,
                    'boundaryGap': 'true',
                    'axisTick': {
                        'alignWithLabel': 'true',
                        'show': 'false'
                    },
                    'axisLabel': {
                        'showMinLabel': 'true',
                        'showMaxLabel': 'true',
                        'hideOverlap': 'true',
                        'rotate': 90,
                        'fontSize': 9
                    }
                },
                'yAxis': {
                    'type': 'value',
                    'name': decomposition_name,
                    'axisLine': {
                        'show': 'true',
                    },
                    'axisTick': {
                        'show': 'true'
                    }
                },
                'tooltip': {
                    'trigger': 'axis'
                },
                'series': series
            }
            if decomposition_format == 'json':
                decomposition_option['legend'] = {}
            decomposition_options.append(decomposition_option)
            decomposition_items.append(ChartItem(decomposition_name, decomposition_option))

        def get_floor_value(value):
            if value < 0:
                new_value = -value
            else:
                new_value = value
            base_value = math.pow(10, len(str(new_value).split('.', maxsplit=1)[0]) - 1)
            new_value = (math.floor(new_value / base_value) + 1) * base_value
            if value < 0:
                return -new_value
            else:
                return new_value
        min_y_data = min(min_max_y_data)
        max_y_data = max(min_max_y_data)
        min_y_data = get_floor_value(min_y_data)
        max_y_data = get_floor_value(max_y_data)
        for i in range(0, len(decomposition_options)):
            decomposition_option = decomposition_options[i]
            if self.decomposition_item_configs[i][2] == 'line':
                decomposition_option['yAxis']['min'] = min_y_data
                decomposition_option['yAxis']['max'] = max_y_data
        return decomposition_items

    def get_summary_plot_items_from_forecasted_result(self):
        if self.exog is None or len(self.exog) == 0:
            logger.warning('Illegal call for get_summary_plot_items_from_forecasted_result method.')
            return [ChartItem('Exogenous Effect', None), ChartItem('Exogenous Effect and Value', None)]

        if self.forecasted_data is None:
            raise ValueError('Please set forecasted data by calling set_forecasted_data method.')
        if self.forecasted_result_explainer is None:
            raise ValueError('Please set explainer of forecasted result by calling set_forecasted_result_explainer method.')

        feature_x_y_z_dict = {}
        for feature_name in self.exog:
            current_feature_data = self.forecasted_feature_values_dict[feature_name]
            current_feature_effect = self.forecasted_feature_effects_dict[feature_name]
            data = []
            index = 0
            for x in self.forecasted_result_explainer_key_data:
                if current_feature_effect[index] == 'undefined':
                    data.append([x, 0.0, current_feature_data[index]])
                else:
                    data.append([x, current_feature_effect[index], current_feature_data[index]])
                index = index + 1
            feature_x_y_z_dict[feature_name] = data

        classified_items = [('Exogenous Effect', []), ('Exogenous Effect and Value', [])]
        classified_feature_names = []
        if self.classified_feature_names is not None:
            for classified_name, feature_names in self.classified_feature_names:
                if feature_names is not None and len(feature_names) > 0:
                    classified_feature_names.append((classified_name, feature_names))
        else:
            classified_feature_names.append(('', self.exog))

        for classified_name, feature_names in classified_feature_names:
            for i in range(0, len(classified_items)):
                option = {
                    'customFalseFlag': ['xAxis.axisTick.show'],
                    'grid': {
                        'show': 'true',
                        'containLabel': 'true'
                    },
                    'legend': {},
                    'xAxis': {
                        'type': 'category',
                        'boundaryGap': 'true',
                        'name': self.key,
                        'axisTick': {
                            'alignWithLabel': 'true',
                            'show': 'false'
                        },
                        'axisLabel': {
                            'showMinLabel': 'true',
                            'showMaxLabel': 'true',
                            'hideOverlap': 'true',
                            'rotate': 90,
                            'fontSize': 9
                        }
                    },
                    'yAxis': {
                        'type': 'value',
                        'name': 'Effect',
                        'axisLine': {
                            'show': 'true',
                        },
                        'axisTick': {
                            'show': 'true'
                        }
                    },
                    'tooltip': {}
                }
                series = []
                if i == 0:
                    for feature_name in feature_names:
                        series.append({
                            'name': feature_name,
                            'type': 'bar',
                            'stack': 'feature_effect',
                            'data': feature_x_y_z_dict[feature_name],
                            'emphasis': {
                                'focus': 'series'
                            }
                        })
                elif i == 1:
                    visual_map = []
                    for feature_name in feature_names:
                        visual_map.append({
                            'show': 'false',
                            'dimension': 2,
                            'min': min(self.forecasted_feature_values_dict[feature_name]),
                            'max': max(self.forecasted_feature_values_dict[feature_name]),
                            'seriesIndex': [self.exog.index(feature_name)],
                            'inRange': {
                                'symbolSize': [5, 20]
                            }
                        })
                    option['visualMap'] = visual_map
                    for feature_name in feature_names:
                        series.append({
                            'name': feature_name,
                            'type': 'scatter',
                            'data': feature_x_y_z_dict[feature_name],
                            'emphasis': {
                                'focus': 'series'
                            }
                        })
                option['series'] = series
                if classified_name == '':
                    classified_items[i][1].append(ChartItem(classified_items[i][0], option))
                else:
                    classified_items[i][1].append(ChartItem('{}({})'.format(classified_items[i][0], classified_name), option))
        items = []
        for cur_classified_items in classified_items:
            items.extend(cur_classified_items[1])
        return items

    def get_force_plot_item_from_forecasted_result(self):
        if self.exog is None or len(self.exog) == 0:
            logger.warning('Illegal call for get_force_plot_item_from_forecasted_result method.')
            return ForcePlotItem('Force Plot of Exogenous', None)

        if self.forecasted_data is None:
            raise ValueError('Please set forecasted data by calling set_forecasted_data method.')
        if self.forecasted_result_explainer is None:
            raise ValueError('Please set explainer of forecasted result by calling set_forecasted_result_explainer method.')

        return ForcePlotItem('Force Plot of Exogenous', {
            'featureNames': self.exog,
            'featureBaseEffect': 0.0,
            'featureValues': self.forecasted_feature_values_dict,
            'featureEffects': self.forecasted_feature_effects_dict
        })

    @staticmethod
    def get_items_from_best_pipeline(best_pipeline: dataframe.DataFrame, highlighted_metric_name):
        report_builder: ReportBuilder = BestPipelineReport.create_report_builder(best_pipeline, highlighted_metric_name)
        items = []
        report_builder.build()
        for page in report_builder.pages:
            for item in page.items:
                items.append(item)
        return items


class ARIMAExplainer(TimeSeriesExplainer):
    def __init__(self, key, endog, exog):
        super().__init__(key, endog, exog)

        def get_cell_feature_effect_dict(json_str_cell):
            feature_effect_dict = {}
            for current_dict_item in json.loads(json_str_cell):
                feature_effect_dict[current_dict_item['attr']] = current_dict_item['val']
            return feature_effect_dict
        self.get_cell_feature_effect_dict_func = get_cell_feature_effect_dict
        self.decomposition_item_configs = [
            ('TREND', 'plain', 'line'),
            ('SEASONAL', 'plain', 'line'),
            ('TRANSITORY', 'plain', 'line'),
            ('IRREGULAR', 'plain', 'line')
        ]


class AdditiveModelForecastExplainer(TimeSeriesExplainer):
    def __init__(self, key, endog, exog):
        super().__init__(key, endog, exog)

        def get_cell_feature_effect_dict(json_str_cell):
            feature_effect_dict = {}
            json_dict_item = json.loads(json_str_cell)
            for feature_name in json_dict_item:
                feature_effect_dict[feature_name] = json_dict_item[feature_name]
            return feature_effect_dict
        self.get_cell_feature_effect_dict_func = get_cell_feature_effect_dict
        self.decomposition_item_configs = [
            ('TREND', 'plain', 'line'),
            ('SEASONAL', 'json', 'line'),
            ('HOLIDAY', 'json', 'bar')
        ]

    def set_seasonality_mode(self, exogenous_names_with_additive_mode, exogenous_names_with_multiplicative_mode):
        self.classified_feature_names = [
            ('Additive Mode', exogenous_names_with_additive_mode),
            ('Multiplicative Mode', exogenous_names_with_multiplicative_mode)
        ]

    def get_summary_plot_items_from_forecasted_result(self):
        if self.classified_feature_names is None:
            raise ValueError('Please set seasonality mode by calling set_seasonality_mode method.')

        return super().get_summary_plot_items_from_forecasted_result()


class BSTSExplainer(TimeSeriesExplainer):
    def __init__(self, key, endog, exog):
        super().__init__(key, endog, exog)

        def get_cell_feature_effect_dict(json_str_cell):
            feature_effect_dict = {}
            for current_dict_item in json.loads(json_str_cell):
                feature_effect_dict[current_dict_item['attr']] = current_dict_item['val']
            return feature_effect_dict
        self.get_cell_feature_effect_dict_func = get_cell_feature_effect_dict
        self.decomposition_item_configs = [
            ('TREND', 'plain', 'line'),
            ('SEASONAL', 'plain', 'line')
        ]


class TimeSeriesReport(ReportBuilder):
    """
    This class is the builder of time series report.

    Parameters
    ----------
    title : str
        The name of time series report.

    Examples
    --------
    0. Importing classes

    >>> from hana_ml.visualizers.time_series_report import TimeSeriesReport, DatasetAnalysis
    >>> from hana_ml.visualizers.report_builder import Page

    1. Creating a report instance:

    >>> report = TimeSeriesReport('Time Series Data Report')

    2. Create a data analysis instance and a page array:

    >>> dataset_analysis = DatasetAnalysis(data=df_acf, endog="Y", key="ID")
    >>> pages = []

    3. Construct the contents of each page of the report:

    >>> page0 = Page('Stationarity')
    >>> page0.addItem(dataset_analysis.stationarity_item())
    >>> pages.append(page0)

    >>> page1 = Page('Partial Autocorrelation')
    >>> page1.addItem(dataset_analysis.pacf_item())
    >>> pages.append(page1)

    >>> page2 = Page('Rolling Mean and Standard Deviation')
    >>> page2.addItems([dataset_analysis.moving_average_item(-3), dataset_analysis.rolling_stddev_item(10)])
    >>> pages.append(page2)

    >>> page3 = Page('Real and Seasonal')
    >>> page3.addItem(dataset_analysis.real_item())
    >>> page3.addItem(dataset_analysis.seasonal_item())
    >>> page3.addItems(dataset_analysis.seasonal_decompose_items())
    >>> pages.append(page3)

    >>> page4 = Page('Box')
    >>> page4.addItem(dataset_analysis.timeseries_box_item('YEAR'))
    >>> page4.addItem(dataset_analysis.timeseries_box_item('MONTH'))
    >>> page4.addItem(dataset_analysis.timeseries_box_item('QUARTER'))
    >>> pages.append(page4)

    >>> page5 = Page('Quarter')
    >>> page5.addItem(dataset_analysis.quarter_item())
    >>> pages.append(page5)

    >>> page6 = Page('Outlier')
    >>> page6.addItem(dataset_analysis.outlier_item())
    >>> pages.append(page6)

    >>> page7 = Page('Change Points')
    >>> bcpd = BCPD(max_tcp=2, max_scp=1, max_harmonic_order =10, random_seed=1, max_iter=10000)
    >>> page7.addItem(dataset_analysis.change_points_item(bcpd))
    >>> pages.append(page7)

    4. Add all pages to report instance:

    >>> report.addPages(pages)

    5. Generating notebook iframe:

    >>> report.build()
    >>> report.generate_notebook_iframe()

    6. Generating a local HTML file:

    >>> report.generate_html("TimeSeriesReport")

    An example of time series data report is below:

    .. image:: image/ts_data_report.png

    """
    # def __init__(self, title):
    #     super(TimeSeriesReport, self).__init__(title)


class DatasetAnalysis(object):
    """
    This class will generate all items of dataset analysis result.

    Parameters
    ----------

    data : DataFrame
        Input data.

    endog : str
        Name of the dependent variable.

    key : str, optional
        Name of the ID column.

        Defaults to the index column of ``data`` (i.e. data.index) if it is set.
    """
    def __init__(self, data, endog, key=None):
        if data is None or data.empty():
            raise ValueError("Dataset is empty.")
        if key is None:
            if data.index:
                key = data.index
            else:
                raise ValueError("Index should be set by key or use set_index function!")
        else:
            data = data.set_index(key)
        if endog is None:
            raise ValueError("Endog should be set by endog!")

        self.dataset = data
        self.columns = data.columns
        self.key = key
        self.endog = endog
        self.features = data.columns
        self.features.remove(key)
        self.features.remove(endog)
        # default: auto render chart
        self.lazy_load = False
        # 10W
        if self.dataset.count() >= 100000:
            # manual load
            self.lazy_load = True

    def pacf_item(self, thread_ratio=None, method=None, max_lag=None, calculate_confint=True, alpha=None, bartlett=None):
        """
        It will plot PACF for two time series data.

        Parameters
        ----------

        thread_ratio : float, optional

            The ratio of available threads.

            - 0: single thread
            - 0~1: percentage
            - Others: heuristically determined

            Valid only when ``method`` is set as 'brute_force'.

            Defaults to -1.

        method : {'auto', 'brute_force', 'fft'}, optional
            Indicates the method to be used to calculate the correlation function.

            Defaults to 'auto'.

        max_lag : int, optional
            Maximum lag for the correlation function.

        calculate_confint : bool, optional
            Controls whether to calculate confidence intervals or not.

            If it is True, two additional columns of confidence intervals are shown in the result.

            Defaults to True.

        alpha : float, optional
            Confidence bound for the given level are returned. For instance if alpha=0.05, 95 % confidence bound is returned.

            Valid only when only ``calculate_confint`` is True.

            Defaults to 0.05.

        bartlett : bool, optional

            - False: using standard error to calculate the confidence bound.
            - True: using Bartlett's formula to calculate confidence bound.

            Valid only when only ``calculate_confint`` is True.

            Defaults to True.

        Returns
        -------
        item : ChartItem
            The item for the plot.
        """
        from hana_ml.algorithms.pal.tsa import correlation_function
        col = self.endog
        res = correlation_function.correlation(data=self.dataset, key=self.key, x=col, thread_ratio=thread_ratio, method=method, max_lag=max_lag, calculate_pacf=True, calculate_confint=calculate_confint, alpha=alpha, bartlett=bartlett)
        fetch_xy = res.select(["LAG", "PACF"]).sort_values("LAG").collect()

        confidence_x = []
        confidence_l = []
        confidence_u = []
        if calculate_confint is True:
            fetch_confint = res.select(["LAG", "PACF_CONFIDENCE_BOUND"]).sort_values("LAG").collect()
            lower_bound = np.negative(fetch_confint["PACF_CONFIDENCE_BOUND"].to_numpy())
            upper_bound = fetch_confint["PACF_CONFIDENCE_BOUND"].to_numpy()

            x = fetch_confint["LAG"].to_numpy().tolist()
            lower_bound_y = lower_bound.tolist()
            upper_bound_y = upper_bound.tolist()
            for i in range(0, len(lower_bound_y)):
                if bool(np.isnan(lower_bound_y[i])) is False:
                    confidence_x.append(x[i])
                    confidence_l.append(lower_bound_y[i])
                    confidence_u.append(upper_bound_y[i])
            confidence_x.insert(0, 'ConfidenceX')
            confidence_l.insert(0, 'ConfidenceL')
            confidence_u.insert(0, 'ConfidenceU')

        X = []
        for xd in list(fetch_xy["LAG"]):
            xd = convert_value_to_target_type(xd)
            push_valid_value(X, xd)
        X.insert(0, 'LAG')
        Y = []
        for yd in list(fetch_xy["PACF"]):
            yd = convert_value_to_target_type(yd)
            push_valid_value(Y, yd)
        Y.insert(0, 'PACF')

        option = {
            'customFalseFlag': ['xAxis.axisTick.show'],
            'dataset': [
                {
                    'source': [
                        X,
                        Y,
                        Y
                    ]
                },
                {
                    'source': [
                        confidence_x,
                        confidence_l,
                        confidence_u
                    ]
                }
            ],
            'grid': {
                'show': 'true',
                'containLabel': 'true'
            },
            'xAxis': {
                'type': 'category',
                'name': 'LAG',
                'axisTick': {
                    'alignWithLabel': 'true',
                    'show': 'false'
                }
            },
            'yAxis': {
                'type': 'value',
                'name': 'PACF',
                'axisLine': {
                    'show': 'true',
                },
                'axisTick': {
                    'show': 'true'
                },
                'max': 1,
                'min': -1
            },
            'tooltip': {},
            'series': [
                {
                    'type': 'scatter',
                    'seriesLayoutBy': 'row',
                    'color': '#5698C6',
                    'symbolSize': 5
                },
                {
                    'type': 'bar',
                    'seriesLayoutBy': 'row',
                    'color': '#5698C6',
                    'barWidth': 1
                },
                {
                    'type': 'line',
                    'datasetIndex': 1,
                    'seriesLayoutBy': 'row',
                    'symbol': 'none',
                    'smooth': 'true',
                    'color': '#C00000',
                    'lineStyle': {
                        'opacity': 0
                    },
                    'areaStyle': {
                        'color': '#C00000',
                        'opacity': 0.3
                    }
                },
                {
                    'type': 'line',
                    'datasetIndex': 1,
                    'seriesLayoutBy': 'row',
                    'symbol': 'none',
                    'smooth': 'true',
                    'color': '#C00000',
                    'lineStyle': {
                        'opacity': 0
                    },
                    'areaStyle': {
                        'color': '#C00000',
                        'opacity': 0.3
                    }
                }
            ]
        }
        if self.lazy_load:
            option['lazyLoad'] = 'true'
        return ChartItem('PACF[{}]'.format(col), option)

    def moving_average_item(self, rolling_window):
        """
        It will plot rolling mean by given rolling window size.

        Parameters
        ----------

        rolling_window : int, optional
                Window size for rolling function. If negative, it will use the points before CURRENT ROW.

        Returns
        -------
        item : ChartItem
            The item for the plot.
        """
        col = self.endog
        data_ = self.dataset.select([self.key, col]).generate_feature(targets=[col], order_by=self.key, trans_func="AVG", rolling_window=rolling_window).collect()

        X = []
        for xd in list(data_.iloc[:, 0].astype(str)):
            xd = convert_value_to_target_type(xd)
            push_valid_value(X, xd)
        X.insert(0, self.key)
        Y1 = []
        for y1d in list(data_.iloc[:, 1].astype(float)):
            y1d = convert_value_to_target_type(y1d)
            push_valid_value(Y1, y1d)
        Y1.insert(0, data_.columns[1])
        Y2 = []
        for y2d in list(data_.iloc[:, 2].astype(float)):
            y2d = convert_value_to_target_type(y2d)
            push_valid_value(Y2, y2d)
        Y2.insert(0, data_.columns[2])

        option = {
            'dataset': {
                'source': [
                    X,
                    Y1,
                    Y2
                ]
            },
            'grid': {
                'show': 'true',
                'containLabel': 'true'
            },
            'legend': {},
            'xAxis': {
                'type': 'category',
                'axisTick': {
                    'alignWithLabel': 'true'
                },
                'axisLabel': {
                    'showMinLabel': 'true',
                    'showMaxLabel': 'true',
                    'hideOverlap': 'true',
                    'rotate': 45,
                    'fontSize': 9,
                }
            },
            'yAxis': {
                'type': 'value',
                'axisLine': {
                    'show': 'true',
                },
                'axisTick': {
                    'show': 'true'
                },
                'axisLabel': {
                    'showMinLabel': 'true',
                    'showMaxLabel': 'true',
                    'hideOverlap': 'true',
                    'rotate': 15,
                    'fontSize': 9,
                    'interval': 0
                }
            },
            'tooltip': {
                'trigger': 'axis'
            },
            'series': [
                {
                    'type': 'line',
                    'seriesLayoutBy': 'row',
                    'color': '#5698C6',
                    'name': data_.columns[1],
                    'emphasis': {
                        'focus': 'self'
                    }
                },
                {
                    'type': 'line',
                    'seriesLayoutBy': 'row',
                    'color': '#FFA65C',
                    'name': data_.columns[2],
                    'emphasis': {
                        'focus': 'self'
                    }
                }
            ]
        }
        if self.lazy_load:
            option['lazyLoad'] = 'true'
        return ChartItem('Rolling Mean[{}]'.format(col), option)

    def rolling_stddev_item(self, rolling_window):
        """
        It will plot rolling standard deviation by given rolling window size.

        Parameters
        ----------

        rolling_window : int, optional
                Window size for rolling function. If negative, it will use the points before CURRENT ROW.

        Returns
        -------
        item : ChartItem
            The item for the plot.
        """
        col = self.endog
        data_ = self.dataset.select([self.key, col]).generate_feature(targets=[col], order_by=self.key, trans_func="STDDEV", rolling_window=rolling_window).collect()

        X = []
        for xd in list(data_.iloc[:, 0].astype(str)):
            xd = convert_value_to_target_type(xd)
            push_valid_value(X, xd)
        X.insert(0, self.key)
        Y = []
        for yd in list(data_.iloc[:, 2].astype(float)):
            yd = convert_value_to_target_type(yd)
            push_valid_value(Y, yd)
        Y.insert(0, data_.columns[2])

        option = {
            'dataset': {
                'source': [
                    X,
                    Y
                ]
            },
            'grid': {
                'show': 'true',
                'containLabel': 'true'
            },
            'legend': {},
            'xAxis': {
                'type': 'category',
                'axisTick': {
                    'alignWithLabel': 'true'
                },
                'axisLabel': {
                    'showMinLabel': 'true',
                    'showMaxLabel': 'true',
                    'hideOverlap': 'true',
                    'rotate': 45,
                    'fontSize': 9,
                }
            },
            'yAxis': {
                'type': 'value',
                'axisLine': {
                    'show': 'true',
                },
                'axisTick': {
                    'show': 'true'
                },
                'axisLabel': {
                    'showMinLabel': 'true',
                    'showMaxLabel': 'true',
                    'hideOverlap': 'true',
                    'rotate': 15,
                    'fontSize': 9,
                    'interval': 0
                }
            },
            'tooltip': {
                'trigger': 'axis'
            },
            'series': [
                {
                    'type': 'line',
                    'seriesLayoutBy': 'row',
                    'color': '#5698C6',
                    'name': data_.columns[2]
                }
            ]
        }
        if self.lazy_load:
            option['lazyLoad'] = 'true'
        return ChartItem('Rolling Standard Deviation[{}]'.format(col), option)

    def seasonal_item(self):
        """
        It will plot time series data by year.

        Returns
        -------
        item : ChartItem
            The item for the plot.
        """
        col = self.endog
        df = self.dataset.select([self.key, col]).generate_feature(targets=[self.key], trans_func="YEAR")
        df_columns = df.columns
        year_column_name = df_columns[len(df_columns) - 1]
        ordered_year_names = list(set(df.collect()[year_column_name]))
        ordered_year_names.sort()

        datasets = []
        series = []
        datasetIndex = -1

        for year_name in ordered_year_names:
            datasetIndex = datasetIndex + 1
            temp_df = df.filter('"{}"={}'.format(year_column_name, year_name))
            temp_df = temp_df.generate_feature(targets=[self.key], trans_func="MONTH")
            temp_df_columns = temp_df.columns
            month_column_name = temp_df_columns[len(temp_df_columns) - 1]
            temp_df = temp_df.agg([('avg', col, 'MONTH_AVG')], group_by=month_column_name).sort(cols=[month_column_name]).collect()

            x = []
            y = []
            for xd in list(temp_df.iloc[:, 0]):
                xd = convert_value_to_target_type(xd)
                push_valid_value(x, xd)
            x.insert(0, 'MONTH')
            for yd in list(temp_df.iloc[:, 1].astype(float)):
                yd = convert_value_to_target_type(yd)
                push_valid_value(y, yd)
            y.insert(0, 'AVG({})'.format(col))
            datasets.append({
                'source': [x, y]
            })
            series.append({
                'datasetIndex': datasetIndex,
                'type': 'line',
                'seriesLayoutBy': 'row',
                'name': year_name,
                'emphasis': {
                    'focus': 'self'
                }
            })

        option = {
            'dataset': datasets,
            'grid': {
                'show': 'true',
                'containLabel': 'true'
            },
            'legend': {
                'type': 'scroll'
            },
            'xAxis': {
                'name': 'MONTH',
                'type': 'value',
                'min': 1,
                'max': 12,
                'interval': 1,
                'axisTick': {
                    'alignWithLabel': 'true'
                },
                'axisLabel': {
                    'showMinLabel': 'true',
                    'showMaxLabel': 'true',
                    'hideOverlap': 'true',
                    'fontSize': 9,
                }
            },
            'yAxis': {
                'name': 'AVG({})'.format(col),
                'type': 'value',
                'axisLine': {
                    'show': 'true',
                },
                'axisTick': {
                    'show': 'true'
                },
                'axisLabel': {
                    'showMinLabel': 'true',
                    'showMaxLabel': 'true',
                    'hideOverlap': 'true',
                    'rotate': 15,
                    'fontSize': 9,
                    'interval': 0
                }
            },
            'tooltip': {
                'trigger': 'axis'
            },
            'series': series
        }
        if self.lazy_load:
            option['lazyLoad'] = 'true'
        return ChartItem('Monthly[{}]'.format(col), option)

    def timeseries_box_item(self, cycle=None):
        """
        It will plot year-wise/month-wise box plot.

        Parameters
        ----------

        cycle : {"YEAR", "QUARTER", "MONTH", "WEEK"}, optional
            It defines the x-axis for the box plot.

        Returns
        -------
        item : ChartItem
            The item for the plot.
        """
        col = self.endog
        data_ = self.dataset.select([self.key, col]).generate_feature(targets=[self.key], trans_func=cycle)
        if cycle != "QUARTER":
            data_ = data_.cast({data_.columns[2]: "INT"})
        temp_tab_name = "#timeseries_box_plot_{}".format(str(uuid.uuid1()).replace('-', '_'))
        data_.save(temp_tab_name)
        data_ = data_.connection_context.table(temp_tab_name)
        temp_data_ = data_.collect().sort_values(data_.columns[2], ascending=True)

        X = []
        for xd in list(temp_data_.iloc[:, 2].astype(str)):
            xd = convert_value_to_target_type(xd)
            push_valid_value(X, xd)
        Y = []
        for yd in list(temp_data_.iloc[:, 1]):
            yd = convert_value_to_target_type(yd)
            push_valid_value(Y, yd)
        temp_dataset = {}
        for i in set(X):
            temp_dataset[i] = []
        for i in range(0, len(X)):
            temp_dataset[X[i]].append(Y[i])
        dataset = []
        sorted_x = list(set(X))
        sorted_x.sort(key=X.index)
        for i in sorted_x:
            dataset.append(temp_dataset[i])
        data_.connection_context.drop_table(temp_tab_name)

        option = {
            'customFn': ['xAxis.axisLabel.formatter', 'tooltip.formatter'],
            'dataset': [
                {
                    'source': dataset
                },
                {
                    'transform': {
                        'type': 'boxplot'
                    }
                },
                {
                    'fromDatasetIndex': 1,
                    'fromTransformResult': 1
                }
            ],
            'grid': {
                'show': 'true',
                'containLabel': 'true'
            },
            'legend': {},
            'xAxis': {
                'type': 'category',
                'axisTick': {
                    'alignWithLabel': 'true'
                },
                'axisLabel': {
                    'showMinLabel': 'true',
                    'showMaxLabel': 'true',
                    'hideOverlap': 'true',
                    'fontSize': 9,
                    'formatter': {
                        'params': ['value'],
                        'body': "".join(['return {}[value]'.format(str(sorted_x))])
                    }
                }
            },
            'yAxis': {
                'type': 'value',
                'axisLine': {
                    'show': 'true',
                },
                'axisTick': {
                    'show': 'true'
                },
                'axisLabel': {
                    'showMinLabel': 'true',
                    'showMaxLabel': 'true',
                    'hideOverlap': 'true',
                    'rotate': 15,
                    'fontSize': 9,
                    'interval': 0
                }
            },
            'tooltip': {
                'trigger': 'item',
                'axisPointer': {
                    'type': 'shadow'
                },
                'formatter': {
                    'params': ['params'],
                    'body': ''.join([
                        "var x={};".format(str(sorted_x)),
                        "return params['seriesName'] + '<br />' + params['marker'] + x[parseInt(params['data'][0])] + ' ---> ' + params['data'][1]"
                    ])
                }
            },
            'series': [
                {
                    'name': 'Boxplot',
                    'type': 'boxplot',
                    'datasetIndex': 1
                },
                {
                    'name': 'Outlier',
                    'type': 'scatter',
                    'datasetIndex': 2,
                    'color': '#C00000'
                }
            ]
        }
        if self.lazy_load:
            option['lazyLoad'] = 'true'
        return ChartItem('Box[{}-{}]'.format(col, cycle), option)

    def seasonal_decompose_items(self, alpha=None, thread_ratio=None, decompose_type=None, extrapolation=None, smooth_width=None):
        """
        It will to decompose a time series into three components: trend, seasonality and random noise, then to plot.

        Parameters
        ----------

        alpha : float, optional
            The criterion for the autocorrelation coefficient.
            The value range is (0, 1). A larger value indicates stricter requirement for seasonality.

            Defaults to 0.2.

        thread_ratio : float, optional
            Controls the proportion of available threads to use.
            The ratio of available threads.

                - 0: single thread.
                - 0~1: percentage.
                - Others: heuristically determined.

            Defaults to -1.

        decompose_type : {'additive', 'multiplicative', 'auto'}, optional
            Specifies decompose type.

              - 'additive': additive decomposition model
              - 'multiplicative': multiplicative decomposition model
              - 'auto': decomposition model automatically determined from input data

            Defaults to 'auto'.

        extrapolation : bool, optional
            Specifies whether to extrapolate the endpoints.
            Set to True when there is an end-point issue.

            Defaults to False.

        smooth_width : int, optional
            Specifies the width of the moving average applied to non-seasonal data.
            0 indicates linear fitting to extract trends.
            Can not be larger than half of the data length.

            Defaults to 0.

        Returns
        -------
        item : ChartItem
            The item for the plot.
        """
        from hana_ml.algorithms.pal.tsa import seasonal_decompose
        col = self.endog
        _, res = seasonal_decompose.seasonal_decompose(data=self.dataset, endog=col, key=self.key, alpha=alpha, thread_ratio=thread_ratio, decompose_type=decompose_type, extrapolation=extrapolation, smooth_width=smooth_width)
        res = res.collect()

        X = []
        for xd in list(res.iloc[:, 0].astype(str)):
            xd = convert_value_to_target_type(xd)
            push_valid_value(X, xd)
        X.insert(0, self.key)
        items = []
        for i in range(0, 3):
            datasets = []
            series = []
            Y = []
            for d in list(res.iloc[:, i + 1]):
                d = convert_value_to_target_type(d)
                push_valid_value(Y, d)
            Y.insert(0, res.columns[i + 1])
            datasets.append({
                'source': [X, Y]
            })
            series.append({
                'type': 'line',
                'seriesLayoutBy': 'row',
                'name': res.columns[i + 1],
                'emphasis': {
                    'focus': 'self'
                }
            })
            option = {
                'dataset': datasets,
                'grid': {
                    'show': 'true',
                    'containLabel': 'true'
                },
                'legend': {},
                'xAxis': {
                    'type': 'category',
                    'axisTick': {
                        'alignWithLabel': 'true'
                    },
                    'axisLabel': {
                        'showMinLabel': 'true',
                        'showMaxLabel': 'true',
                        'hideOverlap': 'true',
                        'rotate': 45,
                        'fontSize': 9,
                    }
                },
                'yAxis': {
                    'type': 'value',
                    'axisLine': {
                        'show': 'true',
                    },
                    'axisTick': {
                        'show': 'true'
                    },
                    'axisLabel': {
                        'showMinLabel': 'true',
                        'showMaxLabel': 'true',
                        'hideOverlap': 'true',
                        'rotate': 15,
                        'fontSize': 9,
                        'interval': 0
                    }
                },
                'tooltip': {
                    'trigger': 'axis'
                },
                'series': series
            }
            if self.lazy_load:
                option['lazyLoad'] = 'true'
            items.append(ChartItem("Seasonal Decompose[{}-{}]".format(col, res.columns[i + 1]), option))
        return items

    def quarter_item(self):
        """
        It performs quarter plot to view the seasonality.

        Returns
        -------
        item : ChartItem
            The item for the plot.
        """
        col = self.endog
        df = self.dataset.select([self.key, col]).generate_feature(targets=[self.key], trans_func="QUARTER")
        new_id = "NEWID_{}".format(str(uuid.uuid1()).replace("-", "_"))
        df = df.split_column(df.columns[2], '-', ["YEAR", "Q"]).add_id(new_id, ref_col=["Q", "YEAR", self.key])
        ordered_years = list(set(df.collect()['YEAR']))
        ordered_years.sort()

        datasets = []
        series = []
        mark_exist = False
        datasetIndex = -1
        my_pos = None
        for quarter in ["Q1", "Q2", "Q3", "Q4"]:
            my_pos = df.filter("Q='{}'".format(quarter)).select([new_id]).median()
            min_x = df.filter("Q='{}'".format(quarter)).select([new_id]).min()
            max_x = df.filter("Q='{}'".format(quarter)).select([new_id]).max()
            avg_q = df.filter("Q='{}'".format(quarter)).select([col]).mean()
            if my_pos is not None:
                my_pos = float(my_pos)
            if min_x is not None:
                min_x = float(min_x)
            if max_x is not None:
                max_x = float(max_x)
            if avg_q is not None:
                avg_q = float(avg_q)
            mark_exist = False

            for year in ordered_years:
                # ID Y
                xx_filter = df.filter("Q='{}' AND YEAR='{}'".format(quarter, year)).select([new_id, col])
                xx_filter_collect = xx_filter.collect()
                X = list(xx_filter_collect.iloc[:, 0])
                new_X = []
                if len(X) == 0:
                    continue
                for xd in X:
                    xd = convert_value_to_target_type(xd)
                    push_valid_value(new_X, xd)
                X = new_X
                X.insert(0, "{}-{}".format(year, quarter))
                Y = []
                for yd in list(xx_filter_collect.iloc[:, 1]):
                    yd = convert_value_to_target_type(yd)
                    push_valid_value(Y, yd)
                Y.insert(0, col)
                datasets.append({
                    'source': [X, Y]
                })
                datasetIndex = datasetIndex + 1

                sery = {
                    'datasetIndex': datasetIndex,
                    'type': 'line',
                    'seriesLayoutBy': 'row',
                    'name': "{}-{}".format(year, quarter),
                    'color': '#5698C6'
                }
                if mark_exist is False and my_pos:
                    # sery['markPoint'] = {
                    #     'data': [
                    #         {
                    #             'name': '',
                    #             'xAxis': my_pos - 1,
                    #             'yAxis': 0,
                    #             'value': quarter
                    #         }
                    #     ],
                    #     'itemStyle': {
                    #         'color': '#FFC957',
                    #         'opacity': 0.8
                    #     },
                    # }
                    sery['markLine'] = {
                        'symbol': ['none', 'none'],
                        'symbolSize': 4,
                        'data': [
                            [
                                {
                                    # 'name': quarter,
                                    'symbol': 'circle',
                                    'lineStyle': {
                                        'color': '#C00000'
                                    },
                                    'label': {
                                        'position': 'middle',
                                        'color': '#C00000'
                                    },
                                    'coord': [min_x - 1, avg_q]
                                },
                                {
                                    'symbol': 'circle',
                                    'coord': [max_x - 1, avg_q]
                                }
                            ],
                            {
                                'name': quarter,
                                'xAxis': my_pos - 1,
                                'label': {
                                    'position': 'start',
                                    'formatter': quarter
                                },
                                'lineStyle': {
                                    'color': '#B7BEC9'
                                }
                            }
                        ]
                    }
                    mark_exist = True
                series.append(sery)
        option = {
            'dataset': datasets,
            'grid': {
                'show': 'true',
                'containLabel': 'true'
            },
            'xAxis': {
                'type': 'category',
                'axisTick': {
                    'alignWithLabel': 'true'
                },
                'axisLabel': {
                    'showMinLabel': 'true',
                    'showMaxLabel': 'true',
                    'hideOverlap': 'true',
                    'rotate': 45,
                    'fontSize': 9,
                    'formatter': ''
                }
            },
            'yAxis': {
                'type': 'value',
                'axisLine': {
                    'show': 'true',
                },
                'axisTick': {
                    'show': 'true'
                },
                'axisLabel': {
                    'showMinLabel': 'true',
                    'showMaxLabel': 'true',
                    'hideOverlap': 'true',
                    'rotate': 15,
                    'fontSize': 9,
                    'interval': 0
                }
            },
            'tooltip': {
                'trigger': 'axis'
            },
            'series': series
        }
        if self.lazy_load:
            option['lazyLoad'] = 'true'
        return ChartItem('Quarter[{}]'.format(col), option)

    def outlier_item(self, window_size=None, detect_seasonality=None, alpha=None, periods=None, outlier_method=None, threshold=None, **kwargs):
        """
        Perform PAL time series outlier detection and plot time series with the highlighted outliers.

        Parameters
        ----------
        window_size : int, optional
            Odd number, the window size for median filter, not less than 3.

            Defaults to 3.

        outlier_method : str, optional

            The method for calculate the outlier score from residual.

              - 'z1' : Z1 score.
              - 'z2' : Z2 score.
              - 'iqr' : IQR score.
              - 'mad' : MAD score.

            Defaults to 'z1'.

        threshold : float, optional
            The threshold for outlier score. If the absolute value of outlier score is beyond the
            threshold, we consider the corresponding data point as an outlier.

            Defaults to 3.

        detect_seasonality : bool, optional
            When calculating the residual,

            - False: Does not consider the seasonal decomposition.
            - True: Considers the seasonal decomposition.

            Defaults to False.

        alpha : float, optional
            The criterion for the autocorrelation coefficient. The value range is (0, 1).
            A larger value indicates a stricter requirement for seasonality.

            Only valid when ``detect_seasonality`` is True.

            Defaults to 0.2.

        periods : int, optional
            When this parameter is not specified, the algorithm will search the seasonal period.
            When this parameter is specified between 2 and half of the series length, autocorrelation value
            is calculated for this number of periods and the result is compared to ``alpha`` parameter.
            If correlation value is equal to or higher than ``alpha``, decomposition is executed with the value of ``periods``.
            Otherwise, the residual is calculated without decomposition. For other value of parameter ``periods``,
            the residual is also calculated without decomposition.

            No Default value.

        thread_ratio : float, optional
            The ratio of available threads.

            - 0: single thread.
            - 0~1: percentage.
            - Others: heuristically determined.

            Only valid when ``detect_seasonality`` is True.

            Defaults to -1.

        Returns
        -------
        item : ChartItem
            The item for the plot.
        """
        from hana_ml.algorithms.pal.tsa.outlier_detection import OutlierDetectionTS
        odts = OutlierDetectionTS(window_size=window_size,
                                detect_seasonality=detect_seasonality,
                                alpha=alpha,
                                periods=periods,
                                outlier_method=outlier_method,
                                threshold=threshold, **kwargs)
        x_data = []
        result = odts.fit_predict(data=self.dataset, key=self.key, endog=self.endog)
        res_col = result.columns
        result = result.select([res_col[0], res_col[1], res_col[4]])
        result = result.collect()
        result.set_index(res_col[0])
        outliers = result.loc[result["IS_OUTLIER"] == 1, [res_col[0], "RAW_DATA"]]

        datasets = []
        series = []
        datasetIndex = -1

        datasetIndex = datasetIndex + 1
        # TIMESTAMP
        X = []
        for xd in list(result.iloc[:, 0].astype(str)):
            xd = convert_value_to_target_type(xd)
            push_valid_value(X, xd)
        x_data.extend(X)
        X.insert(0, 'TIMESTAMP')
        # RAW_DATA
        Y = []
        for yd in list(result.iloc[:, 1]):
            yd = convert_value_to_target_type(yd)
            push_valid_value(Y, yd)
        Y.insert(0, 'RAWDATA')
        datasets.append({
            'source': [X, Y]
        })
        series.append({
            'datasetIndex': datasetIndex,
            'type': 'line',
            'seriesLayoutBy': 'row',
            'name': '',
            'emphasis': {
                'focus': 'self'
            }
        })

        datasetIndex = datasetIndex + 1
        # TIMESTAMP
        X = []
        for xd in list(outliers.iloc[:, 0].astype(str)):
            xd = convert_value_to_target_type(xd)
            push_valid_value(X, xd)
        x_data.extend(X)
        X.insert(0, 'TIMESTAMP')
        # RAW_DATA
        Y = []
        for yd in list(outliers.iloc[:, 1]):
            yd = convert_value_to_target_type(yd)
            push_valid_value(Y, yd)
        Y.insert(0, 'RAWDATA')
        datasets.append({
            'source': [X, Y]
        })
        series.append({
            'datasetIndex': datasetIndex,
            'type': 'scatter',
            'seriesLayoutBy': 'row',
            'name': 'Outlier',
            'color': '#C00000',
            'emphasis': {
                'focus': 'self'
            }
        })

        x_data = list(set(x_data))
        def convert_data(x):
            try:
                return int(x)
            except ValueError:
                return x
        x_data.sort(key=convert_data)

        option = {
            'dataset': datasets,
            'grid': {
                'show': 'true',
                'containLabel': 'true'
            },
            'legend': {},
            'xAxis': {
                'name': 'TIMESTAMP',
                'type': 'category',
                'data': x_data,
                'axisTick': {
                    'alignWithLabel': 'true'
                },
                'axisLabel': {
                    'showMinLabel': 'true',
                    'showMaxLabel': 'true',
                    'hideOverlap': 'true',
                    'rotate': 45,
                    'fontSize': 9,
                }
            },
            'yAxis': {
                'name': 'RAWDATA',
                'type': 'value',
                'axisLine': {
                    'show': 'true',
                },
                'axisTick': {
                    'show': 'true'
                },
                'axisLabel': {
                    'showMinLabel': 'true',
                    'showMaxLabel': 'true',
                    'hideOverlap': 'true',
                    'rotate': 15,
                    'fontSize': 9,
                    'interval': 0
                }
            },
            'tooltip': {
                'trigger': 'axis'
            },
            'series': series
        }
        if self.lazy_load:
            option['lazyLoad'] = 'true'
        return ChartItem('Outlier', option)

    def stationarity_item(self, method=None, mode=None, lag=None, probability=None):
        """
        Stationarity means that a time series has a constant mean and constant variance over time.
        For many time series models, the input data has to be stationary for reasonable analysis.

        Parameters
        ----------
        method : str, optional
            Statistic test that used to determine stationarity. The options are "kpss" and "adf".

            Defaults "kpss".

        mode : str, optional
            Type of stationarity to determine. The options are "level", "trend" and "no".
            Note that option "no" is not applicable to "kpss".

            Defaults to "level".

        lag : int, optional
            The lag order to calculate the test statistic.

            Default value is "kpss": int(12*(data_length / 100)^0.25" ) and "adf": int(4*(data_length / 100)^(2/9)).

        probability : float, optional
            The confidence level for confirming stationarity.

            Defaults to 0.9.

        Returns
        -------
        item : TableItem
            The item for the statistical data.
        """
        from hana_ml.algorithms.pal.tsa import stationarity_test
        stats = stationarity_test.stationarity_test(self.dataset, key=self.key, endog=self.endog, method=method, mode=mode, lag=lag, probability=probability)
        columns = list(stats.columns)
        stats = stats.collect()

        table_item = TableItem('Stationarity')
        table_item.addColumn(columns[0], list(stats.iloc[:, 0].astype(str)))
        table_item.addColumn(columns[1], list(stats.iloc[:, 1].astype(str)))
        return table_item

    def real_item(self):
        """
        It will plot a chart based on the original data.

        Parameters
        ----------
        None

        Returns
        -------
        item : ChartItem
            The item for the plot.
        """
        data_ = self.dataset.collect().sort_values(self.key, ascending=True)

        X = []
        for xd in list(data_[self.key].astype(str)):
            xd = convert_value_to_target_type(xd)
            push_valid_value(X, xd)
        X.insert(0, self.key)
        Y = []
        for yd in list(data_[self.endog]):
            yd = convert_value_to_target_type(yd)
            push_valid_value(Y, yd)
        Y.insert(0, self.endog)

        option = {
            'dataset': {
                'source': [
                    X,
                    Y
                ]
            },
            'grid': {
                'show': 'true',
                'containLabel': 'true'
            },
            'legend': {},
            'xAxis': {
                'type': 'category',
                'axisTick': {
                    'alignWithLabel': 'true'
                },
                'axisLabel': {
                    'showMinLabel': 'true',
                    'showMaxLabel': 'true',
                    'hideOverlap': 'true',
                    'rotate': 45,
                    'fontSize': 9,
                }
            },
            'yAxis': {
                'type': 'value',
                'axisLine': {
                    'show': 'true',
                },
                'axisTick': {
                    'show': 'true'
                },
                'axisLabel': {
                    'showMinLabel': 'true',
                    'showMaxLabel': 'true',
                    'hideOverlap': 'true',
                    'rotate': 15,
                    'fontSize': 9,
                    'interval': 0
                }
            },
            'tooltip': {
                'trigger': 'axis'
            },
            'series': [
                {
                    'type': 'line',
                    'seriesLayoutBy': 'row',
                    'color': '#5698C6',
                    'name': 'Real[{}]'.format(self.endog)
                }
            ]
        }
        if self.lazy_load:
            option['lazyLoad'] = 'true'
        return ChartItem('Real[{}]'.format(self.endog), option)

    def change_points_item(self, cp_object, display_trend=True, cp_style="axvline", title=None):
        """
        Plot time series with the highlighted change points and BCPD is used for change point detection.

        Parameters
        ----------
        cp_object : BCPD object

            An object of BCPD for change points detection. Please initialize a BCPD object first.
            An example is shown below:

            .. raw:: html

                <iframe allowtransparency="true" style="border:1px solid #ccc; background: #eeffcb;"
                    src="_static/eda_example.html" width="100%" height="100%" sandbox="">
                </iframe>
        cp_style : {"axvline", "scatter"}, optional

            The style of change points in the plot.

            Defaults to "axvline".

        display_trend : bool, optional

            If True, draw the trend component based on decomposed component of trend of BCPD fit_predict().

            Default to True.

        title : str, optional

            The title of plot.

            Defaults to "Change Points".

        Returns
        -------
        item : ChartItem
            The item for the plot.
        """
        from hana_ml.algorithms.pal.tsa.changepoint import BCPD #pylint: disable=cyclic-import
        if isinstance(cp_object, BCPD):
            datasets = []
            series = []
            datasetIndex = -1
            axvlines = []
            x_data = []

            tcp, scp, period, components = cp_object.fit_predict(data=self.dataset, key=self.key, endog=self.endog)
            if tcp.shape[0] > 0:
                title = 'TREND_CP'
                if cp_style == "scatter":
                    result = tcp.set_index(title).join(self.dataset, how='left')
                    result = result.collect().sort_values(title, ascending=True)
                    X = []
                    for xd in list(result[title].astype(str)):
                        xd = convert_value_to_target_type(xd)
                        push_valid_value(X, xd)
                    x_data.extend(X)
                    X.insert(0, title)
                    Y = []
                    for yd in list(result[self.endog]):
                        yd = convert_value_to_target_type(yd)
                        push_valid_value(Y, yd)
                    Y.insert(0, self.endog)
                    datasetIndex = datasetIndex + 1
                    datasets.append({
                        'source': [X, Y]
                    })
                    series.append({
                        'datasetIndex': datasetIndex,
                        'type': 'scatter',
                        'color': 'red',
                        'seriesLayoutBy': 'row',
                        'name': 'Trend Change Points',
                        'emphasis': {
                            'focus': 'self'
                        }
                    })
                if cp_style == "axvline":
                    result = tcp
                    result = list(result.collect()[title].astype(str))
                    for i in range(len(result)):
                        d = convert_value_to_target_type(result[i])
                        x_data.append(d)
                        axvlines.append({
                            'xAxis': d,
                            'lineStyle': {
                                'color': 'red'
                            }
                        })

            if scp.shape[0] > 0:
                title = 'SEASON_CP'
                if cp_style == "scatter":
                    result = scp.set_index(title).join(self.dataset, how='left')
                    result = result.collect().sort_values(title, ascending=True)
                    X = []
                    for xd in list(result[title].astype(str)):
                        xd = convert_value_to_target_type(xd)
                        push_valid_value(X, xd)
                    x_data.extend(X)
                    X.insert(0, title)
                    Y = []
                    for yd in list(result[self.endog]):
                        yd = convert_value_to_target_type(yd)
                        push_valid_value(Y, yd)
                    Y.insert(0, self.endog)
                    datasetIndex = datasetIndex + 1
                    datasets.append({
                        'source': [X, Y]
                    })
                    series.append({
                        'datasetIndex': datasetIndex,
                        'type': 'scatter',
                        'color': 'green',
                        'seriesLayoutBy': 'row',
                        'name': 'Seasonal Change Points',
                        'emphasis': {
                            'focus': 'self'
                        }
                    })
                if cp_style == "axvline":
                    result = scp
                    result = list(result.collect()[title].astype(str))
                    for i in range(len(result)):
                        d = convert_value_to_target_type(result[i])
                        x_data.append(d)
                        axvlines.append({
                            'xAxis': d,
                            'lineStyle': {
                                'color': 'green'
                            }
                        })

            if display_trend is True:
                result = components
                result = result.collect().sort_values(self.key, ascending=True)
                X = []
                for xd in list(result[self.key].astype(str)):
                    xd = convert_value_to_target_type(xd)
                    push_valid_value(X, xd)
                x_data.extend(X)
                X.insert(0, self.key)
                Y = []
                for yd in list(result["TREND"]):
                    yd = convert_value_to_target_type(yd)
                    push_valid_value(Y, yd)
                Y.insert(0, 'TREND')
                datasetIndex = datasetIndex + 1
                datasets.append({
                    'source': [X, Y]
                })
                series.append({
                    'datasetIndex': datasetIndex,
                    'type': 'line',
                    'color': 'orange',
                    'seriesLayoutBy': 'row',
                    'name': 'Trend Component',
                    'emphasis': {
                        'focus': 'self'
                    }
                })

            result = self.dataset.select([self.key, self.endog])
            result = result.collect().sort_values(self.key, ascending=True)
            X = []
            for xd in list(result[self.key].astype(str)):
                xd = convert_value_to_target_type(xd)
                push_valid_value(X, xd)
            x_data.extend(X)
            X.insert(0, self.key)
            Y = []
            for yd in list(result[self.endog]):
                yd = convert_value_to_target_type(yd)
                push_valid_value(Y, yd)
            Y.insert(0, 'TREND')
            datasetIndex = datasetIndex + 1
            datasets.append({
                'source': [X, Y]
            })
            series.append({
                'datasetIndex': datasetIndex,
                'type': 'line',
                'color': 'blue',
                'seriesLayoutBy': 'row',
                'name': 'Original Time Series',
                'emphasis': {
                    'focus': 'self'
                },
                'markLine' : {
                    'symbol': ['none', 'none'],
                    'symbolSize': 4,
                    'label': {
                        'show': 'false'
                    },
                    'data': axvlines
                }
            })

            x_data = list(set(x_data))
            def convert_data(x):
                try:
                    return int(x)
                except ValueError:
                    return x
            x_data.sort(key=convert_data)

            if title is None:
                title = "Change Points"
            option = {
                'dataset': datasets,
                'grid': {
                    'show': 'true',
                    'containLabel': 'true'
                },
                'legend': {},
                'xAxis': {
                    'name': self.key,
                    'data': x_data,
                    'type': 'category',
                    'axisTick': {
                        'alignWithLabel': 'true'
                    },
                    'axisLabel': {
                        'showMinLabel': 'true',
                        'showMaxLabel': 'true',
                        'hideOverlap': 'true',
                        'rotate': 45,
                        'fontSize': 9,
                    }
                },
                'yAxis': {
                    'name': self.endog,
                    'type': 'value',
                    'axisLine': {
                        'show': 'true',
                    },
                    'axisTick': {
                        'show': 'true'
                    },
                    'axisLabel': {
                        'showMinLabel': 'true',
                        'showMaxLabel': 'true',
                        'hideOverlap': 'true',
                        'rotate': 15,
                        'fontSize': 9,
                        'interval': 0
                    }
                },
                'tooltip': {
                    'trigger': 'axis'
                },
                'series': series
            }
            if self.lazy_load:
                option['lazyLoad'] = 'true'
            return ChartItem(title, option)
