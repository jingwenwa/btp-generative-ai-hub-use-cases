"""
This module provides some explainers for Shapley values.

The following classes are available:

    * :class:`ShapleyExplainer`
    * :class:`TimeSeriesExplainer`
"""
# pylint: disable=no-else-break
# pylint: disable=pointless-string-statement
# pylint: disable=too-few-public-methods
# pylint: disable=too-many-lines
# pylint: disable=line-too-long
# pylint: disable=too-many-locals
# pylint: disable=too-many-statements
# pylint: disable=too-many-arguments
# pylint: disable=missing-docstring
# pylint: disable=consider-using-enumerate
# pylint: disable=too-many-instance-attributes
# pylint: disable=no-member
# pylint: disable=too-many-branches
# pylint: disable=invalid-name
# pylint: disable=unsubscriptable-object
# pylint: disable=too-many-function-args
# pylint: disable=no-self-use
# pylint: disable=broad-except
# pylint: disable=no-else-continue
# pylint: disable=consider-using-f-string
# pylint: disable=pointless-statement
# pylint: disable=unused-argument
# pylint: disable=too-many-nested-blocks
# pylint: disable=consider-iterating-dictionary

import logging
import json
import math
import random
import numpy as np
import pandas
from hana_ml import dataframe
from hana_ml.visualizers.ui_components import ChartBuilder, ChartConfig, unify_min_max_value_of_yAxis
from hana_ml.visualizers.report_builder import ReportBuilder, Page, ChartItem, ForcePlotItem
from hana_ml.visualizers.shared import EmbeddedUI

logger = logging.getLogger(__name__) #pylint: disable=invalid-name

def build_frame_html(frame_id, frame_src, frame_height):
    frame_html = """
        <iframe
            id="{iframe_id}"
            width="{width}"
            height="{height}"
            srcdoc="{src}"
            style="border:0px solid #ccc"
            allowfullscreen="true"
            webkitallowfullscreen="true"
            mozallowfullscreen="true"
            oallowfullscreen="true"
            msallowfullscreen="true"
        >
        </iframe>
    """.format(
        iframe_id=frame_id,
        width='95%',
        height=frame_height,
        src=frame_src
    )

    return frame_html


class FeatureValueAndEffect(object):
    def __init__(self, feature_value_df: dataframe.DataFrame, reason_code_df: dataframe.DataFrame):
        self.reason_code_pd_df = reason_code_df.collect()
        reason_code_list = list(self.reason_code_pd_df[reason_code_df.columns[0]])

        self.is_lstm_function = False
        for cell in reason_code_list:
            try:
                for current_dict in json.loads(cell):
                    if current_dict['attr'].find("T=") == 0:
                        self.is_lstm_function = True
                        break
            except (TypeError, ValueError):
                pass

        self.feature_value_pd_df = feature_value_df.collect()
        self.feature_values_dict = {}
        self.feature_min_max_values_dict = {}
        self.feature_effects_dict = {}
        self.feature_specific_type_dict = {}
        for specific_type in feature_value_df.dtypes():
            self.feature_specific_type_dict[specific_type[0]] = specific_type[1]
        self.feature_type_dict = {}
        self.existed_feature_values_dict = {}
        self.existed_feature_effects_dict = {}
        self.existed_feature_details = []
        if self.is_lstm_function:
            temp_feature_names = []
            for i in range(0, len(feature_value_df.columns)):
                feature_name = "T={}".format(str(i))
                temp_feature_names.append(feature_name)
                self.feature_values_dict[feature_name] = list(self.feature_value_pd_df[feature_value_df.columns[i]])
                self.feature_effects_dict[feature_name] = []
                self.existed_feature_values_dict[feature_name] = []
                self.existed_feature_effects_dict[feature_name] = []
            self.feature_name_list = temp_feature_names
        else:
            self.feature_name_list = feature_value_df.columns
            for feature_name in self.feature_name_list:
                self.feature_values_dict[feature_name] = list(self.feature_value_pd_df[feature_name])
                self.feature_effects_dict[feature_name] = []
                self.existed_feature_values_dict[feature_name] = []
                self.existed_feature_effects_dict[feature_name] = []

        for feature_name in self.feature_name_list:
            self.feature_type_dict[feature_name] = feature_value_df.is_numeric(feature_name)
        k = -1
        for cell in reason_code_list:
            k = k + 1
            temp_dict = {}
            try:
                for current_dict in json.loads(cell):
                    temp_dict[current_dict['attr']] = current_dict['val']
            except (TypeError, ValueError):
                pass
            for feature_name in self.feature_name_list:
                res = temp_dict.get(feature_name)
                if res is None:
                    self.feature_effects_dict[feature_name].append('undefined')
                else:
                    self.feature_effects_dict[feature_name].append(res)
                    temp_feature_value = self.feature_values_dict[feature_name][k]
                    should_add = True
                    if self.feature_type_dict[feature_name] is False: # is str
                        temp_feature_value = str(temp_feature_value)
                        self.feature_values_dict[feature_name][k] = temp_feature_value
                    else:
                        if np.isnan(temp_feature_value):
                            should_add = False
                            self.feature_values_dict[feature_name][k] = '-'
                    if should_add:
                        self.existed_feature_values_dict[feature_name].append(temp_feature_value)
                        self.existed_feature_effects_dict[feature_name].append(res)
        existed_feature_sums = []
        for feature_name in self.feature_name_list:
            existed_feature_sums.append(np.sum(np.abs(self.existed_feature_effects_dict[feature_name]), axis=0))
        for sum_order in np.argsort(existed_feature_sums):
            feature_name = self.feature_name_list[sum_order]
            temp_values = self.existed_feature_values_dict[feature_name]
            try:
                min_feature_value = np.min(temp_values)
                max_feature_value = np.max(temp_values)
            except (TypeError, ValueError):
                min_feature_value = 0
                max_feature_value = 0
            self.feature_min_max_values_dict[feature_name] = [min_feature_value, max_feature_value]
            self.existed_feature_details.append((feature_name, existed_feature_sums[sum_order], min_feature_value, max_feature_value, self.feature_type_dict[feature_name]))


class BeeswarmPlot(object):
    def __init__(self, feature_value_and_effect: FeatureValueAndEffect):
        # feature value must be number
        axvlines = []
        series = []
        visualMaps = []
        sery_index = -1
        k = -1
        if float(np.version.version.split('.', maxsplit=1)[0]) >= 2:
            np.set_printoptions(legacy="1.25")
        for existed_f_detail in feature_value_and_effect.existed_feature_details:
            current_f_name, current_sum_f_effect, current_min_f_value, current_max_f_value, is_numeric = existed_f_detail
            current_effects = feature_value_and_effect.existed_feature_effects_dict[current_f_name]
            if len(current_effects) == 0:
                continue
            current_values = feature_value_and_effect.existed_feature_values_dict[current_f_name]
            k = k + 1

            should_colored = True
            try:
                current_values = np.array(current_values, dtype=np.float64)
            except BaseException:
                should_colored = False
            if len(current_values) == 0 and len(current_effects) == 0:
                should_colored = False
            if is_numeric is False:
                should_colored = False

            new_current_effects = []
            new_current_values = []
            shuffled_indexs = np.arange(len(current_effects))
            np.random.shuffle(shuffled_indexs)
            for shuffled_index in shuffled_indexs:
                new_current_effects.append(current_effects[shuffled_index])
                new_current_values.append(current_values[shuffled_index])

            x = new_current_effects
            y = (k + 1) + BeeswarmPlot.get_y_bases(new_current_effects)
            z = new_current_values

            option_data = []
            sery_index = sery_index + 1
            if should_colored:
                min_value, max_value = BeeswarmPlot.use_min_max(new_current_values)
                visualMaps.append({
                    'min': min_value,
                    'max': max_value,
                    'dimension': 2,
                    'precision': 2,
                    'seriesIndex': sery_index,
                    'show': 'false',
                    'hoverLink': 'false',
                    'calculable': 'true',
                    'inRange': {
                        'color': ['#0085F9', '#FF0053']
                    }
                })
                for loc in range(0, len(new_current_effects)):
                    option_data.append([x[loc], y[loc], z[loc]])
                series.append({
                    'name': current_f_name,
                    'type': 'scatter',
                    'symbolSize': 5,
                    'data': option_data
                })
            else:
                if len(current_values) == 0 and len(current_effects) == 0:
                    pass
                else:
                    for loc in range(0, len(new_current_effects)):
                        option_data.append([x[loc], y[loc]])
                series.append({
                    'name': current_f_name,
                    'type': 'scatter',
                    'symbolSize': 5,
                    'itemStyle': {
                        'color': '#777',
                    },
                    'data': option_data
                })
            axvlines.append({
                'name': current_f_name,
                'yAxis': k + 1,
                'label': {
                    'formatter': current_f_name,
                    'opacity': 1,
                    'fontSize': 7,
                    'lineHeight': 7,
                },
                'lineStyle': {
                    'color': '#777',
                    'type': 'solid',
                    'width': 1,
                    'opacity': 0.1
                }
            })
        axvlines.append({
            'xAxis': 0,
            'label': {
                'formatter': ''
            },
            'lineStyle': {
                'color': '#777',
                'type': 'solid'
            }
        })
        sery_index = sery_index + 1
        visualMaps.append({
            'min': 0,
            'max': 1,
            'top': '10%',
            'dimension': 2,
            'precision': 2,
            'seriesIndex': sery_index,
            'orient': 'vertical',
            'left': 'left',
            'hoverLink': 'false',
            'textStyle': {
                # 'fontSize': 8,
                'color': '#FFF'
            },
            # 'text': ['High', 'Low'],
            'calculable': 'true',
            'inRange': {
                'color': ['#0085F9', '#FF0053']
            }
        })
        series.append({
            'name': '',
            'type': 'scatter',
            'symbolSize': 1,
            'data': [[0, 0]],
            'markLine' : {
                'symbol': ['none', 'none'],
                'symbolSize': 4,
                'label': {
                    'show': 'true',
                    'position':'end',
                    'width': 100,
                    'overflow': 'break'
                },
                'silent': 'true',
                'data': axvlines
            }
        })
        option = {
            'tips': [
                '1.Using Shapley values to show the distribution of the impacts each feature has on the model output.',
                '2.The color represents the feature value (red high, blue low).',
                '3.The plot below shows the relationship between feature value and Shapley value.',
                '-- If the dots in the left area are blue and the dots in the right area are red, then it means that the feature value and the Shapley value are typically positive correlation.',
                '-- If the dots in the left area are red and the dots in the right area are blue, then it means that the feature value and the Shapley value are typically negative correlation.',
                '-- If all the dots are concentrated near 0, it means that the Shapley value has nothing to do with this feature.'
            ],
            'title': {
                'text': 'Feature Effect and Value',
                'subtext': 'The color represents the feature value (red high, blue low).',
                'left': 'center',
            },
            'visualMap': visualMaps,
            # 'tooltip': {},
            'xAxis': {
                'name': 'Feature Effect(Impact on model output)',
                'nameLocation': 'center',
                'nameGap': 30,
                'type': 'value',
                'splitLine': {
                    'show': 'false'
                }
            },
            'yAxis': {
                'show': 'false',
                'type': 'value',
                'splitLine': {
                    'show': 'false'
                },
                'axisTick': {
                    'show': 'false'
                },
                'axisLine': {
                    'show': 'false'
                },
                'axisLabel': {
                    'show': 'false'
                }
            },
            'grid': {
                'containLabel': 'true',
            },
            'series': series
        }
        self.item = ChartItem('Beeswarm Plot', option, height=k * 40 + 100)

    @staticmethod
    def get_y_bases(values):
        min_value = np.min(values)
        max_value = np.max(values)
        quant = np.round(100 * (values - min_value) / (max_value - min_value + 1e-8))
        inds = np.argsort(quant + np.random.randn(len(values)) * 1e-6)
        layer = 0
        last_bin = -1
        y_bases = np.zeros(len(values))
        for ind in inds:
            if quant[ind] != last_bin:
                layer = 0
            y_bases[ind] = np.ceil(layer / 2) * ((layer % 2) * 2 - 1)
            layer += 1
            last_bin = quant[ind]
        y_bases *= 0.9 * (0.4 / np.max(y_bases + 1))
        return y_bases

    @staticmethod
    def use_min_max(values):
        min_value = np.nanpercentile(values, 5)
        max_value = np.nanpercentile(values, 100 - 5)
        if min_value == max_value:
            min_value = np.nanpercentile(values, 1)
            max_value = np.nanpercentile(values, 100 - 1)
            if min_value == max_value:
                min_value = np.min(values)
                max_value = np.max(values)
        # fixes rare numerical precision issues
        if min_value > max_value:
            min_value = max_value

        for i in range(0, len(values)):
            value = values[i]
            if value > max_value:
                values[i] = max_value
            elif value < min_value:
                values[i] = min_value
        return min_value, max_value


class BarPlot(object):
    def __init__(self, feature_value_and_effect: FeatureValueAndEffect):
        x = []
        y = []
        for existed_f_detail in feature_value_and_effect.existed_feature_details:
            current_f_name, current_sum_f_effect, current_min_f_value, current_max_f_value, is_numeric = existed_f_detail
            x.append(current_sum_f_effect)
            y.append(current_f_name)
        option = {
            'title': {
                'text': 'Feature Effect',
                'subtext': 'Sum of all absolute values',
                'left': 'center'
            },
            'xAxis': {
            },
            'yAxis': {
                'type': 'category',
                'data': y,
                'axisLabel': {
                    'fontSize': 9
                }
            },
            'grid': {
                'containLabel': 'true'
            },
            'tooltip': {
                'trigger': 'axis',
                'axisPointer': {
                    'type': 'shadow'
                }
            },
            'series': [
                {
                    'name': 'Feature Effect',
                    'type': 'bar',
                    'data': x,
                    'color': '#108ee9'
                }
            ],
        }
        self.item = ChartItem('Bar Plot', option, height=len(feature_value_and_effect.existed_feature_details) * 30)


class DependencePlot(object):
    def __init__(self, feature_value_and_effect: FeatureValueAndEffect):
        self.items = []
        for existed_f_detail in feature_value_and_effect.existed_feature_details:
            current_f_name, current_sum_f_effect, current_min_f_value, current_max_f_value, is_numeric = existed_f_detail
            xAxis_type = 'value'
            if is_numeric is False:
                xAxis_type = 'category'
            current_effects = feature_value_and_effect.existed_feature_effects_dict[current_f_name]
            if len(current_effects) == 0:
                continue
            current_values = feature_value_and_effect.existed_feature_values_dict[current_f_name]
            current_min_f_effect = min(current_effects)
            current_max_f_effect = max(current_effects)
            X = current_values.copy()
            X.insert(0, 'Value')
            Y = current_effects.copy()
            Y.insert(0, 'Effect')
            current_option = {
                'title': {},
                'dataset': {
                    'source': [
                        X,
                        Y
                    ]
                },
                'grid': {
                    'containLabel': 'true'
                },
                'xAxis': {
                    'type': xAxis_type,
                    'name': 'Feature Value ({})'.format(current_f_name),
                    'nameLocation': 'center',
                    'nameGap': 30,
                    'axisTick': {
                        'alignWithLabel': 'true'
                    },
                    'splitLine': {
                        'show': 'false'
                    },
                    'axisLabel': {
                        'showMinLabel': 'true',
                        'showMaxLabel': 'true',
                        'hideOverlap': 'true',
                        # 'rotate': 45,
                        'fontSize': 9,
                    },
                    'axisLine': {
                        'show': 'true',
                        'onZero': 'false'
                    }
                },
                'yAxis': {
                    'type': 'value',
                    'name': 'Feature Effect ({})'.format(current_f_name),
                    'axisLine': {
                        'show': 'true',
                        'onZero': 'false'
                    },
                    'axisTick': {
                        'show': 'true'
                    },
                    'splitLine': {
                        'show': 'false'
                    },
                    'axisLabel': {
                        'showMinLabel': 'true',
                        'showMaxLabel': 'true',
                        'hideOverlap': 'true',
                        # 'rotate': 15,
                        'fontSize': 9,
                        'interval': 0
                    },
                },
                'tooltip': {
                    'trigger': 'item',
                    'axisPointer': {
                        'type': 'cross'
                    }
                },
                'series': [
                    {
                        'type': 'scatter',
                        'seriesLayoutBy': 'row',
                        'color': '#108ee9',
                        'symbolSize': 5,
                    }
                ]
            }
            if is_numeric is True:
                current_option['xAxis']['min'] = int(current_min_f_value) - 1
                current_option['xAxis']['max'] = int(current_max_f_value) + 1
                current_option['yAxis']['min'] = int(current_min_f_effect) - 1
                current_option['yAxis']['max'] = int(current_max_f_effect) + 1
                if feature_value_and_effect.feature_specific_type_dict[current_f_name] == 'INT':
                    pass
                else:
                    current_option['xAxis']['axisLabel'] = {
                        'formatter': {
                            'params': ['val'],
                            'body': 'return parseFloat(val.toString().substring(0, 6));'
                        }
                    }
                    current_option['customFn'] = ['xAxis.axisLabel.formatter']
                current_option['dataZoom'] = [{
                    'type': 'slider',
                    'show': 'true',
                    'xAxisIndex': [0]
                }, {
                    'type': 'inside',
                    'xAxisIndex': [0]
                }]
            self.items.append(ChartItem('{}'.format(current_f_name), current_option))


class EnhancedDependencePlot(object):
    def __init__(self, feature_value_and_effect: FeatureValueAndEffect):
        self.items = []
        for feature_name in feature_value_and_effect.feature_name_list:
            original_effects = feature_value_and_effect.existed_feature_effects_dict[feature_name]
            if len(original_effects) == 0:
                continue

            xAxis_type = 'value'
            xAxis_is_numeric = feature_value_and_effect.feature_type_dict[feature_name]
            if xAxis_is_numeric is False:
                xAxis_type = 'category'

            X = feature_value_and_effect.feature_values_dict[feature_name]
            Y = feature_value_and_effect.feature_effects_dict[feature_name]
            for other_feature_name in feature_value_and_effect.feature_name_list:
                if feature_name == other_feature_name:
                    pass
                else:
                    zAxis_is_numeric = feature_value_and_effect.feature_type_dict[other_feature_name]
                    sery_data = []
                    sery_data_dict = {}
                    Z = feature_value_and_effect.feature_values_dict[other_feature_name]
                    z_list = []
                    for loc in range(0, len(X)):
                        if Y[loc] == 'undefined':
                            pass
                        else:
                            sery_data.append([X[loc], Y[loc], Z[loc]])
                            z_list.append(Z[loc])
                            if zAxis_is_numeric:
                                pass
                            else:
                                z_dict = sery_data_dict.get(Z[loc])
                                if z_dict is None:
                                    sery_data_dict[Z[loc]] = [[X[loc], Y[loc]]]
                                else:
                                    z_dict.append([X[loc], Y[loc]])
                    visualMap = None
                    series = []
                    if zAxis_is_numeric:
                        min_z = 0
                        max_z = 0
                        try:
                            min_z = np.min(z_list)
                            max_z = np.max(z_list)
                        except (TypeError, ValueError):
                            pass
                        visualMap = {
                            'type': 'continuous',
                            'min': min_z,
                            'max': max_z,
                            'dimension': 2,
                            'precision': 2,
                            'orient': 'horizontal',
                            'left': 'center',
                            'top': 0,
                            'calculable': 'true',
                            'itemWidth': 10,
                            'hoverLink': 'true',
                            'text': ['High Feature Value ({})'.format(other_feature_name), 'Low Feature Value ({})'.format(other_feature_name)],
                            'inRange': {
                                'color': ['#0085F9', '#FF0053']
                            }
                        }
                        series.append({
                            'type': 'scatter',
                            'seriesLayoutBy': 'row',
                            'symbolSize': 5,
                            'data': sery_data
                        })
                    else:
                        def get_random_hex_colors(num):
                            colors = []
                            hex_digits = '0123456789ABCDEF'
                            while True:
                                if len(colors) == num:
                                    break
                                else:
                                    hex_color = '#' + ''.join([random.choice(hex_digits) for _ in range(6)])
                                    if hex_color in colors:
                                        pass
                                    else:
                                        colors.append(hex_color)
                            return colors
                        categories = list(set(z_list))
                        colors = get_random_hex_colors(len(categories))
                        i = -1
                        for z_name in sery_data_dict:
                            i = i + 1
                            series.append({
                                'name': str(z_name),
                                'color': colors[i],
                                'type': 'scatter',
                                'seriesLayoutBy': 'row',
                                'symbolSize': 5,
                                'data': sery_data_dict[z_name]
                            })
                    current_option = {
                        'lazyLoad': 'true',
                        'title': {},
                        'grid': {
                            'containLabel': 'true'
                        },
                        'xAxis': {
                            'type': xAxis_type,
                            'name': 'Feature Value ({})'.format(feature_name),
                            'nameLocation': 'center',
                            'nameGap': 30,
                            'axisTick': {
                                'alignWithLabel': 'true'
                            },
                            'splitLine': {
                                'show': 'false'
                            },
                            'axisLabel': {
                                'showMinLabel': 'true',
                                'showMaxLabel': 'true',
                                'hideOverlap': 'true',
                                # 'rotate': 45,
                                'fontSize': 9,
                            },
                            'axisLine': {
                                'show': 'true',
                                'onZero': 'false'
                            }
                        },
                        'yAxis': {
                            'type': 'value',
                            'name': 'Feature Effect ({})'.format(feature_name),
                            'axisLine': {
                                'show': 'true',
                                'onZero': 'false'
                            },
                            'axisTick': {
                                'show': 'true'
                            },
                            'splitLine': {
                                'show': 'false'
                            },
                            'axisLabel': {
                                'showMinLabel': 'true',
                                'showMaxLabel': 'true',
                                'hideOverlap': 'true',
                                # 'rotate': 15,
                                'fontSize': 9,
                                'interval': 0
                            },
                        },
                        'tooltip': {
                            'trigger': 'item',
                            'axisPointer': {
                                'type': 'cross'
                            }
                        },
                        'series': series
                    }
                    if visualMap:
                        current_option['visualMap'] = [visualMap]
                    if zAxis_is_numeric is False:
                        current_option['legend'] = {
                            'type': 'scroll'
                        }
                    if xAxis_is_numeric is True:
                        current_effects = feature_value_and_effect.existed_feature_effects_dict[feature_name]
                        min_f_value, max_f_value = feature_value_and_effect.feature_min_max_values_dict[feature_name]
                        current_option['xAxis']['min'] = int(min_f_value) - 1
                        current_option['xAxis']['max'] = int(max_f_value) + 1
                        current_option['yAxis']['min'] = int(min(current_effects)) - 1
                        current_option['yAxis']['max'] = int(max(current_effects)) + 1
                        if feature_value_and_effect.feature_specific_type_dict[feature_name] == 'INT':
                            pass
                        else:
                            current_option['xAxis']['axisLabel'] = {
                                'formatter': {
                                    'params': ['val'],
                                    'body': 'return parseFloat(val.toString().substring(0, 6));'
                                }
                            }
                            current_option['customFn'] = ['xAxis.axisLabel.formatter']
                        current_option['dataZoom'] = [{
                            'type': 'slider',
                            'show': 'true',
                            'xAxisIndex': [0]
                        }, {
                            'type': 'inside',
                            'xAxisIndex': [0]
                        }]
                    self.items.append(ChartItem('{} ({})'.format(feature_name, other_feature_name), current_option))


class ForcePlot(object):
    def __init__(self, feature_value_and_effect: FeatureValueAndEffect):
        self.feature_value_and_effect = feature_value_and_effect
        self.force_plot_json = {
            'title': 'Force Plot',
            'featureNames': self.feature_value_and_effect.feature_name_list,
            'featureBaseEffect': 0.0,
            'featureValues': self.feature_value_and_effect.feature_values_dict,
            'featureEffects': self.feature_value_and_effect.feature_effects_dict
        }
        self.html_str = EmbeddedUI.get_resource_template('shap_force_plot.html').render(frame_id=EmbeddedUI.get_uuid(), force_plot_json=self.force_plot_json)

    def generate_notebook_iframe(self, iframe_height):
        iframe_id = EmbeddedUI.get_uuid()
        if EmbeddedUI.get_runtime_platform()[1] == 'jupyter':
            self.html_str = EmbeddedUI.get_resource_template('shap_force_plot.html').render(frame_id=iframe_id, force_plot_json=self.force_plot_json)
        iframe_str = EmbeddedUI.get_iframe_str(self.html_str, iframe_id, iframe_height)
        EmbeddedUI.render_html_str(iframe_str)

    def generate_html(self, filename: str):
        """
        Saves the force plot as a html file.

        Parameters
        ----------
        filename : str
            Html file name.
        """
        EmbeddedUI.generate_file("{}.html".format(filename), self.html_str)


class ShapleyExplainer(object):
    """
    SHAP (SHapley Additive exPlanations) is a game theoretic approach to explain the output of machine learning model.  \n
    It connects optimal credit allocation with local explanations using the classic Shapley values from game theory and their related extensions. \n
    To get an overview of which features are most important for a model we can plot the Shapley values of every feature for every sample. \n
    If the output table contains the reason code column, the output table can be parsed by this class in most cases, rather than only valid for the tree model.

    Parameters
    ----------
    reason_code_data : DataFrame
        The Dataframe containing reason code values.

    feature_data : DataFrame
        The Dataframe containing feature values.

    reason_code_column_name : str, optional
        The name of reason code column in ``reason_code_data``.

        Defaults to the last column of ``reason_code_data``.

    key : str, optional
        The index column of ``feature_data``.

        If provided, ``feature_data`` will be sorted by ``key``; besides,
        if ``key`` also appears in the columns of ``reason_code_data``,
        ``reason_code_data`` will also be sorted by ``key``.

        Defaults to None, implying that ``feature_data`` and ``reason_code_data``
        have already been sorted so that they correspond by row numbers.

        .. note::

           It is encouraged that the users pre-sort the feature values and reason code
           so that the two inputs simply corresond by row numbers. If this is not the case,
           they should include a common index column(i.e. ``key``) conveying the
           correspondence between input feature values and SHAP values.

    label : str, optional
        If the label exists in ``feature_data``, it should be provided.

        Defaults to None.

    Examples
    --------

    In the following example, training data is called diabetes_train and test data is diabetes_test.

    First, we create an UnifiedClassification instance:

    >>> uc_hgbdt = UnifiedClassification('HybridGradientBoostingTree')

    Then, create a GridSearchCV instance:

    >>> gscv = GridSearchCV(estimator=uc_hgbdt,
    ...                     param_grid={'learning_rate': [0.1, 0.4, 0.7, 1],
    ...                                 'n_estimators': [4, 6, 8, 10],
    ...                                 'split_threshold': [0.1, 0.4, 0.7, 1]},
    ...                     train_control=dict(fold_num=5,
    ...                                        resampling_method='cv',
    ...                                        random_state=1,
    ...                                        ref_metric=['auc']),
    ...                     scoring='error_rate')

    Call the fit() function to train the model:

    >>> gscv.fit(data=diabetes_train, key= 'ID',
    ...          label='CLASS',
    ...          partition_method='stratified',
    ...          partition_random_state=1,
    ...          stratified_column='CLASS',
    ...          build_report=True)
    >>> features = diabetes_train.columns
    >>> features.remove('CLASS')
    >>> features.remove('ID')

    Use diabetes_test for prediction:

    >>> pred_res = gscv.predict(diabetes_test, key='ID', features=features)

    Create a ShapleyExplainer class and then invoke summary_plot() :

    >>> shapley_explainer = ShapleyExplainer(reason_code_data=pred_res.sort('ID').select('REASON_CODE'),
    ...                                      feature_data=diabetes_test.sort('ID').select(features))
    >>> shapley_explainer.summary_plot()

    You can obtain the SAHP summary report by clicking on the corresponding tag and switching to the Beeswarm Plot / Bar Plot / Dependence Plot / Enhanced Dependence Plot page:

    .. image:: image/shap_summary_report.PNG

    .. image:: image/shap_bar_plot.png

    .. image:: image/shap_denpendence_plot.png

    .. image:: image/shap_enhance_dependence_plot.png

    Obtain the force plot:

    >>> shapley_explainer.force_plot()

    .. image:: image/force_plot.png

    """

    def __init__(self, reason_code_data: dataframe.DataFrame, feature_data: dataframe.DataFrame, reason_code_column_name=None, key=None, label=None):
        if reason_code_data is not None:
            if isinstance(reason_code_data, dataframe.DataFrame):
                if reason_code_data.empty():
                    raise ValueError('The reason code table is empty!')
            else:
                raise TypeError('The type of reason code table should be hana_ml.dataframe.DataFrame!')
        else:
            raise ValueError('The reason code table is None!')

        if feature_data is not None:
            if isinstance(feature_data, dataframe.DataFrame):
                if feature_data.empty():
                    raise ValueError('The feature table is empty!')
            else:
                raise TypeError('The type of feature table should be hana_ml.dataframe.DataFrame!')
        else:
            raise ValueError('The feature table is None!')

        if feature_data.count() != reason_code_data.count():
            raise ValueError("The number of rows in reason code table and feature table is inconsistent!")

        if key is not None:
            feature_data = feature_data.sort_values(key)
            feature_data = feature_data.deselect(key)
            if key in reason_code_data.columns:
                reason_code_data = reason_code_data.sort_values(key).deselect(key)
        if label is not None:
            feature_data = feature_data.deselect(label)

        if reason_code_column_name is None:
            reason_code_column_name = reason_code_data.columns[-1]
        if len(reason_code_data.columns) != 1:
            reason_code_data = reason_code_data.select(reason_code_column_name)

        self.__feature_value_and_effect = FeatureValueAndEffect(feature_data, reason_code_data)
        self.__summary_plotter = None
        self.__force_plotter = None

        self.__beeswarm_plot_item = None
        self.__bar_plot_item = None
        self.__dependence_plot_items = None
        self.__enhanced_dependence_plot_items = None
        self.__force_plot_item = None

    def get_feature_value_and_effect(self):
        r"""
        Get feature value and effect.

        Parameters
        ----------
        None

        Returns
        -------
        An object of class 'FeatureValueAndEffect'.
        """
        return self.__feature_value_and_effect

    def get_force_plot_item(self):
        r"""
        Get the force plot item.

        Parameters
        ----------
        None

        Returns
        -------
        An object of class 'ForcePlotItem'.
        """
        if self.__force_plot_item is None:
            if self.__force_plotter is None:
                self.__force_plotter = ForcePlot(self.__feature_value_and_effect)
            self.__force_plot_item = ForcePlotItem('Force Plot', {
                'featureNames': self.__force_plotter.force_plot_json['featureNames'],
                'featureBaseEffect': 0.0,
                'featureValues': self.__force_plotter.force_plot_json['featureValues'],
                'featureEffects': self.__force_plotter.force_plot_json['featureEffects']
            })
        return self.__force_plot_item

    def get_beeswarm_plot_item(self):
        r"""
        Get beeswarm plot item.

        Parameters
        ----------
        None

        Returns
        -------
        An object of class 'BeeswarmPlot'.
        """
        if self.__beeswarm_plot_item is None:
            self.__beeswarm_plot_item = BeeswarmPlot(self.__feature_value_and_effect).item
        return self.__beeswarm_plot_item

    def get_bar_plot_item(self):
        r"""
        Get bar plot item.

        Parameters
        ----------
        None

        Returns
        -------
        An object of class 'BarPlot'.
        """
        if self.__bar_plot_item is None:
            self.__bar_plot_item = BarPlot(self.__feature_value_and_effect).item
        return self.__bar_plot_item

    def get_dependence_plot_items(self):
        r"""
        Get dependence plot item.

        Parameters
        ----------
        None

        Returns
        -------
        An object of class 'DependencePlot'.
        """
        if self.__dependence_plot_items is None:
            self.__dependence_plot_items = DependencePlot(self.__feature_value_and_effect).items
        return self.__dependence_plot_items

    def get_enhanced_dependence_plot_items(self):
        r"""
        Get enhanced dependence plot item.

        Parameters
        ----------
        None

        Returns
        -------
        An object of class 'EnhancedDependencePlot'.
        """
        if self.__enhanced_dependence_plot_items is None:
            self.__enhanced_dependence_plot_items = EnhancedDependencePlot(self.__feature_value_and_effect).items
        return self.__enhanced_dependence_plot_items

    def force_plot(self, iframe_height=800):
        """
        Draw the force plot.

        Parameters
        ----------
        iframe_height : int, optional
            iframe height.

            Defaults to 800.

        Returns
        -------
        Renders the force plot as a notebook iframe.
        """
        if self.__force_plotter is None:
            self.__force_plotter = ForcePlot(self.__feature_value_and_effect)
        self.__force_plotter.generate_notebook_iframe(iframe_height)

    def summary_plot(self, iframe_height=600):
        """
        Global Interpretation using Shapley values. \n
        To get an overview of which features are most important for a model we can plot the Shapley values of every feature for every sample.

        Parameters
        ----------
        iframe_height : int, optional
            iframe height.

            Defaults to 600.

        Returns
        -------
        Renders the summary plot as a notebook iframe.
        """
        if self.__summary_plotter is None:
            self.__summary_plotter = ReportBuilder(title='SHAP (SHapley Additive exPlanations) Summary Report')

            try:
                page1 = Page(title='Beeswarm Plot')
                page1.addItem(self.get_beeswarm_plot_item())
                self.__summary_plotter.addPage(page1)
            except Exception as err:
                logger.error(err)
                pass

            try:
                page2 = Page(title='Bar Plot')
                page2.addItem(self.get_bar_plot_item())
                self.__summary_plotter.addPage(page2)
            except Exception as err:
                logger.error(err)
                pass

            try:
                page3 = Page(title='Dependence Plot')
                page3.addItems(self.get_dependence_plot_items())
                self.__summary_plotter.addPage(page3)
            except Exception as err:
                logger.error(err)
                pass

            try:
                page4 = Page(title='Enhanced Dependence Plot')
                page4.addItems(self.get_enhanced_dependence_plot_items())
                self.__summary_plotter.addPage(page4)
            except Exception as err:
                logger.error(err)
                pass

            self.__summary_plotter.build()

        self.__summary_plotter.generate_notebook_iframe(iframe_height)


class TimeSeriesExplainer(object):
    """
    The TimeSeriesExplainer instance can visualize the training and prediction results of time series.

    The generated html can be embedded in a notebook, including:

      - Compare

          - YHAT
          - YHAT_LOWER
          - YHAT_UPPER
          - REAL_Y
      - Trend
      - Seasonal
      - Holiday
      - Exogenous variable
    """
    def __init__(self):
        pass

    @staticmethod
    def explain_arima_model(arima, iframe_height=800):
        """
        The static method can visualize the training and prediction results of ARIMA.

        The generated html can be embedded in a notebook, including:

          - Compare

              - PREDICTIVE_Y
              - REAL_Y
          - Trend
          - Seasonal
          - Holiday
          - Exogenous variable

        Parameters
        ----------
        arima : ARIMA instance
            An ARIMA instance.
        iframe_height : int, optional
            Specifies iframe height.

            Defaults to 800.
        """
        xAxis_type = 'category'
        if arima.explainer_ is None:
            raise ValueError('The explainer attribute of ARIMA is None!')
        # key_name = arima.hanaml_fit_params['key']
        # label_name = arima.hanaml_fit_params['endog']
        # feature_names = arima.hanaml_fit_params['exog']
        key_name = arima.key
        label_name = arima.endog
        feature_names = arima.exog
        feature_count = len(feature_names)
        explainer_pd_df = arima.explainer_.collect()
        try:
            explainer_pd_df[key_name]
        except BaseException:
            # key_name = '{}(INT)'.format(key_name)
            key_name = explainer_pd_df.columns[0]
        # if xAxis_type == 'time':
        #     if explainer_pd_df[key_name].dtypes != 'datetime64[ns]':
        #         raise TypeError('The data type of the {} column is not a date type!'.format(key_name))
        key_values = explainer_pd_df[key_name].astype(str)

        explainer_na_pd_df = explainer_pd_df.isna().any()
        explainer_column_names = list(explainer_pd_df.columns)
        predict_data_pd_df = arima.predict_data.collect()
        predict_result_pd_df = arima.predict_data.connection_context.sql("SELECT * FROM {}".format(arima.get_predict_output_table_names()[0])).collect()

        # 1.
        decomposed_column_names = []
        decomposed_dict = {
            key_name: key_values
        }
        picked_row = explainer_pd_df.head(1)
        shap_name = None
        feature_shap_dict = {
            key_name: key_values
        }
        feature_shap_pd_df = None
        for column_name in explainer_column_names:
            if key_name == column_name:
                continue
            elif explainer_na_pd_df[column_name]:
                pass
            else:
                try:
                    json.loads(list(picked_row[column_name])[0])
                    shap_name = column_name
                    shap_dict = {}
                    for feature_name in feature_names:
                        shap_dict[feature_name] = []
                    for json_item in list(explainer_pd_df[shap_name]):
                        existed_feature_names = []
                        for feature_dict in json.loads(json_item):
                            existed_feature_names.append(feature_dict['attr'])
                            shap_dict[feature_dict['attr']].append(feature_dict['val'])
                        for feature_name in feature_names:
                            if feature_name not in existed_feature_names:
                                shap_dict[feature_name].append(0.0)
                    for feature_name in feature_names:
                        feature_shap_dict['{}_FEATURE'.format(feature_name)] = predict_data_pd_df[feature_name]
                        feature_shap_dict['{}_SHAP'.format(feature_name)] = shap_dict[feature_name]
                    feature_shap_pd_df = pandas.DataFrame(feature_shap_dict)
                except BaseException:
                    decomposed_column_names.append(column_name)
                    decomposed_dict[column_name] = explainer_pd_df[column_name]
        decomposed_pd_df = pandas.DataFrame(decomposed_dict)

        # 2.3.
        chart_configs = []
        compare_pd_df = None
        chart_name = 'Compare'
        try:
            compare_pd_df = pandas.DataFrame({
                key_name: key_values,
                'REAL_{}'.format(label_name): predict_data_pd_df[label_name],
                'PREDICTIVE_{}'.format(label_name): predict_result_pd_df['FORECAST'],
            })
        except BaseException:
            chart_name = label_name
            compare_pd_df = pandas.DataFrame({
                key_name: key_values,
                'PREDICTIVE_{}'.format(label_name): predict_result_pd_df['FORECAST'],
            })
        chart_config = ChartConfig(compare_pd_df, chart_name, '', key_name, '')
        chart_config.config['xAxis']['type'] = xAxis_type
        chart_config.ignore_yAxis_min_max = True
        chart_configs.append(chart_config)
        chart_config.add_to_series('PREDICTIVE_{}'.format(label_name), 'line', key_name, 'PREDICTIVE_{}'.format(label_name))
        try:
            predict_data_pd_df[label_name]
            chart_config.add_to_series('REAL_{}'.format(label_name), 'line', key_name, 'REAL_{}'.format(label_name))
        except BaseException:
            pass

        # 4.
        decomposed_chart_configs = []
        for decomposed_column_name in decomposed_column_names:
            chart_config = ChartConfig(decomposed_pd_df, decomposed_column_name, '', key_name, '')
            chart_config.config['xAxis']['type'] = xAxis_type
            chart_config.add_to_series(decomposed_column_name, 'line', key_name, decomposed_column_name)
            decomposed_chart_configs.append(chart_config)
            chart_configs.append(chart_config)
        unify_min_max_value_of_yAxis(decomposed_chart_configs)

        # 5.
        x_label = key_name
        y_label = 'SHAP'
        z_label = 'FEATURE'

        chart_config = ChartConfig(feature_shap_pd_df, shap_name, '', x_label, y_label)
        chart_config.config['xAxis']['type'] = xAxis_type
        chart_config.config['xAxis']['boundaryGap'] = 'true'
        chart_config.ignore_yAxis_min_max = True
        chart_configs.append(chart_config)
        for feature_name in feature_names:
            chart_config.add_to_series(feature_name, 'bar', x_label, '{}_{}'.format(feature_name, y_label))
        for series_item in chart_config.config['series']:
            series_item['stack'] = y_label
            series_item['emphasis'] = {
                'focus': 'series'
            }
        chart_config.yAxis_min_max_value_magnification_factor = 2

        chart_config = ChartConfig(feature_shap_pd_df, shap_name, '', x_label, y_label)
        chart_config.config['xAxis']['type'] = xAxis_type
        if xAxis_type == 'category':
            chart_config.config['xAxis']['boundaryGap'] = 'true'
        chart_config.ignore_yAxis_min_max = True
        chart_configs.append(chart_config)
        for feature_name in feature_names:
            chart_config.add_to_series(feature_name, 'scatter', x_label, '{}_{}'.format(feature_name, y_label))
        chart_config.config['visualMap'] = []
        column_names = list(feature_shap_pd_df.columns)
        for feature_name in feature_names:
            column_name = '{}_{}'.format(feature_name, z_label)
            chart_config.config['visualMap'].append({
                'show': 'false',
                'dimension': column_names.index(column_name),
                'min': feature_shap_pd_df[column_name].min(),
                'max': feature_shap_pd_df[column_name].max(),
                'seriesIndex': [feature_names.index(feature_name)],
                'inRange': {
                    'symbolSize': [5, 20]
                }
            })
        chart_config.yAxis_min_max_value_magnification_factor = 2
        # 6.
        chart_bulder = ChartBuilder(len(chart_configs), 1)
        index = 0
        for chart_config in chart_configs:
            chart_bulder.add_chart(chart_config, (index, 0))
            index = index + 1
        chart_bulder.build()
        chart_bulder.generate_notebook_iframe(iframe_height)

    @staticmethod
    def explain_additive_model(amf, iframe_height=800):
        """
        The static method can visualize the training and prediction results of AdditiveModelForecast.

        The generated html can be embedded in a notebook, including:

          - Compare

              - YHAT
              - YHAT_LOWER
              - YHAT_UPPER
              - REAL_Y
          - Trend
          - Seasonal
          - Holiday
          - Exogenous variable

        Parameters
        ----------
        amf : additive_model_forecast.AdditiveModelForecast
            AdditiveModelForecast instances.
        iframe_height : int, optional
            Specifies iframe height.

            Defaults to 800.
        """
        xAxis_type = 'category'
        if amf.explainer_ is None:
            raise ValueError('The explainer attribute of AdditiveModelForecast is None!')
        key_name = amf.hanaml_fit_params['key']
        feature_names = amf.hanaml_fit_params['exog']
        feature_count = len(feature_names)
        label_name = amf.hanaml_fit_params['endog']

        explainer_pd_df = amf.explainer_.collect()
        if explainer_pd_df[key_name].dtypes != 'datetime64[ns]':
            raise TypeError('The data type of the {} column is not a date type!'.format(key_name))
        key_values = explainer_pd_df[key_name].astype(str)
        predict_data_pd_df = amf.predict_data.collect()
        predict_result_pd_df = amf.predict_data.connection_context.sql("SELECT * FROM {}".format(amf.get_predict_output_table_names()[0])).collect()
        # 1.compare
        u = list(predict_result_pd_df['YHAT_UPPER'])
        l = list(predict_result_pd_df['YHAT_LOWER'])
        confidence_base = l[0]
        for l_value in l:
            confidence_base = math.floor(min(confidence_base, l_value))
        confidence_base = -confidence_base

        compare_dict = None
        chart_name = 'Compare'
        try:
            compare_dict = {
                key_name: key_values,
                'REAL_{}'.format(label_name): list(np.array(list(predict_data_pd_df[label_name])) + confidence_base),
                'YHAT_UPPER': list(np.array(u) - np.array(l)),
                'YHAT_LOWER': list(np.array(l) + confidence_base),
                'YHAT': list(np.array(list(predict_result_pd_df['YHAT'])) + confidence_base)
            }
        except BaseException:
            chart_name = label_name
            compare_dict = {
                key_name: key_values,
                'YHAT_UPPER': list(np.array(u) - np.array(l)),
                'YHAT_LOWER': list(np.array(l) + confidence_base),
                'YHAT': list(np.array(list(predict_result_pd_df['YHAT'])) + confidence_base)
            }
        compare_pd_df = pandas.DataFrame(compare_dict)
        # 2.trend
        trend_pd_df = pandas.DataFrame({
            key_name: key_values,
            'TREND': explainer_pd_df['TREND']
        })
        # 3.seasonal
        seasonal_dict = {
            key_name: key_values
        }
        for json_item in list(explainer_pd_df['SEASONAL']):
            dict_item = json.loads(json_item)
            for key in dict_item:
                if seasonal_dict.get(key) is None:
                    seasonal_dict[key] = []
                seasonal_dict[key].append(dict_item[key])
        seasonal_pd_df = pandas.DataFrame(seasonal_dict)

        # 4.holiday
        holiday_dict = {
            key_name: key_values
        }
        for json_item in list(explainer_pd_df['HOLIDAY']):
            dict_item = json.loads(json_item)
            for key in dict_item:
                if holiday_dict.get(key) is None:
                    holiday_dict[key] = []
                holiday_dict[key].append(dict_item[key])
        holiday_pd_df = pandas.DataFrame(holiday_dict)

        # 5.shap
        shap_name = 'EXOGENOUS'
        shap_dict = {}
        for feature_name in feature_names:
            shap_dict[feature_name] = []
        for json_item in list(explainer_pd_df[shap_name]):
            dict_item = json.loads(json_item)
            for key in dict_item:
                shap_dict[key].append(dict_item[key])
        feature_shap_dict = {
            key_name: key_values
        }
        for feature_name in feature_names:
            feature_shap_dict['{}_FEATURE'.format(feature_name)] = predict_data_pd_df[feature_name]
            feature_shap_dict['{}_SHAP'.format(feature_name)] = shap_dict[feature_name]
        feature_shap_pd_df = pandas.DataFrame(feature_shap_dict)

        chart_configs = []

        # 6.
        chart_config = ChartConfig(compare_pd_df, chart_name, '', key_name, '')
        chart_config.config['xAxis']['type'] = xAxis_type
        chart_config.ignore_yAxis_min_max = True
        chart_configs.append(chart_config)

        for item_name in ['YHAT_LOWER', 'YHAT_UPPER']:
            chart_config.add_to_series('', 'line', key_name, item_name)
        for item_name in ['YHAT']:
            chart_config.add_to_series(item_name, 'line', key_name, item_name)
        try:
            predict_data_pd_df[label_name]
            for item_name in ['REAL_{}'.format(label_name)]:
                chart_config.add_to_series(item_name, 'line', key_name, item_name)
        except BaseException:
            pass

        chart_config.config['yAxis']['axisLabel'] = {
            'formatter': 'function(val){return (val- ' + str(confidence_base) + ')}'
        }

        formatter_str_list = [
            "function (params) {",
            # "var x=params[2]['data'][params[2]['encode']['x'][0]];"
            "var x = params[2]['axisValueLabel'];",
            "var label0 ='YHAT_LOWER';",
            "var label1 ='YHAT_UPPER';",
            "var label2 =params[2]['seriesName'];",
            "var label3 =params[3]['seriesName'];",
            "var value0=params[0]['data'][params[0]['encode']['y'][0]]", "- ", str(confidence_base), ";",
            "var value1=params[1]['data'][params[1]['encode']['y'][0]]", "+value0", ";",
            "var value2=params[2]['data'][params[2]['encode']['y'][0]]", "- ", str(confidence_base), ";",
            "var value3=params[3]['data'][params[3]['encode']['y'][0]]", "- ", str(confidence_base), ";",
            "return x + ",
            "'<br />'+label0+': '+value0 + ",
            "'<br />'+label1+': '+value1 + ",
            "'<br />'+label2+': '+value2 + ",
            "'<br />'+label3+': '+value3",
            "}"
            ]
        chart_config.config['tooltip'] = {
            'trigger': 'axis',
            'formatter': "".join(formatter_str_list)
            }
        chart_config.config['series'][0]['lineStyle'] = {
            'opacity': 0
        }
        chart_config.config['series'][0]['stack'] = 'confidence'
        chart_config.config['series'][0]['symbol'] = 'none'
        chart_config.config['series'][1]['lineStyle'] = {
            'opacity': 0
        }
        chart_config.config['series'][1]['stack'] = 'confidence'
        chart_config.config['series'][1]['symbol'] = 'none'
        chart_config.config['series'][1]['areaStyle'] = {
            'color': '#ccc'
        }

        common_yAxis_chart_configs = []
        # 7.
        chart_config = ChartConfig(trend_pd_df, 'TREND', '', key_name, '')
        chart_config.config['xAxis']['type'] = xAxis_type
        chart_configs.append(chart_config)
        common_yAxis_chart_configs.append(chart_config)
        chart_config.add_to_series('TREND', 'line', key_name, 'TREND')

        # 8.
        chart_config = ChartConfig(seasonal_pd_df, 'SEASONAL', '', key_name, '')
        chart_config.config['xAxis']['type'] = xAxis_type
        chart_configs.append(chart_config)
        common_yAxis_chart_configs.append(chart_config)

        series_names = list(seasonal_dict.keys())
        series_names.remove(key_name)
        for item_name in series_names:
            chart_config.add_to_series(item_name, 'line', key_name, item_name)
        # 9.
        unify_min_max_value_of_yAxis(common_yAxis_chart_configs)
        # 10.
        chart_config = ChartConfig(holiday_pd_df, 'HOLIDAY', '', key_name, '')
        chart_config.config['xAxis']['type'] = xAxis_type
        chart_config.config['xAxis']['boundaryGap'] = 'true'
        chart_config.ignore_yAxis_min_max = True
        chart_configs.append(chart_config)
        holiday_names = list(holiday_pd_df.columns)
        holiday_names.remove(key_name)
        for holiday_name in holiday_names:
            chart_config.add_to_series(holiday_name, 'bar', key_name, holiday_name)
        for series_item in chart_config.config['series']:
            series_item['stack'] = 'holiday'
            series_item['emphasis'] = {
                'focus': 'series'
            }
        chart_config.yAxis_min_max_value_magnification_factor = 2
        # 11.
        query_sql = '''
        SELECT *
        FROM JSON_TABLE({}.MODEL_CONTENT, '$'
        COLUMNS
            (
                REGRESSOR_NAME VARCHAR(5000) FORMAT JSON PATH '$.regressor_name[*][*]',
                REGRESSOR_MODE VARCHAR(5000) FORMAT JSON PATH '$.regressor_mode[*][*]'
            )
        ) AS JT;'''.replace('\n', '').format(amf.model_.__dict__['source_table']['TABLE_NAME'])
        result = amf.predict_data.connection_context.sql(query_sql).collect()
        # 0: additive,1: multiplicative
        REGRESSOR_NAME = list(result['REGRESSOR_NAME'])[0].split(',')
        REGRESSOR_MODE = list(result['REGRESSOR_MODE'])[0].split(',')
        additive_mode_names = []
        multiplicative_mode_names = []
        for i in range(0, len(REGRESSOR_NAME)):
            REGRESSOR_NAME[i] = REGRESSOR_NAME[i].replace('"', '')
            REGRESSOR_MODE[i] = REGRESSOR_MODE[i].replace('"', '')
            if REGRESSOR_MODE[i] == '0':
                additive_mode_names.append(REGRESSOR_NAME[i])
            else:
                multiplicative_mode_names.append(REGRESSOR_NAME[i])
        x_label = key_name
        y_label = 'SHAP'
        z_label = 'FEATURE'
        # additive
        if len(additive_mode_names) > 0:
            chart_config = ChartConfig(feature_shap_pd_df, shap_name, 'Additive Mode', x_label, y_label)
            chart_config.config['xAxis']['type'] = xAxis_type
            chart_config.config['xAxis']['boundaryGap'] = 'true'
            chart_config.ignore_yAxis_min_max = True
            chart_configs.append(chart_config)
            for feature_name in feature_names:
                if feature_name in additive_mode_names:
                    chart_config.add_to_series(feature_name, 'bar', x_label, '{}_{}'.format(feature_name, y_label))
            for series_item in chart_config.config['series']:
                series_item['stack'] = y_label
                series_item['emphasis'] = {
                    'focus': 'series'
                }
            chart_config.yAxis_min_max_value_magnification_factor = 2
        # multiplicative
        if len(multiplicative_mode_names) > 0:
            chart_config = ChartConfig(feature_shap_pd_df, shap_name, 'Multiplicative Mode', x_label, y_label)
            chart_config.config['xAxis']['type'] = xAxis_type
            chart_config.config['xAxis']['boundaryGap'] = 'true'
            chart_config.ignore_yAxis_min_max = True
            chart_configs.append(chart_config)
            for feature_name in feature_names:
                if feature_name in multiplicative_mode_names:
                    chart_config.add_to_series(feature_name, 'bar', x_label, '{}_{}'.format(feature_name, y_label))
            for series_item in chart_config.config['series']:
                series_item['stack'] = y_label
                series_item['emphasis'] = {
                    'focus': 'series'
                }
            chart_config.yAxis_min_max_value_magnification_factor = 2
        # additive
        if len(additive_mode_names) > 0:
            chart_config = ChartConfig(feature_shap_pd_df, shap_name, 'Additive Mode', x_label, y_label)
            chart_config.config['xAxis']['type'] = xAxis_type
            if xAxis_type == 'category':
                chart_config.config['xAxis']['boundaryGap'] = 'true'
            chart_config.ignore_yAxis_min_max = True
            chart_configs.append(chart_config)
            for feature_name in feature_names:
                if feature_name in additive_mode_names:
                    chart_config.add_to_series(feature_name, 'scatter', x_label, '{}_{}'.format(feature_name, y_label))
            chart_config.config['visualMap'] = []
            column_names = list(feature_shap_pd_df.columns)
            for feature_name in feature_names:
                column_name = '{}_{}'.format(feature_name, z_label)
                chart_config.config['visualMap'].append({
                    'show': 'false',
                    'dimension': column_names.index(column_name),
                    'min': feature_shap_pd_df[column_name].min(),
                    'max': feature_shap_pd_df[column_name].max(),
                    'seriesIndex': [feature_names.index(feature_name)],
                    'inRange': {
                        'symbolSize': [5, 20]
                    }
                })
            chart_config.yAxis_min_max_value_magnification_factor = 2

        # multiplicative
        if len(multiplicative_mode_names) > 0:
            chart_config = ChartConfig(feature_shap_pd_df, shap_name, 'Multiplicative Mode', x_label, y_label)
            chart_config.config['xAxis']['type'] = xAxis_type
            if xAxis_type == 'category':
                chart_config.config['xAxis']['boundaryGap'] = 'true'
            chart_config.ignore_yAxis_min_max = True
            chart_configs.append(chart_config)
            for feature_name in feature_names:
                if feature_name in multiplicative_mode_names:
                    chart_config.add_to_series(feature_name, 'scatter', x_label, '{}_{}'.format(feature_name, y_label))
            chart_config.config['visualMap'] = []
            column_names = list(feature_shap_pd_df.columns)
            for feature_name in feature_names:
                column_name = '{}_{}'.format(feature_name, z_label)
                chart_config.config['visualMap'].append({
                    'show': 'false',
                    'dimension': column_names.index(column_name),
                    'min': feature_shap_pd_df[column_name].min(),
                    'max': feature_shap_pd_df[column_name].max(),
                    'seriesIndex': [feature_names.index(feature_name)],
                    'inRange': {
                        'symbolSize': [5, 20]
                    }
                })
            chart_config.yAxis_min_max_value_magnification_factor = 2
        # 12.
        chart_bulder = ChartBuilder(len(chart_configs), 1)
        index = 0
        for chart_config in chart_configs:
            chart_bulder.add_chart(chart_config, (index, 0))
            index = index + 1
        chart_bulder.build()
        chart_bulder.generate_notebook_iframe(iframe_height)
