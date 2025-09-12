"""
This module contains report builders for model. Only for internal use, do not show them in the doc.
"""

# pylint: disable=too-many-lines
# pylint: disable=line-too-long
# pylint: disable=too-many-locals
# pylint: disable=too-many-arguments
# pylint: disable=missing-docstring
# pylint: disable=consider-using-enumerate
# pylint: disable=too-many-instance-attributes
# pylint: disable=no-member
# pylint: disable=too-many-branches
# pylint: disable=invalid-name
# pylint: disable=protected-access
# pylint: disable=broad-except
# pylint: disable=consider-using-f-string
# pylint: disable=too-few-public-methods
# pylint: disable=duplicate-string-formatting-argument
# pylint: disable=too-many-nested-blocks
import logging
import html
from threading import Lock
import math
import numpy as np
import pandas
from hana_ml import dataframe
from hana_ml.algorithms.pal.preprocessing import Sampling
from hana_ml.visualizers.report_builder import ChartItem, ReportBuilder, TableItem, Page
from hana_ml.visualizers.shap import ShapleyExplainer
from hana_ml.visualizers.shared import EmbeddedUI


logger = logging.getLogger(__name__)


class TemplateUtil(object):
    __SECTION_METADATA = {
        'container': '<div class="section">{}</div>',
        'name': '<h3 class="text-left section_name">{}</h3>',
        'content': '<div class="section_content">{}</div>',
        'content_style': '<div class="section_content" style="text-align:center">{}</div>'
    }

    __TAB_METADATA = {
        'id': 1,
        'lock': Lock(),
        # {nav_id} {nav_items}
        'nav': '<ul id="{}" class="nav nav-tabs" role="tablist">{}</ul>',
        # {nav_item_id} {nav_item_title}
        'nav_active_item': '<li class="nav-item"><a class="nav-link active" href="#{}" role="tab" data-toggle="tab">{}</a></li>',
        'nav_item': '<li class="nav-item"><a class="nav-link" href="#{}" role="tab" data-toggle="tab">{}</a></li>',
        # {pane_id} {pane_items}
        'pane': '<div id="{}" class="tab-content">{}</div>',
        # {pane_item_id} {pane_item_content}
        'pane_active_item': '<div class="tab-pane fade show active" id="{}">{}</div>',
        'pane_item': '<div class="tab-pane fade" id="{}">{}</div>'
    }

    __TABLE_METADATA = {
        'container': '<table class="table table-bordered table-hover">{}</table>',
        'head_container': '<thead>{}</thead>',
        'body_container': '<tbody>{}</tbody>',
        'row_container': '<tr>{}</tr>',
        'head_column': '<th>{}</th>',
        'body_column': '<td>{}</td>'
    }

    __ECHART_METADATA = {
        'id': 1,
        'id_prefix': 'echarts',
        'container': '<div id="{}" style="height:500px;margin-top:10px"></div>',
        'lock': Lock()
    }

    @staticmethod
    def generate_echart(chart_id):
        return TemplateUtil.__ECHART_METADATA['container'].format(chart_id)

    @staticmethod
    def construct_tab_item_data(title, content):
        return {
            'title': title,
            'content': content
        }

    @staticmethod
    def get_echart_id():
        lock = TemplateUtil.__ECHART_METADATA['lock']
        lock.acquire()

        echart_id = TemplateUtil.__ECHART_METADATA['id']
        TemplateUtil.__ECHART_METADATA['id'] = echart_id + 1

        lock.release()

        return '{}_chart_{}'.format(
            TemplateUtil.__ECHART_METADATA['id_prefix'], echart_id)

    @staticmethod
    def get_tab_id():
        lock = TemplateUtil.__TAB_METADATA['lock']
        lock.acquire()

        tab_id = TemplateUtil.__TAB_METADATA['id']
        TemplateUtil.__TAB_METADATA['id'] = tab_id + 1

        lock.release()

        return tab_id

    @staticmethod
    def generate_tab(data):
        # data = [{'title': '','content': ''},{...}]
        element_id = TemplateUtil.get_tab_id()
        nav_id = 'nav_{}'.format(element_id)
        pane_id = 'pane_{}'.format(element_id)
        nav_html = ''
        pane_html = ''
        for i in range(0, len(data)):
            pane = data[i]
            pane_item_id = '{}_{}'.format(pane_id, i)
            if i == 0:
                nav_html = nav_html + \
                    TemplateUtil.__TAB_METADATA['nav_active_item'].format(pane_item_id, pane['title'])
                pane_html = pane_html + \
                    TemplateUtil.__TAB_METADATA['pane_active_item'].format(pane_item_id, pane['content'])
            else:
                nav_html = nav_html + \
                    TemplateUtil.__TAB_METADATA['nav_item'].format(pane_item_id, pane['title'])
                pane_html = pane_html + \
                    TemplateUtil.__TAB_METADATA['pane_item'].format(pane_item_id, pane['content'])

        nav_html = TemplateUtil.__TAB_METADATA['nav'].format(nav_id, nav_html)
        pane_html = TemplateUtil.__TAB_METADATA['pane'].format(pane_id, pane_html)

        tab_html = nav_html + pane_html

        return tab_html, nav_id

    @staticmethod
    def generate_table_html(data: dataframe.DataFrame, column_names=None, table_name=None):
        column_data = []

        for column in data.columns:
            column_data.append(list(data.collect()[column]))

        formatted_data = []
        for i in range(0, data.count()):
            row_data = []
            for j in range(0, len(data.columns)):
                origin_data = column_data[j][i]
                if isinstance(origin_data, str):
                    row_data.append(html.escape(origin_data))
                else:
                    row_data.append(origin_data)
            formatted_data.append(row_data)

        if column_names:
            return TemplateUtil.generate_table(column_names, formatted_data, table_name)
        else:
            return TemplateUtil.generate_table(data.columns, formatted_data, table_name)

    @staticmethod
    def generate_table(columns, data, table_name=None):
        columns_html = ''
        for column in columns:
            columns_html += TemplateUtil.__TABLE_METADATA['head_column'].format(column)
        row_html = TemplateUtil.__TABLE_METADATA['row_container'].format(columns_html)
        head_html = TemplateUtil.__TABLE_METADATA['head_container'].format(row_html)
        if table_name:
            head_html = "<title>{}</title>".format(table_name) + head_html
        rows_html = ''
        for row_data in data:
            columns_html = ''
            for column_data in row_data:
                columns_html += TemplateUtil.__TABLE_METADATA['body_column'].format(column_data)
            rows_html += TemplateUtil.__TABLE_METADATA['row_container'].format(columns_html)
        body_html = TemplateUtil.__TABLE_METADATA['body_container'].format(rows_html)

        return TemplateUtil.__TABLE_METADATA['container'].format(head_html + body_html)


class EchartsUtil(object):
    __LINE_TOOLTIP = {
        'trigger': 'axis'
    }

    __PIE_TOOLTIP = {
        'trigger': 'item',
        'formatter': '{a} <br/>{b} : {c} ({d}%)'
    }

    __BAR_TOOLTIP = {
        'trigger': 'axis',
        'axisPointer': {
            'type': 'shadow'
        }
    }

    __LINE_GRID = {
        'left': '5%',
        'right': '2%',
        'containLabel': 'true',
        'show': 'true'
    }

    __BAR_GRID = {
        'left': '3%',
        'right': '4%',
        'bottom': '3%',
        'containLabel': 'true'
    }

    __BAR_XAXIS = {
        'type': 'value',
        'boundaryGap': [0, 0.01]
    }

    __LINE_TOOLBOX = {
        'feature': {
            'saveAsImage': {'title': 'Save as Image'}
        }
    }

    __COLORS = ['dodgerblue', 'forestgreen', 'firebrick']

    __BAR_COLOR = __COLORS[0]

    @staticmethod
    def generate_line_default_option():
        title = {
            'text': '',
            'top': 0,
            'left': 'center'
        }

        legend = {
            'orient': 'vertical',
            'left': '70%',
            'top': -4,
            'data': []
        }

        x_axis = {
            'type': 'value',
            # 'boundaryGap': 'false',
            'name': '',
            'nameLocation': 'middle',
            'nameTextStyle': {
                'color': 'black',
                'fontSize': 16,
                'padding': 10
            }
        }

        y_axis = {
            'type': 'value',
            'name': '',
            'nameLocation': 'middle',
            'nameTextStyle': {
                'color': 'black',
                'fontSize': 16,
                'padding': 30
            }
        }

        y_data = {
            'name': '',
            'type': 'line',
            'data': [],
            'color': ''
        }

        series = []

        return title, legend, x_axis, y_axis, y_data, series

    @staticmethod
    def generate_pie_default_option():
        legend = {
            'type': 'scroll',
            'orient': 'horizontal',
            'left': 'left',
            'data': []
        }

        series = [
            {
                'name': '',
                'type': 'pie',
                # 'radius': '55%',
                # 'center': ['50%', '60%'],
                'data': [],
                'emphasis': {
                    'itemStyle': {
                        'shadowBlur': 10,
                        'shadowOffsetX': 0,
                        'shadowColor': 'rgba(0, 0, 0, 0.5)'
                    }
                }
            }
        ]

        return legend, series

    @staticmethod
    def generate_bar_default_option():
        y_axis = {
            'type': 'category',
            'data': None
        }

        series = [
            {
                'type': 'bar',
                'data': None
            }
        ]

        return y_axis, series

    @staticmethod
    def generate_load_line_js(chart_id, chart_text, chart_data):
        title, legend, x_axis, y_axis, y_data, series = EchartsUtil.generate_line_default_option()

        title['text'] = chart_text['label']
        x_axis['name'] = chart_text['xlabel']
        y_axis['name'] = chart_text['ylabel']

        legend['data'] = []

        for data in chart_data:
            y_data_copy = y_data.copy()

            if (chart_text['title'] == 'ROC') and (
                    data['label'] == 'Random model'):
                y_data_copy['smooth'] = 'false'
                y_data_copy['itemStyle'] = {
                    'normal': {
                        'lineStyle': {
                            'type': 'dotted'
                        }
                    }
                }

            legend['data'].append(data['label'])
            y_data_copy['name'] = data['label']
            y_data_copy['color'] = EchartsUtil.__COLORS[data['color_index']]

            temp = []
            for index in range(0, len(data['x'])):
                temp.append([data['x'][index], data['y'][index]])
            y_data_copy['data'] = temp

            series.append(y_data_copy)

        option = {}
        option['title'] = title
        option['tooltip'] = EchartsUtil.__LINE_TOOLTIP
        option['legend'] = legend
        option['grid'] = EchartsUtil.__LINE_GRID
        option['toolbox'] = EchartsUtil.__LINE_TOOLBOX
        option['xAxis'] = x_axis
        option['yAxis'] = y_axis
        option['series'] = series

        js_str = '''
            var {id} = echarts.init(document.getElementById('{id}'));\n
            var {id}_option = {option};\n
            {id}.setOption({id}_option);\n
        '''.format(id=chart_id, option=option)

        return js_str.replace("'true'", "true").replace("'false'", "false"), option

    @staticmethod
    def generate_load_bar_js(chart_id, data_names, data_values):
        y_axis, series = EchartsUtil.generate_bar_default_option()
        y_axis['data'] = data_names
        series[0]['data'] = data_values

        option = {}
        option['tooltip'] = EchartsUtil.__BAR_TOOLTIP
        option['grid'] = EchartsUtil.__BAR_GRID
        option['xAxis'] = EchartsUtil.__BAR_XAXIS
        option['yAxis'] = y_axis
        option['series'] = series
        option['color'] = EchartsUtil.__BAR_COLOR

        js_str = '''
            var {id} = echarts.init(document.getElementById('{id}'));\n
            var {id}_option = {option};\n
            {id}.setOption({id}_option);\n
        '''.format(id=chart_id, option=option)

        return js_str.replace("'true'", "true").replace("'false'", "false")

    @staticmethod
    def generate_load_heatmap_js(chart_id, title, name_list, label_list, z_max, x_y_z_list, y_inverse=False):
        option = {
            'xAxis': {
                'type': 'category',
                'data': name_list,
                'name': label_list[0],
                'nameLocation': 'center',
                'splitArea': {
                    'show': 'true'
                }
            },
            'yAxis': {
                'type': 'category',
                'data': name_list,
                'name': label_list[1],
                'nameLocation': 'center',
                'splitArea': {
                    'show': 'true'
                }
            },
            'visualMap': [{
                'min': 0,
                'max': z_max,
                # 'left': 'right',
                # 'top': 'center',
                'orient': 'horizontal',
                'left': 'center',
                'bottom': '-%',
                'calculable': 'true',
                'realtime': 'false',
                'splitNumber': 4,
                'inRange': {
                    'color': [
                        '#e0f3f8',
                        '#abd9e9',
                        '#74add1',
                        '#4575b4',
                    ]
                }
            }],
            'series': [
                {
                    'name': title,
                    'type': 'heatmap',
                    'data': x_y_z_list,
                    'label': {
                        'show': 'true',
                        'color': 'black'
                    },
                    'emphasis': {
                        'itemStyle': {
                            'shadowBlur': 10,
                            'shadowColor': 'rgba(0, 0, 0, 0.5)'
                        }
                    }
                }
            ]
        }

        if y_inverse:
            option['yAxis']['inverse'] = 'true'

        js_str = '''
              var {id} = echarts.init(document.getElementById('{id}'));\n
              var {id}_option = {option};\n
              {id}.setOption({id}_option);\n
          '''.format(id=chart_id, option=option)

        return js_str.replace("'true'", "true").replace("'false'", "false")

    @staticmethod
    def generate_load_pie_js(chart_id, data_names, data_values):
        legend, series = EchartsUtil.generate_pie_default_option()

        for index in range(0, len(data_names)):
            series[0]['data'].append({
                'value': data_values[index],
                'name': data_names[index]
            })

        legend['data'] = data_names

        option = {}
        option['tooltip'] = EchartsUtil.__PIE_TOOLTIP
        option['legend'] = legend
        option['series'] = series

        js_str = '''
              var {id} = echarts.init(document.getElementById('{id}'));\n
              var {id}_option = {option};\n
              {id}.setOption({id}_option);\n
          '''.format(id=chart_id, option=option)

        return js_str.replace("'true'", "true").replace("'false'", "false")


class StatisticReportBuilder(object):
    def __init__(self):
        self.__statistic_table: pandas.DataFrame = None
        self.__statistic_table_columns = ['STAT_NAME', 'STAT_VALUE', 'CLASS_NAME']

        self.__column_data_map = None
        self.__table1_title = 'Scoring Table'
        self.__table2_title = 'Stats Table'
        self.__table1_data = None
        self.__table2_data = None
        self.__table1_columns = ['CLASS', 'PRECISION', 'RECALL', 'F1_SCORE', 'SUPPORT']
        self.__table2_columns = ['STAT NAME', 'STAT VALUE', 'CLASS']
        self.__table1_column_data_map = None
        self.__table2_column_data_map = None

        self.__generated_html = None
        self.__generated_items = None

    def get_generated_html(self):
        return self.__generated_html

    def get_generated_items(self):
        return self.__generated_items

    def set_statistic_table(self, target_df):
        self.__statistic_table = None
        self.__column_data_map = None
        self.__table1_data = None
        self.__table2_data = None
        self.__table1_column_data_map = None
        self.__table2_column_data_map = None
        self.__generated_html = None
        self.__generated_items = None
        if target_df is not None:
            if isinstance(target_df, dataframe.DataFrame):
                target_df = target_df.collect()
            if target_df.empty is False:
                self.__statistic_table = target_df

    def __build_tables(self):
        self.__column_data_map = {}
        self.__table1_data = []
        self.__table2_data = []
        self.__table1_column_data_map = {}
        self.__table2_column_data_map = {}

        for i in range(0, len(self.__statistic_table.columns)):
            self.__column_data_map[self.__statistic_table.columns[i]] = list(self.__statistic_table.iloc[:, i].astype(str))
        names = self.__column_data_map[self.__statistic_table_columns[0]]
        values = self.__column_data_map[self.__statistic_table_columns[1]]
        class_names = self.__column_data_map[self.__statistic_table_columns[2]]

        for name in self.__table1_columns:
            self.__table1_column_data_map[name] = []
        for name in self.__table2_columns:
            self.__table2_column_data_map[name] = []

        clazz_clazz_map = {}
        name_value_map = {}
        for i in range(0, len(names)):
            name = names[i]
            value = values[i]
            class_name = class_names[i]
            if name in (self.__table1_columns[1], self.__table1_columns[2], self.__table1_columns[3], self.__table1_columns[4]):
                clazz_clazz_map[class_name] = class_name
                name_value_map[name + class_name] = value
            else:
                self.__table2_data.append([name, value, class_name])

                self.__table2_column_data_map[self.__table2_columns[0]].append(name)
                self.__table2_column_data_map[self.__table2_columns[1]].append(value)
                self.__table2_column_data_map[self.__table2_columns[2]].append(class_name)

        clazz_clazz_map = list(clazz_clazz_map.keys())
        for i in range(0, len(clazz_clazz_map)):
            data = []
            class_name = clazz_clazz_map[i]
            data.append(class_name)
            data.append(name_value_map.get(self.__table1_columns[1] + class_name))
            data.append(name_value_map.get(self.__table1_columns[2] + class_name))
            data.append(name_value_map.get(self.__table1_columns[3] + class_name))
            data.append(name_value_map.get(self.__table1_columns[4] + class_name))
            self.__table1_data.append(data)

            self.__table1_column_data_map[self.__table1_columns[0]].append(class_name)
            self.__table1_column_data_map[self.__table1_columns[1]].append(name_value_map.get(self.__table1_columns[1] + class_name))
            self.__table1_column_data_map[self.__table1_columns[2]].append(name_value_map.get(self.__table1_columns[2] + class_name))
            self.__table1_column_data_map[self.__table1_columns[3]].append(name_value_map.get(self.__table1_columns[3] + class_name))
            self.__table1_column_data_map[self.__table1_columns[4]].append(name_value_map.get(self.__table1_columns[4] + class_name))

    def build(self):
        if self.__generated_html:
            return
        if self.__statistic_table is None:
            return
        if self.__column_data_map is None:
            self.__build_tables()
        table1_html = TemplateUtil.generate_table(self.__table1_columns, self.__table1_data)
        table2_html = TemplateUtil.generate_table(self.__table2_columns, self.__table2_data)
        self.__generated_html = '<h5><span>{}</span></h5><br>{}<br><h5><span>{}</span></h5>{}'.format(self.__table1_title, table1_html, self.__table2_title, table2_html)

    def build_items(self):
        if self.__generated_items:
            return self
        if self.__statistic_table is None:
            return self
        if self.__column_data_map is None:
            self.__build_tables()

        table1_item = TableItem(self.__table1_title)
        for name in self.__table1_columns:
            table1_item.addColumn(name, self.__table1_column_data_map[name])
        table2_item = TableItem(self.__table2_title)
        for name in self.__table2_columns:
            table2_item.addColumn(name, self.__table2_column_data_map[name])
        self.__generated_items = [table1_item, table2_item]
        return self


class ParameterReportBuilder(object):
    def __init__(self, parameter_table_columns=None, generated_table_columns=None):
        self.__parameter_table: pandas.DataFrame = None
        self.__parameter_table_columns = ['PARAM_NAME', 'INT_VALUE', 'DOUBLE_VALUE', 'STRING_VALUE']
        if parameter_table_columns:
            self.__parameter_table_columns = parameter_table_columns
        self.__generated_table_columns = ['PARAM NAME', 'INT VALUE', 'DOUBLE VALUE', 'STRING VALUE']
        if generated_table_columns:
            self.__generated_table_columns = generated_table_columns

        self.__table1_title = 'Parameter Table'

        self.__generated_html = None
        self.__generated_items = None

    def get_generated_html(self):
        return self.__generated_html

    def get_generated_items(self):
        return self.__generated_items

    def set_parameter_table(self, target_df):
        self.__parameter_table = None
        self.__generated_html = None
        self.__generated_items = None
        if target_df is not None:
            if isinstance(target_df, dataframe.DataFrame):
                target_df = target_df.collect()
            if target_df.empty is False:
                self.__parameter_table = target_df

    def build(self):
        if self.__generated_html:
            return
        if self.__parameter_table is None:
            return

        cols = []
        for name in self.__parameter_table_columns:
            cols.append(list(self.__parameter_table[name]))
        temp_html_table_data = []
        for i in range(0, len(cols[0])):
            data = []
            for idx in cols:
                data.append(str(idx[i]))
            temp_html_table_data.append(data)
        self.__generated_html = TemplateUtil.generate_table(self.__generated_table_columns, temp_html_table_data)

    def build_items(self):
        if self.__generated_items:
            return self
        if self.__parameter_table is None:
            return self

        table1_item = TableItem(self.__table1_title)
        for i in range(0, len(self.__parameter_table_columns)):
            table1_item.addColumn(self.__generated_table_columns[i], list(self.__parameter_table.iloc[:, i].astype(str)))
        self.__generated_items = [table1_item]
        return self


class ConfusionMatrixReportBuilder(object):
    def __init__(self):
        self.__confusion_matrix_table: pandas.DataFrame = None
        self.__confusion_matrix_table_columns = ['PREDICTED_CLASS', 'ACTUAL_CLASS', 'COUNT']

        self.__confusion_matrix_data = None
        self.__table1_title = 'Confusion Matrix'
        self.__table1_columns = ['Predicted Label', 'True Label']

        self.__generated_html = None
        self.__generated_js = None
        self.__generated_items = None

    def get_generated_html_and_js(self):
        return self.__generated_html, self.__generated_js

    def get_generated_items(self):
        return self.__generated_items

    def set_confusion_matrix_table(self, target_df):
        self.__confusion_matrix_table = None
        self.__confusion_matrix_data = None
        self.__generated_html = None
        self.__generated_js = None
        self.__generated_items = None
        if target_df is not None:
            if isinstance(target_df, dataframe.DataFrame):
                target_df = target_df.collect()
            if target_df.empty is False:
                self.__confusion_matrix_table = target_df

    def __build_confusion_matrix_data(self):
        pandas_df = self.__confusion_matrix_table
        class_names = list(np.unique(pandas_df[self.__confusion_matrix_table_columns[1]]))
        classname_index_map = {}
        for i in range(0, len(class_names)):
            classname_index_map[class_names[i]] = i
        confusion_matrix_list = []
        actual_class_list = list(pandas_df[self.__confusion_matrix_table_columns[1]])
        predicted_class_list = list(pandas_df[self.__confusion_matrix_table_columns[0]])
        count_list = list(pandas_df[self.__confusion_matrix_table_columns[2]])
        for i in range(0, len(predicted_class_list)):
            confusion_matrix_list.append([classname_index_map[predicted_class_list[i]], classname_index_map[actual_class_list[i]], count_list[i]])
        self.__confusion_matrix_data = [class_names, max(count_list), confusion_matrix_list]

    def build(self):
        if self.__generated_html:
            return
        if self.__confusion_matrix_table is None:
            return
        if self.__confusion_matrix_data is None:
            self.__build_confusion_matrix_data()

        classname_list, z_max, x_y_z_list = self.__confusion_matrix_data
        chart_id = TemplateUtil.get_echart_id()
        height = len(classname_list) * 100 + 200
        self.__generated_html = '<div style="height:800px;width:100%;overflow: scroll;margin-top:10px"><div id="{}" style="height:{}px;width:{}px;"></div></div>'.format(chart_id, height, height)
        self.__generated_js = EchartsUtil.generate_load_heatmap_js(chart_id, self.__table1_title, classname_list, self.__table1_columns, z_max, x_y_z_list, True)

    def build_items(self):
        if self.__generated_items:
            return self
        if self.__confusion_matrix_table is None:
            return self
        if self.__confusion_matrix_data is None:
            self.__build_confusion_matrix_data()

        classname_list, z_max, x_y_z_list = self.__confusion_matrix_data
        option = {
            'xAxis': {
                'type': 'category',
                'data': classname_list,
                'name': self.__table1_columns[0],
                'nameLocation': 'center',
                'splitArea': {
                    'show': 'true'
                }
            },
            'yAxis': {
                'inverse': 'true',
                'type': 'category',
                'data': classname_list,
                'name': self.__table1_columns[1],
                'nameLocation': 'center',
                'splitArea': {
                    'show': 'true'
                }
            },
            'visualMap': [{
                'min': 0,
                'max': z_max,
                'orient': 'horizontal',
                'left': 'center',
                'bottom': '-%',
                'calculable': 'true',
                'realtime': 'false',
                'splitNumber': 4,
                'inRange': {
                    'color': [
                        '#e0f3f8',
                        '#abd9e9',
                        '#74add1',
                        '#4575b4',
                    ]
                }
            }],
            'series': [
                {
                    'name': self.__table1_title,
                    'type': 'heatmap',
                    'data': x_y_z_list,
                    'label': {
                        'show': 'true',
                        'color': 'black'
                    },
                    'emphasis': {
                        'itemStyle': {
                            'shadowBlur': 10,
                            'shadowColor': 'rgba(0, 0, 0, 0.5)'
                        }
                    }
                }
            ]
        }
        self.__generated_items = [ChartItem(self.__table1_title, option)]
        return self


class VariableImportanceReportBuilder(object):
    def __init__(self):
        self.__variable_importance_table: pandas.DataFrame = None
        self.__variable_importance_table_columns = ['VARIABLE_NAME', 'IMPORTANCE']

        self.__variable_importance_data = None
        self.__chart1_title = 'Pie Chart'
        self.__chart2_title = 'Bar Chart'

        self.__generated_html = None
        self.__generated_js = None
        self.__generated_items = None

    def get_generated_html_and_js(self):
        return self.__generated_html, self.__generated_js

    def get_generated_items(self):
        return self.__generated_items

    def set_variable_importance_table(self, target_df):
        self.__variable_importance_data = None
        self.__generated_html = None
        self.__generated_js = None
        self.__generated_items = None
        if target_df is not None:
            if isinstance(target_df, dataframe.DataFrame) and target_df.empty() is False:
                self.__variable_importance_table = target_df.sort_values(by=self.__variable_importance_table_columns[1], ascending=False).collect()
            elif isinstance(target_df, pandas.DataFrame) and target_df.empty is False:
                self.__variable_importance_table = target_df.sort_values(by=self.__variable_importance_table_columns[1], ascending=False)

    def __build_variable_importance_data(self):
        pandas_df = self.__variable_importance_table
        pandas_df[self.__variable_importance_table_columns[1]] = pandas.to_numeric(pandas_df[self.__variable_importance_table_columns[1]])
        pandas_df = pandas_df[pandas_df[self.__variable_importance_table_columns[1]] > 0]
        data_names = list(pandas_df[self.__variable_importance_table_columns[0]])
        data_values = list(pandas_df[self.__variable_importance_table_columns[1]])
        self.__variable_importance_data = [data_names, data_values]

    def build(self):
        if self.__generated_html:
            return
        if self.__variable_importance_table is None:
            return
        if self.__variable_importance_data is None:
            self.__build_variable_importance_data()

        data_names, data_values = self.__variable_importance_data
        height = 400
        if len(data_names) > 13:
            height = 35 * len(data_names)
        chart_id = TemplateUtil.get_echart_id()
        chart1_html = '<div style="height:800px;width:100%;overflow:scroll;margin-top:10px"><div id="{}" style="height:{}px;"></div></div>'.format(chart_id, height + 50)
        load_chart1_js = EchartsUtil.generate_load_pie_js(
            chart_id, data_names=data_names, data_values=data_values)
        # [max, min] -> [min, max]
        reverse_data_names = list(data_names)
        reverse_data_names.reverse()
        reverse_data_values = list(data_values)
        reverse_data_values.reverse()
        chart_id = TemplateUtil.get_echart_id()
        chart2_html = '<div style="height:800px;width:100%;overflow:scroll;margin-top:10px"><div id="{}" style="height:{}px;"></div></div>'.format(chart_id, height)
        load_chart2_js = EchartsUtil.generate_load_bar_js(
            chart_id, data_names=reverse_data_names, data_values=reverse_data_values)

        data = []
        data.append(TemplateUtil.construct_tab_item_data(self.__chart1_title, chart1_html))
        data.append(TemplateUtil.construct_tab_item_data(self.__chart2_title, chart2_html))
        section_html, nav_id = TemplateUtil.generate_tab(data)
        load_chart_js = '''
            {js_1}
            $(function(){{
                $('{id}').on('shown.bs.tab', function (e) {{
                    var activeTab_name = $(e.target).text();
                    if (activeTab_name === '{name1}')
                    {{
                        {js_1}
                    }}
                    else
                    {{
                        {js_2}
                    }}
                }});
            }});
        '''.format(id='#{} a[data-toggle="tab"]'.format(nav_id),
                   name1='Pie Chart',
                   js_1=load_chart1_js,
                   js_2=load_chart2_js)

        self.__generated_html = section_html
        self.__generated_js = load_chart_js

    def build_items(self):
        if self.__generated_items:
            return self
        if self.__variable_importance_table is None:
            return self
        if self.__variable_importance_data is None:
            self.__build_variable_importance_data()

        data_names, data_values = self.__variable_importance_data
        series_data = []
        for index in range(0, len(data_names)):
            series_data.append({
                'value': data_values[index],
                'name': data_names[index]
            })

        option = {
            'legend': {
                'type': 'scroll',
                'orient': 'horizontal',
                'left': 'left',
                'data': data_names
            },
            'tooltip': {
                'trigger': 'item',
                'formatter': '{a} <br/>{b} : {c} ({d}%)'
            },
            'series': [
                {
                    'name': '',
                    'type': 'pie',
                    'data': series_data,
                    'emphasis': {
                        'itemStyle': {
                            'shadowBlur': 10,
                            'shadowOffsetX': 0,
                            'shadowColor': 'rgba(0, 0, 0, 0.5)'
                        }
                    }
                }
            ]
        }
        item1 = ChartItem(self.__chart1_title, option, height=len(data_names) * 10 + 300)

        # [max, min] -> [min, max]
        reverse_data_names = list(data_names)
        reverse_data_names.reverse()
        reverse_data_values = list(data_values)
        reverse_data_values.reverse()
        option = {
            'tooltip': {
                'trigger': 'axis',
                'axisPointer': {
                    'type': 'shadow'
                }
            },
            'grid': {
                'left': '3%',
                'right': '4%',
                'bottom': '3%',
                'containLabel': 'true'
            },
            'xAxis': {
                'type': 'value',
                'boundaryGap': [0, 0.01]
            },
            'yAxis': {
                'type': 'category',
                'data': reverse_data_names
            },
            'color': 'dodgerblue',
            'series': [
                {
                    'type': 'bar',
                    'data': reverse_data_values
                }
            ]
        }
        item2 = ChartItem(self.__chart2_title, option, height=len(data_names) * 30)
        self.__generated_items = [item1, item2]
        return self


class MetricReportBuilder(object):
    __ROC_DESC = {
        'title': 'ROC',
        'label': 'ROC Curve',
        'xlabel': 'False Positive Rate (FPR)',
        'ylabel': 'True Positive Rate (TPR)'
    }

    __CUM_GAIN_DESC = {
        'title': 'Cumulative Gains',
        'label': 'Cumulative Gains Curve',
        'xlabel': 'Population',
        'ylabel': 'True Positive Rate (TPR)'
    }

    __CUM_LIFT_DESC = {
        'title': 'Cumulative Lift',
        'label': 'Cumulative Lift Curve',
        'xlabel': 'Population',
        'ylabel': 'Lift'
    }

    __LIFT_DESC = {
        'title': 'Lift',
        'label': 'Lift Curve',
        'xlabel': 'Population',
        'ylabel': 'Interval Lift'
    }

    def __init__(self):
        self.__metric_table: dataframe.DataFrame = None
        self.__table_columns = [
            'ROC_TPR', 'ROC_FPR',
            'CUMGAINS', 'RANDOM_CUMGAINS', 'PERF_CUMGAINS',
            'LIFT', 'RANDOM_LIFT', 'PERF_LIFT',
            'CUMLIFT', 'RANDOM_CUMLIFT', 'PERF_CUMLIFT'
        ]

        self.__roc_sampling: Sampling = None
        self.__other_samplings: Sampling = None
        self.__table_map = None
        self.__sampling_table_map = None

        self.__roc_chart_html = None
        self.__roc_chart_option = None
        self.__load_roc_chart_js = ''

        self.__cumgain_chart_html = None
        self.__cumgain_chart_option = None
        self.__load_cumgain_chart_js = ''

        self.__lift_chart_html = None
        self.__lift_chart_option = None
        self.__load_lift_chart_js = ''

        self.__cumlift_chart_html = None
        self.__cumlift_chart_option = None
        self.__load_cumlift_chart_js = ''

        self.__generated_html = None
        self.__generated_js = None
        self.__generated_items = None

    def get_generated_html_and_js(self):
        return self.__generated_html, self.__generated_js

    def get_generated_items(self):
        return self.__generated_items

    def set_metric_table(self, target_df):
        self.__metric_table = None
        self.__roc_sampling: Sampling = None
        self.__other_samplings: Sampling = None
        self.__table_map = None
        self.__sampling_table_map = None
        self.__generated_html = None
        self.__generated_js = None
        self.__generated_items = None
        if target_df is not None:
            if isinstance(target_df, dataframe.DataFrame) and target_df.empty() is False:
                self.__metric_table = target_df

    def set_metric_samplings(self, roc_sampling: Sampling = None, other_samplings: dict = None):
        self.__roc_sampling = roc_sampling
        self.__other_samplings = other_samplings
        self.__table_map = None
        self.__sampling_table_map = None

    def do_sampling_and_build_data_if_not(self):
        if self.__table_map:
            return

        self.__table_map = {}
        self.__sampling_table_map = {}
        for name in self.__table_columns:
            temp_df = self.__metric_table.filter("NAME='{}'".format(name))
            self.__table_map[name] = temp_df
            self.__sampling_table_map[name] = temp_df

        roc_sampling: Sampling = self.__roc_sampling
        other_samplings: dict = self.__other_samplings
        if roc_sampling:
            self.__sampling_table_map[self.__table_columns[0]] = roc_sampling.fit_transform(data=self.__sampling_table_map.get(self.__table_columns[0]))
            self.__sampling_table_map[self.__table_columns[1]] = roc_sampling.fit_transform(data=self.__sampling_table_map.get(self.__table_columns[1]))

        if other_samplings:
            if other_samplings.get('CUMGAINS'):
                self.__sampling_table_map[self.__table_columns[2]] = other_samplings.get('CUMGAINS').fit_transform(data=self.__sampling_table_map.get(self.__table_columns[2]))
            if other_samplings.get('RANDOM_CUMGAINS'):
                self.__sampling_table_map[self.__table_columns[3]] = other_samplings.get('RANDOM_CUMGAINS').fit_transform(data=self.__sampling_table_map.get(self.__table_columns[3]))
            if other_samplings.get('PERF_CUMGAINS'):
                self.__sampling_table_map[self.__table_columns[4]] = other_samplings.get('PERF_CUMGAINS').fit_transform(data=self.__sampling_table_map.get(self.__table_columns[4]))

            if other_samplings.get('LIFT'):
                self.__sampling_table_map[self.__table_columns[5]] = other_samplings.get('LIFT').fit_transform(data=self.__sampling_table_map.get(self.__table_columns[5]))
            if other_samplings.get('RANDOM_LIFT'):
                self.__sampling_table_map[self.__table_columns[6]] = other_samplings.get('RANDOM_LIFT').fit_transform(data=self.__sampling_table_map.get(self.__table_columns[6]))
            if other_samplings.get('PERF_LIFT'):
                self.__sampling_table_map[self.__table_columns[7]] = other_samplings.get('PERF_LIFT').fit_transform(data=self.__sampling_table_map.get(self.__table_columns[7]))

            if other_samplings.get('CUMLIFT'):
                self.__sampling_table_map[self.__table_columns[8]] = other_samplings.get('CUMLIFT').fit_transform(data=self.__sampling_table_map.get(self.__table_columns[8]))
            if other_samplings.get('RANDOM_CUMLIFT'):
                self.__sampling_table_map[self.__table_columns[9]] = other_samplings.get('RANDOM_CUMLIFT').fit_transform(data=self.__sampling_table_map.get(self.__table_columns[9]))
            if other_samplings.get('PERF_CUMLIFT'):
                self.__sampling_table_map[self.__table_columns[10]] = other_samplings.get('PERF_CUMLIFT').fit_transform(data=self.__sampling_table_map.get(self.__table_columns[10]))

        self.build_roc_data()
        self.build_cumgain_data()
        self.build_lift_data()
        self.build_cumlift_data()

    @staticmethod
    def __get_chart_data(x, y, random_x, random_y, perf_x, perf_y):# pylint: disable=invalid-name
        return [
            {
                'x': x,
                'y': y,
                'label': 'model',
                'color_index': 0
            },
            {
                'x': perf_x,
                'y': perf_y,
                'label': 'Perfect model',
                'color_index': 1
            },
            {
                'x': random_x,
                'y': random_y,
                'label': 'Random model',
                'color_index': 2
            }
        ]

    @staticmethod
    def __generate_chart_code(chart_text, chart_data):
        chart_id = TemplateUtil.get_echart_id()
        chart_html = TemplateUtil.generate_echart(chart_id)
        load_chart_js, option = EchartsUtil.generate_load_line_js(
            chart_id, chart_text=chart_text, chart_data=chart_data)
        return chart_html, load_chart_js, option

    def build_roc_data(self):
        tpr_table = self.__sampling_table_map.get(self.__table_columns[0])
        fpr_table = self.__sampling_table_map.get(self.__table_columns[1])
        if tpr_table.shape[0] == 0 or fpr_table.shape[0] == 0:
            return

        roc_chart_data = [
            {
                'x': list(fpr_table.select('Y').sort('Y').collect()['Y']),
                'y': list(tpr_table.select('Y').sort('Y').collect()['Y']),
                'label': 'model',
                'color_index': 0
            },
            {
                'x': [0, 0, 1],
                'y': [0, 1, 1],
                'label': 'Perfect model',
                'color_index': 1
            },
            {
                'x': [0, 1],
                'y': [0, 1],
                'label': 'Random model',
                'color_index': 2
            }
        ]

        self.__roc_chart_html, self.__load_roc_chart_js, self.__roc_chart_option = MetricReportBuilder.__generate_chart_code(
            MetricReportBuilder.__ROC_DESC, roc_chart_data)

    def build_cumgain_data(self):
        cumgain_table = self.__sampling_table_map.get(self.__table_columns[2])
        random_cumgain_table = self.__sampling_table_map.get(self.__table_columns[3])
        perf_cumgain_table = self.__sampling_table_map.get(self.__table_columns[4])
        if cumgain_table.shape[0] == 0:
            return

        cumgain_chart_data = self.__get_chart_data(
            list(cumgain_table.select('X').collect()['X']), list(cumgain_table.select('Y').collect()['Y']),
            list(random_cumgain_table.select('X').collect()['X']), list(random_cumgain_table.select('Y').collect()['Y']),
            list(perf_cumgain_table.select('X').collect()['X']), list(perf_cumgain_table.select('Y').collect()['Y']))

        self.__cumgain_chart_html, self.__load_cumgain_chart_js, self.__cumgain_chart_option = MetricReportBuilder.__generate_chart_code(
            MetricReportBuilder.__CUM_GAIN_DESC, cumgain_chart_data)

    def build_lift_data(self):
        lift_table = self.__sampling_table_map.get(self.__table_columns[5])
        random_lift_table = self.__sampling_table_map.get(self.__table_columns[6])
        perf_lift_table = self.__sampling_table_map.get(self.__table_columns[7])
        if lift_table.shape[0] == 0:
            return

        lift_chart_data = self.__get_chart_data(
            list(lift_table.select('X').collect()['X']), list(lift_table.select('Y').collect()['Y']),
            list(random_lift_table.select('X').collect()['X']), list(random_lift_table.select('Y').collect()['Y']),
            list(perf_lift_table.select('X').collect()['X']), list(perf_lift_table.select('Y').collect()['Y']))

        self.__lift_chart_html, self.__load_lift_chart_js, self.__lift_chart_option = MetricReportBuilder.__generate_chart_code(
            MetricReportBuilder.__LIFT_DESC, lift_chart_data)

    def build_cumlift_data(self):
        cumlift_table = self.__sampling_table_map.get(self.__table_columns[8])
        random_cumlift_table = self.__sampling_table_map.get(self.__table_columns[9])
        perf_cumlift_table = self.__sampling_table_map.get(self.__table_columns[10])
        if cumlift_table.shape[0] == 0:
            return

        cumlift_chart_data = self.__get_chart_data(
            list(cumlift_table.select('X').collect()['X']), list(cumlift_table.select('Y').collect()['Y']),
            list(random_cumlift_table.select('X').collect()['X']), list(random_cumlift_table.select('Y').collect()['Y']),
            list(perf_cumlift_table.select('X').collect()['X']), list(perf_cumlift_table.select('Y').collect()['Y']))

        self.__cumlift_chart_html, self.__load_cumlift_chart_js, self.__cumlift_chart_option = MetricReportBuilder.__generate_chart_code(
            MetricReportBuilder.__CUM_LIFT_DESC, cumlift_chart_data)

    def build(self):
        if self.__generated_html:
            return
        if self.__metric_table is None:
            return

        self.do_sampling_and_build_data_if_not()

        data = []
        if self.__roc_chart_html:
            data.append(TemplateUtil.construct_tab_item_data(MetricReportBuilder.__ROC_DESC['title'], self.__roc_chart_html))
        if self.__cumgain_chart_html:
            data.append(TemplateUtil.construct_tab_item_data(MetricReportBuilder.__CUM_GAIN_DESC['title'], self.__cumgain_chart_html))
        if self.__cumlift_chart_html:
            data.append(TemplateUtil.construct_tab_item_data(MetricReportBuilder.__CUM_LIFT_DESC['title'], self.__cumlift_chart_html))
        if self.__lift_chart_html:
            data.append(TemplateUtil.construct_tab_item_data(MetricReportBuilder.__LIFT_DESC['title'], self.__lift_chart_html))
        section_html, nav_id = TemplateUtil.generate_tab(data)
        load_chart_js = '''
            {js_1}
            $(function(){{
                $('{id}').on('shown.bs.tab', function (e) {{
                    var activeTab_name = $(e.target).text();
                    if (activeTab_name === '{name1}')
                    {{
                        {js_1}
                    }}
                    else if (activeTab_name === '{name2}')
                    {{
                        {js_2}
                    }}
                    else if (activeTab_name === '{name3}')
                    {{
                        {js_3}
                    }}
                    else
                    {{
                        {js_4}
                    }}
                }});
            }});
        '''.format(id='#{} a[data-toggle="tab"]'.format(nav_id),
                   name1=MetricReportBuilder.__ROC_DESC['title'],
                   name2=MetricReportBuilder.__LIFT_DESC['title'],
                   name3=MetricReportBuilder.__CUM_LIFT_DESC['title'],
                   js_1=self.__load_roc_chart_js,
                   js_2=self.__load_lift_chart_js,
                   js_3=self.__load_cumlift_chart_js,
                   js_4=self.__load_cumgain_chart_js)

        self.__generated_html = section_html
        self.__generated_js = load_chart_js

    def build_items(self):
        if self.__generated_items:
            return self
        if self.__metric_table is None:
            return self

        self.do_sampling_and_build_data_if_not()

        temp_items = []
        if self.__roc_chart_option:
            temp_items.append(ChartItem(MetricReportBuilder.__ROC_DESC['title'], self.__roc_chart_option))
        if self.__cumgain_chart_option:
            temp_items.append(ChartItem(MetricReportBuilder.__CUM_GAIN_DESC['title'], self.__cumgain_chart_option))
        if self.__cumlift_chart_option:
            temp_items.append(ChartItem(MetricReportBuilder.__CUM_LIFT_DESC['title'], self.__cumlift_chart_option))
        if self.__lift_chart_option:
            temp_items.append(ChartItem(MetricReportBuilder.__LIFT_DESC['title'], self.__lift_chart_option))

        self.__generated_items = temp_items
        return self


class ComparisonItemBuilder(object):
    def __init__(self):
        self.line_configs = []
        self.interval_configs = []

    def add_line(self, title, data, x_name, y_name=None, interval_names=None, color=None):
        if y_name is not None:
            self.line_configs.append((title, data, x_name, y_name, 'line', color))
        if interval_names is not None and len(interval_names) == 2:
            lower_line_name = interval_names[0]
            upper_line_name = interval_names[1]
            if lower_line_name is not None and lower_line_name != '' and upper_line_name is not None and upper_line_name != '':
                if color is None or color == '':
                    color = '#ccc'
                self.interval_configs.append((title, data, x_name, interval_names, 'line', color))
            else:
                logger.error('The parameter interval_names of add_line method is not correct.')
        return self

    def get_item(self, title="Comparison"):
        if len(self.line_configs) == 0 and len(self.interval_configs) == 0:
            logger.error('Please add line by calling add_line method.')

        y_base = 0
        pair_confidence_names = []
        if self.interval_configs is not None:
            for config in self.interval_configs:
                pair_confidence_names.append(['{}({})'.format(config[0], config[3][0]), '{}({})'.format(config[0], config[3][1])])
                d_data = []
                for d in list(config[1].collect()[config[3][0]]):  # lower_data
                    if d is not None:
                        d_data.append(d)
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
                x = list(result[config[2]].astype(str))
                y = []
                for d in list(result[config[3]]):
                    if d is None or np.isnan(float(d)):
                        y.append('-')
                    else:
                        y.append(float(d) + float(y_base))
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
        for config in self.interval_configs:
            if config[0] in line_titles:
                raise ValueError('Multiple lines use the same title {}.'.format(config[0]))
            else:
                line_titles.append(config[0])
            k = k + 1
            if config[1] is not None:
                result = config[1]
                try:
                    result = result.collect().sort_values(config[2], ascending=True)
                    x = list(result[config[2]].astype(str))
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
                                if d is None or np.isnan(float(d)):
                                    lower.append('-')
                                else:
                                    lower.append(float(d) + float(y_base))
                            datasets.append({
                                'source': [x, lower]
                            })
                            lower_line_series_names.append('{}({})'.format(config[0], config[3][i]))
                        elif i == 1:
                            j = -1
                            for d in temp_upper:
                                j = j + 1
                                if d is None or np.isnan(float(d)):
                                    upper.append('-')
                                else:
                                    upper.append(float(d) - float(temp_lower[j]))
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
                'name': 'ID',
                'boundaryGap': 'true',
                'axisTick': {
                    'alignWithLabel': 'true',
                    'show': 'false'
                },
                'axisLabel': {
                    'showMinLabel': 'true',
                    'showMaxLabel': 'true',
                    'hideOverlap': 'true',
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


class _UnifiedClassificationReportBuilder(object):
    def __init__(self, param_columns=None, display_columns=None):
        self.__build_error_msg = 'To generate a report, you must call the build_report method firstly.'

        self.__statistic_report_builder = StatisticReportBuilder()
        self.__scoring_statistic_report_builder = StatisticReportBuilder()

        self.__parameter_report_builder = ParameterReportBuilder(param_columns, display_columns)
        self.__optimal_parameter_report_builder = ParameterReportBuilder()

        self.__confusion_matrix_report_builder = ConfusionMatrixReportBuilder()
        self.__scoring_confusion_matrix_report_builder = ConfusionMatrixReportBuilder()

        self.__variable_importance_report_builder = VariableImportanceReportBuilder()

        self.__metric_report_builder = MetricReportBuilder()
        self.__scoring_metric_report_builder = MetricReportBuilder()

        self.__shapley_explainer_of_predict_phase: ShapleyExplainer = None
        self.__display_force_plot_of_predict_phase = True

        self.__shapley_explainer_of_score_phase: ShapleyExplainer = None
        self.__display_force_plot_of_score_phase = True

        self.__all_html = None
        self.__all_js = None

        self.__roc_sampling = None
        self.__other_samplings = None

        self.__report_html = None
        self.__iframe_report_html = None

        self._report_builder: ReportBuilder = None
        self.framework_version = 'v2'

    def __add_html(self, generated_html):
        if generated_html:
            self.__all_html = self.__all_html + generated_html

    def __add_js(self, generated_js):
        if generated_js:
            self.__all_js = self.__all_js + generated_js

    def _set_statistic_table(self, statistic_table):
        self.__statistic_report_builder.set_statistic_table(statistic_table)
        return self

    def _set_scoring_statistic_table(self, statistic_table):
        self.__all_html = None
        self.__all_js = None
        self.__report_html = None
        self.__iframe_report_html = None
        self._report_builder: ReportBuilder = None
        self.__scoring_statistic_report_builder.set_statistic_table(statistic_table)
        return self

    def _set_parameter_table(self, parameter_table):
        self.__parameter_report_builder.set_parameter_table(parameter_table)
        return self

    def _set_optimal_parameter_table(self, parameter_table):
        self.__optimal_parameter_report_builder.set_parameter_table(parameter_table)
        return self

    def _set_confusion_matrix_table(self, confusion_matrix_table):
        self.__confusion_matrix_report_builder.set_confusion_matrix_table(confusion_matrix_table)
        return self

    def _set_scoring_confusion_matrix_table(self, confusion_matrix_table):
        self.__scoring_confusion_matrix_report_builder.set_confusion_matrix_table(confusion_matrix_table)
        return self

    def _set_variable_importance_table(self, variable_importance_table):
        self.__variable_importance_report_builder.set_variable_importance_table(variable_importance_table)
        return self

    def set_metric_samplings(self, roc_sampling: Sampling = None, other_samplings: dict = None):
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
        >>> model.set_metric_samplings(roc_sampling, other_samplings)
        """
        self.__roc_sampling = roc_sampling
        self.__other_samplings = other_samplings
        self.__metric_report_builder.set_metric_samplings(self.__roc_sampling, self.__other_samplings)
        self.__scoring_metric_report_builder.set_metric_samplings(self.__roc_sampling, self.__other_samplings)

    def _set_metric_table(self, metric_table):
        self.__metric_report_builder.set_metric_table(metric_table)
        return self

    def _set_scoring_metric_table(self, metric_table):
        self.__all_html = None
        self.__all_js = None
        self.__report_html = None
        self.__iframe_report_html = None
        self._report_builder: ReportBuilder = None
        self.__scoring_metric_report_builder.set_metric_table(metric_table)
        return self

    def set_shapley_explainer_of_predict_phase(self, shapley_explainer, display_force_plot=True):
        """
        Use the reason code generated during the prediction phase to build a ShapleyExplainer instance. \n
        When this instance is passed in, the execution results of this instance will be included in the report of v2 version.

        Parameters
        ----------
        shapley_explainer : :class:`~hana_ml.visualizers.shap.ShapleyExplainer`
            A ShapleyExplainer instance.

        display_force_plot : bool, optional
            Whether to display the force plot.

            Defaults to True.
        """
        if shapley_explainer is None:
            self.__shapley_explainer_of_predict_phase = None
        else:
            if isinstance(shapley_explainer, ShapleyExplainer):
                self.__shapley_explainer_of_predict_phase = shapley_explainer
                if display_force_plot in (True, False):
                    self.__display_force_plot_of_predict_phase = display_force_plot
                else:
                    raise TypeError('The type of passed parameter[display_force_plot] is not bool!')
            else:
                raise TypeError('The type of passed parameter[shapley_explainer] is not hana_ml.visualizers.shap.ShapleyExplainer!')

    def set_shapley_explainer_of_score_phase(self, shapley_explainer, display_force_plot=True):
        """
        Use the reason code generated during the scoring phase to build a ShapleyExplainer instance. \n
        When this instance is passed in, the execution results of this instance will be included in the report of v2 version.

        Parameters
        ----------
        shapley_explainer : :class:`~hana_ml.visualizers.shap.ShapleyExplainer`
            A ShapleyExplainer instance.

        display_force_plot : bool, optional
            Whether to display the force plot.

            Defaults to True.
        """
        if shapley_explainer is None:
            self.__shapley_explainer_of_score_phase = None
        else:
            if isinstance(shapley_explainer, ShapleyExplainer):
                self.__shapley_explainer_of_score_phase = shapley_explainer
                if display_force_plot in (True, False):
                    self.__display_force_plot_of_score_phase = display_force_plot
                else:
                    raise TypeError('The type of passed parameter[display_force_plot] is not bool!')
            else:
                raise TypeError('The type of passed parameter[shapley_explainer] is not hana_ml.visualizers.shap.ShapleyExplainer!')

    def _render_report(self):
        if self.framework_version in ('v2', 'v1'):
            pass
        else:
            raise Exception('The passed parameter[framework_version] value is not correct!')

        if self.framework_version == 'v2':
            pages = []
            items = self.__statistic_report_builder.build_items().get_generated_items()
            if items:
                page = Page('Statistic')
                page.addItems(items)
                pages.append(page)
            items = self.__parameter_report_builder.build_items().get_generated_items()
            if items:
                page = Page('Parameter')
                page.addItems(items)
                pages.append(page)
            items = self.__optimal_parameter_report_builder.build_items().get_generated_items()
            if items:
                page = Page('Optimal Parameter')
                page.addItems(items)
                pages.append(page)
            items = self.__confusion_matrix_report_builder.build_items().get_generated_items()
            if items:
                page = Page('Confusion Matrix')
                page.addItems(items)
                pages.append(page)
            items = self.__variable_importance_report_builder.build_items().get_generated_items()
            if items:
                page = Page('Variable Importance')
                page.addItems(items)
                pages.append(page)
            items = self.__metric_report_builder.build_items().get_generated_items()
            if items:
                page = Page('Metrics')
                page.addItems(items)
                pages.append(page)
            items = self.__scoring_statistic_report_builder.build_items().get_generated_items()
            if items:
                page = Page('Scoring Statistic')
                page.addItems(items)
                pages.append(page)
            items = self.__scoring_confusion_matrix_report_builder.build_items().get_generated_items()
            if items:
                page = Page('Scoring Confusion Matrix')
                page.addItems(items)
                pages.append(page)
            items = self.__scoring_metric_report_builder.build_items().get_generated_items()
            if items:
                page = Page('Scoring Metrics')
                page.addItems(items)
                pages.append(page)
            if self.__shapley_explainer_of_predict_phase is not None:
                page = Page('Shap of predict phase')
                page.addItem(self.__shapley_explainer_of_predict_phase.get_beeswarm_plot_item())
                page.addItem(self.__shapley_explainer_of_predict_phase.get_bar_plot_item())
                page.addItems(self.__shapley_explainer_of_predict_phase.get_dependence_plot_items())
                if self.__display_force_plot_of_predict_phase is True:
                    page.addItem(self.__shapley_explainer_of_predict_phase.get_force_plot_item())
                pages.append(page)
            if self.__shapley_explainer_of_score_phase is not None:
                page = Page('Shap of score phase')
                page.addItem(self.__shapley_explainer_of_score_phase.get_beeswarm_plot_item())
                page.addItem(self.__shapley_explainer_of_score_phase.get_bar_plot_item())
                page.addItems(self.__shapley_explainer_of_score_phase.get_dependence_plot_items())
                if self.__display_force_plot_of_score_phase is True:
                    page.addItem(self.__shapley_explainer_of_score_phase.get_force_plot_item())
                pages.append(page)
            self._report_builder = ReportBuilder(title='Unified Classification Model Report')
            self._report_builder.addPages(pages)
            self._report_builder.build()
        else:
            self.__all_html = ''
            self.__all_js = ''
            self.__statistic_report_builder.build()
            self.__scoring_statistic_report_builder.build()
            self.__parameter_report_builder.build()
            self.__optimal_parameter_report_builder.build()
            self.__confusion_matrix_report_builder.build()
            self.__scoring_confusion_matrix_report_builder.build()
            self.__variable_importance_report_builder.build()
            self.__metric_report_builder.build()
            self.__scoring_metric_report_builder.build()

            statistic_body_html = self.__statistic_report_builder.get_generated_html()
            scoring_statistic_body_html = self.__scoring_statistic_report_builder.get_generated_html()
            if scoring_statistic_body_html is None:
                scoring_statistic_body_html = 'If you want to view the scoring statistics, please call the score function.'
            parameter_body_html = self.__parameter_report_builder.get_generated_html()
            optimal_parameter_body_html = self.__optimal_parameter_report_builder.get_generated_html()
            confusion_matrix_body_html, load_chart_js = self.__confusion_matrix_report_builder.get_generated_html_and_js()
            self.__add_js(load_chart_js)
            scoring_confusion_matrix_body_html, scoring_load_chart_js = self.__scoring_confusion_matrix_report_builder.get_generated_html_and_js()
            if scoring_confusion_matrix_body_html is None:
                scoring_confusion_matrix_body_html = 'If you want to view the scoring confusion matrix, please call the score function.'
            else:
                self.__add_js(scoring_load_chart_js)
            variable_importance_body_html, load_chart_js = self.__variable_importance_report_builder.get_generated_html_and_js()
            self.__add_js(load_chart_js)
            metrics_body_html, load_chart_js = self.__metric_report_builder.get_generated_html_and_js()
            self.__add_js(load_chart_js)
            scoring_metrics_body_html, scoring_load_chart_js = self.__scoring_metric_report_builder.get_generated_html_and_js()
            if scoring_metrics_body_html is None:
                scoring_metrics_body_html = 'If you want to view the scoring metrics, please call the score function.'
            else:
                self.__add_js(scoring_load_chart_js)

            self.__report_html = EmbeddedUI.get_resource_template('unified_classification_model_report.html').render(
                title='Unified Classification Model Report',
                start_time=EmbeddedUI.get_current_time(),
                statistic=statistic_body_html,
                scoring_statistic=scoring_statistic_body_html,
                parameter=parameter_body_html,
                optimal_parameter=optimal_parameter_body_html,
                confusion_matrix=confusion_matrix_body_html,
                scoring_confusion_matrix=scoring_confusion_matrix_body_html,
                variable_importance=variable_importance_body_html,
                metrics=metrics_body_html,
                scoring_metrics=scoring_metrics_body_html,
                load_echarts_js=self.__all_js)
            self.__iframe_report_html = EmbeddedUI.get_iframe_str(self.__report_html)

    def build_report(self):
        """
        Build model report.
        """
        raise NotImplementedError # to be implemented by subclass

    def set_framework_version(self, framework_version):
        """
        Switch v1/v2 version of report.

        Parameters
        ----------
        framework_version : {'v2', 'v1'}, optional
            v2: using report builder framework.
            v1: using pure html template.

            Defaults to 'v2'.
        """
        self.framework_version = framework_version

    @property
    def report(self):
        if self.framework_version in ('v2', 'v1'):
            pass
        else:
            raise Exception('The passed parameter[framework_version] value is not correct!')

        if self.framework_version == 'v2':
            try:
                self._report_builder.build()
                iframe_str = EmbeddedUI.get_iframe_str(self._report_builder.html_str)
                return iframe_str
            except BaseException:
                raise Exception(self.__build_error_msg)
        else:
            if self.__iframe_report_html is None:
                self._render_report()
            if self.__iframe_report_html is None:
                raise Exception(self.__build_error_msg)
            return self.__iframe_report_html

    def generate_html_report(self, filename):
        """
        Save model report as a html file.

        Parameters
        ----------
        filename : str
            Html file name.
        """
        if self.framework_version == 'v2':
            try:
                self._report_builder.generate_html(filename)
            except BaseException:
                raise Exception(self.__build_error_msg)
        else:
            if self.__report_html is None:
                self._render_report()
            if self.__report_html is None:
                raise Exception(self.__build_error_msg)
            EmbeddedUI.generate_file('{}.html'.format(filename), self.__report_html)

    def generate_notebook_iframe_report(self):
        """
        Render model report as a notebook iframe.

        """
        if self.framework_version in ('v2', 'v1'):
            pass
        else:
            raise Exception('The passed parameter[framework_version] value is not correct!')

        if self.framework_version == 'v2':
            try:
                self._report_builder.generate_notebook_iframe()
            except BaseException:
                raise Exception(self.__build_error_msg)
        else:
            if self.__iframe_report_html is None:
                self._render_report()
            if self.__iframe_report_html is None:
                raise Exception(self.__build_error_msg)
            print('\033[31m{}'.format('To better review the unified classification model report, please adjust the size of the left area or hide the left area temporarily!'))
            EmbeddedUI.render_html_str(self.__iframe_report_html)


class _UnifiedRegressionReportBuilder(object):
    def __init__(self, param_columns=None, display_columns=None):
        self.__build_error_msg = 'To generate a report, you must call the build_report method firstly.'

        self.__statistic_table: dataframe.DataFrame = None
        self.__scoring_statistic_table: dataframe.DataFrame = None
        self.__scoring_data_table: dataframe.DataFrame = None
        self.__scoring_result_table: dataframe.DataFrame = None

        self.__parameter_report_builder = ParameterReportBuilder(param_columns, display_columns)
        self.__optimal_parameter_report_builder = ParameterReportBuilder()

        self.__variable_importance_report_builder = VariableImportanceReportBuilder()

        self.__shapley_explainer_of_predict_phase: ShapleyExplainer = None
        self.__display_force_plot_of_predict_phase = True

        self.__shapley_explainer_of_score_phase: ShapleyExplainer = None
        self.__display_force_plot_of_score_phase = True

        self.__all_html = None
        self.__all_js = None

        self.__report_html = None
        self.__iframe_report_html = None

        self._report_builder: ReportBuilder = None
        self.framework_version = 'v2'

    def __add_html(self, generated_html):
        if generated_html:
            self.__all_html = self.__all_html + generated_html

    def __add_js(self, generated_js):
        if generated_js:
            self.__all_js = self.__all_js + generated_js

    def _set_statistic_table(self, statistic_table):
        if statistic_table and statistic_table.shape[0] > 0:
            self.__statistic_table = statistic_table
        return self

    def _set_scoring_statistic_table(self, statistic_table):
        self.__all_html = None
        self.__all_js = None
        self.__report_html = None
        self.__iframe_report_html = None
        self._report_builder: ReportBuilder = None
        if statistic_table and statistic_table.shape[0] > 0:
            self.__scoring_statistic_table = statistic_table
        return self

    def _set_scoring_result_table(self, input_table, result_table):
        self.__all_html = None
        self.__all_js = None
        self.__report_html = None
        self.__iframe_report_html = None
        self._report_builder: ReportBuilder = None
        if result_table and result_table.shape[0] > 0:
            self.__scoring_result_table = result_table
        if input_table and input_table.shape[0] > 0:
            self.__scoring_data_table = input_table
        return self

    def _set_parameter_table(self, parameter_table):
        self.__parameter_report_builder.set_parameter_table(parameter_table)
        return self

    def _set_optimal_parameter_table(self, parameter_table):
        self.__optimal_parameter_report_builder.set_parameter_table(parameter_table)
        return self

    def _set_variable_importance_table(self, variable_importance_table):
        self.__variable_importance_report_builder.set_variable_importance_table(variable_importance_table)
        return self

    def set_shapley_explainer_of_predict_phase(self, shapley_explainer, display_force_plot=True):
        """
        Use the reason code generated during the prediction phase to build a ShapleyExplainer instance. \n
        When this instance is passed in, the execution results of this instance will be included in the report of v2 version.

        Parameters
        ----------
        shapley_explainer : :class:`~hana_ml.visualizers.shap.ShapleyExplainer`
            ShapleyExplainer instance.

        display_force_plot : bool, optional
            Whether to display the force plot.

            Defaults to True.
        """
        if shapley_explainer is None:
            self.__shapley_explainer_of_predict_phase = None
        else:
            if isinstance(shapley_explainer, ShapleyExplainer):
                self.__shapley_explainer_of_predict_phase = shapley_explainer
                if display_force_plot in (True, False):
                    self.__display_force_plot_of_predict_phase = display_force_plot
                else:
                    raise TypeError('The type of passed parameter[display_force_plot] is not bool!')
            else:
                raise TypeError('The type of passed parameter[shapley_explainer] is not hana_ml.visualizers.shap.ShapleyExplainer!')

    def set_shapley_explainer_of_score_phase(self, shapley_explainer, display_force_plot=True):
        """
        Use the reason code generated during the scoring phase to build a ShapleyExplainer instance. \n
        When this instance is passed in, the execution results of this instance will be included in the report of v2 version.

        Parameters
        ----------
        shapley_explainer : :class:`~hana_ml.visualizers.shap.ShapleyExplainer`
            ShapleyExplainer instance.

        display_force_plot : bool, optional
            Whether to display the force plot.

            Defaults to True.
        """
        if shapley_explainer is None:
            self.__shapley_explainer_of_score_phase = None
        else:
            if isinstance(shapley_explainer, ShapleyExplainer):
                self.__shapley_explainer_of_score_phase = shapley_explainer
                if display_force_plot in (True, False):
                    self.__display_force_plot_of_score_phase = display_force_plot
                else:
                    raise TypeError('The type of passed parameter[display_force_plot] is not bool!')
            else:
                raise TypeError('The type of passed parameter[shapley_explainer] is not hana_ml.visualizers.shap.ShapleyExplainer!')

    def _render_report(self):
        if self.framework_version in ('v2', 'v1'):
            pass
        else:
            raise Exception('The passed parameter[framework_version] value is not correct!')

        if self.framework_version == 'v2':
            pages = []
            if self.__statistic_table:
                table_data = self.__statistic_table.collect()
                table_item = TableItem('Stats Table')
                table_item.addColumn('STAT NAME', list(table_data.iloc[:, 0].astype(str)))
                table_item.addColumn('STAT VALUE', list(table_data.iloc[:, 1].astype(str)))
                page = Page('Statistic')
                page.addItem(table_item)
                pages.append(page)
            items = self.__parameter_report_builder.build_items().get_generated_items()
            if items:
                page = Page('Parameter')
                page.addItems(items)
                pages.append(page)
            items = self.__optimal_parameter_report_builder.build_items().get_generated_items()
            if items:
                page = Page('Optimal Parameter')
                page.addItems(items)
                pages.append(page)
            items = self.__variable_importance_report_builder.build_items().get_generated_items()
            if items:
                page = Page('Variable Importance')
                page.addItems(items)
                pages.append(page)
            page = Page('Scoring phase')
            if self.__scoring_statistic_table:
                table_data = self.__scoring_statistic_table.collect()
                table_item = TableItem('Statistic Table')
                table_item.addColumn('STAT NAME', list(table_data.iloc[:, 0].astype(str)))
                table_item.addColumn('STAT VALUE', list(table_data.iloc[:, 1].astype(str)))
                page.addItem(table_item)
            if self.__scoring_result_table:
                column_names = self.__scoring_result_table.columns
                interval_names = ['LOWER_BOUND', 'UPPER_BOUND']
                lower_bound_hasna = True
                upper_bound_hasna = True
                if interval_names[0] in column_names:
                    lower_bound_hasna = self.__scoring_result_table.hasna(interval_names[0])
                if interval_names[1] in column_names:
                    upper_bound_hasna = self.__scoring_result_table.hasna(interval_names[1])
                if lower_bound_hasna is False and upper_bound_hasna is False:
                    comparison_item_builder = ComparisonItemBuilder()
                    comparison_item_builder.add_line('Actual', self.__scoring_data_table, self.__scoring_data_table.columns[0], self.__scoring_data_table.columns[1])
                    comparison_item_builder.add_line('SCORE', self.__scoring_result_table, self.__scoring_result_table.columns[0], 'SCORE')
                    comparison_item_builder.add_line('Scoring Bound', self.__scoring_result_table, self.__scoring_result_table.columns[0], interval_names=interval_names)
                    page.addItem(comparison_item_builder.get_item())
            if len(page.items) > 0:
                pages.append(page)
            if self.__shapley_explainer_of_predict_phase is not None:
                page = Page('Shap of predict phase')
                page.addItem(self.__shapley_explainer_of_predict_phase.get_beeswarm_plot_item())
                page.addItem(self.__shapley_explainer_of_predict_phase.get_bar_plot_item())
                page.addItems(self.__shapley_explainer_of_predict_phase.get_dependence_plot_items())
                if self.__display_force_plot_of_predict_phase is True:
                    page.addItem(self.__shapley_explainer_of_predict_phase.get_force_plot_item())
                pages.append(page)
            if self.__shapley_explainer_of_score_phase is not None:
                page = Page('Shap of score phase')
                page.addItem(self.__shapley_explainer_of_score_phase.get_beeswarm_plot_item())
                page.addItem(self.__shapley_explainer_of_score_phase.get_bar_plot_item())
                page.addItems(self.__shapley_explainer_of_score_phase.get_dependence_plot_items())
                if self.__display_force_plot_of_score_phase is True:
                    page.addItem(self.__shapley_explainer_of_score_phase.get_force_plot_item())
                pages.append(page)
            self._report_builder = ReportBuilder(title='Unified Regression Model Report')
            self._report_builder.addPages(pages)
            self._report_builder.build()
        else:
            self.__all_html = ''
            self.__all_js = ''
            self.__parameter_report_builder.build()
            self.__optimal_parameter_report_builder.build()
            self.__variable_importance_report_builder.build()

            statistic_body_html = None
            if self.__statistic_table:
                statistic_body_html = TemplateUtil.generate_table_html(self.__statistic_table, ['STAT NAME', 'STAT VALUE'])
            scoring_statistic_body_html = None
            if self.__scoring_statistic_table:
                scoring_statistic_body_html = TemplateUtil.generate_table_html(self.__scoring_statistic_table, ['STAT NAME', 'STAT VALUE'])
            if scoring_statistic_body_html is None:
                scoring_statistic_body_html = 'If you want to view the scoring statistics, please call the score function.'
            parameter_body_html = self.__parameter_report_builder.get_generated_html()
            optimal_parameter_body_html = self.__optimal_parameter_report_builder.get_generated_html()
            variable_importance_body_html, load_chart_js = self.__variable_importance_report_builder.get_generated_html_and_js()
            self.__add_js(load_chart_js)

            self.__report_html = EmbeddedUI.get_resource_template('unified_regression_model_report.html').render(
                title='Unified Regression Model Report',
                start_time=EmbeddedUI.get_current_time(),
                statistic=statistic_body_html,
                scoring_statistic=scoring_statistic_body_html,
                parameter=parameter_body_html,
                optimal_parameter=optimal_parameter_body_html,
                variable_importance=variable_importance_body_html,
                load_echarts_js=self.__all_js)
            self.__iframe_report_html = EmbeddedUI.get_iframe_str(self.__report_html)

    def build_report(self):
        """
        Build model report.
        """

        raise NotImplementedError # to be implemented by subclass

    def set_framework_version(self, framework_version):
        """
        Switch v1/v2 version of report.

        Parameters
        ----------
        framework_version : {'v2', 'v1'}, optional
            v2: using report builder framework.
            v1: using pure html template.

            Defaults to 'v2'.
        """
        self.framework_version = framework_version

    @property
    def report(self):
        if self.framework_version in ('v2', 'v1'):
            pass
        else:
            raise Exception('The passed parameter[framework_version] value is not correct!')

        if self.framework_version == 'v2':
            try:
                self._report_builder.build()
                iframe_str = EmbeddedUI.get_iframe_str(self._report_builder.html_str)
                return iframe_str
            except BaseException:
                raise Exception(self.__build_error_msg)
        else:
            if self.__iframe_report_html is None:
                self._render_report()
            if self.__iframe_report_html is None:
                raise Exception(self.__build_error_msg)
            return self.__iframe_report_html

    def generate_html_report(self, filename):
        """
        Save model report as a html file.

        Parameters
        ----------
        filename : str
            Html file name.
        """
        if self.framework_version in ('v2', 'v1'):
            pass
        else:
            raise Exception('The passed parameter[framework_version] value is not correct!')

        if self.framework_version == 'v2':
            try:
                self._report_builder.generate_html(filename)
            except BaseException:
                raise Exception(self.__build_error_msg)
        else:
            if self.__iframe_report_html is None:
                self._render_report()
            if self.__report_html is None:
                raise Exception(self.__build_error_msg)
            EmbeddedUI.generate_file('{}.html'.format(filename), self.__report_html)

    def generate_notebook_iframe_report(self):
        """
        Render model report as a notebook iframe.
        """
        if self.framework_version in ('v2', 'v1'):
            pass
        else:
            raise Exception('The passed parameter[framework_version] value is not correct!')

        if self.framework_version == 'v2':
            try:
                self._report_builder.generate_notebook_iframe()
            except BaseException:
                raise Exception(self.__build_error_msg)
        else:
            if self.__iframe_report_html is None:
                self._render_report()
            if self.__iframe_report_html is None:
                raise Exception(self.__build_error_msg)
            print('\033[31m{}'.format('To better review the unified regression model report, you need to adjust the size of the left area or hide the left area temporarily!'))
            EmbeddedUI.render_html_str(self.__iframe_report_html)
