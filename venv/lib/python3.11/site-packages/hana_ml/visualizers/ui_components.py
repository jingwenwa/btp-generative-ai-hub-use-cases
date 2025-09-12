# pylint: disable=too-many-arguments
# pylint: disable=too-few-public-methods
# pylint: disable=line-too-long
# pylint: disable=too-many-locals
# pylint: disable=missing-docstring
# pylint: disable=invalid-name
# pylint: disable=unused-variable
# pylint: disable=too-many-instance-attributes
# pylint: disable=too-many-branches
# pylint: disable=too-many-statements
# pylint: disable=trailing-newlines
# pylint: disable=consider-using-f-string
# pylint: disable=use-maxsplit-arg
import time
from threading import Lock
import math
import pandas
from hana_ml.visualizers.shared import EmbeddedUI


class IdGenerator(object):
    def __init__(self):
        self.base_id = 0
        self.lock = Lock()
        self.current_time = (str(int(time.time() * 1000)))[::-1]

    def id(self):
        self.lock.acquire()
        new_id = self.base_id + 1
        self.base_id = new_id
        self.lock.release()
        return str(new_id) + self.current_time


idGenerator = IdGenerator()


def convert_to_array(df: pandas.DataFrame):
    if isinstance(df, pandas.DataFrame) is False:
        raise TypeError("The type of parameter 'df' must be pandas.DataFrame!")
    else:
        if df.empty:
            raise ValueError("The value of parameter 'df' is empty!")
        else:
            data = []
            data.append(list(df.columns))
            for i in range(0, list(df.count())[0]):
                data.append(list(df.T[i]))
            return data


def get_floor_value(value):
    if value < 0:
        new_value = -value
    else:
        new_value = value
    base_value = math.pow(10, len(str(new_value).split('.')[0]) - 1)
    new_value = (math.floor(new_value / base_value) + 1) * base_value
    if value < 0:
        return -new_value
    else:
        return new_value


class ChartConfig(object):
    def __init__(self, dataset: pandas.DataFrame, title='', sub_title='', xAxis_name='', yAxis_name=''):
        self.dataset: pandas.DataFrame = dataset
        self.column_names = list(dataset.columns)
        self.dataset_array = convert_to_array(dataset)
        self.ignore_yAxis_min_max = False
        self.yAxis_min_max_value_magnification_factor = 1
        self.yAxis_values = []
        self.config = {
            'dataset': {
                'source': self.dataset_array
            },
            'title': {
                'text': title,
                'subtext': sub_title,
                'left': 'center'
            },
            'tooltip': {
                'trigger': 'axis'
            },
            'grid': {
                'containLabel': 'true',
                'show': 'true'
            },
            'xAxis': {
                'name': xAxis_name,
                # 'nameLocation': 'center',
                # 'nameGap': 35,
                'type': 'category',
                'boundaryGap': 'false',
                'splitLine': {
                    'show': 'false'
                },
                'axisLabel': {
                    'showMinLabel': 'true',
                    'showMaxLabel': 'true',
                    'rotate': 20,
                    'fontSize': 9,
                    # 'interval': 1
                },
                'axisTick': {
                    'alignWithLabel': 'true'
                }
            },
            'yAxis': {
                'name': yAxis_name,
                # 'nameLocation': 'center',
                # 'nameGap': 35,
                'type': 'value',
                'splitLine': {
                    'show': 'true'
                },
                'axisLabel': {
                    'showMinLabel': 'true',
                    'showMaxLabel': 'true',
                    'rotate': 15,
                    'fontSize': 9,
                    'interval': 0
                },
            },
            'series': []
        }

    def add_to_series(self, name, chart_type, x, y):
        self.config['series'].append({
            'emphasis': {
                'focus': 'self'
            },
            'name': name,
            'type': chart_type,
            'encode': {
                'x': x,
                'y': y
            },
            'showSymbol': 'true'
        })

        temp_pd_df = self.dataset[y]
        if self.ignore_yAxis_min_max is False:
            self.yAxis_values.append(temp_pd_df.min())
            self.yAxis_values.append(temp_pd_df.max())
        return self

    def build(self):
        if len(self.yAxis_values) > 0:
            min_value = min(self.yAxis_values)
            max_value = max(self.yAxis_values)
            self.config['yAxis']['min'] = get_floor_value(min_value) * self.yAxis_min_max_value_magnification_factor
            self.config['yAxis']['max'] = get_floor_value(max_value) * self.yAxis_min_max_value_magnification_factor

        return self


class ChartBuilder(object):
    def __init__(self, rows: int, columns: int):
        self.html_str = None

        self.layout_rows = rows
        self.layout_columns = columns
        self.chart_configs = []
        self.config = {
            'layout': {
                'rows': rows,
                'columns': columns
            },
            'charts': []
        }

    def build(self, grid_height=400):
        if len(self.chart_configs) == 0:
            raise ValueError('Please add chart.')
        for chart_config in self.chart_configs:
            self.config['charts'].append({
                'location': chart_config['location'],
                'config': chart_config['chart_config'].build().config
            })
        self.html_str = EmbeddedUI.get_resource_template('charts.html').render(data_json=self.config, height=grid_height)

    def add_chart(self, chart_config: ChartConfig, layout_location: tuple):
        if layout_location[0] in range(0, self.layout_rows) and layout_location[1] in range(0, self.layout_columns):
            pass
        else:
            raise ValueError('Illegall Arguments Error.')
        legend_names = []
        for i in range(0, len(chart_config.config['series'])):
            legend_names.append(chart_config.config['series'][i]['name'])
        chart_config.config['legend'] = {
            'type': 'scroll',
            'data': legend_names,
            'bottom': 10,
        }

        self.chart_configs.append({
            'location': [layout_location[0], layout_location[1]],
            'chart_config': chart_config
        })

    def generate_html(self, filename):
        if self.html_str is None:
            self.build()
        EmbeddedUI.generate_file('{}.html'.format(filename), self.html_str)

    def generate_notebook_iframe(self, iframe_height=600):
        if self.html_str is None:
            self.build()
        iframe_id = EmbeddedUI.get_uuid()
        iframe_str = EmbeddedUI.get_iframe_str(self.html_str, iframe_id, iframe_height)
        EmbeddedUI.render_fullscreen_button(iframe_id)
        EmbeddedUI.render_html_str(iframe_str)


def unify_min_max_value_of_yAxis(chart_configs):
    yAxis_values = []
    for chart_config in chart_configs:
        yAxis_values = yAxis_values + chart_config.yAxis_values
    for chart_config in chart_configs:
        chart_config.yAxis_values = yAxis_values
