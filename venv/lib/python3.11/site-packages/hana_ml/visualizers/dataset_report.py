"""
This module contains report builders for dataset.

The following class is available:

    * :class:`DatasetReportBuilder`
"""

# pylint: disable=invalid-name
# pylint: disable=missing-docstring
# pylint: disable=protected-access
# pylint: disable=no-self-use
# pylint: disable=too-many-instance-attributes
# pylint: disable=too-few-public-methods
import logging
import sys
import pandas as pd
from tqdm import tqdm
from hana_ml import dataframe
from hana_ml.algorithms.pal import stats
from hana_ml.algorithms.pal.preprocessing import Sampling
from hana_ml.algorithms.pal.utility import check_pal_function_exist
from hana_ml.visualizers.eda import EDAVisualizer
from hana_ml.visualizers.report_builder import ReportBuilder, Page, AlertItem, TableItem, ChartItem
from hana_ml.visualizers.shared import EmbeddedUI


logger = logging.getLogger(__name__)


class DatasetAnalyzer(object):
    def __init__(self, data: dataframe.DataFrame, key):
        self.start_time = EmbeddedUI.get_current_time()

        # step 1
        DatasetAnalyzer.check_input_parameters(data, key)
        self.input_df: dataframe.DataFrame = data
        self.key_dtype = None
        if key:
            key_data = data.select([key])
            variable_type = key_data.dtypes()[0][1]
            if variable_type in ['DATE', 'TIME', 'TIMESTAMP']:
                self.key_dtype = 'datetime'
            elif variable_type in ['NVARCHAR']:
                self.key_dtype = 'categorical'
            elif key_data.is_numeric(key):
                self.key_dtype = 'numeric'
            else:
                self.key_dtype = 'unknown'

        # step 2
        variable_data = data.deselect([key]) if key else data
        renamed_ID_column = DatasetAnalyzer.find_renamed_ID_column(variable_data)
        if renamed_ID_column:
            variable_data = variable_data.rename_columns({'ID': renamed_ID_column})
        self.variable_type: VariableType = VariableType()
        self.variable_type.init(variable_data)
        self.variable_df: dataframe.DataFrame = DatasetAnalyzer.cast_dtype(variable_data)
        self.variable_df_without_missing: dataframe.DataFrame = self.variable_df.dropna()
        self.variable_df_row_num = self.variable_df.count()

        # step 3
        self.variable_statistics: VariableStatistics = None
        self.variable_distribution: VariableDistribution = None
        self.variable_correlation: VariableCorrelation = None
        self.variable_scatter_matrix: VariableScatterMatrix = None

    def add_variable_statistics(self):
        self.variable_statistics = VariableStatistics()
        self.variable_statistics.init(self.variable_df)

    def add_variable_distribution(self, variable_bins_dict):
        self.variable_distribution = VariableDistribution()
        self.variable_distribution.init(self.variable_df, self.variable_type, variable_bins_dict)

    def add_variable_correlation(self):
        self.variable_correlation = VariableCorrelation()
        self.variable_correlation.init(self.variable_df_without_missing, self.variable_type.numeric_variables)

    def add_variable_scatter_matrix(self, sampling):
        self.variable_scatter_matrix = VariableScatterMatrix()
        if len(self.variable_type.numeric_variables) > 0:
            new_variable_df_without_missing = DatasetAnalyzer.do_sampling(self.variable_df_without_missing, sampling)
            self.variable_scatter_matrix.init(new_variable_df_without_missing, self.variable_type.numeric_variables)

    @staticmethod
    def find_renamed_ID_column(variable_df: dataframe.DataFrame):
        # When the name 'ID' already exists in the analyzed variables
        renamed_ID_column = 'ID'
        index = 1
        while True:
            if renamed_ID_column in variable_df.columns:
                renamed_ID_column = 'ID_{}'.format(index)
                index = index + 1
            else:
                break
        return renamed_ID_column if renamed_ID_column != 'ID' else None

    @staticmethod
    def check_input_parameters(data, key):
        if data:
            if isinstance(data, dataframe.DataFrame):
                if data.empty():
                    raise ValueError("The parameter 'data' value is empty.")
            else:
                raise TypeError("The parameter 'data' type is not hana_ml.dataframe.DataFrame.")
        else:
            raise ValueError("The parameter 'data' value is none.")

        if key and key not in data.columns:
            raise ValueError("The parameter 'key' value is invalid.")

    @staticmethod
    def do_sampling(df: dataframe.DataFrame, sampling: Sampling):
        new_df = df
        default_sampling_num = 200
        if sampling is None:
            if df.count() >= default_sampling_num:
                logger.info("Too many data points. Apply the default sampling method to reduce the data points.")
                sampling = Sampling('simple_random_without_replacement', sampling_size=default_sampling_num)
        else:
            logger.info("Using input sampling method.")
        if sampling:
            new_df = sampling.fit_transform(data=df)
        return new_df

    @staticmethod
    def cast_dtype(df: dataframe.DataFrame):
        to_cast_dict = {}
        for variable_name, variable_type, *rest in df.dtypes():
            if df.is_numeric(variable_name):
                if variable_type in ['BIGINT', 'DECIMAL']:
                    to_cast_dict[variable_name] = 'DOUBLE'
                    logger.warning('The variable %s has been cast from %s to DOUBLE.', variable_name, variable_type)
            elif variable_type not in ['NVARCHAR']:
                to_cast_dict[variable_name] = 'NVARCHAR(5000)'
                logger.warning('The variable %s has been cast from %s to NVARCHAR.', variable_name, variable_type)

        if to_cast_dict:
            return df.cast(to_cast_dict)
        else:
            return df


class VariableType(object):
    def __init__(self):
        self.numeric_variables = []
        self.categorical_variables = []
        self.datetime_variables = []
        self.unknown_type_variables = []

    def init(self, variable_df: dataframe.DataFrame):
        for variable_name, variable_type, *rest in variable_df.dtypes():
            if variable_type in ['DATE', 'TIME', 'TIMESTAMP']:
                self.datetime_variables.append(variable_name)
            elif variable_type in ['NVARCHAR']:
                self.categorical_variables.append(variable_name)
            elif variable_df.is_numeric(variable_name):
                self.numeric_variables.append(variable_name)
            else:
                self.unknown_type_variables.append(variable_name)


class VariableStatistics(object):
    stats_fullname_dict = {
        'count': 'Number of rows',
        'unique': 'Number of distinct values',
        'nulls': 'Number of nulls',
        'mean': 'Average',
        'std': 'Standard deviation',
        'median': 'Median',
        'min': 'Minimum value',
        'max': 'Maximum value',
        '25_percent_cont': '25% percentile when treated as continuous variable',
        '25_percent_disc': '25% percentile when treated as discrete variable',
        '50_percent_cont': '50% percentile when treated as continuous variable',
        '50_percent_disc': '50% percentile when treated as discrete variable',
        '75_percent_cont': '75% percentile when treated as continuous variable',
        '75_percent_disc': '75% percentile when treated as discrete variable',
        'CI for mean, lower bound': 'Confidence interval for mean (lower bound)',
        'CI for mean, upper bound': 'Confidence interval for mean (upper bound)',
        'kurtosis': 'kurtosis',
        'lower quartile': 'the 25% percentile',
        'upper quartile': 'the 75% percentile',
        'standard deviation': 'Standard deviation',
        'skewness': 'skewness',
        'trimmed mean': 'the average of the data after removing 5% at both head and tail',
        'valid observations': 'Number of valid rows',
        'variance': 'Variance'
    }

    def __init__(self):
        self.variable_to_stats_pairs_dict = None  # str -> [[key1, value1], [key2, value2]...]
        self.high_cardinality_pairs = None  # [[key1, value1], [key2, value2]...]
        self.highly_skewed_pairs = None  # [[key1, value1], [key2, value2]...]

    def init(self, df: dataframe.DataFrame):
        stats_pd_df: pd.DataFrame = df.stats
        stats_column_names = stats_pd_df.columns
        stats_tbl_id = 'VARIABLE_NAME'
        variable_names = list(stats_pd_df[stats_tbl_id])
        variable_to_stats_pairs_dict = {}
        for variable_name in variable_names:
            variable_to_stats_pairs_dict[variable_name] = []
        for stats_name in stats_column_names:
            if stats_name == stats_tbl_id:
                continue
            stats_values = list(stats_pd_df[stats_name])
            for i in range(0, len(stats_values)):
                stats_value = str(stats_values[i])
                if stats_value == 'nan':
                    stats_value = 'NaN'
                variable_to_stats_pairs_dict[variable_names[i]].append([VariableStatistics.stats_fullname_dict[stats_name], stats_value])
        self.variable_to_stats_pairs_dict = variable_to_stats_pairs_dict

        def get_pairs(keys, values):
            key_value_list = []
            for i in range(0, len(keys)):
                key_value_list.append([keys[i], values[i]])
            return key_value_list

        target_column_name = 'unique'
        temp_high_cardinality_pairs = []
        if target_column_name in stats_column_names:
            temp_pd_df: pd.DataFrame = stats_pd_df[[stats_tbl_id, target_column_name]].dropna()
            temp_pd_df = temp_pd_df.sort_values(target_column_name, ascending=False)
            temp_high_cardinality_pairs = get_pairs(list(temp_pd_df[stats_tbl_id]), list(temp_pd_df[target_column_name]))
        self.high_cardinality_pairs = temp_high_cardinality_pairs

        target_column_name = "skewness"
        temp_highly_skewed_pairs = []
        if target_column_name in stats_column_names:
            temp_pd_df: pd.DataFrame = stats_pd_df[[stats_tbl_id, target_column_name]].dropna()
            temp_pd_df = temp_pd_df.reindex(temp_pd_df[target_column_name].abs().sort_values(ascending=False).index)
            temp_highly_skewed_pairs = get_pairs(list(temp_pd_df[stats_tbl_id]), list(temp_pd_df[target_column_name]))
        self.highly_skewed_pairs = temp_highly_skewed_pairs


class VariableDistribution(object):
    def __init__(self):
        self.variable_distribution_dict = None  # str -> [x_data, y_data]

    def init(self, df: dataframe.DataFrame, variable_type: VariableType, variable_bins_dict):
        variable_distribution_dict = {}
        for variable_name in df.columns:
            x_data = []
            y_data = []
            if variable_name in variable_type.numeric_variables:
                data = VariableDistribution.get_numeric_distribution(df, variable_name, variable_bins_dict)
                if data is not None:
                    x_data = list(data['BANDING'])
                    y_data = list(data['COUNT'])
            else:
                data = df.agg([('count', variable_name, 'COUNT')], group_by=variable_name).sort('COUNT').collect()
                x_data = list(data[variable_name].astype(str))
                y_data = list(data['COUNT'])
            variable_distribution_dict[variable_name] = [x_data, y_data]
        self.variable_distribution_dict = variable_distribution_dict

    @staticmethod
    def get_numeric_distribution(df: dataframe.DataFrame, variable_name, variable_bins_dict):
        def get_variable_bins(variable_name):
            bins = 20
            if variable_bins_dict is not None:
                if variable_name in variable_bins_dict:
                    bins = variable_bins_dict[variable_name]
            if bins > 1:
                bins = bins - 1
            return bins

        return_data = None
        temp_data = df.dropna(subset=[variable_name])
        if temp_data.count() > 0:
            return_data = EDAVisualizer(no_fig=True, enable_plotly=False).distribution_plot(data=temp_data, column=variable_name, bins=get_variable_bins(variable_name), return_bin_data_only=True)
        return return_data


class VariableCorrelation(object):
    def __init__(self):
        self.numeric_variables = None  # []
        self.x_y_z_list = None  # [[variablex_index, variabley_index, correlation_value], ...]
        self.significant_correlation_tips = None  # []

    def init(self, df: dataframe.DataFrame, numeric_variables):
        if len(numeric_variables) >= 2:
            using_pal_function = check_pal_function_exist(df.connection_context, '%MULTIPLE_CORRELATION%', like=True)
            correlation_matrix_df: dataframe.DataFrame = None
            if using_pal_function:
                correlation_matrix_df = stats._correlation_matrix(data=df.add_id('ID'), key='ID', cols=numeric_variables).deselect(["LAG", "CV", "PACF"])  # must have an ID column
            else:
                correlation_matrix_df = stats.pearsonr_matrix(data=df, cols=numeric_variables)
            self.numeric_variables = numeric_variables
            self.generate_core_data(correlation_matrix_df, using_pal_function)

    def generate_core_data(self, correlation_matrix_df: dataframe.DataFrame, using_pal_function: bool):
        x_y_z_list = []
        significant_correlation_tip_dict = {}

        def parse(variable_x, variable_y, correlation_value):
            i = self.numeric_variables.index(variable_x)
            j = self.numeric_variables.index(variable_y)
            if using_pal_function:
                x_y_z_list.append([i, j, correlation_value])
                x_y_z_list.append([j, i, correlation_value])
            else:
                x_y_z_list.append([i, j, correlation_value])

            if i != j:
                temp_value = abs(correlation_value)
                text = None
                if 0.3 <= temp_value < 0.5:
                    text = "-  {} and {} are moderately correlated, p = {}"
                elif temp_value >= 0.5:
                    text = "-  {} and {} are highly correlated, p = {}"
                if text:
                    # Remove duplicate, eg:
                    # x1 and y1 are moderately correlated, p = 0.47
                    # y1 and x1 are moderately correlated, p = 0.47
                    significant_correlation_tip_dict[str(i + j)] = text.format(variable_x, variable_y, correlation_value)

        correlation_matrix_pd_df: pd.DataFrame = correlation_matrix_df.collect()
        if using_pal_function:
            variable_x_list = correlation_matrix_pd_df['PAIR_LEFT']
            variable_y_list = correlation_matrix_pd_df['PAIR_RIGHT']
            correlation_value_list = correlation_matrix_pd_df['CF']
            for k in range(0, len(variable_x_list)):
                parse(variable_x_list[k], variable_y_list[k], round(correlation_value_list[k], 2))
            for i in range(0, len(self.numeric_variables)):
                x_y_z_list.append([i, i, 1.0])
        else:
            id_values = list(correlation_matrix_pd_df['ID'])
            for y_name in id_values:
                x_y_values = list(correlation_matrix_pd_df[y_name])
                for k in range(0, len(x_y_values)):
                    parse(id_values[k], y_name, round(x_y_values[k], 2))

        significant_correlation_tips = list(significant_correlation_tip_dict.values())
        text = "There are {} pair(s) of variables that are show significant correlation{}"
        if len(significant_correlation_tips) == 0:
            text = text.format(0, '.')
        else:
            text = text.format(len(significant_correlation_tips), ':')
        significant_correlation_tips.insert(0, text)

        self.x_y_z_list = x_y_z_list
        self.significant_correlation_tips = significant_correlation_tips


class VariableScatterMatrix(object):
    def __init__(self):
        self.variable_values_dict = None  # str -> []

    def init(self, df: dataframe.DataFrame, numeric_variables):
        numeric_variable_pd_df: pd.DataFrame = df.select(numeric_variables).collect()
        self.variable_values_dict = {}  # str -> []
        for variable_name in numeric_variables:
            self.variable_values_dict[variable_name] = list(numeric_variable_pd_df[variable_name])


class HTMLBuilderUtils(object):
    @staticmethod
    def convert_pandas_to_html(pd_df: pd.DataFrame):
        return pd_df.to_html()\
            .replace('\n', '').replace('  ', '')\
            .replace(' class="dataframe"', 'class="table table-bordered table-hover"')\
            .replace('border="1"', '')\
            .replace(' style="text-align: right;"', '')\
            .replace('<th></th>', '<th style="width: 10px">#</th>')\
            .replace('</thead><tbody>', '')\
            .replace('<thead>', '<tbody>')

    @staticmethod
    def get_dataset_info(dataset_analyzer: DatasetAnalyzer):
        dataset_info = []
        dataset_info.append(['Dataset name', dataset_analyzer.variable_df.name])
        dataset_info.append(['Number of rows', dataset_analyzer.variable_df_row_num])
        dataset_info.append(['Number of columns', len(dataset_analyzer.variable_df.columns) + (1 if dataset_analyzer.key_dtype else 0)])
        dataset_info.append(['Number of numerical', len(dataset_analyzer.variable_type.numeric_variables) + (1 if dataset_analyzer.key_dtype == 'numeric' else 0)])
        dataset_info.append(['Number of categorical', len(dataset_analyzer.variable_type.categorical_variables) + (1 if dataset_analyzer.key_dtype == 'categorical' else 0)])
        dataset_info.append(['Number of datetime', len(dataset_analyzer.variable_type.datetime_variables) + (1 if dataset_analyzer.key_dtype == 'datetime' else 0)])
        dataset_info.append(['Number of other types', len(dataset_analyzer.variable_type.unknown_type_variables) + (1 if dataset_analyzer.key_dtype == 'unknown' else 0)])
        dataset_info.append(['Missing rows(%)', round((dataset_analyzer.variable_df_row_num - dataset_analyzer.variable_df_without_missing.count()) / dataset_analyzer.variable_df_row_num * 100, 1)])
        return dataset_info

    @staticmethod
    def get_variable_correlation_chart_option(variable_correlation: VariableCorrelation):
        chart_option = {
            'id': EmbeddedUI.get_uuid(),
            'variable_names': [],
            'data': ''
        }
        if variable_correlation and variable_correlation.x_y_z_list:
            chart_option['variable_names'] = variable_correlation.numeric_variables
            chart_option['z_min'] = -1
            chart_option['z_max'] = 1
            chart_option['type'] = 'heatmap'
            chart_option['data'] = variable_correlation.x_y_z_list
        return chart_option

    @staticmethod
    def get_warning_variable_correlation_tips(variable_correlation: VariableCorrelation):
        if variable_correlation and variable_correlation.significant_correlation_tips:
            return variable_correlation.significant_correlation_tips
        else:
            return []

    @staticmethod
    def get_high_cardinality_pairs_chart_option(variable_statistics: VariableStatistics):
        key_value_pair_list = variable_statistics.high_cardinality_pairs
        x_data = []
        y_data = []
        for pair in key_value_pair_list:
            x_data.append(pair[0])
            y_data.append(pair[1])
        return {
            'id': EmbeddedUI.get_uuid(),
            'title': '',
            'type': 'horizontalBar',
            'x_data': x_data,
            'y_data': y_data
        }

    @staticmethod
    def get_highly_skewed_pairs_chart_option(variable_statistics: VariableStatistics):
        key_value_pair_list = variable_statistics.highly_skewed_pairs
        x_data = []
        y_data = []
        for pair in key_value_pair_list:
            x_data.append(pair[0])
            y_data.append(pair[1])
        return {
            'id': EmbeddedUI.get_uuid(),
            'title': '',
            'type': 'horizontalBar',
            'x_data': x_data,
            'y_data': y_data
        }

    @staticmethod
    def get_variable_type_chart_option(variable_type: VariableType):
        return {
            'id': EmbeddedUI.get_uuid(),
            'height': 200,
            'title': '\n',
            'type': 'doughnut',
            'x_data': ['Numeric', 'Categorical', 'Datetime', 'Other Types'],
            'y_data': [
                len(variable_type.numeric_variables),
                len(variable_type.categorical_variables),
                len(variable_type.datetime_variables),
                len(variable_type.unknown_type_variables)
            ],
            'showLegend': 'true'
        }

    @staticmethod
    def get_variable_scatter_matrix_chart_options(dataset_analyzer: DatasetAnalyzer):
        chart_options = []
        numeric_variables = dataset_analyzer.variable_type.numeric_variables
        variable_values_dict = dataset_analyzer.variable_scatter_matrix.variable_values_dict
        variable_distribution_dict = dataset_analyzer.variable_distribution.variable_distribution_dict
        for variable_x in numeric_variables:
            x_to_y_chart_options = []
            for variable_y in numeric_variables:
                if variable_x == variable_y:
                    x_data, y_data = variable_distribution_dict.get(variable_x)
                    x_to_y_chart_options.append({
                        'type': 'hist',
                        'x_name': variable_x,
                        'y_name': '',
                        'data': [list(x) for x in zip(x_data, y_data)]  # [[x1, x2, x3...], [y1, y2, y3...]] -> [[x1, y1], [x2, y2], [x3, y3]...]
                    })
                else:
                    x_data, y_data = [variable_values_dict[variable_x], variable_values_dict[variable_y]]
                    x_to_y_chart_options.append({
                        'type': 'scatter',
                        'x_name': variable_x,
                        'y_name': variable_y,
                        'data': [list(x) for x in zip(x_data, y_data)]
                    })
            chart_options.append(x_to_y_chart_options)
        return chart_options


class HTMLBuilderV1(HTMLBuilderUtils):
    def __init__(self):
        self.html = None

    def get_overview_page_config(self, dataset_analyzer: DatasetAnalyzer):
        return {
            'title': 'Overview',
            'id': EmbeddedUI.get_uuid(),
            'dataset_info': {
                'title': 'Dataset Info',
                'pairs': self.get_dataset_info(dataset_analyzer)
            },
            'variable_types': {
                'title': 'Variable Type Distribution',
                'chart': self.get_variable_type_chart_option(dataset_analyzer.variable_type)
            },
            'high_cardinality_variables': {
                'title': 'High Cardinality Variables',
                'chart': self.get_high_cardinality_pairs_chart_option(dataset_analyzer.variable_statistics)
            },
            'highly_skewed_variables': {
                'title': 'Highly Skewed Variables',
                'chart': self.get_highly_skewed_pairs_chart_option(dataset_analyzer.variable_statistics)
            }
        }

    def get_sample_page_config(self, dataset_analyzer: DatasetAnalyzer):
        return {
            'title': 'Sample',
            'html': self.convert_pandas_to_html(dataset_analyzer.input_df.head(10).collect()),
            'id': EmbeddedUI.get_uuid()
        }

    def get_variable_distribution_page_config(self, dataset_analyzer: DatasetAnalyzer):
        info = []
        config = {
            'id': EmbeddedUI.get_uuid(),
            'title': 'Variable Distribution'
        }
        for variable_name in dataset_analyzer.variable_df.columns:
            x_data, y_data = dataset_analyzer.variable_distribution.variable_distribution_dict.get(variable_name)

            chart_type = 'bar'
            chart_height = 500
            if variable_name in dataset_analyzer.variable_type.categorical_variables:
                total_count = 0
                for count in y_data:
                    total_count = total_count + count

                if dataset_analyzer.variable_df_row_num != total_count:
                    x_data.append('NaN')
                    y_data.append(dataset_analyzer.variable_df_row_num - total_count)

                chart_type = 'doughnut'
                count_temp = len(x_data)
                if count_temp <= 15:
                    chart_height = 300
                else:
                    chart_height = 50 * (count_temp / 8 + 1) + 100

            variable_id = EmbeddedUI.get_uuid()
            chart_id = '{}_chart'.format(variable_id)

            info.append({
                'name': variable_name,
                'variable_id': variable_id,
                'chart_id': chart_id
            })
            chart_config = {
                'pairs': dataset_analyzer.variable_statistics.variable_to_stats_pairs_dict.get(variable_name),
                'chart': {
                    'id': chart_id,
                    'title': 'Distribution of {}'.format(variable_name),
                    'height': chart_height,
                    'type': chart_type,
                    'x_data': x_data,
                    'y_data': y_data
                }
            }
            config[variable_id] = chart_config

        config['info'] = info
        return config

    def get_variable_correlation_page_config(self, dataset_analyzer: DatasetAnalyzer):
        return {
            'id': EmbeddedUI.get_uuid(),
            'title': 'Variable Correlation',
            'chart': HTMLBuilderV1.get_variable_correlation_chart_option(dataset_analyzer.variable_correlation),
            'list': HTMLBuilderV1.get_warning_variable_correlation_tips(dataset_analyzer.variable_correlation)
        }

    def get_scatter_matrix_page_config(self, dataset_analyzer: DatasetAnalyzer):
        chart_options = ''
        if dataset_analyzer.variable_scatter_matrix:
            chart_options = self.get_variable_scatter_matrix_chart_options(dataset_analyzer)
        return {
            'id': EmbeddedUI.get_uuid(),
            'title': 'Variable Scatter Matrix',
            'chart': chart_options
        }

    def build(self, dataset_analyzer: DatasetAnalyzer):
        self.html = EmbeddedUI.get_resource_template('dataset_report.html').render(template_config={
            'start_time': 'Created Time: {}'.format(dataset_analyzer.start_time),
            'overview_page': self.get_overview_page_config(dataset_analyzer),
            'sample_page': self.get_sample_page_config(dataset_analyzer),
            'variable_distribution_page': self.get_variable_distribution_page_config(dataset_analyzer),
            'variable_correlation_page': self.get_variable_correlation_page_config(dataset_analyzer),
            'scatter_matrix_page': self.get_scatter_matrix_page_config(dataset_analyzer)
        })


class HTMLBuilderV2(HTMLBuilderUtils):
    def __init__(self):
        self.html = None

    def get_overview_page(self, dataset_analyzer: DatasetAnalyzer) -> Page:
        page = Page('Overview')

        item = TableItem('Dataset Info')
        dataset_info = self.get_dataset_info(dataset_analyzer)
        item.addColumn('STAT NAME', list(map(lambda pair: pair[0], dataset_info)))
        item.addColumn('STAT VALUE', list(map(lambda pair: pair[1], dataset_info)))
        page.addItem(item)

        x_data = ['Numerical', 'Categorical', 'Datetime', 'Other Types']
        y_data = [
            len(dataset_analyzer.variable_type.numeric_variables),
            len(dataset_analyzer.variable_type.categorical_variables),
            len(dataset_analyzer.variable_type.datetime_variables),
            len(dataset_analyzer.variable_type.unknown_type_variables)
        ]
        series_data = list(map(lambda item: {'name': item[0], 'value': item[1]}, [list(x) for x in zip(x_data, y_data)]))
        chart_config = {
            'grid': {
                'containLabel': 1
            },
            'legend': {
                'type': 'scroll',
                'orient': 'vertical',
                'right': 10,
                'top': 20,
                'bottom': 20,
                'data': x_data
            },
            'tooltip': {
                'trigger': 'item'
            },
            'series': [
                {
                    'name': '',
                    'type': 'pie',
                    'radius': ['40%', '70%'],  # 'radius': '55%',
                    'center': ['30%', '50%'],
                    'data': series_data,
                    'label': {
                        'show': 0,
                        'position': 'center'
                    },
                    'emphasis': {
                        'label': {
                            'show': 1,
                            'fontSize': 10,
                            'fontWeight': 'bold'
                        },
                        'itemStyle': {
                            'shadowBlur': 10,
                            'shadowOffsetX': 0,
                            'shadowColor': 'rgba(0, 0, 0, 0.5)'
                        }
                    }
                }
            ]
        }
        item = ChartItem(title='Variable Type Distribution', config=chart_config)
        page.addItem(item)

        pairs = dataset_analyzer.variable_statistics.high_cardinality_pairs
        x_data = list(map(lambda pair: pair[0], pairs))
        y_data = list(map(lambda pair: pair[1], pairs))
        chart_config = {
            'grid': {
                'containLabel': 1
            },
            'tooltip': {
                'trigger': 'axis',
                'axisPointer': {
                    'type': 'shadow'
                }
            },
            'xAxis': {
                'type': 'value',
            },
            'yAxis': {
                'type': 'category',
                'data': x_data,
                'inverse': 1,
                'axisLabel': {
                    'interval': 0
                }
            },
            'series': [
                {
                    'type': 'bar',
                    'data': y_data,
                    'color': '#108ee9'
                }
            ]
        }
        item = ChartItem(title='High Cardinality Variables', config=chart_config, height=len(x_data) * 30)
        page.addItem(item)

        pairs = dataset_analyzer.variable_statistics.highly_skewed_pairs
        x_data = list(map(lambda pair: pair[0], pairs))
        y_data = list(map(lambda pair: pair[1], pairs))
        chart_config = {
            'grid': {
                'containLabel': 1
            },
            'tooltip': {
                'trigger': 'axis',
                'axisPointer': {
                    'type': 'shadow'
                }
            },
            'xAxis': {
                'type': 'value',
                'axisLine': {
                    'show': 1
                }
            },
            'yAxis': {
                'type': 'category',
                'data': x_data,
                'inverse': 1,
                'axisLabel': {
                    'interval': 0
                },
                'axisLine': {
                    'show': 1
                }
            },
            'series': [
                {
                    'type': 'bar',
                    'data': y_data,
                    'color': '#108ee9'
                }
            ]
        }
        item = ChartItem(title='Highly Skewed Variables', config=chart_config, height=len(x_data) * 30)
        page.addItem(item)

        return page

    def get_sample_page(self, dataset_analyzer: DatasetAnalyzer) -> Page:
        page = Page('Sample')

        item = TableItem('Sampling')
        temp_pd_df: pd.DataFrame = dataset_analyzer.input_df.head(10).collect()
        for variable in temp_pd_df.columns:
            item.addColumn(variable, list(temp_pd_df[variable].astype(str)))

        page.addItem(item)
        return page

    def get_variable_distribution_page(self, dataset_analyzer: DatasetAnalyzer) -> Page:
        page = Page('Variable Distribution & Statistics')
        for variable in dataset_analyzer.variable_df.columns:
            x_data, y_data = dataset_analyzer.variable_distribution.variable_distribution_dict.get(variable)

            chart_config = None
            if variable in dataset_analyzer.variable_type.categorical_variables:
                total_count = 0
                for count in y_data:
                    total_count = total_count + count
                if dataset_analyzer.variable_df_row_num != total_count:
                    x_data.append('NaN')
                    y_data.append(dataset_analyzer.variable_df_row_num - total_count)

                series_data = list(map(lambda item: {'name': item[0], 'value': item[1]}, [list(x) for x in zip(x_data, y_data)]))
                chart_config = {
                    'grid': {
                        'containLabel': 1
                    },
                    'legend': {
                        'type': 'scroll',
                        'orient': 'vertical',
                        'right': 10,
                        'top': 20,
                        'bottom': 20,
                        'data': x_data
                    },
                    'tooltip': {
                        'trigger': 'item'
                    },
                    'series': [
                        {
                            'name': '',
                            'type': 'pie',
                            'radius': ['40%', '70%'],  # 'radius': '55%',
                            'center': ['30%', '50%'],
                            'data': series_data,
                            'label': {
                                'show': 0,
                                'position': 'center'
                            },
                            'emphasis': {
                                'label': {
                                    'show': 1,
                                    'fontSize': 10,
                                    'fontWeight': 'bold'
                                },
                                'itemStyle': {
                                    'shadowBlur': 10,
                                    'shadowOffsetX': 0,
                                    'shadowColor': 'rgba(0, 0, 0, 0.5)'
                                }
                            }
                        }
                    ]
                }
            elif variable in dataset_analyzer.variable_type.numeric_variables:
                chart_config = {
                    'grid': {
                        'containLabel': 1
                    },
                    'tooltip': {
                        'trigger': 'axis',
                        'axisPointer': {
                            'type': 'shadow'
                        }
                    },
                    'xAxis': {
                        'type': 'category',
                        'data': x_data,
                        'axisLabel': {
                            'interval': 0,
                            'rotate': 45,
                        }
                    },
                    'yAxis': {
                        'type': 'value'
                    },
                    'series': [
                        {
                            'type': 'bar',
                            'data': y_data,
                            'color': '#108ee9'
                        }
                    ]
                }
            item = ChartItem('Distribution of {}'.format(variable), config=chart_config)
            page.addItem(item)

            item = TableItem('Statistics of {}'.format(variable))
            pairs = dataset_analyzer.variable_statistics.variable_to_stats_pairs_dict.get(variable)
            item.addColumn('STAT NAME', list(map(lambda pair: pair[0], pairs)))
            item.addColumn('STAT VALUE', list(map(lambda pair: pair[1], pairs)))
            page.addItem(item)

        return page

    def get_variable_correlation_page(self, dataset_analyzer: DatasetAnalyzer) -> Page:
        title = 'Variable Correlation'
        page = Page(title)

        variable_correlation = dataset_analyzer.variable_correlation
        if variable_correlation:
            if variable_correlation.x_y_z_list:
                chart_config = {
                    'customFn': ['tooltip.formatter'],
                    'tooltip': {
                        'position': 'bottom',
                        'formatter': {
                            'params': ['p'],
                            'body': ''.join([
                                "const br='<br />';",
                                "const correlation_names=" + str(variable_correlation.numeric_variables) + ";",
                                "return 'Variable Correlation' + br + 'x: ' + correlation_names[p.data[0]] + br + 'y: ' + correlation_names[p.data[1]] + br + 'value: ' + p.data[2];"
                            ])
                        }
                    },
                    'grid': {
                        'containLabel': 1
                    },
                    'xAxis': {
                        'type': 'category',
                        'data': variable_correlation.numeric_variables,
                        'name': '',
                        'nameLocation': 'center',
                        'splitArea': {
                            'show': 1
                        },
                        'axisLabel': {
                            'rotate': 45,
                            'interval': 0
                        }
                    },
                    'yAxis': {
                        'type': 'category',
                        'data': variable_correlation.numeric_variables,
                        'name': '',
                        'nameLocation': 'center',
                        'splitArea': {
                            'show': 1
                        },
                        'axisLabel': {
                            'interval': 0
                        }
                    },
                    'visualMap': [{
                        'min': -1,
                        'max': 1,
                        'left': 'right',
                        'top': 'center',
                        'calculable': 1,
                        'realtime': 0,
                        'splitNumber': 8,
                        'inRange': {
                            'color': ['#a50026', '#d73027', '#f46d43', '#fdae61', '#fee090', '#ffffbf', '#e0f3f8', '#abd9e9', '#74add1', '#4575b4', '#313695']
                        }
                    }],
                    'series': [{
                        'name': 'Variable Correlation',
                        'type': 'heatmap',
                        'data': variable_correlation.x_y_z_list,
                        'label': {
                            'show': 1,
                            'color': 'black'
                        },
                        'emphasis': {
                            'itemStyle': {
                                'opacity': 0.5,
                                'shadowBlur': 10,
                                'shadowColor': 'rgba(0, 0, 0, 0.5)'
                            }
                        }
                    }]
                }
                significant_correlation_tips = variable_correlation.significant_correlation_tips
                if significant_correlation_tips:
                    chart_config['tips'] = significant_correlation_tips

                size = len(variable_correlation.numeric_variables) * 50 + 200
                if len(variable_correlation.numeric_variables) > 10:
                    page.addItem(ChartItem(title, config=chart_config, width=size, height=size))
                else:
                    page.addItem(ChartItem(title, config=chart_config, height=size))
            else:
                item = AlertItem(title)
                item.add_success_msg('The correlation plot is empty.')
                page.addItem(item)
        else:
            item = AlertItem(title)
            item.add_warning_msg('The correlation plot has been ignored.')
            page.addItem(item)

        return page

    def get_variable_scatter_matrix_page(self, dataset_analyzer: DatasetAnalyzer) -> Page:
        def generate_chart_config(chart_options):
            CATEGORY_DIM_COUNT = len(chart_options)
            GAP, BASE_LEFT, BASE_TOP = 3, 5, 5
            GRID_WIDTH = (100 - BASE_LEFT - GAP) / CATEGORY_DIM_COUNT - GAP
            GRID_HEIGHT = (100 - BASE_TOP - GAP) / CATEGORY_DIM_COUNT - GAP
            grid, xAxis, yAxis, series = [], [], [], []
            index = 0
            for i in range(0, CATEGORY_DIM_COUNT):
                for j in range(0, CATEGORY_DIM_COUNT):
                    grid.append({
                        'containLabel': 1,
                        'left': str(BASE_LEFT + i * (GRID_WIDTH + GAP)) + '%',
                        'top': str(BASE_TOP + j * (GRID_HEIGHT + GAP)) + '%',
                        'width': str(GRID_WIDTH) + '%',
                        'height': str(GRID_HEIGHT) + '%'
                    })
                    xAxis.append({
                        'splitNumber': 3,
                        'name': chart_options[i][j]['x_name'],
                        'nameLocation': 'center',
                        'nameTextStyle': {
                            'fontSize': 8
                        },
                        'nameGap': 5,
                        'position': 'bottom',
                        'axisLine': {
                            'show': 1,
                            'onZero': 0
                        },
                        'axisTick': {
                            'show': 0,
                            'inside': 0
                        },
                        'axisLabel': {
                            'show': 0,
                        },
                        'type': 'category' if i == j else 'value',
                        'gridIndex': index,
                        'scale': 1
                    })
                    yAxis.append({
                        'splitNumber': 3,
                        'name': chart_options[i][j]['y_name'],
                        'nameGap': 5,
                        'nameLocation': 'center',
                        'nameTextStyle': {
                            'fontSize': 8
                        },
                        'position': 'left',
                        'axisLine': {
                            'show': 1,
                            'onZero': 0
                        },
                        'axisTick': {
                            'show': 0,
                            'inside': 1
                        },
                        'axisLabel': {
                            'show': 0,
                        },
                        'type': 'value',
                        'gridIndex': index,
                        'scale': 1
                    })
                    series_option = {
                        'xAxisIndex': index,
                        'yAxisIndex': index,
                        'color': '#108ee9',
                        'data': chart_options[i][j]['data']
                    }
                    if i == j:
                        series_option['type'] = 'bar'
                    else:
                        series_option['type'] = 'scatter'
                        series_option['symbolSize'] = 4
                    series.append(series_option)

                    index = index + 1
            return {
                'animation': 0,
                'tooltip': {
                    'trigger': 'item'
                },
                'grid': grid,
                'xAxis': xAxis,
                'yAxis': yAxis,
                'series': series
            }

        title = 'Variable Scatter Matrix'
        page = Page(title)

        item = None
        if dataset_analyzer.variable_scatter_matrix:
            chart_options = self.get_variable_scatter_matrix_chart_options(dataset_analyzer)
            if chart_options:
                size = len(chart_options) * 100 + 400
                if len(chart_options) > 10:
                    item = ChartItem(title, config=generate_chart_config(chart_options), width=size, height=size)
                else:
                    item = ChartItem(title, config=generate_chart_config(chart_options), height=size)
            else:
                item = AlertItem(title)
                item.add_success_msg('The scatter matrix plot is empty.')
        else:
            item = AlertItem(title)
            item.add_warning_msg('The scatter matrix plot has been ignored.')

        page.addItem(item)
        return page

    def build(self, dataset_analyzer: DatasetAnalyzer):
        report = ReportBuilder(title='Dataset Report')
        report.addPage(self.get_overview_page(dataset_analyzer))
        report.addPage(self.get_sample_page(dataset_analyzer))
        report.addPage(self.get_variable_distribution_page(dataset_analyzer))
        report.addPage(self.get_variable_correlation_page(dataset_analyzer))
        report.addPage(self.get_variable_scatter_matrix_page(dataset_analyzer))
        report.build()
        self.html = report.html_str


class DatasetReportBuilder(object):
    """
    The DatasetReportBuilder instance can analyze the dataset and generate a report in HTML format. \n
    The instance will call the dropna method of DataFrame internally to handle the missing value of dataset.

    The generated report can be embedded in a notebook, including: \n
    - Overview
        - Dataset info
        - Variable type distribution
        - High cardinality variables
        - Highly skewed variables
    - Sample
        - Top ten rows of dataset
    - Variable Distribution
        - Numeric variable distribution
        - Categorical variable distribution
        - Variable statistics
    - Variable Correlation
    - Variable Scatter Matrix


    Examples
    --------

    Create a DatasetReportBuilder instance:

    >>> from hana_ml.visualizers.dataset_report import DatasetReportBuilder
    >>> datasetReportBuilder = DatasetReportBuilder()

    Assume the dataset DataFrame is df and then analyze the dataset:

    >>> datasetReportBuilder.build(df, key="ID")

    Display the dataset report as a notebook iframe.

    >>> datasetReportBuilder.generate_notebook_iframe_report()

     .. image:: image/dataset_report_example.png

    Switch to v1 version of report.

    >>> datasetReportBuilder.set_framework_version('v1')

    Display the dataset report as a notebook iframe.

    >>> datasetReportBuilder.generate_notebook_iframe_report()

    """
    def __init__(self):
        self.dataset_analyzer: DatasetAnalyzer = None
        self.html_builder = None

    def build(self, data: dataframe.DataFrame, key=None, scatter_matrix_sampling: Sampling = None,
              ignore_scatter_matrix: bool = False, ignore_correlation: bool = False, subset_bins=None):
        """
        Build a report for dataset. By default, use the report builder framework(v2) to generate dataset report.

        Note that the name of data is used as the dataset name in this function.
        If the name of data (which is a dataframe.DataFrame object) is not set explicitly in the object instantiation,
        a name like 'DT_XX' will be assigned to the data.

        Parameters
        ----------
        data : DataFrame
            DataFrame to use to build the dataset report.
        key : str
            Name of ID column.
        scatter_matrix_sampling : :class:`~hana_ml.algorithms.pal.preprocessing.Sampling`, optional
            Scatter matrix sampling.
        ignore_scatter_matrix : bool, optional
            Skip calculating scatter matrix.

            Defaults to False.
        ignore_correlation : bool, optional
            Skip calculating correlation.

            Defaults to False.
        subset_bins : dict, optional
            Define the bin number in distribution chart for each column, e.g. {"col_A": 20}.
        """
        pbar = tqdm(total=5, desc="Analyzing dataset...", file=sys.stdout, position=0, leave=False)

        import numpy as np
        if float(np.version.version.split('.', maxsplit=1)[0]) >= 2:
            np.set_printoptions(legacy="1.25")
        self.dataset_analyzer = DatasetAnalyzer(data, key)
        pbar.update(1)

        self.dataset_analyzer.add_variable_statistics()
        pbar.update(1)

        self.dataset_analyzer.add_variable_distribution(subset_bins)
        pbar.update(1)

        if ignore_correlation is False:
            self.dataset_analyzer.add_variable_correlation()
        pbar.update(1)

        if ignore_scatter_matrix is False:
            self.dataset_analyzer.add_variable_scatter_matrix(scatter_matrix_sampling)
        pbar.update(1)
        pbar.close()

        if self.html_builder is None:
            self.html_builder = HTMLBuilderV2()
        self.html_builder.build(self.dataset_analyzer)

    def set_framework_version(self, framework_version):
        """
        Switch v1/v2 version of report.

        Parameters
        ----------
        framework_version : {'v2', 'v1'}
            v2: using report builder framework.
            v1: using pure html template.
        """
        if framework_version in ('v2', 'v1'):
            if framework_version == 'v2':
                self.html_builder = HTMLBuilderV2()
            else:
                self.html_builder = HTMLBuilderV1()
            if self.dataset_analyzer:
                self.html_builder.build(self.dataset_analyzer)
        else:
            raise Exception('The passed parameter[framework_version] value is not correct!')

    def get_report_html(self):
        """
        Return the html report.
        """
        if self.html_builder is None:
            raise Exception('To generate a report, you must call the build method firstly.')
        return self.html_builder.html

    def get_iframe_report_html(self, iframe_height: int = 800):
        """
        Return the iframe report.

        Parameters
        ----------
        iframe_height : int, optional
            Height of iframe.

            Defaults to 800.
        """
        return EmbeddedUI.get_iframe_str(self.get_report_html(), iframe_height=iframe_height)

    def generate_html_report(self, filename):
        """
        Save the dataset report as a html file.

        Parameters
        ----------
        filename : str
            Html file name.
        """
        EmbeddedUI.generate_file('{}.html'.format(filename), self.get_report_html())

    def generate_notebook_iframe_report(self, iframe_height: int = 800):
        """
        Render the dataset report as a notebook iframe.

        Parameters
        ----------
        iframe_height : int, optional
            Height of iframe.

            Defaults to 800.
        """
        EmbeddedUI.render_html_str(self.get_iframe_report_html(iframe_height))
