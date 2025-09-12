"""
This module represents an eda plotter. Matplotlib is used for all visualizations.

    * :class:`EDAVisualizer`
    * :class:`Profiler`
    * :func:`kdeplot`
    * :func:`hist`
    * :func:`quarter_plot`
    * :func:`seasonal_plot`
    * :func:`timeseries_box_plot`
    * :func:`plot_acf`
    * :func:`plot_pacf`
    * :func:`plot_time_series_outlier`
    * :func:`plot_change_points`
    * :func:`plot_moving_average`
    * :func:`plot_rolling_stddev`
    * :func:`plot_seasonal_decompose`
    * :func:`plot_psd`
    * :func:`bubble_plot`
    * :func:`parallel_coordinates`
"""
#pylint: disable=too-many-lines, no-else-return
#pylint: disable=superfluous-parens
#pylint: disable=too-many-arguments
#pylint: disable=line-too-long
#pylint: disable=unused-variable
#pylint: disable=deprecated-method
#pylint: disable=too-many-locals
#pylint: disable=too-many-statements
#pylint: disable=too-many-branches
#pylint: disable=invalid-name
#pylint: disable=unnecessary-comprehension
#pylint: disable=eval-used
#pylint: disable=super-with-arguments
#pylint: disable=too-many-nested-blocks
#pylint: disable=unbalanced-tuple-unpacking
#pylint: disable=raising-bad-type, consider-using-enumerate
#pylint: disable=bare-except, misplaced-bare-raise, use-dict-literal
import logging
import operator
import sys
import math
import uuid
import numpy as np
import pandas as pd
try:
    import matplotlib
    import matplotlib.pyplot as plt
    from matplotlib import ticker
    from matplotlib.offsetbox import AnchoredText
except:
    pass
from deprecated import deprecated

try:
    import plotly.graph_objs as go
    import plotly.express as px
    from plotly.subplots import make_subplots
except:
    pass
from hana_ml.dataframe import create_dataframe_from_pandas, quotename, DataFrame
from hana_ml.ml_exceptions import Error
from hana_ml.visualizers.visualizer_base import Visualizer
from hana_ml.algorithms.pal import stats, kernel_density
from hana_ml.algorithms.pal.tsa import correlation_function, seasonal_decompose
from hana_ml.algorithms.pal.preprocessing import Sampling
from hana_ml.algorithms.pal.tsa.outlier_detection import OutlierDetectionTS
from hana_ml.algorithms.pal.tsa.periodogram import periodogram
from hana_ml.algorithms.pal.utility import version_compare
from hana_ml.visualizers import eda_plotly
from hana_ml.algorithms.pal.tsa.changepoint import BCPD

from hana_ml.algorithms.pal.pal_base import (
    arg)
logger = logging.getLogger(__name__)

if sys.version_info.major == 2:
    #pylint: disable=undefined-variable
    _INTEGER_TYPES = (int, long)
    _STRING_TYPES = (str, unicode)
else:
    _INTEGER_TYPES = (int,)
    _STRING_TYPES = (str,)

def quarter_plot(data,
                 col,
                 key=None,
                 ax=None,
                 fig=None,
                 enable_plotly=True,
                 **kwargs):
    """
    Perform quarter plot to view the seasonality.

    Parameters
    ----------

    data : DataFrame
        HANA DataFrame containing the data.

    col : str
        Name of the time series data column.

    key : str, optional
        Name of the ID column.

        Defaults to the index column of ``data`` (i.e. data.index) if it is set.

    ax : matplotlib.axes.Axes, optional
        The axes for the plot.

        Default to None.

    enable_plotly : bool, optional
        Use plotly instead of matplotlib.

        Defaults to True.

    fig : plotly.graph_objects.Figure, optional
        If None, a new graph object will be created. Valid when ``enable_plotly`` is True.

        Defaults to None.

    kwargs : optional
        Keyword/value pair of properties to be updated when ``enable_plotly`` is True.

        Defaults to None.

    Returns
    -------
    matplotlib:

      If ``enable_plotly`` is False, returns a matplotlib.axes.Axes object.

    plotly:

      If ``enable_plotly`` is True, returns a plotly.graph_objects.Figure object.

    Examples
    --------
    Matplotlib:

    >>> quarter_plot(data=df, col="Y", key="ID", enable_plotly=False)

    .. image:: image/quarter_plot.png
       :align: center
       :scale: 60 %

    Plotly:

    >>> quarter_plot(data=df, col="Y", key="ID", width=600, height=400)

    .. image:: image/quarter_plotly.png
       :align: center
       :scale: 60 %

    """
    new_id = "NEWID_{}".format(str(uuid.uuid1()).replace("-", "_"))
    temp_tab = "#temp_tab_{}".format(str(uuid.uuid1()).replace("-", "_"))
    temp_df = data.select([key, col]).generate_feature(targets=[key], trans_func="QUARTER")
    temp_df = temp_df.split_column(temp_df.columns[2], '-', ["YEAR", "Q"]).add_id(new_id, ref_col=["Q", "YEAR", "ID"])
    years = temp_df.select("YEAR").distinct().collect().iloc[:,0]
    temp_df.save(temp_tab)
    temp_df = data.connection_context.table(temp_tab)

    my_pos = []
    quarter_min = []
    labels = ['Q1', 'Q2', 'Q3', 'Q4']
    for quarter, c in zip(labels, ['#1f77b4',  '#ff7f0e', '#2ca02c', '#9467bd']):
        for year in years:
            xx_filter = temp_df.filter("Q='{}' AND YEAR='{}'".format(quarter, year)).select([new_id, col])
            xx_filter_collect = xx_filter.collect()
            if enable_plotly:
                if not fig:
                    fig=go.Figure()
                fig.add_trace(go.Scatter(x=xx_filter_collect.iloc[:, 0].values,
                                         y=xx_filter_collect.iloc[:, 1].values,
                                         mode="lines", showlegend=False, marker=dict(size=10, color=c)))
            else:
                if ax is None:
                    ax = plt.axes()
                ax.plot(xx_filter_collect.iloc[:, 0].values,
                        xx_filter_collect.iloc[:, 1].values,
                        color=c)
        my_pos.append(temp_df.filter("Q='{}'".format(quarter)).select([new_id]).median())
        min_x = temp_df.filter("Q='{}'".format(quarter)).select([new_id]).min()
        max_x = temp_df.filter("Q='{}'".format(quarter)).select([new_id]).max()
        avg_q = temp_df.filter("Q='{}'".format(quarter)).select([col]).mean()
        quarter_min.append((min_x, max_x, avg_q))

    if enable_plotly:
        for hline in quarter_min:
            fig.add_shape(y0=hline[2], y1=hline[2], x0=hline[0], x1=hline[1], line=dict(color='red'))
        fig.update_layout(xaxis=dict(tickmode='array', tickvals=my_pos, ticktext=labels), **kwargs)
        data.connection_context.drop_table(temp_tab)
        return fig
    else:
        ax.xaxis.set_major_locator(ticker.FixedLocator(my_pos))
        ax.xaxis.set_major_formatter(ticker.FixedFormatter(labels))
        count = 1
        for hline in quarter_min:
            ax.hlines(y=hline[2], xmin=hline[0], xmax=hline[1], color="red")
            count += 1
        data.connection_context.drop_table(temp_tab)
        return ax

def seasonal_plot(data,
                  col,
                  key=None,
                  ax=None,
                  enable_plotly=True,
                  fig=None,
                  **kwargs):
    """
    Plot time series data by year.

    Parameters
    ----------

    data : DataFrame
        HANA DataFrame containing the data.

    col : str
        Name of the time series data column.

    key : str, optional
        Name of the ID column.

        Defaults to the index column of ``data`` (i.e. data.index) if it is set.

    ax : matplotlib.axes.Axes, optional
        The axes for the plot.

        Default to None.

    enable_plotly : bool, optional
        Use plotly instead of matplotlib.

        Defaults to True.

    fig : plotly.graph_objects.Figure, optional
        If None, a new graph object will be created. Valid when ``enable_plotly`` is True.

        Defaults to None.

    kwargs : optional
        Keyword/value pair of properties to be updated when ``enable_plotly`` is True.

        Defaults to None.


    Returns
    -------
    matplotlib:

      The axes for the plot, returns a matplotlib.axes.Axes object.

    plotly:

      If ``enable_plotly`` is True, returns a plotly.graph_objects.Figure object.

    Examples
    --------
    Matplotlib:

    >>> seasonal_plot(data=df, col="Y", key="ID", enable_plotly=False)

    .. image:: image/seasonal_plot.png
       :align: center
       :width: 400px

    Plotly:

    >>> seasonal_plot(data=df, col="Y", key="ID")

    .. image:: image/seasonal_plotly.png
       :align: center
       :width: 400px
    """
    if key is None:
        if data.index:
            key = data.index
        else:
            raise ValueError("Index should be set by key or use set_index function!")

    data_ = data.select([key, col]).generate_feature(targets=[key], trans_func="YEAR")
    temp_tab_name = "#timeseries_seasonal_plot_{}".format(str(uuid.uuid1()).replace('-', '_'))
    data_.save(temp_tab_name)
    data_ = data_.connection_context.table(temp_tab_name)

    lines_to_plot = data_.distinct(data_.columns[2]).collect()[data_.columns[2]].to_list()
    for line_to_plot in lines_to_plot:
        temp_df = data_.filter('"{}"={}'.format(data_.columns[2], line_to_plot))
        temp_df = temp_df.generate_feature(targets=[key], trans_func="MONTH")
        temp_df = temp_df.agg([('avg', col, 'MONTH_AVG')], group_by=temp_df.columns[3])
        temp_pf = temp_df.collect().sort_values(temp_df.columns[0])
        if enable_plotly:
            if not fig:
                fig=go.Figure()
            fig.add_trace(go.Scatter(x=temp_pf[temp_df.columns[0]].to_numpy(), y=temp_pf[temp_df.columns[1]].to_numpy(), mode="lines", name=line_to_plot))
            fig.update_layout(xaxis=dict(title="MONTH"), yaxis=dict(title="AVG({})".format(col)), **kwargs)
        else:
            if ax is None:
                ax = plt.axes()
            ax.plot(temp_pf[temp_df.columns[0]].to_numpy(), temp_pf[temp_df.columns[1]].to_numpy(), label=line_to_plot)

    if enable_plotly:
        data_.connection_context.drop_table(temp_tab_name)
        return fig

    handles, labels = ax.get_legend_handles_labels()
    hl = sorted(zip(handles, labels), key=operator.itemgetter(1))
    handles2, labels2 = zip(*hl)
    ax.legend(handles2, labels2)

    ax.set_xlabel("MONTH")
    ax.set_ylabel("AVG({})".format(col))
    data_.connection_context.drop_table(temp_tab_name)
    return ax

def timeseries_box_plot(data,
                        col,
                        key=None,
                        ax=None,
                        cycle="MONTH",
                        fig=None,
                        enable_plotly=True,
                        **kwargs):
    """
    Plot year-wise/month-wise box plot.

    Parameters
    ----------

    data : DataFrame
        HANA DataFrame containing the data.

    col : str
        Name of the time series data column.

    key : str, optional
        Name of the ID column.

        Defaults to the index column of ``data`` (i.e. data.index) if it is set.

    ax : matplotlib.axes.Axes, optional
        The axes for the plot.

        Default to None.

    cycle : {"YEAR", "QUARTER", "MONTH", "WEEK"}, optional
        It defines the x-axis for the box plot.

        Defaults to "MONTH".

    enable_plotly : bool, optional
        Use plotly instead of matplotlib.

        Defaults to True.

    fig : plotly.graph_objects.Figure, optional
        If None, a new graph object will be created. Valid when ``enable_plotly`` is True.

        Defaults to None.

    kwargs : optional
        Keyword/value pair of properties to be updated when ``enable_plotly`` is True.

        Defaults to None.

    Returns
    -------
    matplotlib:

      The axes for the plot, returns a matplotlib.axes.Axes object.

    plotly:

      If ``enable_plotly`` is True, returns a plotly.graph_objects.Figure object.

    Examples
    --------

    Example 1: cycle = 'YEAR'

    Matplotlib:

    >>> timeseries_box_plot(data=df, col="Y", key="ID", cycle="YEAR", enable_plotly=False)

    .. image:: image/ts_box_year_plot.png
       :align: center
       :width: 400px

    Plotly:

    >>> timeseries_box_plot(data=df, col="Y", key="ID", cycle="YEAR")

    .. image:: image/ts_box_year_plotly.png
       :align: center
       :width: 400px

    Example 2: cycle = 'QUARTER'

    Matplotlib:

    >>> timeseries_box_plot(data=df, col="Y", key="ID", cycle="QUARTER", enable_plotly=False)

    .. image:: image/ts_box_quarter_plot.png
       :align: center
       :width: 400px

    Plotly:

    >>> timeseries_box_plot(data=df, col="Y", key="ID", cycle="QUARTER")

    .. image:: image/ts_box_quarter_plotly.png
       :align: center
       :width: 400px


    Example 3: cycle = 'MONTH'

    Matplotlib:

    >>> timeseries_box_plot(data=df, col="Y", key="ID", cycle="MONTH", enable_plotly=False)

    .. image:: image/ts_box_month_plot.png
       :align: center
       :width: 400px

    Plotly:

    >>> timeseries_box_plot(data=df, col="Y", key="ID", cycle="MONTH")

    .. image:: image/ts_box_month_plotly.png
       :align: center
       :width: 400px

    """
    if key is None:
        if data.index:
            key = data.index
        else:
            raise ValueError("Index should be set by key or use set_index function!")

    data_ = data.select([key, col]).generate_feature(targets=[key], trans_func=cycle)
    if cycle != "QUARTER":
        data_ = data_.cast({data_.columns[2]: "INT"})
    temp_tab_name = "#timeseries_box_plot_{}".format(str(uuid.uuid1()).replace('-', '_'))
    data_.save(temp_tab_name)
    data_ = data_.connection_context.table(temp_tab_name)
    if enable_plotly:
        fig, _ = eda_plotly.box_plot(data=data_, column=col,
                                   groupby=data_.columns[2], fig=fig, vert=True, **kwargs)
        return fig
    if ax is None:
        ax = plt.axes()
    eda_plot = EDAVisualizer(ax=ax, enable_plotly=enable_plotly)
    ax, _ = eda_plot.box_plot(data=data_, column=col,
                              groupby=data_.columns[2], ax=ax, legend=False, vert=True, outliers=True)
    data_.connection_context.drop_table(temp_tab_name)
    return ax

def bubble_plot(data, x, y, size, color=None, alpha=None, title=None, ax=None,
                enable_plotly=True,
                **kwargs):
    """
    A bubble plot is a type of chart that displays data points as bubbles (or circles) in a two-dimensional space. Similar to a scatter plot, a bubble plot uses the x and y coordinates to represent the variables of interest. A third dimension of the data is shown through the size of bubbles.

    Parameters
    ----------

    data : DataFrame
        Input HANA dataframe.

    x : str
        Column name containing x coordinate.

    y : str
        Column name contraining y coordinate.

    size : str
        Column name containing the size of bubbles.

    color : a list of str, optional
        The marker colors.

        Defaults to None.

    alpha : float, optional

        The alpha blending value, between 0 (transparent) and 1 (opaque).

        Only valid when matplotlib is used.

        Defaults to None.

    ax : Axes, optional
        The axes for the plot.

    enable_plotly : bool, optional
        Use plotly instead of matplotlib.

        Defaults to True.

    kwargs : optional
        Keyword/value pair of properties to be updated when ``enable_plotly`` is True.

        Defaults to None.

    Returns
    -------
    matplotlib:

      The axes for the plot.

    plotly:

      If ``enable_plotly`` is True, returns a plotly.graph_objects.Figure object.

    Examples
    --------

    Matplotlib:

    >>> bubble_plot(data=df, x='X', y='Y', size='S', alpha=0.5, title="Bubble Plot", enable_plotly=False)

    .. image:: image/bubble_plot.png
       :align: center
       :width: 400px

    Plotly:

    >>> bubble_plot(data=df, x='X', y='Y', size='S', width=600, height=400)

    .. image:: image/bubble_plotly.png
       :align: center
       :width: 400px
    """
    df = data.select([x,y,size]).collect()
    if enable_plotly:
        fig = px.scatter(df, x=x, y=y, color=color, size=size, title=title)
        fig.update_layout(**kwargs)
        return fig

    if ax is None:
        ax = plt.axes()
    ax.scatter(x=df[x], y=df[y], s=df[size], c=color, alpha=alpha)
    if title:
        ax.set_title(title)
    ax.set_xlabel(x)
    ax.set_ylabel(y)
    return ax

def parallel_coordinates(data, label,
                         cols=None, color=None,
                         axvlines=None, sort_labels=None, ax=None,
                         enable_plotly=True, **kwargs):
    """
    A parallel coordinates plot is a visualization technique used to display and analyze multivariate data. Currently, this function supports the dataset that have multiple numberical variables.
    In a parallel coordinates plot, each variable is represented by a vertical axis, and lines are drawn to connect the points representing each data observation across these axes. Each line represents an individual data point in the dataset. The position of the line on each axis corresponds to the value of the corresponding variable.

    Parameters
    ----------

    data : DataFrame
        Input HANA dataframe.

    label : str
        Column name containing class names.

    cols : str or a list of str, optional
        A list of column names to use. If the value is not provided, all columns in the data except for label column will be used.

        Default to None.
    color : list or tuple (matplotlib) or str or int or Series or array-like(plotly), optional
        Colors to use for the different classes.

        Defaults to None.
    axvlines : bool, optional
        If true, vertical lines will be added at each xtick.

        Only valid when matplotlib is used.

        Defaults to None.
    sort_labels : bool, optional

        Sort classes in label column, useful when assigning colors.

        Only valid when matplotlib is used.

        Defaults to None.
    ax : Axes, optional
        The axes for the plot.

    enable_plotly : bool, optional
        Use plotly instead of matplotlib.

        Defaults to True.
    kwargs : optional
        Keyword/value pair of properties to be updated when ``enable_plotly`` is True.

        Defaults to None.

    Returns
    -------
    matplotlib:

      The axes for the plot.

    plotly:

      If ``enable_plotly`` is True, returns a plotly.graph_objects.Figure object.

    Examples
    --------

    Matplotlib:

    >>> parallel_coordinates(data=df, label='SPECIES',
                             cols=['SEPALLENGTHCM', 'SEPALWIDTHCM', 'PETALLENGTHCM', 'PETALWIDTHCM'],
                             axvlines=True, sort_labels=True, enable_plotly=False)

    .. image:: image/parallel_coordinates_plot.png
       :align: center
       :width: 400px

    Plotly:

    >>> parallel_coordinates(data=df, label='SPECIES',
                             cols=['SEPALLENGTHCM', 'SEPALWIDTHCM', 'PETALLENGTHCM', 'PETALWIDTHCM'],
                             width=600, height=400)

    .. image:: image/parallel_coordinates_plotly.png
       :align: center
       :width: 400px
    """

    if cols:
        selected_cols = cols[:]
        selected_cols.append(label)
    else:
        selected_cols = data.columns
    select_data = data.select(selected_cols)

    df = select_data.collect()
    if enable_plotly:
        if 'CHAR' in data.select(label).dtypes()[0][1] and color is None:
            color = df[label].astype('category').cat.codes
        fig = px.parallel_coordinates(data_frame=df, dimensions=cols, color=color)
        fig.update_layout(**kwargs)
        return fig

    if ax is None:
        ax = plt.axes()
    ax = pd.plotting.parallel_coordinates(frame=df, class_column=label,
                                          cols=cols, axvlines=axvlines,
                                          color=color, sort_labels=sort_labels)
    return ax

def plot_acf(data,
             col,
             key=None,
             thread_ratio=None,
             method=None,
             max_lag=None,
             calculate_confint=True,
             alpha=None,
             bartlett=None,
             ax=None,
             title=None,
             enable_plotly=True,
             fig=None,
             **kwargs):
    """
    Autocorrelation function plot (ACF).

    Parameters
    ----------

    data : DataFrame
        HANA DataFrame containing the data.

    col : str
        Name of the time series column.

    key : str, optional
        Name of the ID column.

        Defaults to the index column of ``data`` (i.e. data.index) if it is set.

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

        Defaults to sqrt(n), where n is the data number.

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

    ax : matplotlib.axes.Axes, optional
        The axes for the plot.

        Default to None.

    title : str, optional

        The title of plot.

        Defaults to "Autocorrelation".

    enable_plotly : bool, optional
        Use plotly instead of matplotlib.

        Defaults to True.

    fig : plotly.graph_objects.Figure, optional
        If None, a new graph object will be created. Valid when ``enable_plotly`` is True.

        Defaults to None.

    kwargs : optional
        Keyword/value pair of properties to be updated when ``enable_plotly`` is True.

        Defaults to None.

    Returns
    -------
    matplotlib:

      The axes for the plot, returns a matplotlib.axes.Axes object.

    plotly:

      If ``enable_plotly`` is True, returns a plotly.graph_objects.Figure object.

    Examples
    --------
    Matplotlib:

    >>> plot_acf(data=df, key='ID', col='ts', method='fft', enable_plotly=False)

    .. image:: image/acf_plot.png
       :align: center
       :width: 400px

    Plotly:

    >>> plot_acf(data=df, key='ID', col='ts', method='fft', width=600, height=400)

    .. image:: image/acf_plotly.png
       :align: center
       :width: 400px

    """
    res = correlation_function.correlation(data=data,
                                           key=key,
                                           x=col,
                                           thread_ratio=thread_ratio,
                                           method=method,
                                           max_lag=max_lag,
                                           calculate_pacf=False,
                                           calculate_confint=calculate_confint,
                                           alpha=alpha,
                                           bartlett=bartlett)

    fetch_xy = res.select(["LAG", "CF"]).sort_values("LAG").collect()
    if calculate_confint is True:
        fetch_confint = res.select(["LAG", "ACF_CONFIDENCE_BOUND"]).sort_values("LAG").collect()
        lower_bound = np.negative(fetch_confint["ACF_CONFIDENCE_BOUND"].to_numpy())
        upper_bound = fetch_confint["ACF_CONFIDENCE_BOUND"].to_numpy()

    if enable_plotly:
        trace_acf = go.Scatter(x=fetch_xy["LAG"], y=fetch_xy["CF"], mode="markers", marker=dict(size=10, color="blue"), name="ACF")

        if fig:
            fig.add_trace()
        else:
            fig=go.Figure()

        for i in range(len(fetch_xy["LAG"])):
            fig.add_trace(go.Scatter(x=[fetch_xy["LAG"][i], fetch_xy["LAG"][i]], y=[0, fetch_xy["CF"][i]], mode="lines", line=dict(color="blue", width=2), showlegend=False))
        if calculate_confint is True:
            fig.add_trace(go.Scatter(x=fetch_xy["LAG"], y=upper_bound, fill='tozeroy',  mode="lines", name="upper_bound"))
            fig.add_trace(go.Scatter(x=fetch_xy["LAG"], y=lower_bound, fill='tozeroy',  mode="lines", name="lower_bound"))

        fig.update_layout(**kwargs)
        return fig

    if ax is None:
        ax = plt.axes()
    ax.stem(fetch_xy["LAG"].to_numpy(), fetch_xy["CF"].to_numpy())

    if calculate_confint is True:
        ax.fill_between(fetch_confint["LAG"].to_numpy(), lower_bound, upper_bound, alpha=0.35)
    ax.set_xlabel("LAG")
    ax.set_ylabel("ACF")

    if title is None:
        title = "Autocorrelation"
    ax.set_title(title)

    return ax

def plot_pacf(data,
              col,
              key=None,
              thread_ratio=None,
              method=None,
              max_lag=None,
              calculate_confint=True,
              alpha=None,
              bartlett=None,
              ax=None,
              title=None,
              enable_plotly=True,
              fig=None,
              **kwargs):
    """
    Plot partial autocorrelation function (PACF).

    Parameters
    ----------

    data : DataFrame
        HANA DataFrame containing the data.

    col : str, optional
        Name of the time series data column.

    key : str, optional
        Name of the ID column.

        Defaults to the index column of ``data`` (i.e. data.index) if it is set.

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

        Defaults to sqrt(n), where n is the data number.

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

    ax : matplotlib.axes.Axes, optional
        The axes for the plot.

        Default to None.

    title : str, optional

        The title of plot.

        Defaults to "Partial Autocorrelation".

    enable_plotly : bool, optional
        Use plotly instead of matplotlib.

        Defaults to True.

    fig : plotly.graph_objects.Figure, optional
        If None, a new graph object will be created. Valid when ``enable_plotly`` is True.

        Defaults to None.

    kwargs : optional
        Keyword/value pair of properties to be updated when ``enable_plotly`` is True.

        Defaults to None.

    Returns
    -------
    matplotlib:

      The axes for the plot, returns a matplotlib.axes.Axes object.

    plotly:

      If ``enable_plotly`` is True, returns a plotly.graph_objects.Figure object.

    Examples
    --------
    Matplotlib:

    >>> plot_pacf(data=df, key='ID', col='ts', method='fft', enable_plotly=False)

    .. image:: image/pacf_plot.png
       :align: center
       :width: 400px

    Plotly:

    >>> plot_pacf(data=df, key='ID', col='ts', method='fft', width=600, height=400)

    .. image:: image/pacf_plotly.png
       :align: center
       :width: 400px

    """
    res = correlation_function.correlation(data=data,
                                           key=key,
                                           x=col,
                                           thread_ratio=thread_ratio,
                                           method=method,
                                           max_lag=max_lag,
                                           calculate_pacf=True,
                                           calculate_confint=calculate_confint,
                                           alpha=alpha,
                                           bartlett=bartlett)
    fetch_xy = res.select(["LAG", "PACF"]).sort_values("LAG").collect()
    if calculate_confint is True:
        fetch_confint = res.select(["LAG", "PACF_CONFIDENCE_BOUND"]).sort_values("LAG").collect()
        lower_bound = np.negative(fetch_confint["PACF_CONFIDENCE_BOUND"].to_numpy())
        upper_bound = fetch_confint["PACF_CONFIDENCE_BOUND"].to_numpy()

    if enable_plotly:
        trace_pacf = go.Scatter(x=fetch_xy["LAG"], y=fetch_xy["PACF"], mode="markers", marker=dict(size=10, color="blue"), name="PACF")

        if fig:
            fig.add_trace(trace_pacf)
        else:
            fig=go.Figure([trace_pacf])

        for i in range(len(fetch_xy["LAG"])):
            fig.add_trace(go.Scatter(x=[fetch_xy["LAG"][i], fetch_xy["LAG"][i]], y=[0, fetch_xy["PACF"][i]], mode="lines", line=dict(color="blue", width=2), showlegend=False))
        if calculate_confint is True:
            fig.add_trace(go.Scatter(x=fetch_xy["LAG"], y=upper_bound, fill='tozeroy',  mode="lines", name="upper_bound"))
            fig.add_trace(go.Scatter(x=fetch_xy["LAG"], y=lower_bound, fill='tozeroy',  mode="lines", name="lower_bound"))

        fig.update_layout(**kwargs)
        return fig

    if ax is None:
        ax = plt.axes()
    ax.stem(fetch_xy["LAG"].to_numpy(), fetch_xy["PACF"].to_numpy())
    if calculate_confint is True:
        ax.fill_between(fetch_confint["LAG"].to_numpy(), lower_bound, upper_bound, alpha=0.25)

    ax.set_xlabel("LAG")
    ax.set_ylabel("PACF")
    if title is None:
        title = "Partial Autocorrelation"
    ax.set_title(title)

    return ax

def plot_time_series_outlier(data,
                             col,
                             key=None,
                             tso_object=None,
                             window_size=None,
                             outlier_method=None,
                             threshold=None,
                             detect_seasonality=None,
                             alpha=None,
                             extrapolation=None,
                             periods=None,
                             random_state=None,
                             n_estimators=None,
                             max_samples=None,
                             bootstrap=None,
                             contamination=None,
                             minpts=None,
                             eps=None,
                             thread_ratio=None,
                             title=None,
                             ax=None,
                             enable_plotly=True,
                             fig=None,
                             **kwargs):
    """
    Perform OutlierDetectionTS and plot the time series with the highlighted outliers.

    Parameters
    ----------
    data : DataFrame
        HANA DataFrame containing the data.

        ``data`` should have at least two columns: one is ID column,
        the other is raw data.

    col : str
        Column name of endog.

    key : str, optional
        Name of the ID column.

        Defaults to the index column of ``data`` (i.e. data.index) if it is set.

    tso_object : OutlierDetectionTS object, optional
        An object of OutlierDetectionTS for time series outlier. Please initialize a OutlierDetectionTS object first.
        You could either enter a OutlierDetectionTS object or set values of parameters to create a new OutlierDetectionTS object in this function.

        Defaults to None.

    window_size : int, optional
        Odd number, the window size for median filter, not less than 3.

        Defaults to 3.

    outlier_method : str, optional

        The method for calculate the outlier score from residual.

          - 'z1' : Z1 score.
          - 'z2' : Z2 score.
          - 'iqr' : IQR score.
          - 'mad' : MAD score.
          - 'isolationforest' : isolation forest score.
          - 'dbscan' : DBSCAN.

        Defaults to 'z1'.

    threshold : float, optional
        The threshold for outlier score. If the absolute value of outlier score is beyond the
        threshold, we consider the corresponding data point as an outlier.

        Only valid when ``outlier_method`` = 'iqr', 'isolationforest', 'mad', 'z1', 'z2'. For ``outlier_method`` = 'isolationforest', when ``contamination`` is provided, ``threshold`` is not valid and outliers are decided by ``contamination``.

        Defaults to 3 when ``outlier_method`` is 'mad', 'z1' and 'z2'.
        Defaults to 1.5 when ``outlier_method`` is 'iqr'.
        Defaults to 0.7 when ``outlier_method`` is 'isolationforest'.

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

    extrapolation : bool, optional
       Specifies whether to extrapolate the endpoints.
       Set to True when there is an end-point issue.

       Only valid when ``detect_seasonality`` is True.

       Defaults to False.

    periods : int, optional
        When this parameter is not specified, the algorithm will search the seasonal period.
        When this parameter is specified between 2 and half of the series length, autocorrelation value
        is calculated for this number of periods and the result is compared to ``alpha`` parameter.
        If correlation value is equal to or higher than ``alpha``, decomposition is executed with the value of ``periods``.
        Otherwise, the residual is calculated without decomposition. For other value of parameter ``periods``, the residual is also calculated without decomposition.

        Only valid when ``detect_seasonality`` is True. If the user knows the seasonal period, specifying ``periods`` can speed up the calculation, especially when the time series is long.

        No Default value.

    random_state : int, optional
        Specifies the seed for random number generator.

        - 0: Uses the current time (in second) as seed.
        - Others: Uses the specified value as seed.

        Only valid when ``outlier_method`` is 'isolationforest'.

        Default to 0.

    n_estimators : int, optional
        Specifies the number of trees to grow.

        Only valid when ``outlier_method`` is 'isolationforest'.

        Default to 100.

    max_samples : int, optional
        Specifies the number of samples to draw from input to train each tree.
        If ``max_samples`` is larger than the number of samples provided,
        all samples will be used for all trees.

        Only valid when ``outlier_method`` is 'isolationforest'.

        Default to 256.

    bootstrap : bool, optional
        Specifies sampling method.

        - False: Sampling without replacement.
        - True: Sampling with replacement.

        Only valid when ``outlier_method`` is 'isolationforest'.

        Default to False.

    contamination : double, optional
        The proportion of outliers in the data set. Should be in the range (0, 0.5].

        Only valid when ``outlier_method`` is 'isolationforest'. When ``outlier_method`` is 'isolationforest' and ``contamination`` is specified, ``threshold`` is not valid.

        No Default value.

    minpts : int, optional
        Specifies the minimum number of points required to form a cluster. The point itself is not included in ``minpts``.

        Only valid when ``outlier_method`` is 'dbscan'.

        Defaults to 1.
    eps : float, optional
        Specifies the scan radius.

        Only valid when ``outlier_method`` is 'dbscan'.

        Defaults to 0.5.

    thread_ratio : float, optional
        The ratio of available threads.

          - 0: single thread.
          - 0~1: percentage.
          - Others: heuristically determined.

        Only valid when ``detect_seasonality`` is True or ``outlier_method`` is 'isolationforest' or 'dbscan'.

        Defaults to -1.

    title : str, optional

        The title of plot.

        Defaults to "Outliers".

    ax : matplotlib.axes.Axes, optional
        The axes for the plot.

        Default to None.

    enable_plotly : bool, optional
        Use plotly instead of matplotlib.

        Defaults to True.

    fig : plotly.graph_objects.Figure, optional
        If None, a new graph object will be created. Valid when ``enable_plotly`` is True.

        Defaults to None.

    kwargs : optional
        Keyword/value pair of properties to be updated when ``enable_plotly`` is True.

        Defaults to None.

    Returns
    -------
    matplotlib:

      The axes for the plot, returns a matplotlib.axes.Axes object.

    plotly:

      If ``enable_plotly`` is True, returns a plotly.graph_objects.Figure object.

    Examples
    --------
    Matplotlib:

    >>> plot_time_series_outlier(data=df, key='ID', col='ts', enable_plotly=False)

    .. image:: image/time_series_outlier_plot.png
       :align: center
       :width: 400px

    Plotly:

    >>> plot_time_series_outlier(data=df, key='ID', col='ts', width=600, height=400)

    .. image:: image/time_series_outlier_plotly.png
       :align: center
       :width: 400px


    """
    if key is None:
        if data.index:
            key = data.index
        else:
            raise ValueError("Index should be set by key or use set_index function!")

    if tso_object is None:
        tso_object = OutlierDetectionTS(window_size=window_size,
                                        outlier_method=outlier_method,
                                        threshold=threshold,
                                        detect_seasonality=detect_seasonality,
                                        alpha=alpha,
                                        extrapolation=extrapolation,
                                        periods=periods,
                                        random_state=random_state,
                                        n_estimators=n_estimators,
                                        max_samples=max_samples,
                                        bootstrap=bootstrap,
                                        contamination=contamination,
                                        minpts=minpts,
                                        eps=eps,
                                        thread_ratio=thread_ratio)

    result = tso_object.fit_predict(data=data, key=key, endog=col)
    res_col = result.columns
    result = result.select([res_col[0], res_col[1], res_col[4]]).collect()
    result.set_index(res_col[0])
    outliers = result.loc[result[res_col[4]] == 1, [res_col[0], res_col[1]]]

    if enable_plotly:
        trace_raw = go.Scatter(x=result[res_col[0]].values, y=result[res_col[1]].values, name=col)
        trace_outliers = go.Scatter(x=outliers[res_col[0]].values,
                                    y=outliers[res_col[1]].values, mode='markers',
                                    marker=dict(size=10), name="Outliers")

        if fig:
            fig.add_trace(trace_raw)
            fig.add_trace(trace_outliers)
        else:
            fig = go.Figure([trace_raw])
            fig.add_trace(trace_outliers)
        fig.update_layout(**kwargs)

        return fig

    if ax is None:
        ax = plt.axes()

    ax.plot(result[res_col[0]].values, result[res_col[1]].values, color='blue')
    ax.scatter(outliers[res_col[0]].values, outliers[res_col[1]].values, color='red')
    if title is None:
        title = "Outliers"
    ax.set_title(title)
    ax.set_xlabel(key)
    ax.set_ylabel(col)
    if data.select(key).dtypes()[0][1] != "INT":
        for tick in ax.get_xticklabels():
            tick.set_rotation(30)

    return ax

def plot_change_points(data,
                       cp_object,
                       col,
                       key=None,
                       display_trend=True,
                       cp_style="axvline",
                       title=None,
                       ax=None,
                       enable_plotly=True,
                       fig=None,
                       **kwargs):
    """
    Plot the time series with the highlighted change points and BCPD is used for change point detection.

    Parameters
    ----------
    data : DataFrame
        HANA DataFrame containing the data.

        ``data`` should have at least two columns: one is ID column,
        the other is raw data.

    col : str
        Name of the time series data column.

    cp_object : BCPD object
        An object of BCPD for change points detection. Please initialize a BCPD object first.

        An example is shown below:

        .. only:: latex

            >>> bcpd = BCPD(max_tcp=2, max_scp=1, max_harmonic_order =10, random_seed=1, max_iter=10000)
            >>> plot_change_points(data=df, cp_object=bcpd, cp_style="axvline")

        .. raw:: html

            <iframe allowtransparency="true" style="border:1px solid #ccc; background: #eeffcb;"
                src="_static/eda_example.html" width="100%" height="100%" sandbox="">
            </iframe>

    key : str, optional
        Name of the ID column.

        Defaults to the index column of ``data`` (i.e. data.index) if it is set.

    cp_style : {"axvline", "scatter"}, optional

        The style of change points in the plot.

        Defaults to "axvline".

    display_trend : bool, optional

        If True, draw the trend component based on decomposed component of trend of BCPD fit_predict().

        Default to True.

    title : str, optional

        The title of plot.

        Defaults to "Change Points".

    ax : matplotlib.axes.Axes, optional
        The axes for the plot.

        Default to None.

    enable_plotly : bool, optional
        Use plotly instead of matplotlib.

        Defaults to True.

    fig : plotly.graph_objects.Figure, optional
        If None, a new graph object will be created. Valid when ``enable_plotly`` is True.

        Defaults to None.

    kwargs : optional
        Keyword/value pair of properties to be updated when ``enable_plotly`` is True.

        Defaults to None.

    Returns
    -------
    matplotlib:

      The axes for the plot, returns a matplotlib.axes.Axes object.

    plotly:

      If ``enable_plotly`` is True, returns a plotly.graph_objects.Figure object.

    Examples
    --------
    Matplotlib:

    >>> bcpd = BCPD(max_tcp=5, max_scp=0, random_seed=1, max_iter=1000)
    >>> plot_change_points(data=df, key='ts', col='y', cp_object=bcpd, enable_plotly=False)

    .. image:: image/change_points_plot.png
       :align: center
       :width: 400px

    Plotly:

    >>> bcpd = BCPD(max_tcp=5, max_scp=0, random_seed=1, max_iter=1000)
    >>> plot_change_points(data=df, key='ts', col='y', cp_object=bcpd, width=600, height=400)

    .. image:: image/change_points_plotly.png
       :align: center
       :width: 400px

    """
    if key is None:
        if data.index:
            key = data.index
        else:
            raise ValueError("Index should be set by key or use set_index function!")

    data_ = data.select([key, col]).sort_values(key).collect()
    display_trend = arg('display_trend', display_trend, bool)
    cp_style = arg('cp_style', cp_style, {"scatter":"scatter", "axvline":"axvline"})
    title = arg('title', title, str)

    if isinstance(cp_object, BCPD):
        tcp, scp, period, components = cp_object.fit_predict(data=data, key=key, endog=col)

        if enable_plotly:
            if isinstance(cp_object, BCPD):
                tcp, scp, period, components = cp_object.fit_predict(data=data, key=key, endog=col)
                trace_raw = go.Scatter(x=data_.iloc[:, 0], y=data_.iloc[:, 1], name="Original Time Series")
                if fig:
                    fig.add_trace(trace_raw)
                else:
                    fig = go.Figure([trace_raw])

                if display_trend is True:
                    fig.add_trace(go.Scatter(x=components.collect().iloc[:, 0], y=components.collect().iloc[:, 2], name="Trend Component"))

                trace_tcp = None
                trace_scp = None
                if tcp.shape[0] > 0:
                    result_trend = tcp.set_index('TREND_CP').join(data.set_index(key), how='left')
                    fig.add_trace(go.Scatter(x=result_trend.collect().iloc[:, 1], y=result_trend.collect().iloc[:, 2], marker=dict(size=10), name="Trend Change Points", mode="markers"))
                    tcp_list = list(tcp.collect()["TREND_CP"])
                    for i in range(len(tcp_list)):
                        fig.add_vline(x=tcp_list[i], line_width=3, line_dash="dash")

                if scp.shape[0] > 0:
                    result_seasonal = scp.set_index('SEASON_CP').join(data.set_index(key), how='left')
                    fig.add_trace(go.Scatter(x=result_seasonal.collect().iloc[:, 1], y=result_seasonal.collect().iloc[:, 2], marker=dict(size=10), name="Seasonal Change Points", mode="markers"))
                    scp_list = list(scp.collect()["SEASON_CP"])
                    for i in range(len(scp_list)):
                        fig.add_vline(x=scp_list[i], line_width=3, line_dash="dash")

                fig.update_layout(**kwargs)
                return fig

        if ax is None:
            ax = plt.axes()
        if tcp.shape[0] > 0:
            if cp_style == "scatter":
                result = tcp.set_index('TREND_CP').join(data.set_index(key), how='left')
                ax.scatter(result.collect()['TREND_CP'], result.collect()[col], color='red', label="Trend Change Points")
            if cp_style == "axvline":
                tcp_list = list(tcp.collect()["TREND_CP"])
                for i in range(len(tcp_list)):
                    if i == 0:
                        ax.axvline(tcp_list[i], color='red', linestyle='dashed', label="Trend Change Points")
                    ax.axvline(tcp_list[i], color='red', linestyle='dashed')

        if scp.shape[0] > 0:
            if cp_style == "scatter":
                result = scp.set_index('SEASON_CP').join(data.set_index(key), how='left')
                ax.scatter(result.collect()['SEASON_CP'], result.collect()[col], color='green', label="Seasonal Change Points")
            if cp_style == "axvline":
                scp_list = list(scp.collect()["SEASON_CP"])
                for i in range(len(scp_list)):
                    if i == 0:
                        ax.axvline(scp_list[i], color='green', linestyle='dashed', label="Seasonal Change Points")
                    ax.axvline(scp_list[i], color='green', linestyle='dashed')

        if display_trend is True:
            ax.plot(components.collect()[key].values, components.collect()["TREND"].values, color='orange', label="Trend Component")

        ax.plot(data_.iloc[:, 0].values, data_.iloc[:, 1].values, color='blue', label="Original Time Series")
        if title is None:
            title = "Change Points"
        ax.set_title(title)
        ax.set_xlabel(key)
        ax.set_ylabel(col)

        pos = ax.get_position()
        ax.set_position([pos.x0, pos.y0, pos.width * 0.9, pos.height])
        ax.legend(loc='center right', bbox_to_anchor=(1.55, 0.5))
        ax.autoscale(True)

        if data.select(key).dtypes()[0][1] != "INT":
            for tick in ax.get_xticklabels():
                tick.set_rotation(30)
    return ax

def plot_moving_average(data, col, rolling_window, key=None, ax=None, compare=True,
                        enable_plotly=True, fig=None, **kwargs):
    """
    Plot the rolling mean by the given rolling window size.

    Parameters
    ----------
    data : DataFrame
        HANA DataFrame containing the data.

    col : str
        Name of the time series data column.

    rolling_window : int
        Window size for rolling function. If negative, it will use the points before CURRENT ROW.

    key : str, optional
        Name of the ID column.

        Defaults to the index column of ``data`` (i.e. data.index) if it is set.

    ax : matplotlib.axes.Axes, optional
        The axes for the plot.

        Default to None.

    compare : bool, optional
        If True, it will plot the data and its moving average. Otherwise, only moving average will be plotted.

        Defaults to True.

    enable_plotly : bool, optional
        Use plotly instead of matplotlib.

        Defaults to True.

    fig : plotly.graph_objects.Figure, optional
        If None, a new graph object will be created. Valid when ``enable_plotly`` is True.

        Defaults to None.

    kwargs : optional
        Keyword/value pair of properties to be updated when ``enable_plotly`` is True.

        Defaults to None.

    Returns
    -------
    matplotlib:

      The axes for the plot, returns a matplotlib.axes.Axes object.

    plotly:

      If ``enable_plotly`` is True, returns a plotly.graph_objects.Figure object.

    Examples
    --------
    Matplotlib:

    >>> plot_moving_average(data=df, key='ID', col='ts', rolling_window=10, enable_plotly=False)

    .. image:: image/moving_average_plot.png
       :align: center
       :width: 400px

    Plotly:

    >>> plot_moving_average(data=df, key='ID', col='ts', rolling_window=10, width=600, height=400)

    .. image:: image/moving_average_plotly.png
       :align: center
       :width: 400px
    """
    if key is None:
        if data.index:
            key = data.index
        else:
            raise ValueError("Index should be set by key or use set_index function!")
    data_ = data.select([key, col]).generate_feature(targets=[col], order_by=key, trans_func="AVG", rolling_window=rolling_window).collect()

    if enable_plotly:
        trace_avg = go.Scatter(x=data_.iloc[:, 0], y=data_.iloc[:, 2], name=data_.columns[1])
        trace_raw = go.Scatter(x=data_.iloc[:, 0], y=data_.iloc[:, 1], name=data_.columns[2])

        if fig:
            fig.add_trace(trace_avg)
            if compare:
                fig.add_trace(trace_raw)
        else:
            fig = go.Figure([trace_avg])
            if compare:
                fig.add_trace(trace_raw)
        fig.update_layout(**kwargs)
        return fig

    if ax is None:
        ax = plt.axes()
    if compare:
        ax.plot(data_.iloc[:, 0].values, data_.iloc[:, 1].values, label=data_.columns[1])
    ax.plot(data_.iloc[:, 0].values, data_.iloc[:, 2].values, label=data_.columns[2])
    ax.legend()

    if data.select(key).dtypes()[0][1] != "INT":
        for tick in ax.get_xticklabels():
            tick.set_rotation(30)
    return ax

def plot_rolling_stddev(data, col, rolling_window, key=None, ax=None,
                        enable_plotly=True, fig=None, **kwargs):
    """
    Plot the rolling standard deviation by given rolling window size.

    Parameters
    ----------
    data : DataFrame
        HANA DataFrame containing the data.

    col : str
        Name of the time series data column.

    rolling_window : int, optional
            Window size for rolling function. If negative, it will use the points before CURRENT ROW.

    key : str, optional
        Name of the ID column.

        Defaults to the index column of ``data`` (i.e. data.index) if it is set.

    ax : matplotlib.axes.Axes, optional
        The axes for the plot.

        Default to None.

    enable_plotly : bool, optional
        Use plotly instead of matplotlib.

        Defaults to True.

    fig : plotly.graph_objects.Figure, optional
        If None, a new graph object will be created. Valid when ``enable_plotly`` is True.

        Defaults to None.

    kwargs : optional
        Keyword/value pair of properties to be updated when ``enable_plotly`` is True.

        Defaults to None.

    Returns
    -------
    matplotlib:

      The axes for the plot, returns a matplotlib.axes.Axes object.

    plotly:

      If ``enable_plotly`` is True, returns a plotly.graph_objects.Figure object.

    Examples
    --------
    Matplotlib:

    >>> plot_rolling_stddev(data=df, key='ID', col='ts', rolling_window=10, enable_plotly=False)

    .. image:: image/rolling_stddev_plot.png
       :align: center
       :width: 400px

    Plotly:

    >>> plot_rolling_stddev(data=df, key='ID', col='ts', rolling_window=10, width=600, height=400)

    .. image:: image/rolling_stddev_plotly.png
       :align: center
       :width: 400px
    """
    if key is None:
        if data.index:
            key = data.index
        else:
            raise ValueError("Index should be set by key or use set_index function!")
    data_ = data.select([key, col]).generate_feature(targets=[col], order_by=key, trans_func="STDDEV", rolling_window=rolling_window).collect()

    if enable_plotly:
        trace_sd = go.Scatter(x=data_.iloc[:, 0].values, y=data_.iloc[:, 2].values, name=data_.columns[2], showlegend=True)

        if fig:
            fig.add_trace(trace_sd)
        else:
            fig = go.Figure([trace_sd])
        fig.update_layout(**kwargs)
        return fig

    if ax is None:
        ax = plt.axes()
    ax.plot(data_.iloc[:, 0].values, data_.iloc[:, 2].values, label=data_.columns[2])
    ax.legend()

    if data.select(key).dtypes()[0][1] != "INT":
        for tick in ax.get_xticklabels():
            tick.set_rotation(30)
    return ax

def plot_seasonal_decompose(data,
                            col,
                            key=None,
                            alpha=None,
                            thread_ratio=None,
                            decompose_type=None,
                            extrapolation=None,
                            smooth_width=None,
                            axes=None,
                            enable_plotly=True,
                            fig=None,
                            **kwargs):
    """
    Plot the seasonal decomposition.

    Parameters
    ----------
    data : DataFrame
        HANA DataFrame containing the data.

    col : str
        Name of the time series data column.

    key : str, optional
        Name of the ID column.

        Defaults to the index column of ``data`` (i.e. data.index) if it is set.

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

        - 'additive': Additive decomposition model.
        - 'multiplicative': Multiplicative decomposition model.
        - 'auto': Decomposition model automatically determined from input data.

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

    axes : Axes array, optional
        The axes for the plot.

    enable_plotly : bool, optional
        Use plotly instead of matplotlib.

        Defaults to True.

    fig : plotly.graph_objects.Figure, optional
        If None, a new graph object will be created. Valid when ``enable_plotly`` is True.

        Defaults to None.

    kwargs : optional
        Keyword/value pair of properties to be updated when ``enable_plotly`` is True.

        Defaults to None.

    Returns
    -------
    matplotlib:

      The axes for the plot, returns a matplotlib.axes.Axes object.

    plotly:

      If ``enable_plotly`` is True, returns a plotly.graph_objects.Figure object.

    Examples
    --------
    Matplotlib:

    >>> plot_seasonal_decompose(data=df, col='ts', key= 'ID', enable_plotly=False)

    .. image:: image/seasonal_decompose_plot.png
       :align: center
       :width: 400px

    Plotly:

    >>> plot_seasonal_decompose(data=df, col='ts', key= 'ID', width=600, height=400)

    .. image:: image/seasonal_decompose_plotly.png
       :align: center
       :width: 400px

    """
    _, res = seasonal_decompose.seasonal_decompose(data=data,
                                                   endog=col,
                                                   key=key,
                                                   alpha=alpha,
                                                   thread_ratio=thread_ratio,
                                                   decompose_type=decompose_type,
                                                   extrapolation=extrapolation,
                                                   smooth_width=smooth_width)
    x_data=res.collect()[res.columns[0]].values
    if enable_plotly:
        if fig is None:
            fig = make_subplots(rows=3, cols=1, shared_xaxes=True)
        for i in [1,2,3]:
            fig.add_trace(go.Scatter(x=x_data,
                                     y=res.select(res.columns[i]).collect()[res.columns[i]].values,
                                     name=res.columns[i]),
                                     row=i,
                                     col=1)

        fig.update_layout(**kwargs)
        return fig

    if axes is None:
        fig, axes = plt.subplots(3, 1, figsize=(12, 8))
    for num, color in zip(range(0, 3), ['b', 'r', 'g']):
        ax = axes[num]
        ax.plot(res.select(res.columns[0]).collect()[res.columns[0]].values,
                res.select(res.columns[num + 1]).collect()[res.columns[num + 1]].values,
                label=res.columns[num + 1], color=color)
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', ncol=1)
    if ax is None:
        fig.tight_layout()
    return axes

def kdeplot(data, key, features=None, kde=kernel_density.KDE(), points=1000, enable_plotly=True, **kwargs):
    """
    Display a kernel density estimate plot for SAP HANA DataFrame.

    Parameters
    ----------
    data : DataFrame

        HANA DataFrame containing the data.

    key : str
        Name of the ID column in the data.

    features : str/list of str, optional
        Name of the feature columns in the data.

    kde : hana_ml.algorithms.pal.kernel_density.KDE, optional
        KDE Calculation.

        Defaults to KDE().

    points : int, optional
        The number of points for plotting.

        Defaults to 1000.

    enable_plotly : bool, optional
        Use plotly instead of matplotlib.

        Defaults to True.

    Returns
    -------
    matplotlib:

      - The axes for the plot, returns a matplotlib.axes.Axes object.
      - Poly3DCollection, The surface plot object. Only valid for matplotlib 2D plotting.

    plotly:

      If ``enable_plotly`` is True, returns a plotly.graph_objects.Figure object.

    Examples
    --------

    Matplotlib:

    >>> f = plt.figure(figsize=(19, 10))
    >>> ax = kdeplot(data=df, key="PASSENGER_ID", features=["AGE"], enable_plotly=False)
    >>> ax.grid()
    >>> plt.show()

    .. image:: image/kde_plot.png
       :align: center
       :width: 400px

    >>> f = plt.figure(figsize=(19, 10))
    >>> ax, surf = kdeplot(data=df, key="PASSENGER_ID", features=["AGE", "FARE"], enable_plotly=False)
    >>> ax.grid()
    >>> plt.show()

    .. image:: image/kde_plot2.png
       :align: center
       :width: 400px

    Plotly:

    >>> fig = kdeplot(data=df.filter("SURVIVED = 1"), key="PASSENGER_ID", features=["AGE"], width=600, height=600)
    >>> fig.show()

    .. image:: image/kde_plotly.png
       :align: center
       :width: 400px

    >>> fig = kdeplot(data=df, key="PASSENGER_ID", features=["AGE", "FARE"], width=600, height=600)
    >>> fig.show()

    .. image:: image/kde_plotly2.png
       :align: center
       :width: 400px

    """
    conn_context = data.connection_context
    kde.fit(data=data, key=key, features=features)
    columns = data.columns
    if key in columns:
        columns.remove(key)
    if features is not None:
        if isinstance(features, str):
            columns = [features]
        else:
            columns = features
    temp_tab_name = "#KDEPLOT" + str(uuid.uuid1()).replace('-', '_').upper()
    if len(columns) == 1:
        query = "SELECT MAX({}) FROM ({})".format(quotename(columns[0]), data.select_statement)
        xmax = conn_context.sql(query).collect(geometries=False).values[0][0]
        query = "SELECT MIN({}) FROM ({})".format(quotename(columns[0]), data.select_statement)
        xmin = conn_context.sql(query).collect(geometries=False).values[0][0]
        x_axis_ticks = eval("np.mgrid[xmin:xmax:{0}j]".format(points)).flatten()
        kde_df = create_dataframe_from_pandas(conn_context,
                                              pandas_df=pd.DataFrame({key: [item for item in range(1, len(x_axis_ticks) + 1)],
                                                                      columns[0] : x_axis_ticks}),
                                              table_name=temp_tab_name,
                                              force=True,
                                              disable_progressbar=True)
        res, _ = kde.predict(data=kde_df, key=key)
        if enable_plotly:
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=x_axis_ticks, y=np.exp(res.select(res.columns[1]).collect(geometries=False).to_numpy().flatten()), mode='lines'))
            fig.update_layout(yaxis_title='Density Value', **kwargs)
            return fig
        ax = plt.axes()
        ax.plot(x_axis_ticks,
                np.exp(res.select(res.columns[1]).collect(geometries=False).to_numpy().flatten()),
                **kwargs)
        y_label = 'Density Value'
        ax.set_ylabel(y_label)
        return ax
    elif len(columns) == 2:
        query = "SELECT MAX({}) FROM ({})".format(quotename(columns[0]), data.select_statement)
        xmax = conn_context.sql(query).collect(geometries=False).values[0][0]
        query = "SELECT MIN({}) FROM ({})".format(quotename(columns[0]), data.select_statement)
        xmin = conn_context.sql(query).collect(geometries=False).values[0][0]
        query = "SELECT MAX({}) FROM ({})".format(quotename(columns[1]), data.select_statement)
        ymax = conn_context.sql(query).collect(geometries=False).values[0][0]
        query = "SELECT MIN({}) FROM ({})".format(quotename(columns[1]), data.select_statement)
        ymin = conn_context.sql(query).collect(geometries=False).values[0][0]
        xx, yy = eval("np.mgrid[xmin:xmax:{0}j, ymin:ymax:{0}j]".format(int(math.sqrt(points))))
        kde_df = create_dataframe_from_pandas(conn_context,
                                              pandas_df=pd.DataFrame({key: [item for item in range(1, len(xx.flatten()) + 1)],
                                                                      columns[0]: xx.flatten(),
                                                                      columns[1]: yy.flatten()}),
                                              table_name=temp_tab_name,
                                              force=True,
                                              disable_progressbar=True)
        res, _ = kde.predict(data=kde_df, key=key)
        fetched_result = np.exp(res.select(res.columns[1]).collect(geometries=False).to_numpy().flatten())
        zz = fetched_result.reshape(xx.shape)
        if enable_plotly:
            fig = go.Figure(data=[go.Surface(z=zz, x=xx, y=yy)])
            fig.update_layout(scene=dict(xaxis_title=columns[0], yaxis_title=columns[1], zaxis_title='Density Value'), **kwargs)
            return fig
        ax = plt.axes(projection='3d')
        surf = None
        if "cmap" not in kwargs:
            surf = ax.plot_surface(xx, yy, zz, cmap='coolwarm', **kwargs)
        else:
            surf = ax.plot_surface(xx, yy, zz, **kwargs)
        plt.colorbar(surf, shrink=0.5, aspect=5, ax=ax)
        ax.set_xlabel(columns[0])
        ax.set_ylabel(columns[1])
        ax.set_zlabel('Density Value')
        return ax, surf
    else:
        raise ValueError("The feature number exceeds 2!")

def hist(data, columns, bins=None, debrief=False, x_axis_fontsize=10,
         x_axis_rotation=0, title_fontproperties=None, default_bins=20,
         rounding_precision=3, replacena=0, enable_plotly=True, **kwargs):
    """
    Plot histograms for SAP HANA DataFrame.

    Parameters
    ----------
    data : DataFrame
        HANA DataFrame containing the data.

    columns : list of str
        Columns in the DataFrame being plotted.

    bins : int or dict, optional
        The number of bins to create based on the value of column.

        Defaults to 20.

    debrief : bool, optional
        Whether to include the skewness debrief.

        Defaults to False.

    x_axis_fontsize : int, optional
        The size of x axis labels.

        Defaults to 10.

    x_axis_rotation : int, optional
        The rotation of x axis labels.

        Defaults to 0.

    title_fontproperties : FontProperties, optional
        Change the font properties for title. Only for Matplotlib plot.

        Defaults to None.

    default_bins : int, optional
        The number of bins to create for the column that has not been specified in bins when bins is `dict`.

        Defaults to 20.

    debrief : bool, optional
        Whether to include the skewness debrief.

        Defaults to False.

    rounding_precision : int, optional
        The rounding precision for bin size.

        Defaults to 3.

    replacena : float, optional
        Replace na with the specified value.

        Defaults to 0.

    enable_plotly : bool, optional
        Use plotly instead of matplotlib.

        Defaults to True.

    Returns
    -------
    matplotlib:

      The axes for the plot, returns a matplotlib.axes.Axes object.

    plotly:

      If ``enable_plotly`` is True, returns a plotly.graph_objects.Figure object.

    Examples
    --------
    Matplotlib:

    >>> hist(data=df, columns=['PCLASS', 'AGE', 'SIBSP', 'PARCH', 'FARE'], default_bins=10, bins={"AGE": 10}, enable_plotly=False)

    .. image:: image/hist_plot.png
       :align: center
       :width: 400px

    Plotly:

    >>> hist(data=df, columns=['PCLASS', 'AGE', 'SIBSP', 'PARCH', 'FARE'], default_bins=10, bins={"AGE": 10})

    .. image:: image/hist_plotly.png
       :align: center
       :width: 400px
    """
    if enable_plotly:
        tracelist = []
        subtitles = []
        _bins = default_bins
        if bins is not None:
            _bins = bins
        for feature in columns:
            if isinstance(bins, dict):
                if feature in bins:
                    _bins = bins[feature]
                else:
                    _bins = default_bins
            _, trace, _ = eda_plotly.distribution_plot(data=data,
                                                       column=feature,
                                                       bins=_bins,
                                                       debrief=debrief,
                                                       rounding_precision=rounding_precision,
                                                       replacena=replacena)
            tracelist.append(trace)
            subtitles.append(feature + " Distribution")
        fig = make_subplots(
            rows=math.ceil(len(tracelist)/2), cols=2,
            subplot_titles=tuple(subtitles))
        for num, trace in enumerate(tracelist):
            fig.add_trace(
                trace,
                row=int(num/2+1), col=int(num%2+1))
        fig.update_layout(showlegend=False, **kwargs)
        fig.update_xaxes(tickangle=x_axis_rotation, tickfont_size=x_axis_fontsize)
        fig.show()
    else: # matplotlib
        fig = plt.figure(figsize=(20, 20))
        rows = math.ceil(len(columns) / 2)
        _bins = default_bins
        if bins is not None:
            _bins = bins
        for num, feature in enumerate(columns):
            axis = fig.add_subplot(rows, 2, num + 1)
            eda_plot = EDAVisualizer(ax=axis, enable_plotly=False)
            if isinstance(bins, dict):
                if feature in bins:
                    _bins = bins[feature]
                else:
                    _bins = default_bins
            eda_plot.distribution_plot(data=data,
                                       column=feature,
                                       bins=_bins,
                                       title=feature + " Distribution",
                                       debrief=debrief,
                                       x_axis_fontsize=x_axis_fontsize,
                                       x_axis_rotation=x_axis_rotation,
                                       title_fontproperties=title_fontproperties,
                                       rounding_precision=rounding_precision,
                                       replacena=replacena,
                                       **kwargs)

def plot_psd(data,
             col,
             key=None,
             sampling_rate = None,
             num_fft = None,
             freq_range = None,
             spectrum_type = None,
             window = None,
             alpha = None,
             beta = None,
             attenuation = None,
             mode = None,
             precision = None,
             r = None,
             title = None,
             xlabel_name = None,
             ylabel_name = None,
             semilogy = False,
             ax = None,
             periodogram_res=None,
             enable_plotly=True,
             fig=None,
             **kwargs):
    """
    Plot Power Spectral Density (PSD) with periodogram.

    Parameters
    ----------

    data : DataFrame
        HANA DataFrame containing the data.

    col : str
        Name of the time series data column.

    key : str, optional
        Name of the ID column.

        Defaults to the index column of ``data`` (i.e. data.index) if it is set.

    sampling_rate : float, optional

        Sampling frequency of the sequence.

        Defaults to 1.0.

    num_fft : integer, optional

        Number of DFT points. If ``num_fft`` is smaller than the length of the input, the input is cropped. If it is larger, the input is padded with zeros.

        Defaults to the length of sequence.

    freq_range : {"one_sides", "two_sides"}, optional

        Indicates result frequency range.

        Defaults to "one_sides".

    spectrum_type : {"density", "spectrum"}, optional

        Indicates power spectrum scaling type.

        - "density": power spectrum density.
        - "spectrum": power spectrum.

        Defaults to "density".

    window : str, optional
        Available input window type:

        - 'none',
        - 'bartlett',
        - 'bartlett_hann',
        - 'blackman',
        - 'blackman_harris',
        - 'bohman',
        - 'chebwin',
        - 'cosine',
        - 'flattop',
        - 'gaussian',
        - 'hamming',
        - 'hann',
        - 'kaiser',
        - 'nuttall',
        - 'parzen',
        - 'tukey'

        No default value.

    alpha : float, optional
        Window parameter.
        Only valid for blackman and gaussian window.
        Default values:

          - "Blackman", defaults to 0.16.
          - "Gaussian", defaults to 2.5.

    beta : float, optional
        Parameter for Kaiser Window.
        Only valid for kaiser window.

        Defaults to 8.6.

    attenuation : float, optional
        Parameter for Chebwin.
        Only valid for chewin window.

        Defaults to 50.0.

    mode : {'symmetric', 'periodic'}, optional
        Parameter for Flattop Window. Can be:

        - 'symmetric'.
        - 'periodic'.

        Only valid for flattop window.
        Defaults to 'symmetric'.

    precision : str, optional
        Parameter for Flattop Window. Can be:

        - 'none'
        - 'octave'

        Only valid for flattop window.
        Defaults to 'none'.

    r : float, optional
        Parameter for Tukey Window.
        Only valid for tukey window.

        Defaults to 0.5.

    title : str, optional

        The plot title.

        Defaults to "Periodogram".

    xlabel_name : str, optional

        Name of x label.

        Defaults to None.

    ylabel_name : str, optional

        Name of y label.

        Defaults to None.

    semilogy : bool, optional
        Whether to make a plot with log scaling on the y axis.

        Defaults to False.

    ax : matplotlib.axes.Axes, optional
        The axes for the plot.

        Default to None.

    periodogram_res : DataFrame, optional
        The returned result DataFrame from function periodogram().

        Defaults to None.

    enable_plotly : bool, optional
        Use plotly instead of matplotlib.

        Defaults to True.

    fig : plotly.graph_objects.Figure, optional
        If None, a new graph object will be created. Valid when ``enable_plotly`` is True.

        Defaults to None.

    kwargs : optional
        Keyword/value pair of properties to be updated when ``enable_plotly`` is True.

        Defaults to None.

    Returns
    -------
    matplotlib:

      The axes for the plot, returns a matplotlib.axes.Axes object.

    plotly:

      If ``enable_plotly`` is True, returns a plotly.graph_objects.Figure object.

    Examples
    --------
    Matplotlib:

    >>> plot_psd(data=df, col="ts",  key="ID", sampling_rate=100.0, window="hamming", freq_range="two_sides", title="Periodogram", semilogy=True, enable_plotly=False)

    .. image:: image/psd_plot.png
       :align: center
       :width: 400px

    Plotly:

    >>> plot_psd(data=df, col="ts",  key="ID", sampling_rate=100.0, window="hamming", freq_range="two_sides", title="Periodogram", width=600, height=400, semilogy=True)

    .. image:: image/psd_plotly.png
       :align: center
       :width: 400px

    """
    if periodogram_res is None:
        res = periodogram(data=data,
                          key=key,
                          endog=col,
                          sampling_rate = sampling_rate,
                          num_fft = num_fft,
                          freq_range = freq_range,
                          spectrum_type = spectrum_type,
                          window = window,
                          alpha =alpha,
                          beta = beta,
                          attenuation = attenuation,
                          mode = mode,
                          precision = precision,
                          r = r)

        if key is None:
            if data.index:
                key = data.index
            else:
                raise ValueError("Index should be set by key or use set_index function!")
    else:
        res = periodogram_res
    fetch_xy = res.select(["FREQ", "PXX"]).sort_values("FREQ").collect()

    if enable_plotly:
        trace_psd = go.Scatter(x=fetch_xy["FREQ"], y=fetch_xy["PXX"], mode="markers+lines")
        if fig:
            fig.add_trace(trace_psd)
        else:
            fig = go.Figure([trace_psd])
        if semilogy is True:
            fig.update_yaxes(type='log')

        fig.update_layout(**kwargs)
        return fig

    if ax is None:
        ax = plt.axes()
    if semilogy is True:
        ax.semilogy(fetch_xy["FREQ"].to_numpy(), fetch_xy["PXX"].to_numpy())
    else:
        ax.plot(fetch_xy["FREQ"].to_numpy(), fetch_xy["PXX"].to_numpy())

    if xlabel_name is not None:
        ax.set_xlabel(xlabel_name)
    if ylabel_name is not None:
        ax.set_ylabel(ylabel_name)

    if title is None:
        title = "Periodogram"
    ax.set_title(title)

    return ax



class EDAVisualizer(Visualizer):
    """
    Class for all EDA visualizations, including:

        - bar_plot
        - box_plot
        - correlation_plot
        - distribution_plot
        - pie_plot
        - scatter_plot

    Examples
    --------
    >>> import matplotlib.pyplot as plt
    >>> f = plt.figure(figsize=(10,10))
    >>> ax = f.add_subplot(111)
    >>> eda = EDAVisualizer(ax=ax, enable_plotly=False)

    Parameters
    ----------

    ax : matplotlib.axes.Axes, optional
        The axes used to plot the figure. Only for matplotlib plot.

        Default value is current axes.
    size : tuple of integers, optional
        (width, height) of the plot in dpi. Only for matplotlib plot.

        Default value is the current size of the plot.
    cmap : matplotlib.pyplot.colormap, optional
        Color map used for the plot. Only for matplotlib plot.

        Defaults to None.
    enable_plotly : bool, optional
        Use plotly instead of matplotlib.

        Defaults to True.
    fig : Figure, optional
        Plotly's figure. Only valid when enable_plotly is True.

        Defaults to None.
    """

    def __init__(self, ax=None, size=None, cmap=None,
                 enable_plotly=True, fig=None, no_fig=False, show_plotly=True):
        super(EDAVisualizer, self).__init__(ax=ax, size=size, cmap=cmap,
                                            enable_plotly=enable_plotly, fig=fig, no_fig=no_fig,
                                            show_plotly=show_plotly)

    def distribution_plot(self, data, column, bins, title=None, x_axis_fontsize=10, #pylint: disable= too-many-locals, too-many-arguments
                          x_axis_rotation=0, debrief=False, rounding_precision=3, title_fontproperties=None,
                          replacena=0, x_axis_label="", y_axis_label="", subplot_pos=(1, 1), return_bin_data_only=False, **kwargs):
        """
        Displays a distribution plot for the SAP HANA DataFrame column specified.

        Parameters
        ----------
        data : DataFrame
            HANA DataFrame containing the data.

        column : str
            Column in the DataFrame being plotted.

        bins : int
            Number of bins to create based on the value of column.

        title : str, optional
            Title for the plot.

            Defaults to None.

        x_axis_fontsize : int, optional
            Size of x axis labels.

            Defaults to 10.

        x_axis_rotation : int, optional
            Rotation of x axis labels.

            Defaults to 0.

        debrief : bool, optional
            Whether to include the skewness debrief.

            Defaults to False.

        rounding_precision : int, optional
            The rounding precision for bin size.

            Defaults to 3.

        title_fontproperties : FontProperties, optional
            Change the font properties for title.

            Defaults to None.

        replacena : float, optional
            Replace na with the specified value.

            Defaults to 0.

        x_axis_label : str, optional
            x axis label. Only for plotly plot.

            Defaults to "".

        y_axis_label : str, optional
            y axis label. Only for plotly plot.

            Defaults to "".

        subplot_pos : tuple, optional
            (row, col) for plotly subplot. Only for plotly plot.

            Defaults to (1, 1).

        Returns
        -------
        matplotlib:

          - The axes for the plot, returns a matplotlib.axes.Axes object.
          - pandas.DataFrame. The data used in the plot.

        plotly:

          If ``enable_plotly`` is True:

          - plotly.graph_objects.Figure object of the distribution plot.
          - graph object trace. The trace of the plot, used in hist().
          - pandas.DataFrame. The data used in the plot.

        Examples
        --------
        Matplotlib:

        >>> import matplotlib.pyplot as plt
        >>> f = plt.figure(figsize=(35, 10))
        >>> ax = f.add_subplot(111)
        >>> eda = EDAVisualizer(ax=ax, enable_plotly=False)
        >>> ax, dist_data = eda.distribution_plot(data=df, column="FARE", bins=10, title="Distribution of FARE")

        .. image:: image/distribution_plot.png
           :align: center
           :width: 400px

        Plotly:

        >>> eda = EDAVisualizer(enable_plotly=True)
        >>> fig, trace, bin_data = eda.distribution_plot(data=df, column="FARE", bins=10, title="Distribution of FARE", width=600, height=400)

        .. image:: image/distribution_plotly.png
           :align: center
           :width: 400px

        """
        if self.enable_plotly:
            fig, trace, bin_data = eda_plotly.distribution_plot(data, column, bins, title=title, x_axis_label=x_axis_label, y_axis_label=y_axis_label,
                                                x_axis_fontsize=x_axis_fontsize, x_axis_rotation=x_axis_rotation, debrief=debrief,
                                                rounding_precision=rounding_precision, replacena=replacena, fig=self.fig,
                                                subplot_pos=subplot_pos, **kwargs)
            if self.show_plotly:
                fig.show()
            return fig, trace, bin_data
        else: # matplotlib
            bins = max(bins, 2)
            bins = bins - 1
            conn_context = data.connection_context
            data_ = data
            if replacena is not None:
                if data.hasna(cols=[column]):
                    data_ = data.fillna(value=replacena, subset=[column])
                    logger.warn("NULL values will be replaced by %s.", replacena)
            query = "SELECT MAX({}) FROM ({})".format(quotename(column), data_.select_statement)
            maxi = conn_context.sql(query).collect(geometries=False).values[0][0]
            query = "SELECT MIN({}) FROM ({})".format(quotename(column), data_.select_statement)
            mini = conn_context.sql(query).collect(geometries=False).values[0][0]
            if mini == maxi:
                maxi = mini + 1
            diff = maxi-mini
            bin_size = round(float(diff)/float(bins), rounding_precision)
            x_axis_ticks = [round(math.floor(mini / bin_size) \
                * bin_size + item * bin_size, rounding_precision) for item in range(0, bins + 1)]
            query = "SELECT {0}, ROUND(FLOOR({0}/{1}), {2}) AS BAND,".format(quotename(column), bin_size, rounding_precision)
            query += " '[' || ROUND(FLOOR({0}/{1})*{1}, {2}) || ', ".format(quotename(column), bin_size, rounding_precision)
            query += "' || ROUND((FLOOR({0}/{1})*{1})+{1}, {2}) || ')'".format(quotename(column), bin_size, rounding_precision)
            query += " AS BANDING FROM ({}) ORDER BY BAND ASC".format(data_.select_statement)
            bin_data = conn_context.sql(query)
            bin_data = bin_data.agg([('count', column, 'COUNT')], group_by='BANDING')
            bin_data = bin_data.collect(geometries=False)
            bin_data["BANDING"] = bin_data.apply(lambda x: float(x["BANDING"].split(',')[0].replace('[', '')), axis=1)
            for item in x_axis_ticks:
                if item not in bin_data["BANDING"].to_list():
                    if version_compare(pd.__version__, "1.4.0"):
                        bin_data = pd.concat([bin_data, pd.DataFrame({"BANDING": [item], "COUNT": [0]})], ignore_index=True)
                    else:
                        bin_data = bin_data.append({"BANDING": item, "COUNT": 0}, ignore_index=True)
            bin_data.sort_values(by="BANDING", inplace=True)
            if return_bin_data_only:
                return bin_data
            ax = self.ax
            ax.bar(x=x_axis_ticks, height=bin_data['COUNT'], width=0.8 * bin_size, align='edge', **kwargs)
            for item in [ax.xaxis.label] + ax.get_xticklabels():
                item.set_fontsize(x_axis_fontsize)
            ax.xaxis.set_tick_params(rotation=x_axis_rotation)
            if title is not None:
                if title_fontproperties is None:
                    ax.set_title(title)
                else:
                    ax.set_title(title, fontproperties=title_fontproperties)
            ax.grid(which="major", axis="y", color='black', linestyle='-', linewidth=1, alpha=0.2)
            ax.set_axisbelow(True)
            # Turn spines off
            ax.spines['right'].set_visible(False)
            ax.spines['left'].set_visible(False)
            ax.spines['top'].set_visible(False)
            if debrief:
                query = "SELECT (A.RX3 - 3*A.RX2*A.AV + 3*A.RX*A.AV*A.AV - "
                query += "A.RN*A.AV*A.AV*A.AV) / (A.STDV*A.STDV*A.STDV) * A.RN /"
                query += " (A.RN-1) / (A.RN-2) AS SKEWNESS FROM (SELECT SUM(1.0*{})".format(quotename(column))
                query += " AS RX, SUM(POWER(1.0*{},2)) AS RX2, ".format(quotename(column))
                query += "SUM(POWER(1.0*{},3))".format(quotename(column))
                query += " AS RX3, COUNT(1.0*{0}) AS RN, STDDEV(1.0*{0}) AS STDV,".format(quotename(column))
                query += " AVG(1.0*{0}) AS AV FROM ({1})) A".format(quotename(column), data_.select_statement)
                # Calculate skewness
                skewness = conn_context.sql(query)
                skewness = skewness.collect(geometries=False)['SKEWNESS'].values[0]
                ax.text(max(bin_data.index)*0.9, max(bin_data['COUNT'].values)*0.95,
                        'Skewness: {:.2f}'.format(skewness), style='italic',
                        bbox={'facecolor':'white', 'pad':0.65, 'boxstyle':'round'})
            else:
                pass
            # Turn off y ticks
            ax.yaxis.set_ticks_position('none')
            return ax, bin_data

    def pie_plot(self, data, column, explode=0.03, title=None, legend=True,
                 title_fontproperties=None, legend_fontproperties=None,
                 subplot_pos=(1, 1), **kwargs): #pylint: disable=too-many-arguments
        """
        Displays a pie plot for the SAP HANA DataFrame column specified.

        Parameters
        ----------
        data : DataFrame
            HANA DataFrame containing the data.

        column : str
            Column in the DataFrame being plotted.

        explode : float, optional
            Relative spacing between pie segments. Only for matplotlib plot.

            Defaults to 0.03.

        title : str, optional
            Title for the plot.

            Defaults to None.

        legend : bool, optional
            Whether to show the legend for the plot. Only for matplotlib plot.

            Defaults to True.

        title_fontproperties : FontProperties, optional
            Change the font properties for title. Only for matplotlib plot.

            Defaults to None.

        legend_fontproperties : FontProperties, optional
            Change the font properties for legend. Only for matplotlib plot.

            Defaults to None.

        subplot_pos : tuple, optional
            (row, col) for plotly subplot. Only for plotly plot.

            Defaults to (1, 1).

        Examples
        --------
        Matplotlib:

        >>> import matplotlib.pyplot as plt
        >>> f = plt.figure(figsize=(8, 8))
        >>> ax = f.add_subplot(111)
        >>> eda = EDAVisualizer(ax=ax, enable_plotly=False)
        >>> ax, pie_data = eda.pie_plot(data=df, column="PCLASS", title="% of passengers in each class")

        .. image:: image/pie_plot.png
           :align: center
           :width: 400px

        Plotly:

        >>> eda = EDAVisualizer(enable_plotly=True)
        >>> fig, pie_data = eda.pie_plot(data=df, column="PCLASS", title="% of passengers in each class", width=600, height=600)

        .. image:: image/pie_plotly.png
           :align: center
           :width: 400px

        Returns
        -------
        matplotlib:

          - The axes for the plot, returns a matplotlib.axes.Axes object.
          - pandas.DataFrame. The data used in the plot.

        plotly:

          If ``enable_plotly`` is True:

          - plotly.graph_objects.Figure object of the plot.
          - pandas.DataFrame. The data used in the plot.

        """
        if self.enable_plotly:
            fig, pie_data = eda_plotly.pie_plot(data, column, title=title, title_fontproperties=title_fontproperties, fig=self.fig,
                                       subplot_pos=subplot_pos, **kwargs)
            if self.show_plotly:
                fig.show()
            return fig, pie_data
        else:
            data = data.agg([('count', column, 'COUNT')], group_by=column).sort(column)
            pie_data = data.collect(geometries=False)
            explode = (explode,)*len(pie_data)
            ax = self.ax  #pylint: disable=invalid-name
            ax.pie(x=pie_data['COUNT'], explode=explode, labels=pie_data[column],
                   autopct='%1.1f%%', **kwargs)
            if legend:
                if legend_fontproperties is not None:
                    ax.legend(pie_data[column], loc='best', edgecolor='w', fontproperties=legend_fontproperties)
                else:
                    ax.legend(pie_data[column], loc='best', edgecolor='w')
            else:
                pass
            if title is not None:
                if title_fontproperties is not None:
                    ax.set_title(title, fontproperties=title_fontproperties)
                else:
                    ax.set_title(title)
            return ax, pie_data

    def correlation_plot(self, data, key=None, corr_cols=None, label=True, cmap=None, title="Pearson's correlation (r)", **kwargs): #pylint: disable=too-many-locals
        """
        Displays a correlation plot for the SAP HANA DataFrame columns specified.

        Parameters
        ----------
        data : DataFrame
            HANA DataFrame containing the data.

        key : str, optional
            Name of ID column.

            Defaults to None.
        corr_cols : list of str, optional
            Columns in the DataFrame being plotted. If None then all numeric columns will be plotted.

            Defaults to None.

        label : bool, optional
            Plot a colorbar. Only for matplotlib plot.

            Defaults to True.

        cmap : matplotlib.pyplot.colormap or str, optional
            Color map used for the plot.

            Defaults to "RdYlBu" for matplotlib and "blues" for plotly.

        title : str, optional
            Title of the plot.

            Defaults to "Pearson's correlation (r)".

        Returns
        -------
        matplotlib:

          - The axes for the plot, returns a matplotlib.axes.Axes object.
          - pandas.DataFrame. The data used in the plot.

        plotly:

          If ``enable_plotly`` is True:

          - plotly.graph_objects.Figure object of the plot.
          - pandas.DataFrame. The data used in the plot.

        Examples
        --------
        Matplotlib:

        >>> import matplotlib.pyplot as plt
        >>> f = plt.figure(figsize=(35, 10))
        >>> ax = f.add_subplot(111)
        >>> eda = EDAVisualizer(ax=ax, enable_plotly=False)
        >>> ax, corr = eda.correlation_plot(data=df, corr_cols=['PCLASS', 'AGE', 'SIBSP', 'PARCH', 'FARE'], cmap="Blues")

        .. image:: image/correlation_plot.png
           :align: center
           :width: 400px

        Plotly:

        >>> eda = EDAVisualizer(enable_plotly=True)
        >>> fig, _ = eda.correlation_plot(data=df, corr_cols=['PCLASS', 'AGE', 'SIBSP', 'PARCH', 'FARE'], cmap="Blues", width=600, height=600, title="correlation plot")

        .. image:: image/correlation_plotly.png
           :align: center
           :width: 400px

        """
        if self.enable_plotly:
            if not cmap:
                cmap = "blues"
            return eda_plotly.correlation_plot(data, key=key, corr_cols=corr_cols, cmap=cmap, title=title, **kwargs)
        else:
            if not cmap:
                cmap = "RdYlBu"
            if not isinstance(data, DataFrame):
                raise TypeError('Parameter data must be a DataFrame')
            if corr_cols is None:
                cols = data.columns
            else:
                cols = corr_cols
            message = 'Parameter corr_cols must be a string or a list of strings'
            if isinstance(cols, _STRING_TYPES):
                cols = [cols]
            if (not cols or not isinstance(cols, list) or
                    not all(isinstance(col, _STRING_TYPES) for col in cols)):
                raise TypeError(message)
            # Get only the numerics
            if len(cols) < 2:
                raise ValueError('Must have at least 2 correlation columns that are numeric')
            if key is not None and key in cols:
                cols.remove(key)
            cols = [i for i in cols if data.is_numeric(i)]
            data_ = data[cols]
            if data.hasna():
                data_wo_na = data_.dropna(subset=cols)
                corr = stats.pearsonr_matrix(data=data_wo_na,
                                             cols=cols).collect(geometries=False)
            else:
                corr = stats.pearsonr_matrix(data=data_,
                                             cols=cols).collect(geometries=False)
            corr = corr.set_index(list(corr.columns[[0]]))
            ax = self.ax  #pylint: disable=invalid-name
            cp = ax.matshow(corr, cmap=cmap, **kwargs) #pylint: disable=invalid-name
            for (i, j), z in np.ndenumerate(corr): #pylint: disable=invalid-name
                ax.text(j, i, '{:0.2f}'.format(z), ha='center', va='center')
            ticks = np.arange(0, len(cols), 1)
            ax.set_xticks(ticks)
            ax.xaxis.set_tick_params(rotation=90)
            ax.set_yticks(ticks)
            ax.set_xticklabels(cols)
            ax.set_yticklabels(cols)
            # Turn spines off and create white grid.
            for edge, spine in ax.spines.items(): #pylint: disable=unused-variable
                spine.set_visible(False)
            ticks = np.arange(0, len(cols), 1)-0.5
            ax.set_xticks(ticks, minor=True)
            ax.set_yticks(ticks, minor=True)
            ax.grid(which="minor", color="w", linestyle='-', linewidth=2)
            ax.tick_params(which="minor", bottom=False, right=False, left=False, top=False)
            ax.tick_params(which="both", bottom=False, right=False)
            if label:
                cb = ax.get_figure().colorbar(cp, ax=ax) #pylint: disable=invalid-name
                mpl_ver = matplotlib.__version__.split('.')
                if version_compare(matplotlib.__version__, "3.3.0"):
                    cb.mappable.set_clim(-1, 1)
                else:
                    cb.set_clim(-1, 1)
                cb.set_label(title)
            return ax, corr

    def scatter_plot(self, data, x, y, x_bins=None, y_bins=None, title=None, label=None, #pylint: disable=too-many-locals, too-many-arguments, too-many-statements, invalid-name
                     cmap=None, debrief=True, rounding_precision=3, label_fontsize=12,
                     title_fontproperties=None, sample_frac=1.0, **kwargs):
        """
        Displays a scatter plot for the SAP HANA DataFrame columns specified.

        Parameters
        ----------
        data : DataFrame
            HANA DataFrame containing the data.

        x : str
            Column to be plotted on the x axis.

        y : str
            Column to be plotted on the y axis.

        x_bins : int, optional
            Number of x axis bins to create based on the value of column.

            Defaults to None.

        y_bins : int
            Number of y axis bins to create based on the value of column.

            Defaults to None.

        title : str, optional
            Title for the plot.

            Defaults to None.

        label : str, optional
            Label for the color bar.

            Defaults to None.

        cmap : matplotlib.pyplot.colormap or str, optional
            Color map used for the plot.

            Defaults to "Blues" for matplotlib and "blues" for plotly.

        debrief : bool, optional
            Whether to include the correlation debrief.

            Defaults to True

        rounding_precision : int, optional
            The rounding precision for bin size. Only for matplotlib plot.

            Defaults to 3.

        label_fontsize : int, optional
            Change the font size for label. Only for matplotlib plot.

            Defaults to 12.

        title_fontproperties : FontProperties, optional
            Change the font properties for title.

            Defaults to None.

        sample_frac : float, optional
            Sampling method is applied to data. Valid if x_bins and y_bins are not set.

            Defaults to 1.0.

        Returns
        -------
        matplotlib:
          If enable_plotly is False:

          - The axes for the plot, returns a matplotlib.axes.Axes object.
          - pandas.DataFrame. The data used in the plot.

        plotly:

          If ``enable_plotly`` is True:

          - plotly.graph_objects.Figure object of the plot.

        Examples
        --------
        Matplotlib:

        >>> import matplotlib.pyplot as plt
        >>> f = plt.figure(figsize=(10, 10))
        >>> ax = f.add_subplot(111)
        >>> eda = EDAVisualizer(ax=ax, enable_plotly=False)
        >>> ax, corr = eda.scatter_plot(data=df, x="AGE", y="SIBSP", x_bins=5, y_bins=5)

        .. image:: image/scatter_plot.png
           :align: center
           :width: 400px

        Plotly:

        >>> eda = EDAVisualizer(enable_plotly=True)
        >>> fig = eda.scatter_plot(data=df, x="AGE", y="SIBSP", x_bins=5, y_bins=5, width=600, height=600)
        >>> fig.show()

        .. image:: image/scatter_plotly.png
           :align: center
           :width: 400px

        >>> f = plt.figure(figsize=(10, 10))
        >>> ax2 = f.add_subplot(111)
        >>> eda = EDAVisualizer(ax=ax2, enable_plotly=False)
        >>> ax2 = eda.scatter_plot(data=df, x="AGE", y="SIBSP", sample_frac=0.8, s=10, marker='o')

        .. image:: image/scatter_plot2.png
           :align: center
           :width: 400px

        Plotly:

        >>> eda = EDAVisualizer(enable_plotly=True)
        >>> fig = eda.scatter_plot(data=df, x="AGE", y="SIBSP", sample_frac=0.8, width=600, height=600)

        .. image:: image/scatter_plotly2.png
           :align: center
           :width: 400px

        """
        if self.enable_plotly:
            if not cmap:
                cmap = "blues"
            return eda_plotly.scatter_plot(data, x, y, x_bins=x_bins, y_bins=y_bins, title=title,
                                           cmap=cmap, debrief=debrief, sample_frac=sample_frac,
                                           title_fontproperties=title_fontproperties, **kwargs)
        else:
            if not cmap:
                cmap = "Blues"
            if x_bins is not None and y_bins is not None:
                if x_bins <= 1 or y_bins <= 1:
                    raise Error("bins size should be greater than 1")
                conn_context = data.connection_context
                x_max = "SELECT MAX({}) FROM ({})".format(quotename(x), data.select_statement)
                x_maxi = conn_context.sql(x_max).collect(geometries=False).values[0][0]
                x_min = "SELECT MIN({}) FROM ({})".format(quotename(x), data.select_statement)
                x_mini = conn_context.sql(x_min).collect(geometries=False).values[0][0]
                x_diff = x_maxi-x_mini
                x_bin_size = round(float(x_diff)/float(x_bins), rounding_precision)
                x_axis_ticks = [round(math.floor(x_mini / x_bin_size) * x_bin_size + item * x_bin_size, rounding_precision)for item in range(0, x_bins + 2)]
                y_max = "SELECT MAX({}) FROM ({})".format(quotename(y), data.select_statement)
                y_maxi = conn_context.sql(y_max).collect(geometries=False).values[0][0]
                y_min = "SELECT MIN({}) FROM ({})".format(quotename(y), data.select_statement)
                y_mini = conn_context.sql(y_min).collect(geometries=False).values[0][0]
                y_diff = y_maxi-y_mini
                y_bin_size = round(float(y_diff)/float(y_bins), rounding_precision)
                y_axis_ticks = [round(math.floor(y_mini / y_bin_size) * y_bin_size + item * y_bin_size, rounding_precision) for item in range(y_bins + 1, -1, -1)]
                query = "SELECT *, TO_DOUBLE(FLOOR({0}/{1})) AS BAND_X,".format(quotename(x), x_bin_size)
                query += "ROUND(TO_DOUBLE(FLOOR({0}/{1})*{1}), {2}) AS BANDING_X, ".format(quotename(x), x_bin_size, rounding_precision)
                query += "TO_DOUBLE(FLOOR({0}/{1})) AS BAND_Y, ".format(quotename(y), y_bin_size)
                query += "ROUND(TO_DOUBLE(FLOOR({0}/{1})*{1}), {2}) AS BANDING_Y ".format(quotename(y), y_bin_size, rounding_precision)
                query += "FROM ({})".format(data.select_statement)
                bin_data = conn_context.sql(query)
                bin_data = bin_data.agg([('count', x, 'COUNT'),
                                         ('avg', 'BAND_X', 'ORDER_X'),
                                         ('avg', 'BAND_Y', 'ORDER_Y')], group_by=['BANDING_X', 'BANDING_Y']).collect(geometries=False)
                bin_matrix = pd.crosstab(bin_data['BANDING_Y'],
                                         bin_data['BANDING_X'],
                                         values=bin_data['COUNT'],
                                         aggfunc='sum',
                                         dropna=False).sort_index(ascending=False)
                for axs in x_axis_ticks:
                    if axs not in bin_matrix.columns:
                        bin_matrix[axs] = [np.nan] * bin_matrix.shape[0]
                bin_matrix = bin_matrix[x_axis_ticks]
                bin_matrix = bin_matrix.reindex(y_axis_ticks)
                ax = self.ax  #pylint: disable=invalid-name
                cp = ax.imshow(bin_matrix, cmap=cmap, **kwargs) #pylint: disable=invalid-name
                ax.set_xticks(np.arange(-.5, len(x_axis_ticks) - 1, 1))
                ax.set_xticklabels(x_axis_ticks)
                ax.set_xlabel(x, fontdict={'fontsize':label_fontsize})
                ax.set_yticks(np.arange(.5, len(y_axis_ticks), 1))
                ax.set_yticklabels(y_axis_ticks)
                ax.set_ylabel(y, fontdict={'fontsize':label_fontsize})
                # Turn spines off and create white grid.
                for edge, spine in ax.spines.items(): #pylint: disable=unused-variable
                    spine.set_visible(False)
                ax.grid(which="minor", color="w", linestyle='-', linewidth=1)
                if debrief:
                    # Calculate correlation
                    corr = data.corr(x, y).collect(geometries=False).values[0][0]
                    at = AnchoredText('Correlation: {:.2f}'.format(corr) if corr is not None else 'Correlation: None',
                                      loc='upper right')
                    ax.add_artist(at)
                else:
                    pass
                if title is not None:
                    if title_fontproperties is not None:
                        ax.set_title(title, fontproperties=title_fontproperties)
                    else:
                        ax.set_title(title)
                if label is not None:
                    cb = ax.get_figure().colorbar(cp, ax=ax) #pylint: disable=invalid-name
                    cb.set_label(label)

                return ax, bin_matrix
            if x_bins is None and y_bins is None:
                if sample_frac < 1:
                    samp = Sampling(method='stratified_without_replacement', percentage=sample_frac)
                    sampled_data = None
                    if "ID" in data.columns:
                        sampled_data = samp.fit_transform(data=data, features=['ID']).select([x, y]).collect(geometries=False)
                    else:
                        sampled_data = samp.fit_transform(data=data.add_id("ID"), features=['ID']).select([x, y]).collect(geometries=False)
                else:
                    sampled_data = data.select([x, y]).collect(geometries=False)
                ax = self.ax #pylint: disable=invalid-name
                ax.scatter(x=sampled_data[x].to_numpy(),
                           y=sampled_data[y].to_numpy(),
                           cmap=cmap,
                           **kwargs)
                ax.set_xlabel(x, fontdict={'fontsize':label_fontsize})
                ax.set_ylabel(y, fontdict={'fontsize':label_fontsize})
                if debrief:
                    # Calculate correlation
                    corr = data.corr(x, y).collect(geometries=False).values[0][0]
                    at = AnchoredText('Correlation: {:.2f}'.format(corr) if corr is not None else 'Correlation: None',
                                      loc='upper right')
                    ax.add_artist(at)
                if title is not None:
                    if title_fontproperties is not None:
                        ax.set_title(title, fontproperties=title_fontproperties)
                    else:
                        ax.set_title(title)
                if label is not None:
                    cb = ax.get_figure().colorbar(cp, ax=ax) #pylint: disable=invalid-name
                    cb.set_label(label)
                return ax
            return False

    def bar_plot(self, data, column, aggregation, title=None, label_fontsize=12, title_fontproperties=None, orientation=None, **kwargs): #pylint: disable=too-many-branches, too-many-statements
        r"""
        Displays a bar plot for the SAP HANA DataFrame column specified.

        Parameters
        ----------
        data : DataFrame
            HANA DataFrame containing the data.

        column : str
            Column to be aggregated.

        aggregation : dict
            Aggregation conditions ('avg', 'count', 'max', 'min').

        title : str, optional
            Title for the plot.

            Defaults to None.

        label_fontsize : int, optional
            The size of label. Only for matplotlib plot.

            Defaults to 12.

        title_fontproperties : FontProperties, optional
            Change the font properties for title.

            Defaults to None.

        orientation : str, optional
            One of 'h' for horizontal or 'v' for vertical.

            Only valid when plotly plot is enabled.

            Defaults to 'v'.

        Returns
        -------
        matplotlib:

          - The axes for the plot, returns a matplotlib.axes.Axes object.
          - pandas.DataFrame. The data used in the plot.

        plotly:

          If ``enable_plotly`` is True:

          - plotly.graph_objects.Figure object of the plot.
          - pandas.DataFrame. The data used in the plot.

        Examples
        --------
        Matplotlib:

        >>> import matplotlib.pyplot as plt
        >>> f = plt.figure(figsize=(10,10))
        >>> ax = f.add_subplot(111)
        >>> eda = EDAVisualizer(ax=ax, enable_plotly=False)
        >>> ax, bar = eda.bar_plot(data=df, column="PCLASS", aggregation={'AGE':'avg'})

        .. image:: image/bar_plot.png
           :align: center
           :width: 400px

        Plotly:

        >>> eda = EDAVisualizer(enable_plotly=True)
        >>> fig, bar = eda.bar_plot(data=df, column="PCLASS", aggregation={'AGE':'avg'}, width=600, height=600, title="bar plot")

        .. image:: image/bar_plotly.png
           :align: center
           :width: 400px

        """
        if self.enable_plotly:
            fig, bar_data = eda_plotly.bar_plot(data, column, aggregation, title=title, orientation=orientation,
                                       title_fontproperties=title_fontproperties, **kwargs)
            if self.show_plotly:
                fig.show()
            return fig, bar_data
        else:
            if list(aggregation.values())[0] == 'count':
                data = data.agg([('count', column, 'COUNT')], group_by=column).sort(column)
                bar_data = data.collect(geometries=False)
                if len(bar_data.index) <= 20:
                    ax = self.ax #pylint: disable=invalid-name
                    ax.barh(bar_data[column].values.astype(str), bar_data['COUNT'].values, **kwargs)
                    for item in [ax.xaxis.label] + ax.get_xticklabels():
                        item.set_fontsize(10)
                    ax.set_ylabel(column, fontdict={'fontsize':label_fontsize})
                    ax.set_xlabel('COUNT', fontdict={'fontsize':label_fontsize})
                    if title is not None:
                        if title_fontproperties is not None:
                            ax.set_title(title, fontproperties=title_fontproperties)
                        else:
                            ax.set_title(title)
                    ax.grid(which="major", axis="x", color='black', linestyle='-',
                            linewidth=1, alpha=0.2)
                    ax.set_axisbelow(True)
                    # Turn spines off
                    ax.spines['right'].set_visible(False)
                    ax.spines['bottom'].set_visible(False)
                    ax.spines['top'].set_visible(False)
                    # Turn off y ticks
                    ax.xaxis.set_ticks_position('none')
                else:
                    ax = self.ax #pylint: disable=invalid-name
                    ax.bar(bar_data[column].values.astype(str), bar_data['COUNT'].values, **kwargs)
                    for item in [ax.xaxis.label] + ax.get_xticklabels():
                        item.set_fontsize(10)
                    ax.set_xlabel(column, fontdict={'fontsize':label_fontsize})
                    ax.set_ylabel('COUNT', fontdict={'fontsize':label_fontsize})
                    if title is not None:
                        if title_fontproperties is not None:
                            ax.set_title(title, fontproperties=title_fontproperties)
                        else:
                            ax.set_title(title)
                    ax.grid(which="major", axis="y", color='black', linestyle='-',
                            linewidth=1, alpha=0.2)
                    ax.set_axisbelow(True)
                    # Turn spines off
                    ax.spines['right'].set_visible(False)
                    ax.spines['left'].set_visible(False)
                    ax.spines['top'].set_visible(False)
                    # Turn off y ticks
                    ax.yaxis.set_ticks_position('none')
            elif list(aggregation.values())[0] == 'avg':
                data = data.agg([('avg', list(aggregation.keys())[0], 'AVG')],
                                group_by=column).sort(column)
                bar_data = data.collect(geometries=False)
                if len(bar_data.index) <= 20:
                    ax = self.ax #pylint: disable=invalid-name
                    ax.barh(bar_data[column].values.astype(str), bar_data['AVG'].values, **kwargs)
                    for item in [ax.xaxis.label] + ax.get_xticklabels():
                        item.set_fontsize(10)
                    ax.set_ylabel(column, fontdict={'fontsize':label_fontsize})
                    ax.set_xlabel('Average '+list(aggregation.keys())[0],
                                  fontdict={'fontsize':label_fontsize})
                    if title is not None:
                        if title_fontproperties is not None:
                            ax.set_title(title, fontproperties=title_fontproperties)
                        else:
                            ax.set_title(title)
                    ax.grid(which="major", axis="x", color='black', linestyle='-',
                            linewidth=1, alpha=0.2)
                    ax.set_axisbelow(True)
                    # Turn spines off
                    ax.spines['right'].set_visible(False)
                    ax.spines['bottom'].set_visible(False)
                    ax.spines['top'].set_visible(False)
                    # Turn off y ticks
                    ax.xaxis.set_ticks_position('none')
                else:
                    ax = self.ax #pylint: disable=invalid-name
                    ax.bar(bar_data[column].values.astype(str), bar_data['AVG'].values, **kwargs)
                    for item in [ax.xaxis.label] + ax.get_xticklabels():
                        item.set_fontsize(10)
                    ax.set_xlabel(column, fontdict={'fontsize':label_fontsize})
                    ax.set_ylabel('Average '+list(aggregation.keys())[0],
                                  fontdict={'fontsize':label_fontsize})
                    if title is not None:
                        if title_fontproperties is not None:
                            ax.set_title(title, fontproperties=title_fontproperties)
                        else:
                            ax.set_title(title)
                    ax.grid(which="major", axis="y", color='black', linestyle='-',
                            linewidth=1, alpha=0.2)
                    ax.set_axisbelow(True)
                    # Turn spines off
                    ax.spines['right'].set_visible(False)
                    ax.spines['left'].set_visible(False)
                    ax.spines['top'].set_visible(False)
                    # Turn off y ticks
                    ax.yaxis.set_ticks_position('none')
            elif list(aggregation.values())[0] == 'min':
                data = data.agg([('min', list(aggregation.keys())[0], 'MIN')],
                                group_by=column).sort(column)
                bar_data = data.collect(geometries=False)
                if len(bar_data.index) <= 20:
                    ax = self.ax #pylint: disable=invalid-name
                    ax.barh(bar_data[column].values.astype(str), bar_data['MIN'].values, **kwargs)
                    for item in [ax.xaxis.label] + ax.get_xticklabels():
                        item.set_fontsize(10)
                    ax.set_ylabel(column, fontdict={'fontsize':label_fontsize})
                    ax.set_xlabel('Min '+list(aggregation.keys())[0], fontdict={'fontsize':label_fontsize})
                    if title is not None:
                        if title_fontproperties is not None:
                            ax.set_title(title, fontproperties=title_fontproperties)
                        else:
                            ax.set_title(title)
                    ax.grid(which="major", axis="x", color='black', linestyle='-',
                            linewidth=1, alpha=0.2)
                    ax.set_axisbelow(True)
                    # Turn spines off
                    ax.spines['right'].set_visible(False)
                    ax.spines['bottom'].set_visible(False)
                    ax.spines['top'].set_visible(False)
                    # Turn off y ticks
                    ax.xaxis.set_ticks_position('none')
                else:
                    ax = self.ax #pylint: disable=invalid-name
                    ax.bar(bar_data[column].values.astype(str), bar_data['MIN'].values, **kwargs)
                    for item in [ax.xaxis.label] + ax.get_xticklabels():
                        item.set_fontsize(10)
                    ax.set_xlabel(column, fontdict={'fontsize':label_fontsize})
                    ax.set_ylabel('Min '+list(aggregation.keys())[0], fontdict={'fontsize':label_fontsize})
                    if title is not None:
                        if title_fontproperties is not None:
                            ax.set_title(title, fontproperties=title_fontproperties)
                        else:
                            ax.set_title(title)
                    ax.grid(which="major", axis="y", color='black', linestyle='-',
                            linewidth=1, alpha=0.2)
                    ax.set_axisbelow(True)
                    # Turn spines off
                    ax.spines['right'].set_visible(False)
                    ax.spines['left'].set_visible(False)
                    ax.spines['top'].set_visible(False)
                    # Turn off y ticks
                    ax.yaxis.set_ticks_position('none')
            elif list(aggregation.values())[0] == 'max':
                data = data.agg([('max', list(aggregation.keys())[0], 'MAX')],
                                group_by=column).sort(column)
                bar_data = data.collect(geometries=False)
                if len(bar_data.index) <= 20:
                    ax = self.ax #pylint: disable=invalid-name
                    ax.barh(bar_data[column].values.astype(str), bar_data['MAX'].values, **kwargs)
                    for item in [ax.xaxis.label] + ax.get_xticklabels():
                        item.set_fontsize(10)
                    ax.set_ylabel(column, fontdict={'fontsize':label_fontsize})
                    ax.set_xlabel('Max '+list(aggregation.keys())[0], fontdict={'fontsize':label_fontsize})
                    if title is not None:
                        if title_fontproperties is not None:
                            ax.set_title(title, fontproperties=title_fontproperties)
                        else:
                            ax.set_title(title)
                    ax.grid(which="major", axis="x", color='black', linestyle='-',
                            linewidth=1, alpha=0.2)
                    ax.set_axisbelow(True)
                    # Turn spines off
                    ax.spines['right'].set_visible(False)
                    ax.spines['bottom'].set_visible(False)
                    ax.spines['top'].set_visible(False)
                    # Turn off y ticks
                    ax.xaxis.set_ticks_position('none')
                else:
                    ax = self.ax #pylint: disable=invalid-name
                    ax.bar(bar_data[column].values.astype(str), bar_data['MAX'].values, **kwargs)
                    #for item in ([ax.xaxis.label] + ax.get_xticklabels()):
                    for item in [ax.xaxis.label] + ax.get_xticklabels():
                        item.set_fontsize(10)
                    ax.set_xlabel(column, fontdict={'fontsize':label_fontsize})
                    ax.set_ylabel('Max '+list(aggregation.keys())[0], fontdict={'fontsize':label_fontsize})
                    if title is not None:
                        if title_fontproperties is not None:
                            ax.set_title(title, fontproperties=title_fontproperties)
                        else:
                            ax.set_title(title)
                    ax.grid(which="major", axis="y", color='black', linestyle='-',
                            linewidth=1, alpha=0.2)
                    ax.set_axisbelow(True)
                    # Turn spines off
                    ax.spines['right'].set_visible(False)
                    ax.spines['left'].set_visible(False)
                    ax.spines['top'].set_visible(False)
                    # Turn off y ticks
                    ax.yaxis.set_ticks_position('none')
            return ax, bar_data

    def box_plot(self,
                 data,
                 column,
                 outliers=False,
                 title=None,
                 groupby=None,
                 lower_outlier_fence_factor=0,
                 upper_outlier_fence_factor=0,
                 title_fontproperties=None,
                 vert=False,
                 legend=True,
                 multiplier=1.5,
                 **kwargs): #pylint: disable=too-many-locals, too-many-arguments, too-many-branches, too-many-statements
        """
        Displays a box plot for the SAP HANA DataFrame column specified.

        Parameters
        ----------
        data : DataFrame
            HANA DataFrame containing the data.

        column : str
            Column in the DataFrame being plotted.

        outliers : bool
            Whether to plot suspected outliers and outliers.

            Defaults to False.

        title : str, optional
            Title for the plot.

            Defaults to None.

        groupby : str, optional
            Column to group by and compare.

            Defaults to None.

        lower_outlier_fence_factor : float, optional
            The lower bound of outlier fence factor.

            Defaults to 0.

        upper_outlier_fence_factor
            The upper bound of outlier fence factor.

            Defaults to 0.

        title_fontproperties : FontProperties, optional
            Change the font properties for title.

            Defaults to None.

        vert : bool, optional
            Vertical box plot if True.

            Defaults to False.

        legend : bool, optional
            Display legend if True. Only available for matplotlib.

            Defaults to True.

        multiplier : float, optional
            The multiplier used in the IQR test.

            Defaults to 1.5.

        Returns
        -------
        matplotlib:

          - The axes for the plot, returns a matplotlib.axes.Axes object.
          - pandas.DataFrame. The data used in the plot.

        plotly:

          If ``enable_plotly`` is True:

          - plotly.graph_objects.Figure object of the plot.
          - pandas.DataFrame. The data used in the plot.

        Examples
        --------
        Matplotlib:

        >>> import matplotlib.pyplot as plt
        >>> f = plt.figure(figsize=(10, 10))
        >>> ax = f.add_subplot(111)
        >>> eda = EDAVisualizer(ax=ax, enable_plotly=False)
        >>> ax, corr = eda.box_plot(data=df, column="AGE", vert=True, groupby="SEX")

        .. image:: image/box_plot.png
           :align: center
           :width: 400px

        Plotly:

        >>> eda = EDAVisualizer(enable_plotly=True)
        >>> fig, corr = eda.box_plot(data=df, column="AGE", groupby="SEX", vert=True, width=600, height=600, title="box plot")

        .. image:: image/box_plotly.png
           :align: center
           :width: 400px

        """
        if self.enable_plotly:
            fig, sta_table = eda_plotly.box_plot(data,
                                       column,
                                       outliers=outliers,
                                       title=title,
                                       groupby=groupby,
                                       lower_outlier_fence_factor=lower_outlier_fence_factor,
                                       upper_outlier_fence_factor=upper_outlier_fence_factor,
                                       title_fontproperties=title_fontproperties,
                                       fig=self.fig,
                                       vert=vert,
                                       multiplier=multiplier,
                                       **kwargs)
            if self.show_plotly:
                fig.show()
            return fig, sta_table
        else:
            conn_context = data.connection_context
            data = data.fillna(value='MISSING', subset=[groupby])
            if groupby is None:
                cont, _ = stats.univariate_analysis(data=data, cols=[column])
                sta_table = cont.collect(geometries=False)
                median = sta_table[sta_table["STAT_NAME"] == 'median'].iat[0, 2]
                mini = sta_table[sta_table["STAT_NAME"] == 'min'].iat[0, 2]
                maxi = sta_table[sta_table["STAT_NAME"] == 'max'].iat[0, 2]
                lq = sta_table[sta_table["STAT_NAME"] == 'lower quartile'].iat[0, 2]
                uq = sta_table[sta_table["STAT_NAME"] == 'upper quartile'].iat[0, 2]
                iqr = uq-lq
                suspected_upper_outlier_fence = uq + (multiplier * iqr)
                suspected_lower_outlier_fence = lq - (multiplier * iqr)
                suspected_upper_outlier_fence = suspected_upper_outlier_fence if suspected_upper_outlier_fence < maxi else maxi
                suspected_lower_outlier_fence = suspected_lower_outlier_fence if suspected_lower_outlier_fence > mini else mini
                upper_outlier_fence = suspected_upper_outlier_fence + upper_outlier_fence_factor * iqr
                lower_outlier_fence = suspected_lower_outlier_fence - lower_outlier_fence_factor * iqr
                # Create axis
                ax = self.ax #pylint: disable=invalid-name
                if vert:
                    ax.set_xticks(np.arange(0, 1, 1)+0.5)
                    ax.set_xticklabels([column])
                else:
                    ax.set_yticks(np.arange(0, 1, 1)+0.5)
                    ax.set_yticklabels([column])
                # Add vertical lines
                if vert:
                    ax.axhline(y=lower_outlier_fence, xmin=0.4, xmax=0.6,
                            color='black', linestyle=':', label='Outlier fence')
                    ax.axhline(y=suspected_lower_outlier_fence, xmin=0.4, xmax=0.6,
                            color='black', label='Suspected outlier fence')
                    ax.axhline(y=median, xmin=0.33, xmax=0.67, color='black',
                            linewidth=2, linestyle='--', label='Median')
                    ax.axhline(y=suspected_upper_outlier_fence, xmin=0.4, xmax=0.6,
                            color='black')
                    ax.axhline(y=upper_outlier_fence, xmin=0.4, xmax=0.6, color='black',
                            linestyle=':')
                    # Add horizontal lines
                    ax.vlines(x=0.5, ymin=suspected_lower_outlier_fence, ymax=lq)
                    ax.vlines(x=0.5, ymin=uq, ymax=suspected_upper_outlier_fence)
                    # Add box
                    ax.axhspan(ymin=lq, ymax=uq, xmin=0.35, xmax=0.65)
                else:
                    ax.axvline(x=lower_outlier_fence, ymin=0.4, ymax=0.6,
                            color='black', linestyle=':', label='Outlier fence')
                    ax.axvline(x=suspected_lower_outlier_fence, ymin=0.4, ymax=0.6,
                            color='black', label='Suspected outlier fence')
                    ax.axvline(x=median, ymin=0.33, ymax=0.67, color='black',
                            linewidth=2, linestyle='--', label='Median')
                    ax.axvline(x=suspected_upper_outlier_fence, ymin=0.4, ymax=0.6,
                            color='black')
                    ax.axvline(x=upper_outlier_fence, ymin=0.4, ymax=0.6, color='black',
                            linestyle=':')
                    # Add horizontal lines
                    ax.hlines(y=0.5, xmin=suspected_lower_outlier_fence, xmax=lq)
                    ax.hlines(y=0.5, xmin=uq, xmax=suspected_upper_outlier_fence)
                    # Add box
                    ax.axvspan(xmin=lq, xmax=uq, ymin=0.35, ymax=0.65)
                if outliers:
                    # Fetch and plot suspected outliers and true outliers
                    query = "SELECT DISTINCT({}) FROM ({})".format(quotename(column), data.select_statement)
                    query += " WHERE {} > {} ".format(quotename(column), suspected_upper_outlier_fence)
                    query += "OR {} < {}".format(quotename(column), suspected_lower_outlier_fence)
                    suspected_outliers = conn_context.sql(query)
                    n = 0 #pylint: disable=invalid-name
                    for i in suspected_outliers.collect(geometries=False).values:
                        if n == 0:
                            if vert:
                                ax.plot(0.5, i, 'o', color='grey', markersize=5, alpha=0.3,
                                        label='Suspected outlier')
                            else:
                                ax.plot(i, 0.5, 'o', color='grey', markersize=5, alpha=0.3,
                                        label='Suspected outlier')
                            n += 1 #pylint: disable=invalid-name
                        else:
                            if vert:
                                ax.plot(0.5, i, 'o', color='grey', markersize=5, alpha=0.3)
                            else:
                                ax.plot(i, 0.5, 'o', color='grey', markersize=5, alpha=0.3)
                    query = "SELECT DISTINCT({}) FROM ".format(quotename(column))
                    query += "({}) WHERE {} > ".format(data.select_statement, quotename(column))
                    query += "{} OR {} < {}".format(upper_outlier_fence, quotename(column), lower_outlier_fence)
                    outliers = conn_context.sql(query)
                    n = 0 #pylint: disable=invalid-name
                    for i in outliers.collect(geometries=False).values:
                        if n == 0:
                            if vert:
                                ax.plot(0.5, i, 'o', color='red', markersize=5, alpha=0.3,
                                        label='Outlier')
                            else:
                                ax.plot(i, 0.5, 'o', color='red', markersize=5, alpha=0.3,
                                        label='Outlier')
                            n += 1 #pylint: disable=invalid-name
                        else:
                            if vert:
                                ax.plot(0.5, i, 'o', color='red', markersize=5, alpha=0.3)
                            else:
                                ax.plot(i, 0.5, 'o', color='red', markersize=5, alpha=0.3)
                # Add legend
                if legend:
                    ax.legend(loc='upper right', edgecolor='w')
                # Turn spines off
                for i in ['top', 'bottom', 'right', 'left']:
                    ax.spines[i].set_visible(False)
                # Add gridlines
                ax.grid(which="major", axis="x", color='black', linestyle='-',
                        linewidth=1, alpha=0.2)
                ax.set_axisbelow(True)
                ax.xaxis.set_ticks_position('none')
                ax.yaxis.set_ticks_position('none')
                if title is not None:
                    if title_fontproperties is not None:
                        ax.set_title(title, fontproperties=title_fontproperties)
                    else:
                        ax.set_title(title)
            else:
                query = "SELECT DISTINCT({}) FROM ({})".format(quotename(groupby), data.select_statement)
                values = conn_context.sql(query).collect(geometries=False).values
                values = [i[0] for i in values]
                values = sorted(values)
                yticklabels = values
                tick_values = int(len(values)+1)
                ax = self.ax #pylint: disable=invalid-name
                if vert:
                    ax.set_xticks(np.arange(0, 1, 1/tick_values)+1/tick_values)
                else:
                    ax.set_yticks(np.arange(0, 1, 1/tick_values)+1/tick_values)
                if version_compare(matplotlib.__version__, "3.3.0"):
                    yticklabels = yticklabels + [""]
                if vert:
                    ax.set_xticklabels(yticklabels)
                else:
                    ax.set_yticklabels(yticklabels)
                sta_table = []
                median = []
                mini = []
                maxi = []
                lq = [] #pylint: disable=invalid-name
                uq = [] #pylint: disable=invalid-name
                iqr = []
                suspected_upper_outlier_fence = []
                suspected_lower_outlier_fence = []
                upper_outlier_fence = []
                lower_outlier_fence = []
                suspected_outliers = []
                outliers_pt = []
                for i in values:
                    data_groupby = data.filter("{} = '{}'".format(quotename(groupby), i))
                    # Get statistics
                    cont, _ = stats.univariate_analysis(data=data_groupby, cols=[column])
                    cont_fetch = cont.collect(geometries=False)
                    sta_table.append(cont_fetch)
                    median_val = cont_fetch[cont_fetch["STAT_NAME"] == 'median'].iat[0, 2]
                    median.append(median_val)
                    minimum = cont_fetch[cont_fetch["STAT_NAME"] == 'min'].iat[0, 2]
                    mini.append(minimum)
                    maximum = cont_fetch[cont_fetch["STAT_NAME"] == 'max'].iat[0, 2]
                    maxi.append(maximum)
                    low_quart = cont_fetch[cont_fetch["STAT_NAME"] == 'lower quartile'].iat[0, 2]
                    lq.append(low_quart)
                    upp_quart = cont_fetch[cont_fetch["STAT_NAME"] == 'upper quartile'].iat[0, 2]
                    uq.append(upp_quart)
                    int_quart_range = upp_quart-low_quart
                    iqr.append(int_quart_range)
                    sus_upp_out_fence = upp_quart+(1.5*int_quart_range)
                    sus_upp_out_fence = sus_upp_out_fence if sus_upp_out_fence < maximum else maximum
                    suspected_upper_outlier_fence.append(sus_upp_out_fence)
                    sus_low_out_fence = low_quart-(1.5*int_quart_range)
                    sus_low_out_fence = sus_low_out_fence if sus_low_out_fence > minimum else minimum
                    suspected_lower_outlier_fence.append(sus_low_out_fence)
                    upp_out_fence = sus_upp_out_fence + upper_outlier_fence_factor * int_quart_range
                    upper_outlier_fence.append(upp_out_fence)
                    low_out_fence = sus_low_out_fence - lower_outlier_fence_factor * int_quart_range
                    lower_outlier_fence.append(low_out_fence)
                    # Fetch and plot suspected outliers and true outliers
                    query = "SELECT DISTINCT({}) FROM ({}) ".format(quotename(column),
                                                                    data_groupby.select_statement)
                    query += "WHERE {} > {} ".format(quotename(column), sus_upp_out_fence)
                    query += "OR {} < {}".format(quotename(column), sus_low_out_fence)
                    suspected_outliers.append(list(conn_context.sql(query).collect(geometries=False).values))
                    query = "SELECT DISTINCT({}) FROM ({}) ".format(quotename(column),
                                                                    data_groupby.select_statement)
                    query += "WHERE {} > {} ".format(quotename(column), upp_out_fence)
                    query += "OR {} < {}".format(quotename(column), low_out_fence)
                    outliers_pt.append(list(conn_context.sql(query).collect(geometries=False).values))
                n = 0 #pylint: disable=invalid-name
                m = 1 #pylint: disable=invalid-name
                height = (1/len(values))/4
                while n < len(values):
                    # Plot vertical lines
                    if m == 1:
                        if vert:
                            ax.axhline(y=float(median[n]),
                                       xmin=(m/tick_values)-(height),
                                       xmax=(m/tick_values)+(height),
                                       color='black', linestyle='--',
                                       linewidth=2, label='Median')
                            ax.axhline(y=float(lower_outlier_fence[n]),
                                       xmin=(m/tick_values)-(height*0.5),
                                       xmax=(m/tick_values)+(height*0.5),
                                       color='black',
                                       linestyle=':', label='Outlier fence')
                            ax.axhline(y=float(suspected_lower_outlier_fence[n]),
                                       xmin=(m/tick_values)-(height*0.5),
                                       xmax=(m/tick_values)+(height*0.5), color='black',
                                       linestyle='-', label='Suspected outlier fence')
                        else:
                            ax.axvline(x=float(median[n]),
                                       ymin=(m/tick_values)-(height),
                                       ymax=(m/tick_values)+(height),
                                       color='black', linestyle='--',
                                       linewidth=2, label='Median')
                            ax.axvline(x=float(lower_outlier_fence[n]),
                                       ymin=(m/tick_values)-(height*0.5),
                                       ymax=(m/tick_values)+(height*0.5),
                                       color='black',
                                       linestyle=':', label='Outlier fence')
                            ax.axvline(x=float(suspected_lower_outlier_fence[n]),
                                       ymin=(m/tick_values)-(height*0.5),
                                       ymax=(m/tick_values)+(height*0.5), color='black',
                                       linestyle='-', label='Suspected outlier fence')
                    else:
                        if vert:
                            ax.axhline(y=float(median[n]),
                                       xmin=(m/tick_values)-(height),
                                       xmax=(m/tick_values)+(height),
                                       color='black', linestyle='--', linewidth=2)
                            ax.axhline(y=float(lower_outlier_fence[n]),
                                       xmin=(m/tick_values)-(height*0.5),
                                       xmax=(m/tick_values)+(height*0.5),
                                       color='black', linestyle=':')
                            ax.axhline(y=float(suspected_lower_outlier_fence[n]),
                                       xmin=(m/tick_values)-(height*0.5),
                                       xmax=(m/tick_values)+(height*0.5),
                                       color='black', linestyle='-')
                        else:
                            ax.axvline(x=float(median[n]), ymin=(m/tick_values)-(height),
                                       ymax=(m/tick_values)+(height),
                                       color='black', linestyle='--', linewidth=2)
                            ax.axvline(x=float(lower_outlier_fence[n]),
                                       ymin=(m/tick_values)-(height*0.5),
                                       ymax=(m/tick_values)+(height*0.5),
                                       color='black', linestyle=':')
                            ax.axvline(x=float(suspected_lower_outlier_fence[n]),
                                       ymin=(m/tick_values)-(height*0.5),
                                       ymax=(m/tick_values)+(height*0.5),
                                       color='black', linestyle='-')
                    if vert:
                        ax.axhline(y=float(suspected_upper_outlier_fence[n]),
                                   xmin=(m/tick_values)-(height*0.5),
                                   xmax=(m/tick_values)+(height*0.5),
                                   color='black', linestyle='-')
                        ax.axhline(y=float(upper_outlier_fence[n]),
                                   xmin=(m/tick_values)-(height*0.5),
                                   xmax=(m/tick_values)+(height*0.5),
                                   color='black', linestyle=':')
                    else:
                        ax.axvline(x=float(suspected_upper_outlier_fence[n]),
                                   ymin=(m/tick_values)-(height*0.5),
                                   ymax=(m/tick_values)+(height*0.5),
                                   color='black', linestyle='-')
                        ax.axvline(x=float(upper_outlier_fence[n]),
                                   ymin=(m/tick_values)-(height*0.5),
                                   ymax=(m/tick_values)+(height*0.5),
                                   color='black', linestyle=':')
                    n += 1 #pylint: disable=invalid-name
                    m += 1 #pylint: disable=invalid-name

                n = 0 #pylint: disable=invalid-name
                m = 1 #pylint: disable=invalid-name
                if vert:
                    ax.set_xlim([0, 1])
                else:
                    ax.set_ylim([0, 1])
                while n < len(values):
                    if vert:
                        ax.vlines(x=m/tick_values, ymin=suspected_lower_outlier_fence[n], ymax=lq[n])
                        ax.vlines(x=m/tick_values, ymin=uq[n], ymax=suspected_upper_outlier_fence[n])
                        # Add box
                        ax.axhspan(ymin=lq[n], ymax=uq[n], xmin=(m/tick_values)-(height*0.75),
                                   xmax=(m/tick_values)+(height*0.75))
                    else:
                        # Plot horizontal lines
                        ax.hlines(y=m/tick_values, xmin=suspected_lower_outlier_fence[n], xmax=lq[n])
                        ax.hlines(y=m/tick_values, xmin=uq[n], xmax=suspected_upper_outlier_fence[n])
                        # Add box
                        ax.axvspan(xmin=lq[n], xmax=uq[n], ymin=(m/tick_values)-(height*0.75),
                                ymax=(m/tick_values)+(height*0.75))
                    n += 1 #pylint: disable=invalid-name
                    m += 1 #pylint: disable=invalid-name
                if outliers:
                    n = 0 #pylint: disable=invalid-name
                    m = 1 #pylint: disable=invalid-name
                    l = 0 #pylint: disable=invalid-name
                    # Plot suspected outliers
                    while n < len(values):
                        data_points = suspected_outliers[n]
                        for i in data_points:
                            if l == 0:
                                if vert:
                                    ax.plot(m/tick_values, i, 'o', color='grey', markersize=5, alpha=0.3,
                                            label='Suspected outlier')
                                else:
                                    ax.plot(i, m/tick_values, 'o', color='grey', markersize=5, alpha=0.3,
                                            label='Suspected outlier')
                                l += 1
                            else:
                                if vert:
                                    ax.plot(m/tick_values, i, 'o', color='grey', markersize=5, alpha=0.3)
                                else:
                                    ax.plot(i, m/tick_values, 'o', color='grey', markersize=5, alpha=0.3)
                        n += 1
                        m += 1
                    n = 0
                    m = 1
                    l = 0
                    # Plot outliers
                    while n < len(values):
                        data_points = outliers_pt[n]
                        for i in data_points:
                            if l == 0:
                                if vert:
                                    ax.plot(m/tick_values, i, 'o', color='red', markersize=5,
                                            alpha=0.3, label='Outlier')
                                else:
                                    ax.plot(i, m/tick_values, 'o', color='red', markersize=5,
                                            alpha=0.3, label='Outlier')
                                l += 1 #pylint: disable=invalid-name
                            else:
                                if vert:
                                    ax.plot(m/tick_values, i, 'o', color='red', markersize=5, alpha=0.3)
                                else:
                                    ax.plot(i, m/tick_values, 'o', color='red', markersize=5, alpha=0.3)
                        n += 1 #pylint: disable=invalid-name
                        m += 1 #pylint: disable=invalid-name
                # Add legend
                if legend:
                    ax.legend(loc='upper right', edgecolor='w')
                # Turn spines off
                for i in ['top', 'bottom', 'right', 'left']:
                    ax.spines[i].set_visible(False)
                # Add gridlines
                ax.grid(which="major", axis="x", color='black', linestyle='-', linewidth=1, alpha=0.2)
                ax.set_axisbelow(True)
                ax.xaxis.set_ticks_position('none')
                ax.yaxis.set_ticks_position('none')
                if title is not None:
                    if title_fontproperties is not None:
                        ax.set_title(title, fontproperties=title_fontproperties)
                    else:
                        ax.set_title(title)
            return ax, sta_table

@deprecated("This method is deprecated. Please use dataset_report instead.")
class Profiler(): # pragma: no cover
    """
    A class to build a SAP HANA Profiler, including:

      - Variable descriptions
      - Missing values %
      - High cardinality %
      - Skewness
      - Numeric distributions
      - Categorical distributions
      - Correlations
      - High correlation warnings

    """

    def description(self, data, key, bins=20, missing_threshold=10, card_threshold=100, #pylint: disable=too-many-locals, too-many-arguments, too-many-branches, too-many-statements, no-self-use
                    skew_threshold=0.5, figsize=None):
        """
        Returns a SAP HANA profiler, including:

        - Variable descriptions
        - Missing values %
        - High cardinality %
        - Skewness
        - Numeric distributions
        - Categorical distributions
        - Correlations
        - High correlation warnings

        Parameters
        ----------
        data : DataFrame
            HANA DataFrame containing the data.

        key : str, optional
            Name of the key column in the DataFrame.

        bins : int, optional
            Number of bins for numeric distributions.

            Defaults to 20.

        missing_threshold : float
            Percentage threshold to display missing values.

            Defaults to 10.

        card_threshold : int
            Threshold for column to be considered with high cardinality.

            Defaults to 100.

        skew_threshold : float
            Absolute value threshold for column to be considered as highly skewed.

            Defaults to 0.5.

        figsize : tuple, optional
            Size of figure to be plotted. First element is width, second is height.

            Defaults to None.

        Note: categorical columns with cardinality warnings are not plotted.

        Returns
        -------
        The matplotlib axis of the profiler
        """
        conn_context = data.connection_context
        print("- Creating data description")
        number_variables = len(data.columns)
        number_observations = data.count()
        numeric = [i for i in data.columns if data.is_numeric(i)]
        categorical = [i[0] for i in data.dtypes() if (i[1] == 'NVARCHAR') or (i[1] == 'VARCHAR')]
        date = [i[0] for i in data.dtypes() if (i[1] == 'DATE') or (i[1] == 'TIMESTAMP')]
        print("- Counting missing values")
        # missing values
        warnings_missing = {}
        for i in data.columns:
            query = 'SELECT SUM(CASE WHEN {0} is NULL THEN 1 ELSE 0 END) AS "nulls" FROM ({1})'
            pct_missing = conn_context.sql(query.format(quotename(i), data.select_statement))
            pct_missing = pct_missing.collect(geometries=False).values[0][0]
            pct_missing = pct_missing/number_observations
            if pct_missing > missing_threshold/100:
                warnings_missing[i] = pct_missing
        print("- Judging high cardinality")
        # cardinality
        warnings_cardinality = {}
        warnings_constant = {}
        for i in data.columns:
            query = 'SELECT COUNT(DISTINCT {0}) AS "unique" FROM ({1})'
            cardinality = conn_context.sql(query.format(quotename(i), data.select_statement))
            cardinality = cardinality.collect(geometries=False).values[0][0]
            if cardinality > card_threshold:
                warnings_cardinality[i] = (cardinality/number_observations)*100
            elif cardinality == 1:
                warnings_constant[i] = data.collect(geometries=False)[i].unique()
        print("- Finding skewed variables")
        # Skewed
        warnings_skewness = {}
        cont, _ = stats.univariate_analysis(data=data, cols=numeric) #pylint: disable=unused-variable
        cont_fetch = cont.collect()
        for i in numeric:
            skewness = cont_fetch['STAT_VALUE']
            stat = 'STAT_NAME'
            val = 'skewness'
            var = 'VARIABLE_NAME'
            skewness = skewness.loc[(cont_fetch[stat] == val) & (cont_fetch[var] == i)]
            skewness = skewness.values[0]
            if abs(skewness) > skew_threshold:
                warnings_skewness[i] = skewness
            else:
                pass
        if key:
            if key in numeric:
                numeric.remove(key)
            elif key in categorical:
                categorical.remove(key)
            elif key in date:
                date.remove(key)
        for i in warnings_cardinality:
            if i in categorical:
                categorical.remove(i)
            else:
                pass
        rows = 4
        m = 0 #pylint: disable=invalid-name
        o = 0 #pylint: disable=invalid-name
        while o < len(numeric):
            if m <= 4:
                m += 1 #pylint: disable=invalid-name
            elif m > 4:
                rows += 2
                m = 0 #pylint: disable=invalid-name
                m += 1 #pylint: disable=invalid-name
            o += 1 #pylint: disable=invalid-name
        rows += 2
        rows += 1
        m = 0 #pylint: disable=invalid-name
        o = 0 #pylint: disable=invalid-name
        while o < len(categorical):
            if m <= 4:
                m += 1 #pylint: disable=invalid-name
            elif m > 4:
                rows += 2
                m = 0 #pylint: disable=invalid-name
                m += 1 #pylint: disable=invalid-name
            o += 1 #pylint: disable=invalid-name
        rows += 2
        rows += 1
        rows += 4
        # Make figure
        fig = plt.figure(figsize=(20, 40))
        ax1 = plt.subplot2grid((rows, 5), (0, 0), rowspan=1, colspan=5) #pylint: disable=unused-variable
        alignment = {'horizontalalignment': 'left', 'verticalalignment': 'baseline'}
        t = plt.text(0, 0, "Data Description", fontdict={'size':30, 'fontweight':'bold'}, #pylint: disable=unused-variable, invalid-name
                     **alignment)
        plt.axis('off')
        # Data description
        ax2 = plt.subplot2grid((rows, 5), (1, 0), rowspan=2, colspan=1)
        labels = "Numeric", "Categorical", "Date"
        sizes = [len([i for i in data.columns if data.is_numeric(i)]),
                 len([i[0] for i in data.dtypes() if (i[1] == 'NVARCHAR') or (i[1] == 'VARCHAR')]),
                 len([i[0] for i in data.dtypes() if (i[1] == 'DATE') or (i[1] == 'TIMESTAMP')])]
        ax2.barh(labels[::-1], sizes[::-1])
        ax2.grid(which="major", axis="x", color='black', linestyle='-', linewidth=1, alpha=0.2)
        ax2.set_axisbelow(True)
        ax2.spines['right'].set_visible(False)
        ax2.spines['bottom'].set_visible(False)
        ax2.spines['top'].set_visible(False)
        ax2.xaxis.set_ticks_position('none')
        ax2.set_title("Variable Types")
        # Missing values
        ax3 = plt.subplot2grid((rows, 5), (1, 1), rowspan=2, colspan=1)
        labels = list(warnings_missing.keys())
        sizes = list(warnings_missing.values())
        ax3.barh(labels[::-1], sizes[::-1])
        ax3.grid(which="major", axis="x", color='black', linestyle='-', linewidth=1, alpha=0.2)
        ax3.set_axisbelow(True)
        ax3.spines['right'].set_visible(False)
        ax3.spines['bottom'].set_visible(False)
        ax3.spines['top'].set_visible(False)
        ax3.xaxis.set_ticks_position('none')
        ax3.set_title("Missing Values %")
        # High cardinality
        ax4 = plt.subplot2grid((rows, 5), (1, 2), rowspan=2, colspan=1)
        labels = list(warnings_cardinality.keys())
        sizes = list(warnings_cardinality.values())
        ax4.barh(labels[::-1], sizes[::-1])
        ax4.grid(which="major", axis="x", color='black', linestyle='-', linewidth=1, alpha=0.2)
        ax4.set_axisbelow(True)
        ax4.spines['right'].set_visible(False)
        ax4.spines['bottom'].set_visible(False)
        ax4.spines['top'].set_visible(False)
        ax4.xaxis.set_ticks_position('none')
        ax4.set_title("High cardinality %")
        # Skewed variables
        ax5 = plt.subplot2grid((rows, 5), (1, 3), rowspan=2, colspan=1)
        labels = list(warnings_skewness.keys())
        sizes = list(warnings_skewness.values())
        ax5.barh(labels[::-1], sizes[::-1])
        ax5.grid(which="major", axis="x", color='black', linestyle='-', linewidth=1, alpha=0.2)
        ax5.set_axisbelow(True)
        ax5.spines['right'].set_visible(False)
        ax5.spines['bottom'].set_visible(False)
        ax5.spines['top'].set_visible(False)
        ax5.xaxis.set_ticks_position('none')
        ax5.set_title("Highly skewed variables")
        # Description summary
        ax6 = plt.subplot2grid((rows, 5), (1, 4), rowspan=2, colspan=1)
        alignment = {'horizontalalignment': 'left', 'verticalalignment': 'baseline'}
        t = plt.text(0, 0.7, "Data description summary", fontweight='bold', #pylint: disable=invalid-name
                     size=12, **alignment)
        text = "-  There are {} variables and {} rows in this dataset"
        t = plt.text(0, 0.6, text.format(number_variables, number_observations), **alignment) #pylint: disable=invalid-name
        high_missing_values_pct = [i for i in warnings_missing.values() if i > 0.1]
        if warnings_missing:
            text = "-  There are {} variables with a high % of missing values"
            t = plt.text(0, 0.5, text.format(len(high_missing_values_pct)), **alignment) #pylint: disable=invalid-name
        else:
            t = plt.text(0, 0.5, "-  No missing values", **alignment) #pylint: disable=invalid-name
        if warnings_constant:
            text = "-  {} variables are constant: [{}]"
            t = plt.text(0, 0.4, text.format(len(warnings_constant), #pylint: disable=invalid-name
                                             list(warnings_constant.keys())), **alignment)
        else:
            text = "-  {} variables have high cardinality and 0 are constant"
            t = plt.text(0, 0.4, text.format(len(list(warnings_cardinality.keys()))), #pylint: disable=invalid-name
                         **alignment)
        if warnings_skewness:
            text = "-  {} variables are skewed, consider transformation"
            ax6.text(0, 0.3, text.format(len(warnings_skewness)), **alignment)
        else:
            ax6.text(0, 0.3, "-  No variables are skewed", **alignment)
        plt.axis('off')
        ax7 = plt.subplot2grid((rows, 5), (3, 0), rowspan=1, colspan=5) #pylint: disable=unused-variable
        alignment = {'horizontalalignment': 'left', 'verticalalignment': 'baseline'}
        t = plt.text(0, 0, "Numeric Distributions", #pylint: disable=invalid-name
                     fontdict={'size':30, 'fontweight':'bold'}, **alignment)
        plt.axis('off')
        # Numeric distributions
        print("- Calculating numeric distributions")
        n = 4 #pylint: disable=invalid-name
        m = 0 #pylint: disable=invalid-name
        o = 0 #pylint: disable=invalid-name
        for i in numeric:
            if m <= 4:
                ax = plt.subplot2grid((rows, 5), (n, m), rowspan=2, colspan=1) #pylint: disable=invalid-name
                eda = EDAVisualizer(ax)
                ax, dist_data = eda.distribution_plot(data=data, column=i, bins=bins, #pylint: disable=invalid-name, unused-variable
                                                      title="Distribution of {}".format(i),
                                                      debrief=False)
                m += 1 #pylint: disable=invalid-name
            elif m > 4:
                n += 2 #pylint: disable=invalid-name
                m = 0 #pylint: disable=invalid-name
                ax = plt.subplot2grid((rows, 5), (n, m), rowspan=2, colspan=1) #pylint: disable=invalid-name, unused-variable
                eda = EDAVisualizer(ax)
                ax, dist_data = eda.distribution_plot(data=data, column=i, #pylint: disable=invalid-name
                                                      bins=bins,
                                                      title="Distribution of {}".format(i),
                                                      debrief=False)
                m += 1 #pylint: disable=invalid-name
            o += 1 #pylint: disable=invalid-name
        n += 2 #pylint: disable=invalid-name
        ax8 = plt.subplot2grid((rows, 5), (n, 0), rowspan=1, colspan=5) #pylint: disable=unused-variable
        alignment = {'horizontalalignment': 'left', 'verticalalignment': 'baseline'}
        t = plt.text(0, 0, "Categorical Distributions", #pylint: disable=invalid-name
                     fontdict={'size':30, 'fontweight':'bold'}, **alignment)
        plt.axis('off')
        n += 1 #pylint: disable=invalid-name
        # Categorical distributions
        print("- Calculating categorical distributions")
        m = 0 #pylint: disable=invalid-name
        o = 0 #pylint: disable=invalid-name
        for i in categorical:
            if m <= 4:
                ax = plt.subplot2grid((rows, 5), (n, m), rowspan=2, colspan=1) #pylint: disable=invalid-name
                eda = EDAVisualizer(ax)
                ax, pie_data = eda.pie_plot(data=data, column=i, title="% of {}".format(i), #pylint: disable=invalid-name, unused-variable
                                            legend=False)
                m += 1 #pylint: disable=invalid-name
            elif m > 4:
                n += 2 #pylint: disable=invalid-name
                m = 0 #pylint: disable=invalid-name
                ax = plt.subplot2grid((rows, 5), (n, m), rowspan=2, colspan=1) #pylint: disable=invalid-name
                eda = EDAVisualizer(ax)
                ax, pie_data = eda.pie_plot(data=data, column=i, title="% of {}".format(i), #pylint: disable=invalid-name
                                            legend=False)
                m += 1 #pylint: disable=invalid-name
            o += 1 #pylint: disable=invalid-name
        n += 2 #pylint: disable=invalid-name
        ax9 = plt.subplot2grid((rows, 5), (n, 0), rowspan=1, colspan=5) #pylint: disable=unused-variable
        alignment = {'horizontalalignment': 'left', 'verticalalignment': 'baseline'}
        t = plt.text(0, 0, "Data Correlations", fontdict={'size':30, 'fontweight':'bold'}, #pylint: disable=invalid-name
                     **alignment)
        plt.axis('off')
        n += 1 #pylint: disable=invalid-name
        # Correlation plot
        print("- Calculating correlations")
        ax10 = plt.subplot2grid((rows, 5), (n, 0), rowspan=4, colspan=3)
        eda = EDAVisualizer(ax10)
        ax10, corr = eda.correlation_plot(data=data, corr_cols=numeric, label=True)
        warnings_correlation = {}
        if len(numeric) > 1:
            if data.hasna():
                logger.warn("NULL values will be dropped.")
            for i, col in enumerate(numeric): #pylint: disable=unused-variable
                for j in range(i+1, len(numeric)):
                    dfc = stats.pearsonr_matrix(data=data.dropna(),
                                                cols=[numeric[i], numeric[j]]).collect(geometries=False)
                    dfc = dfc.iloc[1, 1].values
                    if (i != j) and (abs(dfc) > 0.3):
                        warnings_correlation[numeric[i], numeric[j]] = dfc
                    else:
                        pass
        ax11 = plt.subplot2grid((rows, 5), (n, 3), rowspan=4, colspan=2) #pylint: disable=unused-variable
        alignment = {'horizontalalignment': 'left', 'verticalalignment': 'baseline'}
        t = plt.text(0, 0.8, "Data correlations summary", fontweight='bold', size=20, **alignment) #pylint: disable=invalid-name
        text = "There are {} pair(s) of variables that are show significant correlation"
        t = plt.text(0, 0.7, text.format(len(warnings_correlation), **alignment)) #pylint: disable=invalid-name
        n = 0.7 #pylint: disable=invalid-name
        m = 1 #pylint: disable=invalid-name
        for i in warnings_correlation:
            corr = warnings_correlation.get(i)
            if abs(corr) >= 0.5:
                v = n-(m*0.05) #pylint: disable=invalid-name
                text = "-  {} and {} are highly correlated, p = {:.2f}"
                t = plt.text(0, v, text.format(i[0], i[1], warnings_correlation.get(i)), #pylint: disable=invalid-name
                             **alignment)
                m += 1 #pylint: disable=invalid-name
            elif 0.3 <= abs(corr) < 0.5:
                v = n-(m*0.05) #pylint: disable=invalid-name
                text = "-  {} and {} are moderately correlated, p = {:.2f}"
                t = plt.text(0, v, text.format(i[0], i[1], warnings_correlation.get(i)), #pylint: disable=invalid-name
                             **alignment)
                m += 1 #pylint: disable=invalid-name
            else:
                pass
        plt.axis('off')
        if isinstance(figsize, tuple):
            a, b = figsize #pylint: disable=invalid-name
            plt.figure(figsize=(a, b))
        plt.tight_layout()
        plt.close()
        print("\n ---> Profiler is ready to plot, run the " +
              "returned figure to display the results.")
        return fig

    def set_size(self, fig, figsize):#pylint: disable=no-self-use
        """
        Set the size of the data description plot, in inches.

        Parameters
        ----------
        fig : ax
            The returned axes constructed by the description method.

        figsize : tuple
            Tuple of width and height for the plot.
        """
        while True:
            if isinstance(figsize, tuple):
                fig.set_figwidth(figsize[0])
                fig.set_figheight(figsize[1])
                print("Axes size set: width = {0}, height = {1} inches".format(figsize[0],
                                                                               figsize[1]))
                print("\n ---> Profiler is ready to plot, run the " +
                      "returned figure to display the results.")
            else:
                print("Please enter a tuple for figsize.")
            break
