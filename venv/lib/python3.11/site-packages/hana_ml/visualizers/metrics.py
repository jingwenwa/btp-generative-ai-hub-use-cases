"""
This module represents a visualizer for metrics.

The following class is available:

    * :class:`MetricsVisualizer`

"""
#pylint: disable=too-many-lines, line-too-long, too-many-arguments, super-with-arguments
#pylint: disable=consider-using-f-string
#pylint: disable=bare-except
import logging
import itertools
import numpy as np
try:
    import plotly.figure_factory as ff
except:
    pass
try:
    import matplotlib.pyplot as plt
except:
    pass
from hana_ml.ml_base import quotename
from hana_ml.visualizers.visualizer_base import Visualizer

logger = logging.getLogger(__name__) #pylint: disable=invalid-name

def _get_confusion_matrix_as_nparray(df): #pylint: disable=invalid-name
    classes = df.distinct(df.columns[0]).collect().values.flatten()
    label = df.columns[0]
    score = df.columns[1]
    value = df.columns[2]
    confusion_matrix = list()
    for y_lo in classes:
        tem_xlo = list()
        for x_lo in classes:
            tem_xlo.append(df.filter("{}='{}'".format(quotename(label), x_lo))\
                           .filter("{}='{}'".format(quotename(score), y_lo))\
                           .select(value).collect().iat[0, 0])
        confusion_matrix.append(tem_xlo)
    return classes, np.array(confusion_matrix)

class MetricsVisualizer(Visualizer, object):
    """
    The MetricVisualizer is used to visualize metrics.

    Parameters
    ----------
    ax : matplotlib.Axes, optional
        The axes to use to plot the figure.

        Default value : Current axes

    size : tuple of integers, optional
        (width, height) of the plot in dpi

        Default value: Current size of the plot.

    cmap : matplotlib.pyplot.colormap or str, optional
        Color map used for the plot.

        Defaults to None.

    title : str, optional
        Title for the plot.

        Defaults to None.

    enable_plotly : bool, optional
        Use plotly instead of matplotlib.

        Defaults to True.

    """
    def __init__(self, ax=None, size=None, cmap=None, title=None, enable_plotly=True, show_plotly=True):
        super(MetricsVisualizer, self).__init__(ax=ax, size=size, cmap=cmap,
                                                enable_plotly=enable_plotly,
                                                show_plotly=show_plotly)
        self.title = title

    def plot_confusion_matrix(self, df, normalize=False, rotate=False, **kwargs): #pylint: disable=invalid-name
        """
        This function plots the confusion matrix and returns the Axes where
        this is drawn.

        Parameters
        ----------
        df : DataFrame
            Data points to the resulting confusion matrix.
            This dataframe's columns should match columns ('CLASS', '')

        normalize : bool, optional
            Whether to normalize the input data.

            Defaults to False.
        """
        classes, confusion_matrix = _get_confusion_matrix_as_nparray(df)
        if normalize:
            confusion_matrix = (confusion_matrix.astype('float') /
                                confusion_matrix.sum(axis=1)[:, np.newaxis])
        if self.enable_plotly:
            x_label = "Predicted label"
            y_label = "True label"
            if rotate:
                confusion_matrix = confusion_matrix.T
                y_label = "Predicted label"
                x_label = "True label"
            class_text = [[str(yy) for yy in xx] for xx in confusion_matrix]
            params = {**kwargs}
            if 'annotation_text' not in params:
                params['annotation_text'] = class_text
            if 'colorscale' not in params:
                params['colorscale'] = 'Blues'
            fig = ff.create_annotated_heatmap(z=confusion_matrix.tolist(), x=list(classes), y=list(classes), **params)
            if self.title:
                fig.update_layout(title_text='<i><b>{}</b></i>'.format(self.title))
            fig.add_annotation(dict(font=dict(color="black", size=14),
                                    x=0.5,
                                    y=-0.15,
                                    showarrow=False,
                                    text=x_label,
                                    xref="paper",
                                    yref="paper"))
            fig.add_annotation(dict(font=dict(color="black", size=14),
                                    x=-0.15,
                                    y=0.5,
                                    showarrow=False,
                                    text=y_label,
                                    textangle=-90,
                                    xref="paper",
                                    yref="paper"))
            fig.update_layout(margin=dict(t=50, l=200))
            if self.show_plotly:
                fig.show()
            return fig
        else:
            ax = self.ax #pylint: disable=invalid-name
            #ax.imshow(cm, interpolation='nearest', cmap=self.cmap)
            # This is incorrect.  We need to use methods in Axes.
            x_label = "True label"
            y_label = "Predicted label"
            if not rotate:
                confusion_matrix = confusion_matrix.T
                classes = np.array(classes).T.tolist()
                x_label = "Predicted label"
                y_label = "True label"
            plt.imshow(confusion_matrix, interpolation='nearest', cmap=self.cmap, **kwargs)
            plt.colorbar(ax=self.ax)
            tick_marks = np.arange(len(classes))
            ax.set_xticks(tick_marks)
            ax.set_yticks(tick_marks)
            ax.set_xticklabels(classes, rotation=45)
            ax.set_yticklabels(classes)
            fmt = '.2f' if normalize else 'd'
            thresh = confusion_matrix.max() / 2.
            # Need to remove the hard coding of the text colors
            for i, j in itertools.product(range(confusion_matrix.shape[0]),
                                          range(confusion_matrix.shape[1])):
                ax.text(j, i, format(confusion_matrix[i, j], fmt),
                        horizontalalignment="center",
                        color="white" if confusion_matrix[i, j] > thresh else "black")

            #plt.tight_layout()
            ax.set_xlabel(x_label)
            ax.set_ylabel(y_label)
            if self.title:
                ax.set_title(self.title)
            return ax
