r"""
This module provides a unified Python interface for outlier detection using
`outlier profiling` defined via several PAL implemented algorithms.

The following classes are available:

    * :class:`OutlierProfiling`
"""
#pylint: disable=too-many-arguments,too-many-instance-attributes, too-many-locals, too-many-statements, too-many-branches
#pylint: disable=too-many-lines, line-too-long, relative-beyond-top-level, attribute-defined-outside-init, invalid-name
#pylint: disable=c-extension-no-member, unused-import, exec-used, unused-argument, dangerous-default-value
#pylint: disable=duplicate-string-formatting-argument
import logging
from hana_ml.ml_base import quotename
from .clustering import KMeansOutlier, DBSCAN
from .svm import OneClassSVM
from .stats import grubbs_test, iqr
from .preprocessing import IsolationForest
from .pal_base import PALBase, pal_param_register, ListOfStrings
logger = logging.getLogger(__name__) #pylint: disable=invalid-name

class OutlierProfiling(PALBase):
    r"""
    This class provides a way to detect outliers within dataset from multiple perspective,
    through an **outlier profiling**. Namely, an **outlier profiling** is a parametric
    specification of multiple hana-ml classes/functionsfor outlier detection. It is defined by
    the detection methods used, together with each method's parameter specification.

    Parameters
    ----------
    profiling : dict or str, optional
        Specifies the outlier profile in a dictionary, and for each key-value pair in that dictionary:

          - key is the hana-ml classes/functions for outlier detection in str format. Valid options include:

            - **'KMeansOutlier'** : see :class:`~hana_ml.algorithms.pal.clustering.KMeansOutlier`
            - **'grubbs_test'** : for :func:`grubbs_test`, see :func:`~hana_ml.algorithms.pal.stats.grubbs_test`
            - **'iqr'** : see :func:`~hana_ml.algorithms.pal.stats.iqr`
            - **'DBSCAN'** : see :class:`~hana_ml.algorithms.pal.clustering.DBSCAN`
            - **'OneClassSVM'** : see :class:`~hana_ml.algorithms.pal.svm.OneClassSVM`
            - **'IsolationForest'** : see :class:`~hana_ml.algorithms.pal.preprocessing.IsolationForest`

          - value is the parameter setting for the hana-ml classes/functions defined by the corresponding key,
            also in a dictionary format, with keys being parameter names and values being parameter values.
            For each hana-ml classes/functions, its valid parameters are listed as follows:
              - :class:`~hana_ml.algorithms.pal.clustering.KMeansOutlier`:
                ``n_clusters``, ``distance_level``, ``contamination``,
                ``sum_distance``, ``init``, ``max_iter``, ``normalization``, ``tol``, ``distance_threshold``
              - :func:`~hana_ml.algorithms.pal.stats.grubbs_test`: ``method``, ``alpha``
              - :func:`~hana_ml.algorithms.pal.stats.iqr`: ``multiplier``
              - :class:`~hana_ml.algorithms.pal.clustering.DBSCAN`: ``minpts``, ``eps``, ``metric``,
                ``minkowski_power``, ``algorithm``
              - :class:`~hana_ml.algorithms.pal.svm.OneClassSVM`: ``c``, ``kernel``, ``degree``,
                ``gamma``, ``coef_lin``, ``coef_const``, ``shrink``, ``tol``, ``nu``,
                ``scale_info``, ``handle_missing``
              - :class:`~hana_ml.algorithms.pal.preprocessing.IsolationForest`: ``n_estimators``,
                ``max_samples``, ``max_features``, ``bootstrap``, ``random_state``, ``contamination``

            .. note ::
                ``contamination`` is not a parameter in the initialization method of class **IsolationForest**,
                but one in the `predict()` method, so please see the `predict()` method of **IsolationForest** for
                its description.

        One can also use 'default' to set profiling the default one.

        Defaults to 'default'.

    Attributes
    ----------
    prifiling : dict
        Stores the profiling used for outlier detection.
    """
    all_algorithms = ['KMeansOutlier', 'DBSCAN',
                      'OneClassSVM', 'IsolationForest',
                      'grubbs_test', 'iqr']
    valid_params = {
        'KMeansOutlier': [
            'n_clusters', 'distance_level', 'contamination', 'sum_distance',
            'init', 'max_iter', 'normalization', 'tol', 'distance_threshold'
        ],
        'grubbs_test': ['method', 'alpha'],
        'iqr': ['multiplier'],
        'DBSCAN': ['minpts', 'eps', 'metric', 'minkowski_power', 'algorithm'],
        'OneClassSVM': [
            'c', 'kernel', 'degree', 'gamma', 'coef_lin', 'coef_const',
            'shrink', 'tol', 'nu', 'scale_info', 'handle_missing'
        ],
        'IsolationForest': [
            'n_estimators', 'max_samples', 'max_features',
            'bootstrap', 'random_state', 'contamination'
        ]
    }
    label_column = {'DBSCAN':'CLUSTER_ID',
                    'OneClassSVM':'SCORE',
                    'IsolationForest':'LABEL'}
    default_profiling = {'KMeansOutlier':{'contamination':0.1},
                         'DBSCAN':{'minpts': None, 'eps':None},
                         'OneClassSVM':{'kernel':'rbf', 'nu':0.1},
                         'IsolationForest':{'contamination':0.1},
                         'grubbs_test':{'method':'two_sides'},
                         'iqr':{'multiplier':1.5}}
    light_profiling = {'KMeansOutlier':{'contamination':0.1},
                       'DBSCAN':{'minpts': None, 'eps':None},
                       'IsolationForest':{'contamination':0.1}}

    def __init__(self,
                 profiling='default'):
        super(OutlierProfiling, self).__init__()
        setattr(self, 'hanaml_parameters', pal_param_register())
        self.profiling = None
        if isinstance(profiling, str):
            if profiling == 'default':
                self.profiling = self.default_profiling
            elif profiling == 'light':
                self.profiling = self.light_profiling
            else:
                raise ValueError("profiling can only be either 'default' or 'light'.")
        else:
            self.profiling = self._arg('profiling', profiling, dict)
            for alg in self.profiling:
                if alg not in self.all_algorithms:
                    msg = ("'{}' is not a valid method for outlier detection ".format(alg)+
                           "in outlier profiling.")
                    logger.error(msg)
                    raise ValueError(msg)
                for param in self.profiling[alg]:
                    if param not in self.valid_params[alg]:
                        msg = ("'{}' is not a valid parameter ".format(param)+
                               "for {}".format(alg))
                        logger.error(msg)
                        raise ValueError(msg)

    def fit_predict(self, data,
                    key=None,
                    categorical_variable=None,
                    string_variable=None,
                    variable_weight=None,
                    grubbs_cols=None,
                    iqr_cols=None):
        r"""
        Detection of outliers in the input data using outlier profiling.

        Parameters
        ----------
        data : DataFrame
            DataFrame containing the data used for outlier detection via outlier profiling.

            There must be an ID column in ``data``.

        key : str, optional
            Specifies the name of ID column in ``data``.

            Mandatory if ``data`` is not indexed by a single column.

            Defaults to the single index column of ``data`` if there is one.

        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.

        string_variable : str or a list of str, optional
            Indicates a string(i.e. of type VARCHAR/NVARCHAR) column storing not categorical data.
            Levenshtein distance is used to calculate similarity between two strings.
            Ignored if it is not a string column.

            Effective only when **DBSCAN** is included in the outlier profiling.

            By default all columns of type VARCHAR/NVARCHAR are categorical.

        grubbs_cols : str or a list of str, optional
            Specifies the (numerical)columns used for Grubbs' test.

            Categorical columns specifies in ``categorical_variable`` are ignored automatically.

            Effective only when **grubbs_test** is included in the outlier profiling.

            Defaults to all numerical columns in ``data``.

        iqr_cols : str or a list of str, optional
            Specifies the (numerical)columns used for Inter-Quantile-Range(IQR) test.

            Categorical columns specifies in ``categorical_variable`` are ignored automatically.

            Effective only when **iqr** is included in the outlier profiling.

            Defaults to all numerical columns in ``data``.

        .. note ::
            **IsolationForest, grubbs_test, iqr** apply only when ``data`` contains numerical columns. In the absence of
            such columns, the aforementioned three methods shall be disabled.

        Returns
        -------
        Dict
            The detected outliers by different algorithms within the profiling, structured as follows:

                - key : the algorithm applied for outlier detection
                - value : the DataFrame containing the detected outliers by the algorithm specified by the key
        """
        if not isinstance(data.index, str):
            key = self._arg('key', key, str, required=True)
        key = data.index if key is None else key
        if isinstance(grubbs_cols, str):
            grubbs_cols = [grubbs_cols]
        grubbs_cols = self._arg('grubbs_cols', grubbs_cols,
                                ListOfStrings)
        if isinstance(iqr_cols, str):
            iqr_cols = [iqr_cols]
        iqr_cols = self._arg('iqr_cols', iqr_cols, ListOfStrings)
        num_cols = []
        data_woid = data.deselect(key)
        cat_cols = []
        if isinstance(categorical_variable, str):
            categorical_variable = [categorical_variable]
        if isinstance(string_variable, str):
            string_variable = [string_variable]
        categorical_variable = self._arg('categorical_variable',
                                         categorical_variable,
                                         ListOfStrings)
        string_variable = self._arg('string_variable',
                                    string_variable,
                                    ListOfStrings)
        if categorical_variable is not None:
            cat_cols = categorical_variable
        num_cols = []
        for colinf in data_woid.dtypes():
            if any(x in colinf[1] for x in ['DOU', 'DEC']):
                num_cols.append(colinf[0])
            elif 'INT' in colinf[1] and colinf[0] not in cat_cols:
                num_cols.append(colinf[0])
            else:
                continue
        if grubbs_cols is None:
            grubbs_cols = num_cols
        if iqr_cols is None:
            iqr_cols = num_cols
        outliers = {}
        for alg in self.profiling:
            opms = self.profiling[alg]
            if alg == 'KMeansOutlier':
                det_res = KMeansOutlier(**opms).fit_predict(data=data,
                                                            key=key,
                                                            features=None)[0][[key]]
                select = data.filter('{} IN ({})'.format(quotename(key),
                                                         det_res.select_statement))
                outliers[alg] = select
            elif alg == 'DBSCAN':
                det_res = DBSCAN(**opms).fit_predict(data=data,
                                                     key=key,
                                                     features=None,
                                                     categorical_variable=categorical_variable,
                                                     string_variable=string_variable,
                                                     variable_weight=variable_weight)
                det_ids = det_res.filter('CLUSTER_ID < 0').select(key)
                select = data.filter('{} IN ({})'.format(quotename(key),
                                                         det_ids.select_statement))
                outliers[alg] = select
            elif alg == 'IsolationForest' and len(num_cols) > 0:
                isf_contamination = None
                if 'contamination' in opms:
                    isf_contamination = opms['contamination']
                    del opms['contamination']
                data_ = data.deselect(key)
                data_ = data[[key] + num_cols]
                det_res = IsolationForest(**opms).fit_predict(data=data_,
                                                              key=key,
                                                              contamination=isf_contamination)
                det_ids = det_res.filter('LABEL < 0').select(key)
                select = data.filter('{} IN ({})'.format(quotename(key),
                                                         det_ids.select_statement))
                outliers[alg] = select
            elif alg == 'OneClassSVM':
                det_obj = OneClassSVM(**opms).fit(data=data,
                                                  key=key,
                                                  features=None,
                                                  categorical_variable=categorical_variable)
                det_res = det_obj.predict(data, key, None)
                det_ids = det_res.filter('SCORE < 0').select(key)
                select = data.filter('{} IN ({})'.format(quotename(key),
                                                         det_ids.select_statement))
                outliers[alg] = select
            elif alg == 'grubbs_test' and len(num_cols) > 0:
                for col in grubbs_cols:
                    det_res = grubbs_test(data=data, key=key, col=col, **opms)[0][[key]]
                    select = data.filter('{} IN ({})'.format(quotename(key),
                                                             det_res.select_statement))
                    outliers[alg + '-{}'.format(col)] = select
            elif alg == 'iqr' and len(num_cols) > 0:
                for col in iqr_cols:
                    det_res = iqr(data=data, key=key, col=col, **opms)[0]
                    det_res = det_res.filter('IS_OUT_OF_RANGE = 1')[[key]]
                    select = data.filter('{} IN ({})'.format(quotename(key),
                                                             det_res.select_statement))
                    outliers[alg + '-{}'.format(col)] = select
        return outliers
