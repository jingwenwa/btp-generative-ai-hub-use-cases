#pylint: disable=too-many-lines, too-many-arguments, invalid-name, unused-variable, too-many-locals, too-many-statements
#pylint: disable=line-too-long, bad-option-value, too-few-public-methods, useless-object-inheritance, too-many-branches
#pylint: disable=relative-beyond-top-level, attribute-defined-outside-init, too-many-instance-attributes
#pylint: disable=consider-using-f-string, c-extension-no-member, too-many-positional-arguments, simplifiable-if-expression
"""
This module contains Python wrappers for PAL clustering algorithms.

The following classes are available:

    * :class:`AffinityPropagation`
    * :class:`AgglomerateHierarchicalClustering`
    * :class:`DBSCAN`
    * :class:`GeometryDBSCAN`
    * :class:`KMeans`
    * :class:`KMedians`
    * :class:`KMedoids`
    * :class:`SpectralClustering`
    * :class:`ConstrainedClustering`
    * :class:`KMeansOutlier`
    * :func:`SlightSilhouette`
    * :func:`outlier_detection_kmeans`
"""
import logging
import uuid
from typing import List
from hdbcli import dbapi
from hana_ml.dataframe import DataFrame
from hana_ml.ml_exceptions import FitIncompleteError
from .sqlgen import trace_sql
from .utility import check_pal_function_exist
from .pal_base import (
    PALBase,
    ParameterTable,
    arg,
    try_drop,
    pal_param_register,
    colspec_from_df,
    ListOfStrings,
    require_pal_usable,
    call_pal_auto_with_hint
)
logger = logging.getLogger(__name__)

class _ClusterAssignmentMixin(PALBase):
    def __init__(self):
        super(_ClusterAssignmentMixin, self).__init__()
        self.pal_funcname = 'PAL_CLUSTER_ASSIGNMENT'

    @trace_sql
    def _predict(self, data, key=None, features=None):
        r"""
        Assign clusters to data based on a fitted model.

        The output structure of this method does not match that of
        fit_predict().

        Parameters
        ----------

        data : DataFrame

            Data points to match against computed clusters.

            This dataframe's column structure should match that
            of the data used for fit().

        key : str, optional

            Name of ID column.
            Defaults to the index column of data (i.e. data.index) if it is set.
            If the index of column of data is not provided, please enter the value of key.

        features : a list of str, optional.

            Names of feature columns.
            If ``features`` is not provided, it defaults to all non-key columns.

        Returns
        -------

        DataFrame
            Cluster assignment results:

              - Data point ID, with name and type taken from the input
                ID column.
              - CLUSTER_ID, INTEGER type, representing the cluster the
                data point is assigned to.
              - DISTANCE, DOUBLE type, representing the distance between
                the data point and the cluster center (for K-means), the
                nearest core object (for DBSCAN), or the weight vector
                (for SOM).
        """
        conn = data.connection_context
        if getattr(self, 'model_') is None:
            raise FitIncompleteError()

        cols = data.columns
        index = data.index
        key = self._arg('key', key, str, not isinstance(index, str))
        if isinstance(index, str):
            if key is not None and index != key:
                warn_msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(warn_msg)
        key = index if key is None else key
        cols.remove(key)

        features = self._arg('features', features, ListOfStrings)
        if features is None:
            features = cols
        data_ = data[[key] + features]

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        assignment_tbl = '#PAL_CLUSTER_ASSIGNMENT_ASSIGNMENT_TBL_{}_{}'.format(self.id, unique_id)

        try:
            self._call_pal_auto(conn,
                                'PAL_CLUSTER_ASSIGNMENT',
                                data_,
                                self.model_,
                                ParameterTable(),
                                assignment_tbl)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, assignment_tbl)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, assignment_tbl)
            raise
        return conn.table(assignment_tbl)

    def create_model_state(self, model=None, function=None,
                           pal_funcname='PAL_CLUSTER_ASSIGNMENT',
                           state_description=None, force=False):
        r"""
        Create PAL model state.

        Parameters
        ----------
        model : DataFrame, optional
            Specify the model for AFL state.

            Defaults to self.model\_.

        function : str, optional
            Specify the function in the unified API.

            A placeholder parameter, not effective for cluster assignment.

        pal_funcname : int or str, optional
            PAL function name. Must be a valid PAL procedure that supports model state.

            Defaults to 'PAL_CLUSTER_ASSIGNMENT'.

        state_description : str, optional
            Description of the state as model container.

            Defaults to None.

        force : bool, optional
            If True it will delete the existing state.

            Defaults to False.
        """
        super()._create_model_state(model, function, pal_funcname, state_description, force)

    def set_model_state(self, state):
        """
        Set the model state by state information.

        Parameters
        ----------
        state: DataFrame or dict
            If state is DataFrame, it has the following structure:

                - NAME: VARCHAR(100), it mush have STATE_ID, HINT, HOST and PORT.
                - VALUE: VARCHAR(1000), the values according to NAME.

            If state is dict, the key must have STATE_ID, HINT, HOST and PORT.
        """
        super()._set_model_state(state)

    def delete_model_state(self, state=None):
        """
        Delete PAL model state.

        Parameters
        ----------
        state : DataFrame, optional
            Specified the state.

            Defaults to self.state.
        """
        super()._delete_model_state(state)

def SlightSilhouette(data,
                     features=None,
                     label=None,
                     distance_level=None,
                     minkowski_power=None,
                     normalization=None,
                     thread_number=None,
                     categorical_variable=None,
                     category_weights=None):
    r"""
    Silhouette refers to a method used to validate the cluster of data which provides a succinct graphical representation of how well each object lies within its cluster.
    SAP HNAN PAL provides a light version of silhouette called slight silhouette.

    Parameters
    ----------

    data : DataFrame
        DataFrame containing the data.
    features : a list of str, optional
        The names of feature columns.

        If ``features`` is not provided, it defaults to all non-label columns.
    label: str, optional
        The name of the label column which indicate the cluster id.

        If ``label`` is not provided, it defaults to the last column of data.
    distance_level : {'manhattan', 'euclidean', 'minkowski', 'chebyshev', 'cosine'}, optional
        Specifies the method for computing the distance between a data point and the cluster center.
        The 'cosine' method is only valid when the ``accelerated = False`` condition is applied.

        Defaults to 'euclidean'.
    minkowski_power : float, optional
        Determines the power to be used in the Minkowski distance calculation. It is only applicable when the ``distance_level`` parameter is set to 'minkowski'.

        Defaults to 3.0.
    normalization : {'no', 'l1_norm', 'min_max'}, optional
        Specifies the type of normalization to be applied to the data points.

        - 'no': No normalization is applied.
        - 'l1_norm': This applies L1 normalization. For each point X (x\ :sub:`1`\, x\ :sub:`2`\, ..., x\ :sub:`n`\), the normalized value will be X'(x\ :sub:`1` /S,x\ :sub:`2` /S,...,x\ :sub:`n` /S),
          where S = \|x\ :sub:`1`\ \|+\|x\ :sub:`2`\ \|+...\|x\ :sub:`n`\ \|.
        - 'min_max': The Min-Max normalization method is applied. For each column C, get the min and max value of C,
          and then C[i] = (C[i]-min)/(max-min).

        Defaults to 'no'.
    thread_number : int, optional
        Number of threads.

        Defaults to 1.
    categorical_variable : str or a list of str, optional
        Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

        No default value.
    category_weights : float, optional
        Represents the weight of category attributes.

        Defaults to 0.707.

    Returns
    -------
    DataFrame
        A DataFrame containing the validation value of Slight Silhouette.


    Examples
    --------

    Input data df:

    >>> df.collect()
        V000 V001 V002 CLUSTER
    0    0.5    A  0.5       0
    1    1.5    A  0.5       0
    ...
    18  15.5    D  1.5       3
    19  15.7    A  1.6       3

    Call the function:

    >>> res = SlightSilhouette(data=df, label="CLUSTER")

    Result:

    >>> res.collect()
      VALIDATE_VALUE
    0      0.9385944
    """
    distance_map = {'manhattan':1, 'euclidean':2, 'minkowski':3, 'chebyshev':4}
    distance_level = arg('distance_level', distance_level, distance_map)
    if distance_level != 3 and minkowski_power is not None:
        msg = 'Minkowski_power will only be valid if distance_level is Minkowski.'
        logger.error(msg)
        raise ValueError(msg)
    minkowski_power = arg('minkowski_power', minkowski_power, float)
    normalization_map = {'no' : 0, 'l1_norm' : 1, 'min_max' : 2}
    normalization = arg('normalization', normalization, normalization_map)
    thread_number = arg('thread_number', thread_number, int)
    category_weights = arg('category_weights', category_weights, float)

    #handle CATEGORY_COL, transform from categorical_variable
    if categorical_variable is not None:
        column_choose = None
        if isinstance(categorical_variable, str):
            categorical_variable = [categorical_variable]
        categorical_variable = arg('categorical_variable', categorical_variable, ListOfStrings)
        try:
            column_choose = []
            for var in categorical_variable:
                column_choose.append(data.columns.index(var))
        except:
            msg = "Not all categorical_variable is in the features!"
            logger.error(msg)
            raise TypeError(msg)

    conn = data.connection_context
    require_pal_usable(conn)
    label = arg('label', label, str)
    features = arg('features', features, ListOfStrings)

    cols = data.columns
    if label is None:
        label = cols[-1]
    cols.remove(label)
    if features is None:
        features = cols

    data_ = data[features + [label]]

    unique_id = str(uuid.uuid1()).replace('-', '_').upper()
    result_tbl = '#PAL_SLIGHT_SIL_RESULT_TBL_{}_{}'.format(id, unique_id)

    param_rows = [('DISTANCE_LEVEL', distance_level, None, None),
                  ('MINKOWSKI_POWER', None, minkowski_power, None),
                  ('NORMALIZATION', normalization, None, None),
                  ('THREAD_NUMBER', thread_number, None, None),
                  ('CATEGORY_WEIGHTS', None, category_weights, None)]
    if categorical_variable is not None:
        param_rows.extend([('CATEGORY_COL', col, None, None)
                           for col in column_choose])

    try:
        sql, _ = call_pal_auto_with_hint(conn,
                                         None,
                                         'PAL_SLIGHT_SILHOUETTE',
                                         data_,
                                         ParameterTable().with_data(param_rows),
                                         result_tbl)
        conn.execute_statement = sql
    except dbapi.Error as db_err:
        logger.exception(str(db_err))
        try_drop(conn, result_tbl)
        raise
    except Exception as db_err:
        logger.exception(str(db_err))
        try_drop(conn, result_tbl)
        raise
    return conn.table(result_tbl)

class AffinityPropagation(PALBase):
    r"""
    Affinity Propagation is an algorithm that identifies exemplars among data points and forms clusters of
    data points around these exemplars. It operates by simultaneously considering all data point as
    potential exemplars and exchanging messages between data points until a good set of exemplars and clusters emerges.

    Parameters
    ----------

    affinity : {'manhattan', 'standardized_euclidean', 'euclidean', 'minkowski', 'chebyshev', 'cosine'}
        Ways to compute the distance between two points.

    n_clusters : int
        Number of clusters.

          - 0: does not adjust Affinity Propagation cluster result.
          - Non-zero int: If Affinity Propagation cluster number is bigger than ``n_clusters``,
            PAL will merge the result to make the cluster number be the value specified for ``n_clusters``.

    max_iter : int, optional
        Specifies the maximum number of iterations.

        Defaults to 500.

    convergence_iter : int, optional
        Specifies the number of iterations for which cluster stability should be maintained. If the clusters remain stable for the specified number of iterations, the algorithm terminates.

        Defaults to 100.

    damping : float
        Controls the updating velocity. Value range: (0, 1).

        Defaults to 0.9.

    preference : float, optional
        Determines the preference. Value range: [0,1].

        Defaults to 0.5.

    seed_ratio : float, optional
        Select a portion of (seed_ratio * data_number) the input data as seed,
        where data_number is the row_size of the input data.

        Value range: (0,1].

        If ``seed_ratio`` is set to 1, the entire input dataset will be used as seed data.

        Defaults to 1.

    times : int, optional
        Specifies the number of sampling iterations. Only valid when ``seed_ratio`` is less than 1.

        Defaults to 1.

    minkowski_power : int, optional
        Specifies the power parameter for the Minkowski distance calculation method.
        This parameter is relevant only when the 'affinity' is set to 'minkowski'.

        Defaults to 3.

    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 0.

    Attributes
    ----------

    labels_ : DataFrame
        Label assigned to each sample.

    Examples
    --------
    Input DataFrame df:

    >>> df.collect()
        ID  ATTRIB1  ATTRIB2
    0    1     0.10     0.10
    1    2     0.11     0.10
    ...
    22  23    10.13    10.14
    23  24    10.14    10.13

    Create an AffinityPropagation instance:

    >>> ap = AffinityPropagation(
                affinity='euclidean',
                n_clusters=0,
                max_iter=500,
                convergence_iter=100,
                damping=0.9,
                preference=0.5,
                seed_ratio=None,
                times=None,
                minkowski_power=None,
                thread_ratio=1)

    Perform fit():

    >>> ap.fit(data=df, key='ID')

    Expected output:

    >>> ap.labels_.collect()
        ID  CLUSTER_ID
    0    1           0
    1    2           0
    ...
    22  23           1
    23  24           1
    """
    affinity_map = {'manhattan':1, 'euclidean':2, 'minkowski':3,
                    'chebyshev':4, 'standardized_euclidean':5, 'cosine':6}

    def __init__(self,
                 affinity,
                 n_clusters,
                 max_iter=None,
                 convergence_iter=None,
                 damping=None,
                 preference=None,
                 seed_ratio=None,
                 times=None,
                 minkowski_power=None,
                 thread_ratio=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(AffinityPropagation, self).__init__()

        self.affinity = self._arg('affinity', affinity, self.affinity_map)
        self.n_clusters = self._arg('n_clusters', n_clusters, int)
        self.max_iter = self._arg('max_iter', max_iter, int)
        self.convergence_iter = self._arg('convergence_iter', convergence_iter, int)
        self.damping = self._arg('damping', damping, float)
        self.preference = self._arg('preference', preference, float)
        self.seed_ratio = self._arg('seed_ratio', seed_ratio, float)
        self.times = self._arg('times', times, int)
        self.minkowski_power = self._arg('minkowski_power', minkowski_power, int)
        self.thread_ratio = self._arg('thread_ratio', thread_ratio, float)

    def fit(self, data, key=None, features=None):
        """
        Fit the model to the training dataset.

        Parameters
        ----------

        data : DataFrame
            DataFrame containing the data.

        key : str, optional
            Name of ID column.

            Defaults to the index column of data (i.e. data.index) if it is set.
            If the index column of data is not provided, please enter the value of key.

        features : a list of str, optional
            Names of the feature columns.

            If ``features`` is not provided, it defaults to all non-key columns.
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, 'training_data', data)
        conn = data.connection_context
        require_pal_usable(conn)

        cols = data.columns
        index = data.index
        key = self._arg('key', key, str, not isinstance(index, str))
        if isinstance(index, str):
            if key is not None and index != key:
                warn_msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(warn_msg)
        key = index if key is None else key
        cols.remove(key)

        features = self._arg('features', features, ListOfStrings)
        if features is None:
            features = cols
        data_ = data[[key] + features]

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        outputs = ['#AFFINITY_PROPAGATION_{}_TBL_{}_{}'.format(name, self.id, unique_id)
                   for name in ['RESULT', 'STAT']]
        result_tbl, stats_tbl = outputs

        param_rows = [('DISTANCE_METHOD', self.affinity, None, None),
                      ('CLUSTER_NUMBER', self.n_clusters, None, None),
                      ('MAX_ITERATION', self.max_iter, None, None),
                      ('CON_ITERATION', self.convergence_iter, None, None),
                      ('DAMP', None, self.damping, None),
                      ('PREFERENCE', None, self.preference, None),
                      ('SEED_RATIO', None, self.seed_ratio, None),
                      ('TIMES', self.times, None, None),
                      ('MINKOW_P', self.minkowski_power, None, None),
                      ('THREAD_RATIO', None, self.thread_ratio, None)]

        seed_df = conn.sql("SELECT TOP 0 * FROM (SELECT NULL ID, NULL SEED_ID FROM DUMMY) dt;").cast({"ID": "INTEGER", "SEED_ID":"INTEGER"})

        try:
            self._call_pal_auto(conn,
                                'PAL_AFFINITY_PROPAGATION',
                                data_,
                                seed_df,
                                ParameterTable().with_data(param_rows),
                                *outputs)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, outputs)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, outputs)
            raise
        self.labels_ = conn.table(result_tbl)

    def fit_predict(self, data, key=None, features=None):
        """
        Fit with the dataset and return labels.

        Parameters
        ----------

        data : DataFrame
            DataFrame containing the data.

        key : str, optional
            Name of ID column.

            Defaults to the index column of data (i.e. data.index) if it is set.
            If the index column of data is not provided, please enter the value of key.

        features : a list of str, optional
            Names of the features columns.

            If ``features`` is not provided, it defaults to all non-key columns.

        Returns
        -------
        DataFrame
            Labels of each point.
        """
        self.fit(data, key, features)
        return self.labels_

class AgglomerateHierarchicalClustering(PALBase):
    r"""
    Agglomerate Hierarchical Clustering is a widely used clustering method which can find natural groups within a set of data. The idea is to group the data into  a hierarchy or a binary tree of the subgroups. A hierarchical clustering can be either agglomerate or divisive, depending on the method of hierarchical decomposition.

    The implementation in HANA PAL follows the agglomerate approach, which merges the clusters  with a bottom-up strategy. Initially, each data point is considered as an own cluster.
    The algorithm iteratively merges two clusters based on the dissimilarity measure in  a greedy manner and forms a larger cluster.

    Parameters
    ----------

    n_clusters : int, optional

        Number of clusters after agglomerate hierarchical clustering algorithm.
        Value range: between 1 and the initial number of input data.

        Defaults to 1.

    affinity : {'manhattan', 'euclidean', 'minkowski', 'chebyshev', 'cosine', 'pearson correlation', 'squared euclidean', 'jaccard', 'gower', 'precomputed'}, optional

        Determines the method for calculating the distance between two points.

        .. Note ::

            - (1) For jaccard distance, non-zero input data will be treated as 1,
              and zero input data will be treated as 0.
              jaccard distance = (M01 + M10) / (M11 + M01 + M10)

            - (2) Only gower distance supports category attributes.
              When linkage is 'centroid clustering', 'median clustering', or 'ward',
              this parameter must be set to 'squared euclidean'.

        Defaults to 'squared euclidean'.

    linkage : {'nearest neighbor', 'furthest neighbor', 'group average', 'weighted average', 'centroid clustering', 'median clustering', 'ward'}, optional

        Linkage type between two clusters.

        - 'nearest neighbor' : single linkage.
        - 'furthest neighbor' : complete linkage.
        - 'group average' : UPGMA.
        - 'weighted average' : WPGMA.
        - 'centroid clustering'.
        - 'median clustering'.
        - 'ward'.

        Defaults to 'centroid clustering'.

            .. note::
                For linkage 'centroid clustering', 'median clustering', or 'ward',
                the corresponding ``affinity`` must be set to 'squared euclidean'.

    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 0.
    distance_dimension : float, optional

        Distance dimension can be set if ``affinity`` is set to 'minkowski'. The value should be no less than 1.

        Only valid when ``affinity`` is 'minkowski'.

        Defaults to 3.

    normalization : str, optional

        Specifies the type of normalization applied.

            - 'no': No normalization
            - 'z-score': Z-score standardization
            - 'zero-centred-min-max': Zero-centred min-max normalization, transforming to new range [-1, 1].
            - 'min-max': Standard min-max normalization, transforming to new range [0, 1].

        Valid only when ``affinity`` is not 'precomputed'.

        Defaults to 'no'.

    category_weights : float, optional

        Represents the weight of category columns.

        Defaults to 1.

    Attributes
    ----------

    combine_process_ : DataFrame
        Structured as follows:

              - 1st column: int, STAGE, cluster stage.
              - 2nd column: ID (in input table) data type, LEFT\_ + ID (in input table) column + name,
                One of the clusters that is to be combined in one combine stage, name as its row number in the input data table.
                After the combining, the new cluster is named after the left one.
              - 3rd column: ID (in input table) data type, RIGHT\_ + ID (in input table) column name,
                The other cluster to be combined in the same combine stage, named as its row number in the input data table.
              - 4th column: float, DISTANCE. Distance between the two combined clusters.

    labels_ : DataFrame
        Label assigned to each sample. structured as follows:

              - 1st column: Name of the ID column in the input data(or that of the first column of the input
                DataFrame when ``affinity`` is 'precomputed'), record ID.
              - 2nd column: CLUSTER_ID, cluster number after applying the hierarchical agglomerate algorithm.

    Examples
    --------
    >>> df.collect()
       POINT       X1     X2    X3
    0      0      0.5    0.5     1
    1      1      1.5    0.5     2
    ...
    18    18     15.5    1.5     1
    19    19     15.7    1.6     1

    Create an AgglomerateHierarchicalClustering instance:

    >>> hc = AgglomerateHierarchicalClustering(
                 n_clusters=4,
                 affinity='Gower',
                 linkage='weighted average',
                 thread_ratio=None,
                 distance_dimension=3,
                 normalization='no',
                 category_weights= 0.1)

    Perform fit():

    >>> hc.fit(data=df, key='POINT', categorical_variable=['X3'])

    Expected output:

    >>> hc.combine_process_.collect().head(3)
       STAGE  LEFT_POINT   RIGHT_POINT    DISTANCE
    0      1          18           19       0.0187
    1      2          13           14       0.0250
    2      3           7            9       0.0437

    >>> hc.labels_.collect().head(3)
       POINT  CLUSTER_ID
    0      0           1
    1      1           1
    2      2           1

    """

    affinity_map = {'manhattan':1, 'euclidean':2, 'minkowski':3, 'chebyshev':4,
                    'cosine':6, 'pearson correlation':7, 'squared euclidean':8,
                    'jaccard':9, 'gower':10, 'precomputed':11}
    linkage_map = {'nearest neighbor':1, 'furthest neighbor':2, 'group average':3, 'weighted average':4,
                   'centroid clustering':5, 'median clustering':6, 'ward':7}
    normalization_map = {'no': 0, 'z-score': 1, 'zero-centred-min-max': 2, 'min-max': 3}

    def __init__(self,
                 n_clusters=None,
                 affinity=None,
                 linkage=None,
                 thread_ratio=None,
                 distance_dimension=None,
                 normalization=None,
                 category_weights=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(AgglomerateHierarchicalClustering, self).__init__()
        self.n_clusters = self._arg('n_clusters', n_clusters, int)
        self.affinity = self._arg('affinity', affinity, self.affinity_map)
        self.linkage = self._arg('linkage', linkage, self.linkage_map)
        linkage_range = [5, 6, 7]
        if self.linkage in linkage_range and self.affinity != 8:
            msg = ('For linkage is centroid clustering, median clustering or ward, ' +
                   'the corresponding affinity must be set to squared euclidean!')
            logger.error(msg)
            raise ValueError(msg)

        self.thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        self.distance_dimension = self._arg('distance_dimension', distance_dimension, float)
        self.normalization = self._arg('normalization', normalization, (int, str))
        if isinstance(self.normalization, str):
            self.normalization = self._arg('normalization', normalization,
                                           self.normalization_map)
        self.category_weights = self._arg('category_weights', category_weights, float)

    @trace_sql
    def fit(self, data, key=None, features=None, categorical_variable=None):
        r"""
        Fit the model to the training dataset.

        Parameters
        ----------

        data : DataFrame
            DataFrame containing the data.

            If ``affinity`` is specified as 'precomputed' in initialization, then ``data``
            must be a structured DataFrame that reflects the affinity information between
            points as follows:

                - 1st column: ID of the first point.
                - 2nd column: ID of the second point.
                - 3rd column: Precomputed distance between first point & second point.

        key : str, optional
            Name of ID column in ``data``.
            Defaults to the index column of data (i.e. data.index) if it is set.
            If the index column of data is not provided and ``affinity`` is not set as 'precomputed'
            in initialization, please enter the value of key.

            Valid only when ``affinity`` is not 'precomputed' in initialization.

        features : a list of str, optional
            Names of the features columns.
            If ``features`` is not provided, it defaults to all non-key columns.

            Valid only when ``affinity`` is not 'precomputed' in initialization.
        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.

        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, 'training_data', data)
        conn = data.connection_context
        require_pal_usable(conn)
        if self.affinity != 11:
            if isinstance(categorical_variable, str):
                categorical_variable = [categorical_variable]
                categorical_variable = self._arg('categorical_variable', categorical_variable,
                                                 ListOfStrings)
            cols = data.columns
            index = data.index
            key = self._arg('key', key, str, not isinstance(index, str))
            if isinstance(index, str):
                if key is not None and index != key:
                    warn_msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                    "and the designated index column '{}'.".format(index)
                    logger.warning(warn_msg)
            key = index if key is None else key
            cols.remove(key)

            features = self._arg('features', features, ListOfStrings)
            if features is None:
                features = cols

            data_ = data[[key] + features]
        else:
            data_ = data

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        outputs = ['#HIERARCHICAL_CLUSTERING_{}_TBL_{}_{}'.format(name, self.id, unique_id) for
                   name in ['COMBINE', 'RESULT']]
        combine_process_tbl, result_tbl = outputs

        param_rows = [('CLUSTER_NUM', self.n_clusters, None, None),
                      ('DISTANCE_FUNC', self.affinity, None, None),
                      ('CLUSTER_METHOD', self.linkage, None, None),
                      ('THREAD_RATIO', None, self.thread_ratio, None),
                      ('DISTANCE_DIMENSION', None, self.distance_dimension, None),
                      ('NORMALIZE_TYPE', self.normalization, None, None),
                      ('CATEGORY_WEIGHTS', None, self.category_weights, None)]

        if self.affinity != 11 and categorical_variable is not None:
            param_rows.extend([('CATEGORICAL_VARIABLE', None, None, name)
                               for name in categorical_variable])

        try:
            self._call_pal_auto(conn,
                                'PAL_HIERARCHICAL_CLUSTERING',
                                data_,
                                ParameterTable().with_data(param_rows),
                                *outputs)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, outputs)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, outputs)
            raise
        self.combine_process_ = conn.table(combine_process_tbl)
        self.labels_ = conn.table(result_tbl)

    def fit_predict(self, data, key=None, features=None, categorical_variable=None):
        r"""
        Fit with the dataset and return the labels.

        Parameters
        ----------

        data : DataFrame
            DataFrame containing the data.

            If ``affinity`` is specified as 'precomputed' in initialization, then ``data``
            must be a structured DataFrame that reflects the affinity information between
            points as follows:

            - 1st column: ID of the first point.
            - 2nd column: ID of the second point.
            - 3rd column: Precomputed distance between first point & second point.

        key : str, optional
            Name of ID column in ``data``.

            Defaults to the index column of data (i.e. data.index) if it is set.
            If the index column of data is not provided and ``affinity`` is not set as 'precomputed'
            in initialization, please enter the value of key.

            Valid only when ``affinity`` is not 'precomputed' in initialization.
        features : a list of str, optional
            Names of the features columns.
            If ``features`` is not provided, it defaults to all non-key columns.
            Valid only when ``affinity`` is not 'precomputed' in initialization.
        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.

        Returns
        -------
        DataFrame
            Label of each points.
        """
        self.fit(data, key, features, categorical_variable)
        return self.labels_

class DBSCAN(_ClusterAssignmentMixin):
    r"""
    DBSCAN (Density-Based Spatial Clustering of Applications with Noise) is
    a density-based data clustering algorithm that finds a number of clusters
    starting from the estimated density distribution of corresponding nodes.

    It separates high-density regions from low-density ones, allowing it to discover clusters of arbitrary shape in data containing noise and outliers.

    Parameters
    ----------

    minpts : int, optional
        Represents the minimum number of points required to form a cluster.

            .. note ::

                ``minpts`` and ``eps`` need to be provided together by
                user or these two parameters are automatically determined.

    eps : float, optional
        Specifies the scanning radius around a point.

            .. note::

                ``minpts`` and ``eps`` need to be provided together
                by user or these two parameters are automatically determined.

    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to heuristically determined.
    metric : {'manhattan', 'euclidean', 'minkowski', 'chebyshev', 'standardized_euclidean', 'cosine'}, optional
        Ways to compute the distance between two points.

        Defaults to 'euclidean'.
    minkowski_power : int, optional
        When minkowski is chosen for ``metric``, this parameter
        controls the value of power. Only applicable when ``metric`` is 'minkowski'.

        Defaults to 3.
    categorical_variable : str or a list of str, optional
        Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

        No default value.
    category_weights : float, optional
        Represents the weight of category attributes.

        Defaults to 0.707.
    algorithm : {'brute-force', 'kd-tree'}, optional
        Represents the chosen method to search for neighbouring data points.

        Defaults to 'kd-tree'.
    save_model : bool, optional
        If set to true, the generated model will be saved.
        It must be mentioned that ``save_model`` has to be set to true in order to utilize the function predict().

        Defaults to True.

    Attributes
    ----------

    labels_ : DataFrame
        Label assigned to each sample.

    model_ : DataFrame
        Model content. Set to None if  ``save_model`` is False.

    Examples
    --------

    Input DataFrame df:

    >>> df.collect()
        ID     V1     V2 V3
    0    1   0.10   0.10  B
    1    2   0.11   0.10  A
    ...
    28  29  20.11  20.12  C
    29  30  15.12  15.11  A

    Create a DSBCAN instance:

    >>> dbscan = DBSCAN(thread_ratio=0.2, metric='manhattan')

    Perform fit():

    >>> dbscan.fit(data=df, key='ID')

    Output:

    >>> dbscan.labels_.collect()
        ID  CLUSTER_ID
    0    1           0
    1    2           0
    2    3           0
    ...
    27  28          -1
    28  29          -1
    29  30          -1
    """
    metric_map = {'manhattan':1, 'euclidean':2, 'minkowski':3, 'chebyshev':4,
                  'standardized_euclidean':5, 'cosine':6}
    algorithm_map = {'brute-force':0, 'kd-tree':1}
    def __init__(self, minpts=None, eps=None, thread_ratio=None,
                 metric=None, minkowski_power=None, categorical_variable=None,
                 category_weights=None, algorithm=None, save_model=True):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(DBSCAN, self).__init__()
        self.auto_param = 'true' if any(param is None for param in (minpts, eps)) else 'false'
        if (minpts is not None and eps is None) or (minpts is None and eps is not None):
            msg = 'minpts and eps need to be provided together.'
            logger.error(msg)
            raise ValueError(msg)
        self.minpts = self._arg('minpts', minpts, int)
        self.eps = self._arg('eps', eps, float)
        self.thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        self.metric = self._arg('metric', metric,
                                self.metric_map)
        if self.metric != 3 and minkowski_power is not None:
            msg = 'minkowski_power will only be applicable if metric is minkowski.'
            logger.error(msg)
            raise ValueError(msg)
        self.minkowski_power = self._arg('minkowski_power', minkowski_power, int)
        if isinstance(categorical_variable, str):
            categorical_variable = [categorical_variable]
        self.categorical_variable = self._arg('categorical_variable', categorical_variable,
                                              ListOfStrings)
        self.category_weights = self._arg('category_weights', category_weights, float)
        self.algorithm = self._arg('algorithm', algorithm, self.algorithm_map)
        self.save_model = self._arg('save_model', save_model, bool)
        self.string_variable = None
        self.variable_weight = None

    def _check_variable_weight(self, variable_weight):
        self.variable_weight = self._arg('variable_weight', variable_weight, dict)
        for key, value in  self.variable_weight.items():
            if not isinstance(key, str):
                msg = "The key of variable_weight must be a string!"
                logger.error(msg)
                raise TypeError(msg)
            if not isinstance(value, (float, int)):
                msg = "The value of variable_weight must be a float!"
                logger.error(msg)
                raise TypeError(msg)

    @trace_sql
    def fit(self, data, key=None, features=None, categorical_variable=None, string_variable=None, variable_weight=None):
        """
        Fit the model to the training dataset.

        Parameters
        ----------

        data : DataFrame
            DataFrame containing the data.
        key : str, optional
            Name of ID column.

            Defaults to the index column of data (i.e. data.index) if it is set.
            If the index column of data is not provided, please enter the value of key.
        features : a list of str, optional
            A list of Names of the feature columns.
            Since the introduction of SAP HANA Cloud 24 QRC03, the data type support for features has been expanded to include VECTOR TYPE, in addition to the previously supported types such as INTEGER, DOUBLE, DECIMAL(p, s), VARCHAR, and NVARCHAR.

            If ``features`` is not provided, it defaults to all non-key columns. This means that all columns except the key column will be considered as features.

        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.
        string_variable : str or a a list of str, optional
            Indicates a string column storing not categorical data.
            Levenshtein distance is used to calculate similarity between two strings. Ignored if it is not a string column.

            Defaults to None.
        variable_weight : dict, optional
            Specifies the weight of a variable participating in distance calculation.
            The value must be greater or equal to 0. Defaults to 1 for variables not specified.

            Defaults to None.

        Returns
        -------
        A fitted object of class "DBSCAN".
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, 'training_data', data)
        conn = data.connection_context
        require_pal_usable(conn)

        cols = data.columns
        index = data.index
        key = self._arg('key', key, str, not isinstance(index, str))
        if isinstance(index, str):
            if key is not None and index != key:
                warn_msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(warn_msg)
        key = index if key is None else key
        cols.remove(key)

        features = self._arg('features', features, ListOfStrings)
        if features is None:
            features = cols

        if isinstance(categorical_variable, str):
            categorical_variable = [categorical_variable]
        categorical_variable = self._arg('categorical_variable', categorical_variable, ListOfStrings)
        if string_variable is not None:
            if isinstance(string_variable, str):
                string_variable = [string_variable]
            try:
                self.string_variable = self._arg('string_variable', string_variable, ListOfStrings)
            except:
                msg = "`string_variable` must be list of strings or string."
                logger.error(msg)
                raise TypeError(msg)
        if variable_weight is not None:
            self._check_variable_weight(variable_weight)

        data_ = data[[key] + features]

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        outputs = ['#DBSCAN_{}_TBL_{}_{}'.format(name, self.id, unique_id) for
                   name in ['RESULT', 'MODEL', 'STAT', 'PL']]
        result_tbl, model_tbl = outputs[:2]
        param_rows = [('AUTO_PARAM', None, None, self.auto_param),
                      ('MINPTS', self.minpts, None, None),
                      ('RADIUS', None, self.eps, None),
                      ('THREAD_RATIO', None, self.thread_ratio, None),
                      ('DISTANCE_METHOD', self.metric, None, None),
                      ('MINKOW_P', self.minkowski_power, None, None),
                      ('CATEGORY_WEIGHTS', None, self.category_weights, None),
                      ('METHOD', self.algorithm, None, None),
                      ('SAVE_MODEL', self.save_model, None, None)]
        if categorical_variable is not None:
            param_rows.extend([('CATEGORICAL_VARIABLE', None, None, name)
                               for name in categorical_variable])
        elif self.categorical_variable is not None:
            param_rows.extend([('CATEGORICAL_VARIABLE', None, None, name)
                               for name in self.categorical_variable])
        if self.string_variable is not None:
            param_rows.extend(('STRING_VARIABLE', None, None, variable)
                              for variable in self.string_variable)
        if self.variable_weight is not None:
            param_rows.extend(('VARIABLE_WEIGHT', None, value, key)
                              for key, value in self.variable_weight.items())

        try:
            self._call_pal_auto(conn,
                                'PAL_DBSCAN',
                                data_,
                                ParameterTable().with_data(param_rows),
                                *outputs)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, outputs)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, outputs)
            raise
        self.labels_ = conn.table(result_tbl)
        self.model_ = conn.table(model_tbl) if self.save_model else None
        return self

    def fit_predict(self, data, key=None, features=None, categorical_variable=None, string_variable=None, variable_weight=None):
        """
        Fit with the dataset and return the labels.

        Parameters
        ----------

        data : DataFrame
            DataFrame containing the data.
        key : str, optional
            Name of ID column.

            Defaults to the index column of data (i.e. data.index) if it is set.
            If the index column of data is not provided, please enter the value of key.
        features : a list of str, optional
            A list of Names of the feature columns.
            Since the introduction of SAP HANA Cloud 24 QRC03, the data type support for features has been expanded to include VECTOR TYPE, in addition to the previously supported types such as INTEGER, DOUBLE, DECIMAL(p, s), VARCHAR, and NVARCHAR.

            If ``features`` is not provided, it defaults to all non-key columns. This means that all columns except the key column will be considered as features.

        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.
        string_variable : str or a list of str, optional
            Indicates a string column storing not categorical data.
            Levenshtein distance is used to calculate similarity between two strings. Ignored if it is not a string column.

            Defaults to None.
        variable_weight : dict, optional
            Specifies the weight of a variable participating in distance calculation. The value must be greater or equal to 0.

            Defaults to 1 for variables not specified.

        Returns
        -------
        DataFrame
            Label assigned to each sample.
        """
        self.fit(data, key, features, categorical_variable, string_variable, variable_weight)
        return self.labels_

    @trace_sql
    def predict(self, data, key=None, features=None):
        """
        Assign clusters to data based on a fitted model. This fucntion does not support the data with VECTOR type.
        The output structure of this method does not match that of fit_predict().

        Parameters
        ----------

        data : DataFrame
            Data points to match against computed clusters.

            This dataframe's column structure should match that
            of the data used for fit().

        key : str, optional
            Name of ID column.

            Defaults to the index column of data (i.e. data.index) if it is set.
            If the index column of data is not provided, please enter the value of key.

        features : a list of str, optional.
            Names of feature columns.

            If ``features`` is not provided, it defaults to all non-key columns.

        Returns
        -------
        DataFrame

            Cluster assignment results, with 3 columns:

                - Data point ID, with name and type taken from the input
                  ID column.
                - CLUSTER_ID, type INTEGER, representing the cluster the
                  data point is assigned to.
                - DISTANCE, type DOUBLE, representing the distance between
                  the data point and the nearest core point.
        """
        return super(DBSCAN, self)._predict(data, key, features)#pylint:disable=line-too-long

class GeometryDBSCAN(PALBase):
    r"""
    GeometryDBSCAN is a geometry version of DBSCAN, which only accepts geometry points as input data.
    It works with geospatial data where distances between points can be computed in a geometrically efficient manner.
    Currently GeometryDBSCAN only accepts 2D points.

    Parameters
    ----------

    minpts : int, optional
        Represents the minimum number of points required to form a cluster.

        .. note ::

            ``minpts`` and ``eps`` need to be provided together by user or
            these two parameters are automatically determined.

    eps : float, optional
        Specifies the scanning radius around a point.

        .. note ::

            ``minpts`` and ``eps`` need to be provided together by user or
            these two parameters are automatically determined.

    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to -1.
    metric : {'manhattan', 'euclidean','minkowski', 'chebyshev', 'standardized_euclidean', 'cosine'}, optional

        Defines the metric used to compute the distance between two points.

        Defaults to 'euclidean'.

    minkowski_power : int, optional
        When minkowski is chosen for ``metric``, this parameter controls the value of power.

        Only applicable when ``metric`` is 'minkowski'.

        Defaults to 3.

    algorithm : {'brute-force', 'kd-tree'}, optional

        Represents the chosen method to search for neighbouring data points.

        Defaults to 'kd-tree'.

    save_model : bool, optional

        If set to true, the generated model will be saved.

        It must be mentioned that ``save_model`` has to be set to true in order to utilize the function predict().

        Defaults to True.

    Attributes
    ----------

    labels_ : DataFrame
        Label assigned to each sample.

    model_ : DataFrame
        Model content. Set to None if ``save_model`` is False.

    Examples
    --------

    In SAP HANA, the test table PAL_GEO_DBSCAN_DATA_TBL can be created by the following SQL::

        CREATE COLUMN TABLE PAL_GEO_DBSCAN_DATA_TBL (
            "ID" INTEGER,
            "POINT" ST_GEOMETRY);

    Input DataFrame df:

    >>> df = conn.table("PAL_GEO_DBSCAN_DATA_TBL")

    Create a GeometryDBSCAN instance:

    >>> geo_dbscan = GeometryDBSCAN(thread_ratio=0.2, metric='manhattan')

    Perform fit():

    >>> geo_dbscan.fit(data=df, key='ID')

    Output:

    >>> geo_dbscan.labels_.collect()
         ID  CLUSTER_ID
    0     1           0
    1     2           0
    2     3           0
    ......
    28   29          -1
    29   30          -1

    >>> geo_dbsan.model_.collect()
        ROW_INDEX    MODEL_CONTENT
    0      0         {"Algorithm":"DBSCAN","Cluster":[{"ClusterID":...

    Perform fit_predict():

    >>> result = geo_dbscan.fit_predict(data=df, key='ID')

    Output:

    >>> result.collect()
        ID  CLUSTER_ID
    0    1           0
    1    2           0
    2    3           0
    ......
    28  29          -1
    29  30          -1

    """
    metric_map = {'manhattan':1, 'euclidean':2, 'minkowski':3, 'chebyshev':4,
                  'standardized_euclidean':5, 'cosine':6}
    algorithm_map = {'brute-force':0, 'kd-tree':1}
    def __init__(self, minpts=None, eps=None, thread_ratio=None,
                 metric=None, minkowski_power=None, algorithm=None, save_model=True):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(GeometryDBSCAN, self).__init__()
        self.auto_param = 'true' if any(param is None for param in (minpts, eps)) else 'false'
        if (minpts is not None and eps is None) or (minpts is None and eps is not None):
            msg = 'minpts and eps need to be provided together.'
            logger.error(msg)
            raise ValueError(msg)
        self.minpts = self._arg('minpts', minpts, int)
        self.eps = self._arg('eps', eps, float)
        self.thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        self.metric = self._arg('metric', metric,
                                self.metric_map)
        if self.metric != 3 and minkowski_power is not None:
            msg = 'minkowski_power will only be applicable if metric is minkowski.'
            logger.error(msg)
            raise ValueError(msg)
        self.minkowski_power = self._arg('minkowski_power', minkowski_power, int)

        self.algorithm = self._arg('algorithm', algorithm, self.algorithm_map)
        self.save_model = self._arg('save_model', save_model, bool)

    def fit(self, data, key=None, features=None):
        """
        Fit the model to the training dataset.

        Parameters
        ----------

        data : DataFrame
            DataFrame containing the data.

            It must contain at least two columns: one ID column, and another for storing 2-D geometry points and the data type is 'ST_GEOMETRY'.

        key : str, optional
            Name of ID column.

            Defaults to the index column of data (i.e. data.index) if it is set.
            If the index column of data is not provided, please enter the value of key.

        features : str, optional
            Name of a column for storing geometry points and the data type is 'ST_GEOMETRY'.

            If not provided, it defaults the first non-key column.
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, 'training_data', data)
        conn = data.connection_context
        require_pal_usable(conn)

        cols = data.columns
        index = data.index
        key = self._arg('key', key, str, not isinstance(index, str))
        if isinstance(index, str):
            if key is not None and index != key:
                warn_msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(warn_msg)
        key = index if key is None else key
        cols.remove(key)

        if isinstance(features, str):
            features = [features]
        features = self._arg('features', features, ListOfStrings)
        if features is not None and len(features) > 1:
            msg = "Only the column for storing 2D geometry points should be specified in features."
            logger.error(msg)
            raise ValueError(msg)
        if features is None:
            features = cols[0]

        data_ = data[[key] + [features]]

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        outputs = ['#GEO_DBSCAN_{}_TBL_{}_{}'.format(name, self.id, unique_id) for
                   name in ['RESULT', 'MODEL', 'STAT', 'PL']]
        result_tbl, model_tbl = outputs[:2]
        param_rows = [('AUTO_PARAM', None, None, self.auto_param),
                      ('MINPTS', self.minpts, None, None),
                      ('RADIUS', None, self.eps, None),
                      ('THREAD_RATIO', None, self.thread_ratio, None),
                      ('DISTANCE_METHOD', self.metric, None, None),
                      ('MINKOW_P', self.minkowski_power, None, None),
                      ('METHOD', self.algorithm, None, None),
                      ('SAVE_MODEL', self.save_model, None, None)]

        try:
            self._call_pal_auto(conn,
                                'PAL_GEO_DBSCAN',
                                data_,
                                ParameterTable().with_data(param_rows),
                                *outputs)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, outputs)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, outputs)
            raise

        self.labels_ = conn.table(result_tbl)
        self.model_ = conn.table(model_tbl) if self.save_model else None

    def fit_predict(self, data, key=None, features=None):
        """
        Fit with the dataset and return the labels.

        Parameters
        ----------

        data : DataFrame
            DataFrame containing the data. The structure is as follows.
            It must contain at least two columns: one ID column, and another for storing 2-D geometry points.

        key : str, optional
            Name of ID column.

            Defaults to the index column of data (i.e. data.index) if it is set.
            If the index column of data is not provided, please enter the value of key.
        features : str, optional
            Name of the column for storing 2-D geometry points.

            If not provided, it defaults to the first non-key column.

        Returns
        -------
        DataFrame
            Label assigned to each sample.
        """
        self.fit(data, key, features)
        return self.labels_

class HDBSCAN(PALBase):
    r"""
    DBSCAN(Density-Based Spatial Clustering of Applications with Noise) is a popular algorithm in clustering. HDBSCAN(Hierarchical Density-Based Spatial Clustering of Applications with Noise) on the other hand is a novel algorithm as well based on the idea of DENSITY but in a hierarchical way.
    It first builds a hierarchical structure of clusters based on the density of points, which includes all possible splitting ways of points over different densities. Then instead of selecting clusters based on some fixed density like DBSCAN does, HDBSCAN selects a set of flat clusters from the structure built before aiming at maximizing a so called concept STABILITY.
    Due to the special selection method applied, HDBSCAN has the ability of extracting clusters with different densities, which gives it the advantage of more robust. And it discards the hyper-parameter "radius" in DBSCAN which is difficult to choose for deciding a proper density.

    Parameters
    ----------
    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 1.0.
    min_cluster_size : int, optional
        Specifies the minimum number of points in a cluster.

        Defaults to 5.
    max_cluster_size : int, optional
        Specifies the maximum number of points in a cluster. 0 means no limitation.
        This size might be overridden in some cases.

        Defaults to 0.
    min_sample : int, optional
        A heuristic value to indicate the minimum density to form a cluster.

        Defaults to value of ``min_cluster_size``.
    cluster_selection_eps : float, optional
        Clusters of linking value less than this parameter will be merged.
        Used to merge small clusters. 0.0 means no such merging.

        Defaults to 0.0.
    allow_single_cluster : bool, optional
        Indicates whether allow the whole dataset to form a single cluster.

        Defaults to False.
    metric : {'Manhattan', 'Euclidean', 'Minkowski', 'Chebyshev'}, optional
        Indicates using which metric to measure distance between two points.

        Defaults to 'Euclidean'.
    minkowski_power : float, optional
        When minkowski is chosen for ``metric``, this parameter
        controls the value of power. Only applicable when ``metric`` is 'minkowski'.

        Defaults to 3.0.
    category_weights : float, optional
        Represents the weight of categorical variable.

        Defaults to 0.707.
    algorithm : {'brute-force', 'kd-tree'}, optional
        Specifies the method to accomplish HDBSCAN. optionas are 'brute-force' and 'kd-tree'.
        KD tree can be used to accelerate the process when N >> 2^D,
        where N is the number of points and D is the number of continuous dimensions of data.

        Defaults to 'brute-force'.
    min_leaf_kdtree : int, optional
        KD tree related parameter to specify the minimum number of points contained in a leaf node.

        Defaults to 16.

    Attributes
    ----------

    labels_ : DataFrame
        Label assigned to each sample.

    Examples
    --------
    >>> hdbscan = HDBSCAN(min_cluster_size=3, min_sample=3,
                         allow_single_cluster=True,algorithm='brute-force',
                         metric='euclidean', thread_ratio=1.0)

    Perform fit():

    >>> hdbscan.fit(data=df, key='ID')
    >>> hdbscan.labels_.collect()

    Perform fit_predict():

    >>> res = hdbscan.fit_predict(data=df, key='ID')
    >>> res.collect()

    """
    metric_map = {'manhattan':1, 'euclidean':2, 'minkowski':3, 'chebyshev':4}
    algorithm_map = {'brute-force':0, 'kd-tree':1}
    def __init__(self, thread_ratio=None, min_cluster_size=None, max_cluster_size=None,
                 min_sample=None, cluster_selection_eps=None,
                 allow_single_cluster=False, metric=None, minkowski_power=None,
                 category_weights=None, algorithm=None, min_leaf_kdtree=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(HDBSCAN, self).__init__()
        self.thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        self.min_cluster_size = self._arg('min_cluster_size', min_cluster_size, int)
        self.max_cluster_size = self._arg('max_cluster_size', max_cluster_size, int)
        self.min_sample = self._arg('min_sample', min_sample, int)
        self.cluster_selection_eps = self._arg('cluster_selection_eps', cluster_selection_eps, float)
        self.allow_single_cluster = self._arg('allow_single_cluster', allow_single_cluster, bool)
        self.metric = self._arg('metric', metric, self.metric_map)
        self.minkowski_power = self._arg('minkowski_power', minkowski_power, float)
        self.category_weights = self._arg('category_weights', category_weights, float)
        self.algorithm = self._arg('algorithm', algorithm, self.algorithm_map)
        self.min_leaf_kdtree = self._arg('min_leaf_kdtree', min_leaf_kdtree, int)

    @trace_sql
    def fit(self, data, key=None, features=None, categorical_variable=None):
        """
        Fit the HDBSCAN clustering to the training dataset.

        Parameters
        ----------
        data : DataFrame
            DataFrame containing the data.
        key : str, optional
            Name of ID column.

            Defaults to the index column of data (i.e. data.index) if it is set.
            If the index column of data is not provided, please enter the value of key.
        features : a list of str, optional
            Names of the feature columns.

            If ``features`` is not provided, it defaults to all non-key columns.
        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.

        Returns
        -------
        A fitted object of class "HDBSCAN".
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, 'training_data', data)
        conn = data.connection_context
        require_pal_usable(conn)

        cols = data.columns
        index = data.index
        key = self._arg('key', key, str, not isinstance(index, str))
        if isinstance(index, str):
            if key is not None and index != key:
                warn_msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(warn_msg)
        key = index if key is None else key
        cols.remove(key)

        features = self._arg('features', features, ListOfStrings)
        if features is None:
            features = cols

        if isinstance(categorical_variable, str):
            categorical_variable = [categorical_variable]
        categorical_variable = self._arg('categorical_variable', categorical_variable, ListOfStrings)
        data_ = data[[key] + features]

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        outputs = ['#HDBSCAN_{}_TBL_{}_{}'.format(name, self.id, unique_id) for
                   name in ['RESULT', 'PL', 'STAT']]
        result_tbl = outputs[0]

        param_rows = [('THREAD_RATIO', None, self.thread_ratio, None),
                      ('MIN_CLUSTER_SIZE', self.min_cluster_size, None, None),
                      ('MAX_CLUSTER_SIZE', self.max_cluster_size, None, None),
                      ('MIN_SAMPLE', self.min_sample, None, None),
                      ('CLUSTER_SELECTION_EPS', None, self.cluster_selection_eps, None),
                      ('ALLOW_SINGLE_CLUSTER', self.allow_single_cluster, None, None),
                      ('DISTANCE_METRIC', self.metric, None, None),
                      ('MINKOW_P', None, self.minkowski_power, None),
                      ('CATEGORY_WEIGHTS', None, self.category_weights, None),
                      ('METHOD', self.algorithm, None, None),
                      ('MIN_LEAF_KDTREE', self.min_leaf_kdtree, None, None)]
        if categorical_variable is not None:
            param_rows.extend([('CATEGORICAL_VARIABLE', None, None, name)
                               for name in categorical_variable])
        if not (check_pal_function_exist(conn, '%HDBSCAN%', like=True) or \
        self._disable_hana_execution):
            msg = 'The version of your SAP HANA does not support HDBSCAN!'
            logger.error(msg)
            raise ValueError(msg)
        try:
            self._call_pal_auto(conn,
                                'PAL_HDBSCAN',
                                data_,
                                ParameterTable().with_data(param_rows),
                                *outputs)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, outputs)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, outputs)
            raise
        self.labels_ = conn.table(result_tbl)
        return self

    def fit_predict(self, data, key=None, features=None, categorical_variable=None):
        """
        Invoke HDBSCAN to the given data and return the cluster labels.

        Parameters
        ----------

        data : DataFrame
            DataFrame containing the data.
        key : str, optional
            Name of ID column.

            Defaults to the index column of data (i.e. data.index) if it is set.
            If the index column of data is not provided, please enter the value of key.
        features : a list of str, optional
            A list of Names of the feature columns.
            Since the introduction of SAP HANA Cloud 24 QRC03, the data type support for features has been expanded to include VECTOR TYPE, in addition to the previously supported types such as INTEGER, DOUBLE, DECIMAL(p, s), VARCHAR, and NVARCHAR.

            If ``features`` is not provided, it defaults to all non-key columns. This means that all columns except the key column will be considered as features.
        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.

        Returns
        -------
        DataFrame
            Label assigned to each sample.
        """
        self.fit(data, key, features, categorical_variable)
        return self.labels_

class KMeans(_ClusterAssignmentMixin):
    r"""
    K-means is one of the simplest and most commonly used unsupervised machine learning algorithms for partitioning a dataset into K distinct, non-overlapping clusters based on the distances between the center of the cluster (centroid) and the data points.

    Parameters
    ----------

    n_clusters : int, optional
        Specifies the number of clusters (k) required. The acceptable range is between 2 to the number of training records.

        If this parameter is not specified, you must specify the range of k using ``n_clusters_min'' and ``n_clusters_max`` parameters instead. Then the algorithm will iterate through the range and return the k with the highest slight silhouette.

    n_clusters_min : int, optional
        Provides the lower boundary of the range that k falls in.

        You must specify either an exact value (``n_clusters``) or a range (``n_clusters_min'' and ``n_clusters_max``) for k. If both are specified, the exact value will be used.

        No default value.
    n_clusters_max : int, optional
        Provides the upper boundary of the range that k falls in.

        You must specify either an exact value (``n_clusters``) or a range (``n_clusters_min'' and ``n_clusters_max``) for k. If both are specified, the exact value will be used.

        No default value.
    init : {'first_k', 'replace', 'no_replace', 'patent'}, optional
        Governs the selection of initial cluster centers:

          - 'first_k': First k observations.
          - 'replace': Random with replacement.
          - 'no_replace': Random without replacement.
          - 'patent': Patent of selecting the init center (US 6,882,998 B1).

        Defaults to 'patent'.

    max_iter : int, optional
        Specifies the maximal number of iterations to be performed.

        Defaults to 100.

    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 0.
    distance_level : {'manhattan', 'euclidean', 'minkowski', 'chebyshev', 'cosine'}, optional
        Ways to compute the distance between the item and the cluster center.

        'cosine' is only applicable when ``accelerated`` is set to False.

        Defaults to 'euclidean'.
    minkowski_power : float, optional
        When Minkowski distance is used, this parameter controls the
        value of power. Only valid when ``distance_level`` is 'minkowski'.

        Defaults to 3.0.
    category_weights : float, optional
        Represents the weight of category attributes.

        Defaults to 0.707.
    normalization : {'no', 'l1_norm', 'min_max'}, optional
        Normalization type.

          - 'no': No normalization will be applied.
          - 'l1_norm': Yes, for each point X (x\ :sub:`1`\, x\ :sub:`2`\, ..., x\ :sub:`n`\), the normalized
            value will be X'(x\ :sub:`1` /S,x\ :sub:`2` /S,...,x\ :sub:`n` /S),
            where S = \|x\ :sub:`1`\ \|+\|x\ :sub:`2`\ \|+...\|x\ :sub:`n`\ \|.
          - 'min_max': Yes, for each column C, get the min and max value of C,
            and then C[i] = (C[i]-min)/(max-min).

        Defaults to 'no'.
    categorical_variable : str or a list of str, optional
        Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

        No default value.
    tol : float, optional
        Threshold (actual value) for exiting the iterations.

        Only valid when ``accelerated`` is False.

        Defaults to 1.0e-6.
    memory_mode : {'auto', 'optimize-speed', 'optimize-space'}, optional
        Indicates the memory mode that the algorithm uses.

          - 'auto': Chosen by algorithm.
          - 'optimize-speed': Prioritizes speed.
          - 'optimize-space': Prioritizes memory.

        Only applicable when ``accelerated`` is set to True.

        Defaults to 'auto'.
    accelerated : bool, optional
        Indicates whether to use technology like cache to accelerate the
        calculation process:

        - If True, the calculation process will be accelerated.
        - If False, the calculation process will not be accelerated.

        Defaults to False.
    use_fast_library : bool, optional
        Use vectorized accelerated operation when it is set to True.

        Defaults to False.
    use_float : bool, optional
        Floating point type:

        - True: float
        - False: double

        Only valid when ``use_fast_library`` is set to True.

        Defaults to True.

    Attributes
    ----------

    labels_ : DataFrame
        Label assigned to each sample.

    cluster_centers_ : DataFrame
        Coordinates of cluster centers.

    model_ : DataFrame
        Model content.

    statistics_ : DataFrame
        Statistics.

    Examples
    --------

    Input DataFrame df:

    >>> df.collect()
        ID  V000 V001  V002
    0    0   0.5    A   0.5
    1    1   1.5    A   0.5
    ...
    19  19  15.7    A   1.6

    Create a KMeans instance:

    >>> km = clustering.KMeans(n_clusters=4, init='first_k',
    ...                        max_iter=100, tol=1.0E-6, thread_ratio=0.2,
    ...                        distance_level='Euclidean', category_weights=0.5)

    Perform fit_predict():

    >>> labels = km.fit_predict(data=df, 'ID')
    >>> labels.collect()
        ID  CLUSTER_ID  DISTANCE  SLIGHT_SILHOUETTE
    0    0           0  0.891088           0.944370
    1    1           0  0.863917           0.942478
    ...
    19  19           1  1.102342           0.925679

    Input DataFrame df:

    >>> df.collect()
        ID  V000 V001  V002
    0    0   0.5    A     0
    1    1   1.5    A     0
    ...
    19  19  15.7    A     1

    Create Accelerated Kmeans instance:

    >>> akm = clustering.KMeans(init='first_k',
    ...                         thread_ratio=0.5, n_clusters=4,
    ...                         distance_level='euclidean',
    ...                         max_iter=100, category_weights=0.5,
    ...                         categorical_variable=['V002'],
    ...                         accelerated=True)

    Perform fit_predict():

    >>> labels = akm.fit_predict(data=df, key='ID')
    >>> labels.collect()
        ID  CLUSTER_ID  DISTANCE  SLIGHT_SILHOUETTE
    0    0           0  1.198938           0.006767
    1    1           0  1.123938           0.068899
    ...
    19  19           1  0.915565           0.937717
    """

    distance_map_km = {'manhattan':1, 'euclidean':2, 'minkowski':3, 'chebyshev':4, 'cosine':6}
    distance_map_acc = {'manhattan':1, 'euclidean':2, 'minkowski':3, 'chebyshev':4}
    normalization_map = {'no':0, 'l1_norm':1, 'min_max':2}
    init_map = {"first_k":1, "replace":2, "no_replace":3, "patent":4}
    mem_mode_map = {'auto':0, 'optimize-speed':1, 'optimize-space':2}

    def __init__(self, n_clusters=None, n_clusters_min=None, n_clusters_max=None,
                 init=None, max_iter=None, thread_ratio=None, distance_level=None,
                 minkowski_power=None, category_weights=None, normalization=None,
                 categorical_variable=None, tol=None, memory_mode=None, accelerated=False,
                 use_fast_library=None, use_float=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(KMeans, self).__init__()
        if n_clusters is None:
            if n_clusters_max is None or n_clusters_min is None:
                msg = 'You must specify either an exact value or a range for number of clusters'
                logger.error(msg)
                raise ValueError(msg)
        else:
            if n_clusters_min is not None or n_clusters_max is not None:
                msg = ('Both exact value and range ending points' +
                       ' are provided for number of groups, please choose one or the other.')
                logger.error(msg)
                raise ValueError(msg)
        self.n_clusters = self._arg('n_clusters', n_clusters, int)
        self.n_clusters_min = self._arg('n_clusters_min', n_clusters_min, int)
        self.n_clusters_max = self._arg('n_clusters_max', n_clusters_max, int)
        self.init = self._arg('init', init, self.init_map)
        self.max_iter = self._arg('max_iter', max_iter, int)
        self.thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        self.minkowski_power = self._arg('minkowski_power', minkowski_power, float)
        self.category_weights = self._arg('category_weights', category_weights, float)
        self.normalization = self._arg('normalization', normalization, self.normalization_map)
        if isinstance(categorical_variable, str):
            categorical_variable = [categorical_variable]
        self.categorical_variable = self._arg('categorical_variable', categorical_variable,
                                              ListOfStrings)
        if accelerated:
            self.mem_mode = self._arg('memory_mode', memory_mode, self.mem_mode_map)
            distance_map = self.distance_map_acc
            if tol is not None:
                msg = 'Tol is only valid when accelerated is false.'
                logger.error(msg)
                raise ValueError(msg)
        else:
            self.tol = self._arg('tol', tol, float)
            distance_map = self.distance_map_km
            if memory_mode is not None:
                msg = 'Memory_mode is only valid when accelerated is true.'
                logger.error(msg)
                raise ValueError(msg)
        self.distance_level = self._arg('distance_level', distance_level, distance_map)
        if self.distance_level != 3 and minkowski_power is not None:
            msg = 'Minkowski_power will only be valid if distance_level is Minkowski.'
            logger.error(msg)
            raise ValueError(msg)
        self.accelerated = self._arg('accelerated', accelerated, bool)
        self.use_fast_library = self._arg('use_fast_library', use_fast_library, bool)
        self.use_float = self._arg('use_float', use_float, bool)

    def _prep_param(self):
        param_rows = [('GROUP_NUMBER', self.n_clusters, None, None),
                      ('GROUP_NUMBER_MIN', self.n_clusters_min, None, None),
                      ('GROUP_NUMBER_MAX', self.n_clusters_max, None, None),
                      ('DISTANCE_LEVEL', self.distance_level, None, None),
                      ('MINKOWSKI_POWER', None, self.minkowski_power, None),
                      ('CATEGORY_WEIGHTS', None, self.category_weights, None),
                      ('MAX_ITERATION', self.max_iter, None, None),
                      ('INIT_TYPE', self.init, None, None),
                      ('NORMALIZATION', self.normalization, None, None),
                      ('THREAD_RATIO', None, self.thread_ratio, None),
                      ('USE_FAST_LIBRARY', self.use_fast_library, None, None),
                      ('USE_FLOAT', self.use_float, None, None)]
        #for categorical variable
        if self.categorical_variable is not None:
            param_rows.extend([('CATEGORICAL_VARIABLE', None, None, variable)
                               for variable in self.categorical_variable])
        if self.accelerated:
            proc_name = "PAL_ACCELERATED_KMEANS"
            param_rows.append(('MEMORY_MODE', self.mem_mode, None, None))
        else:
            proc_name = "PAL_KMEANS"
            param_rows.append(('EXIT_THRESHOLD', None, self.tol, None))
        return proc_name, param_rows

    @trace_sql
    def fit(self, data, key=None, features=None, categorical_variable=None):
        """
        Fit the model to the training dataset.

        Parameters
        ----------

        data : DataFrame
            DataFrame containing the data.
        key : str, optional
            Name of ID column.
            Defaults to the index column of data (i.e. data.index) if it is set.

            If the index column of data is not provided, please enter the value of key.
        features : a list of str, optional
            Names of feature columns.

            If ``features`` is not provided, it defaults to all non-key columns.
        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.

        Returns
        -------
        A fitted object of class "KMeans".
        """
        #PAL input format ID, Features
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, 'training_data', data)
        conn = data.connection_context
        require_pal_usable(conn)

        if isinstance(categorical_variable, str):
            categorical_variable = [categorical_variable]
        categorical_variable = self._arg('categorical_variable', categorical_variable, ListOfStrings)

        cols = data.columns
        index = data.index
        key = self._arg('key', key, str, not isinstance(index, str))
        if isinstance(index, str):
            if key is not None and index != key:
                warn_msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(warn_msg)
        key = index if key is None else key
        cols.remove(key)

        features = self._arg('features', features, ListOfStrings)
        if features is None:
            features = cols

        data_ = data[[key] + features]

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        outputs = ['#PAL_KMEANS_{}_TBL_{}_{}'.format(name, self.id, unique_id)
                   for name in ['RESULT', 'CENTERS', 'MODEL', 'STATISTICS', 'PLACEHOLDER']]
        result_tbl, centers_tbl, model_tbl, statistics_tbl, placeholder_tbl = outputs

        proc_name, param_rows = self._prep_param()
        if categorical_variable is not None:
            param_rows.extend([('CATEGORICAL_VARIABLE', None, None, variable)
                               for variable in categorical_variable])
        try:
            self._call_pal_auto(conn,
                                proc_name,
                                data_,
                                ParameterTable().with_data(param_rows),
                                *outputs)

        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, outputs)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, outputs)
            raise
        self.cluster_centers_ = conn.table(centers_tbl)
        self.labels_ = conn.table(result_tbl)
        self.model_ = conn.table(model_tbl)
        self.statistics_ = conn.table(statistics_tbl)
        return self

    def fit_predict(self, data, key=None, features=None, categorical_variable=None):
        """
        Fit with the dataset and return the labels.

        Parameters
        ----------

        data : DataFrame
            DataFrame containing the data.
        key : str, optional
            Name of ID column.

            Defaults to the index column of data (i.e. data.index) if it is set.
            If the index column of data is not provided, please enter the value of key.
        features : a list of str, optional
            Names of feature columns.

            If ``features`` is not provided, it defaults to all non-key columns.
        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.


        Returns
        -------

        DataFrame

            Label assigned to each sample.
        """
        self.fit(data, key, features, categorical_variable)
        return self.labels_

    @trace_sql
    def predict(self, data, key=None, features=None):
        """
        Assign clusters to data based on a fitted model.

        The output structure of this method does not match that of
        fit_predict().

        Parameters
        ----------

        data : DataFrame
            Data points to match against computed clusters.

            This dataframe's column structure should match that
            of the data used for fit().
        key : str, optional
            Name of ID column.

            Defaults to the index column of data (i.e. data.index) if it is set.
            If the index column of data is not provided, please enter the value of key.
        features : a list of str, optional.
            Names of feature columns.

            If ``features`` is not provided, it defaults to all
            non-key columns.

        Returns
        -------

        DataFrame

            Cluster assignment results, with 3 columns:

              - Data point ID, with name and type taken from the input
                ID column.
              - CLUSTER_ID, INTEGER type, representing the cluster the
                data point is assigned to.
              - DISTANCE, DOUBLE type, representing the distance between
                the data point and the cluster center.
        """
        return super(KMeans, self)._predict(data, key, features)#pylint:disable=line-too-long

class _KClusteringBase(PALBase):
    """Base class for K-Medians and K-Medoids clustering algorithms."""

    clustering_type_proc_map = {'KMedians' :'PAL_KMEDIANS', 'KMedoids':'PAL_KMEDOIDS'}
    #meant to be override
    distance_map = {}
    normalization_map = {'no':0, 'l1_norm':1, 'min_max':2}
    init_map = {"first_k":1, "replace":2, "no_replace":3, "patent":4}

    def __init__(self,
                 n_clusters,
                 init=None,
                 max_iter=None,
                 tol=None,
                 thread_ratio=None,
                 distance_level=None,
                 minkowski_power=None,
                 category_weights=None,
                 normalization=None,
                 categorical_variable=None
                ):
        if not hasattr(self, 'hanaml_parameters'):
            setattr(self, 'hanaml_parameters', pal_param_register())
        super(_KClusteringBase, self).__init__()
        self.n_cluster = self._arg('n_clusters', n_clusters, int, True)
        self.init = self._arg('init', init, self.init_map)
        self.max_iter = self._arg('max_iter', max_iter, int)
        self.tol = self._arg('tol', tol, float)
        self.thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        if not self.distance_map:
            raise NotImplementedError
        self.distance_level = self._arg('distance_level', distance_level, self.distance_map)
        if minkowski_power is not None and self.distance_level != 3:
            msg = ("Invalid minkowski_power, " +
                   "valid when distance_level is Minkowski distance")
            logger.error(msg)
            raise ValueError(msg)
        self.minkowski_power = self._arg('minkowski_power', minkowski_power, float)
        self.category_weights = self._arg('category_weights', category_weights, float)
        self.normalization = self._arg('normalization', normalization, self.normalization_map)
        if isinstance(categorical_variable, str):
            categorical_variable = [categorical_variable]
        self.categorical_variable = self._arg('categorical_variable', categorical_variable,
                                              ListOfStrings)
        self.labels_ = None
        self.cluster_centers_ = None
        self._proc_name = self.clustering_type_proc_map[self.__class__.__name__]

    def _prep_param(self):
        param_data = [
            ('GROUP_NUMBER', self.n_cluster, None, None),
            ('DISTANCE_LEVEL', self.distance_level, None, None),
            ('MINKOWSKI_POWER', None, self.minkowski_power, None),
            ('CATEGORY_WEIGHTS', None, self.category_weights, None),
            ('MAX_ITERATION', self.max_iter, None, None),
            ('INIT_TYPE', self.init, None, None),
            ('NORMALIZATION', self.normalization, None, None),
            ('THREAD_RATIO', None, self.thread_ratio, None),
            ('EXIT_THRESHOLD', None, self.tol, None)
            ]
        if self.categorical_variable is not None:
            param_data.extend(('CATEGORICAL_VARIABLE', None, None, variable)
                              for variable in self.categorical_variable)
        return param_data

    @trace_sql
    def fit(self, data, key=None, features=None, categorical_variable=None):
        """
        Fit the model to the training dataset.

        Parameters
        ----------

        data : DataFrame
            DataFrame containing the input data.
        key : str, optional
            Name of ID column.

            Defaults to the index column of data (i.e. data.index) if it is set.
            If the index column of data is not provided, please enter the value of key.
        features : a list of str, optional
            Names of feature columns.
            If ``features`` is not provided, it defaults to all non-key columns.
        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.
        """
        if not hasattr(self, 'hanaml_fit_params'):
            setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, 'training_data', data)
        conn = data.connection_context
        require_pal_usable(conn)

        if isinstance(categorical_variable, str):
            categorical_variable = [categorical_variable]
        categorical_variable = self._arg('categorical_variable', categorical_variable, ListOfStrings)

        cols = data.columns
        index = data.index
        key = self._arg('key', key, str, not isinstance(index, str))
        if isinstance(index, str):
            if key is not None and index != key:
                warn_msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(warn_msg)
        key = index if key is None else key
        cols.remove(key)

        features = self._arg('features', features, ListOfStrings)
        if features is None:
            features = cols

        data_ = data[[key] + features]

        if self.distance_level == 5:
            for col_name, col_type in colspec_from_df(data[data.columns[1:]]):
                if (col_type == "DOUBLE" or
                        col_type == "INT" and self.categorical_variable is None or
                        col_type == "INT" and col_name not in self.categorical_variable):
                    msg = "When jaccard distance is used, all columns must be categorical."
                    logger.error(msg)
                    raise ValueError(msg)

        param_data = self._prep_param()
        if categorical_variable is not None:
            param_data.extend(('CATEGORICAL_VARIABLE', None, None, variable)
                              for variable in categorical_variable)

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        result_tbl = '#{}_RESULT_TBL_{}_{}'.format(self._proc_name, self.id, unique_id)
        centers_tbl = '#{}_CENTERS_TBL_{}_{}'.format(self._proc_name, self.id, unique_id)

        try:
            self._call_pal_auto(conn,
                                self._proc_name,
                                data_,
                                ParameterTable().with_data(param_data),
                                result_tbl,
                                centers_tbl)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, result_tbl)
            try_drop(conn, centers_tbl)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, result_tbl)
            try_drop(conn, centers_tbl)
            raise
        self.cluster_centers_ = conn.table(centers_tbl)
        self.labels_ = conn.table(result_tbl)

    def fit_predict(self, data, key=None, features=None, categorical_variable=None):
        """
        Perform clustering algorithm and return labels.

        Parameters
        ----------

        data : DataFrame
            DataFrame containing input data.
        key : str, optional
            Name of ID column.

            Defaults to the index column of data (i.e. data.index) if it is set.
            If the index column of data is not provided, please enter the value of key.
        features : a list of str, optional
            Names of feature columns.

            If ``features`` is not provided, it defaults to all non-key columns.
        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.

        Returns
        -------
        DataFrame

            Fit result, structured as follows:

              - ID column, with the same name and type as ``data`` 's ID column.
              - CLUSTER_ID, type INTEGER, cluster ID assigned to the data
                point.
              - DISTANCE, type DOUBLE, the distance between the given
                point and the cluster center.
        """
        self.fit(data, key, features, categorical_variable)
        return self.labels_

class KMedians(_KClusteringBase):
    r"""
    K-Medians clustering algorithm that partitions n observations into
    K clusters according to their nearest cluster center. It uses the medians
    of the points to define the center. This makes it more robust against outliers.

    Parameters
    ----------

    n_clusters : int
        Number of groups.
    init : {'first_k', 'replace', 'no_replace', 'patent'}, optional
        Controls how the initial centers are selected:

          - 'first_k': First k observations.
          - 'replace': Random with replacement.
          - 'no_replace': Random without replacement.
          - 'patent': Patent of selecting the init center (US 6,882,998 B1).

        Defaults to 'patent'.
    max_iter : int, optional
        Max iterations.

        Defaults to 100.
    tol : float, optional
        Convergence threshold for exiting iterations.

        Defaults to 1.0e-6.
    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 0.
    distance_level : {'manhattan', 'euclidean', 'minkowski', 'chebyshev', 'cosine'}, optional
        Ways to compute the distance between the item and the cluster center.

        Defaults to 'euclidean'.
    minkowski_power : float, optional
        When Minkowski distance is used, this parameter controls the value of
        power. Only valid when ``distance_level`` is 'minkowski'.

        Defaults to 3.0.
    category_weights : float, optional
        Represents the weight of category attributes.

        Defaults to 0.707.
    normalization : {'no', 'l1_norm', 'min_max'}, optional
        Normalization type.

        - 'no': No, normalization will not be applied.
        - 'l1_norm': Yes, for each point X (x\ :sub:`1`\, x\ :sub:`2`\, ..., x\ :sub:`n`\), the normalized
          value will be X'(x\ :sub:`1`\ /S,x\ :sub:`2`\ /S,...,x\ :sub:`n`\ /S),
          where S = \|x\ :sub:`1`\ \|+\|x\ :sub:`2`\ \|+...\|x\ :sub:`n`\ \|.
        - 'min_max': Yes, for each column C, get the min and max value of C,
          and then C[i] = (C[i]-min)/(max-min).

        Defaults to 'no'.
    categorical_variable : str or a list of str, optional
        Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

        No default value.

    Attributes
    ----------

    cluster_centers_ : DataFrame
        Coordinates of cluster centers.

    labels_ : DataFrame
        Cluster assignment and distance to cluster center for each point.

    Examples
    --------

    Input DataFrame df:

    >>> df.collect()
        ID  V000 V001  V002
    0    0   0.5    A   0.5
    1    1   1.5    A   0.5
    ...
    18  18  15.5    D   1.5
    19  19  15.7    A   1.6

    Creating KMedians instance:

    >>> kmedians = KMedians(n_clusters=4, init='first_k',
    ...                     max_iter=100, tol=1.0E-6,
    ...                     distance_level='Euclidean',
    ...                     thread_ratio=0.3, category_weights=0.5)

    Performing fit() and obtain the result:

    >>> kmedians.fit(data=df, key='ID')
    >>> kmedians.cluster_centers_.collect()
       CLUSTER_ID  V000 V001  V002
    0           0   1.1    A   1.2
    1           1  15.7    D   1.5
    2           2  15.6    C  16.2
    3           3   1.2    B  16.1

    Performing fit_predict():

    >>> kmedians.fit_predict(data=df1, key='ID').collect()
        ID  CLUSTER_ID  DISTANCE
    0    0           0  0.921954
    1    1           0  0.806226
    ...
    18  18           1  0.200000
    19  19           1  0.807107
    """
    distance_map = {'manhattan':1, 'euclidean':2,
                    'minkowski':3, 'chebyshev':4,
                    'cosine':6}

class KMedoids(_KClusteringBase):
    r"""
    K-Medoids clustering algorithm that partitions n observations into
    K clusters according to their nearest cluster center. K-medoids uses the most central observation, known as the medoid. K-Medoids is more robust to noise and outliers.

    Parameters
    ----------

    n_clusters : int
        Number of groups.
    init : {'first_k', 'replace', 'no_replace', 'patent'}, optional
        Controls how the initial centers are selected:

        - 'first_k': First k observations.
        - 'replace': Random with replacement.
        - 'no_replace': Random without replacement.
        - 'patent': Patent of selecting the init center (US 6,882,998 B1).

        Defaults to 'patent'.
    max_iter : int, optional
        Max iterations.

        Defaults to 100.
    tol : float, optional
        Convergence threshold for exiting iterations.

        Defaults to 1.0e-6.
    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 0.
    distance_level : {'manhattan', 'euclidean', 'minkowski', 'chebyshev', 'cosine'}, optional
        Ways to compute the distance between the item and the cluster center.

        Defaults to 'euclidean'.
    minkowski_power : float, optional
        When Minkowski distance is used, this parameter controls the
        value of power. Only valid when ``distance_level`` is 'minkowski'.

        Defaults to 3.0.
    category_weights : float, optional
        Represents the weight of category attributes.

        Defaults to 0.707.
    normalization : {'no', 'l1_norm', 'min_max'}, optional
        Normalization type.

        - 'no': No, normalization will not be applied.
        - 'l1_norm': Yes, for each point X (x\ :sub:`1`\, x\ :sub:`2`\, ..., x\ :sub:`n`\), the normalized
            value will be X'(x\ :sub:`1`\ /S,x\ :sub:`2`\ /S,...,x\ :sub:`n`\ /S),
            where S = \|x\ :sub:`1`\ \|+\|x\ :sub:`2`\ \|+...\|x\ :sub:`n`\ \|.
        - 'min_max': Yes, for each column C, get the min and max value of C,
            and then C[i] = (C[i]-min)/(max-min).

        Defaults to 'no'.
    categorical_variable : str or a list of str, optional
        Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

        No default value.

    Attributes
    ----------

    cluster_centers_ : DataFrame
        Coordinates of cluster centers.

    labels_ : DataFrame
        Cluster assignment and distance to cluster center for each point.

    Examples
    --------

    Input DataFrame df:

    >>> df.collect()
        ID  V000 V001  V002
    0    0   0.5    A   0.5
    1    1   1.5    A   0.5
    ...
    18  18  15.5    D   1.5
    19  19  15.7    A   1.6

    Creating a KMedoids instance:

    >>> kmedoids = KMedoids(n_clusters=4, init='first_K',
    ...                     max_iter=100, tol=1.0E-6,
    ...                     distance_level='Euclidean',
    ...                     thread_ratio=0.3, category_weights=0.5)

    Performing fit() and obtain the result:

    >>> kmedoids.fit(data=df, key='ID')
    >>> kmedoids.cluster_centers_.collect()
       CLUSTER_ID  V000 V001  V002
    0           0   1.5    A   1.5
    1           1  15.5    D   1.5
    2           2  15.5    C  16.5
    3           3   1.5    B  16.5

    Performing fit_predict():

    >>> kmedoids.fit_predict(data=df, key='ID').collect()
        ID  CLUSTER_ID  DISTANCE
    0    0           0  1.414214
    1    1           0  1.000000
    ...
    18  18           1  0.000000
    19  19           1  0.930714
    """
    distance_map = {'manhattan':1, 'euclidean':2,
                    'minkowski':3, 'chebyshev':4,
                    'jaccard':5, 'cosine':6}

def outlier_detection_kmeans(data, # pylint: disable=too-many-locals
                             key=None,
                             features=None,
                             n_clusters=None,
                             distance_level=None,
                             contamination=None,
                             sum_distance=True,
                             init=None,
                             max_iter=None,
                             normalization=None,
                             tol=None,
                             distance_threshold=None,
                             thread_number=None):
    r"""
    Outlier detection based on k-means clustering. It uses the K-means algorithm to find the farthest point from the centroid as an outlier.

    Parameters
    ----------
    data : DataFrame
        Input data.

    key : str, optional
        Name of ID column.

        Defaults to the index column of data (i.e. data.index) if it is set.
        If the index column of data is not provided, please enter the value of key.

    features : str or ListOfStrings
        Names of the features columns in ``data`` that are used for
        calculating distances of points in ``data`` for clustering.

        Feature columns must be numerical.

        Defaults to all non-key columns if not provided.

    n_clusters : int, optional
        Number of clusters to be grouped.

        If this number is not specified, the G-means method will be used to determine the
        number of clusters.
    distance_level : {'manhattan', 'euclidean', 'minkowski'}, optional
        Specifies the distance type between data points and cluster center.

        - 'manhattan' : Manhattan distance.
        - 'euclidean' : Euclidean distance.
        - 'minkowski' : Minkowski distance.

        Defaults to 'euclidean'.

    contamination : float, optional
        Specifies the proportion of outliers in ``data``.

        Expected to be a positive number no greater than 1.

        Defaults to 0.1.

    sum_distance : bool, optional
        Specifies whether or not to use the sum distance of a point to all cluster
        centers as its distance value for outlier score. If False, only the distance of a point
        to the center it belongs to is used its distance value calculation.

        Defaults to True.

    init : {'first_k', 'replace', 'no_replace', 'patent'}, optional
        Controls how the initial centers are selected:

        - 'first_k': First k observations.
        - 'replace': Random with replacement.
        - 'no_replace': Random without replacement.
        - 'patent': Patent of selecting the init center (US 6,882,998 B1).

        Defaults to 'patent'.

    max_iter : int, optional
        Maximum number of iterations for k-means clustering.

        Defaults to 100.

    normalization : {'no', 'l1_norm', 'min_max'}, optional
        Normalization type.

          - 'no': No normalization will be applied.
          - 'l1_norm': Yes, for each point X (x\ :sub:`1`\, x\ :sub:`2`\, ..., x\ :sub:`n`\), the normalized
            value will be X'(x\ :sub:`1` /S,x\ :sub:`2` /S,...,x\ :sub:`n` /S),
            where S = \|x\ :sub:`1`\ \|+\|x\ :sub:`2`\ \|+...\|x\ :sub:`n`\ \|.
          - 'min_max': Yes, for each column C, get the min and max value of C,
            and then C[i] = (C[i]-min)/(max-min).

        Defaults to 'no'.

    tol : float, optional
        Convergence threshold for exiting iterations in k-means clustering.

        Defaults to 1.0e-9.

    distance_threshold : float, optional
        Specifies the threshold distance value for outlier detection.

        A point with distance value no greater than the threshold is not considered to be outlier.

        Defaults to -1.

    thread_number : int, optional
        Specifies the number of threads that can be used by this function.

        Defaults to 1.

    Returns
    -------
    DataFrames

      DataFrame, detected outliers, structured as follows:

        - 1st column : ID of detected outliers in ``data``.
        - other columns : feature values for detected outliers

      DataFrame, statistics of detected outliers, structured as follows:

        - 1st column : ID of detected outliers in ``data``.
        - 2nd column : ID of the corresponding cluster centers.
        - 3rd column : Outlier score, which is the distance value.

      DataFrame, centers of clusters produced by k-means algorithm,
      structured as follows:

        - 1st column : ID of cluster center.
        - other columns : Coordinate(i.e. feature) values of cluster center.

    Examples
    --------
    Input data for outlier detection:

    >>> df.collect()
        ID  V000  V001
    0    0   0.5   0.5
    1    1   1.5   0.5
    2    2   1.5   1.5
    3    3   0.5   1.5
    4    4   1.1   1.2
    5    5   0.5  15.5
    6    6   1.5  15.5
    7    7   1.5  16.5
    8    8   0.5  16.5
    9    9   1.2  16.1
    10  10  15.5  15.5
    11  11  16.5  15.5
    12  12  16.5  16.5
    13  13  15.5  16.5
    14  14  15.6  16.2
    15  15  15.5   0.5
    16  16  16.5   0.5
    17  17  16.5   1.5
    18  18  15.5   1.5
    19  19  15.7   1.6
    20  20  -1.0  -1.0

    Invoke the function and obtain the results:

    >>> outliers, stats, centers = outlier_detection_kmeans(data=df, key='ID',
    ...                                                     distance_level='euclidean',
    ...                                                     contamination=0.15,
    ...                                                     sum_distance=True,
    ...                                                     distance_threshold=3)
    >>> outliers.collect()
       ID  V000  V001
    0  20  -1.0  -1.0
    1  16  16.5   0.5
    2  12  16.5  16.5
    >>> stats.collect()
       ID  CLUSTER_ID      SCORE
    0  20           2  60.619864
    1  16           1  54.110424
    2  12           3  53.954274
    """
    kmodt = KMeansOutlier(n_clusters, distance_level,
                          contamination, sum_distance,
                          init, max_iter, normalization,
                          tol, distance_threshold)
    return kmodt.fit_predict(data=data, key=key, features=features,
                             thread_number=thread_number)

class SpectralClustering(PALBase):
    r"""
    Spectral clustering is an algorithm evolved from graph theory,
    and has been widely used in clustering. Its main idea is to treat
    all data as points in space, which can be connected by edges.
    The edge weight between two points farther away is low, while the edge
    weight between two points closer is high. Cutting the graph composed
    of all data points to make the edge weight sum between different subgraphs
    after cutting as low as possible, while make the edge weight sum within the
    subgraph as high as possible to achieve the purpose of clustering.

    It performs a low-dimension embedding of the affinity matrix between samples,
    followed by k-means clustering of the components of the eigenvectors
    in the low dimensional space.

    Parameters
    ----------
    n_clusters : int
        The number of clusters for spectral clustering.

        The valid range for this parameter is from 2 to the number of records
        in the input data.

    n_components : int, optional
        The number of eigenvectors used for spectral embedding.

        Defaults to the value of ``n_clusters``.

    gamma : float, optional
        The RBF kernel coefficient :math:`\gamma` used in constructing affinity matrix with
        distance metric `d`, illustrated as :math:`\exp(-\gamma * d^2)`.

        Defaults to 1.0.

    affinity : str, optional
        Specifies the type of graph used to construct
        the affinity matrix. Valid options include:

        - 'knn' : binary affinity matrix constructed from the
          graph of k-nearest-neighbors(knn).
        - 'mutual-knn' : binary affinity matrix constructed from
          the graph of mutual k-nearest-neighbors(mutual-knn).
        - 'fully-connected' : affinity matrix constructed from
          fully-connected graph, with weights defined by RBF
          kernel coefficients.

        Defaults to 'fully-connected'.

    n_neighbors : int, optional
        The number neighbors to use when constructing the affinity matrix using
        nearest neighbors method.

        Valid only when ``graph`` is 'knn' or 'mutual-knn'.

        Defaults to 10.

    cut : str, optional
        Specifies the method to cut the graph.

        - 'ratio-cut' : Ratio-Cut.
        - 'n-cut' : Normalized-Cut.

        Defaults to 'ratio-cut'.

    eigen_tol : float, optional
        The stopping criterion for eigendecomposition of the Laplacian matrix.

        Defaults to 1e-10.

    krylov_dim : int, optional
        Specifies the dimension of Krylov subspaces used in Eigenvalue decomposition.
        In general, this parameter controls the convergence speed of the algorithm.
        Typically a larger ``krylov_dim`` means faster convergence,
        but it may also result in greater memory use and more matrix operations
        in each iteration.

        Defaults to 2*``n_components``.

        .. Note::
            This parameter must satisfy

            ``n_components`` < ``krylov_dim`` :math:`\leq`
            `the number of training records`.

    distance_level : str, optional
        Specifies the method for computing the distance between data records
        and cluster centers:

        - 'manhattan' : Manhattan distance.
        - 'euclidean' : Euclidean distance.
        - 'minkowski' : Minkowski distance.
        - 'chebyshev' : Chebyshev distance.
        - 'cosine' : Cosine distance.

        Defaults to 'euclidean'.

    minkowski_power : float, optional
        Specifies the power parameter in Minkowski distance.

        Valid only when ``distance_level`` is 'minkowski'.

        Defaults to 3.0.

    category_weights : float, optional
        Represents the weight of category attributes.

        Defaults to 0.707.

    max_iter : int, optional
        Maximum number of iterations for K-Means algorithm.

        Defaults to 100.

    init : {'first_k', 'replace', 'no_replace', 'patent'}, optional
        Controls how the initial centers are selected in K-Means algorithm:

        - 'first_k': First k observations.
        - 'replace': Random with replacement.
        - 'no_replace': Random without replacement.
        - 'patent': Patent of selecting the init center (US 6,882,998 B1).

        Defaults to 'patent'.

    tol : float, optional
        Specifies the exit threshold for K-Means iterations.

        Defaults to 1e-6.

    onehot_min_frequency :  int, optional
        Specifies the minimum frequency below which a category will be considered infrequent.

        Defaults to 1.

    onehot_max_categories : int, optional
        Specifies an upper limit to the number of output features for each input feature. It includes the feature that combines infrequent categories.

        Defaults to 0.

    Attributes
    ----------
    labels_ : DataFrame
        DataFrame that holds the cluster labels.

    stats_ : DataFrame
        Statistics.

    Examples
    --------
    >>> spc = SpectralClustering(n_clusters=4, n_neighbors=4,
                                 init='patent', distance_level='euclidean',
                                 max_iter=100, tol=1e-6, category_weights=0.5)
    >>> labels = spc.fit_predict(data=df, thread_ratio=0.2)

    """
    affinity_map = {'knn':0, 'mutual-knn':1, 'fully-connected':2}
    cut_map = {'ratio-cut':0, 'n-cut':1}
    distance_map = {"manhattan": 1, "euclidean": 2, "minkowski": 3, "chebyshev": 4, "cosine":6}
    init_map = {'first_k':1, 'replace':2, 'no_replace':3, 'patent':4}

    def __init__(self,
                 n_clusters,
                 n_components=None,
                 gamma=None,
                 affinity=None,
                 n_neighbors=None,
                 cut=None,
                 eigen_tol=None,
                 krylov_dim=None,
                 distance_level=None,
                 minkowski_power=None,
                 category_weights=None,
                 max_iter=None,
                 init=None,
                 tol=None,
                 onehot_min_frequency=None,
                 onehot_max_categories=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(SpectralClustering, self).__init__()
        self.n_clusters = self._arg('n_clusters', n_clusters, int, required=True)
        self.n_components = self._arg('n_components', n_components, int)
        self.gamma = self._arg('gamma', gamma, (float, int))
        self.affinity = self._arg('affinity', affinity, self.affinity_map)
        self.n_neighbors = self._arg('n_neighbors', n_neighbors, int)
        self.cut = self._arg('cut', cut, self.cut_map)
        self.eigen_tol = self._arg('eigen_tol', eigen_tol, float)
        self.krylov_dim = self._arg('krylov_dim', krylov_dim, int)
        self.distance_level = self._arg('distance_level', distance_level, self.distance_map)
        self.minkowski_power = self._arg('minkowski_power', minkowski_power, (float, int))
        self.category_weights = self._arg('category_weights', category_weights, (float, int))
        self.max_iter = self._arg('max_iter', max_iter, int)
        self.init = self._arg('init', init, self.init_map)
        self.tol = self._arg('tol', tol, float)
        self.onehot_min_frequency = self._arg('onehot_min_frequency', onehot_min_frequency, int)
        self.onehot_max_categories = self._arg('onehot_max_categories', onehot_max_categories, int)
        self.labels_ = None
        self.stats_ = None
        self.statistics_ = self.stats_

    def fit(self, data, key=None, features=None, thread_ratio=None):
        r"""
        Fit the model to the training dataset.

        Parameters
        ----------
        data : DataFrame
            DataFrame containing the input data.

        key : str, optional
            Name of ID column.

            Mandatory if ``data`` is not indexed, or indexed by multiple columns.

            Defaults to the index column of ``data`` if there is one.

        features : a list of str, optional
            Names of the feature columns.

            If ``features`` is not provided, it defaults to all non-key columns of ``data``.

        thread_ratio : float, optional
            Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
            Values outside the range will be ignored and this function heuristically determines the number of threads to use.

            Defaults to 0.
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, 'training_data', data)
        conn = data.connection_context
        require_pal_usable(conn)
        thread_ratio = self._arg('thread_ratio', thread_ratio, (float, int))
        cols = data.columns
        index = data.index
        key = self._arg('key', key, str, not isinstance(index, str))
        if isinstance(index, str):
            if key is not None and index != key:
                warn_msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(warn_msg)
        key = index if key is None else key
        cols.remove(key)
        features = self._arg('features', features, ListOfStrings)
        if features is None:
            features = cols
        data_ = data[[key] + features]

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        outputs = ['#PAL_SPECTRAL_CLUSTERING_{}_TBL_{}_{}'.format(name, self.id, unique_id)
                   for name in ['RESULT', 'STAT', 'PH']]
        result_tbl, stats_tbl, ph_tbl = outputs
        param_rows = [('GROUP_NUMBER', self.n_clusters, None, None),
                      ('N_COMPONENTS', self.n_components, None, None),
                      ('GAMMA', None, self.gamma, None),
                      ('AFFINITY', self.affinity, None, None),
                      ('N_NEIGHBOURS', self.n_neighbors, None, None),
                      ('CUT_METHOD', self.cut, None, None),
                      ('EIGEN_TOL', None, self.eigen_tol, None),
                      ('KRYLOY_DIMENSION', self.krylov_dim, None, None),
                      ('KRYLOV_DIMENSION', self.krylov_dim, None, None),
                      ('DISTANCE_LEVEL', self.distance_level, None, None),
                      ('MINKOWSKI_POWER', None, self.minkowski_power, None),
                      ('CATEGORY_WEIGHTS', None, self.category_weights, None),
                      ('MAX_ITERATION', self.max_iter, None, None),
                      ('INIT_TYPE', self.init, None, None),
                      ('EXIT_THRESHOLD', None, self.tol, None),
                      ('THREAD_RATIO', None, thread_ratio, None),
                      ('ONEHOT_MIN_FREQUENCY', self.onehot_min_frequency, None, None),
                      ('ONEHOT_MAX_CATEGORIES', self.onehot_max_categories, None, None)]
        try:
            self._call_pal_auto(conn,
                                'PAL_SPECTRAL_CLUSTERING',
                                data_,
                                ParameterTable().with_data(param_rows),
                                *outputs)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, outputs)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, outputs)
            raise
        self.stats_ = conn.table(stats_tbl)
        self.statistics_ = self.stats_
        self.labels_ = conn.table(result_tbl)

    def fit_predict(self, data, key=None, features=None, thread_ratio=None):
        r"""
        Given data, perform spectral clustering and return the corresponding cluster labels.

        Parameters
        ----------
        data : DataFrame
            DataFrame containing the input data.

        key : str, optional
            Name of ID column in ``data``.

            Mandatory if ``data`` is not indexed, or indexed by multiple columns.

            Defaults to the index column of ``data`` if there is one.

        features : a list of str, optional
            Names of the feature columns.

            If ``features`` is not provided, it defaults to all non-key columns of ``data``.

        thread_ratio : float, optional
            Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
            Values outside the range will be ignored and this function heuristically determines the number of threads to use.

            Defaults to 0.

        Returns
        -------
        DataFrame
            The cluster labels of all records in ``data``.
        """
        self.fit(data, key, features, thread_ratio)
        return self.labels_

    def predict(self, data=None, key=None, features=None, thread_ratio=None):
        r"""
        Given data, perform spectral clustering and return the corresponding cluster labels.

        Parameters
        ----------
        data : DataFrame
            DataFrame containing the input data.

            Defaults to None.

        key : str, optional
            Name of ID column in ``data``.

            Mandatory if ``data`` is not indexed, or indexed by multiple columns.

            Defaults to the index column of ``data`` if there is one.

        features : a list of str, optional
            Names of the feature columns.

            If ``features`` is not provided, it defaults to all non-key columns of ``data``.

        thread_ratio : float, optional
            Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
            Values outside the range will be ignored and this function heuristically determines the number of threads to use.

            Defaults to 0.

        Returns
        -------
        DataFrame
            The cluster labels of all records in ``data``.
        """
        if hasattr(self, 'labels_'):
            if data is not None:
                self.fit(data, key, features, thread_ratio)
        else:
            raise FitIncompleteError("Model has not been initialized. Perform a fit first.")
        return self.labels_

class ConstrainedClustering(PALBase):
    r"""
    Constraints are additional information that guide the clustering process
    to produce results more in line with specific requirements or prior knowledge.

    Pairwise Constraints: Must-Link constraints specify that two data points should be in the same cluster.
    Cannot-Link constraints indicate that two data points should not be in the same cluster.

    Triplet Constraints: Given an anchor instance a,
    positive instance p and negative instance n the constraint indicates that instance a is more similar to p than to n.

    Parameters
    ----------
    n_clusters : int
        The number of clusters for constrained clustering.

        The valid range for this parameter is from 2 to the number of records in the input data.

    encoder_hidden_dims : str, optional
        Specifies the hidden layer sizes of encoder.

        Defaults to '8, 16'.

    embedding_dim : int, optional
        Specifies the dimension of latent space.

        Defaults to 3.

    normalization : int, optional
        Specifies whether to use normalization.

        Defaults to 1.

    seed : int, optional
        Specifies the seed for random number generator. Use system time when 0 is specified.

        Defaults to 0.

    pretrain_learning_rate : float, optional
        Specifies the learning rate of pretraining stage.

        Defaults to 0.01.

    pretrain_epochs : int, optional
        Specifies the number of pretraining epochs.

        Defaults to 10.

    pretrain_batch_size : int, optional
        Specifies the number of training samples in a batch.

        Defaults to 16.

    thread_ratio : float, optional
        Specifies the ratio of total number of threads that can be used by this function.
        The value range is from 0 to 1, where 0 means only using 1 thread, and 1 means using at most all the currently available threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 1.0.

    gamma : float, optional
        Specifies the degree of distorting latent space.

        Defaults to 0.1.

    ml_penalty : float, optional
        Specifies the penalty for must-link constraints.

        Only valid when constraint_type is 'pairwise'.

        Defaults to 0.1.

    cl_penalty : float, optional
        Specifies the penalty for cannot-link constraints.

        Only valid when constraint_type is 'pairwise'.

        Defaults to 1.0.

    theta : float, optional
        Specifies the margin in triplet loss.

        Only valid when constraint_type is 'triplet'.

        Defaults to 0.1.

    learning_rate : float, optional
        Specifies the learning rate.

        Defaults to 0.01.

    max_epochs :  int, optional
        Specifies the maximum number of training epochs.

        Defaults to 5.

    batch_size :  int, optional
        Specifies the number of training samples in a batch.

        Defaults to 16.

    update_interval :  int, optional
        Specifies the frequency of updating target distribution.

        Defaults to 1.

    ml_batch_size :  int, optional
        Specifies the number of must-link constraints in a batch.

        Only valid when constraint_type is 'pairwise'.

        Defaults to 16.

    cl_batch_size :  int, optional
        Specifies the number of cannot-link constraints in a batch.

        Only valid when constraint_type is 'pairwise'.

        Defaults to 16.

    triplet_batch_size : int, optional
        Specifies the number of triplet constraints in a batch.

        Only valid when constraint_type is 'triplet'.

        Defaults to 16.

    ml_update_interval : int, optional
        Specifies the frequency of training with must-link constraints.

        Only valid when constraint_type is 'pairwise'.

        Defaults to 1.

    cl_update_interval : int, optional
        Specifies the frequency of training with cannot-link constraints.

        Only valid when constraint_type is 'pairwise'.

        Defaults to 1.

    triplet_update_interval : int, optional
        Specifies the frequency of training with triplet constraints.

        Only valid when constraint_type is 'triplet'.

        Defaults to 1.

    tolerance : float, optional
        Specifies the stopping threshold.

        Defaults to 0.001.

    verbose : int, optional
        Specifies the verbosity of log.

        Defaults to 0.

    Attributes
    ----------
    labels_ : DataFrame
        DataFrame that holds the cluster labels.

    model_ : DataFrame
        Model.

    training_log_ : DataFrame
        Training log.

    statistics_ : DataFrame
        Statistics.

    Examples
    --------
    >>> from hana_ml.algorithms.pal.clustering import ConstrainedClustering
    >>> constrained_clustering = ConstrainedClustering(n_clusters=3,
                                                       encoder_hidden_dims='4',
                                                       embedding_dim=3,
                                                       seed=1,
                                                       pretrain_learning_rate=0.01,
                                                       pretrain_epochs=350,
                                                       learning_rate=0.01,
                                                       max_epochs=200,
                                                       update_interval=1)
    >>> import numpy as np
    >>> import pandas as pd
    >>> from hana_ml.dataframe import create_dataframe_from_pandas
    >>> constraints_data_structure = {'TYPE': 'INTEGER', 'ID1': 'INTEGER', 'ID2': 'INTEGER'}
    >>> constraints_data = np.array([
            [1, 1, 30],
            [-1, 1, 130],
            [-1, 80, 130]
        ])
    >>> constraints_df = create_dataframe_from_pandas(conn,
                                                      pd.DataFrame(constraints_data, columns=list(constraints_data_structure.keys())),
                                                      'CONSTRAINTS_TBL',
                                                      force=True,
                                                      table_structure=constraints_data_structure)
    >>> labels = constrained_clustering.fit_predict(data=iris_df,
                                                    constraint_type='pairwise',
                                                    constraints=constraints_df,
                                                    key='ID',
                                                    features=['SEPALLENGTHCM', 'SEPALWIDTHCM', 'PETALLENGTHCM', 'PETALWIDTHCM'])
    """
    def __init__(self,
                 n_clusters: int,
                 encoder_hidden_dims: str=None,
                 embedding_dim: int=None,
                 normalization: int=None,
                 seed: int=None,
                 pretrain_learning_rate: float=None,
                 pretrain_epochs: int=None,
                 pretrain_batch_size: int=None,
                 thread_ratio: float=None,
                 gamma: float=None,
                 ml_penalty: float=None,
                 cl_penalty: float=None,
                 theta: float=None,
                 learning_rate: float=None,
                 max_epochs: int=None,
                 batch_size: int=None,
                 update_interval: int=None,
                 ml_batch_size: int=None,
                 cl_batch_size: int=None,
                 triplet_batch_size: int=None,
                 ml_update_interval: int=None,
                 cl_update_interval: int=None,
                 triplet_update_interval: int=None,
                 tolerance: float=None,
                 verbose: int=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(ConstrainedClustering, self).__init__()

        self.n_clusters = self._arg('n_clusters', n_clusters, int, required=True)
        self.encoder_hidden_dims = self._arg('encoder_hidden_dims', encoder_hidden_dims, str)
        self.embedding_dim = self._arg('embedding_dim', embedding_dim, int)
        self.normalization = self._arg('normalization', normalization, int)
        self.seed = self._arg('seed', seed, int)
        self.pretrain_learning_rate = self._arg('pretrain_learning_rate', pretrain_learning_rate, float)
        self.pretrain_epochs = self._arg('pretrain_epochs', pretrain_epochs, int)
        self.pretrain_batch_size = self._arg('pretrain_batch_size', pretrain_batch_size, int)
        self.thread_ratio = self._arg('thread_ratio', thread_ratio, (float, int))
        self.gamma = self._arg('gamma', gamma, float)
        self.ml_penalty = self._arg('ml_penalty', ml_penalty, float)
        self.cl_penalty = self._arg('cl_penalty', cl_penalty, float)
        self.theta = self._arg('theta', theta, float)
        self.learning_rate = self._arg('learning_rate', learning_rate, float)
        self.max_epochs = self._arg('max_epochs', max_epochs, int)
        self.batch_size = self._arg('batch_size', batch_size, int)
        self.update_interval = self._arg('update_interval', update_interval, int)
        self.ml_batch_size = self._arg('ml_batch_size', ml_batch_size, int)
        self.cl_batch_size = self._arg('cl_batch_size', cl_batch_size, int)
        self.triplet_batch_size = self._arg('triplet_batch_size', triplet_batch_size, int)
        self.ml_update_interval = self._arg('ml_update_interval', ml_update_interval, int)
        self.cl_update_interval = self._arg('cl_update_interval', cl_update_interval, int)
        self.triplet_update_interval = self._arg('triplet_update_interval', triplet_update_interval, int)
        self.tolerance = self._arg('tolerance', tolerance, float)
        self.verbose = self._arg('verbose', verbose, int)

        self.labels_ = None
        self.model_ = None
        self.training_log_ = None
        self.statistics_ = None

    def fit(self, data: DataFrame, constraint_type: str, constraints: DataFrame, key: str=None, features: List[str]=None, pre_model: DataFrame=None):
        r"""
        Fit the model to the training dataset.

        Parameters
        ----------
        data : DataFrame
            DataFrame containing the input data, expected to be structured as follows:

            - 1st column : Record ID.
            - other columns : Attribute data.

        constraint_type : {'pairwise', 'triplet'}
            Specifies the type of constraints:

            - 'pairwise' : Pairwise Constraints.
            - 'triplet' : Triplet Constraints.

        constraints : DataFrame
            Constraints data for pairwise constraints, expected to be structured as follows:

                - 1st column : Pairwise constraint type. Only the values 1 and -1 are considered valid, with 1 representing a must-link and -1 indicating a cannot-link.
                - 2nd column : Instance 1 ID.
                - 3rd column : Instance 2 ID.

            Constraints data for triplet constraints, expected to be structured as follows:

                - 1st column : Anchor instance ID.
                - 2nd column : Positive instance ID.
                - 3rd column : Negative instance ID.

        key : str, optional
            Name of ID column.

            Mandatory if ``data`` is not indexed, or indexed by multiple columns.

            Defaults to the index column of ``data`` if there is one.

        features : a list of str, optional
            Names of the feature columns.

            If ``features`` is not provided, it defaults to all non-key columns of ``data``.

        pre_model : DataFrame, optional
            DataFrame containing the pre-model data, expected to be structured as follows:

                - 1st column : Indicates the ID of the row.
                - 2nd column : Model content.

            Defaults to None.
        """
        # 0.
        data_ = self._arg('data', data, DataFrame, required=True)
        index = data.index
        key = self._arg('key', key, str, not isinstance(index, str))
        if isinstance(index, str):
            if key is not None and index != key:
                warn_msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(warn_msg)
        key = index if key is None else key
        features = self._arg('features', features, ListOfStrings)
        if features is None:
            features = data.columns
            features.remove(key)
        data_ = data[[key] + features]
        int_constraint_type = self._arg('constraint_type', constraint_type, {'pairwise':0, 'triplet':2}, required=True)
        constraints = self._arg('constraints', constraints, DataFrame, required=True)
        pre_model = self._arg('pre_model', pre_model, DataFrame)

        self.constraint_type = constraint_type
        self.constraints = constraints
        self.pre_model = pre_model

        # 1.
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, 'training_data', data)
        procedure_name = 'PAL_CONSTRAINED_CLUSTERING'
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        conn = data.connection_context
        require_pal_usable(conn)

        # 2.
        param_rows = [('GROUP_NUMBER', self.n_clusters, None, None),
                      ('ENCODER_HIDDEN_DIMS', None, None, self.encoder_hidden_dims),
                      ('EMBEDDING_DIM', self.embedding_dim, None, None),
                      ('NORMALIZATION', self.normalization, None, None),
                      ('SEED', self.seed, None, None),
                      ('PRETRAIN_LEARNING_RATE', None, self.pretrain_learning_rate, None),
                      ('PRETRAIN_EPOCHS', self.pretrain_epochs, None, None),
                      ('PRETRAIN_BATCH_SIZE', self.pretrain_batch_size, None, None),
                      ('CONSTRAINT_TYPE', int_constraint_type, None, None),
                      ('THREAD_RATIO', None, self.thread_ratio, None),
                      ('GAMMA', None, self.gamma, None),
                      ('ML_PENALTY', None, self.ml_penalty, None),
                      ('CL_PENALTY', None, self.cl_penalty, None),
                      ('THETA', None, self.theta, None),
                      ('LEARNING_RATE', None, self.learning_rate, None),
                      ('MAX_EPOCHS', self.max_epochs, None, None),
                      ('BATCH_SIZE', self.batch_size, None, None),
                      ('UPDATE_INTERVAL', self.update_interval, None, None),
                      ('ML_BATCH_SIZE', self.ml_batch_size, None, None),
                      ('CL_BATCH_SIZE', self.cl_batch_size, None, None),
                      ('TRIPLET_BATCH_SIZE', self.triplet_batch_size, None, None),
                      ('ML_UPDATE_INTERVAL', self.ml_update_interval, None, None),
                      ('CL_UPDATE_INTERVAL', self.cl_update_interval, None, None),
                      ('TRIPLET_UPDATE_INTERVAL', self.triplet_update_interval, None, None),
                      ('TOLERANCE', None, self.tolerance, None),
                      ('VERBOSE', self.verbose, None, None)]

        # 3.
        default_pre_model_tbl = None
        if pre_model is None:
            pre_model_tbl = '#{}_{}_TBL_{}_{}'.format(procedure_name, 'PRE_MODEL', self.id, unique_id)
            try_drop(conn, pre_model_tbl)
            table_structure = {"ROW_INDEX" : "INTEGER", "MODEL_CONTENT" : "NVARCHAR(5000)"}
            conn.create_table(pre_model_tbl, table_structure)
            pre_model = conn.table(pre_model_tbl)
            default_pre_model_tbl = pre_model_tbl

        # 4.
        output_tbls = ['#{}_{}_TBL_{}_{}'.format(procedure_name, name, self.id, unique_id)
                   for name in ['RESULT', 'MODEL', 'TRAINING_LOG', 'STAT']]

        # 5.
        try:
            self._call_pal_auto(conn, procedure_name, data_, constraints, ParameterTable().with_data(param_rows), pre_model,
                                *output_tbls)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, output_tbls)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, output_tbls)
            raise
        if default_pre_model_tbl:
            try_drop(conn, default_pre_model_tbl)

        # 6.
        result_tbl, model_tbl, training_log_tbl, stats_tbl = output_tbls
        self.labels_ = conn.table(result_tbl)
        self.model_ = conn.table(model_tbl)
        self.training_log_ = conn.table(training_log_tbl)
        self.statistics_ = conn.table(stats_tbl)

    def fit_predict(self, data: DataFrame, constraint_type: str, constraints: DataFrame, key: str=None, features: List[str]=None, pre_model: DataFrame=None):
        r"""
        Given data, perform constrained clustering and return the corresponding cluster labels.

        Parameters
        ----------
        data : DataFrame
            DataFrame containing the input data, expected to be structured as follows:

            - 1st column : Record ID.
            - other columns : Attribute data.

        constraint_type : {'pairwise', 'triplet'}
            Specifies the type of constraints:

            - 'pairwise' : Pairwise Constraints.
            - 'triplet' : Triplet Constraints.

        constraints : DataFrame
            Constraints data for pairwise constraints, expected to be structured as follows:

                - 1st column : Pairwise constraint type. Only the values 1 and -1 are considered valid, with 1 representing a must-link and -1 indicating a cannot-link.
                - 2nd column : Instance 1 ID.
                - 3rd column : Instance 2 ID.

            Constraints data for triplet constraints, expected to be structured as follows:

                - 1st column : Anchor instance ID.
                - 2nd column : Positive instance ID.
                - 3rd column : Negative instance ID.

        key : str, optional
            Name of ID column.

            Mandatory if ``data`` is not indexed, or indexed by multiple columns.

            Defaults to the index column of ``data`` if there is one.

        features : a list of str, optional
            Names of the feature columns.

            If ``features`` is not provided, it defaults to all non-key columns of ``data``.

        pre_model : DataFrame, optional
            DataFrame containing the pre-model data, expected to be structured as follows:

                - 1st column : Indicates the ID of the row.
                - 2nd column : Model content.

            Defaults to None.

        Returns
        -------
        DataFrame
            The cluster labels of all records in ``data``.
        """
        self.fit(data, constraint_type, constraints, key, features, pre_model)
        return self.labels_

    def predict(self, data: DataFrame=None, key: str=None, features: List[str]=None):
        r"""
        Given data, perform constrained clustering and return the corresponding cluster labels.

        Parameters
        ----------
        data : DataFrame
            DataFrame containing the input data, expected to be structured as follows:

            - 1st column : Record ID.
            - other columns : Attribute data.

            Defaults to None.

        key : str, optional
            Name of ID column.

            Mandatory if ``data`` is not indexed, or indexed by multiple columns.

            Defaults to the index column of ``data`` if there is one.

        features : a list of str, optional
            Names of the feature columns.

            If ``features`` is not provided, it defaults to all non-key columns of ``data``.

        Returns
        -------
        DataFrame
            The cluster labels of all records in ``data``.
        """
        if hasattr(self, 'constraint_type') and hasattr(self, 'constraints'):
            if data is not None:
                self.fit(data, self.constraint_type, self.constraints, key, features, self.pre_model)
        else:
            raise FitIncompleteError("Model has not been initialized. Perform a fit first.")
        return self.labels_

class KMeansOutlier(PALBase):
    r"""
    Outlier detection based on k-means clustering. It uses the K-means algorithm to find the farthest point from the centroid as an outlier.

    Parameters
    ----------
    n_clusters : int, optional
        Number of clusters to be grouped.

        If this number is not specified, the G-means method will be used to determine the
        number of clusters.
    distance_level : {'manhattan', 'euclidean', 'minkowski'}, optional
        Specifies the distance type between data points and cluster center.

           - 'manhattan' : Manhattan distance
           - 'euclidean' : Euclidean distance
           - 'minkowski' : Minkowski distance

        Defaults to 'euclidean'.

    contamination : float, optional
        Specifies the proportion of outliers within the input data to be detected.

        Expected to be a positive number no greater than 1.

        Defaults to 0.1.

    sum_distance : bool, optional
        Specifies whether or not to use the sum distance of a point to all cluster
        centers as its distance value for outlier score. If False, only the distance of a point
        to the center it belongs to is used its distance value calculation.

        Defaults to True.

    init : {'first_k', 'replace', 'no_replace', 'patent'}, optional
        Controls how the initial centers are selected:

          - 'first_k': First k observations.
          - 'replace': Random with replacement.
          - 'no_replace': Random without replacement.
          - 'patent': Patent of selecting the init center (US 6,882,998 B1).

        Defaults to 'patent'.

    max_iter : int, optional
        Maximum number of iterations for k-means clustering.

        Defaults to 100.

    normalization : {'no', 'l1_norm', 'min_max'}, optional
        Normalization type.

          - 'no': No normalization will be applied.
          - 'l1_norm': Yes, for each point X (x\ :sub:`1`\, x\ :sub:`2`\, ..., x\ :sub:`n`\), the normalized
            value will be X'(x\ :sub:`1` /S,x\ :sub:`2` /S,...,x\ :sub:`n` /S),
            where S = \|x\ :sub:`1`\ \|+\|x\ :sub:`2`\ \|+...\|x\ :sub:`n`\ \|.
          - 'min_max': Yes, for each column C, get the min and max value of C,
            and then C[i] = (C[i]-min)/(max-min).

        Defaults to 'no'.

    tol : float, optional
        Convergence threshold for exiting iterations.

        Defaults to 1.0e-9.

    distance_threshold : float, optional
        Specifies the threshold distance value for outlier detection.

        A point with distance value no greater than the threshold is not considered to be outlier.

        Defaults to -1.

    Examples
    --------
    Input data df:

    >>> df.collect()
        ID  V000  V001
    0    0   0.5   0.5
    1    1   1.5   0.5
    ...
    19  19  15.7   1.6
    20  20  -1.0  -1.0

    Initialize a KMeansOutlier instance

    >>> kmsodt = KMeansOutlier(distance_level='euclidean',
    ...                        contamination=0.15,
    ...                        sum_distance=True,
    ...                        distance_threshold=3)
    >>> outliers, stats, centers = kmsodt.fit_predict(data=df, key='ID')
    >>> outliers.collect()
       ID  V000  V001
    0  20  -1.0  -1.0
    1  16  16.5   0.5
    2  12  16.5  16.5
    >>> stats.collect()
       ID  CLUSTER_ID      SCORE
    0  20           2  60.619864
    1  16           1  54.110424
    2  12           3  53.954274
    """
    def __init__(self,
                 n_clusters=None,
                 distance_level=None,
                 contamination=None,
                 sum_distance=True,
                 init=None,
                 max_iter=None,
                 normalization=None,
                 tol=None,
                 distance_threshold=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(KMeansOutlier, self).__init__()
        self.n_clusters = self._arg('n_clusters', n_clusters, int)
        self.distance_level = self._arg('distance_level', distance_level,
                                        {"manhattan": 1, "euclidean": 2, "minkowski": 3})
        self.contamination = self._arg('contamination', contamination, float)
        self.sum_distance = self._arg('sum_distance', sum_distance, bool)
        self.init = self._arg('init', init, {'first_k':1, 'replace':2,
                                             'no_replace':3, "patent":4})
        self.max_iter = self._arg('max_iter', max_iter, int)
        self.normalization = self._arg('normalization', normalization,
                                       {'no':0, 'l1_norm':1, 'min_max':2})
        self.tol = self._arg('tol', tol, float)
        self.distance_threshold = self._arg('distance_threshold', distance_threshold, float)

    @trace_sql
    def fit_predict(self,
                    data,
                    key=None,
                    features=None,
                    thread_number=None):
        r"""
        Performing k-means clustering on an input dataset and extracting the corresponding outliers.

        Parameters
        ----------
        data : DataFrame
            Input data for outlier detection.
        key : str, optional
            Name of ID column.

            Defaults to the index column of data (i.e. data.index) if it is set.
            If the index column of data is not provided, please enter the value of key.
        features : str or a list of str
            Names of the features columns in ``data`` that are used for
            calculating distances of points in ``data`` for clustering.

            Feature columns must be numerical.

            Defaults to all non-key columns if not provided.
        thread_number : int, optional
            Specifies the number of threads that can be used by this function.

            Defaults to 1.

        Returns
        -------
        DataFrame

          DataFrame 1, detected outliers, structured as follows:

             - 1st column : ID of detected outliers in ``data``.
             - other columns : feature values for detected outliers

          DataFrame 2, statistics of detected outliers, structured as follows:

             - 1st column : ID of detected outliers in ``data``.
             - 2nd column : ID of the corresponding cluster centers.
             - 3rd column : Outlier score, which is the distance value.

          DataFrame 3, centers of clusters produced by k-means algorithm,
          structured as follows:

             - 1st column : ID of cluster center.
             - other columns : Coordinate(i.e. feature) values of cluster center.
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        thread_number = self._arg('thread_number', thread_number, int)
        conn = data.connection_context
        cols = data.columns
        index = data.index
        key = arg('key', key, str, not isinstance(index, str))
        if isinstance(index, str):
            if key is not None and index != key:
                warn_msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(warn_msg)
        key = index if key is None else key
        cols.remove(key)
        features = arg('features', features, ListOfStrings)
        if features is None:
            features = cols
        data_ = data[[key] + features]
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        tables = ['#ANOMALY_DETECTION_KMEANS_{}_{}_{}'.format(name, self.id, unique_id)
                  for name in ['OUTLIERS', 'STATS', 'CENTERS']]
        outliers_tbl, stats_tbl, centers_tbl = tables
        param_array = [('GROUP_NUMBER', self.n_clusters, None, None),
                       ('DISTANCE_LEVEL', self.distance_level, None, None),
                       ('OUTLIER_PERCENTAGE', None, self.contamination, None),
                       ('OUTLIER_DEFINE', int(self.sum_distance) + 1, None, None),
                       ('MAX_ITERATION', self.max_iter, None, None),
                       ('INIT_TYPE', self.init, None, None),
                       ('DISTANCE_LEVEL', self.distance_level, None, None),
                       ('NORMALIZATION', self.normalization, None, None),
                       ('EXIT_THRESHOLD', None, self.tol, None),
                       ('DISTANCE_THRESHOLD', None, self.distance_threshold, None),
                       ('THREAD_NUMBER', thread_number, None, None)]
        try:
            self._call_pal_auto(conn,
                                'PAL_ANOMALY_DETECTION',
                                data_,
                                ParameterTable().with_data(param_array),
                                *tables)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, tables)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, tables)
            raise
        return conn.table(outliers_tbl), conn.table(stats_tbl), conn.table(centers_tbl)
