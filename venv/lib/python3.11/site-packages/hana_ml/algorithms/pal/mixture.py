"""
This module contains Python wrappers for Gaussian mixture model algorithm.

The following class is available:

    * :class:`GaussianMixture`
"""
#pylint: disable=invalid-name, too-many-instance-attributes, too-few-public-methods, too-many-arguments
#pylint: disable=too-many-locals,unused-variable, line-too-long, relative-beyond-top-level
#pylint: disable=consider-using-f-string
import logging
import uuid
from hdbcli import dbapi
from hana_ml.ml_base import try_drop
from .sqlgen import trace_sql
from .clustering import _ClusterAssignmentMixin
from .pal_base import (
    ParameterTable,
    ListOfStrings,
    pal_param_register,
    require_pal_usable)
logger = logging.getLogger(__name__)

class GaussianMixture(_ClusterAssignmentMixin):
    """
    Gaussian Mixture Model (GMM) is a probabilistic model used for modeling data points that are assumed to be generated from a mixture of Gaussian distributions. It is a parametric model that represents the probability distribution of the data as a weighted sum of multiple Gaussian distributions, also known as components or clusters.

    Parameters
    ----------

    init_param : {'farthest_first_traversal', 'manual', 'random_means', 'kmeans++'}
        Specifies the initialization mode.

        - 'farthest_first_traversal': The farthest-first traversal algorithm provides the initial cluster centers.
        - 'manual': User-provided values (`init_centers`) serve as initial centers.
        - 'random_means': Initial centers become the weighted means of randomly chosen data points.
        - 'kmeans++': Initial centers are determined by the k-means++ method.

    n_components : int, optional
        Specifies the number of Gaussian distributions.

        This parameter becomes mandatory when ``init_param`` is not 'manual'.

    init_centers : list of int/str
        List of row identifiers in ``data`` that are to be used as initial centers.

        This parameter becomes mandatory when ``init_param`` is 'manual'.

    covariance_type : {'full', 'diag', 'tied_diag'}, optional
        Specifies the type of covariance matrices to be utilized in the model.

        - 'full': Utilizes full covariance matrices.
        - 'diag': Implements diagonal covariance matrices.
        - 'tied_diag': Applies diagonal covariance matrices with equal diagonal elements.

        Defaults to 'full'.

    shared_covariance : bool, optional
        If set to True, all clusters will share the same covariance matrix.

        Defaults to False.

    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 0.

    max_iter : int, optional
        Defines the maximum iterations the EM algorithm can undertake.

        Defaults to 100.

    categorical_variable : str or a list of str, optional
        Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

        No default value.
    category_weight : float, optional
        Represents the weight of category attributes.

        Defaults to 0.707.

    error_tol : float, optional
        Defines the error tolerance, serving as a termination condition for the algorithm.

        Defaults to 1e-5.

    regularization : float, optional
        Represents the regularization factor added to the diagonal elements of covariance matrices to guarantee their positive-definiteness.

        Defaults to 1e-6.

    random_seed : int, optional
        Indicates the seed used to initialize the random number generator:

        - 0: The system time is deployed as the default seed.
        - Not 0: The user-defined value is used as the seed.

        Defaults to 0.

    Attributes
    ----------
    model_ : DataFrame
        Model content.

    labels_ : DataFrame
        Cluster membership probabilities for each data point.

    stats_ : DataFrame
        Statistics.

    Examples
    --------
    Input DataFrame df:

    >>> df.collect()
        ID     X1     X2  X3
    0    0   0.10   0.10   1
    1    1   0.11   0.10   1
    ...
    22  22  10.13  10.14   2
    23  23  10.14  10.13   2

    Create a GMM instance:

    >>> gmm = GaussianMixture(init_param='farthest_first_traversal',
    ...                       n_components=2, covariance_type='full',
    ...                       shared_covariance=False, max_iter=500,
    ...                       error_tol=0.001, thread_ratio=0.5,
    ...                       categorical_variable=['X3'], random_seed=1)

    Perform fit():

    >>> gmm.fit(data=df, key='ID')

    Output:

    >>> gmm.labels_.head(14).collect()
        ID  CLUSTER_ID  PROBABILITY
    0    0           0          0.0
    1    1           0          0.0
    ...
    12  13           0          1.0
    13  14           0          0.0

    >>> gmm.stats_.collect()
             STAT_NAME     STAT_VALUE
    0   log-likelihood        11.7199
    1              aic      -504.5536
    2              bic      -480.3900

    >>> gmm.model_collect()
       ROW_INDEX    CLUSTER_ID           MODEL_CONTENT
    0          0            -1           {"Algorithm":"GMM","Metadata":{"DataP...
    1          1             0           {"GuassModel":{"covariance":[22.18895...
    2          2             1           {"GuassModel":{"covariance":[22.19450...
    """
    init_param_map = {'farthest_first_traversal': 0,
                      'manual': 1,
                      'random_means': 2,
                      'k_means++': 3}
    covariance_type_map = {'full': 0, 'diag': 1, 'tied_diag': 2}

    def __init__(self,
                 init_param,
                 n_components=None,
                 init_centers=None,
                 covariance_type=None,
                 shared_covariance=False,
                 thread_ratio=None,
                 max_iter=None,
                 categorical_variable=None,
                 category_weight=None,
                 error_tol=None,
                 regularization=None,
                 random_seed=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(GaussianMixture, self).__init__()

        self.init_param = self._arg('init_param',
                                    init_param,
                                    self.init_param_map,
                                    required=True)
        self.n_components = self._arg('n_components', n_components, int)
        self.init_centers = self._arg('init_centers', init_centers, list)
        if init_param == 'manual':
            if init_centers is None:
                msg = "Parameter init_centers is required when init_param is manual."
                logger.error(msg)
                raise ValueError(msg)
            if n_components is not None:
                msg = ("Parameter n_components is not applicable when " +
                       "init_param is manual.")
                logger.error(msg)
                raise ValueError(msg)
        else:
            if n_components is None:
                msg = ("Parameter n_components is required when init_param is " +
                       "farthest_first_traversal, random_means and k_means++.")
                logger.error(msg)
                raise ValueError(msg)
            if init_centers is not None:
                msg = ("Parameter init_centers is not applicable when init_param is " +
                       "farthest_first_traversal, random_means and k_means++.")
                logger.error(msg)
                raise ValueError(msg)
        self.covariance_type = self._arg('covariance_type',
                                         covariance_type,
                                         self.covariance_type_map)
        self.shared_covariance = self._arg('shared_covariance', shared_covariance, bool)
        self.thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        self.max_iter = self._arg('max_iter', max_iter, int)
        if isinstance(categorical_variable, str):
            categorical_variable = [categorical_variable]
        self.categorical_variable = self._arg('categorical_variable',
                                              categorical_variable, ListOfStrings)
        self.category_weight = self._arg('category_weight',
                                         category_weight, float)
        self.error_tol = self._arg('error_tol', error_tol, float)
        self.regularization = self._arg('regularization', regularization, float)
        self.random_seed = self._arg('random_seed', random_seed, int)

    @trace_sql
    def fit(self, data, key=None, features=None, categorical_variable=None):
        """
        Perform GMM clustering on input dataset.

        Parameters
        ----------
        data : DataFrame
            Data to be clustered.

        key : str, optional

            Name of ID column.
            Defaults to the index column of data (i.e. data.index) if it is set.
            If the index of column of data is not provided, please enter the value of key.

        features : a list of str, optional
            List of strings specifying feature columns.

            If a list of features is not given, all the columns except the ID column
            are taken as features.
        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
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
        outputs = ['RESULT', 'MODEL', 'STATISTICS', 'PLACEHOLDER']
        outputs = ['#PAL_GMM_{}_TBL_{}_{}'.format(name, self.id, unique_id)
                   for name in outputs]
        result_tbl, model_tbl, statistics_tbl, placeholder_tbl = outputs
        init_param_data = self._prep_init_param(conn, data, key)

        param_rows = [
            ("INIT_MODE", self.init_param, None, None),
            ("COVARIANCE_TYPE", self.covariance_type, None, None),
            ("SHARED_COVARIANCE", self.shared_covariance, None, None),
            ("CATEGORY_WEIGHT", None, self.category_weight, None),
            ("MAX_ITERATION", self.max_iter, None, None),
            ("THREAD_RATIO", None, self.thread_ratio, None),
            ("ERROR_TOL", None, self.error_tol, None),
            ("REGULARIZATION", None, self.regularization, None),
            ("SEED", self.random_seed, None, None)
            ]
        if self.categorical_variable is not None:
            param_rows.extend(("CATEGORICAL_VARIABLE", None, None, variable)
                              for variable in self.categorical_variable)

        if categorical_variable is not None:
            param_rows.extend(("CATEGORICAL_VARIABLE", None, None, variable)
                              for variable in categorical_variable)

        try:
            self._call_pal_auto(conn,
                                'PAL_GMM',
                                data_,
                                init_param_data,
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
        # pylint: disable=attribute-defined-outside-init
        self.model_ = conn.table(model_tbl)
        self.labels_ = conn.table(result_tbl)
        self.stats_ = conn.table(statistics_tbl)
        self.statistics_ = self.stats_

    def fit_predict(self, data, key, features=None, categorical_variable=None):
        """
        Perform GMM clustering on input dataset and return cluster membership
        probabilities for each data point.

        Parameters
        ----------
        data : DataFrame
            Data to be clustered.

        key : str, optional

            Name of ID column.
            Defaults to the index column of data (i.e. data.index) if it is set.
            If the index of column of data is not provided, please enter the value of key.

        features : a list of str, optional
            List of strings specifying feature columns.

            If a list of features is not given, all the columns except the ID column
            are taken as features.

        categorical_variable : str or a list of str, optional
        Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

        No default value.

        Returns
        -------
        DataFrame
            Cluster membership probabilities.
        """
        self.fit(data, key, features, categorical_variable)
        return self.labels_

    def _prep_init_param(self, conn, data, key):
        if self.n_components is not None:
            init_param_data = conn.sql("SELECT 1 ID, {} CLUSTER_NUMBER FROM DUMMY;".format(self.n_components)).cast({"ID" : "INTEGER", "CLUSTER_NUMBER" : "INTEGER"})
        elif self.init_centers is not None:
            id_type = data.dtypes([key])[0][1]
            sql_dummy = ""
            for idx, val in enumerate(self.init_centers):
                if id_type in ['VARCHAR', 'NVARCHAR']:
                    new_union = "SELECT {} ID, {} SEEDS FROM DUMMY UNION ".format(idx, "'"+val+"'")
                else:
                    new_union = "SELECT {} ID, {} SEEDS FROM DUMMY UNION ".format(idx, val)
                sql_dummy = sql_dummy + new_union
            sql_dummy = sql_dummy[:-6]
            init_param_data = conn.sql(sql_dummy)

        return init_param_data

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
            Names of the feature columns.

            If ``features`` is not provided, it defaults to all non-key columns.

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
        return super(GaussianMixture, self)._predict(data, key, features)#pylint:disable=line-too-long
