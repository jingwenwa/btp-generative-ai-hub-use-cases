"""
This module contains Python API of PAL
T-distributed Stochastic Neighbour Embedding algorithm.
The following classes are available:

    * :class:`TSNE`
"""
#pylint: disable=consider-using-f-string
import logging
import uuid
from hdbcli import dbapi
from .pal_base import (
    PALBase,
    ParameterTable,
    ListOfStrings,
    try_drop,
    require_pal_usable,
    pal_param_register
)
logger = logging.getLogger(__name__)#pylint: disable=invalid-name

class TSNE(PALBase):#pylint: disable=too-many-instance-attributes, too-few-public-methods
    r"""
    t-Distributed Stochastic Neighbor Embedding (t-SNE) is a non-linear dimensionality reduction technique that is particularly well-suited for visualizing high-dimensional datasets by reducing them to lower dimensions (typically 2D or 3D) for effective visualization.

    Parameters
    ----------
    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 0.0.
    n_iter : int, optional
        Specifies the maximum number of iterations for the TSNE algorithm.

        Default to 250.
    random_state : int, optional
        The seed for random number generate.

        Default to 0.
    exaggeration : float, optional
        Value to be multiplied on :math:`p_{ij}` before 250 iterations.
        The natural clusters would be more separated with larger value, which means
        there would be more empty space on the map.

        Default to 12.0.
    angle : float, optional
        The legal value should be between 0.0 to 1.0.
        Setting it to 0.0 means using the "exact" method which would run :math:`O(N^2)` time,
        otherwise TSNE would employ Barnes-Hut approximation which would run :math:`O(N*log{N})`.
        This value is a tradeoff between accuracy and training speed for Barnes-Hut
        approximation. The training speed would be faster with higher value.

        Default to 0.5.
    n_components : int, optional
        Dimension of the embedded space.
        Values other than 2 and 3 are illegal.

        Default to 2.
    object_frequency : int, optional
        Frequency of calculating the objective function and putting the result
        into OBJECTIVES table.
        This parameter value should not be larger than the value assigned to ``n_iter``.

        Default to 50.
    learning_rate : float, optional
        Learning rate.

        Default to 200.0.
    perplexity : float, optional
        The perplexity is related to the number of nearest neighbors and  mentioned
        above. Larger value is suitable for large dataset. Make sure ``preplexity`` * 3 < [no. of samples]

        Default to 30.0.

    Examples
    --------
    >>> tsne = TSNE(n_iter=500, n_components=3, angle=0, object_frequency=50, random_state=30)

    Performing fit_predict():

    >>> res, stats, obj = tsne.fit_predict(data=df_train, key='ID', perplexity=1.0)
    >>> res.collect()
    >>> stats.collect()
    >>> obj.collect()

    """
    def __init__(self,#pylint: disable=too-many-arguments, too-many-locals, too-many-branches, too-many-statements
                 n_iter=None,
                 learning_rate=None,
                 object_frequency=None,
                 n_components=None,
                 angle=None,
                 exaggeration=None,
                 thread_ratio=None,
                 random_state=None,
                 perplexity=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(TSNE, self).__init__()
        self.n_iter = self._arg('n_iter', n_iter, int)
        self.learning_rate = self._arg('learning_rate', learning_rate, float)
        self.object_frequency = self._arg('object_frequency', object_frequency, int)
        if self.object_frequency is not None:
            if self.n_iter is not None and self.n_iter < self.object_frequency:
                msg = ("'object_frequency' should not exceed the value of 'n_iter'")
                logger.error(msg)
                raise ValueError(msg)
        self.n_components = self._arg('n_components', n_components, int)
        if self.n_components is not None and self.n_components not in (2, 3):
            msg = ("'n_components' of the embedded space cannot have a value other than 2 or 3.")
            logger.error(msg)
            raise ValueError(msg)
        self.angle = self._arg('angle', angle, float)
        self.exaggeration = self._arg('exaggeration', exaggeration, float)
        self.thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        self.random_state = self._arg('random_state', random_state, int)
        self.perplexity = self._arg('perplexity', perplexity, float)

    def fit_transform(self, data, key, features=None):
        """
        Fit the TSNE model with input data.
        Model parameters should be given by initializing the model first.

        Parameters
        ----------
        data : DataFrame
            Data to be fit.
        key : str, optional
            Name of the ID column.
        features : ListofStrings/str, optional
            Name of the features column.

            If not specified, the feature columns should be all
            columns in the input DataFrame except the key column.

        Returns
        -------
        DataFrames
            - Result table with coordinate value of different dimensions.
            - Table of statistical values.
            - Table of objective values of iterations.
        """
        conn = data.connection_context
        require_pal_usable(conn)
        setattr(self, 'hanaml_fit_params', pal_param_register())
        key = self._arg('key', key, str, True)
        data_ = data
        if features is not None:
            if isinstance(features, str):
                features = [features]
            try:
                features = self._arg('features', features, ListOfStrings)#pylint: disable=undefined-variable
                data_ = data[[key] + features]
            except:
                msg = ("'features' must be list of string or string.")
                logger.error(msg)
                raise TypeError(msg)
        if self.perplexity is not None and not self.perplexity * 3 < int(data.count()):
            msg = ("'Perplexity' * 3 must be less than number of samples in input dataframe.")
            logger.error(msg)
            raise ValueError(msg)
        param_rows = [('THREAD_RATIO', None, self.thread_ratio, None),
                      ('SEED', self.random_state, None, None),
                      ('MAX_ITER', self.n_iter, None, None),
                      ('EXAGGERATION', None, self.exaggeration, None),
                      ('PERPLEXITY', None, self.perplexity, None),
                      ('THETA', None, self.angle, None),
                      ('NO_DIM', self.n_components, None, None),
                      ('OBJ_FREQ', self.object_frequency, None, None),
                      ('ETA', None, self.learning_rate, None)]

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        tables = ['RESULT', 'STATISTICS', 'OBJECTIVES']
        tables = ["#PAL_TSNE_{}_TBL_{}_{}".format(tbl, self.id, unique_id) for tbl in tables]
        res_tbl, stats_tbl, obj_tbl = tables
        try:
            self._call_pal_auto(conn,
                                "PAL_TSNE",
                                data_,
                                ParameterTable().with_data(param_rows),
                                *tables)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, tables)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, tables)
            raise
        return conn.table(res_tbl), conn.table(stats_tbl), conn.table(obj_tbl)#pylint: disable=line-too-long

    def fit_predict(self, data, key, features=None):
        r"""
        Alias of fit_transform(). Reserved for backward compatibility.
        """
        return self.fit_transform(data, key, features)
