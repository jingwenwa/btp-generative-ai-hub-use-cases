"""
This module contains Python wrapper for PAL ROCKET algorithm.

The following class are available:

    * :class:`ROCKET`
"""
#pylint: disable=too-many-lines, line-too-long, too-many-locals, too-many-arguments, too-many-branches
#pylint: disable=c-extension-no-member, super-with-arguments, too-many-statements, invalid-name
#pylint: disable=duplicate-string-formatting-argument, too-many-instance-attributes, too-few-public-methods
#pylint: disable=too-many-arguments, too-many-branches, too-many-statements, attribute-defined-outside-init
import logging
import uuid
from hdbcli import dbapi
from hana_ml.ml_exceptions import FitIncompleteError
from ..utility import check_pal_function_exist
from .utility import _col_index_check
from ..pal_base import (
    PALBase,
    ParameterTable,
    pal_param_register,
    try_drop,
    require_pal_usable
)
from ..sqlgen import trace_sql

logger = logging.getLogger(__name__)

class ROCKET(PALBase):
    r"""
    RandOm Convolutional KErnel Transform (ROCKET) is an exceptionally efficient algorithm for time series classification. Unlike other proposed time series classification algorithms which attain excellent accuracy, ROCKET maintains its performance with a fraction of the computational expense by transforming input time series using random convolutional kernels.

    Parameters
    ----------
    method : str, optional
        The options are "MiniRocket" and "MultiRocket".

        Defaults to "MiniRocket".

    num_features : int, optional
        Number of transformed features for each time series.

        Defaults to 9996 when ``method`` = "MiniRocket", 49728 when ``method`` = "MultiRocket".

    data_dim : int, optional
        Dimensionality of the multivariate time series.

        1 means univariate time series and others for multivariate. Cannot be smaller than 1.

        Defaults to 1.

    random_seed : int, optional
        0 indicates using machine time as seed.

        Defaults to 0.

    Attributes
    ----------
    model\_ : DataFrame
        Model content.

    Examples
    --------
    Example 1: Univariate time series fitted and transformed by MiniRocket
    Input DataFrame df:

    >>> df.collect()
        RECORD_ID  VAL_1  VAL_2  VAL_3  VAL_4  VAL_5  VAL_6  ...  VAL_10  VAL_11  VAL_12  VAL_13  VAL_14  VAL_15  VAL_16
    0           0  1.598  1.599  1.571  1.550  1.507  1.434  ...   1.117   1.024   0.926   0.828   0.739   0.643   0.556
    1           1  1.701  1.671  1.619  1.547  1.475  1.391  ...   1.070   0.985   0.899   0.816   0.733   0.658   0.581
    ...
    10         10  1.614  1.574  1.557  1.521  1.460  1.406  ...   1.045   0.957   0.862   0.771   0.681   0.587   0.497
    11         11  1.652  1.665  1.656  1.623  1.571  1.499  ...   1.155   1.058   0.973   0.877   0.797   0.704   0.609

    Create an instance of ROCKET:

    >>> ro = ROCKET(method="MiniRocket", random_seed=1)

    Perform fit():

    >>> ro.fit(data=df)

    Model:

    >>> ro.model_.collect()
        ID                                      MODEL_CONTENT
    0   -1                                         MiniRocket
    1    0  {"SERIES_LENGTH":16,"NUM_CHANNELS":1,"BIAS_SIZ...
    2    1  843045766098464,1.0396523357230486,2.005001093...
    ......

    Make a transformation:

    >>> result = ro.transform(data=df)
    >>> result.collect()
          ID                                     STRING_CONTENT
    0      0  {"NUM_FEATURES_PER_DATA":9996,"FEATURES":[{"DA...
    1      1  ,0.375,0.875,0.125,0.5,1.0,0.25,0.75,1.0,0.375...
    ...
    126  126  .0,0.0,0.75,0.0,0.375,1.0,0.0,0.75,1.0,0.125,0...
    127  127  25,0.625,0.1875,0.375,0.75,0.0,0.625,0.0,0.0,0...

    Example 2: Multivariate time series (with dimensionality 8) fitted and transformed by MultiRocket
    Input DataFrame df:

    >>> df.collect()
        RECORD_ID  VAL_1  VAL_2  VAL_3  VAL_4  VAL_5  VAL_6  ...  VAL_10  VAL_11  VAL_12  VAL_13  VAL_14  VAL_15  VAL_16
    0           0  1.645  1.646  1.621  1.585  1.540  1.470  ...   1.161   1.070   0.980   0.893   0.798   0.705   0.620
    1           1  1.704  1.705  1.706  1.680  1.632  1.560  ...   1.186   1.090   0.994   0.895   0.799   0.702   0.605
    ...
    30         30  1.688  1.648  1.570  1.490  1.408  1.327  ...   1.011   0.930   0.849   0.768   0.687   0.606   0.524
    31         31  1.708  1.663  1.595  1.504  1.411  1.318  ...   0.951   0.861   0.794   0.704   0.614   0.529   0.446

    Create an instance of ROCKET:

    >>> ro = ROCKET(method="multirocket", data_dim=8)

    Perform fit():

    >>> ro.fit(data=df)

    Model:

    >>> ro.model_.collect()
        ID                                      MODEL_CONTENT
    0   -1                                        MultiRocket
    1    0  {"SERIES_LENGTH":16,"NUM_CHANNELS":8,"BIAS_SIZ...
    2    1  HANNELS":[6]},{"ID":77,"CHANNELS":[1,4,7,6,5]}...
    ......

    Make a transformation:

    >>> result = ro.transform(data=df)
    >>> result.collect()
          ID                                     STRING_CONTENT
    0      0  {"NUM_FEATURES_PER_DATA":49728,"FEATURES":[{"D...
    1      1  .5625,0.8125,0.125,0.6875,0.9375,0.5,0.6875,0....
    ..   ...                                                ...
    241  241  .857142,7.357142,12.333333,9.0,12.5,11.8,7.692...
    242  242  1.0,3.0,3.0,5.0,3.0,-1.0,3.0,3.0,-1.0,3.0,-1.0...
    """

    def __init__(self,
                 method=None,
                 num_features=None,
                 data_dim=None,
                 random_seed=None):

        setattr(self, 'hanaml_parameters', pal_param_register())
        super(ROCKET, self).__init__()
        method_map = {"minirocket":0, "multirocket":1}
        self.method       = self._arg('method',       method,       method_map)
        self.num_features = self._arg('num_features', num_features, int)
        self.data_dim     = self._arg('data_dim',     data_dim,     int)
        self.random_seed  = self._arg('random_seed',  random_seed,  int)

        self.model_ = None

    @trace_sql
    def fit(self, data, key=None):
        r"""
        Fit the model to the training dataset.

        Parameters
        ----------
        data : DataFrame
            Input data.

            For univariate time series, each row represents one time series, while for multivariate time series, a fixed number of consecutive rows forms one time series,
            and that number is designated by the parameter ``data_dim`` when initialize a ROCKET instance.

        key : str, optional
            The ID column.

            Defaults to the first column of data if the index column of data is not provided.
            Otherwise, defaults to the index column of data.

        Returns
        -------
        A fitted object of class "ROCKET".

        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        index = data.index
        cols  = data.columns

        key = self._arg('key', key, str)
        index = data.index
        if index is not None:
            key = _col_index_check(key, 'key', index, cols)
        else:
            if key is None:
                key = cols[0]

        if key is not None and key not in cols:
            msg = f"Please select key from {cols}!"
            logger.error(msg)
            raise ValueError(msg)
        cols.remove(key)

        ts    = cols
        data_ = data[[key] + ts]

        conn = data.connection_context
        require_pal_usable(conn)

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        model_tbl = f'#PAL_ROCKET_MODEL_TBL_{self.id}_{unique_id}'
        outputs = [model_tbl]
        param_rows = [
            ('METHOD',       self.method,       None, None),
            ('NUM_FEATURES', self.num_features, None, None),
            ('DATA_DIM',     self.data_dim,     None, None),
            ('RANDOM_SEED',  self.random_seed,  None, None)
            ]
        if not (check_pal_function_exist(conn, '%ROCKET%', like=True) or \
        self._disable_hana_execution):
            msg = 'The version of your SAP HANA does not support ROCKET!'
            logger.error(msg)
            raise ValueError(msg)
        try:
            self._call_pal_auto(conn,
                                'PAL_ROCKET_FIT',
                                data_,
                                ParameterTable().with_data(param_rows),
                                *outputs)
        except dbapi.Error as db_err:
            msg = str(conn.hana_version())
            logger.exception("HANA version: %s. %s", msg, str(db_err))
            try_drop(conn, outputs)
            raise
        except Exception as db_err:
            msg = str(conn.hana_version())
            logger.exception("HANA version: %s. %s", msg, str(db_err))
            try_drop(conn, outputs)
            raise

        self.model_ = conn.table(model_tbl)
        return self

    @trace_sql
    def transform(self, data, key=None, thread_ratio=None):
        r"""
        Transform time series based on a given ROCKET model fitted by fit(). Hence, The data should be in the exact same format as that in fit(), especially the length and dimensionality of time series.
        The model\_ used in transform comes from fit() as well.

        Parameters
        ----------

        data : DataFrame
            Input data.
            For univariate time series, each row represents one time series, while for multivariate time series, a fixed number of consecutive rows forms one time series,
            and that number is designated by the parameter ``data_dim`` when initialize a ROCKET instance.

        key : str, optional
            The ID column.

            Defaults to the first column of data if the index column of data is not provided.
            Otherwise, defaults to the index column of data.

        thread_ratio : float, optional
            Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
            Values outside the range will be ignored and this function heuristically determines the number of threads to use.

            Defaults to 1.0.

        Returns
        -------

        DataFrame
            Features, structured as follows:

            - ID, type INTEGER, ROW_INDEX, indicates the ID of current row.
            - STRING_CONTENT, type NVARCHAR, transformed features in JSON format.
        """
        if getattr(self, 'model_') is None:
            raise FitIncompleteError()

        thread_ratio = self._arg('thread_ratio', thread_ratio, float)

        index = data.index
        cols = data.columns

        key = self._arg('key', key, str)
        index = data.index
        if index is not None:
            key = _col_index_check(key, 'key', index, cols)
        else:
            if key is None:
                key = cols[0]

        if key is not None and key not in cols:
            msg = f"Please select key from {cols}!"
            logger.error(msg)
            raise ValueError(msg)
        cols.remove(key)

        ts    = cols
        data_ = data[[key] + ts]

        conn = data.connection_context
        param_rows = [('THREAD_RATIO', None, thread_ratio, None)]

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        result_tbl = f"#PAL_ROCKET_TRANSFORM_RESULT_TBL_{self.id}_{unique_id}"
        if not (check_pal_function_exist(conn, '%ROCKET%', like=True) or \
        self._disable_hana_execution):
            msg = 'The version of your SAP HANA does not support ROCKET!'
            logger.error(msg)
            raise ValueError(msg)
        try:
            self._call_pal_auto(conn,
                                'PAL_ROCKET_TRANSFORM',
                                data_,
                                self.model_,
                                ParameterTable().with_data(param_rows),
                                result_tbl)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, result_tbl)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, result_tbl)
            raise

        return conn.table(result_tbl)
