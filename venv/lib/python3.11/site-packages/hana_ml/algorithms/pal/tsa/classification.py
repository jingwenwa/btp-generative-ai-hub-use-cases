"""
This module contains Python wrapper for PAL TimeSeriesClassification algorithm.

The following class is available:

    * :class:`TimeSeriesClassification`
"""
#pylint: disable=too-many-lines, line-too-long, too-many-locals, too-many-arguments, too-many-branches, consider-iterating-dictionary
#pylint: disable=c-extension-no-member, super-with-arguments, too-many-statements, invalid-name, consider-using-dict-items
#pylint: disable=duplicate-string-formatting-argument, too-many-instance-attributes, too-few-public-methods
#pylint: disable=too-many-arguments, too-many-branches, too-many-statements, attribute-defined-outside-init
import logging
import uuid
from hdbcli import dbapi
from hana_ml.ml_exceptions import FitIncompleteError
from ..utility import check_pal_function_exist, _map_param
from .utility import _col_index_check
from ..pal_base import (
    arg,
    PALBase,
    ParameterTable,
    pal_param_register,
    try_drop,
    require_pal_usable
)
from ..sqlgen import trace_sql

logger = logging.getLogger(__name__)

def _params_check(input_dict, param_map, classification_method, transform_method):
    update_params = {}
    if not input_dict:
        return {}
    for parm in input_dict:
        if parm in param_map.keys():
            parm_val = input_dict[parm]
            arg_map = param_map[parm]
            #if arg_map[1] == ListOfStrings and isinstance(parm_val, str):
            #    parm_val = [parm_val]
            if len(arg_map) == 2:
                update_params[arg_map[0]] = (arg(parm, parm_val, arg_map[1]), arg_map[1])
            else:
                update_params[arg_map[0]] = (arg(parm, parm_val, arg_map[2]), arg_map[1])
        else:
            err_msg = f"'{parm}' is not a valid parameter name for initializing a {classification_method} with {transform_method} model"
            logger.error(err_msg)
            raise KeyError(err_msg)
    return update_params

class TimeSeriesClassification(PALBase):
    r"""
    Time series classification.
    Currently, we support RandOm Convolutional KErnel Transform (ROCKET) in this class. Unlike other proposed time series classification algorithms which attain excellent accuracy, ROCKET maintains its performance with a fraction of the computational expense by transforming input time series using random convolutional kernels.
    Afterwards, a simple classifier is enough to train the transformed features and render the state-of-the-art accuracy. Specifically, in our implementation, the multi-class logistic regression is employed.
    Instead of the original ROKCET algorithm, we provide two variants of it to transform the input time series, that is, MiniRocket and MultiRocket. MiniRokcet is even faster than ROCKET while maintaining essentially the same accuracy.
    MultiRocket takes the first order difference of the input time series and improves the diversity of generated features to boost the accuracy with the extra cost of time and space.
    Particularly, this function supports both univariate and multivariate time series.

    Parameters
    ----------
    classification_method : str, optional
        The options is "LogisticRegression".

        Defaults to "LogisticRegression".
    transform_method : str, optional
        The options are "MiniRocket" and "MultiRocket".

        Defaults to "MiniRocket".
    **kwargs : keyword arguments
        Arbitrary keyword arguments and please referred to the responding algorithm for the parameters' key-value pair.

        For "MiniRocket"/ "MultiRocket":

        - `num_features` : int, optional

          Number of transformed features for each time series.

          Defaults to 9996 when ``transform_method`` is "MiniRocket", 49728 when ``transform_method`` is "MultiRocket".
        - `data_dim` : int, optional

          Dimensionality of the multivariate time series.

          1 means univariate time series and others for multivariate.
          Cannot be smaller than 1.

          Defaults to 1.
        - `random_seed` : int, optional

          0 indicates using machine time as seed.

          Defaults to 0.

    Attributes
    ----------
    model_ : DataFrame
        Model content.

    statistics_ : DataFrame
        Statistics.

    forecast_ : DataFrame
        Forecast values.

    Examples
    --------
    Example 1: Univariate time series fitted and transformed by MiniRocket
    Input DataFrame:

    >>> df.collect()
        RECORD_ID  VAL_1  VAL_2  VAL_3  VAL_4  VAL_5  VAL_6  ...  VAL_10  VAL_11  VAL_12  VAL_13  VAL_14  VAL_15  VAL_16
    0           0  1.598  1.599  1.571  1.550  1.507  1.434  ...   1.117   1.024   0.926   0.828   0.739   0.643   0.556
    1           1  1.701  1.671  1.619  1.547  1.475  1.391  ...   1.070   0.985   0.899   0.816   0.733   0.658   0.581
    ...
    11         11  1.652  1.665  1.656  1.623  1.571  1.499  ...   1.155   1.058   0.973   0.877   0.797   0.704   0.609

    The Dataframe of label of time series:

    >>> label_df.collect()
        DATA_ID LABEL
    0         0     A
    1         1     B
    ...
    11       11     A

    Create an instance of TimeSeriesClassification:

    >>> tsc = TimeSeriesClassification(classification_method="LogisticRegression",
                                       transform_method="MiniRocket")

    Perform fit():

    >>> tsc.fit(data=df, label=label_df)

    Output:

    >>> tsc.model_.collect()
          ID                                      MODEL_CONTENT
    0     -1                                         MiniRocket
    1      0  {"SERIES_LENGTH":16,"NUM_CHANNELS":1,"BIAS_SIZ...
    2      1  3005121315,1.685720499622002,2.819106917236017...
    ..   ...                                                ...
    121  120  90682684,0.05522285367663122,0.0,0.0,0.0,0.0,0...
    >>> tsc.statistics_.collect()
                       STAT_NAME   STAT_VALUE
    0  MINIROCKET_TRANSFORM_TIME       0.010s
    1              TRAINING_TIME       0.043s
    2          TRAINING_ACCURACY            1
    3               TRAINING_OBJ  6.45594e-14
    4              TRAINING_ITER           56

    Perform predict():

    >>> result = tsc.predict(data=df)
    >>> result.collect()
        ID CLASS  PROBABILITY
    0    0     A          1.0
    1    1     B          1.0
    ...
    11  11     A          1.0

    Example 2: Multivariate time series (with dimensionality 8) fitted and transformed by MultiRocket
    Input DataFrame:

    >>> df.collect()
        RECORD_ID  VAL_1  VAL_2  VAL_3  VAL_4  VAL_5  VAL_6  ...  VAL_10  VAL_11  VAL_12  VAL_13  VAL_14  VAL_15  VAL_16
    0           0  1.645  1.646  1.621  1.585  1.540  1.470  ...   1.161   1.070   0.980   0.893   0.798   0.705   0.620
    1           1  1.704  1.705  1.706  1.680  1.632  1.560  ...   1.186   1.090   0.994   0.895   0.799   0.702   0.605
    ...
    31         31  1.708  1.663  1.595  1.504  1.411  1.318  ...   0.951   0.861   0.794   0.704   0.614   0.529   0.446

    The Dataframe of label of time series:

    >>> label_df.collect()
       DATA_ID LABEL
    0        0     A
    1        1     B
    2        2     C
    3        3     A

    Create an instance of TimeSeriesClassification:

    >>> tscm = TimeSeriesClassification(classification_method="LogisticRegression",
                                        transform_method = "MultiRocket",
                                        data_dim=8,
                                        random_seed=1)

    Perform fit():

    >>> tscm.fit(data=df, label=label_df)

    Output:

    >>> tscm.model_.collect()
          ID                                      MODEL_CONTENT
    0     -1                                        MultiRocket
    1      0  {"SERIES_LENGTH":16,"NUM_CHANNELS":8,"BIAS_SIZ...
    2      1  HANNELS":[6]},{"ID":77,"CHANNELS":[1,4,7,6,5]}...
    ...
    537  536  99,-0.000006703700582543153,0.0,-0.00132556403...

    >>> tscm.statistics_.collect()
                        STAT_NAME   STAT_VALUE
    0  MULTIROCKET_TRANSFORM_TIME       0.005s
    1               TRAINING_TIME       0.147s
    2           TRAINING_ACCURACY            1
    3                TRAINING_OBJ  7.96585e-14
    4               TRAINING_ITER           48

    Perform predict():

    >>> tscm.predict(data=df_predict).collect()
       ID CLASS  PROBABILITY
    0   0     A          1.0
    1   1     B          1.0
    2   2     C          1.0
    3   3     A          1.0

    """
    ROCKET_map = {
            'num_features' : ('NUM_FEATURES', int),
            'data_dim' : ('DATA_DIM', int),
            'random_seed' : ('RANDOM_SEED', int)}

    map_dict = {
        'minirocket' : ROCKET_map,
        'multirocket' : ROCKET_map}

    def __init__(self,
                 classification_method="LogisticRegression",
                 transform_method="MiniRocket",
                 **kwargs):

        super(TimeSeriesClassification, self).__init__()
        setattr(self, 'hanaml_parameters', pal_param_register())

        self.classification_method = self._arg('classification_method', classification_method, str).lower()
        self.transform_method = self._arg('transform_method', transform_method, str).lower()
        self.params = dict(**kwargs)

        ROCKET_method = ['minirocket', 'multirocket']
        if self.classification_method == "logisticregression" and self.transform_method in ROCKET_method:
            func_map = self.map_dict[self.transform_method]

        self.__pal_params = {}
        self.__pal_params = _params_check(input_dict=self.params,
                                          param_map=func_map,
                                          classification_method=classification_method,
                                          transform_method=transform_method)
        self.model_ = None
        self.statistics_ = None
        self.forecast_ = None

    @trace_sql
    def fit(self,
            data,
            label=None,
            key=None,
            thread_ratio=None):
        """
        Fit the model to the training dataset.

        Parameters
        ----------
        data : DataFrame
            Input data.
            When transform_method="MiniRocket", for univariate time series, each row represents one time series.
            when transform_method="MultiRocket", for multivariate time series , a fixed number of consecutive rows forms one time series,
            and that number is designated by the parameter ``data_dim`` when initializing a TimeSeriesClassification instance.

        label : DataFrame, optional
            The label of time series.
            If classification_method is "LogisticRegression" and transform_method is "MiniRocket"/"MultiRocket", label is a mandatory parameter.

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
        A fitted object of class "TimeSeriesClassification".
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        ROCKET_method_map = {'minirocket' : 0, 'multirocket' : 1}

        if self.classification_method == "logisticregression" and self.transform_method in ROCKET_method_map.keys():
            if label is None:
                msg = 'The label for fit cannot be None!'
                logger.error(msg)
                raise ValueError(msg)

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
        model_tbl = f'#PAL_TS_CLASSIFICATION_MODEL_TBL_{self.id}_{unique_id}'
        stat_tbl  = f'#PAL_TS_CLASSIFICATION_STATS_TBL_{self.id}_{unique_id}'

        param_rows = []
        for name in self.__pal_params:
            value, typ = self.__pal_params[name]
            tpl = [_map_param(name, value, typ)]
            param_rows.extend(tpl)

        if self.classification_method == "logisticregression" and self.transform_method in ROCKET_method_map.keys():
            param_rows.extend([('METHOD', ROCKET_method_map[self.transform_method], None, None)])
            outputs = [model_tbl, stat_tbl]
            if not (check_pal_function_exist(conn, '%ROCKET%', like=True) or \
            self._disable_hana_execution):
                msg = 'The version of your SAP HANA does not support ROCKET!'
                logger.error(msg)
                raise ValueError(msg)
            try:
                self._call_pal_auto(conn,
                                    'PAL_ROCKET_TRAIN',
                                    data_,
                                    label,
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
        self.statistics_ = conn.table(stat_tbl)

    @trace_sql
    def predict(self, data, key=None, thread_ratio=None):
        """
        Generates time series forecasts based on the fitted model.

        Parameters
        ----------
        data : DataFrame
            Input data.
            For univariate time series, each row represents one time series, while for multivariate time series, a fixed number of consecutive rows forms one time series, and that number is designated by the parameter ``data_dim`` when initializing a TimeSeriesClassification instance.

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
            Prediction.
        """
        setattr(self, 'hanaml_predict_params', pal_param_register())
        if getattr(self, 'model_') is None:
            raise FitIncompleteError()

        thread_ratio = self._arg('thread_ratio', thread_ratio, float)

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

        result_tbl  = f'#PAL_TS_CLASSIFICATION_RES_TBL_{self.id}_{unique_id}'

        ROCKET_method = ['minirocket', 'multirocket']
        if self.classification_method == "logisticregression" and self.transform_method in ROCKET_method:
            outputs = [result_tbl]
            param_rows = [('THREAD_RATIO', None, thread_ratio, None)]
            if not (check_pal_function_exist(conn, '%ROCKET%', like=True) or \
            self._disable_hana_execution):
                msg = 'The version of your SAP HANA does not support ROCKET!'
                logger.error(msg)
                raise ValueError(msg)
            try:
                self._call_pal_auto(conn,
                                    'PAL_ROCKET_PREDICT',
                                    data_,
                                    self.model_,
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

        result = conn.table(result_tbl)
        self.forecast_ = result

        return result
