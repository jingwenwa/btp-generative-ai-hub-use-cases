"""
This module contains Python wrapper for PAL LTSF algorithm.

The following class are available:

    * :class:`LTSF`
"""
#pylint: disable=too-many-lines, line-too-long, too-many-locals, too-many-arguments, too-many-branches, broad-except
#pylint: disable=c-extension-no-member, super-with-arguments, too-many-statements, invalid-name, no-member
#pylint: disable=duplicate-string-formatting-argument, too-many-instance-attributes, too-few-public-methods, anomalous-backslash-in-string
import logging
import uuid
from hdbcli import dbapi
from hana_ml.ml_exceptions import FitIncompleteError
from hana_ml.ml_base import quotename
from .permutation_importance import PermutationImportanceMixin
from .utility import _convert_index_from_timestamp_to_int, _is_index_int, _get_forecast_starttime_and_timedelta, _col_index_check
from ..utility import check_pal_function_exist
from ..pal_base import (
    PALBase,
    ParameterTable,
    pal_param_register,
    try_drop,
    ListOfStrings,
    require_pal_usable
)
from ..sqlgen import trace_sql
logger = logging.getLogger(__name__)

class LTSF(PALBase, PermutationImportanceMixin):
    r"""
    Long-term time series forecasting (LTSF) is a specialized approach within the realm of predictive analysis, focusing on making predictions for extended periods into the long future.
    Although traditional algorithms are capable of predicting values in the near future, their performance will deteriorate greatly when it comes
    to long-term series forecasting. With the help of deep learning,
    this function implements a novel neural network architecture to achieve
    the state-of-the-art performance among the PAL family.

    Parameters
    ----------

    network_type : str, optional
        The type of network:

        - 'NLinear'
        - 'DLinear'
        - 'XLinear'
        - 'SCINet'
        - 'RLinear'
        - 'RMLP'

        Defaults to 'NLinear'.

    batch_size : int, optional
        The number of pieces of data for training in one iteration.

        Defaults to 8.
    num_epochs : int, optional
        The number of training epochs.

        Defaults to 1.
    random_seed : int, optional
        0 indicates using machine time as seed.

        Defaults to 0.
    adjust_learning_rate: bool, optional
        Decays the learning rate to its half after every epoch.

        - False: Do not use.
        - True: Use.

        Defaults to True.
    learning_rate : float, optional
        The initial learning rate for Adam optimizer.

        Defaults to 0.005.
    num_levels : int, optional
        The number of levels in the network architecture.
        This parameter is valid when ``network_type`` is 'SCINet'.

        Note that if :code:`warm_start = True` in `fit()`,
        then this parameter is not valid.

        Defaults to 2.
    kernel_size : int, optional
        Kernel size of Conv1d layer.
        This parameter is valid when ``network_type`` is 'SCINet'.

        Note that if :code:`warm_start = True` in `fit()`,
        then this parameter is not valid.

        Defaults to 3.
    hidden_expansion : int, optional
        Expands the input channel size of Conv1d layer.
        This parameter is valid when ``network_type`` is 'SCINet'.
        Note that if :code:`warm_start = True` in `fit()`,
        then this parameter is not valid.

        Defaults to 3.
    position_encoding:  bool, optional
        Position encoding adds extra positional embeddings to the training series.

        - False: Do not use.
        - True: Use.

        This parameter is valid when ``network_type`` is 'SCINet'.

        Defaults to True.
    dropout_prob : float, optional
        Dropout probability of Dropout layer.
        This parameter is valid when ``network_type`` is 'SCINet'.

        Defaults to 0.05.

    Attributes
    ----------
    model_ : DataFrame
        Model content.

    loss_ : DataFrame
        Indicates the information of training loss either batch ID or average batch loss indicator.

    explainer_ : DataFrame
        The explanations with decomposition of exogenous variables.
        The attribute only appear when ``show_explainer=True`` and ``network_type`` is 'XLinear' in the predict() function.

    permutation\_importance\_ : DataFrame
        The importance of exogenous variables as determined by permutation importance analysis.
        The attribute only appear when invoking get_permutation_importance() function after a trained model is obtained, structured as follows:

        - 1st column : PAIR, measure name.
        - 2nd column : NAME, exogenous regressor name.
        - 3rd column : VALUE, the importance of the exogenous regressor.


    Examples
    --------

    Input DataFrame is df_fit and create an instance of LTSF:

    >>> ltsf = LTSF(batch_size = 8,
                    num_epochs = 2,
                    adjust_learning_rate = True,
                    learning_rate = 0.005,
                    random_seed = 1)

    Performing fit():

    >>> ltsf.fit(data=df_fit,
                 train_length=32,
                 forecast_length=16,
                 key="TIME_STAMP",
                 endog="TARGET",
                 exog=["FEAT1", "FEAT2", "FEAT3", "FEAT4"])
    >>> ltsf.loss_.collect()
        EPOCH          BATCH      LOSS
    0       1              0  1.177407
    1       1              1  0.925078
    ...
    12      2              5  0.571699
    13      2  epoch average  0.618181

    Input DataFrame df_predict and perform predict():

    >>> result = ltsf.predict(data=df_predict)
    >>> result.collect()
       ID  FORECAST
    1   0  52.28396
    2   1  57.03466
    ...
    16 15  69.33713

    We also provide the continuous training which uses a parameter warm_start to control.
    The model used in the training is the attribute of `model\_` of a "LTSF" object.
    You could also use load_model() to load a trained model for continous training.

    >>> ltsf.num_epochs = 2
    >>> ltsf.learning_rate = 0.002
    >>> ltsf.fit(data=df_fit,
                 key="TIME_STAMP",
                 endog="TARGET",
                 exog=["FEAT1", "FEAT2", "FEAT3", "FEAT4"],
                 warm_start=True)

    """
#pylint: disable=too-many-arguments
    def __init__(self,
                 batch_size=None,
                 num_epochs=None,
                 random_seed=None,
                 network_type=None,
                 adjust_learning_rate=None,
                 learning_rate=None,
                 num_levels=None,
                 kernel_size=None,
                 hidden_expansion=None,
                 position_encoding=None,
                 dropout_prob=None):

        if not hasattr(self, 'hanaml_parameters'):
            setattr(self, 'hanaml_parameters', pal_param_register())
        super(LTSF, self).__init__()

        self.network_type_map = {'nlinear':0, 'dlinear':1, 'xlinear':2, 'scinet':3, 'rlinear':4, 'rmlp':5}
        if network_type:
            network_type = network_type.lower()
        self.network_type = self._arg('network_type', network_type, self.network_type_map)
        self.adjust_learning_rate = self._arg('adjust_learning_rate', adjust_learning_rate, bool)
        self.learning_rate = self._arg('learning_rate', learning_rate, float)
        self.batch_size = self._arg('batch_size', batch_size, int)
        self.num_epochs = self._arg('num_epochs', num_epochs, int)
        self.random_seed = self._arg('random_seed', random_seed, int)
        self.num_levels = self._arg('num_levels', num_levels, int)
        self.kernel_size = self._arg('kernel_size', kernel_size, int)
        self.hidden_expansion = self._arg('hidden_expansion', hidden_expansion, int)
        self.position_encoding = self._arg('position_encoding', position_encoding, bool)
        self.dropout_prob = self._arg('dropout_prob', dropout_prob, float)
        self.train_length = None
        self.forecast_length = None
        self.forecast_start = None
        self.timedelta = None
        self.is_index_int = True
        self.explainer_ = None

#pylint: disable=too-many-arguments, too-many-branches, too-many-statements
    @trace_sql
    def fit(self,
            data,
            train_length=None,
            forecast_length=None,
            key=None,
            endog=None,
            exog=None,
            warm_start=False):
        r"""
        Fit the model to the training dataset.

        Parameters
        ----------
        data : DataFrame
            Input data.

        train_length : int
            Length of training series inputted to the network.

            Note that if :code:`warm_start = True`, then this parameter is not valid.

        forecast_length : int
            Length of predictions.

            The constraint is that :code:`train_length + forecat_length <= data.count()``.

            Note that if :code:`warm_start = True`, then this parameter is not valid.

        key : str, optional

            The timestamp column of data. The type of key column should be INTEGER,
            TIMESTAMP, DATE or SECONDDATE.

            Defaults to the first column of data if the index column of data is not provided. Otherwise, defaults to the index column of data.

        endog : str, optional

            The endogenous variable, i.e. target time series. The type of endog column could be INTEGER, DOUBLE or DECIMAL(p,s).

            Defaults to the first non-key column.

        exog : str or a list of str, optional

            An optional array of exogenous variables. The type of exog column could be INTEGER, DOUBLE or DECIMAL(p,s).

            Defaults to None. Please set this parameter explicitly if you have exogenous variables.

        warm_start : bool, optional

            When set to True, reuse the ``model_`` of current object to continuously train the model.
            We provide a method called `load_model()` to load a pretrain model.
            Otherwise, just to train a new model.

            Defaults to False.

        Returns
        -------
        A fitted object of class "LTSF".
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)

        if warm_start is True:
            if self.model_ is None:
                msg = 'warm_start mode requires the model of previous fit and self.model_ should not be None!'
                logger.error(msg)
                raise ValueError(msg)

            if self.model_ and (train_length is not None or forecast_length is not None):
                warn_msg = "The value of train_length or forecast_length in the model will be used."
                logger.warning(warn_msg)

            self.train_length = None
            self.forecast_length = None

        else:
            self.train_length = self._arg('train_length', train_length, int, required=True)
            self.forecast_length = self._arg('forecast_length', forecast_length, int, required=True)

        cols = data.columns
        index = data.index
        key = self._arg('key', key, str)
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

        endog = self._arg('endog', endog, str)
        if endog is not None:
            if endog not in cols:
                msg = f"Please select endog from {cols}!"
                logger.error(msg)
                raise ValueError(msg)
        else:
            endog = cols[0]
        cols.remove(endog)

        if exog is not None:
            if isinstance(exog, str):
                exog = [exog]
            exog = self._arg('exog', exog, ListOfStrings)
            if set(exog).issubset(set(cols)) is False:
                msg = f"Please select exog from {cols}!"
                logger.error(msg)
                raise ValueError(msg)
        else:
            exog = []

        setattr(self, 'key', key)
        setattr(self, 'endog', endog)
        setattr(self, 'exog', exog)

        data_ = data[[key] + [endog] + exog]
        self.is_index_int = _is_index_int(data_, key)
        if not self.is_index_int:
            data_= _convert_index_from_timestamp_to_int(data_, key)

        conn = data.connection_context
        require_pal_usable(conn)
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        loss_tbl = f'#PAL_LTSF_LOSS_TBL_{self.id}_{unique_id}'
        model_tbl = f'#PAL_LTSF_MODEL_TBL_{self.id}_{unique_id}'
        outputs = [loss_tbl, model_tbl]
        param_rows = [
            ('NETWORK_TYPE',         self.network_type,                       None,   None),
            ('TRAIN_LENGTH',         self.train_length,                       None,   None),
            ('FORECAST_LENGTH',      self.forecast_length,                    None,   None),
            ('NUM_LEVELS',           self.num_levels,                         None,   None),
            ('KERNEL_SIZE',          self.kernel_size,                        None,   None),
            ('HIDDEN_EXPANSION',     self.hidden_expansion,                   None,   None),
            ('BATCH_SIZE',           self.batch_size,                         None,   None),
            ('NUM_EPOCHS',           self.num_epochs,                         None,   None),
            ('POSITION_ENCODING',    self.position_encoding,                  None,   None),
            ('ADJUST_LEARNING_RATE', self.adjust_learning_rate,               None,   None),
            ('RANDOM_SEED',          self.random_seed,                        None,   None),
            ('DROPOUT_PROB',         None,                       self.dropout_prob,   None),
            ('LEARNING_RATE',        None,                      self.learning_rate,   None)
            ]
        if not (check_pal_function_exist(conn, '%LTSF%', like=True) or \
        self._disable_hana_execution):
            msg = 'The version of your SAP HANA does not support LTSF!'
            logger.error(msg)
            raise ValueError(msg)
        try:
            if warm_start is not True:
                self._call_pal_auto(conn,
                                    'PAL_LTSF_TRAIN',
                                    data_,
                                    ParameterTable().with_data(param_rows),
                                    *outputs)
            else:
                self._call_pal_auto(conn,
                                    'PAL_LTSF_TRAIN_CONTINUE',
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
        #pylint: disable=attribute-defined-outside-init
        self.loss_ = conn.table(loss_tbl)
        self.model_ = conn.table(model_tbl)
        setattr(self, 'fit_data', data_)
        return self

    @trace_sql
    def predict(self, data, key=None, endog=None, allow_new_index=True, show_explainer=False, reference_dict=None):
        """
        Generates time series forecasts based on the fitted model. The number of rows of input predict data must be equal to the value of
        ``train_length`` during training and the length of predictions is equal to the value of ``forecast_length``.

        Parameters
        ----------

        data : DataFrame
            Input data for making forecasts.

            Formally, ``data`` should contain an ID column, the target time series and exogenous features specified in the training
            phase(i.e. ``endog`` and ``exog`` in `fit()` function), but no other columns.

            The length of ``data`` must be equal to the value of parameter ``train_length`` in `fit()`.

        key : str, optional

            Name of the ID column.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        endog : str, optional

            The endogenous variable, i.e. target time series. The type of endog column could be
            INTEGER, DOUBLE or DECIMAL(p,s).

            Defaults to the first non-key column of ``data``.

        allow_new_index : bool, optional

            Indicates whether a new index column is allowed in the result.
            - True: return the result with new integer or timestamp index column.
            - False: return the result with index column starting from 0.

            Defaults to True.

        show_explainer : bool, optional
            Indicates whether to invoke the LTSF with explanations function in the predict.

            If True, the contributions of each exog and its value and percentage are
            shown in a attribute called explainer\_ of a LTSF instance.

            Only valid when ``network_type`` is 'XLinear'.

            Defaults to False.

        reference_dict : dict, optional

            Define the reference value of an exogenous variable. The type of reference value need to be the same as the type of exogenous variable.

            Only valid when ``show_explainer`` is True.

            Defaults to the average value of exogenous variable in the training data if not provided.

        Returns
        -------

        DataFrame 1
            Forecasted values, structured as follows:

            - ID: type INTEGER, timestamp.
            - VALUE: type DOUBLE, forecast value.

        DataFrame 2 (optional)

            The explanations with decomposition of exogenous variables.
            Only valid if ``show_explainer`` is True and ``network_type`` is 'XLinear'.
        """
        if getattr(self, 'model_') is None:
            raise FitIncompleteError()

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

        endog = self._arg('endog', endog, str)
        if endog is not None:
            if endog not in cols:
                msg = f"Please select endog from {cols}!"
                logger.error(msg)
                raise ValueError(msg)
        else:
            endog = cols[0]
        cols.remove(endog)

        exog = cols

        data_ = data[[key] + [endog] + exog]

        self.is_index_int = _is_index_int(data_, key)
        if not self.is_index_int:
            data_= _convert_index_from_timestamp_to_int(data_, key)
        try:
            self.forecast_start, self.timedelta = _get_forecast_starttime_and_timedelta(data, key, self.is_index_int)
        except Exception as err:
            logger.warning(err)

        conn = data.connection_context
        param_rows = []
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        forecast_tbl = f"#PAL_LTSF_FORECAST_RESULT_TBL_{self.id}_{unique_id}"
        if not (check_pal_function_exist(conn, '%LTSF%', like=True) or \
        self._disable_hana_execution):
            msg = 'The version of your SAP HANA does not support LTSF!'
            logger.error(msg)
            raise ValueError(msg)
        # DECOMPOSE
        if show_explainer is True:
            if reference_dict:
                data_types = ["_", int, float, str]
                for e in reference_dict:
                    cur_param = ["__REF__" + e, None, None, None]
                    cur_param[data_types.index(type(reference_dict[e]))] = reference_dict[e]
                    param_rows.append(tuple(cur_param))
            decompose_tbl = f'#PAL_LTSF_DECOMPOSE_TBL_{self.id}_{unique_id}'
            output_tbl = [forecast_tbl, decompose_tbl]
            try:
                self._call_pal_auto(conn,
                                    'PAL_LTSF_DECOMPOSE',
                                    data_,
                                    self.model_,
                                    ParameterTable().with_data(param_rows),
                                    forecast_tbl,
                                    decompose_tbl)
            except dbapi.Error as db_err:
                logger.exception(str(db_err))
                try_drop(conn, output_tbl)
                raise
            except Exception as db_err:
                logger.exception(str(db_err))
                try_drop(conn, output_tbl)
                raise
            forecast = conn.table(forecast_tbl)
            decompose = conn.table(decompose_tbl)
            self.explainer_ = decompose

            if allow_new_index is not True:
                return forecast, decompose

            if self.is_index_int is True:
                forecast_int = conn.sql("""
                                        SELECT ({0}+({1} * {2})) AS {5},
                                        {4}
                                        FROM ({3})
                                        """.format(self.forecast_start,
                                                   quotename(forecast.columns[0]),
                                                   self.timedelta,
                                                   forecast.select_statement,
                                                   quotename(forecast.columns[1]),
                                                   quotename(key)))
                return forecast_int, decompose

            # ID column is TIMESTAMP
            forecast_timestamp = conn.sql("""
                                          SELECT ADD_SECONDS('{0}', {1} * {2}) AS {5},
                                          {4}
                                          FROM ({3})
                                          """.format(self.forecast_start,
                                                     quotename(forecast.columns[0]),
                                                     self.timedelta,
                                                     forecast.select_statement,
                                                     quotename(forecast.columns[1]),
                                                     quotename(key)))

            return forecast_timestamp, decompose

        # PREDICT
        output_tbl = [forecast_tbl]
        try:
            self._call_pal_auto(conn,
                                'PAL_LTSF_PREDICT',
                                data_,
                                self.model_,
                                ParameterTable().with_data(param_rows),
                                *output_tbl)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, output_tbl)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, output_tbl)
            raise
        forecast = conn.table(forecast_tbl)

        if allow_new_index is not True:
            return forecast

        if self.is_index_int is True:
            forecast_int = conn.sql("""
                                    SELECT ({0}+({1} * {2})) AS {5},
                                    {4}
                                    FROM ({3})
                                    """.format(self.forecast_start,
                                               quotename(forecast.columns[0]),
                                               self.timedelta,
                                               forecast.select_statement,
                                               quotename(forecast.columns[1]),
                                               quotename(key)))
            return forecast_int

        # ID column is TIMESTAMP
        forecast_timestamp = conn.sql("""
                                      SELECT ADD_SECONDS('{0}', {1} * {2}) AS {5},
                                      {4}
                                      FROM ({3})
                                      """.format(self.forecast_start,
                                                 quotename(forecast.columns[0]),
                                                 self.timedelta,
                                                 forecast.select_statement,
                                                 quotename(forecast.columns[1]),
                                                 quotename(key)))
        return forecast_timestamp

    def get_permutation_importance(self, data, model=None, key=None, endog=None, exog=None, repeat_time=None, random_state=None, thread_ratio=None,
                                   partition_ratio=None, regressor_top_k=None, accuracy_measure=None, ignore_zero=None):
        """
        Please see :ref:`permutation_imp_ts-label` for details.
        """
        if key is None:
            key = self.key
        if endog is None:
            endog = self.endog
        if exog is None:
            exog = self.exog
        if model is None:
            if getattr(self, 'model_') is None:
                raise FitIncompleteError()
            model = self.model_
        if isinstance(model, str):
            model = model.lower()
            if model == 'rdt':
                model = None
            else:
                msg = "Permutation importance only supports 'rdt' as model free method!"
                logger.error(msg)
                raise ValueError(msg)

        if model is None:
            permutation_data = data[[key] + [endog] + exog]
            real_data=None
        else:
            permutation_data = data[[key] + [endog] + exog]
            data = data.sort_values(by=[key])
            real_data = data[[key] + [endog]]
            real_data = real_data.tail(self.forecast_length)

        is_index_int = _is_index_int(permutation_data, key)
        if not is_index_int:
            if model:
                permutation_data = _convert_index_from_timestamp_to_int(permutation_data, key)
                real_data = _convert_index_from_timestamp_to_int(real_data, key)
            if model is None:
                permutation_data = _convert_index_from_timestamp_to_int(permutation_data, key)
        return super()._get_permutation_importance(permutation_data, real_data, model, repeat_time, random_state, thread_ratio,
                                                   partition_ratio, regressor_top_k, accuracy_measure, ignore_zero)
