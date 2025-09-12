"""
This module contains Python wrapper for PAL LSTM algorithm.

The following class are available:

    * :class:`LSTM`
"""

#pylint: disable=too-many-lines, line-too-long, too-many-locals, too-many-arguments, attribute-defined-outside-init
#pylint: disable=too-many-instance-attributes, too-few-public-methods, too-many-branches, too-many-statements
#pylint: disable=c-extension-no-member, super-with-arguments, no-member
import logging
import uuid
from hdbcli import dbapi
from hana_ml.ml_exceptions import FitIncompleteError
from hana_ml.visualizers.report_builder import Page
from hana_ml.visualizers.time_series_report import TimeSeriesExplainer
from ..pal_base import (
    PALBase,
    ParameterTable,
    pal_param_register,
    try_drop,
    require_pal_usable,
    ListOfStrings
)
from ..sqlgen import trace_sql

logger = logging.getLogger(__name__)#pylint: disable=invalid-name

class LSTM(PALBase):
    r"""
    Long short-term memory (LSTM) is one of the most famous modules of Recurrent Neural Networks(RNN). It can not only process single data point, but also the entire sequences of data, such as speech and stock prices.
    This function in PAL is used for time series prediction. It is first given a time series table, by which it will be trained. Then, it is enabled to predict the next value of the time series after a few time-steps as the user specifies.
    In PAL, both LSTM and its widely used variant Gated Recurrent Units (GRU) are implemented.

    Parameters
    ----------

    learning_rate : float, optional
        The learning rate for gradient descent.

        Defaults to 0.01.

    gru : {'gru', 'lstm'}, optional
        Choose GRU or LSTM.

        Defaults to 'lstm'.

    batch_size : int, optional
        The number of pieces of data for training in one iteration.

        Defaults to 32.

    time_dim : int, optional
        It specifies how many time steps in a sequence that will be trained by LSTM/GRU and then for time series prediction.

        The value of it must be smaller than the length of input time series minus 1.

        Defaults to 16.

    hidden_dim : int, optional
        The number of hidden neuron in LSTM/GRU unit.

        Defaults to 128.

    num_layers : int, optional
        The number of layers in LSTM/GRU unit.

        Defaults to 1.

    max_iter : int, optional
        The number of batches of data by which LSTM/GRU is trained.

        Defaults to 1000.

    interval : int, optional
        Outputs the average loss within every INTERVAL iterations.

        Defaults to 100.

    optimizer_type : {'SGD', 'RMSprop', 'Adam', 'Adagrad'}, optional
        Chooses the optimizer.

        Defaults to 'Adam'.

    stateful : bool, optional
        If the value is True, it enables stateful LSTM/GRU.

        Defaults to True.

    bidirectional : bool, optional
        If the value is True, it uses BiLSTM/BiGRU. Otherwise, it uses LSTM/GRU.

        Defaults to False.

    Attributes
    ----------
    loss_ : DateFrame
        Loss.

    model_ : DataFrame
        Model content.

    Examples
    --------
    Input DataFrame df:

    >>> df.head(3).collect()
       TIMESTAMP  SERIES
    0          0    20.7
    1          1    17.9
    2          2    18.8

    Create a LSTM model:

    >>> lstm = LSTM(gru='lstm',
                    bidirectional=False,
                    time_dim=16,
                    max_iter=1000,
                    learning_rate=0.01,
                    batch_size=32,
                    hidden_dim=128,
                    num_layers=1,
                    interval=1,
                    stateful=False,
                    optimizer_type='Adam')

    Perform fit():

    >>> lstm.fit(data=df)

    Perform predict():

    >>> res = lstm.predict(data=df_predict)

    Output:

    >>> res.head(3).collect()
       ID      VALUE                                        REASON_CODE
    0   0  11.673560  [{"attr":"T=0","pct":28.926935203430372,"val":...
    1   1  14.057195  [{"attr":"T=3","pct":24.729787064691735,"val":...
    2   2  15.119411  [{"attr":"T=2","pct":41.616207151605458,"val":...

    """
    gru_map = {'lstm' : 0, 'gru' : 1}
    optimizer_map = {'sgd' : 0, 'rmsprop' : 1, 'adam' : 2, 'adagrad' : 3}

    def __init__(self,
                 learning_rate=None,
                 gru=None,
                 batch_size=None,
                 time_dim=None,
                 hidden_dim=None,
                 num_layers=None,
                 max_iter=None,
                 interval=None,
                 optimizer_type=None,
                 stateful=None,
                 bidirectional=None):

        setattr(self, 'hanaml_parameters', pal_param_register())
        super(LSTM, self).__init__()
        self.learning_rate = self._arg('learning_rate', learning_rate, float)
        self.gru = self._arg('gru', gru, self.gru_map)
        self.batch_size = self._arg('batch_size', batch_size, int)
        self.time_dim = self._arg('time_dim', time_dim, int)
        self.hidden_dim = self._arg('hidden_dim', hidden_dim, int)
        self.num_layers = self._arg('num_layers', num_layers, int)
        self.max_iter = self._arg('max_iter', max_iter, int)
        self.interval = self._arg('interval', interval, int)
        self.optimizer_type = self._arg('optimizer_type', optimizer_type, self.optimizer_map)
        self.stateful = self._arg('stateful', stateful, (int, bool))
        self.bidirectional = self._arg('bidirectional', bidirectional, (int, bool))

    @trace_sql
    def fit(self, data, key=None, endog=None, exog=None):
        r"""
        Fit the model to the training dataset.

        Parameters
        ----------
        data : DataFrame
            Input data, structured as follows.

              - The 1st column : index/timestamp, type INTEGER.
              - The 2nd column : time-series value, type INTEGER, DOUBLE, or DECIMAL(p,s).
              - Other columns : external data(regressors), type INTEGER, DOUBLE,
                DECIMAL(p,s), VARCHAR or NVARCHAR.

        key : str, optional
            The timestamp column of data. The type of key column is INTEGER.

            Defaults to the first column of data if the index column of data is not provided.
            Otherwise, defaults to the index column of data.

        endog : str, optional
            The endogenous variable, i.e. time series. The type of endog column is INTEGER, DOUBLE, or DECIMAL(p, s).

            Defaults to the first non-key column of data if not provided.

        exog : str or a list of str, optional
            An optional array of exogenous variables. The type of exog column is INTEGER, DOUBLE, or DECIMAL(p, s).

            Defaults to None. Please set this parameter explicitly if you have exogenous variables.

        Returns
        -------
        A fitted object of class "LSTM".
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        setattr(self, 'key', key)
        setattr(self, 'endog', endog)
        setattr(self, 'exog', exog)
        # validate key, endog, exog
        cols = data.columns
        index = data.index
        key = self._arg('key', key, str)

        if index is not None:
            if key is None:
                if not isinstance(index, str):
                    key = cols[0]
                    warn_msg = "The index of data is not a single column and key is None, so the first column of data is used as key!"
                    logger.warning(warn_msg)
                else:
                    key = index
            else:
                if key != index:
                    warn_msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                    "and the designated index column '{}'.".format(index)
                    logger.warning(warn_msg)
        else:
            if key is None:
                key = cols[0]

        if key is not None and key not in cols:
            msg = ('Please select key from name of columns!')
            logger.error(msg)
            raise ValueError(msg)
        cols.remove(key)

        endog = self._arg('endog', endog, str)
        if endog is not None:
            if endog not in cols:
                msg = ('Please select endog from name of columns!')
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
                msg = ('Please select exog from name of columns!')
                logger.error(msg)
                raise ValueError(msg)
        else:
            exog = []

        data_ = data[[key] + [endog] + exog]

        conn = data.connection_context
        require_pal_usable(conn)
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        loss_tbl = '#PAL_LSTM_LOSS_TBL_{}_{}'.format(self.id, unique_id)
        model_tbl = '#PAL_LSTM_MODEL_TBL_{}_{}'.format(self.id, unique_id)
        outputs = [loss_tbl, model_tbl]
        param_rows = [
            ('LEARNING_RATE', None, self.learning_rate, None),
            ('GRU', self.gru, None, None),
            ('BATCH_SIZE', self.batch_size, None, None),
            ('TIME_DIM', self.time_dim, None, None),
            ('HIDDEN_DIM', self.hidden_dim, None, None),
            ('NUM_LAYERS', self.num_layers, None, None),
            ('MAX_ITER', self.max_iter, None, None),
            ('INTERVAL', self.interval, None, None),
            ('OPTIMIZER_TYPE', self.optimizer_type, None, None),
            ('STATEFUL', self.stateful, None, None),
            ('BIDIRECTIONAL', self.bidirectional, None, None)
            ]
        try:
            self._call_pal_auto(conn,
                                'PAL_LSTM_TRAIN',
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

        self.loss_ = conn.table(loss_tbl)
        self.model_ = conn.table(model_tbl)
        return self

    @trace_sql
    def predict(self, data, top_k_attributions=None):
        """
        Generates time series forecasts based on the fitted model.

        Parameters
        ----------

        data : DataFrame
            Data for prediction. Every row in the ``data`` should contain one piece of
            record data for prediction, i.e. it should be structured as follows:

                - First column: Record ID, type INTEGER.
                - Other columns : Time-series and external data values,
                  arranged in time order.

            The number of all columns but the first id column should be equal to
            the value of ``time_dim`` * (M-1), where M is the number of columns of
            the input data in the training phase.

        top_k_attributions : int, optional
            Specifies the number of features with highest attributions to output.

            Defaults to 10 or 0 depending on the SAP HANA version.

        Returns
        -------

        DataFrame
            The aggregated forecasted values.
            Forecasted values, structured as follows:

              - ID, type INTEGER, timestamp.
              - VALUE, type DOUBLE, forecast value.
              - REASON_CODE, type NCLOB, Sorted SHAP values for test data at each time step.
        """
        if getattr(self, 'model_') is None:
            raise FitIncompleteError()
        conn = data.connection_context
        param_rows = [
            ("TOP_K_ATTRIBUTIONS", top_k_attributions, None, None)]

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        result_tbl = "#PAL_LSTM_FORECAST_RESULT_TBL_{}_{}".format(self.id, unique_id)
        try:
            self._call_pal_auto(conn,
                                'PAL_LSTM_PREDICT',
                                data,
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
        result_df = conn.table(result_tbl)
        setattr(self, "forecast_result", result_df)
        return result_df

    def build_report(self):
        r"""
        Generate time series report.
        """
        from hana_ml.visualizers.time_series_report_template_helper import TimeSeriesTemplateReportHelper
        if self.key is None:
            self.key = self.training_data.columns[0]
        if self.endog is None:
            self.endog = self.training_data.columns[1]
        if len(self.training_data.columns) > 2:
            if self.exog is None:
                self.exog = self.training_data.columns
                self.exog.remove(self.key)
                self.exog.remove(self.endog)
        self.report = TimeSeriesTemplateReportHelper(self)
        pages = []
        page0 = Page("Forecast Result Analysis")
        tse = TimeSeriesExplainer(key=self.key, endog=self.endog, exog=self.exog)
        tse.add_line_to_comparison_item("Loss", data=self.loss_, x_name=self.loss_.columns[0], y_name=self.loss_.columns[1])
        page0.addItems(tse.get_comparison_item("Loss"))
        if hasattr(self, 'forecast_result'):
            if self.forecast_result:
                tse2 = TimeSeriesExplainer(key=self.key, endog=self.endog, exog=self.exog)
                tse2.add_line_to_comparison_item("Forecast Result", data=self.forecast_result, x_name=self.forecast_result.columns[0], y_name=self.forecast_result.columns[1])
                page0.addItems(tse2.get_comparison_item("Forecast"))
        pages.append(page0)
        self.report.add_pages(pages)
        self.report.build_report()

    def generate_html_report(self, filename=None):
        """
        Display function.
        """
        self.report.generate_html_report(filename)

    def generate_notebook_iframe_report(self):
        """
        Display function.
        """
        self.report.generate_notebook_iframe_report()
