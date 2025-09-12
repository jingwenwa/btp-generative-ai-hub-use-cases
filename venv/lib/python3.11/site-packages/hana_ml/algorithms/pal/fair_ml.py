"""
This module contains PAL wrapper for Fair Machine Learning algorithms.

The following classes are available:
    * :class:`FairMLClassification`
    * :class:`FairMLRegression`
"""
#pylint: disable=too-many-lines, line-too-long, invalid-name, relative-beyond-top-level
#pylint: disable=consider-iterating-dictionary, too-many-branches
#pylint: disable=too-many-instance-attributes, too-many-arguments, too-many-locals
import logging
import uuid
import numpy as np
from hdbcli import dbapi
from hana_ml.ml_exceptions import FitIncompleteError
from hana_ml.dataframe import DataFrame
from .trees import HybridGradientBoostingClassifier, HybridGradientBoostingRegressor
from .pal_base import (
    PALBase,
    ParameterTable,
    ListOfStrings,
    pal_param_register,
    try_drop
)
logger = logging.getLogger(__name__) #pylint: disable=invalid-name

def _extract_param_rows_from_execute_statement(execute_statement):
    statement_split = execute_statement.split('\n')
    param_indc = ['] := ' in stm for stm in statement_split]#param indicator
    param_list = np.array(statement_split)[param_indc]
    param_list = [pm.split(' := ')[1][0:-1] for pm in param_list]
    num_params = int(len(param_list) / 4)
    param_rows = []
    for pidx in range(num_params):
        start_idx = int(pidx * 4)
        param_rows.append((param_list[start_idx + 0][2:-1],
                           None if param_list[start_idx + 1] == 'NULL' else int(param_list[start_idx + 1]),
                           None if param_list[start_idx + 2] == 'NULL' else float(param_list[start_idx + 2]),
                           None if param_list[start_idx + 3] == 'NULL' else param_list[start_idx + 3][2:-1]))
    return param_rows

class _FairMLBase(PALBase):
    available_models = {"HGBT": "HGBT", "hgbt": "HGBT"}
    available_constraints = {"demographic_parity": "demographic_parity",
                             "equalized_odds": "equalized_odds",
                             "true_positive_rate_parity": "true_positive_rate_parity",
                             "false_positive_rate_parity": "false_positive_rate_parity",
                             "error_rate_parity": "error_rate_parity",
                             "bounded_group_loss": "bounded_group_loss"}
    available_loss_func = {"error_rate": "error_rate",
                           "mse": "mse",
                           "mae": "mae"}
    available_loss_func_for_constraint = {"mse": "mse", "mae": "mae"}
    def  __init__(self,
                  #fair_sensitive_variable,
                  fair_submodel=None,
                  fair_constraint=None,
                  fair_loss_func=None,
                  fair_loss_func_for_constraint=None,
                  fair_num_max_iter=None,
                  fair_num_min_iter=None,
                  fair_learning_rate=None,
                  fair_norm_bound=None,
                  fair_ratio=None,
                  fair_relax=None,
                  fair_bound=None,
                  fair_threshold=None,
                  fair_exclude_sensitive_variable=None):
        if not hasattr(self, 'hanaml_parameters'):
            setattr(self, 'hanaml_parameters', pal_param_register())
        super(_FairMLBase, self).__init__()
        self.fair_submodel = self._arg("fair_submodel", fair_submodel, self.available_models)
        self.fair_constraint = self._arg("fair_constraint", fair_constraint, self.available_constraints)
        self.fair_loss_func = self._arg("fair_loss_func", fair_loss_func, self.available_loss_func)
        self.fair_loss_func_for_constraint = self._arg("fair_loss_func_for_constraint", fair_loss_func_for_constraint, self.available_loss_func_for_constraint)
        self.fair_num_max_iter = self._arg("fair_num_max_iter", fair_num_max_iter, int)
        self.fair_num_min_iter = self._arg("fair_num_min_iter", fair_num_min_iter, int)
        self.fair_learning_rate = self._arg("fair_learning_rate", fair_learning_rate, float)
        self.fair_norm_bound = self._arg("fair_norm_bound", fair_norm_bound, float)
        self.fair_ratio = self._arg("fair_ratio", fair_ratio, float)
        self.fair_relax = self._arg("fair_relax", fair_relax, float)
        self.fair_bound = self._arg("fair_bound", fair_bound, float)
        self.fair_threshold = self._arg("fair_threshold", fair_threshold, float)
        self.fair_exclude_sensitive_variable = self._arg('fair_exclude_sensitive_variable',
                                                         fair_exclude_sensitive_variable, bool)
        self.fair_sensitive_variable = None
        self.submodel_ = None

    def _fit(self,
             data,
             key=None,
             features=None,
             label=None,
             fair_sensitive_variable=None,
             categorical_variable=None,
             fair_positive_label=None,
             thread_ratio=None
             ):
        self.fair_sensitive_variable = self._arg('fair_sensitive_variable',
                                                 fair_sensitive_variable, (list, str))
        if isinstance(self.fair_sensitive_variable, str):
            self.fair_sensitive_variable = [self.fair_sensitive_variable]
        if isinstance(categorical_variable, str):
            categorical_variable = [categorical_variable]
        categorical_variable = self._arg('categorical_variable',
                                         categorical_variable,
                                         ListOfStrings)
        thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        if self.fair_constraint in ('true_positive_rate_parity', 'false_positive_rate_parity'):
            if fair_positive_label is None:
                raise ValueError("Mandatory if fair_constraint is set to true_positive_rate_parity or false_positive_rate_parity.")
        #pylint:disable=attribute-defined-outside-init
        cols = data.columns
        #has_id input is process here
        if key is not None:
            id_col = [key]
            cols.remove(key)
        else:
            id_col = []
        if label is None:
            label = cols[-1]
        cols.remove(label)
        #retrieve data type for  the label column
        #crucial for distinguish between regression and classification problems
        #and related error handling
        if features is None:
            features = cols
        data_ = data[id_col + features + [label]]
        outputs = ['MODEL', 'STATS']
        outputs = ['#PAL_FAIRML_{}_TBL_{}'.format(name, self.gen_id)
                   for name in outputs]
        model_tbl, stats_tbl = outputs
        conn = data.connection_context
        param_rows = [('FAIR_POSITIVE_LABEL', None, None, fair_positive_label),
                      ('HAS_ID', key is not None, None, None),
                      ('DEPENDENT_VARIABLE', None, None, label),
                      ('THREAD_RATIO', None, thread_ratio, None),
                      ('FAIR_SUBMODEL', None, None, self.fair_submodel),
                      ('FAIR_CONSTRAINT', None, None, self.fair_constraint),
                      ('FAIR_LOSS_FUNC', None, None, self.fair_loss_func),
                      ('FAIR_LOSS_FUNC_FOR_CONSTRAINT', None, None, self.fair_loss_func_for_constraint),
                      ('FAIR_NUM_MAX_ITER', self.fair_num_max_iter, None, None),
                      ('FAIR_NUM_MIN_ITER', self.fair_num_min_iter, None, None),
                      ('FAIR_LEARNING_RATE', None, self.fair_learning_rate, None),
                      ('FAIR_NORM_BOUND', None, self.fair_norm_bound, None),
                      ('FAIR_RATIO', None, self.fair_ratio, None),
                      ('FAIR_RELAX', None, self.fair_relax, None),
                      ('FAIR_BOUND', None, self.fair_bound, None),
                      ('FAIR_THRESHOLD', None, self.fair_threshold, None),
                      ('FAIR_EXCLUDE_SENSITIVE_VARIABLE', self.fair_exclude_sensitive_variable,
                       None, None)]
        if categorical_variable is not None:
            param_rows.extend(('CATEGORICAL_VARIABLE', None, None, variable)
                              for variable in categorical_variable)
        if self.fair_sensitive_variable is not None:
            param_rows.extend(('FAIR_SENSITIVE_VARIABLE', None, None, variable)
                              for variable in self.fair_sensitive_variable)
        if self.submodel_ is not None:
            self.submodel_.disable_hana_execution()
            unique_id = str(uuid.uuid1()).replace('-', '_').upper()
            dummy_df = DataFrame(conn, f'FAIR_SUBMODEL_DUMMY_{unique_id}')
            dummy_label = "DUMMY_{}".format(str(uuid.uuid1()).replace('-', '_').upper())
            dummy_df._columns = [key, 'X', dummy_label]#pylint:disable=protected-access
            dummy_df._dtypes = [(key, 'INTEGER', 10),#pylint:disable=protected-access
                                ('X', 'DOUBLE', 15),
                                (dummy_label, 'INTEGER', 15)]

            self.submodel_.fit(data=dummy_df, key=key, label=dummy_label)
            submodel_param_rows = _extract_param_rows_from_execute_statement(self.submodel_.execute_statement)
            if ('CATEGORICAL_VARIABLE', None, None, dummy_label) in submodel_param_rows:
                submodel_param_rows.remove(('CATEGORICAL_VARIABLE', None, None, dummy_label))
            param_rows.extend(submodel_param_rows)
            param_rows = list(set(param_rows))
        try:
            self._call_pal_auto(conn,
                                'PAL_FAIRML',
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
        self.model_ = conn.table(model_tbl)
        self.stats_ = conn.table(stats_tbl)
        self.statistics_ = self.stats_
        return self

    def predict(self,
                data,
                key=None,
                features=None,
                thread_ratio=None,
                model=None
                ):
        r"""
        Predict function for Fair ML.

        Parameters
        ----------
        data :  DataFrame
            Data to be predicted.
        key : str, optional
            Name of the ID column.
            Mandatory if ``data`` is not indexed, or is indexed by multiple columns.

            Defaults to the index of ``data`` if ``data`` is indexed by a single column.
        features : a list of str, optional
            Names of the feature columns.

            If ``features`` is not provided, it defaults to all non-ID columns.
        thread_ratio : float, optional
            Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
            Values outside the range will be ignored and this function heuristically determines the number of threads to use.

            Defaults to 1.0.
        model : DataFrame, optional
            The model to be used for prediction.

            Defaults to the fitted model (model\_).

        Returns
        -------
        DataFrame
            Predicted result.
        """
        if not hasattr(self, 'model_') or getattr(self, 'model_') is None:
            if model is None:
                raise FitIncompleteError()
        if model is None:
            model = self.model_
        conn = data.connection_context
        cols = data.columns
        #has_id input is process here
        if key is not None:
            id_col = [key]
            cols.remove(key)
        else:
            id_col = []
        #retrieve data type for  the label column
        #crucial for distinguish between regression and classification problems
        #and related error handling
        if features is None:
            features = cols
        data_ = data[id_col + features]

        outputs = ['RESULT']
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        outputs = ['#PAL_FARIMLPREDICT_{}_TBL_{}_{}'.format(name, self.gen_id, unique_id) for name in outputs]
        result_tbl = outputs[0]
        param_rows = [('THREAD_RATIO', None, thread_ratio, None)]
        try:
            self._call_pal_auto(conn,
                                'PAL_FAIRML_PREDICT',
                                data_,
                                model,
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
        return conn.table(result_tbl)

class FairMLClassification(_FairMLBase):
    """
    FairMLClassification aims at mitigating unfairness of prediction model due to some possible "bias" within dataset regarding features such as sex, race, age etc. It is a framework that can utilize other machine learning models or technologies which makes it quite flexible.

    Parameters
    ----------
    fair_submodel : {'HGBT'}, optional
        Specifies submodel type.

        Defaults to 'HGBT'.
    fair_constraint : {'demographic_parity', 'equalized_odds', 'true_positive_rate_parity', 'false_positive_rate_parity', 'error_rate_parity'}, optional
        Specifies constraint.

        Defaults to 'demographic_parity'.
    fair_loss_func : {'error_rate'}, optional
        Specifies loss function.

        Defaults to 'error_rate'.
    fair_num_max_iter : int, optional
        Specifies the maximum number of iteration performed. Must be greater than or equal to ``fair_num_min_iter``.

        Defaults to 50.
    fair_num_min_iter : int, optional
        Specifies the minimum number of iteration performed. Must be less than or equal to ``fair_num_max_iter``.

        Defaults to 5.
    fair_learning_rate : float, optional
        Specifies learning rate.

        Defaults to 0.02.
    fair_norm_bound : float, optional
        Specifies bound of Lagrange multiplier. Must be positive.

        Defaults to 100.
    fair_ratio : float, optional
        Specifies ratio of error allowed in constraint. Must in range (0, 1].

        Defaults to 1.0.
    fair_relax : float, optional
        Specifies relaxation of constraint. Must be non-negative.

        Defaults to 0.01.
    fair_threshold : float, optional
        Specifies a threshold indicating the timing of stopping algorithm iterations, the greater value the more accuracy but more time consuming, must be positive. If zero is given, then it is decided heuristically.

        Defaults to 0.0.
    fair_exclude_sensitive_variable : bool, optional
        Specifies whether or not to exclude sensitive variables when training the fairness-aware model.

        Defaults to True, i.e. by default the sensitive variable(s) is excluded in the trained model.
    **kwargs: keyword arguments
        Parameters for initializing the submodel used for fair classification.
        In our case these should be the initialization parameters for HybridGradientBoostingClassifiers.

        Please see :class:`~hana_ml.algorithms.pal.trees.HybridGradientBoostingClassifier` for more details.

    Attributes
    ----------
    model_ : DataFrame
        Model content.

    stats_ : DataFrame
        Statistics.

    Examples
    --------
    >>> fair_ml = FairMLClassification(fair_submodel='HGBT', fair_constraint='demographic_parity')
    >>> fair_ml.fit(data=df, fair_sensitive_variable='gender')
    >>> res = fair_ml.predict(data=df_predict)
    """
    def __init__(self,
                 #fair_sensitive_variable,
                 fair_submodel='HGBT',
                 fair_constraint='demographic_parity',
                 fair_loss_func='error_rate',
                 fair_num_max_iter=None,
                 fair_num_min_iter=None,
                 fair_learning_rate=None,
                 fair_norm_bound=None,
                 fair_ratio=None,
                 fair_relax=None,
                 fair_threshold=None,
                 fair_exclude_sensitive_variable=None,
                 **kwargs):
        #pylint:disable=too-many-locals
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(FairMLClassification, self).__init__(#fair_sensitive_variable=fair_sensitive_variable,
                                               fair_submodel=fair_submodel,
                                               fair_constraint=fair_constraint,
                                               fair_loss_func=fair_loss_func,
                                               fair_num_max_iter=fair_num_max_iter,
                                               fair_num_min_iter=fair_num_min_iter,
                                               fair_learning_rate=fair_learning_rate,
                                               fair_norm_bound=fair_norm_bound,
                                               fair_ratio=fair_ratio,
                                               fair_relax=fair_relax,
                                               fair_threshold=fair_threshold,
                                               fair_exclude_sensitive_variable=fair_exclude_sensitive_variable)
        if len(kwargs) != 0:
            self.submodel_ = HybridGradientBoostingClassifier(**kwargs)
        self.op_name = 'FairMLClassification'

    def fit(self,
            data,
            key=None,
            features=None,
            label=None,
            fair_sensitive_variable=None,
            categorical_variable=None,
            fair_positive_label=None,
            thread_ratio=None):
        r"""
        Fit the model to the training dataset.

        Parameters
        ----------
        data : DataFrame
            The input data for training.
        key : str, optional
            Specifies the ID column.

            If ``data`` is indexed by a single column, then ``key`` defaults
            to that index column; otherwise ``key`` must be specified(i.e. is mandatory).
        features : a list of str, optional
            Names of the feature columns.

            If ``features`` is not provided, it defaults to all non-ID,
            non-label columns.
        label : str, optional
            Name of the dependent variable.
            If ``label`` is not provided, it defaults to the last non-ID column.
        fair_sensitive_variable : str or list of str
            Specifies names of sensitive variable. Can have multiple entities.

            Defautls to None.
        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.
        fair_positive_label : str, optional
            Specifies label that stands for positive case. Mandatory if ``fair_constraint`` is set to 'true_positive_rate_parity' or 'false_positive_rate_parity'.

            Defautls to None.
        thread_ratio : float, optional
            Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
            Values outside the range will be ignored and this function heuristically determines the number of threads to use.

            Defaults to 1.0.

        Returns
        -------
        A fitted object of class "FairMLClassification".
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, 'training_data', data)
        self._fit(data=data, key=key, features=features, label=label,
                  fair_sensitive_variable=fair_sensitive_variable,
                  categorical_variable=categorical_variable,
                  fair_positive_label=fair_positive_label,
                  thread_ratio=thread_ratio)
        return self

class FairMLRegression(_FairMLBase):
    """
    FairMLRegression aims at mitigating unfairness of prediction model due to some possible "bias" within dataset regarding features such as sex, race, age etc. It is a framework that can utilize other machine learning models or technologies which makes it quite flexible.

    Parameters
    ----------
    fair_bound : int
        Specifies upper bound of constraint. Must be positive.
    fair_submodel : {'HGBT'}, optional
        Specifies submodel type.

        Defaults to 'HGBT'.
    fair_constraint : {'bounded_group_loss'}, optional
        Specifies constraint.

        Defaults to 'bounded_group_loss'.
    fair_loss_func : {'mse', 'mae'}, optional
        Specifies loss function.

        Defaults to 'mse'.
    fair_loss_func_for_constraint: {'mse', 'mae'}, optional
        Specifies loss function that is part of constraint configuration.

        Defaults to 'mse'.
    fair_num_max_iter : int, optional
        Specifies the maximum number of iteration performed. Must be greater than or equal to ``fair_num_min_iter``.

        Defaults to 50.
    fair_num_min_iter : int, optional
        Specifies the minimum number of iteration performed. Must be less than or equal to ``fair_num_max_iter``.

        Defaults to 5.
    fair_learning_rate : float, optional
        Specifies learning rate.

        Defaults to 0.02.
    fair_norm_bound : float, optional
        Specifies bound of Lagrange multiplier. Must be positive.

        Defaults to 100.
    fair_threshold : float, optional
        Specifies a threshold indicating the timing of stopping algorithm iterations, the greater value the more accuracy but more time consuming, must be positive. If zero is given, then it is decided heuristically.

        Defaults to 0.0.
    fair_exclude_sensitive_variable : bool, optional
        Specifies whether or not to exclude sensitive variables when training the fairness-aware model.

        Defaults to True, i.e. by default the sensitive variable(s) is excluded in the trained model.
    **kwargs: keyword arguments
        Parameters for initializing the submodel used for fair regression.
        In our case these should be initialization parameters for HybridGradientBoostingRegressor.

        See :class:`~hana_ml.algorithms.pal.trees.HybridGradientBoostingRegressor` for more details.

    Attributes
    ----------
    model_ : DataFrame
        Model content.
    stats_ : DataFrame
        Statistics.

    Examples
    --------
    >>> fair_ml = FairMLRegression(fair_bound=0.5)
    >>> fair_ml.fit(data=df, fair_sensitive_variable='gender')
    >>> res = fair_ml.predict(data=df_predict)
    """
    def  __init__(self,
                  #fair_sensitive_variable,
                  fair_bound,
                  fair_submodel='HGBT',
                  fair_constraint='bounded_group_loss',
                  fair_loss_func='mse',
                  fair_loss_func_for_constraint='mse',
                  fair_num_max_iter=None,
                  fair_num_min_iter=None,
                  fair_learning_rate=None,
                  fair_norm_bound=None,
                  fair_threshold=None,
                  fair_exclude_sensitive_variable=None,
                  **kwargs):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(FairMLRegression, self).__init__(#fair_sensitive_variable=fair_sensitive_variable,
                                               fair_bound=fair_bound,
                                               fair_submodel=fair_submodel,
                                               fair_constraint=fair_constraint,
                                               fair_loss_func=fair_loss_func,
                                               fair_loss_func_for_constraint=fair_loss_func_for_constraint,
                                               fair_num_max_iter=fair_num_max_iter,
                                               fair_num_min_iter=fair_num_min_iter,
                                               fair_learning_rate=fair_learning_rate,
                                               fair_norm_bound=fair_norm_bound,
                                               fair_threshold=fair_threshold,
                                               fair_exclude_sensitive_variable=fair_exclude_sensitive_variable)
        if len(kwargs) != 0:
            self.submodel_ = HybridGradientBoostingRegressor(**kwargs)
        self.op_name = 'FairMLRegression'

    def fit(self,
            data,
            key=None,
            features=None,
            label=None,
            fair_sensitive_variable=None,
            categorical_variable=None,
            thread_ratio=None):
        r"""
        Fit the model to the training dataset.

        Parameters
        ----------
        data : DataFrame
            The input data for training.
        key : str, optional
            Specifies the ID column.

            If ``data`` is indexed by a single column, then ``key`` defaults to that index column; otherwise ``key`` must be specified(i.e. is mandatory).
        features : a list of str, optional
            Names of the feature columns.

            If ``features`` is not provided, it defaults to all non-ID, non-label columns.
        label : str, optional
            Name of the dependent variable.
            If ``label`` is not provided, it defaults to the last non-ID column.
        fair_sensitive_variable : str or list of str
            Specifies names of sensitive variable. Can have multiple entities.
        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.
        thread_ratio : float, optional
            Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
            Values outside the range will be ignored and this function heuristically determines the number of threads to use.

            Defaults to 1.0.

        Returns
        -------
        A fitted object of class "FairMLRegression".
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, 'training_data', data)
        self._fit(data=data, key=key, features=features, label=label,
                  fair_sensitive_variable=fair_sensitive_variable,
                  categorical_variable=categorical_variable,
                  thread_ratio=thread_ratio)
        return self
