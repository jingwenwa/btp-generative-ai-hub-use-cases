"""This module contains PAL wrapper for discriminant analysis algorithm.
The following class is available:

    * :class:`LinearDiscriminantAnalysis`
"""
#pylint: disable=too-many-locals, line-too-long, too-many-arguments, too-many-lines, relative-beyond-top-level
#pylint:disable=invalid-name, too-many-instance-attributes
#pylint: disable=consider-using-f-string
import logging
import uuid
from hdbcli import dbapi
from hana_ml.ml_exceptions import FitIncompleteError
from hana_ml.ml_base import try_drop
from .pal_base import (
    PALBase,
    ParameterTable,
    ListOfStrings,
    pal_param_register,
    require_pal_usable
    )
logger = logging.getLogger(__name__)

class LinearDiscriminantAnalysis(PALBase):
    r"""
    Linear Discriminant Analysis is a supervised learning technique used for classification problems.
    It is particularly useful when the classes are well-separated and the dataset features follow a Gaussian distribution.
    LDA works by projecting high-dimensional data onto a lower-dimensional space where class separation is maximized.
    The goal is to find a linear combination of features that best separates the classes.
    This makes LDA a dimensionality reduction technique as well, similar to Principal Component Analysis (PCA), but with the distinction that LDA takes class labels into account.

    Parameters
    ----------

    regularization_type : {'mixing', 'diag', 'pseudo'}, optional
        The strategy for handling ill-conditioning or rank-deficiency
        of the empirical covariance matrix.

        Defaults to 'mixing'.
    regularization_amount : float, optional
        The convex mixing weight assigned to the diagonal matrix
        obtained from diagonal of the empirical covariance matrix.
        Valid range for this parameter is [0,1].
        Valid only when ``regularization_type`` is 'mixing'.

        Defaults to the smallest number in [0,1] that makes the
        regularized empirical covariance matrix invertible.
    projection : bool, optional
        Whether or not to compute the projection model.

        Defaults to True.

    Attributes
    ----------
    basic_info_ : DataFrame
        Basic information of the training data for linear discriminant analysis.
    priors_ : DataFrame
        The empirical priors for each class in the training data.
    coef_ : DataFrame
        Coefficients (inclusive of intercepts) of each class' linear score function
        for the training data.
    proj_info : DataFrame
        Projection related info, such as standard deviations of the discriminants,
        variance proportion to the total variance explained by each discriminant, etc.
    proj_model : DataFrame
         The projection matrix and overall means for features.

    Examples
    --------
    >>> lda = LinearDiscriminantAnalysis(regularization_type='mixing', projection=True)

    Perform fit():

    >>> lda.fit(data=df, features=['X1', 'X2'], label='CLASS')
    >>> lda.coef_.collect()
    >>> lda.proj_model_.collect()

    Perform predict():

    >>> res = lda.predict(data=df_pred, key='ID',
                          features=['X1', 'X2'], verbose=False)
    >>> res.collect()

    Perform project():

    >>> res_proj = lda.project(data=df_proj, key='ID',
                               features=['X1','X2'], proj_dim=2)
    >>> res_proj.collect()

    """
    regularization_map = {'diag':1, 'pseudo':2, 'mixing':0}
    def __init__(self,
                 regularization_type=None,
                 regularization_amount=None,
                 projection=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(LinearDiscriminantAnalysis, self).__init__()
        self.regularization_type = self._arg('regularization_type',
                                             regularization_type,
                                             self.regularization_map)
        self.regularization_amount = self._arg('regularization_amount',
                                               regularization_amount,
                                               float)
        self.projection = self._arg('projection', projection, bool)
        self.basic_info_ = None
        self.priors_ = None
        self.coef_ = None
        self.proj_info_ = None
        self.proj_model_ = None
        self.model_ = None

    def fit(self, data, key=None, features=None, label=None):
        r"""
        Fit the model to the given dataset.

        Parameters
        ----------

        data : DataFrame
            Training data.

        key : str, optional
            Name of the ID column.
            If not provided, then:

                - if ``data`` is indexed by a single column, then ``key`` defaults to that index column
                - otherwise, it is assumed that ``data`` contains no ID column

        features : a list of str, optional
            Names of the feature columns.

            If not provided, its defaults to all non-ID, non-label columns.
        label : str, optional
            Name of the class label.

            if not provided, it defaults to the last non-ID column.

        Returns
        -------
            A fitted object of class "LinearDiscriminantAnalysis".
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, 'training_data', data)
        conn = data.connection_context
        require_pal_usable(conn)
        key = self._arg('key', key, str)
        index = data.index
        if isinstance(index, str):
            if key is not None and index != key:
                msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(msg)
        key = index if key is None else key
        features = self._arg('features', features, ListOfStrings)
        label = self._arg('label', label, str)
        cols = data.columns
        if key is not None:
            cols.remove(key)
        if label is None:
            label = cols[-1]
        cols.remove(label)
        if features is None:
            features = cols
        data_ = data[features + [label]]
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        tables = ['BASIC_INFO', 'PRIORS', 'COEF', 'PROJ_INFO', 'PROJ_MODEL']
        tables = ['#PAL_LINEAR_DISCRIMINANT_ANALYSIS_{}_TBL_{}_{}'.format(name, self.id, unique_id)
                  for name in tables]
        basic_info_tbl, priors_tbl, coef_tbl, proj_info_tbl, proj_model_tbl = tables
        param_rows = [
            ('REGULARIZATION_TYPE', self.regularization_type, None, None),
            ('REGULARIZATION_AMOUNT', self. regularization_amount, None, None),
            ('DO_PROJECTION', self.projection, None, None)
        ]
        try:
            self._call_pal_auto(conn,
                                'PAL_LINEAR_DISCRIMINANT_ANALYSIS',
                                data_,
                                ParameterTable().with_data(param_rows),
                                basic_info_tbl,
                                priors_tbl,
                                coef_tbl,
                                proj_info_tbl,
                                proj_model_tbl)
        except dbapi.Error as db_err:
            #msg = ("HANA error while attempting to fit linear discriminat analysis model.")
            logger.exception(str(db_err))
            try_drop(conn, tables)
            raise
        except Exception as db_err:
            #msg = ("HANA error while attempting to fit linear discriminat analysis model.")
            logger.exception(str(db_err))
            try_drop(conn, tables)
            raise
        self.basic_info_ = conn.table(basic_info_tbl)
        self.priors_ = conn.table(priors_tbl)
        self.coef_ = conn.table(coef_tbl)
        self.model_ = [self.priors_, self.coef_]
        if self.projection is not False:
            self.proj_info_ = conn.table(proj_info_tbl)
            self.proj_model_ = conn.table(proj_model_tbl)
        return self

    def predict(self, data, key=None, features=None, verbose=None, verbose_top_n=None):
        r"""
        Predict class labels using fitted linear discriminators.

        Parameters
        ----------

        data : DataFrame
            Data for predicting the class labels.

        key : str, optional
            Name of the ID column.
            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.
        features : a list of str, optional
            Name of the feature columns.

            If not provided, defaults to all non-ID columns.
        verbose : bool, optional
            Whether or not outputs scores of all classes.
            If False, only score of the predicted class will be outputted.

            Defaults to False.
        verbose_top_n : bool, optional
            Specifies the number of top n classes to present after sorting with confidences.
            It cannot exceed the number of classes in label of the training data, and it can be 0,
            which means to output the confidences of `all` classes.
            Effective only when ``verbose`` is set as True.

            Defaults to 0.

        Returns
        -------

        DataFrame
            Predicted class labels and the corresponding scores.

        """
        conn = data.connection_context
        require_pal_usable(conn)
        if getattr(self, 'model_') is None:
            raise FitIncompleteError()
        index = data.index
        key = self._arg('key', key, str, required=not isinstance(index, str))
        if isinstance(index, str):
            if key is not None and index != key:
                msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(msg)
        key = index if key is None else key
        features = self._arg('features', features, ListOfStrings)
        verbose = self._arg('verbose', verbose, bool)
        verbose_top_n = self._arg('verbose_top_n', verbose_top_n, int)

        cols = data.columns
        cols.remove(key)
        if features is None:
            features = cols
        data_ = data[[key] + features]
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        result_tbl = '#PAL_LINEAR_DISCRIMINANT_ANALYSIS_RESULT_TBL_{}_{}'.format(self.id, unique_id)
        param_rows = [('VERBOSE_OUTPUT', verbose, None, None),
                      ('VERBOSE_TOP_N', verbose_top_n, None, None)]
        try:
            self._call_pal_auto(conn,
                                'PAL_LINEAR_DISCRIMINANT_ANALYSIS_CLASSIFY',
                                data_,
                                self.model_[0],
                                self.model_[1],
                                ParameterTable().with_data(param_rows),
                                result_tbl)
        except dbapi.Error as db_err:
            #msg = ("HANA error during linear discriminant analysis prediction.")
            logger.exception(str(db_err))
            try_drop(conn, result_tbl)
            raise
        except Exception as db_err:
            #msg = ("HANA error during linear discriminant analysis prediction.")
            logger.exception(str(db_err))
            try_drop(conn, result_tbl)
            raise
        return conn.table(result_tbl)

    def project(self, data, key=None, features=None, proj_dim=None):
        r"""
        Project `data` into lower dimensional spaces using the fitted LDA projection model.

        Parameters
        ----------

        data : DataFrame
            Data for linear discriminant projection.
        key : str, optional
            Name of the ID column.
            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.
        features : a list of str, optional
            Name of the feature columns.

            If not provided, defaults to all non-ID columns.
        proj_dim : int, optional
            Dimension of the projected space, equivalent to the number
            of discriminant used for projection.

            Defaults to the number of obtained discriminants.

        Returns
        -------

        DataFrame

            Projected data, structured as follows:

            - 1st column: ID, with the same name and data type as ``data`` for projection.
            - other columns with name DISCRIMINANT_i, where i iterates from 1 to the number
              of elements in ``features``, data type DOUBLE.

        """
        conn = data.connection_context
        require_pal_usable(conn)
        if getattr(self, 'proj_model_', None) is not None:
            proj_model = self.proj_model_
        else:
            raise FitIncompleteError("Projection model has not been initialized. Set `projection` to True and perform a fit() method first.")
        index = data.index
        key = self._arg('key', key, str, required=not isinstance(index, str))
        if isinstance(index, str):
            if key is not None and index != key:
                msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(msg)
        key = index if key is None else key
        features = self._arg('features', features, ListOfStrings)
        proj_dim = self._arg('proj_dim', proj_dim, int)

        cols = data.columns
        cols.remove(key)
        if features is None:
            features = cols
        data_ = data[[key] + features]

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()

        projected_tbl = ('#PAL_LINEAR_DISCRIMINANT_'+
                         'ANALYSIS_PROJECTION_TBL_{}_{}'.format(self.id, unique_id))
        param_rows = [
            ('DISCRIMINANT_NUMBER', proj_dim, None, None)
        ]

        try:
            self._call_pal_auto(conn,
                                'PAL_LINEAR_DISCRIMINANT_ANALYSIS_PROJECT',
                                data_,
                                proj_model,
                                ParameterTable().with_data(param_rows),
                                projected_tbl)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, projected_tbl)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, projected_tbl)
            raise
        return conn.table(projected_tbl)
