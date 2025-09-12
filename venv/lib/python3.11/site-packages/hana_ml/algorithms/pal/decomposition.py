"""
This module contains Python wrappers for PAL decomposition algorithms.

The following classes are available:

    * :class:`PCA`
    * :class:`CATPCA`
    * :class:`LatentDirichletAllocation`
    * :class:`VectorPCA`
"""

#pylint: disable=too-many-locals, line-too-long, too-many-arguments, too-many-lines, too-many-instance-attributes
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
logger = logging.getLogger(__name__)#pylint: disable=invalid-name

class _PCABase(PALBase):
    r"""
    Base class for PCA and CATPCA.
    """
    def __init__(self,
                 scaling=None,
                 thread_ratio=None,
                 scores=None,
                 allow_cat=False,
                 n_components=None,
                 component_tol=None,
                 random_state=None,
                 max_iter=None,
                 tol=None,
                 lanczos_iter=None,
                 svd_alg=None,
                 lanczos_tol=None):
        if not hasattr(self, 'hanaml_parameters'):
            setattr(self, 'hanaml_parameters', pal_param_register())
        super(_PCABase, self).__init__()
        self.scaling = self._arg('scaling', scaling, bool)
        self.thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        self.scores = self._arg('scores', scores, bool)
        self.allow_cat = self._arg('allow_cat', allow_cat, bool)
        self.n_components = self._arg('n_components', n_components, int, required=allow_cat)
        self.component_tol = self._arg('component_tol', component_tol, float)
        self.random_state = self._arg('random_state', random_state, int)
        self.max_iter = self._arg('max_iter', max_iter, int)
        self.tol = self._arg('tol', tol, float)
        self.lanczos_iter = self._arg('lanczos_iter', lanczos_iter, int)
        self.svd_alg = self._arg('svd_alg', svd_alg, {'lanczos':0, 'jacobi':1})
        self.lanczos_tol = self._arg('lanczos_tol', lanczos_tol, float)
        self.loadings_ = None
        self.loadings_stat_ = None
        self.scores_ = None
        self.scaling_stat_ = None
        self.quantification_ = None
        self.stat_ = None

    def _fit(self, data, key=None, features=None, label=None,#pylint:disable=too-many-statements
             categorical_variable=None):#pylint:disable=too-many-locals, invalid-name
        conn = data.connection_context
        features = self._arg('features', features, ListOfStrings)
        if isinstance(categorical_variable, str):
            categorical_variable = [categorical_variable]
        categorical_variable = self._arg('categorical_variable',
                                         categorical_variable,
                                         ListOfStrings)
        if not self._disable_hana_execution:
            require_pal_usable(conn)
            cols = data.columns
            key = self._arg('key', key, str, required=not isinstance(data.index, str))
            if key is None:
                key = data.index
            cols.remove(key)
            label = self._arg('label', label, str)
            if label is not None:
                cols.remove(label)
            if features is None:
                features = cols
            data_ = data[[key] + features]
            #rownum, colnum = data_.shape
            #colnum = colnum - 1
            #max_components = min(rownum, colnum)
            #n_components = self.n_components
            #n_components validity check should be changed since now REAL_VECTOR type gets involved
            #if n_components is not None and not 0 < n_components <= max_components:
            #    msg = 'n_components {!r} is out of bounds, '.format(n_components) +\
            #    'it should be within the range of [1, {}] for the input data.'.format(max_components)
            #    logger.error(msg)
            #    raise ValueError(msg)
        else:
            data_ = data
        #coltypes = [dtp[1] for dtp in data_.dtypes()]
        #if any('VARCHAR' in x for x in coltypes) or categorical_variable is not None:
        #    self.allow_cat = True
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        outputs = ['LOADINGS', 'LOADINGS_INFO', 'SCORES', 'SCALING_INFO']
        outputs = ['#PAL_PCA_{}_TBL_{}_{}'.format(name, self.id, unique_id)
                   for name in outputs]
        loadings_tbl, loadingsinfo_tbl, scores_tbl, scalinginfo_tbl = outputs

        param_rows = [
            ("SCALING", self.scaling, None, None),
            ("SCORES", self.scores, None, None),
            ("THREAD_RATIO", None, self.thread_ratio, None),
            ("N_COMPONENTS", self.n_components, None, None)
        ]
        pal_proc = 'PAL_PCA'
        if self.allow_cat is True:
            pal_proc = 'PAL_CATPCA'
            outputs_new = ['#PAL_PCA_{}_TBL_{}_{}'.format(name, self.id, unique_id)
                           for name in ('QUANTIFICATION', 'STAT')]
            quantification_tbl, stat_tbl = outputs_new
            outputs.extend(outputs_new)
            param_rows_new = [
                ('COMPONENT_TOL', None, self.component_tol, None),
                ('SEED', self.random_state, None, None),
                ('MAX_ITERATION', self.max_iter, None, None),
                ('CONVERGE_TOL', None, self.tol, None),
                ('LANCZOS_ITERATION', self.lanczos_iter, None, None),
                ('SVD_CALCULATOR', self.svd_alg, None, None),
                ('LANCZOS_TOL', None, self.lanczos_tol, None)]
            if categorical_variable is not None:
                param_rows_new.extend([('CATEGORICAL_VARIABLE', None, None,
                                        cat_var) for cat_var in categorical_variable])
            param_rows.extend(param_rows_new)
        try:
            self._call_pal_auto(conn,
                                pal_proc,
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
        # pylint: disable=attribute-defined-outside-init
        if not self._disable_hana_execution:
            self.loadings_ = conn.table(loadings_tbl)
            self.loadings_stat_ = conn.table(loadingsinfo_tbl)
            self.scores_ = conn.table(scores_tbl) if self.scores is True else None
            self.scaling_stat_ = conn.table(scalinginfo_tbl)
            self.model_ = [self.loadings_, self.scaling_stat_]
            if self.allow_cat is True:
                self.quantification_ = conn.table(quantification_tbl)
                self.stat_ = conn.table(stat_tbl)
                self.model_.extend([self.quantification_])
        return self

    def _fit_transform(self, data, key=None,
                       features=None,
                       n_components=None,
                       label=None,
                       ignore_unknown_category=None):#pylint:disable=invalid-name
        self._fit(data, key, features, label)
        if not self._disable_hana_execution:
            index = data.index
            key = self._arg('key', key, str, required=not isinstance(index, str))
            if isinstance(index, str):
                if key is not None and index != key:
                    msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                    "and the designated index column '{}'.".format(index)
                    logger.warning(msg)
            key = index if key is None else key
            data_ = data
            scores_ = self.scores_
            if scores_ is None:
                scores_ = self._transform(data_, key, features, n_components, label, ignore_unknown_category)
            if label is None:
                return scores_
            return scores_.alias('L').join(data_.alias('R'), 'L.%s' % key + '= R.%s' % key, select=['L.*', label])
        return data

    def _transform(self, data, key=None,
                   features=None,
                   n_components=None,
                   label=None,
                   ignore_unknown_category=None,
                   thread_ratio=None):#pylint:disable=invalid-name, too-many-locals
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
        n_components = self._arg('n_components', n_components, int)
        thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        cols = data.columns
        cols.remove(key)
        if label is not None:
            cols.remove(label)
        if features is None:
            features = cols
        max_components = len(features) if self.n_components is None else self.n_components
        if n_components is not None and not 0 < n_components <= max_components:
            msg = 'n_components {!r} is out of bounds'.format(n_components)
            logger.error(msg)
            raise ValueError(msg)

        data_ = data[[key] + features]

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        scores_tbl = '#PAL_PCA_SCORE_TBL_{}_{}'.format(self.id, unique_id)

        param_rows = [
            ('SCALING', self.scaling, None, None),
            ('MAX_COMPONENTS', n_components, None, None),
            ('THREAD_RATIO', None, thread_ratio, None),
            ('IGNORE_UNKNOWN_CATEGORY', ignore_unknown_category,
             None, None)
            ]
        if len(self.model_) == 3:
            self.allow_cat = True
        pal_proc = 'PAL_CATPCA_PROJECT' if self.allow_cat else 'PAL_PCA_PROJECT'
        try:
            self._call_pal_auto(conn,
                                pal_proc,
                                data_,
                                *self.model_,
                                ParameterTable().with_data(param_rows),
                                scores_tbl)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, scores_tbl)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, scores_tbl)
            raise
        return conn.table(scores_tbl)

    def create_model_state(self, model=None, function=None,
                           pal_funcname=None,
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

            A placeholder parameter, not effective for (CAT)PCA.

        pal_funcname : int or str, optional
            PAL function name.

            Defaults to self.pal_funcname.

        state_description : str, optional
            Description of the state as model container.

            Defaults to None.

        force : bool, optional
            If True it will delete the existing state.

            Defaults to False.
        """
        if pal_funcname is None:
            pal_funcname =  'PAL_CATPCA' if self.allow_cat else 'PAL_PCA'
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

class PCA(_PCABase):
    r"""
    Principal component analysis (PCA) aims at reducing the dimensionality of multivariate data while accounting for as much of the variation in the original dataset as possible. This technique is especially useful when the variables within the dataset are highly correlated.
    Principal components seek to transform the original variables to a new set of variables that are:

    - linear combinations of the variables in the dataset;
    - uncorrelated with each other;
    - ordered according to the amount of variations of the original variables that they explain.

    Parameters
    ----------

    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        No default value.
    scaling : bool, optional
        If true, scale variables to have unit variance before the analysis
        takes place.

        Defaults to False.
    scores : bool, optional
        If true, output the scores on each principal component when fitting.

        Defaults to False.
    n_components : int, optional
        Specifies the number of components to keep after tranforming input data.

        Defaults to None.

    Attributes
    ----------
    loadings_ : DataFrame
        The weights by which each standardized original variable should be multiplied when computing component scores.

    loadings_stat_ : DataFrame
        Loadings statistics on each component.

    scores_ : DataFrame
        The transformed variable values corresponding to each data point. Set to None if ``scores`` is False.

    scaling_stat_ : DataFrame
        Mean and scale values of each variable.

        .. Note::

            Variables cannot be scaled if there exists one variable which has constant value across data items.

    Examples
    --------
    >>> pca = PCA(scaling=True, thread_ratio=0.5, scores=True)

    Perform fit():

    >>> pca.fit(data=df, key='ID')

    Output:

    >>> pca.loadings_.collect()
    >>> pca.loadings_stat_.collect()
    >>> pca.scaling_stat_.collect()

    Perform transform():

    >>> result = pca.transform(data=df_trasform, key='ID', n_components=4)
    >>> result.collect()

    """
    def __init__(self,
                 scaling=None,
                 thread_ratio=None,
                 scores=None,
                 n_components=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(PCA, self).__init__(scaling=scaling,
                                  thread_ratio=thread_ratio,
                                  scores=scores,
                                  n_components=n_components)
        self.pal_funcname = 'PAL_PCA'
        self.op_name = "CATPCA"

    def fit(self, data, key=None, features=None, label=None):
        r"""
        Fit the model to the given dataset.

        Parameters
        ----------

        data : DataFrame
            Data to be fitted.
        key : str, optional
            Name of the ID column.
            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.
        features : a list of str, optional
            Names of the feature columns.
            If ``features`` is not provided, it defaults to all non-ID columns.
        label : str, optional
            Label of data.

        Returns
        -------
        A fitted 'PCA' object.
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, 'training_data', data)
        return super(PCA, self)._fit(data=data, key=key,
                                     features=features,
                                     label=label)
    def fit_transform(self, data, key=None, features=None, n_components=None, label=None):
        r"""
        Fit with the data and return the scores.

        Parameters
        ----------

        data : DataFrame
            Data to be analyzed.

        key : str, optional
            Name of the ID column.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.
        features : a list of str, optional
            Names of the feature columns.

            If ``features`` is not provided, it defaults to all non-ID columns,
            non-label columns.
        n_components : int, optional
            Number of components to be retained.
            The value range is from 1 to number of features.

            Defaults to number of features if `self.n_components` is None,
            else defaults to self.n_components.
        label : str, optional
            Label of data.

        Returns
        -------

        DataFrame
            Transformed variable values corresponding to each data point,
            structured as follows:

              - ID column, with same name and type as ``data`` 's ID column.
              - SCORE columns, type DOUBLE, representing the component score
                values of each data point.
              - LABEL column, same as the label column in ``data``, valid only
                when parameter ``label`` is set.
        """
        if n_components is None:
            n_components = self.n_components
        return super(PCA, self)._fit_transform(data=data, key=key,
                                               features=features,
                                               n_components=n_components,
                                               label=label)
    def transform(self, data, key=None, features=None, n_components=None, label=None):
        r"""
        Principal component analysis projection function using a trained model.

        Parameters
        ----------

        data : DataFrame
            Data to be analyzed.
        key : str, optional
            Name of the ID column.
            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.
        features : a list of str, optional
            Names of the feature columns.

            If ``features`` is not provided, it defaults to all non-ID columns.

        n_components : int, optional
            Number of components to be retained.
            The value range is from 1 to number of features.

            Defaults to number of features.
        label : str, optional
            Label of data.

        Returns
        -------

        DataFrame
            Transformed variable values corresponding to each data point.
        """
        return super(PCA, self)._transform(data=data, key=key,
                                           features=features,
                                           n_components=n_components,
                                           label=label)

class CATPCA(_PCABase):
    r"""
    Principal components analysis algorithm that supports categorical features.
    Current implementation uses Alternating Least Square algorithm to find the optimal scaling quantification for categorical data.

    Parameters
    ----------
    scaling : bool, optional
        If true, scale variables to have unit variance before the analysis takes place.

        Defaults to False.
    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        No default value.
    scores : bool, optional
        If true, output the scores on each principal component when fitting.

        Defaults to False.
    n_components : int
        Specifies the number of components to keep.

        Should be greater than or equal to 1.

    component_tol : float, optional
        Specifies the threshold for dropping principal components. More precisely,
        if the ratio between a singular value of some component and the largest
        singular value is *less* than the specified threshold, then the
        corresponding component will be dropped.

        Defaults to 0(indicating no component is dropped).
    random_state : int, optional
        Specifies the random seed used to generate initial quantification for
        categorical variables. Should be nonnegative.

          - 0 : Use current system time as seed(always changing).
          - Others : The deterministic seed value.

        Defaults to 0.
    max_iter : int, optional
        Specifies the maximum number of iterations allowed in computing the
        quantification for categorical variables.

        Defaults to 100.
    tol : int, optional
        Specifies the threshold to determine when the iterative quantification process
        should be stopped. More precisely, if the improvement of loss value is less
        than this threshold between consecutive iterations, the quantification process
        will terminate and regarded as converged.

        Valid range is (0, 1).

        Defaults to 1e-5.
    svg_alg : {'lanczos', 'jacobi'}, optional
        Specifies the choice of SVD algorithm.

            - 'lanczos' : The LANCZOS algorithms.
            - 'jacobi' : The Divide and conquer with Jacobi algorithm.

        Defaults to 'jacobi'.
    lanczos_iter : int, optional
        Specifies the maximum allowed interactions for computing SVD using LANCZOS algorithm.
        Valid only when ``svg_alg`` is 'lanczos'.

        Defaults to 1000.
    lanczos_tol : float, optional
        Specifies precision number of LANCZOS algorithm for computing the eigen value.
        Valid only when ``svg_alg`` is 'lanczos'.

        Valid range is (0, 1).

        Defaults to 1e-7.

    Attributes
    ----------

    loadings_ : DataFrame
       The weights by which each standardized original variable should be
       multiplied when computing component scores.

    loadings_stat_ : DataFrame
        Loadings statistics on each component.

    scores_ : DataFrame
        The transformed variable values corresponding to each data point.

        Set to None if ``scores`` is False.

    scaling_stat_ : DataFrame
        Mean and scale values of each variable.

        .. Note::

            Variables cannot be scaled if there exists one variable which has constant
            value across data items.

    Examples
    --------
    >>> cpc = CATPCA(scaling=TRUE,
                     thread_ratio=0.0,
                     scores=TRUE,
                     n_components=2,
                     component_tol=1e-5)

    Perform fit():

    >>> cpc.fit(data=df, key='ID', categorical_variable='X4')
    >>> cpc.loadings_.collect()

    Perform transform():

    >>> result = cpc.transform(data=df_transform, key="ID", n_components=2,
                               thread_ratio = 0.5, ignore_unknown_category=False)
    >>> result.collect()

    """
    def __init__(self,
                 scaling=None,
                 thread_ratio=None,
                 scores=None,
                 n_components=None,
                 component_tol=None,
                 random_state=None,
                 max_iter=None,
                 tol=None,
                 svd_alg=None,
                 lanczos_iter=None,
                 lanczos_tol=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(CATPCA, self).__init__(scaling=scaling,
                                     thread_ratio=thread_ratio,
                                     scores=scores,
                                     allow_cat=True,
                                     n_components=n_components,
                                     component_tol=component_tol,
                                     random_state=random_state,
                                     max_iter=max_iter,
                                     tol=tol,
                                     lanczos_iter=lanczos_iter,
                                     svd_alg=svd_alg,
                                     lanczos_tol=lanczos_tol)
        self.pal_funcname = 'PAL_CATPCA'
        self.op_name = "CATPCA"

    def fit(self, data, key=None, features=None, categorical_variable=None):
        r"""
        Fit the model to the given dataset.

        Parameters
        ----------

        data : DataFrame

            Data to be fitted.

            The number of rows in ``data`` are expected to be no less than *self.n_components*.

        key : str, optional

            Name of the ID column.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        features : a list of str, optional

            Names of the feature columns.

            The number of features should be no less than *self.n_components*.

            If ``features`` is not provided, it defaults to all non-ID columns.

        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.

        Returns
        -------
        A fitted object of class "CATPCA".
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, 'training_data', data)
        return super(CATPCA, self)._fit(data=data, key=key,
                                        features=features,
                                        label=None,
                                        categorical_variable=categorical_variable)

    def fit_transform(self, data, key=None, features=None,
                      n_components=None,
                      ignore_unknown_category=None):
        r"""
        Fit with the dataset and return the scores.

        Parameters
        ----------

        data : DataFrame

            Data to be analyzed.

        key : str, optional

            Name of the ID column.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        features : a list of str, optional

            Names of the feature columns.

            If ``features`` is not provided, it defaults to all non-ID columns,
            non-label columns.

        n_components : int, optional
            Number of components to be retained.

            The value range is from 1 to number of features.

            Defaults to number of features.

        ignore_unknown_category : bool, optional
            Specifies whether or not to ignore unknown category in ``data``.

            If set to True, any unknown category shall be ignored with quantify 0;
            otherwise, an error message shall be raised in case of unknown category.

            Defaults to False.

        Returns
        -------

        DataFrame

            Transformed variable values for ``data``, structured as follows:

                - 1st column, with same name and type as ``data`` 's ID column.
                - 2nd column, type INTEGER, named 'COMPONENT_ID', representing the IDs
                  for principle components.
                - 3rd column, type DOUBLE, named 'COMPONENT_SCORE', representing the
                  score values of each data points in different components.
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        return super(CATPCA, self)._fit_transform(data=data, key=key,
                                                  features=features,
                                                  n_components=n_components,
                                                  label=None,
                                                  ignore_unknown_category=ignore_unknown_category)

    def transform(self, data, key=None,
                  features=None,
                  n_components=None,
                  ignore_unknown_category=None,
                  thread_ratio=None):
        r"""
        Principal component analysis projection function using a trained model.

        Parameters
        ----------

        data : DataFrame

            Data to be analyzed.

        key : str, optional

            Name of the ID column.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        features : a list of str, optional

            Names of the feature columns.

            If ``features`` is not provided, it defaults to all non-ID columns.

        n_components : int, optional
            Number of components to be retained.

            The value range is from 1 to number of features.

            Defaults to number of features.

        Returns
        -------
        DataFrame

            Transformed variable values corresponding to each data point,
            structured as follows:

                - 1st column, with same name and type as ``data`` 's ID column.
                - 2nd column, type INTEGER, named 'COMPONENT_ID', representing the IDs
                  for principle components.
                - 3rd column, type DOUBLE, named 'COMPONENT_SCORE', representing the
                  score values of each data points in different components.
        """
        return super(CATPCA, self)._transform(data=data, key=key,
                                              features=features,
                                              n_components=n_components,
                                              label=None,
                                              ignore_unknown_category=ignore_unknown_category,
                                              thread_ratio=thread_ratio)

class LatentDirichletAllocation(PALBase):#pylint: disable=too-many-instance-attributes
    r"""
    Latent Dirichlet allocation (LDA) is a generative model in which each item
    (word) of a collection (document) is generated from a finite mixture over
    several latent groups (topics).

    Parameters
    ----------

    n_components : int

        Expected number of topics in the corpus.

    doc_topic_prior : float, optional

        Specifies the prior weight related to document-topic distribution.

        Defaults to 50/``n_components``.

    topic_word_prior : float, optional

        Specifies the prior weight related to topic-word distribution.

        Defaults to 0.1.

    burn_in : int, optional

        Number of omitted Gibbs iterations at the beginning.

        Generally, samples from the beginning may not accurately represent the
        desired distribution and are usually discarded.

        Defaults to 0.

    iteration : int, optional

        Number of Gibbs iterations.

        Defaults to 2000.

    thin : int, optional

        Number of omitted in-between Gibbs iterations.

        Value must be greater than 0.

        Defaults to 1.

    seed : int, optional

        Indicates the seed used to initialize the random number generator:

          - 0: Uses the system time.
          - Not 0: Uses the provided value.

        Defaults to 0.

    max_top_words : int, optional

        Specifies the maximum number of words to be output for each topic.

        Defaults to 0.

    threshold_top_words : float, optional

        The algorithm outputs top words for each topic if the probability
        is larger than this threshold.

        It cannot be used together with parameter ``max_top_words``.
    gibbs_init : str, optional

        Specifies initialization method for Gibbs sampling:

          - 'uniform': Assign each word in each document a topic by uniform distribution.
          - 'gibbs': Assign each word in each document a topic by one round
            of Gibbs sampling using ``doc_topic_prior`` and ``topic_word_prior``.

        Defaults to 'uniform'.

    delimiters : a list of str, optional

        Specifies the set of delimiters to separate words in a document.

        Each delimiter must be one character long.

        Defaults to [' '].

    output_word_assignment : bool, optional

        Controls whether to output the `word_topic_assignment_` DataFrame or not.
        If True, output the `word_topic_assignment_` DataFrame.

        Defaults to False.

    Attributes
    ----------

    doc_topic_dist_ : DataFrame
        Document-topic distribution table, structured as follows:

          - Document ID column, with same name and type as ``data``'s document ID column from fit().
          - TOPIC_ID, type INTEGER, topic ID.
          - PROBABILITY, type DOUBLE, probability of topic given document.

    word_topic_assignment_ : DataFrame
        Word-topic assignment table, structured as follows:

          - Document ID column, with same name and type as ``data``'s document ID column from fit().
          - WORD_ID, type INTEGER, word ID.
          - TOPIC_ID, type INTEGER, topic ID.

        Set to None if ``output_word_assignment`` is set to False.

    topic_top_words_ : DataFrame
        Topic top words table, structured as follows:

          - TOPIC_ID, type INTEGER, topic ID.
          - WORDS, type NVARCHAR(5000), topic top words separated by
            spaces.

        Set to None if neither ``max_top_words`` nor ``threshold_top_words`` is provided.

    topic_word_dist_ : DataFrame
        Topic-word distribution table, structured as follows:

          - TOPIC_ID, type INTEGER, topic ID.
          - WORD_ID, type INTEGER, word ID.
          - PROBABILITY, type DOUBLE, probability of word given topic.

    dictionary_ : DataFrame
        Dictionary table, structured as follows:

          - WORD_ID, type INTEGER, word ID.
          - WORD, type NVARCHAR(5000), word text.

    statistic_ : DataFrame
        Statistics table, structured as follows:

          - STAT_NAME, type NVARCHAR(256), statistic name.
          - STAT_VALUE, type NVARCHAR(1000), statistic value.

        .. note::

          - Parameters ``max_top_words`` and ``threshold_top_words`` cannot be used together.
          - Parameters ``burn_in``, ``thin``, ``iteration``, ``seed``, ``gibbs_init`` and ``delimiters``
            set in transform() will take precedence over the corresponding ones in __init__().

    Examples
    --------
    Input DataFrame df:

    >>> df.collect()
       DOCUMENT_ID                                               TEXT
    0           10  cpu harddisk graphiccard cpu monitor keyboard ...
    1           20  tires mountainbike wheels valve helmet mountai...
    2           30  carseat toy strollers toy toy spoon toy stroll...
    3           40  sweaters sweaters sweaters boots sweaters ring...

    Create a LDA instance:

    >>> lda = LatentDirichletAllocation(n_components=6, burn_in=50, thin=10,
                                        iteration=100, seed=1,
                                        max_top_words=5, doc_topic_prior=0.1,
                                        output_word_assignment=True,
                                        delimiters=[' ', '\r', '\n'])

    Perform fit():

    >>> lda.fit(data=df, key='DOCUMENT_ID', document='TEXT')

    Output:

    >>> lda.doc_topic_dist_.collect()
        DOCUMENT_ID  TOPIC_ID  PROBABILITY
    0            10         0     0.010417
    1            10         1     0.010417
    ...
    22           40         4     0.009434
    23           40         5     0.009434

    >>> lda.word_topic_assignment_.collect()
        DOCUMENT_ID  WORD_ID  TOPIC_ID
    0            10        0         4
    1            10        1         4
    ...
    37           40       20         2
    38           40       16         2

    >>> lda.topic_top_words_.collect()
       TOPIC_ID                                       WORDS
    0         0     spoon strollers tires graphiccard valve
    1         1       toy strollers carseat graphiccard cpu
    ...
    5         5       strollers tires graphiccard cpu valve

    >>> lda.topic_word_dist_.head(40).collect()
        TOPIC_ID  WORD_ID  PROBABILITY
    0          0        0     0.050000
    1          0        1     0.050000
    ...
    39         3        9     0.014286

    >>> lda.dictionary_.collect()
        WORD_ID          WORD
    0        17         boots
    1        12       carseat
    ...
    19       19          vest
    20        8        wheels

    >>> lda.statistic_.collect()
             STAT_NAME          STAT_VALUE
    0        DOCUMENTS                   4
    1  VOCABULARY_SIZE                  21
    2   LOG_LIKELIHOOD  -64.95765414596762

    Input DataFrame df_transform to transform:

    >>> df_transform.collect()
       DOCUMENT_ID               TEXT
    0           10  toy toy spoon cpu

    Perfor transform():

    >>> res = lda.transform(data=df_transform, key='DOCUMENT_ID', document='TEXT', burn_in=2000, thin=100,
                            iteration=1000, seed=1, output_word_assignment=True)

    >>> doc_top_df, word_top_df, stat_df = res

    >>> doc_top_df.collect()
       DOCUMENT_ID  TOPIC_ID  PROBABILITY
    0           10         0     0.239130
    1           10         1     0.456522
    ...
    4           10         4     0.239130
    5           10         5     0.021739

    >>> word_top_df.collect()
       DOCUMENT_ID  WORD_ID  TOPIC_ID
    0           10       13         1
    1           10       13         1
    2           10       15         0
    3           10        0         4

    >>> stat_df.collect()
             STAT_NAME          STAT_VALUE
    0        DOCUMENTS                   1
    1  VOCABULARY_SIZE                  21
    2   LOG_LIKELIHOOD  -7.925092991875363
    3       PERPLEXITY   7.251970666272191
    """
    init_method_map = {'uniform':0, 'gibbs':1}
    def __init__(self, n_components, doc_topic_prior=None, topic_word_prior=None,#pylint: disable=too-many-arguments
                 burn_in=None, iteration=None, thin=None, seed=None, max_top_words=None,
                 threshold_top_words=None, gibbs_init=None, delimiters=None,
                 output_word_assignment=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(LatentDirichletAllocation, self).__init__()
        self.pal_funcname = 'PAL_LATENT_DIRICHLET_ALLOCATION'
        self.n_components = self._arg('n_components', n_components, int, True)
        self.doc_topic_prior = self._arg('doc_topic_prior', doc_topic_prior, float)
        self.topic_word_prior = self._arg('topic_word_prior', topic_word_prior, float)
        self.burn_in = self._arg('burn_in', burn_in, int)
        self.iteration = self._arg('iteration', iteration, int)
        self.thin = self._arg('thin', thin, int)
        self.seed = self._arg('seed', seed, int)
        self.max_top_words = self._arg('max_top_words', max_top_words, int)
        self.threshold_top_words = self._arg('threshold_top_words', threshold_top_words, float)
        if all(x is not None for x in (self.max_top_words, self.threshold_top_words)):
            msg = ('Parameter max_top_words and threshold_top_words cannot be provided together, '+
                   'please choose one of them.')
            logger.error(msg)
            raise ValueError(msg)
        self.gibbs_init = self._arg('gibbs_init', gibbs_init, self.init_method_map)
        self.delimiters = self._arg('delimiters', delimiters, ListOfStrings)
        if self.delimiters is not None:
            if any(len(delimiter) != 1 for delimiter in self.delimiters):
                msg = 'Each delimiter must be one character long.'
                logger.error(msg)
                raise ValueError(msg)
            self.delimiters = ''.join(self.delimiters)
        self.output_word_assignment = self._arg('output_word_assignment', output_word_assignment,
                                                bool)

    def fit(self, data, key=None, document=None):#pylint: disable=too-many-locals
        """
        Fit the model to the given dataset.

        Parameters
        ----------

        data : DataFrame

            Training data.

        key : str, optional

            Name of the document ID column.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        document : str, optional

            Name of the document text column.

            If ``document`` is not provided, ``data`` must have exactly 1
            non-key(non-index) column, and ``document`` defaults to that column.
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, 'training_data', data)
        conn = data.connection_context
        require_pal_usable(conn)
        index = data.index
        key = self._arg('key', key, str, required=not isinstance(index, str))
        if isinstance(index, str):
            if key is not None and index != key:
                msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(msg)
        key = index if key is None else key
        document = self._arg('document', document, str)
        cols = data.columns
        cols.remove(key)
        if document is None:
            if len(cols) != 1:
                msg = 'LDA requires exactly one document column.'
                logger.error(msg)
                raise ValueError(msg)
            document = cols[0]

        param_rows = [('TOPICS', self.n_components, None, None),
                      ('ALPHA', None, self.doc_topic_prior, None),
                      ('BETA', None, self.topic_word_prior, None),
                      ('BURNIN', self.burn_in, None, None),
                      ('ITERATION', self.iteration, None, None),
                      ('THIN', self.thin, None, None),
                      ('SEED', self.seed, None, None),
                      ('MAX_TOP_WORDS', self.max_top_words, None, None),
                      ('THRESHOLD_TOP_WORDS', None, self.threshold_top_words, None),
                      ('INIT', self.gibbs_init, None, None),
                      ('DELIMIT', None, None, self.delimiters),
                      ('OUTPUT_WORD_ASSIGNMENT', self.output_word_assignment, None, None)]

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        outputs = ['#LDA_{}_TBL_{}_{}'.format(name, self.id, unique_id)
                   for name in ['DOC_TOPIC_DIST',
                                'WORD_TOPIC_ASSIGNMENT',
                                'TOPIC_TOP_WORDS',
                                'TOPIC_WORD_DIST',
                                'DICT',
                                'STAT',
                                'CV_PARAM']]
        (doc_top_dist_tbl, word_topic_assignment_tbl,
         topic_top_words_tbl, topic_word_dist_tbl, dict_tbl, stat_tbl, cv_param_tbl) = outputs

        try:
            self._call_pal_auto(conn,
                                'PAL_LATENT_DIRICHLET_ALLOCATION',
                                data.select([key, document]),
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
        self.doc_topic_dist_ = conn.table(doc_top_dist_tbl)
        if self.output_word_assignment:
            self.word_topic_assignment_ = conn.table(word_topic_assignment_tbl)
        else:
            self.word_topic_assignment_ = None
        if any(x is not None for x in (self.threshold_top_words, self.max_top_words)):
            self.topic_top_words_ = conn.table(topic_top_words_tbl)
        else:
            self.topic_top_words_ = None
        self.topic_word_dist_ = conn.table(topic_word_dist_tbl)
        self.dictionary_ = conn.table(dict_tbl)
        self.statistic_ = conn.table(stat_tbl)
        self._cv_param = conn.table(cv_param_tbl)
        self.model_ = [self.topic_word_dist_, self.dictionary_, self._cv_param]

    def fit_transform(self, data, key=None, document=None):
        """
        Fit LDA model based on training data and return the topic assignment
        for the training documents.

        Parameters
        ----------

        data : DataFrame

            Training data.

        key : str, optional

            Name of the document ID column.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        document : str, optional

            Name of the document text column.

            If ``document`` is not provided, ``data`` must have exactly 1
            non-key column, and ``document`` defaults to that column.

        Returns
        -------

        DataFrame

            Document-topic distribution table, structured as follows:

              - Document ID column, with same name and type as ``data`` 's
                document ID column.
              - TOPIC_ID, type INTEGER, topic ID.
              - PROBABILITY, type DOUBLE, probability of topic given document.

        """
        self.fit(data, key, document)
        return self.doc_topic_dist_

    def transform(self, data, key=None, document=None, burn_in=None, #pylint: disable=too-many-arguments, too-many-locals
                  iteration=None, thin=None, seed=None, gibbs_init=None,
                  delimiters=None, output_word_assignment=None):
        """
        Transform the topic assignment for new documents based on the previous
        LDA estimation results.

        Parameters
        ----------

        data : DataFrame

            Independent variable values used for transform.

        key : str, optional

            Name of the document ID column.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        document : str, optional

            Name of the document text column.

            If ``document`` is not provided, ``data`` must have exactly 1
            non-key column, and ``document`` defaults to that column.

        burn_in : int, optional

            Number of omitted Gibbs iterations at the beginning.

            Generally, samples from the beginning may not accurately represent the
            desired distribution and are usually discarded.

            Defaults to 0.

        iteration : int, optional

            Numbers of Gibbs iterations.

            Defaults to 2000.

        thin : int, optional

            Number of omitted in-between Gibbs iterations.

            Defaults to 1.

        seed : int, optional

            Indicates the seed used to initialize the random number generator:

              - 0: Uses the system time.
              - Not 0: Uses the provided value.

            Defaults to 0.

        gibbs_init : str, optional

            Specifies initialization method for Gibbs sampling:

              - 'uniform': Assign each word in each document a topic by uniform
                distribution.
              - 'gibbs': Assign each word in each document a topic by one round
                of Gibbs sampling using ``doc_topic_prior`` and ``topic_word_prior``.

            Defaults to 'uniform'.

        delimiters : a list of str, optional

            Specifies the set of delimiters to separate words in a document.
            Each delimiter must be one character long.

            Defaults to [' '].

        output_word_assignment : bool, optional

            Controls whether to output the ``word_topic_df`` or not.

            If True, output the ``word_topic_df``.

            Defaults to False.

        Returns
        -------

        DataFrame

          DataFrame 1, document-topic distribution table, structured as follows:

          - Document ID column, with same name and type as ``data`` 's
            document ID column.
          - TOPIC_ID, type INTEGER, topic ID.
          - PROBABILITY, type DOUBLE, probability of topic given document.

          DataFrame 2, word-topic assignment table, structured as follows:

          - Document ID column, with same name and
            type as ``data`` 's document ID column.
          - WORD_ID, type INTEGER, word ID.
          - TOPIC_ID, type INTEGER, topic ID.

          Set to None if ``output_word_assignment`` is False.

          DataFrame 3, statistics table, structured as follows:

          - STAT_NAME, type NVARCHAR(256), statistic name.
          - STAT_VALUE, type NVARCHAR(1000), statistic value.
        """
        #check for table existence, here it requires: topic_word_dist,
        #dictionary, cv_param
        conn = data.connection_context
        require_pal_usable(conn)
        if getattr(self, 'model_') is None:
            raise FitIncompleteError()

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        index = data.index
        key = self._arg('key', key, str, required=not isinstance(index, str))
        if isinstance(index, str):
            if key is not None and index != key:
                msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(msg)
        key = index if key is None else key
        document = self._arg('document', document, str)
        cols = data.columns
        cols.remove(key)
        if document is None:
            if len(cols) != 1:
                msg = 'LDA requires exactly one document column.'
                logger.error(msg)
                raise ValueError(msg)
            document = cols[0]

        burn_in = self._arg('burn_in', burn_in, int)
        iteration = self._arg('iteration', iteration, int)
        thin = self._arg('thin', thin, int)
        gibbs_init = self._arg('gibbs_init', gibbs_init, self.init_method_map)
        delimiters = self._arg('delimiters', delimiters, ListOfStrings)

        if delimiters is not None:
            if any(len(delimiter) != 1 for delimiter in delimiters):
                msg = 'Each delimiter must be one character long.'
                logger.error(msg)
                raise ValueError(msg)
            delimiters = ''.join(delimiters)
        output_word_assignment = self._arg('output_word_assignment', output_word_assignment, bool)

        names = ['DOC_TOPIC_DIST', 'WORD_TOPIC_ASSIGNMENT', 'STAT']
        outputs = ['#LDA_PRED_{}_TBL_{}_{}'.format(name, self.id, unique_id) for name in names]
        (doc_top_dist_tbl, word_topic_assignment_tbl, stat_tbl) = outputs

        param_rows = [('BURNIN', burn_in, None, None),
                      ('ITERATION', iteration, None, None),
                      ('THIN', thin, None, None),
                      ('SEED', seed, None, None),
                      ('INIT', gibbs_init, None, None),
                      ('DELIMIT', None, None, delimiters),
                      ('OUTPUT_WORD_ASSIGNMENT', output_word_assignment, None, None)]

        try:
            self._call_pal_auto(conn,
                                'PAL_LATENT_DIRICHLET_ALLOCATION_INFERENCE',
                                data.select([key, document]),
                                self.model_[0],
                                self.model_[1],
                                self.model_[2],
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
        return (conn.table(doc_top_dist_tbl),
                conn.table(word_topic_assignment_tbl) if output_word_assignment
                else None,
                conn.table(stat_tbl))

    def create_model_state(self, model=None, function=None,
                           pal_funcname='PAL_LATENT_DIRICHLET_ALLOCATION',
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

            A placeholder parameter, not effective for Latent Dirichlet Allocation.

        pal_funcname : int or str, optional
            PAL function name.

            Defaults to 'PAL_LATENT_DIRICHLET_ALLOCATION'.

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

class VectorPCA(PALBase):
    r"""Principal component analysis for real vector data in SAP HANA Cloud.

    Parameters
    ----------
    n_components : int
        Specifies the number of components to keep.

        Should be greater than or equal to 1.

    scaling : bool, optional
        If True, scale variables to have unit variance before analysis.

        Defaults to False.

    thread_ratio : float, optional
         Specifies the ratio of total available thread. The value range is [0,1], where
         0 means single thread, and 1 means all available threads.

    scores : bool, optional
        If True, output the scores on each principal component after fitting.

        Defaults to False.

    component_tol : float, optional
        Specifies the threshold for dropping principal components. More precisely,
        if the ratio between a singular value of some component and the largest
        singular value is *less* than the specified threshold, then the
        corresponding component will be dropped.

        Defaults to 0(indicating no component is dropped).

    svg_alg : {'lanczos', 'jacobi'}, optional
        Specifies the choice of SVD algorithm.

            - 'lanczos' : The LANCZOS algorithms.
            - 'jacobi' : The Divide and conquer with Jacobi algorithm.

        Defaults to 'jacobi'.
    lanczos_tol : float, optional
        Specifies precision number of LANCZOS algorithm for computing the eigen value.
        Valid only when ``svg_alg`` is "lanczos".

        Valid range is (0, 1).

    lanczos_iter : int, optional
        Specifies the maximum allowed interactions for computing SVD using LANCZOS algorithm.
        Valid only when ``svg_alg`` is 'lanczos'.

        Defaults to 1000.

    Attributes
    ----------
    loadings_ : DataFrame
       The weights by which each standardized original variable should be
       multiplied when computing component scores.

    loadings_stat_ : DataFrame
        Loadings statistics on each component.

    scores_ : DataFrame
        The transformed variable values corresponding to each data point.

        Set to None if ``scores`` is False.

    scaling_stat_ : DataFrame
        Mean and scale values of each variable.

    Examples
    --------
    Input data with real vectors:

    >>> df.dtypes()
    [('ID', 'INT', 10, 10, 10, 0),
     ('V1', 'REAL_VECTOR', 16, 16, 3, 0),
     ('V2', 'REAL_VECTOR', 12, 12, 2, 0)]
    >>> df.collect()
       ID                   V1            V2
    0   0      [1.0, 1.0, 1.0]    [1.0, 1.0]
    1   1     [1.0, 1.0, -1.0]   [1.0, -1.0]
    2   2    [-1.0, -1.0, 1.0]   [-1.0, 1.0]
    3   3   [-1.0, -1.0, -1.0]  [-1.0, -1.0]

    Train a VectorPCA model and return the transformed data:

    >>> from hana_ml.algorithms.pal.decomposition import VectorPCA
    >>> vecpca = VectorPCA(n_components=2)
    >>> pca_res = vecpca.fit_transform(data=df, key='ID')
    >>> pca_res.collect()
        ID                                SCORE_VECTOR
    0    0   [1.7320507764816284, -1.4142135381698608]
    1    1    [1.7320507764816284, 1.4142135381698608]
    2    2  [-1.7320507764816284, -1.4142135381698608]
    3    3   [-1.7320507764816284, 1.4142135381698608]
    """
    def __init__(self,
                 n_components,
                 scaling=None,
                 thread_ratio=None,
                 scores=True,
                 component_tol=None,
                 svd_alg=None,
                 lanczos_tol=None,
                 lanczos_iter=None):
        super(VectorPCA, self).__init__()
        self.scaling = self._arg('scaling', scaling, bool)
        self.thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        self.scores = self._arg('scores', scores, bool)
        self.n_components = self._arg('n_components', n_components, int)
        self.component_tol = self._arg('component_tol', component_tol, float)
        self.lanczos_tol = self._arg('lanczos_tol', lanczos_tol, float)
        self.lanczos_iter = self._arg('lanczos_iter', lanczos_iter, int)
        self.svd_alg = self._arg('svd_alg', svd_alg, {'lanczos':0, 'jacobi':1})
        self.loadings_ = None
        self.loadings_stat_ = None
        self.scores_ = None
        self.scaling_stat_ = None

    def fit(self, data, key=None):
        r"""
        The `fit()` method for VectorPCA.

        Parameters
        ----------
        data : DataFrame
            Data to be fitted for obtaining a **VectorPCA** model.
            In particular, all columns of ``data`` should be *REAL_VECTOR* type except its ID column,
            otherwise an error shall be thrown.

        key : str, optional
            Name of the ID column.
            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        Returns
        -------
        A fitted object of class **VectorPCA**.
        """
        conn = data.connection_context
        cols = data.columns
        idx = data.index
        key = self._arg('key', key, str,
                        required=not isinstance(idx, str))
        if key is not None:
            if isinstance(idx, str) and key != idx:
                msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(idx)
                logger.warning(msg)
        else:
            key = idx
        cols.remove(key)
        outputs = ["LOADINGS", "LOADINGS_INFO", "SCORES", "SCALING_INFO"]
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        outputs = [f"#VECPCA_FIT_{x}_TBL_{unique_id}" for x in outputs]
        param_rows = [
            ("SCALING", self.scaling, None, None),
            ("SCORES", self.scores, None, None),
            ("THREAD_RATIO", None, self.thread_ratio, None),
            ("N_COMPONENTS", self.n_components, None, None),
            ("COMPONENT_TOL", None, self.component_tol, None),
            ("LANCZOS_TOL", None, self.lanczos_tol, None),
            ("LANCZOS_ITERATION", self.lanczos_iter, None, None),
            ("SVD_CALCULATOR", self.svd_alg, None, None)]
        try:
            self._call_pal_auto(conn,
                                'PAL_VECPCA',
                                data[[key] + cols],
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
        self.loadings_ = conn.table(outputs[0])
        self.loadings_stat_ = conn.table(outputs[1])
        self.scores_ = conn.table(outputs[2])
        self.scaling_stat_ = conn.table(outputs[3])
        self.model_ = [self.loadings_, self.scaling_stat_]
        return self

    def fit_transform(self, data, key=None):
        r"""
        Fit a **VectorPCA** model and in the meantime return the transformed data.

        Parameters
        ----------
        data : DataFrame
            Data to be fitted for obtaining a **VectorPCA** model.
            In particular, all columns of ``data`` should be *REAL_VECTOR* type except its ID column,
            otherwise an error shall be thrown.

        key : str, optional
            Name of the ID column.
            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        Returns
        -------
        DataFrame
            The transformed data, structured as follows:

            - 1st column, with column name/type same as the ID (``key``) column in ``data``.
            - 2nd column, with column name **SCORE_VECTOR** and column data type *REAL_VECTOR*, which
              stores the vector of component score for ``data`` after PCA transformation.
        """
        setattr(self, 'scores', True)#make sure that ``scores`` is set as True.
        self.fit(data=data, key=key)
        return self.scores_

    def transform(self, data, key=None,
                  thread_ratio=None,
                  max_components=None):
        r"""
        Tranform data with real vectors with a trained VectorPCA model.

        Parameters
        ----------
        data : DataFrame
            Data to be transformed by the fitted **VectorPCA** model.
            In particular, it should be structured the same as the one used for training the VectorPCA model,
            otherwise an error shall be thrown.

        key : str, optional
            Name of the ID column.
            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        thread_ratio : float, optional
            Specifies the ratio of available threads used for performing the transformation.
            The value range is [0, 1], where 0 means single thread while 1 means all available threads.

        max_components : int, optional
            Specifies the component dimension to keep in output, with default value being the value of ``n_components``
            specified when training the VectorPCA model.

            The valid range should be between 1 and default value.

        Returns
        -------
        DataFrame
            The transformed data, structured as follows:

            - 1st column, with column name/type same as the ID (``key``) column in ``data``.
            - 2nd column, with column name **SCORE_VECTOR** and column data type *REAL_VECTOR*, which
              stores the vector of component score for ``data`` after PCA transformation.
        """
        conn = data.connection_context
        idx = data.index
        key = self._arg('key', key, str,
                        required=not isinstance(idx, str))
        cols = data.columns
        if key is not None:
            if isinstance(idx, str) and key != idx:
                msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(idx)
                logger.warning(msg)
        else:
            key = idx
        cols.remove(key)
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        output = f"#VECPCA_PROJECT_SCORE_TBL_{unique_id}"
        param_rows = [('THREAD_RATIO', None, thread_ratio, None),
                      ('MAX_COMPONENTS', max_components, None, None)]
        try:
            self._call_pal_auto(conn,
                                'PAL_VECPCA_PROJECT',
                                data[[key] + cols],
                                *self.model_,
                                ParameterTable().with_data(param_rows),
                                output)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, output)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, output)
            raise
        return conn.table(output)
