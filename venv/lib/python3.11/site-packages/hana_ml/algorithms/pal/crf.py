"""
This module contains Python wrapper for SAP HANA PAL conditional random field(CRF) algorithm.

The following class is available:

    * :class:`CRF`
"""
#pylint:disable=line-too-long, too-many-instance-attributes, too-few-public-methods
#pylint:disable=invalid-name, too-many-arguments, too-many-locals
#pylint: disable=consider-using-f-string
import logging
import uuid
from hdbcli import dbapi
from hana_ml.ml_exceptions import FitIncompleteError
from .pal_base import (
    PALBase,
    ParameterTable,
    pal_param_register,
    try_drop,
    require_pal_usable
)
logger = logging.getLogger(__name__)

class CRF(PALBase):
    """
    Conditional random fields (CRFs) are a probabilistic framework for labeling and segmenting structured data, such as sequences.
    The underlying idea is that of defining a conditional probability distribution over label sequences given an observation sequences,
    rather than a joint distribution over both label and observation sequences.

    Parameters
    ----------

    epsilon : float, optional
        Convergence tolerance of the optimization algorithm.

        Defaults to 1e-4.
    lamb : float, optional
        Regularization weight, should be greater than 0.

        Defaults t0 1.0.
    max_iter : int, optional
        Maximum number of iterations in optimization.

        Defaults to 1000.
    lbfgs_m : int, optional
        Number of memories to be stored in L_BFGS optimization algorithm.

        Defaults to 25.
    use_class_feature : bool, optional
        To include a feature for class/label.
        This is the same as having a bias vector in a model.

        Defaults to True.
    use_word : bool, optional
        If True, gives you feature for current word.

        Defaults to True.
    use_ngrams : bool, optional
        Whether to make feature from letter n-grams, i.e. substrings of the word.

        Defaults to True.
    mid_ngrams : bool, optional
        Whether to include character n-gram features for n-grams that
        contain neither the beginning or the end of the word.

        Defaults to False.
    max_ngram_length : int, optional
        Upper limit for the size of n-grams to be included.
        Effective only this parameter is positive.

        Defaults to 6.
    use_prev : bool, optional
        Whether or not to include a feature for previous word and current word,
        and together with other options enables other previous features.

        Defaults to True.
    use_next : bool, optional
        Whether or not to include a feature for next word and current word.

        Defaults to True.
    disjunction_width : int, optional
        Defines the width for disjunctions of words, see ``use_disjunctive``.

        Defaults to 4.
    use_disjunctive : bool, optional
        Whether or not to include in features giving disjunctions of words
        anywhere in left or right ``disjunction_width`` words.

        Defaults to True.
    use_seqs : bool, optional
        Whether or not to use any class combination features.

        Defaults to True.
    use_prev_seqs : bool, optional
        Whether or not to use any class combination features using the previous class.

        Defaults to True.
    use_type_seqs : bool, optional
        Whether or not to use basic zeroth order word shape features.

        Defaults to True.
    use_type_seqs2 : bool, optional
        Whether or not to add additional first and second order word shape features.

        Defaults to True.
    use_type_yseqs : bool, optional
        Whether or not to use some first order word shape patterns.

        Defaults to True.
    word_shape : int, optional
        Word shape, e.g. whether capitalized or numeric.
        Only supports chris2UseLC currently.
        Do not use word shape if this is 0.

        Defaults to 0.
    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 1.0.

    Attributes
    ----------

    model_ : DataFrame
        Model content.

    stats_ : DataFrame
        Statistics.

    Examples
    --------
    Input data for training:

    >>> df.head(10).collect()
       DOC_ID  WORD_POSITION      WORD LABEL
    0       1              1    RECORD     O
    1       1              2   #497321     O
    ...
    9       1             10   7368393     O

    Set up an instance of CRF model:

    >>> crf = CRF(lamb=0.1,
    ...           max_iter=1000,
    ...           epsilon=1e-4,
    ...           lbfgs_m=25,
    ...           word_shape=0,
    ...           thread_ratio=1.0)

    Perform fit():

    >>> crf.fit(data=df, doc_id="DOC_ID",
                word_pos="WORD_POSITION",
    ...         word="WORD", label="LABEL")

    Check the trained CRF model and related statistics:

    >>> crf.model_.collect()
       ROW_INDEX                                      MODEL_CONTENT
    0          0  {"classIndex":[["O","OxygenSaturation"]],"defa...
    >>> crf.stats_.head(10).collect()
             STAT_NAME           STAT_VALUE
    0              obj  0.44251900977373015
    1             iter                   22
    ...
    9           iter 4           obj=2.4382

    Input data for predicting labels using trained CRF model

    >>> df_pred.head(10).collect()
       DOC_ID  WORD_POSITION         WORD
    0       2              1      GENERAL
    1       2              2     PHYSICAL
    ...
    9       2             10        86g52

    Perform prediction():

    >>> res = crf.predict(data=df_pred, doc_id='DOC_ID', word_pos='WORD_POSITION',
                          word='WORD', thread_ratio=1.0)
    >>> df_pred.head(10).collect()
       DOC_ID  WORD_POSITION         WORD
    0       2              1      GENERAL
    1       2              2     PHYSICAL
    ...
    8       2              9     pressure
    9       2             10        86g52
    """
    pal_funcname = 'PAL_CRF'
    def __init__(self,
                 lamb=None,
                 epsilon=None,
                 max_iter=None,
                 lbfgs_m=None,
                 use_class_feature=None,
                 use_word=None,
                 use_ngrams=None,
                 mid_ngrams=False,
                 max_ngram_length=None,
                 use_prev=None,
                 use_next=None,
                 disjunction_width=None,
                 use_disjunctive=None,
                 use_seqs=None,
                 use_prev_seqs=None,
                 use_type_seqs=None,
                 use_type_seqs2=None,
                 use_type_yseqs=None,
                 word_shape=None,
                 thread_ratio=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(CRF, self).__init__()
        self.lamb = self._arg('lamb', lamb, float)
        self.epsilon = self._arg('epsilon', epsilon, float)
        self.max_iter = self._arg('max_iter', max_iter, int)
        self.lbfgs_m = self._arg('lbfgs_m', lbfgs_m, int)
        self.use_class_feature = self._arg('use_class_feature',
                                           use_class_feature, bool)
        self.use_word = self._arg('use_word', use_word, bool)
        self.use_ngrams = self._arg('use_ngrams', use_ngrams, bool)
        self.mid_ngrams = self._arg('mid_ngrams', mid_ngrams, bool)
        if self.mid_ngrams is None:
            self.mid_ngrams = False
        self.max_ngram_length = self._arg('max_ngram_length', max_ngram_length, int)
        self.use_next = self._arg('use_next', use_next, bool)
        self.use_prev = self._arg('use_prev', use_prev, bool)
        self.disjunction_width = self._arg('disjunction_width',
                                           disjunction_width, int)
        self.use_disjunctive = self._arg('use_disjunctive',
                                         use_disjunctive, bool)
        self.use_seqs = self._arg('use_seqs', use_seqs, bool)
        self.use_prev_seqs = self._arg('use_prev_seqs', use_prev_seqs, bool)
        self.use_type_seqs = self._arg('use_type_seqs', use_type_seqs, bool)
        self.use_type_seqs2 = self._arg('use_type_seqs2', use_type_seqs2, bool)
        self.use_type_yseqs = self._arg('use_type_yseqs', use_type_yseqs, bool)
        #word-shape parameter may needs more consideration.
        self.word_shape = self._arg('word_shape', word_shape, int)
        self.thread_ratio = self._arg('thread_ratio', thread_ratio, float)

    def fit(self, data, doc_id=None, word_pos=None,
            word=None, label=None):
        """
        Fit the model to the given dataset.

        Parameters
        ----------

        data : DataFrame
            Input data. It should contain at least 4 columns, corresponding to document ID,
            word position, word and label, respectively.

        doc_id : str, optional

            Name of the column for document ID.

            Defaults to the first column of the input data.

        word_pos : str, optional

            Name of the column for word position.

            Defaults to the 1st non-doc_id column of the input data.

        word : str, optional

            Name of the column for word.

            Defaults to 1st non-doc_id, non-word_pos column of the input data.

        label : str, optional

            Name of the label column.

            Defaults to the last non-doc_id, non-word_pos, non-word column of the input data.
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, 'training_data', data)
        conn = data.connection_context
        require_pal_usable(conn)
        cols = data.columns
        if len(cols) < 4:
            msg = ("Input data contains only {} columns, ".format(len(cols))+
                   "while CRF model fitting requires at least 4.")
            logger.error(msg)
            raise ValueError(msg)
        doc_id = self._arg('doc_id', doc_id, str)
        word_pos = self._arg('word_pos', word_pos, str)
        word = self._arg('word', word, str)
        label = self._arg('label', label, str)
        if doc_id is None:
            doc_id = cols[0]
        cols.remove(doc_id)
        if word_pos is None:
            word_pos = cols[0]
        cols.remove(word_pos)
        if word is None:
            word = cols[0]
        cols.remove(word)
        if label is None:
            label = cols[-1]
        used_cols = [doc_id, word_pos, word, label]
        data_ = data[used_cols]
        param_rows = [('ENET_LAMBDA', None, self.lamb, None),
                      ('EXIT_THRESHOLD', None, self.epsilon, None),
                      ('MAX_ITERATION', self.max_iter, None, None),
                      ('LBFGS_M', self.lbfgs_m, None, None),
                      ('USE_CLASS_FEATURE', self.use_class_feature, None, None),
                      ('USE_WORD', self.use_word, None, None),
                      ('USE_NGRAMS', self.use_ngrams, None, None),
                      ('NO_MIDNGRAMS', not self.mid_ngrams, None, None),
                      ('MAX_NGRAM_LENGTH', self.max_ngram_length, None, None),
                      ('USE_PREV', self.use_prev, None, None),
                      ('USE_NEXT', self.use_next, None, None),
                      ('USE_DISJUNCTIVE', self.use_disjunctive, None, None),
                      ('DISJUNCTION_WIDTH', self.disjunction_width, None, None),
                      ('USE_SEQUENCES', self.use_seqs, None, None),
                      ('USE_PREVSEQUENCES', self.use_prev_seqs, None, None),
                      ('USE_TYPE_SEQS', self.use_type_seqs, None, None),
                      ('USE_TYPE_SEQS2', self.use_type_seqs2, None, None),
                      ('USE_TYPE_YSEQUENCES', self.use_type_yseqs, None, None),
                      ('WORD_SHAPE', self.word_shape, None, None),
                      ('THREAD_RATIO', None, self.thread_ratio, None)]
        unique_id = str(uuid.uuid1()).replace('-', '_')
        tables = ['MODEL', 'STATS', 'OPTIMAL_PARAM']
        tables = ["#PAL_CRF_{}_TBL_{}_{}".format(table, self.id, unique_id)
                  for table in tables]
        model_tbl, stats_tbl, optim_param_tbl = tables
        try:
            self._call_pal_auto(conn,
                                'PAL_CRF',
                                data_,
                                ParameterTable().with_data(param_rows),
                                model_tbl,
                                stats_tbl,
                                optim_param_tbl)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, tables)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, tables)
            raise
        #pylint:disable=attribute-defined-outside-init
        self.model_ = conn.table(model_tbl)
        self.stats_ = conn.table(stats_tbl)
        self.statistics_ = self.stats_
        self.optim_param_ = conn.table(optim_param_tbl)
        return self

    def predict(self,
                data,
                doc_id=None,
                word_pos=None,
                word=None,
                thread_ratio=None):
        """
        Predicts text labels using a trained CRF model.

        Parameters
        ----------
        data : DataFrame
            Input data to predict the labels.
            It should contain at least 3 columns, corresponding to document ID,
            word position and word, respectively.

        doc_id : str, optional
            Name of the column for document ID.

            Defaults to the 1st column of the input data.

        word_pos : str, optional
            Name of the column for word position.

            Defaults to the 1st non-doc_id column of the input data.

        word : str, optional
            Name of the column for word.

            Defaults to the 1st non-doc_id, non-word_pos column of the input data.

        thread_ratio : float, optional
            Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
            Values outside the range will be ignored and this function heuristically determines the number of threads to use.

            Defaults to 1.0.

        Returns
        -------
        DataFrame
            Prediction result for the input data, structured as follows:

                - 1st column: document ID,
                - 2nd column: word position,
                - 3rd column: label.

        """
        conn = data.connection_context
        if getattr(self, 'model_') is None:
            raise FitIncompleteError()
        cols = data.columns
        if len(cols) < 3:
            msg = ("Input data contains only {} columns, ".format(len(cols))+
                   "while CRF label prediction requires at least 3.")
            logger.error(msg)
            raise ValueError(msg)
        doc_id = self._arg('doc_id', doc_id, str)
        word_pos = self._arg('word_pos', word_pos, str)
        word = self._arg('word', word, str)
        thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        if doc_id is None:
            doc_id = cols[0]
        cols.remove(doc_id)
        if word_pos is None:
            word_pos = cols[0]
        cols.remove(word_pos)
        if word is None:
            word = cols[0]
        used_cols = [doc_id, word_pos, word]
        data_ = data[used_cols]
        param_rows = [('THREAD_RATIO', None, thread_ratio, None)]
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        result_tbl = "#PAL_CRF_INFERENCE_RESULT_TBL_{}_{}".format(self.id, unique_id)
        try:
            self._call_pal_auto(conn,
                                'PAL_CRF_INFERENCE',
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

    def create_model_state(self, model=None, function=None,
                           pal_funcname='PAL_CRF',
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

            A placeholder parameter, not effective for CRF.

        pal_funcname : int or str, optional
            PAL function name. Must be a valid PAL procedure that supports model state.

            Defaults to 'PAL_CRF'.

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
