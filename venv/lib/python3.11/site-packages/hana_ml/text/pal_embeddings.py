"""
Embeddings inside HANA

    * :class:`PALEmbeddings`
"""

#pylint: disable=redefined-builtin
#pylint: disable=bare-except
import uuid
import logging
from hdbcli import dbapi
from hana_ml.algorithms.pal.pal_base import (
    PALBase,
    ParameterTable,
    arg,
    try_drop
)

logger = logging.getLogger(__name__)#pylint: disable=invalid-name

class PALEmbeddingsBase(PALBase):
    """
    PAL embeddings base class.
    """
    def __init__(self, model_version=None, max_token_num=None):
        super(PALEmbeddingsBase, self).__init__()
        self.result_ = None
        self.connection_context = None
        self.embedding_col = None
        self.target = None
        self.model_version = arg('model_version', model_version, str)
        self.max_token_num = arg('max_token_num', max_token_num, int)

    def _fit_transform(self, data, key, target, thread_number=None, batch_size=None, is_query=None, max_token_num=None):
        """
        Predict the embeddings.

        Parameters:
        -----------
        data: DataFrame
            Data.
        key: str
            Key.
        target: str
            Target.
        thread_number: int
            Thread number.
        batch_size: int
            Batch size.
        is_query: bool
            Use different embedding model for query purpose.
        max_token_num: int
            Maximum number of tokens.
        """
        conn = data.connection_context
        self.connection_context = conn
        self.target = target
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        embeddings_tbl = '#PAL_EMBEDDINGS_RESULT_TBL_{}_{}'.format(0, unique_id)
        stats_tbl = '#PAL_EMBEDDINGS_STATS_TBL_{}_{}'.format(0, unique_id)
        outputs = [embeddings_tbl, stats_tbl]
        param_rows = [("IS_QUERY", is_query, None, None),
                      ("BATCH_SIZE", batch_size, None, None),
                      ("MODEL_VERSION", None, None, self.model_version),
                      ("THREAD_NUMBER", thread_number, None, None),
                      ("MAX_TOKEN_NUM", max_token_num if max_token_num else self.max_token_num, None, None)]
        data_ = data.select([key, target])
        try:
            self._call_pal_auto(conn,
                                'PAL_TEXTEMBEDDING',
                                data_,
                                ParameterTable().with_data(param_rows),
                                *outputs)
        except dbapi.Error as db_err:
            msg = str(conn.hana_version())
            logger.exception("HANA version: %s. %s", msg, str(db_err))
            try_drop(conn, outputs)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, outputs)
            raise
        return conn.table(embeddings_tbl), conn.table(stats_tbl)

class PALEmbeddings(PALEmbeddingsBase):
    """
    Embeds input documents into vectors.

    Parameters:
    -----------
    model_version: str, optional
        Model version. Options are 'SAP_NEB.20240715' and 'SAP_GXY.20250407'.

        Defaults to 'SAP_NEB.20240715'.

    Attributes
    ----------
    result_ : DataFrame
        The embedding result.

    stat_ : DataFrame
        The statistics.

    Examples
    --------
    Assume we have a hana dataframe df which has two columns: 'ID' and 'TEXT' and
    then we could invoke create a PALEmbeddings instance to embed the documents into vectors.

    >>> embed = PALEmbeddings()
    >>> vectors = embed.fit_transform(data=df, key='ID', target='TEXT')
    >>> vectors.collect()
    """
    def __init__(self, model_version=None, max_token_num=None):
        super(PALEmbeddings, self).__init__(model_version=model_version, max_token_num=max_token_num)

    def fit_transform(self, data, key, target, thread_number=None, batch_size=None, is_query=None, max_token_num=None):
        """
        Getting the embeddings.

        Parameters:
        -----------
        data: DataFrame
            The input data.
        key: str
            Key column name.
        target: str
            Target column name.
        thread_number: int, optional
            Indicates the number of HTTP connections that are established simultaneously to the backend embedding service.
            The value range is from 1 to 10.

            Defaults to 6.
        batch_size: int, optional
            Indicates the number of documents that are batched into one request before sending to the embedding service.
            The value range is from 1 to 50.

            Defaults to 10.
        is_query: bool, optional
            Set to true for Asymmetric Semantic Search query embedding.

            - False : Normal embedding.
            - True : Query embedding.

            Defaults to False.

        max_token_num: int, optional
            The maximum number of tokens.

            Defaults to None. Depends on the text embedding model used.

        Returns
        -------
        DataFrame

            The result.
        """
        thread_number = arg('thread_number', thread_number, int)
        batch_size = arg('batch_size', batch_size, int)
        is_query = arg('is_query', is_query, bool)
        max_token_num = arg('max_token_num', max_token_num, int)
        self.stat_ = None
        self.result_ = None
        if isinstance(target, (list, tuple)):
            for tar_col in target:
                result, stats = self._fit_transform(data, key, tar_col, thread_number=thread_number, batch_size=batch_size, is_query=is_query, max_token_num=max_token_num)
                result = result.select([result.columns[0], result.columns[1]])
                if self.result_ is None:
                    self.result_ = result.rename_columns({result.columns[1]: result.columns[1] + '_' + tar_col})
                    self.stat_ = {tar_col: stats}
                else:
                    result = result.rename_columns({result.columns[1]: result.columns[1] + '_' + tar_col})
                    self.result_ = self.result_.set_index(result.columns[0]).join(result.set_index(result.columns[0]))
                    self.stat_[tar_col] = stats
        else:
            result, stats = self._fit_transform(data, key, target, thread_number=thread_number, batch_size=batch_size, is_query=is_query, max_token_num=max_token_num)
            self.result_ = result
            self.stat_ = stats
        self.result_ = data.set_index(key).join(self.result_.set_index(result.columns[0]))
        return self.result_
