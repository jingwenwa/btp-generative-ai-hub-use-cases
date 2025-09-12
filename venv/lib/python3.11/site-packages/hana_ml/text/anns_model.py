"""
This module contains the ANN search model class.

    * :class:`ANNSModel`
"""
#pylint: disable=too-many-instance-attributes

import uuid
import logging

from hdbcli import dbapi
from hana_ml.algorithms.pal.pal_base import (
    PALBase,
    call_pal_auto_with_hint,
    try_drop,
    arg,
    ParameterTable)

logger = logging.getLogger(__name__)

class ANNSModel(PALBase):
    """
    ANNS model create with IVF indexing.

    Parameters
    ----------
    state_id : str, optional
        The state id of the ANNS model.

        Defaults to None.

    by_doc : bool, optional
        Wether to use document or vector as input.

        Defaults to False.

    Attributes
    ----------
    state_ : DataFrame
        The state information when fit() has been invoked.

    If the parameter 'by_doc' is True and fit() has been invoked:

    embedding_result_ : DataFrame
        The embedding result.

    stat_ : DataFrame
        The statistics after predict() has been invoked.

    Examples
    --------
    Assume we have a hana dataframe df which has an 'ID' column and a 'TEXT' column and a query dataframe query_df,
    then we could invoke create an ANNSModel object:

    >>> anns = ANNSModel(by_doc=True)

    Then, invoke fit():

    >>> anns.fit(data=df, key='ID', target='TEXT')

    Then, invoke predict() to get the nearest neighbours:

    >>> query_res = anns.predict(data=query_df, key='ID', target='QUERY',
                                 is_query=True, k_nearest_neighbours=10)
    >>> query_res.collect()
    """
    connection_context = None
    init_map = {"first_k":1, "replace":2, "no_replace":3, "patent":4}

    def __init__(self, state_id=None, by_doc=False):
        super(ANNSModel, self).__init__()
        self.state_id = arg('state_id', state_id, str)
        self.by_doc = arg('by_doc', by_doc, bool)
        self.historical_state_ids = set()

    def fit(self, data, key, target,
            thread_ratio=None,
            group_number=None,
            init_type=None,
            max_iteration=None,
            exit_threshold=None,
            comment=None,
            model_version=None):
        """
        Fits the model.

        Parameters
        ----------
        data : DataFrame
            Input data.
        key : str
            Key column name.
        target : str
            Vector/doc column name.
        thread_ratio : int, optional
            The ratio of the number of threads to the number of logical processors.

            Defaults to 1.0.
        group_number : int, optional
            Number of groups (k). The value range is from 1 to the number of training records.
            This function splitting the vectors into ``group_number`` clusters, and during search time, only K_CLUSTER clusters are searched.
            If gives 1 then ANNS will perform just like KNN.

            Defaults to 1.
        init_type : {'first_k', 'replace', 'no_replace', 'patent'}, optional
            Governs the selection of initial cluster centers:

            - 'first_k': First k observations.
            - 'replace': Random with replacement.
            - 'no_replace': Random without replacement.
            - 'patent': Patent of selecting the init center (US 6,882,998 B1).

            Defaults to 'no_replace'.
        max_iteration : int, optional
            Maximum iterations when doing IVF clustering.

            Only valid when ``group_number`` is greater than 1.

            Defaults to 100.
        exit_threshold : float, optional
            Threshold (actual value) for exiting the iterations when doing IVF clustering.

            Only valid when ``group_number`` is greater than 1.
            Defaults to 1e-6.
        comment : str, optional
            Some extra comments for that model.

            Defaults to None.
        model_version : str, optional
            Indicate which embedding model version will be used.

            Defaults to the latest embedding model.
        """
        conn = data.connection_context
        self.connection_context = conn
        data_ = data.select([key, target])
        self._data = data
        self._key = key
        self._vector_col = target
        thread_ratio = arg('thread_ratio', thread_ratio, float)
        group_number = arg('group_number', group_number, int)
        init = arg('init_type', init_type, self.init_map)
        max_iteration = arg('max_iteration', max_iteration, int)
        comment = arg('comment', comment, str)
        model_version = arg('model_version', model_version, str)
        exit_threshold = arg('exit_threshold', exit_threshold, float)
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        embedding_result_tbl = '#PAL_ANNS_MODEL_RESULT_TBL_{}_{}'.format(0, unique_id)
        state_vecdb_tbl = '#PAL_ANNS_MODEL_STATE_TBL_{}_{}'.format(0, unique_id)
        if self.by_doc:
            outputs = [embedding_result_tbl, state_vecdb_tbl]
        else:
            outputs = [state_vecdb_tbl]
        param_rows = [("GROUP_NUMBER", group_number, None, None),
                      ("INIT_TYPE", init, None, None),
                      ("MAX_ITERATION", max_iteration, None, None),
                      ("EXIT_THRESHOLD", None, exit_threshold, None),
                      ("MODEL_VERSION", None, None, model_version),
                      ("COMMENT", None, None, comment),
                      ("THREAD_RATIO", None, thread_ratio, None)]
        pal_function = 'PAL_ANNS_MODEL_CREATE_BY_DOC' if self.by_doc else 'PAL_ANNS_MODEL_CREATE_BY_VECTOR'
        try:
            self._call_pal_auto(conn,
                                pal_function,
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
        if self.by_doc:
            self.embedding_result_ = conn.table(embedding_result_tbl)
        self.state_ = conn.table(state_vecdb_tbl)
        self.state_id = self.state_.select("STATE_ID").collect().iat[0, 0]
        self.historical_state_ids.add(self.state_id)

    def predict(self, data, key, target,
                thread_ratio=None,
                k_cluster=None,
                k_nearest_neighbours=None,
                batch_size=None,
                is_query=None,
                state_id=None):
        """
        Predicts the model.

        Parameters
        ----------
        data : DataFrame
            Input data.
        key : str
            Key column name.
        target : str
            Vector/doc column name.
        thread_ratio : int, optional
            The ratio of the number of threads to the number of logical processors.

            Defaults to 1.0.
        k_cluster : int, optional
            Number of groups to search (k). The value range is from 1 to the number of ``group_number`` used when model created.

            Defaults to 1.
        k_nearest_neighbours : int, optional
            The number of nearest neighbors (k).

            Defaults to 1.
        batch_size : int, optional
            The batch size. Only available when ``by_doc=True``.

            Defaults to 10.
        is_query: bool, optional
            Use query embedding or not. Only available when ``by_doc=True``.

            - True: Use query embedding.
            - False: Use normal embedding.

            Defaults to False.
        state_id : str, optional
            The state id of the ANNS model.

            Defaults to None.

        Returns
        -------
        DataFrame

            The result.

        """
        if state_id is None:
            state_id = self.state_id
        conn = data.connection_context
        self.connection_context = conn
        data_ = data.select([key, target])
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        result_tbl = '#PAL_ANNS_MODEL_PRED_TBL_{}_{}'.format(0, unique_id)
        stat_tbl = '#PAL_ANNS_MODEL_STAT_TBL_{}_{}'.format(0, unique_id)
        outputs = [result_tbl, stat_tbl]
        param_rows = [("STATE_ID", None, None, state_id),
                      ("K_CLUSTER", k_cluster, None, None),
                      ("K_NEAREST_NEIGHBOURS", k_nearest_neighbours, None, None),
                      ("BATCH_SIZE", batch_size, None, None),
                      ("IS_QUERY", is_query, None, None),
                      ("THREAD_RATIO", None, thread_ratio, None)]
        pal_function = "PAL_ANN_SEARCH_BY_DOC" if self.by_doc else "PAL_ANN_SEARCH_BY_VECTOR"
        try:
            self._call_pal_auto(conn,
                                pal_function,
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
        self.stat_ = conn.table(stat_tbl)
        return conn.table(result_tbl)

    def delete_models(self, state_ids=None, connection_context=None, force_status=None):
        """
        Deletes the models.

        Parameters
        ----------
        state_ids : list of str, optional
            The state IDs.
        connection_context : ConnectionContext, optional
            The connection context.

            Defaults to self.connection_context.

        force_status : bool, optional
            Throw the error message or force deletion, if the state id is invalid.

            - False : Does not delete the element and throw the error message.
            - True : Forcing the element to be deleted.

            Defaults to False.
        """
        if connection_context is None:
            connection_context = self.connection_context
        if state_ids is None:
            for state_id in self.historical_state_ids:
                delete_model(state_id, connection_context, force_status)
        else:
            for state_id in state_ids:
                delete_model(state_id, connection_context, force_status)

    def delete_model(self, state_id=None, connection_context=None, force_status=None):
        """
        Deletes the model.

        Parameters
        ----------
        state_id : str, optional
            The state id of the ANNS model.

            Defaults to the self.state_id.
        connection_context : ConnectionContext, optional
            The connection context.

            Defaults to the self.connection_context.

        force_status : bool, optional
            Throw the error message or force deletion, if the state id is invalid.

            - False : Does not delete the element and throw the error message.
            - True : Forcing the element to be deleted.

            Defaults to False.

        Returns
        -------
        DataFrames

            The table containing the model information.
        """
        if state_id is None:
            state_id = self.state_id
        if connection_context is None:
            connection_context = self.connection_context
        if connection_context is None:
            raise ValueError("connection_context is None")
        return delete_model(state_id, connection_context, force_status)

def delete_model(state_id=None, connection_context=None, force_status=None):
    """
    Deletes the model.

    Parameters
    ----------
    state_id : str, optional
        The state id of the ANNS model.

        Defaults to the self.state_id.
    connection_context : ConnectionContext, optional
        The connection context.

        Defaults to the self.connection_context.
    force_status : bool, optional
        Throw the error message or force deletion, if the state id is invalid.

        - False : Does not delete the element and throw the error message.
        - True : Forcing the element to be deleted.

        Defaults to False.

    Returns
    -------
    DataFrame

        The table containing the model information.
    """
    if connection_context is None:
        raise ValueError("connection_context is None")
    dummy_df = connection_context.sql("SELECT '{}' STATE_ID FROM DUMMY".format(state_id))
    unique_id = str(uuid.uuid1()).replace('-', '_').upper()
    stat_tbl = '#PAL_ANNS_MODEL_DELETE_TBL_{}_{}'.format(0, unique_id)
    outputs = [stat_tbl]
    param_rows = [("FORCE_STATUS", force_status, None, None)]
    try:
        call_pal_auto_with_hint(connection_context,
                                None,
                                "PAL_ANNS_MODEL_DELETE",
                                dummy_df,
                                ParameterTable().with_data(param_rows),
                                *outputs)
    except dbapi.Error as db_err:
        msg = str(connection_context.hana_version())
        logger.exception("HANA version: %s. %s", msg, str(db_err))
    except Exception as db_err:
        msg = str(connection_context.hana_version())
        logger.exception("HANA version: %s. %s", msg, str(db_err))
    return connection_context.table(stat_tbl)

def delete_models(state_ids, connection_context=None):
    """
    Deletes the models.

    Parameters
    ----------
    state_ids : list of str
        The state IDs.
    connection_context : ConnectionContext, optional
        The connection context.
    """
    for state_id in state_ids:
        delete_model(state_id, connection_context)

def list_models(connection_context):
    """
    List the ANNS models.

    Parameters
    ----------
    connection_context : ConnectionContext
        The connection context.
    """
    unique_id = str(uuid.uuid1()).replace('-', '_').upper()
    result_tbl = '#PAL_ANNS_MODEL_LIST_TBL_{}_{}'.format(0, unique_id)
    outputs = [result_tbl]
    param_rows = []
    try:
        call_pal_auto_with_hint(connection_context,
                                None,
                                'PAL_ANNS_MODEL_LIST',
                                ParameterTable().with_data(param_rows),
                                *outputs)
    except dbapi.Error as db_err:
        msg = str(connection_context.hana_version())
        logger.exception("HANA version: %s. %s", msg, str(db_err))
        try_drop(connection_context, outputs)
        raise
    except Exception as db_err:
        msg = str(connection_context.hana_version())
        logger.exception("HANA version: %s. %s", msg, str(db_err))
        try_drop(connection_context, outputs)
        raise
    return connection_context.table(result_tbl)
