"""This module contains python wrapper for PAL PageRank algorithm.

The following class is available:

    * :class:`PageRank`
"""
#pylint: disable=consider-using-f-string
import logging
import uuid
from hdbcli import dbapi
from hana_ml.ml_base import try_drop
from .pal_base import (
    PALBase,
    ParameterTable,
    require_pal_usable
)
logger = logging.getLogger(__name__)#pylint: disable=invalid-name

def pagerank(data,
             damping=None,
             max_iter=None,
             tol=None,
             thread_ratio=None):
    r"""
    PageRank is an algorithm used by a search engine to measure the importance of website pages.
    A website page is considered more important if it receives more links from other websites.
    PageRank represents the likelihood that a visitor will visit a particular page by randomly clicking of other webpages.
    Higher rank in PageRank means greater probability of the site being reached.

    Parameters
    ----------
    data : DataFrame
        Data for predicting the class labels.
    damping : float, optional
        The damping factor d.

        Defaults to 0.85.
    max_iter : int, optional
        The maximum number of iterations of power method.
        The value 0 means no maximum number of iterations is set
        and the calculation stops when the result converges.

        Defaults to 0.
    tol : float, optional
        Specifies the stop condition.
        When the mean improvement value of ranks is less than this value,
        the program stops calculation.

        Defaults to 1e-6.
    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 0.

    Returns
    -------

    DataFrame
        Calculated rank values and corresponding node names.

    """
    return PageRank(damping,
                    max_iter,
                    tol,
                    thread_ratio).run(data)


class PageRank(PALBase):#pylint:disable=too-few-public-methods
    r"""
    A page rank model.

    Parameters
    ----------

    damping : float, optional
        The damping factor d.

        Defaults to 0.85.
    max_iter : int, optional
        The maximum number of iterations of power method.

        The value 0 means no maximum number of iterations is set
        and the calculation stops when the result converges.

        Defaults to 0.
    tol : float, optional
        Specifies the stop condition.

        When the mean improvement value of ranks is less than this value,
        the program stops calculation.

        Defaults to 1e-6.
    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 0.

    Attributes
    ----------

    None

    Examples
    --------

    Input DataFrame df:

    >>> df.collect()
       FROM_NODE    TO_NODE
    0   Node1       Node2
    1   Node1       Node3
    ...
    6   Node4       Node1
    7   Node4       Node3

    Create a PageRank instance:

    >>> pr = PageRank()
    >>> pr.run(data=df).collect()
        NODE    RANK
    0   NODE1   0.368152
    1   NODE2   0.141808
    2   NODE3   0.287962
    3   NODE4   0.202078
    """
    #pylint: disable=too-many-arguments
    def __init__(self,
                 damping=None, # float
                 max_iter=None, # int
                 tol=None, # float
                 thread_ratio=None):  # float
        super(PageRank, self).__init__()
        self.damping = self._arg('damping', damping, float)
        self.max_iter = self._arg('max_iter', max_iter, int)
        self.tol = self._arg('tol', tol, float)
        self.thread_ratio = self._arg('thread_ratio', thread_ratio, float)

    def run(self, data):
        r"""
        This method reads link information and calculates rank for each node.

        Parameters
        ----------

        data : DataFrame
            Data for predicting the class labels.

        Returns
        -------

        DataFrame
            Calculated rank values and corresponding node names.

        """
        conn = data.connection_context
        require_pal_usable(conn)
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        result_tbl = '#PAL_PAGERANK_RESULT_TBL_{}_{}'.format(self.id, unique_id)
        param_rows = [
            ('DAMPING', None, self.damping, None),
            ('MAX_ITERATION', self.max_iter, None, None),
            ('THRESHOLD', None, self.tol, None),
            ('THREAD_RATIO', None, self.thread_ratio, None)]
        try:
            self._call_pal_auto(conn,
                                'PAL_PAGERANK',
                                data,
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
