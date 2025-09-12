"""
This module contains Python wrapper for PAL fast dtw algorithm.

The following function is available:

    * :func:`fast_dtw`
"""
#pylint: disable=line-too-long, too-many-arguments, too-many-locals
#pylint: disable=consider-using-f-string, c-extension-no-member, invalid-name
#pylint:disable=too-many-arguments, too-few-public-methods, unused-argument
import logging
import uuid
from hdbcli import dbapi
from ..pal_base import (
    ParameterTable,
    arg,
    try_drop,
    require_pal_usable,
    call_pal_auto_with_hint
)
logger = logging.getLogger(__name__)

def fast_dtw(data,
             radius,
             thread_ratio=None,
             distance_method=None,
             minkowski_power=None,
             save_alignment=None):
    r"""
    Dynamic time warping (DTW) calculates the distance or similarity between two time series. DTW stretches or compresses one or both of the two time series to make one match the other as much as possible. It also provides the optimal match between two given sequences with certain constraints and rules.
    Fast DTW is a twisted version of DTW to accelerate the computation when size of time series is huge.
    It recursively reduces the size of time series and calculate the DTW path on the reduced version, then refine the DTW path on the original ones. It may loss some accuracy of actual DTW distance in exchange of acceleration of computing.

    Parameters
    ----------

    data : DataFrame
        Input data, expected to be structured as follows:

        - ID for multiple time series.
        - Timestamps.
        - Attributes of time series.

    radius : int
        Used for balancing the accuracy and run time of DTW. Bigger value with more accuracy and slower run time. Must be positive.

    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to -1.
    distance_method : {'manhattan', 'euclidean', 'minkowski', 'chebyshev', 'cosine'}, optional

        Specifies the method to compute the distance between two points.

            - 'manhattan': Manhattan distance
            - 'euclidean': Euclidean distance
            - 'minkowski': Minkowski distance
            - 'chebyshev': Chebyshev distance
            - 'cosine': Cosine distance

        Defaults to 'euclidean'.
    minkowski_power : float, optional
        Specifies the power of the Minkowski distance method.

        Only valid when ``distance_method`` is 'minkowski'.

        Defaults to 3.
    save_alignment : bool, optional
        Specifies if output alignment information. If True, output the table.

        Defaults to False.

    Returns
    -------

    DataFrames

        DataFrame 1 : Result, structured as follows:
            - LEFT_<ID column name of input table>: ID of one time series.
            - RIGHT_<ID column name of input table>: ID of the other time series.
            - DISTANCE: DTW distance of two time series.

        DataFrame 2 : Alignment table, structured as follows:
            - LEFT_<ID column name of input table>: ID of one time series.
            - RIGHT_<ID column name of input table>: ID of the other time series.
            - LEFT_INDEX: Corresponding to index of timestamps of time series with ID of 1st column.
            - RIGHT_INDEX : Corresponding to index of timestamps of time series with ID of 2nd column.

        DataFrame 3 : Statistics.

    Examples
    --------
    >>> result, align, stats = fast_dtw(data=df, radius=5)

    """
    conn = data.connection_context
    require_pal_usable(conn)
    distance_map = {'manhattan': 1, 'euclidean': 2, 'minkowski': 3, 'chebyshev': 4, 'cosine': 6}
    radius = arg('radius', radius, int)
    thread_ratio = arg('thread_ratio', thread_ratio, float)
    distance_method = arg('distance_method', distance_method, (int, str))
    if isinstance(distance_method, str):
        distance_method = arg('distance_method', distance_method, distance_map)
    minkowski_power = arg('minkowski_power', minkowski_power, float)
    save_alignment = arg('save_alignment', save_alignment, bool)

    unique_id = str(uuid.uuid1()).replace('-', '_').upper()
    outputs = ['RESULT', 'ALIGNMENT', 'STATS']
    outputs = [f'#PAL_FAST_DTW_{name}_TBL_{unique_id}' for name in outputs]
    res_tbl, align_tbl, stats_tbl = outputs

    param_rows = [('RADIUS', radius, None, None),
                  ('THREAD_RATIO', None, thread_ratio, None),
                  ('DISTANCE_METHOD', distance_method, None, None),
                  ('MINKOWSKI_POWER', None, minkowski_power, None),
                  ('SAVE_ALIGNMENT', save_alignment, None, None)]

    try:
        call_pal_auto_with_hint(conn,
                                None,
                                'PAL_FAST_DTW',
                                data,
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

    return conn.table(res_tbl), conn.table(align_tbl), conn.table(stats_tbl)
