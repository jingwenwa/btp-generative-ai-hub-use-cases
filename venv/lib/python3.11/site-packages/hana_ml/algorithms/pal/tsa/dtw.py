"""
This module contains Python wrapper for PAL fast dtw algorithm.

The following function is available:

    * :func:`dtw`

"""
#pylint:disable=line-too-long, too-many-arguments, too-many-locals, invalid-name
#pylint:disable=too-many-arguments, too-few-public-methods, too-many-locals, c-extension-no-member
import logging
import uuid
from hdbcli import dbapi
from ..pal_base import (
    ParameterTable,
    arg,
    try_drop,
    require_pal_usable,
    ListOfTuples,
    call_pal_auto_with_hint
)
logger = logging.getLogger(__name__)

def dtw(query_data,
        ref_data,
        radius=None,
        thread_ratio=None,
        distance_method=None,
        minkowski_power=None,
        alignment_method=None,
        step_pattern=None,
        save_alignment=None):
    r"""
    DTW is an abbreviation for Dynamic Time Warping. It is a method for calculating distance or similarity between two time series.
    It makes one series match the other one as much as possible by stretching or compressing one or both two.

    Parameters
    ----------

    query_data : DataFrame
        Query data for DTW, expected to be structured as follows:

            - 1st column : ID of query time-series, type INTEGER, VARCHAR or NVARCHAR.
            - 2nd column : Order(timestamps) of query time-series, type INTEGER, VARCHAR or NVARCHAR.
            - Other columns : Series data, type INTEGER, DOUBLE or DECIMAL.

    ref_data : DataFrame
        Reference data for DTW, expected to be structured as follows:

            - 1st column : ID of reference time-series, type INTEGER, VARCHAR or NVARCHAR
            - 2nd column : Order(timestamps) of reference time-series, type INTEGER, VARCHAR or NVARCHAR
            - Other columns : Series data, type INTEGER, DOUBLE or DECIMAL, must have the same cardinality(i.e. number of columns)
              as that of ``data``.

    radius : int, optional
        Specifies a constraint to restrict match curve in an area near diagonal.

        To be specific, it makes sure that the absolute difference for each pair of
        subscripts in the match curve is no greater than ``radius``.

        -1 means no such constraint, otherwise ``radius`` must be nonnegative.

        Defaults to -1.

    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to -1.

    distance_method : {'manhattan', 'euclidean', 'minkowski', 'chebyshev', 'cosine'}, optional
        Specifies the method to compute the distance between two points.

            - 'manhattan' : Manhattan distance
            - 'euclidean' : Euclidean distance
            - 'minkowski' : Minkowski distance
            - 'chebyshev' : Chebyshev distance
            - 'cosine' : Cosine distance

        Defaults to 'euclidean'.
    minkowski_power : double, optional
        Specifies the power of the Minkowski distance method.

        Only valid when ``distance_method`` is 'minkowski'.

        Defaults to 3.
    alignment_method : {'closed', 'open_begin', 'open_end', 'open'}
        Specifies the alignment constraint w.r.t. beginning and end points in reference time-series.

            - 'closed' : Both beginning and end points must be aligned.
            - 'open_end' : Only beginning point needs to be aligned.
            - 'open_begin': Only end point needs to be aligned.
            - 'open': Neither beginning nor end point need to be aligned.

        Defaults to 'closed'.

    step_pattern : int or ListOfTuples
        Specifies the type of step patterns for DTW algorithm.

        There are five predefined types of step patterns, ranging from 1 to 5.

        Users can also specify custom defined step patterns by providing a list tuples.

        Defaults to 3.

        .. note::
            A custom defined step pattern is represented either by a single triad or a tuple of consecutive triads, where
            each triad is in the form of :math:`(\Delta x, \Delta y, \omega)` with :math:`\Delta x` being the increment in
            query data index, :math:`\Delta y` being the increment in reference data index, and :math:`\omega` being the weight.

            A custom defined step pattern type is simply a list of steps patterns.

            For example, the predefined step patterns of type 5 can also be specified via custom defined
            step pattern type as follows:

            [((1,1,1), (1,0,1)), (1,1,1), ((1,1,0.5), (0,1,0.5))].

            For more details on step patterns, one may go to
            `PAL DTW`_ for reference.

            .. _PAL DTW: https://help.sap.com/viewer/319d36de4fd64ac3afbf91b1fb3ce8de/2021_01_QRC/en-US/2b949ae44191490b8a89261ed2f21728.html

    save_alignment : bool, optional
        Specifies whether to output alignment information or not.

            - True : Output the alignment information.
            - False : Do not output the alignment information.

        Defaults to False.

    Returns
    -------
    DataFrames

        DataFrame 1 : Result for DTW, structured as follows:

          - QUERY_<ID column name of query data table> : ID of the query time-series.
          - REF_<ID column name of reference data table> : ID of the reference time-series.
          - DISTANCE : DTW distance of the two series. NULL if there is no valid result.
          - WEIGHT : Total weight of match.
          - AVG_DISTANCE : Normalized distance of two time-series. NULL if WEIGHT is near 0.

        DataFrame 2 : Alignment information table, structured as follows:

          - QUERY_<ID column name of query data table> : ID of query time-series.
          - REF_<ID column name of input table> : ID of reference time-series.
          - QUERY_INDEX : Corresponding to index of query time-series.
          - REF_INDEX : Corresponding to index of reference time-series.

        DataFrame 3 : Statistics.

    Examples
    --------
    >>> res, align, stats = dtw(query_data=df_1,
                                ref_data=df_2,
    ...                         step_pattern=[((1,1,1),(1,0,1)), (1,1,1), ((1,1,0.5),(0,1,0.5))],
    ...                         save_alignment=True)
    >>> res.collect()
    """
    conn = query_data.connection_context
    require_pal_usable(conn)
    distance_map = dict(manhattan=1, euclidean=2, minkowski=3, chebyshev=4, cosine=6)
    alignment_map = dict(closed='CLOSED', open_end='OPEN_END',
                         open_begin='OPEN_BEGIN', open='OPEN_BEGIN_END')
    radius = arg('radius', radius, int)
    thread_ratio = arg('thread_ratio', thread_ratio, float)
    distance_method = arg('distance_method', distance_method, distance_map)
    alignment_method = arg('alignment_method', alignment_method, alignment_map)
    if not isinstance(step_pattern, int):
        step_pattern = arg('step_pattern', step_pattern, ListOfTuples)
    pattern_type = step_pattern if isinstance(step_pattern, int) else None
    minkowski_power = arg('minkowski_power', minkowski_power, float)
    save_alignment = arg('save_alignment', save_alignment, bool)

    unique_id = str(uuid.uuid1()).replace('-', '_').upper()
    outputs = ['RESULT', 'ALIGNMENT', 'STATS']
    outputs = [f'#PAL_DTW_{name}_TBL_{unique_id}' for name in outputs]
    res_tbl, align_tbl, stats_tbl = outputs
    patterns = None
    param_rows = [('WINDOW', radius, None, None),
                  ('THREAD_RATIO', None, thread_ratio, None),
                  ('DISTANCE_METHOD', distance_method, None, None),
                  ('STEP_PATTERN_TYPE', pattern_type, None, None),
                  ('BEGIN_END_ALIGNMENT', None, None, alignment_method),
                  ('MINKOWSKI_POWER', None, minkowski_power, None),
                  ('SAVE_ALIGNMENT', save_alignment, None, None)]
    if step_pattern is not None and pattern_type is None:
        patterns = [str(s).replace('((', '(').replace('))', ')') for s in step_pattern] if \
        isinstance(step_pattern, list) else None
        if patterns:
            param_rows.extend([('STEP_PATTERN', None, None, pattern) for pattern in patterns])
    try:
        sql, _ = call_pal_auto_with_hint(conn,
                                         None,
                                         'PAL_DTW',
                                         query_data,
                                         ref_data,
                                         ParameterTable().with_data(param_rows),
                                         *outputs)
        conn.execute_statement = sql
    except dbapi.Error as db_err:
        logger.exception(str(db_err))
        try_drop(conn, outputs)
        raise
    except Exception as db_err:
        logger.exception(str(db_err))
        try_drop(conn, outputs)
        raise
    return conn.table(res_tbl), conn.table(align_tbl), conn.table(stats_tbl)
