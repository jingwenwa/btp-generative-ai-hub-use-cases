"""
This module contains a Python wrapper for PAL accuracy measure algorithm.

The following function is available:

    * :func:`accuracy_measure`
"""
#pylint: disable=too-many-lines, line-too-long, relative-beyond-top-level, invalid-name, consider-using-dict-items, consider-using-f-string
#pylint: disable=c-extension-no-member, too-many-branches, too-many-statements, too-many-arguments, too-many-locals
import logging
import uuid
from hdbcli import dbapi
from ..utility import check_pal_function_exist, _map_param
from .utility import _delete_none_key_in_dict
from ..pal_base import (
    ParameterTable,
    arg,
    try_drop,
    ListOfStrings,
    require_pal_usable,
    call_pal_auto_with_hint
)
logger = logging.getLogger(__name__)

def _params_check(input_dict, param_map):
    update_params = {}
    if not input_dict or input_dict is None:
        return update_params

    for parm in input_dict:
        if parm in param_map.keys():
            if parm == 'evaluation_metric':
                if input_dict.get('evaluation_metric') is not None:
                    ac = input_dict.get('evaluation_metric')
                    if isinstance(ac, str):
                        ac = [ac]
                    acc_list = {'mpe': 'MPE', 'mse': 'MSE', 'rmse': 'RMSE',
                                'et': 'ET', 'mad': 'MAD', 'mase': 'MASE',
                                'wmape': 'WMAPE', 'smape': 'SMAPE', 'mape': 'MAPE',
                                'spec': 'SPEC'}
                    for acc in ac:
                        acc = acc.lower()
                        arg('evaluation_metric', acc, acc_list)
                    update_params['evaluation_metric'] = (ac, ListOfStrings)
            else:
                parm_val = input_dict[parm]
                arg_map = param_map[parm]
                update_params[arg_map[0]] = (arg(parm, parm_val, arg_map[1]), arg_map[1])
        else:
            err_msg = f"'{parm}' is not a valid parameter name for accuracy_measure!"
            logger.error(err_msg)
            raise KeyError(err_msg)

    return update_params

def accuracy_measure(data,
                     evaluation_metric=None,
                     ignore_zero=None,
                     alpha1=None,
                     alpha2=None,
                     massive=False,
                     group_params=None):
    r"""
    Evaluates the forecast accuracy using measures such as:

        - 'mpe': mean percentage error (MPE)
        - 'mse': mean square error (MSE)
        - 'rmse': root mean square error (RMSE)
        - 'et': error total (ET)
        - 'mad': mean absolute deviation (MAD)
        - 'mase': out-of-sample mean absolute scaled error (MASE)
        - 'wmape': weighted mean absolute percentage error (WMAPE)
        - 'smape': symmetric mean absolute percentage error (SMAPE)
        - 'mape': mean absolute percentage error (MAPE)
        - 'spec': stock-keeping-oriented prediction error costs (SPEC)

    Parameters
    ----------

    data : DataFrame

        Input data.
        In single mode:

        - If ``data`` contains 2 columns:

          - 1st column : actual data.
          - 2nd column : forecasted data.
        - If ``data`` contains 3 columns:

          - 1st column : ID.
          - 2nd column : actual data.
          - 3rd column : forecasted data.

        In massive mode (when ``massive`` is True):

        - If ``data`` contains 3 columns:

          - 1st column : Group ID.
          - 2nd column : actual data.
          - 3rd column : forecasted data.

        - If ``data`` contains 4 columns:

          - 1st column : Group ID.
          - 2nd column : ID.
          - 3rd column : actual data.
          - 4th column : forecasted data.

    evaluation_metric : str or a list of str
        Specifies the accuracy measure name(s), with valid options listed as follows:

        - 'mpe': mean percentage error (MPE)
        - 'mse': mean square error (MSE)
        - 'rmse': root mean square error (RMSE)
        - 'et': error total (ET)
        - 'mad': mean absolute deviation (MAD)
        - 'mase': out-of-sample mean absolute scaled error (MASE)
        - 'wmape': weighted mean absolute percentage error (WMAPE)
        - 'smape': symmetric mean absolute percentage error (SMAPE)
        - 'mape': mean absolute percentage error (MAPE)
        - 'spec': stock-keeping-oriented prediction error costs (SPEC)

        .. note::

          In single mode, if ``evaluation_metric`` is specified as 'spec' or contains 'spec' as one of its elements, then
          ``data`` must have 3 columns (i.e. contain an ID column). In massive mode, similarly, ``data`` must have 4 columns (i.e. contain a Group ID column and an ID column).

    ignore_zero : bool, optional
        Specifies whether or not to ignore zero values in ``data`` when calculating
        MPE or MAPE. Valid only when 'mpe' or 'mape' is specified/included in ``evaluation_metric``.

        Defaults to False, i.e., use the zero values in ``data`` when calculating MPE or MAPE.
    alpha1 : float, optional
        Specifies the unit opportunity cost parameter in the SPEC measure,
        should be no less than 0. Valid only when 'spec' is specified/included in ``evaluation_metric``.

        Defaults to 0.5.
    alpha2 : float, optional
        Specifies the unit stock-keeping cost parameter in the SPEC measure,
        should be no less than 0. Valid only when 'spec' is specified/included in ``evaluation_metric``.

        Defaults to 0.5.
    massive : bool, optional
        Specifies whether or not to use massive mode.

        - True : massive mode.
        - False : single mode.

        For parameter setting in massive mode, you could use both
        group_params (please see the example below) or the original parameters.
        Using original parameters will apply for all groups. However, if you define some parameters of a group,
        the value of all original parameter settings will not be applicable to such a group.

        An example is as follows:

        .. only:: latex

            >>> res, error = accuracy_measure(data=df,
                                              massive=True,
                                              evaluation_metric='spec',
                                              alpha2=0.4,
                                              group_params={'Group_1': {'evaluation_metric': ['spec', 'mse'], 'alpha1': 0.6}})

        .. raw:: html

            <iframe allowtransparency="true" style="border:1px solid #ccc; background: #eeffcb;"
                 src="../../_static/acc_measure_example.html" width="100%" height="60%" sandbox="">
            </iframe>

        In this example, as ``alpha1`` and ``evaluation_metric`` are set in group_params for Group_1,
        ``alpha2`` and ``evaluation_metric`` are not applicable to Group_1.

        Defaults to False.

    group_params : dict, optional
        If massive mode is activated (``massive`` is True), input data for accuracy_measure shall be divided into different
        groups with different parameters applied. This parameter specifies the parameter values of different groups in a dict format,
        where keys correspond to group IDs while values should be a dict for parameter value assignments.

        An example is as follows:

        .. only:: latex

            >>> res, error = accuracy_measure(data=df,
                                              massive=True,
                                              group_params={'Group_1': {'evaluation_metric': 'spec', 'alpha1': 0.6},
                                                            'Group_2': {'evaluation_metric': 'spec', 'alpha2': 0.7}})

        .. raw:: html

            <iframe allowtransparency="true" style="border:1px solid #ccc; background: #eeffcb;"
                 src="../../_static/acc_measure_group_example.html" width="100%" height="60%" sandbox="">
            </iframe>

        Valid only when ``massive`` is True and defaults to None.

    Returns
    -------
    DataFrame 1
        Result of the forecast accuracy measurement, structured as follows:

        - STAT_NAME: Name of accuracy measures.
        - STAT_VALUE: Value of accuracy measures.

    DataFrame 2 (optional)
        Error message.
        Only valid if ``massive`` is True.

    Examples
    --------
    Input data df:

    >>> df.collect()
        ACTUAL  FORECAST
    0   1130.0    1270.0
    1   2410.0    2340.0
    ...
    10  2345.0    2340.0
    11  2650.0    2560.0

    Perform accuracy measurement:

    >>> res = accuracy_measure(data=df,
                               evaluation_metric=['mse', 'rmse', 'mpe', 'et',
                                                  'mad', 'mase', 'wmape', 'smape',
                                                  'mape'])
    >>> res.collect()
      STAT_NAME   STAT_VALUE
    0        ET   412.000000
    1       MAD    83.500000
    ...
    7     SMAPE     0.040876
    8     WMAPE     0.037316

    """
    conn = data.connection_context
    require_pal_usable(conn)
    __init_param_dict = {'evaluation_metric' : ('MEASURE_NAME', ListOfStrings),
                         'ignore_zero' : ('IGNORE_ZERO', bool),
                         'alpha1'  : ('ALPHA1', float),
                         'alpha2'  : ('ALPHA2', float)}

    init_params = {'evaluation_metric' : evaluation_metric,
                   'ignore_zero' : ignore_zero,
                   'alpha1' : alpha1,
                   'alpha2' : alpha2}
    init_params = _delete_none_key_in_dict(init_params)
    __pal_params = {}
    param_rows = []

    massive = arg('massive', massive, bool)
    unique_id = str(uuid.uuid1()).replace('-', '_').upper()

    if massive is not True: # single mode
        __pal_params = _params_check(input_dict=init_params,
                                     param_map=__init_param_dict)

        if evaluation_metric is None:
            msg = "evaluation_metric is mandatory in accuracy_measure!"
            logger.error(msg)
            raise KeyError(msg)

        if 'spec' in evaluation_metric and len(data.columns) < 3:
            msg = 'If \'spec\' is specified in evaluation_metric, then ' +\
            'the input data must have 3 columns.'
            logger.error(msg)
            raise ValueError(msg)

        for name in __pal_params:
            value, typ = __pal_params[name]
            if name == 'evaluation_metric':
                if isinstance(value, str):
                    value = [value]
                for each_ac in value:
                    param_rows.extend([('MEASURE_NAME', None, None, each_ac)])
            else:
                tpl = [_map_param(name, value, typ)]
                param_rows.extend(tpl)

        result_tbl = "#ACCURACY_MEASURE_RESULT_{}".format(unique_id)
        try:
            call_pal_auto_with_hint(conn,
                                    None,
                                    'PAL_ACCURACY_MEASURES',
                                    data,
                                    ParameterTable().with_data(param_rows),
                                    result_tbl)
            return conn.table(result_tbl)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, result_tbl)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, result_tbl)
            raise

    # massive mode
    group_params = arg('group_params', group_params, dict)
    group_params = {} if group_params is None else group_params
    for group in group_params:
        arg('Parameters with GROUP ID ' + str(group), group_params[group], dict)

    for group in group_params:
        __pal_params[group] = _params_check(input_dict=group_params[group],
                                            param_map=__init_param_dict)
    if init_params:
        special_group_name = 'PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID'
        __pal_params[special_group_name] = _params_check(input_dict=init_params,
                                                         param_map=__init_param_dict)

    if evaluation_metric:
        if 'spec' in evaluation_metric and len(data.columns) < 4:
            msg = 'If \'spec\' is specified in evaluation_metric in massive mode, then ' +\
            'the input data must have 4 columns.'
            logger.error(msg)
            raise ValueError(msg)

    for group in __pal_params:
        for name in __pal_params[group]:
            value, typ = __pal_params[group][name]
            if name == 'evaluation_metric':
                if isinstance(value, str):
                    value = [value]
                for each_ac in value:
                    param_rows.extend([(group, 'MEASURE_NAME', None, None, each_ac)])
            else:
                tpl = [tuple([group] + list(_map_param(name, value, typ)))]
                param_rows.extend(tpl)

    outputs = ['RESULT', 'ERROR_MSG']
    outputs = ['#PAL_MASSIVE_ACCURACY_MEASURE_TBL_{}_{}'.format(tbl, unique_id)
               for tbl in outputs]
    result_tbl, error_msg_tbl = outputs
    if not (check_pal_function_exist(conn, '%MASSIVE_ACCURACY%', like=True) or \
    conn.disable_hana_execution):
        msg = 'The version of your SAP HANA does not support massive accuracy_measures!'
        logger.error(msg)
        raise ValueError(msg)
    try:
        call_pal_auto_with_hint(conn,
                                None,
                                'PAL_MASSIVE_ACCURACY_MEASURES',
                                data,
                                ParameterTable().with_data(param_rows),
                                *outputs)
    except dbapi.Error as db_err:
        logger.error(str(db_err))
        try_drop(conn, outputs)
        raise
    except Exception as db_err:
        logger.error(str(db_err))
        try_drop(conn, outputs)
        raise
    return conn.table(result_tbl), conn.table(error_msg_tbl)
