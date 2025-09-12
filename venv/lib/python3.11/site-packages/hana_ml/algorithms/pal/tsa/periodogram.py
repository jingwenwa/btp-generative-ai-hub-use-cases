"""
This module contains Python wrapper for PAL Periodogram.

The following function is available:

    * :func:`periodogram`
"""
#pylint:disable=line-too-long, attribute-defined-outside-init, unused-variable, too-many-arguments
#pylint:disable=invalid-name, too-few-public-methods, too-many-statements, too-many-locals
#pylint:disable=too-many-branches, c-extension-no-member
import logging
import uuid
from hdbcli import dbapi
from ..utility import check_pal_function_exist
from .utility import _convert_index_from_timestamp_to_int, _is_index_int
from ..pal_base import (
    ParameterTable,
    arg,
    try_drop,
    require_pal_usable,
    call_pal_auto_with_hint
)
logger = logging.getLogger(__name__)

def periodogram(data,
                key=None,
                endog=None,
                sampling_rate = None,
                num_fft = None,
                freq_range = None,
                spectrum_type = None,
                window = None,
                alpha = None,
                beta = None,
                attenuation = None,
                mode = None,
                precision = None,
                r = None):
    r"""
    Periodogram is an estimate of the spectral density of a signal or time series. It can help determine if a particular frequency is a meaningful component of the data or if it is just a random noise.


    Parameters
    ----------

    data : DataFrame
        The input data should comprise at least two columns. One is ID column, while the other is raw data.

    key : str, optional
        The ID column.

        Defaults to the first column of data if the index column of data is not provided.
        Otherwise, defaults to the index column of data.

    endog : str, optional
        The column of series to be tested.

        Defaults to the first non-key column.

    sampling_rate : float, optional
        Represents the frequency of sampling in the sequence.

        Defaults to 1.0.

    num_fft : integer, optional
        Denotes the number of DFT (Discrete Fourier Transform) points. If ``num_fft`` is smaller than the length of the input data, the input gets trimmed. If it is larger, the input gets appended with zeroes by default.

        Defaults to the length of sequence.

    freq_range : {"one_side", "two_sides"}, optional
        Reflects the desired frequency range for the result.

        Defaults to "one_side".

    spectrum_type : {"density", "spectrum"}, optional
        Indicates the chosen scale type for the power spectrum.

        Defaults to "density".

    window : str, optional
        Available input window type:

        - 'none',
        - 'bartlett',
        - 'bartlett_hann',
        - 'blackman',
        - 'blackman_harris',
        - 'bohman',
        - 'chebwin',
        - 'cosine',
        - 'flattop',
        - 'gaussian',
        - 'hamming',
        - 'hann',
        - 'kaiser',
        - 'nuttall',
        - 'parzen',
        - 'tukey'

        No default value.

    alpha : float, optional
        A window parameter for Blackman and Gaussian window.
        Only valid for Blackman and Gaussian window.
        Defaults to:
        - "Blackman" : 0.16.
        - "Gaussian" : 2.5.

    beta : float, optional
        A parameter specific to the Kaiser window.
        Only valid for Kaiser window.

        Defaults to 8.6.

    attenuation : float, optional
        A parameter specific to the Chebwin window.
        Only valid for Chebwin window.

        Defaults to 50.0.

    mode : {'symmetric', 'periodic'}, optional
        A parameter specific to the Flattop window.
        Only valid for Flattop window.

        Defaults to 'symmetric'.

    precision : {'none', 'octave'}, optional
        A parameter specific to the Flattop window.
        Only valid for Flattop window.

        Defaults to 'none'.

    r : float, optional
        A parameter specific to the Tukey window.
        Only valid for Tukey window.

        Defaults to 0.5.

    Returns
    -------
    DataFrame
        Result, structured as follows:

        - ID: ID column.
        - FREQ: Value of sample frequencies.
        - PXX: Power spectral density or power spectrum of input data.

    Examples
    --------
    >>> res = periodogram(data=df,
                          key='ID',
                          endog='X',
                          sampling_rate=100,
                          window="hamming",
                          freq_range="two_sides")
    >>> res.collect()
    """
    conn = data.connection_context
    require_pal_usable(conn)
    window_map = {'none' : 'none',
                  'bartlett' : 'bartlett',
                  'bartlett_hann' : 'bartlett_hann',
                  'blackman' : 'blackman',
                  'blackman_harris' : 'blackman_harris',
                  'bohman' : 'bohman',
                  'chebwin' : 'chebwin',
                  'cosine' : 'cosine',
                  'flattop' : 'flattop',
                  'gaussian' : 'gaussian',
                  'hamming' : 'hamming',
                  'hann' : 'hann',
                  'kaiser' : 'kaiser',
                  'nuttall' : 'nuttall',
                  'parzen' : 'parzen',
                  'tukey' : 'tukey'}
    freq_range_map = {"one_sides" : 0, "one_side" : 0,
                      "two_sides" : 1}
    spectrum_type_map = {"density" : 0, "spectrum" : 1}
    mode_map = {"symmetric" : "symmetric", "periodic" : "periodic"}
    sampling_rate = arg('sampling_rate', sampling_rate, float)
    num_fft = arg('num_fft', num_fft, int)
    freq_range = arg('freq_range', freq_range, freq_range_map)
    spectrum_type = arg('spectrum_type', spectrum_type, spectrum_type_map)
    window = arg('window', window, window_map)
    alpha = arg('alpha', alpha, float)
    beta = arg('beta', beta, float)
    attenuation = arg('attenuation', attenuation, float)
    mode = arg('mode', mode, mode_map)
    precision = arg('precision', precision, str)
    r = arg('r', r, float)

    key = arg('key', key, str)
    endog = arg('endog', endog, str)

    cols = data.columns
    if len(cols) < 2:
        msg = ("Input data should contain at least 2 columns: " +
               "one for ID, another for raw data.")
        logger.error(msg)
        raise ValueError(msg)

    if key is not None and key not in cols:
        msg = ('Please select key from name of columns!')
        logger.error(msg)
        raise ValueError(msg)

    index = data.index
    if index is not None:
        if key is None:
            if not isinstance(index, str):
                key = cols[0]
                warn_msg = "The index of data is not a single column and key is None, so the first column of data is used as key!"
                logger.warning(warn_msg)
            else:
                key = index
        else:
            if key != index:
                warn_msg = f"Discrepancy between the designated key column '{key}' and the designated index column '{index}'"
                logger.warning(warn_msg)
    else:
        if key is None:
            key = cols[0]
    cols.remove(key)

    if endog is not None:
        if endog not in cols:
            msg = 'Please select endog from name of columns!'
            logger.error(msg)
            raise ValueError(msg)
    else:
        endog = cols[0]

    data_ = data[[key] + [endog]]

    # key column type check
    is_index_int = _is_index_int(data_, key)
    if not is_index_int:
        data_ = _convert_index_from_timestamp_to_int(data_, key)

    unique_id = str(uuid.uuid1()).replace('-', '_').upper()
    res_tbl = f'#PAL_PERIODOGRAM_RESULT_TBL_{unique_id}'
    param_rows = [('SAMPLING_RATE', None, sampling_rate,  None),
                  ('NUM_FFT', num_fft, None, None),
                  ('FREQ_RANGE', freq_range, None, None),
                  ('SPECTRUM_TYPE', spectrum_type, None, None),
                  ('WINDOW', None, None, window),
                  ('ALPHA', None, alpha, None),
                  ('BETA', None, beta, None),
                  ('ATTENUATION', None, attenuation, None),
                  ('MODE', None, None, mode),
                  ('PRECISION', None, None, precision),
                  ('R', None, r, None)]
    hana_exec_flag = conn.disable_hana_execution
    if not (check_pal_function_exist(conn, '%PERIODOGRAM%', like=True) or hana_exec_flag):
        msg = 'The version of your SAP HANA does not support periodogram!'
        logger.error(msg)
        raise ValueError(msg)
    try:
        sql, _ = call_pal_auto_with_hint(conn,
                                         None,
                                         'PAL_PERIODOGRAM',
                                         data_,
                                         ParameterTable().with_data(param_rows),
                                         res_tbl)
        conn.execute_statement = sql
    except dbapi.Error as db_err:
        logger.exception(str(db_err))
        try_drop(conn, res_tbl)
        raise
    except Exception as db_err:
        logger.exception(str(db_err))
        try_drop(conn, res_tbl)
        raise
    result_df = conn.table(res_tbl)
    if not is_index_int:
        result_cols = result_df.columns
        result_int = result_df.rename_columns({result_cols[0]:'ID_RESULT'})
        data_int = data.add_id('ID_DATA', ref_col=key)
        result_df = result_int.join(data_int, 'ID_RESULT=ID_DATA').select(key, result_cols[1], result_cols[2])
    return result_df
