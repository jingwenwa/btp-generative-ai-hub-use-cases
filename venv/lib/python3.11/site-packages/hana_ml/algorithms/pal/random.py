"""
This module contains wrappers for PAL Random distribution sampling algorithms.

The following distribution functions are available:

    * :func:`bernoulli`
    * :func:`beta`
    * :func:`binomial`
    * :func:`cauchy`
    * :func:`chi_squared`
    * :func:`exponential`
    * :func:`gumbel`
    * :func:`f`
    * :func:`gamma`
    * :func:`geometric`
    * :func:`lognormal`
    * :func:`negative_binomial`
    * :func:`normal`
    * :func:`pert`
    * :func:`poisson`
    * :func:`student_t`
    * :func:`uniform`
    * :func:`weibull`
    * :func:`multinomial`
    * :func:`mcmc`
"""

# pylint: disable=too-many-lines, invalid-name
# pylint: disable=consider-using-f-string
import logging
import uuid
import numpy as np
from hdbcli import dbapi
from .pal_base import (
    Table,
    ParameterTable,
    DOUBLE,
    NVARCHAR,
    arg,
    create,
    try_drop,
    call_pal_auto_with_hint,
    require_pal_usable,
)

logger = logging.getLogger(__name__)#pylint: disable=invalid-name

def _rds(conn_context, dist_params, num_random, seed, thread_ratio):#pylint: disable=too-many-locals
    require_pal_usable(conn_context)
    num_random = arg('num_random', num_random, int)
    seed = arg('seed', seed, int)
    thread_ratio = arg('thread_ratio', thread_ratio, float)
    if num_random < 0:
        msg = 'Parameter num_random should be greater than or equal to zero.'
        logger.error(msg)
        raise ValueError(msg)
    unique_id = str(uuid.uuid1()).replace('-', '_').upper()
    dist_name = dict(dist_params)['DISTRIBUTIONNAME']
    tables = ['#{}_{}_{}'.format(dist_name, tbl_name, unique_id) for tbl_name in
              ['DISTRIBUTION_PARAMETER', 'PARAMETER', 'RESULT']]
    dist_param_tbl, general_param_tbl, res_tbl = tables
    dist_param_spec = [('NAME', NVARCHAR(100)), ('VALUE', NVARCHAR(100))]
    general_params = [('NUM_RANDOM', num_random, None, None),
                      ('SEED', seed, None, None),
                      ('THREAD_RATIO', None, thread_ratio, None)]

    try:
        create(conn_context, Table(dist_param_tbl, dist_param_spec).with_data(dist_params))
        call_pal_auto_with_hint(conn_context,
                                None, 'PAL_DISTRIBUTION_RANDOM', conn_context.table(dist_param_tbl),
                                ParameterTable(general_param_tbl).with_data(general_params),
                                res_tbl)
    except dbapi.Error as db_err:
        logger.exception(str(db_err))
        try_drop(conn_context, tables)
        raise
    except Exception as db_err:
        logger.exception(str(db_err))
        try_drop(conn_context, tables)
        raise
    return conn_context.table(res_tbl)

#For now PAL only supports multinomial, there is no point doing any
#abstraction. Maybe later if other multivariate sampling algorithms like:
#multivariate normal is supported, then we can add it.
def multinomial(conn_context, n, pvals, num_random=100, seed=None, thread_ratio=None):# pylint: disable=too-many-arguments, too-many-locals
    """
    Draw samples from a multinomial distribution.

    Parameters
    ----------

    conn_context : ConnectionContext

        Database connection object.

    n : int

        Number of trials.

    pvals : tuple of float and int

        Success fractions of each category.

    num_random : int, optional

        Specifies the number of random data to be generated.

        Defaults to 100.

    seed : int, optional

        Indicates the seed used to initialize the random number generator:

          - 0: Uses the system time.
          - Not 0: Uses the specified seed.

        .. note ::

            When multithreading is enabled, the random number sequences
            of different runs might be different even if the SEED value remains
            the same.

        Defaults to 0.
    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 0.

    Returns
    -------

    DataFrame

        Dataframe containing the generated random samples,
        structured as follows:

          - ID, type INTEGER, ID column.
          - Generated random number columns, named by appending index number
            (starting from 1 to length of `pvals`) to ``Random_P``,
            type DOUBLE. There will be as many columns here as there are values
            in ``pvals``.

    Examples
    --------
    >>> res = multinomial(conn_context=cc, n=10, pvals=(0.1, 0.2, 0.3, 0.4), num_random=10)
    >>> res.collect()
    """
    require_pal_usable(conn_context)
    n = arg('n', n, int, True)
    pvals = arg('pvals', pvals, tuple, True)
    if any((not isinstance(pval, (float, int)) or pval < 0) for pval in pvals):
        msg = ('Parameter pvals should be a tuple of non-negative floats ' +
               'and ints.')
        logger.error(msg)
        raise ValueError(msg)
    num_random = arg('num_random', num_random, int)
    seed = arg('seed', seed, int)
    thread_ratio = arg('thread_ratio', thread_ratio, float)
    if num_random < 0:
        msg = 'Parameter num_random should be greater than or equal to zero.'
        logger.error(msg)
        raise ValueError(msg)
    unique_id = str(uuid.uuid1()).replace('-', '_').upper()
    tables = ['#MULTINOMIAL_{}_{}'.format(tbl_name, unique_id) for tbl_name in
              ['DISTRIBUTION_PARAMETER', 'PARAMETER', 'RESULT']]
    dist_param_tbl, general_param_tbl, res_tbl = tables
    dist_param_spec = [('TRIALS', NVARCHAR(100))]
    dist_param_spec.extend([('P{}'.format(str(i + 1)), DOUBLE) for i in range(len(pvals))])
    dist_param_data = [(n,) + pvals]
    general_params = [('DISTRIBUTIONNAME', None, None, 'MULTINOMIAL'),
                      ('NUM_RANDOM', num_random, None, None),
                      ('SEED', seed, None, None),
                      ('THREAD_RATIO', None, thread_ratio, None)]

    try:
        create(conn_context, Table(dist_param_tbl, dist_param_spec).with_data(dist_param_data))
        call_pal_auto_with_hint(conn_context,
                                None, 'PAL_DISTRIBUTION_RANDOM_MULTIVARIATE',
                                conn_context.table(dist_param_tbl),
                                ParameterTable(general_param_tbl).with_data(general_params),
                                res_tbl)
    except dbapi.Error as db_err:
        logger.exception(str(db_err))
        try_drop(conn_context, tables)
        raise
    except Exception as db_err:
        logger.exception(str(db_err))
        try_drop(conn_context, tables)
        raise
    return conn_context.table(res_tbl)

def bernoulli(conn_context, p=0.5, num_random=100, seed=None, thread_ratio=None):
    """
    Draw samples from a Bernoulli distribution.

    Parameters
    ----------

    conn_context : ConnectionContext

        Database connection object.

    p : float, optional

        Success fraction. The value range is from 0 to 1.

        Defaults to 0.5.

    num_random : int, optional

        Specifies the number of random data to be generated.

        Defaults to 100.

    seed : int, optional

        Indicates the seed used to initialize the random number generator:

          - 0: Uses the system time.
          - Not 0: Uses the specified seed.

        .. note ::

            When multithreading is enabled, the random number sequences
            of different runs might be different even if the SEED value remains
            the same.

        Defaults to 0.
    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 0.


    Returns
    -------

    DataFrame

        Dataframe containing the generated random samples.

    Examples
    --------
    >>> res = bernoulli(conn_context=cc, p=0.5, num_random=10)
    >>> res.collect()
    """
    p = arg('p', p, float)
    if p < 0 or p > 1:
        msg = 'Parameter p should be in the range of 0 and 1.'
        logger.error(msg)
        raise ValueError(msg)
    dist_param = [('DISTRIBUTIONNAME', 'BERNOULLI'),
                  ('SUCCESS_FRACTION', p)]
    return _rds(conn_context, dist_param, num_random, seed, thread_ratio)

def beta(conn_context, a=0.5, b=0.5, num_random=100, seed=None, thread_ratio=None):# pylint: disable=too-many-arguments
    """
    Draw samples from a Beta distribution.

    Parameters
    ----------

    conn_context : ConnectionContext

        Database connection object.

    a : float, optional

        Alpha value, positive.

        Defaults to 0.5.

    b : float, optional

        Beta value, positive.

        Defaults to 0.5.

    num_random : int, optional

        Specifies the number of random data to be generated.

        Defaults to 100.

    seed : int, optional

        Indicates the seed used to initialize the random number generator:

          - 0: Uses the system time.
          - Not 0: Uses the specified seed.

        .. note ::

            When multithreading is enabled, the random number sequences
            of different runs might be different even if the SEED value remains
            the same.

        Defaults to 0.
    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 0.


    Returns
    -------

    DataFrame

        Dataframe containing the generated random samples.

    Examples
    --------
    >>> res = beta(conn_context=cc, a=0.5, b=0.5, num_random=10)
    >>> res.collect()
    """
    a = arg('a', a, float)
    b = arg('b', b, float)
    if any(x <= 0 for x in (a, b)):
        msg = 'Parameters a and b should be greater than zero.'
        logger.error(msg)
        raise ValueError(msg)
    dist_param = [('DISTRIBUTIONNAME', 'BETA'),
                  ('SHAPE1', a),
                  ('SHAPE2', b)]
    return _rds(conn_context, dist_param, num_random, seed, thread_ratio)

def binomial(conn_context, n=1, p=0.5, num_random=100, seed=None, thread_ratio=None):# pylint: disable=too-many-arguments
    """
    Draw samples from a binomial distribution.



    Parameters
    ----------

    conn_context : ConnectionContext

        Database connection object.

    n : int, optional

        Number of trials.

        Defaults to 1.

    p : float, optional

        Successful fraction. The value range is from 0 to 1.

        Defaults to 0.5.

    num_random : int, optional

        Specifies the number of random data to be generated.

        Defaults to 100.

    seed : int, optional

        Indicates the seed used to initialize the random number generator:

          - 0: Uses the system time.
          - Not 0: Uses the specified seed.

        .. note ::

            When multithreading is enabled, the random number sequences
            of different runs might be different even if the SEED value remains
            the same.

        Defaults to 0.
    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 0.


    Returns
    -------
    DataFrame

        Dataframe containing the generated random samples.

    Examples
    --------
    >>> res = binomial(conn_context=cc, n=1, p=0.5, num_random=10)
    >>> res.collect()

    """
    p = arg('p', p, float)
    n = arg('n', n, int)
    if p < 0 or p > 1:
        msg = 'Parameter p should be in the range of 0 and 1.'
        logger.error(msg)
        raise ValueError(msg)
    if n < 0:
        msg = 'Parameter n should be at least zero.'
        logger.error(msg)
        raise ValueError(msg)
    dist_param = [('DISTRIBUTIONNAME', 'BINOMIAL'),
                  ('SUCCESS_FRACTION', p),
                  ('TRIALS', n)]
    return _rds(conn_context, dist_param, num_random, seed, thread_ratio)

def cauchy(conn_context, location=0, scale=1, num_random=100, seed=None, thread_ratio=None):# pylint: disable=too-many-arguments
    """
    Draw samples from a cauchy distribution.

    Parameters
    ----------
    conn_context : ConnectionContext

        Database connection object.

    location : float, optional

        Defaults to 0.

    scale : float, optional

        Defaults to 1.

    num_random : int, optional

        Specifies the number of random data to be generated.

        Defaults to 100.

    seed : int, optional

        Indicates the seed used to initialize the random number generator:

          - 0: Uses the system time.
          - Not 0: Uses the specified seed.

        .. note ::

            When multithreading is enabled, the random number sequences
            of different runs might be different even if the SEED value remains
            the same.

        Defaults to 0.
    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 0.


    Returns
    -------
    DataFrame

        Dataframe containing the generated random samples.

    Examples
    --------
    >>> res = cauchy(conn_context=cc, location=0, scale=1, num_random=10)
    >>> res.collect()
    """
    location = arg('location', location, float)
    scale = arg('scale', scale, float)
    if scale <= 0:
        msg = 'Parameter scale should be greater than zero.'
        logger.error(msg)
        raise ValueError(msg)
    dist_param = [('DISTRIBUTIONNAME', 'CAUCHY'),
                  ('LOCATION', location),
                  ('SCALE', scale)]
    return _rds(conn_context, dist_param, num_random, seed, thread_ratio)

def chi_squared(conn_context, dof=1, num_random=100, seed=None, thread_ratio=None):
    """
    Draw samples from a chi_squared distribution.

    Parameters
    ----------

    conn_context : ConnectionContext

        Database connection object.

    dof : int, optional

        Degrees of freedom.

        Defaults to 1.

    num_random : int, optional

        Specifies the number of random data to be generated.

        Defaults to 100.

    seed : int, optional

        Indicates the seed used to initialize the random number generator:
          - 0: Uses the system time.
          - Not 0: Uses the specified seed.

        .. note ::

            When multithreading is enabled, the random number sequences
            of different runs might be different even if the SEED value remains
            the same.

        Defaults to 0.
    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 0.


    Returns
    -------

    DataFrame

        Dataframe containing the generated random samples.

    Examples
    --------
    >>> res = chi_squared(conn_context=cc, dof=1, num_random=10)
    >>> res.collect()
    """
    dof = arg('dof', dof, int)
    if dof <= 0:
        msg = 'Parameter dof should be greater than zero.'
        logger.error(msg)
        raise ValueError(msg)
    dist_param = [('DISTRIBUTIONNAME', 'CHI_SQUARED'),
                  ('DEGREES_OF_FREEDOM', dof)]
    return _rds(conn_context, dist_param, num_random, seed, thread_ratio)

def exponential(conn_context, lamb=1, num_random=100, seed=None, thread_ratio=None):
    r"""
    Draw samples from an exponential distribution.

    Parameters
    ----------
    conn_context : ConnectionContext

        Database connection object.


    lamb : float, optional

        The rate parameter, which is the inverse of the scale parameter.

        Defaults to 1.

    num_random : int, optional

        Specifies the number of random data to be generated.

        Defaults to 100.

    seed : int, optional

        Indicates the seed used to initialize the random number generator:

          - 0: Uses the system time.
          - Not 0: Uses the specified seed.

        .. note ::

            When multithreading is enabled, the random number sequences
            of different runs might be different even if the SEED value remains
            the same.

        Defaults to 0.
    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 0.


    Returns
    -------
    DataFrame

        Dataframe containing the generated random samples.

    Examples
    --------
    >>> res = exponential(conn_context=cc, scale=1, num_random=10)
    >>> res.collect()
    """
    lamb = arg('lamb', lamb, float)
    if lamb <= 0:
        msg = 'Parameter lamb should be greater than zero.'
        logger.error(msg)
        raise ValueError(msg)
    dist_param = [('DISTRIBUTIONNAME', 'EXPONENTIAL'),
                  ('RATE', lamb)]
    return _rds(conn_context, dist_param, num_random, seed, thread_ratio)

def gumbel(conn_context, location=0, scale=1, num_random=100, seed=None,# pylint: disable=too-many-arguments
           thread_ratio=None):
    r"""
    Draw samples from a Gumbel distribution, which is one of a class of
    Generalized Extreme Value (GEV) distributions used in modeling
    extreme value problems.


    Parameters
    ----------
    conn_context : ConnectionContext
        Database connection object.

    location : float, optional
        Defaults to 0.

    scale : float, optional
        Defaults to 1.

    num_random : int, optional
        Specifies the number of random data to be generated.

        Defaults to 100.

    seed : int, optional
        Indicates the seed used to initialize the random number generator:

          - 0: Uses the system time.
          - Not 0: Uses the specified seed.

        .. note ::

            When multithreading is enabled, the random number sequences
            of different runs might be different even if the SEED value remains
            the same.

        Defaults to 0.

    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 0.

    Returns
    -------
    DataFrame

        Dataframe containing the generated random samples.

    Examples
    --------
    >>> res = gumbel(conn_context=cc, location=0, scale=1, num_random=10)
    >>> res.collect()
    """
    location = arg('location', location, float)
    scale = arg('scale', scale, float)
    if scale <= 0:
        msg = 'Parameter scale should be greater than zero.'
        logger.error(msg)
        raise ValueError(msg)
    dist_param = [('DISTRIBUTIONNAME', 'EXTREME_VALUE'),
                  ('LOCATION', location),
                  ('SCALE', scale)]
    return _rds(conn_context, dist_param, num_random, seed, thread_ratio)

def f(conn_context, dof1=1, dof2=1, num_random=100, seed=None, thread_ratio=None):# pylint: disable=too-many-arguments, invalid-name
    """
    Draw samples from an f distribution.

    Parameters
    ----------

    conn_context : ConnectionContext

        Database connection object.

    dof1 : int, optional

        DEGREES_OF_FREEDOM1.

        Defaults to 1.

    dof2 : int, optional

        DEGREES_OF_FREEDOM2.

        Defaults to 1.

    num_random : int, optional

        Specifies the number of random data to be generated.

        Defaults to 100.

    seed : int, optional

        Indicates the seed used to initialize the random number generator:

          - 0: Uses the system time.
          - Not 0: Uses the specified seed.

        .. note ::
            When multithreading is enabled, the random number sequences
            of different runs might be different even if the SEED value
            remains the same.

        Defaults to 0.

    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 0.


    Returns
    -------
    DataFrame

        Dataframe containing the generated random samples.

    Examples
    --------
    >>> res = f(conn_context=cc, dof1=1, dof2=1, num_random=10)
    >>> res.collect()
    """
    dof1 = arg('dof1', dof1, float)
    dof2 = arg('dof2', dof2, float)
    if any(dof <= 0 for dof in (dof1, dof2)):
        msg = 'Parameters dof1 and dof2 should be positive.'
        logger.error(msg)
        raise ValueError(msg)
    dist_param = [('DISTRIBUTIONNAME', 'FISHER_F'),
                  ('DEGREES_OF_FREEDOM1', dof1),
                  ('DEGREES_OF_FREEDOM2', dof2)]
    return _rds(conn_context, dist_param, num_random, seed, thread_ratio)

def gamma(conn_context, shape=1, scale=1, num_random=100, seed=None, thread_ratio=None):# pylint: disable=too-many-arguments
    """
    Draw samples from a gamma distribution.

    Parameters
    ----------
    conn_context : ConnectionContext

        Database connection object.

    shape : float, optional

        Defaults to 1.

    scale : float, optional

        Defaults to 1.

    num_random : int, optional

        Specifies the number of random data to be generated.

        Defaults to 100.

    seed : int, optional

        Indicates the seed used to initialize the random number generator:

          - 0: Uses the system time.
          - Not 0: Uses the specified seed.

        .. note ::

            When multithreading is enabled, the random number sequences
            of different runs might be different even if the SEED value remains
            the same.

        Defaults to 0.

    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 0.


    Returns
    -------
    DataFrame

        Dataframe containing the generated random samples.

    Examples
    --------
    >>> res = gamma(conn_context=cc, shape=1, scale=1, num_random=10)
    >>> res.collect()

    """
    shape = arg('shape', shape, float)
    scale = arg('scale', scale, float)
    if any(x <= 0 for x in (shape, scale)):
        msg = 'Parameters shape and scale should be greater than zero.'
        logger.error(msg)
        raise ValueError(msg)
    dist_param = [('DISTRIBUTIONNAME', 'GAMMA'),
                  ('SHAPE', shape),
                  ('SCALE', scale)]
    return _rds(conn_context, dist_param, num_random, seed, thread_ratio)

def geometric(conn_context, p=0.5, num_random=100, seed=None, thread_ratio=None):# pylint: disable=too-many-arguments
    """
    Draw samples from a geometric distribution.

    Parameters
    ----------

    conn_context : ConnectionContext

        Database connection object.

    p : float, optional

        Successful fraction. The value range is from 0 to 1.

        Defaults to 0.5.

    num_random : int, optional

        Specifies the number of random data to be generated.

        Defaults to 100.

    seed : int, optional

        Indicates the seed used to initialize the random number generator:
          - 0: Uses the system time.
          - Not 0: Uses the specified seed.

        .. note::

            When multithreading is enabled, the random number sequences
            of different runs might be different even if the SEED value
            remains the same.

        Defaults to 0.

    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 0.


    Returns
    -------
    DataFrame

        Dataframe containing the generated random samples.

    Examples
    --------
    >>> res = geometric(conn_context=cc, p=0.5, num_random=10)
    >>> res.collect()

    """
    p = arg('p', p, float)
    if p < 0 or p > 1:
        msg = 'Parameter p should be in the range of 0 and 1'
        logger.error(msg)
        raise ValueError(msg)
    dist_param = [('DISTRIBUTIONNAME', 'GEOMETRIC'),
                  ('SUCCESS_FRACTION', p)]
    return _rds(conn_context, dist_param, num_random, seed, thread_ratio)

def lognormal(conn_context, mean=0, sigma=1, num_random=100, seed=None,# pylint: disable=too-many-arguments
              thread_ratio=None):
    """
    Draw samples from a lognormal distribution.

    Parameters
    ----------

    conn_context : ConnectionContext

        Database connection object.

    mean : float, optional

        Mean value of the underlying normal distribution.

        Defaults to 0.

    sigma : float, optional

        Standard deviation of the underlying normal distribution.

        Defaults to 1.

    num_random : int, optional

        Specifies the number of random data to be generated.

        Defaults to 100.

    seed : int, optional

        Indicates the seed used to initialize the random number generator:

          - 0: Uses the system time.
          - Not 0: Uses the specified seed.

        .. note ::

            When multithreading is enabled, the random number sequences
            of different runs might be different even if the SEED value
            remains the same.

        Defaults to 0.
    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 0.


    Returns
    -------
    DataFrame

        Dataframe containing the generated random samples.

    Examples
    --------
    >>> res = lognormal(conn_context=cc, mean=0, sigma=1, num_random=10)
    >>> res.collect()

    """
    mean = arg('mean', mean, float)
    sigma = arg('sigma', sigma, float)
    if sigma <= 0:
        msg = 'Parameter sigma should be greater than zero.'
        logger.error(msg)
        raise ValueError(msg)
    dist_param = [('DISTRIBUTIONNAME', 'LOGNORMAL'),
                  ('LOCATION', mean),
                  ('SCALE', sigma)]
    return _rds(conn_context, dist_param, num_random, seed, thread_ratio)

#Parameter n is related to the sucess number, and it should be int.
#However PAL also accepts float, and truncates it as int.
def negative_binomial(conn_context, n=1, p=0.5, num_random=100, seed=None,# pylint: disable=too-many-arguments
                      thread_ratio=None):
    """
    Draw samples from a negative_binomial distribution.

    Parameters
    ----------

    conn_context : ConnectionContext

        Database connection object.

    n : int, optional

        Number of successes.

        Defaults to 1.

    p : float, optional

        Successful fraction. The value range is from 0 to 1.

        Defaults to 0.5.

    num_random : int, optional

        Specifies the number of random data to be generated.

        Defaults to 100.

    seed : int, optional

        Indicates the seed used to initialize the random number generator:

          - 0: Uses the system time.
          - Not 0: Uses the specified seed.

        .. note ::

            When multithreading is enabled, the random number sequences
            of different runs might be different even if the SEED value
            remains the same.

        Defaults to 0.

    thread_ratio : float, optional

        Controls the proportion of available threads to use.

        The value range is from 0 to 1, where 0 indicates a single thread,
        and 1 indicates up to all available threads.

        Values between 0 and 1 will use that percentage of available threads.

        Values outside the range [0, 1] tell PAL to heuristically determine
        the number of threads to use.

        Defaults to 0.


    Returns
    -------
    DataFrame
        Dataframe containing the generated random samples.

    Examples
    --------
    >>> res = negative_binomial(conn_context=cc, n=1, p=0.5, num_random=10)
    >>> res.collect()

    """
    n = arg('n', n, (int, float))
    if n is not None:
        n = int(n)
    p = arg('p', p, float)
    if n <= 0:
        msg = 'Parameter n should be greater than zero.'
        logger.error(msg)
        raise ValueError(msg)
    if p < 0 or p > 1:
        msg = 'Parameter p should be in the range of 0 and 1.'
        logger.error(msg)
        raise ValueError(msg)
    dist_param = [('DISTRIBUTIONNAME', 'NEGATIVE_BINOMIAL'),
                  ('SUCCESSES', n),
                  ('SUCCESS_FRACTION', p)]
    return _rds(conn_context, dist_param, num_random, seed, thread_ratio)

def normal(conn_context, mean=0, sigma=None, variance=None, num_random=100,# pylint: disable=too-many-arguments
           seed=None, thread_ratio=None):
    """
    Draw samples from a normal distribution.

    Parameters
    ----------

    conn_context : ConnectionContext

        Database connection object.

    mean : float, optional

        Mean value.

        Defaults to 0.

    sigma : float, optional

        Standard deviation. It cannot be used together with `variance`.

        Defaults to 1.

    variance : float, optional

        Variance. It cannot be used together with `sigma`.

        Defaults to 1.

    num_random : int, optional

        Specifies the number of random data to be generated.

        Defaults to 100.

    seed : int, optional

        Indicates the seed used to initialize the random number generator:

            - 0: Uses the system time.
            - Not 0: Uses the specified seed.

        .. note ::

            When multithreading is enabled, the random number sequences
            of different runs might be different even if the SEED value
            remains the same.

        Defaults to 0.

    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 0.

    Returns
    -------
    DataFrame

        Dataframe containing the generated random samples.

    Examples
    --------
    >>> res = normal(conn_context=cc, mean=0, sigma=1, num_random=10)
    >>> res.collect()

    """
    mean = arg('mean', mean, float)
    sigma = arg('sigma', sigma, float)
    variance = arg('variance', variance, float)
    if sigma is not None and variance is not None:
        msg = ('Parameters variance and sigma cannot be used together. ' +
               'Please choose one from them.')
        logger.error(msg)
        raise ValueError(msg)
    if sigma <= 0:
        msg = 'Parameter sigma should be greater than zero.'
        logger.error(msg)
        raise ValueError(msg)
    dist_param = [('DISTRIBUTIONNAME', 'NORMAL'),
                  ('MEAN', mean),
                  ('VARIANCE', variance),
                  ('SD', sigma)]
    return _rds(conn_context, dist_param, num_random, seed, thread_ratio)

def pert(conn_context, minimum=-1, mode=0, maximum=1, scale=4,# pylint: disable=too-many-arguments
         num_random=100, seed=None, thread_ratio=None):
    """
    Draw samples from a PERT distribution.

    Parameters
    ----------

    conn_context : ConnectionContext

        Database connection object.

    minimum : int, optional

        Minimum value.

        Defaults to -1.

    mode : float, optional

        Most likely value.

        Defaults to 0.

    maximum : float, optional

        Maximum value.

        Defaults to 1.

    scale : float, optional

        Defaults to 4.

    num_random : int, optional

        Specifies the number of random data to be generated.

        Defaults to 100.

    seed : int, optional

        Indicates the seed used to initialize the random number generator:

          - 0: Uses the system time.
          - Not 0: Uses the specified seed.

        .. note ::

            When multithreading is enabled, the random number sequences
            of different runs might be different even if the SEED value
            remains the same.

        Defaults to 0.

    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 0.


    Returns
    -------
    DataFrame

        Dataframe containing the generated random samples.

    Examples
    --------
    >>> res = pert(conn_context=cc, minimum=-1, mode=0, maximum=1, num_random=10)
    >>> res.collect()

    """
    minimum = arg('minimum', minimum, float)
    mode = arg('mode', mode, float)
    maximum = arg('maximum', maximum, float)
    scale = arg('scale', scale, float)
    # MIN < MODE < MAX
    if sorted([minimum, mode, maximum]) != [minimum, mode, maximum]:
        msg = ('minimum should be less than or equal to mode, ' +
               'and mode should be less than or equal to maximum.')
        logger.error(msg)
        raise ValueError(msg)
    dist_param = [('DISTRIBUTIONNAME', 'PERT'),
                  ('MIN', minimum),
                  ('MODE', mode),
                  ('MAX', maximum),
                  ('SCALE', scale)]
    return _rds(conn_context, dist_param, num_random, seed, thread_ratio)

def poisson(conn_context, theta=1.0, num_random=100, seed=None, thread_ratio=None):
    """
    Draw samples from a poisson distribution.

    Parameters
    ----------

    conn_context : ConnectionContext

        Database connection object.

    theta : float, optional

        The average number of events in an interval.

        Defaults to 1.0.

    num_random : int, optional

        Specifies the number of random data to be generated.

        Defaults to 100.

    seed : int, optional

        Indicates the seed used to initialize the random number generator:

          - 0: Uses the system time.
          - Not 0: Uses the specified seed.

        .. note ::

            When multithreading is enabled, the random number sequences
            of different runs might be different even if the SEED value
            remains the same.

        Defaults to 0.

    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 0.


    Returns
    -------
    DataFrame

        Dataframe containing the generated random samples.

    Examples
    --------
    >>> res = poisson(conn_context=cc, theta=1, num_random=10)
    >>> res.collect()

    """
    theta = arg('theta', theta, float)
    if theta <= 0:
        msg = 'Parameter theta should be greater than zero.'
        logger.error(msg)
        raise ValueError(msg)
    dist_param = [('DISTRIBUTIONNAME', 'POISSON'),
                  ('THETA', theta)]
    return _rds(conn_context, dist_param, num_random, seed, thread_ratio)

def student_t(conn_context, dof=1, num_random=100, seed=None, thread_ratio=None):
    """
    Draw samples from a Student's t-distribution.

    Parameters
    ----------

    conn_context : ConnectionContext

        Database connection object.

    dof : float, optional

        Degrees of freedom.

        Defaults to 1.

    num_random : int, optional

        Specifies the number of random data to be generated.

        Defaults to 100.

    seed : int, optional

        Indicates the seed used to initialize the random number generator:

          - 0: Uses the system time.
          - Not 0: Uses the specified seed.

        .. note ::

            When multithreading is enabled, the random number sequences
            of different runs might be different even if the SEED value
            remains the same.

        Defaults to 0.

    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 0.


    Returns
    -------
    DataFrame

        Dataframe containing the generated random samples.

    Examples
    --------
    >>> res = student_t(conn_context=cc, dof=1, num_random=10)
    >>> res.collect()
    """
    dof = arg('dof', dof, float)
    if dof <= 0:
        msg = 'Parameter dof should be greater than zero.'
        logger.error(msg)
        raise ValueError(msg)
    dist_param = [('DISTRIBUTIONNAME', 'STUDENT_T'),
                  ('DEGREES_OF_FREEDOM', dof)]
    return _rds(conn_context, dist_param, num_random, seed, thread_ratio)

def uniform(conn_context, low=0, high=1, num_random=100, seed=None, thread_ratio=None):# pylint: disable=too-many-arguments
    """
    Draw samples from a uniform distribution.

    Parameters
    ----------

    conn_context : ConnectionContext

        Database connection object.

    low : float, optional

        The lower bound.

        Defaults to 0.

    high : float, optional
        The upper bound.

        Defaults to 1.

    num_random : int, optional

        Specifies the number of random data to be generated.

        Defaults to 100.

    seed : int, optional

        Indicates the seed used to initialize the random number generator:

          - 0: Uses the system time.
          - Not 0: Uses the specified seed.

        .. note ::

            When multithreading is enabled, the random number sequences
            of different runs might be different even if the SEED value
            remains the same.

        Defaults to 0.

    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 0.


    Returns
    -------
    DataFrame

        Dataframe containing the generated random samples.

    Examples
    --------

    Draw samples from a uniform distribution.

    >>> res = uniform(conn_context=cc, low=-1, high=1, num_random=10)
    >>> res.collect()
       ID  GENERATED_NUMBER
    0   0          0.032920
    1   1          0.201923
    2   2          0.823313
    3   3         -0.495260
    4   4         -0.138329
    5   5          0.677732
    6   6          0.685200
    7   7          0.363627
    8   8          0.024849
    9   9         -0.441779
    """
    low = arg('low', low, float)
    high = arg('high', high, float)
    if low >= high:
        msg = 'Value of low should be lower than high.'
        logger.error(msg)
        raise ValueError(msg)
    dist_param = [('DISTRIBUTIONNAME', 'UNIFORM'),
                  ('MIN', low),
                  ('MAX', high)]
    return _rds(conn_context, dist_param, num_random, seed, thread_ratio)

def weibull(conn_context, shape=1, scale=1, num_random=100, seed=None, thread_ratio=None):# pylint: disable=too-many-arguments
    """
    Draw samples from a weibull distribution.

    Parameters
    ----------

    conn_context : ConnectionContext

        Database connection object.

    shape : float, optional

        Defaults to 1.

    scales : float, optional

        Defaults to 1.

    num_random : int, optional

        Specifies the number of random data to be generated.

        Defaults to 100.

    seed : int, optional
        Indicates the seed used to initialize the random number generator:

            - 0: Uses the system time.
            - Not 0: Uses the specified seed.

        .. note ::

            When multithreading is enabled, the random number sequences
            of different runs might be different even if the SEED value
            remains the same.

        Defaults to 0.

    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 0.


    Returns
    -------
    DataFrame

        Dataframe containing the generated random samples.

    Examples
    --------

    Draw samples from a weibull distribution.

    >>> res = weibull(conn_context=cc, shape=1, scale=1, num_random=10)
    >>> res.collect()
       ID  GENERATED_NUMBER
    0   0          2.188750
    1   1          0.247628
    2   2          0.339884
    3   3          0.902187
    4   4          0.909629
    5   5          0.514740
    6   6          4.627877
    7   7          0.143767
    8   8          0.847514
    9   9          2.368169
    """
    shape = arg('shape', shape, float)
    scale = arg('scale', scale, float)
    if shape <= 0 or scale <= 0:
        msg = 'Parameters shape and scale should be greater than zero.'
        logger.error(msg)
        raise ValueError(msg)
    dist_param = [('DISTRIBUTIONNAME', 'WEIBULL'),
                  ('SHAPE', shape),
                  ('SCALE', scale)]
    return _rds(conn_context, dist_param, num_random, seed, thread_ratio)

#pylint:disable=line-too-long
def mcmc(conn_context, distribution, location=0.0,#pylint:disable=too-many-locals, too-many-arguments, too-many-statements, too-many-branches
         scale=1.0, shape=1.0, dof=1.0, chain_iter=None,
         random_state=None, init_radius=None, adapt=None,
         warmup=None, thin=None, adapt_gamma=None,
         adapt_delta=None, adapt_kappa=None, adapt_offset=None,
         adapt_init_buffer=None, adapt_term_buffer=None,
         adapt_window=None, stepsize=None, stepsize_jitter=None,
         max_depth=None, mu=None, sigma=None, xi=None,
         alpha=None, beta=None, nu=None, omega=None, L=None,#pylint:disable=redefined-outer-name
         y_min=None, lambda_=None):#pylint:disable=redefined-outer-name
    r"""
    Given a distribution, this function generates samples of the distribution
    using Markov chain  Monte Carlo simulation.

    Parameters
    ----------

    conn_context :  ConnectionContext

        Connection to HANA database.

    distribution : str

        Specifies the name of distribution.

        Valid options include:

          - 'normal' : normal distribution
          - 'skew_normal' : skew normal distribution
          - 'student_t' : student-t distribution
          - 'cauchy' : Cauchy distribution
          - 'laplace' : Laplace distribution
          - 'logistic' : Logistic distribution
          - 'gumbel' : Gumbel distribution
          - 'exponential' : Exponential distribution
          - 'chi_square' : Chi-square distribution
          - 'invchi_square' : Inverse Chi-square distribution
          - 'gamma' : Gamma distribution
          - 'weibull' : Weibull distribution
          - 'frechet' : Frechet distribution
          - 'rayleigh' : Rayleigh distribution
          - 'multinormal' : Multivariate normal distribution
          - 'multinormalprec' : Multivariate normal distribution with precision parameterization
          - 'multinormalcholesky' : Multivariate normal distribution with Cholesky parameterization
          - 'multistudent_t' : Multivariate student-t distribution
          - 'dirichlet' : Dirichlet distribution
          - 'lognormal' : Lognormal distribution
          - 'invgamma' : Inverse Gamma distribution
          - 'beta' : Beta distribution
          - 'pareto' : Pareto distribution
          - 'lomax' : Lomax distribution

        For parameterized probability density functions(PDFs) of each distribution listed as above,
        please see :ref:`Probability Density Functions<mcmc_pdfs-label>` for more details.

    location : float, optional(deprecated)

       Specifies the location parameter for a distribution.

       Valid when ``distribution`` is set as one of the following values:
       'normal', 'skew_normal', 'student_t', 'cauchy', 'laplace', 'logistic'.

       Defaults to 0.

       This parameter is deprecated, please use ``xi`` for skew normal distribution,
       and ``mu`` for other valid distributions instead.

    scale : float, optional(deprecated)

        Specifies the scale parameter for a distribution.

        Valid only when ``distribution`` is set to one of the following values:
        'normal', 'skew_normal', 'student_t', 'cauchy', 'laplace', 'logistic', 'gumbel', 'exponential'.

        Defaults to 1.0.

        This parameter is deprecated, please use ``omega`` for skew normal distribution,
        ``beta`` for gumbel and exponential distribution(the value needs to be inversed for
        exponential distribution), and ``sigma`` for other valid distributions instead.

    shape : float, optional(deprecated)

        Specifies the shape parameter for a distribution.

        Valid only when ``distribution`` is set as 'skew_normal'.

        Defaults to 1.0.

        This parameter is deprecated, please use ``alpha`` instead.
    dof : float, optional(deprecated)

        Specifies the degree of freedom of a distribution.
        Valid only when ``distribution`` is 'student_t' or 'chi_square'.

        Defaults to 1.0.

        This parameter is deprecated, please use ``nu`` instead.

    chain_iter : int, optional

        Specifies number of iterations for each Markov chain including warmup.

        Defaults to 2000.

    random_state : int, optional

        Specifies the seed used to initialize the random number generator,
        where 0 means current system time as seed, while other values are
        simply seed values.

        Defaults to 0.

    init_radius : float, optional

        Specifies the radius to initialize the process.

        Defaults to 2.0.

    adapt : bool, optional

        Specifies whether or not to use adaption.

        Defaults to True.

    warmup : int, optional

        Specifies the number of warm-up iterations.

        Defaults to half of ``chain_iter``.

    thin : int, optional

        Specifies the period for saving samples.

        Defaults to 1.

    adapt_gamma : float, optional

        Specifies the regularization scale for adaption, must be positive.

        Invalid when ``adapt`` is False.

        Defaults to 0.05.

    adapt_delta : float, optional

        Specifies the target Metropolis acceptance rate, must be restricted
        between 0 and 1(inclusive of both limits).

        Not valid when ``adapt`` is False.

        Defaults to 0.8.

    adapt_kappa : float, optional

        Specifies the relaxation exponent, must be positive.

        Not valid when ``adapt`` is False.

        Defaults to 0.75.

    adapt_offset : float, optional

        Specifies the adaption iteration offset, must be positive.

        Not valid when ``adapt`` is False.

        Defaults to 10.0.

    adapt_init_buffer : int, optional

        Specifies the width of initial fast adaption interval.

        Not valid when ``adapt`` is False.

        Defaults to 75.

    adapt_term_buffer : int, optional

        Specifies the width of terminal(final) fast adaption interval.

        Not valid when ``adapt`` is False.

        Defaults to 50.

    adapt_window : int, optional

        Specifies the initial width of slow adaption interval.

        Not valid when ``adapt`` is False.

        Defaults to 25.


    stepsize : float, optional

        Specifies the value for discretizing the time interval.

        Defaults to 1.0.

    stepsize_jitter : float, optional

        Specifies the uniform random jitter of step-size.

        Defaults to 0.

    max_depth : int, optional

        Specifies the maximum tree depth.

        Defaults to 10.

    mu : float, list or numpy.ndarray, optional

        Specifies value of parameter :math:`\mu` in a probability density function.

        Mandatory and valid only when the corresponding distribution takes
        :math:`\mu` as a parameter in its probability density function.
        See ``distribution`` for more details.

    sigma : float, list or numpy.ndarray

        Specifies the value of parameter :math:`\sigma` or :math:`\Sigma` in a
        probability density function.

        Mandatory and valid only when the corresponding distribution takes
        :math:`\sigma` or :math:`\Sigma` as a parameter in its probability density function.
        See ``distribution`` for more details.

    xi : float, optional

        Specifies the value of parameter :math:`\xi` for the probability density function
        of skew normal distribution.

        Valid only when the ``distribution`` is 'skew_normal'

    alpha : float list or numpy.ndarray, optional
        Specifies value of parameter :math:`\alpha` in a probability density function.

        Mandatory and valid only when the corresponding distribution takes
        :math:`\alpha` as a parameter in its probability density function.
        See ``distribution`` for more details.

    beta : float, optional
        Specifies value of parameter :math:`\beta` in a probability density function.

        Mandatory and valid only when the corresponding distribution takes
        :math:`\beta` as a parameter in its probability density function.
        See ``distribution`` for more details.

    nu : float, optional
        Specifies value of parameter :math:`\nu` in a probability density function.

        Mandatory and valid only when the corresponding distribution takes
        :math:`\nu` as a parameter in its probability density function.
        See ``distribution`` for more details.

    omega : float, list or numpy.ndarray, optional
        Specifies the value of parameter :math:`\omega` or :math:`\Omega` in a
        probability density function.

        Mandatory and valid only when the corresponding distribution takes
        :math:`\omega` or :math:`\Omega` as a parameter in its probability density function.
        See ``distribution`` for more details.

    L : list of numpy.ndarray, optional
        Specifies the value of parameter `L` in the probability density function of
        multivariate normal distribution with Cholesky parameterization. It should be a lower
        triangular matrix provided  in the form of either a list or a numpy.ndarray.

        Mandatory and valid only when ``distribution`` is 'multinormalcholesky'.

    y_min : float, optional
        Specifies the value of parameter :math:`y_{min}` in Pareto distribution.

        Mandatory and valid only when ``distribution`` is 'pareto'.

    lambda_ : float, optional
        Specifies the value of parameter :math:`\lambda` in Lomax distribution.

        Mandatory and valid only when ``distribution`` is 'lomax'.

    Returns
    -------
    DataFrame
       Samples of the specified distribution generated from Markov Chain Monte-Carlo process.

    Examples
    --------
    >>> res = mcmc(conn_context=cc, distribution='student_t', mu=0, sigma=1,
                   nu=1, chain_iter=50, thin=10, init_radius=0)
    >>> res.collect()
    """
    require_pal_usable(conn_context)
    dstr_param_lst = {'normal':['mu', 'sigma'],
                      'skew_normal':['xi', 'omega', 'alpha'],
                      'student_t':['nu', 'mu', 'sigma'],
                      'cauchy':['mu', 'sigma'],
                      'laplace':['mu', 'sigma'],
                      'logistic':['mu', 'sigma'],
                      'gumbel':['mu', 'beta'],
                      'exponential':['beta'],
                      'chi_square':['nu'],
                      'invchi_square':['nu'],
                      'weibull':['alpha', 'sigma'],
                      'frechet':['alpha', 'sigma'],
                      'rayleigh':['sigma'],
                      'multinormal':['mu', 'sigma'],
                      'multinormalprec':['mu', 'omega'],
                      'multinormalcholesky':['mu', 'L'],
                      'mutistudent_t':['nu', 'mu', 'sigma'],
                      'dirichlet':['alpha'],
                      'lognormal':['mu', 'sigma'],
                      'invgamma':['alpha', 'beta'],
                      'beta':['alpha', 'beta'],
                      'pareto':['y_min', 'alpha'],
                      'lomax':['lambda', 'alpha']}
    distribution = arg('distribution', distribution,
                       {dtr:dtr for dtr in dstr_param_lst},
                       True)
    location = arg('location', location, (float, int))
    shape = arg('shape', shape, (float, int))
    scale = arg('scale', scale, (float, int))
    dof = arg('dof', dof, (float, int))
    chain_iter = arg('chain_iter', chain_iter, int)
    random_state = arg('random_state', random_state, int)
    init_radius = arg('init_radius', init_radius, float)
    adapt = arg('adapt', adapt, bool)
    warmup = arg('warmup', warmup, int)
    thin = arg('thin', thin, int)
    adapt_gamma = arg('adapt_gamma', adapt_gamma, float)
    adapt_delta = arg('adapt_delta', adapt_delta, float)
    adapt_kappa = arg('adapt_kappa', adapt_kappa, float)
    adapt_offset = arg('adapt_offset', adapt_offset, float)
    adapt_init_buffer = arg('adapt_init_buffer', adapt_init_buffer,
                            int)
    adapt_term_buffer = arg('adapt_term_buffer', adapt_term_buffer, int)
    adapt_window = arg('adapt_window', adapt_window, int)
    stepsize = arg('stepsize', stepsize, float)
    stepsize_jitter = arg('stepsize_jitter', stepsize_jitter, float)
    max_depth = arg('max_depth', max_depth, int)
    dim = 1
    mu = arg('mu', mu,
             (list, np.ndarray) if 'multi' in distribution else (float, int),
             required='multi' in distribution or distribution == 'lognormal')
    if isinstance(mu, np.ndarray):
        mu = list(mu.reshape([-1]))
    if isinstance(mu, list) and 'multi' in distribution:
        dim = len(mu)
    if mu is None:
        mu = location
    sigma = arg('sigma', sigma,
                (list, np.ndarray) if distribution in ['multinormal', 'multistudent_t']\
                else (float, int),
                required=distribution in ['multinormal', 'multistudent_t'])
    if isinstance(sigma, np.ndarray):
        sigma = list(sigma.reshape([-1]))
    if sigma is None:
        sigma = scale
    xi = arg('xi', xi, float)
    if xi is None:
        xi = location
    alpha = arg('alpha', alpha,
                (list, np.ndarray) if distribution == 'dirichlet' else (float, int),
                required=distribution in ['dirichlet', 'weibull',
                                          'frechet', 'beta',
                                          'pareto', 'lomax',
                                          'invgamma'])
    if isinstance(alpha, np.ndarray):
        alpha = list(alpha.reshape([-1]))
    if isinstance(alpha, list) and 'diri' in distribution:
        dim = len(alpha)
    if alpha is None:
        alpha = shape
    beta = arg('beta', beta, (float, int),
               required=distribution in ['invgamma', 'beta'])
    if beta is None:
        beta = scale if distribution == 'gumbel' else 1.0/scale
    nu = arg('nu', nu, (float, int),
             required=distribution == 'invchi_square')
    if nu is None:
        nu = dof
    omega = arg('omega', omega,
                (list, np.ndarray) if 'normalprec' in distribution else float,
                required=distribution == 'multinormalprec')
    if isinstance(omega, np.ndarray):
        omega = list(omega.reshape([-1]))
    elif isinstance(omega, list) and isinstance(omega[0], list):
        omega = list(np.array(omega).reshape([-1]))
    L = arg('L', L, (list, np.ndarray),
            required=distribution == 'multinormalcholesky')
    if isinstance(L, np.ndarray):
        L = list(L.reshape([-1]))
    elif isinstance(L, list) and isinstance(L[0], list):
        L = list(np.array(L).reshape([-1]))
    y_min = arg('y_min', y_min, float,
                required=distribution == 'pareto')
    lambda_ = arg('lambda_', lambda_, float,
                  required=distribution == 'lomax')
    param_value = dict(mu=mu, sigma=sigma, xi=xi,
                       alpha=alpha, beta=beta,
                       nu=nu, omega=omega, L=L,
                       y_min=y_min)
    param_value['lambda'] = lambda_
    distr_param = {param.upper():[param_value[param]] if not isinstance(param_value[param], list) \
                   else param_value[param] for param in dstr_param_lst[distribution]}
    param_rows = [('DISTRIBUTION_NAME', None, None, distribution)]
    param_rows.extend([('DISTRIBUTION_PARAM', None, None,
                        str(distr_param).replace("'", '"')),
                       ('DIMENSION', dim, None, None)])
    extend_array = [('ITER', chain_iter, None, None),
                    ('RANDOM_SEED', random_state, None, None),
                    ('INIT_RADIUS', None, init_radius, None),
                    ('ADAPT_ENGAGED', adapt, None, None),
                    ('WARMUP', warmup, None, None),
                    ('THIN', thin, None, None),
                    ('STEPSIZE', stepsize, None, None),
                    ('STEPSIZE_JITTER', stepsize_jitter, None, None),
                    ('MAX_TREEDEPTH', max_depth, None, None)]
    param_rows.extend(extend_array)
    if adapt is not False:
        extend_rows = [('ADAPT_GAMMA', None, adapt_gamma, None),
                       ('ADAPT_DELTA', None, adapt_delta, None),
                       ('ADAPT_KAPPA', None, adapt_kappa, None),
                       ('ADAPT_T0', None, adapt_offset, None),
                       ('ADAPT_INIT_BUFFER', adapt_init_buffer, None, None),
                       ('ADAPT_TERM_BUFFER', adapt_term_buffer, None, None),
                       ('ADAPT_WINDOW', adapt_window, None, None)]
        param_rows.extend(extend_rows)
    unique_id = str(uuid.uuid1()).replace('-', '_').upper()
    tables = ["#PAL_MCMC_{}_TBL_{}".format(name, unique_id)
              for name in ['PARAMETER', 'RESULT']]
    param_tbl, result_tbl = tables
    try:
        call_pal_auto_with_hint(conn_context,
                                None, "PAL_MCMC",
                                ParameterTable(param_tbl).with_data(param_rows),
                                result_tbl)
    except dbapi.Error as db_err:
        logger.exception(str(db_err))
        try_drop(conn_context, tables)
        raise
    except Exception as db_err:
        logger.exception(str(db_err))
        try_drop(conn_context, tables)
        raise
    return conn_context.table(result_tbl)
