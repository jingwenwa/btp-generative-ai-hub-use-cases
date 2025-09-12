"""
Utilities for generating SQL to cal PAL procedures.
"""
#pylint: disable=invalid-name, line-too-long, no-else-return, too-many-statements
#pylint: disable=consider-using-f-string, super-with-arguments
import itertools
import operator
import textwrap
import sys
from functools import wraps
from hana_ml.ml_base import quotename, colspec_from_df, Table, INTEGER, DOUBLE, NVARCHAR, TIMESTAMP

if sys.version_info.major == 2:
    #pylint: disable=undefined-variable
    _INT_TYPES = (int, long)
    _TEXT_TYPES = (str, unicode)
else:
    _INT_TYPES = (int,)
    _TEXT_TYPES = (str,)

DECLARE_PARAM_ARRAYS = textwrap.dedent('''\
    DECLARE param_name VARCHAR(5000) ARRAY;
    DECLARE int_value INTEGER ARRAY;
    DECLARE double_value DOUBLE ARRAY;
    DECLARE string_value VARCHAR(5000) ARRAY;
    ''')

DECLARE_HOLIDAY_ARRAYS = textwrap.dedent('''\
    DECLARE ts TIMESTAMP ARRAY;
    DECLARE name VARCHAR(255) ARRAY;
    DECLARE lower_window INTEGER ARRAY;
    DECLARE upper_window INTEGER ARRAY;
    ''')

ONE_PARAM_ROW_TEMPLATE = textwrap.dedent('''\
    param_name[{i}] := {name};
    int_value[{i}] := {ival};
    double_value[{i}] := {dval};
    string_value[{i}] := {sval};
    ''')

ONE_HOLIDAY_ROW_TEMPLATE = textwrap.dedent('''\
    ts[{i}] := {ts};
    name[{i}] := {name};
    lower_window[{i}] := {lower_window};
    upper_window[{i}] := {upper_window};
    ''')

UNNEST = 'params = UNNEST(:param_name, :int_value, :double_value, :string_value);\n'
UNNEST_HOLIDAY = 'holidays = UNNEST(:ts, :name, :lower_window, :upper_window);\n'

ONE_MASS_PARAM_ROW_TEMPLATE = textwrap.dedent('''\
    group_id[{i}] := {group};
    param_name[{i}] := {name};
    int_value[{i}] := {ival};
    double_value[{i}] := {dval};
    string_value[{i}] := {sval};
    ''')

ONE_MASS_HOLIDAY_ROW_TEMPLATE = textwrap.dedent('''\
    holiday_group_id[{i}] := {group};
    ts[{i}] := {ts};
    name[{i}] := {name};
    lower_window[{i}] := {lower_window};
    upper_window[{i}] := {upper_window};
    ''')

UNNESTMASS = 'params = UNNEST(:group_id, :param_name, :int_value, :double_value, :string_value);\n'
UNNESTMASS_HOLIDAY = 'holidays = UNNEST(:holiday_group_id, :ts, :name, :lower_window, :upper_window);\n'

def _declare_massive_param_arrays(itype):
    declare_massive_param_arrays = textwrap.dedent('''\
        DECLARE group_id {} ARRAY;
        DECLARE param_name VARCHAR(5000) ARRAY;
        DECLARE int_value INTEGER ARRAY;
        DECLARE double_value DOUBLE ARRAY;
        DECLARE string_value VARCHAR(5000) ARRAY;
        ''').format(itype)
    return declare_massive_param_arrays

def _declare_massive_holiday_arrays(itype):
    declare_massive_holiday_arrays = textwrap.dedent('''\
        DECLARE holiday_group_id {} ARRAY;
        DECLARE ts TIMESTAMP ARRAY;
        DECLARE name VARCHAR(255) ARRAY;
        DECLARE lower_window INTEGER ARRAY;
        DECLARE upper_window INTEGER ARRAY;
        ''').format(itype)
    return declare_massive_holiday_arrays

def literal(value):
    """
    Return a SQL literal representing the given value.

    Parameters
    ----------
    value : int, float, string, or None
        Python equivalent of the desired SQL value.

    Returns
    -------
    str
        String representing the SQL equivalent of the given value.
    """
    # Eventually, we'll probably need something like this functionality
    # in the public API. We're leaving this private for now to try to
    # avoid locking ourselves into design decisions we might regret, like
    # whether a string becomes a VARCHAR or NVARCHAR literal (or even a
    # VARBINARY expression), or how we handle the types of numeric values.
    if value is None:
        return 'NULL'
    elif isinstance(value, _INT_TYPES):
        return str(value)
    elif isinstance(value, float):
        return repr(value)
    elif isinstance(value, _TEXT_TYPES):
        # This will need better unicode handling eventually. I'm not sure
        # how to do that best, given that SAP HANA has VARCHAR, NVARCHAR, and
        # VARBINARY as 3 distinct types while Python (2 or 3) has only 2
        # corresponding types, bytestrings and unicode strings.
        # For now, for how we're using this function, NVARCHAR is fine.
        return "N'{}'".format(value.replace("'", "''"))
    else:
        raise TypeError("Unexpected value of type {}".format(type(value)))

def create_params(param_rows, holiday_rows=None):
    """
    Return SQL to build a parameter table (variable) from the given rows.

    Parameters
    ----------

    param_rows : list of tuple
        Data rows for a parameter table.

    Returns
    -------

    str
        SQL text that would generate a table variable named "params"
        with the given rows.
    """
    populate_params = []
    for i, (name, ival, dval, sval) in enumerate(param_rows, 1):
        # int(operator.index(ival)) works around the fact that booleans don't
        # count as ints in SQL. Passing booleans as ints in query parameters
        # works fine in execute(), but using True in SQL where an int
        # is needed doesn't work. This is awkward, and there may be
        # a better option. Possibly query parameters.
        # (The operator.index call rejects things that don't "behave like"
        # ints, and the int call converts int subclasses (like bool) to int.)
        name, ival, dval, sval = map(literal, [
            name,
            None if ival is None else int(operator.index(int(ival))),
            dval,
            sval
        ])
        populate_params.append(ONE_PARAM_ROW_TEMPLATE.format(
            i=i, name=name, ival=ival, dval=dval, sval=sval))
    declares = [DECLARE_PARAM_ARRAYS]
    unnest = [UNNEST]
    if holiday_rows is not None:
        declares += [DECLARE_HOLIDAY_ARRAYS]
        for i, (ts, name, lower_window, upper_window) in enumerate(holiday_rows, 1):
            ts, name, lower_window, upper_window = map(literal, [
                ts,
                name,
                lower_window if lower_window is None else int(operator.index(lower_window)),
                upper_window if upper_window is None else int(operator.index(upper_window)),
            ])
            populate_params.append(ONE_HOLIDAY_ROW_TEMPLATE.format(
                i=i, ts=ts, name=name, lower_window=lower_window, upper_window=upper_window))
        unnest += [UNNEST_HOLIDAY]
    return ''.join(declares + populate_params + unnest)

def create_massive_params(param_rows, itype, holiday_rows=None):
    """
    Return SQL to build a massive parameter table (variable) from the given rows.

    Parameters
    ----------

    param_rows : list of tuple
        Data rows for a parameter table.

    Returns
    -------

    str
        SQL text that would generate a table variable named "params"
        with the given rows.
    """
    populate_params = []
    for i, (group, name, ival, dval, sval) in enumerate(param_rows, 1):
        # int(operator.index(ival)) works around the fact that booleans don't
        # count as ints in SQL. Passing booleans as ints in query parameters
        # works fine in execute(), but using True in SQL where an int
        # is needed doesn't work. This is awkward, and there may be
        # a better option. Possibly query parameters.
        # (The operator.index call rejects things that don't "behave like"
        # ints, and the int call converts int subclasses (like bool) to int.)
        group, name, ival, dval, sval = map(literal, [
            group,
            name,
            None if ival is None else int(operator.index(ival)),
            dval,
            sval
        ])
        populate_params.append(ONE_MASS_PARAM_ROW_TEMPLATE.format(
            i=i, group=group, name=name, ival=ival, dval=dval, sval=sval))
    declares = [_declare_massive_param_arrays(itype)]
    unnest = [UNNESTMASS]
    if holiday_rows is not None:
        declares += [_declare_massive_holiday_arrays(itype)]
        for i, (group, ts, name, lower_window, upper_window) in enumerate(holiday_rows, 1):
            group, ts, name, lower_window, upper_window = map(literal, [
                group,
                ts,
                name,
                lower_window if lower_window is None else int(operator.index(lower_window)),
                upper_window if upper_window is None else int(operator.index(upper_window)),
            ])
            populate_params.append(ONE_MASS_HOLIDAY_ROW_TEMPLATE.format(
                i=i, group=group, ts=ts, name=name, lower_window=lower_window, upper_window=upper_window))
        unnest += [UNNESTMASS_HOLIDAY]
    return ''.join(declares + populate_params + unnest)

def tabletype(df):
    """
    Express a DataFrame's type in SQL.

    Parameters
    ----------

    df : DataFrame
        DataFrame to take the type of.

    Returns
    -------

    str
        "TABLE (...)" SQL string representing the table type.
    """
    spec = colspec_from_df(df)
    return 'TABLE ({})'.format(', '.join(
        quotename(name)+' '+sqltype for name, sqltype in spec))

SAFETY_TEST_TEMPLATE = textwrap.dedent('''\
    DO BEGIN
    IF 0=1 THEN
        {};
    END IF;
    END''')

def safety_test(df):
    """
    Return SQL to test a dataframe's temporary-table-safety.

    If the dataframe needs local temporary tables, executing this SQL
    will throw an error.

    Parameters
    ----------

    df : DataFrame
        DataFrame to test.

    Returns
    -------

    str
        A do-nothing anonymous block that includes the argument
        dataframe's select statement inside a never-executed IF.
    """
    return SAFETY_TEST_TEMPLATE.format(df.select_statement)

def call_pal_tabvar(name, sql_tracer, with_hint, *args): # SQLTRACE added sql_tracer
    """
    Return SQL to call a PAL function, using table variables.

    Parameters
    ----------

    name : str
        Name of the PAL function to execute. Should not include the "_SYS_AFL."
        part.
    arg1, arg2, ..., argN : DataFrame, ParameterTable, or str
        Arguments for the PAL function, in the same order the PAL function
        takes them.
        DataFrames represent input tables, a ParameterTable object represents
        the parameter table, and strings represent names for output
        tables. Output names with a leading "#" will produce local temporary
        tables.

    Returns
    -------

    str
        SQL string that would call the PAL function with the given inputs
        and place output in tables with the given output names.
    """
    # pylint:disable=too-many-locals,too-many-branches
    from hana_ml.dataframe import DataFrame
    if not isinstance(name, _TEXT_TYPES):
        raise TypeError("name argument not a string - it may have been omitted")

    in_palarg_gen = ('in_'+str(i) for i in itertools.count())
    out_palarg_gen = ('out_'+str(i) for i in itertools.count())
    group_id_type = "VARCHAR(5000)"
    pal_args = []
    assign_dfs = []
    arg_dfs = []
    output_names = []
    param_rows = None
    holiday_rows = None
    # pylint:disable=protected-access
    for arg in args:
        if isinstance(arg, DataFrame):
            in_name = next(in_palarg_gen)
            record = (in_name, arg)
            if arg._ttab_handling in ('safe', 'unknown') or with_hint in ["VIRTUAL_EXEC", "NO_HANA_EXECUTION"]:
                assign_dfs.append(record)
            elif arg._ttab_handling == 'ttab':
                arg_dfs.append(record)
            else:
                raise ValueError("call_pal_tabvar can't handle input DataFrame.")
            pal_args.append(':'+in_name)
            # SQLTRACE
            if sql_tracer:
                sql_tracer.trace_object({
                    'name': in_name, # Generated name is unknown
                    'auto_name': in_name,
                    'table_type': arg.generate_table_type(),
                    'select': arg.select_statement
                }, sub_cat='auto')
        elif isinstance(arg, ParameterTable):
            if param_rows is not None:
                # I know there are a few PAL functions with no parameter table,
                # such as PAL_CHISQUARED_GOF_TEST, but I don't know any with
                # multiple parameter tables. We can adjust this if we need to.
                raise TypeError('Multiple parameter tables not supported')
            #print(arg.data)
            param_rows = arg.data if arg.data is not None else []
            # if len(param_rows) > 0 and len(param_rows[0]) == 5:
            #     group_id_type = arg.spec[0][1]
            pal_args.append(':params')
        elif isinstance(arg, HolidayTable):
            holiday_rows = arg.data if arg.data is not None else []
            pal_args.append(':holidays')
        elif isinstance(arg, _TEXT_TYPES):
            output_names.append(arg)
            pal_args.append(next(out_palarg_gen))
            # SQLTRACE Getting mapping of out variable to internal table name
            if sql_tracer:
                sql_tracer.trace_object({
                    'auto_name': 'out_'+str(len(output_names)-1),
                    'name': arg,
                    'table_type': 'auto',
                    'select': None
                }, sub_cat='auto')
        else:
            raise TypeError('Unexpected argument type {}'.format(type(arg)))

    if arg_dfs and with_hint != "NO_HANA_EXECUTION":
        header = 'DO ({})\nBEGIN\n'.format(',\n    '.join(
            'IN {} {} => {}'.format(argname, tabletype(df), df._ttab_reference)
            for argname, df in arg_dfs))
    else:
        header = 'DO\nBEGIN\n'

    if param_rows is not None:
        if len(param_rows) > 0 and len(param_rows[0]) == 5:
            param_table_creation = create_massive_params(param_rows,
                                                         group_id_type,
                                                         holiday_rows)
        elif 'MASSIVE' in name.upper():
            param_table_creation = create_massive_params(param_rows, group_id_type, holiday_rows)
        else:
            param_table_creation = create_params(param_rows, holiday_rows)
    else:
        param_table_creation = ''
    input_assignments = ''.join('{} = {};\n'.format(argname, df_in.select_statement)
                                for argname, df_in in assign_dfs)
    invocation = 'CALL _SYS_AFL.{}({});\n'.format(name, ', '.join(pal_args))
    with_hint_anonymous_block = False
    if with_hint is not None and with_hint not in ["NO_HANA_EXECUTION", "VIRTUAL_EXEC"]:
        if not with_hint.startswith('*'):
            invocation = 'CALL _SYS_AFL.{}({}) WITH HINT ({});\n'\
                .format(name, ', '.join(pal_args), with_hint)
        else:
            with_hint_anonymous_block=True
            with_hint = with_hint[1:]
    extract_outputs = ''.join(
        'CREATE {}COLUMN TABLE {} AS (SELECT * FROM :out_{});\n'.format(
            'LOCAL TEMPORARY ' if output_name.startswith('#') else '',
            quotename(output_name),
            i
        ) for i, output_name in enumerate(output_names)
    )
    if with_hint_anonymous_block:
        return (
            header
            + param_table_creation
            + input_assignments
            + invocation
            + extract_outputs
            + 'END WITH HINT ({})\n'.format(with_hint)
        )
    return (
        header
        + param_table_creation
        + input_assignments
        + invocation
        + extract_outputs
        + 'END\n'
    )

PARAMETER_TABLE_SPEC = [
    ("PARAM_NAME", NVARCHAR(5000)),
    ("INT_VALUE", INTEGER),
    ("DOUBLE_VALUE", DOUBLE),
    ("STRING_VALUE", NVARCHAR(5000))
]

HOLIDAY_TABLE_SPEC = [
    ("ts", TIMESTAMP),
    ("NAME", NVARCHAR(255)),
    ("LOWER_WINDOW", INTEGER),
    ("UPPER_WINDOW", INTEGER)
]

class HolidayTable(Table):
    """
    Represents a PAL additive model forecast holiday table to be created on SAP HANA.
    """
    def __init__(self, name=None, itype=None):
        if itype is None:
            super(HolidayTable, self).__init__(name, HOLIDAY_TABLE_SPEC)
        else:
            if 'VARCHAR' in itype:
                super(HolidayTable, self).__init__(name, [("GROUP_ID", 'NVARCHAR(100)')] +\
                HOLIDAY_TABLE_SPEC)
            else:
                super(HolidayTable, self).__init__(name, [("GROUP_ID", itype)] +\
                HOLIDAY_TABLE_SPEC)
    def with_data(self, data):
        """
        Like Table.with_data, but filters out rows with no parameter value.

        Parameters
        ----------

        data : list of tuple
            PAL additive model forecast holiday table rows.

        Returns
        -------

        HolidayTable
            New HolidayTable with data.
        """
        start_idx = 1
        if len(data) != 0:
            start_idx = len(data[0]) - 3
        filtered_data = [param for param in data
                         if any(val is not None for val in param[start_idx:])]
        return super(HolidayTable, self).with_data(filtered_data)

class ParameterTable(Table):
    """
    Represents a PAL parameter table to be created on SAP HANA.
    """
    def __init__(self, name=None, itype=None):
        if itype is None:
            super(ParameterTable, self).__init__(name, PARAMETER_TABLE_SPEC)
        else:
            if 'VARCHAR' in itype:
                super(ParameterTable, self).__init__(name, [("GROUP_ID", 'NVARCHAR(5000)')] +\
                PARAMETER_TABLE_SPEC)
            else:
                super(ParameterTable, self).__init__(name, [("GROUP_ID", itype)] +\
                PARAMETER_TABLE_SPEC)
    def with_data(self, data):
        """
        Like Table.with_data, but filters out rows with no parameter value.

        Parameters
        ----------

        data : list of tuple
            PAL parameter table rows. Rows where the only non-None element
            is the parameter name will be automatically removed.

        Returns
        -------

        ParameterTable
            New ParameterTable with data.
        """
        start_idx = 1
        if len(data) != 0:
            start_idx = len(data[0]) - 3
        filtered_data = [param for param in data
                         if any(val is not None for val in param[start_idx:])]
        return super(ParameterTable, self).with_data(filtered_data)

def trace_sql(func):
    """
    SQL tracer for PAL functions.
    """
    @wraps(func)
    def function_with_sql_tracing(*args, **kwargs):
        # SQLTRACE
        if len(args) > 1:
            conn = args[1].connection_context
        else:
            conn = None
            if kwargs.get('data') is None:
                if hasattr(args[0], 'conn_context') and args[0].conn_context is not None:
                    conn = args[0].conn_context
                elif hasattr(args[0], 'model_') and args[0].model_ is not None:
                    conn = args[0].model_.connection_context#Currently for GARCH only
            else:
                conn = kwargs.get('data').connection_context
        conn.sql_tracer.set_sql_trace(args[0], args[0].__class__.__name__, func.__name__.lower().replace('_', ''))
        return func(*args, **kwargs)
    return function_with_sql_tracing
