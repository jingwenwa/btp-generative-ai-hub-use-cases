"""
PAL-specific helper functionality.
"""
import logging
import os
import uuid
import inspect
import json
import re
import time
from hdbcli import dbapi
import hana_ml.ml_base
import hana_ml.ml_exceptions
from hana_ml.ml_exceptions import Error, FitIncompleteError
from hana_ml.ml_base import quotename
from hana_ml.dataframe import DataFrame
from hana_ml.algorithms.pal import sqlgen
from hana_ml.model_storage_services import ModelSavingServices

# Expose most contents of ml_base in pal_base for import convenience.
# pylint: disable=unused-import
# pylint: disable=attribute-defined-outside-init
# pylint: disable=bare-except
# pylint: disable=too-many-locals
# pylint: disable=too-many-arguments
# pylint: disable=line-too-long
# pylint: disable=super-with-arguments
# pylint: disable=too-many-instance-attributes
# pylint: disable=superfluous-parens
# pylint: disable=anomalous-backslash-in-string
# pylint: disable=too-many-public-methods
# pylint: disable=consider-using-f-string, raising-bad-type
# pylint: disable=broad-except
# pylint: disable=no-self-use
from hana_ml.ml_base import (
    Table,
    INTEGER,
    DOUBLE,
    NVARCHAR,
    NCLOB,
    _exist_hint,
    arg,
    create,
    materialize,
    try_drop,
    parse_one_dtype,
    execute_logged,
    logged_without_execute,
    colspec_from_df,
    ListOfStrings,
    ListOfTuples,
    TupleOfIntegers,
    _TEXT_TYPES,
    _INT_TYPES,
)
from .sqlgen import ParameterTable

logger = logging.getLogger(__name__)

MINIMUM_HANA_VERSION_PREFIX = '2.00.030'

_SELECT_HANA_VERSION = ("SELECT VALUE FROM SYS.M_SYSTEM_OVERVIEW " +
                        "WHERE NAME='Version'")
_SELECT_PAL = "SELECT * FROM SYS.AFL_PACKAGES WHERE PACKAGE_NAME='PAL'"
_SELECT_PAL_PRIVILEGE = (
    "SELECT * FROM SYS.EFFECTIVE_ROLES " +
    "WHERE USER_NAME=CURRENT_USER AND " +
    "ROLE_SCHEMA_NAME IS NULL AND "
    "ROLE_NAME IN ('AFL__SYS_AFL_AFLPAL_EXECUTE', " +
    "'AFL__SYS_AFL_AFLPAL_EXECUTE_WITH_GRANT_OPTION')"
)

def pal_param_register():
    """
    Register PAL parameters after PAL object has been initialized.
    """
    frame = inspect.currentframe()
    params = frame.f_back.f_locals
    serializable_params = {}
    for param_key, param_value in params.items():
        if param_key not in ['self', 'functionality', 'data']:
            try:
                json.dumps(param_value)
                serializable_params[param_key] = param_value
            except:
                pass
    return serializable_params

class PALBase(hana_ml.ml_base.MLBase, ModelSavingServices):
    """
    Subclass for PAL-specific functionality.
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, conn_context=None):
        super(PALBase, self).__init__(conn_context)
        ModelSavingServices.__init__(self)
        self.execute_statement = None
        self.with_hint = None
        self.with_hint_anonymous_block = False
        self._fit_param = None
        self._predict_param = None
        self._score_param = None
        self._fit_call = None
        self._predict_call = None
        self._score_call = None
        self._fit_anonymous_block = None
        self._predict_anonymous_block = None
        self._score_anonymous_block = None
        self._fit_output_table_names = None
        self._predict_output_table_names = None
        self._score_output_table_names = None
        self._fit_args = None
        self._predict_args = None
        self._score_args = None
        self.runtime = None
        self.base_fit_proc_name = None
        self.base_predict_proc_name = None
        self._convert_bigint = False
        self.model_ = None
        self.pal_funcname = None
        self.state = None
        self.real_func = None
        self.connection_context = None
        self.gen_id = "{}".format(uuid.uuid1()).replace('-', '_').upper()
        self._disable_hana_execution = False
        self._disable_arg_check = False
        self.materialize_dict = {}
        self._extended_pal_parameters = {}
        self._virtual_exec = False

    def _arg(self, attr, value, constraint, required=False):
        """
        Validate and possibly transform an argument.

        See ``hana_ml.arg`` for full documentation.
        """
        if hasattr(self, "_disable_arg_check"):
            if self._disable_arg_check:
                if isinstance(constraint, dict):
                    if value in constraint:
                        return constraint[value]
                return value
        return arg(attr, value, constraint, required)

    def _get_state_algo_id(self, funcname):
        algo_map = os.path.join(os.path.dirname(__file__), "templates",
                                "state_algorithm_map.json")
        with open(algo_map) as input_file:
            return json.load(input_file)[funcname]

    def _add_state_to_parametertable(self, parameter_table : ParameterTable):
        param_rows_ = parameter_table.data
        if param_rows_ is None:
            param_rows_ = []
        if isinstance(self.state, DataFrame):
            for row in self.state.collect().values:
                param_rows_.extend([(row[0], None, None, row[1])])
        return ParameterTable().with_data(param_rows_)

    def _extend_pal_parameter(self, pal_parameters : dict):
        self._extended_pal_parameters.update(pal_parameters)

    def _add_extended_pal_parameters_to_parametertable(self, parameter_table : ParameterTable):
        param_rows_ = parameter_table.data
        if param_rows_ is None:
            param_rows_ = []
        for kkey, vval in self._extended_pal_parameters.items():
            if isinstance(vval, str):
                param_rows_.extend([(kkey, None, None, vval)])
            elif isinstance(vval, float):
                param_rows_.extend([(kkey, None, vval, None)])
            else:
                param_rows_.extend([(kkey, vval, None, None)])
        return ParameterTable().with_data(param_rows_)

    def apply_with_hint(self, with_hint, apply_to_anonymous_block=True):
        """

        Apply with hint.

        Parameters
        ----------
        with_hint : str
            The hint clauses.

        apply_to_anonymous_block : bool, optional
            If True, it will be applied to the anonymous block.

            Defaults to True.
        """
        self.with_hint = with_hint
        if apply_to_anonymous_block:
            self.with_hint = "*{}".format(self.with_hint)

    def enable_workload_class(self, workload_class_name):
        """
        HANA WORKLOAD CLASS is applied for the statement execution.

        Parameters
        ----------
        workload_class_name : str
            The name of HANA WORKLOAD CLASS.
        """
        self.apply_with_hint('WORKLOAD_CLASS("{}")'.format(workload_class_name), True)

    def disable_arg_check(self):
        """
        Disable argument check.
        """
        self._disable_arg_check = True

    def enable_arg_check(self):
        """
        Enable argument check.
        """
        self._disable_arg_check = False

    def disable_hana_execution(self):
        """
        HANA execution will be disabled and only SQL script will be generated.
        """
        self.disable_arg_check()
        self._disable_hana_execution = True

    def enable_hana_execution(self):
        """
        HANA execution will be enabled.
        """
        self._disable_hana_execution = False

    def _create_model_state(self, model=None, function=None, pal_funcname=None, state_description=None, force=False):
        r"""
        Create PAL model state.

        Parameters
        ----------
        model : DataFrame, optional
            Specify the model for AFL state.

            Defaults to self.model\_.

        function : str, optional
            Specify the function in the unified API.

            Defaults to self.real_func.

        pal_funcname : int or str, optional
            PAL function name.

            Defaults to self.pal_funcname.

        state_description : str, optional
            Description of the state as model container.

            Defaults to None.

        force : bool, optional
            If True it will delete the existing state.

            Defaults to False.
        """
        if force and self.state is not None:
            self._delete_model_state()
        if self.state is None:
            if pal_funcname is None and self.pal_funcname is None:
                raise ValueError("Fail to infer the funcname. Need to specify the funcname.")
            if model is None and self.model_ is None:
                raise ValueError("No model is found.")
            if pal_funcname is None:
                pal_funcname = self.pal_funcname
            if function is None:
                function = self.real_func
            if model is None:
                model = self.model_
            if isinstance(pal_funcname, str):
                pal_funcname = int(self._get_state_algo_id(pal_funcname))
            if pal_funcname in (30, 31):
                if isinstance(model, (list, tuple)):
                    model = model[0]
            unique_id = str(uuid.uuid1()).replace('-', '_').upper()
            param_rows = [('ALGORITHM', pal_funcname, None, None),
                          ('STATE_DESCRIPTION', None, None, state_description),
                          ('FUNCTION', None, None, function)]
            empty_tbl = '#PAL_EMPTY_TBL_{}'.format(unique_id)
            state_tbl = '#PAL_STATE_TBL_{}'.format(unique_id)
            if isinstance(model, (list, tuple)):
                conn = model[0].connection_context
            else:
                conn = model.connection_context
            conn.create_table(table=empty_tbl,
                              table_structure={'ID': 'DOUBLE'})
            empty_df = conn.table(empty_tbl)
            model_l = [empty_df, empty_df, empty_df, empty_df, empty_df]
            if isinstance(model, (list, tuple)):
                if pal_funcname != 10:
                    for idx, ind_model in enumerate(model):
                        model_l[idx] = ind_model
                else:
                    model_l[0] = model[0]
                    param_rows.extend(self.param_rows)#pylint:disable=no-member
            else:
                model_l[0] = model
            try:
                self._call_pal_auto(conn,
                                    'PAL_CREATE_MODEL_STATE',
                                    model_l[0],
                                    model_l[1],
                                    model_l[2],
                                    model_l[3],
                                    model_l[4],
                                    ParameterTable().with_data(param_rows),
                                    state_tbl)
            except dbapi.Error as db_err:
                logger.exception(str(db_err))
                try_drop(conn, state_tbl)
                raise
            except Exception as db_err:
                logger.exception(str(db_err))
                try_drop(conn, state_tbl)
                raise
            finally:
                try_drop(conn, empty_tbl)
            self.state = conn.table(state_tbl)
        else:
            logger.warning("State already exists. Set force=True to delete the existing state.")

    def _set_model_state(self, state):
        """
        Set the model state by state information.

        Parameter
        ---------
        state: DataFrame or dict
            If state is DataFrame, it has the following structure:

                - NAME: VARCHAR(100), it mush have STATE_ID, HINT, HOST and PORT.
                - VALUE: VARCHAR(1000), the values according to NAME.
            If state is dict, the key must have STATE_ID, HINT, HOST and PORT.
        """
        if isinstance(state, DataFrame):
            self.state = state
        elif isinstance(state, dict):
            self.state = self.connection_context.sql("SELECT 'STATE_ID' NAME, '{}' VALUE FROM DUMMY UNION\
                                                      SELECT 'HINT' NAME, '{}' VALUE FROM DUMMY UNION\
                                                      SELECT 'HOST' NAME, '{}' VALUE FROM DUMMY UNION\
                                                      SELECT 'PORT' NAME, '{}' VALUE FROM DUMMY".format(state["STATE_ID"],
                                                                                                      state["HINT"],
                                                                                                      state["HOST"],
                                                                                                      state["PORT"]))

    def _delete_model_state(self, state=None):
        """
        Delete PAL model state.

        Parameters
        ----------
        state : DataFrame, optional
            Specified the state.

            Defaults to self.state.
        """
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        param_rows = []
        if state is None:
            state = self.state
        conn = state.connection_context
        deletestate_tbl = '#PAL_DELETESTATE_TBL_{}'.format(unique_id)
        try:
            self._call_pal_auto(conn,
                                'PAL_DELETE_MODEL_STATE',
                                state,
                                ParameterTable().with_data(param_rows),
                                deletestate_tbl)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, deletestate_tbl)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, deletestate_tbl)
            raise
        self.state = None
        return conn.table(deletestate_tbl)

    def enable_parallel_by_parameter_partitions(self, apply_to_anonymous_block=False):
        """
        Enable parallel by parameter partitions.

        Parameters
        ----------
        apply_to_anonymous_block : bool, optional
            If True, it will be applied to the anonymous block.

            Defaults to False.
        """
        self.with_hint = 'PARALLEL_BY_PARAMETER_PARTITIONS(p1)'
        if apply_to_anonymous_block:
            self.with_hint = "*{}".format(self.with_hint)

    def enable_no_inline(self, apply_to_anonymous_block=True):
        """
        Enable no inline.

        Parameters
        ----------
        apply_to_anonymous_block : bool, optional
            If True, it will be applied to the anonymous block.

            Defaults to True.
        """
        self.with_hint = 'no_inline'
        if apply_to_anonymous_block:
            self.with_hint = "*{}".format(self.with_hint)

    def disable_with_hint(self):
        """
        Disable with hint.
        """
        self.with_hint = None

    def enable_convert_bigint(self):
        """
        Allows the conversion from bigint to double.

        Defaults to True.
        """
        self._convert_bigint = True

    def disable_convert_bigint(self):
        """
        Disable the bigint conversion.

        Defaults to False.
        """
        self._convert_bigint = False

    @property
    def fit_hdbprocedure(self):
        """
        Returns the generated hdbprocedure for fit.
        """
        if self._fit_call is None:
            raise FitIncompleteError("Please run the fit() function first!")
        proc_name = "HANAMLAPI_BASE_{}_TRAIN".format(re.findall("_SYS_AFL.(.+)\(", self._fit_call)[0])
        self.base_fit_proc_name = proc_name
        inputs = []
        outputs = []
        count = 0
        conn = None
        for inp in self._fit_args:
            if isinstance(inp, DataFrame):
                conn = inp.connection_context
                input_tt = []
                for key, val in inp.get_table_structure().items():
                    input_tt.append("{} {}".format(quotename(key), val))
                inputs.append("in in_{} TABLE({})".format(count,
                                                          ", ".join(input_tt)))
                count = count + 1
        count = 0
        for output in self._fit_output_table_names:
            output_tt = []
            for key, val in conn.table(output).get_table_structure().items():
                output_tt.append("{} {}".format(quotename(key), val))
            outputs.append("out out_{} TABLE({})".format(count,
                                                         ", ".join(output_tt)))
            count = count + 1
        proc_header = "PROCEDURE {}(\n{})\nLANGUAGE SQLSCRIPT\nSQL SECURITY INVOKER\nAS\nBEGIN\n".format(proc_name.lower(),\
            ",\n".join(inputs + outputs))
        if hasattr(self, 'massive'):
            if self.massive is True:
                body = re.search(r'DECLARE [\s\S]+UNNEST\(:group_id, :param_name, :int_value, :double_value, :string_value\);', self._fit_anonymous_block).group(0)
            else:
                body = re.search(r'DECLARE [\s\S]+UNNEST\(:param_name, :int_value, :double_value, :string_value\);', self._fit_anonymous_block).group(0)
        else:
            body = re.search(r'DECLARE [\s\S]+UNNEST\(:param_name, :int_value, :double_value, :string_value\);', self._fit_anonymous_block).group(0)
        return proc_header + body + "\nCALL " + self._fit_call + "\nEND"

    @property
    def predict_hdbprocedure(self):
        """
        Returns the generated hdbprocedure for predict.
        """
        if self._predict_call is None:
            raise Error("Please run predict function first!")
        proc_name = "HANAMLAPI_BASE_{}_APPLY".format(re.findall("_SYS_AFL.(.+)\(", self._predict_call)[0])
        self.base_predict_proc_name = proc_name
        inputs = []
        outputs = []
        count = 0
        conn = None
        for inp in self._predict_args:
            if isinstance(inp, DataFrame):
                conn = inp.connection_context
                input_tt = []
                for key, val in inp.get_table_structure().items():
                    input_tt.append("{} {}".format(key, val))
                inputs.append("IN in_{} TABLE({})".format(count,
                                                          ", ".join(input_tt)))
                count = count + 1
        count = 0
        for output in self._predict_output_table_names:
            output_tt = []
            for key, val in conn.table(output).get_table_structure().items():
                output_tt.append("{} {}".format(key, val))
            outputs.append("OUT out_{} TABLE({})".format(count,
                                                         ", ".join(output_tt)))
            count = count + 1
        proc_header = "PROCEDURE {}(\n{})\nLANGUAGE SQLSCRIPT\nSQL SECURITY INVOKER\nREADS SQL DATA\nAS\nBEGIN\n".format(proc_name,\
            ",\n".join(inputs + outputs))
        if hasattr(self, 'massive'):
            if self.massive is True:
                body = re.search(r'DECLARE [\s\S]+UNNEST\(:group_id, :param_name, :int_value, :double_value, :string_value\);', self._predict_anonymous_block).group(0)
            else:
                body = re.search(r'DECLARE [\s\S]+UNNEST\(:param_name, :int_value, :double_value, :string_value\);', self._predict_anonymous_block).group(0)
        else:
            body = re.search(r'DECLARE [\s\S]+UNNEST\(:param_name, :int_value, :double_value, :string_value\);', self._predict_anonymous_block).group(0)
        return proc_header + body + "\nCALL " + self._predict_call + "\nEND"

    def consume_fit_hdbprocedure(self, proc_name, in_tables=None, out_tables=None):
        """
        Return the generated consume hdbprocedure for fit.

        Parameters
        ----------

        proc_name : str
            The procedure name.
        in_tables : list, optional
            The list of input table names.
        out_tables : list, optional
            The list of output table names.
        """
        result = {}
        result["base"] = self.fit_hdbprocedure
        base_proc_header = re.search(r'PROCEDURE[\s\S]+LANGUAGE SQLSCRIPT', result["base"])
        out_tbls = re.search(r'out_[\s\S]+LANGUAGE', base_proc_header.group(0)).group(0)
        if out_tbls is None:
            out_tbls = ')\n'
        else:
            out_tbls = out_tbls.replace('LANGUAGE', '')
        proc_header = "PROCEDURE {}({}LANGUAGE SQLSCRIPT\nSQL SECURITY INVOKER\nAS\nBEGIN\n".format(proc_name, out_tbls)
        if in_tables:
            in_tables = list(map(quotename, in_tables))
        if in_tables is None:
            in_tables = []
            for inp in self._fit_args:
                if isinstance(inp, DataFrame):
                    if "#" in inp.select_statement:
                        raise Error("You cannot use temporary table in the procedure. Please materialize the input DataFrame or specify the input table names.")
                    in_tables.append("({})".format(inp.select_statement))
        in_vars = []
        call_in_vars = []
        for seq, in_var in enumerate(in_tables):
            in_vars.append("in_{} = SELECT * FROM {};".format(seq, in_var))
            call_in_vars.append(":in_{}".format(seq))
        body = "\n".join(in_vars) + "\n"
        call_out_vars = []
        outputs = []
        if out_tables:
            for seq, out_var in enumerate(out_tables):
                call_out_vars.append("out_{}".format(seq))
                outputs.append("TRUNCATE TABLE {0};\nINSERT INTO {0} SELECT * FROM :{1};".format(quotename(out_var),
                                                                                                "out_{}".format(seq)))
        body = body + "CALL {} ({});".format(self.base_fit_proc_name,
                                             re.findall("\((.+)\)", self._fit_call)[0].replace(":params, ", ""))
        result["consume"] = proc_header + body + "\n" + "\n".join(outputs) + "\nEND"
        return result

    def consume_predict_hdbprocedure(self, proc_name, in_tables=None, out_tables=None):
        """
        Return the generated consume hdbprocedure for predict.

        Parameters
        ----------
        proc_name : str
            The procedure name.
        in_tables : list, optional
            The list of input table names.
        out_tables : list, optional
            The list of output table names.
        """
        result = {}
        result["base"] = self.predict_hdbprocedure
        base_proc_header = re.search(r'PROCEDURE[\s\S]+LANGUAGE SQLSCRIPT', result["base"])
        out_tbls = re.search(r'out_[\s\S]+LANGUAGE', base_proc_header.group(0)).group(0)
        if out_tbls is None:
            out_tbls = ')\n'
        else:
            out_tbls = out_tbls.replace('LANGUAGE', '')
        proc_header = "PROCEDURE {}({}LANGUAGE SQLSCRIPT\nSQL SECURITY INVOKER\nAS\nBEGIN\n".format(proc_name, out_tbls)
        if in_tables:
            in_tables = list(map(quotename, in_tables))
        if in_tables is None:
            in_tables = []
            for inp in self._predict_args:
                if isinstance(inp, DataFrame):
                    if "#" in inp.select_statement:
                        raise Error("You cannot use temporary table in the procedure. Please materialize the input DataFrame or specify the input table names.")
                    in_tables.append("({})".format(inp.select_statement))
        in_vars = []
        call_in_vars = []
        for seq, in_var in enumerate(in_tables):
            in_vars.append("in_{} = SELECT * FROM {};".format(seq, in_var))
            call_in_vars.append(":in_{}".format(seq))
        body = "\n".join(in_vars) + "\n"
        call_out_vars = []
        outputs = []
        if out_tables:
            for seq, out_var in enumerate(out_tables):
                call_out_vars.append("out_{}".format(seq))
                outputs.append("TRUNCATE TABLE {0};\nINSERT INTO {0} SELECT * FROM :{1};".format(quotename(out_var),
                                                                                                "out_{}".format(seq)))
        body = body + "CALL {} ({});".format(self.base_predict_proc_name,
                                             re.findall("\((.+)\)", self._predict_call)[0].replace(":params, ", ""))
        result["consume"] = proc_header + body + "\n" + "\n".join(outputs) + "\nEND"
        return result

    def set_scale_out(self,
                      route_to=None,
                      no_route_to=None,
                      route_by=None,
                      route_by_cardinality=None,
                      data_transfer_cost=None,
                      route_optimization_level=None,
                      workload_class=None,
                      apply_to_anonymous_block=True):
        """
        SAP HANA statement routing.

        Parameters
        ----------
        route_to : str, optional
            Routes the query to the specified volume ID or service type.

            Defaults to None.

        no_route_to : str or a list of str, optional
            Avoids query routing to a specified volume ID or service type.

            Defaults to None.

        route_by : str, optional
            Routes the query to the hosts related to the base table(s) of the specified projection view(s).

            Defaults to None.

        route_by_cardinality : str or a list of str, optional
            Routes the query to the hosts related to the base table(s) of the specified projection view(s) with the highest cardinality from the input list.

        data_transfer_cost : int, optional
            Guides the optimizer to use the weighting factor for the data transfer cost. The value 0 ignores the data transfer cost.

            Defaults to None.

        route_optimization_level : {'minimal', 'all'}, optional

            Guides the optimizer to compile with ``route_optimization_level`` 'minimal' or to default to ``route_optimization_level``.
            If the 'minimal' compiled plan is cached, then it compiles once more using the default optimization level during the first execution.
            This hint is primarily used to shorten statement routing decisions during the initial compilation.

        workload_class : str, optional
            Routes the query via workload class. ``route_to`` statement hint has higher precedence than ``workload_class`` statement hint.

            Defaults to None.

        apply_to_anonymous_block : bool, optional
            If True it will be applied to the anonymous block.

            Defaults to True.
        """
        hint_str = []
        if route_to:
            hint_str.append('ROUTE_TO({})'.format(route_to))
        if no_route_to:
            if isinstance(no_route_to, (list, tuple)):
                no_route_to = list(map(str, no_route_to))
                no_route_to = ", ".join(no_route_to)
            hint_str.append('NO_ROUTE_TO({})'.format(no_route_to))
        if route_by:
            hint_str.append('ROUTE_BY({})'.format(route_by))
        if route_by_cardinality:
            if isinstance(route_by_cardinality, (list, tuple)):
                route_by_cardinality = list(map(str, route_by_cardinality))
                route_by_cardinality = ", ".join(route_by_cardinality)
            hint_str.append('ROUTE_BY_CARDINALITY({})'.format(route_by_cardinality))
        if data_transfer_cost:
            hint_str.append('DATA_TRANSFER_COST({})'.format(str(data_transfer_cost)))
        if route_optimization_level:
            self._arg('route_optimization_level', route_optimization_level.upper(), {'minimal': 'MINIMAL', 'all': 'ALL'})
            hint_str.append('ROUTE_OPTIMIZATION_LEVEL({})'.format(route_optimization_level.upper()))
        if workload_class:
            hint_str.append('WORKLOAD_CLASS({})'.format(quotename(workload_class)))
        if len(hint_str) > 0:
            self.with_hint = ", ".join(hint_str)
        if apply_to_anonymous_block:
            self.with_hint = "*{}".format(self.with_hint)

    def get_fit_execute_statement(self):
        """
        Returns the execute_statement for training.
        """
        return self._fit_anonymous_block

    def get_predict_execute_statement(self):
        """
        Returns the execute_statement for predicting.
        """
        return self._predict_anonymous_block

    def get_score_execute_statement(self):
        """
        Returns the execute_statement for scoring.
        """
        return self._score_anonymous_block

    def get_parameters(self):
        """
        Parse sql lines containing the parameter definitions. In the sql code all the parameters
        are defined by four arrays, where the first one contains the parameter name, and one of the other
        three contains the value fitting to the parameter, while the other two are NULL. This format
        should be changed into a simple key-value based storage.

        Returns
        -------
        dict
            Dict of list of tuples, where each tuple describes a parameter like (name, value, type)
        """
        result = {}
        if self._fit_param:
            result["fit"] = self._fit_param
        if self._predict_param:
            result["predict"] = self._predict_param
        if self._score_param:
            result["score"] = self._score_param
        return result

    def get_fit_parameters(self):
        """
        Get PAL fit parameters.

        Returns
        -------
        list
            List of tuples, where each tuple describes a parameter like (name, value, type)
        """
        return self._fit_param

    def get_predict_parameters(self):
        """
        Get PAL predict parameters.

        Returns
        -------
        list
            List of tuples, where each tuple describes a parameter like (name, value, type)
        """
        return self._predict_param

    def get_score_parameters(self):
        """
        Get SAP HANA PAL score parameters.

        Returns
        -------
        list
            List of tuples, where each tuple describes a parameter like (name, value, type)
        """
        return self._score_param

    def get_fit_output_table_names(self):
        """
        Get the generated result table names in fit function.

        Returns
        -------
        list
            List of table names.
        """
        return self._fit_output_table_names

    def get_predict_output_table_names(self):
        """
        Get the generated result table names in predict function.

        Returns
        -------
        list
            List of table names.
        """
        return self._predict_output_table_names

    def get_score_output_table_names(self):
        """
        Get the generated result table names in score function.

        Returns
        -------
        list
            List of table names.
        """
        return self._score_output_table_names

    def get_model_metrics(self):
        """
        Get the model metrics.

        Returns
        -------
        DataFrame
            The model metrics.
        """
        if hasattr(self, 'statistics_'):
            return self.statistics_
        return None

    def get_score_metrics(self):
        """
        Get the score metrics.

        Returns
        -------
        DataFrame
            The score metrics.
        """
        if hasattr(self, 'score_metrics_'):
            return self.score_metrics_
        return None

    def _get_parameters(self):
        if "DECLARE group_id" in self.execute_statement:
            return _parse_params_with_group_id(_extract_params_definition_from_sql_with_group_id(self.execute_statement.split(";\n")))
        else:
            return _parse_params(_extract_params_definition_from_sql(self.execute_statement.split(";\n")))

    def get_pal_function(self):
        """
        Extract the specific function call of the SAP HANA PAL function from the sql code. Nevertheless it only detects
        the synonyms that have to be resolved afterwards.

        Returns
        -------
        dict
            The procedure name synonym: CALL "SYS_AFL.PAL_RANDOM_FORREST" (...) -> SYS_AFL.PAL_RANDOM_FORREST"
        """
        result = {}
        if self._fit_call:
            result["fit"] = self._fit_call
        if self._predict_call:
            result["predict"] = self._predict_call
        if self._score_call:
            result["score"] = self._score_call
        return result

    def _get_pal_function(self):
        if self.execute_statement:
            for line in self.execute_statement.split("\n"):
                calls = re.findall('CALL (.+)', line)
                if len(calls) > 0:
                    return calls[0]
        return None

    def _extract_output_table_names(self):
        sql = self.execute_statement.split(";\n")
        start_index, end_index = None, None
        for i, line in enumerate(sql):
            if re.match("CREATE LOCAL TEMPORARY COLUMN TABLE .+", line) and not start_index:
                start_index = i
            if re.match("END", line):
                end_index = i
                break
        if start_index is None:
            start_index = end_index
        res = []
        for line in sql[start_index:end_index]:
            res.append(re.findall(r'"(.*?)"', line)[0])
        return res

    def _call_pal_auto(self, conn_context, funcname, *args):
        def _check_pal_function_exist(connection_context, func_name, like=False):
            """
            Check the existence of pal function.
            """
            operator = '='
            if like:
                operator = 'like'
            exist = connection_context.sql('SELECT * FROM "SYS"."AFL_FUNCTIONS" WHERE AREA_NAME = \'AFLPAL\' and FUNCTION_NAME {} \'{}\';'.format(operator, func_name))
            if len(exist.collect()) > 0:
                return True
            return False
        self.runtime = None
        self.connection_context = conn_context
        start_time = time.time()
        cast_dict = {}
        _args = list(args)
        for _arg in _args:
            if isinstance(_arg, ParameterTable):
                _arg = self._add_extended_pal_parameters_to_parametertable(_arg)
        if self._convert_bigint:
            for idx, _arg in enumerate(_args):
                if isinstance(_arg, DataFrame):
                    for col_name, col_type in _arg.get_table_structure().items():
                        if 'BIGINT' in col_type :
                            cast_dict[col_name] = 'DOUBLE'
                            logger.warning("%s has been cast from %s to DOUBLE", col_name, col_type)
                    _args[idx] = _arg.cast(cast_dict)
        if any(x in funcname.upper() for x in ["INFERENCE", "PREDICT", "FORECAST", "EXPLAIN",
                                               "SCORE", "ASSIGNMENT", "PROJECT"]):
            if self.state:
                for _arg in _args:
                    if isinstance(_arg, ParameterTable):
                        _arg = self._add_state_to_parametertable(_arg)
        # changes:  the procedure name is changed for tracking
        if "LOG_ML_TRACK" in self._extended_pal_parameters:
            # check PAL procedure exists for PAL_UNIFIED_REGRESSION_TRACK
            if _check_pal_function_exist(conn_context, '%UNIFIED_REGRESSION_TRACK%', like=True):
                if funcname in ["PAL_UNIFIED_REGRESSION",
                                "PAL_UNIFIED_REGRESSION_SCORE",
                                "PAL_UNIFIED_REGRESSION_PIVOT",
                                "PAL_UNIFIED_REGRESSION_SCORE_PIVOT",
                                "PAL_UNIFIED_CLASSIFICATION",
                                "PAL_UNIFIED_CLASSIFICATION_SCORE",
                                "PAL_UNIFIED_CLASSIFICATION_PIVOT",
                                "PAL_UNIFIED_CLASSIFICATION_SCORE_PIVOT",
                                "PAL_PIPELINE_FIT",
                                "PAL_PIPELINE_EVALUATE",
                                "PAL_PIPELINE_SCORE"
                                ]:
                    funcname = funcname + '_TRACK'
                else:
                    logger.warning("PAL procedure %s does not support tracking.", funcname)
            else:
                logger.warning("HANA instance does not support TRACKING. Please check the version.")
        if self._disable_hana_execution or self._virtual_exec:
            self.execute_statement = sqlgen.call_pal_tabvar(funcname,
                                                            None,
                                                            "VIRTUAL_EXEC" if self._virtual_exec else "NO_HANA_EXECUTION",
                                                            *_args)

        else:
            self.execute_statement, materialize_dict = call_pal_auto_with_hint(conn_context,
                                                                               self.with_hint,
                                                                               funcname,
                                                                               *_args)
            self.materialize_dict.update(materialize_dict)
        self.runtime = time.time() - start_time
        try:
            call_string = self._get_pal_function()
            if call_string:
                if any(xx in call_string for xx in ["INFERENCE", "PROJECT", "PREDICT", "FORECAST", "CLASSIFY", "EXPLAIN", "CLUSTERING_ASSIGNMENT"]):
                    self._predict_anonymous_block = self.execute_statement
                    self._predict_call = call_string
                    self._predict_param = self._get_parameters()
                    self._predict_output_table_names = self._extract_output_table_names()
                    self._predict_args = list(_args)
                elif "SCORE" in call_string.upper():
                    self._score_anonymous_block = self.execute_statement
                    self._score_call = call_string
                    self._score_param = self._get_parameters()
                    self._score_output_table_names = self._extract_output_table_names()
                    self._score_args = list(_args)
                else:
                    if funcname not in ("PAL_CREATE_MODEL_STATE", "PAL_DELETE_MODEL_STATE"):
                        self._fit_anonymous_block = self.execute_statement
                        self._fit_call = call_string
                        self._fit_param = self._get_parameters()
                        self._fit_output_table_names = self._extract_output_table_names()
                        self._fit_args = list(_args)
        except Exception as err:
            logger.warning(err)
            pass

    def load_model(self, model):
        """
        Function to load the fitted model.

        Parameters
        ----------
        model : DataFrame
            SAP HANA DataFrame for fitted model.
        """
        self.model_ = model

    def _load_model_tables(self, schema_name, model_table_names, name, version, conn_context=None):
        """
        Function to load models.
        """
        if conn_context is None:
            conn_context = self.conn_context
        if isinstance(model_table_names, str):
            self.model_ = conn_context.table(model_table_names, schema=schema_name)
        elif isinstance(model_table_names, list):
            self.model_ = []
            for model_name in model_table_names:
                self.model_.append(conn_context.table(model_name, schema=schema_name))
        else:
            raise ValueError('Cannot load the model table. Unknwon values ({}, \
            {})'.format(schema_name, str(model_table_names)))

    def add_attribute(self, attr_key, attr_val):
        """
        Function to add attribute.

        Parameters
        ----------
        attr_key : str
            The key.

        attr_val : str
            The value.
        """
        setattr(self, attr_key, attr_val)

    def create_apply_func(self, func_name, data, key=None, model=None, output_table_structure=None, execute=True, force=True):
        r"""
        Create HANA TUDF SQL code.

        Parameters
        ----------
        func_name : str
            The function name of TUDF.
        data : DataFrame
            The data to be predicted.
        key : str, optional
            The key column name in the predict dataframe.
        model : DataFrame, optional
            The model dataframe for prediction. If not specified, it will use model\_.
        output_table_structure : dict, optional
            The return table structure.
        execute : bool, optional
            Execute the creation SQL.

            Defaults to True.
        force : bool, optional
            If True, it will drop the existing TUDF.

        Returns
        -------
        str
            The generated TUDF SQL code.
        """
        if model is None:
            if isinstance(self.model_, (list, tuple)):
                model = self.model_[0]
            else:
                model = self.model_
        #model has temporary table, raise error
        if self._predict_call is None:
            raise Error("Please run predict function first!")
        if '#' in model.select_statement:
            raise Error("Temporary table in model dataframe is not supported when creating TUDF!")
        #check key
        if key is None:
            if data.index:
                key = data.index
            else:
                key = data.columns[0]
        input_tt = data.get_table_structure()
        id_type = input_tt[key]
        #output_table_struture is None, detect object
        #AutoML/PIPELINE: ID VARCHAR(256), SCORES VARCHAR(256)
        #Unified Classification: ID(detect input key type), SCORE VARCHAR(256), CONFIDENCE DOUBLE, REASON_CODE NCLOB
        #Unified Regression: ID(detect input key type), SCORE DOUBLE, UPPER_BOUND DOUBLE, LOWER_BOUND DOUBLE, REASON NCLOB
        if output_table_structure is None:
            if 'Automatic' in self.__str__() or 'Pipeline' in self.__str__():
                output_table_structure = {"ID": "VARCHAR(256)", "SCORES": "VARCHAR(256)"}
            elif 'UnifiedClassification' in self.__str__():
                output_table_structure = {"ID": id_type, "SCORE": "VARCHAR(256)", "CONFIDENCE": "DOUBLE", "REASON_CODE": "NCLOB"}
            elif 'UnifiedRegression' in self.__str__():
                output_table_structure = {"ID": id_type, "SCORE": "VARCHAR(256)", "UPPER_BOUND": "DOUBLE", "LOWER_BOUND": "DOUBLE", "REASON": "NCLOB"}
            else:
                raise Error("Please specify the output table structure.")
        #create function header
        input_tt_str = ''
        for kkey, vval in input_tt.items():
            input_tt_str = input_tt_str + '\n' + quotename(kkey) + ' ' + vval + ','
        out_tt_str = ''
        for kkey, vval in output_table_structure.items():
            out_tt_str = out_tt_str + quotename(kkey) + ' ' + vval + ','
        func_header = "CREATE FUNCTION {0}(in_0 Table({1})) RETURNS TABLE({2})\nREADS SQL DATA\nAS BEGIN\n".format(quotename(func_name),
                                                                                                input_tt_str[:-1],
                                                                                                out_tt_str[:-1])
        #create function body
        body = re.search(r'DECLARE [\s\S]+UNNEST\(:param_name, :int_value, :double_value, :string_value\);', self._predict_anonymous_block).group(0)
        model_str = "\nin_1 = {};".format(model.select_statement)
        #return table
        sql = func_header + body + model_str + "\nCALL " + self._predict_call + "\nRETURN SELECT * FROM :{};".format("out_0") + "\nEND"
        if execute:
            with model.connection_context.connection.cursor() as cur:
                if force:
                    try:
                        execute_logged(cur, "DROP FUNCTION {}".format(quotename(func_name)))
                    except Exception as err:
                        logger.warning(err)
                        pass
                execute_logged(cur, sql)
        return sql

def _parse_line(sql):
    return re.findall(":= (?:N')?([\":0-9A-Za-z_. \\[\\],{}-]+)'?", sql)[0]

def _extract_params_definition_from_sql(sql):
    start_index, end_index = None, None
    for i, line in enumerate(sql):
        if re.match("param_name\\[[1-9]+\\] := .+", line) and not start_index:
            start_index = i
        if re.match("params = UNNEST(.+)", line):
            end_index = i
            break
    if start_index is None:
        start_index = end_index
    return sql[start_index:end_index]

def _extract_params_definition_from_sql_with_group_id(sql):
    start_index, end_index = None, None
    for i, line in enumerate(sql):
        if re.match("group_id\\[[1-9]+\\] := .+", line) and not start_index:
            start_index = i
        if re.match("params = UNNEST(.+)", line):
            end_index = i
            break
    if start_index is None:
        start_index = end_index
    return sql[start_index:end_index]

def _parse_params_with_group_id(param_sql_raw):
    params = []
    param_names = []
    for i in range(0, len(param_sql_raw), 5):
        group_id = _parse_line(param_sql_raw[i])
        name = _parse_line(param_sql_raw[i + 1])
        param_i = _parse_line(param_sql_raw[i + 2])
        param_d = _parse_line(param_sql_raw[i + 3])
        param_s = _parse_line(param_sql_raw[i + 4])
        if param_i == 'NULL' and param_d == 'NULL':
            if name not in param_names:
                params.append((group_id, name, param_s, "string"))
                param_names.append(name)
            else:
                params[param_names.index(name)] = (group_id, name, params[param_names.index(name)][1] + ',' + param_s, "string")
        elif param_i == 'NULL' and param_s == 'NULL':
            params.append((group_id, name, float(param_d), "float"))
            param_names.append(name)
        elif param_d == 'NULL' and param_s == 'NULL':
            params.append((group_id, name, int(param_i), "integer"))
            param_names.append(name)
    return params

def _parse_params(param_sql_raw):
    params = []
    param_names = []
    for i in range(0, len(param_sql_raw), 4):
        name = _parse_line(param_sql_raw[i])
        param_i = _parse_line(param_sql_raw[i + 1])
        param_d = _parse_line(param_sql_raw[i + 2])
        param_s = _parse_line(param_sql_raw[i + 3])
        if param_i == 'NULL' and param_d == 'NULL':
            if name not in param_names:
                params.append((name, param_s, "string"))
                param_names.append(name)
            else:
                params[param_names.index(name)] = (name, params[param_names.index(name)][1] + ',' + param_s, "string")
        elif param_i == 'NULL' and param_s == 'NULL':
            params.append((name, float(param_d), "float"))
            param_names.append(name)
        elif param_d == 'NULL' and param_s == 'NULL':
            params.append((name, int(param_i), "integer"))
            param_names.append(name)
    return params

def attempt_version_comparison(minimum, actual):
    """
    Make our best guess at checking whether we have a high-enough version.

    This may not be a reliable comparison. The version number format has
    changed before, and it may change again. It is unclear what comparison,
    if any, would be reliable.

    Parameters
    ----------
    minimum : str
        (The first three components of) the version string for the
        minimum acceptable SAP HANA version.

    actual : str
        The actual SAP HANA version string.

    Returns
    -------
    bool
        True if (we think) the version is okay.
    """
    truncated_actual = actual.split()[0]
    min_as_ints = [int(x) for x in minimum.split('.')]
    actual_as_ints = [int(x) for x in truncated_actual.split('.')]
    return min_as_ints <= actual_as_ints

def require_pal_usable(conn):
    """
    Raises an error if no compatible SAP HANA PAL version is usable.

    To pass this check, SAP HANA must be version 2 SPS 03,
    PAL must be installed, and the user must have one of the roles
    required to execute PAL procedures (AFL__SYS_AFL_AFLPAL_EXECUTE
    or AFL__SYS_AFL_AFLPAL_EXECUTE_WITH_GRANT_OPTION).

    A successful result is cached, to avoid redundant checks.

    Parameters
    ----------
    conn : ConnectionContext
        ConnectionContext on which PAL must be available.

    Raises
    ------
    hana_ml.ml_exceptions.PALUnusableError.
        If the wrong HANA version is installed, PAL is uninstalled,
        or PAL execution permission is unavailable.
    """
    # pylint: disable=protected-access
    if not conn._pal_check_passed:
        with conn.connection.cursor() as cur:
            # Check HANA version. (According to SAP note 1898497, this
            # should match the PAL version.)
            cur.execute(_SELECT_HANA_VERSION)
            hana_version_string = cur.fetchone()[0]

            if not attempt_version_comparison(
                    minimum=MINIMUM_HANA_VERSION_PREFIX,
                    actual=hana_version_string):
                template = ('hana_ml version {} PAL support is not ' +
                            'compatible with this version of HANA. ' +
                            'HANA version must be at least {!r}, ' +
                            'but actual version string was {!r}.')
                msg = template.format(hana_ml.__version__,
                                      MINIMUM_HANA_VERSION_PREFIX,
                                      hana_version_string)
                raise hana_ml.ml_exceptions.PALUnusableError(msg)

            # Check PAL installation.
            cur.execute(_SELECT_PAL)
            if cur.fetchone() is None:
                raise hana_ml.ml_exceptions.PALUnusableError('PAL is not installed.')

            # Check required role.
            cur.execute(_SELECT_PAL_PRIVILEGE)
            if cur.fetchone() is None:
                msg = ('Missing needed role - PAL procedure execution ' +
                       'needs role AFL__SYS_AFL_AFLPAL_EXECUTE or ' +
                       'AFL__SYS_AFL_AFLPAL_EXECUTE_WITH_GRANT_OPTION')
                raise hana_ml.ml_exceptions.PALUnusableError(msg)
        conn._pal_check_passed = True

def call_pal(conn, funcname, *tablenames):
    """
    Call a PAL function.

    Parameters
    ----------
    conn : ConnectionContext
        HANA connection.
    funcname : str
        PAL procedure name.
    tablenames : list of str
        Table names to pass to PAL.
    """
    # This currently takes function names as "PAL_KMEANS".
    # Should that just be "KMEANS"?

    # callproc doesn't seem to handle table parameters.
    # It looks like we have to use execute.

    # In theory, this function should only be called with function and
    # table names that are safe without quoting.
    # We quote them anyway, for a bit of extra safety in case things
    # change or someone makes a typo in a call site.
    header = 'CALL _SYS_AFL.{}('.format(quotename(funcname))
    arglines_nosep = ['    {}'.format(quotename(tabname))
                      for tabname in tablenames]
    arglines_string = ',\n'.join(arglines_nosep)
    footer = ') WITH OVERVIEW'
    call_string = '{}\n{}\n{}'.format(header, arglines_string, footer)

    # SQLTRACE
    conn.sql_tracer.trace_object({
        'name':funcname,
        'schema': '_SYS_AFL',
        'type': 'pal'
    }, sub_cat='function')

    with conn.connection.cursor() as cur:
        execute_logged(cur, call_string, conn.sql_tracer, conn) # SQLTRACE added sql_tracer

def anon_block_safe(*dataframes):
    """
    Checks if these dataframes are compatible with call_pal_auto_with_hint.

    Parameters
    ----------
    df1, df2, ... : DataFrame
        DataFrames to be fed to PAL.

    Returns
    -------
    bool
        True if call_pal_auto_with_hintcan be used.
    """
    # pylint:disable=protected-access
    return all(df._ttab_handling in ('safe', 'ttab') for df in dataframes)

def call_pal_auto(conn,
                  funcname,
                  *args):
    """
    Uses an anonymous block to call a PAL function.

    DataFrames that are not known to be safe in anonymous blocks will be
    temporarily materialized.

    Parameters
    ----------
    conn : ConnectionContext
        HANA connection to use.
    funcname : str
        Name of the PAL function to execute. Should not include the "_SYS_AFL."
        part.
    arg1, arg2, ..., argN : DataFrame, ParameterTable, or str
        Arguments for the PAL function, in the same order the PAL function
        takes them.
        DataFrames represent input tables, a ParameterTable object represents
        the parameter table, and strings represent names for output
        tables. Output names with a leading "#" will produce local temporary
        tables.
    """
    args = list(args)
    sql, _ = call_pal_auto_with_hint(conn, None, funcname, *args)
    return sql

def call_pal_auto_with_hint(conn,
                            with_hint,
                            funcname,
                            *args):
    """
    Uses an anonymous block to call a PAL function.

    DataFrames that are not known to be safe in anonymous blocks will be
    temporarily materialized.

    Parameters
    ----------
    conn : ConnectionContext
        HANA connection to use.
    with_hint : str,
        If 'PARALLEL_BY_PARAMETER_PARTITIONS(p1)', it will use parallel with hint PARALLEL_BY_PARAMETER_PARTITIONS.
    funcname : str
        Name of the PAL function to execute. Should not include the "_SYS_AFL."
        part.
    arg1, arg2, ..., argN : DataFrame, ParameterTable, or str
        Arguments for the PAL function, in the same order the PAL function
        takes them.
        DataFrames represent input tables, a ParameterTable object represents
        the parameter table, and strings represent names for output
        tables. Output names with a leading "#" will produce local temporary
        tables.
    """
    adjusted_args = list(args)
    temporaries = []
    unknown_indices = []
    materialized = {}

    def materialize_at(i):
        "Materialize the i'th element of adjusted_args."
        tag = str(uuid.uuid4()).upper().replace('-', '_')
        name = '#{}_MATERIALIZED_INPUT_{}'.format(funcname, tag)
        materialize_dict = {name: adjusted_args[i].select_statement}
        adjusted_args[i] = adjusted_args[i].save(name)
        temporaries.append(name)
        return materialize_dict

    def try_exec(cur, sql, conn):
        """
        Try to execute the given sql. Returns True on success, False if
        execution fails due to an anonymous block trying to read a local
        temporary table. Other exceptions are propagated.
        """
        conn.last_execute_statement = sql

        if _exist_hint(sql) and 'pyodbc' not in str(type(cur)):
            try:
                cur.prepare(sql)
                cur.executeprepared()
            except dbapi.Error as err:
                if not err.errortext.startswith(
                        'feature not supported: Cannot use local temporary table'):
                    raise
                if "has invalid SQL type" in str(err):
                    logger.error(" [HINT] The error may be due to the unsupported type such as BIGINT. Try to use enable_convert_bigint() to allow the bigint conversion.")
                else:
                    logger.info("Facing the error %s. Try to fix...", str(err))
                return False
            return True
        else:
            try:
                cur.execute(sql)
            except dbapi.Error as err:
                if not err.errortext.startswith(
                        'feature not supported: Cannot use local temporary table'):
                    raise
                if "has invalid SQL type" in str(err):
                    logger.error(" [HINT] The error may be due to the unsupported type such as BIGINT. Try to use enable_convert_bigint() to allow the bigint conversion.")
                else:
                    logger.info("Facing the error %s. Try to fix...", str(err))
                return False
            except Exception as err:
                if 'feature not supported: Cannot use local temporary table' not in str(err):
                    raise
                if "has invalid SQL type" in str(err):
                    logger.error(" [HINT] The error may be due to the unsupported type such as BIGINT. Try to use enable_convert_bigint() to allow the bigint conversion.")
                else:
                    logger.info("Facing the error %s. Try to fix...", str(err))
                return False
            return True


    try:
        for i, argument in enumerate(args):
            if isinstance(argument, DataFrame):
                # pylint: disable=protected-access
                if argument._ttab_handling == 'unknown':
                    unknown_indices.append(i)
                elif argument._ttab_handling == 'unsafe':
                    materialized.update(materialize_at(i))

        # SQLTRACE added sql_tracer
        sql = sqlgen.call_pal_tabvar(funcname,
                                     conn.sql_tracer,
                                     with_hint,
                                     *adjusted_args)
        # SQLTRACE
        conn.sql_tracer.trace_object({
            'name':funcname,
            'schema': '_SYS_AFL',
            'type': 'pal'
        }, sub_cat='function')

        # Optimistic execution.
        with conn.connection.cursor() as cur:
            logged_without_execute(sql, conn.sql_tracer, conn)
            if try_exec(cur, sql, conn):
                # Optimistic execution succeeded, meaning all arguments with
                # unknown ttab safety are safe.
                for i in unknown_indices:
                    adjusted_args[i].declare_lttab_usage(False)
                return sql, materialized

        # If we reach this point, optimistic execution failed.

        if len(unknown_indices) == 1:
            # Only one argument of unknown ttab safety, so that one needs
            # materialization.
            adjusted_args[unknown_indices[0]].declare_lttab_usage(True)
            materialized.update(materialize_at(unknown_indices[0]))
        else:
            # Multiple arguments of unknown safety. Test which ones are safe.
            for i in unknown_indices:
                with conn.connection.cursor() as cur:
                    ttab_used = not try_exec(cur, sqlgen.safety_test(adjusted_args[i]), conn)
                adjusted_args[i].declare_lttab_usage(ttab_used)
                if ttab_used:
                    materialized.update(materialize_at(i))

        # SQLTRACE added sql_tracer
        sql = sqlgen.call_pal_tabvar(funcname,
                                     conn.sql_tracer,
                                     with_hint,
                                     *adjusted_args)
        with conn.connection.cursor() as cur:
            execute_logged(cur, sql, conn.sql_tracer, conn) # SQLTRACE added sql_tracer
        return sql, materialized
    finally:
        try_drop(conn, temporaries)
