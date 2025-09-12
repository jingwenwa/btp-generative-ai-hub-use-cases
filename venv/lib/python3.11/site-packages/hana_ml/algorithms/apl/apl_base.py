#pylint: disable=too-many-lines, too-many-arguments
"""
This module contains root abstract class and common helper for SAP HANA APL.

"""

import re
import logging
from math import isnan
import sys
import uuid
import functools
import warnings
import pandas as pd

from hdbcli import dbapi
from hana_ml.dataframe import DataFrame, quotename
from hana_ml.ml_base import execute_logged
from hana_ml.ml_base import (
    MLBase,
    ListOfStrings,
    try_drop,
    _TEXT_TYPES
)
from hana_ml.ml_exceptions import FitIncompleteError
from hana_ml.model_storage_services import ModelSavingServices
from hana_ml.algorithms.apl.apl_artifact import (
    APLArtifactTable, APLArtifactApplyOutTable, APLArtifactView)
from hana_ml.algorithms.apl.sqlgen import AplSqlGenerator, SqlGenerationMode
from hana_ml.algorithms.apl.artifacts_recorder import AplSqlArtifactsRecorder
from hana_ml.hana_scheduler import HANAScheduler

logger = logging.getLogger(__name__) #pylint: disable=invalid-name

# ----- Static  ---------------------

# Type conversion mapping for ANY
TYPE_CONVERSION_MAP = {
    'TINYINT': 'TO_INTEGER({NAME})',
    'SMALLINT': 'TO_INTEGER({NAME})',
    'BOOLEAN': 'TO_INTEGER({NAME})',
    'ALPHANUM': 'TO_NVARCHAR({NAME})',
    'SHORTTEXT': 'TO_NVARCHAR({NAME})',
    'SMALLDECIMAL': 'TO_DOUBLE({NAME})',
    'REAL': 'TO_DOUBLE({NAME})',
    'FLOAT': 'TO_DOUBLE({NAME})'}


def get_apl_version(conn):
    """
    Gets the version and configuration information about the installation of SAP HANA APL.

    Parameters
    ----------

    conn: ConnectionContext
        The connection object to a SAP HANA database.

    Returns
    -------

    A pandas Dataframe with detailed information about the current version.

    Notes
    -----

    Error is raised when the call fails.
    The cause can be that either SAP HANA APL is not installed
    or the current user does not have the appropriate rights.
    """
    with conn.connection.cursor() as cur:
        try:
            sql = 'DROP TABLE #PING_OUTPUT'
            execute_logged(cur, sql)
        except dbapi.Error:
            pass
        try:
            # sql = ('create local temporary column table #PING_OUTPUT'
            #        + APLArtifactTable.get_tbl_def_for_ping())
            # execute_logged(cur, sql)
            # sql = 'call _SYS_AFL.APL_AREA_PING_PROC(#PING_OUTPUT) with overview'
            # execute_logged(cur, sql)
            sql = '\n'.join(
                [
                    'DO',
                    'BEGIN',
                    'DECLARE OUT_PING_OUTPUT TABLE("name" NVARCHAR(128), "value" NCLOB);',
                    'CALL "SAP_PA_APL"."sap.pa.apl.base::PING"(:OUT_PING_OUTPUT);',
                    'create local temporary column table #PING_OUTPUT as '
                    '(select * from :OUT_PING_OUTPUT);',
                    'END;',
                ])
            execute_logged(cur, sql)

            sql = 'select * from #PING_OUTPUT'
            execute_logged(cur, sql)
            res = cur.fetchall()
            return pd.DataFrame(res, columns=['name', 'value'])
        except dbapi.Error:
            raise Exception('Unable to ping APL. please check with your database administrator')  # pylint: disable=broad-exception-raised


def is_apl_version_compatible(conn):
    """
    Checks the installed SAP HANA APL version is the right one.
    Arguments:
    ---------
    - conn_context : SAP HANA connection
    Returns:
    -------
    True if the installed version is correct
    False if it is not
    """
    try:
        version = get_apl_version(conn)
        apl_major_ver = int(version.loc[version['name'] == 'APL.Version.Major',
                                        'value'].values[0])
        apl_minor_ver = int(version.loc[version['name'] == 'APL.Version.Minor',
                                        'value'].values[0])
        apl_sp = int(version.loc[version['name'] == 'APL.Version.ServicePack',
                                 'value'].values[0])
        afl_ver = version.loc[version['name'] == 'AFLSDK.Info', 'value'].values[0]
        msg = "Installed Version (APL={apl_major_ver}.{apl_minor_ver} SP {apl_sp}/" \
              " AFL={afl_ver})".format(apl_major_ver=apl_major_ver,
                                       apl_minor_ver=apl_minor_ver,
                                       apl_sp=apl_sp,
                                       afl_ver=afl_ver)
        if apl_sp >= 1811 and afl_ver.startswith('2.13'):
            msg = msg + " is compatible"
            logger.info(msg)
            return True

        msg = msg + " is NOT compatible"
        logger.error(msg)
        return False
    except dbapi.Error:
        # The version checking simply because APL versionning has been changed over time.
        # The easiest is to update APL to a most recent version
        errmsg = sys.exc_info()[0]
        msg = "Cannot check version (Error: {}). Please update APL to the latest version".format(
            errmsg
        )
        logger.error(msg)
    return False


def config_logger(
        log_path=None, logfile_name=None,
        log_console=True, log_level=logging.ERROR):
    """
    Configures the logger so it can either display the Python API log in a console
    or in a file.
    Arguments:
    - log_path : str
        The folder in which logs will be written
    - logfile_name : str
        The file name
    - log_console : boolean
        If true, the log will display in a console
    - log_level : int (logging.ERROR, logging.INFO, ...)
    """
    log_formatter = logging.Formatter(
        "%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(log_level)
    if log_path and logfile_name:
        file_handler = logging.FileHandler(
            "{0}/{1}.log".format(log_path, logfile_name))
        file_handler.setFormatter(log_formatter)
        root_logger.addHandler(file_handler)
    if log_console:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(log_formatter)
        root_logger.addHandler(console_handler)


def _show_deprecated_warning(func_name, message):
    # Cannot use decorator. If doing so, the function would not be exposed to documentation.
    warnings.warn("{} is a deprecated method. {}".format(func_name, message),
                  category=DeprecationWarning,
                  stacklevel=2)
    warnings.simplefilter('default', DeprecationWarning)

#pylint: disable=protected-access
#pylint: disable=too-many-instance-attributes
class APLBase(MLBase, ModelSavingServices):
    """
    Common abstract root class for all APL algorithms.
    """
    # Dictionary : model parameter <-> APL alias (to be used in OPERATION_CONFIG)
    APL_ALIAS_KEYS = {
        'correlations_lower_bound': 'APL/CorrelationsLowerBound',
        'correlations_max_kept': 'APL/CorrelationsMaxKept',
        'cutting_strategy': 'APL/CuttingStrategy',
        'exclude_low_predictive_confidence': 'APL/ExcludeLowPredictiveConfidence',
        'polynomial_degree': 'APL/PolynomialDegree',
        'risk_fitting': 'APL/RiskFitting',
        'risk_fitting_min_cumulated_frequency': 'APL/RiskFittingMinCumulatedFrequency',
        'risk_fitting_nb_pdo': 'APL/RiskFittingNbPDO',
        'risk_fitting_use_weights': 'APL/RiskFittingUseWeights',
        'risk_gdo': 'APL/RiskGDO',
        'risk_mode': 'APL/RiskMode',
        'risk_pdo': 'APL/RiskPDO',
        'risk_score': 'APL/RiskScore',
        'score_bins_count': 'APL/ScoreBinsCount',
        'target_key': 'APL/TargetKey',
        'variable_auto_selection': 'APL/VariableAutoSelection',
        'variable_selection_best_iteration': 'APL/VariableSelectionBestIteration',
        'variable_selection_min_nb_of_final_variables':
            'APL/VariableSelectionMinNbOfFinalVariables',
        'variable_selection_max_nb_of_final_variables':
            'APL/VariableSelectionMaxNbOfFinalVariables',
        'variable_selection_mode': 'APL/VariableSelectionMode',
        'variable_selection_nb_variables_removed_by_step':
            'APL/VariableSelectionNbVariablesRemovedByStep',
        'variable_selection_percentage_of_contribution_kept_by_step':
            'APL/VariableSelectionPercentageOfContributionKeptByStep',
        'variable_selection_quality_bar': 'APL/VariableSelectionQualityBar',
        'variable_selection_quality_criteria': 'APL/VariableSelectionQualityCriteria',
        'segment_column_name': 'APL/SegmentColumnName'
    }

    def __init__(self, #pylint: disable=too-many-arguments
                 conn_context=None,
                 variable_storages=None,
                 variable_value_types=None,
                 variable_missing_strings=None,
                 extra_applyout_settings=None,
                 ** other_params):
        super(APLBase, self).__init__(conn_context)
        ModelSavingServices.__init__(self)  # for ModelSavingServices new attributes
        self.log_level = 8
        self.model_format = 'bin'
        self.language = 'en'
        self.check_operation_config = 'true'
        self.max_tasks = None
        self.with_hint = None  # used for scale out (ECN)
        # --- Uniq id
        # --- overwrite id set by MLBase
        uniq_id = str(uuid.uuid4()).replace('-', '_').upper()
        self.id = uniq_id + '_' + str(self.id)
        # --- parameters for variables description
        self.variable_storages = None
        self.variable_value_types = None
        self.variable_missing_strings = None
        self.extra_applyout_settings = None
        if variable_storages is not None:
            self.set_params(variable_storages=variable_storages)
        if variable_value_types is not None:
            self.set_params(variable_value_types=variable_value_types)
        if variable_missing_strings is not None:
            self.set_params(variable_missing_strings=variable_missing_strings)
        if extra_applyout_settings is not None:
            self.set_params(extra_applyout_settings=extra_applyout_settings)
        # other params
        self.set_params(**other_params)
        # technical params
        self._model_type = None  # it must be set by final class
        self._view_name = None
        self._force_target_var_type = None  # continuous or nominal or none
        self.model_table_ = None
        self.model_ = None
        self.debrief_metric_table_ = None
        self.debrief_property_table_ = None
        self.applyout_ = None
        # memorize artifact tables 'table_name':APLArtifactTable
        self._artifact_tables = {}
        self._apl_version = None
        self._hana_version = None
        self._sql_generation_mode = SqlGenerationMode.UNDEFINED    # How the sql will be generated
        self._sql_generation_mode_forced = False
        if self.conn_context:
            self._init_versions_and_options()
        # Recorder of SQL artifacts (used for SQL artifacts generation)
        self._artifacts_recorder = None
        self._disable_hana_execution = False
        self._indicator_dataset = 'Validation'

    def _set_conn_context(self, conn):
        """
        Sets connection context.
        If the connection is changed, HANA and APL versions will be checked.
        Parameters
        ----------
        conn: A hanaml ConnectionContext object
        """
        if not conn:
            raise ValueError('The connection context is none.')
        if self.conn_context and self.conn_context is conn:
            # The connection has no changed
            return
        self.conn_context = conn
        self._init_versions_and_options()

    def _init_versions_and_options(self):
        """
        Determines the HANA and APL versions.
        Infers the options based on the current versions of HANA and APL.
        In particular, '_sql_generation_mode' is determined.

        Returns
        -------
        None
        """
        if not self.conn_context:
            raise Exception('Cannot determine HANA and APL version. Connection is not set.')  # pylint: disable=broad-exception-raised
        # Gets HANA version
        with self.conn_context.connection.cursor() as cur:
            try:
                sql = 'select VERSION from SYS.M_DATABASE'
                execute_logged(cur, sql)
                self._hana_version = cur.fetchall()[0][0]
            except dbapi.Error:
                # pylint: disable=broad-exception-raised
                raise Exception('Unable to get HANA version.'
                                ' please check with your database administrator')
        # Infers '_sql_generation_mode'
        # if '_sql_generation_mode' has not been forced
        if not (self._sql_generation_mode_forced and self._sql_generation_mode):
            first_digit = int(self._hana_version.split('.')[0])
            if first_digit >= 4:
                # Check the revision number (example: 2023.6)
                # if before 2023.6 (problem "feature not supported: Cannot use local temporary table inside a procedure/function")
                if self._get_hana_revision_number() < 2023.6:
                    self._sql_generation_mode = SqlGenerationMode.SQLBLOCK_DU
                else:
                    # Sql block + Any without overview: cannot work because of debrief which requires DU
                    self._sql_generation_mode = SqlGenerationMode.SQLBLOCK_PROCANY
            else:
                self._sql_generation_mode = SqlGenerationMode.LEGACY  # Legacy proc any direct
        # APL Version
        self._apl_version = self.get_apl_version()
        if self._get_apl_version() < 2325:
            self.check_operation_config = 'false'

    def _get_hana_revision_number(self):
        """
        Get the revision number (example: 2023.6)
        Returns
        -------
            float
        Example:
        -------
        >>> self._get_hana_revision_number()
        2023.6
        """
        if self.conn_context is None:
            raise ValueError("Cannot get HANA version. Connection not defined.")
        # hana_version() returns a string like '4.00.000.00.1676367855 (CE2023.6)'
        full_version = self.conn_context.hana_version()
        res = re.findall(r"\([a-zA-Z/]*([0-9.]*)\)", full_version)  # returns ['2023.6']
        if not res:
            raise ValueError("Cannot get HANA revision number. The version string does not contain expected pattern.")
        return float(res[0])  # example: 2023.6

    def _get_apl_version(self):
        if self._apl_version is None:
            raise ValueError("Cannot get APL version.")
        return int(self._apl_version.loc[self._apl_version['name'] == 'APL.Version.ServicePack'].iloc[0]['value'])

    def _force_sql_generation_mode(self, mode):
        """
        Forces _the option for 'WITH OVERVIEW' regardless to the HANA & APL version.
        It is not supposed to be set the end user.
        It is essentially used for testing.
        :param force_bool: bool
        0: 'WITH OVERVIEW' supported, ie stay with the legacy mode
        1: Use anymous block + proc any without overview
        2: Use DU as option
        """
        self._sql_generation_mode = mode
        self._sql_generation_mode_forced = True

    def get_params(self):
        """
        Retrieves attributes of the current object.
        This method is implemented for compatibility with Scikit Learn.

        Returns
        -------
        The attribute-values of the model : dictionary

        """
        attrs = {}
        for attrname, value in self.__dict__.items():
            # except id, it is for class attribut
            if attrname != 'id':
                attrs[attrname] = value
        return attrs

    def set_params(self, **parameters):
        """
        Sets attributes of the current model.
        This method is implemented for compatibility with Scikit Learn.

        Parameters
        ----------
        params : dictionary
            The attribute names and values
        """
        # check the param names are subset of possible values
        possible_param_names = set(self.APL_ALIAS_KEYS.keys())

        for parameter, value in parameters.items():
            if parameter == 'variable_storages':
                self.variable_storages = self._arg_dictionary(
                    name='variable_storages',
                    dictionary=value,
                    possible_values=['integer', 'number', 'string', 'date',
                                     'datetime', 'angle']
                    )
                continue
            if parameter == 'variable_value_types':
                # 'continuous,' 'nominal', 'ordinal'
                self.variable_value_types = self._arg_dictionary(
                    name='variable_value_types',
                    dictionary=value,
                    possible_values=['continuous', 'nominal', 'ordinal',
                                     'textual']
                    )
                continue
            if parameter == 'variable_missing_strings':
                self.variable_missing_strings = self._arg(
                    'variable_missing_strings',
                    value,
                    dict)
                continue
            if parameter == 'extra_applyout_settings':
                self.extra_applyout_settings = self._arg(
                    'extra_applyout_settings',
                    value,
                    dict)
                continue
            if parameter == 'other_train_apl_aliases':
                self.other_train_apl_aliases = self._arg(
                    'other_train_apl_aliases',
                    value,
                    dict)
                continue
            if parameter not in possible_param_names and parameter not in self.__dict__:
                raise KeyError("'{}' is not a valid parameter name!".format(parameter))

            setattr(self, parameter, value)
        return self

    def _create_artifact_table(self, table):
        """
        Creates a temporary table in the SAP HANA database that is used as artifact
        for SAP HANA APL.
        Argument:
        -------
        - Table : An object of APLArtifactTable
        """
        table.create_table_with_hana_type(conn=self.conn_context)
        self._artifact_tables[table.name] = table

    def _drop_artifact_tables(self, tables=None):
        if not tables:
            tables = self._artifact_tables.values()
        table_names = [t.name for t in tables]
        self._try_drop(table_names)
        for tab in table_names:
            self._artifact_tables.pop(tab, None)


    def _call_apl_legacy(self, funcname, *tables):
        """
        Calls a SAP HANA APL procedure.
        This method won't work with HCE.
        Parameters
        ----------
        conn : ConnectionContext
            The SAP HANA connection
        funcname : str
            The procedure name
        tables : list of APLArtifactTable or APLArtifactView
            The table names used in procedure call
        """
        # Retrieve tablenames. Element in the list can be Table or string
        tablenames = [t.name for t in tables]
        header = 'CALL _SYS_AFL.{}('.format(quotename(funcname))
        arglines_nosep = ['{}'.format(quotename(tabname))
                          for tabname in tablenames]
        arglines_string = ', '.join(arglines_nosep)
        footer = ') WITH OVERVIEW'
        call_string = '{} {} {}'.format(header, arglines_string, footer)
        with self.conn_context.connection.cursor() as cur:
            execute_logged(cur, call_string)

    def _call_apl_without_overview_procany(self, funcname, input_tables, output_tables):
        # pylint: disable=too-many-locals
        """
        Call APL procedure without OVERVIEW using procedure ANY
        Generates and executes a anonymous block sql.
        That corresponds to the case: model._sql_generation_mode == SqlGenerationMode.SQLBLOCK_PROCANY

        Example of generated block sql for proc any:
        DO (
            IN IN_FUNC_HEADER TABLE (KEY NVARCHAR(50), VALUE NVARCHAR(255))
            => #FUNC_HEADER_642345E1_29B0_4902_9D35_E91A865B0656_0,
            IN IN_GUESSVAR_CONFIG TABLE (KEY NVARCHAR(1000), VALUE NCLOB, CONTEXT NVARCHAR(100))
            => #GUESSVAR_CONFIG_642345E1_29B0_4902_9D35_E91A865B0656_0
        )
        BEGIN
            DECLARE OUT_MODEL TABLE(OID NVARCHAR(50), FORMAT NVARCHAR(50), LOB CLOB);
            DECLARE OUT_VARIABLE_DESC TABLE(RANK INT, NAME NVARCHAR(127), STORAGE NVARCHAR(10),
                VALUETYPE NVARCHAR(10), KEYLEVEL INT, ORDERLEVEL INT, MISSINGSTRING NVARCHAR(255),
                GROUPNAME NVARCHAR(255), DESCRIPTION NVARCHAR(255), OID NVARCHAR(50));
            IN_DATASET = select * from TRAIN_DATA_VIEW_642345E1_29B0_4902_9D35_E91A865B0656_0;
            CALL _SYS_AFL."APL_CREATE_MODEL"(
                -- Input
                :IN_FUNC_HEADER,
                :IN_GUESSVAR_CONFIG,
                :IN_DATASET,
                -- Output
                OUT_MODEL,
                OUT_VARIABLE_DESC
                );
            CREATE LOCAL TEMPORARY COLUMN TABLE "#GUESSVAR_MODEL_TRAIN_BIN_642345E1" as
                (select * from :OUT_MODEL);
            CREATE LOCAL TEMPORARY COLUMN TABLE "#VARIABLE_DESC_642345E1" as
                (select * from :OUT_VARIABLE_DESC);
        END;

        Parameters
        ----------
        funcname : str
            The procedure name
        input_tables : list of str (a table name) or APLArtifactTable
            The input table names to be placed in procedure call
        output_tables : list Table (mandatory)
            The output table names to be placed in procedure call
        """

        sqlgen = AplSqlGenerator(
            funcname=funcname,
            input_tables=input_tables,
            output_tables=output_tables,
            sql_generation_mode=SqlGenerationMode.SQLBLOCK_PROCANY
        )
        final_sql = sqlgen.generate()

        # To be optimized later : do not pre-create output temp table
        self._drop_artifact_tables(tables=output_tables)
        with self.conn_context.connection.cursor() as cur:
            execute_logged(cur, final_sql)

    def set_scale_out(self,
                      route_to=None,
                      no_route_to=None,
                      route_by=None,
                      route_by_cardinality=None,
                      data_transfer_cost=None,
                      route_optimization_level=None,
                      workload_class=None):
        """
        Specifies hints for scaling-out environment.
        The execution of APL functions can then be routed to a specific computation node.
        If all the parameters are none, i.e. not given, all the existing hints are cleared.

        Parameters
        ----------
        route_to : str, optional
            Routes the query to the specified volume ID or service type.

            Defaults to None.

        no_route_to : str or list of str, optional
            Avoids query routing to a specified volume ID or service type.

            Defaults to None.

        route_by : str, optional
            Routes the query to the hosts related to the base table(s) of the specified projection view(s).

            Defaults to None.

        route_by_cardinality : str or list of str, optional
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

        Examples
        --------
        >>> from hana_ml.algorithms.apl.gradient_boosting_classification import GradientBoostingBinaryClassifier
        >>> model = GradientBoostingBinaryClassifier()
        >>> # Routes the execution to a specific volume ID.
        >>> model.set_scale_out(route_to=1025)
        >>> # Routes the execution to a specific service type.
        >>> # model.set_scale_out(route_to='computeserver')
        >>> # Maps the execution to a specific workload class.
        >>> # model.set_scale_out(workload_class="WC4")
        >>> # Activates the sql trace
        >>> # connection_context.sql_tracer.enable_sql_trace(True)
        >>> model.fit(data=hdb_df, key='KEY', label='Y')
        >>> # You can check whether the queries were effectively routed by querying:
        >>> # select HOST, VOLUME_ID, APPLICATION_NAME, STATEMENT_STRING from M_SQL_PLAN_CACHE.
        """
        hint_str = []
        if route_to:
            if isinstance(route_to, int):
                hint_str.append('ROUTE_TO({})'.format(route_to))
            else:
                hint_str.append("ROUTE_TO('{}')".format(route_to))
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
            # Whenever a hint is specified, force the sql mode to 'CREATE PROCEDURE'
            self._force_sql_generation_mode(SqlGenerationMode.PROCEDURE_PROCANY)
        else:
            # Removes hint
            self.with_hint = None

    def _call_apl_procedure_with_hint(self, funcname, input_tables, tmp_output_tables):
        """
        Call APL by creating first a temporary new procedure.
        That corresponds to the case: model._sql_generation_mode == SqlGenerationMode.PROCEDURE_PROCANY

        Example of generated new procedure:
        create procedure toto (
            IN IN_FUNC_HEADER TABLE (KEY NVARCHAR(50), VALUE NVARCHAR(255)),
            IN IN_GUESSVAR_CONFIG TABLE (KEY NVARCHAR(1000), VALUE NCLOB, CONTEXT NVARCHAR(100))
        )
        BEGIN
            DECLARE OUT_MODEL TABLE(OID NVARCHAR(50), FORMAT NVARCHAR(50), LOB CLOB);
            DECLARE OUT_VARIABLE_DESC TABLE(RANK INT, NAME NVARCHAR(127), STORAGE NVARCHAR(10),
                VALUETYPE NVARCHAR(10), KEYLEVEL INT, ORDERLEVEL INT, MISSINGSTRING NVARCHAR(255),
                GROUPNAME NVARCHAR(255), DESCRIPTION NVARCHAR(255), OID NVARCHAR(50));
            IN_DATASET = select * from TRAIN_DATA_VIEW_642345E1_29B0_4902_9D35_E91A865B0656_0;
            CALL _SYS_AFL."APL_CREATE_MODEL"(
                -- Input
                :IN_FUNC_HEADER,
                :IN_GUESSVAR_CONFIG,
                :IN_DATASET,
                -- Output
                OUT_MODEL,
                OUT_VARIABLE_DESC
                );
            /* Temp tables are not supported for routing */
            INSERT INTO "HANAML_GUESSVAR_MODEL_TRAIN_BIN_642345E1" as
                (select * from :OUT_MODEL);
            INSERT INTO "HANAML_VARIABLE_DESC_642345E1" as
                (select * from :OUT_VARIABLE_DESC);
        END;

        The procedure is then called with hint (after having called set_scale_out method).

        Parameters
        ----------
        funcname : str
            The procedure name
        input_tables : list of str (a table name) or APLArtifactTable
            The input table names to be placed in procedure call
        output_tables : list Table (mandatory)
            The output table names to be placed in procedure call
        """

         # New procedure name
        new_proc_name = "HANAML_{funcname}_{uniq_id}".format(
            funcname=funcname,
            uniq_id=str(uuid.uuid4()).replace('-', '_').upper(),
            )

        # For scaling out (ECN), we cannot use temporary output table in a procedure
        # As a result, we "transfer" those tmp tables to permanent tables for the time of the procedure life cycle.
        # After the procedure is called, we will move them back to the initial temporary tables.
        permanent_output_tables = [tmp_table.replicate_as_permanent_table(self.conn_context)
                                   for tmp_table in tmp_output_tables]
        sqlgen = AplSqlGenerator(
            funcname=funcname,
            input_tables=input_tables,
            output_tables=permanent_output_tables,
            sql_generation_mode=SqlGenerationMode.PROCEDURE_PROCANY,
            new_procedure_name=new_proc_name,
            with_hint=self.with_hint
        )
        sql_create_proc = sqlgen.generate()
        sql_call_proc = sqlgen.generate_call_procedure()
        sql_drop_proc = sqlgen.generate_drop_procedure()

        #Execute call procedure
        with self.conn_context.connection.cursor() as cur:
            try:
                # Create procedure
                execute_logged(cur, sql_create_proc)
                # Call procedure. The statement must be prepared if a routing is requested.
                execute_logged(cur, sql_call_proc)
            except Exception as ex:
                raise ex
            finally:
                # Drop procedure
                try:
                    execute_logged(cur, sql_drop_proc)
                except:  #pylint: disable=bare-except
                    pass
                # Move the permanent output tables back to the tmp tables
                self._copy_and_drop_tables(src_tables=permanent_output_tables, dest_tables=tmp_output_tables)

    def _copy_and_drop_tables(self, src_tables, dest_tables):
        """
        Copy the content of <src_tables> to <dest_tables>.
        Then, drop the <src_tables>.
        Parameters:
        -----------
        - src_tables : list of APLArtifactTable instances
          List of the tables to be moved.
          They will be dropped after the content is copied.
        - src_tables : list of APLArtifactTable instances
          List of the destination tables.
        """
        for src, dest in zip(src_tables, dest_tables):
            with self.conn_context.connection.cursor() as cur:
                sql = "INSERT INTO {} (SELECT * FROM {})".format(dest.name, src.name)
                execute_logged(cur, sql)
                sql = "DROP TABLE {}".format(src.name)
                execute_logged(cur, sql)


    def _call_apl_without_overview_du(self, funcname, input_tables, output_tables):
        # pylint: disable=too-many-locals
        """
        Call APL procedure without OVERVIEW using DU
        Generates and executes a anonymous block sql.
        That corresponds to the case: model._sql_generation_mode == SqlGenerationMode.SQLBLOCK_DU

        Example of generated block sql for DU:
        DO (
            IN IN_FUNC_HEADER TABLE (KEY NVARCHAR(50), VALUE NVARCHAR(255))
            => #FUNC_HEADER_642345E1_29B0_4902_9D35_E91A865B0656_0,
            IN IN_GUESSVAR_CONFIG TABLE (KEY NVARCHAR(1000), VALUE NCLOB, CONTEXT NVARCHAR(100))
            => #GUESSVAR_CONFIG_642345E1_29B0_4902_9D35_E91A865B0656_0
        )
        BEGIN
            DECLARE OUT_MODEL TABLE(OID NVARCHAR(50), FORMAT NVARCHAR(50), LOB CLOB);
            DECLARE OUT_VARIABLE_DESC TABLE(RANK INT, NAME NVARCHAR(127), STORAGE NVARCHAR(10),
                VALUETYPE NVARCHAR(10), KEYLEVEL INT, ORDERLEVEL INT, MISSINGSTRING NVARCHAR(255),
                GROUPNAME NVARCHAR(255), DESCRIPTION NVARCHAR(255), OID NVARCHAR(50));
            DECLARE v_current_schema NVARCHAR(255);
            select current_schema into v_current_schema from dummy;

            CALL "SAP_PA_APL"."sap.pa.apl.base::CREATE_MODEL"(
                -- Input
                :IN_FUNC_HEADER,
                :IN_GUESSVAR_CONFIG,
                --:IN_DATASET,
                :v_current_schema, 'TRAIN_DATA_VIEW_642345E1_29B0_4902_9D35_E91A865B0656_0',
                -- Output
                :OUT_MODEL,
                :OUT_VARIABLE_DESC
                );
            CREATE LOCAL TEMPORARY COLUMN TABLE "#GUESSVAR_MODEL_TRAIN_BIN_642345E1" as
                (select * from :OUT_MODEL);
            CREATE LOCAL TEMPORARY COLUMN TABLE "#VARIABLE_DESC_642345E1" as
                (select * from :OUT_VARIABLE_DESC);

        END;
        Parameters
        ----------
        funcname : str
            The procedure name
        input_tables : list of str (a table name) or APLArtifactTable
            The input table names to be placed in procedure call
        output_tables : list Table (mandatory)
            The output table names to be placed in procedure call
        """
        sqlgen = AplSqlGenerator(
            funcname=funcname,
            input_tables=input_tables,
            output_tables=output_tables,
            sql_generation_mode=SqlGenerationMode.SQLBLOCK_DU
        )
        final_sql = sqlgen.generate()
        # To be optimized later : do not pre-create output temp table
        self._drop_artifact_tables(tables=output_tables)
        with self.conn_context.connection.cursor() as cur:
            execute_logged(cur, final_sql)


    def _call_apl(self, funcname, input_tables, output_tables):
        """
        Call APL procedure
        Parameters
        ----------
        funcname : str
            The procedure name
        input_tables : list of str (a table name) or APLArtifactTable
            The input table names to be placed in procedure call
        output_tables : list Table (mandatory)
            The output table names to be placed in procedure call
        """
        if self._sql_generation_mode == SqlGenerationMode.LEGACY:
            # Stay with legacy call (call with overview) while awaiting for fix from Hana AFL
            return self._call_apl_legacy(funcname, *(input_tables + output_tables))
        if self._sql_generation_mode == SqlGenerationMode.SQLBLOCK_PROCANY:
            # Using anonymous bloc sql with procdure ANY
            return self._call_apl_without_overview_procany(funcname, input_tables, output_tables)
        if self._sql_generation_mode == SqlGenerationMode.SQLBLOCK_DU:
            # Using anonymous bloc sql with DU
            return self._call_apl_without_overview_du(funcname, input_tables, output_tables)
        if self._sql_generation_mode == SqlGenerationMode.PROCEDURE_PROCANY:
            # Using Procedure + ProcAny + possible Hint for ECN
            return self._call_apl_procedure_with_hint(funcname, input_tables, output_tables)
        raise Exception("Sql generation mode undefined") # pylint: disable=broad-exception-raised

    def _create_func_header_table(self):
        """
        Creates a new APLArtifactTable object for FUNCTION_HEADER table.
        Returns:
        ------
            An APLArtifactTable object for Function Header with data
        """
        # -- Prepare Input parameter tables
        data = [
            ('Oid', '{}'.format(self.id)),
            ('LogLevel', str(self.log_level)),
            ('ModelFormat', self.model_format),
            ('CheckOperationConfig', self.check_operation_config)]
        if self.max_tasks is not None:
            data.append(('MaxTasks', str(self.max_tasks)))
        if self.language is not None:
            data.append(('Language', self.language))
        func_header_table = APLArtifactTable(
            name='#FUNC_HEADER_{}'.format(self.id),
            type_name=APLArtifactTable.FUNCTION_HEADER,
            apl_version=self._apl_version,
            data=data)
        return func_header_table

    def _create_operation_log_table(self, name=None):
        """
        Creates a new APLArtifactTable object for OPERATION_LOG table.
        Returns:
        ------
            An APLArtifactTable object for Function Header with data
        """
        # -- Prepare Input parameter tables
        if name is None:
            name = '#OPERATION_LOG_{}'.format(self.id)
        table = APLArtifactTable(
            name=name,
            type_name=APLArtifactTable.OPERATION_LOG,
            apl_version=self._apl_version)
        return table

    def _create_summary_table(self, name=None):
        if name is None:
            name = '#SUMMARY_{}'.format(self.id)
        table = APLArtifactTable(
            name=name,
            type_name=APLArtifactTable.SUMMARY,
            apl_version=self._apl_version)
        return table

    def _create_debrief_metric_table(self, name=None):
        if name is None:
            name = '#DEBRIEF_METRIC_{}'.format(self.id)
        table = APLArtifactTable(
            name=name,
            type_name=APLArtifactTable.DEBRIEF_METRIC,
            apl_version=self._apl_version)
        return table

    def _create_debrief_property_table(self, name=None):
        if name is None:
            name = '#DEBRIEF_PROPERTY_{}'.format(self.id)
        table = APLArtifactTable(
            name=name,
            type_name=APLArtifactTable.DEBRIEF_PROPERTY,
            apl_version=self._apl_version)
        return table

    def _create_basic_config_table(self, name=None, has_output_indicator_table=True):
        if name is None:
            name = '#OPERATION_CONFIG_{}'.format(self.id)
        config_data_tuples = []
        segment_column_name = getattr(self, 'segment_column_name', None)
        if segment_column_name is not None:
            config_data_tuples.append(('APL/SegmentColumnName', segment_column_name, None))
        if has_output_indicator_table:
            config_data_tuples.append(('APL/IndicatorDataset', self._indicator_dataset, None))
        table = APLArtifactTable(
            name=name,
            type_name=APLArtifactTable.OPERATION_CONFIG_EXTENDED,
            apl_version=self._apl_version,
            data=config_data_tuples)
        return table

    def _get_train_config_data(self, has_output_indicator_table=True):
        """
        Add parameters (apl aliases) for train configuration.
        That consists in translating the current model attributes into APL OPERATION_CONFIG data.
        Parameters
        ----------
        train_config_ar : 2D list
            The list of parameter values for the APL configuration table.
        Returns:
        ------
        List of tuples [(alias_name, alias_value),]
        """
        config_data = []
        # The current model attributes will be mapped into APL alias.
        for k in self.APL_ALIAS_KEYS:
            param_val = getattr(self, k, None)
            if param_val is not None:
                # Boolean values must be in lower case, except for target_key
                if k != 'target_key' and str(param_val) in ['True', 'False']:
                    param_val = str(param_val).lower()
                config_data.append(
                    (self.APL_ALIAS_KEYS[k], str(param_val), None)
                )
        param_val = getattr(self, 'other_train_apl_aliases', None)
        # The attribute value is a dictionary
        if param_val:
            for alias_name, alias_val in param_val.items():
                config_data.append(
                    (alias_name, str(alias_val), None)
                )
        if has_output_indicator_table:
            config_data.append(
                ('APL/IndicatorDataset', self._indicator_dataset, None)
            )
        return config_data

    def _create_train_config_table(self, has_output_indicator_table=True):
        """
        Creates a new APLArtifactTable object for TRAIN_OPERATION_LOG table.
        Returns:
        ------
            An APLArtifactTable object with data
        """
        if self._model_type is None:
            raise FitIncompleteError("Model type undefined.")
        train_config_ar = [('APL/ModelType', self._model_type, None)]
        # add params to OPERATION_CONFIG table
        train_config_ar = train_config_ar + self._get_train_config_data(has_output_indicator_table=has_output_indicator_table)
        train_config_df = pd.DataFrame(train_config_ar)
        if train_config_df is None:
            raise FitIncompleteError("Train configuration undefined.")
        train_config_table = self._create_aplartifact_table_with_data_frame(
            name='#CREATE_AND_TRAIN_CONFIG_{}'.format(self.id),
            type_name=APLArtifactTable.OPERATION_CONFIG_EXTENDED,
            data_df=train_config_df
            )
        return train_config_table

    def _create_var_roles_table(self,
                                data,
                                key,
                                label,
                                features,
                                weight):
        """
        Creates a new APLArtifactTable object for VARABLE_ROLES table.

        Returns
        -------
        An APLArtifactTable object with data
        """
        role_tuples = []
        if features is not None:
            role_tuples.extend(
                [(feat, 'input', None, None, None) for feat in features])
            # skip all those that are not in features
            segment_column = getattr(self, 'segment_column_name', None)
            for col_name in data.columns:
                if (col_name not in features) and (col_name != label) and \
                   (col_name != weight) and (col_name != segment_column):
                    role_tuples.extend([(col_name, 'skip', None, None, None)])
        if label is not None:
            role_tuples.append((label, 'target', None, None, None))
        if weight is not None:
            role_tuples.append((weight, 'weight', None, None, None))

        # Unless features are explicitly specified, key will be skipped
        if key and not features:
            role_tuples.append((key, 'skip', None, None, None))

        var_roles_table = APLArtifactTable(
            name='#VARIABLE_ROLES_{}'.format(self.id),
            type_name=APLArtifactTable.VARIABLE_ROLES_WITH_COMPOSITES_OID,
            apl_version=self._apl_version,
            data=role_tuples)
        return var_roles_table

    def _create_var_desc_table(self, key, label,
                               data_view_name):
        """
        Creates a new APLArtifactTable object for VAR_DESC table.
        Returns:
        ------
            An APLArtifactTable object with data
        """
        vardesc_df = self._guess_var_description(data_view_name)
        if key is not None:
            # --- If key is given,
            # Overwrite the ID column by setting Key=1
            row_idx = vardesc_df[vardesc_df.NAME == key].index
            vardesc_df.loc[row_idx, 'KEYLEVEL'] = 1
        # Remove the segment column
        segment_column_name = getattr(self, 'segment_column_name', None)
        if segment_column_name is not None:
            row_idx = vardesc_df[vardesc_df.NAME == segment_column_name].index[0]
            if row_idx + 1 < len(vardesc_df):
                vardesc_df.iloc[row_idx + 1 :, 0] -= 1
            vardesc_df.drop(row_idx, inplace=True)
        # Force target variable to nominal (classification)
        if label is not None:
            if self._force_target_var_type is not None:
                vardesc_df.loc[vardesc_df.NAME == label,
                               'VALUETYPE'] = self._force_target_var_type
        if self.variable_storages is not None:
            for var_name in self.variable_storages.keys():
                vardesc_df.loc[vardesc_df.NAME == var_name,
                               'STORAGE'] = self.variable_storages[var_name]
        if self.variable_value_types is not None:
            for var_name in self.variable_value_types.keys():
                vardesc_df.loc[vardesc_df.NAME == var_name,
                               'VALUETYPE'] = self.variable_value_types[var_name]
        if self.variable_missing_strings is not None:
            for var_name in self.variable_missing_strings.keys():
                vardesc_df.loc[vardesc_df.NAME == var_name,
                               'MISSINGSTRING'] = self.variable_missing_strings[var_name]
        # Create APLArtifactTable object for VARIABLE_DESC
        var_desc_table = self._create_aplartifact_table_with_data_frame(
            name='#VARIABLE_DESC_{}'.format(self.id),  # name of table
            type_name=APLArtifactTable.VARIABLE_DESC_OID,  # type
            data_df=vardesc_df
            )
        return var_desc_table

    def _create_aplartifact_table_with_data_frame(
            self,
            name,
            type_name,
            data_df):
        #pylint: disable=too-many-branches
        """
        Creates a new APLArtifactTable object.
        Parameter
        ---------
            name: str,
                The name of Hana table, example, '#VAR_DESC_GUESS'
            type_name: str,
                The predefined APL type , for example, APLArtifactTable.VARIABLE_DESC_OID
            data_df: Pandas dataframe or None
                The content of the table to be inserted.
        Returns:
        ------
            An APLArtifactTable object
        """
        artifact_table = APLArtifactTable(name=name,
                                          type_name=type_name,
                                          apl_version=self._apl_version)
        # Include data into the new APLArtifactTable object
        if data_df is None:
            artifact_table = APLArtifactTable(name,
                                              type_name=type_name,
                                              apl_version=self._apl_version)
        else:
            # Transform dataframe data_df into a list of tuples [(col1, col2, ...)*]
            data_tuples = [tuple(row) for _, row in data_df.iterrows()]
            artifact_table = APLArtifactTable(name,
                                              type_name=type_name,
                                              apl_version=self._apl_version,
                                              data=data_tuples)
        return artifact_table

    @staticmethod
    def _determine_target_var_names(
            input_df,
            label=None,
            vardesc_df=None,
            varroles_df=None):
        """
        Determines the target variable name.

        Arguments:
        --------
        - input_df: hana.ml.dataframe
          The input dataset
        - label: str, optional
            The column name of label
        - vardesc_df : pandas.DataFrame, optional
            The description of the dataset.
            For every column, provide the following:
            RANK, NAME, STORAGE, VALUETYPE, KEYLEVEL, ORDERLEVEL,
                MISSINGSTRING, GROUPNAME, DESCRIPTION, OID
            If it is given, it will be directly applied. Default setting will be skipped.
        - varroles_df: pandas.DataFrame, optional
            Description about the roles of the variables.
            For every column, provide the following:
            NAME, ROLE, COMPOSITION_TYPE, COMPONENT_NAME, OID
        Returns:
        ------
        a list of names
        """
        # if varroles_df is provided,
        # find first var name with role='target' in varroles_df
        if varroles_df is not None:
            return varroles_df.loc[
                varroles_df['ROLE'] == 'target',
                'NAME'].values
        if label is not None:
            return [label]
        if vardesc_df is not None:
            # var name with max rank in vardesc_df
            return [vardesc_df.loc[vardesc_df['RANK'].idxmax(), 'NAME']]
        return [input_df.columns[-1]]

    def _check_value_type_of_target_vars( #pylint: disable=too-many-arguments
            self,
            data,
            good_type,
            label=None,
            vardesc_df=None,
            varroles_df=None):
        """
        Checks the target variable name is correctly set in vardesc table.

        Arguments:
        --------
        - data: hana.ml.dataframe
          The input dataset
        - good_type: 'continuous' for regression or 'nominal' for classification
        - vardesc_df : pandas.DataFrame
            The description of the dataset.
            For every column, provide the following:
            RANK, NAME, STORAGE, VALUETYPE, KEYLEVEL, ORDERLEVEL,
                MISSINGSTRING, GROUPNAME, DESCRIPTION, OID
            If it is given, it will be directly applied. Default setting will be skipped.
        - varroles_df: pandas.DataFrame
            Description about the roles of the variables.
            For every column, provide the following:
            NAME, ROLE, COMPOSITION_TYPE, COMPONENT_NAME, OID
        """
        target_var_names = self._determine_target_var_names(
            input_df=data,
            label=label,
            vardesc_df=vardesc_df,
            varroles_df=varroles_df)
        for name in target_var_names:
            value_type_of_target = vardesc_df.loc[vardesc_df.NAME == name,
                                                  'VALUETYPE'].values
            value_type_of_target = value_type_of_target[0]
            if value_type_of_target is not good_type:
                err = "Incorrect VALUETYPE. {varName} must be {good_type} instead of {badType} ".\
                    format(varName=name,
                           good_type=good_type,
                           badType=value_type_of_target)
                raise FitIncompleteError(err)

    def _arg_dictionary(self, name,
                        dictionary,
                        possible_values,
                        required=None):
        """
        Checks and returns the value of a parameter.
        This method works similarly to the _arg method defined in the ML_BASE class.
        This is a helper for argument value checking.
        It verifies that the given dictionary contains correct values
        For example, check the given dictionary has values in
                 ['continuous,' 'nominal', 'ordinal']:
        self._arg_dictionary(
            name='variable_value_types',
            dictionary=variable_value_types,
            possible_values=['continuous,' 'nominal', 'ordinal']
            )
        Arguments:
        -------
        name : str
            The parameter name. Used for error messages.
        dictionary : dict
            A dictionary.
        possible_values : list of values
            Possible values in the dictionary.
        required : boolean, optional
            Whether the argument is a required argument. Default is False.
        Returns:
        -------
        The dictionary if values are correct.
        The TypeError Exception if not.
        """
        if dictionary is None:
            return None
        # Check whether dictionary of type dict
        self._arg(name, dictionary, dict, required)
        # Check all values in dictionary are among possible_values
        bools = [v in possible_values for v in dictionary.values()]
        if not all(bools):
            template = 'Parameter {!r} must contain value in {!r}'
            raise TypeError(template.format(
                name, possible_values))
        return dictionary

    # ----- materialization and type conversions

    @staticmethod
    def _get_select_w_type_conv(data):
        """
        Makes a SQL statement for select * from <dataframe.select_statement>.
        The select statement includes all required type conversions for ANY.
        Arguments
        --------
        data : hana_ml DataFrame
            The input dataset
        Returns
        ------
        select SQL statement : str
        """
        sql = 'SELECT '
        for i, col_type in enumerate(data.dtypes()):
            name, type_ori = col_type[0], col_type[1]
            if i > 0:
                sql += ', '
            conv_fct = TYPE_CONVERSION_MAP.get(type_ori, None)
            if conv_fct is None:
                # no conversion
                sql += quotename(name)
            else:
                sql += (conv_fct.format(NAME=quotename(name))
                        + ' as '
                        + quotename(name))
        sql += ' FROM ({select})'.format(select=data.select_statement)
        return sql

    def _materialize_w_type_conv(self, name, data, force=True):
        """
        Materializes a DataFrame into a table.
        Local temporary column tables only.
        includes the type conversion for ANY procedures.

        Parameters
        ----------
        name : str
            The new table name
        data: hana_ml DataFrame
            The input dataset
        force : boolean, optional
            Whether to delete any existing table with the same name
        Returns
        -------
        a APLArtifactView
        """
        select_clause = self._get_select_w_type_conv(data)
        create_stmt = 'CREATE LOCAL TEMPORARY COLUMN TABLE {} AS ({})'.format(
            quotename(name), select_clause)
        with self.conn_context.connection.cursor() as cur:
            if force:
                try_drop(self.conn_context, name)
            execute_logged(cur, create_stmt)
        return APLArtifactView(name=name, select_clause=select_clause)

    def _create_view(self,
                     view_name,
                     data,
                     order_by=None,
                     force=True):
        """
        Creates a view with the DataFrame.
        The view includes type conversions for ANY if required.

        Parameters
        ----------
        view_name : str
            The new table name
        data: hana_ml DataFrame
            The input dataset given for fit or predict
        order_by: str
            The ORDER BY clause
        force : boolean, optional
            Whether to delete any existing table with the same name

        Returns
        -------
        An instance of APLArtifactView
        """
        # get select statement on data with type conversions
        select_clause = self._get_create_view_select_clause(data, order_by)
        create_view_statement = (
            'CREATE VIEW {name} AS ({select})'.format(
                name=quotename(view_name),
                select=select_clause
                )
            )
        self._view_name = view_name
        if force:
            self._try_drop_view(view_name)
        with self.conn_context.connection.cursor() as cur:
            execute_logged(cur, create_view_statement)
        return APLArtifactView(name=view_name, select_clause=select_clause)

    def _get_create_view_select_clause(self, data, order_by):
        """
        get select statement on data with type conversions
        """
        select_w_type_conv = self._get_select_w_type_conv(data)
        if order_by is not None:
            select_w_type_conv += ' ORDER BY ' + order_by
        return select_w_type_conv

    def _try_drop_view(self, names):
        """
        Drop the view or views.

        Parameters
        ----------
        conn : ConnectionContext
            The SAP HANA connection.
        names : str or list of str
            The name or names of the tables to drop.
        """
        if isinstance(names, _TEXT_TYPES):
            names = [names]
        with self.conn_context.connection.cursor() as cur:
            for name in names:
                sql = 'DROP VIEW {name}'.format(name=quotename(name))
                try:
                    execute_logged(cur, sql)
                except dbapi.Error:
                    pass

    @staticmethod
    def _get_new_column_name(old_col_re, old_col, new_col_re):
        r"""
        Returns the new name of a column provided in predictions.

        Parameters
        ---------
        old_col_re: str
            A regular expression like r'kts_(\d+)' to be applied on the old column
        old_col: str
            The old column name
        new_col_re: str
            Replacement string with the regular expression.

        Returns
        -------
        The new column name if old_col matches old_col_re.
        None if not

        Examples
        -------
        >>> _get_new_column_name(old_col_re=r'kts_(\d+)',
                                 old_col='kts_42',
                                 new_col_re=r'PREDICTED_\1')
        PREDICTED_44
        """
        new_col = None
        if re.search(old_col_re, old_col):
            new_col = re.sub(old_col_re, new_col_re, old_col)
        return new_col

    # --- get methods

    def get_summary(self):
        """
        Retrieves the summary table after model training.

        Returns
        -------
        The reference to the SUMMARY table : hana_ml DataFrame
            This contains execution summary of the last model training
        """
        if not hasattr(self, 'summary_'):
            raise FitIncompleteError("Summary not initialized. Perform a fit first.")
        return self.summary_

    def get_indicators(self):
        """
        Retrieves the Indicator table after model training.

        Returns
        -------
        The reference to INDICATORS table : hana_ml DataFrame
            This table provides the performance metrics of the last model training
        """
        if not hasattr(self, 'indicators_'):
            raise FitIncompleteError("Indicators not initialized. Perform a fit first.")
        return self.indicators_


    def get_fit_operation_log(self):
        """
        Retrieves the operation log table after the model training.
        This table contains the log given by SAP HANA APL during the last fit operation.

        Returns
        -------
        The reference to OPERATION_LOG table : hana_ml DataFrame
            This table provides detailed logs of the last model training
        """
        if not hasattr(self, 'fit_operation_log_'):
            raise FitIncompleteError("Fit operation log not found. Run the fit method first.")
        return self.fit_operation_log_

    def _get_apply_out_table_def(self, apply_in_table_name, apply_config_table): #pylint: disable=too-many-locals
        """
        Returns the table spec [(columnName, columnType)*] of the table apply-out
        (the output table of predict).

        Parameters
        ----------
        apply_in_table_name : str
            The table name of apply in dataset (String)
        apply_config_table : APLArtifactTable object
            The APPLY_CONFIG table that contains configuration for apply

        Returns
        -------
        Table definition.
        For instance: '(col_1 char(1), col_2 integer)'
        """
        if getattr(self, 'model_table_', None) is None:
            raise FitIncompleteError("Model not initialized. Perform a fit first.")

        apply_in_table = APLArtifactView(
            name=apply_in_table_name,
            select_clause=f"SELECT * FROM {apply_in_table_name}")

        # Header func
        func_header_table = self._create_func_header_table()

        # OPERATION_LOG,
        operation_log_table = self._create_operation_log_table(
            '#GET_TABLE_TYPE_FOR_APPLY_LOG_{}'.format(self.id))
        # SUMMARY,
        apply_out_table_type = APLArtifactTable(
            name='#APLYOUT_TABLE_TYPE_{}'.format(self.id),
            type_name=APLArtifactTable.TABLE_TYPE,
            apl_version=self._apl_version,
            )
        try:
            self._create_artifact_table(func_header_table)
            self._create_artifact_table(operation_log_table)
            self._create_artifact_table(apply_out_table_type)

            # Call procedure
            self._call_apl(
                "APL_GET_TABLE_TYPE_FOR_APPLY",
                input_tables=[
                    func_header_table,
                    self.model_table_,
                    apply_config_table,
                    apply_in_table,
                ],
                output_tables=[
                    apply_out_table_type,
                    operation_log_table
                ]
            )
        except dbapi.Error as db_er:
            logger.error('Error while calling \'APL_GET_TABLE_TYPE_FOR_APPLY\': %s',
                         db_er, exc_info=True)
            raise

        # Read the output and translate into spec [(ColumnName, ColumnType)*]
        # spec = []
        table_def = '({col_defs})'

        # get Dataframe mapped to table apply_out_table_type
        h_table_df = self.conn_context.table(apply_out_table_type.name)
        # sort by column no
        types = pd.DataFrame(h_table_df.collect()).sort_values(['POSITION'])
        # Create a list ['col_i type_i', ...]
        col_defs = []
        for i in range(len(types)):
            col_name = types.NAME.iloc[i]
            kind = types.KIND.iloc[i]
            max_len = types.MAXIMUM_LENGTH.iloc[i]
            col_type = kind
            if (max_len is not None) and (not isnan(max_len)):
                col_type = col_type + '(' + str(int(max_len)) + ')'
            # spec.append((col_name, col_type))
            col_defs.append('{col_name} {col_type}'.format(
                col_name=quotename(col_name),
                col_type=col_type
            ))

        col_defs = ', '.join(col_defs)
        table_def = table_def.format(col_defs=col_defs)
        logger.info("ApplyOut Table def: %s", table_def)
        return table_def

    def get_predict_operation_log(self):
        """
        Retrieves the operation log table after the model training.

        Returns
        -------
        The reference to the OPERATION_LOG table : hana_ml DataFrame
            This table provides detailed logs about the last prediction
        """
        if not hasattr(self, 'predict_operation_log_'):
            raise AttributeError("Predict operation log not found. Run the predict method first.")
        return self.predict_operation_log_

    def _guess_var_description(self, table_name):
        """
        Guesses a variable description.

        Parameters
        ----------
        table_name : str.
            The name of the table for which the variable description will be guessed

        Returns
        -------
        A pandas DataFrame object containing the variable description
        """
        # Input dataset
        input_table = APLArtifactView(
            name=table_name,
            select_clause=f"SELECT * FROM {table_name}")

        # -- Prepare Input parameter tables
        func_header_table = self._create_func_header_table()

        # OPERATION_CONFIG
        config_table = APLArtifactTable(
            name='#GUESSVAR_CONFIG_{}'.format(self.id),
            type_name=APLArtifactTable.OPERATION_CONFIG_EXTENDED,
            apl_version=self._apl_version)
        config_table = config_table.with_data(
            [('APL/ModelType', 'regression/classification', None)]
            )

        # MODEL_TRAIN_BIN
        model_table = APLArtifactTable(
            name='#GUESSVAR_MODEL_TRAIN_BIN_{}'.format(self.id),
            type_name=APLArtifactTable.MODEL_BIN_OID,
            apl_version=self._apl_version)
        # VARIABLE_DESC (output)
        var_desc_table = APLArtifactTable(
            name='#VARIABLE_DESC_{}'.format(self.id),
            type_name=APLArtifactTable.VARIABLE_DESC_OID,
            apl_version=self._apl_version
            )
        try:
            self._create_artifact_table(func_header_table)
            self._create_artifact_table(config_table)
            self._create_artifact_table(model_table)
            self._create_artifact_table(var_desc_table)

            # --- Call procedure
            self._call_apl("APL_CREATE_MODEL",
                           input_tables=[
                               func_header_table,
                               config_table,
                               input_table,
                           ],
                           output_tables=[
                               model_table,
                               var_desc_table
                           ])
        except dbapi.Error as db_er:
            # clean up the table used in fit function
            logger.error("Failed to guess variable description, the error message: %s",
                         db_er, exc_info=True)
            self._drop_artifact_tables()
            raise
        pd_df = self.conn_context.table(var_desc_table.name).collect()
        return pd_df

    def save_artifact(
            self,
            artifact_df,
            schema_name, table_name,
            if_exists='fail',
            new_oid=None):
        #pylint: disable=too-many-arguments
        #pylint: disable=too-many-locals
        """
        Saves an artifact, a temporary table, into a permanent table.
        The model has to be trained or fitted beforehand.

        Parameters
        ----------
        schema_name: str
            The schema name
        artifact_df : hana_ml DataFrame
            The artifact created after fit or predict methods are called
        table_name: str
            The table name
        if_exists: str. {'fail', 'replace', 'append'}, default 'fail'
            The behavior when the table already exists:

            - fail: Raises a ValueError
            - replace: Drops the table before inserting new values
            - append: Inserts new values to the existing table

        new_oid: str. Optional.
            If it is given, it will be inserted as a new OID value.
            It is useful when one like to save data into the same table.

        Examples
        --------
        >>> myModel.save_artifact(artifactTable=myModel.indicators_,
        ...                       schema_name='MySchema',
        ...                       table_name='MyModel_Indicators',
        ...                       if_exists='replace')
        """
        if artifact_df is None:
            raise FitIncompleteError(
                "Artifact is none. Call fit or predict first.")

        # Check if table exists
        with self.conn_context.connection.cursor() as cursor:
            # Try to drop table if already exists
            table_exists = False
            try:
                sql = 'select 1 from {schema_name}.{table_name} limit 1'.format(
                    schema_name=quotename(schema_name),
                    table_name=quotename(table_name))
                execute_logged(cursor, sql)
                table_exists = True
            except dbapi.Error:
                pass

            # Replace OID with new_oid
            colnames = '*'
            if new_oid is not None:
                # Replace OID by new_oid
                # colnames = 'col1, col2, ..., coln'
                colnames = functools.reduce(
                    (lambda x, y: x + ',' + y),
                    [quotename(colname) for colname in artifact_df.columns])
                # colnames = ', '.join(artifact_df.columns)
                colnames = colnames.replace(quotename('OID'),
                                            "'{}' OID".format(new_oid),
                                            1)
            if table_exists:
                if if_exists == 'fail':
                    raise FitIncompleteError(
                        "Table {schema_name}.{table_name} already exists.".format(
                            schema_name=quotename(schema_name),
                            table_name=quotename(table_name)))
                if if_exists == 'replace':
                    # drop table if already exists
                    sql = 'DROP TABLE {schema_name}.{table_name}'.format(
                        schema_name=quotename(schema_name),
                        table_name=quotename(table_name))
                    execute_logged(cursor, sql)

                    # Create table and insert data
                    # artifact_df.save(where=(schema_name, table_name),
                    #                 table_type='COLUMN')
                    sql = 'CREATE COLUMN TABLE {schema_name}.{table_name}'.format(
                        schema_name=quotename(schema_name),
                        table_name=quotename(table_name))
                    sql = sql + ' AS (SELECT {COLS} FROM ({SOURCE}))'.format(
                        SOURCE=artifact_df.select_statement,
                        COLS=colnames
                        )
                    execute_logged(cursor, sql)
                elif if_exists == 'append':
                    # append data of artifact_df into table
                    sql = 'INSERT INTO {schema_name}.{table_name}'.format(
                        schema_name=quotename(schema_name),
                        table_name=quotename(table_name))
                    sql = sql + ' (SELECT {COLS} FROM ({SOURCE}))'.format(
                        SOURCE=artifact_df.select_statement,
                        COLS=colnames
                        )
                    execute_logged(cursor, sql)
            else:
                # If table does exist
                sql = 'CREATE COLUMN TABLE {schema_name}.{table_name}'.format(
                    schema_name=quotename(schema_name),
                    table_name=quotename(table_name))
                sql = sql + ' AS (SELECT {COLS} FROM ({SOURCE}))'.format(
                    SOURCE=artifact_df.select_statement,
                    COLS=colnames
                    )
                execute_logged(cursor, sql)

        logger.info(
            'Artifact (%s) is saved into {%s}.{%s}',
            artifact_df.select_statement,
            schema_name,
            table_name)

    def save_model(self, schema_name, table_name, if_exists='fail',
                   new_oid=None):
        """
        Saves the model into a table.
        The model has to be trained beforehand.
        The model can be saved either into a new table (if_exists='replace'), or an existing table
        (if_exists='append').
        In the latter case, the user can provide an identifier value (new_oid).
        The oid must be unique.
        By default, this oid is set when the model is created in Python (model.id attribute).

        .. warning::
            This method is deprecated. Please use hana_ml.model_storage.ModelStorage.

        Parameters
        ----------
        schema_name: str
            The schema name
        table_name: str
            Table name
        if_exists: str. {'fail', 'replace', 'append'}, default 'fail'
            The behavior when the table already exists:

            - fail: Raises an Error
            - replace: Drops the table before inserting new values
            - append: Inserts new values to the existing table

        new_oid: str. Optional.
            If it is given, it will be inserted as a new OID value.
            It is useful when one wants to save data into the same table.

        Returns
        -------
        None
            The model is saved into a table with the following columns:

            - "OID" NVARCHAR(50),     -- Serve as ID
            - "FORMAT" NVARCHAR(50),  -- APL technical info
            - "LOB" CLOB MEMORY THRESHOLD NULL  -- binary content of the model
        """
        _show_deprecated_warning('save_model',
                                 'Please use hana_ml.model_storage.ModelStorage instead')
        if getattr(self, 'model_', None) is None:
            raise FitIncompleteError(
                "Model not initialized. Perform a fit first.")
        self.save_artifact(artifact_df=self.model_,
                           schema_name=schema_name,
                           table_name=table_name,
                           if_exists=if_exists,
                           new_oid=new_oid)

    def load_model(self, schema_name, table_name, oid=None):
        """
        Loads the model from a table.

        .. warning::
            This method is deprecated. Please use hana_ml.model_storage.ModelStorage.

        Parameters
        ----------
        schema_name: str
            The schema name
        table_name: str
            The table name
        oid : str. optional
            If the table contains several models,
            the oid must be given as an identifier.
            If it is not provided, the whole table is read.
        """
        _show_deprecated_warning('load_model',
                                 'Please use hana_ml.model_storage.ModelStorage instead')
        # Create a temporary table
        temp_table_name = '#MODEL_TRAIN_BIN_{}'.format(self.id)
        model_table = APLArtifactTable(
            name=temp_table_name,
            type_name=APLArtifactTable.MODEL_BIN_OID,
            apl_version=self._apl_version)
        self._create_artifact_table(model_table)
        # Copy the content of the existing model
        with self.conn_context.connection.cursor() as cursor:
            sql = ('INSERT INTO {temp_table_name} '
                   + '(SELECT * FROM {schema_name}.{table_name}').format(
                       temp_table_name=quotename(temp_table_name),
                       schema_name=quotename(schema_name),
                       table_name=quotename(table_name))
            if oid is None:
                sql = sql + ')'
            else:
                sql = sql + " WHERE OID='{OID}')".format(OID=oid)
            execute_logged(cursor, sql)
        self.model_table_ = model_table
        self.model_ = self.conn_context.table(temp_table_name)

    def _load_model_tables(self, schema_name, model_table_names, name, version, conn_context=None):
        """
        Copies the model content into a new artifact table.
        This method is used for model storage services.

        Parameters
        ----------
        schema: str
            The schema name
        model_table_names: dict
            The dictionary is composed of
                Key = Model attribute name (for example, model_)
                value = the table name
        """

        if conn_context is None:
            conn_context = self.conn_context
        elif not self.conn_context:
            self._set_conn_context(conn_context)

        table_name = model_table_names[0]
        # Creates a temporary table
        temp_table_name = '#MODEL_TRAIN_BIN_{}'.format(self.id)

        # Creates artifact table for model
        model_table = APLArtifactTable(
            name=temp_table_name,
            type_name=APLArtifactTable.MODEL_BIN_OID,
            apl_version=self._apl_version)
        self._create_artifact_table(model_table)

        # Determines the column names to be copied (do not take NAME and VERSION)
        sql = 'select * FROM {temp_table_name} limit 1'.format(
            temp_table_name=quotename(temp_table_name))
        tmp_table_df = DataFrame(conn_context, sql)
        col_names = functools.reduce(
            (lambda x, y: x + ',' + y),
            [quotename(colname) for colname in tmp_table_df.columns])

        # Copies the content of the existing model
        with conn_context.connection.cursor() as cursor:
            sql = ('INSERT INTO {temp_table_name} '
                   + '(SELECT {col_names} FROM {schema_name}.{table_name}').format(
                       col_names=col_names,
                       temp_table_name=quotename(temp_table_name),
                       schema_name=quotename(schema_name),
                       table_name=quotename(table_name))
            sql = sql + " WHERE NAME='{name}' and VERSION={version})".format(
                name=name.replace("'", "''"),
                version=version)
            execute_logged(cursor, sql)
        self.model_table_ = model_table
        self.model_ = conn_context.table(temp_table_name)


    def _check_valid(self, data, key=None, features=None, label=None, weight=None):
        """
        Checks whether key, features, label, weight, and segment_column_name are valid with respect to the given DataFrame (data).
        Raises ValueError if any parameter is invalid, mentioning only the first invalid one.
        """
        if features is not None:
            if not features:
                raise ValueError('The list of features provided is empty.')

        # Define the names and values to check
        params = [
            ('key', key),
            ('label', label),
            ('weight', weight),
            ('segment_column_name', getattr(self, 'segment_column_name', None))
        ]

        # Check features individually if provided
        if features is not None:
            for feature_index, feature in enumerate(features):
                if feature not in data.columns:
                    raise ValueError(f"Feature at index {feature_index} ('{feature}') is not a valid column in the DataFrame.")

        # Check other parameters
        for name, value in params:
            if value is not None and value not in data.columns:
                raise ValueError(f"The parameter '{name}' with value '{value}' is not a valid column in the DataFrame.")

    def _fit(self,
             data,
             key=None,
             features=None,
             label=None,
             weight=None):
        # pylint:disable=too-many-locals
        # take in all feature labels except label column
        """
        Fits the model when given training dataset and other attributes.

        Parameters
        ----------
        data : DataFrame
            The training dataset.
        key : str, optional
            The name of the ID column.
            If `key` is not provided, the input is supposed to have no ID.
            If 'key' is given, it will be set and skipped as feature column.
        features : list of str, optional
            The names of the feature columns.
            If `features` is not provided, it defaults to all non-ID,
            non-label columns.
        label : str, optional
            The name of the dependent variable. Defaults to the last column.
        weight : str, optional
            The name of the weight variable.
            A weight variable allows one to assign a relative weight to each of the observations.

        """
        # -- get and check the validity of input params key, features, label
        key = self._arg('key', key, str)
        features = self._arg('features', features, ListOfStrings)
        label = self._arg('label', label, str)
        weight = self._arg('weight', weight, str)
        self._check_valid(data, key, features, label, weight)

        self._set_conn_context(data.connection_context)

        # -- prepare TRAIN_CONFIG artifact
        train_config_table = self._create_train_config_table()

        try:
            # ---- prepare train dataset
            # Before doing a guess variable, need to materialize the dataset.
            # We can either make a view or create a new table
            # View offers more performance
            # But view is impossible when the dataset is temporary table
            try:
                data_view_name = "TRAIN_DATA_VIEW_{}".format(self.id)
                # Create a hana view and return a APLArtifactView instance
                data_view = self._create_view(view_name=data_view_name, data=data)
            except dbapi.Error:
                # fall back if view can't be created
                # (because the original dataset is a temp table)
                data_view_name = "#TRAIN_DATA_VIEW_{}".format(self.id)
                data_view = self._materialize_w_type_conv(name=data_view_name, data=data)

            # ---- VARIABLE_DESCRIPTION
            var_desc_table = self._create_var_desc_table(key, label, data_view_name)

            # --- HEADER_FUNCTION
            func_header_table = self._create_func_header_table()

            # --- VARIABLE_ROLES
            var_roles_table = self._create_var_roles_table(
                data=data,
                key=key,
                label=label,
                features=features,
                weight=weight,
            )
            # --- Prepare Output tables
            # MODEL_TRAIN_BIN
            model_table = APLArtifactTable(
                name='#MODEL_TRAIN_BIN_{}'.format(self.id),
                type_name=APLArtifactTable.MODEL_BIN_OID,
                apl_version=self._apl_version)
            # OPERATION_LOG,
            operation_log_table = self._create_operation_log_table(
                '#FIT_LOG_{}'.format(self.id))

            # SUMMARY,
            summary_table = self._create_summary_table()

            # INDICATORS
            indicators_table = APLArtifactTable(
                name='#INDICATORS_{}'.format(self.id),
                type_name=APLArtifactTable.INDICATORS,
                apl_version=self._apl_version)

            # Materialize artifacts prior to calling APL
            self._create_artifact_table(func_header_table)
            self._create_artifact_table(train_config_table)
            self._create_artifact_table(var_desc_table)
            self._create_artifact_table(var_roles_table)
            self._create_artifact_table(model_table)
            self._create_artifact_table(operation_log_table)
            self._create_artifact_table(summary_table)
            self._create_artifact_table(indicators_table)

            # --- Call procedure
            if not self._disable_hana_execution:
                self._call_apl(
                    "APL_CREATE_MODEL_AND_TRAIN",
                    input_tables=[
                        func_header_table,
                        train_config_table,
                        var_desc_table,
                        var_roles_table,
                        data_view,
                    ],
                    output_tables=[
                        model_table,
                        operation_log_table,
                        summary_table,
                        indicators_table
                    ]
                    )
            # Records the SQL artifacts that can be restituted later for HDI
            self._record_sql_artifact(
                model_method='fit',
                apl_function="APL_CREATE_MODEL_AND_TRAIN",
                input_tables=[
                    func_header_table,
                    train_config_table,
                    var_desc_table,
                    var_roles_table,
                    data_view,
                ],
                output_tables=[
                    model_table,
                    operation_log_table,
                    summary_table,
                    indicators_table
                ])

            self.debrief_metric_table_ = None
            self.debrief_property_table_ = None
        except dbapi.Error as db_er:
            # do stuff with the error, and also raise to the caller,
            # clean up the table used in fit function
            logger.error("Fit failure, the error message: %s",
                         db_er, exc_info=True)
            self._drop_artifact_tables()
            raise
        finally:
            # clean created view
            self._try_drop_view(data_view_name)

        # --- Save returned artifacts
        # --- Table containing the model
        self.model_table_ = model_table #pylint:disable=attribute-defined-outside-init
        # Convert the model Table into a Hana DataFrame
        self.model_ = self.conn_context.table(model_table.name) #pylint:disable=attribute-defined-outside-init
        # It is useless to keep the model as Dataframe, just for compatibility
        # --- capture the other output artifacts as hana Dataframes
        self.summary_ = self.conn_context.table(summary_table.name) #pylint:disable=attribute-defined-outside-init
        self.indicators_ = self.conn_context.table(indicators_table.name) #pylint:disable=attribute-defined-outside-init
        self.fit_operation_log_ = self.conn_context.table( #pylint:disable=attribute-defined-outside-init
            operation_log_table.name)
        self.var_desc_ = self.conn_context.table(var_desc_table.name) #pylint:disable=attribute-defined-outside-init
        self.applyout_ = None

        return self

    def _predict(self, data, apply_config_data_df=None, generate_debrief=False):
        """
        Predicts values based on the fitted model.

        Parameters
        ----------
        data : hana-ml DataFrame
            The dataset to be used for prediction
        apply_config_data_df: pandas DataFrame, mandatory
            APL Apply function parameters.
            It must be provided by the call.
            It defined the content expected in the output.
        generate_debrief: boolean, optional
            If True, the debriefing tables are generated.

        Returns
        -------
        Prediction output: hana_ml DataFrame
        """
        # -- Gets and check args
        if getattr(self, 'model_table_', None) is None:
            raise FitIncompleteError("Model not initialized. Perform a fit first.")

        if apply_config_data_df is None:
            raise FitIncompleteError("Apply config undefined")
        # Converts into List of tuples
        apply_config_data_tuples = [tuple(row)
                                    for _, row in
                                    apply_config_data_df.iterrows()]

        segment_column_name = getattr(self, 'segment_column_name', None)
        if segment_column_name is not None:
            apply_config_data_tuples.append(('APL/SegmentColumnName', segment_column_name, None))

        apply_config_data_tuples.append(('DataSets/ApplyOut/Parameters/MappingReportFailureUILevel', 'FullReport', None))

        self._set_conn_context(data.connection_context)
        try:
            # ApplyIn dataset
            # We can either make a view or create a new table
            # View offers more performance
            # But view is impossible when the dataset is temporary table
            try:
                apply_in_tbl = "APPLYIN_TBL_{}".format(self.id)
                apply_in_view = self._create_view(view_name=apply_in_tbl, data=data)
            except dbapi.Error:
                apply_in_tbl = "#APPLYIN_TBL_{}".format(self.id)
                apply_in_view = self._materialize_w_type_conv(name=apply_in_tbl, data=data)

            # -- Prepare Input parameter tables
            apply_config_table = APLArtifactTable(
                name='#APPLY_CONFIG_{}'.format(self.id),
                type_name=APLArtifactTable.OPERATION_CONFIG_EXTENDED,
                apl_version=self._apl_version,
                data=apply_config_data_tuples)
            # Need to materialize table prior to request applyout table definition
            self._create_artifact_table(apply_config_table)

            # call APL_GET_TABLE_TYPE_FOR_APPLY to get applyout table definition
            # Warning: this APL call will overwrite artifact table
            # such as FUNCT_HEADER, OPERATION_LOG_
            # So, do not create them beforehand
            applyout_table_def = self._get_apply_out_table_def(apply_in_tbl, apply_config_table)
            # Create Apply-out table
            applyout_table = APLArtifactApplyOutTable(
                name='#APPLYOUT_%s' % (self.id),
                table_definition=applyout_table_def,
                apl_version=self._apl_version,
                data=None
            )

            # FUNC_HEADER
            func_header_table = self._create_func_header_table()

            # OPERATION_LOG
            apply_log_table = self._create_operation_log_table(
                name='#APPL_LOG_{}'.format(self.id))

            # SUMMARY
            summary_table = self._create_summary_table()

            # Materialize artifacts prior to calling APL
            self._create_artifact_table(func_header_table)
            self._create_artifact_table(applyout_table)
            self._create_artifact_table(apply_log_table)
            self._create_artifact_table(summary_table)

            call_apply_model_debrief = (generate_debrief and self._get_apl_version() >= 2325)

            if call_apply_model_debrief:
                # DEBRIEF_METRIC
                debrief_metric_table = self._create_debrief_metric_table()

                # DEBRIEF_PROPERTY
                debrief_property_table = self._create_debrief_property_table()

                self._create_artifact_table(debrief_metric_table)
                self._create_artifact_table(debrief_property_table)

                apply_procedure_name = 'APL_APPLY_MODEL_DEBRIEF'
                apply_overload_procedure_name = 'APL_APPLY_MODEL__OVERLOAD_4_5'
            else:
                apply_procedure_name = 'APL_APPLY_MODEL'
                apply_overload_procedure_name = 'APL_APPLY_MODEL__OVERLOAD_4_3'

            # --- Call procedure
            if self._sql_generation_mode == SqlGenerationMode.SQLBLOCK_DU:
                procedure_name = apply_procedure_name
            else:
                procedure_name = apply_overload_procedure_name
            if not self._disable_hana_execution:
                if self._sql_generation_mode in (SqlGenerationMode.SQLBLOCK_PROCANY,\
                                                 SqlGenerationMode.PROCEDURE_PROCANY):
                    output_tables = [
                        applyout_table,
                        apply_log_table,
                        summary_table
                    ]
                    if call_apply_model_debrief:
                        output_tables.extend([debrief_metric_table, debrief_property_table])
                    self._call_apl(
                        procedure_name,
                        input_tables=[
                            func_header_table,
                            self.model_table_,
                            apply_config_table,
                            apply_in_view,
                        ],
                        output_tables=output_tables)
                else:
                    # Block SQL + DU
                    output_tables = [
                        apply_log_table,
                        summary_table
                    ]
                    if call_apply_model_debrief:
                        output_tables.extend([debrief_metric_table, debrief_property_table])
                    self._call_apl(
                        procedure_name,
                        input_tables=[
                            func_header_table,
                            self.model_table_,
                            apply_config_table,
                            apply_in_view,
                            # For DU, the applyout table must be given as string (it suppose it was created beforehand)
                            APLArtifactView(name=applyout_table.name,
                                            select_clause=f"SELECT * FROM {applyout_table.name}"),
                        ],
                        output_tables=output_tables)
            # Records the SQL artifacts that can be restituted later for HDI
            output_tables = [
                applyout_table,
                apply_log_table,
                summary_table
            ]
            if call_apply_model_debrief:
                output_tables.extend([debrief_metric_table, debrief_property_table])
            self._record_sql_artifact(
                model_method='predict',
                apl_function=apply_overload_procedure_name,
                input_tables=[
                    func_header_table,
                    self.model_table_,
                    apply_config_table,
                    apply_in_view,
                ],
                output_tables=output_tables,
                filled_table_names=(self.model_table_.name)
                )
            self.applyout_ = self.conn_context.table(applyout_table.name) #pylint:disable=attribute-defined-outside-init
            self.applyout_table_ = applyout_table #pylint:disable=attribute-defined-outside-init
            self.predict_operation_log_ = self.conn_context.table( #pylint:disable=attribute-defined-outside-init
                apply_log_table.name)
            self.summary_ = self.conn_context.table(summary_table.name) #pylint:disable=attribute-defined-outside-init
            if call_apply_model_debrief:
                self.debrief_metric_table_ = debrief_metric_table #pylint:disable=attribute-defined-outside-init
                self.debrief_property_table_ = debrief_property_table #pylint:disable=attribute-defined-outside-init
            return self.applyout_
        except dbapi.Error as db_er:
            logger.error('Error while calling \'APL_APPLY_MODEL\': %s', db_er, exc_info=True)
            raise
        finally:
            self._try_drop_view(apply_in_tbl)

    def get_model_info(self):
        """
        Get information about an existing model.
        This method is especially useful when a trained model was saved and reloaded.
        After having called this method, the model can provide summary and metrics again
        as there were in the last fit.

        Returns
        -------
            list
                List of HANA DataFrames respectively corresponding to the following tables:

                - Summary table
                - Variable roles table
                - Variable description table
                - Indicators table
                - Profit curves table
        """
        # === INPUT ===============
        # Model table must exists beforehand
        if getattr(self, 'model_table_', None) is None:
            raise FitIncompleteError("Model not initialized. Please perform a fit first.")
        # --- HEADER_FUNCTION
        func_header_table = self._create_func_header_table()
        self._create_artifact_table(func_header_table)
        # --- OPERATION_CONFIG
        config_table = self._create_basic_config_table()
        self._create_artifact_table(config_table)
        # === OUTPUT ===============
        # --- SUMMARY
        summary_table = self._create_summary_table()
        self._create_artifact_table(summary_table)
        # --- VARIABLE_ROLES
        varroles_table = APLArtifactTable(
            name='#VARIABLE_ROLES_{}'.format(self.id),
            type_name=APLArtifactTable.VARIABLE_ROLES_WITH_COMPOSITES_OID,
            apl_version=self._apl_version)
        self._create_artifact_table(varroles_table)
        # --- VARIABLE_DESC
        vardesc_table = APLArtifactTable(
            name='#VARIABLE_DESC_{}'.format(self.id),
            type_name=APLArtifactTable.VARIABLE_DESC_OID,
            apl_version=self._apl_version)
        self._create_artifact_table(vardesc_table)
        # --- INDICATORS
        indicators_table = APLArtifactTable(
            name='#INDICATORS_{}'.format(self.id),
            type_name=APLArtifactTable.INDICATORS,
            apl_version=self._apl_version)
        self._create_artifact_table(indicators_table)
        # --- PROFITCURVES
        profitcurves_table = APLArtifactTable(
            name='#PROFITCURVES_{}'.format(self.id),
            type_name=APLArtifactTable.PROFITCURVES,
            apl_version=self._apl_version)
        self._create_artifact_table(profitcurves_table)
        # --- Call procedure
        output_tables = [
            summary_table,
            varroles_table,
            vardesc_table,
            indicators_table,
            profitcurves_table,
        ]
        self._call_apl(
            "APL_GET_MODEL_INFO",
            input_tables=[
                func_header_table,
                self.model_table_,
                config_table,
            ],
            output_tables=output_tables,
        )
        # pylint:disable=attribute-defined-outside-init
        self.summary_ = self.conn_context.table(summary_table.name)
        # pylint:disable=attribute-defined-outside-init
        self.indicators_ = self.conn_context.table(indicators_table.name)
        return [self.conn_context.table(output_t.name) for output_t in output_tables]

    def get_apl_version(self):
        """
        Gets the version and configuration information about the installation of SAP HANA APL.

        Returns
        -------
        A pandas Dataframe with detailed information about the current version.

        Notes
        -----
        Error is raised when the call fails.
        The cause can be that either SAP HANA APL is not installed
        or the current user does not have the appropriate rights.
        """
        ping_output_table = APLArtifactTable(
            name='#PING_OUTPUT{}'.format(self.id),
            type_name=APLArtifactTable.PING_OUTPUT,
            apl_version=None,
        )
        try:
            self._create_artifact_table(ping_output_table)
            self._call_apl(
                "APL_AREA_PING_PROC",
                input_tables=[],
                output_tables=[ping_output_table]
            )
        except dbapi.Error as db_er:
            # do stuff with the error, and also raise to the caller,
            # clean up the table used in fit function
            logger.error("Fit failure, the error message: %s",
                         db_er, exc_info=True)
            self._drop_artifact_tables()
            raise
        return self.conn_context.table(ping_output_table.name).collect()

    def _generate_debrief_tables(self):
        if self.model_table_ is None:
            raise FitIncompleteError("Model not initialized. Perform a fit first.")

        try:
            func_header_table = self._create_func_header_table()
            config_table = self._create_basic_config_table()

            # DEBRIEF_METRIC
            debrief_metric_table = self._create_debrief_metric_table()

            # DEBRIEF_PROPERTY
            debrief_property_table = self._create_debrief_property_table()

            # SUMMARY
            summary_table = self._create_summary_table(name='#DEBRIEF_SUMMARY_{}'.format(self.id))

            self._create_artifact_table(func_header_table)
            self._create_artifact_table(config_table)
            self._create_artifact_table(debrief_metric_table)
            self._create_artifact_table(debrief_property_table)
            self._create_artifact_table(summary_table)

            self._call_apl(
                "APL_GET_MODEL_DEBRIEF",
                input_tables=[
                    func_header_table,
                    self.model_table_,
                    config_table
                ],
                output_tables=[
                    debrief_metric_table,
                    debrief_property_table,
                    summary_table
                ]
            )

            self.debrief_metric_table_ = debrief_metric_table
            self.debrief_property_table_ = debrief_property_table
        except dbapi.Error as db_er:
            logger.error("Error while generating the debrief tables: %s", db_er, exc_info=True)
            raise

    def get_debrief_report(self, report_name, **params):
        """
        Retrieves a standard statistical report.
        See `Statistical Reports
        <https://help.sap.com/viewer/7223667230cb471ea916200712a9c682/latest/en-US/3914ca6b6fd049d6a1b5d6513b248a2b.html>`_ in the SAP HANA APL Developer Guide.

        Parameters
        ----------
        report_name: str
        params: dict
            Additional parameters to be passed to the report query.

        Returns
        -------
        Statistical report: hana_ml DataFrame
        """
        if self.debrief_metric_table_ is None or self.debrief_property_table_ is None:
            self._generate_debrief_tables()

        params_str = ', '.join([f'{k} => {v}' for k, v in params.items() if v is not None])
        if params_str:
            params_str = ', ' + params_str

        query = f'SELECT * FROM "SAP_PA_APL"."sap.pa.apl.debrief.report::{report_name}"({self.debrief_property_table_.name}, {self.debrief_metric_table_.name}{params_str});'

        return self.conn_context.sql(query)

    def export_apply_code(self, code_type, key=None, label=None, schema_name=None, table_name=None,
                          other_params=None):
        """
        Exports code (SQL, JSON, etc.) so that you can apply a trained model outside SAP HANA APL.
        See Function Reference > Predictive Model Services > *EXPORT_APPLY_CODE*
        in the `SAP HANA APL Developer Guide <https://help.sap.com/viewer/p/apl>`_.

        Parameters
        ----------

        code_type: str
            The type of code exported.
            The supported code types for each type of model are given in the
            developer guide (see APL/CodeType).
        key: str, optional
            The name of the primary key column.
            Required for some code types.
        label: str, optional
            The name of the label (target) column.
            Used only when the model supports multiple targets.
            When left to "", this means that all targets must be generated.
        schema_name: str, optional
            The schema name of the apply-in table.
            Required for some code types.
        table_name: str, optional
            The apply-in table name.
            Required for some code types.
        other_params: dict, optional
            The additional parameters to be included in the configuration.
            The available parameters are given in the developer guide.

        Returns
        -------

        str or pandas DataFrame
            If no segment column is given, the exported code.

            If a segment column is given, a pandas DataFrame which contains the exported code
            for each segment.

        Examples
        --------

        Exporting a model in JSON format (available for Gradient Boosting and Robust Regression)

        >>> json_export = model.export_apply_code('JSON')

        APL provides a JavaScript runtime in which you can make predictions based on any model that
        has been exported in JSON format. See *JavaScript Runtime* in the `SAP HANA APL Developer
        Guide <https://help.sap.com/viewer/p/apl>`_.

        Exporting SQL apply code (available for Robust Regression and Clustering)

        >>> sql = model.export_apply_code(
        ...     code_type='HANA',
        ...     key='id',
        ...     schema_name='APL_SAMPLES',
        ...     table_name='CENSUS')

        Exporting SQL apply code (probability generated in the output)

        >>> sql = model.export_apply_code(
        ...     code_type='HANA',
        ...     key='id',
        ...     schema_name='APL_SAMPLES',
        ...     table_name='CENSUS',
        ...     other_params={'APL/ApplyExtraMode': 'Advanced Apply Settings',
        ...                   'APL/ApplyProba': 'true'})
        """

        if self.model_table_ is None:
            raise FitIncompleteError("Model not initialized. Perform a fit first.")

        try:
            func_header_table = self._create_func_header_table()

            config = []
            config.append(('APL/CodeType', code_type, None))
            if key is not None:
                config.append(('APL/CodeKey', key, None))
            if label is not None:
                config.append(('APL/CodeTarget', label, None))
            if schema_name is not None and table_name is not None:
                config.append(('APL/CodeSpace', schema_name + '.' + table_name, None))
            if other_params is not None:
                for k in other_params:
                    config.append((k, other_params[k], None))

            segment_column_name = getattr(self, 'segment_column_name', None)
            if segment_column_name is not None:
                config.append(('APL/SegmentColumnName', segment_column_name, None))

            export_config = APLArtifactTable(
                name='#EXPORT_CONFIG{}'.format(self.id),
                type_name=APLArtifactTable.OPERATION_CONFIG_EXTENDED,
                apl_version=self._apl_version,
                data=config)

            export_result = APLArtifactTable(
                name='#EXPORT_RESULT_{}'.format(self.id),
                type_name=APLArtifactTable.RESULT,
                apl_version=self._apl_version)

            self._create_artifact_table(func_header_table)
            self._create_artifact_table(export_config)
            self._create_artifact_table(export_result)

            self._call_apl(
                "APL_EXPORT_APPLY_CODE",
                input_tables=[
                    func_header_table,
                    self.model_table_,
                    export_config
                ],
                output_tables=[
                    export_result
                ]
            )
        except dbapi.Error as db_er:
            logger.error("Error while exporting the apply code: %s", db_er, exc_info=True)
            raise

        query = "SELECT * FROM #EXPORT_RESULT_{}".format(self.id) + " WHERE KEY = 'code'"
        if segment_column_name is not None:
            export_result = self.conn_context.sql(query).collect()[['OID', 'VALUE']]
            export_result.columns = [segment_column_name, 'Code']
            return export_result
        else:
            return self.conn_context.sql(query).collect()['VALUE'].values[0]

    def _score(self, data):
        if self.model_table_ is None:
            raise FitIncompleteError('Model not initialized. Perform a fit first.')

        self._set_conn_context(data.connection_context)
        try:
            # ApplyIn dataset
            # We can either make a view or create a new table
            # View offers more performance
            # But view is impossible when the dataset is temporary table
            try:
                dataset_table = 'TEST_DATASET_{}'.format(self.id)
                data_view = self._create_view(view_name=dataset_table, data=data)
            except dbapi.Error:
                dataset_table = '#TEST_DATASET_{}'.format(self.id)
                data_view = self._materialize_w_type_conv(name=dataset_table, data=data)

            config_table = self._create_basic_config_table(has_output_indicator_table=False)
            func_header_table = self._create_func_header_table()

            model_table = APLArtifactTable(
                name='#MODEL_TEST_BIN_{}'.format(self.id),
                type_name=APLArtifactTable.MODEL_BIN_OID,
                apl_version=self._apl_version)

            test_log_table = self._create_operation_log_table(name='#TEST_LOG_{}'.format(self.id))
            summary_table = self._create_summary_table()
            debrief_metric_table = self._create_debrief_metric_table()
            debrief_property_table = self._create_debrief_property_table()

            self._create_artifact_table(func_header_table)
            self._create_artifact_table(config_table)
            self._create_artifact_table(model_table)
            self._create_artifact_table(test_log_table)
            self._create_artifact_table(summary_table)
            self._create_artifact_table(debrief_metric_table)
            self._create_artifact_table(debrief_property_table)

            if self._sql_generation_mode == SqlGenerationMode.SQLBLOCK_DU:
                procedure_name = 'APL_TEST_MODEL_DEBRIEF'
            else:
                procedure_name = 'APL_TEST_MODEL__OVERLOAD_4_5'
            if not self._disable_hana_execution:
                self._call_apl(
                    procedure_name,
                    input_tables=[
                        func_header_table,
                        self.model_table_,
                        config_table,
                        data_view
                    ],
                    output_tables=[
                        model_table,
                        test_log_table,
                        summary_table,
                        debrief_metric_table,
                        debrief_property_table
                    ])
            # Records the SQL artifacts that can be restituted later for HDI
            self._record_sql_artifact(
                model_method='score',
                apl_function='APL_TEST_MODEL__OVERLOAD_4_5',
                input_tables=[
                    func_header_table,
                    self.model_table_,
                    config_table,
                    data_view
                ],
                output_tables=[
                    model_table,
                    test_log_table,
                    summary_table,
                    debrief_metric_table,
                    debrief_property_table
                ],
                filled_table_names=(self.model_table_.name[1:]
                                    if  self.model_table_.name.startswith('#')
                                    else self.model_table_.name)
                )

            self.model_table_ = model_table
            self.model_ = self.conn_context.table(model_table.name)
            self.test_operation_log_ = self.conn_context.table(test_log_table.name) # pylint:disable=attribute-defined-outside-init
            self.summary_ = self.conn_context.table(summary_table.name) # pylint:disable=attribute-defined-outside-init
            self.debrief_metric_table_ = debrief_metric_table # pylint:disable=attribute-defined-outside-init
            self.debrief_property_table_ = debrief_property_table # pylint:disable=attribute-defined-outside-init
        except dbapi.Error as db_er:
            logger.error('Error while calling \'APL_TEST_MODEL_DEBRIEF\': %s', db_er, exc_info=True)
            raise
        finally:
            self._try_drop_view(dataset_table)

    def _record_sql_artifact(self, model_method, apl_function, input_tables, output_tables, filled_table_names=None):
        """
        Records the SQL artifacts that can be restituted later in HANA design-time artifacts generation
        see module src/hana_ml/artifacts/generators/hana.py
        Parameters:
        ----------
        model_method: str
            Should be 'fit', 'predict', 'score'
        apl_function: str
            The lower level APL function (example: 'APL_CREATE_MODEL_AND_TRAIN')
        input_tables : list of APLArtifactTable or ArtifactView
            Those instances will provide the name and the definition of the underlying table/view.
        output_tables : list Table (mandatory)
            The output table names to be placed in procedure call
        """
        if self._artifacts_recorder is None:
            self._artifacts_recorder = AplSqlArtifactsRecorder(self.conn_context)
        self._artifacts_recorder(model_method, apl_function, input_tables, output_tables, filled_table_names)

    def get_artifacts_recorder(self):
        """
        Return the object recorder (for Design-time artifacts generation)
        """
        return self._artifacts_recorder

    @property
    def _fit_call(self):
        recorder = self.get_artifacts_recorder()
        if recorder is None:
            raise ValueError("Nothing was recorded. The model should be fitted first.")
        return recorder._fit_call

    @property
    def _fit_args(self):
        recorder = self.get_artifacts_recorder()
        if recorder is None:
            raise ValueError("Nothing was recorded. The model should be fitted first.")
        return recorder._fit_args

    @property
    def _fit_output_table_names(self):
        recorder = self.get_artifacts_recorder()
        if recorder is None:
            raise ValueError("Nothing was recorded. The model should be fitted first.")
        return recorder._fit_output_table_names

    @property
    def _fit_anonymous_block(self):
        recorder = self.get_artifacts_recorder()
        if recorder is None:
            raise ValueError("Nothing was recorded. The model should be fitted first.")
        return recorder._fit_anonymous_block

    @property
    def _predict_call(self):
        recorder = self.get_artifacts_recorder()
        if recorder is None:
            raise ValueError("Nothing was recorded. The model should be predictted first.")
        return recorder._predict_call

    @property
    def _predict_args(self):
        recorder = self.get_artifacts_recorder()
        if recorder is None:
            raise ValueError("Nothing was recorded. The model should be predictted first.")
        return recorder._predict_args

    @property
    def _predict_output_table_names(self):
        recorder = self.get_artifacts_recorder()
        if recorder is None:
            raise ValueError("Nothing was recorded. The model should be predictted first.")
        return recorder._predict_output_table_names

    @property
    def _predict_anonymous_block(self):
        recorder = self.get_artifacts_recorder()
        if recorder is None:
            raise ValueError("Nothing was recorded. The model should be predictted first.")
        return recorder._predict_anonymous_block

    @_predict_anonymous_block.setter
    def _predict_anonymous_block(self, value):
        recorder = self.get_artifacts_recorder()
        if recorder is None:
            raise ValueError("Nothing was recorded. The model should be predictted first.")
        recorder._predict_anonymous_block = value


    @property
    def _fit_predict_call(self):
        recorder = self.get_artifacts_recorder()
        if recorder is None:
            raise ValueError("Nothing was recorded. The model should be fit_predictted first.")
        return recorder._fit_predict_call

    @property
    def _fit_predict_args(self):
        recorder = self.get_artifacts_recorder()
        if recorder is None:
            raise ValueError("Nothing was recorded. The model should be fit_predictted first.")
        return recorder._fit_predict_args

    @property
    def _fit_predict_output_table_names(self):
        recorder = self.get_artifacts_recorder()
        if recorder is None:
            raise ValueError("Nothing was recorded. The model should be fit_predictted first.")
        return recorder._fit_predict_output_table_names

    @property
    def _fit_predict_anonymous_block(self):
        recorder = self.get_artifacts_recorder()
        if recorder is None:
            raise ValueError("Nothing was recorded. The model should be fit_predictted first.")
        return recorder._fit_predict_anonymous_block

    @property
    def _score_call(self):
        recorder = self.get_artifacts_recorder()
        if recorder is None:
            raise ValueError("Nothing was recorded. The model should be scoreted first.")
        return recorder._score_call

    @property
    def _score_args(self):
        recorder = self.get_artifacts_recorder()
        if recorder is None:
            raise ValueError("Nothing was recorded. The model should be scoreted first.")
        return recorder._score_args

    @property
    def _score_output_table_names(self):
        recorder = self.get_artifacts_recorder()
        if recorder is None:
            raise ValueError("Nothing was recorded. The model should be scoreted first.")
        return recorder._score_output_table_names

    @property
    def _score_anonymous_block(self):
        recorder = self.get_artifacts_recorder()
        if recorder is None:
            raise ValueError("Nothing was recorded. The model should be scoreted first.")
        return recorder._score_anonymous_block

    def disable_hana_execution(self):
        """
        HANA execution will be disabled and only SQL script will be generated.
        """
        self._disable_hana_execution = True

    def enable_hana_execution(self):
        """
        HANA execution will be enabled.
        """
        self._disable_hana_execution = False

    def schedule_fit(self,
                     output_table_name_model,
                     output_table_name_log,
                     output_table_name_summary,
                     output_table_name_indicators,
                     **schedule_kwargs
                     ):
        """
        Creates a HANA scheduler job for the model fitting.
        It is a wrapper function of the HANAScheduler.create_training_schedule() method.
        Its interest allows users to better specify the arguments such as the output table names.

        Parameters
        ----------
        output_table_name_model: str
            The output table name for the model binary.
        output_table_name_log: str
            The output table name for the log data.
        output_table_name_summary: str
            The output table name for the model summary.
        output_table_name_indicators: str
            The output table name for the model indicators.
        schedule_kwargs: kwargs dictionary
            Arguments forwarded to the HANAScheduler.create_training_schedule method.
            Please refer to the documentation of the HANAScheduler.create_applying_schedule method.

        Examples
        --------
        >>> model = GradientBoostingBinaryClassifier()
        >>> model.fit(
                data=data,
                key= 'id',
                label='class',
            )
        >>> model.schedule_fit(
                output_table_name_model='OUTPUT_MODEL_BINARY',
                output_table_name_log='OUTPUT_FIT_LOG',
                output_table_name_summary='OUTPUT_SUMMARY_LOG',
                output_table_name_indicators='OUTPUT_FIT_INDICATORS',
                job_name=job_name,
                obj=model,
                cron="* * * mon,tue,wed,thu,fri 1 23 45",
                procedure_name=procedure_name,
                force=True)
        """
        if self.conn_context is None:
            raise ValueError("Connection context is not set yet. Please fit the model first.")
        schedule_kwargs['output_table_names'] = [
            output_table_name_model,
            output_table_name_log,
            output_table_name_summary,
            output_table_name_indicators
        ]
        hana_schedule = HANAScheduler(self.conn_context)
        hana_schedule.create_training_schedule(**schedule_kwargs)


    def schedule_predict(
            self,
            input_table_name_model,
            output_table_name_applyout,
            output_table_name_log,
            output_table_name_summary,
            **schedule_kwargs
            ):
        """
        Creates a HANA scheduler job for the model prediction.
        It is a wrapper function of the HANAScheduler.create_applying_schedule() method.
        Its interest allows users to better specify the arguments such as the output table names.

        Parameters
        ----------
        input_table_name_model: str
            The input table name for the model binary.
        output_table_name_applyout: str
            The output table name for the prediction data.
        output_table_name_log: str
            The output table name for the log data.
        output_table_name_summary: str
            The output table name for the model summary.
        schedule_kwargs: kwargs dictionary
            Arguments forwarded to the HANAScheduler.create_applying_schedule method.
            Please refer to the documentation of the HANAScheduler.create_applying_schedule method.

        Examples
        --------
        >>> model = GradientBoostingBinaryClassifier()
        >>> model.fit(
                data=data,
                key= 'id',
                label='class',
            )
        >>> model.predict(data)
        >>> model.schedule_predict(
                input_table_name_model='MODEL_BINARY',
                output_table_name_applyout="OUTPUT_PREDICT_APPLYOUT",
                output_table_name_log="OUTPUT_PREDICT_LOG",
                output_table_name_summary="OUTPUT_PREDICT_SUMMARY",
                job_name=job_name,
                obj=model,
                cron="* * * mon,tue,wed,thu,fri 1 23 45",
                procedure_name=procedure_name,
                force=True)
        """
        if self.conn_context is None:
            raise ValueError("Connection context is not set yet. Please fit the model first.")
        if input_table_name_model is None or len(input_table_name_model) == 0:
            raise ValueError("Model table name is not provided.")

        schedule_kwargs['output_table_names'] = [
            output_table_name_applyout,
            output_table_name_log,
            output_table_name_summary
        ]
        # set the model table name in _predict_anonymous_block
        sql_old = self._predict_anonymous_block
        sql_new = re.sub(
            r" FROM #MODEL_TRAIN_BIN_.*?;",
            f" FROM {input_table_name_model};",
            sql_old
        )
        self._predict_anonymous_block = sql_new
        # if the model table doesn't exist yet, the procedure can't be compiled => create this table
        if not self.conn_context.has_table(input_table_name_model):
            old_model_table_name = self.model_table_.name   # tmp model table created in model.fit()
            self.conn_context.table(old_model_table_name).save(input_table_name_model, force=False)

        hana_schedule = HANAScheduler(self.conn_context)
        hana_schedule.create_applying_schedule(**schedule_kwargs)
