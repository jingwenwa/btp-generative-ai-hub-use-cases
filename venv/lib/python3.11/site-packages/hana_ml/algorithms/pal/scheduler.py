"""
This module contains the python implementation of PAL Scheduled Execution.

The following class is available:

    * :class:`ScheduledExecution`
"""
import logging
import re
import uuid
from datetime import datetime
from hdbcli import dbapi
from pandas import Timestamp
from hana_ml.dataframe import smart_quote
from hana_ml.ml_base import execute_logged
from hana_ml.hana_scheduler import _find_temp_table
from hana_ml.algorithms.pal.pal_base import PALBase

logger = logging.getLogger(__name__)

def _materialize_temp_tab(conn, obj, sql, task_id, force):
    materialized_from_temp_tabs = []
    in_str = sql.split("BEGIN", 1)[0]
    in_vars = re.findall(r"IN \S+ TABLE", in_str)
    in_tab_names = re.findall(r"=> (\S+),", in_str) + re.findall(r"=> (\S+)\)", in_str)
    for idx, in_var in enumerate(in_vars):
        in_vars[idx] = in_var.replace("IN", "").replace("TABLE", "").strip()
    for idx, in_tab in enumerate(in_tab_names):
        in_tab_names[idx] = in_tab.replace("\"", "").strip()
    in_statement = re.findall(r"in_\d+ = [^;]+;", sql)
    for idx, line in enumerate(in_statement):
        if hasattr(obj, "materialize_dict"):
            for kkey, vval in obj.materialize_dict.items():
                if kkey in line:
                    in_var, _ = line.split(" = ", 1)
                    new_tab_name = "in_{}{}".format(task_id, in_var.split("_")[1])
                    conn.sql(vval).save(new_tab_name, force=force)
                    materialized_from_temp_tabs.append(new_tab_name)
                    logger.warning("Materialize %s to %s.", kkey, new_tab_name)
                    sql = sql.replace(line, in_var + " = " + "SELECT * FROM {};".format(smart_quote(new_tab_name)))
        if "#" in line:
            in_var, sel_statement = line.split(" = ", 1)
            new_tab_name = "in_{}{}".format(task_id, in_var.split("_")[1])
            conn.sql(sel_statement).save(new_tab_name, force=force)
            logger.warning("Materialize %s to %s.", sel_statement, new_tab_name)
            sql = sql.replace(line, in_var + " = " + "SELECT * FROM {};".format(smart_quote(new_tab_name)))
            materialized_from_temp_tabs.append(new_tab_name)
    in_statement = []
    for in_var, in_tab_name in zip(in_vars, in_tab_names):
        new_tab_name = in_tab_name
        if hasattr(obj, "materialize_dict"):
            for kkey, vval in obj.materialize_dict.items():
                if kkey in in_tab_name:
                    new_tab_name = "in_{}{}".format(task_id, in_var.split("_")[1])
                    logger.warning("Materialize %s to %s.", kkey, new_tab_name)
                    conn.sql(vval).save(new_tab_name, force=force)
                    materialized_from_temp_tabs.append(new_tab_name)
        in_statement.append("{} = SELECT * FROM {};\n".format(in_var, smart_quote(new_tab_name)))
    return sql.replace("CALL _SYS_AFL", "".join(in_statement) + "CALL _SYS_AFL"), materialized_from_temp_tabs

def _drop_if_exist_create(sql, output_table_names, task_id):
    def _extract_table_names(block):
        res = []
        for sub_block in block.split(';'):
            find0 = re.findall(r"CREATE LOCAL TEMPORARY COLUMN TABLE ([\S\s]+) AS \(SELECT", sub_block)
            if find0:
                res += find0
        return res

    block0, block1 = sql.split("CREATE LOCAL TEMPORARY COLUMN TABLE", 1)
    block1 = "CREATE LOCAL TEMPORARY COLUMN TABLE" + block1
    block2 = ''
    declare_st = []
    out_tabs = output_table_names.copy()
    for idx, _ in enumerate(_extract_table_names(block1)):
        declare_st.append("DECLARE cnt_{} INTEGER;".format(idx))
        out_tab_name = "out_{}{}".format(task_id, idx)
        if idx < len(output_table_names):
            out_tab_name = output_table_names[idx]
        else:
            out_tabs.append(out_tab_name)
        block2 += "cnt_{} := 0;\n".format(idx)
        block2 += "SELECT COUNT(*) INTO cnt_{} FROM M_TABLES WHERE TABLE_NAME='{}';\n".format(idx, out_tab_name)
        block2 += "IF :cnt_{0} > 0 THEN\n  DROP TABLE {1};\nEND IF;\n".format(idx, smart_quote(out_tab_name))
        block2 += "CREATE TABLE {1} AS (SELECT * FROM :out_{0});\n".format(idx, smart_quote(out_tab_name))


    block0 = block0.replace("DECLARE", "\n".join(declare_st) + "\nDECLARE", 1)
    return block0 + block2 + "END;", out_tabs

class ScheduledExecution(object):#pylint:disable=too-many-public-methods, too-many-instance-attributes
    r"""
    Python implementation of PAL scheduled execution. Basically, with an instance of class **ScheduledExecution**,
    users can take the following actions:

    - create a task
    - create a scheduled execution for a task
    - alter the scheduled execution for a task
    - pause the scheduled execution for a task
    - resume the scheduled execution for a task
    - remove the scheduled execution for a task
    - remove a task
    - create a scheduled execution of the `fit()` method of a hana-ml object
    - create a scheduled execution of the `predict()` method of a hana-ml object
    - create a scheduled execution of the `score()` method of a hana-ml object (limited to the case
      that the score() method is associated with a PAL SCORE procedure)

    Parameters
    ----------
    connection_context : ConnectionContext
        Specifies the valid connection to SAP HANA Cloud database.

    Attributes
    ----------
    connection_context : ConnectionContext
        Representing the connection to SAP HANA Cloud database.

    current_user : str
        Representing the info of `CURRENT_USER` reflected by the connection to SAP HANA.

    Examples
    --------
    Scenario : There is a dataset that has been updated continously. Assuming that the dataset is stored
    in a table called 'EXPERIMENT_DATA_FULL_TBL' in SAP HANA Cloud database. We want to schedule the training of an HGBT model
    on this dataset at 8AM each monday, and having the lasted HGBT model stored in a table called 'EXPERIMENT_MODEL_TBL'.

    The entire scheduling process of the scenario above can be illustrated as follows:

    Firstly we Create a ScheduledExecution instance as follows:

    >>> from hana_ml.dataframe import ConnectionContext
    >>> url, port, user, pwd = 'mocksite.com', 30015, 'MOCK_USER', 'pt&%$sdxy'
    >>> conn = ConnnectionContext(url, port, user, pwd)
    >>> sexec = ScheduledExecution(conn)
    >>> sexec.current_user
    ... 'MOCK_USER'

    Then we can execute the following SQL statement to create a stored SQL procedure for each single training process:

    .. code-block:: sql
       :linenos:

       CREATE PROCEDURE EXPERIMENT_HGBT_TRAIN(TREE_NUM INTEGER)
       LANGUAGE SQLSCRIPT
       SQL SECURITY INVOKER
       AS
       BEGIN
       DECLARE param_tab TABLE("PARAM_NAME" VARCHAR(256), "INT_VALUE" INTEGER, "DOUBLE_VALUE" DOUBLE, "STRING_VALUE" VARCHAR(1000));
       :param_tab.insert(('HAS_ID', 1, NULL, NULL));
       :param_tab.insert(('DEPENDENT_VARIABLE', NULL, NULL, 'median_house_value'));
       :param_tab.insert(('ITER_NUM', :TREE_NUM, NULL, NULL));
       data_tab = SELECT * FROM EXPERIMENT_DATA_FULL_TBL;
       CALL _SYS_AFL.PAL_HGBT(:data_tab, :param_tab, model_tab, varimp_tab, cm_tab, stat_tab, cv_tab);
       TRUNCATE TABLE EXPERIMENT_MODEL_TBL;
       INSERT INTO EXPERIMENT_MODEL_TBL SELECT * FROM :model_tab;
       END

    Once created, the procedure will be under the schema of current user (i.e. 'MOCK_USER' shown in the connection).
    Then, we can create a task for it, demonstrated as follows:

    >>> task_info = sexec.create_task(task_id='EXPERIMENT_DATA_HGBT_FIT',
    ...                               proc_name='EXPERIMENT_HGBT_TRAIN',
    ...                               proc_schema='MOCK_USER',
    ...                               task_desc='Fitting HGBT model using EXPERIMENT dataset',
    ...                               task_params=[('TREE_NUM', None, 10, 2)]
    ...                               force=True)#drop the old task with same task id if exists

    The task is suconnessfully created if no error is raised.
    We can then attached the prescribed schedule mentioned in the beginning of this section to
    the created task, illustrated as follows:

    >>> schedule_info = sexec.create_task_schedule(task_id='EXPERIMENT_DATA_HGBT_FIT',
    ...                                            cron="* * * 'mon' 8 0 0")#means 8AM each Monday.

    If we change our mind and want to postpone the training process to 9AM each Tuesday, then we only need to alter the schedule
    using a different execution frequency pattern, illustrated as follows:

    >>> schedule_info = sexec.alter_task_schedule(task_id='EXPERIMENT_DATA_HGBT_FIT',
    ...                                           cron="* * * 'tue' 9 0 0")#means 9AM each Tuesday.

    We can pause & resume the schedule anytime we want, illustrated as follows:

    >>> sexec.pause_task_schedule(task_id='EXPERIMENT_DATA_HGBT_FIT')
    >>> sexec.remove_task_schedule(task_id='EXPERIMENT_DATA_HGBT_FIT')

    If we no longer need the task to be scheduled, we can remove the schedule:

    >>> sexec.remove_task_schedule(task_id='EXPERIMENT_DATA_HGBT_FIT')

    Finally if the task is no longer needed, we can remove the task:

    >>> sexec.remove_task(task_id='EXPERIMENT_DATA_HGBT_FIT')
    """
    def __init__(self, connection_context):
        self.connection_context = connection_context
        self.current_user = connection_context.sql('SELECT CURRENT_USER FROM DUMMY;').collect().iloc[0,0]#pylint:disable=line-too-long
        self.fit_sql_proc_create_statement = None
        self.predict_sql_proc_create_statement = None
        self.score_sql_proc_create_statement = None
        self.fit_materialized_tables = []
        self.fit_output_tables = []
        self.predict_materialized_tables = []
        self.predict_output_tables = []
        self.score_materialized_tables = []
        self.score_output_tables = []

    def create_task(self,#pylint:disable=too-many-positional-arguments
                    task_id,
                    proc_name,
                    proc_schema,
                    task_owner=None,
                    task_params=None,#a triple (param_name, param_schema, param_value, param_type)
                    task_desc='',
                    force=False):
        r"""
        Create a **task** to be scheduled for execution. Basically, a **task** is consisted of the `task ID`, the `owner` and
        a stored SQL procedure (with parameters) to be invoked.

        Parameters
        ----------
        task_id : str
            Specifies the name of the task to be created.
            The name must be unique and does not conflict with names of existing tasks.

        proc_name : str
            Specifies the name of the stored SQL procedure to be invoked.

        proc_schema : str
            Specifies the schema of the stored SQL procedure given in ``proc_name``.

            Two simple examples for illustration:

            - If the stored SQL procedure to be invoked is created by user 'PAL_TESTER', then ``proc_schema`` should
              be assigned the value of 'PAL_TESTER'.
            - All PAL procedures are under the schema '_SYS_AFL'. If the stored SQL procedure to be invoked is a
              PAL procedure, then ``proc_schema`` should be assigned the value of '_SYS_AFL'.

        task_ower : str, optional
            Specifies the task owner, whom must be granted the priviledge to call the stored the SQL procedure specified by
            ``proc_name``.

            Defaults to `CURRENT_USER`.

        task_params : list of tuples, optional
            Specifies the parameters of the stored SQL procedure, each parameter must be specified with a tuple
            described as follows:

                (**parameter name, parameter schema, parameter value, parameter type**).

            Currently **parameter type** can take the following values

            - 0 : table
            - 1 : view
            - 2 : literal

            Note that if **parameter type** is literal (i.e. takes the value of 2), then its corresponding **parameter schema**
            should be None.

        task_desc : str, optional
            Description of the task.

            Defaults to empty string.

        force : bool, optional
            Specifies whether or not to drop the previously created task with the same ``task_id``.

            Set as True if you want to drop the old task with the same ``task_id``. In this case,
            if the old task is scheduled for execution, the schedule is dropped as well.

            If set as False, and a task with the same ``task_id`` already exists, error message shall be thrown.

            Default to False.

        Returns
        -------
        DataFrame
            DataFrame containing the informatoin of the created task.
        """
        conn = self.connection_context
        if force is True:
            try:
                self.remove_task(task_id, force=force)
            except dbapi.Error as db_err:
                logger.warning(str(db_err))
                pass
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        task_owner = self.current_user if task_owner is None else task_owner
        task_info_tbl = '#TASK_INFO_TBL_' + task_id.upper() + f'_{unique_id}'
        header = "DO\nBegin\n"
        header = header + 'DECLARE task_params TABLE("PARAM_NAME" NVARCHAR(256),' +\
        ' "ARGUMENT_SCHEMA" NVARCHAR(256), "ARGUMENT" NCLOB, "ARGUMENT_CATEGORY" INTEGER);\n'
        header = header + 'DECLARE info TABLE("TASK_ID" NVARCHAR(256), "TASK_OWNER" NVARCHAR(256),' +\
        ' "PROC_SCHEMA" NVARCHAR(256), "PROC_NAME" NVARCHAR(256));\n'
        if task_params is not None and len(task_params) > 0:
            for i in range(len(task_params)):
                param_name, param_schema, param_val, param_type = tuple(task_params[i])
                param_schema = f'\'{param_schema}\'' if param_schema is not None else 'NULL'
                insert_statement = f':task_params.INSERT((\'{param_name}\', {param_schema}, \'{str(param_val)}\', {param_type}));\n'#pylint:disable=line-too-long
                header = header + insert_statement
        create_proc = f'CALL _SYS_AFL.AFLPAL_CREATE_TASK_PROC("TASK_ID" => \'{task_id}\', "PROC_SCHEMA" => \'{proc_schema}\',' +\
                      f' "PROC_NAME" => \'{proc_name}\', "TASK_DESC" => \'{task_desc}\',' +\
                      ' "TASK_PARAM_TAB" => :task_params, "TASK_INFO" => info);\n'
        exec_statement = header + create_proc
        select_statement = f'CREATE LOCAL TEMPORARY COLUMN TABLE "{task_info_tbl}" AS (SELECT * FROM :info);\n'
        exec_statement = exec_statement + select_statement + 'END;'
        try:
            with conn.connection.cursor() as cur:
                execute_logged(cur, exec_statement, conn.sql_tracer, conn)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            raise
        return conn.table(task_info_tbl)

    def remove_task(self, task_id, force=False):
        """
        Remove a task.

        task_id : str
            Specifies the name of the task to be removed.

        force : bool, optional
            Specifies whether or not to continue removing the specified task if the task scheduled.

            If it is set as True and the task is scheduled, the schedule will be removed as well in
            order to facilitate the removal of the task (otherwise error will be thrown).

            Defaults to False.

        Returns
        -------
        DataFrame
            DataFrame containing the information of the task that has been removed.
        """
        conn = self.connection_context
        task_flag = self.get_task_log(task_id).filter('EVENT_KEY=\'TASK\'').count()
        if task_flag == 0:
            return
        if force is True:
            try:
                self.remove_task_schedule(task_id=task_id)
            except Exception:
                pass
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        task_info_tbl = '#TASK_INFO_TBL_' + task_id.upper() + f'_{unique_id}'
        header = "DO\nBEGIN\n"
        header = header + 'DECLARE info TABLE("TASK_ID" NVARCHAR(256), "TASK_OWNER" NVARCHAR(256), "PROC_SCHEMA" NVARCHAR(256), "PROC_NAME" NVARCHAR(256));\n'
        call_statement = f'CALL _SYS_AFL.AFLPAL_REMOVE_TASK_PROC(TASK_ID => \'{task_id}\', TASK_INFO => info);\n'
        create_statement = f'CREATE LOCAL TEMPORARY COLUMN TABLE "{task_info_tbl}" AS (SELECT * FROM :info);\n'
        exec_statement = header + call_statement + create_statement + 'END;'
        try:
            with conn.connection.cursor() as cur:
                execute_logged(cur, exec_statement, conn.sql_tracer, conn)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            raise
        return conn.table(task_info_tbl)

    def get_task_log(self, task_id):
        """
        Get the log of a specified task given its task_id.

        Parameters
        ----------
        task_id : str
            Task ID.
        """
        conn = self.connection_context
        return conn.sql(f"SELECT * FROM PAL_SCHEDULED_EXECUTION.TASK_LOG WHERE TASK_ID = '{task_id}';")

    def get_task_definition(self, task_id):
        """
        Get the definition of a created task given task ID.

        Parameters
        ----------
        task_id : str
            Task ID.
        """
        conn = self.connection_context
        select_statement = f"SELECT * FROM PAL_SCHEDULED_EXECUTION.TASK_DEFINITION WHERE TASK_ID = '{task_id}';"
        return conn.sql(select_statement)

    def get_task_param(self, task_id):
        """
        Get the parameters of a created task given a task_id.

        Parameters
        ----------
        task_id : str
            Task ID.
        """
        conn = self.connection_context
        select_statement = f"SELECT * FROM PAL_SCHEDULED_EXECUTION.TASK_PARAM WHERE TASK_ID = '{task_id}';"
        return conn.sql(select_statement)

    def __exec_task_schedule(self, task_id, exec_type, cron, recurrence_range=None):
        conn = self.connection_context
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        schedule_info_tbl = '#SCHEDULE_INFO_TBL_' + task_id.upper() + f'_{unique_id}'
        header = "DO\nBEGIN\n"
        header = header + 'DECLARE schedule_frequency TABLE(FREQUENCY NVARCHAR(10), FREQUENCY_PATTERN NVARCHAR(20));\n'
        header = header + 'DECLARE schedule_recurrence_range TABLE(END_POINT NVARCHAR(5), TIME_POINT TIMESTAMP);\n'
        header = header + 'DECLARE task_schedule_info TABLE(OWNER NVARCHAR(256), SCHEMA_NAME NVARCHAR(256),' +\
        ' SCHEDULE_NAME NVARCHAR(256), CRON NVARCHAR(256), START_TIME TIMESTAMP,' +\
        ' END_TIME TIMESTAMP, IS_ENABLED NVARCHAR(5), CREATE_TIME TIMESTAMP);\n'
        if recurrence_range is not None:
            for end_point in recurrence_range:
                tmsp = recurrence_range[end_point]
                if not isinstance(tmsp, (str, datetime, Timestamp)):
                    msg = "Endpoints of recurrence range should be either time string or datetime or Timestamp."
                    raise TypeError(msg)
                insert_statement = f':schedule_recurrence_range.INSERT((\'{end_point.upper()}\', TO_TIMESTAMP(\'{str(tmsp)}\')));\n'
                header = header + insert_statement
        frequency_keys = ['YEAR', 'MONTH', 'MONTHDAY', 'WEEKDAY', 'HOUR', 'MINUTE', 'SECOND']
        if exec_type == 'CREATE' and cron is None:
            msg = "'cron' cannot be None when creating a task schedule."
            raise ValueError(msg)
        if cron is not None:
            freq_patterns = cron.split()
            if len(freq_patterns) != len(frequency_keys):
                msg = "Incollect 'cron' value specified."
                raise ValueError(msg)
            for idx, pttrn in enumerate(freq_patterns):
                insert_statement = f':schedule_frequency.INSERT((\'{frequency_keys[idx]}\', \'{pttrn}\'));\n'
                header = header + insert_statement
        call_statement = f'CALL _SYS_AFL.AFLPAL_{exec_type.upper()}_TASK_SCHEDULE_PROC("TASK_ID" => \'{task_id}\',' +\
        ' "SCHEDULE_FREQUENCY" => :schedule_frequency,' +\
        '"SCHEDULE_RECURRENCE_RANGE" => :schedule_recurrence_range,' +\
        ' "TASK_SCHEDULE_INFO" => task_schedule_info);\n'
        exec_statement = header + call_statement
        create_statement = f'CREATE LOCAL TEMPORARY COLUMN TABLE "{schedule_info_tbl}" AS (SELECT * FROM :task_schedule_info);\n'
        exec_statement = exec_statement + create_statement + 'END;'
        try:
            with conn.connection.cursor() as cur:
                execute_logged(cur, exec_statement, conn.sql_tracer, conn)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            raise
        return conn.table(schedule_info_tbl)

    def create_task_schedule(self, task_id, cron, recurrence_range=None, force=False):
        r"""
        Create scheduled execution for a task.

        Parameters
        ----------
        task_id : str
            Name of the task to be scheduled for execution.

        cron : str
            Specifies the frequency pattern of task to be executed. It should be a string of the following format
            (please note that there is a `space` between neighboring frequency categories)

                         "<YEAR> <MONTH> <DATE> <WEEKDAY> <HOUR> <MINUTE> <SECOND>"

            where

            ===========  ==============================================================================
            **YEAR**     Four digit number, representing the year
            **MONTH**    1 - 12, representing the month
            **DATE**     1 - 31, representing the date (monthday)
            **WEEKDAY**  'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun', representing the day of week
            **HOUR**     0 - 23, representing the hour
            **MINUTE**   0 - 59, representing the minute
            **SECOND**   0 - 59, representing the second
            ===========  ==============================================================================

            Besides valid values for each frequency category listed above,
            each frequency pattern also supports wildcard character,
            range pattern and cycle pattern, illustrated as follows:

            =======   =========================================================
             \*       Any frequency value
             \*/n     From the first valid value then any other value step n
             a:b      Valid values ranging from a to b, inclusive of end points
             a:b/n    Valid values from a to b with step n
            =======   =========================================================

            Moreover, each frequency pattern can also be entered in a comma separated list. For example,
            the <WEEKDAY> frequency pattern can be specified as 'mon, wed, fri', which means that
            task is scheduled for execution on Monday, Wednesday and Friday.

            Example

            .. code-block:: python

               cron = "2025 2 25 * 14:16 0 0"

            specifies a frequency pattern of `hourly` task execution from 14:00PM to 16:00PM, Feb 25, 2025.

        recurrence_range : dict, optional
            This parameter Specifies the range of time allowed for scheduled task execution.
            This setting is optional, user can set either the lower bound (i.e. start)
            or upper bound (i.e. end) of the range, or neither.

            For specifying the start or end points of the reconnurence range (or both),
            user should alway use string timestamp of the format "YYYY-MM-DD HH24:MI:SS.FF7", or a
            python object of class `datetime.datetime`.

            Example recurrence range in dict : {'start': '2025-02-22 14:00:00.0000000', 'end': '2025-02-28 15:00:00.0000000'},
            which specifies a recurrence range from 14PM, Feb 22, 2025 to 15PM, Feb 28, 2025.

        Returns
        -------
        DataFrame
            DataFrame containing the created schedule for task execution.
        """
        if force is True:
            try:
                self.remove_task_schedule(task_id)
            except dbapi.Error as err:
                logger.warning(str(err))
                pass
        return self.__exec_task_schedule(task_id=task_id,
                                         exec_type='CREATE',
                                         cron=cron,
                                         recurrence_range=recurrence_range)

    def alter_task_schedule(self, task_id, cron=None, recurrence_range=None):
        r"""
        Alter a schedule.

        Parameters
        ----------
        task_id : str
            Name of the task to be scheduled for execution.

        cron : str
            Specifies the frequency pattern of task to be executed, it format is the same as the format ``cron``
            parameter in :func:`create_task_schedule<hana_ml.algorithms.pal.scheduler.ScheduledExecution.create_task_schedule>`.

        recurrence_range : dict, optional
            This parameter Specifies the range of time allowed for scheduled task execution.
            The settings of this parameter is the same as the settings of ``recurrence_range`` parameter in
            :func:`create_task_schedule<hana_ml.algorithms.pal.scheduler.ScheduledExecution.create_task_schedule>`.

        Returns
        -------
        DataFrame
            DataFrame containing the information of the altered scheduled execution.
        """
        return self.__exec_task_schedule(task_id=task_id,
                                         exec_type='ALTER',
                                         cron=cron,
                                         recurrence_range=recurrence_range)

    def __por_task_schedule(self, task_id, action):
        conn = self.connection_context
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        schedule_info_tbl = '#SCHEDULE_INFO_TBL_' + task_id.upper() + f'_{unique_id}'
        header = "DO\nBEGIN\n"
        header = header + 'DECLARE task_schedule_info TABLE(OWNER NVARCHAR(256), SCHEMA_NAME NVARCHAR(256),'+\
        ' SCHEDULE_NAME NVARCHAR(256), CRON NVARCHAR(256), START_TIME TIMESTAMP, END_TIME TIMESTAMP,' +\
        ' IS_ENABLED NVARCHAR(5), CREATE_TIME TIMESTAMP);\n'
        call_statement = f'CALL _SYS_AFL.AFLPAL_{action}_TASK_SCHEDULE_PROC("TASK_ID" => \'{task_id}\', "TASK_SCHEDULE_INFO" => :task_schedule_info);\n'
        create_statement = f'CREATE LOCAL TEMPORARY COLUMN TABLE "{schedule_info_tbl}" AS (SELECT * FROM :task_schedule_info);\n'
        exec_statement = header + call_statement + create_statement + 'END;'
        try:
            with conn.connection.cursor() as cur:
                execute_logged(cur, exec_statement, conn.sql_tracer, conn)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            raise
        return conn.table(schedule_info_tbl)

    def pause_task_schedule(self, task_id):
        """
        Pause a running shedule.

        Parameters
        ----------
        task_id : str
            Task ID.

        Returns
        -------
        DataFrame
            DataFrame containing the information  of the (paused) task schedule.
        """
        return self.__por_task_schedule(task_id=task_id, action='PAUSE')

    def resume_task_schedule(self, task_id):
        """
        Resume a paused schedule.

        Parameters
        ----------
        task_id : str
            Task ID.

        Returns
        -------
        DataFrame
            DataFrame containing the information  of the (resumed) task schedule.
        """
        return self.__por_task_schedule(task_id=task_id, action='RESUME')

    def get_executed_task_jobs(self, task_id, job_id=None, order='desc'):
        """
        Retrieving the executed task jobs (from table "PAL_SCHEDULED_EXECUTION"."TASK_SCHEDULE_JOB").

        Parameters
        ----------
        task_id : str
            Task ID.

        job_id : int, optional
            Job ID. Defaults to None.

        order : {'asc', 'desc'}, optional
            The displaying order of retrieved records in start time of execution.

            Defaults to 'desc'.

        Returns
        -------
        DataFrame
            DataFrame containing the information of the executed task jobs.
        """
        conn = self.connection_context
        select_statement = 'SELECT * FROM PAL_SCHEDULED_EXECUTION.TASK_SCHEDULE_JOB ' +\
        f'WHERE TASK_ID = \'{task_id}\''
        job_info = '' if job_id is None else f' AND JOB_ID = {job_id}'
        order_info = f'ORDER BY START_TIME {order.upper()};'
        exec_statement = select_statement + job_info + order_info
        return conn.sql(exec_statement)

    def get_task_schedules(self, task_owner=None):
        """
        Get the info of scheduled jobs from system view SCHEDULER_JOBS via task owner specification.

        Parameters
        ----------
        task_owner : str, optional
            Task owner.

            Defaults to the value of class attribute `current_user`.

        Returns
        -------
        DataFrame
            Filtered view of SCHEDULER_JOBS.
        """
        conn = self.connection_context
        task_owner = self.current_user if task_owner is None else task_owner
        select_statement = f'SELECT * FROM SCHEDULER_JOBS WHERE USER_NAME = \'{task_owner}\'' +\
        ' AND SCHEMA_NAME = \'PAL_SCHEDULED_EXECUTION\';'
        return conn.sql(select_statement)

    def cancel_schedule_job(self, task_id, max_wait_duration):
        """
        Cancel running scheduled job.

        Parameters
        ----------
        task_id : str
            Task ID.

        max_wait_duration : int
            Maximum wait duration for canceling the schedule job, in seconds.

        Returns
        -------
        DataFrame
            DataFrame containing result message of the cancel process.
        """
        conn = self.connection_context
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        cancel_info_tbl = '#JOB_CANCEL_INFO_TBL_' + task_id.upper() + f'_{unique_id}'
        header = "DO\nBEGIN\n"
        header = header + 'DECLARE cancel_result NVARCHAR(16);\n'
        header = header + 'DECLARE cancel_info TABLE("CANCEL_STATUS" NVARCHAR(16));\n'
        call_statement = f'CALL _SYS_AFL.AFLPAL_CANCEL_SCHEDULE_JOB_PROC("TASK_ID" => \'{task_id}\',' +\
        f'"MAX_WAIT_DURATION" => {max_wait_duration}, "RESULT" => cancel_result);\n'
        insert_statement = ':cancel_info.insert((:cancel_result));\n'
        create_statement = f'CREATE LOCAL TEMPORARY COLUMN TABLE {cancel_info_tbl} ' +\
        'AS (SELECT * FROM :cancel_info);\n'
        exec_statement = header + call_statement + insert_statement + create_statement + 'END;'
        check_sentences = ['No Running Schedule Job', 'not exist']
        try:
            with conn.connection.cursor() as cur:
                execute_logged(cur, exec_statement, conn.sql_tracer, conn)
            return conn.table(cancel_info_tbl)
        except dbapi.Error as db_err:
            if any(check_st in str(db_err) for check_st in check_sentences):
                msg = f'No running scheduled job for scheduled task \'{task_id}\' to cancel.'
                logger.warning(msg)
                return
            else:
                logger.exception(str(db_err))
                raise
        except Exception as db_err:
            if any(check_st in str(db_err) for check_st in check_sentences):
                msg = f'No running scheduled job for task \'{task_id}\' to cancel.'
                logger.warning(msg)
                return
            else:
                logger.exception(str(db_err))
                raise


    def remove_task_schedule(self, task_id):
        """
        Remove the schedule execution of a task.

        Parameters
        ----------
        task_id : str
            Task ID.

        Returns
        -------
        DataFrame
            DataFrame containing the information of the scheduled task execution.
        """
        conn = self.connection_context
        schedule_flag = self.get_task_log(task_id).filter('EVENT_KEY=\'SCHEDULE\'').count()
        if schedule_flag == 0:
            return
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        task_info_tbl = '#TASK_INFO_TBL_' + task_id.upper() + f'_{unique_id}'
        header = "DO\nBegin\n"
        header = header + 'DECLARE task_schedule_info TABLE(OWNER NVARCHAR(256), SCHEMA_NAME NVARCHAR(256),' +\
        ' SCHEDULE_NAME NVARCHAR(256), CRON NVARCHAR(256), START_TIME TIMESTAMP, END_TIME TIMESTAMP,' +\
        ' IS_ENABLED NVARCHAR(5), CREATE_TIME TIMESTAMP);\n'
        call_statement = f'CALL _SYS_AFL.AFLPAL_REMOVE_TASK_SCHEDULE_PROC(TASK_ID => \'{task_id}\', TASK_SCHEDULE_INFO => task_schedule_info);\n'
        exec_statement = header + call_statement
        create_statement = f'CREATE LOCAL TEMPORARY COLUMN TABLE "{task_info_tbl}" AS (SELECT * FROM :task_schedule_info);\n'
        exec_statement = exec_statement + create_statement + 'END;'
        try:
            with conn.connection.cursor() as cur:
                execute_logged(cur, exec_statement, conn.sql_tracer, conn)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            raise
        return conn.table(task_info_tbl)

    def create_fit_schedule(self,#pylint:disable=too-many-positional-arguments
                            obj,
                            fit_params,
                            task_id,
                            cron,
                            recurrence_range=None,
                            output_table_names=None,
                            proc_name=None,
                            force=True):
        r"""
        Create a scheduled execution of the `fit()` method of a hana-ml object. To achieve this designated objective,
        the following actions will be taken subsequently:

        - 1. A **stored SQL procedure** is firstly created for the `fit()` method to be executed
        - 2. A **task** is created for the stored SQL procedure
        - 3. The **task** created in Step 2 is scheduled for future execution

        Parameters
        ----------
        obj : hana-ml object
           A hana-ml object (i.e. an instance of some hana-ml class) that with callable `fit()` method.

           For example, ``obj`` can be a hana-ml object defined as follows:

           .. code-block:: python
             :linenos:

             from hana_ml.algorithms.pal.unified_classfication import UnifiedClassification
             obj = UnifiedClassification(func='HybridGradientBoostingTree', n_estimators=100)

        fit_params : dict
           The key-value arguments (parameters) passed to the `fit()` method of ``obj``. Intrinsically it
           is the execution of

           .. code-block:: python

                obj.fit(**fit_params)

           to be scheduled.

        task_id : str
           The task ID for the task associated with the stored SQL procedure associated with the execution of the
           ``fit()`` method of ``obj``.

        cron : str
           Specifies the frequency pattern of task to be executed, which is the same as the definition of ``cron`` in
           method :func:`create_task_schedule<hana_ml.algorithms.pal.scheduler.ScheduledExecution.create_task_schedule>`.

        recurrence_range : dict, optional
            This parameter Specifies the range of time allowed for scheduled task execution.
            It is the same as the definition of ``recurrence_range`` in method
            :func:`create_task_schedule<hana_ml.algorithms.pal.scheduler.ScheduledExecution.create_task_schedule>`.

        output_table_names : ListOfStrings, optional
            User Specified names of output tables for the corresponding PAL procedure for model fitting.

            If not provided, the table names will be automatically generated.

        proc_name : str, optional
            Procedure name of the generated stored SQL procedure.

            Defaults to f"PROCEDURE_{task_id}" if not provided.

        force : bool, optional
             Specifies whether or not to force the creation of the task schedule for the execution of the
             `fit()` method of ``obj``.

             It set as True, it will firstly try to drop previously existing procedures with the same name as well as tasks/schedules
             with the same ``task_id``, then re-created and re-schedule.

            Defaults to True.

        Examples
        --------
        Assuming a dataset for classification is stored in table "CLS_DATA_TBL" (with ID column "ID" and label column "CLASS"),
        we want to schedule the training of an HGBT model using the UnifiedClassficiation
        interface provided in hana-ml, the we can proceed as follows:

        >>> from hana_ml.dataframe import ConnectionContext
        >>> cc = ConnectionContext(address=..., port=..., user=..., password=...)
        >>> data = cc.table()
        >>> fit_params = dict(data=data, key="ID", label="CLASS")
        >>> scheduler = ScheduledExecution(cc)
        >>> from hana_ml.algorithms.pal.unified_classification import UnifiedClassficiation
        >>> uhgc = UnifiedClassficiation(func="HybridGradientBoostingTree", n_estimators=100)
        >>> schedule_info = scheduler.create_fit_schedule(obj=uhgc,
        ...                                               fit_params=fit_params,
        ...                                               task_id="CLS_DATA_TBL_FIT",
        ...                                               cron="2025 3 14 * 9 0 0",
        ...                                               force=True)
        """
        if not isinstance(obj, PALBase):
            raise TypeError('obj must be a valid instance of hana-ml class')
        if output_table_names is None:
            output_table_names = []
        #create output tables
        if proc_name is None:
            proc_name = "PROCEDURE_{}".format(task_id)
        obj._virtual_exec  = True#pylint:disable=protected-access
        obj.fit(**fit_params)
        obj._virtual_exec  = False#pylint:disable=protected-access
        sql = obj._fit_anonymous_block#pylint:disable=protected-access
        sql, materialized_tabs = _materialize_temp_tab(self.connection_context, obj, sql, task_id, force)
        if isinstance(output_table_names, str):
            output_table_names = [output_table_names]
        sql, out_tabs = _drop_if_exist_create(sql, output_table_names, task_id)
        if force:
            with self.connection_context.connection.cursor() as cur:
                try:
                    # SQLTRACE added sql_tracer
                    execute_logged(cur, f"DROP PROCEDURE {(smart_quote(proc_name))}")
                except dbapi.Error:
                    pass
                except Exception as err:
                    pass
        sql = "CREATE PROCEDURE {} ()\nAS\nBEGIN".format(smart_quote(proc_name)) + sql.split("BEGIN", 1)[1]
        sql = sql[:-1] + ';'
        self.sql_procedure = sql
        self.connection_context.execute_sql(sql)
        self.create_task(task_id=task_id, proc_name=proc_name, proc_schema=self.current_user,
                         force=force)
        self.fit_sql_proc_create_statement = sql
        self.fit_materialized_tables = materialized_tabs
        self.fit_output_tables = out_tabs
        return self.create_task_schedule(task_id=task_id,
                                         cron=cron,
                                         recurrence_range=recurrence_range,
                                         force=force)

    def get_fit_sql_proc_create_statement(self):
        """Get the SQL statement for creating the fit procedure that has been scheduled for execution."""
        return self.fit_sql_proc_create_statement

    def list_materialized_tables_fit(self):
        """
        Get the materialization table names of temp tables for the scheduled hana-ml fit() execution.
        """
        return self.fit_materialized_tables

    def list_output_tables_fit(self):
        """
        Get the output table names for the scheduled hana-ml fit() execution.
        """
        return self.fit_output_tables

    def __create_inference_schedule(self,#pylint:disable=too-many-positional-arguments
                                    inference_task,
                                    obj,
                                    inference_params,
                                    task_id,
                                    cron,
                                    proc_name=None,
                                    recurrence_range=None,
                                    output_table_names=None,
                                    force=True):
        if not isinstance(obj, PALBase):
            raise TypeError('obj must be a valid instance of hana-ml class')
        if output_table_names is None:
            output_table_names = []
        if proc_name is None:
            proc_name = "PROCEDURE_{}".format(task_id)
        obj._virtual_exec  = True#pylint:disable=protected-access
        try:
            if inference_task == "predict":
                obj.predict(**inference_params)
            else:
                obj.score(**inference_params)
        except dbapi.ProgrammingError:
            pass
        obj._virtual_exec  = False#pylint:disable=protected-access
        sql = obj._predict_anonymous_block if inference_task == 'predict' else obj._score_anonymous_block#pylint:disable=protected-access, line-too-long
        if inference_task == "score" and "_SCORE(" not in sql:
            raise NotImplementedError("No score function executed or found in {}!".format(obj.__module__))
        sql, materialized_tabs = _materialize_temp_tab(self.connection_context, obj, sql, task_id, force)
        if isinstance(output_table_names, str):
            output_table_names = [output_table_names]
        sql, out_tabs = _drop_if_exist_create(sql, output_table_names, task_id)
        unhandled_temp_tab = _find_temp_table(sql)
        for idx, model_materialization in enumerate(unhandled_temp_tab):
            self.connection_context.table(model_materialization).save(model_materialization.replace("#", ""), force=force)
            logger.warning("Materialize %s to %s.", model_materialization, model_materialization.replace("#", ""))
            sql = sql.replace(model_materialization, model_materialization.replace("#", ""))
            materialized_tabs.append(model_materialization.replace("#", ""))
        if force:
            with self.connection_context.connection.cursor() as cur:
                try:
                    # SQLTRACE added sql_tracer
                    execute_logged(cur, f"DROP PROCEDURE {(smart_quote(proc_name))}")
                except dbapi.Error:
                    pass
                except Exception as err:
                    pass
        sql = "CREATE PROCEDURE {} ()\nAS\nBEGIN".format(smart_quote(proc_name)) + sql.split("BEGIN", 1)[1]
        sql = sql[:-1] + ';'
        self.connection_context.execute_sql(sql)
        self.create_task(task_id=task_id, proc_name=proc_name, proc_schema=self.current_user,
                         force=force)
        setattr(self, f"{inference_task}_sql_proc_create_statement", sql)
        setattr(self, f"{inference_task}_materialized_tables", materialized_tabs)
        setattr(self, f"{inference_task}_output_tables", out_tabs)
        return self.create_task_schedule(task_id=task_id,
                                         cron=cron,
                                         recurrence_range=recurrence_range,
                                         force=force)

    def create_predict_schedule(self,#pylint:disable=too-many-positional-arguments
                                obj,
                                predict_params,
                                task_id,
                                cron,
                                proc_name=None,
                                recurrence_range=None,
                                output_table_names=None,
                                force=True):
        r"""
        Create a scheduled execution of the `predict()` method of a hana-ml object. A prerequisite step
        is to execute the `fit()` method of the hana-ml object first, so that a model is available for making inferences.

        Then, to achieve this designated objective,
        the following actions will be taken subsequently:

        - 1. A **stored SQL procedure** is firstly created for the `predict()` method to be executed
        - 2. A **task** is created for the stored SQL procedure
        - 3. The **task** created in Step 2 is scheduled for future execution

        Parameters
        ----------
        obj : hana-ml object
           A hana-ml object (i.e. an instance of some hana-ml class) with a callable `predict()` method.
           It needs to be fitted firstly.

           For example, ``obj`` can be a hana-ml object defined as follows:

           .. code-block:: python
             :linenos:

             from hana_ml.algorithms.pal.unified_classfication import UnifiedClassification
             obj = UnifiedClassification(func='HybridGradientBoostingTree', n_estimators=100).fit(data=data, key=..., label=...)

        predict_params : dict
           The key-value arguments (parameters) passed to the `predict()` method of ``obj``. Intrinsically it
           is the execution of

           .. code-block:: python

                obj.predict(**predict_params)

           to be scheduled.

        task_id : str
           The task ID for the task associated with the stored SQL procedure associated with the execution of the
           ``predict()`` method of ``obj``.

        cron : str
           Specifies the frequency pattern of task to be executed, which is the same as the definition of ``cron`` in
           method :func:`create_task_schedule<hana_ml.algorithms.pal.scheduler.ScheduledExecution.create_task_schedule>`.

        recurrence_range : dict, optional
            This parameter Specifies the range of time allowed for scheduled task execution.
            It is the same as the definition of ``recurrence_range`` in method
            :func:`create_task_schedule<hana_ml.algorithms.pal.scheduler.ScheduledExecution.create_task_schedule>`.

        output_table_names : ListOfStrings, optional
            User Specified names of output tables for the corresponding PAL procedure for model fitting.

            If not provided, the table names will be automatically generated.

        proc_name : str, optional
            Procedure name of the generated stored SQL procedure.

            Defaults to f"PROCEDURE_{task_id}" if not provided.

        force : bool, optional
             Specifies whether or not to force the creation of the task schedule for the execution of the
             `predict()` method of ``obj``.

             It set as True, it will firstly try to drop previously existing procedures with the same name as well as tasks/schedules
             with the same ``task_id``, then re-created and re-schedule.

            Defaults to True.

        Examples
        --------
        Assuming training dataset for classification is stored in table "CLS_DATA_TBL_TRAIN",
        and a sepearate data for prediction is stored in table "CLS_DATA_TBL_PREDICT".
        we want to schedule the prediction of an HGBT model in UnifiedClassficiation
        interface for the prediction dataset. Then, we can proceed as follows:

        >>> from hana_ml.dataframe import ConnectionContext
        >>> cc = ConnectionContext(address=..., port=..., user=..., password=...)
        >>> scheduler = ScheduledExecution(cc)
        >>> train_data = cc.table("CLS_DATA_TBL_TRAIN")
        >>> from hana_ml.algorithms.pal.unified_classification import UnifiedClassficiation
        >>> uhgc = UnifiedClassficiation(func="HybridGradientBoostingTree",
        ...                              n_estimators=100).fit(data=train_data, key=...)
        >>> predict_data = cc.table("CLS_DATA_TBL_PREDICT")
        >>> predict_params = dict(data=predict_data, key=...)
        >>> schedule_info = scheduler.create_predict_schedule(obj=uhgc,
        ...                                                   predict_params=predict_params,
        ...                                                   task_id="CLS_DATA_TBL_PREDICT",
        ...                                                   cron="2025 3 14 * 9 0 0",#means 9:00 AM, March 14, 2025
        ...                                                   force=True)
        """
        return self.__create_inference_schedule(inference_task="predict",
                                                obj=obj,
                                                inference_params=predict_params,
                                                task_id=task_id,
                                                proc_name=proc_name,
                                                cron=cron,
                                                recurrence_range=recurrence_range,
                                                output_table_names=output_table_names,
                                                force=force)

    def get_predict_sql_proc_create_statement(self):
        """Get the SQL statement for creating the predict procedure that has been scheduled for execution."""
        return self.predict_sql_proc_create_statement

    def list_materialized_tables_predict(self):
        """
        Get the materialization table names of temp tables for the scheduled hana-ml predict() execution.
        """
        return self.predict_materialized_tables

    def list_output_tables_predict(self):
        """
        Get the output table names for the scheduled hana-ml predict() execution.
        """
        return self.predict_output_tables

    def create_score_schedule(self,#pylint:disable=too-many-positional-arguments
                              obj,
                              score_params,
                              task_id,
                              cron,
                              proc_name=None,
                              recurrence_range=None,
                              output_table_names=None,
                              force=True):
        r"""
        Create a scheduled execution of the `score()` method of a hana-ml object (the method must invoke a PAL SCORE procedure internally).
        A prerequisite step is to execute the `fit()` method of the hana-ml object first, so that a model is available for scoring on test data.

        Then, to achieve this designated objective,
        the following actions will be taken subsequently:

        - 1. A **stored SQL procedure** is firstly created for the `score()` method to be executed
        - 2. A **task** is created for the stored SQL procedure
        - 3. The **task** created in Step 2 is scheduled for future execution

        Parameters
        ----------
        obj : hana-ml object
           A hana-ml object (i.e. an instance of some hana-ml class) with a callable `score()` method which can invoke
           the execution of a PAL SCORE procedure.
           It needs to be fitted firstly.

           For example, ``obj`` can be a hana-ml object defined as follows:

           .. code-block:: python
             :linenos:

             from hana_ml.algorithms.pal.unified_classfication import UnifiedClassification
             obj = UnifiedClassification(func='HybridGradientBoostingTree', n_estimators=100).fit(data=data, key=..., label=...)

        score_params : dict
           The key-value arguments (parameters) passed to the `score()` method of ``obj``. Intrinsically it
           is the execution of

           .. code-block:: python

                obj.score(**score_params)

           to be scheduled.

        task_id : str
           The task ID for the task associated with the stored SQL procedure associated with the execution of the
           ``score()`` method of ``obj``.

        cron : str
           Specifies the frequency pattern of task to be executed, which is the same as the definition of ``cron`` in
           method :func:`create_task_schedule<hana_ml.algorithms.pal.scheduler.ScheduledExecution.create_task_schedule>`.

        proc_name : str, optional
            Procedure name of the generated stored SQL procedure.

            Defaults to f"PROCEDURE_{task_id}" if not provided.

        recurrence_range : dict, optional
            This parameter Specifies the range of time allowed for scheduled task execution.
            It is the same as the definition of ``recurrence_range`` in
            :func:`create_task_schedule<hana_ml.algorithms.pal.scheduler.ScheduledExecution.create_task_schedule>`.

        output_table_names : ListOfStrings, optional
            User specified names of output tables for the corresponding PAL procedure for model fitting.

            If not provided, the table names will be automatically generated.

        force : bool, optional
            Specifies whether or not to force the creation of the task schedule for the execution of the
            `score` method of ``obj``.

            It set as True, it will firstly try to drop previously existing procedures with the same name as well as tasks/schedules
            with the same ``task_id``, then re-created and re-schedule.

            Defaults to True.

        Examples
        --------
        Assuming a dataset for classification is split into train and test parts,
        stored separately in table "CLS_DATA_TBL_TRAIN" and table "CLS_DATA_TBL_TEST",
        we want to schedule the training of an HGBT model using the UnifiedClassficiation
        interface provided in hana-ml, the we can proceed as follows:

        >>> from hana_ml.dataframe import ConnectionContext
        >>> cc = ConnectionContext(address=..., port=..., user=..., password=...)
        >>> scheduler = ScheduledExecution(cc)
        >>> from hana_ml.algorithms.pal.unified_classification import UnifiedClassficiation
        >>> uhgc = UnifiedClassficiation(func="HybridGradientBoostingTree",
        ...                              n_estimators=100)
        >>> train_data = cc.table("CLS_DATA_TBL_TRAIN")
        >>> uhgc.fit(data=train_data, key=...)#fit the train data firstly to generated a model for inference task
        >>> test_data = cc.table("CLS_DATA_TBL_TEST")
        >>> score_params = dict(data=test_data, key=..., label=...)
        >>> schedule_info = scheduler.create_score_schedule(obj=uhgc,
        ...                                                 score_params=score_params,
        ...                                                 task_id="CLS_DATA_TBL_SCORE",
        ...                                                 cron="2025 3 14 * 9 0 0",#means 9:00 AM, March 14, 2025
        ...                                                 force=True)
        """
        return self.__create_inference_schedule(inference_task="score",
                                                obj=obj,
                                                inference_params=score_params,
                                                task_id=task_id,
                                                proc_name=proc_name,
                                                cron=cron,
                                                recurrence_range=recurrence_range,
                                                output_table_names=output_table_names,
                                                force=force)

    def get_score_sql_proc_create_statement(self):
        """Get the SQL statement for creating the score procedure that has been scheduled for execution."""
        return self.score_sql_proc_create_statement

    def list_materialized_tables_score(self):
        """
        Get the materialization table names of temp tables for the scheduled hana-ml score() execution.
        """
        return self.score_materialized_tables

    def list_output_tables_score(self):
        """
        Get the output table names for the scheduled hana-ml score() execution.
        """
        return self.score_output_tables
