"""
This module contains the usage of HANA Scheduler.

The following classes and functions are available:

    * :class:`HANAScheduler`
"""

#pylint: disable=invalid-name
#pylint: disable=protected-access
#pylint: disable=unused-argument
#pylint: disable=broad-except

import re
import logging

from datetime import datetime, timezone
from hdbcli import dbapi
from hana_ml.dataframe import smart_quote
from hana_ml.ml_base import execute_logged

logger = logging.getLogger(__name__)

def _drop_if_exist_create(sql, output_table_names, job_name):
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
    for idx, _ in enumerate(_extract_table_names(block1)):
        declare_st.append("DECLARE cnt_{} INTEGER;".format(idx))
        out_tab_name = "out_{}{}".format(job_name, idx)
        if idx < len(output_table_names):
            out_tab_name = output_table_names[idx]
        block2 += "cnt_{} := 0;\n".format(idx)
        block2 += "SELECT COUNT(*) INTO cnt_{} FROM M_TABLES WHERE TABLE_NAME='{}';\n".format(idx, out_tab_name)
        block2 += "IF :cnt_{0} > 0 THEN\n  DROP TABLE {1};\nEND IF;\n".format(idx, smart_quote(out_tab_name))
        block2 += "CREATE TABLE {1} AS (SELECT * FROM :out_{0});\n".format(idx, smart_quote(out_tab_name))

    block0 = block0.replace("DECLARE", "\n".join(declare_st) + "\nDECLARE", 1)
    return block0 + block2 + "END;"

def _find_temp_table(sql):
    res = re.findall(r" \"(#\S+)\";", sql) +\
          re.findall(r" (#\S+);", sql) +\
          re.findall(r" \"(#\S+)\" ", sql) +\
          re.findall(r" (#\S+) ", sql)
    return list(set(res))

class HANAScheduler(object):
    r"""
    HANA Scheduler.

    Attributes
    ----------
    connection_context : ConnectionContext
        The connection to the HANA system.

    Examples
    --------
    >>> #auto_c.disable_hana_execution()
    >>> auto_c.fit(df_fit, key='ID')
    >>> hana_schedule = HANAScheduler(connection_context)
    >>> hana_schedule.create_training_schedule(job_name='my_job',
                                               obj=auto_c,
                                               cron="* * * mon,tue,wed,thu,fri 1 23 45",
                                               output_table_names=['BEST_PIPELINE_1122', 'MODEL_1122', 'INFO_1122'],
                                               force=True)
    >>> hana_schedule.list_schedules()
        USER_NAME  SCHEMA_NAME  SCHEDULER_JOB_NAME  PROCEDURE_SCHEMA_NAME  PROCEDURE_NAME                               CRON  START_TIME  END_TIME  IS_ENABLED  IS_VALID  COMMENTS              CREATE_TIME
    0    PAL_TEST     PAL_TEST              my_job               PAL_TEST     PROC_my_job  * * * mon,tue,wed,thu,fri 1 23 45        None      None        TRUE      TRUE      None  2023-02-14 08:57:11.689
    >>> hana_schedule.delete_schedule("my_job")
    >>> auto_c.predict(df_predict, key='ID')
    >>> hana_schedule.create_applying_schedule(job_name='my_job2',
                                               obj=auto_c,
                                               cron="* * * mon,tue,wed,thu,fri 1 23 45",
                                               output_table_names=['RESULT_1123', 'INFO_1123'],
                                               force=True)
    """
    def __init__(self, connection_context):
        self.connection_context = connection_context

    def _check_scheduled_procedure(self, procedure_name):
        if '.' in procedure_name:
            procedure_schema, procedure_name = procedure_name.split('.', 1)
        else:
            procedure_schema = self.connection_context.get_current_schema()
        list_schedules = self.list_schedules()
        schema_job_pair_list = list_schedules[(list_schedules["PROCEDURE_SCHEMA_NAME"] == procedure_schema) & (list_schedules["PROCEDURE_NAME"] == procedure_name)][["SCHEMA_NAME", "SCHEDULER_JOB_NAME"]].values.tolist()
        schedule_status = self.display_schedule_status()
        for pair in schema_job_pair_list:
            schedule_status_pair = schedule_status[(schedule_status["SCHEMA_NAME"] == pair[0]) & (schedule_status["SCHEDULER_JOB_NAME"] == pair[1]) & schedule_status["STATUS"].str.contains("SCHEDULED")]
            print(schedule_status_pair)
            if len(schedule_status_pair) > 0:
                return True
        return False

    def _check_client_server_utctime(self):
        client_time = datetime.now(timezone.utc)
        server_time = self.connection_context.sql("SELECT CURRENT_UTCTIMESTAMP FROM DUMMY").collect().iat[0, 0]
        server_time = datetime.fromtimestamp(server_time.timestamp()).replace(tzinfo=timezone.utc)
        if abs((client_time - server_time).total_seconds()) > 60:
            logger.warning("The client's time does not align with the server's time. The server utctime is: %s. The client utctime is: %s.", server_time, client_time)

    def check_scheduler_job_exist(self, job_name):
        r"""
        Check if the job exists in HANA Scheduler.

        Parameters
        ----------
        job_name : str
            Specifies the name of the scheduled job.
        """
        if '.' in job_name:
            schema, name = job_name.split('.', 1)
        else:
            schema = self.connection_context.get_current_schema()
            name = job_name
        return self.connection_context.sql("SELECT * FROM SCHEDULER_JOBS WHERE SCHEMA_NAME='{}' AND SCHEDULER_JOB_NAME='{}'".format(schema, name)).count() > 0

    def list_schedules(self):
        """
        List all the schedule jobs.
        """
        return self.connection_context.sql("SELECT * FROM SCHEDULER_JOBS").collect()

    def get_job_names(self):
        """
        Get all the job names of the current user.
        """
        current_user = self.connection_context.get_current_schema()
        return self.connection_context.sql("SELECT SCHEDULER_JOB_NAME FROM SCHEDULER_JOBS WHERE USER_NAME='{}'".format(current_user)).collect()["SCHEDULER_JOB_NAME"].to_list()

    def display_schedule_status(self, sort_by="START_TIME", ascending=False):
        """
        Display the status of all the schedule jobs.

        Parameters
        ----------
        sort_by : str, optional
            Specifies the column to sort by.

            Defaults to "START_TIME".
        ascending : bool, optional
            Specifies the sort order.

            Defaults to False.
        """
        return self.connection_context.sql("SELECT * FROM M_SCHEDULER_JOBS").collect().sort_values(by=sort_by, ascending=ascending)

    def set_schedule(self, job_name, cron=None, procedure_name=None, job_start_time=None, job_end_time=None, status='active', procedure_params=None, force=False, disable_timezone_check=True):
        r"""
        Create or alter the schedule job.

        Parameters
        ----------
        job_name : str
            Specifies the name of the scheduled job.
        cron : str
            Specifies the frequency of the job.
            <cron> ::= <year> <month> <date> <weekday> <hour> <minute> <seconds>

            - <year>
              A four-digit number.
            - <month>
              A number from 1 to 12.
            - <date>
              A number from 1 to 31.
            - <weekday>
              A three-character day of the week: mon,tue,wed,thu,fri,sat,sun.
            - <hour>
              A number from 0 to 23 (expressed in 24-hour format).
            - <minute>
              A number from 0 to 59.
            - <seconds>
              A number from 0 to 59.

            Each cron field also supports wildcard characters as follows.

            - \* - Any value.
            - \*/n - Any n-th value. For example, \*/1 for the day of the month means run every day of the month, \*/3 means run every third day of the month.
            - a:b - Any value between a and b.
            - a:b/n - Any n-th value between a and b. For example, 1:10/3 for the day of the month means every 3rd day between 1 and 10 or the 3rd, 6th, and 9th day of the month.
            - n.a - (For <weekday> only) A day of the week where n is a number from -5 to 5 for the n-th occurrence of the day in week a. For example, for the year 2019, 2.3 means Tuesday, January 15th. -3.22 means Friday, May 31st.
        procedure_name : str, optional
            Specifies the name of the procedure in the scheduled job. If not specified, it will use "PROC_<job_name>".
        job_start_time : str, optional when server_side_scheduler is `True`
            Specifies the earliest time after which the scheduled job can start to run.
        job_end_time : str, optional when server_side_scheduler is `True`
            Specifies the latest time before which the scheduled job can start to run.
        status : {'active', 'inactive'}
            Enable or disable the schedule job.
        procedure_params : dict, optional
            Specifies the parameter name and value in the scheduled procedure. E.g., procedure_params={"THREAD_RATIO": 1}
        force : bool, optional
            If force is `True`, it will disable the existing job and delete it.

            Defaults to False.
        disable_timezone_check : bool, optional
            If disable_timezone_check is `True`, it will disable the timezone check.

            Defaults to True.
        """
        create_or_alter = 'CREATE'
        if not disable_timezone_check:
            self._check_client_server_utctime()
        is_job_exist = self.check_scheduler_job_exist(job_name)
        is_proc_exist = self._check_scheduled_procedure(procedure_name)
        if is_proc_exist:
            logger.warning("The procedure %s is already scheduled.", procedure_name)
        if is_job_exist:
            if force:
                self.delete_schedule(job_name)
            else:
                create_or_alter = 'ALTER'
        sql = "{} SCHEDULER JOB {} ".format(create_or_alter, smart_quote(job_name))
        if cron:
            sql = sql + "CRON '{}' ".format(cron)
        if job_start_time:
            sql = sql + "FROM '{}' ".format(job_start_time)
        if job_end_time:
            sql = sql + "UNTIL '{}' ".format(job_end_time)
        if status == 'inactive':
            sql = sql + "DISABLE"
        else:
            sql = sql + "ENABLE"
        if procedure_name:
            if create_or_alter == 'CREATE':
                sql = sql + " PROCEDURE {}".format(smart_quote(procedure_name))
        if procedure_params:
            p_params_str = ' PARAMETERS '
            for kkey, vval in procedure_params.items():
                if isinstance(vval, str):
                    p_params_str = p_params_str + " {}='{}',".format(smart_quote(kkey), vval)
                else:
                    p_params_str = p_params_str + " {}={},".format(smart_quote(kkey), vval)
            if p_params_str[-1] == ',':
                p_params_str = p_params_str[:-1]
            sql = sql +  p_params_str
        sql = sql + ";"
        self.connection_context.execute_sql(sql)
        return sql

    def delete_schedule(self, job_name):
        r"""
        Delete the given schedule job.

        Parameters
        ----------
        job_name : str
            Specifies the name of the scheduled job.
        """
        try:
            self.set_schedule(job_name=job_name, status='inactive', disable_timezone_check=True)
        except dbapi.Error:
            pass
        except Exception as err:
            logger.error(str(err))
            pass
        try:
            self.connection_context.execute_sql("DROP SCHEDULER JOB {}".format(smart_quote(job_name)))
        except dbapi.Error:
            pass
        except Exception as err:
            logger.error(str(err))
            pass

    def delete_schedules(self, job_names):
        r"""
        Delete the given schedule jobs.

        Parameters
        ----------
        job_names : list of str
            Specifies the names of the scheduled jobs.
        """
        for job_name in job_names:
            self.delete_schedule(job_name)

    def drop_procedure(self, procedure_name):
        """
        Drop the given procedure.

        Parameters
        ----------
        procedure_name : str
            Specifies the name of the procedure.
        """
        try:
            self.connection_context.execute_sql("DROP PROCEDURE {}".format(smart_quote(procedure_name)))
        except dbapi.Error:
            pass
        except Exception as err:
            logger.error(str(err))
            pass

    def clean_up_schedules(self):
        """
        Clean up all the schedule jobs.
        """
        job_names = self.get_job_names()
        self.delete_schedules(job_names)

    def _materialize_temp_tab(self, obj, sql, job_name, force):
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
                        new_tab_name = "in_{}{}".format(job_name, in_var.split("_")[1])
                        self.connection_context.sql(vval).save(new_tab_name, force=force)
                        logger.warning("Materialize %s to %s.", kkey, new_tab_name)
                        sql = sql.replace(line, in_var + " = " + "SELECT * FROM {};".format(smart_quote(new_tab_name)))
            if "#" in line:
                in_var, sel_statement = line.split(" = ", 1)
                new_tab_name = "in_{}{}".format(job_name, in_var.split("_")[1])
                self.connection_context.sql(sel_statement).save(new_tab_name, force=force)
                logger.warning("Materialize %s to %s.", sel_statement, new_tab_name)
                sql = sql.replace(line, in_var + " = " + "SELECT * FROM {};".format(smart_quote(new_tab_name)))
        in_statement = []
        for in_var, in_tab_name in zip(in_vars, in_tab_names):
            new_tab_name = in_tab_name
            if hasattr(obj, "materialize_dict"):
                for kkey, vval in obj.materialize_dict.items():
                    if kkey in in_tab_name:
                        new_tab_name = "in_{}{}".format(job_name, in_var.split("_")[1])
                        logger.warning("Materialize %s to %s.", kkey, new_tab_name)
                        self.connection_context.sql(vval).save(new_tab_name, force=force)
            in_statement.append("{} = SELECT * FROM {};\n".format(in_var, smart_quote(new_tab_name)))
        return sql.replace("CALL _SYS_AFL", "".join(in_statement) + "CALL _SYS_AFL")

    def create_training_schedule(self,
                                 job_name,
                                 obj,
                                 cron=None,
                                 job_start_time=None,
                                 job_end_time=None,
                                 status='active',
                                 output_table_names=None,
                                 procedure_name=None,
                                 force=True):
        r"""
        Creates the training procedure and the scheduled job.

        Parameters
        ----------
        job_name : str
            Specifies the name of the scheduled job.
        obj : hana-ml object
            The hana-ml object that has generated the execution statement. If the execution statement contains temporary table, it will perform the materialization.
            The materialized table name is according to "in_<job_name><idx>". <idx> is the index of the input table of the schedule procedure.
        cron : str
            Specifies the frequency of the job.
            <cron> ::= <year> <month> <date> <weekday> <hour> <minute> <seconds>

            - <year>
              A four-digit number.
            - <month>
              A number from 1 to 12.
            - <date>
              A number from 1 to 31.
            - <weekday>
              A three-character day of the week: mon,tue,wed,thu,fri,sat,sun.
            - <hour>
              A number from 0 to 23 (expressed in 24-hour format).
            - <minute>
              A number from 0 to 59.
            - <seconds>
              A number from 0 to 59.

            Each cron field also supports wildcard characters as follows.

            - \* - Any value.
            - \*/n - Any n-th value. For example, \*/1 for the day of the month means run every day of the month, \*/3 means run every third day of the month.
            - a:b - Any value between a and b.
            - a:b/n - Any n-th value between a and b. For example, 1:10/3 for the day of the month means every 3rd day between 1 and 10 or the 3rd, 6th, and 9th day of the month.
            - n.a (For <weekday> only) A day of the week where n is a number from -5 to 5 for the n-th occurrence of the day in week a. For example, for the year 2019, 2.3 means Tuesday, January 15th. -3.22 means Friday, May 31st.
        job_start_time : str, optional when server_side_scheduler is `True`
            Specifies the earliest time after which the scheduled job can start to run.
        job_end_time : str, optional when server_side_scheduler is `True`
            Specifies the latest time before which the scheduled job can start to run.
        status : {'active', 'inactive'}
            Enable or disable the schedule job.
        output_table_names : list of str, optional
            Specifies the output table names for the training result. If not specified, it will generate according to "out_<job_name><idx>". <idx> counts from 0.
        procedure_name : str, optional
            Specifies the name of the procedure in the scheduled job. If not specified, it will use "PROC_<job_name>".
        force : bool, optional
            If force is `True`, it will disable the existing job and delete it.

            Defaults to False.
        """
        if cron is None:
            raise ValueError('cron must be set.')
        if output_table_names is None:
            output_table_names = []
        #create output tables
        if procedure_name is None:
            procedure_name = "PROC_{}".format(job_name)
        sql = obj._fit_anonymous_block
        sql = self._materialize_temp_tab(obj, sql, job_name, force)
        sql = _drop_if_exist_create(sql, output_table_names, job_name)
        if force:
            self.drop_procedure(procedure_name)
        sql = "CREATE PROCEDURE {} ()\nAS\nBEGIN".format(smart_quote(procedure_name)) + sql.split("BEGIN", 1)[1]
        sql = sql[:-1] + ';'
        self.connection_context.execute_sql(sql)
        self.set_schedule(job_name=job_name,
                          cron=cron,
                          procedure_name=procedure_name,
                          job_start_time=job_start_time,
                          job_end_time=job_end_time,
                          status=status,
                          force=force)

    def create_applying_schedule(self,
                                 job_name,
                                 obj,
                                 cron=None,
                                 job_start_time=None,
                                 job_end_time=None,
                                 status='active',
                                 model_table_names=None,
                                 output_table_names=None,
                                 procedure_name=None,
                                 force=True):
        r"""
        Creates the applying procedure and the scheduled job.

        Parameters
        ----------
        job_name : str
            Specifies the name of the scheduled job.
        obj : hana-ml object
            The hana-ml object that has generated the execution statement. If the execution statement contains temporary table, it will perform the materialization.
            The materialized table name is according to "in_<job_name><idx>". <idx> is the index of the input table of the schedule procedure.
        cron : str
            Specifies the frequency of the job.
            <cron> ::= <year> <month> <date> <weekday> <hour> <minute> <seconds>

            - <year>
              A four-digit number.
            - <month>
              A number from 1 to 12.
            - <date>
              A number from 1 to 31.
            - <weekday>
              A three-character day of the week: mon,tue,wed,thu,fri,sat,sun.
            - <hour>
              A number from 0 to 23 (expressed in 24-hour format).
            - <minute>
              A number from 0 to 59.
            - <seconds>
              A number from 0 to 59.

            Each cron field also supports wildcard characters as follows.

            - \* - Any value.
            - \*/n - Any n-th value. For example, \*/1 for the day of the month means run every day of the month, \*/3 means run every third day of the month.
            - a:b - Any value between a and b.
            - a:b/n - Any n-th value between a and b. For example, 1:10/3 for the day of the month means every 3rd day between 1 and 10 or the 3rd, 6th, and 9th day of the month.
            - n.a - (For <weekday> only) A day of the week where n is a number from -5 to 5 for the n-th occurrence of the day in week a. For example, for the year 2019, 2.3 means Tuesday, January 15th. -3.22 means Friday, May 31st.
        job_start_time : str, optional when server_side_scheduler is `True`
            Specifies the earliest time after which the scheduled job can start to run.
        job_end_time : str, optional when server_side_scheduler is `True`
            Specifies the latest time before which the scheduled job can start to run.
        status : {'active', 'inactive'}
            Enable or disable the schedule job.
        model_table_names : str or list of str, optional
            Specifies the model table names for the predict result. If not, it will use temporary table name for materialization.
        output_table_names : list of str, optional
            Specifies the output table names for the predict result. If not specified, it will generate according to "out_<job_name><idx>". <idx> counts from 0.
        procedure_name : str, optional
            Specifies the name of the procedure in the scheduled job. If not specified, it will use "PROC_<job_name>".
        force : bool, optional
            If force is `True`, it will disable the existing job and delete it.

            Defaults to False.
        """
        if cron is None:
            raise ValueError('cron must be set.')
        if output_table_names is None:
            output_table_names = []
        if procedure_name is None:
            procedure_name = "PROC_{}".format(job_name)
        sql = obj._predict_anonymous_block
        sql = self._materialize_temp_tab(obj, sql, job_name, force)
        sql = _drop_if_exist_create(sql, output_table_names, job_name)
        unhandled_temp_tab = _find_temp_table(sql)
        if model_table_names is None:
            model_table_names = []
        if isinstance(model_table_names, str):
            model_table_names = [model_table_names]
        for idx, model_materialization in enumerate(unhandled_temp_tab):
            if idx < len(model_table_names):
                self.connection_context.table(model_materialization).save(model_table_names[idx], force=force)
                logger.warning("Materialize %s to %s.", model_materialization, model_table_names[idx])
                sql = sql.replace(model_materialization, model_table_names[idx])
            else:
                self.connection_context.table(model_materialization).save(model_materialization.replace("#", ""), force=force)
                logger.warning("Materialize %s to %s.", model_materialization, model_materialization.replace("#", ""))
                sql = sql.replace(model_materialization, model_materialization.replace("#", ""))
        if force:
            with self.connection_context.connection.cursor() as cur:
                if force:
                    try:
                        execute_logged(cur, "DROP PROCEDURE {}".format(smart_quote(procedure_name)))
                    except Exception as err:
                        pass
        sql = "CREATE PROCEDURE {} ()\nAS\nBEGIN".format(smart_quote(procedure_name)) + sql.split("BEGIN", 1)[1]
        sql = sql[:-1] + ';'
        self.connection_context.execute_sql(sql)
        self.set_schedule(job_name=job_name,
                          cron=cron,
                          procedure_name=procedure_name,
                          job_start_time=job_start_time,
                          job_end_time=job_end_time,
                          status=status,
                          force=force)

    def create_scoring_schedule(self,
                                job_name,
                                obj,
                                cron=None,
                                job_start_time=None,
                                job_end_time=None,
                                status='active',
                                model_table_names=None,
                                output_table_names=None,
                                procedure_name=None,
                                force=True):
        r"""
        Creates the scoring procedure and the scheduled job.

        Parameters
        ----------
        job_name : str
            Specifies the name of the scheduled job.
        obj : hana-ml object
            The hana-ml object that has generated the execution statement. If the execution statement contains temporary table, it will perform the materialization.
            The materialized table name is according to "in_<job_name><idx>". <idx> is the index of the input table of the schedule procedure.
        cron : str
            Specifies the frequency of the job.
            <cron> ::= <year> <month> <date> <weekday> <hour> <minute> <seconds>

            - <year>
              A four-digit number.
            - <month>
              A number from 1 to 12.
            - <date>
              A number from 1 to 31.
            - <weekday>
              A three-character day of the week: mon,tue,wed,thu,fri,sat,sun.
            - <hour>
              A number from 0 to 23 (expressed in 24-hour format).
            - <minute>
              A number from 0 to 59.
            - <seconds>
              A number from 0 to 59.

            Each cron field also supports wildcard characters as follows.

            - \* - Any value.
            - \*/n - Any n-th value. For example, \*/1 for the day of the month means run every day of the month, \*/3 means run every third day of the month.
            - a:b - Any value between a and b.
            - a:b/n - Any n-th value between a and b. For example, 1:10/3 for the day of the month means every 3rd day between 1 and 10 or the 3rd, 6th, and 9th day of the month.
            - n.a - (For <weekday> only) A day of the week where n is a number from -5 to 5 for the n-th occurrence of the day in week a. For example, for the year 2019, 2.3 means Tuesday, January 15th. -3.22 means Friday, May 31st.
        job_start_time : str, optional when server_side_scheduler is `True`
            Specifies the earliest time after which the scheduled job can start to run.
        job_end_time : str, optional when server_side_scheduler is `True`
            Specifies the latest time before which the scheduled job can start to run.
        status : {'active', 'inactive'}
            Enable or disable the schedule job.
        model_table_names : str or list of str, optional
            Specifies the model table names for the score result. If not, it will use temporary table name for materialization.
        output_table_names : list of str, optional
            Specifies the output table names for the score result. If not specified, it will generate according to "out_<job_name><idx>". <idx> counts from 0.
        procedure_name : str, optional
            Specifies the name of the procedure in the scheduled job. If not specified, it will use "PROC_<job_name>".
        force : bool, optional
            If force is `True`, it will disable the existing job and delete it.

            Defaults to False.
        """
        if output_table_names is None:
            output_table_names = []
        if procedure_name is None:
            procedure_name = "PROC_{}".format(job_name)
        sql = obj._score_anonymous_block
        if sql is None:
            raise NotImplementedError("No score function executed or found in {}!".format(obj.__module__))
        else:
            if 'SCORE' not in sql:
                raise NotImplementedError("No score function executed or found in {}!".format(obj.__module__))
        sql = self._materialize_temp_tab(obj, sql, job_name, force)
        sql = _drop_if_exist_create(sql, output_table_names, job_name)
        unhandled_temp_tab = _find_temp_table(sql)
        if model_table_names is None:
            model_table_names = []
        if isinstance(model_table_names, str):
            model_table_names = [model_table_names]
        for idx, model_materialization in enumerate(unhandled_temp_tab):
            if idx < len(model_table_names):
                self.connection_context.table(model_materialization).save(model_table_names[idx], force=force)
                logger.warning("Materialize %s to %s.", model_materialization, model_table_names[idx])
                sql = sql.replace(model_materialization, model_table_names[idx])
            else:
                self.connection_context.table(model_materialization).save(model_materialization.replace("#", ""), force=force)
                logger.warning("Materialize %s to %s.", model_materialization, model_materialization.replace("#", ""))
                sql = sql.replace(model_materialization, model_materialization.replace("#", ""))
        if force:
            try:
                self.connection_context.execute_sql("DROP PROCEDURE {}".format(smart_quote(procedure_name)))
            except dbapi.Error:
                pass
            except Exception as err:
                logger.error(str(err))
                pass
        sql = "CREATE PROCEDURE {} ()\nAS\nBEGIN".format(smart_quote(procedure_name)) + sql.split("BEGIN", 1)[1]
        sql = sql[:-1] + ';'
        self.connection_context.execute_sql(sql)
        self.set_schedule(job_name=job_name,
                          cron=cron,
                          procedure_name=procedure_name,
                          job_start_time=job_start_time,
                          job_end_time=job_end_time,
                          status=status,
                          force=force)
