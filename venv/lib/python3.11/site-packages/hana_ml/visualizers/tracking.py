"""
This module contains related classes for monitoring the MLTrack and scheduled task.

The following classes are available:

    * :class:`ExperimentMonitor`
    * :class:`ScheduledTaskMonitor`
"""
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
# pylint: disable=simplifiable-if-expression
# pylint: disable=simplifiable-if-statement
# pylint: disable=unused-argument
# pylint: disable=too-many-instance-attributes
# pylint: disable=protected-access
# pylint: disable=too-few-public-methods
# pylint: disable=invalid-name
# pylint: disable=too-many-positional-arguments
import os
import threading
import time
from urllib.parse import quote
from typing import List
from enum import Enum
from queue import Queue
import pandas as pd
from hdbcli import dbapi
from hana_ml.dataframe import ConnectionContext
from hana_ml.visualizers.shared import EmbeddedUI


TRUE_FLAG = '__js_true'
FALSE_FLAG = '__js_false'
BINDED_PROPERTY = 'TRACKING_IFRAME_P_S'
EXPERIMENT_SPLITTER = "_<EXPERIMENT_SPLITTER>_"


class TaskStatus(Enum):
    Cancelled = -1       # raise exception or fronted send cancel cmd
    Initialization = 0
    Running = 1
    Completed = 2        # execute complete, not included: fetch data and read data


class AbstractTaskSchedulerConfig(object):
    def __init__(self):
        self.debug = False
        self.runtime_platform = EmbeddedUI.get_runtime_platform()[1]
        if self.runtime_platform == 'databricks':
            self.debug = True

        self.connection_context = None

        self.msg_queue = Queue()
        self.read_task: AbstractReadTask = None
        self.write_task: AbstractWriteTask = None
        self.task_execution_interval = None
        self.status = TaskStatus.Initialization

        self.monitor_html_template = None
        self.monitor_uri = None

    def enable_debug(self):
        self.debug = True

    def set_connection_context(self, connection_context: ConnectionContext):
        if connection_context:
            self.connection_context = connection_context

    def set_task_execution_interval(self, task_execution_interval):
        self.task_execution_interval = task_execution_interval

    def create_write_task(self):
        if self.debug:
            self.write_task = WriteToLocalFileTask(self)
        else:
            self.write_task = WriteToUITask(self)

    def start_task_scheduler(self):
        self.create_read_task()
        self.create_write_task()
        self.task_scheduler = TaskScheduler(self)
        self.task_scheduler.start()

    def get_html_template_str(self):
        pass

    def create_read_task(self):
        pass


class AbstractReadTask(threading.Thread):
    def __init__(self, config: AbstractTaskSchedulerConfig):
        threading.Thread.__init__(self)
        self.config = config

        self.connection_cursor: dbapi.Cursor = config.connection_context.connection.cursor()
        self.connection_cursor.setfetchsize(32000)

    def read(self):
        pass

    def run(self):
        while True:
            if self.config.status == TaskStatus.Cancelled:
                break
            self.read()
            time.sleep(self.config.task_execution_interval)
        self.config.connection_context.close()


class AbstractWriteTask(threading.Thread):
    def __init__(self, config: AbstractTaskSchedulerConfig):
        threading.Thread.__init__(self)
        self.config = config
        self.task_id = EmbeddedUI.get_uuid()

    def init(self):
        pass

    def write(self, msgs):
        pass

    def on_task_did_complete(self):
        pass

    def run(self):
        self.init()
        time.sleep(2)

        target_msg_queue = self.config.msg_queue
        while True:
            if self.config.status == TaskStatus.Cancelled:
                self.write([{'cancelled': TRUE_FLAG}])
                break

            size = 0
            msgs = []
            min_size = min(target_msg_queue.qsize(), 1000)  # 1000: Maximum number of UI status updates per time
            while size < min_size:
                msgs.append(target_msg_queue.get())
                size = size + 1
            if size > 0:
                self.write(msgs)
            time.sleep(self.config.task_execution_interval)
        self.on_task_did_complete()

    @staticmethod
    def convert_msgs_to_str(msgs: List[dict]):
        return str(msgs).replace("'{}'".format(TRUE_FLAG), 'true').replace("'{}'".format(FALSE_FLAG), 'false')


class WriteToUITask(AbstractWriteTask):
    def __init__(self, config: AbstractTaskSchedulerConfig):
        AbstractWriteTask.__init__(self, config)

    # @override
    def init(self):
        EmbeddedUI.render_html_str(EmbeddedUI.get_iframe_str(self.config.get_html_template_str(), self.task_id, 600))

        if self.config.runtime_platform == 'bas':
            EmbeddedUI.execute_js_str("")
        else:
            EmbeddedUI.execute_js_str("", self_display_id=self.task_id)

    # @override
    def write(self, msgs):
        msgs_str = self.convert_msgs_to_str(msgs)
        js_str = "targetWindow['{}']={};".format(BINDED_PROPERTY, msgs_str)
        js_str = "for (let i = 0; i < window.length; i++) {const targetWindow = window[i];if(targetWindow['iframeId']){if(targetWindow['iframeId'] === '" + self.task_id + "'){" + js_str + "}}}"

        if self.config.runtime_platform == 'bas':
            EmbeddedUI.execute_js_str("{};".format(js_str))
        elif self.config.runtime_platform == 'jupyter':
            EmbeddedUI.execute_js_str_for_update("{};".format(js_str), updated_display_id=self.task_id)
        elif self.config.runtime_platform == 'vscode':
            vscode_script = "const scripts = document.getElementsByTagName('script');for (let i = 0; i < scripts.length; i++) {const hanamlPipelinePNode = scripts[i].parentNode;if(hanamlPipelinePNode.tagName == 'DIV' && scripts[i].innerText.indexOf('hanamlPipelinePNode') >= 0){hanamlPipelinePNode.remove();}}"
            EmbeddedUI.execute_js_str_for_update("{};{};".format(js_str, vscode_script), updated_display_id=self.task_id)


class WriteToLocalFileTask(AbstractWriteTask):
    def __init__(self, config: AbstractTaskSchedulerConfig):
        AbstractWriteTask.__init__(self, config)
        self.msg_file_path = EmbeddedUI.get_resource_temp_file_path(self.task_id + "_{}.txt".format(self.config.monitor_html_template))
        self.msg_file = open(self.msg_file_path, 'a', encoding="utf-8")
        self.all_msgs = []

    # @override
    def init(self):
        if self.config.runtime_platform == 'databricks':
            from hana_ml.visualizers.server import CommServerManager
            comm_server_manager = CommServerManager()
            comm_server_manager.start()
            comm_server_url = comm_server_manager.get_comm_server_url()

            html_str = self.config.get_html_template_str(comm_server_url=comm_server_url)
            EmbeddedUI.generate_file(EmbeddedUI.get_resource_temp_file_path("{}.html".format(self.task_id)), html_str)
            print('Page URL: {}/page?id={}&type={}'.format(comm_server_url, self.task_id, self.config.monitor_uri))

    # @override
    def write(self, msgs):
        msgs_str = self.convert_msgs_to_str(msgs)
        self.msg_file.write(msgs_str + '\n')
        for msg in msgs:
            self.all_msgs.append(msg)

    # @override
    def on_task_did_complete(self):
        self.msg_file.close()
        if self.config.runtime_platform != 'databricks':
            msgs_str = self.convert_msgs_to_str(self.all_msgs)
            html_str = self.config.get_html_template_str(initial_msgs_str=msgs_str)
            html_path = EmbeddedUI.get_resource_temp_file_path("{}.html".format(self.task_id))
            EmbeddedUI.generate_file(html_path, html_str)
            # print("Generated message file for monitor: ", self.msg_file_path)
            print("Generated HTML file for monitor: ", html_path)


class TaskScheduler(threading.Thread):
    def __init__(self, config: AbstractTaskSchedulerConfig):
        threading.Thread.__init__(self)
        self.config = config

    def run(self):
        self.config.read_task.start()
        self.config.write_task.start()
        self.config.status = TaskStatus.Running

        interrupt_file_path = EmbeddedUI.get_resource_temp_file_path(self.config.write_task.task_id)
        while True:
            if os.path.exists(interrupt_file_path):
                self.config.status = TaskStatus.Cancelled
                os.remove(interrupt_file_path)

            if self.config.status == TaskStatus.Cancelled:
                break
            if self.config.status == TaskStatus.Completed:
                break


class Experiment(object):
    def __init__(self, execution_id):
        self.execution_id = execution_id
        self.state = 'UNKNOWN'  # ACTIVE, FINISHED, FAILED

        self.seq_2_key = {}
        self.seq_2_msgs = {}
        self.seq_2_status = {}

        self.can_read_max_seq = -1
        self.already_read_seq = -1

        self.fetch_offset = 0
        self.fetch_completed = False

    def add_msg(self, seq, key, msg, timestamp, state):
        self.can_read_max_seq = seq - 1  # when current is 2, max current of can read is 1.
        if self.seq_2_msgs.get(seq) is None:
            self.seq_2_key[seq] = key
            self.seq_2_status[seq] = {'id': self.execution_id, 't': str(timestamp), 'k': key}
            self.seq_2_msgs[seq] = []
        if msg is not None and msg.strip() != '':
            self.seq_2_msgs[seq].append(msg)
        if self.state == 'UNKNOWN' or key == 'END_OF_TRACK':
            self.seq_2_status[seq]['state'] = state
            self.state = state
        if key == 'END_OF_TRACK':
            self.can_read_max_seq = self.can_read_max_seq + 1
            self.fetch_completed = True

    def read_next_status(self):
        next_msg_dict = None
        next_seq = self.already_read_seq + 1

        if self.can_read_max_seq >= 0 and next_seq <= self.can_read_max_seq:
            next_msg_list = self.seq_2_msgs.get(next_seq)
            if next_msg_list is not None:
                next_msg_str = ''.join(next_msg_list).strip()
                next_msg_str = next_msg_str if next_msg_str != '' else 'None'
                next_msg_dict = self.seq_2_status.get(next_seq)
                next_msg_dict['v'] = next_msg_str
                self.already_read_seq = next_seq
        return next_msg_dict


class ReadTaskOfExperimentMonitor(AbstractReadTask):
    def __init__(self, config: AbstractTaskSchedulerConfig):
        AbstractReadTask.__init__(self, config)

        self.TRACK_SCHEMA = "PAL_ML_TRACK"
        self.TRACK_TABLE = "TRACK_LOG"
        self.TRACK_METADATA_TABLE = "TRACK_METADATA"

        self.id_2_experiment = {}  # execution_id -> experiment
        self.last_ids = []  # execution_id, ...

    # @override
    def read(self):
        current_execution_ids = []
        current_execution_id_2_status = {}

        # 1. fetch execution_ids from track_metadata_table
        fetched_columns = ["TRACK_ID", "STATUS"]
        fetched_sql = "SELECT {} from {}.{}".format(', '.join(fetched_columns), self.TRACK_SCHEMA, self.TRACK_METADATA_TABLE)
        self.connection_cursor.execute(fetched_sql)
        fetched_data = self.connection_cursor.fetchall()
        fetched_count = len(fetched_data)
        if fetched_count > 0:
            fetched_pd_df = pd.DataFrame(fetched_data, columns=fetched_columns)
            execution_id_list = list(fetched_pd_df[fetched_columns[0]])
            status_list = list(fetched_pd_df[fetched_columns[1]])
            for i in range(0, fetched_pd_df.shape[0]):
                execution_id = execution_id_list[i]
                if execution_id.find(EXPERIMENT_SPLITTER) > 0:
                    current_execution_ids.append(execution_id)
                    current_execution_id_2_status[execution_id] = status_list[i]

        # 2. delete execution_ids
        deleted_execution_ids = list(set(self.last_ids) - set(current_execution_ids))
        self.last_ids = current_execution_ids
        for deleted_execution_id in deleted_execution_ids:
            del self.id_2_experiment[deleted_execution_id]
            self.config.msg_queue.put({'id': deleted_execution_id, 'state': 'deleted'})

        # 3. fetch logs from_track_table
        fetched_columns = ["SEQ", "EVENT_KEY", "EVENT_TIMESTAMP", "EVENT_MESSAGE"]
        for execution_id in current_execution_ids:
            experiment: Experiment = self.id_2_experiment.get(execution_id)
            current_state = current_execution_id_2_status[execution_id]
            if experiment is None:
                experiment = Experiment(execution_id)
                self.id_2_experiment[execution_id] = experiment
            if experiment.fetch_completed:
                if current_state != experiment.state:
                    experiment.state = current_state
                    self.config.msg_queue.put({'id': execution_id, 'state': current_state})
            else:
                fetch_sql = "SELECT {} from {}.{} WHERE EXECUTION_ID='{}' limit 1000 offset {}".format(', '.join(fetched_columns),
                                                                                                       self.TRACK_SCHEMA,
                                                                                                       self.TRACK_TABLE,
                                                                                                       execution_id,
                                                                                                       experiment.fetch_offset)
                self.connection_cursor.execute(fetch_sql)
                fetched_data = self.connection_cursor.fetchall()
                fetched_count = len(fetched_data)
                if fetched_count > 0:
                    experiment.fetch_offset = experiment.fetch_offset + fetched_count
                    fetched_pd_df = pd.DataFrame(fetched_data, columns=fetched_columns)
                    seq_list = list(fetched_pd_df[fetched_columns[0]])
                    key_list = list(fetched_pd_df[fetched_columns[1]])
                    time_list = list(fetched_pd_df[fetched_columns[2]])
                    msg_list = list(fetched_pd_df[fetched_columns[3]])
                    for i in range(0, fetched_pd_df.shape[0]):
                        experiment.add_msg(seq_list[i], key_list[i], msg_list[i], time_list[i], current_state)
                    while True:
                        next_msg = experiment.read_next_status()  # next_msg: None | 'xxx'
                        if next_msg is not None:
                            self.config.msg_queue.put(next_msg)
                        else:
                            break


class ScheduledTask(object):
    def __init__(self, task_id):
        self.task_id = task_id
        self.current_job_id = -1
        self.query_task_log_timestamp_condition = None
        self.task_definition_dict = None
        self.scheduler_definition_dict = None
        self.param_dict = {}  # param -> True


class ReadTaskOfScheduledTaskMonitor(AbstractReadTask):
    def __init__(self, config: AbstractTaskSchedulerConfig):
        AbstractReadTask.__init__(self, config)

        self.TASK_SCHEMA = "PAL_SCHEDULED_EXECUTION"

        self.task_id_2_scheduled_task = {}  # task_id -> scheduled_task
        self.last_task_ids = []  # task_id, ...

    # @override
    def read(self):
        current_task_ids = []
        current_added_task_ids = []

        # 0. fetch TASK_CHAIN table
        fetched_columns = ["TASK_ID", "TASK_OWNER", "PARENT_TASK_ID", "TASK_TYPE", "SEQ_ORDER", "PARALLEL_ATTRIBUTE", "CURRENT_JOB_ID", "CREATED_AT"]
        fetched_sql = "SELECT {} from {}.{}".format(', '.join(fetched_columns), self.TASK_SCHEMA, "TASK_CHAIN")
        self.connection_cursor.execute(fetched_sql)
        fetched_data = self.connection_cursor.fetchall()
        fetched_count = len(fetched_data)
        if fetched_count > 0:
            fetched_pd_df = pd.DataFrame(fetched_data, columns=fetched_columns)
            for index, row in fetched_pd_df.iterrows():
                task_definition_dict = row.to_dict()
                task_definition_dict['PARENT_TASK_ID'] = str(task_definition_dict['PARENT_TASK_ID'])
                task_definition_dict['SEQ_ORDER'] = str(task_definition_dict['SEQ_ORDER'])
                task_definition_dict['PARALLEL_ATTRIBUTE'] = str(task_definition_dict['PARALLEL_ATTRIBUTE'])
                task_definition_dict['CREATED_AT'] = str(task_definition_dict['CREATED_AT'])
                task_id = task_definition_dict['TASK_ID']
                del task_definition_dict['TASK_ID']
                current_task_ids.append(task_id)
                scheduled_task: ScheduledTask = self.task_id_2_scheduled_task.get(task_id)
                if scheduled_task is None:
                    current_added_task_ids.append(task_id)
                    scheduled_task = ScheduledTask(task_id)
                    scheduled_task.current_job_id = task_definition_dict['CURRENT_JOB_ID']
                    scheduled_task.query_task_log_timestamp_condition = task_definition_dict['CREATED_AT']
                    scheduled_task.task_definition_dict = task_definition_dict
                    self.task_id_2_scheduled_task[task_id] = scheduled_task
                else:
                    if scheduled_task.current_job_id != task_definition_dict['CURRENT_JOB_ID']:
                        scheduled_task.current_job_id = task_definition_dict['CURRENT_JOB_ID']
                        self.config.msg_queue.put({'id': task_id, 'op': 'update', 'task_definition': task_definition_dict})

        # 1. fetch TASK_DEFINITION table
        fetched_columns = ["TASK_ID", "PROC_SCHEMA", "PROC_NAME", "TASK_DESCRIPTION"]
        fetched_sql = "SELECT {} from {}.{}".format(', '.join(fetched_columns), self.TASK_SCHEMA, "TASK_DEFINITION")
        self.connection_cursor.execute(fetched_sql)
        fetched_data = self.connection_cursor.fetchall()
        fetched_count = len(fetched_data)
        if fetched_count > 0:
            fetched_pd_df = pd.DataFrame(fetched_data, columns=fetched_columns)
            for index, row in fetched_pd_df.iterrows():
                task_definition_dict = row.to_dict()
                task_id = task_definition_dict['TASK_ID']
                if task_id in current_added_task_ids:
                    del task_definition_dict['TASK_ID']
                    scheduled_task: ScheduledTask = self.task_id_2_scheduled_task.get(task_id)
                    task_definition_dict.update(scheduled_task.task_definition_dict)
                    scheduled_task.task_definition_dict = task_definition_dict
                    self.config.msg_queue.put({'id': task_id, 'op': 'add', 'task_definition': task_definition_dict})

        # 2. delete task_ids
        deleted_task_ids = list(set(self.last_task_ids) - set(current_task_ids))
        self.last_task_ids = current_task_ids
        for deleted_task_id in deleted_task_ids:
            del self.task_id_2_scheduled_task[deleted_task_id]
            self.config.msg_queue.put({'id': deleted_task_id, 'op': 'delete'})

        # 3. fetch TASK_PARAM table
        fetched_columns = ["TASK_ID", "PARAMETER_NAME", "ARGUMENT_SCHEMA", "ARGUMENT", "ARGUMENT_CATEGORY"]
        fetched_sql = "SELECT {} from {}.{}".format(', '.join(fetched_columns), self.TASK_SCHEMA, "TASK_PARAM")
        self.connection_cursor.execute(fetched_sql)
        fetched_data = self.connection_cursor.fetchall()
        fetched_count = len(fetched_data)
        if fetched_count > 0:
            fetched_pd_df = pd.DataFrame(fetched_data, columns=fetched_columns)
            for index, row in fetched_pd_df.iterrows():
                task_param_dict = row.to_dict()
                task_id = task_param_dict['TASK_ID']
                del task_param_dict['TASK_ID']
                scheduled_task: ScheduledTask = self.task_id_2_scheduled_task.get(task_id)
                param_name = task_param_dict['PARAMETER_NAME']
                if scheduled_task.param_dict.get(param_name) is None:
                    scheduled_task.param_dict[param_name] = True
                    self.config.msg_queue.put({'id': task_id, 'op': 'add', 'task_param': task_param_dict})

        # 4. fetch SCHEDULER_JOBS_TABLE
        fetched_columns = ["USER_NAME", "SCHEMA_NAME", "SCHEDULER_JOB_NAME", "PROCEDURE_SCHEMA_NAME", "PROCEDURE_NAME", "CRON", "START_TIME", "END_TIME", "IS_ENABLED", "IS_VALID", "COMMENTS", "CREATE_TIME"]
        fetched_sql = "SELECT {} from SCHEDULER_JOBS".format(', '.join(fetched_columns))
        self.connection_cursor.execute(fetched_sql)
        fetched_data = self.connection_cursor.fetchall()
        fetched_count = len(fetched_data)
        if fetched_count > 0:
            fetched_pd_df = pd.DataFrame(fetched_data, columns=fetched_columns)
            for index, row in fetched_pd_df.iterrows():
                task_scheduler_definition_dict = row.to_dict()
                task_id = task_scheduler_definition_dict['SCHEDULER_JOB_NAME']
                scheduled_task: ScheduledTask = self.task_id_2_scheduled_task.get(task_id)
                if scheduled_task and scheduled_task.scheduler_definition_dict is None:
                    del task_scheduler_definition_dict['USER_NAME']
                    del task_scheduler_definition_dict['SCHEDULER_JOB_NAME']
                    task_scheduler_definition_dict['START_TIME'] = str(task_scheduler_definition_dict['START_TIME'])
                    task_scheduler_definition_dict['END_TIME'] = str(task_scheduler_definition_dict['END_TIME'])
                    task_scheduler_definition_dict['COMMENTS'] = str(task_scheduler_definition_dict['COMMENTS'])
                    task_scheduler_definition_dict['CREATE_TIME'] = str(task_scheduler_definition_dict['CREATE_TIME'])
                    scheduled_task.scheduler_definition_dict = task_scheduler_definition_dict
                    self.config.msg_queue.put({'id': task_id, 'op': 'add', 'task_scheduler_definition': task_scheduler_definition_dict})

        # 5. fetch TASK_LOG table
        fetched_columns = ["JOB_ID", "SEQ", "EVENT_KEY", "EVENT_TIMESTAMP", "EVENT_MESSAGE"]
        for task_id in current_task_ids:
            scheduled_task: ScheduledTask = self.task_id_2_scheduled_task.get(task_id)
            fetched_sql = "SELECT {} from {}.{} WHERE TASK_ID = '{}' AND EVENT_TIMESTAMP > '{}' ORDER BY EVENT_TIMESTAMP ASC".format(', '.join(fetched_columns), self.TASK_SCHEMA, "TASK_LOG", task_id, scheduled_task.query_task_log_timestamp_condition)
            self.connection_cursor.execute(fetched_sql)
            fetched_data = self.connection_cursor.fetchall()
            fetched_count = len(fetched_data)
            if fetched_count > 0:
                fetched_pd_df = pd.DataFrame(fetched_data, columns=fetched_columns)
                for index, row in fetched_pd_df.iterrows():
                    task_log_dict = row.to_dict()
                    task_log_dict['EVENT_TIMESTAMP'] = str(task_log_dict['EVENT_TIMESTAMP'])
                    scheduled_task.query_task_log_timestamp_condition = task_log_dict['EVENT_TIMESTAMP']
                    self.config.msg_queue.put({'id': task_id, 'op': 'add', 'task_log': task_log_dict})


class TaskSchedulerConfigOfExperimentMonitor(AbstractTaskSchedulerConfig):
    def __init__(self):
        AbstractTaskSchedulerConfig.__init__(self)
        self.monitor_html_template = 'experiment_monitor'
        self.monitor_uri = 'ExperimentMonitorUI'

    # @override
    def get_html_template_str(self, initial_msgs_str='[]', comm_server_url=''):
        html_str = EmbeddedUI.get_resource_template('{}.html'.format(self.monitor_html_template)).render(iframe_id=self.write_task.task_id,
                                                                                                          msgs_str=initial_msgs_str,
                                                                                                          runtime_platform=self.runtime_platform,
                                                                                                          will_be_binded_property=BINDED_PROPERTY,
                                                                                                          experiment_splitter=EXPERIMENT_SPLITTER,
                                                                                                          monitor_uri=self.monitor_uri,
                                                                                                          comm_server=quote(comm_server_url, safe=':/?=&'))
        return html_str

    # @override
    def create_read_task(self):
        self.read_task = ReadTaskOfExperimentMonitor(self)


class TaskSchedulerConfigOfScheduledTaskMonitor(AbstractTaskSchedulerConfig):
    def __init__(self):
        AbstractTaskSchedulerConfig.__init__(self)
        self.monitor_html_template = 'scheduledtask_monitor'
        self.monitor_uri = 'ScheduledTaskMonitorUI'

    # @override
    def get_html_template_str(self, initial_msgs_str='[]', comm_server_url=''):
        html_str = EmbeddedUI.get_resource_template('{}.html'.format(self.monitor_html_template)).render(iframe_id=self.write_task.task_id,
                                                                                                          msgs_str=initial_msgs_str,
                                                                                                          runtime_platform=self.runtime_platform,
                                                                                                          will_be_binded_property=BINDED_PROPERTY,
                                                                                                          monitor_uri=self.monitor_uri,
                                                                                                          comm_server=quote(comm_server_url, safe=':/?=&'))
        return html_str

    # @override
    def create_read_task(self):
        self.read_task = ReadTaskOfScheduledTaskMonitor(self)


class ExperimentMonitor(object):
    """
    The instance of this class can monitor the MLTrack.

    Parameters
    ----------
    connection_context : :class:`~hana_ml.dataframe.ConnectionContext`
        The connection to the SAP HANA system.

    Examples
    --------
    Establish a ExperimentMonitor object and then invoke start():

    >>> experiment_monitor = ExperimentMonitor(connection_context=dataframe.ConnectionContext(url, port, user, pwd))
    >>> experiment_monitor.start()
    """
    def __init__(self, connection_context: ConnectionContext):
        self.config = TaskSchedulerConfigOfExperimentMonitor()
        self.config.set_connection_context(EmbeddedUI.create_connection_context(connection_context))
        self.config.set_task_execution_interval(1)

    def start(self):
        """
        Call the method to create an experiment monitor UI.
        """
        self.config.start_task_scheduler()

    def cancel(self):
        """
        Call the method to interrupt the execution of the experiment monitor.
        """
        self.config.status = TaskStatus.Cancelled
        print("The experiment monitor has been cancelled!")


class ScheduledTaskMonitor(object):
    """
    The instance of this class can monitor the scheduled task.

    Parameters
    ----------
    connection_context : :class:`~hana_ml.dataframe.ConnectionContext`
        The connection to the SAP HANA system.

    Examples
    --------
    Establish a ScheduledTaskMonitor object and then invoke start():

    >>> scheduled_task_monitor = ScheduledTaskMonitor(connection_context=dataframe.ConnectionContext(url, port, user, pwd))
    >>> scheduled_task_monitor.start()
    """
    def __init__(self, connection_context: ConnectionContext):
        self.config = TaskSchedulerConfigOfScheduledTaskMonitor()
        self.config.set_connection_context(EmbeddedUI.create_connection_context(connection_context))
        self.config.set_task_execution_interval(1)

    def start(self):
        """
        Call the method to create a scheduled task monitor UI.
        """
        self.config.start_task_scheduler()

    def cancel(self):
        """
        Call the method to interrupt the execution of the scheduled task monitor.
        """
        self.config.status = TaskStatus.Cancelled
        print("The scheduled task monitor has been cancelled!")
