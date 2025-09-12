"""
This module contains related classes for monitoring the pipeline progress status.

The following classes are available:

    * :class:`PipelineProgressStatusMonitor`
    * :class:`SimplePipelineProgressStatusMonitor`
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
import os
import threading
import time
from urllib.parse import quote
from typing import List
from enum import Enum
import json
import pandas as pd
from prettytable import PrettyTable
from hana_ml.dataframe import ConnectionContext
from hana_ml.visualizers.shared import EmbeddedUI


TRUE_FLAG = '__js_true'
FALSE_FLAG = '__js_false'
BINDED_PROPERTY = 'FRAME_P_S'


class TaskSchedulerConfig(object):
    def __init__(self):
        self.automatic_obj = None
        self.is_simple_mode = True
        self.highlight_metric = None
        self.connection_context = None
        self.is_use_native_log_table = False
        self.executed_connection_id = None
        self.task_execution_interval = None

        self.runtime_platform = EmbeddedUI.get_runtime_platform()[1]
        self.debug = False if self.runtime_platform != 'databricks' else True

        self.progress_indicator_id = None   # progress_indicator_id <==> iframe_id
        self.execution_ids = []    # progress_indicator_id -> execution_id1, execution_id2, ...
        self.is_grouped = False
        self.group_ids = []

        self.task_scheduler: TaskScheduler = None

    def enable_debug(self):
        self.debug = True

    def set_task_execution_interval(self, task_execution_interval):
        self.task_execution_interval = task_execution_interval

    def set_automatic_obj(self, automatic_obj):
        self.automatic_obj = automatic_obj
        self.is_simple_mode = False if automatic_obj else True

    def set_connection_context(self, connection_context: ConnectionContext):
        if connection_context:
            self.connection_context = connection_context

    def use_native_log_table(self):
        self.is_use_native_log_table = True

    def use_custom_log_table(self):
        self.is_use_native_log_table = False

    def set_progress_indicator_id(self, progress_indicator_id):
        if progress_indicator_id:
            self.progress_indicator_id = progress_indicator_id
            self.execution_ids = [progress_indicator_id]

    def set_grouped(self, is_grouped):
        self.is_grouped = is_grouped

    def set_execution_ids(self, execution_ids, group_ids):
        if execution_ids and group_ids and len(execution_ids) == len(group_ids) and len(execution_ids) > 0:
            self.execution_ids = execution_ids
            self.group_ids = group_ids
            self.set_grouped(True)

    def set_highlight_metric(self, highlight_metric):
        if highlight_metric:
            if isinstance(highlight_metric, str):
                self.highlight_metric = highlight_metric
            elif isinstance(highlight_metric, dict) and len(self.group_ids) > 0:
                temp_highlight_metric = {}
                for group_id in self.group_ids:
                    temp_highlight_metric[group_id] = str(highlight_metric.get(group_id))
                self.highlight_metric = temp_highlight_metric

    def set_executed_connection_id(self, executed_connection_id):
        self.executed_connection_id = executed_connection_id

    def start_progress_status_monitor(self):
        self.task_scheduler = TaskScheduler(self)
        self.task_scheduler.start()
        if self.is_grouped:
            del self.automatic_obj.progress_monitor_config


class TaskStatus(Enum):
    Cancelled = -1    # automatic_obj raise exception or fronted send cancel cmd
    Running = 0
    Completed = 1     # automatic_obj execute complete, not included: fetch data and read data


class TaskScheduler(threading.Thread):
    def __init__(self, config: TaskSchedulerConfig):
        threading.Thread.__init__(self)
        self.config = config
        self.status = TaskStatus.Running

        self.fetch_progress_status_task = FetchProgressStatusTask(self)
        self.output_progress_status_task = None
        if self.config.debug:
            self.output_progress_status_task = OutputProgressStatusToLocalFileTask(self)
        else:
            if self.config.runtime_platform == 'console':
                self.output_progress_status_task = OutputProgressStatusToConsoleTask(self)
            else:
                self.output_progress_status_task = OutputProgressStatusToUITask(self)

    def run(self):
        self.fetch_progress_status_task.start()
        self.output_progress_status_task.start()

        interrupt_file_path = EmbeddedUI.get_resource_temp_file_path(self.output_progress_status_task.task_id)
        while True:
            automatic_obj_execution_status = self.config.automatic_obj._status if self.config.automatic_obj else 0
            if automatic_obj_execution_status < 0:
                self.status = TaskStatus.Cancelled  # interrupted for exception
            elif automatic_obj_execution_status > 0:
                self.status = TaskStatus.Completed
            elif os.path.exists(interrupt_file_path):
                if self.config.automatic_obj:
                    if self.config.executed_connection_id is None:
                        if hasattr(self.config.automatic_obj, 'fit_data'):
                            self.config.executed_connection_id = self.config.automatic_obj.fit_data.connection_context.connection_id
                    # sql = "ALTER SYSTEM DISCONNECT SESSION '{}'"  or  sql = "ALTER SYSTEM CANCEL WORK IN SESSION '{}'"
                    temp_connection_context = EmbeddedUI.create_connection_context(self.config.connection_context)
                    temp_connection_context.execute_sql("ALTER SYSTEM CANCEL WORK IN SESSION '{}'".format(self.config.executed_connection_id))
                    temp_connection_context.close()
                self.status = TaskStatus.Cancelled
                os.remove(interrupt_file_path)

            if self.status == TaskStatus.Cancelled:
                if self.config.runtime_platform in ['vscode', 'bas']:
                    print('task.cancel: {}'.format(self.output_progress_status_task.task_id))
                break
            if self.status == TaskStatus.Completed:
                if self.config.runtime_platform in ['vscode', 'bas']:
                    print('task.end: {}'.format(self.output_progress_status_task.task_id))
                break


class ProgressStatus(object):
    def __init__(self, execution_id, function_name, group_id=None):
        self.execution_id = execution_id
        self.group_id = group_id
        self.function_name = function_name

        self.current_2_msg = {}
        self.current_2_status = {}

        self.fetch_completed = False

        self.can_read_max_current = -1
        self.already_read_current = -1

    def add_msg(self, current, msg, timestamp):
        self.can_read_max_current = current - 1  # when current is 2, max current of can read is 1.
        if self.current_2_msg.get(current) is None:
            self.current_2_msg[current] = []
            current_status = {'c': current, 't': str(timestamp), 'running': TRUE_FLAG}
            if self.group_id:
                current_status.update({'g': self.group_id})
            if current == 0:
                current_status.update({'f': self.function_name})
            self.current_2_status[current] = current_status
        if msg is not None and msg.strip() != '':
            self.current_2_msg[current].append(msg)
            if msg.find('{"state":"finished"}') >= 0:
                self.can_read_max_current = self.can_read_max_current + 1
                self.fetch_completed = True

    def is_read_completed(self):
        if self.fetch_completed and self.already_read_current == self.can_read_max_current:
            return True
        else:
            return False

    def read_next_msg(self):
        next_msg_dict = None
        next_current = self.already_read_current + 1

        if self.can_read_max_current >= 0 and next_current <= self.can_read_max_current:
            if self.current_2_msg.get(next_current) is not None:
                next_msg_str = ''.join(self.current_2_msg.get(next_current)).strip()
                next_msg_str = next_msg_str if next_msg_str != '' else 'None'
                next_msg_dict = self.current_2_status.get(next_current)
                next_msg_dict['m'] = next_msg_str
                self.already_read_current = next_current
        return next_msg_dict


class ProgressTable(object):
    def __init__(self, config: TaskSchedulerConfig):
        self.config = config

        self.name = "AUTOML_LOG" if not config.is_use_native_log_table else "FUNCTION_PROGRESS_IN_AFLPAL"
        self.schema = "PAL_CONTENT" if not config.is_use_native_log_table else "_SYS_AFL"
        self.execution_id = "EXECUTION_ID"
        self.progress_current = "SEQ" if not config.is_use_native_log_table else "PROGRESS_CURRENT"
        self.function_name = "EVENT_KEY" if not config.is_use_native_log_table else "FUNCTION_NAME"
        self.progress_timestamp = "EVENT_TIMESTAMP" if not config.is_use_native_log_table else "PROGRESS_TIMESTAMP"
        self.progress_message = "EVENT_MESSAGE" if not config.is_use_native_log_table else "PROGRESS_MESSAGE"

        self.fetched_columns = [self.execution_id, self.progress_current, self.function_name, self.progress_timestamp, self.progress_message]
        self.fetch_sql = "SELECT {} from {}.{} WHERE {}".format(', '.join(self.fetched_columns),
                                                                                self.schema,
                                                                                self.name,
                                                                                ' OR '.join(list(map(lambda x: f"EXECUTION_ID='{x}'", self.config.execution_ids))) if self.config.is_grouped else f"EXECUTION_ID='{self.config.progress_indicator_id}'")

        self.connection_cursor = config.connection_context.connection.cursor()
        self.connection_cursor.setfetchsize(32000)
        self.fetch_offset = 0

    def fetch_data(self):
        self.connection_cursor.execute(self.fetch_sql + " limit 1000 offset {}".format(self.fetch_offset))
        fetched_data = self.connection_cursor.fetchall()
        fetched_count = len(fetched_data)
        if fetched_count > 0:
            self.fetch_offset = self.fetch_offset + fetched_count
            return pd.DataFrame(fetched_data, columns=self.fetched_columns)
        else:
            return None


class FetchProgressStatusTask(threading.Thread):
    def __init__(self, task_scheduler: TaskScheduler):
        threading.Thread.__init__(self)
        self.task_scheduler = task_scheduler
        self.progress_table = ProgressTable(task_scheduler.config)

        self.execution_id_2_progress_status = {}

    def parse_fetched_data(self, fetched_pd_df: pd.DataFrame):
        execution_ids = list(fetched_pd_df[self.progress_table.execution_id])
        current_list = list(fetched_pd_df[self.progress_table.progress_current])
        msg_list = list(fetched_pd_df[self.progress_table.progress_message])
        timestamp_list = list(fetched_pd_df[self.progress_table.progress_timestamp])
        for row_index in range(0, fetched_pd_df.shape[0]):
            execution_id = execution_ids[row_index]
            current = current_list[row_index]
            msg = msg_list[row_index]
            timestamp = timestamp_list[row_index]

            progress_status = self.execution_id_2_progress_status.get(execution_id)
            if progress_status is None:
                function_name = str(list(fetched_pd_df.head(1)[self.progress_table.function_name])[0])
                group_id = None
                if self.task_scheduler.config.is_grouped:
                    for eid in self.task_scheduler.config.execution_ids:
                        if eid == execution_id:
                            group_id = self.task_scheduler.config.group_ids[self.task_scheduler.config.execution_ids.index(eid)]
                            break
                progress_status = ProgressStatus(execution_id, function_name, group_id)
                self.execution_id_2_progress_status[execution_id] = progress_status
            progress_status.add_msg(current, msg, timestamp)

    def is_fetch_completed(self):
        fetch_completed_count = 0
        for execution_id in self.task_scheduler.config.execution_ids:
            progress_status: ProgressStatus = self.execution_id_2_progress_status.get(execution_id)
            if progress_status and progress_status.fetch_completed:
                fetch_completed_count = fetch_completed_count + 1
        if fetch_completed_count == len(self.task_scheduler.config.execution_ids):
            return True
        else:
            return False

    def read_msgs(self, filter_str=None):
        msgs = []
        size = 0
        for execution_id in self.execution_id_2_progress_status:
            progress_status: ProgressStatus = self.execution_id_2_progress_status[execution_id]
            while size <= 999:  # 1000: Maximum number of UI status updates per time
                next_msg = progress_status.read_next_msg()  # next_msg: None | 'xxx'
                if next_msg is not None:
                    if filter_str and next_msg['m'].find(filter_str) >= 0:
                        pass
                    else:
                        msgs.append(next_msg)
                        size = size + 1
                else:
                    break

        if len(msgs) == 0:
            return None
        else:
            return msgs

    def is_read_completed(self):
        read_completed_count = 0
        for execution_id in self.task_scheduler.config.execution_ids:
            progress_status: ProgressStatus = self.execution_id_2_progress_status.get(execution_id)
            if progress_status and progress_status.is_read_completed():
                read_completed_count = read_completed_count + 1

        if read_completed_count == len(self.task_scheduler.config.execution_ids):
            return True
        else:
            return False

    def run(self):
        while True:
            if self.task_scheduler.status == TaskStatus.Cancelled or self.is_fetch_completed():
                break
            fetched_pd_df = self.progress_table.fetch_data()
            if fetched_pd_df is not None:
                self.parse_fetched_data(fetched_pd_df)
            time.sleep(self.task_scheduler.config.task_execution_interval)

        automatic_obj = self.task_scheduler.config.automatic_obj
        connection_context = self.task_scheduler.config.connection_context
        if automatic_obj:
            if self.task_scheduler.config.is_grouped:
                for execution_id in self.task_scheduler.config.execution_ids:
                    automatic_obj._progress_table_cleanup(connection_context, execution_id)
                    automatic_obj.progress_indicator_cleanup = None
            else:
                self.task_scheduler.config.automatic_obj.cleanup_progress_log(connection_context)
        connection_context.close()


class AbstractOutputProgressStatusTask(threading.Thread):
    def __init__(self, task_scheduler: TaskScheduler):
        threading.Thread.__init__(self)
        self.task_scheduler = task_scheduler
        self.task_id = EmbeddedUI.get_uuid()
        self.filter_str = None

    def output_msgs(self, msgs):
        pass

    def init(self):
        pass

    def on_task_did_complete(self):
        pass

    def run(self):
        self.init()

        while True:
            if self.task_scheduler.status == TaskStatus.Cancelled:
                self.output_msgs([{'running': FALSE_FLAG, 'cancelled': TRUE_FLAG}])
                break

            if self.task_scheduler.fetch_progress_status_task.is_read_completed():
                self.output_msgs([{'running': FALSE_FLAG}])
                break

            msgs = self.task_scheduler.fetch_progress_status_task.read_msgs(self.filter_str)
            if msgs is not None:
                self.output_msgs(msgs)

            time.sleep(self.task_scheduler.config.task_execution_interval)

        if self.task_scheduler.config.automatic_obj is None:
            self.task_scheduler.status = TaskStatus.Completed

        self.on_task_did_complete()

    @staticmethod
    def convert_msgs_to_str(msgs: List[dict]):
        return str(msgs).replace("'{}'".format(TRUE_FLAG), 'true').replace("'{}'".format(FALSE_FLAG), 'false')

    def get_html_str(self, initial_msgs_str='[]', comm_server_url=''):
        config = self.task_scheduler.config
        if config.is_grouped:
            html_str = EmbeddedUI.get_resource_template('massive_pipeline_progress.html').render(execution_ids=config.execution_ids,
                                                                                                 group_ids=config.group_ids,
                                                                                                 iframe_id=self.task_id,
                                                                                                 highlighted_metric_dict=config.highlight_metric if config.highlight_metric else {},
                                                                                                 is_simple_mode='true' if config.is_simple_mode else 'false',
                                                                                                 msgs_str=initial_msgs_str,
                                                                                                 runtime_platform=config.runtime_platform,
                                                                                                 will_be_binded_property=BINDED_PROPERTY,
                                                                                                 comm_server=quote(comm_server_url, safe=':/?=&'))
        else:
            html_str = EmbeddedUI.get_resource_template('pipeline_progress.html').render(progress_indicator_id=config.progress_indicator_id,
                                                                                         iframe_id=self.task_id,
                                                                                         highlighted_metric=config.highlight_metric if config.highlight_metric else '',
                                                                                         is_simple_mode='true' if config.is_simple_mode else 'false',
                                                                                         msgs_str=initial_msgs_str,
                                                                                         runtime_platform=config.runtime_platform,
                                                                                         will_be_binded_property=BINDED_PROPERTY,
                                                                                         comm_server=quote(comm_server_url, safe=':/?=&'))
        return html_str


class OutputProgressStatusToUITask(AbstractOutputProgressStatusTask):
    def __init__(self, task_scheduler: TaskScheduler):
        AbstractOutputProgressStatusTask.__init__(self, task_scheduler)

    # @override
    def init(self):
        iframe_height = 1000
        config = self.task_scheduler.config
        if config.is_grouped:
            iframe_height = 500 * len(config.group_ids)
        EmbeddedUI.render_html_str(EmbeddedUI.get_iframe_str(self.get_html_str(), self.task_id, iframe_height))

        if config.runtime_platform == 'bas':
            EmbeddedUI.execute_js_str("")
        else:
            EmbeddedUI.execute_js_str("", self_display_id=self.task_id)

        if config.is_grouped:
            self.filter_str = '"evaluating":'

        if config.runtime_platform in ['vscode', 'bas']:
            if config.is_simple_mode:
                print('task.type: simple mode')
            print('task.start: {}: {}'.format(EmbeddedUI.get_resource_temp_dir_path() + os.sep, self.task_id))
            print('In order to cancel AutoML execution or monitor execution on the BAS or VSCode platform, you must import the VSCode extension package manually.')
            print('VSCode extension package path: \n{}'.format(EmbeddedUI.get_resource_temp_file_path('hanamlapi-monitor-1.2.0.vsix')))

    # @override
    def output_msgs(self, msgs):
        msgs_str = self.convert_msgs_to_str(msgs)
        js_str = "targetWindow['{}']={}".format(BINDED_PROPERTY, msgs_str)
        js_str = "for (let i = 0; i < window.length; i++) {const targetWindow = window[i];if(targetWindow['iframeId']){if(targetWindow['iframeId'] === '" + self.task_id + "'){" + js_str + "}}}"

        if self.task_scheduler.config.runtime_platform == 'bas':
            EmbeddedUI.execute_js_str("{};".format(js_str))
        elif self.task_scheduler.config.runtime_platform == 'jupyter':
            EmbeddedUI.execute_js_str_for_update("{};".format(js_str), updated_display_id=self.task_id)
        elif self.task_scheduler.config.runtime_platform == 'vscode':
            vscode_script = "const scripts = document.getElementsByTagName('script');for (let i = 0; i < scripts.length; i++) {const hanamlPipelinePNode = scripts[i].parentNode;if(hanamlPipelinePNode.tagName == 'DIV' && scripts[i].innerText.indexOf('hanamlPipelinePNode') >= 0){hanamlPipelinePNode.remove();}}"
            EmbeddedUI.execute_js_str_for_update("{};{};".format(js_str, vscode_script), updated_display_id=self.task_id)


class OutputProgressStatusToLocalFileTask(AbstractOutputProgressStatusTask):
    def __init__(self, task_scheduler: TaskScheduler):
        AbstractOutputProgressStatusTask.__init__(self, task_scheduler)
        self.progress_file = open(EmbeddedUI.get_resource_temp_file_path(self.task_id + "_progress.txt"), 'a', encoding="utf-8")

    # @override
    def init(self):
        if self.task_scheduler.config.runtime_platform == 'databricks':
            from hana_ml.visualizers.server import CommServerManager
            comm_server_task_scheduler = CommServerManager()
            comm_server_task_scheduler.start()
            comm_server_url = comm_server_task_scheduler.get_comm_server_url()

            html_str = self.get_html_str(comm_server_url=comm_server_url)
            EmbeddedUI.generate_file(EmbeddedUI.get_resource_temp_file_path("{}.html".format(self.task_id)), html_str)
            print('Page URL: {}/page?id={}&type=PipelineProgressStatusUI'.format(comm_server_url, self.task_id))

    # @override
    def output_msgs(self, msgs):
        msgs_str = self.convert_msgs_to_str(msgs)
        self.progress_file.write(msgs_str + '\n')

    # @override
    def on_task_did_complete(self):
        self.progress_file.close()


class OutputProgressStatusToConsoleTask(AbstractOutputProgressStatusTask):
    def __init__(self, task_scheduler: TaskScheduler):
        AbstractOutputProgressStatusTask.__init__(self, task_scheduler)
        self.all_msgs = []
        self.status = 'Running'  # 'Running' 'Completed' 'Cancelled AutoML Execution' 'Cancelled Monitor Execution'
        self.current_total_progress = 0
        self.self_group_ids = None
        self.group_2_details = {}

        self.progress_table = PrettyTable()
        self.progress_table.field_names = [
            "Group",
            "Progress",
            "Early Stop",
            "Creation Time",
            "Pipeline Number",
            "Generation Number",
            "Function Name",
            "Current Generation"
        ]

    # @override
    def init(self):
        if self.task_scheduler.config.is_grouped:
            self.self_group_ids = self.task_scheduler.config.group_ids
        else:
            self.self_group_ids = ['None']
        for group_id in self.self_group_ids:
            self.group_2_details[group_id] = {
                'group': group_id,
                'execution_id': '*',
                'function_name': '*',
                'pipeline_number': '*',
                'generation_number': '*',
                'creation_time': '*',
                'current_generation': -1,
                'current_progress': 0,
                'early_stop': False
            }

    def output_to_console(self):
        self.progress_table.clear_rows()
        temp_current_total_progress = 0
        for group_id in self.self_group_ids:
            details = self.group_2_details[group_id]
            temp_current_total_progress = temp_current_total_progress + details['current_progress']
            self.progress_table.add_row([
                group_id,
                "{} %".format(int(details['current_progress'])),
                details['early_stop'],
                details['creation_time'],
                details['pipeline_number'],
                details['generation_number'],
                details['function_name'],
                "Initialization" if details['current_generation'] == 0 else details['current_generation']
            ])
        self.current_total_progress = "{} %".format(int(temp_current_total_progress / len(self.self_group_ids)))

        # EmbeddedUI.clear_output(self.task_scheduler.config.runtime_platform)
        print('Progress Indicator Id: {}  |  Status: {}  |  Progress: {}'.format(self.task_scheduler.config.progress_indicator_id, self.status, self.current_total_progress))
        print(self.progress_table)
        print("")

    def output_page(self):
        msgs_str = self.convert_msgs_to_str(self.all_msgs)
        html_str = self.get_html_str(initial_msgs_str=msgs_str)
        html_path = EmbeddedUI.get_resource_temp_file_path(self.task_id) + '.html'
        EmbeddedUI.generate_file(html_path, html_str)
        print("Generated file for pipeline progress status: ", html_path)

    # @override
    def output_msgs(self, msgs):
        for msg in msgs:
            self.all_msgs.append(msg)

            t = msg.get('t')
            m = msg.get('m')
            f = msg.get('f')
            running = msg.get('running')
            cancelled = msg.get('cancelled')

            if running == TRUE_FLAG:
                g = self.self_group_ids[0]
                if self.task_scheduler.config.is_grouped:
                    g = msg.get('g')
                details = self.group_2_details[g]

                if details['creation_time'] == '*':
                    details['creation_time'] = t
                if details['function_name'] == '*':
                    details['function_name'] = f

                m = json.loads(m)
                if m.get('pipeline_num') is not None:
                    details['pipeline_number'] = m.get('pipeline_num')
                elif m.get('generation_num') is not None:
                    details['generation_number'] = m.get('generation_num')
                elif m.get('generation') is not None:
                    details['current_generation'] = m.get('generation')
                    details['current_progress'] = (details['current_generation'] + 1) / (details['generation_number'] + 1 + 1) * 100  # Initialization 0 | * | Completed
                    self.output_to_console()
                elif m.get('early_stop') is not None:
                    details['early_stop'] = True
                elif m.get('state') == 'finished':
                    details['current_progress'] = 100
                    details['current_generation'] = -1
            else:
                if cancelled == TRUE_FLAG:
                    self.status = 'Cancelled Monitor Execution' if self.task_scheduler.config.is_simple_mode else 'Cancelled AutoML Execution'
                else:
                    self.status = 'Completed'
                self.output_to_console()
                self.output_page()


class PipelineProgressStatusMonitor(object):
    """
    The instance of this class can monitor the progress of AutoML execution.

    This real-time monitoring allows users to understand at what stage the automated machine learning execution is,
    thus providing insights and transparency about the process.

    Parameters
    ----------
    connection_context : :class:`~hana_ml.dataframe.ConnectionContext`
        The connection to the SAP HANA system.

        For example:

        .. only:: latex

            >>> from hana_ml.dataframe import ConnectionContext as CC
            >>> progress_status_monitor = PipelineProgressStatusMonitor(connection_context=CC(url, port, user, pwd),
                                                                        automatic_obj=auto_c)

        .. raw:: html

            <iframe allowtransparency="true" style="border:1px solid #ccc; background: #eeffcb;"
                src="_static/automl_progress_example.html" width="100%" height="100%" sandbox="">
            </iframe>

    automatic_obj : :class:`~hana_ml.algorithms.pal.auto_ml.AutomaticClassification` or :class:`~hana_ml.algorithms.pal.auto_ml.AutomaticRegression`
        An instance object of the AutomaticClassification type or AutomaticRegression type
        that contains the progress_indicator_id attribute.

    fetch_table_interval : float, optional
        Specifies the time interval of fetching the table of pipeline progress.

        Defaults to 1s.

    runtime_platform : str, optional
        Specify the running environment of the monitor.

        - 'console': output content in plain text format.
        - 'jupyter': running on the JupyterLab or Jupyter Notebook platform.
        - 'vscode': running on the VSCode platform.
        - 'bas': running on the SAP Business Application Studio platform.
        - 'databricks': running on the Databricks platform.

        By default, the running platform will be automatically detected. If an incorrect value is passed in, this parameter will be ignored.

        Defaults to None.

    Examples
    --------
    Create an AutomaticClassification instance:

    >>> progress_id = "automl_{}".format(uuid.uuid1())
    >>> auto_c = AutomaticClassification(generations=5,
                                         population_size=10,
                                         offspring_size=10,
                                         progress_indicator_id=progress_id)
    >>> auto_c.enable_workload_class("MY_WORKLOAD")

    Establish a PipelineProgressStatusMonitor object and then invoke start():

    >>> progress_status_monitor = PipelineProgressStatusMonitor(connection_context=dataframe.ConnectionContext(url, port, user, pwd),
                                                                automatic_obj=auto_c)
    >>> progress_status_monitor.start()
    >>> auto_c.fit(data=df_train)

    Output:

    .. image:: image/progress_classification.png

    In order to cancel AutoML execution on the BAS or VSCode platform, you must import the Visual Studio Code Extension (VSIX) manually.

    - .. image:: image/cancel_automl_execution_button.png

    Follow the image below to install hanamlapi-monitor-1.2.0.vsix file on VSCode or BAS.

    - .. image:: image/import_vscode_extension_0.png

    - .. image:: image/import_vscode_extension_2.png

    - .. image:: image/import_vscode_extension_3.png

    - .. image:: image/import_vscode_extension_4.png
    """
    def __init__(self, connection_context: ConnectionContext, automatic_obj, fetch_table_interval=1, runtime_platform=None):
        if automatic_obj is None:
            raise ValueError("The value of parameter automatic_obj is None!")
        self.original_connection_context = connection_context
        config = TaskSchedulerConfig()
        config.set_automatic_obj(automatic_obj)
        config.set_task_execution_interval(fetch_table_interval)  # Specifies the time interval of updating the UI of pipeline progress.
        self.config = config
        if 'massive_auto_ml' in str(automatic_obj):
            config.set_grouped(True)

    def start(self):
        """
        Call the method before executing the fit method of Automatic Object.
        """
        automatic_obj = self.config.automatic_obj
        new_connection_context = EmbeddedUI.create_connection_context(self.original_connection_context)

        automatic_obj._exist_auto_sql_content_log(new_connection_context)
        automatic_obj.persist_progress_log()
        automatic_obj._status = 0

        if automatic_obj._use_auto_sql_content is False:
            self.config.use_native_log_table()
        self.config.set_connection_context(new_connection_context)
        if automatic_obj.progress_indicator_id is None:
            automatic_obj.progress_indicator_id = EmbeddedUI.get_uuid()

        if self.config.is_grouped:
            automatic_obj.progress_monitor_config = self.config
        else:
            self.config.set_progress_indicator_id(automatic_obj.progress_indicator_id)
            if hasattr(automatic_obj, '_get_highlight_metric'):
                self.config.set_highlight_metric(automatic_obj._get_highlight_metric())
            self.config.start_progress_status_monitor()


class SimplePipelineProgressStatusMonitor(object):
    """
    An instance of this class offers functionality to monitor and track the progress of AutoML's execution at any given time through the progress_indicator_id.

    Parameters
    ----------
    connection_context : :class:`~hana_ml.dataframe.ConnectionContext`
        The connection to the SAP HANA system.

    fetch_table_interval : float, optional
        Specifies the time interval of fetching the table of pipeline progress.

        Defaults to 1s.

    runtime_platform : str, optional
        Specify the running environment of the monitor.

        - 'console': output content in plain text format.
        - 'jupyter': running on the JupyterLab or Jupyter Notebook platform.
        - 'vscode': running on the VSCode platform.
        - 'bas': running on the SAP Business Application Studio platform.
        - 'databricks': running on the Databricks platform.

        By default, the running platform will be automatically detected. If an incorrect value is passed in, this parameter will be ignored.

        Defaults to None.

    Examples
    --------
    Create an AutomaticClassification instance:

    >>> progress_id = "automl_{}".format(uuid.uuid1())
    >>> auto_c = AutomaticClassification(generations=5,
                                         population_size=10,
                                         offspring_size=10,
                                         progress_indicator_id=progress_id)
    >>> auto_c.enable_workload_class("MY_WORKLOAD")

    Establish a SimplePipelineProgressStatusMonitor object and invoke start():

    >>> progress_status_monitor = SimplePipelineProgressStatusMonitor(connection_context=dataframe.ConnectionContext(url, port, user, pwd))
    >>> progress_status_monitor.start(progress_indicator_id=progress_id, highlight_metric='ACCURACY')
    >>> auto_c.persist_progress_log()
    >>> auto_c.fit(data=df_train)

    Output:

    .. image:: image/simple_progress_classification.png

    In order to cancel monitor execution on the BAS or VSCode platform, you must import the Visual Studio Code Extension (VSIX) manually.

    - .. image:: image/cancel_monitor_execution_button.png

    Follow the image below to install hanamlapi-monitor-1.2.0.vsix file on VSCode or BAS.

    - .. image:: image/import_vscode_extension_0.png

    - .. image:: image/import_vscode_extension_2.png

    - .. image:: image/import_vscode_extension_3.png

    - .. image:: image/import_vscode_extension_4.png
    """
    def __init__(self, connection_context: ConnectionContext, fetch_table_interval=1, runtime_platform=None):
        self.original_connection_context = connection_context
        self.config = TaskSchedulerConfig()
        self.config.set_task_execution_interval(fetch_table_interval)  # Specifies the time interval of updating the UI of pipeline progress.

    def start(self, progress_indicator_id=None, highlight_metric=None):
        """
        This method can be called at any time.

        Parameters
        ----------
        progress_indicator_id : str
            A unique identifier which represents the ongoing automatic task.

        highlight_metric : str, optional
            Specify the metric that need to be displayed on the UI.
        """
        if progress_indicator_id is None:
            raise ValueError("The value of parameter progress_indicator_id is None!")
        self.config.set_progress_indicator_id(progress_indicator_id)
        self.config.set_highlight_metric(highlight_metric)
        self.config.set_connection_context(EmbeddedUI.create_connection_context(self.original_connection_context))
        if bool(self.original_connection_context.has_schema(schema='PAL_CONTENT')) is False:
            self.config.use_native_log_table()
        self.config.start_progress_status_monitor()
