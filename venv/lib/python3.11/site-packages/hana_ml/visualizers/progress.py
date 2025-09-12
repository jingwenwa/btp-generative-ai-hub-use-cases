"""
This module only for internal use, do not show them in the doc.

The following class is available:

    * :class:`GeneralProgressStatusMonitor`
"""

# pylint: disable=missing-function-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=too-few-public-methods
# pylint: disable=too-many-instance-attributes
# pylint: disable=protected-access
# pylint: disable=no-else-break
import threading
import pandas as pd
from hana_ml.dataframe import ConnectionContext
from hana_ml.visualizers.automl_progress import PipelineProgressStatusMonitor
from hana_ml.algorithms.pal.auto_ml import AutomaticClassification, AutomaticRegression
from hana_ml.visualizers.shared import EmbeddedUI


class ProgressStatus(object):
    def __init__(self):
        self.progress_current_2_message = {}
        self.progress_current_2_status = {}
        self.base_progress_status = None

        self.data_columns_tuples = []
        self.data_columns_tuples_count = 0
        self.parse_data_columns_tuple_index = 0

        self.progress_max = 9999
        self.available_max_progress_current = -1
        self.read_progress_current = -1

        self.fetch_end = False

    def fetch_done(self):
        return self.fetch_end

    def get_next_progress_status(self):
        current_data_columns_tuples_count = self.data_columns_tuples_count
        for data_columns_tuple_index in range(self.parse_data_columns_tuple_index, current_data_columns_tuples_count):
            self.parse_data_columns_tuple(self.data_columns_tuples[data_columns_tuple_index])
        self.parse_data_columns_tuple_index = current_data_columns_tuples_count

        progress_current_status = None
        if self.available_max_progress_current > -1:
            if self.read_progress_current + 1 <= self.available_max_progress_current:
                next_progress_current = self.read_progress_current + 1
                progress_message = ''.join(self.progress_current_2_message.get(next_progress_current))
                if progress_message.strip() == '':
                    progress_message = 'None'
                progress_current_status = self.progress_current_2_status.get(next_progress_current)
                progress_current_status['PROGRESS_MESSAGE'] = progress_message
                self.read_progress_current = next_progress_current
                progress_current_status.update(self.base_progress_status)
                return progress_current_status

        return progress_current_status

    def parse_data_columns_tuple(self, data_columns_tuple):
        pandas_df = pd.DataFrame(data_columns_tuple[0], columns=data_columns_tuple[1])
        if self.base_progress_status is None:
            head_row = pandas_df.head(1)
            self.base_progress_status = {
                'PROGRESS_MAX': list(head_row['PROGRESS_MAX'])[0],
                'EXECUTION_ID': str(list(head_row['EXECUTION_ID'])[0]),
                'FUNCTION_NAME': str(list(head_row['FUNCTION_NAME'])[0]),
                'HOST': str(list(head_row['HOST'])[0]),
                'PORT': list(head_row['PORT'])[0],
                'CONNECTION_ID': str(list(head_row['CONNECTION_ID'])[0]),
                'PROGRESS_TIMESTAMP': str(list(head_row['PROGRESS_TIMESTAMP'])[0]),
            }
            self.progress_max = list(head_row['PROGRESS_MAX'])[0]

        row_count = pandas_df.shape[0]
        progress_current_list = list(pandas_df['PROGRESS_CURRENT'])
        progress_msg_list = list(pandas_df['PROGRESS_MESSAGE'])
        progress_elapsedtime_list = list(pandas_df['PROGRESS_ELAPSEDTIME'])
        for row_index in range(0, row_count):
            progress_current = progress_current_list[row_index]
            if progress_current >= 1:
                self.available_max_progress_current = progress_current - 1
            progress_msg = progress_msg_list[row_index]
            progress_elapsedtime = progress_elapsedtime_list[row_index]
            if self.progress_current_2_message.get(progress_current) is None:
                if progress_msg.strip() == '' or progress_msg is None:
                    progress_msg = 'None'
                self.progress_current_2_message[progress_current] = [progress_msg]
                self.progress_current_2_status[progress_current] = {
                    'PROGRESS_CURRENT': progress_current,
                    'PROGRESS_ELAPSEDTIME': progress_elapsedtime
                }
            else:
                self.progress_current_2_message[progress_current].append(progress_msg)

    def push_data_columns_tuple(self, data_columns_tuple):
        self.data_columns_tuples.append(data_columns_tuple)
        self.data_columns_tuples_count += 1


class ProgressStatusMonitorThread(threading.Thread):
    def __init__(self, connection_context, pal_obj, interval):
        threading.Thread.__init__(self)
        self.done = False
        self.interrupted = False
        self.pal_obj = pal_obj
        self.progress_status = ProgressStatus()
        self.fetch_progress_status_thread = FetchProgressStatusThread(self, connection_context)
        self.display_progress_status_timer = DisplayProgressStatusTimer(self, interval)

    def is_interrupted(self):
        return self.interrupted

    def is_done(self):
        return self.done

    def run(self):
        self.fetch_progress_status_thread.start()
        self.display_progress_status_timer.start()

    def do_interrupt(self):
        self.interrupted = True

    def do_end(self):
        self.done = True


class FetchProgressStatusThread(threading.Thread):
    def __init__(self, manager: ProgressStatusMonitorThread, connection_context):
        threading.Thread.__init__(self)
        self.already_init = False
        self.manager = manager
        self.connection_context = connection_context
        self.cur = self.connection_context.connection.cursor()
        self.cur.setfetchsize(32000)
        self.target_columns1 = ['PROGRESS_CURRENT', 'PROGRESS_MESSAGE', 'PROGRESS_ELAPSEDTIME']
        self.target_columns2 = ['EXECUTION_ID', 'FUNCTION_NAME', 'HOST', 'PORT', 'CONNECTION_ID', 'PROGRESS_TIMESTAMP', 'PROGRESS_ELAPSEDTIME', 'PROGRESS_CURRENT', 'PROGRESS_MAX', 'PROGRESS_LEVEL', 'PROGRESS_MESSAGE']
        self.sql3 = "SELECT PROGRESS_CURRENT from _SYS_AFL.FUNCTION_PROGRESS_IN_AFLPAL WHERE EXECUTION_ID='{}'".format(self.manager.pal_obj.progress_indicator_id)

    def get_data_columns_tuple(self, sql, target_columns):
        self.cur.execute(sql)
        return (self.cur.fetchall(), target_columns)

    def run(self):
        offset = 0
        limit = 1000
        while True:
            if self.manager.is_interrupted():
                break
            current_data_columns_tuple = None
            if self.already_init is True:
                current_data_columns_tuple = self.get_data_columns_tuple("SELECT PROGRESS_CURRENT, PROGRESS_MESSAGE, PROGRESS_ELAPSEDTIME from _SYS_AFL.FUNCTION_PROGRESS_IN_AFLPAL WHERE EXECUTION_ID='{}' limit {} offset {}".format(self.manager.pal_obj.progress_indicator_id, limit, offset), self.target_columns1)
            else:
                current_data_columns_tuple = self.get_data_columns_tuple("SELECT * from _SYS_AFL.FUNCTION_PROGRESS_IN_AFLPAL WHERE EXECUTION_ID='{}' limit {} offset {}".format(self.manager.pal_obj.progress_indicator_id, limit, offset), self.target_columns2)
            current_count = len(current_data_columns_tuple[0])
            if current_count == 0:
                if self.already_init is True:
                    if self.manager.progress_status.read_progress_current >= self.manager.progress_status.progress_max - 1:
                        self.manager.do_end()
                        break
                    elif len(self.get_data_columns_tuple(self.sql3, ['PROGRESS_CURRENT'])[0]) == 0:
                        self.manager.do_end()
                        break
            else:
                self.already_init = True
                self.manager.progress_status.push_data_columns_tuple(current_data_columns_tuple)
                offset = offset + current_count
        self.manager.progress_status.fetch_end = True
        self.connection_context.close()


class DisplayProgressStatusTimer(object):
    def __init__(self, manager: ProgressStatusMonitorThread, interval):
        self.manager = manager
        self.interval = interval
        self.self_timer = None

        self.frame_id = EmbeddedUI.get_uuid()
        html_str = EmbeddedUI.get_resource_template('progress.html').render(executionId=self.manager.pal_obj.progress_indicator_id, frameId=self.frame_id)
        self.iframe_str = EmbeddedUI.get_iframe_str(html_str, iframe_id=self.frame_id, iframe_height=1000)

    def display(self, js_str):
        EmbeddedUI.execute_js_str("{}".format(js_str), self_display_id=self.frame_id)

    def update_display(self, js_str):
        EmbeddedUI.execute_js_str_for_update("{}".format(js_str), updated_display_id=self.frame_id)

    def generate_js_str(self, progress_status_str):
        js_str = "targetWindow['{}']={}".format(self.manager.pal_obj.progress_indicator_id, progress_status_str)
        js_str = "for (let i = 0; i < window.length; i++) {const targetWindow = window[i];if(targetWindow['frameId']){if(targetWindow['frameId'] === '" + self.frame_id + "'){" + js_str + "}}}"
        return js_str

    def update_progress_status(self, progress_status):
        progress_status['pending'] = '__js_true'
        progress_status_str = str(progress_status).replace("'__js_true'", 'true')
        self.update_display(self.generate_js_str(progress_status_str))

    def delete_progress_status(self, progress_status):
        progress_status['pending'] = '__js_false'
        progress_status_str = str(progress_status).replace("'__js_false'", 'false')
        self.update_display(self.generate_js_str(progress_status_str))

    def __task(self):
        if self.manager.is_interrupted():
            self.update_display("document.getElementById('{}').style.display = 'none';".format(self.frame_id))
            return
        next_progress_status = self.manager.progress_status.get_next_progress_status()
        if next_progress_status is None:
            if self.manager.progress_status.fetch_done():
                self.delete_progress_status({})
            else:
                self.__run()
        else:
            self.update_progress_status(next_progress_status)
            self.__run()

    def __run(self):
        self.self_timer = threading.Timer(self.interval, self.__task)
        self.self_timer.start()

    def start(self):
        self.display("")
        EmbeddedUI.render_html_str(self.iframe_str)
        self.self_timer = threading.Timer(self.interval, self.__task)
        self.self_timer.start()


class GeneralProgressStatusMonitor(object):
    """
    If an ID of progress indicator is set when a function is executed, the class can monitor the execution progress of the function.

    Parameters
    ----------
    connection_context : ConnectionContext
        The connection to the SAP HANA system.

    pal_obj : :class:`~hana_ml.algorithms.pal.*`
        An instance object from PAL algorithms that contains the progress_indicator_id attribute.

    interval : float
        Time interval for querying progress information.
    """
    def __init__(self, connection_context: ConnectionContext, pal_obj, interval=0.01):
        self.connection_context = connection_context
        self.pal_obj = pal_obj
        self.interval = interval
        self.manager = None

    def start(self):
        """
        Call this method before function execution.
        """
        if isinstance(self.pal_obj, (AutomaticClassification, AutomaticRegression)):
            PipelineProgressStatusMonitor(self.connection_context, self.pal_obj, self.interval).start()
        else:
            self.manager = ProgressStatusMonitorThread(self.connection_context, self.pal_obj, self.interval)
            self.manager.start()

    def interrupt(self):
        """
        This method is called to interrupt the monitoring of function execution progress.
        """
        if self.manager is not None:
            self.manager.do_interrupt()
            self.manager = None
