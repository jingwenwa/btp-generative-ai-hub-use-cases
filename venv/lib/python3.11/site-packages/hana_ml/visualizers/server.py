"""
This module contains related class for more convenient communication on Databricks platform.

The following class is available:

    * :class:`CommServerManager`
"""

# pylint: disable=missing-function-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=invalid-name
# pylint: disable=unidiomatic-typecheck
# pylint: disable=bare-except
# pylint: disable=simplifiable-if-expression
# pylint: disable=import-error
import os
import json
import multiprocessing
import time
import socket
from flask import Flask, request
import requests
from waitress import serve
# https://learn.microsoft.com/zh-cn/azure/databricks/dev-tools/sdk-python
# The databricks platform has already provided the databricks-sdk library
# No need to install it yourself
from databricks.sdk.runtime import dbutils, spark   # type: ignore
from hana_ml.visualizers.shared import EmbeddedUI


def run_comm_server(port):
    app = Flask(__name__)

    @app.route("/check")
    def do_check():
        return "Comm Server"

    @app.route("/page")
    def get_page():
        html_str = "ERROR"
        iframe_id = request.args.get('id', default=None)
        page_type = request.args.get('type', default=None)
        if iframe_id and page_type:
            page_path = EmbeddedUI.get_resource_temp_file_path("{}.html".format(iframe_id))
            html_str = EmbeddedUI.get_file_str(page_path)
            if html_str is None:
                html_str = "Requested page is not exist! The requested page can only be accessed once!"
            if page_type == "AutoMLConfigUI":
                os.remove(page_path)  # The page can only be loaded once | Avoid occurrence: Multiple browsers, multiple pages
        return html_str

    @app.route("/cmd")
    def get_cmd():
        html_str = "ERROR"
        iframe_id = request.args.get('id', default=None)
        page_type = request.args.get('type', default=None)
        if iframe_id and page_type and page_type == 'AutoMLConfigUI':
            temp_comm_file_path = EmbeddedUI.get_resource_temp_file_path(iframe_id + '_config.json')
            temp_comm_file_str = EmbeddedUI.get_file_str(temp_comm_file_path)
            if temp_comm_file_str == "get_config_dict":
                html_str = "get_config_dict"
            else:
                html_str = "OK"
        return html_str

    @app.route("/interrupt_monitor")
    def interrupt_monitor():
        html_str = "ERROR"
        iframe_id = request.args.get('id', default=None)
        page_type = request.args.get('type', default=None)
        if iframe_id and page_type:
            EmbeddedUI.generate_file(EmbeddedUI.get_resource_temp_file_path(iframe_id), '')
            html_str = "OK"
        return html_str

    @app.route("/pipeline_progress_status")
    def get_pipeline_progress_status():
        html_str = "ERROR"
        iframe_id = request.args.get('id', default=None)
        page_type = request.args.get('type', default=None)
        start_position = request.args.get('pos', default=None)
        if iframe_id and page_type and start_position and page_type == 'PipelineProgressStatusUI':
            start_position = int(start_position)
            if start_position >= 0:
                file_path = EmbeddedUI.get_resource_temp_file_path(iframe_id + '_progress.txt')
                if os.path.exists(file_path):
                    file = open(file_path, 'r', encoding="utf-8")
                    lines = file.readlines()
                    file.close()
                    if start_position < len(lines):
                        html_str = lines[start_position:]
                    else:
                        html_str = '[]'
        return html_str

    @app.route("/experiment_monitor_status")
    def get_experiment_monitor_status():
        html_str = "ERROR"
        iframe_id = request.args.get('id', default=None)
        page_type = request.args.get('type', default=None)
        start_position = request.args.get('pos', default=None)
        if iframe_id and page_type and start_position and page_type == 'ExperimentMonitorUI':
            start_position = int(start_position)
            if start_position >= 0:
                file_path = EmbeddedUI.get_resource_temp_file_path(iframe_id + '_experiment_monitor.txt')
                if os.path.exists(file_path):
                    file = open(file_path, 'r', encoding="utf-8")
                    lines = file.readlines()
                    file.close()
                    if start_position < len(lines):
                        html_str = lines[start_position:]
                    else:
                        html_str = '[]'
        return html_str

    @app.route("/scheduled_task_monitor_status")
    def get_scheduled_task_monitor_status():
        html_str = "ERROR"
        iframe_id = request.args.get('id', default=None)
        page_type = request.args.get('type', default=None)
        start_position = request.args.get('pos', default=None)
        if iframe_id and page_type and start_position and page_type == 'ScheduledTaskMonitorUI':
            start_position = int(start_position)
            if start_position >= 0:
                file_path = EmbeddedUI.get_resource_temp_file_path(iframe_id + '_scheduledtask_monitor.txt')
                if os.path.exists(file_path):
                    file = open(file_path, 'r', encoding="utf-8")
                    lines = file.readlines()
                    file.close()
                    if start_position < len(lines):
                        html_str = lines[start_position:]
                    else:
                        html_str = '[]'
        return html_str

    @app.route('/save_config_dict', methods=['POST'])
    def save_config_dict():
        html_str = "ERROR"
        iframe_id = request.args.get('id', default=None)
        page_type = request.args.get('type', default=None)
        if iframe_id and page_type and page_type == 'AutoMLConfigUI':
            config_str = json.dumps(request.json)
            if config_str:
                EmbeddedUI.generate_file(EmbeddedUI.get_resource_temp_file_path(iframe_id + '_config.json'), config_str)
                html_str = "OK"
        return html_str

    serve(app, port=port)


class CommServerManager(object):
    def __init__(self) -> None:
        self.runtime_platform = EmbeddedUI.get_runtime_platform()[1]

        self.workspace_id = None
        self.cluster_id = None
        self.host_name = None
        self.notebook_id = "Local"

        if self.runtime_platform == 'databricks':
            ctx = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())      # type: ignore
            self.workspace_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")         # type: ignore
            self.cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")                   # type: ignore
            self.host_name = ctx.get("tags").get("browserHostName")
            self.notebook_id = ctx.get("tags").get("notebookId")

        self.process = None
        self.port = None

    @staticmethod
    def find_port(port):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(("localhost", port)) == 0:
                return CommServerManager.find_port(port=port + 1)
            else:
                return port

    @staticmethod
    def check_server_status(port):
        try:
            response = requests.get("http://localhost:{}/check".format(port), timeout=60)
            return True if response.text == "Comm Server" else False
        except:
            return False

    def get_port_file_path(self):
        return EmbeddedUI.get_resource_temp_file_path("{}_CommServerPort.txt".format(self.notebook_id))

    def start(self):
        port_file_path = self.get_port_file_path()
        port = None
        if os.path.exists(port_file_path):  # flie is exist, but server is closed
            port_file = open(port_file_path, 'r', encoding="utf-8")
            port_str = port_file.read()
            port_file.close()
            port = int(port_str)
            if CommServerManager.check_server_status(port):
                self.port = port
                return
            else:
                os.remove(port_file_path)

        port = CommServerManager.find_port(8888)
        self.process = multiprocessing.Process(name="Comm Server", target=run_comm_server, kwargs={"port": port})
        self.process.start()

        time.sleep(2)
        normal_server = False
        if CommServerManager.check_server_status(port):
            normal_server = True
        else:
            time.sleep(2)
            if CommServerManager.check_server_status(port):
                normal_server = True

        if normal_server:
            EmbeddedUI.generate_file(self.get_port_file_path(), str(port))
            self.port = port
        else:
            self.interrupt()
            raise Exception("Comm Server failed to start!")

    def interrupt(self):
        if self.process and self.process.is_alive():
            port_file_path = self.get_port_file_path()
            if os.path.exists(port_file_path):
                os.remove(port_file_path)
            self.process.terminate()
            # print("Comm Server is closed on {}!".format(self.port))

    def get_comm_server_url(self):
        # create dynamic url instead of 'http://localhost:[port]'
        url = None
        if self.runtime_platform != 'databricks':
            url = "http://localhost:{}".format(self.port)
        else:
            url = "https://" + self.host_name.replace("adb", "adb-dp") + "/driver-proxy/o/{}/{}/{}".format(self.workspace_id, self.cluster_id, self.port)
        return url
