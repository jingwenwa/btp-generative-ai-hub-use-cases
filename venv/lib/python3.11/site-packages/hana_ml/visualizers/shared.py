# pylint: disable=missing-module-docstring
# pylint: disable=missing-function-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=no-else-break
# pylint: disable=no-else-continue
# pylint: disable=protected-access, bare-except
import os
import sys
import uuid
import html
import time
import warnings
from IPython.display import display, update_display, HTML, Javascript, clear_output
from jinja2 import Environment, PackageLoader
from hana_ml.dataframe import ConnectionContext
from hana_ml.ml_exceptions import Error
warnings.filterwarnings('ignore')


class EmbeddedUI(object):
    def __init__(self) -> None:
        pass

    @staticmethod
    def get_runtime_platform():  # -1: console | 1: jupyter | 2: vscode | 3: bas | 4: databricks
        runtime_platform = (1, 'jupyter')
        if str(sys.__dict__['displayhook']).find('<built-in') >= 0:
            runtime_platform = (-1, 'console')
        else:
            for k in os.environ:
                if "ELECTRON_RUN_AS_NODE" in k:
                    runtime_platform = (2, 'vscode')
                    break
                elif "BAS" in k:
                    runtime_platform = (3, 'bas')
                    break
                elif "DATABRICKS" in k:
                    runtime_platform = (4, 'databricks')
                    break
        return runtime_platform

    @staticmethod
    def get_resource_package_loader():
        return PackageLoader('hana_ml.visualizers', 'templates')

    @staticmethod
    def get_resource_template(name):
        resource_dir = Environment(loader=EmbeddedUI.get_resource_package_loader())
        return resource_dir.get_template(name)

    @staticmethod
    def get_resource_root_dir_path():
        templates_dir_path = EmbeddedUI.get_resource_package_loader()._template_root
        return templates_dir_path

    @staticmethod
    def get_resource_temp_dir_path():
        temp_dir_path = EmbeddedUI.get_resource_root_dir_path() + os.sep + 'temp'
        if os.path.exists(temp_dir_path) is False:
            os.mkdir(temp_dir_path)
        return temp_dir_path

    @staticmethod
    def get_resource_temp_file_path(file_name):
        if file_name is None:
            raise ValueError('The parameter file_name cannot be None!')
        return EmbeddedUI.get_resource_temp_dir_path() + os.sep + file_name

    @staticmethod
    def generate_file(file_path, file_str):
        file = open(file_path, 'w', encoding="utf-8")
        file.write(file_str)
        file.close()

    @staticmethod
    def get_iframe_str(html_str, iframe_id: str = None, iframe_height: int = 800, min_iframe_height: int = 300, max_iframe_height: int = 5000):
        iframe_height = int(iframe_height)
        min_iframe_height = int(min_iframe_height)
        max_iframe_height = int(max_iframe_height)
        if iframe_height < min_iframe_height or iframe_height > max_iframe_height:
            raise ValueError("The parameter 'iframe_height' value is invalid! The effective range is [{}, {}].".format(min_iframe_height, max_iframe_height))
        if iframe_id is None:
            iframe_id = EmbeddedUI.get_uuid()

        iframe_template = '<iframe id="{id}" width="{w}" height="{h}" srcdoc="{html_str}"'
        iframe_template = iframe_template + ' style="border: 1px solid #ccc;"'
        iframe_template = iframe_template + ' allowfullscreen="true" webkitallowfullscreen="true" mozallowfullscreen="true" oallowfullscreen="true" msallowfullscreen="true" allow="fullscreen"'
        iframe_template = iframe_template + ' sandbox="allow-same-origin allow-scripts"'
        iframe_template = iframe_template + '></iframe>'

        #minified_html_str = htmlmin.minify(html_str, remove_all_empty_space=True, remove_comments=True, remove_optional_attribute_quotes=False)
        escaped_html_str = html.escape(html_str)
        iframe_str = iframe_template.format(id=iframe_id, w='99.80%', h='{}px'.format(iframe_height), html_str=escaped_html_str)
        return iframe_str

    @staticmethod
    def render_html_str(html_str):
        display(HTML(html_str))

    @staticmethod
    def execute_js_str(js_str, self_display_id=None):
        if js_str is None:
            raise ValueError('The parameter js_str cannot be None!')
        if self_display_id is None:
            display(Javascript(js_str))
        else:
            display(Javascript(js_str), display_id=self_display_id)

    @staticmethod
    def execute_js_str_for_update(js_str, updated_display_id):
        if js_str is None:
            raise ValueError('The parameter js_str cannot be None!')
        if updated_display_id is None:
            raise ValueError('The parameter updated_display_id cannot be None!')
        update_display(Javascript("{};".format(js_str)), display_id=updated_display_id)

    @staticmethod
    def get_uuid():
        return '{}'.format(uuid.uuid4()).replace('-', '_').upper()

    @staticmethod
    def create_connection_context(original_connection_context: ConnectionContext) -> ConnectionContext:
        if original_connection_context.userkey is None:
            conn_str = original_connection_context.connection.__str__().replace('<dbapi.Connection Connection object : ', '')[:-1]
            if conn_str.count(',') >= 4:
                for i in range(0, conn_str.count(',') - 4):
                    try:
                        url, remain_str = conn_str.split(',', 1)
                        port, remain_str = remain_str.split(',', 1)
                        user, remain_str = remain_str.split(',', 1)
                        password = remain_str.rsplit(',', i + 1)[0]
                        conn = ConnectionContext(url, port, user, password, **original_connection_context.properties)
                        return conn
                    except Exception as err:
                        if i < conn_str.count(',') - 5:
                            continue
                        else:
                            raise Error(err)
            conn_config = conn_str.split(',')
            url = conn_config[0]
            port = conn_config[1]
            user = conn_config[2]
            password = conn_config[3]
        try:
            if original_connection_context.userkey:
                conn = ConnectionContext(userkey=original_connection_context.userkey, **original_connection_context.properties)
            elif original_connection_context.sslKeyStore:
                conn = ConnectionContext(url, port, user, password="", sslKeyStore=original_connection_context.sslKeyStore, **original_connection_context.properties)
            else:
                conn = ConnectionContext(url, port, user, password, **original_connection_context.properties)
        except:
            if original_connection_context.userkey:
                conn = ConnectionContext(userkey=original_connection_context.userkey, encrypt='true', sslValidateCertificate='false', **original_connection_context.properties)
            elif original_connection_context.sslKeyStore:
                conn = ConnectionContext(url, port, user, password="", sslKeyStore=original_connection_context.sslKeyStore, encrypt='true', sslValidateCertificate='false', **original_connection_context.properties)
            else:
                conn = ConnectionContext(url, port, user, password, encrypt='true', sslValidateCertificate='false', **original_connection_context.properties)
        return conn

    @staticmethod
    def clear_output(runtime_platform):
        if runtime_platform is None:
            raise ValueError('The parameter runtime_platform cannot be None!')
        if runtime_platform == 'console':
            os.system('cls' if os.name == 'nt' else 'clear')  # windows: nt  |  linux/mac: posix
        else:
            clear_output(wait=True)

    @staticmethod
    def get_current_time():
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    @staticmethod
    def render_fullscreen_button(target_iframe_id):
        if EmbeddedUI.get_runtime_platform()[1] == 'jupyter':
            enter_fullscreen_button_html_sr = EmbeddedUI.get_resource_template('fullscreen.html').render(iframe_id=target_iframe_id)
            EmbeddedUI.render_html_str(EmbeddedUI.get_iframe_str(enter_fullscreen_button_html_sr, iframe_height=40, min_iframe_height=40))

    @staticmethod
    def get_file_str(file_path):
        file_str = None
        if file_path and os.path.exists(file_path):
            file = open(file_path, 'r', encoding="utf-8")
            file_str = file.read()
            file.close()
        return file_str


class JSONViewer(EmbeddedUI):
    def __init__(self, data_json_dict):  # data_json_dict format: {'key1': 'json string1', 'key2': 'json string2'}
        super().__init__()
        if data_json_dict is None:
            raise Exception("The value of parameter '{}' is null".format('data'))
        self.html_str = self.get_resource_template('json.html').render(data_json_dict=data_json_dict)

    def generate_notebook_iframe(self, iframe_height: int = 300):
        iframe_id = self.get_uuid()
        iframe_str = self.get_iframe_str(self.html_str, iframe_id, iframe_height)
        self.render_fullscreen_button(iframe_id)
        self.render_html_str(iframe_str)

    def generate_html(self, filename: str):
        self.generate_file('{}_json.html'.format(filename), self.html_str)


class XMLViewer(EmbeddedUI):
    def __init__(self, data_xml_dict):  # data_xml_dict format: {'key1': 'xml string1', 'key2': 'xml string2'}
        super().__init__()
        if data_xml_dict is None:
            raise Exception("The value of parameter '{}' is null".format('data'))

        self.html_str = self.get_resource_template('xml.html').render(data_xml_dict=data_xml_dict)

    def generate_notebook_iframe(self, iframe_height: int = 300):
        iframe_id = self.get_uuid()
        iframe_str = self.get_iframe_str(self.html_str, iframe_id, iframe_height)
        self.render_fullscreen_button(iframe_id)
        self.render_html_str(iframe_str)

    def generate_html(self, filename: str):
        self.generate_file('{}_xml.html'.format(filename), self.html_str)


class Graphviz(EmbeddedUI):  # https://www.graphviz.org/
    def __init__(self, graphviz_str: str):
        super().__init__()
        if graphviz_str is None or graphviz_str == '':
            raise ValueError('No value was passed to the graphviz_str parameter!')
        self.html_str = self.get_resource_template('graphviz.html').render(graphviz_str=graphviz_str.replace('\n', '').replace('\r\n', ''))

    def generate_notebook_iframe(self, iframe_height: int = 1000):
        iframe_str = self.get_iframe_str(self.html_str, iframe_height=iframe_height)
        self.render_html_str(iframe_str)

    def generate_html(self, filename: str):
        self.generate_file("{}_graphviz.html".format(filename), self.html_str)
