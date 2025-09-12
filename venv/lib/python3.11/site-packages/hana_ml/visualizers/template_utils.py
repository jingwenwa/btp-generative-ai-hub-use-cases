# pylint: disable=too-many-lines
# pylint: disable=line-too-long
# pylint: disable=too-many-locals
# pylint: disable=too-many-arguments
# pylint: disable=missing-docstring
# pylint: disable=consider-using-enumerate
# pylint: disable=too-many-instance-attributes
# pylint: disable=no-member
# pylint: disable=too-many-branches
# pylint: disable=invalid-name
# pylint: disable=protected-access
# pylint: disable=broad-except
# pylint: disable=consider-using-f-string
# pylint: disable=too-few-public-methods
# pylint: disable=duplicate-string-formatting-argument
import logging
import html
from threading import Lock
try:
    from jinja2 import Environment, PackageLoader
except BaseException as error:
    logging.getLogger(__name__).error("%s: %s", error.__class__.__name__, str(error))
    pass
from hana_ml import dataframe


class TemplateUtil(object):
    __ENV = None
    try:
        __ENV = Environment(loader=PackageLoader('hana_ml.visualizers', 'templates'))
    except BaseException as error:
        logging.getLogger(__name__).error("%s: %s", error.__class__.__name__, str(error))
        pass
    __SECTION_METADATA = {
        'container': '<div class="section">{}</div>',
        'name': '<h3 class="text-left section_name">{}</h3>',
        'content': '<div class="section_content">{}</div>',
        'content_style': '<div class="section_content" style="text-align:center">{}</div>'
    }

    __TAB_METADATA = {
        'id': 1,
        'lock': Lock(),
        # {nav_id} {nav_items}
        'nav': '<ul id="{}" class="nav nav-tabs" role="tablist">{}</ul>',
        # {nav_item_id} {nav_item_title}
        'nav_active_item': '<li class="nav-item"><a class="nav-link active" href="#{}" role="tab" data-toggle="tab">{}</a></li>',
        'nav_item': '<li class="nav-item"><a class="nav-link" href="#{}" role="tab" data-toggle="tab">{}</a></li>',
        # {pane_id} {pane_items}
        'pane': '<div id="{}" class="tab-content">{}</div>',
        # {pane_item_id} {pane_item_content}
        'pane_active_item': '<div class="tab-pane fade show active" id="{}">{}</div>',
        'pane_item': '<div class="tab-pane fade" id="{}">{}</div>'
    }

    __TABLE_METADATA = {
        'container': '<table class="table table-bordered table-hover">{}</table>',
        'head_container': '<thead>{}</thead>',
        'body_container': '<tbody>{}</tbody>',
        'row_container': '<tr>{}</tr>',
        'head_column': '<th>{}</th>',
        'body_column': '<td>{}</td>'
    }

    __ECHART_METADATA = {
        'id': 1,
        'id_prefix': 'echarts',
        'container': '<div id="{}" style="height:500px;margin-top:10px"></div>',
        'lock': Lock()
    }

    @staticmethod
    def generate_echart(chart_id):
        return TemplateUtil.__ECHART_METADATA['container'].format(chart_id)

    @staticmethod
    def construct_tab_item_data(title, content):
        return {
            'title': title,
            'content': content
        }

    @staticmethod
    def get_echart_id():
        lock = TemplateUtil.__ECHART_METADATA['lock']
        lock.acquire()

        echart_id = TemplateUtil.__ECHART_METADATA['id']
        TemplateUtil.__ECHART_METADATA['id'] = echart_id + 1

        lock.release()

        return '{}_chart_{}'.format(
            TemplateUtil.__ECHART_METADATA['id_prefix'], echart_id)

    @staticmethod
    def get_tab_id():
        lock = TemplateUtil.__TAB_METADATA['lock']
        lock.acquire()

        tab_id = TemplateUtil.__TAB_METADATA['id']
        TemplateUtil.__TAB_METADATA['id'] = tab_id + 1

        lock.release()

        return tab_id

    @staticmethod
    def generate_tab(data):
        # data = [{'title': '','content': ''},{...}]
        element_id = TemplateUtil.get_tab_id()
        nav_id = 'nav_{}'.format(element_id)
        pane_id = 'pane_{}'.format(element_id)
        nav_html = ''
        pane_html = ''
        for i in range(0, len(data)):
            pane = data[i]
            pane_item_id = '{}_{}'.format(pane_id, i)
            if i == 0:
                nav_html = nav_html + \
                    TemplateUtil.__TAB_METADATA['nav_active_item'].format(pane_item_id, pane['title'])
                pane_html = pane_html + \
                    TemplateUtil.__TAB_METADATA['pane_active_item'].format(pane_item_id, pane['content'])
            else:
                nav_html = nav_html + \
                    TemplateUtil.__TAB_METADATA['nav_item'].format(pane_item_id, pane['title'])
                pane_html = pane_html + \
                    TemplateUtil.__TAB_METADATA['pane_item'].format(pane_item_id, pane['content'])

        nav_html = TemplateUtil.__TAB_METADATA['nav'].format(nav_id, nav_html)
        pane_html = TemplateUtil.__TAB_METADATA['pane'].format(pane_id, pane_html)

        tab_html = nav_html + pane_html

        return tab_html, nav_id

    @staticmethod
    def generate_table_html(data: dataframe.DataFrame, column_names=None, table_name=None):
        column_data = []

        for column in data.columns:
            column_data.append(list(data.collect()[column]))

        formatted_data = []
        for i in range(0, data.count()):
            row_data = []
            for j in range(0, len(data.columns)):
                origin_data = column_data[j][i]
                if isinstance(origin_data, str):
                    row_data.append(html.escape(origin_data))
                else:
                    row_data.append(origin_data)
            formatted_data.append(row_data)

        if column_names:
            return TemplateUtil.generate_table(column_names, formatted_data, table_name)
        else:
            return TemplateUtil.generate_table(data.columns, formatted_data, table_name)

    @staticmethod
    def generate_table(columns, data, table_name=None):
        columns_html = ''
        for column in columns:
            columns_html += TemplateUtil.__TABLE_METADATA['head_column'].format(column)
        row_html = TemplateUtil.__TABLE_METADATA['row_container'].format(columns_html)
        head_html = TemplateUtil.__TABLE_METADATA['head_container'].format(row_html)
        if table_name:
            head_html = "<title>{}</title>".format(table_name) + head_html
        rows_html = ''
        for row_data in data:
            columns_html = ''
            for column_data in row_data:
                columns_html += TemplateUtil.__TABLE_METADATA['body_column'].format(column_data)
            rows_html += TemplateUtil.__TABLE_METADATA['row_container'].format(columns_html)
        body_html = TemplateUtil.__TABLE_METADATA['body_container'].format(rows_html)

        return TemplateUtil.__TABLE_METADATA['container'].format(head_html + body_html)

    @staticmethod
    def get_template(template_name):
        return TemplateUtil.__ENV.get_template(template_name)

    @staticmethod
    def generate_html_file(filename, content):
        file = open(filename, 'w', encoding="utf-8")
        file.write(content)
        file.close()

    @staticmethod
    def get_notebook_iframe(src_html):
        iframe = """
            <iframe
                width="{width}"
                height="{height}"
                srcdoc="{src}"
                frameborder="0"
                allowfullscreen>
            </iframe>
        """.format(
            width='100%',
            height='800px',
            src=html.escape(src_html),
        )
        return iframe
