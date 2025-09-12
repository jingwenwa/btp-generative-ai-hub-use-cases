"""
This module represents the whole report builder.
A report can contain many pages, and each page can contain many items.
You can assemble different items into different pages.

The following classes are available:
    * :class:`ReportBuilder`
    * :class:`Page`
    * :class:`ChartItem`
    * :class:`TableItem`
    * :class:`DescriptionItem`
    * :class:`AlertItem`
    * :class:`LocalImageItem`
    * :class:`RemoteImageItem`
    * :class:`DigraphItem`
    * :class:`GraphvizItem`
    * :class:`ForcePlotItem`
    * :class:`ConnectionsItem`
"""

# pylint: disable=invalid-name
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
# pylint: disable=no-member
# pylint: disable=too-few-public-methods
# pylint: disable=super-init-not-called
# pylint: disable=attribute-defined-outside-init
from typing import List, Tuple, Union
import base64
from urllib.parse import quote
from hana_ml.visualizers.shared import EmbeddedUI
from hana_ml.visualizers.digraph import Digraph, MultiDigraph


class Item(object):
    def __init__(self) -> None:
        pass


class DescriptionItem(Item):
    """
    This item represents an description type, it contains multiple key and value values.

    Parameters
    ----------
    title : str
        The description item name.
    """
    def __init__(self, title: str):
        self.title = str(title)
        self.type = 'description'
        self.config = None

    def add(self, key: str, value: str):
        """
        Add a key-value pair.

        Parameters
        ----------
        key : str
            The key of description item.

        value : str
            The value of description item.
        """
        if key and value:
            if self.config is None:
                self.config = []
            self.config.append({
                'name': str(key),
                'value': str(value)
            })


class ChartItem(Item):
    """
    This item represents an chart type.

    Parameters
    ----------
    title : str
        The chart item name.

    config : dict
        The chart item config.

    width : int, optional
        The chart's width.

        Default to None.

    height : int, optional
        The chart's height.

        Default to None.
    """
    def __init__(self, title: str, config: dict, width: int = None, height: int = None):
        self.title = str(title)
        self.type = 'chart'
        self.config = None

        if config and isinstance(config, dict):
            self.config = config
        self.width = int(width) if width else None
        self.height = int(height) if height else None


class TableItem(Item):
    """
    This item represents an table type.

    Parameters
    ----------
    title : str
        The table item name.
    """
    def __init__(self, title: str):
        self.title = str(title)
        self.type = 'table'
        self.config = None

    def addColumn(self, name: str, data: List):
        """
        Add a dataset of single column.

        Parameters
        ----------
        name : str
            The column name of the single dataset.

        data : List
            The single dataset.
        """
        if name and data:
            if self.config is None:
                self.config = {
                    'columns': [],
                    'data': {}
                }
            self.config['columns'].append(name)
            self.config['data'][name] = data


class RemoteImageItem(Item):
    """
    This item represents an remote image type.

    Parameters
    ----------
    title : str
        The image item name.

    url : str
        The image address.

    width : int, optional
        The image width.

        Default to None.

    height : int, optional
        The image height.

        Default to None.
    """
    def __init__(self, title: str, url: str, width: int = None, height: int = None):
        self.title = str(title)
        self.type = 'image'
        self.config = None

        if url:
            self.config = {
                'url': str(url)
            }
        if width:
            self.config['width'] = int(width)
        if height:
            self.config['height'] = int(height)


class LocalImageItem(Item):
    """
    This item represents an local image type.

    Parameters
    ----------
    title : str
        The image item name.

    file_path : str
        The image file path.

    width : int, optional
        The image width.

        Default to None.

    height : int, optional
        The image height.

        Default to None.
    """
    def __init__(self, title: str, file_path: str, width: int = None, height: int = None):
        self.title = str(title)
        self.type = 'image'
        self.config = None

        file = open(file_path, 'rb')
        img_str = file.read()
        file.close()
        if img_str:
            img_base64_str = "data:{mime_type};base64,{image_data}".format(mime_type="image/png", image_data=quote(base64.b64encode(img_str)))
            self.config = {
                'content': img_base64_str
            }
        if width:
            self.config['width'] = int(width)
        if height:
            self.config['height'] = int(height)


class GraphvizItem(Item):
    def __init__(self, title: str, graphviz_str: str):
        self.title = str(title)
        self.type = 'graphviz'
        self.config = str(graphviz_str) if graphviz_str else None


class ForcePlotItem(Item):
    def __init__(self, title: str, config):
        self.title = str(title)
        self.type = 'sp.force-plot'
        self.config = config if config else None


class DigraphItem(Item):
    def __init__(self, title: str, digraph: Union[Digraph, MultiDigraph]):
        self.title = str(title)
        self.type = 'sp.digraph'
        self.config = None

        if digraph and isinstance(digraph, (Digraph, MultiDigraph)):
            self.config = digraph.embedded_unescape_html.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;").replace('\'', "&quot;")


class ConnectionsItem(Item):
    def __init__(self, title: str, connections_str: str):
        self.title = str(title)
        self.type = 'sp.connections'
        self.config = str(connections_str) if connections_str else None


class AlertItem(Item):
    """
    This item represents an alert type.
    There are four styles to describe message arrays: success, info, warning, error.

    Parameters
    ----------
    title : str
        The chart item name.
    """
    def __init__(self, title: str):
        self.title = str(title)
        self.type = 'alert'
        self.config = None

        self.success_msgs = []
        self.info_msgs = []
        self.warning_msgs = []
        self.error_msgs = []
        self.config = [
            {'type': 'success', 'msgs': self.success_msgs},
            {'type': 'info', 'msgs': self.info_msgs},
            {'type': 'warning', 'msgs': self.warning_msgs},
            {'type': 'error', 'msgs': self.error_msgs}
        ]

    def add_success_msg(self, msg: Union[str, List[str]]):
        """
        Add a successful message.

        Parameters
        ----------
        msg : str
            Message content.
        """
        if msg:
            if isinstance(msg, (list, tuple)):
                for m in msg:
                    self.success_msgs.append(str(m))
            else:
                self.success_msgs.append(str(msg))

    def add_info_msg(self, msg: Union[str, List[str]]):
        """
        Add a informational message.

        Parameters
        ----------
        msg : str
            Message content.
        """
        if msg:
            if isinstance(msg, (list, tuple)):
                for m in msg:
                    self.info_msgs.append(str(m))
            else:
                self.info_msgs.append(str(msg))

    def add_warning_msg(self, msg: Union[str, List[str]]):
        """
        Add a warning message.

        Parameters
        ----------
        msg : str
            Message content.
        """
        if msg:
            if isinstance(msg, (list, tuple)):
                for m in msg:
                    self.warning_msgs.append(str(m))
            else:
                self.warning_msgs.append(str(msg))

    def add_error_msg(self, msg: Union[str, List[str]]):
        """
        Add a error message.

        Parameters
        ----------
        msg : str
            Message content.
        """
        if msg:
            if isinstance(msg, (list, tuple)):
                for m in msg:
                    self.error_msgs.append(str(m))
            else:
                self.error_msgs.append(str(msg))


class Page(object):
    """
    Every report consists of many pages. Each page contains multiple items.

    Parameters
    ----------
    title : str
        The page name.
    """
    def __init__(self, title: str):
        self.title = str(title)
        self.items: List[Item] = []

    def addItem(self, item: Item):
        """
        Add a item instance to page instance.

        Parameters
        ----------
        item : Item
            Each page contains multiple items.
        """
        if item and isinstance(item, Item):
            self.items.append(item)

    def addItems(self, items: Union[Item, List[Item], Tuple[Item]]):
        """
        Add many item instances to page instance.

        Parameters
        ----------
        items : Item or List[Item] or Tuple[Item]
            Each page contains multiple items.
        """
        if items:
            if isinstance(items, (list, tuple)):
                for item in items:
                    self.addItem(item)
            elif isinstance(items, Item):
                self.addItem(items)


class ReportBuilder(object):
    """
    This class is a report builder and the base class for report building. Can be inherited by custom report builder classes.

    Parameters
    ----------
    title : str
        The report name.
    """
    def __init__(self, title: str):
        self.title = str(title)
        self.pages: List[Page] = []
        self.html_str = None

    def addPage(self, page: Page):
        """
        Add a page instance to report instance.

        Parameters
        ----------
        page : Page
            Every report consists of many pages.
        """
        if page and isinstance(page, Page):
            self.pages.append(page)

    def addPages(self, pages: Union[Page, List[Page], Tuple[Page]]):
        """
        Add many page instances to report instance.

        Parameters
        ----------
        pages : Page or List[Page] or Tuple[Page]
            Every report consists of many pages.
        """
        if pages:
            if isinstance(pages, (list, tuple)):
                for page in pages:
                    self.addPage(page)
            elif isinstance(pages, Page):
                self.addPage(pages)

    def build(self, debug: bool = False):
        """
        Build HTML string based on current config.

        Parameters
        ----------
        debug : bool
            Whether the log should be printed to the browser console.

            Defaults to False.
        """
        debug = 'true' if debug else 'false'

        page_list = []
        for page in self.pages:
            item_list = []
            for item in page.items:
                if item.title and item.config:
                    item_data = {
                        'title': item.title,
                        'type': item.type,
                        'config': item.config
                    }
                    if item.type == 'chart':
                        if item.width:
                            item_data['width'] = item.width
                        if item.height:
                            item_data['height'] = item.height
                    item_list.append(item_data)
            page_list.append({
                'title': page.title,
                'items': item_list
            })

        self.html_str = EmbeddedUI.get_resource_template('report_builder.html').render(debug=debug, reportConfig={
            'title': self.title,
            'pages': page_list
        })

    def generate_html(self, filename):
        """
        Save the report as a html file.

        Parameters
        ----------
        filename : str
            HTML file name.
        """
        if self.html_str is None:
            self.build()
        EmbeddedUI.generate_file('{}.html'.format(filename), self.html_str)

    def generate_notebook_iframe(self, iframe_height: int = 600):
        """
        Render the report as a notebook iframe.

        Parameters
        ----------
        iframe_height : int
            iframe height.

            Defaults to 600.
        """
        if self.html_str is None:
            self.build()
        iframe_str = EmbeddedUI.get_iframe_str(self.html_str, iframe_height=iframe_height)
        EmbeddedUI.render_html_str(iframe_str)
