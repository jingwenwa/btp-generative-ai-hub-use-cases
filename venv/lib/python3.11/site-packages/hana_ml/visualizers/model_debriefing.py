"""
This module represents a visualizer for tree model.

The following class is available:

    * :class:`TreeModelDebriefing`
"""

# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
# pylint: disable=protected-access
import logging
import uuid
import html
from typing import Union
import pydotplus
from hdbcli import dbapi
from hana_ml import dataframe
from hana_ml.ml_base import try_drop
from hana_ml.algorithms.pal.pal_base import (
    ParameterTable,
    call_pal_auto_with_hint
)
from hana_ml.visualizers.shared import JSONViewer, XMLViewer
from hana_ml.visualizers.digraph import DigraphConfig, Digraph, MultiDigraph, Node
from hana_ml.visualizers.shap import ShapleyExplainer
from hana_ml.visualizers.shared import EmbeddedUI


logger = logging.getLogger(__name__)


class TreeModelAnalyzer(object):
    def __init__(self):
        self.model_dict = None
        self.dot_model_dict = None
        self.text_model_dict = None
        self.viewer: Union[XMLViewer, JSONViewer] = None
        self.digraph: Union[Digraph, MultiDigraph] = None

    @staticmethod
    def get_instance(model: dataframe.DataFrame):
        if isinstance(model, dataframe.DataFrame) is False:
            raise TypeError("The type of parameter 'model' must be hana_ml.dataframe.DataFrame!")

        tree_model_analyzer = model.__dict__.get('tree_model_analyzer')
        if tree_model_analyzer is None:
            tree_model_analyzer = TreeModelAnalyzer()
            model.__dict__['tree_model_analyzer'] = tree_model_analyzer
        return tree_model_analyzer

    @staticmethod
    def get_analyzable_model(model: dataframe.DataFrame):
        if model.__dict__.get('_is_uni_dt'):
            model = model.deselect("PART_INDEX")
        if model.hasna(model.columns[1]):
            model = model.deselect(model.columns[1])
        return model

    @staticmethod
    def get_model_dict(model):
        model = TreeModelAnalyzer.get_analyzable_model(model)
        temp_model_dict = {}
        if len(model.columns) == 3:  # multiple trees  |ROW_INDEX|TREE_INDEX|MODEL_CONTENT|
            for tree_index, single_tree_list in model.sort(model.columns[0]).collect().groupby(model.columns[1])[model.columns[2]]:
                temp_model_dict[str(tree_index)] = "".join(single_tree_list)
        else:  # single tree  |ROW_INDEX|MODEL_CONTENT|
            temp_model_dict['0'] = "".join(model.sort(model.columns[0]).collect()[model.columns[1]])
        return temp_model_dict

    def parse_model(self, model):
        if self.model_dict is None:
            self.model_dict = TreeModelAnalyzer.get_model_dict(model)
            if self.model_dict[list(self.model_dict.keys())[0]].endswith('</PMML>'):
                self.viewer = XMLViewer(self.model_dict)
            else:
                self.viewer = JSONViewer(self.model_dict)

    def parse_dot_model(self, model: dataframe.DataFrame):
        if self.dot_model_dict is None:
            dot_tbl_name = '#PAL_DT_DOT_TBL_{}'.format(str(uuid.uuid1()).replace('-', '_').upper())
            tables = [dot_tbl_name]
            try:
                call_pal_auto_with_hint(model.connection_context,
                                        None,
                                        'PAL_VISUALIZE_MODEL',
                                        model,
                                        ParameterTable().with_data([]),
                                        *tables)
                model = TreeModelAnalyzer.get_analyzable_model(model.connection_context.table(dot_tbl_name))
                self.dot_model_dict = TreeModelAnalyzer.get_model_dict(model)
                self.text_model_dict = {}
                tree_keys = list(self.dot_model_dict.keys())
                if len(tree_keys) > 1:
                    digraph = MultiDigraph('Tree Model')
                    for tree_key in tree_keys:
                        child_digraph: MultiDigraph.ChildDigraph = digraph.add_child_digraph('Tree_'+str(tree_key))
                        self.configure_digraph(child_digraph, self.dot_model_dict[tree_key])
                else:
                    digraph = Digraph('Tree Model')
                    self.configure_digraph(digraph, self.dot_model_dict[tree_keys[0]])
                self.digraph = digraph
            except dbapi.Error as db_err:
                logger.exception(str(db_err))
                try_drop(model.connection_context, dot_tbl_name)
                raise
            except Exception as db_err:
                logger.exception(str(db_err))
                try_drop(model.connection_context, dot_tbl_name)
                raise

    def configure_digraph(self, digraph: Union[Digraph, MultiDigraph.ChildDigraph], dot_data: str) -> None:
        id_2_node_dict = {}
        id_2_label_dict = {}
        out_ids = []
        in_ids = []
        left_id_2_right_ids = {}
        edges = []

        dot_graph = pydotplus.graph_from_dot_data(dot_data.encode('utf8'))

        for edge in dot_graph.get_edges():
            left_node_id = edge.get_source()
            right_node_id = edge.get_destination()
            if left_id_2_right_ids.get(left_node_id) is None:
                left_id_2_right_ids[left_node_id] = []
            left_id_2_right_ids[left_node_id].append(right_node_id)
            out_ids.append(left_node_id)
            in_ids.append(right_node_id)
            edges.append([left_node_id, right_node_id])

        for node in dot_graph.get_nodes():
            node_id = node.get_name()
            node_label = node.get_label()
            if node_label:
                node_label = html.unescape(node_label[1:-1])
                id_2_label_dict[node_id] = node_label
                # node only has 2 ports: 1 in and 1 out
                id_2_node_dict[node_id] = digraph.add_model_node(
                    node_label.replace('<br/>', '\n'),
                    '',
                    ['in'] if node_id in in_ids else [],
                    ['out'] if node_id in out_ids else [])

        for left_node_id, right_node_id in edges:
            left_node: Node = id_2_node_dict.get(left_node_id)
            right_node: Node = id_2_node_dict.get(right_node_id)
            digraph.add_edge(left_node.out_ports[0], right_node.in_ports[0])

        spacing = 3
        head_node_id = None
        self.text_model_dict[digraph.name] = ""
        for out_id in out_ids:
            if out_id in in_ids:
                pass
            else:
                head_node_id = out_id
                break

        def generate_text_model(node_id, depth, text_model_key):
            indent = ("|" + (" " * spacing)) * depth
            indent = indent[:-spacing] + "-" * spacing
            self.text_model_dict[digraph.name] = self.text_model_dict[digraph.name] + "{}{}\n".format(indent, ' '.join(id_2_label_dict[node_id].split('<br/>')))
            sub_node_ids = left_id_2_right_ids.get(node_id)
            if sub_node_ids:
                for sub_node_id in sub_node_ids:
                    generate_text_model(sub_node_id, depth + 1, text_model_key)

        generate_text_model(head_node_id, 1, digraph.name)


class TreeModelDebriefing(object):
    r"""
    Visualize tree model.

    Currently, the TreeModelDebriefing class can be used to parse tree model built with the PAL algorithm,
    but it cannot be used to parse tree model built with the APL algorithm.

    The TreeModelDebriefing class can be used to parse tree model generated by the following classes:

    - Classes in `hana_ml.algorithms.pal.trees` module

        - RDTClassifier
        - RDTRegressor
        - RandomForestClassifier
        - RandomForestRegressor
        - DecisionTreeClassifier
        - DecisionTreeRegressor
        - HybridGradientBoostingClassifier
        - HybridGradientBoostingRegressor

    - Class UnifiedClassification

        Supported the following values of parameter ``func``:

            - RandomDecisionTree
            - DecisionTree
            - HybridGradientBoostingTrees

    Examples
    --------

    1. Using RDTClassifier class

    Input DataFrame:

    >>> df.collect()
          OUTLOOK     TEMP  HUMIDITY WINDY        LABEL
     0      Sunny     75.0      70.0   Yes         Play
     1      Sunny     80.0      90.0   Yes  Do not Play
    ...
    12       Rain     68.0      80.0    No         Play
    13       Rain     70.0      96.0    No         Play

    Creating RDTClassifier instance:

    >>> from hana_ml.algorithms.pal.trees import RDTClassifier
    >>> rdtc = RDTClassifier(n_estimators=3,
    ...                      max_features=3,
    ...                      random_state=2,
    ...                      split_threshold=0.00001,
    ...                      calculate_oob=True,
    ...                      min_samples_leaf=1,
    ...                      thread_ratio=1.0)

    Performing fit():

    >>> rdtc.fit(data=df, features=['OUTLOOK', 'TEMP', 'HUMIDITY', 'WINDY'], label='CLASS')

    Visualize tree model in JSON format:

    >>> TreeModelDebriefing.tree_debrief(rdtc.model_)

    .. image:: image/rdtc01.png

    Visualize tree model in DOT format:

    >>> TreeModelDebriefing.tree_debrief_with_dot(rdtc.model_, iframe_height=500)

    .. image:: image/rdtc02.png

    Visualize tree model in XML format:

    >>> rdtc = RDTClassifier(n_estimators=3,
    ...                      max_features=3,
    ...                      random_state=2,
    ...                      split_threshold=0.00001,
    ...                      calculate_oob=True,
    ...                      min_samples_leaf=1,
    ...                      thread_ratio=1.0,
    ...                      model_format='pmml')

    >>> rdtc.fit(data=df, features=['OUTLOOK', 'TEMP', 'HUMIDITY', 'WINDY'], label='CLASS')

    >>> TreeModelDebriefing.tree_debrief(rdtc.model_)

    .. image:: image/rdtc03pmml.png

    2. Using UnifiedClassification class

    >>> from hana_ml.algorithms.pal.unified_classification import UnifiedClassification

    >>> rdt_params = dict(random_state=2,
                          split_threshold=1e-7,
                          min_samples_leaf=1,
                          n_estimators=10,
                          max_depth=55)

    >>> uc_rdt = UnifiedClassification(func='RandomDecisionTree', **rdt_params)

    >>> uc_rdt.fit(data=df,
                   partition_method='stratified',
                   stratified_column='CLASS',
                   partition_random_state=2,
                   training_percent=0.7,
                   ntiles=2)

    >>> TreeModelDebriefing.tree_debrief(uc_rdt.model_[0])

    >>> TreeModelDebriefing.tree_debrief_with_dot(uc_rdt.model_[0], iframe_height=500)
    """
    def __init__(self):
        pass

    @staticmethod
    def tree_debrief(model, display=True):
        """
        Visualize tree model by data in JSON or XML format.

        Parameters
        ----------

        model : DataFrame
            Tree model.
        display : bool, optional
            Whether to display the tree model.

            Defaults to True.

        Returns
        -------

        HTML Page

            This HTML page can be rendered by browser.
        """
        tree_model_analyzer: TreeModelAnalyzer = TreeModelAnalyzer.get_instance(model)
        tree_model_analyzer.parse_model(model)
        if display:
            tree_model_analyzer.viewer.generate_notebook_iframe()

    @staticmethod
    def tree_parse(model: dataframe.DataFrame):
        """
        Transform tree model content using DOT language.

        Parameters
        ----------

        model : DataFrame
            Tree model.
        """
        tree_model_analyzer: TreeModelAnalyzer = TreeModelAnalyzer.get_instance(model)
        tree_model_analyzer.parse_dot_model(model)

    @staticmethod
    def tree_debrief_with_dot(model, iframe_height: int = 800, digraph_config: DigraphConfig = None, display=True):
        """
        Visualize tree model by data in DOT format.

        Parameters
        ----------
        model : DataFrame
            Tree model.
        iframe_height : int, optional
            Frame height.

            Defaults to 800.
        digraph_config : DigraphConfig, optional
            Configuration instance of digraph.

        display : bool, optional
            Whether to display the tree model.

            Defaults to True.

        Returns
        -------
        HTML Page
            This HTML page can be rendered by browser.
        """
        tree_model_analyzer: TreeModelAnalyzer = TreeModelAnalyzer.get_instance(model)
        tree_model_analyzer.parse_dot_model(model)
        if digraph_config is None:
            digraph_config = DigraphConfig()
            digraph_config.set_digraph_layout('vertical')
        tree_model_analyzer.digraph.build(digraph_config)
        if display:
            tree_model_analyzer.digraph.generate_notebook_iframe(iframe_height)

    @staticmethod
    def tree_debrief_with_text(model):
        """
        Visualize tree model by data in text format.

        Parameters
        ----------
        model : DataFrame
            Tree model.

        Returns
        -------
        Text
            Print text to the console.
        """
        tree_model_analyzer: TreeModelAnalyzer = TreeModelAnalyzer.get_instance(model)
        tree_model_analyzer.parse_dot_model(model)
        for k in tree_model_analyzer.text_model_dict.keys():
            print("{}:\n{}".format(k, tree_model_analyzer.text_model_dict[k]))

    @staticmethod
    def tree_export(model, filename):
        """
        Save the tree model as a html file.

        Parameters
        ----------
        model : DataFrame
            Tree model.

        filename : str
            Html file name.
        """
        tree_model_analyzer: TreeModelAnalyzer = TreeModelAnalyzer.get_instance(model)
        tree_model_analyzer.parse_model(model)
        tree_model_analyzer.viewer.generate_html(filename)

    @staticmethod
    def tree_export_with_dot(model, filename):
        """
        Save the tree model as a html file.

        Parameters
        ----------
        model : DataFrame
            Tree model.

        filename : str
            Html file name.
        """
        tree_model_analyzer: TreeModelAnalyzer = TreeModelAnalyzer.get_instance(model)
        tree_model_analyzer.parse_dot_model(model)
        tree_model_analyzer.digraph.generate_html(filename)

    @staticmethod
    def tree_export_with_text(model, filename):
        """
        Save the tree model as a text file.

        Parameters
        ----------
        model : DataFrame
            Tree model.

        filename : str
            Html file name.
        """
        tree_model_analyzer: TreeModelAnalyzer = TreeModelAnalyzer.get_instance(model)
        tree_model_analyzer.parse_dot_model(model)
        output_text = ""
        for k in tree_model_analyzer.text_model_dict.keys():
            output_text = output_text + "{}:\n{}\n".format(k, tree_model_analyzer.text_model_dict[k])
        EmbeddedUI.generate_file('{}.txt'.format(filename), output_text)

    @staticmethod
    def shapley_explainer(reason_code_data: dataframe.DataFrame, feature_data: dataframe.DataFrame, reason_code_column_name=None, **kwargs):
        """
        Create Shapley explainer to explain the output of machine learning model. \n
        It connects optimal credit allocation with local explanations using the classic Shapley values from game theory and their related extensions. \n
        To get an overview of which features are most important for a model we can plot the Shapley values of every feature for every sample.

        Parameters
        ----------
        reason_code_data : DataFrame
            The Dataframe containing only reason code values.

        feature_data : DataFrame
            The Dataframe containing only feature values.

        reason_code_column_name : str, optional
            The name of reason code column.

            Defaults to None.

        Returns
        -------
        :class:`~hana_ml.visualizers.shap.ShapleyExplainer`
            Shapley explainer.
        """
        return ShapleyExplainer(reason_code_data, feature_data, reason_code_column_name, **kwargs)
