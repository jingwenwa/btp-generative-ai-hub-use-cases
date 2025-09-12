#pylint: disable=line-too-long, fixme, protected-access, too-few-public-methods, too-many-arguments
#pylint: disable=too-many-locals, too-many-branches, too-many-statements, no-self-use, useless-object-inheritance
#pylint: disable=consider-using-f-string, superfluous-parens, too-many-instance-attributes, raising-bad-type
#pylint: disable=unbalanced-tuple-unpacking, bare-except，invalid-name, too-many-nested-blocks
"""
This module handles generation of all HANA design-time artifacts based on the provided
base and consumption layer elements. These artifacts can incorporate into development projects in SAP Web IDE for SAP HANA or SAP Business Application Studio and be deployed via HANA Deployment Infrastructure (HDI) into a SAP HANA system.

The following class is available:

    * :class:`HANAGeneratorForCAP`
    * :class:`HanaGenerator`

"""

import json
import os
import re
import shutil
import uuid

from hana_ml.artifacts.generators.filewriter.hana import MTAYamlWriter
from hana_ml.artifacts.generators.filewriter.hana import HDBSynonymWriter
from hana_ml.artifacts.generators.filewriter.hana import HDBGrantWriter
from hana_ml.artifacts.generators.filewriter.hana import HDBRoleWriter
from hana_ml.artifacts.generators.filewriter.hana import HDBProcedureWriter
from hana_ml.artifacts.generators.filewriter.hana import HDBVirtualTableWriter
from hana_ml.artifacts.generators.filewriter.hana import HDBCDSWriter

from hana_ml.artifacts.config import ConfigConstants, ConfigHandler
from hana_ml.artifacts.utils import DirectoryHandler, cds_convert
from hana_ml.artifacts.utils import StringUtils

from hana_ml.artifacts.generators.sql_processor import SqlProcessor
from hana_ml.dataframe import DataFrame, quotename
from hana_ml.ml_exceptions import FitIncompleteError

def _insert_string_between_braces(input_string, insert_string):
    start = input_string.find('{') + 1
    end = input_string.find('}')
    if start == 0 or end == -1:
        return input_string  # No braces found
    return input_string[:start] + insert_string + input_string[start:]

def _remove_category_srv_content(srv_content, categories):
    lines = srv_content.strip().split('\n')
    projection_lines = []
    for category in categories:
        projection_lines += [line for line in lines if f"as projection on hanaml.{category}." in line]
    filtered_lines = [line for line in lines if line not in projection_lines]
    return '\n'.join(filtered_lines)

def _find_single_content(cds_content, scenario):
    start = cds_content.find(f"\ncontext {scenario} ")
    end = None
    count = 0
    find_first_bracket = False
    for i, char in enumerate(cds_content[start:], start=start):
        if char.strip() == "{":
            find_first_bracket = True
            count += 1
        if char.strip() == "}":
            count -= 1
        if count == 0:
            end = i
            if find_first_bracket:
                break
    return cds_content[start:end+1]

def _find_object_schemas(select_statement):
    results = re.findall(r"(?i)from ([\w\"]+)\.", select_statement)
    results += re.findall(r"(?i)join ([\w\"]+)\.", select_statement)
    for res in results:
        try:
            if res[0] == '"' and res[-1] != '"':
                results.remove(res)
        except:
            pass
    for idx, res in enumerate(results):
        results[idx] = res.replace('"', "")
    return results

def _create_synonyms(select_statement, namespace):
    results = re.findall(r"(?i)from ([\w\"]+\.[\w\"]+)", select_statement)
    results += re.findall(r"(?i)join ([\w\"]+\.[\w\"]+)", select_statement)
    for res in results:
        try:
            if res[0] == '"' and res[-1] == '"':
                if res.split(".", 1)[0][-1] != '"':
                    results.remove(res)
        except:
            pass
    m_select_statement = select_statement
    synonyms = {}
    for res in results:
        schema, table = res.split(".", 1)
        m_select_statement = m_select_statement.replace(res, namespace.replace(".", "_").upper() + '_' + table.replace('"', ""))
        schema = schema.replace('"', "")
        table = table.replace('"', "")
        synonyms[namespace.replace(".", "_").upper() + '_' + table] = {
            "target" : {
                "object" : table,
                "schema" : schema
            }
        }
    return m_select_statement, synonyms

class HANAGeneratorForCAP(object):
    """
    HANA artifacts generator for the existing CAP project.

    Parameters
    ----------
    project_name : str
        The name of project.

    outputdir : str
        The directory of output.

    namespace : str, optional
        Specifies the namespace for the project.

        Defaults to "hana.ml".

    Examples
    --------
    >>> my_pipeline = Pipeline([
                        ('PCA', PCA(scaling=True, scores=True)),
                        ('HGBT_Classifier', HybridGradientBoostingClassifier(
                                                n_estimators=4, split_threshold=0,
                                                learning_rate=0.5, fold_num=5,
                                                max_depth=6))])
    >>> my_pipeline.fit(diabetes_train, key="ID", label="CLASS")
    >>> my_pipeline.predict(diabetes_test_m, key="ID")
    >>> hanagen = HANAGeneratorForCAP(project_name="my_proj",
                                      output_dir=".",
                                      namespace="hana.ml")
    >>> hanagen.generate_artifacts(my_pipeline)
    """
    def __init__(self, project_name, output_dir, namespace=None):
        if namespace is None:
            namespace = "hana.ml"
        self.project_name = project_name
        self.output_dir = output_dir
        self.namespace = namespace
        self._hdbtables = {}
        self._hdbviews = {}
        self._fit_context = "Fit"
        self._predict_context = "Predict"
        self._score_context = "Score"
        self._cdses = {self._fit_context: [], self._predict_context: [], self._score_context: []}
        self._synonyms = {}
        self._grants = {
            "ServiceName_1":{
                "object_owner":{
                    "global_roles" : [
                    ],
                    "schema_privileges": [
                    ]
                },
                "application_user":{
                    "global_roles" : [
                    ],
                    "schema_privileges": [
                    ]
                }
            }
        }
        self._base_fit_proc = None
        self._cons_fit_proc = None
        self._in_fit_tables = []
        self._out_fit_tables = []
        self._call_base_fit_proc = None
        self._base_fit_proc_name = None
        self._cons_fit_proc_name = None
        self._cons_fit_proc_header = None
        self._base_fit_proc_filename = None
        self._cons_fit_proc_filename = None
        self._base_predict_proc = None
        self._cons_predict_proc = None
        self._in_predict_tables = []
        self._out_predict_tables = []
        self._call_base_predict_proc = None
        self._base_predict_proc_name = None
        self._cons_predict_proc_name = None
        self._cons_predict_proc_header = None
        self._base_predict_proc_filename = None
        self._cons_predict_proc_filename = None

        self._in_score_tables = []
        self._out_score_tables = []
        self._call_base_score_proc = None
        self._base_score_proc_name = None
        self._cons_score_proc_name = None
        self._cons_score_proc_header = None
        self._base_score_proc_filename = None
        self._cons_score_proc_filename = None

        self._apply_func_name = None
        self._apply_func_filename = None
        self._apply_hdbfunction = None
        self._model_name = None
        self._model_filename = None
        self._model_cds_entity = None
        self._consume_tudf = []

        self._service_projection = []
        self._service_projection_category = set()
        self._materialize_ds_data = False
        self._tudf = False

    def materialize_ds_data(self, to_materialize=True):
        """
        Create input table for the input DataFrame.

        Parameters
        ----------
        to_materialize : bool, optional
            If True, the input DataFrame will be materialized.

            Defaults to True.
        """
        self._materialize_ds_data = to_materialize

    def _is_APL(self, obj):
        if obj.__module__.startswith('hana_ml.algorithms.apl'):
            return True
        return False

    def _clean_up(self):
        self._in_fit_tables = []
        self._out_fit_tables = []
        self._in_predict_tables = []
        self._out_predict_tables = []
        self._in_score_tables = []
        self._out_score_tables = []
        self._hdbtables = {}
        self._hdbviews = {}
        self._cdses = {self._fit_context: [], self._predict_context: [], self._score_context: []}
        self._synonyms = {}
        self._grants = {
            "ServiceName_1":{
                "object_owner":{
                    "global_roles" : [
                    ],
                    "schema_privileges": [
                    ]
                },
                "application_user":{
                    "global_roles" : [
                    ],
                    "schema_privileges": [
                    ]
                }
            }
        }
        self._consume_tudf = []
        self._service_projection = []
        self._service_projection_category = set()

    def _generate_base_fit_procedure(self, obj, cds_gen=False, model_position=None):
        input_block = []
        output_block = []
        if obj._fit_call is None:
            raise FitIncompleteError("Please run fit function first!")
        afl_func = re.findall(r"_SYS_AFL.(.+)\(", obj._fit_call)
        proc_name = "{}".format(afl_func[0].lower())
        self._base_fit_proc_name = self.namespace.replace(".", "_") + "_base_" + proc_name
        self._call_base_fit_proc = "CALL {}(".format(self._base_fit_proc_name)
        self._cons_fit_proc_name = self.namespace.replace(".", "_") + "_cons_" + proc_name
        self._model_name = "{}_FIT_MODEL{}".format(self.namespace.replace(".", "_").upper(), self._cons_fit_proc_name.upper().replace("_", ""))
        self._model_filename = "{}-model-{}".format(self.namespace.replace(".", "-").lower(), self._cons_fit_proc_name.lower().replace("_", "-"))
        self._model_cds_entity = "Model{}".format(self._cons_fit_proc_name.replace("_", " ").title().replace(" ", ""))
        self._base_fit_proc_filename = self._base_fit_proc_name.replace("_", "-") + ".hdbprocedure"
        self._cons_fit_proc_filename = self._cons_fit_proc_name.replace("_", "-") + ".hdbprocedure"
        entity_prefix = proc_name.replace("_"," ").title().replace(" ", "")
        inputs = []
        outputs = []
        outputs_cons = []
        count = 0
        conn = None
        object_schemas = []
        current_schema = None
        annotation = None
        if cds_gen:
            annotation = ['@cds.persistence.exists']
        for inp in obj._fit_args:
            if isinstance(inp, DataFrame):
                conn = inp.connection_context
                if current_schema is None:
                    current_schema = conn.get_current_schema()
                    object_schemas += [current_schema]
                object_schemas += _find_object_schemas(inp.select_statement)
                m_select_statement, synonyms = _create_synonyms(inp.select_statement, self.namespace)
                self._synonyms.update(synonyms)
                input_tt = []
                for key, val in inp.get_table_structure().items():
                    input_tt.append("{} {}".format(quotename(key), val))
                context = self._fit_context
                entity="Input{}{}".format(count, entity_prefix)
                in_name = "{}_{}_{}".format(self.namespace.replace(".", "_").upper(), context.replace(".", "_").upper(), entity.replace(".", "_").upper())
                in_var = "in_{}_{}".format(count, self._base_fit_proc_name)
                in_cons_var = "in_{}_{}".format(count, self._cons_fit_proc_name)
                hdbtable, hdbview, cds = cds_convert.create_cds_artifacts(data=inp, namespace=self.namespace, context=context, entity=entity, annotation=annotation, hdbtable_name="{}::{}.{}".format(self.namespace, context.capitalize(), entity.capitalize()))
                if self._materialize_ds_data:
                    in_cons_name = in_name
                    self._service_projection += ["    @readonly entity {1} as projection on hanaml.{0}.{1};".format(context, entity)]
                    self._service_projection_category.add(context)
                    self._cdses[context].append(cds)
                    self._hdbtables[self.namespace.replace(".", "-") + '-' + in_cons_var.replace("_", "-") + ".hdbtable"] = hdbtable
                    self._hdbviews[self.namespace.replace(".", "-") + '-' + in_cons_var.replace("_", "-") + "-mapping.hdbview"] = hdbview
                    m_select_statement = "SELECT * FROM {}".format(in_cons_name)
                if "#" in m_select_statement:
                    raise ValueError("Temporary table is not supported!" + m_select_statement)
                self._in_fit_tables.append("{} = {};".format(in_cons_var, m_select_statement))
                inputs.append("in {} TABLE({})".format(in_var,
                                                       ", ".join(input_tt)))
                self._call_base_fit_proc += ":" + in_cons_var + ", "
                input_block.append("in_{} = SELECT * FROM :{};".format(count, in_var))
                count = count + 1
        count = 0
        for idx, output in enumerate(obj._fit_output_table_names):
            output_tt = []
            if output.upper().startswith("SELECT "):
                output_df = conn.sql(output)
            else:
                output_df = conn.table(output)
            if hasattr(obj, 'fit_output_signature'):
                if obj.fit_output_signature is not None:
                    output_df._columns = list(obj.fit_output_signature[idx].keys())
                    output_df._mock_table_structure = obj.fit_output_signature[idx]
            for key, val in output_df.get_table_structure().items():
                output_tt.append("{} {}".format(quotename(key), val))
            context = self._fit_context
            entity="Output{}{}".format(count, entity_prefix)
            out_name = "{}_{}_{}".format(self.namespace.replace(".", "_").upper(), context.replace(".", "_").upper(), entity.replace(".", "_").upper())
            out_var = "out_{}_{}".format(count, self._base_fit_proc_name)
            out_cons_var = "out_{}_{}".format(count, self._cons_fit_proc_name)
            filename = self.namespace.replace(".", "_") + '-' + out_cons_var
            #no need to create hdbview for the output table

            if model_position['out'] == idx:
                out_name = self._model_name
                filename = out_name
                self._service_projection += ["    @readonly entity {1} as projection on hanaml.{0}.{1};".format(context, self._model_cds_entity)]
                self._service_projection_category.add(context)
                hdbtable, cds = cds_convert.create_cds_artifacts(data=output_df, namespace=self.namespace, context=context, entity=self._model_cds_entity, create_view=False, annotation=annotation, hdbtable_name=out_name, hdbview_name=out_name)
                self._hdbtables[self._model_filename + ".hdbtable"] = hdbtable
            else:
                self._service_projection += ["    @readonly entity {1} as projection on hanaml.{0}.{1};".format(context, entity)]
                self._service_projection_category.add(context)
                hdbtable, cds = cds_convert.create_cds_artifacts(data=output_df, namespace=self.namespace, context=context, entity=entity, create_view=False, annotation=annotation)
                self._hdbtables[filename.replace("_", "-") + ".hdbtable"] = hdbtable

            self._cdses[context].append(cds)
            self._call_base_fit_proc += out_cons_var + ", "
            self._out_fit_tables.append("TRUNCATE TABLE {0};\nINSERT INTO {0} SELECT * FROM :{1};".format(out_name, out_cons_var))
            outputs.append("out {} TABLE({})".format(out_var,
                                                     ", ".join(output_tt)))
            outputs_cons.append("out {} TABLE({})".format(out_cons_var,
                                                          ", ".join(output_tt)))
            output_block.append("{} = SELECT * FROM :out_{};".format(out_var, count))
            count = count + 1
        self._call_base_fit_proc = self._call_base_fit_proc[:-2] + ");"
        proc_header = "PROCEDURE {}(\n{})\nLANGUAGE SQLSCRIPT\nSQL SECURITY INVOKER\nAS\nBEGIN\n".format(self._base_fit_proc_name.lower(),\
            ",\n".join(inputs + outputs))
        self._cons_fit_proc_header = "PROCEDURE {}(\n{})\nLANGUAGE SQLSCRIPT\nSQL SECURITY INVOKER\nAS\nBEGIN\n".format(self._cons_fit_proc_name.lower(),\
            ",\n".join(outputs_cons))
        if hasattr(obj, 'massive'):
            if obj.massive is True:
                body = re.search(r'DECLARE [\s\S]+UNNEST\(:group_id, :param_name, :int_value, :double_value, :string_value\);', obj._fit_anonymous_block).group(0)
            else:
                body = re.search(r'DECLARE [\s\S]+UNNEST\(:param_name, :int_value, :double_value, :string_value\);', obj._fit_anonymous_block).group(0)
        else:
            body = re.search(r'DECLARE [\s\S]+UNNEST\(:param_name, :int_value, :double_value, :string_value\);', obj._fit_anonymous_block).group(0)
        proc_synonym = "SYSAFL::{}".format(afl_func[0].replace("_", ""))
        self._synonyms[proc_synonym] = {
            "target" : {
                "object" : afl_func[0],
                "schema" : "_SYS_AFL"
            }
        }
        object_schemas = list(set(object_schemas))
        schema_privileges = []
        for schema in object_schemas:
            schema_privileges.append({
                "reference": schema,
                "privileges_with_grant_option": ["SELECT"]
            })
        self._grants['ServiceName_1']['object_owner']['schema_privileges'] = schema_privileges
        self._grants['ServiceName_1']['application_user']['schema_privileges'] = schema_privileges
        self._base_fit_proc = proc_header + body + "\n" + "\n".join(input_block) + "\nCALL " + obj._fit_call.replace("_SYS_AFL.{}".format(afl_func[0]), quotename(proc_synonym)) + "\n" + "\n".join(output_block) + "\nEND"

    def _generate_consume_fit_procedure(self):
        self._cons_fit_proc = self._cons_fit_proc_header + "\n".join(self._in_fit_tables) + "\n" + self._call_base_fit_proc + "\n" + "\n".join(self._out_fit_tables) + "\nEND"

    def _generate_base_predict_procedure(self, obj, cds_gen=False, model_position=None):
        input_block = []
        output_block = []
        afl_func = re.findall(r"_SYS_AFL.(.+)\(", obj._predict_call)
        proc_name = "{}".format(afl_func[0].lower())
        self._base_predict_proc_name = self.namespace.replace(".", "_") + "_base_" + proc_name
        self._call_base_predict_proc = "CALL {}(".format(self._base_predict_proc_name)
        self._cons_predict_proc_name = self.namespace.replace(".", "_") + "_cons_" + proc_name
        self._apply_func_name = self.namespace.replace(".", " ").title().replace(" ", "") + "ApplyFunc" + proc_name.replace("_", " ").title().replace(" ", "")
        self._base_predict_proc_filename = self._base_predict_proc_name.replace("_", "-") + ".hdbprocedure"
        self._cons_predict_proc_filename = self._cons_predict_proc_name.replace("_", "-") + ".hdbprocedure"
        self._apply_func_filename = self.namespace.replace(".", "-") + "-" + "apply-func-" + proc_name.replace("_", "-") + ".hdbfunction"
        self._apply_func_view_filename = self.namespace.replace(".", "-") + "-" + "consume-apply-func-" + proc_name.replace("_", "-") + ".hdbview"
        entity_prefix = proc_name.replace("_"," ").title().replace(" ", "")
        inputs = []
        outputs = []
        outputs_cons = []
        count = 0
        conn = None
        annotation = None
        if cds_gen:
            annotation = ['@cds.persistence.exists']
        m_select_statement_list = []
        for idx, inp in enumerate(obj._predict_args):
            if isinstance(inp, DataFrame):
                conn = inp.connection_context
                m_select_statement, synonyms = _create_synonyms(inp.select_statement, self.namespace)
                self._synonyms.update(synonyms)
                #if first_sel is None:
                #    first_sel = m_select_statement
                input_tt = []
                if hasattr(obj, 'predict_input_signature'):
                    if obj.predict_input_signature is not None:
                        inp._columns = list(obj.predict_input_signature[idx].keys())
                        inp._mock_table_structure = obj.predict_input_signature[idx]
                for key, val in inp.get_table_structure().items():
                    input_tt.append("{} {}".format(quotename(key), val))
                context = self._predict_context
                entity="Input{}{}".format(count, entity_prefix)
                in_name = "{}_{}_{}".format(self.namespace.replace(".", "_").upper(), context.replace(".", "_").upper(), entity.replace(".", "_").upper())
                in_var = "in_{}_{}".format(count, self._base_predict_proc_name)
                in_cons_var = "in_{}_{}".format(count, self._cons_predict_proc_name)
                if self._materialize_ds_data:
                    in_cons_name = in_name
                    filename = self.namespace.replace(".", "-") + '-' + in_cons_var
                if model_position['in'] == idx:
                    in_name = self._model_name
                    m_select_statement = "SELECT * FROM {}".format(in_name)
                    if self._materialize_ds_data:
                        in_cons_name = in_name
                        filename = in_name
                else:
                    if self._materialize_ds_data:
                        self._service_projection += ["    @readonly entity {1} as projection on hanaml.{0}.{1};".format(context, entity)]
                        self._service_projection_category.add(context)
                        hdbtable, hdbview, cds = cds_convert.create_cds_artifacts(data=inp, namespace=self.namespace, context=context, entity=entity, annotation=annotation, hdbtable_name="{}::{}.{}".format(self.namespace, context.capitalize(), entity.capitalize()))
                        self._hdbtables[filename.replace("_", "-") + ".hdbtable"] = hdbtable
                        self._hdbviews[filename.replace("_", "-") + "-mapping.hdbview"] = hdbview
                        self._cdses[context].append(cds)

                if self._materialize_ds_data:
                    m_select_statement = "SELECT * FROM {}".format(in_cons_name)
                if self._tudf:
                    self._consume_tudf.append(in_name)
                if "#" in m_select_statement:
                    raise ValueError("Temporary table is not supported!" + m_select_statement)
                self._in_predict_tables.append("{} = {};".format(in_cons_var, m_select_statement))
                inputs.append("in {} TABLE({})".format(in_var,
                                                       ", ".join(input_tt)))
                self._call_base_predict_proc += ":" + in_cons_var + ", "
                input_block.append("in_{} = SELECT * FROM :{};".format(count, in_var))
                count = count + 1
                m_select_statement_list.append(m_select_statement)
        apply_return = None
        apply_return_tt = None
        consume_tudf_res = None
        count = 0
        for idx, output in enumerate(obj._predict_output_table_names):
            output_tt = []
            if output.upper().startswith("SELECT "):
                output_df = conn.sql(output)
            else:
                output_df = conn.table(output)
            if hasattr(obj, 'predict_output_signature'):
                if obj.predict_output_signature is not None:
                    output_df._columns = list(obj.predict_output_signature[idx].keys())
                    output_df._mock_table_structure = obj.predict_output_signature[idx]
            for key, val in output_df.get_table_structure().items():
                output_tt.append("{} {}".format(quotename(key), val))
            context = self._predict_context
            entity="Output{}{}".format(count, entity_prefix)
            #no need to create hdbview for the output table
            if count == 0:
                if output.upper().startswith("SELECT "):
                    consume_tudf_res = conn.sql(output)
                else:
                    consume_tudf_res = conn.table(output)
            self._service_projection += ["    @readonly entity {1} as projection on hanaml.{0}.{1};".format(context, entity)]
            self._service_projection_category.add(context)
            hdbtable, cds = cds_convert.create_cds_artifacts(data=output_df, namespace=self.namespace, context=context, entity=entity, create_view=False, annotation=annotation)
            self._cdses[context].append(cds)
            out_name = "{}_{}_{}".format(self.namespace.replace(".", "_").upper(), context.replace(".", "_").upper(), entity.replace(".", "_").upper())
            out_var = "out_{}_{}".format(count, self._base_predict_proc_name)
            out_cons_var = "out_{}_{}".format(count, self._cons_predict_proc_name)
            self._hdbtables[self.namespace.replace(".", "-") + '-' + out_cons_var.replace("_", "-") + ".hdbtable"] = hdbtable
            self._call_base_predict_proc += out_cons_var + ", "
            self._out_predict_tables.append("TRUNCATE TABLE {0};\nINSERT INTO {0} SELECT * FROM :{1};".format(out_name, out_cons_var))
            outputs.append("out {} TABLE({})".format(out_var,
                                                     ", ".join(output_tt)))
            outputs_cons.append("out {} TABLE({})".format(out_cons_var,
                                                          ", ".join(output_tt)))
            output_block.append("{} = SELECT * FROM :out_{};".format(out_var, count))
            if count == 0:
                apply_return_tt = "TABLE(\n{})".format(",\n".join(output_tt))
                apply_return = "RETURN SELECT * FROM :out_{};".format(count)
            count += 1
        self._call_base_predict_proc = self._call_base_predict_proc[:-2] + ");"
        proc_header = "PROCEDURE {}(\n{})\nLANGUAGE SQLSCRIPT\nSQL SECURITY INVOKER\nAS\nBEGIN\n".format(self._base_predict_proc_name.lower(),\
            ",\n".join(inputs + outputs))
        func_header = "FUNCTION {}() RETURNS {}\nREADS SQL DATA\nAS\nBEGIN\n".format(self.namespace.replace(".", "_") + "_" + self._predict_context + "_" + self._apply_func_name, apply_return_tt)
        if self._tudf:
            self._hdbviews[self._apply_func_view_filename] = "VIEW {} AS SELECT\n  *\nFROM {}();".format("{}_{}_Consume{}".format(self.namespace.replace(".", "_"), self._predict_context, self._apply_func_name),
                                                                                                                self.namespace.replace(".", "_") + "_" + self._predict_context + "_" + self._apply_func_name)
            self._service_projection += ["    @readonly entity {1} as projection on hanaml.{0}.{1};".format(context, self._apply_func_name)]
            self._service_projection_category.add(context)
            _, consume_tudf_cds = cds_convert.create_cds_artifacts(data=consume_tudf_res, namespace=self.namespace, context=context, entity=self._apply_func_name, create_view=False, annotation=["@cds.persistence.exists", "@cds.persistence.udf"])
            self._cdses[context].append(consume_tudf_cds)
        self._cons_predict_proc_header = "PROCEDURE {}(\n{})\nLANGUAGE SQLSCRIPT\nSQL SECURITY INVOKER\nAS\nBEGIN\n".format(self._cons_predict_proc_name.lower(),\
            ",\n".join(outputs_cons))
        if hasattr(obj, 'massive'):
            if obj.massive is True:
                body = re.search(r'DECLARE [\s\S]+UNNEST\(:group_id, :param_name, :int_value, :double_value, :string_value\);', obj._predict_anonymous_block).group(0)
            else:
                body = re.search(r'DECLARE [\s\S]+UNNEST\(:param_name, :int_value, :double_value, :string_value\);', obj._predict_anonymous_block).group(0)
        else:
            body = re.search(r'DECLARE [\s\S]+UNNEST\(:param_name, :int_value, :double_value, :string_value\);', obj._predict_anonymous_block).group(0)
        proc_synonym = "SYSAFL::{}".format(afl_func[0].replace("_", ""))
        self._synonyms[proc_synonym] = {
            "target" : {
                "object" : afl_func[0],
                "schema" : "_SYS_AFL"
            }
        }
        self._base_predict_proc = proc_header + body + "\n" + "\n".join(input_block) + "\nCALL " + obj._predict_call.replace("_SYS_AFL.{}".format(afl_func[0]), quotename(proc_synonym)) + "\n" + "\n".join(output_block) + "\nEND"
        if self._tudf:
            function_inputs = ''
            for idx, sel_elem in enumerate(m_select_statement_list):
                function_inputs += "\nin_{} = {};".format(idx, sel_elem)
            self._apply_hdbfunction = func_header + body + "\n" + function_inputs + "\nCALL " + obj._predict_call.replace("_SYS_AFL.{}".format(afl_func[0]), quotename(proc_synonym)) + "\n" + apply_return + "\nEND"

    def _generate_consume_predict_procedure(self):
        self._cons_predict_proc = self._cons_predict_proc_header + "\n".join(self._in_predict_tables) + "\n" + self._call_base_predict_proc + "\n" + "\n".join(self._out_predict_tables) + "\nEND"

    def _generate_base_score_procedure(self, obj, cds_gen=False, model_position=None):
        input_block = []
        output_block = []
        afl_func = re.findall(r"_SYS_AFL.(.+)\(", obj._score_call)
        proc_name = "{}".format(afl_func[0].lower())
        self._base_score_proc_name = self.namespace.replace(".", "_") + "_base_" + proc_name
        self._call_base_score_proc = "CALL {}(".format(self._base_score_proc_name)
        self._cons_score_proc_name = self.namespace.replace(".", "_") + "_cons_" + proc_name

        self._base_score_proc_filename = self._base_score_proc_name.replace("_", "-") + ".hdbprocedure"
        self._cons_score_proc_filename = self._cons_score_proc_name.replace("_", "-") + ".hdbprocedure"

        entity_prefix = proc_name.replace("_"," ").title().replace(" ", "")
        inputs = []
        outputs = []
        outputs_cons = []
        count = 0
        conn = None
        annotation = None
        if cds_gen:
            annotation = ['@cds.persistence.exists']
        for idx, inp in enumerate(obj._score_args):
            if isinstance(inp, DataFrame):
                conn = inp.connection_context
                m_select_statement, synonyms = _create_synonyms(inp.select_statement, self.namespace)
                self._synonyms.update(synonyms)
                input_tt = []
                if hasattr(obj, 'score_input_signature'):
                    if obj.score_input_signature is not None:
                        inp._columns = list(obj.score_input_signature[idx].keys())
                        inp._mock_table_structure = obj.score_input_signature[idx]
                for key, val in inp.get_table_structure().items():
                    input_tt.append("{} {}".format(quotename(key), val))
                context = self._score_context
                entity="Input{}{}".format(count, entity_prefix)
                in_name = "{}_{}_{}".format(self.namespace.replace(".", "_").upper(), context.replace(".", "_").upper(), entity.replace(".", "_").upper())
                in_cons_name = in_name
                in_var = "in_{}_{}".format(count, self._base_score_proc_name)
                in_cons_var = "in_{}_{}".format(count, self._cons_score_proc_name)
                filename = self.namespace.replace(".", "-") + '-' + in_cons_var
                if model_position['in'] == idx:
                    in_name = self._model_name
                    filename = in_name
                    m_select_statement = "SELECT * FROM {}".format(in_name)
                else:
                    hdbtable, hdbview, cds = cds_convert.create_cds_artifacts(data=inp, namespace=self.namespace, context=context, entity=entity, annotation=annotation, hdbtable_name="{}::{}.{}".format(self.namespace, context.capitalize(), entity.capitalize()))
                    if self._materialize_ds_data:
                        self._service_projection += ["    @readonly entity {1} as projection on hanaml.{0}.{1};".format(context, entity)]
                        self._service_projection_category.add(context)
                        self._hdbtables[filename.replace("_", "-") + ".hdbtable"] = hdbtable
                        self._hdbviews[filename.replace("_", "-") + "-mapping.hdbview"] = hdbview
                        self._cdses[context].append(cds)

                if self._materialize_ds_data:
                    m_select_statement = "SELECT * FROM {}".format(in_cons_name)
                if "#" in m_select_statement:
                    raise ValueError("Temporary table is not supported!" + m_select_statement)
                self._in_score_tables.append("{} = {};".format(in_cons_var, m_select_statement))
                inputs.append("in {} TABLE({})".format(in_var,
                                                       ", ".join(input_tt)))
                self._call_base_score_proc += ":" + in_cons_var + ", "
                input_block.append("in_{} = SELECT * FROM :{};".format(count, in_var))
                count = count + 1
        count = 0
        for idx, output in enumerate(obj._score_output_table_names):
            output_tt = []
            if output.upper().startswith("SELECT "):
                output_df = conn.sql(output)
            else:
                output_df = conn.table(output)
            if hasattr(obj, 'score_output_signature'):
                if obj.score_output_signature is not None:
                    output_df._columns = list(obj.score_output_signature[idx].keys())
                    output_df._mock_table_structure = obj.score_output_signature[idx]
            for key, val in output_df.get_table_structure().items():
                output_tt.append("{} {}".format(quotename(key), val))
            context = self._score_context
            entity="Output{}{}".format(count, entity_prefix)
            self._service_projection += ["    @readonly entity {1} as projection on hanaml.{0}.{1};".format(context, entity)]
            self._service_projection_category.add(context)
            #no need to create hdbview for the output table
            hdbtable, cds = cds_convert.create_cds_artifacts(data=output_df, namespace=self.namespace, context=context, entity=entity, create_view=False, annotation=annotation)
            self._cdses[context].append(cds)
            out_name = "{}_{}_{}".format(self.namespace.replace(".", "_").upper(), context.replace(".", "_").upper(), entity.replace(".", "_").upper())
            out_var = "out_{}_{}".format(count, self._base_score_proc_name)
            out_cons_var = "out_{}_{}".format(count, self._cons_score_proc_name)
            self._hdbtables[self.namespace.replace(".", "-") + '-' + out_cons_var.replace("_", "-") + ".hdbtable"] = hdbtable
            self._call_base_score_proc += out_cons_var + ", "
            self._out_score_tables.append("TRUNCATE TABLE {0};\nINSERT INTO {0} SELECT * FROM :{1};".format(out_name, out_cons_var))
            outputs.append("out {} TABLE({})".format(out_var,
                                                     ", ".join(output_tt)))
            outputs_cons.append("out {} TABLE({})".format(out_cons_var,
                                                          ", ".join(output_tt)))
            output_block.append("{} = SELECT * FROM :out_{};".format(out_var, count))

            count += 1
        self._call_base_score_proc = self._call_base_score_proc[:-2] + ");"
        proc_header = "PROCEDURE {}(\n{})\nLANGUAGE SQLSCRIPT\nSQL SECURITY INVOKER\nAS\nBEGIN\n".format(self._base_score_proc_name.lower(),\
            ",\n".join(inputs + outputs))
        self._cons_score_proc_header = "PROCEDURE {}(\n{})\nLANGUAGE SQLSCRIPT\nSQL SECURITY INVOKER\nAS\nBEGIN\n".format(self._cons_score_proc_name.lower(),\
            ",\n".join(outputs_cons))
        if hasattr(obj, 'massive'):
            if obj.massive is True:
                body = re.search(r'DECLARE [\s\S]+UNNEST\(:group_id, :param_name, :int_value, :double_value, :string_value\);', obj._score_anonymous_block).group(0)
            else:
                body = re.search(r'DECLARE [\s\S]+UNNEST\(:param_name, :int_value, :double_value, :string_value\);', obj._score_anonymous_block).group(0)
        else:
            body = re.search(r'DECLARE [\s\S]+UNNEST\(:param_name, :int_value, :double_value, :string_value\);', obj._score_anonymous_block).group(0)
        proc_synonym = "SYSAFL::{}".format(afl_func[0].replace("_", ""))
        self._synonyms[proc_synonym] = {
            "target" : {
                "object" : afl_func[0],
                "schema" : "_SYS_AFL"
            }
        }
        self._base_score_proc = proc_header + body + "\n" + "\n".join(input_block) + "\nCALL " + obj._score_call.replace("_SYS_AFL.{}".format(afl_func[0]), quotename(proc_synonym)) + "\n" + "\n".join(output_block) + "\nEND"

    def _generate_consume_score_procedure(self):
        self._cons_score_proc = self._cons_score_proc_header + "\n".join(self._in_score_tables) + "\n" + self._call_base_score_proc + "\n" + "\n".join(self._out_score_tables) + "\nEND"


    def generate_artifacts(self, obj, cds_gen=False, model_position=None, tudf=False, archive=True):
        """
        Generate CAP artifacts.

        Parameters
        ----------
        obj : hana-ml object
            The hana-ml object that has generated the execution statement.
        cds_gen : bool, optional
            Control whether to allow Python client to generate HANA tables, procedures, and so on. If True, it will generate HANA artifacts from cds.

            Defaults to False.
        model_position : bool or dict, optional
            Specifies the model table position from the procedure outputs and the procedure inputs such that {"out": 0, "in" : 1}. If True, the model position {"out": 0, "in" : 1} will be used.

            Defaults to None.
        tudf : bool, optional
            If True, it will generate a table UDF for applying.
            Defaults to False.
        """
        self._tudf = tudf
        if model_position:
            if not isinstance(model_position, dict):
                model_position = {"out": 0, "in": 1}
        else:
            model_position = {'out': 0, 'in': 1}
        root_path = os.path.join(self.output_dir, self.project_name)
        db_path = os.path.join(root_path, "db", "src")
        srv_path = os.path.join(root_path, "srv")
        if os.path.isdir(db_path) and archive:
            arc_dir_name = "archive_{}".format(uuid.uuid4())
            arc_dir = os.path.join(root_path, arc_dir_name)
            prefix = self.namespace.replace(".", "-")
            if len(os.listdir(db_path)) > 0:
                #create archive folder
                try:
                    os.makedirs(os.path.join(arc_dir, "db", "src"))
                except:
                    pass
                #copy package.json
                if os.path.isfile(os.path.join(root_path, "package.json")):
                    shutil.copyfile(os.path.join(root_path, "package.json"), os.path.join(arc_dir, "package.json"))
                #move db/ files with namespace prefix
                sub_path = os.path.join(root_path, "db")
                for file_or_dir in os.listdir(sub_path):
                    if file_or_dir.startswith(prefix):
                        shutil.move(os.path.join(sub_path, file_or_dir), os.path.join(arc_dir, "db"))
                #move db/src files with namespace prefix
                sub_path = os.path.join(root_path, "db", "src")
                for file_or_dir in os.listdir(sub_path):
                    if file_or_dir.startswith(prefix):
                        shutil.move(os.path.join(sub_path, file_or_dir), os.path.join(arc_dir, "db", "src"))
            if os.path.isdir(srv_path):
                if len(os.listdir(srv_path)) > 0:
                    #create archive folder
                    try:
                        os.makedirs(os.path.join(arc_dir, "srv"))
                    except:
                        pass
                    #move srv/ files with namespace prefix
                    for file_or_dir in os.listdir(srv_path):
                        if file_or_dir.startswith(prefix):
                            shutil.move(os.path.join(srv_path, file_or_dir), os.path.join(arc_dir, "srv"))
            else:
                try:
                    os.makedirs(srv_path)
                except:
                    pass

        else:
            try:
                os.makedirs(db_path)
                os.makedirs(srv_path)
            except:
                pass
        #srv folder
        if obj._fit_call:
            self._generate_base_fit_procedure(obj, cds_gen, model_position)
            self._generate_consume_fit_procedure()
        if obj._predict_call:
            self._generate_base_predict_procedure(obj, cds_gen, model_position)
            self._generate_consume_predict_procedure()
        if obj._score_call:
            self._generate_base_score_procedure(obj, cds_gen, model_position)
            self._generate_consume_score_procedure()
        #under db/src
        #write hdbtable files
        if cds_gen:
            for kkey, vval in self._hdbtables.items():
                with open(os.path.join(db_path, kkey), "w+") as hdbtablefile:
                    hdbtablefile.write(vval)
            #write hdbview files
            for kkey, vval in self._hdbviews.items():
                with open(os.path.join(db_path, kkey), "w+") as hdbviewfile:
                    hdbviewfile.write(vval)
        else:
            for kkey, vval in self._hdbviews.items():
                if hasattr(self, "_apply_func_view_filename"):
                    if kkey == self._apply_func_view_filename:
                        with open(os.path.join(db_path, kkey), "w+") as hdbviewfile:
                            hdbviewfile.write(vval)
                    if self._tudf:
                        if kkey == self._consume_tudf[0].replace('_', '-').lower() + '.hdbview':
                            if not self._materialize_ds_data:
                                with open(os.path.join(db_path, kkey), "w+") as hdbviewfile:
                                    hdbviewfile.write(vval)
        #write synonym files
        synonym_filename = self.namespace.replace(".", "-") + "-synonyms.hdbsynonym"
        synonym_filepath = os.path.join(db_path, synonym_filename)
        if os.path.exists(synonym_filepath):
            with open(synonym_filepath, "r") as hdbsynonymfile:
                existing_synonyms = json.load(hdbsynonymfile)
                existing_synonyms.update(self._synonyms)
            with open(synonym_filepath, "w") as hdbsynonymfile:
                json.dump(existing_synonyms, hdbsynonymfile, indent=4)
        else:
            with open(synonym_filepath, "w") as hdbsynonymfile:
                json.dump(self._synonyms, hdbsynonymfile, indent=4)
        #write procedures
        if obj._fit_call:
            with open(os.path.join(db_path, self._base_fit_proc_filename), "w+") as hdbprocfile:
                hdbprocfile.write(self._base_fit_proc)
            with open(os.path.join(db_path, self._cons_fit_proc_filename), "w+") as hdbprocfile:
                hdbprocfile.write(self._cons_fit_proc)
        if obj._predict_call:
            with open(os.path.join(db_path, self._base_predict_proc_filename), "w+") as hdbprocfile:
                hdbprocfile.write(self._base_predict_proc)
            with open(os.path.join(db_path, self._cons_predict_proc_filename), "w+") as hdbprocfile:
                hdbprocfile.write(self._cons_predict_proc)
            #write apply hdbfunction
            if self._tudf:
                with open(os.path.join(db_path, self._apply_func_filename), "w+") as hdbfunction:
                    hdbfunction.write(self._apply_hdbfunction)
        if obj._score_call:
            with open(os.path.join(db_path, self._base_score_proc_filename), "w+") as hdbprocfile:
                hdbprocfile.write(self._base_score_proc)
            with open(os.path.join(db_path, self._cons_score_proc_filename), "w+") as hdbprocfile:
                hdbprocfile.write(self._cons_score_proc)
        #write grants
        if self._is_APL(obj):
            self._grants['ServiceName_1']['object_owner']['global_roles'] = \
                [{"roles": ["AFL__SYS_AFL_APL_AREA_EXECUTE", "sap.pa.apl.base.roles::APL_EXECUTE", "AFLPM_CREATOR_ERASER_EXECUTE"]}]
            self._grants['ServiceName_1']['application_user']['global_roles']= \
                [{"roles": ["AFL__SYS_AFL_APL_AREA_EXECUTE", "sap.pa.apl.base.roles::APL_EXECUTE", "AFLPM_CREATOR_ERASER_EXECUTE"]}]
        else:
            self._grants['ServiceName_1']['object_owner']['global_roles'] = [{"roles": ["AFL__SYS_AFL_AFLPAL_EXECUTE"]}]
            self._grants['ServiceName_1']['application_user']['global_roles'] = [{"roles": ["AFL__SYS_AFL_AFLPAL_EXECUTE"]}]
        grant_filename = self.namespace.replace(".", "-") + "-grants.hdbgrants"
        with open(os.path.join(db_path, grant_filename), "w+") as hdbgrantsfile:
            json.dump(self._grants, hdbgrantsfile, indent=4)
        #under db/
        #write cds
        cds_filename = self.namespace.replace(".", "-") + "-cds"
        # if cds_filename exists
        if os.path.exists(os.path.join(root_path, "db", cds_filename + ".cds")):
            with open(os.path.join(root_path, "db", cds_filename + ".cds"), "r") as cdsfile:
                cds_content = cdsfile.read()
            for kkey, vval in self._cdses.items():
                single_content = ""
                if len(vval) > 0:
                    single_content += "\ncontext {} ".format(kkey) + "{\n"
                    for cds in vval:
                        single_content += cds + "\n"
                    single_content += "}"
                    if "\ncontext {} ".format(kkey) in cds_content:
                        cds_content = cds_content.replace(_find_single_content(cds_content, kkey), single_content)
                    else:
                        cds_content += single_content
        else:
            cds_content = "namespace {};\n".format(self.namespace)
            for kkey, vval in self._cdses.items():
                if len(vval) > 0:
                    cds_content += "\ncontext {} ".format(kkey) + "{\n"
                    for cds in vval:
                        cds_content += cds + "\n"
                    cds_content += "}"
        with open(os.path.join(root_path, "db", cds_filename + ".cds"), "w+") as cdsfile:
            cdsfile.write(cds_content)
        #write srv
        srv_filename = self.namespace.replace(".", "-") + "-cat-service"
        if os.path.exists(os.path.join(srv_path,  srv_filename + ".cds")):
            with open(os.path.join(srv_path,  srv_filename + ".cds"), "r") as srvfile:
                srv_content = srvfile.read()
                service_projection_categories = set()
                for srv_proj in self._service_projection:
                    if self._fit_context in srv_proj and self._fit_context in srv_content:
                        service_projection_categories.add(self._fit_context)
                    if self._predict_context in srv_proj and self._predict_context in srv_content:
                        service_projection_categories.add(self._predict_context)
                    if self._score_context in srv_proj and self._score_context in srv_content:
                        service_projection_categories.add(self._score_context)
                srv_content = _remove_category_srv_content(srv_content, list(service_projection_categories))
                srv_content = _insert_string_between_braces(srv_content, "\n" + "\n".join(self._service_projection))
        else:
            srv_content = "using {} as hanaml from '../db/{}';\n\nservice CatalogService {{\n{}\n}}".format(self.namespace, cds_filename, "\n".join(self._service_projection))
        with open(os.path.join(srv_path,  srv_filename + ".cds"), "w+") as srvfile:
            srvfile.write(srv_content)
        if not cds_gen:
            package_json_file = os.path.join(root_path, "package.json")
            package_json_template = {
                "build": {
                    "tasks": [
                        {
                            "for": "hana",
                            "dest": "../db"
                        },
                        {
                            "for": "node-cf"
                        }
                    ]
                },
                "requires": {
                    "db": {
                        "kind": "hana-cloud"
                    }
                }
            }
            if os.path.exists(package_json_file):
                with open(package_json_file, "r") as file:
                    package_json = json.load(file)
                with open(package_json_file, "w") as file:
                    package_json["cds"] = package_json_template
                    json.dump(package_json, file, indent=4)
            else:
                with open(package_json_file, "w+") as file:
                    package_json = {}
                    package_json["cds"] = package_json_template
                    json.dump(package_json, file, indent=4)
        self._clean_up()

class HanaGenerator(object):
    """
    This class provides HANA specific generation functionality. It also extends the config file
    to cater for HANA specific config generation.

    Parameters
    ----------
    project_name : str
        The name of project.

    version : str
        The version name.

    grant_service : str
        The grant service.

    connection_context : str
        The connection to the SAP HANA.

    outputdir : str
        The directory of output.

    generation_merge_type : int, optional
        Merge type is which operations should be merged together. There are at this stage
        only 2 options:

        - 1: GENERATION_MERGE_NONE: All operations are generated separately (i.e. individual
          procedures in HANA)
        - 2: GENERATION_MERGE_PARTITION: A partition operation is merged into the respective
          related operation and generated as 1 (i.e. procedure in HANA).

        Defaults to 1.

    generation_group_type : int, optional

        - 11: GENERATION_GROUP_NONE # No grouping is applied. This means that solution specific
          implementation will define how to deal with this
        - 12: GENERATION_GROUP_FUNCTIONAL # Grouping is based on functional grouping. Meaning
          that logical related elements such as partition / fit / and related score will be
          put together.

        Defaults to 12.

    sda_grant_service: str, optional
        The grant service of Smart Data Access (SDA).

        Defaults to None.

    remote_source : str, optional
        The name of remote source.

        Defaults to ''.

    Examples
    --------
    Let's assume we have a connection to SAP HANA called connection_context and a basic Random Decision Trees Classifier 'rfc' with training data 'diabetes_train_valid' and prediction data 'diabetes_test'.

    >>> rfc_params = dict(n_estimators=5, split_threshold=0, max_depth=10)
    >>> rfc = UnifiedClassification(func="randomdecisiontree", **rfc_params)
    >>> rfc.fit(diabetes_train_valid,
                key='ID',
                label='CLASS',
                categorical_variable=['CLASS'],
                partition_method='stratified',
                stratified_column='CLASS',)
    >>> rfc.predict(diabetes_test.drop(cols=['CLASS']), key="ID")

    Then, we could generate HDI artifacts:

    >>> hg = hana.HanaGenerator(project_name="test", version='1', grant_service='', connection_context=connection_context, outputdir="./hana_out")

    >>> hg.generate_artifacts()

    Returns a output path of the root folder where the hana related artifacts are stored:

    >>> './hana_out\\test\\hana'

    """
    def __init__(self, project_name, version, grant_service, connection_context, outputdir,
                 generation_merge_type=ConfigConstants.GENERATION_MERGE_NONE,
                 generation_group_type=ConfigConstants.GENERATION_GROUP_FUNCTIONAL,
                 sda_grant_service=None, remote_source=''):
        self.config = ConfigHandler.init_config(project_name,
                                                version,
                                                grant_service,
                                                outputdir,
                                                generation_merge_type,
                                                generation_group_type,
                                                sda_grant_service,
                                                remote_source)
        self.hana_helper = HanaGeneratorHelper(self.config)
        sql_processor = SqlProcessor(self.config)
        sql_processor.parse_sql_trace(connection_context)
        self._extend_config()

    def generate_artifacts(self, base_layer=True, consumption_layer=True, sda_data_source_mapping_only=False):
        """
        Generate the artifacts by first building up the required folder structure for artifacts storage and then
        generating the different required files. Be aware that this method only generates the generic files
        and offloads the generation of artifacts where traversal of base and consumption layer
        elements is required.

        Parameters
        ----------
        base_layer : bool, optional
            The base layer is the low level procedures that will be generated.

            Defaults to True.

        consumption_layer : bool, optional
            The consumption layer is the layer that will consume the base layer artifacts.

            Defaults to True.

        sda_data_source_mapping_only : bool, optional
            In case data source mapping is provided, you can force to only do this for the
            Smart Data Access (SDA) HANA deployment infrastructure (HDI) container.

            Defaults to False.

        Returns
        -------
        str
            Return the output path of the root folder where the hana related artifacts are stored.
        """
        self.hana_helper._build_folder_structure(ConfigConstants.PROJECT_TEMPLATE_BASE_STRUCT,
                                                 self.config.get_entry(ConfigConstants.CONFIG_KEY_OUTPUT_PATH_HANA),
                                                 self.config.get_entry(ConfigConstants.CONFIG_KEY_OUTPUT_PATH_MODULE))

        # Instantiate file writers
        yaml_writer = MTAYamlWriter(self.config)
        grant_writer = HDBGrantWriter(self.config)
        synonym_writer = HDBSynonymWriter(self.config)
        role_writer = HDBRoleWriter(self.config)
        consumption_processor = HanaConsumptionProcessor(self.config)

        # Generate mta yaml file
        output_path = self.config.get_entry(ConfigConstants.CONFIG_KEY_OUTPUT_PATH_HANA)
        app_id = self.config.get_entry(ConfigConstants.CONFIG_KEY_APPID)
        module_name = self.config.get_entry(ConfigConstants.CONFIG_KEY_MODULE_NAME)
        version = self.config.get_entry(ConfigConstants.CONFIG_KEY_VERSION)
        schema = self.config.get_entry(ConfigConstants.CONFIG_KEY_SCHEMA)
        grant_service = self.config.get_entry(ConfigConstants.CONFIG_KEY_GRANT_SERVICE)
        yaml_writer.generate(output_path, app_id, module_name, version, schema, grant_service)

        # Generate hdbgrants
        grant_writer.generate(self.config.get_entry(ConfigConstants.CONFIG_KEY_GRANTS_PATH))

        # Generate dataset/function synonym
        synonym_writer.generate(self.config.get_entry(ConfigConstants.CONFIG_KEY_SYNONYMS_PATH))

        # Generate hdbroles for external access
        role_writer.generate(self.config.get_entry(ConfigConstants.CONFIG_KEY_ROLES_PATH),
                             name=self.config.get_entry(ConfigConstants.CONFIG_KEY_MODULE_NAME))

        # Generate Consumption Artifacts
        consumption_processor.generate(base_layer, consumption_layer, sda_data_source_mapping_only)

        return output_path

    def _extend_config(self):
        """
        Extend the config to cater for HANA generation specific config.
        """
        # Split made between project_name, module_name, app_id as these are seperate entries. However for now set as project_name
        # This allows more finegrained control of these config items in future releases if required.
        module_name = self.config.get_entry(ConfigConstants.CONFIG_KEY_MODULE_NAME)

        # Specific folder locations for the different category of artifacts.
        output_path_hana = os.path.join(self.config.get_entry(ConfigConstants.CONFIG_KEY_OUTPUT_PATH),
                                        ConfigConstants.HANA_BASE_PATH)
        output_path_module = os.path.join(output_path_hana, module_name)
        output_path_module_src = os.path.join(output_path_module, ConfigConstants.MODULE_SOURCE_PATH)
        module_template_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                            ConfigConstants.PROJECT_TEMPLATEDR)
        grants_path = os.path.join(output_path_module_src, ConfigConstants.GRANTS_SOURCE_PATH)
        synonyms_path = os.path.join(output_path_module_src, ConfigConstants.SYNONYMS_SOURCE_PATH)
        procedures_path = os.path.join(output_path_module_src, ConfigConstants.PROCEDURES_SOURCE_PATH)
        roles_path = os.path.join(output_path_module_src, ConfigConstants.ROLES_SOURCE_PATH)
        cds_path = os.path.join(output_path_module_src, ConfigConstants.CDS_SOURCE_PATH)

        self.config.add_entry(ConfigConstants.CONFIG_KEY_OUTPUT_PATH_HANA, output_path_hana)
        self.config.add_entry(ConfigConstants.CONFIG_KEY_OUTPUT_PATH_MODULE, output_path_module)
        self.config.add_entry(ConfigConstants.CONFIG_KEY_OUTPUT_PATH_MODULE_SRC, output_path_module_src)
        self.config.add_entry(ConfigConstants.CONFIG_KEY_MODULE_TEMPLATE_PATH, module_template_path)
        self.config.add_entry(ConfigConstants.CONFIG_KEY_GRANTS_PATH, grants_path)
        self.config.add_entry(ConfigConstants.CONFIG_KEY_SYNONYMS_PATH, synonyms_path)
        self.config.add_entry(ConfigConstants.CONFIG_KEY_PROCEDURES_PATH, procedures_path)
        self.config.add_entry(ConfigConstants.CONFIG_KEY_ROLES_PATH, roles_path)
        self.config.add_entry(ConfigConstants.CONFIG_KEY_CDS_PATH, cds_path)


class HanaConsumptionProcessor(object):
    """
    This class provides HANA specific generation functionality for the base and consumption layer.
    It generates the files for which traversal of the base and comptiontion layer elements is
    required. The actual generation of the base layer is initiated here but is offloaded to the
    HanaGeneratorHelper class for reusability and logic seperation.
    """

    def __init__(self, config):
        """
        This class allow to generate the arifacts for the base and consumption layer.

        Parameters
        ----------
        config : dict
            Central config object
        """
        self.hana_helper = HanaGeneratorHelper(config)
        self.config = config

    def generate(self, base_layer=True, consumption_layer=True, sda_data_source_mapping_only=False):
        """
        Method for generating the actual artifacts content.

        Parameters
        ----------
        base_layer : bool, optional
            The base layer is the low level procedures that will be generated.

            Defaults to True.
        consumption_layer : bool, optional
            The consumption layer is the layer that will consume the base layer artifacts

            Defaults to True.
        sda_data_source_mapping_only : bool, optional
            In case data source mapping is provided you can force to only do this for the
            Smart Data Access (SDA) HANA deployment infrastructure (HDI) container

            Defaults to False.
        """
        procedure_writer = HDBProcedureWriter(self.config)
        cds_writer = HDBCDSWriter(self.config)
        sql_key_sql = SqlProcessor.TRACE_KEY_SQL_PROCESSED
        if base_layer:
            # Base SDA layer procedures
            self.hana_helper._build_base_layer_artifacts(
                self.config.get_entry(ConfigConstants.CONFIG_KEY_PROCEDURES_PATH),
                data_source_mapping=sda_data_source_mapping_only)

        if consumption_layer:
            sql_processed_cons_layer = self.config.get_entry(ConfigConstants.CONFIG_KEY_SQL_PROCESSED)[
                SqlProcessor.TRACE_KEY_CONSUMPTION_LAYER]
            # Consumption layer procedures
            # We create for the output tables cds tables which we generate here
            for element in sql_processed_cons_layer:
                if not isinstance(element, dict):
                    continue  # ignore TODO: proper doc

                if sql_key_sql in element:  # TODO: gen warning if no sql
                    inputs = []
                    output = []
                    body = []

                    proc_name = element['name']

                    if 'input' in element[sql_key_sql]:
                        inputs = element[sql_key_sql]['input']
                    if 'body' in element[sql_key_sql]:
                        body = element[sql_key_sql]['body']
                    if 'output' in element[sql_key_sql]:
                        output = element[sql_key_sql]['output']

                    # Build SQL array
                    sql = []
                    for item in inputs:
                        sql_str = ''
                        if 'sql_vars_syn' in item and item['sql_vars_syn']:
                            sql_str = item[sql_key_sql].format(*item['sql_vars_syn'])
                        else:
                            sql_str = item[sql_key_sql].format(*item['sql_vars'])
                        sql.append(sql_str)
                    for item in body:
                        sql_str = item[sql_key_sql].format(*item['sql_vars'])
                        sql.append(sql_str)
                    for item in output:
                        if 'sql_vars' in item:
                            sql_str = item[sql_key_sql].format(*item['sql_vars'])
                        sql.append(sql_str)

                    # Explicitly disabling inputs on consumption layer as these are stand alone objects
                    signature_str = self.hana_helper._build_procedure_signature(None, output)
                    sql_str = StringUtils.flatten_string_array(sql)
                    if not sda_data_source_mapping_only:
                        sql_str = self.config.data_source_mapping(sql_str)
                    procedure_writer.generate(self.config.get_entry(ConfigConstants.CONFIG_KEY_PROCEDURES_PATH),
                                              proc_name, sql_str, signature_str)

        # --CDS Generation
        # We always create CDS views as these are common components that can be used by solution specific implementation of the consumption layer
        sql_processed_cons_layer = self.config.get_entry(ConfigConstants.CONFIG_KEY_SQL_PROCESSED)[
            SqlProcessor.TRACE_KEY_CONSUMPTION_LAYER]
        cds_entries = []
        for element in sql_processed_cons_layer:
            if not isinstance(element, dict):
                continue  # ignore TODO: proper doc
            if sql_key_sql in element:  # TODO: gen warning if no sql
                output = []
                if 'output' in element[sql_key_sql]:
                    output = element[sql_key_sql]['output']
                for item in output:
                    if 'object_name' in item and 'cds_type' in item:
                        # Sanity check for not duplicating cds views
                        if not any(item['object_name'] in cds_entry for cds_entry in cds_entries):
                            cds_entries.append(
                                self.hana_helper._build_cds_entity_entry(item['object_name'], item['cds_type']))
        # Generate hdbcds based on the generated cds entries
        cds_content = StringUtils.flatten_string_array(cds_entries)
        cds_writer.generate(self.config.get_entry(ConfigConstants.CONFIG_KEY_CDS_PATH),
                            self.config.get_entry(ConfigConstants.CONFIG_KEY_CDS_CONTEXT), cds_content)


class HanaSDAGenerator(object):
    """
    This class provides HANA specific generation functionality for the Smart Data Access (SDA)
    scenario. It only creates the artifacts for the second SDA HANA deployment infrastructure (HDI) container which loads and
    uses data out of the first container which has been created before this class os called.
    It also extend the config to cater for specific required config.

    Parameters
    ----------
    project_name : str
        The name of project.

    version : str
        The version name.

    grant_service : str
        The grant service.

    connection_context : str
        The connection to the SAP HANA.

    outputdir : str
        The directory of output.

    generation_merge_type : int, optional
        The generation type of merge.

        Defaults to 1.

    generation_group_type : int, optional
        The generation type of group.

        Defaults to 12.

    sda_grant_service: str, optional
        The grant service of Smart Data Access (SDA).

        Defaults to None.

    remote_source : str, optional
        The name of remote source.

        Defaults to ''.

    """

    def __init__(self, project_name, version, grant_service, connection_context, outputdir,
                 generation_merge_type=ConfigConstants.GENERATION_MERGE_NONE,
                 generation_group_type=ConfigConstants.GENERATION_GROUP_FUNCTIONAL,
                 sda_grant_service=None, remote_source=''):
        """
        This is main entry point for generating the HANA related artifacts.

        Parameters
        ----------
        config : dict
            Central config object
        """
        self.config = ConfigHandler.init_config(project_name,
                                                version,
                                                grant_service,
                                                outputdir,
                                                generation_merge_type,
                                                generation_group_type,
                                                sda_grant_service,
                                                remote_source)
        self.hana_helper = HanaGeneratorHelper(self.config)
        sql_processor = SqlProcessor(self.config)
        sql_processor.parse_sql_trace(connection_context)
        self._extend_config()

    def generate_artifacts(self, model_only=True, sda_data_source_mapping_only=False):
        """
        Generate the artifacts by first building up the required folder structure for artifacts storage and then
        generating the different required files. Be aware that this method only generates the generic files
        and offloads the generation of artifacts where traversal of base and consumption layer
        elements is required.

        Parameters
        ----------
        model_only: bool, optional
            In the sda case we are only interested in transferring the model using SDA.
            This forces the HANA artifacts generation to cater only for this scenario.

            Defaults to True.

        sda_data_source_mapping_only : bool
            In case data source mapping is provided you can forrce to only do this for the
            sda hdi container

            Defaults to False.


        Returns
        -------
        output_path : str
            Return the output path of the root folder where the related artifacts are stored.
        """
        HanaGenerator(self.config.get_entry(ConfigConstants.CONFIG_KEY_PROJECT_NAME),
                      self.config.get_entry(ConfigConstants.CONFIG_KEY_VERSION),
                      self.config.get_entry(ConfigConstants.CONFIG_KEY_GRANT_SERVICE),
                      self.config.get_entry(ConfigConstants.CONFIG_KEY_OUTPUT_DIR),
                      self.config.get_entry(ConfigConstants.CONFIG_KEY_MERGE_STRATEGY),
                      self.config.get_entry(ConfigConstants.CONFIG_KEY_GROUP_STRATEGY),
                      self.config.get_entry(ConfigConstants.CONFIG_KEY_GROUP_STRATEGY),
                      self.config.get_entry(ConfigConstants.CONFIG_KEY_SDA_GRANT_SERVICE),
                      self.config.get_entry(ConfigConstants.CONFIG_KEY_SDA_REMOTE_SOURCE),
                      ).generate_artifacts(True, True, sda_data_source_mapping_only)
        self.hana_helper._build_folder_structure(ConfigConstants.PROJECT_TEMPLATE_BASE_SDA_STRUCT,
                                                 self.config.get_entry(ConfigConstants.CONFIG_KEY_SDA_OUTPUT_PATH_HANA),
                                                 self.config.get_entry(
                                                     ConfigConstants.CONFIG_KEY_SDA_OUTPUT_PATH_MODULE))

        # Instantiate file writers
        yaml_writer = MTAYamlWriter(self.config)
        grant_writer = HDBGrantWriter(self.config)
        synonym_writer = HDBSynonymWriter(self.config)
        role_writer = HDBRoleWriter(self.config)
        consumption_processor = HanaSDAConsumptionProcessor(self.config)

        # Generate mta yaml file
        output_path = self.config.get_entry(ConfigConstants.CONFIG_KEY_SDA_OUTPUT_PATH_HANA)
        app_id = self.config.get_entry(ConfigConstants.CONFIG_KEY_SDA_APPID)
        module_name = self.config.get_entry(ConfigConstants.CONFIG_KEY_SDA_MODULE_NAME)
        version = self.config.get_entry(ConfigConstants.CONFIG_KEY_VERSION)
        schema = self.config.get_entry(ConfigConstants.CONFIG_KEY_SDA_SCHEMA)
        grant_service = self.config.get_entry(ConfigConstants.CONFIG_KEY_SDA_GRANT_SERVICE)
        yaml_writer.generate(output_path, app_id, module_name, version, schema, grant_service)

        # Generate hdbgrants
        remote_source = self.config.get_entry(ConfigConstants.CONFIG_KEY_SDA_REMOTE_SOURCE)
        grant_writer.generate(self.config.get_entry(ConfigConstants.CONFIG_KEY_SDA_GRANTS_PATH), remote_access=True,
                              remote_source=remote_source)

        # Generate dataset/function synonym
        synonym_writer.generate(self.config.get_entry(ConfigConstants.CONFIG_KEY_SDA_SYNONYMS_PATH))

        # Generate hdbroles for external access
        role_writer.generate(self.config.get_entry(ConfigConstants.CONFIG_KEY_SDA_ROLES_PATH),
                             name=self.config.get_entry(ConfigConstants.CONFIG_KEY_SDA_MODULE_NAME))

        # Generate Consumption Artifacts
        consumption_processor.generate(model_only)
        return output_path

    def _extend_config(self):
        """
        Extend the config to cater for HANA SDA generation specific config.
        """
        project_name = self.config.get_entry(ConfigConstants.CONFIG_KEY_PROJECT_NAME)
        sda_module_name = project_name + '_sda'
        sda_app_id = sda_module_name
        sda_schema = '"' + (sda_module_name + '_SCHEMA').upper() + '"'

        sda_output_path_hana = os.path.join(self.config.get_entry(ConfigConstants.CONFIG_KEY_OUTPUT_PATH),
                                            ConfigConstants.SDA_HANA_BASE_PATH)
        sda_output_path_module = os.path.join(sda_output_path_hana, sda_module_name)
        sda_output_path_module_src = os.path.join(sda_output_path_module, ConfigConstants.MODULE_SOURCE_PATH)
        sda_grants_path = os.path.join(sda_output_path_module_src, ConfigConstants.GRANTS_SOURCE_PATH)
        sda_synonyms_path = os.path.join(sda_output_path_module_src, ConfigConstants.SYNONYMS_SOURCE_PATH)
        sda_procedures_path = os.path.join(sda_output_path_module_src, ConfigConstants.PROCEDURES_SOURCE_PATH)
        sda_roles_path = os.path.join(sda_output_path_module_src, ConfigConstants.ROLES_SOURCE_PATH)
        sda_virtual_table_path = os.path.join(sda_output_path_module_src, ConfigConstants.VIRTUAL_TABLE_SOURCE_PATH)
        sda_cds_path = os.path.join(sda_output_path_module_src, ConfigConstants.CDS_SOURCE_PATH)

        self.config.add_entry(ConfigConstants.CONFIG_KEY_SDA_MODULE_NAME, sda_module_name)
        self.config.add_entry(ConfigConstants.CONFIG_KEY_SDA_APPID, sda_app_id)

        self.config.add_entry(ConfigConstants.CONFIG_KEY_SDA_OUTPUT_PATH_HANA, sda_output_path_hana)
        self.config.add_entry(ConfigConstants.CONFIG_KEY_SDA_OUTPUT_PATH_MODULE, sda_output_path_module)
        self.config.add_entry(ConfigConstants.CONFIG_KEY_SDA_OUTPUT_PATH_MODULE_SRC, sda_output_path_module_src)
        self.config.add_entry(ConfigConstants.CONFIG_KEY_SDA_GRANTS_PATH, sda_grants_path)
        self.config.add_entry(ConfigConstants.CONFIG_KEY_SDA_SYNONYMS_PATH, sda_synonyms_path)
        self.config.add_entry(ConfigConstants.CONFIG_KEY_SDA_PROCEDURES_PATH, sda_procedures_path)
        self.config.add_entry(ConfigConstants.CONFIG_KEY_SDA_ROLES_PATH, sda_roles_path)
        self.config.add_entry(ConfigConstants.CONFIG_KEY_SDA_VIRTUALTABLE_PATH, sda_virtual_table_path)
        self.config.add_entry(ConfigConstants.CONFIG_KEY_SDA_CDS_PATH, sda_cds_path)

        self.config.add_entry(ConfigConstants.CONFIG_KEY_SDA_SCHEMA, sda_schema)


class HanaSDAConsumptionProcessor(object):
    """
    This class provides HANA SDA specific generation functionality for the SDA HDI container.
    It utilizes the consumption layer as reference to generate the respective required
    artifacts.
    """

    def __init__(self, config):
        """
        This class allows to generate the arifacts for the SDA HDI container.

        Parameters
        ----------
        config : dict
            Central config object
        """
        self.hana_helper = HanaGeneratorHelper(config)
        self.config = config

    def generate(self, model_only=True):
        """
        Method for generating the actual artifacts content.

        Parameters
        ----------
        model_only : bool, optional
            In the sda case we are only interested in transferring the model using SDA.
            This forces the HANA artifacts generation to cater only for this scenario.

            Defaults to True.
        """
        cds_writer = HDBCDSWriter(self.config)
        procedure_writer = HDBProcedureWriter(self.config)
        sql_key_sql = SqlProcessor.TRACE_KEY_SQL_PROCESSED
        procedure_gen_filter = None
        if model_only:
            procedure_gen_filter = ['predict', 'partition']

        # Base SDA layer procedures
        self.hana_helper._build_base_layer_artifacts(
            self.config.get_entry(ConfigConstants.CONFIG_KEY_SDA_PROCEDURES_PATH), procedure_gen_filter,
            data_source_mapping=True)  # Always do datasource mapping

        # Consumption layer procedures
        sql_processed_cons_layer = self.config.get_entry(ConfigConstants.CONFIG_KEY_SQL_PROCESSED)[
            SqlProcessor.TRACE_KEY_CONSUMPTION_LAYER]
        # We create for the output tables cds tables which we generate here
        cds_sda_entries = []
        for element in sql_processed_cons_layer:
            if not isinstance(element, dict):
                continue  # ignore TODO: proper doc

            if sql_key_sql in element:  # TODO: gen warning if no sql
                proc_name = element['name']
                include_procedure = True

                # We only want the partition and predict procedures. For now others are taken out for the SDA
                # However the fit result we do want. So we only set a flag and process as normal, but we will
                # not generate the procedure artefact for this consumption layer element.
                if procedure_gen_filter:
                    if not any(gen_filter in proc_name for gen_filter in procedure_gen_filter):
                        include_procedure = False

                inputs = []
                output = []
                body = []

                if 'input' in element[sql_key_sql]:
                    inputs = element[sql_key_sql]['input']
                if 'body' in element[sql_key_sql]:
                    body = element[sql_key_sql]['body']
                if 'output' in element[sql_key_sql]:
                    output = element[sql_key_sql]['output']

                # Build SQL array (slight overhead due to inlude_proc flag. if false then no need to do the rest)
                # TODO refactor
                sql = []
                for item in inputs:
                    sql_str = item[sql_key_sql].format(*item['sql_vars'])
                    sql.append(sql_str)
                for item in body:
                    sql_str = item[sql_key_sql].format(*item['sql_vars'])
                    sql.append(sql_str)
                for item in output:
                    # Only generate cds entity if the procedure is included
                    if include_procedure:
                        sql_str = ''
                        if 'sql_vars_syn' in item and item['sql_vars_syn']:
                            sql_str = item[sql_key_sql].format(*item['sql_vars_syn'])
                        else:
                            sql_str = item[sql_key_sql].format(*item['sql_vars'])
                        sql.append(sql_str)
                        cds_sda_entries.append(self._generate_sda_cds(item, extend=False))

                    if model_only:
                        # Only generate SDA in case of MODEL output otherwise continue
                        # if not item['cat'] == 'MODEL':
                        if not self.config.is_model_category(item['cat']):
                            continue
                    # We force the model to also be created as antity as we need it for the load proc
                    cds_sda_entries.append(self._generate_sda_cds(item, extend=True))
                    self._generate_sda(proc_name, item)

                # Check if we need to filter out any procedure creation
                if not include_procedure:
                    continue

                # Explicitly disabling inputs on consumption layer as these are stand alone objects
                signature_str = self.hana_helper._build_procedure_signature(None, output)
                sql_str = StringUtils.flatten_string_array(sql)
                sql_str = self.config.data_source_mapping(sql_str)
                procedure_writer.generate(self.config.get_entry(ConfigConstants.CONFIG_KEY_SDA_PROCEDURES_PATH),
                                          proc_name, sql_str, signature_str)

        cds_sda_content = StringUtils.flatten_string_array(cds_sda_entries)
        cds_writer.generate(self.config.get_entry(ConfigConstants.CONFIG_KEY_SDA_CDS_PATH),
                            self.config.get_entry(ConfigConstants.CONFIG_KEY_CDS_CONTEXT), cds_sda_content)

    def _generate_sda(self, proc_name, item):
        """
        Method for generating the sda specific content for a consumption layer element.

        Parameters
        ----------
        proc_name: str
            In the sda case we are only interested in transferring the model using SDA.
            This forces the HANA artifacts generation to cater only for this scenario.

        item : dict
            Consumption layer element.
        """
        # Writers
        procedure_writer = HDBProcedureWriter(self.config)
        virtual_table_writer = HDBVirtualTableWriter(self.config)
        load_proc_name = 'load_' + proc_name + '_' + item[SqlProcessor.TRACE_KEY_TABLES_ATTRIB_INT_NAME]
        source_table = item[SqlProcessor.TRACE_KEY_TABLES_ATTRIB_DBOBJECT_NAME]
        source_schema = item[SqlProcessor.TRACE_KEY_TABLES_ATTRIB_SCHEMA]
        virtual_table = 'remote_' + item[SqlProcessor.TRACE_KEY_TABLES_ATTRIB_INT_NAME]
        target_table = item[
            SqlProcessor.TRACE_KEY_TABLES_ATTRIB_DBOBJECT_NAME]  # name in the SDA container is same as source container
        virtual_table_output_path = self.config.get_entry(ConfigConstants.CONFIG_KEY_SDA_VIRTUALTABLE_PATH)
        sda_load_proc_output_path = self.config.get_entry(ConfigConstants.CONFIG_KEY_SDA_PROCEDURES_PATH)
        remote_source = self.config.get_entry(ConfigConstants.CONFIG_KEY_SDA_REMOTE_SOURCE)
        virtual_table_writer.generate(virtual_table_output_path, remote_source, source_schema, source_table,
                                      virtual_table)
        load_sql = self._build_sda_load_sql(virtual_table, target_table, self._get_sda_cds_extension_values())
        procedure_writer.generate(sda_load_proc_output_path, load_proc_name, load_sql, '')

    def _generate_sda_cds(self, item, extend=False):
        """
        Method for generating the sda specific cds content for a consumption layer element.

        Parameters
        ----------
        item : dict
            Consumption layer element.

        extend : bool, optional
            Add additional fields which are specifically required for SDA

            Defaults to False.
        Returns
        -------
        cds_entity_item : dict
            The build cds entity item
        """
        cds_type_extension_values = self._get_sda_cds_extension_values()
        cds_type_extension = None
        if extend:
            cds_type_extension = self._build_sda_cds_type_extension(cds_type_extension_values)
        return self.hana_helper._build_cds_entity_entry(item['object_name'], item['cds_type'], cds_type_extension)

    def _get_sda_cds_extension_values(self):
        """
        Currently the additional required fields required for SDA generation are defined here.
        For it is not dynamically setup. But this can be catered for by extending this methd.

        Returns
        -------
        cds_type_extension_values : dict
            The build cds entity type additional SDA fields
        """
        cds_type_extension_values = [
            {
                'column': 'VERSION',
                'data_type': 'Integer',
                'sql': '1'
            },
            {
                'column': 'LOADED_ON',
                'data_type': 'UTCTimestamp',
                'sql': 'CURRENT_UTCTIMESTAMP'
            }
        ]  # TODO impove hardcoded version to support model versioning
        return cds_type_extension_values

    def _build_sda_cds_type_extension(self, elements):
        """
        Build the actual cds type extension

        Returns
        -------
        extension_str : str
            The extension string that needs to be appended to the cds entity type
        """
        indent = '      '
        extension_str = ''
        for element in elements:
            column = element['column']
            data_type = element['data_type']
            extension_str += indent + column + ' : ' + data_type + ';\n'
        return extension_str

    def _build_sda_load_sql(self, virtual_table, target_table, cds_type_extension_values=None):
        """
        Build the sda load sql for loading data from the first HDI container.

        Returns
        -------
        sql : str
            The sql that needs to be written as part of the procedure file
        """
        sql = 'TRUNCATE TABLE {}; \n'
        sql += 'INSERT INTO {} SELECT *'
        for extension in cds_type_extension_values:
            sql += ',' + extension['sql']
        sql += ' FROM "::{}";\n'
        sql += 'SELECT * FROM {};'
        return sql.format(target_table, target_table, virtual_table, target_table)


class HanaGeneratorHelper(object):
    """
    This class provides generic helper function for HANA generation functionality.
    It generates the files for the base layer elements.
    """

    def __init__(self, config):
        """
        This class provides helper methods when generating arifacts plus provides the
        generation of base layer artifacts which are HANA only.

        Parameters
        ----------
        config : dict
            Central config object
        """
        self.directory_handler = DirectoryHandler()
        self.config = config

    def _build_base_layer_artifacts(self, path, gen_filters=None, data_source_mapping=False):
        """
        Build the sda load sql for loading data from the first HDI container.

        Parameters
        ----------
        path : str
            output path of the base layer artifacts

        gen_filters : list, optional
            string list of procedures to include

            Defaults to None.
        data_source_mapping: bool, optional
            Whether to apply data source mapping

            Defaults to False.

        Returns
        -------
        sql : str
            The sql that needs to be written as part of the procedure file
        """
        procedure_writer = HDBProcedureWriter(self.config)
        sql_key_input = SqlProcessor.TRACE_KEY_TABLES_INPUT_PROCESSED
        sql_key_tables_output = SqlProcessor.TRACE_KEY_TABLES_OUTPUT_PROCESSED
        sql_key_vars_output = SqlProcessor.TRACE_KEY_VARS_OUTPUT_PROCESSED
        sql_key_sql = SqlProcessor.TRACE_KEY_SQL_PROCESSED
        sql_processed_base_layer = self.config.get_entry(ConfigConstants.CONFIG_KEY_SQL_PROCESSED)[
            SqlProcessor.TRACE_KEY_BASE_LAYER]
        for algo in sql_processed_base_layer:
            if not isinstance(sql_processed_base_layer[algo], dict):
                continue
            for function in sql_processed_base_layer[algo]:
                if sql_key_sql in sql_processed_base_layer[algo][function]:
                    proc_name = sql_processed_base_layer[algo][function][SqlProcessor.TRACE_KEY_METADATA_PROCESSED][
                        SqlProcessor.TRACE_KEY_METADATA_ATTRIB_PROC_NAME]
                    if gen_filters:
                        if not any(gen_filter in proc_name for gen_filter in gen_filters):
                            continue
                    inputs = []
                    output = []
                    if sql_key_input in sql_processed_base_layer[algo][function]:
                        inputs = sql_processed_base_layer[algo][function][sql_key_input]
                    if sql_key_tables_output in sql_processed_base_layer[algo][function]:
                        output = sql_processed_base_layer[algo][function][sql_key_tables_output]
                    if sql_key_vars_output in sql_processed_base_layer[algo][function]:
                        output.extend(sql_processed_base_layer[algo][function][sql_key_vars_output])

                    signature_str = self._build_procedure_signature(inputs, output)
                    sql_str = StringUtils.flatten_string_array(sql_processed_base_layer[algo][function][sql_key_sql])
                    if data_source_mapping:
                        sql_str = self.config.data_source_mapping(sql_str)
                    procedure_writer.generate(path, proc_name, sql_str, signature_str)

    def _build_cds_entity_entry(self, entity_name, cds_type, cds_type_extension=None):
        """
        Build up the cds entity entry string which can be used as element in the hdbdd file

        Parameters
        ----------
        entity_name : str
            Name of the entity
        cds_type : str
            Type to use for the entity
        cds_type_extension: str, optional
            Whether to add type extension fields.

        Returns
        -------
        cds_entry : str
            The cds entry string
        """
        if cds_type:
            indent = '  '
            cds_entry = indent + 'entity ' + entity_name + ' {\n'
            cds_entry += cds_type
            if cds_type_extension:
                cds_entry += cds_type_extension
            cds_entry += indent + '};\n'
            return cds_entry
        return None

    def _build_folder_structure(self, base_structure, output_path, module_output_path):
        """
        Build up the folder structure based on a template folder structure provided as part
        of the artifacts package. The templates are stored in:
        /artifacts/generators/<module_template_path>/<project_template_base_dir>/<base_structure>
        ie: /hana_ml_artifacts/generators/templates/hana/base_sda_structure

        Parameters
        ----------
        base_structure : str
            Physical location of the template base folder structure
        output_path : str
            Physical root target location which will be cleaned
        module_output_path: str
            Physical location of where the module (HDI container) needs to be populated with the
            respective required artifacts.
        """
        self._clean_folder_structure(output_path)

        # Parse and copy template base project structure
        module_template_path = self.config.get_entry(ConfigConstants.CONFIG_KEY_MODULE_TEMPLATE_PATH)
        base_structure_template_path = os.path.join(module_template_path, ConfigConstants.PROJECT_TEMPLATE_BASE_DIR,
                                                    base_structure)
        self.directory_handler.copy_directory(base_structure_template_path, module_output_path)

    def _clean_folder_structure(self, path):
        """
        Clean up physical folder structure.

        Parameters
        ----------
        path : str
            The physical location to clean.
        """
        if os.path.exists(path):
            self.directory_handler.delete_directory_content(path)
            os.rmdir(path)

    def _build_procedure_signature(self, input_tables, output_tables):
        """
        Based on input and output tables to generate the procedure signature.

        Parameters
        ----------
        input_tables : list
            list of input tables.
        output_tables : list
            list of output tables.

        Returns
        -------
        signature_str: str
            the signature string which can be used in the procedure (hdbprocedure)
        """
        signature_str = ''

        # Input
        if input_tables:
            signature_str += self._build_procedure_interface(input_tables, 'in')
            if output_tables:
                signature_str += ', '

        # Output
        if output_tables:
            signature_str += self._build_procedure_interface(output_tables, 'out')

        return signature_str

    def _build_procedure_interface(self, items, str_type):
        """
        Build input or output signature part.

        Parameters
        ----------
        items : list
            list of items to generate as part of this signature part.
        str_type : str
            whether it is the in or the out part of the procedure signature.

        Returns
        -------
        interface_str: str
            the signature part of these items
        """
        sql_key_ttype = SqlProcessor.TRACE_KEY_TABLES_ATTRIB_TYPE
        sql_key_vtype = SqlProcessor.TRACE_KEY_VARS_ATTRIB_DATA_TYPE
        sql_key_name = SqlProcessor.TRACE_KEY_TABLES_ATTRIB_INT_NAME
        interface_str = ''
        for idx, item in enumerate(items):
            if idx > 0:
                interface_str += ', '
            interface_str += str_type
            if sql_key_ttype in item:
                interface_str += ' ' + item[sql_key_name] + ' ' + item[sql_key_ttype]
            if sql_key_vtype in item:
                interface_str += ' ' + item[sql_key_name] + ' ' + item[sql_key_vtype]
        return interface_str
