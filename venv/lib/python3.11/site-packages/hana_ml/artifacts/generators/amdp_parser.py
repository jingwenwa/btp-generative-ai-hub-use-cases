#pylint: disable=too-few-public-methods
"""
This module parses AMDP (ABAP Managed Database Procedure) related artifacts based on the provided
consumption layer elements. Currently this is experimental code only.

The following class is available:

    * :class:`AMDPParser`

"""

import re

from hana_ml.artifacts.utils import StringUtils
from hana_ml.artifacts.config import ConfigConstants

NULL_VALUE = 'NULL'

class AMDPParser(object):
    """
    This static class parses AMDP (ABAP Managed Database Procedure) specific artifacts and generates replacements
    which are used to create an AMDP class from a template

 .. note::
    Supported hana-ml algorithm for AMDP: **UnifiedClassification**.
    """

    @staticmethod
    def generate_replacements(amdp_class_name, training_cds_view_name, apply_cds_view_name, train_input_sql_structure, apply_input_sql_structure, no_reason_features, train_sql_parameters, apply_sql_parameters, target_column_name):
        """
        Generate replacements defined in a AMDP template.

        Parameters
        ----------
        amdp_class_name : str, required
            Name of training dataset.
        training_cds_view_name : str, required
            Name of training CDS (core data services) view.
        apply_cds_view_name : str, required
            Name of apply CDS (core data services) view.
        train_input_sql_structure : str, required
            string that defines input sql structure for training
            Example: 'table_type': 'table ("ID" INT, "PREGNANCIES" INT, "GLUCOSE" INT, "SKINTHICKNESS" INT)
        apply_input_sql_structure : str, required
            string that defines input sql structure for prediction
            Example: 'table_type': 'table ("ID" INT, "PREGNANCIES" INT, "GLUCOSE" INT, "SKINTHICKNESS" INT)
        no_reason_features : int, required
            The number of features that contribute to the classification decision the most.
            This reason code information is to be displayed during the prediction phase.
        train_sql_parameters : list[str], required
            string that defines PAL SQL parameters for training
            Example: ["param_name[1] := N'FUNCTION';", 'int_value[1] := NULL;', 'double_value[1] := NULL;', "string_value[1] := N'RDT';"]
        apply_sql_parameters : list[str], required
            string that defines PAL SQL parameters for apply/prediction
            Example: ["param_name[1] := N'FUNCTION';", 'int_value[1] := NULL;', 'double_value[1] := NULL;', "string_value[1] := N'RDT';"]
        target_column_name : str, required
            name of target column
        """

        train_input_abap_structure = AMDPParser._generate_abap_structure(training_cds_view_name, train_input_sql_structure)
        apply_output_abap_structure = AMDPParser._generate_abap_structure(apply_cds_view_name, apply_input_sql_structure)
        apply_output_abap_structure = AMDPParser._adjust_apply_output_abap_structure(apply_output_abap_structure, target_column_name)
        apply_output_abap_reason_code_structure = AMDPParser._generate_reason_code_output_abap_structure(no_reason_features)
        parameters = AMDPParser._parse_params(train_sql_parameters, "train") + AMDPParser._parse_params(apply_sql_parameters, "apply")
        model_parameters, model_parameter_defaults = AMDPParser._build_parameters_and_default_values(parameters)
        apply_input_abap_structure = AMDPParser._generate_apply_abap_input_structure(train_input_abap_structure, target_column_name)
        apply_output_sql_structure = AMDPParser._generate_apply_output_sql_structure(apply_input_sql_structure, apply_cds_view_name, target_column_name)
        if no_reason_features > 0:
            apply_output_sql_structure += ","
        apply_output_reason_code_sql_structure = AMDPParser._generate_reason_code_output_sql_structure(no_reason_features)

        replacements = {
            ConfigConstants.AMDP_TEMPLATE_AMDP_NAME_PLACEHOLDER: amdp_class_name.lower(),
            ConfigConstants.AMDP_TEMPLATE_TRAIN_INPUT_STRUCTURE: train_input_abap_structure,
            # ConfigConstants.AMDP_TEMPLATE_INPUT_COLUMNS_WITHOUT_KEY: (",".join(
            #     re.sub("(TYPE [^,]+)|\n", '', train_structure_in).split(',')[1:-1])).replace(" ", "").replace(",", ", "),

            # Prediction structure not custom defined, as it's always the exact same as coming from pal itself unless
            # you remodel it inside the `predict_with_model_version` inside the abap class after the pal call. But this
            # isn't yet possible from inside the python code:
            ConfigConstants.AMDP_TEMPLATE_PREDICTION_STRUCTURE: apply_output_abap_structure,

            ConfigConstants.AMDP_TEMPLATE_RESULT_FIELDS: apply_output_sql_structure,
            ConfigConstants.AMDP_TEMPLATE_PARAMETER: model_parameters,
            ConfigConstants.AMDP_TEMPLATE_PARAMETER_DEFAULT: model_parameter_defaults,
            ConfigConstants.AMDP_TEMPLATE_TARGET_COLUMN: target_column_name.upper(),
            ConfigConstants.AMDP_TEMPLATE_TRAINING_DATASET: training_cds_view_name.upper(),
            ConfigConstants.AMDP_TEMPLATE_APPLY_DATASET: apply_cds_view_name.upper(),
            ConfigConstants.AMDP_TEMPLATE_REASON_CODE: apply_output_abap_reason_code_structure,
            ConfigConstants.AMDP_TEMPLATE_RESULT_REASON_CODE_FIELDS: apply_output_reason_code_sql_structure,
            ConfigConstants.AMDP_TEMPLATE_PREDICT_DATA_COLS: apply_input_abap_structure
        }

        return replacements

    @staticmethod
    def _generate_abap_structure(training_dataset, input_sql_table_type):
        # input_sql_table_type is fit_input
        if input_sql_table_type == "":
            raise ValueError("Input sql table type is empty")
        if training_dataset == "":
            raise ValueError("Training dataset name is empty")
        train_input_structure = ""
        column_names = re.findall('"([A-Za-z_]+)"[A-Za-z0-9 ]+,?', input_sql_table_type)
        if len(column_names) == 0:
            return ""
        for column_name in column_names:
            train_input_structure += ' ' * 8 + f"{column_name} type {training_dataset}-{column_name},\n"
        train_input_structure = train_input_structure[:-1]
        return train_input_structure.lower().replace(" type ", " TYPE ")

    @staticmethod
    def _adjust_apply_output_abap_structure(abap_structure, target_column_name):
        result_abap_structure = ""
        columns = re.findall(r'([A-Za-z_]+ TYPE [A-Za-z0-9\-_]+,?)', abap_structure)
        if len(columns) == 0:
            return ""
        for column in columns:
            column_name = re.findall(r'([A-Za-z_]+) TYPE [A-Za-z0-9\-_]+,?', column)[0]
            if column_name.lower() == "score":
                result_abap_structure += ' ' * 8 + re.sub("score", target_column_name, column) + "\n"
            elif column_name.lower() == "confidence":
                result_abap_structure += ' ' * 8 + re.sub("[A-Za-z0-9_]+-confidence", "shemi_predict_confidence", column) + "\n"
            elif column_name.lower() == "reason_code":
                result_abap_structure += ' ' * 8 + re.sub("[A-Za-z0-9_]+-reason_code", "shemi_reason_code", column) + "\n"
            else:
                result_abap_structure += ' ' * 8 + f"{column}\n"
        result_abap_structure = result_abap_structure[:-1]
        return result_abap_structure.lower().replace(" type ", " TYPE ")

    @staticmethod
    def _generate_reason_code_output_abap_structure(no_reason_code_features):
        reason_code_abap_structure = ''
        for item in range(1, no_reason_code_features + 1):
            reason_code_abap_structure += ' ' * 8 + f"reason_code_feature_{item} TYPE shemi_reason_code_feature_name,\n"
            reason_code_abap_structure += ' ' * 8 + f"reason_code_percentage_{item} TYPE shemi_reason_code_feature_pct,\n"
        reason_code_abap_structure = reason_code_abap_structure[:-1]
        return reason_code_abap_structure

    @staticmethod
    def _generate_reason_code_output_sql_structure(no_reason_code_features):
        output_reason_code_sql_structure = ''
        for item in range(1, no_reason_code_features + 1):
            output_reason_code_sql_structure += ' ' * 24 + \
                    f"""trim(both '"' from json_query(result.reason_code, '$[{item - 1}].attr')) as reason_code_feature_{item},\n"""
            output_reason_code_sql_structure += ' ' * 24 + \
                    f"""json_query(result.reason_code, '$[{item - 1}].pct' ) as reason_code_percentage_{item},\n"""
        output_reason_code_sql_structure = output_reason_code_sql_structure[:-2]
        return output_reason_code_sql_structure

    @staticmethod
    def _generate_apply_abap_input_structure(train_input_structure, target_column_name):
        predict_input_columns = ""
        train_input_columns = re.findall("([A-Za-z_]+) TYPE", train_input_structure)
        for column_name in train_input_columns:
            if column_name != target_column_name.lower():
                predict_input_columns += ' ' * 18 + f"{column_name},\n"
        predict_input_columns = predict_input_columns.lower()
        predict_input_columns = predict_input_columns[:-2]
        return predict_input_columns

    @staticmethod
    def _generate_apply_output_sql_structure(result_output_structure, cds_view_name, target_column_name):
        # result_output_structure is predict_output['table_type']
        if result_output_structure == "":
            raise ValueError("Input sql table type is empty")
        if result_output_structure == "":
            raise ValueError("Training dataset name is empty")
        apply_output_sql_structure = ""
        column_names = re.findall('"([A-Za-z_]+)"[A-Za-z0-9 ]+,?', result_output_structure)
        if len(column_names) == 0:
            return ""
        for column_name in column_names:
            column_name = column_name.lower()
            abap_type = ""
            if column_name == "confidence":
                abap_type = "shemi_predict_confidence"
            elif column_name == "score":
                abap_type = f"{cds_view_name}-{target_column_name}"
            elif column_name == "reason_code":
                abap_type = "shemi_reason_code"
            else:
                abap_type = f"{cds_view_name}-{column_name}"
            column_name_alias = ""
            if column_name == "score":
                column_name_alias = target_column_name
            else:
                column_name_alias = column_name

            apply_output_sql_structure += ' ' * 24 + f'cast(result.{column_name} as "$ABAP.type( {abap_type} )") as {column_name_alias},\n'

        apply_output_sql_structure = apply_output_sql_structure[:-2]
        return apply_output_sql_structure.lower().replace(" type ", " TYPE ").replace("$abap.type", "$ABAP.type")

    @staticmethod
    def _build_parameters_and_default_values(params):

        # Initiate the each table definition with `Value #( `
        model_params = ConfigConstants.AMDP_TEMPLATE_MODEL_PARAM_INIT
        model_params_default = ConfigConstants.AMDP_TEMPLATE_MODEL_PARAM_INIT

        for _name, _value, _type, _role in params:
            model_params += StringUtils.multi_replace(
                ConfigConstants.AMDP_TEMPLATE_MODEL_PARAM_SAMPLE,
                {
                    ConfigConstants.AMDP_TEMPLATE_MODEL_PARAM_SAMPLE_NAME: _name,
                    ConfigConstants.AMDP_TEMPLATE_MODEL_PARAM_SAMPLE_ROLE: _role,
                    ConfigConstants.AMDP_TEMPLATE_MODEL_PARAM_SAMPLE_TYPE: _type,
                    ConfigConstants.AMDP_TEMPLATE_MODEL_PARAM_SAMPLE_CONFIGURABLE: ConfigConstants.AMDP_TEMPLATE_ABAP_TRUE,  # Only ABAP_FALSE/ABAP_TRUE possible
                    ConfigConstants.AMDP_TEMPLATE_MODEL_PARAM_SAMPLE_CONTEXT: ConfigConstants.AMDP_TEMPLATE_ABAP_FALSE  # Only ABAP_FALSE/ABAP_TRUE possible
                }
            )
            if _name not in model_params_default:
                model_params_default += StringUtils.multi_replace(
                    ConfigConstants.AMDP_TEMPLATE_MODEL_PARAM_DEFAULT_SAMPLE,
                    {
                        ConfigConstants.AMDP_TEMPLATE_MODEL_PARAM_DEFAULT_SAMPLE_NAME: _name,
                        # _value can be string, int or float, therefore cast it to str
                        ConfigConstants.AMDP_TEMPLATE_MODEL_PARAM_DEFAULT_SAMPLE_VALUE: str(_value)
                    })

        # Wrap up the value definitions with ` )`
        model_params += ConfigConstants.AMDP_TEMPLATE_MODEL_PARAM_END
        model_params_default += ConfigConstants.AMDP_TEMPLATE_MODEL_PARAM_END
        return model_params, model_params_default

    @staticmethod
    def _parse_params(param_sql_raw, role):
        """
        Parse sql lines containing the parameter definitions. In the sql code all the parameters
        are defined by four arrays, where the first one contains the parameter name, and one of the other
        three contains the value fitting to the parameter, while the other two are NULL. This format
        should be changed into a simple key-value based storage.

        Parameters
        ----------
        param_sql_raw : List
            A list of sql statements each of them belonging to the param definition section
        role : str
            The role defines whether the sql belongs to training or applying
            only possible values are `train` and `apply`, otherwise throws

        Returns
        -------
            array of tuples, where each tuple describes a parameter like (name, value, role)

        """
        if role not in ("train", "apply"):
            raise ValueError("role value can only be 'train' or 'apply', NOT {}".format(role))
        params = []
        param_names = []
        for i in range(0, len(param_sql_raw), 4):
            name = AMDPParser._parse_line(param_sql_raw[i])
            param_i = AMDPParser._parse_line(param_sql_raw[i + 1])
            param_d = AMDPParser._parse_line(param_sql_raw[i + 2])
            param_s = AMDPParser._parse_line(param_sql_raw[i + 3])
            if param_i == NULL_VALUE and param_d == NULL_VALUE:
                if name not in param_names:
                    params.append((name, param_s, "string", role))
                    param_names.append(name)
                else:
                    params[param_names.index(name)] = (name, params[param_names.index(name)][1] + ',' + param_s, "string", role)
            elif param_i == NULL_VALUE and param_s == NULL_VALUE:
                params.append((name, float(param_d), "double", role))
                param_names.append(name)
            elif param_d == NULL_VALUE and param_s == NULL_VALUE:
                params.append((name, int(param_i), "integer", role))
                param_names.append(name)
        return params

    @staticmethod
    def _parse_line(_sql):
        """
        Parse a single line from the param definitions from sql to get the name or the value of the parameter

        Parameters
        ----------
        _sql: str
            parameter definition sql line

        Returns
        -------
        Either the name of the following parameter, or the value

        Examples
        --------
        param_name[0] := N'PARAMETER_NAME'; -> PARAMETER_NAME
        string_value[0] := N'VALUE'; -> VALUE
        double_value[0] := NULL; -> NULL
        int_value[0] := 1; -> 1
        """
        return re.findall(":= (?:N')?([0-9A-Za-z_. \\[\\],{}-]+)'?;", _sql)[0]
