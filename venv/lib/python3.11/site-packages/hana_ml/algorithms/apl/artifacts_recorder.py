"""
Module for SQL artifacts recording.
During the different steps, such as fit, predict and score, the sql queries were recorded.
Thoses info will be restitued later for in HANA design-time artifacts generation
(see module hana_ml.artifacts.generators.hana).
"""

from hana_ml.algorithms.apl.sqlgen import AplSqlGeneratorForArtifactsGen

#pylint: disable= too-many-instance-attributes
#pylint: disable=too-few-public-methods
class AplSqlArtifactsRecorder:
    """
    Class used as a recorder of the SQL artifacts which are stored in the class instance variables (_fit_call, _fit_anonymous_block, etc)
    All the sql artifacts generation (strings generation) are indeed executed in the AplSqlGeneratorForArtifactsGen class.
    """
    def __init__(self, conn):
        self.connection_context = conn
        # Attributes requested in Artifact Generator (such as HANAGeneratorForCAP)
        self._fit_call = None  # 'CALL _SYS_AFL."APL_CREATE_MODEL_AND_TRAIN"(...)
        self._fit_args = None  # [{table_name | select_statement}*]
        self._fit_output_table_names = None  #[{table_name}*]
        self._fit_anonymous_block = None  # 'DO BEGIN .... END;'
        self._predict_call = None  # 'CALL _SYS_AFL."APL_CREATE_MODEL_AND_TRAIN"(...)
        self._predict_args = None  # [{table_name | select_statement}*]
        self._predict_output_table_names = None  #[{table_name}*]
        self._predict_anonymous_block = None  # 'DO BEGIN .... END;'
        self._fit_predict_call = None  # 'CALL _SYS_AFL."APL_CREATE_MODEL_AND_TRAIN"(...)
        self._fit_predict_args = None  # [{table_name | select_statement}*]
        self._fit_predict_output_table_names = None  #[{table_name}*]
        self._fit_predict_anonymous_block = None  # 'DO BEGIN .... END;'
        self._score_call = None  # 'CALL _SYS_AFL."APL_CREATE_MODEL_AND_TRAIN"(...)
        self._score_args = None  # [{table_name | select_statement}*]
        self._score_output_table_names = None  #[{table_name}*]
        self._score_anonymous_block = None  # 'DO BEGIN .... END;'

    def __call__(self, model_method, apl_function, input_tables, output_tables, filled_table_names=None):
        """
        Records the SQL artifacts that can be restituted later in HANA design-time artifacts generation
        see module src/hana_ml/artifacts/generators/hana.py
        Parameters:
        ----------
        model_function: str
            Should be 'fit', 'predict', 'fit_predict','score'
        apl_function: str
            The lower level APL function (example: 'APL_CREATE_MODEL_AND_TRAIN')
        input_tables : list of APLArtifactTable or ArtifactView
            Those instances will provide the name and the definition of the underlying table/view.
        output_tables : list Table (mandatory)
            The output table names to be placed in procedure call
        """
        # SQL statement generator
        sqlgen = AplSqlGeneratorForArtifactsGen(
            conn=self.connection_context,
            funcname=apl_function,
            input_tables=input_tables,
            output_tables=output_tables,
            filled_table_names=filled_table_names
        )
        def _remove_prefix(astring, prefix):
            return astring[len(prefix):] if astring.startswith(prefix) else astring

        if model_method == 'fit':
            self._fit_anonymous_block = sqlgen.generate()
            self._fit_call = _remove_prefix(sqlgen.call_statement, "CALL ")
            self._fit_args = sqlgen.call_args
            self._fit_output_table_names = sqlgen.output_tablenames
        elif model_method == 'predict':
            self._predict_anonymous_block = sqlgen.generate()
            self._predict_call = _remove_prefix(sqlgen.call_statement, "CALL ")
            self._predict_args = sqlgen.call_args
            self._predict_output_table_names = sqlgen.output_tablenames
        elif model_method == 'fit_predict':
            self._fit_predict_anonymous_block = sqlgen.generate()
            self._fit_predict_call = _remove_prefix(sqlgen.call_statement, "CALL ")
            self._fit_predict_args = sqlgen.call_args
            self._fit_predict_output_table_names = sqlgen.output_tablenames
        elif model_method == 'score':
            self._score_anonymous_block = sqlgen.generate()
            self._score_call = _remove_prefix(sqlgen.call_statement, "CALL ")
            self._score_args = sqlgen.call_args
            self._score_output_table_names = sqlgen.output_tablenames
        else:
            raise ValueError(f"Unexpected argument value model_method={model_method}")

    def _show(self):
        """
        For debug purpose
        """
        print("---- Show recorder")
        for att_name in [
            '_fit_call', '_fit_args', '_fit_output_table_names', '_fit_anonymous_block',
            '_predict_call', '_predict_args', '_predict_output_table_names', '_predict_anonymous_block',
            '_fit_predict_call', '_fit_predict_args', '_fit_predict_output_table_names', '_fit_predict_anonymous_block',
            '_score_call', '_score_args', '_score_output_table_names', '_score_anonymous_block',
        ]:
            attrval = getattr(self, att_name)
            if attrval is not None:
                print(f"{att_name}:\n{attrval}\n")
