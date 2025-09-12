"""
This module contains Python wrappers for PAL preprocessing algorithms.

The following classes and functions are available:

    * :class:`FeatureNormalizer`
    * :Class:`FeatureSelection`
    * :class:`IsolationForest`
    * :class:`KBinsDiscretizer`
    * :class:`Imputer`
    * :class:`Discretize`
    * :class:`MDS`
    * :class:`SMOTE`
    * :class:`SMOTETomek`
    * :class:`TomekLinks`
    * :class:`Sampling`
    * :class:`ImputeTS`
    * :class:`PowerTransform`
    * :class:`QuantileTransform`
    * :func:`variance_test`
"""

#pylint: disable=line-too-long, unused-variable, raise-missing-from, too-many-locals, super-with-arguments, use-a-generator
#pylint: disable=too-many-lines, invalid-name, too-many-arguments, c-extension-no-member, bare-except, consider-using-dict-items
#pylint: disable=attribute-defined-outside-init, too-many-statements, too-many-branches, too-many-instance-attributes, simplifiable-if-expression
import logging
import uuid
import json
import itertools
from deprecated import deprecated
from hdbcli import dbapi
from hana_ml.ml_exceptions import FitIncompleteError
from .utility import check_pal_function_exist, _map_param, _key_index_check
from .tsa.utility import _delete_none_key_in_dict, _col_index_check
from .sqlgen import trace_sql
from .pal_base import (
    PALBase,
    ParameterTable,
    INTEGER,
    DOUBLE,
    NVARCHAR,
    ListOfStrings,
    ListOfTuples,
    arg,
    pal_param_register,
    require_pal_usable,
    call_pal_auto_with_hint,
    try_drop
)
logger = logging.getLogger(__name__)

def _params_check(input_dict, param_map):
    update_params = {}
    if not input_dict or input_dict is None:
        return update_params

    for parm in input_dict:
        if parm in param_map.keys():
            parm_val = input_dict[parm]
            arg_map = param_map[parm]
            if len(arg_map) == 2:
                update_params[arg_map[0]] = (arg(parm, parm_val, arg_map[1]), arg_map[1])
            else:
                update_params[arg_map[0]] = (arg(parm, parm_val, arg_map[2]), arg_map[1])
        else:
            err_msg = f"'{parm}' is not a valid parameter name for initializing a Isolation Forest model!"
            logger.error(err_msg)
            raise KeyError(err_msg)
    return update_params

class IsolationForest(PALBase):
    r"""
    Isolation Forest generates anomaly scores for each sample.

    Parameters
    ----------

    n_estimators : int, optional
        Specifies the number of trees to grow.

        Defaults to 100.

    max_samples : int, optional
        Specifies the number of samples to draw from input to train each tree.
        If ``max_samples`` is larger than the number of samples provided,
        all samples will be used for all trees.

        Defaults to 256.

    max_features : int, optional
        Specifies the number of features to draw from input to train each tree.
        0 means no sampling.

        Defaults to 0.

    bootstrap : bool, optional
        Specifies the sampling method.

        - False: Sampling without replacement.
        - True: Sampling with replacement.

        Defaults to False.

    random_state : int, optional
        Specifies the seed for the random number generator.

        - 0: Uses the current time (in seconds) as the seed.
        - Others: Uses the specified value as the seed.

        Defaults to 0.

    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored, and this function heuristically determines the number of threads to use.

        Defaults to -1.

    massive : bool, optional
        Specifies whether or not to use massive mode.

        - True: massive mode.
        - False: single mode.

        For parameter settings in massive mode, you can use both
        group_params (please see the example below) or the original parameters.
        Using original parameters will apply to all groups. However, if you define some parameters for a group,
        the value of all original parameter settings will not be applicable to such a group.

        An example is as follows:

        .. only:: latex

            >>> mif = IsolationForest(massive=True,
                                      random_state=2,
                                      group_params={'Group_1': {'n_estimators': 50}})

        .. raw:: html

            <iframe allowtransparency="true" style="border:1px solid #ccc; background: #eeffcb;"
                 src="../../_static/isolation_example.html" width="100%" height="60%" sandbox="">
            </iframe>

        In this example, as 'n_estimators' is set in group_params for Group_1,
        the parameter setting of 'random_state' is not applicable to Group_1.

        Defaults to False.

    group_params : dict, optional
        If massive mode is activated (``massive`` is True),
        input data shall be divided into different groups with different parameters applied.

        An example is as follows:

        .. only:: latex

            >>> mif = IsolationForest(massive=True,
                                      random_state=2,
                                      group_params={'Group_1': {'n_estimators': 50}})
        .. raw:: html

            <iframe allowtransparency="true" style="border:1px solid #ccc; background: #eeffcb;"
                 src="../../_static/isolation_example.html" width="100%" height="60%" sandbox="">
            </iframe>

        Valid only when ``massive`` is True and defaults to None.

    Attributes
    ----------
    model_ : DataFrame
        Model content.

    error_msg_ : DataFrame
        Error message.
        Only valid if ``massive`` is True when initializing an 'IsolationForest' instance.

    Examples
    --------

    >>> isof = IsolationForest(random_state=2, thread_ratio=0)
    >>> isof.fit(data=df_fit, key='ID', features=['V000', 'V001'])
    >>> res = isof.predict(data=df_predict,
                           key='ID',
                           features=['V000', 'V001'],
                           contamination=0.25)
    >>> res.collect()
    """
    __init_param_dict = {'n_estimators' : ('N_ESTIMATORS', int),
                         'max_samples' : ('MAX_SAMPLES', int),
                         'max_features' : ('MAX_FEATURES', int),
                         'bootstrap' : ('BOOTSTRAP', int, bool),
                         'random_state' : ('SEED', int),
                         'thread_ratio' : ('THREAD_RATIO', float)}

    __predict_param_dict = {'contamination' : ('CONTAMINATION', float),
                            'thread_ratio' : ('THREAD_RATIO', float),
                            'explain_scope' : ('EXPLAIN_SCOPE', int, {'outliers': 0, 'all': 1}),
                            'top_k_attributions' : ('TOP_K_ATTRIBUTIONS', int)}

    def __init__(self,
                 n_estimators=None,
                 max_samples=None,
                 max_features=None,
                 bootstrap=None,
                 random_state=None,
                 thread_ratio=None,
                 massive=False,
                 group_params=None):

        super(IsolationForest, self).__init__()
        setattr(self, 'hanaml_parameters', pal_param_register())

        init_params = {'n_estimators' : n_estimators,
                       'max_samples' : max_samples,
                       'max_features' : max_features,
                       'bootstrap' : bootstrap,
                       'random_state' : random_state,
                       'thread_ratio' : thread_ratio}
        init_params = _delete_none_key_in_dict(init_params)
        self.init_params = init_params
        self.__pal_params = {}
        self.__pal_predict_params = {}

        self.massive = self._arg('massive', massive, bool)
        if self.massive is not True:
            self.__pal_params = _params_check(input_dict=self.init_params,
                                              param_map=self.__init_param_dict)
        else: # massive mode
            group_params = self._arg('group_params', group_params, dict)
            group_params = {} if group_params is None else group_params
            for group in group_params:
                self._arg('Parameters with GROUP ID ' + str(group), group_params[group], dict)
            self.group_params = group_params

            for group in self.group_params:
                self.__pal_params[group] = _params_check(input_dict=self.group_params[group],
                                                         param_map=self.__init_param_dict)
            if self.init_params:
                special_group_name = 'PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID'
                self.__pal_params[special_group_name] = _params_check(input_dict=self.init_params,
                                                                      param_map=self.__init_param_dict)

        if self.massive is True:
            self.error_msg_= None

    @trace_sql
    def fit(self,
            data,
            key=None,
            features=None,
            group_key=None):
        """
        Fit the model to the training dataset.

        Parameters
        ----------
        data : DataFrame

            DataFrame containing the data.

        key : str, optional

            Name of the ID column.

            If ``key`` is not provided, then:

                - if ``data`` is indexed by a single column, then ``key`` defaults
                  to that index column
                - otherwise, it is assumed that ``data`` contains no ID column

        features : str or a list of str, optional

            Names of the feature columns.

            If ``features`` is not provided, it defaults to all
            non-key, non-group_key (if massive=True) columns.

        group_key : str, optional
            The column of group_key. The data type can be INT or NVARCHAR/VARCHAR.
            This parameter is only valid when ``massive`` is True.

            Defaults to the first column of data if the index columns of data is not provided.
            Otherwise, defaults to the first column of index columns.

        Returns
        -------
        A fitted object of class "IsolationForest".
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        conn = data.connection_context
        require_pal_usable(conn)
        param_rows = []

        if self.massive is not True: # single mode
            has_id = False
            index_col = None
            key = self._arg('key', key, str)
            index = data.index
            if isinstance(index, str):
                if key is not None and index != key:
                    msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                    "and the designated index column '{}'.".format(index)
                    logger.warning(msg)
            key = index if key is None else key
            if key is not None:
                has_id = True
                index_col = key
            cols_left = data.columns
            if has_id:
                cols_left.remove(index_col)
            if features is None:
                features = cols_left
            used_cols = [col for col in itertools.chain([index_col], features)
                         if col is not None]
            training_df = data[used_cols]

            for name in self.__pal_params:
                value, typ = self.__pal_params[name]
                tpl=[_map_param(name, value, typ)]
                param_rows.extend(tpl)
            param_rows.extend([('HAS_ID', has_id, None, None)])
            unique_id = str(uuid.uuid1()).replace('-', '_').upper()
            model_tbl = '#PAL_ISOLATION_FOREST_MODEL_TBL_{}_{}'.format(self.id, unique_id)
            outputs = [model_tbl]

            try:
                self._call_pal_auto(conn,
                                    'PAL_ISOLATION_FOREST',
                                    training_df,
                                    ParameterTable().with_data(param_rows),
                                    *outputs)
            except dbapi.Error as db_err:
                msg = str(conn.hana_version())
                logger.exception("HANA version: %s. %s", msg, str(db_err))
                try_drop(conn, outputs)
                raise
            except Exception as db_err:
                msg = str(conn.hana_version())
                logger.exception("HANA version: %s. %s", msg, str(db_err))
                try_drop(conn, outputs)
                raise

            self.model_ = conn.table(model_tbl)
            return self

        # massive mode
        index = data.index
        cols = data.columns
        group_key = self._arg('group_key', group_key, str)
        if index is not None:
            group_key = _col_index_check(group_key, 'group_key', index[0], cols)
        else:
            if group_key is None:
                group_key = cols[0]

        if group_key is not None and group_key not in cols:
            msg = f"Please select group_key from {cols}!"
            logger.error(msg)
            raise ValueError(msg)
        data_groups = list(data[[group_key]].collect()[group_key].drop_duplicates())
        param_keys = list(self.group_params.keys())
        if not self._disable_hana_execution:
            gid_type = {tp[0]:tp for tp in data.dtypes()}[group_key]
            if not all([(int(ky) if 'INT' in gid_type[1] else ky) in data_groups for ky in param_keys]):
                msg = 'Invalid group key identified in group parameters!'
                logger.error(msg)
                raise ValueError(msg)

        group_id = [group_key]
        cols.remove(group_key)

        key = self._arg('key', key, str)
        if index is not None:
            key = _key_index_check(key, 'key', index[1])
        has_id = False
        if key is not None:
            id_col = [key]
            has_id = True
            cols.remove(key)
        else:
            id_col = []

        if isinstance(features, str):
            features = [features]
        features = self._arg('features', features, ListOfStrings)
        if features is None:
            features = cols

        training_df = data[group_id + id_col + features]
        param_rows.extend([('PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID', 'HAS_ID', has_id, None, None)])
        for group in self.__pal_params:
            if self.__pal_params[group]:
                for name in self.__pal_params[group]:
                    value, typ = self.__pal_params[group][name]
                    tpl = [tuple([group] + list(_map_param(name, value, typ)))]
                    param_rows.extend(tpl)
            param_rows.extend([(group, 'HAS_ID', has_id, None, None)])

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        outputs = ['MODEL', 'ERROR_MSG']
        outputs = ['#PAL_MASSIVE_ISOLATION_FOREST_{}_TBL_{}_{}'.format(tbl, self.id, unique_id)
                   for tbl in outputs]
        model_tbl, error_msg_tbl = outputs
        if not (check_pal_function_exist(conn, '%MASSIVE_ISOLATION_FOREST%', like=True) or \
        self._disable_hana_execution):
            msg = 'The version of your SAP HANA does not support massive Isolation Forest!'
            logger.error(msg)
            raise ValueError(msg)
        try:
            self._call_pal_auto(conn,
                                'PAL_MASSIVE_ISOLATION_FOREST',
                                training_df,
                                ParameterTable().with_data(param_rows),
                                *outputs)
        except dbapi.Error as db_err:
            logger.error(str(db_err))
            try_drop(conn, outputs)
            raise
        except Exception as db_err:
            logger.error(str(db_err))
            try_drop(conn, outputs)
            raise

        self.model_ = conn.table(model_tbl)
        self.error_msg_ = conn.table(error_msg_tbl)
        return self

    @trace_sql
    def predict(self,
                data,
                key=None,
                features=None,
                contamination=None,
                thread_ratio=None,
                group_key=None,
                group_params=None,
                show_explainer=False,
                explain_scope=None,
                top_k_attributions=None):
        r"""
        Obtain the anomaly score of each sample based on the given Isolation Forest model.

        Parameters
        ----------

        data : DataFrame

            DataFrame containing the data.

        key : str, optional

            Name of the ID column.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        features : a list of str, optional

            Names of the feature columns.

            If ``features`` is not provided, it defaults to all
            non-key columns.

        contamination : float, optional

            The proportion of outliers in the dataset. Should be in the range (0, 0.5].

            Defaults to 0.1.

        thread_ratio : float, optional
            Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
            Values outside the range will be ignored and this function heuristically determines the number of threads to use.

            Defaults to -1.

        group_key : str, optional
            The column of group_key. Data type can be INT or NVARCHAR/VARCHAR.
            This parameter is only valid when ``massive`` is set as True in
            class instance initialization.

            Defaults to the first column of data if the index columns of data is not provided.
            Otherwise, defaults to the first column of index columns.

        group_params : dict, optional
            If massive mode is activated (``massive`` is set as True in class instance initialization), input data
            shall be divided into different groups with different parameters applied.
            This parameter specifies the parameter values of different groups in a dict format, where keys corresponding to ``group_key``
            while values should be a dict for parameter value assignments.

            An example is as follows:

            .. only:: latex

                >>> mif = IsolationForest(massive=True,
                                          random_state=2,
                                          group_params={'Group_1': {'n_estimators':50}})
                >>> mif.fit(data=df, key="ID", group_key="GROUP_ID", features=['F1', 'F2'])
                >>> res, err = mif.predict(data=df_predict, key="ID", group_key="GROUP_ID", features=['F1', 'F2'],
                                           group_params={'Group_1': {'contamination':0.2}})

            .. raw:: html

                <iframe allowtransparency="true" style="border:1px solid #ccc; background: #eeffcb;"
                    src="../../_static/isolation_predict_example.html" width="100%" height="100%" sandbox="">
                </iframe>

            Valid only when ``massive`` is set as True in class instance initialization.

            Defaults to None.

        show_explainer : bool, optional
            If True, output the shapley value in the REASON_CODE column.

            Defaults to False.

        explain_scope : str, optional
            Defines the scope of explanation.
                - 'outliers': Only outliers
                - 'all': All samples

            Available when ``show_explainer`` is True.

            Defaults to 'outliers'.

        top_k_attributions : int, optional
            Specifies the number (k) of key features to output that have the most contribution to the model's predictions or outcomes.

            Available when ``show_explainer`` is True.

            Defaults to 10.

        Returns
        -------
        DataFrame 1
            The forecast values, structured as follows:

            - GROUP_ID, group key column name. (only valid if ``massive`` is True when initializing an 'IsolationForest' instance)
            - ID, ID column name.
            - SCORE, type DOUBLE, scoring result.
            - LABEL, type INTEGER, -1 for outliers and 1 for inliers.
            - REASON_CODE, type DOUBLE, Shapley value. (available only if ``show_explainer`` is True).

        DataFrame 2
            Error message.
            Only valid if ``massive`` is True when initializing an 'IsolationForest' instance.
        """
        setattr(self, 'hanaml_predict_params', pal_param_register())
        if getattr(self, 'model_') is None:
            raise FitIncompleteError()
        conn = data.connection_context

        predict_params = {'contamination' : contamination,
                          'thread_ratio' : thread_ratio,
                          'top_k_attributions' : top_k_attributions,
                          'explain_scope' : explain_scope}
        predict_params = _delete_none_key_in_dict(predict_params)
        self.predict_params = predict_params

        show_explainer = self._arg('show_explainer', show_explainer, bool)

        # Single mode
        if self.massive is not True:
            index = data.index
            key = self._arg('key', key, str, required=not isinstance(index, str))
            if isinstance(index, str):
                if key is not None and index != key:
                    msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                    "and the designated index column '{}'.".format(index)
                    logger.warning(msg)
            key = index if key is None else key
            features = self._arg('features', features, ListOfStrings)
            thread_ratio = self._arg('thread_ratio', thread_ratio, float)
            contamination = self._arg('contamination', contamination, float)

            cols = data.columns
            cols.remove(key)
            if features is None:
                features = cols
            data_ = data[[key] + features]

            param_rows = [
                ("THREAD_RATIO", None, thread_ratio, None),
                ("CONTAMINATION", None, contamination, None)]
            if show_explainer:
                explain_scope_map = {'outliers': 0, 'all': 1}
                param_rows.append(('EXPLAIN_SCOPE', explain_scope_map[explain_scope], None, None))
                param_rows.append(('TOP_K_ATTRIBUTIONS', top_k_attributions, None, None))
            unique_id = str(uuid.uuid1()).replace('-', '_').upper()
            result_tbl = "#PAL_ISOLATION_FOREST_RESULT_TBL_{}_{}".format(self.id, unique_id)
            if show_explainer:
                pal_procedure = 'PAL_ISOLATION_FOREST_EXPLAIN'
            else:
                pal_procedure = 'PAL_ISOLATION_FOREST_PREDICT'
            if show_explainer is True and not (check_pal_function_exist(conn, '%ISOLATION_FOREST_EXPLAIN%', like=True) or \
            self._disable_hana_execution):
                msg = 'The version of SAP HANA does not support isolation forest explainer. Please set show_explainer=False!'
                logger.error(msg)
                raise ValueError(msg)
            try:
                self._call_pal_auto(conn,
                                    pal_procedure,
                                    data_,
                                    self.model_,
                                    ParameterTable().with_data(param_rows),
                                    result_tbl)
            except dbapi.Error as db_err:
                logger.exception(str(db_err))
                try_drop(conn, result_tbl)
                raise
            except Exception as db_err:
                logger.exception(str(db_err))
                try_drop(conn, result_tbl)
                raise

            return conn.table(result_tbl)

        # massive mode
        group_params = self._arg('group_params', group_params, dict)
        if group_params:
            for group in group_params:
                self._arg(' Parameters with group_key ' + str(group),
                          group_params[group], dict)

        cols = data.columns
        index = data.index
        group_id = []

        group_key = self._arg('group_key', group_key, str)
        if index is not None:
            group_key = _col_index_check(group_key, 'group_key', index[0], cols)
        else:
            if group_key is None:
                group_key = cols[0]

        if group_key is not None and group_key not in cols:
            msg = f"Please select group_key from {cols}!"
            logger.error(msg)
            raise ValueError(msg)
        data_groups = list(data.collect()[group_key].drop_duplicates())
        param_keys = list(self.group_params.keys())
        if not self._disable_hana_execution:
            gid_type = {tp[0]:tp for tp in data.dtypes()}[group_key]
            if not all([(int(ky) if 'INT' in gid_type[1] else ky) in data_groups for ky in param_keys]):
                msg = 'Invalid group key identified in group parameters!'
                logger.error(msg)
                raise ValueError(msg)
        group_id = [group_key]
        cols.remove(group_key)

        key = self._arg('key', key, str)
        if index is not None:
            key = _col_index_check(key, 'key', index[1], cols)
        else:
            key = self._arg('key', key, str, required=True)

        if key is not None and key not in cols:
            msg = f"Please select key from {cols}!"
            logger.error(msg)
            raise ValueError(msg)
        cols.remove(key)

        if isinstance(features, str):
            features = [features]
        features = self._arg('features', features, ListOfStrings)
        if features is None:
            features = cols
        data_ = data[group_id + [key] + features]

        special_group_name = 'PAL_MASSIVE_PROCESSING_SPECIAL_GROUP_ID'
        param_rows = []
        general_params = {}
        general_params = _params_check(input_dict=self.predict_params,
                                       param_map=self.__predict_param_dict)

        if general_params:
            self.__pal_predict_params[special_group_name] = general_params

        # for each group
        if group_params:
            for group in group_params:
                each_group_params = {}
                each_group_params = _params_check(input_dict=group_params[group],
                                                  param_map=self.__predict_param_dict)
                if each_group_params:
                    self.__pal_predict_params[group] = each_group_params

        for group in self.__pal_predict_params:
            if self.__pal_predict_params[group]:
                for name in self.__pal_predict_params[group]:
                    value, typ = self.__pal_predict_params[group][name]
                    tpl = [tuple([group] + list(_map_param(name, value, typ)))]
                    param_rows.extend(tpl)

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        result_tbl = "#PAL_MASSIVE_ISOLATION_FOREST_RESULT_PREDICT_TBL_{}_{}".format(self.id, unique_id)
        error_msg_tbl = "#PAL_MASSIVE_ISOLATION_FOREST_ERROR_PREDICT_TBL_{}_{}".format(self.id, unique_id)
        outputs = result_tbl, error_msg_tbl
        if show_explainer:
            pal_procedure = 'PAL_MASSIVE_ISOLATION_FOREST_EXPLAIN'
        else:
            pal_procedure = 'PAL_MASSIVE_ISOLATION_FOREST_PREDICT'
        try:
            if check_pal_function_exist(conn, '%MASSIVE_ISOLATION_FOREST%', like=True) or self._disable_hana_execution:
                self._call_pal_auto(conn,
                                    pal_procedure,
                                    data_,
                                    self.model_,
                                    ParameterTable().with_data(param_rows),
                                    *outputs)
            else:
                msg = 'The version of your SAP HANA does not support massive isolation forest!'
                logger.error(msg)
                raise ValueError(msg)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, result_tbl)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, result_tbl)
            raise

        return conn.table(result_tbl), conn.table(error_msg_tbl)

    @trace_sql
    def fit_predict(self,
                    data,
                    key=None,
                    features=None,
                    contamination=None,
                    thread_ratio=None,
                    group_key=None,
                    group_params=None,
                    show_explainer=False,
                    explain_scope=None,
                    top_k_attributions=None):
        """
        Train the isolation forest model and return labels for input data.

        Parameters
        ----------

        data : DataFrame

            DataFrame containing the data.

        key : str, optional

            Name of the ID column.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        features : a list of str, optional

            Names of the feature columns.

            If ``features`` is not provided, it defaults to all
            non-key columns.

        contamination : float, optional

            The proportion of outliers in the dataset. Should be in the range (0, 0.5].

            Defaults to 0.1.

        thread_ratio : float, optional
            Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
            Values outside the range will be ignored, and this function heuristically determines the number of threads to use.

            Defaults to -1.
        group_key : str, optional
            The column of group_key. Data type can be INT or NVARCHAR/VARCHAR.
            This parameter is only valid when ``massive`` is set as True in
            class instance initialization.

            Defaults to the first column of data if the index columns of data are not provided.
            Otherwise, defaults to the first column of index columns.

        group_params : dict, optional
            If massive mode is activated (``massive`` is set as True in class instance initialization), input data
            shall be divided into different groups with different parameters applied.
            This parameter specifies the parameter values of different groups in a dict format, where keys correspond to ``group_key``
            while values should be a dict for parameter value assignments.

            An example is as follows:

            .. only:: latex

            >>> mif = IsolationForest(massive=True,
                          random_state=2,
                          group_params={'Group_1': {'n_estimators': 50}})
            >>> mif.fit_predict(data=df, key="ID", group_key="GROUP_ID", features=['F1', 'F2'],
                        group_params={'Group_1': {'contamination': 0.2}})

            .. raw:: html

            <iframe allowtransparency="true" style="border:1px solid #ccc; background: #eeffcb;"
                src="../../_static/isolation_fit_predict_example.html" width="100%" height="100%" sandbox="">
            </iframe>

            Valid only when ``massive`` is set as True in class instance initialization.

            Defaults to None.

        show_explainer : bool, optional
            If True, output the shapley value in the REASON_CODE column.

            Defaults to False.

        explain_scope : str, optional
            Defines the scope of explanation.
                - 'outliers': Only outliers
                - 'all': All samples

            Available when ``show_explainer`` is True.

            Defaults to 'outliers'.

        top_k_attributions : int, optional
            Specifies the number (k) of key features to output that have the most contribution to the model's predictions or outcomes.

            Available when ``show_explainer`` is True.

            Defaults to 10.

        Returns
        -------

        DataFrame 1
            The forecast values, structured as follows:

            - GROUP_ID, group key column name. (only valid if ``massive`` is True when initializing an 'IsolationForest' instance)
            - ID, type INTEGER, ID column name.
            - SCORE, type DOUBLE, scoring result.
            - LABEL, type INTEGER, -1 for outliers and 1 for inliers.
            - SHAP_VALUE, type DOUBLE, Shapley value. (available only if ``show_explainer`` is True).

        DataFrame 2

            Error message.
            Only valid if ``massive`` is True when initializing an 'IsolationForest' instance.
        """
        if self.massive is not True:  #Single mode
            self.fit(data, key, features)
            fit_runtime = self.runtime
            res = self.predict(data=data, key=key, features=features,
                               contamination=contamination, thread_ratio=thread_ratio,
                               show_explainer=show_explainer, explain_scope=explain_scope, top_k_attributions=top_k_attributions)
            predict_runtime = self.runtime
            self.runtime = fit_runtime + predict_runtime
            return res

        # Massive mode
        self.fit(data, key, features, group_key)
        fit_runtime = self.runtime
        res, error_msg = self.predict(data=data, key=key, features=features,
                                      contamination=contamination, thread_ratio=thread_ratio, group_key=group_key, group_params=group_params,
                                      show_explainer=show_explainer, explain_scope=explain_scope, top_k_attributions=top_k_attributions)
        predict_runtime = self.runtime
        self.runtime = fit_runtime + predict_runtime
        return res, error_msg

class FeatureNormalizer(PALBase):
    """
    Normalize a DataFrame. In real world scenarios the collected continuous attributes are usually distributed within different ranges.
    It is a common practice to have the data well scaled so that data mining algorithms like neural networks,
    nearest neighbor classification and clustering can give more reliable results.

    .. Note::

       Note that the data type of the output value is the same as that of the input value.
       Therefore, if the data type of the original data is INTEGER, the output value will be converted to an integer instead of the result you expect.

       For example, if we want to use min-max method to normalize a list [1, 2, 3, 4] and set new_min = 0 and new_max = 1.0,
       we want the result to be [0, 0.33, 0.66, 1], but actually the output is [0, 0, 0, 1]
       due to the rule of consistency of input and output data type.

       Therefore, please cast the feature column(s) from INTEGER to be DOUBLE before invoking the function.

    Parameters
    ----------
    method : {'min-max', 'z-score', 'decimal'}

        Scaling methods:

            - 'min-max': Min-max normalization.
            - 'z-score': Z-Score normalization.
            - 'decimal': Decimal scaling normalization.

    z_score_method : {'mean-standard', 'mean-mean', 'median-median'} or dict, optional

        If `z_score_methods` is not dict, only valid when ``method`` is 'z-score'.

            - 'mean-standard': Mean-Standard deviation
            - 'mean-mean': Mean-Mean deviation
            - 'median-median': Median-Median absolute deviation

        If `z_score_methods` is dict, it specifies the columns for different methods.

    new_max : float, optional

        The new maximum value for min-max normalization.

        Only valid when ``method`` is 'min-max'.

    new_min : float, optional

        The new minimum value for min-max normalization.

        Only valid when ``method`` is 'min-max'.


    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 0.

    division_by_zero_handler : bool or str, optional

        - False or 'ignore': Ignores the column when encountering a division by zero,
          so the column is not scaled.
        - True or 'abort': Throws an error when encountering a division by zero.

        Defaults to True.

    Attributes
    ----------

    result_ : DataFrame
        Scaled dataset from fit and fit_transform methods.

    model_ : DataFrame
        Model content.

    Examples
    --------
    >>> fn = FeatureNormalizer(method="min-max", new_max=1.0, new_min=0.0)
    >>> fn.fit(data=df_train, key='ID')
    >>> res = fn.transform(data=df_transform, key='ID')
    >>> res.collect()
    """

    method_map = {'min-max': 0, 'z-score': 1, 'decimal': 2}
    z_score_method_map = {'mean-standard': 0, 'mean-mean': 1, 'median-median': 2}

    def __init__(self,
                 method=None,
                 z_score_method=None,
                 new_max=None,
                 new_min=None,
                 thread_ratio=None,
                 division_by_zero_handler=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(FeatureNormalizer, self).__init__()
        self.op_name = 'SCALE'
        self.method = self._arg('method', method, self.method_map, required=True if not isinstance(z_score_method, dict) else False)
        if not isinstance(z_score_method, dict):
            self.z_score_method = self._arg('z_score_method', z_score_method, self.z_score_method_map)
        else:
            self.z_score_method = dict((kkey, self.z_score_method_map[vval]) for kkey, vval in z_score_method.items())
        self.new_max = self._arg('new_max', new_max, float)
        self.new_min = self._arg('new_min', new_min, float)
        self.thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        self.division_by_zero_handler = self._arg('division_by_zero_handler',
                                                  division_by_zero_handler, (bool, str))
        if isinstance(division_by_zero_handler, str):
            self.division_by_zero_handler = self._arg('division_by_zero_handler',
                                                      division_by_zero_handler,
                                                      dict(ignore=0, abort=1))
        if z_score_method is not None:
            if not isinstance(z_score_method, dict):
                if method.lower() != 'z-score':
                    msg = 'z_score_method is not applicable when scale method is not z-score.'
                    logger.error(msg)
                    raise ValueError(msg)
        else:
            if method.lower() == 'z-score':
                msg = 'z_score_method must be provided when scale method is z-score.'
                logger.error(msg)
                raise ValueError(msg)
        if not isinstance(z_score_method, dict):
            if method.lower() == 'min-max':
                if new_min is None or new_max is None:
                    msg = 'new_min and new_max must be provided when scale method is min-max.'
                    logger.error(msg)
                    raise ValueError(msg)

            if method.lower() != 'min-max':
                if new_min is not None or new_max is not None:
                    msg = 'new_min or new_max is not applicable when scale method is not min-max.'
                    logger.error(msg)
                    raise ValueError(msg)

    def fit(self, data, key=None, features=None):
        """
        Normalize input data and generate a scaling model using one of the three
        scaling methods: min-max normalization, z-score normalization and
        normalization by decimal scaling.

        Parameters
        ----------

        data : DataFrame

            DataFrame to be normalized.

        key : str, optional

            Name of the ID column.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        features : a list of str, optional

            Names of the feature columns.
            If ``features`` is not provided, it defaults to all non-ID columns.

        Returns
        -------
        A fitted object of class "FeatureNormalizer".
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        conn = data.connection_context
        features = self._arg('features', features, ListOfStrings)
        if not self._disable_hana_execution:
            require_pal_usable(conn)
            index = data.index
            key = self._arg('key', key, str, required=not isinstance(index, str))
            if isinstance(index, str):
                if key is not None and index != key:
                    msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                    "and the designated index column '{}'.".format(index)
                    logger.warning(msg)
            key = index if key is None else key
            cols = data.columns
            cols.remove(key)
            if features is None:
                features = cols

            data_ = data[[key] + features]
        else:
            data_ = data
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        outputs = ['RESULT', 'MODEL', 'STATISTIC', 'PLACEHOLDER']
        outputs = ['#PAL_FN_{}_TBL_{}_{}'.format(name, self.id, unique_id)
                   for name in outputs]
        result_tbl, model_tbl, statistic_tbl, placeholder_tbl = outputs

        param_rows = [
            ('SCALING_METHOD', self.method, None, None),
            ('THREAD_RATIO', None, self.thread_ratio, None),
            ('NEW_MAX', None, self.new_max, None),
            ('NEW_MIN', None, self.new_min, None),
            ('DIVISION_BY_ZERO_HANDLER', self.division_by_zero_handler, None, None)
            ]
        if not isinstance(self.z_score_method, dict):
            param_rows.extend([('Z-SCORE_METHOD', self.z_score_method, None, None)])
        else:
            for kkey, vval in self.z_score_method.items():
                param_rows.extend([('{}_Z-SCORE_METHOD'.format(kkey), vval, None, None)])
        try:
            self._call_pal_auto(conn,
                                'PAL_SCALE',
                                data_,
                                ParameterTable().with_data(param_rows),
                                *outputs)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, outputs)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, outputs)
            raise
        if not self._disable_hana_execution:
            self.result_ = conn.table(result_tbl)
            self.model_ = conn.table(model_tbl)
        return self

    def fit_transform(self, data, key=None, features=None):
        r"""
        Fit with the dataset and return the results.

        Parameters
        ----------

        data : DataFrame
            DataFrame to be normalized.

        key : str, optional
            Name of the ID column.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        features : a list of str, optional
            Names of the feature columns.
            If ``features`` is not provided, it defaults to all non-ID columns.

        Returns
        -------

        DataFrame
            Normalized result, with the same structure as ``data``.
        """
        self.fit(data, key, features)
        if self._disable_hana_execution:
            return data
        return self.result_

    def transform(self, data,
                  key=None,
                  features=None,
                  thread_ratio=None,
                  division_by_zero_handler=None):
        r"""
        Scales data based on the previous scaling model.

        Parameters
        ----------

        data : DataFrame
            DataFrame to be normalized.

        key : str, optional
            Name of the ID column.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        features : a list of str, optional
            Names of the feature columns.

            If ``features`` is not provided, it defaults to all non-ID columns.

        thread_ratio : float, optional
            Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
            Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 0.

        division_by_zero_handler : str, optional
            Specifies the system behavior when division-by-zero is encountered when scaling
            ``data`` using a fitted model.

              - 'ignore': Ignores the column when encountering a division by zero,
                so the column is not scaled.
              - 'abort': Throws an error when encountering a division by zero.

            Defaults to 'abort'.

        Returns
        -------

        DataFrame
            Normalized result, with the same structure as ``data``.
        """
        conn = data.connection_context
        require_pal_usable(conn)
        if getattr(self, 'model_') is None:
            raise FitIncompleteError()
        index = data.index
        key = self._arg('key', key, str, required=not isinstance(index, str))
        if isinstance(index, str):
            if key is not None and index != key:
                msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(msg)
        key = index if key is None else key
        features = self._arg('features', features, ListOfStrings)
        cols = data.columns
        cols.remove(key)
        if features is None:
            features = cols
        thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        division_by_zero_handler = self._arg('division_by_zero_handler',
                                             division_by_zero_handler,
                                             dict(ignore=0, abort=1))
        data_ = data[[key] + features]
        param_rows = [('THREAD_RATIO', None, thread_ratio, None),
                      ('DIVISION_BY_ZERO_HANDLER', division_by_zero_handler,
                       None, None)]

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()

        result_tbl = '#PAL_FN_RESULT_TBL_{}_{}'.format(self.id, unique_id)

        try:
            self._call_pal_auto(conn,
                                'PAL_SCALE_WITH_MODEL',
                                data_,
                                self.model_,
                                ParameterTable(param_rows),
                                result_tbl)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, result_tbl)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, result_tbl)
            raise
        return conn.table(result_tbl)

class KBinsDiscretizer(PALBase):
    r"""
    Bin continuous data into number of intervals and perform local smoothing.

    .. Note::

       Note that the data type of the output value is the same as that of the input value.
       Therefore, if the data type of the original data is INTEGER, the output value will be converted to an integer instead of the result you expect.

       Therefore, please cast the feature column(s) from INTEGER to be DOUBLE before invoking the function.

    Parameters
    ----------

    strategy : {'uniform_number', 'uniform_size', 'quantile', 'sd'}
        Specifies the binning method, valid options include:

            - 'uniform_number': Equal widths based on the number of bins.
            - 'uniform_size': Equal widths based on the bin width.
            - 'quantile': Equal number of records per bin.
            - 'sd': Bins are divided based on the distance from the mean.
              Most bins are one standard deviation wide, except that the
              center bin contains all values within one standard deviation
              from the mean, and the leftmost and rightmost bins contain
              all values more than ``n_sd`` standard deviations from the
              mean in the corresponding directions.

    smoothing : {'means', 'medians', 'boundaries'}
        Specifies the smoothing method, valid options include:

            - 'means': Each value within a bin is replaced by the average of
              all the values belonging to the same bin.
            - 'medians': Each value in a bin is replaced by the median of all
              the values belonging to the same bin.
            - 'boundaries': The minimum and maximum values in a given bin are
              identified as the bin boundaries.
              Each value in the bin is then replaced by its closest boundary value.
              When the distance is equal to both sides, it will be replaced by the front boundary value.

        Values used for smoothing are not re-calculated during transform.

    n_bins : int, optional
        The number of bins.

        Only valid when ``strategy`` is 'uniform_number' or 'quantile'.

        Defaults to 2.
    bin_size : int, optional
        The interval width of each bin.

        Only valid when ``strategy`` is 'uniform_size'.

        Defaults to 10.
    n_sd : int, optional
        The leftmost bin contains all values located further than n_sd
        standard deviations lower than the mean, and the rightmost bin
        contains all values located further than n_sd standard deviations
        above the mean.

        Only valid when ``strategy`` is 'sd'.

        Defaults to 1.

    Attributes
    ----------
    result_ : DataFrame
        Binned dataset from fit and fit_transform methods.

    model_ : DataFrame
        Model content.

    Examples
    --------
    >>> binning = KBinsDiscretizer(strategy='uniform_size', smoothing='means', bin_size=10)
    >>> binning.fit(data=df_train, key='ID')
    >>> res = binning.transform(data=df_transform, key='ID')
    >>> res.collect()
    """

    strategy_map = {'uniform_number': 0, 'uniform_size': 1, 'quantile': 2, 'sd': 3}
    smooth_map = {'means': 0, 'medians': 1, 'boundaries': 2}

    def __init__(self,
                 strategy,
                 smoothing,
                 n_bins=None,
                 bin_size=None,
                 n_sd=None):
        super(KBinsDiscretizer, self).__init__()
        setattr(self, 'hanaml_parameters', pal_param_register())
        self.pal_funcname = 'PAL_BINNING_ASSIGNMENT'
        self.strategy = self._arg('strategy', strategy, self.strategy_map, required=True)
        self.smoothing = self._arg('smoothing', smoothing, self.smooth_map, required=True)
        self.n_bins = self._arg('n_bins', n_bins, int)
        self.bin_size = self._arg('bin_size', bin_size, int)
        self.n_sd = self._arg('n_sd', n_sd, int)
        #following checks are based on PAL docs, pal example has 'sd' with uniform_size
        #tested that pal ignores SD in actual executions
        if (strategy.lower() != 'uniform_number' and strategy.lower() != 'quantile'
                and n_bins is not None):
            msg = "n_bins is only applicable when strategy is uniform_number or quantile."
            logger.error(msg)
            raise ValueError(msg)
        if strategy.lower() != 'uniform_size' and bin_size is not None:
            msg = "bin_size is only applicable when strategy is uniform_size."
            logger.error(msg)
            raise ValueError(msg)
        if strategy.lower() != 'sd' and n_sd is not None:
            msg = "n_sd is only applicable when strategy is sd."
            logger.error(msg)
            raise ValueError(msg)

    def fit(self, data, key=None, features=None):
        """
        Bin input data into number of intervals and smooth.

        Parameters
        ----------

        data : DataFrame
            DataFrame to be discretized.
        key : str, optional
            Name of the ID column.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        features : a list of str, optional
            Names of the feature columns.

            Since the underlying PAL binning algorithm only supports one feature,
            this list can only contain one element.

            If not provided, ``data`` must have exactly 1 non-ID column, and ``features`` defaults to that column.

        Returns
        -------
        A fitted object of class "KBinsDiscretizer".
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        conn = data.connection_context
        require_pal_usable(conn)
        index = data.index
        key = self._arg('key', key, str, required=not isinstance(index, str))
        if isinstance(index, str):
            if key is not None and index != key:
                msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(msg)
        key = index if key is None else key
        features = self._arg('features', features, ListOfStrings)

        cols = data.columns
        cols.remove(key)
        if features is None:
            features = cols

        if len(features) != 1:
            msg = ('PAL binning requires exactly one ' +
                   'feature column.')
            logger.error(msg)
            raise TypeError(msg)

        data_ = data[[key] + features]
        #PAL_BINNING requires stats and placeholder table which is not mentioned in PAL doc
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        outputs = ['RESULT', 'MODEL', 'STATISTIC', 'PLACEHOLDER']
        outputs = ['#PAL_BINNING_{}_TBL_{}_{}'.format(name, self.id, unique_id)
                   for name in outputs]
        result_tbl, model_tbl, statistic_tbl, placeholder_tbl = outputs

        param_rows = [
            ('BINNING_METHOD', self.strategy, None, None),
            ('SMOOTH_METHOD', self.smoothing, None, None),
            ('BIN_NUMBER', self.n_bins, None, None),
            ('BIN_DISTANCE', self.bin_size, None, None),
            ('SD', self.n_sd, None, None)
            ]

        try:
            self._call_pal_auto(conn,
                                'PAL_BINNING',
                                data_,
                                ParameterTable().with_data(param_rows),
                                *outputs)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, outputs)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, outputs)
            raise

        self.result_ = conn.table(result_tbl)
        self.model_ = conn.table(model_tbl)
        return self

    def fit_transform(self, data, key=None, features=None):
        """
        Fit with the dataset and return the results.

        Parameters
        ----------

        data : DataFrame
            DataFrame to be binned.
        key : str, optional
            Name of the ID column.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        features : a list of str, optional
            Names of the feature columns.

            Since the underlying PAL binning algorithm only supports one feature,
            this list can only contain one element.

            If not provided, ``data`` must have exactly 1
            non-ID column, and ``features`` defaults to that column.

        Returns
        -------
        DataFrame
            Binned result, structured as follows:

              - DATA_ID column: with same name and type as ``data``'s ID column.
              - BIN_INDEX: type INTEGER, assigned bin index.
              - BINNING_DATA column: smoothed value, with same name and
                type as ``data``'s feature column.

        """

        self.fit(data, key, features)
        return self.result_

    def transform(self, data, key=None, features=None):
        """
        Bin data based on the previous binning model.

        Parameters
        ----------
        data : DataFrame
            DataFrame to be binned.
        key : str, optional
            Name of the ID column.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.
        features : a list of str, optional
            Names of the feature columns.

            Since the underlying PAL_BINNING_ASSIGNMENT only supports one feature, this list can
            only contain one element.

            If not provided, ``data`` must have exactly 1 non-ID column, and ``features`` defaults to that column.

        Returns
        -------
        DataFrame
            Binned result, structured as follows:

              - DATA_ID column: with same name and type as ``data`` 's ID column.
              - BIN_INDEX: type INTEGER, assigned bin index.
              - BINNING_DATA column: smoothed value, with same name and
                type as ``data`` 's feature column.

        """
        conn = data.connection_context
        require_pal_usable(conn)
        if getattr(self, 'model_') is None:
            raise FitIncompleteError()
        index = data.index
        key = self._arg('key', key, str, required=not isinstance(index, str))
        if isinstance(index, str):
            if key is not None and index != key:
                msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(msg)
        key = index if key is None else key
        features = self._arg('features', features, ListOfStrings)

        cols = data.columns
        cols.remove(key)
        if features is None:
            features = cols

        if len(features) != 1:
            msg = ('PAL binning assignment requires exactly one ' +
                   'feature column.')
            logger.error(msg)
            raise TypeError(msg)

        data_ = data[[key] + features]

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        result_tbl = '#PAL_BINNING_RESULT_TBL_{}_{}'.format(self.id, unique_id)
        param_rows = []
        try:
            self._call_pal_auto(conn,
                                'PAL_BINNING_ASSIGNMENT',
                                data_,
                                self.model_,
                                ParameterTable(param_rows),
                                result_tbl)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, result_tbl)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, result_tbl)
            raise
        return conn.table(result_tbl)

    def create_model_state(self, model=None, function=None,
                           pal_funcname='PAL_BINNING_ASSIGNMENT',
                           state_description=None, force=False):
        r"""
        Create PAL model state.

        Parameters
        ----------
        model : DataFrame, optional
            Specify the model for AFL state.

            Defaults to self.model\_.

        function : str, optional
            Specify the function name of the classification algorithm.

            Valid only for UnifiedClassification and UnifiedRegression.

            Defaults to self.real_func

        pal_funcname : int or str, optional
            PAL function name.

            Defaults to 'PAL_BINNING_ASSIGNMENT'.

        state_description : str, optional
            Description of the state as model container.

            Defaults to None.

        force : bool, optional
            If True it will delete the existing state.

            Defaults to False.
        """
        super()._create_model_state(model, function, pal_funcname, state_description, force)

    def set_model_state(self, state):
        """
        Set the model state by state information.

        Parameters
        ----------
        state: DataFrame or dict
            If state is DataFrame, it has the following structure:

                - NAME: VARCHAR(100), it mush have STATE_ID, HINT, HOST and PORT.
                - VALUE: VARCHAR(1000), the values according to NAME.

            If state is dict, the key must have STATE_ID, HINT, HOST and PORT.
        """
        super()._set_model_state(state)

    def delete_model_state(self, state=None):
        """
        Delete PAL model state.

        Parameters
        ----------
        state : DataFrame, optional
            Specified the state.

            Defaults to self.state.
        """
        super()._delete_model_state(state)

class Imputer(PALBase):
    r"""
    Missing value imputation for DataFrame.

    Parameters
    ----------
    strategy : {'non', 'most_frequent-mean', 'most_frequent-median', 'most_frequent-zero', 'most_frequent_als', 'delete'}, optional
        Specifies the **overall** imputation strategy.

            - 'non' : No imputation for *all* columns.
            - 'most_frequent-mean' : Replacing missing values in any categorical column by its most frequently observed value, and
              missing values in any numerical column by its mean.
            - 'most_frequent-median' : Replacing missing values in any categorical column by its most frequently observed value,
              and missing values in any numerical column by its median.
            - 'most_frequent-zero' : Replacing missing values in any categorical column by its most frequently observed value, and
              missing values in all numerical columns by zeros.
            - 'most_frequent-als' : Replacing missing values in any categorical column by
              its most frequently observed value, and filling the missing values in all numerical columns via a
              matrix completion technique called **alternating least squares**.
            - 'delete' : Delete *all* rows with missing values.

        Defaults to 'most_frequent-mean'.

    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 0.0.

        .. note::

            The following parameters all have pre-fix 'als\_', and are invoked only when 'als' is the overall imputation strategy. \
            Those parameters are for setting up the alternating-least-square(ALS) model for data imputation.

    als_factors : int, optional

        Length of factor vectors in the ALS model.

        It should be less than the number of numerical columns,
        so that the imputation results would be meaningful.

        Defaults to 3.

    als_lambda : float, optional

        L2 regularization applied to the factors in the ALS model.

        Should be non-negative.

        Defaults to 0.01.

    als_maxit : int, optional

        Maximum number of iterations for solving the ALS model.

        Defaults to 20.

    als_randomstate : int, optional

        Specifies the seed of the random number generator used
        in the training of ALS model:

            - 0: Uses the current time as the seed,
            - Others: Uses the specified value as the seed.

        Defaults to 0.

    als_exit_threshold : float, optional

        Specify a value for stopping the training of ALS model.
        If the improvement of the cost function of the ALS model
        is less than this value between consecutive checks, then
        the training process will exit.

        0 means there is no checking of the objective value when
        running the algorithms, and it stops till the maximum number of
        iterations has been reached.

        Defaults to 0.

    als_exit_interval : int, optional

        Specify the number of iterations between consecutive checking of
        cost functions for the ALS model, so that one can see if the
        pre-specified ``exit_threshold`` is reached.

        Defaults to 5.

    als_linsolver : {'cholesky', 'cg'}, optional

        Linear system solver for the ALS model.

          - 'cholesky' is usually much faster.
          - 'cg' is recommended when ``als_factors`` is large.

        Defaults to 'cholesky'.

    als_maxit : int, optional

        Specifies the maximum number of iterations for cg algorithm.

        Invoked only when the 'cg' is the chosen linear system solver for ALS.

        Defaults to 3.

    als_centering : bool, optional

        Whether to center the data by column before training the ALS model.

        Defaults to True.

    als_scaling : bool, optional

        Whether to scale the data by column before training the ALS model.

        Defaults to True.

    Attributes
    ----------

    model_ : DataFrame
        Model content.

    Examples
    --------
    >>> impute = Imputer(strategy='most_frequent-mean')
    >>> result = impute.fit_transform(data=df, categorical_variable=['V1'],
                                     strategy_by_col=[('V1', 'categorical_const', '0')])
    >>> res.collect()

    The above stats/model content of the input DataFrame can be applied
    to imputing another DataFrame with the same data structure.

    With an impute instance being obtained, one can impute the missing values of df_new via the following line of code, and then check
    the result:

    >>> res_new, statistics = impute.transform(data=df_new, key='ID')
    >>> res_new.collect()

    Create an Imputer instance using other strategies, e.g. 'als' strategy
    and then call fit():

    >>> impute = Imputer(strategy='als', als_factors=2, als_randomstate=1)

    Output:

    >>> result2 = impute.fit_transform(data=df, categorical_variable=['V1'])
    >>> result2.collect()

    """
    overall_imputation_map = {'non':0, 'delete': 5,
                              'most_frequent-mean':1, 'mean':1,
                              'most_frequent-median':2, 'median':2,
                              'most_frequent-zero':3, 'zero':3,
                              'most_frequent-als':4, 'als':4}
    column_imputation_map = {'non':0, 'delete':1,
                             'most_frequent':100,
                             'categorical_const':101,
                             'mean':200, 'median':201,
                             'numerical_const':203,
                             'als':204}
    dtype_escp = {'INT':INTEGER, 'DOUBLE':DOUBLE,
                  'NVARCHAR':NVARCHAR(5000), 'VARCHAR':NVARCHAR(256)}
    solver_map = {'cholsky':0, 'cg':1, 'cholesky':0}

    def __init__(self,
                 strategy=None,
                 als_factors=None,
                 als_lambda=None,
                 als_maxit=None,
                 als_randomstate=None,
                 als_exit_threshold=None,
                 als_exit_interval=None,
                 als_linsolver=None,
                 als_cg_maxit=None,
                 als_centering=None,
                 als_scaling=None,
                 thread_ratio=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(Imputer, self).__init__()
        self.strategy = self._arg('strategy', strategy, self.overall_imputation_map)
        self.thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        self.als_factors = self._arg('als_factors', als_factors, int)
        self.als_lambda = self._arg('als_lambda', als_lambda, float)
        self.als_maxit = self._arg('als_maxit', als_maxit, int)
        self.als_randomstate = self._arg('als_randomstate', als_randomstate, int)
        self.als_exit_threshold = self._arg('als_exit_threshold', als_exit_threshold,
                                            float)
        self.als_exit_interval = self._arg('als_exit_interval', als_exit_interval, int)
        self.als_linsolver = self._arg('als_linsolver', als_linsolver, self.solver_map)
        self.als_cg_maxit = self._arg('als_cg_maxit', als_cg_maxit, int)
        self.als_centering = self._arg('als_centering', als_centering, bool)
        self.als_scaling = self._arg('als_scaling', als_scaling, bool)
        self.model_ = None
        self.op_name = 'Imputer'

    def fit(self, data, key=None,
            categorical_variable=None,
            strategy_by_col=None):
        """
        Impute the missing values of a HANA DataFrame and collect the model info for imputation.

        Parameters
        ----------

        data : DataFrame
            Input data with missing values.

        key : str, optional
            Name of the ID column.

            If ``key`` is not provided, then:

                - if ``data`` is indexed by a single column, then ``key`` defaults
                  to that index column;
                - otherwise, it is assumed that ``data`` contains no ID column.

        categorical_variable : str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.
        strategy_by_col : ListOfTuples, optional
            Specifies the imputation strategy for a set of columns, which
            overrides the overall strategy for data imputation.

            Each tuple in the list should contain at least two elements,
            such that:

              - the 1st element is the name of a column;
              - the 2nd element is the imputation strategy of that column,
                valid strategies include: **'non', 'delete', 'most_frequent', 'categorical_const', \
                'mean', 'median', 'numerical_const', 'als'**.
              - If the imputation strategy is 'categorical_const' or 'numerical_const',
                then a 3rd element must be included in the tuple, which specifies
                the constant value to be used to substitute the detected missing values
                in the column.

            An example for illustration:
                [('V1', 'categorical_const', '0'), ('V5','median')]

        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        self.fit_transform(data=data,
                           key=key,
                           categorical_variable=categorical_variable,
                           strategy_by_col=strategy_by_col)
        return self

    def fit_transform(self, data, key=None,
                      categorical_variable=None,
                      strategy_by_col=None):
        """
        Impute the missing values of a DataFrame, return the result,
        and collect the related statistics/model info for imputation.

        Parameters
        ----------

        data : DataFrame
            Input data with missing values.
        key : str, optional
            Name of the ID column.

            If ``key`` is not provided, then:

                - if ``data`` is indexed by a single column, then ``key`` defaults
                  to that index column;
                - otherwise, it is assumed that ``data`` contains no ID column.

        categorical_variable : str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.
        strategy_by_col : ListOfTuples, optional
            Specifies the imputation strategy for a set of columns, which
            overrides the overall strategy for data imputation.

            Each tuple in the list should contain at least two elements,
            such that:

              - the 1st element is the name of a column;
              - the 2nd element is the imputation strategy of that column,
                valid strategies include: **'non', 'delete', 'most_frequent', 'categorical_const', \
                'mean', 'median', 'numerical_const', 'als'**.
              - If the imputation strategy is 'categorical_const' or 'numerical_const',
                then a 3rd element must be included in the tuple, which specifies
                the constant value to be used to substitute the detected missing values
                in the column.

            An example for illustration:
                [('V1', 'categorical_const', '0'),

                ('V5','median')]

        Returns
        -------

        DataFrame
            Imputed result using specified strategy, with the same data structure,
            i.e. column names and data types same as ``data``.
        """
        conn = data.connection_context
        require_pal_usable(conn)
        key = self._arg('key', key, str)
        index = data.index
        if isinstance(index, str):
            if key is not None and index != key:
                msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(msg)
        key = index if key is None else key
        self.categorical_variable = self._arg('categorical_variable',
                                              categorical_variable, ListOfStrings)
        self.strategy_by_col = self._arg('strategy_by_col',
                                         strategy_by_col, ListOfTuples)
        if self.strategy_by_col is not None:
            for col_strategy in self.strategy_by_col:
                if col_strategy[0] not in data.columns:
                    msg = ('{} is not a column name'.format(col_strategy[0]) +
                           ' of the input dataFrame.')
                    logger.error(msg)
                    raise ValueError(msg)

        param_rows = [('IMPUTATION_TYPE', self.strategy, None, None),
                      ('ALS_FACTOR_NUMBER', self.als_factors, None, None),
                      ('ALS_REGULARIZATION', None, self.als_lambda, None),
                      ('ALS_MAX_ITERATION', self.als_maxit, None, None),
                      ('ALS_SEED', self.als_randomstate, None, None),
                      ('ALS_EXIT_THRESHOLD', None, self.als_exit_threshold, None),
                      ('ALS_EXIT_INTERVAL', self.als_exit_interval, None, None),
                      ('ALS_LINEAR_SYSTEM_SOLVER', self.als_linsolver, None, None),
                      ('ALS_CG_MAX_ITERATION', self.als_cg_maxit, None, None),
                      ('ALS_CENTERING', self.als_centering, None, None),
                      ('ALS_SCALING', self.als_scaling, None, None),
                      ('THREAD_RATIO', None, self.thread_ratio, None),
                      ('HAS_ID', key is not None, None, None)]

        if self.categorical_variable is not None:
            param_rows.extend([('CATEGORICAL_VARIABLE', None, None, str(var))
                               for var in self.categorical_variable])
        #override the overall imputation methods for specified columns
        if self.strategy_by_col is not None:
            for col_imp_type in self.strategy_by_col:
                imp_type = self._arg('imp_type', col_imp_type[1], self.column_imputation_map)
                if len(col_imp_type) == 2:
                    param_rows.extend([('{}_IMPUTATION_TYPE'.format(col_imp_type[0]),
                                        imp_type, None, None)])
                elif len(col_imp_type) == 3:
                    if imp_type == 101:
                        param_rows.extend([('{}_IMPUTATION_TYPE'.format(col_imp_type[0]),
                                            imp_type, None, str(col_imp_type[2]))])
                    else:
                        param_rows.extend([('{}_IMPUTATION_TYPE'.format(col_imp_type[0]),
                                            imp_type, col_imp_type[2], None)])
                else:
                    continue

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        outputs = ['RESULT', 'STATS_MODEL']
        outputs = ['#PAL_IMPUTATION_{}_TBL_{}_{}'.format(name, self.id, unique_id)
                   for name in outputs]
        result_tbl, stats_model_tbl = outputs

        try:
            self._call_pal_auto(conn,
                                'PAL_MISSING_VALUE_HANDLING',
                                data,
                                ParameterTable().with_data(param_rows),
                                *outputs)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, outputs)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, outputs)
            raise
        self.model_ = conn.table(stats_model_tbl)
        return conn.table(result_tbl)

    def transform(self, data, key=None, thread_ratio=None):
        """
        The function imputes missing values a DataFrame using
        statistic/model info collected from another DataFrame.

        Parameters
        ----------

        data : DataFrame
           Input DataFrame.
        key : str, optional
           Name of ID column.

           If ``key`` is not provided, then:

                - if ``data`` is indexed by a single column, then ``key`` defaults
                  to that index column;
                - otherwise, it is assumed that ``data`` contains no ID column.

        thread_ratio : float, optional
            Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
            Values outside the range will be ignored and this function heuristically determines the number of threads to use.

           Defaults to 0.0.

        Returns
        -------

        DataFrame
            Imputation result, structured same as ``data``.

            Statistics for the imputation result, structured as:

                - STAT_NAME: type NVACHAR(256), statistics name.
                - STAT_VALUE: type NVACHAR(5000), statistics value.

        """
        conn = data.connection_context
        require_pal_usable(conn)
        if self.model_ is None:
            raise FitIncompleteError("Stats/model has not been initialized. "+
                                     "Please perform a fit_transform() method first!")
        key = self._arg('key', key, str)
        index = data.index
        if isinstance(index, str):
            if key is not None and index != key:
                msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(msg)
        key = index if key is None else key
        thread_ratio = self._arg('thread_ratio', thread_ratio, float)

        param_rows = [('HAS_ID', key is not None, None, None),
                      ('THREAD_RATIO', None, thread_ratio, None)]

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        outputs = ['RESULT', 'STATS']
        outputs = ['#PAL_IMPUTE_PREDICT_{}_TBL_{}_{}'.format(name, self.id, unique_id)
                   for name in outputs]
        result_tbl, stats_tbl = outputs

        try:
            self._call_pal_auto(conn,
                                'PAL_MISSING_VALUE_HANDLING_WITH_MODEL',
                                data,
                                self.model_,
                                ParameterTable().with_data(param_rows),
                                *outputs)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, outputs)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, outputs)
            raise
        return conn.table(result_tbl), conn.table(stats_tbl)

class Discretize(PALBase):
    """
    It is an enhanced version of binning function which can be applied to table with multiple columns.
    This function partitions table rows into multiple segments called bins, then applies smoothing
    methods in each bin of each column respectively.

    Parameters
    ----------

    strategy : {'uniform_number', 'uniform_size', 'quantile', 'sd'}
        Binning methods:

            - 'uniform_number': equal widths based on the number of bins.
            - 'uniform_size': equal widths based on the bin width.
            - 'quantile': equal number of records per bin.
            - 'sd': mean/ standard deviation bin boundaries.

    n_bins : int, optional
        Number of needed bins.

        Required and only valid when ``strategy`` is set as 'uniform_number' or 'quantile'.

        Default to 2.
    bin_size : float, optional
        Specifies the distance for binning.

        Required and only valid when ``strategy`` is set as 'uniform_size'.

        Default to 10.
    n_sd : int, optional
        Specifies the number of standard deviation at each side of the mean.

        For example, if ``n_sd`` equals 2, this function takes mean +/- 2 * standard deviation as
        the upper/lower bound for binning.

        Required and only valid when ``strategy`` is set as 'sd'.
    smoothing : {'no', 'bin_means', 'bin_medians', 'bin_boundaries'}, optional
        Specifies the default smoothing method for all non-categorical columns.

        Default to 'no'.
    save_model : bool, optional
        Indicates whether the model is saved.

        Default to True.

    Attributes
    ----------
    result_ : DataFrame
        Discretize results.

    assign_ : DataFrame
        Assignment results..

    model_ : DataFrame
        Model content.

    stats_ : DataFrame
        Statistics.

    Examples
    --------
    >>> bin = Discretize(method='uniform_number', n_bins=3, smoothing='bin_medians')
    >>> bin.fit(data=df, binning_variable='ATT1',
                col_smoothing=[('ATT2', 'bin_means')],
                categorical_variable='ATT3')
    >>> bin.assign_.collect()
    >>> res = bin.predict(data=predict_data)
    >>> res.collect():

    """
    def __init__(self,
                 strategy,
                 n_bins=None,
                 bin_size=None,
                 n_sd=None,
                 smoothing=None,
                 save_model=True):
        super(Discretize, self).__init__()
        setattr(self, 'hanaml_parameters', pal_param_register())
        self.method_map = {'uniform_number': 0, 'uniform_size': 1, 'quantile': 2, 'sd': 3}
        self.smoothing_method_map = {'no': 0, 'bin_means': 1, 'bin_medians': 2, 'bin_boundaries': 3}
        self.strategy = self._arg('strategy', strategy, self.method_map, required=True)
        self.n_bins = self._arg('n_bins', n_bins, int)
        self.bin_size = self._arg('bin_size', bin_size, float)
        self.n_sd = self._arg('n_sd', n_sd, int)
        self.smoothing = self._arg('smoothing', smoothing, self.smoothing_method_map)
        self.save_model = self._arg('save_model', save_model, bool)
        self.key = None
        self.features = None

    def fit(self, data, binning_variable, key=None, features=None, col_smoothing=None,
            categorical_variable=None):
        """
        Fitting a Discretize model.

        Parameters
        ----------
        data : DataFrame
            Dataframe that contains the training data.

        binning_variable : str
            Attribute name, to which binning operation is applied.

            Variable data type must be numeric.

        key : str, optional
            Name of the ID column in ``data``.

            If ``key`` is not provided, then:

                - if ``data`` is indexed by a single column, then ``key`` defaults
                  to that index column;
                - otherwise, it defaults to the first column of ``data``.

        features : str/ListofStrings, optional
            Name of the feature columns which needs to be considered in the model.

            If not specified, all columns except the key column will be count as feature columns.

        col_smoothing : ListofTuples, optional
            Specifies column name and its method for smoothing, which overwrites the default smoothing method.

            For example: smoothing_method = [('ATT1', 'bin_means'), ('ATT2', 'bin_boundaries')]

            Only applies for none-categorical attributes.

            No default value.
        categorical_variable : str/ListofStrings, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.

        Returns
        -------
        A fitted object of class "Discretize".
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        conn = data.connection_context
        require_pal_usable(conn)
        self.key = self._arg('key', key, str)
        index = data.index
        if isinstance(index, str):
            if key is not None and index != key:
                msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(msg)
            self.key = index if key is None else key
        cols = data.columns
        if self.key is None:
            self.key = cols[0]
        cols.remove(self.key)
        if features is not None:
            if isinstance(features, str):
                features = [features]
            try:
                self.features = self._arg('features', features, ListOfStrings)
            except:
                msg = ("`features` must be list of string or string.")
                logger.error(msg)
                raise TypeError(msg)
        else:
            self.features = cols
        if categorical_variable is not None:
            if isinstance(categorical_variable, str):
                categorical_variable = [categorical_variable]
            try:
                self.categorical_variable = self._arg('categorical_variable', categorical_variable, ListOfStrings)
            except:
                msg = ("`categorical_variable` must be list of string or string.")
                logger.error(msg)
                raise TypeError(msg)
        if isinstance(binning_variable, str):
            binning_variable = [binning_variable]
        try:
            self.binning_variable = self._arg('binning_variable', binning_variable, ListOfStrings)
        except:
            msg = ("`binning_variable` must be list of string or string.")
            logger.error(msg)
            raise TypeError(msg)
        if categorical_variable is not None:
            if not all((data.dtypes([var])[0][1] in ('INT', 'FLOAT', 'DOUBLE') and var not in categorical_variable) for var in binning_variable):
                msg = ("`binning_variable` must indicate attributes of numerical type.")
                logger.error(msg)
                raise TypeError(msg)
        self.col_smoothing = self._arg('col_smoothing', col_smoothing, ListOfTuples)
        if self.col_smoothing is not None:
            for x in self.col_smoothing:
                if len(x) != 2:
                    msg = ("Each tuple that specifies the smoothing method of an attribute"+
                           " should contain exactly 2 elements: 1st is attribute column name,"+
                           " 2nd is a smoothing_method code.")
                    logger.error(msg)
                    raise ValueError(msg)
                if x[1] not in self.smoothing_method_map:
                    msg = ("'{}' is not a valid smoothing method.".format(x[1]))
                    logger.error(msg)
                    raise ValueError(msg)
                if x[0] not in self.features or data.dtypes([x[0]])[0][1] not in ('INT', 'FLOAT', 'DOUBLE') or (x[0] in self.categorical_variable):
                    msg = ("`col_smoothing` can only be applied for numerical attributes.")
                    logger.error(msg)
                    raise ValueError(msg)
        data_ = data[[self.key] + self.features]

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        tables = ['RESULT', 'ASSIGNMENT', 'MODEL', 'STATISTICS']
        tables = ['#PAL_BIN_{}_TBL_{}_{}'.format(name, self.id, unique_id) for name in tables]
        result_tbl, assign_tbl, model_tbl, statistic_tbl = tables
        param_rows = [('METHOD', self.strategy, None, None),
                      ('BIN_NUMBER', self.n_bins, None, None),
                      ('BIN_DISTANCE', None, self.bin_size, None),
                      ('SD', self.n_sd, None, None),
                      ('DEFAULT_SMOOTHING_METHOD', self.smoothing, None, None),
                      ('SAVE_MODEL', self.save_model, None, None)]
        param_rows.extend([('BINNING_VARIABLE', None, None, str(var))
                           for var in self.binning_variable])
        if categorical_variable is not None:
            param_rows.extend([('CATEGORICAL_VARIABLE', None, None, str(var))
                               for var in self.categorical_variable])
        if self.col_smoothing is not None:
            param_rows.extend([('SMOOTHING_METHOD', self.smoothing_method_map[var[1]], None, var[0])
                               for var in self.col_smoothing])
        try:
            self._call_pal_auto(conn,
                                'PAL_DISCRETIZE',
                                data_,
                                ParameterTable().with_data(param_rows),
                                *tables)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, tables)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, tables)
            raise
        self.result_ = conn.table(result_tbl)
        self.model_ = conn.table(model_tbl)
        self.assign_ = conn.table(assign_tbl)
        self.stats_ = conn.table(statistic_tbl)
        self.statistics_ = self.stats_
        #pylint:disable=attribute-defined-outside-init
        return self

    def predict(self, data):
        """
        Discretizing new data using a generated Discretize model.

        Parameters
        ----------
        data : DataFrame
            Dataframe including the predict data.

        Returns
        -------
            DataFrame
                - Discretization result
                - Bin assignment
                - Statistics
        """
        conn = data.connection_context
        require_pal_usable(conn)
        if getattr(self, 'model_') is None:
            raise FitIncompleteError()
        data_ = data
        if (self.key is not None) and (self.features is not None):
            data_ = data[[self.key] + self.features]
            data_.index = data.index
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        tables = ['RESULT', 'ASSIGNMENT', 'STATISTICS']
        tables = ['#PAL_BIN_APPLY{}_TBL_{}_{}'.format(name, self.id, unique_id) for name in tables]
        result_tbl, assign_tbl, stats_tbl = tables
        param_rows = []
        try:
            self._call_pal_auto(conn,
                                'PAL_DISCRETIZE_APPLY',
                                data_,
                                self.model_,
                                ParameterTable().with_data(param_rows),
                                *tables)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, tables)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, tables)
            raise
        return conn.table(result_tbl), conn.table(assign_tbl), conn.table(stats_tbl)

    def transform(self, data):
        """
        Data discretization using generated Discretize models.

        Parameters
        ----------
        data : DataFrame
            Dataframe including the predict data.

        Returns
        -------
            DataFrame
                - Discretization result
                - Bin assignment
                - Statistics
        """
        return self.predict(data)

    def fit_transform(self, data, binning_variable, key=None, features=None, col_smoothing=None, categorical_variable=None):
        """
        Learn a discretization configuration(model) from input data and then discretize it under that configuration.

        Parameters
        ----------
        data : DataFrame
             Dataframe that contains the training data.
        key : str, optional
            Name of the ID column in ``data``.

            If ``key`` is not provided, then:

                - if ``data`` is indexed by a single column, then ``key`` defaults
                  to that index column;
                - otherwise, it is assumed that ``data`` contains no ID column.

        features : str/ListofStrings, optional
            Name of the feature columns which needs to be considered in the model.

            If not specified, all columns except the key column will be count as feature columns.
        binning_variable : str
            Attribute name, to which binning operation is applied.

            Variable data type must be numeric.
        col_smoothing : ListofTuples, optional
            Specifies column name and its method for smoothing, which overwrites the default smoothing method.

            For example: smoothing_method = [('ATT1', 'bin_means'), ('ATT2', 'bin_boundaries')]

            Only applies for non-categorical attributes.

            No default value.
        categorical_variable : str/ListofStrings, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.

        Returns
        -------
        DataFrame

            - Discretization result
            - Bin assignment
            - Statistics
        """
        self.fit(data, binning_variable, key, features, col_smoothing, categorical_variable)
        return self.result_, self.assign_, self.stats_

class MDS(PALBase):
    r"""
    This class serves as a tool for dimensional reduction or data visualization.
    There are two kinds of input formats supported by this function: an :math:`N \times N` **dissimilarity** matrix,
    or a usual entity–feature matrix. The former is a symmetric matrix, with each element representing
    the distance (dissimilarity) between two entities, while the later can be converted to a dissimilarity matrix
    using a method specified by the user.

    Parameters
    ----------
    matrix_type : {'dissimilarity', 'observation_feature'}
        The type of the input DataFrame.

    thread_ratio : float, optional
        Specifies the ratio of total number of threads that can be used by this function.

        The value range is from 0 to 1, where 0 means only using 1 thread, and 1 means using at most all the currently
        available threads.

        Values outside the range will be ignored and this function heuristically
        determines the number of threads to use.

        Default to 0.
    dim : int, optional
        The number of dimension that the input dataset is to be reduced to.

        Default to 2.
    metric : {'manhattan', 'euclidean', 'minkowski'}, optional
        The type of distance during the calculation of dissimilarity matrix.

        Only valid when ``matrix_type`` is set as 'observation_feature'.

        Default to 'euclidean'.
    minkowski_power : float, optional
        When ``metric`` is set as 'minkowski', this parameter controls the value of power.

        Only valid when ``matrix_type`` is set as 'observation_feature' and ``metric`` is set as
        'minkowski'.

        Default to 3.

    Examples
    --------
    >>> mds = MDS(matrix_type='dissimilarity', dim=2, thread_ratio=0.5)
    >>> res, stats = mds.fit_transform(data=df)
    >>> res.collect()
    >>> stats.collect()

    Attributes
    ----------
    None


    """
    def __init__(self, matrix_type, thread_ratio=None, dim=None, metric=None, minkowski_power=None):
        super(MDS, self).__init__()
        matrix_type_map = {'observation_feature': 0, 'dissimilarity': 1}
        metric_map = {'manhattan': 1, 'euclidean': 2, 'minkowski': 3}
        self.matrix_type = arg('matrix_type', matrix_type, matrix_type_map)
        self.metric = arg('metric', metric, metric_map)
        if metric is not None and matrix_type != matrix_type_map['observation_feature']:
            msg = ("`metric` is invalid when input matrix_type is not observation_feature")
            logger.error(msg)
            raise ValueError(msg)
        self.thread_ratio = arg('thread_ratio', thread_ratio, float)
        self.dim = arg('dim', dim, int)
        self.minkowski_power = arg('minkowski_power', minkowski_power, float)
        if minkowski_power is not None and metric != metric_map['minkowski']:
            msg = ("`minkowski_power` is invalid when input metric " + \
            "is not set as 'minkowski'.")
            logger.error(msg)
            raise ValueError(msg)

    def fit_transform(self, data, key=None, features=None):
        """
        Scaling of given datasets in multiple dimensions.

        Parameters
        ----------
        data : DataFrame
             Dataframe that contains the training data.
        key : str, optional
            Name of the ID column ``data``.

            Mandatory if ``data`` is not indexed, or the index of ``data`` contains multiple columns.

            Defaults to the single index column of ``data`` if not provided.

        features : str/ListofStrings, optional
            Name of the feature columns which needs to be considered in the model.

            If not specified, all columns except the key column will be count as feature columns.

        Returns
        -------
        DataFrame

            DataFrame 1, scaling result of `data`, structured as follows:

              - Data ID : IDs from `data`
              - DIMENSION : The dimension number in `data`
              - VALUE : Scaled value

            DataFrame 2, statistics
        """
        conn = data.connection_context
        require_pal_usable(conn)
        cols = data.columns
        index = data.index
        key = self._arg('key', key, str, required=not isinstance(index, str))
        if isinstance(index, str):
            if key is not None and index != key:
                msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(msg)
        key = index if key is None else key
        cols.remove(key)
        if features is not None:
            if isinstance(features, str):
                features = [features]
            try:
                features = arg('features', features, ListOfStrings)
            except:
                msg = ("`features` must be list of string or string.")
                logger.error(msg)
                raise TypeError(msg)
        else:
            features = cols
        data_ = data[[key] + features]
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        tables = ['RESULT', 'STATS']
        tables = ['#MDS_{}_TBL_{}'.format(tbl, unique_id) for tbl in tables]
        res_tbl, stats_tbl = tables
        param_rows = [('K', self.dim, None, None),
                      ('INPUT_TYPE', self.matrix_type, None, None),
                      ('DISTANCE_LEVEL', self.metric, None, None),
                      ('MINKOWSKI_POWER', None, self.minkowski_power, None),
                      ('THREAD_RATIO', None, self.thread_ratio, None)]
        try:
            self._call_pal_auto(conn,
                                "PAL_MDS",
                                data_,
                                ParameterTable().with_data(param_rows),
                                *tables)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, tables)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, tables)
            raise
        return conn.table(res_tbl), conn.table(stats_tbl)

class Sampling(PALBase):
    """
    This class is used to choose a small portion of the records as representatives.

    Parameters
    ----------

    method : str
        Specifies the sampling method.

        Valid options include:
        'first_n', 'middle_n', 'last_n', 'every_nth', 'simple_random_with_replacement',
        'simple_random_without_replacement', 'systematic', 'stratified_with_replacement',
        'stratified_without_replacement'.

        For the random methods, the system time is used for the seed.

    interval : int, optional
        The interval between two samples.

        Only required when ``method`` is 'every_nth'.

        If this parameter is not specified, the ``sampling_size`` parameter will be used.

    sampling_size : int, optional
        Number of the samples.

        Default to 1.

    random_state : int, optional
        Indicates the seed used to initialize the random number generator.

        It can be set to 0 or a positive value, where:
            - 0: Uses the system time
            - Others: Uses the specified seed

        Default to 0.

    percentage : float, optional
        Percentage of the samples.

        Use this parameter when sampling_size is not set.

        If both ``sampling_size`` and ``percentage`` are specified, ``percentage`` takes precedence.

        Default to 0.1.

    Examples
    --------
    >>> smp = Sampling(method='every_nth', interval=5, sampling_size=8)
    >>> res = smp.fit_transform(data=df)
    >>> res.collect()

    Attributes
    ----------
    None

    """
    def __init__(self, method, interval=None, sampling_size=None, random_state=None, percentage=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(Sampling, self).__init__()
        method_map = {'first_n': 0, 'middle_n': 1, 'last_n': 2, 'every_nth': 3,
                      'simple_random_with_replacement': 4, 'simple_random_without_replacement': 5,
                      'systematic': 6, 'stratified_with_replacement': 7,
                      'stratified_without_replacement': 8}
        self.method = arg('method', method, method_map)
        self.interval = arg('interval', interval, int)
        self.sampling_size = arg('sampling_size', sampling_size, int)
        self.random_state = arg('random_state', random_state, int)
        self.percentage = arg('percentage', percentage, float)
        if method == 3 and interval is None:
            msg = ("`interval` is required when `method` is set as 'every_nth'.")
            logger.error(msg)
            raise ValueError(msg)
        if percentage is not None and sampling_size is not None:
            sampling_size = None
        self.op_name = 'SAMPLING'

    def fit_transform(self, data, features=None):
        """
        Sampling the input dataset under specified configuration.

        Parameters
        ----------
        data : DataFrame
            Input DataFrame.

        features : str/ListofStrings, optional
            The column that is used to do the stratified sampling.

            Only required when method is 'stratified_with_replacement',
            or 'stratified_without_replacement'.

            Defaults to None.

        Returns
        -------
        DataFrame
            Sampling results, same structure as defined in the input DataFrame.
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        if self.sampling_size:
            if data.count() < self.sampling_size:
                return data
        conn = data.connection_context

        if not self._disable_hana_execution:
            require_pal_usable(conn)
            column_choose = None
            if features is not None:
                if isinstance(features, str):
                    features = [features]
                try:
                    features = arg('features', features, ListOfStrings)
                    column_choose = []
                    for feature in features:
                        column_choose.append(data.columns.index(feature))
                except:
                    msg = ("`features` must be list of string or string.")
                    logger.error(msg)
                    raise TypeError(msg)
            if self.method in (7, 8) and column_choose is None:
                msg = ("`features` specification is required when `method` " + \
                    "is set to 'stratified_with_replacement' or 'stratified_without_replacement'.")
                logger.error(msg)
                raise ValueError(msg)
        else:
            column_choose = None
            if features is not None:
                if isinstance(features, str):
                    features = [features]
                try:
                    column_choose = []
                    for feature in features:
                        column_choose.append(data.columns.index(feature))
                except:
                    pass

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        res_tbl = "#SAMPLING_RESULT_TBL_{}".format(unique_id)
        param_rows = [('SAMPLING_METHOD', self.method, None, None),
                      ('INTERVAL', self.interval, None, None),
                      ('SAMPLING_SIZE', self.sampling_size, None, None),
                      ('RANDOM_SEED', self.random_state, None, None),
                      ('PERCENTAGE', None, self.percentage, None)]
        if column_choose is not None:
            param_rows.extend([('COLUMN_CHOOSE', col+1, None, None)
                            for col in column_choose])
        try:
            self._call_pal_auto(conn,
                                "PAL_SAMPLING",
                                data,
                                ParameterTable().with_data(param_rows),
                                res_tbl)
        except dbapi.Error as db_err:
            try_drop(conn, res_tbl)
            logger.exception(str(db_err))
            raise
        except Exception as db_err:
            try_drop(conn, res_tbl)
            logger.exception(str(db_err))
            raise
        return conn.table(res_tbl)

class SMOTE(PALBase):
    """
    This class is to handle imbalanced dataset. Synthetic minority over-sampling technique (SMOTE)
    proposes an over-sampling approach in which the minority class is over-sampled by creating
    "synthetic" examples in "feature space".

    Parameters
    ----------

    smote_amount : int, optional
        Amount of SMOTE N%. E.g. 200 means 200%, so each minority class sample will generate 2 synthetic samples.

        The synthetic samples are generated until the minority class sample amount matches
        the majority class sample amount.

    k_nearest_neighbours : int, optional
        Number of nearest neighbors (k).

        Defaults to 1.
    minority_class : str, optional(deprecated)
        Specifies the minority class value in dependent variable column.

        All classes except majority class are re-sampled to match the majority class sample amount.
    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Default to 0.
    random_seed : int, optional
        Specifies the seed for random number generator.

          - 0: Uses the current time (in seconds) as seed
          - Others: Uses the specified value as seed

        Defaults to 0.
    method : int, optional(deprecated)
        Searching method when finding K nearest neighbour.

          - 0: Brute force searching
          - 1: KD-tree searching

        Defaults to 0.
    search_method : str, optional
        Specifies the searching method for finding the k nearest-neighbors.

          - 'brute-force'
          - 'kd-tree'

        Defaults to 'brute-force'.

    category_weights : float, optional
        Represents the weight of category attributes.
        The value must be greater or equal to 0.

    Examples
    --------
    >>> smote = SMOTE(smote_amount=200,
                      k_nearest_neighbours=2,
                      search_method='kd-tree')
    >>> res = smote.fit_transform(data=df,
                                  label='TYPE',
                                  minority_class=2)
    >>> res.collect()

    Attributes
    ----------
    None

    """
    search_method_map = {'brute-force' : 0, 'kd-tree' : 1}

    def __init__(self, smote_amount=None,
                 k_nearest_neighbours=None,
                 minority_class=None,
                 thread_ratio=None,
                 random_seed=None,
                 method=None,
                 search_method=None,
                 category_weights=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(SMOTE, self).__init__()
        self.op_name = 'SMOTE'
        self.smote_amount = self._arg('smote_amount', smote_amount, int)
        self.k_nearest_neighbours = self._arg('k_nearest_neighbours', k_nearest_neighbours,
                                              int)
        self.minority_class = self._arg('minority_class', minority_class, (str, int))
        self.thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        self.random_seed = self._arg('random_seed', random_seed, int)
        self.method = self._arg('method', method, int)
        self.search_method = self._arg('search_method', search_method,
                                       self.search_method_map)
        self.category_weights = self._arg('category_weights', category_weights,
                                          (float, int))


    def fit_transform(self, data, label=None,
                      minority_class=None,
                      categorical_variable=None,
                      variable_weight=None,
                      key=None):
        """
        Upsampling given datasets using SMOTE with specified configuration.

        Parameters
        ----------
        data : DataFrame
            Dataframe containing the data for upsampling via SMOTE.

         key : str, optional
            Name of the ID column in ``data``.

            If ``data`` is indexed by a single column, then ``key`` defaults to that index column;
            otherwise no default value, so ``data`` is assumed having no ID column.
        label : str, optional
            Specifies the dependent variable by name.

            If not specified, defaults to the last column in ``data``.
        minority_class : str/int, optional
            Specifies the minority class value in dependent variable column.

            If not specified, all but the majority classes are resampled to match the majority class sample amount.

        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.
        variable_weight : dict, optional
            Specifies the weights of variables participating in distance calculation in a dictionary,
            illustrated as follows:

            {variable_name0 : value0, variable_name1 : value1, ...}.

            The values must be no less than 0.

            Weights default to 1 for variables not specified.

        Returns
        -------
        DataFrame
             - SMOTE result, the same structure as defined in the input data.
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        conn = data.connection_context
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        label = arg('label', label, str)
        cols = data.columns
        if label is not None and label not in cols:
            msg = "'label' must be selected from columns of 'data'."
            logger.error(msg)
            raise ValueError(msg)
        key = self._arg('key', key, str)
        index = data.index
        if isinstance(index, str):
            if key is not None and index != key:
                msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(msg)
        key = index if key is None else key
        if isinstance(categorical_variable, str):
            categorical_variable = [categorical_variable]
        categorical_variable = self._arg('categorical_variable', categorical_variable,
                                         ListOfStrings)
        variable_weight = self._arg('variable_weight', variable_weight, dict)
        if label is None:
            label = cols[-1]
        if not self._disable_hana_execution:
            require_pal_usable(conn)
            label_type = data.dtypes([label])
            if "INT" in label_type[0][1]:
                minority_class = arg('minority_class', minority_class, int)
            else:
                minority_class = arg('minority_class', minority_class, str)
        data_ = data if key is None else data.deselect(key)
        tables = ['RESULT']
        tables = ['#SMOTE_{}_TBL_{}'.format(tbl, unique_id) for tbl in tables]
        res_tbl = tables[0]
        param_rows = [('SMOTE_AMOUNT', self.smote_amount, None, None),
                      ('K_NEAREST_NEIGHBOURS', self.k_nearest_neighbours, None, None),
                      ('DEPENDENT_VARIABLE', None, None, label),
                      ('MINORITY_CLASS', None, None,
                       self.minority_class if minority_class is None else minority_class),
                      ('THREAD_RATIO', None, self.thread_ratio, None),
                      ('RANDOM_SEED', self.random_seed, None, None),
                      ('METHOD',
                       self.method if self.search_method is None else self.search_method,
                       None, None),
                      ('CATEGORY_WEIGHTS', None, None, self.category_weights)]
        if categorical_variable is not None:
            param_rows.extend([('CATEGORICAL_VARIABLE', None,
                                None, var) for var in categorical_variable])
        if variable_weight is not None:
            param_rows.extend([('VARIABLE_WEIGHT', None,
                                variable_weight[var], var) for var in variable_weight])
        try:
            self._call_pal_auto(conn,
                                "PAL_SMOTE",
                                data_,
                                ParameterTable().with_data(param_rows),
                                *tables)
        except dbapi.Error as db_err:
            msg = str(conn.hana_version())
            logger.exception("HANA version: %s. %s", msg, str(db_err))
            try_drop(conn, tables)
            raise
        except Exception as db_err:
            msg = str(conn.hana_version())
            logger.exception("HANA version: %s. %s", msg, str(db_err))
            try_drop(conn, tables)
            raise
        return conn.table(res_tbl)

class SMOTETomek(PALBase):
    """
    This class combines over-sampling using SMOTE and cleaning(under-sampling) using Tomek links.

    Parameters
    ----------

    smote_amount : int, optional
        Amount of SMOTE N%. E.g. 200 means 200%, so each minority class sample will generate 2 synthetic samples.

        The synthetic samples are generated until the minority class sample amount matches
        the majority class sample amount.
    k_nearest_neighbours : int, optional
        Number of nearest neighbors (k).

        Defaults to 1.
    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Default to 0.
    random_seed : int, optional
        Specifies the seed for random number generator.

          - 0: Uses the current time (in second) as seed
          - Others: Uses the specified value as seed

        Defaults to 0.
    search_method : str, optional
        Specifies the searching method when finding K nearest neighbour.

          - 'brute-force'
          - 'kd-tree'

        Defaults to 'brute-force'.
    sampling_strategy : str, optional
        Specifies the classes targeted by resampling:

          - 'majority' : resamples only the majority class
          - 'non-minority' : resamples all classes except the minority class
          - 'non-majority' : resamples all classes except the majority class
          - 'all' : resamples all classes

        Defaults to 'majority'.

    category_weights : float, optional
        Represents the weight of category attributes.
        The value must be greater or equal to 0.

    Examples
    --------
    >>> smotetomek = SMOTETomek(smote_amount=200,
                                k_nearest_neighbours=2,
                                random_seed=2,
                                search_method='kd-tree',
                                sampling_strategy='all')
    >>> res = smotetomek.fit_transform(data=df,
                                       label='TYPE',
                                       minority_class=2)
    >>> res.collect()

    Attributes
    ----------
    None

    """
    method_map = {'brute-force': 0, 'kd-tree' : 1}
    strategy_map = {'majority': 0, 'non-minority' : 1, 'non-majority' : 2, 'all' : 3}
    def __init__(self, smote_amount=None,
                 k_nearest_neighbours=None,
                 thread_ratio=None,
                 random_seed=None,
                 search_method=None,
                 sampling_strategy=None,
                 category_weights=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(SMOTETomek, self).__init__()
        self.op_name = 'SMOTETomek'
        self.smote_amount = self._arg('smote_amount', smote_amount, int)
        self.k_nearest_neighbours = self._arg('k_nearest_neighbours', k_nearest_neighbours, int)
        self.thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        self.random_seed = self._arg('random_seed', random_seed, int)
        self.method = self._arg('search_method', search_method, self.method_map)
        self.sampling_strategy = self._arg('sampling_strategy', sampling_strategy, self.strategy_map)
        self.category_weights = self._arg('category_weights', category_weights, (float, int))

    def fit_transform(self, data, label=None,
                      minority_class=None,
                      categorical_variable=None,
                      variable_weight=None,
                      key=None):
        """
        Perform both over-sampling using SMOTE and under-sampling by removing Tomek's links on given datasets.

        Parameters
        ----------
        data : DataFrame
            Dataframe that contains the data for resampling via SMOTE and Tomek's links.

        key : str, optional
            Specifies the name of ID column in ``data``.

            If ``data`` is indexed by a single column, the ``key`` defaults to that index column;
            otherwise no default value, and ``data`` is considered having no ID column.
        label : str, optional
            Specifies the dependent variable by name.

            If not specified, defaults to the last column in ``data``.
        minority_class : str/int, optional
            Specifies the minority class value in dependent variable column.

            If not specified, all but the majority classes are resampled to match the majority class sample amount.

        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.
        variable_weight : dict, optional
            Specifies the weights of variables participating in distance calculation in a dictionary:

              - key : variable(column) name
              - value : weight for distance calculation

            No default value.

        Returns
        -------
        DataFrame
            SMOTETomek result, structured the same as ``data`` exclusive
            of the ``key`` column(if there is one).
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        conn = data.connection_context
        require_pal_usable(conn)
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        label = self._arg('label', label, str)
        cols = data.columns
        if label is not None and label not in cols:
            msg = "'label' must be selected from columns of 'data'."
            logger.error(msg)
            raise ValueError(msg)
        key = self._arg('key', key, str)
        index = data.index
        if isinstance(index, str):
            if key is not None and index != key:
                msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(msg)
        key = index if key is None else key
        if label is None:
            label = cols[-1]
        if not self._disable_hana_execution:
            label_type = data.dtypes([label])
            if "INT" in label_type[0][1]:
                minority_class = self._arg('minority_class', minority_class, int)
            else:
                minority_class = self._arg('minority_class', minority_class, str)
        if isinstance(categorical_variable, str):
            categorical_variable = [categorical_variable]
        categorical_variable = self._arg('categorical_variable', categorical_variable,
                                         ListOfStrings)
        variable_weight = self._arg('variable_weight', variable_weight, dict)
        data_ = data if key is None else data.deselect(key)
        tables = ['RESULT']
        tables = ['#SMOTETOMEK_{}_TBL_{}'.format(tbl, unique_id) for tbl in tables]
        res_tbl = tables[0]
        param_rows = [('SMOTE_AMOUNT', self.smote_amount, None, None),
                      ('K_NEAREST_NEIGHBOURS', self.k_nearest_neighbours, None, None),
                      ('DEPENDENT_VARIABLE', None, None, label),
                      ('MINORITY_CLASS', None, None, minority_class),
                      ('THREAD_RATIO', None, self.thread_ratio, None),
                      ('RANDOM_SEED', self.random_seed, None, None),
                      ('METHOD', self.method, None, None),
                      ('SAMPLING_STRATEGY', self.sampling_strategy, None, None),
                      ('CATEGORY_WEIGHTS', None, self.category_weights, None)]
        if categorical_variable is not None:
            param_rows.extend([('CATEGORICAL_VARIABLE', None,
                                None, var) for var in categorical_variable])
        if variable_weight is not None:
            param_rows.extend([('VARIABLE_WEIGHT', None,
                                variable_weight[var], var) for var in variable_weight])
        try:
            self._call_pal_auto(conn,
                                "PAL_SMOTETOMEK",
                                data_,
                                ParameterTable().with_data(param_rows),
                                *tables)
        except dbapi.Error as db_err:
            msg = str(conn.hana_version())
            logger.exception("HANA version: %s. %s", msg, str(db_err))
            try_drop(conn, tables)
            raise
        except Exception as db_err:
            msg = str(conn.hana_version())
            logger.exception("HANA version: %s. %s", msg, str(db_err))
            try_drop(conn, tables)
            raise
        return conn.table(res_tbl)

class TomekLinks(PALBase):
    """
    This class is for performing under-sampling by removing Tomek's links.

    Parameters
    ----------

    distance_level : str, optional
        Specifies the distance method between train data and test data point.

          - 'manhattan'
          - 'euclidean'
          - 'minkowski'
          - 'chebyshev'
          - 'cosine'

        Defaults to 'euclidean'.
    minkowski_power : float, optional
        Specifies the value of power for Minkowski distance calculation.

        Defaults to 3.

        Valid only when ``distance_level`` is 'minkowski'.
    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Default to 0.
    search_method : str, optional
        Specifies the searching method when finding K nearest neighbour.

          - 'brute-force'
          - 'kd-tree'

        Defaults to 'brute-force'.
    sampling_strategy : str, optional

        Specifies the classes targeted by resampling:

          - 'majority' : resamples only the majority class
          - 'non-minority' : resamples all classes except the minority class
          - 'non-majority' : resamples all classes except the majority class
          - 'all' : resamples all classes

        Defaults to 'majority'

    category_weights : float, optional
        Specifies the weight for categorical attributes.

        Defaults to 0.707 if not provided.

    Examples
    --------
    >>> tomeklinks = TomekLinks(search_method='kd-tree',
                                sampling_strategy='majority')
    >>> res = smotetomek.fit_transform(data=df, label='TYPE')

    Attributes
    ----------
    None

    """
    method_map = {'brute-force': 0, 'kd-tree' : 1}
    strategy_map = {'majority': 0, 'non-minority' : 1, 'non-majority' : 2, 'all' : 3}
    distance_map = {'manhattan' : 1, 'euclidean' : 2, 'minkowski' : 3, 'chebyshev' : 4, 'cosine' : 6}

    def __init__(self, distance_level=None,
                 minkowski_power=None,
                 thread_ratio=None,
                 search_method=None,
                 sampling_strategy=None,
                 category_weights=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(TomekLinks, self).__init__()
        self.op_name = 'TomekLinks'
        self.distance_level = self._arg('distance_level', distance_level, self.distance_map)
        self.minkowski_power = self._arg('minkowski_power', minkowski_power, float)
        self.thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        self.search_method = self._arg('search_method', search_method, self.method_map)
        self.sampling_strategy = self._arg('sampling_strategy', sampling_strategy, self.strategy_map)
        self.category_weights = self._arg('category_weights', category_weights, float)

    def fit_transform(self, data,
                      key=None,
                      label=None,
                      categorical_variable=None,
                      variable_weight=None):
        """
        Perform under-sampling on given datasets by removing Tomek's links.

        Parameters
        ----------
        data : DataFrame
            Dataframe that contains the training data.
        key : str, optional
            Specifies the name of the ID column.

            If ``key`` is not provided, then:

                - if ``data`` is indexed by a single column, then ``key`` defaults
                  to that index column;
                - otherwise, it is assumed that ``data`` contains no ID column.

        label : str, optional
            Specifies the dependent variable by name.

            If not specified, defaults to the 1st non-key column in ``data``.
        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.
        variable_weight : dict, optional
            Specifies the weights of variables participating in distance calculation in a dictionary:

              - key : variable(column) name
              - value : weight for distance calculation

            No default value.

        Returns
        -------
        DataFrame
            - Undersampled result, the same structure as defined in the input data.
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        conn = data.connection_context
        require_pal_usable(conn)
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        key = self._arg('key', key, str)
        label = self._arg('label', label, str)
        if isinstance(categorical_variable, str):
            categorical_variable = [categorical_variable]
        categorical_variable = self._arg('categorical_variable',
                                         categorical_variable,
                                         ListOfStrings)
        if not self._disable_hana_execution:
            index = data.index
            if isinstance(index, str):
                if key is not None and index != key:
                    msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                    "and the designated index column '{}'.".format(index)
                    logger.warning(msg)
                key = index if key is None else key
        orig_cols = data.columns
        cols = orig_cols.copy()
        if key is not None:
            cols.remove(key)
        if label is not None and label not in cols:
            msg = "'label' must be selected from columns of 'data'."
            logger.error(msg)
            raise ValueError(msg)
        key_ls = [key] if key else []
        data_ = data[key_ls + cols]
        tables = ['RESULT']
        tables = ['#TOMEK_LINKS_{}_TBL_{}'.format(tbl, unique_id) for tbl in tables]
        res_tbl = tables[0]
        param_rows = [('DISTANCE_LEVEL', self.distance_level, None, None),
                      ('HAS_ID', key is not None, None, None),
                      ('MINKOWSKI_POWER', None, self.minkowski_power, None),
                      ('DEPENDENT_VARIABLE', None, None, label),
                      ('CATEGORY_WEIGHTS', None, self.category_weights, None),
                      ('THREAD_RATIO', None, self.thread_ratio, None),
                      ('METHOD', self.search_method, None, None),
                      ('SAMPLING_STRATEGY', self.sampling_strategy, None, None)]

        if categorical_variable is not None:
            param_rows.extend([('CATEGORICAL_VARIABLE',
                                None, None, var) for var in categorical_variable])
        if variable_weight is not None:
            param_rows.extend([('VARIABLE_WEIGHT',
                                None,
                                variable_weight[varb],
                                varb) for varb in variable_weight])
        try:
            self._call_pal_auto(conn,
                                "PAL_TOMEKLINKS",
                                data_,
                                ParameterTable().with_data(param_rows),
                                *tables)
        except dbapi.Error as db_err:
            msg = str(conn.hana_version())
            logger.exception("HANA version: %s. %s", msg, str(db_err))
            try_drop(conn, tables)
            raise
        except Exception as db_err:
            msg = str(conn.hana_version())
            logger.exception("HANA version: %s. %s", msg, str(db_err))
            try_drop(conn, tables)
            raise
        if not self._disable_hana_execution:
            return conn.table(res_tbl)[orig_cols]#consistency between input & output
        return conn.table(res_tbl)

@deprecated(version='1.0.8', reason="This method is deprecated. Please use MDS instead.")
def mds(data, matrix_type, thread_ratio=None,
        dim=None, metric=None, minkowski_power=None,
        key=None, features=None): # pragma: no cover
    """
    This function serves as a tool for dimensional reduction or data visualization.

    Parameters
    ----------

    data : DataFrame
        DataFrame containing the data.
    matrix_type : {'dissimilarity', 'observation_feature'}
        The type of the input table.
        Mandatory.
    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Default to 0.
    dim : int, optional
        The number of dimension that the input dataset is to be reduced to.

        Default to 2.
    metric : {'manhattan', 'euclidean', 'minkowski'}, optional
        The type of distance during the calculation of dissimilarity matrix.

        Only valid when ``matrix_type`` is set as 'observation_feature'.

        Default to 'euclidean'.
    minkowski_power : float, optional
        When ``metric`` is  'minkowski', this parameter controls the value of power.

        Only valid when ``matrix_type`` is set as 'observation_feature' and ``metric`` is set as
        'minkowski'.

        Default to 3.
    key : str, optional
        Name of the ID column in the dataframe.

        If not specified, the first col will be taken as the ID column.
    features : str or a list of str, optional
        Name of the feature column in the dataframe.

        If not specified, columns except the ID column will be taken as feature columns.

    Returns
    -------
    DataFrame
        Sampling results, structured as follows:

            - DATA_ID: name as shown in input DataFrame.
            - DIMENSION: dimension.
            - VALUE: value.

        Statistic results, structured as follows:

            - STAT_NAME:  statistic name.
            - STAT_VALUE: statistic value.

    Examples
    --------
    Input DataFrame df:

    >>> df.collect()
       ID        X1        X2        X3        X4
    0   1  0.000000  0.904781  0.908596  0.910306
    1   2  0.904781  0.000000  0.251446  0.597502
    2   3  0.908596  0.251446  0.000000  0.440357
    3   4  0.910306  0.597502  0.440357  0.000000

    Apply mds():

    >>> res,stats = mds(data=df, matrix_type='dissimilarity', dim=2, thread_ratio=0.5)
    >>> res.collect()
       ID  DIMENSION     VALUE
    0   1          1  0.651917
    1   1          2 -0.015859
    ...
    6   4          1 -0.184273
    7   4          2  0.342003

    >>> stats.collect()
                              STAT_NAME  STAT_VALUE
    0                        acheived K    2.000000
    1  proportion of variation explaind    0.978901
    """
    conn = data.connection_context
    require_pal_usable(conn)
    matrix_type_map = {'observation_feature': 0, 'dissimilarity': 1}
    metric_map = {'manhattan': 1, 'euclidean': 2, 'minkowski': 3}

    matrix_type = arg('matrix_type', matrix_type, matrix_type_map)
    metric = arg('metric', metric, metric_map)
    if metric is not None and matrix_type != matrix_type_map['observation_feature']:
        msg = ("`metric` is invalid when input matrix_type is not observation_feature")
        logger.error(msg)
        raise ValueError(msg)
    thread_ratio = arg('thread_ratio', thread_ratio, float)
    dim = arg('dim', dim, int)
    minkowski_power = arg('minkowski_power', minkowski_power, float)
    if minkowski_power is not None and metric != metric_map['minkowski']:
        msg = ("`minkowski_power` is invalid when input metric "+
               "is not set as 'minkowski'.")
        logger.error(msg)
        raise ValueError(msg)
    cols = data.columns
    key = arg('key', key, str)
    if key is None:
        key = cols[0]
    cols.remove(key)
    if features is not None:
        if isinstance(features, str):
            features = [features]
        try:
            features = arg('features', features, ListOfStrings)
        except:
            msg = ("`features` must be list of string or string.")
            logger.error(msg)
            raise TypeError(msg)
    else:
        features = cols
    data_ = data[[key] + features]

    unique_id = str(uuid.uuid1()).replace('-', '_').upper()
    tables = ['RESULT', 'STATS']
    tables = ['#MDS_{}_TBL_{}'.format(tbl, unique_id) for tbl in tables]
    res_tbl, stats_tbl = tables
    param_rows = [('K', dim, None, None),
                  ('INPUT_TYPE', matrix_type, None, None),
                  ('DISTANCE_LEVEL', metric, None, None),
                  ('MINKOWSKI_POWER', None, minkowski_power, None),
                  ('THREAD_RATIO', None, thread_ratio, None)]
    try:
        call_pal_auto_with_hint(conn,
                                None,
                                "PAL_MDS",
                                data_,
                                ParameterTable().with_data(param_rows),
                                *tables)
    except dbapi.Error as db_err:
        logger.exception(str(db_err))
        try_drop(conn, tables)
        raise
    return conn.table(res_tbl), conn.table(stats_tbl)

@deprecated(version='1.0.8', reason="This method is deprecated. Please use Sampling instead.")
def sampling(data, method, interval=None, features=None, sampling_size=None, random_state=None, percentage=None):# pragma: no cover
    """
    This function is used to choose a small portion of the records as representatives.

    Parameters
    ----------

    data : DataFrame
        DataFrame containing the data.
    method : str
        Specifies the sampling method.
        Valid options include:

        'first_n', 'middle_n', 'last_n', 'every_nth', 'simple_random_with_replacement',
        'simple_random_without_replacement', 'systematic', 'stratified_with_replacement',
        'stratified_without_replacement'.

        For the random methods, the system time is used for the seed.
    interval : int, optional
        The interval between two samples.

        Only required when ``method`` is 'every_nth'.

        If this parameter is not specified, the sampling_size parameter will be used.
    features : str or a list of str, optional
        The column that is used to do the stratified sampling.

        Only required when ``method is`` 'stratified_with_replacement',
        or 'stratified_without_replacement'.
    sampling_size : int, optional
        Number of the samples.

        Default to 1.
    random_state : int, optional
        Indicates the seed used to initialize the random number generator.

        It can be set to 0 or a positive value.

            - 0: Uses the system time
            - Not 0: Uses the specified seed

        Default to 0.
    percentage : float, optional
        Percentage of the samples.

        Use this parameter when sampling_size is not set.

        If both sampling_size and percentage are specified, percentage takes precedence.

        Default to 0.1.

    Returns
    -------
    DataFrame
        Sampling results.

    Examples
    --------
    >>> res = sampling(data=df, method='every_nth', interval=5, sampling_size=8)
    >>> res.collect()

    """
    conn = data.connection_context
    require_pal_usable(conn)
    method_map = {'first_n': 0, 'middle_n': 1, 'last_n': 2, 'every_nth': 3,
                  'simple_random_with_replacement': 4, 'simple_random_without_replacement': 5,
                  'systematic': 6, 'stratified_with_replacement': 7,
                  'stratified_without_replacement': 8}
    method = arg('method', method, method_map)
    interval = arg('interval', interval, int)
    column_choose = None
    if features is not None:
        if isinstance(features, str):
            features = [features]
        try:
            features = arg('features', features, ListOfStrings)
            column_choose = []
            for feature in features:
                column_choose.append(data.columns.index(feature))
        except:
            msg = ("`features` must be list of string or string.")
            logger.error(msg)
            raise TypeError(msg)
    sampling_size = arg('sampling_size', sampling_size, int)
    random_state = arg('random_state', random_state, int)
    percentage = arg('percentage', percentage, float)

    if method == 3 and interval is None:
        msg = ("`interval` is required when `method` is set as 'every_nth'.")
        logger.error(msg)
        raise ValueError(msg)
    if method in (7, 8) and column_choose is None:
        msg = ("`features` specification is required when `method` "+
               "is set to 'stratified_with_replacement' or 'stratified_without_replacement'.")
        logger.error(msg)
        raise ValueError(msg)
    if percentage is not None and sampling_size is not None:
        sampling_size = None

    unique_id = str(uuid.uuid1()).replace('-', '_').upper()

    res_tbl = "#SAMPLING_RESULT_TBL_{}".format(unique_id)
    param_rows = [('SAMPLING_METHOD', method, None, None),
                  ('INTERVAL', interval, None, None),
                  ('SAMPLING_SIZE', sampling_size, None, None),
                  ('RANDOM_SEED', random_state, None, None),
                  ('PERCENTAGE', None, percentage, None)]
    if column_choose is not None:
        param_rows.extend([('COLUMN_CHOOSE', col+1, None, None)
                           for col in column_choose])
    try:
        call_pal_auto_with_hint(conn,
                                None,
                                "PAL_SAMPLING",
                                data,
                                ParameterTable().with_data(param_rows),
                                res_tbl)
    except dbapi.Error as db_err:
        logger.exception(str(db_err))
        raise
    except Exception as db_err:
        logger.exception(str(db_err))
        raise
    return conn.table(res_tbl)

def variance_test(data, sigma_num, thread_ratio=None, key=None, data_col=None):
    """
    Variance Test is a method to identify the outliers of n number of numeric data {xi} where 0 < i < n+1,
    using the mean and the standard deviation of n number of numeric data.

    Parameters
    ----------

    data : DataFrame
        DataFrame containing the data.

    sigama_num : float
        Multiplier for sigma.

    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Default to 0.
    key : str, optional
        Name of the ID column in ``data``.

        If ``key`` is not specified, then:

            - if ``data`` is indexed by a single column, then ``key`` defaults
              to that index column;
            - otherwise, it defaults to the first column of ``data``.

    data_col : str, optional
        Name of the raw data column in the dataframe.

        If not specified, defaults to the last column of data.

    Returns
    -------
    DataFrame
        Sampling results, structured as follows:

            - DATA_ID: name as shown in input DataFrame.
            - IS_OUT_OF_RANGE: 0 -> in bounds, 1 -> out of bounds.

        Statistic results, structured as follows:

            - STAT_NAME:  statistic name.
            - STAT_VALUE: statistic value.

    Examples
    --------
    >>> res, stats = variance_test(data=df, sigma_num=3.0)
    >>> res.collect()
    >>> stats.collect()

    """
    conn = data.connection_context
    require_pal_usable(conn)
    sigma_num = arg('sigma_num', sigma_num, float)
    thread_ratio = arg('thread_ratio', thread_ratio, float)
    key = arg('key', key, str)
    index = data.index
    if isinstance(index, str):
        if key is not None and index != key:
            msg = "Discrepancy between the designated key column '{}' ".format(key) +\
            "and the designated index column '{}'.".format(index)
            logger.warning(msg)
        key = index if key is None else key
    if key is None:
        key = data.columns[0]
    data_col = arg('data_col', data_col, str)
    if data_col is None:
        data_col = data.columns[-1]
    if key == data_col:
        msg = ("Input data should have at least two columns, "+
               "including the ID column and the data column.")
        logger.error(msg)
        raise ValueError(msg)
    data_ = data[[key] + [data_col]]

    unique_id = str(uuid.uuid1()).replace('-', '_').upper()
    tables = ['RESULT', 'STATS']
    tables = ['#VARIENCE_TEST_{}_TBL_{}'.format(tbl, unique_id) for tbl in tables]
    res_tbl, stats_tbl = tables
    param_rows = [('SIGMA_NUM', None, sigma_num, None),
                  ('THREAD_RATIO', None, thread_ratio, None)]
    try:
        call_pal_auto_with_hint(conn,
                                None,
                                "PAL_VARIANCE_TEST",
                                data_,
                                ParameterTable().with_data(param_rows),
                                *tables)
    except dbapi.Error as db_err:
        logger.exception(str(db_err))
        try_drop(conn, tables)
        raise
    except Exception as db_err:
        logger.exception(str(db_err))
        try_drop(conn, tables)
        raise
    return conn.table(res_tbl), conn.table(stats_tbl)

class FeatureSelection(PALBase):
    r"""
    Feature selection(FS) is a dimensionality reduction technique, which selects a subset of relevant features for model construction,
    thus reducing the memory storage and improving computational efficiency while avoiding significant loss of information.

    Parameters
    ----------

    fs_method : {'anova', 'chi-squared', 'gini-index', 'fisher-score', 'information-gain', 'MRMR', 'JMI', 'IWFS', 'FCBF', 'laplacian-score', 'SPEC', 'ReliefF', 'ADMM', 'CSO'}

        **Statistical based FS methods**

            - 'anova':Anova.
            - 'chi-squared': Chi-squared.
            - 'gini-index': Gini Index.
            - 'fisher-score': Fisher Score.

        **Information theoretical based FS methods**

            - 'information-gain': Information Gain.
            - 'MRMR': Minimum Redundancy Maximum Relevance.
            - 'JMI': Joint Mutual Information.
            - 'IWFS': Interaction Weight Based Feature Selection.
            - 'FCBF': Fast Correlation Based Filter.

        **Similarity based FS methods**

            - 'laplacian-score': Laplacian Score.
            - 'SPEC': Spectral Feature Selection.
            - 'ReliefF': ReliefF.

        **Sparse Learning Based FS method**

            - 'ADMM': ADMM.

        **Wrapper method**

            - 'CSO': Competitive Swarm Optimizer.
    top_k_best : int, optional
        Top k features to be selected. Must be assigned a value except for FCBF and CSO. It will not affect FCBF and CSO.
    thread_ratio, float, optional
        The ratio of available threads.

          - 0: single thread
          - 0~1: percentage
          - others: heuristically determined

        Defaults to -1.
    seed :  int, optional
        Random seed. 0 means using system time as seed.

        Defaults to 0.
    fs_threshold : float, optional
        Predefined threshold for symmetrical uncertainty(SU) values between features and target. Used in FCBF.

        Defaults to 0.01.
    fs_n_neighbours : int, optional
        Number of neighbours considered in the computation of affinity matrix. Used in similarity based FS method.

        Defaults to 5.
    fs_category_weight : float, optional
        The weight of categorical features whilst calculating distance. Used in similarity based FS method.

        Defaults to 0.5*avg(all numerical columns's std)
    fs_sigma : float, optional
        Sigma in affinity matrix. Used in similarity based FS method.

        Defaults to 1.0.
    fs_regularization_power : int, optional
        The order of the power function that penalizes high frequency components. Used in SPEC.

        Defaults to 0.
    fs_rowsampling_ratio : float, optional
        The ratio of random sampling without replacement. Used in ReliefF, ADMM and CSO.

        Defaults to 0.6 in ReliefF, 1.0 in ADMM and CSO.
    fs_max_iter : int, opitional
        Maximal iterations allowed to run optimization. Used in ADMM.

        Defaults to 100.
    fs_admm_tol : float, optional
        Convergence threshold. Used in ADMM.

        Defaults to 0.0001.
    fs_admm_rho : float, optional
        Lagrangian Multiplier. Used in ADMM.

        Defaults to 1.0.
    fs_admm_mu : float, optional
        Gain of fs_admm_rho at each iteration. Used in ADMM.

        Defaults to 1.05.
    fs_admm_gamma : float, optional
        Regularization coefficient.

        Defaults to 1.0.
    cso_repeat_num : int, optional
        Number of repetitions to run CSO. CSO starts with a different initialization at each time. Used in CSO.

        Defaults to 2.
    cso_maxgeneration_num : int, optional
        Maximal number of generations. Used in CSO.

        Defaults to 100.
    cso_earlystop_num : int, optional
        Stop if there's no change in generation. Used in CSO.

        Defaults to 30.
    cso_population_size : int, optional
        Population size of the swarm particles. Used in CSO.

        Defaults to 30.
    cso_phi : float, optional
        Social factor. Used in CSO.

        Defaults to 0.1.
    cso_featurenum_penalty : float, optional
        The ratio for the spliting of training data and testing data.

        Defaults to 0.1.
    cso_test_ratio : float, optional
        The ratio for the spliting of training data and testing data.

        Defaults to 0.2.

    Attributes
    ----------
    result_ : DataFrame
        PAL returned result, structured as follows:

          - ROWID: Indicates the id of current row.
          - OUTPUT: Best set of features.

    Examples
    --------
    >>> fs = FeatureSelection(fs_method='fisher-score',
                              top_k_best=8)
    >>> fs_df = fs.fit_transform(data=df,
                                 categorical_variable=['X1'],
                                 label='Y')
    >>> fs.result_.collect()
    >>> fs_df.collect()
    """
    def __init__(self,
                 fs_method,
                 top_k_best=None,
                 thread_ratio=None,
                 seed=None,
                 fs_threshold=None,
                 fs_n_neighbours=None,
                 fs_category_weight=None,
                 fs_sigma=None,
                 fs_regularization_power=None,
                 fs_rowsampling_ratio=None,
                 fs_max_iter=None,
                 fs_admm_tol=None,
                 fs_admm_rho=None,
                 fs_admm_mu=None,
                 fs_admm_gamma=None,
                 cso_repeat_num=None,
                 cso_maxgeneration_num=None,
                 cso_earlystop_num=None,
                 cso_population_size=None,
                 cso_phi=None,
                 cso_featurenum_penalty=None,
                 cso_test_ratio=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(FeatureSelection, self).__init__()
        self.method_map = {'anova': 0,
                           'chi-squared': 1,
                           'gini-index': 2,
                           'fisher-score': 3,
                           'information-gain': 4,
                           'mrmr': 5,
                           'jmi': 6,
                           'iwfs': 7,
                           'fcbf': 8,
                           'laplacian-score': 9,
                           'spec': 10,
                           'relieff': 11,
                           'admm': 12,
                           'cso': 13}
        if fs_method == 'laplacian-score':
            self.op_name = 'FS_unsupervised'
        else:
            self.op_name = 'FS_supervised'
        self.fs_method = self._arg('fs_method', fs_method.lower(), self.method_map, required=True)
        if self.fs_method not in (8, 13):
            self.top_k_best = self._arg('top_k_best', top_k_best, int, required=True)
        else:
            self.top_k_best = self._arg('top_k_best', top_k_best, int)
        self.thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        self.seed = self._arg('seed', seed, float)
        self.fs_threshold = self._arg('fs_threshold', fs_threshold, float)
        self.fs_n_neighbours = self._arg('fs_n_neighbours', fs_n_neighbours, int)
        self.fs_category_weight = self._arg('fs_category_weight', fs_category_weight, float)
        self.fs_sigma = self._arg('fs_sigma', fs_sigma, float)
        self.fs_regularization_power = self._arg('fs_regularization_power', fs_regularization_power, int)
        self.fs_rowsampling_ratio = self._arg('fs_rowsampling_ratio', fs_rowsampling_ratio, float)
        self.fs_max_iter = self._arg('fs_max_iter', fs_max_iter, int)
        self.fs_admm_tol = self._arg('fs_admm_tol', fs_admm_tol, float)
        self.fs_admm_rho = self._arg('fs_admm_rho', fs_admm_rho, float)
        self.fs_admm_mu = self._arg('fs_admm_mu', fs_admm_mu, float)
        self.fs_admm_gamma = self._arg('fs_admm_gamma', fs_admm_gamma, float)
        self.cso_repeat_num = self._arg('cso_repeat_num', cso_repeat_num, int)
        self.cso_maxgeneration_num = self._arg('cso_maxgeneration_num', cso_maxgeneration_num, int)
        self.cso_earlystop_num = self._arg('cso_earlystop_num', cso_earlystop_num, int)
        self.cso_population_size = self._arg('cso_population_size', cso_population_size, float)
        self.cso_phi = self._arg('cso_phi', cso_phi, float)
        self.cso_featurenum_penalty = self._arg('cso_featurenum_penalty', cso_featurenum_penalty, float)
        self.cso_test_ratio = self._arg('cso_test_ratio', cso_test_ratio, float)
        self.result_ = None

    def fit(self, data,
            key=None,
            label=None,
            categorical_variable=None,
            fixed_feature=None,
            excluded_feature=None,
            verbose=None):
        """
        Perform feature selection for given data with specified configuration.

        Parameters
        ----------
        data : DataFrame
            Input HANA Dataframe.
        key : str, optional
            Name of the ID column. If data has index, it will be set.

            There's no id column by default.
        label : str, optional
            Specifies the dependent variable by name.

            Mandatory for supervised feature selection methods.

            For 'spec' method which can be supervised and unsupervised,
            if ``label`` is not set, the unsupervised version will be performed.
        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.
        fixed_feature : str or a list of str, optional
            Will always be selected out as the best subset.

        excluded_feature : str or a list of str, optional
            Excludes the indicated columns as feature candidates.

        verbose : bool, optional
            Indicates whether to output more specified results.

            Defaults to False.

        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        self.fit_transform(data=data,
                           key=key,
                           label=label,
                           categorical_variable=categorical_variable,
                           fixed_feature=fixed_feature,
                           excluded_feature=excluded_feature,
                           verbose=verbose)
        return self

    def fit_transform(self, data,
                      key=None,
                      label=None,
                      categorical_variable=None,
                      fixed_feature=None,
                      excluded_feature=None,
                      verbose=None):
        """
        Perform feature selection for given data with specified configuration.

        Parameters
        ----------
        data : DataFrame
            Dataframe that contains the training data.
        key : str, optional
            Name of the ID column. If data has index, it will be set.

            There's no id column by default.
        label : str, optional
            Specifies the dependent variable by name.

            Mandatory for supervised feature selection methods.

            For 'spec' method which can be supervised and unsupervised,
            if ``label`` is not set, the unsupervised version will be performed.
        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.
        fixed_feature : str or a list of str, optional
            Will always be selected out as the best subset.
        excluded_feature : str or a list of str, optional
            Excludes the indicated columns as feature candidates.
        verbose : bool, optional
            Indicates whether to output more specified results.

            Defaults to False.

        Returns
        -------
        DataFrame
            Feature selection result from the input data.
        """
        conn = data.connection_context
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        require_pal_usable(conn)
        has_id = key is not None
        if not self._disable_hana_execution:
            index = data.index
            if key:
                has_id = True
            if index:
                has_id = True
            if has_id:
                key = self._arg('key', key, str, required=not isinstance(index, str))
                if isinstance(index, str):
                    if key is not None and index != key:
                        msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                              "and the designated index column '{}'.".format(index)
                        logger.warning(msg)
                key = index if key is None else key
                cols = data.columns
                cols.remove(key)
                data = data[[key] + cols]
        label = arg('label', label, str, required=self.fs_method not in [9, 10])
        if isinstance(fixed_feature, str):
            fixed_feature = [fixed_feature]
        if isinstance(categorical_variable, str):
            categorical_variable = [categorical_variable]
        if isinstance(excluded_feature, str):
            excluded_feature = [excluded_feature]
        fixed_feature = self._arg('fixed_feature', fixed_feature, ListOfStrings)
        categorical_variable = self._arg('categorical_variable', categorical_variable,
                                         ListOfStrings)
        excluded_feature = self._arg('excluded_feature', excluded_feature, ListOfStrings)
        verbose = self._arg('verbose', verbose, bool)
        tables = ['RESULT']
        tables = ['#FEATURESELECTION_{}_TBL_{}'.format(tbl, unique_id) for tbl in tables]
        res_tbl = tables[0]

        param_rows = [('FS_METHOD', self.fs_method, None, None),
                      ('HAS_ID', has_id, None, None),
                      ('TOP_K_BEST', self.top_k_best, None, None),
                      ('DEPENDENT_VARIABLE', None, None, label),
                      ('VERBOSE', verbose, None, None),
                      ('THREAD_RATIO', None, self.thread_ratio, None),
                      ('SEED', self.seed, None, None),
                      ('FS_ROWSAMPLING_RATIO', None, self.fs_rowsampling_ratio, None),
                      ('FS_THRESHOLD', None, self.fs_threshold, None),
                      ('FS_N_NEIGHBOURS', self.fs_n_neighbours, None, None),
                      ('FS_CATEGORY_WEIGHT', None, self.fs_category_weight, None),
                      ('FS_SIGMA', None, self.fs_sigma, None),
                      ('FS_REGULARIZATION_POWER', self.fs_regularization_power, None, None),
                      ('FS_MAX_ITER', self.fs_max_iter, None, None),
                      ('FS_ADMM_TOL', None, self.fs_admm_tol, None),
                      ('FS_ADMM_RHO', None, self.fs_admm_rho, None),
                      ('FS_ADMM_MU', None, self.fs_admm_mu, None),
                      ('FS_ADMM_GAMMA', None, self.fs_admm_gamma, None),
                      ('CSO_REPEAT_NUM', self.cso_repeat_num, None, None),
                      ('CSO_MAXGENERATION_NUM', self.cso_maxgeneration_num, None, None),
                      ('CSO_EARLYSTOP_NUM', self.cso_earlystop_num, None, None),
                      ('CSO_POPULATION_SIZE', self.cso_population_size, None, None),
                      ('CSO_PHI', None, self.cso_phi, None),
                      ('CSO_FEATURENUM_PENALTY', None, self.cso_featurenum_penalty, None),
                      ('CSO_TEST_RATIO', None, self.cso_test_ratio, None)]
        if fixed_feature is not None:
            param_rows.extend([('FIXED_FEATURE', None,
                                None, var) for var in fixed_feature])
        if categorical_variable is not None:
            param_rows.extend([('CATEGORICAL_VARIABLE', None,
                                None, var) for var in categorical_variable])
        if excluded_feature is not None:
            param_rows.extend([('EXCLUDED_FEATURE', None,
                                None, var) for var in excluded_feature])
        try:
            self._call_pal_auto(conn,
                                "PAL_FEATURE_SELECTION",
                                data,
                                ParameterTable().with_data(param_rows),
                                *tables)
        except dbapi.Error as db_err:
            msg = str(conn.hana_version())
            logger.exception("HANA version: %s. %s", msg, str(db_err))
            try_drop(conn, tables)
            raise
        except Exception as db_err:
            msg = str(conn.hana_version())
            logger.exception("HANA version: %s. %s", msg, str(db_err))
            try_drop(conn, tables)
            raise
        self.result_ = conn.table(res_tbl)
        result = self.result_
        if not self._disable_hana_execution:
            select_cols = json.loads(self.result_.collect().iat[0, 1])["__SelectedFeatures__"]
            if has_id:
                if key not in select_cols:
                    select_cols = [key] + select_cols
            result = data.select(select_cols)
        return result


class ImputeTS(PALBase):
    r"""
    Imputation of multi-dimensional time-series data.
    This is the Python wrapper for PAL procedure PAL_IMPUTE_TIME_SERIES.

    Parameters
    ----------
    imputation_type : str, optional
         Specifies the `overall` imputation type for all columns of the time-series data.
         Valid options include:

           - 'non' : Does nothing. Leave all columns untouched.
           - 'most_frequent-allzero' : For any categorical column, fill all missing values by the value that
             appears most often in that column; while for any numerical column, fill all missing values by zero.
           - 'most_frequent-mean' : For any categorical column, fill all missing values by the value that
             appears most often in that column; while for any numerical column, fill all missing its mean.
           - 'most_frequent-median' : For any categorical column, fill all missing values by the value that
             appears most often in that column; while for any numerical column, fill all missing values by median.
           - 'most_frequent-sma' : For any categorical column, fill all missing values by the value that
             appears most often in that column; while for any numerical column, fill all missing values
             via **simple moving average** method.
           - 'most_frequent-lma' : For any categorical column, fill all missing values by the value that
             appears most often in that column; while for any numerical column, fill all missing values via
             **linear moving average** method.
           - 'most_frequent-ema' : For any categorical column, fill all missing values by the value that
             appears most often in that column; while for any numerical column, fill all missing values by
             **exponential moving average** method.
           - 'most_frequent-linterp' : For any categorical column, fill all missing values by the value that
             appears most often in that column; while for any numerical column, fill all missing values via
             linear interpolation.
           - 'most_frequent-sinterp' : For any categorical column, fill all missing values by the value that
             appears most often in that column; while for any numerical column, fill all missing values via
             spline interpolation.
           - 'most_frequent-seadec' : For any categorical column, fill all missing values by the value that
             appears most often in that column; while for any numerical column, fill all missing values via
             seasonal decompose.
           - 'most_frequent-locf' : For any categorical column, fill all missing values by the value that
             appears most often in that column; while for any numerical column, fill all missing values via
             **last observation carried forward**.
           - 'most_frequent-nocb' : For any categorical column, fill all missing values by the value that
             appears most often in that column; while for any numerical column, fill all missing values via
             ***next observation carried back**.

        The preface 'most_frequent' can be omitted for simplicity.

        Defaults to 'most_frequent-mean'.
    base_algorithm : str, optional
        Specifies the base imputation algorithm for **seasonal decompose**.
        Applicable only to numerical data columns that are to be imputed by **seasonal decompose**.
        Valid options include:

            - 'allzero' : Fill all missing values by zero.
            - 'mean' : Fill all missing values by the mean of the column.
            - 'median' : Fill all missing values by the median of the column.
            - 'sma' : Fill all missing values via **simple moving average** method.
            - 'lma' : Fill all missing values via **linear moving average** method.
            - 'ema' : Fill all missing values via **exponential moving average** method.
            - 'linterp' : Fill all missing values via linear interpolation.
            - 'sinterp' : Fill all missing values via spline interpolation.
            - 'locf' : Fill all missing values via **last observation carried forward**.
            - 'nocb' : Fill all missing values via **next observation carried backward**.

        Defaults to 'mean'.
    alpha : float, optional
        Specifies the criterion for the autocorrelation coefficient.
        The value range is (0, 1).
        A larger value indicates stricter requirement for seasonality.

        Defaults to 0.2.
    extrapolation : bool, optional
        Specifies whether or not to extrapolate the endpoints of the time-series data.

        Defaults to False.
    smooth_width : int, optional
        Specifies the width of the moving average applied to non-seasonal data,
        where 0 indicates linear fitting to extract trends.

        Effective only to data columns that are to be imputed via **seasonal decompose**.
    auxiliary_normalitytest : bool, optional
        Specifies whether to use normality test to identify model types or not.

        Defaults False.
    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 1.

    Attributes
    ----------
    model\_ : DataFrame
        A column-wise time-series imputation model stored in statistics format, i.e. with
        stat names and stat values.

    result\_ : DataFrame
        The imputation result, structured the same as the data used for
        obtaining the time-series imputation model, with all missing
        valued filled.

    Examples
    --------
    >>> imp = ImputeTS(imputation_type='most_frequent-linterp')
    >>> res = imp.fit_transform(data=df, key='ID')
    >>> res.collect()

   """
    imputation_type_map = {'non' : 0, 'most_frequent-allzero' : 1,
                           'allzero' : 1,
                           'most_frequent-mean' : 2,
                           'mean' : 2,
                           'most_frequent-median' : 3,
                           'median' : 3,
                           'most_frequent-sma' : 4,
                           'sma' : 4,
                           'most_frequent-lma' : 5,
                           'lma' : 5,
                           'most_frequent-ema' : 6,
                           'ema' : 6,
                           'most_frequent-linterp' : 7,
                           'linterp' : 7,
                           'most_frequent-sinterp' : 8,
                           'sinterp' : 8,
                           'most_frequent-seadec' : 9,
                           'seadec' : 9,
                           'most_frequent-locf' : 10,
                           'locf' : 10,
                           'most_frequent-nocb' : 11,
                           'nocb' : 11}
    col_imputation_type_map = {'non' : 0, 'most_frequent' : 1,
                               'allzero' : 2, 'mean' : 3,
                               'median' : 4,
                               'categorical_const' : 5,
                               'numerical_const' : 6,
                               'sma' : 7, 'lma' : 8, 'ema' : 9,
                               'linterp' : 10,
                               'sinterp' : 11,
                               'seadec' : 12, 'locf' : 13,
                               'nocb' : 14}
    base_imputation_alg_map = {'allzero' : 0, 'mean' : 1,
                               'median' : 2, 'sma' : 3,
                               'lma' : 4, 'ema' : 5,
                               'linterp' : 6, 'sinterp' : 7,
                               'locf' : 8, 'nocb' : 9}
    def __init__(self,
                 imputation_type=None,
                 base_algorithm=None,
                 alpha=None,
                 extrapolation=None,
                 smooth_width=None,
                 auxiliary_normalitytest=None,
                 thread_ratio=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(ImputeTS, self).__init__()
        self.imputation_type = self._arg('imputation_type',
                                         imputation_type,
                                         self.imputation_type_map)
        self.base_algorithm = self._arg('base_algorithm', base_algorithm,
                                        self.base_imputation_alg_map)
        self.alpha = self._arg('alpha', alpha, float)
        self.extrapolation = self._arg('extrapolation', extrapolation, bool)
        self.smooth_width = self._arg('smooth_width', smooth_width, int)
        self.auxiliary_normalitytest = self._arg('auxiliary_normalitytest',
                                                 auxiliary_normalitytest,
                                                 bool)
        self.thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        self.result_ = None
        self.model_ = None
        self.op_name = 'ImputeTS'

    def fit(self, data, key=None, features=None,
            categorical_variable=None,
            col_imputation_type=None):
        r"""
        Fit function for Time-series Imputation.

        Parameters
        ----------
        data : DataFrame
            DataFrame containing the time-series data for imputation.
        key : str, optional
            Specifies the name of the time-stamp column of ``data`` that represents data ordering.

            Data type of the column could be INTEGER, DATE or SECONDDATE.

            Mandatory if ``data`` is not indexed by a single column.

            Defaults to index column of ``data`` if not provided.
        features : str or a list of str, optional
            Specifies the names of the columns in ``data`` that are to be imputed.

            Defaults to all non-key columns of ``data``.
        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.
        col_imputation_type : ListOfTuples or dict, optional
            Specifies the column-wise imputation type that overwrites the generic imputation type
            in class initialization.

            Should be list of tuples, where each tuple contains 2 elements:

            - 1st element : the column name
            - 2nd element : the imputation type or value. Imputation type could be one of the following:

              .. only:: html

                  * 'non' : Does nothing.
                  * 'most_frequent' : Fill all missing values the most frequently observed value.
                  * 'allzero' : Fill all missing values by zero.
                  * 'mean' : Fill all missing values by the mean of the column.
                  * 'median' : Fill all missing values by the median of the column.
                  * 'sma' : Fill all missing values via **simple moving average** method.
                  * 'lma' : Fill all missing values via **linear moving average** method.
                  * 'ema' : Fill all missing values via **exponential moving average** method.
                  * 'linterp' : Fill all missing values via linear interpolation.
                  * 'sinterp' : Fill all missing values via spline interpolation.
                  * 'locf' : Fill all missing values via **last observation carried forward**.
                  * 'nocb' : Fill all missing values via **next observation carried backward**.

              .. only:: latex

                  ===============  ===================================================================
                  Imputation Type  Explanation
                  ===============  ===================================================================
                  'non'            Does nothing.
                  'most_frequent'  Fill all missing values the most frequently observed value.
                  'allzero'        Fill all missing values by zero.
                  'mean'           Fill all missing values by the mean of the column.
                  'median'         Fill all missing values by the median of the column.
                  'sma'            Fill all missing values via **simple moving average** method.
                  'lma'            Fill all missing values via **linear moving average** method.
                  'ema'            Fill all missing values via **exponential moving average** method.
                  'linterp'        Fill all missing values via linear interpolation.
                  'sinterp'        Fill all missing values via spline interpolation.
                  'locf'           Fill all missing values via **last observation carried forward**.
                  'nocb'           Fill all missing values via **next observation carried backward**.
                  ===============  ===================================================================

              Among the above options, 'non' applies to both numerical and categorical columns,
              'most_frequent' applies to categorical columns only, while the rest apply to numerical
              columns only. If the input goes beyond the above list of options, it will be treated as
              a constant value for the **universal replacement** of all missing values in the column.

        Returns
        -------
        A fitted object of class "ImputeTS".
        """
        conn = data.connection_context
        #require_pal_usable(conn)
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        if not isinstance(col_imputation_type, dict):
            col_imputation_type = self._arg('col_imputation_type',
                                            col_imputation_type,
                                            ListOfTuples)
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        index = data.index
        key = self._arg('key', key, str, required=not isinstance(index, str))
        if isinstance(index, str):
            if key is not None and index != key:
                msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(msg)
        key = index if key is None else key
        if not self._disable_hana_execution:
            cols = data.columns
            cols.remove(key)
            if isinstance(features, str):
                features = [features]
            features = self._arg('features', features, ListOfStrings)
            features = cols if features is None else features
            if isinstance(categorical_variable, str):
                categorical_variable = [categorical_variable]
            categorical_variable = self._arg('categorical_variable',
                                             categorical_variable,
                                             ListOfStrings)
            if categorical_variable is None:
                categorical_variable = []
            if col_imputation_type is not None:
                if isinstance(col_imputation_type, (list, tuple)):
                    if not all(len(coltyp) == 2 for coltyp in col_imputation_type):
                        msg = "Each tuple in 'col_imputation_type' must be a pair, " +\
                        "with the 1st element being the column name, and 2nd element " +\
                        "being the imputation type/value."
                        raise ValueError(msg)
                    if not all(coltyp[0] in features for coltyp in col_imputation_type):
                        msg = "The 1st element in each tuple of 'col_imputation_type' " +\
                        "must be a valid column name in data."
                        raise ValueError(msg)
            data_ = data[[key] + features]
            ddtypes = {tp[0]:tp[1] for tp in data.dtypes()}
            for cat_var in categorical_variable:
                if 'INT' not in ddtypes[cat_var]:
                    categorical_variable.remove(cat_var)
        else:
            data_ = data
        tables = ['RESULT', 'STATS_MODEL']
        tables = ['#PAL_IMPUTE_TS_{}_TBL_{}'.format(tbl, unique_id) for tbl in tables]
        param_rows = [('THREAD_RATIO', None, self.thread_ratio, None),
                      ('IMPUTATION_TYPE', self.imputation_type, None, None),
                      ('BASE_ALGORITHM', self.base_algorithm, None, None),
                      ('ALPHA', None, self.alpha, None),
                      ('EXTRAPOLATION', self.extrapolation, None, None),
                      ('SMOOTH_WIDTH', self.smooth_width, None, None),
                      ('AUXILIARY_NORMALITYTEST', self.auxiliary_normalitytest, None, None)]
        if col_imputation_type is not None:
            if isinstance(col_imputation_type, (list, tuple)):
                for coltyp in col_imputation_type:
                    if coltyp[1] in self.col_imputation_type_map:
                        param_rows.extend([('{}_IMPUTATION_TYPE'.format(coltyp[0]),
                                            self.col_imputation_type_map[coltyp[1]],
                                            None, None)])
                    elif isinstance(coltyp[1], str) or coltyp[0] in categorical_variable:
                        param_rows.extend([('{}_IMPUTATION_TYPE'.format(coltyp[0]),
                                            5, None, str(coltyp[1]))])
                    elif isinstance(coltyp[1], (float, int)):
                        param_rows.extend([('{}_IMPUTATION_TYPE'.format(coltyp[0]),
                                            6, coltyp[1], None)])
            elif isinstance(col_imputation_type, dict):
                for kkey, vval in col_imputation_type.items():
                    if vval in self.col_imputation_type_map:
                        param_rows.extend([('{}_IMPUTATION_TYPE'.format(kkey),
                                        self.col_imputation_type_map[vval],
                                        None, None)])
                    elif isinstance(vval, str) or kkey in categorical_variable:
                        param_rows.extend([('{}_IMPUTATION_TYPE'.format(kkey),
                                            5, None, str(vval))])
                    elif isinstance(vval, (float, int)):
                        param_rows.extend([('{}_IMPUTATION_TYPE'.format(kkey),
                                            6, vval, None)])

        if categorical_variable is not None:
            param_rows.extend([('CATEGORICAL_VARIABLE',
                                None, None, var) for var in categorical_variable])
        try:
            self._call_pal_auto(conn,
                                'PAL_IMPUTE_TIME_SERIES',
                                data_,
                                ParameterTable().with_data(param_rows),
                                *tables)
        except dbapi.Error as db_err:
            msg = str(conn.hana_version())
            logger.exception("HANA version: %s. %s", msg, str(db_err))
            try_drop(conn, tables)
            raise
        except Exception as db_err:
            msg = str(conn.hana_version())
            logger.exception("HANA version: %s. %s", msg, str(db_err))
            try_drop(conn, tables)
            raise
        self.result_ = conn.table(tables[0])
        self.model_ = conn.table(tables[1])
        return self

    def fit_transform(self, data, key=None, features=None,
                      categorical_variable=None,
                      col_imputation_type=None):
        r"""
        Impute the input data and returned the imputation result.

        Parameters
        ----------
        data : DataFrame
            DataFrame containing the time-series data for imputation.
        key : str, optional
            Specifies the name of the time-stamp column of ``data`` that represents data ordering.

            Data type of the column could be INTEGER, DATE or SECONDDATE.

            Mandatory if ``data`` is not indexed by a single column.

            Defaults to index column of ``data`` if not provided.
        features : str or a list of str, optional
            Specifies the names of the columns in ``data`` that are to be imputed.

            Defaults to all non-key columns of ``data``.
        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.
        col_imputation_type : ListOfTuples, optional
            Specifies the column-wise imputation type that overwrites the generic imputation type
            in class initialization.

            Should be list of tuples, where each tuple contains 2 elements:

            - 1st element : the column name
            - 2nd element : the imputation type or value. Imputation type could be one of the following:

              .. only:: html

                  * 'non' : Does nothing.
                  * 'most_frequent' : Fill all missing values the most frequently observed value.
                  * 'allzero' : Fill all missing values by zero.
                  * 'mean' : Fill all missing values by the mean of the column.
                  * 'median' : Fill all missing values by the median of the column.
                  * 'sma' : Fill all missing values via **simple moving average** method.
                  * 'lma' : Fill all missing values via **linear moving average** method.
                  * 'ema' : Fill all missing values via **exponential moving average** method.
                  * 'linterp' : Fill all missing values via linear interpolation.
                  * 'sinterp' : Fill all missing values via spline interpolation.
                  * 'locf' : Fill all missing values via **last observation carried forward**.
                  * 'nocb' : Fill all missing values via **next observation carried backward**.

              .. only:: latex

                  ===============  ===================================================================
                  Imputation Type  Explanation
                  ===============  ===================================================================
                  'non'            Does nothing.
                  'most_frequent'  Fill all missing values the most frequently observed value.
                  'allzero'        Fill all missing values by zero.
                  'mean'           Fill all missing values by the mean of the column.
                  'median'         Fill all missing values by the median of the column.
                  'sma'            Fill all missing values via **simple moving average** method.
                  'lma'            Fill all missing values via **linear moving average** method.
                  'ema'            Fill all missing values via **exponential moving average** method.
                  'linterp'        Fill all missing values via linear interpolation.
                  'sinterp'        Fill all missing values via spline interpolation.
                  'locf'           Fill all missing values via **last observation carried forward**.
                  'nocb'           Fill all missing values via **next observation carried backward**.
                  ===============  ===================================================================

             Among the above options, 'non' applies to both numerical and categorical columns,
             'most_frequent' applies to categorical columns only, while the rest apply to numerical
             columns only. If the input goes beyond the above list of options, it will be treated as
             a constant value for the **univeral replacement** of all missing values in the column.

        Returns
        -------
        DataFrame
            The imputed result of ``data``.
        """
        self.fit(data, key, features,
                 categorical_variable,
                 col_imputation_type)
        return self.result_

    def transform(self, data, key=None, features=None,
                  thread_ratio=None, model=None):
        r"""
        Impute TS data using model info.

        Parameters
        ----------
        data : DataFrame
            DataFrame containing the time-series data for imputation by model.
        key : str, optional
            Specifies the name of the time-stamp column of ``data`` that represents data ordering.

            Data type of the column could be INTEGER, DATE or SECONDDATE.

            Mandatory if ``data`` is not indexed by a single column.

            Defaults to index column of ``data`` if not provided.
        features : str or a list of str, optional
            Specifies the names of the columns in ``data`` that are to be imputed by model.

            Defaults to all non-key columns of ``data``.
        thread_ratio : float, optional
            Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
            Values outside the range will be ignored and this function heuristically determines the number of threads to use.

            Defaults to 1.
        model : DataFrame, optional
            Specifies the model for time-series imputation.

            Defaults to self.`model_`.

        Returns
        -------
        DataFrame
            The imputed result of ``data`` by model.

        DataFrame
            Statistics, storing the imputation types of all selected
            feature columns in ``data``.
        """
        conn = data.connection_context
        require_pal_usable(conn)
        thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        index = data.index
        key = self._arg('key', key, str, required=not isinstance(index, str))
        if isinstance(index, str):
            if key is not None and index != key:
                msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(index)
                logger.warning(msg)
        key = index if key is None else key
        cols = data.columns
        cols.remove(key)
        if isinstance(features, str):
            features = [features]
        features = self._arg('features', features, ListOfStrings)
        features = cols if features is None else features
        data = data[[key] + features]
        model = self.model_ if model is None else model
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        tables = ['RESULT', 'STATS']
        tables = ['#PAL_IMPUTE_TS_WMODEL_{}_TBL_{}'.format(tbl, unique_id) for tbl in tables]
        param_rows = [('THREAD_RATIO', None, self.thread_ratio, None)]
        try:
            self._call_pal_auto(conn,
                                'PAL_IMPUTE_TIME_SERIES_WITH_MODEL',
                                data,
                                model,
                                ParameterTable().with_data(param_rows),
                                *tables)
        except dbapi.Error as db_err:
            msg = str(conn.hana_version())
            logger.exception("HANA version: %s. %s", msg, str(db_err))
            try_drop(conn, tables)
            raise
        except Exception as db_err:
            msg = str(conn.hana_version())
            logger.exception("HANA version: %s. %s", msg, str(db_err))
            try_drop(conn, tables)
            raise
        return conn.table(tables[0]), conn.table(tables[1])

class PowerTransform(PALBase):
    r"""
    This class implements a python interface for the power transform algorithm in PAL.
    Power Transform is the family of non-linear transformation methods that can stabilize
    he variance of data, minimize its skewness, and approximate its distribution as Gaussian.

    Power Transform supports **Box-Cox** transformation and **Yeo-Johnson** transformation.
    Both transformations are monotonically increasing functions with one hyper-parameter,
    nominally :math:`\lambda`.

    Box-Cox transformation is restricted to positive data only, with mathematically formula
    illustrated as follows:

    .. math::

        x^{(\lambda)} = \begin{cases} \frac{x^{\lambda} - 1}{\lambda}, &\text{ if }\lambda\neq 0\\
                                      \ln{(x)}, & \text{ if }\lambda = 0 \end{cases}

    where :math:`x^{(\lambda)}` represents the value after transform. In contrast, the
    Yeo-Johnson transformation can be applied to any real data, with the following mathematical formula:

    .. math::

        x^{(\lambda)} = \begin{cases} \frac{(x+1)^{\lambda} - 1}{\lambda}, &\text{ if }\lambda\neq 0, x\geq 0\\
                                      \ln{(x+1)}, &\text{ if }\lambda = 0, x\geq 0\\
                                      -\frac{(1-x)^{(2-\lambda)} - 1}{2-\lambda}, &\text{ if }\lambda\neq 2, x < 0\\
                                      -\ln{(1-x)}, &\text{ if }\lambda = 2, x < 0 \end{cases}

    For given collection of data, the hyperparameter :math:`\lambda` can be estimated by
    maximizing the log-likelihood function.

    Parameters
    ----------
    method : {'boxcox', 'yeojohnson'}, optional
        Specifies the type of power transformation.

        - 'boxcox': Box-Cox transformation.
        - 'yeojohnson': Yeo-Johnson transformation.

        Defaults to 'yeojohnson'.
    standardize : bool, optional
        Specifies whether or not the standardize the result of
        power transformation as output.

        Defaults to True.
    max_iter : int, optional
        Specifies the maximum number of iterations for fitting the power parameter :math:`\lambda`.

        If convergence is not reached after the specified number of iterations, an
        error will be generated. In this case, users should enlarge the number and re-try.

        Defaults to 500.
    tol : float, optional
        Specifies the absolute tolerance to control the accuracy of fitted parameter parameter
        :math:`\lambda`. The value should be positive but less than 1.

        Defaults to 1e-11.
    interval : list, optional
        Specifies the global search interval for power parameter :math:`\lambda` in a list of two numbers:
        1st number for interval start, and 2nd number for interval end. A natural restriction is that interval start
        should be less than interval end.

        Defaults to [-2.0, 2.0].
    interval_hint : bool, optional
        Specifies whether or not to use the specified interval as hint. If True, the specifies interval
        is only used for initial search range, while the final power parameter :math:`\lambda` may fall outside
        the specified interval.

        Defaults to False(means that final :math:`\lambda` must fall into the specified interval).
    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 1.0.

    Attributes
    ----------
    model_ : DataFrame
        Model content.

    result_ : DataFrame
        DataFrame containing the transformed result of the data feeded to the `fit()` function.

        Available and non-empty only calling the `fit_transform()` function.

    Examples
    --------
    >>> pt = PowerTransform(method='yeojohnson')
    >>> res = pt.fit_transform(data=df, key='ID')
    >>> res.collect()
    >>> pt.model_.collect()

    """
    method_map = {'yeojohnson':0, 'boxcox':1}

    def __init__(self,
                method=None,
                standardize=None,
                max_iter=None,
                tol=None,
                interval=None,
                interval_hint=None,
                thread_ratio=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(PowerTransform, self).__init__()
        self.method = self._arg('method', method, self.method_map)
        self.standardize = self._arg('standardize', standardize, bool)
        self.max_iter = self._arg('max_iter', max_iter, int)
        self.tol = self._arg('tol', tol, float)
        self.interval = self._arg('interval', interval, list)
        if self.interval is not None:
            assert len(self.interval) == 2
            se_dict = {'start': 0, 'end': 1}
            for se in se_dict:
                self.interval[se_dict[se]] = self._arg('Interval {}'.format(se),
                                                       self.interval[se_dict[se]],
                                                       (float, int))
        self.interval_hint = self._arg('interval_hint', interval_hint, bool)
        self.thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        self.model_ = None
        self.result_ = None

    @trace_sql
    def __fit_transform(self, data,
                        key=None,
                        features=None,
                        feature_interval=None,
                        feature_interval_hint=None,
                        output=False):
        setattr(self, 'hanaml_fit_params', pal_param_register())
        index = data.index
        cols  = data.columns
        key = key if key else index
        key = self._arg('key', key, str, required=index is None)
        if key not in cols:
            msg = f"Please select key from {cols}!"
            logger.error(msg)
            raise ValueError(msg)
        cols.remove(key)
        if isinstance(features, str):
            features = [features]
        features = self._arg('features', features, ListOfStrings)
        features = features if features else cols
        feature_interval = self._arg('feature_interval', feature_interval, dict)
        feature_interval_hint = self._arg('feature_interval_hint', feature_interval_hint, dict)
        if feature_interval is not None:
            for feature in feature_interval:
                if feature not in features:
                    msg = 'Invalid key {} in feature_interval, it must be selected from: '.format(feature) +\
                          ', '.join(features) + '.'
                    raise ValueError(msg)
                feature_interval[feature] = self._arg(f'Interval for feature {feature}',
                                                      feature_interval[feature],
                                                      list)
        if feature_interval_hint is not None:
            for feature in feature_interval_hint:
                if feature not in features:
                    msg = 'Invalid key {} in feature_interval_hint, it must be selected from: '.format(feature) +\
                          ', '.join(features) + '.'
                    raise ValueError(msg)
                feature_interval_hint[feature] = self._arg(f'Interval hint for feature {feature}',
                                                           feature_interval_hint[feature],
                                                           bool)
        data_ = data[[key] + features]
        conn = data.connection_context
        require_pal_usable(conn)

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        model_tbl = f'#PAL_POWER_TRANSFORM_MODEL_TBL_{self.id}_{unique_id}'
        result_tbl = f'#PAL_POWER_TRANSFORM_RESULT_TBL_{self.id}_{unique_id}'
        outputs = [model_tbl, result_tbl]
        param_rows = [
            ('METHOD', self.method, None, None),
            ('STANDARDIZE', self.standardize, None, None),
            ('OUTPUT_TRANSFORM', output, None, None),
            ('MAX_ITERATION', self.max_iter, None, None),
            ('TOLERANCE', None, self.tol, None),
            ('THREAD_RATIO', None, self.thread_ratio, None),
            ('INTERVAL_HINT', self.interval_hint, None, None)]
        pt_dict = {'START':0, 'END':1}
        if self.interval is not None:
            param_rows.extend([(f'INTERVAL_{pt}', None,
                                self.interval[pt_dict[pt]],
                                None) for pt in pt_dict])
        if feature_interval_hint is not None:
            param_rows.extend([(f'FEATURE_{ft}_INTERVAL_HINT',
                                feature_interval_hint[ft],
                                None, None) for ft in feature_interval_hint])
        if feature_interval is not None:
            param_rows.extend([(f'FEATURE_{ft}_INTERVAL_START', None,
                                feature_interval[ft][0], None)
                               for ft in feature_interval])
            param_rows.extend([(f'FEATURE_{ft}_INTERVAL_END', None,
                                feature_interval[ft][1], None)
                               for ft in feature_interval])
        if not (check_pal_function_exist(conn, '%GAUSSIAN_TRANSFORM%', like=True) or
                self._disable_hana_execution):
            msg = 'The version of your SAP HANA does not support Power Transform!'
            logger.error(msg)
            raise ValueError(msg)
        try:
            self._call_pal_auto(conn,
                                'PAL_POWER_TRANSFORMER_FIT',
                                data_,
                                ParameterTable().with_data(param_rows),
                                *outputs)
        except dbapi.Error as db_err:
            msg = str(conn.hana_version())
            logger.exception("HANA version: %s. %s", msg, str(db_err))
            try_drop(conn, outputs)
            raise
        except Exception as db_err:
            msg = str(conn.hana_version())
            logger.exception("HANA version: %s. %s", msg, str(db_err))
            try_drop(conn, outputs)
            raise
        self.model_ = conn.table(model_tbl)
        self.result_ = conn.table(result_tbl)
        if output:
            return self.result_
        return self

    def fit(self, data, key=None, features=None, feature_interval=None, feature_interval_hint=None):
        r"""
        Fit the data to be transformed to obtain the feature-wise transformation parameter respectively.

        Parameters
        ----------
        data : DataFrame
            DataFrame containing the data to apply power transform.

        key : str, optional
            Specifies the name of ID column in ``data``.

            Defaults to the index of ``data`` if ``data`` is indexed by a single column, otherwise
            it ``key`` must be specified explicitly(i.e. mandatory).

        features : str or a list of str, optional
            Specifies the names of the columns in ``data`` to apply power transform to.

            All columns in ``features`` must be of numerical data type.

            Defaults to all non-key columns of ``data`` if not provided.

        feature_interval : dict, optional
            Specifies the power parameter search intervals for features in a dictionary format.
            For each key-value pair in ``feature_interval``, key is the feature name, and value is
            the specified interval.

            For example, if ``data`` contains a feature 'X1' that needs to be transformed, and we want
            the power parameter to be searched from the range [-3, 3] for 'X1',
            then the key-value pair `'X1':[-3, 3]'` should be specified within ``feature_interval``. This
            overwrites the global interval specified by the parameter ``interval`` in class initialization.

            If not provided, the power parameter will be searched within a global interval specified by parameter
            ``interval`` in class initialization.

        feature_interval_hint: dict, optional
            Specifies whether or not to use the specified intervals as hint for different features.

            For each key-value pair in ``feature_interval_hint``, key is the feature name, and value is bool indicating
            whether or not to use specified interval as hint for the corresponding feature specified by key. This overwrites
            the global hint choice specified by the parameter ``hint`` in class initialization.

            If not provided, the value of ``hint`` parameter in class initialization will be applied to all features.

        Returns
        -------
        A fitted object of class "PowerTransform".

        Examples
        --------
        >>> pt = PowerTransform(method='yeojohnson')
        >>> res = pt.fit(data=df, key='ID', features=['X1', 'X2', 'X3'],
                         feature_interval={'X1':[-3, 3], 'X2':[-4, 4]},
                         feauter_interval_hint={'X1': True, 'X3': True})
        >>> res.collect()
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        return self.__fit_transform(data, key, features, feature_interval, feature_interval_hint)

    def fit_transform(self, data, key=None, features=None, feature_interval=None, feature_interval_hint=None):
        """
        Fit the data to be transformed to obtain the feature-wise transformation parameter respectively,
        then apply transformation to the training data and return the transformation result.

        Parameters
        ----------
        data : DataFrame
            DataFrame containing the data to apply power transform.

        key : str, optional
            Specifies the name of ID column in ``data``.

            Defaults to the index of ``data`` if ``data`` is indexed by a single column, otherwise
            ``key`` must be specified explicitly(i.e. mandatory).

        features : str or a list of str, optional
            Specifies the names of the columns in ``data`` to apply power transform to.

            All columns in ``features`` must be of numerical data type.

            Defaults to all non-key columns of ``data`` if not provided.

        feature_interval : dict, optional
            Specifies the power parameter search intervals for features in a dictionary format.
            For each key-value pair in ``feature_interval``, key is the feature name, and value is
            the specified interval.

            For example, if ``data`` contains a feature 'X1' that needs to be transformed, and we want
            the power parameter to be searched from the range [-3, 3] for 'X1',
            then the key-value pair `'X1':[-3, 3]'` should be specified within ``feature_interval``. This
            overwrites the global interval specified by the parameter ``interval`` in class initialization.

            If not provided, the power parameter will be searched within a global interval specified by parameter
            ``interval`` in class initialization.

        feature_interval_hint: dict, optional
            Specifies whether or not to use the specified intervals as hint for different features.

            For each key-value pair in ``feature_interval_hint``, key is the feature name, and value is bool indicating
            whether or not to use specified interval as hint for the corresponding feature specified by key. This overwrites
            the global hint choice specified by the parameter ``hint`` in class initialization.

            If not provided, the value of ``hint`` parameter in class initialization will be applied to all features.

        Returns
        -------
        DataFrame
            The transformed result of ``features`` selected from ``data``.

        Examples
        --------
        >>> pt = PowerTransform(method='yeojohnson')
        >>> res = pt.fit_transform(data=df, key='ID')
        >>> res.collect()
        """
        return self.__fit_transform(data, key, features, feature_interval, feature_interval_hint, True)

    @trace_sql
    def transform(self, data, key=None, features=None,
                  inverse=False, thread_ratio=None):
        r"""
        Data transformation based on trained power transform parameters.

        Parameters
        ----------

        data : DataFrame
            Input data.

        key : str, optional
            The ID column.

            Defaults to the first column of data if the index column of data is not provided.
            Otherwise, defaults to the index column of data.

        features : str, optional
            Specifies the features to apply power transform.

            Defaults to all non-key columns if not provided.

        inverse : bool, optional
            Specifies whether or not to apply inverse power transform.

            - False : apply forward power transformation, i.e. transform from raw feature data to Gaussian-like data.
            - True : apply inverse power transformation, i.e. transform from Gaussian-like data to raw feature data.

            Defaults to False.

        thread_ratio : float, optional
            Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
            Values outside the range will be ignored and this function heuristically determines the number of threads to use.

            Defaults to 1.0.

        Returns
        -------
        DataFrame
            Forecasted values, structured as follows:

            - ID, the same type as key in ``data``, row ID.
            - STRING_CONTENT, type VARCHAR, transformed features in JSON format.

        Examples
        --------
        >>> pt = PowerTransform(method='yeojohnson')
        >>> pt.fit(data=df_train, key='ID')
        >>> res = pt.transform(data=df_transofrm, key='ID')
        >>> res.collect()
        """
        if getattr(self, 'model_') is None:
            raise FitIncompleteError()
        inverse = self._arg('inverse', inverse, bool)
        thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        index = data.index
        cols = data.columns
        key = key if key else index
        key = self._arg('key', key, str, required=index is None)
        if key not in cols:
            msg = f"Please select key from {cols}!"
            logger.error(msg)
            raise ValueError(msg)
        cols.remove(key)

        if isinstance(features, str):
            features = [features]
        features = self._arg('features', features, ListOfStrings)
        features = features if features else cols
        data_ = data[[key] + features]

        conn = data.connection_context
        param_rows = [('DIRECTION', inverse, None, None),
                      ('THREAD_RATIO', None, thread_ratio, None)]

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        result_tbl = f"#PAL_POWER_TRANSFORM_APPLY_RESULT_TBL_{self.id}_{unique_id}"
        if not (check_pal_function_exist(conn, '%GAUSSIAN_TRANSFORM%', like=True) or self._disable_hana_execution):
            msg = 'The version of your SAP HANA does not support Power Transform!'
            logger.error(msg)
            raise ValueError(msg)
        try:
            self._call_pal_auto(conn,
                                'PAL_POWER_TRANSFORMER_TRANSFORM',
                                data_,
                                self.model_,
                                ParameterTable().with_data(param_rows),
                                result_tbl)

        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, result_tbl)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, result_tbl)
            raise
        return conn.table(result_tbl)

class QuantileTransform(PALBase):
    r"""
    Python wrapper for PAL Quantile Transformer.

    Parameters
    ----------
    num_quantiles : int, optional
       Specifies the number of quantiles to be computed.

       Defaults to 100.

    output_distribution : {'uniform', 'normal'}, optional
        Specifies the marginal distribution of the quantile-transformed data.

        - 'uniform': Uniform distribution
        - 'normal': normal distribution

        Defaults to 'uniform'.

    Attributes
    ----------
    result_ : DataFrame
        Training data with selected features quantile-transformed.

    model_ : list of DataFrames
        The model for transforming subsequent data, consisted of 2 DataFrames:

        - DataFrame 1: Quantiles for the output distribution.
        - DataFrame 2: Other model info for the Quantile Transformer.

    Examples
    --------
    >>> qt = QuantileTransform(num_quantiles=200, output_distribution='uniform')
    >>> qt.fit(data=df, key='ID', features=['X2', 'X6'], categorical_variable='X5')
    >>> qt.result_.collect()
    """
    def __init__(self,
                 num_quantiles=None,
                 output_distribution=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(QuantileTransform, self).__init__()
        self.num_quantiles = self._arg('num_quantiles', num_quantiles, int)
        self.output_distribution = self._arg('output_distribution',
                                             output_distribution,
                                             {x:x for x in ['normal', 'uniform']})
        #elf.bounds_threshold = self._arg('bounds_threshold', bounds_threshold, float)
        self.result_ = None
        self.model_ = None

    @trace_sql
    def fit(self, data, key=None,#pylint:disable=unused-argument
            features=None,
            categorical_variable=None):
        r"""
        Quantile transformation to numerical features.

        Parameters
        ----------
        data : DataFrame
            Input data for fitting a quantile-transformation model(Quantile-Transformer).

        key : str, optional
            Specifies the name of the ID column in ``data``.

            Mandatory if ``data`` is not indexed by a single column; otherwise defaults to
            the index column of ``data``.

        features : str or list of strings, optional
            Specifies the names of columns in ``data`` for which quantile-transformation should be applied.
            However, categorical columns in ``features`` are ignored since only numerical columns
            can be quantile-transformed.

            Defaults to all numerical columns in ``data``(except ``key``).
        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.

        Returns
        -------
        A fitted object of class "QuantileTransform".
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        conn = data.connection_context
        key = self._arg('key', key, str,
                        required=not isinstance(data.index, str))
        key = data.index if key is None else key
        cols = data.columns
        cols.remove(key)
        features = self._arg('features', features, ListOfStrings)
        exclude_features = []
        if features is None:
            features = cols.copy()
        newcols = cols.copy()
        for col in features:
            newcols.remove(col)
        exclude_features.extend(newcols)
        if isinstance(categorical_variable, str):
            categorical_variable = [categorical_variable]
        categorical_variable = self._arg('categorical_variable', categorical_variable,
                                         ListOfStrings)
        data_ = data[[key] + cols]
        param_rows = [('NUM_QUANTILES', self.num_quantiles, None, None),
                      ('DISTRIBUTIONNAME', None, None, self.output_distribution)]
        #             ('BOUNDS_THRESHOLD', None, self.bounds_threshold, None)]
        if categorical_variable is not None:
            param_rows.extend([('CATEGORICAL_VARIABLE', None, None, var) for var in categorical_variable])
        if len(exclude_features) > 0:
            param_rows.extend([('EXCLUDE_COLUMNS', None, None, col) for col in exclude_features])
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        out_tbls = [f"#PAL_QUANTILE_TRANS_{tbl}_TBL_{self.id}_{unique_id}" for tbl in ['RESULT', 'QUANTILE', 'MODEL']]
        if not (check_pal_function_exist(conn, '%QUANTILE_TRANSFORM%', like=True) or self._disable_hana_execution):
            msg = 'The version of your SAP HANA does not support Quantile Transform!'
            logger.error(msg)
            raise ValueError(msg)
        try:
            self._call_pal_auto(conn,
                                'PAL_QUANTILE_TRANSFORM',
                                    data_,
                                    ParameterTable().with_data(param_rows),
                                    *out_tbls)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, out_tbls)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, out_tbls)
            raise
        self.result_ = conn.table(out_tbls[0])
        if not self._disable_hana_execution:
            self.result_ = self.result_[data.columns]
        self.model_ = [conn.table(out_tbls[1]), conn.table(out_tbls[2])]
        return self

    def fit_transform(self, data, key=None, features=None,
                      categorical_variable=None):
        """
        Fit a Quantile Transformer, in the meantime transform the training data and return the result.

        Parameters
        ----------
        data : DataFrame
            Input data for fitting a quantile-transformation model(Quantile-Transformer).

        key : str, optional
            Specifies the name of the ID column in ``data``.

            Mandatory if ``data`` is not indexed by a single column; otherwise defaults to
            the index column of ``data``.

        features : str or list of strings, optional
            Specifies the names of columns in ``data`` for which quantile-transformation should be applied.
            However, categorical columns in ``features`` are ignored since only numerical columns
            can be quantile-transformed.

            Defaults to all numerical columns in ``data``(except ``key``).
        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.

        Returns
        -------
        DataFrame
            The data with selected features being quantile-transformed.
        """
        self.fit(data, key, features, categorical_variable)
        return self.result_

    @trace_sql
    def transform(self, data, key=None):  #pylint:disable=unused-argument
        """
        Transform the test data using a fitted QuantileTransformer.

        Parameters
        ----------
        data : DataFrame
            Input data for applying a trained quantile-transformation model(Quantile-Transformer).

            Should be structured the same as the data used in the model training phase.

        key : str, optional
            Specifies the name of the ID column in ``data``.

            Mandatory if ``data`` is not indexed by a single column; otherwise defaults to
            the index column of ``data``.

        Returns
        -------
        DataFrame
            Quantile-transformed data w.r.t. selected(numerical) features.
        """
        if getattr(self, 'model_') is None:
            raise FitIncompleteError()
        conn = data.connection_context
        key = self._arg('key', key, str,
                        required=not isinstance(data.index, str))
        key = data.index if key is None else key
        cols = data.columns
        cols.remove(key)
        data_ = data[[key] + cols]
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        param_rows = [('DISTRIBUTIONNAME', None, None, self.output_distribution)]
        result = f"#PAL_QUANTILE_WMODEL_RESULT_TBL_{self.id}_{unique_id}"
        if not (check_pal_function_exist(conn, '%QUANTILE_TRANSFORM%', like=True) or self._disable_hana_execution):
            msg = 'The version of your SAP HANA does not support Quantile Transform!'
            logger.error(msg)
            raise ValueError(msg)
        try:
            self._call_pal_auto(conn,
                                'PAL_QUANTILE_TRANSFORM_WITH_MODEL',
                                data_,
                                self.model_[0],
                                self.model_[1],
                                ParameterTable().with_data(param_rows),
                                result)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, result)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, result)
            raise
        if self._disable_hana_execution:
            return conn.table(result)
        return conn.table(result)[data.columns]

class OutlierDetectionRegression(PALBase):#pylint:disable=too-many-instance-attributes
    r"""
    In regression, an outlier is a data point which is different from the general behavior of remaining data points. Outlier detection depends on the model of regression. In this procedure, we include two commonly used regression models, the linear model and the tree model.

    In regression, the outlier detection procedure is divided into two steps. In step 1, we get the residual from the original data and the selected model. In step 2, we detect the outliers from the residual. Specifically, we calculate the outlier score of each data point from residual and compare the score with threshold in step 2. For linear model, the outlier score is the deleted studentized residual. For tree model, the outlier score is the z-score of the residual.

    Parameters
    ----------

    regression_model : str, optional

        - 'linear'' : linear model.
        - 'tree' : tree model.

        Defaults to 'linear'.

    threshold : float, optional

        The threshold for outlier score. If the absolute value of outlier score is beyond the threshold, OutlierDetectionRegression considers the corresponding data point as an outlier.

        Defaults to 3.

    iter_num : int, optional

        Total iteration number, which is equivalent to the number of trees in the final model.

        Only valid when ``regression_model`` is 'tree'.

        Defaults to 10.

    max_depth : int, optional

        The maximum depth of each tree.

        Only valid when ``regression_model`` is 'tree'.

        Defaults to 6.

    eta : float, optional

        Learning rate of each iteration. Range: (0, 1].

        Only valid when ``regression_model`` is 'tree'.

        Defaults to 0.3.

    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Default to -1.


    Attributes
    ----------
    stats_ : DataFrame
        Statistics.

    metrics_ : DataFrame
        Relevant metrics.


    Examples
    --------
    >>> tsreg = OutlierDetectionRegression(regression_model='linear')
    >>> res = tsreg.fit_predict(data=df, key='ID', label='Y')
    >>> res.collect()
    """
    regression_model_methods = {'linear':0, 'tree':1}
    def __init__(self,
                 regression_model=None,
                 threshold=None,
                 iter_num=None,
                 max_depth=None,
                 thread_ratio=None,
                 eta=None):
        if not hasattr(self, 'hanaml_parameters'):
            setattr(self, 'hanaml_parameters', pal_param_register())
        super().__init__()
        self.regression_model = self._arg('regression_model', regression_model, self.regression_model_methods)
        self.threshold = self._arg('threshold', threshold, float)
        self.iter_num = self._arg('iter_num', iter_num, int)
        self.max_depth = self._arg('max_depth', max_depth, int)
        self.eta = self._arg('eta', eta, float)
        self.thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        self.op_name = 'Outlier'
        self.stats_ = None
        self.metrics_ = None

    @trace_sql
    def fit_predict(self, data, key=None, features=None, label=None, categorical_variable=None):
        r"""
        Detection of outliers with regression model.

        Parameters
        ----------

        data : DataFrame

            Input data.

        key : str, optional
            Name of the ID column in ``data``.

            If ``key`` is not provided, then:

            - if ``data`` is indexed by a single column, then ``key`` defaults
              to that index column;
            - otherwise, `key`` defaults to the first column;

        features : a list of str, optional
            Names of the feature columns.

            If ``features`` is not provided, it defaults to all non-ID, non-label columns.

        label : str, optional
            Name of the dependent variable.

            Defaults to the name of the last non-ID column.

        categorical_variable : str or a list of str, optional
            Specifies which INTEGER columns should be treated as categorical, with all other INTEGER columns treated as continuous.

            No default value.

        Returns
        -------
        DataFrame
            Result, structured as follows:

            - ID : ID of data.
            - TARGET : Dependent variable.
            - RESIDUAL : Residual.
            - OUTLIER_SCORE : Outlier score.
            - IS_OUTLIER : 0: normal, 1: outlier.

        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, "training_data", data)
        setattr(self, "key", key)
        setattr(self, "features", features)
        setattr(self, "label", label)
        conn = data.connection_context
        key = self._arg('key', key, str)
        label = self._arg('label', label, str)
        if isinstance(features, str):
            features = [features]
        features = self._arg('features', features, ListOfStrings)
        if isinstance(categorical_variable, str):
            categorical_variable = [categorical_variable]
        categorical_variable = self._arg('categorical_variable', categorical_variable, ListOfStrings)

        if not self._disable_hana_execution:
            require_pal_usable(conn)
            cols = data.columns
            index = data.index
            if index is not None: # key
                key = _col_index_check(key, 'key', index, cols)
            else:
                if key is None:
                    key = cols[0]
            cols.remove(key)
            if label is None: # label
                label = cols[-1]
            cols.remove(label)
            if features is None: # features
                features = cols
            data_ = data[[key, label] + features]
        else:
            data_ = data

        param_rows = []
        param_rows = [
            ('REGRESSION_MODEL', self.regression_model, None, None),
            ('THRESHOLD', None, self.threshold, None),
            ('ITER_NUM', self.iter_num, None, None),
            ('MAX_DEPTH', self.max_depth, None, None),
            ('ETA', None, self.eta, None),
            ('THREAD_RATIO', None, self.thread_ratio, None)
            ]
        if categorical_variable is not None:
            param_rows.extend([('CATEGORICAL_VARIABLE', None, None, var) for var in categorical_variable])

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        outputs = ['RESULT', 'STATS', 'METRICS']
        outputs = [f'#PAL_REGRESSION_OUTLIER_{name}_TBL_{self.id}_{unique_id}' for name in outputs]
        res_tbl, stats_tbl, metrics_tbl = outputs
        if not (check_pal_function_exist(conn, '%OUTLIERDETECTIONFORREGRESSION%', like=True) or \
        self._disable_hana_execution):
            msg = 'The version of your SAP HANA does not support the outlier detection for regression!'
            logger.error(msg)
            raise ValueError(msg)
        try:
            self._call_pal_auto(conn,
                                'PAL_OUTLIER_DETECTION_FOR_REGRESSION',
                                data_,
                                ParameterTable().with_data(param_rows),
                                *outputs)
        except dbapi.Error as db_err:
            logger.exception(str(db_err))
            try_drop(conn, outputs)
            raise
        except Exception as db_err:
            logger.exception(str(db_err))
            try_drop(conn, outputs)
        self.stats_ = conn.table(stats_tbl)
        self.statistics_ = self.stats_
        #pylint:disable=attribute-defined-outside-init
        self.metrics_ = conn.table(metrics_tbl)

        return conn.table(res_tbl)
