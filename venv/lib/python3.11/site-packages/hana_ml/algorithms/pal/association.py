"""
This module contains Python wrappers for PAL association algorithms.

The following classes are available:

    * :class:`Apriori`
    * :class:`AprioriLite`
    * :class:`FPGrowth`
    * :class:`KORD`
    * :class:`SPM`
"""

#pylint:disable=too-many-lines, line-too-long, too-many-instance-attributes, too-few-public-methods, too-many-locals
#pylint: disable=consider-using-f-string
import logging
import sys
import uuid
from hdbcli import dbapi
from hana_ml.ml_base import try_drop
from .pal_base import (
    PALBase,
    ParameterTable,
    ListOfStrings,
    pal_param_register,
    require_pal_usable
)

LOGGER = logging.getLogger(__name__)

#pylint:disable=undefined-variable
if sys.version_info.major == 2:
    _INTEGER_TYPES = (int, long)
    _STRING_TYPES = (str, unicode)
else:
    _INTEGER_TYPES = (int,)
    _STRING_TYPES = (str,)

class _AssociationBase(PALBase):
    """
    Base class of two association rule algorithms: :class:`Apriori` and :class:`FPGrowth`.
    """

    def __init__(self,#pylint:disable=too-many-arguments, too-many-locals
                 relational=None,
                 min_lift=None,
                 max_conseq=None,
                 max_len=None,
                 ubiquitous=None,
                 thread_ratio=None,
                 timeout=None):
        if not hasattr(self, 'hanaml_parameters'):
            setattr(self, 'hanaml_parameters', pal_param_register())
        super(_AssociationBase, self).__init__()
        self.relational = self._arg('relational', relational, bool)
        if self.relational is None:
            self.relational = False
        self.min_lift = self._arg('min_lift', min_lift, float)
        self.max_conseq = self._arg('max_conseq', max_conseq, int)
        self.max_len = self._arg('max_len', max_len, int)
        self.ubiquitous = self._arg('ubiquitous', ubiquitous, float)
        self.thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        self.timeout = self._arg('timeout', timeout, int)

class Apriori(_AssociationBase):#pylint:disable=too-many-instance-attributes
    r"""
    Apriori is a classic algorithm used in machine learning for mining frequent itemsets and relevant association rules.
    It operates on a list of transactions and is particularly effective in market basket analysis, where the goal is to find associations of products bought with other products.

    Parameters
    ----------
    min_support : float

        Specifies the minimum support as determined by the user.

    min_confidence : float

        Specifies the minimum confidence as determined by the user.

    relational : bool, optional

        Determines whether relational logic should be applied within the Apriori algorithm. If set to False, a single combined results table will be produced. Conversely, if set to True, the result will be split across three tables: antecedent, consequent, and statistics.

        Defaults to False.

    min_lift : float, optional

        Specifies the minimum lift value as determined by the user. This parameter is essential in association rule mining for assessing the strength of each rule.

        Defaults to 0.

    max_conseq : int, optional

        Specifies the maximum number of items that can be contained in consequents.

        Defaults to 100.

    max_len : int, optional

        Specifies the maximum number of combined items in both antecedent and consequent sets in the output.

        Defaults to 5.

    ubiquitous : float, optional

        This parameter is used to ignore item sets with support values greater than this threshold during frequent itemset mining.

        Defaults to 1.0.

    use_prefix_tree : bool, optional

        Indicates whether a prefix tree should be used to save memory. A prefix tree (also known as a trie) is a data structure that can increase the efficiency of certain types of lookups.

        Defaults to False.

    lhs_restrict : a list of str, optional (deprecated)

        Allows specific items only on the left-hand-side of association rules.

    rhs_restrict : a list of str, optional (deprecated)

        Allows specific items only on the right-hand-side of the association rules.

    lhs_complement_rhs : bool, optional (deprecated)

        If rhs_restrict is used to restrict some items to the right-hand-side of the association rules, this parameter can be set to True in order to restrict the complementary items to the left-hand-side.

        For example, if you have 100 items (i\ :sub:`1`\, i\ :sub:`2`\, ..., i\ :sub:`100`\), and want to restrict
        i\ :sub:`1`\  and i\ :sub:`2`\  to the right-hand-side, and i\ :sub:`3`\,i\ :sub:`4`\,...,i\ :sub:`100`\  to the left-hand-side,
        you can set the parameters similarly as follows:

            ...

            rhs_restrict = ['i\ :sub:`1`\','i\ :sub:`2`\'],

            lhs_complement_rhs = True,

            ...

        Defaults to False.

    rhs_complement_lhs : bool, optional (deprecated)

        If lhs_restrict is used to restrict some items to the left-hand-side of the association rules, this parameter can be set to True to restrict the complementary items to the right-hand side.

        Defaults to False.

    thread_number : float, optional

        Specifies the ratio of total number of threads that can be used by this function.

        The value range is from 0 to 1, where 0 means only using 1 thread, and 1 means
        using at most all the currently available threads.

        Values outside the range will be ignored and this function heuristically determines the number of threads to use.


        Defaults to 0.

    timeout : int, optional

        Specifies the maximum run time for the algorithm in seconds. The algorithm will cease computation if the specified timeout is exceeded.

        Defaults to 3600.

    pmml_export : {'no', 'single-row', 'multi-row'}, optional

        Defines the method of exporting the Apriori model:

        - 'no' : the model will not be exported,
        - 'single-row' : the Apriori model will be exported as a single row PMML,
        - 'multi-row' : the Apriori model will be exported as a multi-row PMML where each row contains a minimum of 5000 characters.

        Defaults to 'no'.

    Attributes
    ----------

    result_ : DataFrame
        Mined association rules and related statistics, structured as follows:

            - 1st column : antecedent(leading) items.
            - 2nd column : consequent(dependent) items.
            - 3rd column : support value.
            - 4th column : confidence value.
            - 5th column : lift value.

        Available only when ``relational`` is False.

    model_ : DataFrame
        Apriori model trained from the input data, structured as follows:

            - 1st column : model ID,
            - 2nd column : model content, i.e. Apriori model in PMML format.

    antec_ : DataFrame
        Antecedent items of mined association rules, structured as follows:

            - 1st column : association rule ID,
            - 2nd column : antecedent items of the corresponding association rule.

        Available only when ``relational`` is True.

    conseq_ : DataFrame
        Consequent items of mined association rules, structured as follows:

            - 1st column : association rule ID,
            - 2nd column : consequent items of the corresponding association rule.

        Available only when ``relational`` is True.

    stats_ : DataFrame
        Statistics of the mined association rules, structured as follows:

            - 1st column : rule ID,
            - 2nd column : support value of the rule,
            - 3rd column : confidence value of the rule,
            - 4th column : lift value of the rule.

        Available only when ``relational`` is True.


    Examples
    --------

    Input data for associate rule mining:

    >>> df.collect()
        CUSTOMER   ITEM
    0          2  item2
    1          2  item3
    ...
    21         8  item2
    22         8  item3

    Initialize a Apriori object and set its parameters:

    >>> ap = Apriori(min_support=0.1,
                     min_confidence=0.3,
                     relational=False,
                     min_lift=1.1,
                     max_conseq=1,
                     max_len=5,
                     ubiquitous=1.0,
                     use_prefix_tree=False,
                     thread_ratio=0,
                     timeout=3600,
                     pmml_export='single-row')

    Perform the fit() and obtain the result:

    >>> ap.fit(data=df)
    >>> ap.result_.head(5).collect()
        ANTECEDENT CONSEQUENT   SUPPORT  CONFIDENCE      LIFT
    0        item5      item2  0.222222    1.000000  1.285714
    1        item1      item5  0.222222    0.333333  1.500000
    2        item5      item1  0.222222    1.000000  1.500000
    3        item4      item2  0.222222    1.000000  1.285714
    4  item2&item1      item5  0.222222    0.500000  2.250000

    Also, initialize a Apriori object and set its parameters with relational logic:

    >>> apr = Apriori(min_support=0.1,
                      min_confidence=0.3,
                      relational=True,
                      min_lift=1.1,
                      max_conseq=1,
                      max_len=5,
                      ubiquitous=1.0,
                      use_prefix_tree=False,
                      thread_ratio=0,
                      timeout=3600,
                      pmml_export='single-row')

    Perform the fit() and obtain the result:

    >>> apr.antec_.head(5).collect()
       RULE_ID ANTECEDENTITEM
    0        0          item5
    1        1          item1
    2        2          item5
    3        3          item4
    4        4          item2
    >>> apr.conseq_.head(5).collect()
       RULE_ID CONSEQUENTITEM
    0        0          item2
    1        1          item5
    2        2          item1
    3        3          item2
    4        4          item5
    >>> apr.stats_.head(5).collect()
       RULE_ID   SUPPORT  CONFIDENCE      LIFT
    0        0  0.222222    1.000000  1.285714
    1        1  0.222222    0.333333  1.500000
    2        2  0.222222    1.000000  1.500000
    3        3  0.222222    1.000000  1.285714
    4        4  0.222222    0.500000  2.250000

    """
    pmml_export_map = {'no': 0, 'single-row': 1, 'multi-row': 2}
    def __init__(self,#pylint:disable=too-many-arguments, too-many-locals
                 min_support,
                 min_confidence,
                 relational=None,
                 min_lift=None,
                 max_conseq=None,
                 max_len=None,
                 ubiquitous=None,
                 use_prefix_tree=None,
                 lhs_restrict=None,
                 rhs_restrict=None,
                 lhs_complement_rhs=None,
                 rhs_complement_lhs=None,
                 thread_ratio=None,
                 timeout=None,
                 pmml_export=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(Apriori, self).__init__(relational=relational,
                                      min_lift=min_lift,
                                      max_conseq=max_conseq,
                                      max_len=max_len,
                                      ubiquitous=ubiquitous,
                                      thread_ratio=thread_ratio,
                                      timeout=timeout)
        self.min_support = self._arg('min_support', min_support, float,
                                     required=True)
        self.min_confidence = self._arg('min_confidence', min_confidence, float,
                                        required=True)
        #self.relational = self._arg('relational', relational, bool)
        #if self.relational is None:
        #    self.relational = False
        #self.min_lift = self._arg('min_lift', min_lift, float)
        #self.max_conseq = self._arg('max_conseq', max_conseq, int)
        #self.max_len = self._arg('max_len', max_len, int)
        #self.ubiquitous = self._arg('ubiquitous', ubiquitous, float)
        self.use_prefix_tree = self._arg('use_prefix_tree', use_prefix_tree, bool)
        self.lhs_restrict = self._arg('lhs_restrict', lhs_restrict, ListOfStrings)
        self.rhs_restrict = self._arg('rhs_restrict', rhs_restrict, ListOfStrings)
        self.lhs_complement_rhs = self._arg('lhs_complement_rhs',
                                            lhs_complement_rhs, bool)
        self.rhs_complement_lhs = self._arg('rhs_complement_lhs',
                                            rhs_complement_lhs, bool)
        #self.thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        #self.timeout = self._arg('timeout', timeout, int)
        self.pmml_export = self._arg('pmml_export', pmml_export,
                                     self.pmml_export_map)

    #pylint:disable=too-many-locals, too-many-statements
    def fit(self, data,#pylint:disable=too-many-arguments, too-many-branches
            transaction=None,
            item=None,
            lhs_restrict=None,
            rhs_restrict=None,
            lhs_complement_rhs=None,
            rhs_complement_lhs=None):
        r"""
        Association rule mining on the given data.

        Parameters
        ----------

        data : DataFrame
            The input data.
        transaction : str, optional

            Name of the transaction column.

            Defaults to the first column if not provided.

        item : str, optional

            Name of the item ID column.

            Data type of item column can be INTEGER, VARCHAR or NVARCHAR.

            Defaults to the last non-transaction column if not provided.

        lhs_restrict : list of int/str, optional

            Specifies items that are only allowed on the left-hand-side of
            association rules.

            Elements in the list should be the same type as the item column.

        rhs_restrict : list of int/str, optional

            Specifies items that are only allowed on the right-hand-side of
            association rules.

            Elements in the list should be the same type as the item column.

        lhs_complement_rhs :  bool, optional

            If you use ``rhs_restrict`` to restrict some items to the left-hand-side
            of the association rules, you can set this parameter to True to restrict
            the complement items to the left-hand-side.

            For example, if you have 100 items (i\ :sub:`1`\,i\ :sub:`2`\,...,i\ :sub:`100`\), and want to restrict
            i\ :sub:`1`\  and i\ :sub:`2`\  to the right-hand-side, and i\ :sub:`3`\, i\ :sub:`4`\,..., i\ :sub:`100`\  to the left-hand-side,
            you can set the parameters similarly as follows:

                ...

                rhs_restrict = [i\ :sub:`1`\, i\ :sub:`2`\],

                lhs_complement_rhs = True,

                ...

            Defaults to False.
        rhs_complement_lhs : bool, optional

            If you use ``lhs_restrict`` to restrict some items to the left-hand-side
            of association rules, you can set this parameter to True to restrict the
            complement items to the right-hand side.

            Defaults to False.
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, 'training_data', data)
        conn = data.connection_context
        require_pal_usable(conn)
        transaction = self._arg('transaction', transaction, str)
        item = self._arg('item', item, str)
        cols = data.columns
        if len(cols) < 2:
            msg = ("Input data should contain at least 2 columns: " +
                   "one for transaction ID, another for item ID.")
            LOGGER.error(msg)
            raise ValueError(msg)
        if transaction is not None and transaction not in cols:
            msg = ("'{}' is not a recognized column name".format(transaction) +
                   " of the input data.")
            LOGGER.error(msg)
            raise ValueError(msg)
        if item is not None and item not in cols:
            msg = ("'{}' is not a recognized column name".format(item) +
                   " of the input data.")
            LOGGER.error(msg)
            raise ValueError(msg)

        if transaction is None:
            transaction = cols[0]
        cols.remove(transaction)
        if item is None:
            item = cols[-1]
        if any(data.dtypes([opt])[0][1] not in ('INT', 'VARCHAR', 'NVARCHAR') for opt in (transaction, item)):#pylint:disable=line-too-long
            msg = ("Wrong data type for transaction ID or item ID.")
            LOGGER.error(msg)
            raise TypeError(msg)

        if lhs_restrict is not None:
            if data.dtypes([item])[0][1] == 'INT':
                if isinstance(lhs_restrict, list) and all(isinstance(lhs, _INTEGER_TYPES) for lhs in lhs_restrict):#pylint:disable=line-too-long
                    self.lhs_restrict = lhs_restrict
            else:
                self.lhs_restrict = self._arg('rhs_restrict', lhs_restrict, ListOfStrings)

        if rhs_restrict is not None:
            if data.dtypes([item])[0][1] == 'INT':
                if isinstance(rhs_restrict, list) and all(isinstance(rhs, _INTEGER_TYPES) for rhs in rhs_restrict):#pylint:disable=line-too-long
                    self.rhs_restrict = rhs_restrict
            else:
                self.rhs_restrict = self._arg('rhs_restrict', rhs_restrict, ListOfStrings)
        if lhs_complement_rhs is not None:
            self.lhs_complement_rhs = self._arg('lhs_complement_rhs', lhs_complement_rhs, bool)
        if rhs_complement_lhs is not None:
            self.rhs_complement_lhs = self._arg('rhs_complement_lhs', rhs_complement_lhs, bool)
        data_ = data[[transaction]+[item]]
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        outputs = ['RESUTL', 'MODEL', 'STATS', 'ANTEC', 'CONSEQ']
        outputs = ['#PAL_APRIORI_{}_TBL_{}'.format(name, unique_id)
                   for name in outputs]
        result_tbl, model_tbl, stats_tbl, antec_tbl, conseq_tbl = outputs

        param_rows = [
            ('MIN_SUPPORT', None, self.min_support, None),
            ('MIN_CONFIDENCE', None, self.min_confidence, None),
            ('MIN_LIFT', None, self.min_lift, None),
            ('MAX_CONSEQUENT', self.max_conseq, None, None),
            ('MAXITEMLENGTH', self.max_len, None, None),
            ('UBIQUITOUS', None, self.ubiquitous, None),
            ('IS_USE_PREFIX_TREE', self.use_prefix_tree, None, None),
            ('LHS_IS_COMPLEMENTARY_RHS', self.lhs_complement_rhs, None, None),
            ('RHS_IS_COMPLEMENTARY_LHS', self.rhs_complement_lhs, None, None),
            ('THREAD_RATIO', None, self.thread_ratio, None),
            ('TIMEOUT', self.timeout, None, None),
            ('PMML_EXPORT', self.pmml_export, None, None),
        ]

        if self.lhs_restrict is not None:
            param_rows.extend(('LHS_RESTRICT', None, None, str(item_id))
                              for item_id in self.lhs_restrict)
        if self.rhs_restrict is not None:
            param_rows.extend(('RHS_RESTRICT', None, None, str(item_id))
                              for item_id in self.rhs_restrict)

        if self.relational:
            apriori_proc = 'PAL_APRIORI_RELATIONAL'
            output_tbls = [antec_tbl, conseq_tbl, stats_tbl, model_tbl]
        else:
            apriori_proc = 'PAL_APRIORI'
            output_tbls = [result_tbl, model_tbl]

        try:
            self._call_pal_auto(conn,
                                apriori_proc,
                                data_,
                                ParameterTable().with_data(param_rows),
                                *output_tbls)
        except dbapi.Error as db_err:
            #msg = "HANA error while attempting to apply Apriori algorithm."
            LOGGER.exception(str(db_err))
            try_drop(conn, output_tbls)
            raise
        except Exception as db_err:
            #msg = "HANA error while attempting to apply Apriori algorithm."
            LOGGER.exception(str(db_err))
            try_drop(conn, output_tbls)
            raise
        #pylint:disable=attribute-defined-outside-init
        if self.relational:
            self.antec_ = conn.table(antec_tbl)
            self.conseq_ = conn.table(conseq_tbl)
            self.stats_ = conn.table(stats_tbl)
            self.statistics_ = self.stats_
            self.model_ = conn.table(model_tbl) if self.pmml_export else None
            self.result_ = None
        else:
            self.result_ = conn.table(result_tbl)
            self.model_ = conn.table(model_tbl) if self.pmml_export else None
            self.antec_ = None
            self.conseq_ = None
            self.stats_ = None
            self.statistics_ = self.stats_

class AprioriLite(PALBase):#pylint:disable=too-few-public-methods
    r"""
    This function runs a lightweight version of the Apriori algorithm for association rule mining.
    It significantly reduces the computational overhead by only focusing on the creation and analysis of up to two-item sets,
    which makes it particularly useful for large datasets where traditional Apriori applications could be computationally expensive.

    Parameters
    ----------

    min_support : float

        Specifies the minimum support as determined by the user.

    min_confidence : float

        Specifies the minimum confidence as determined by the user.

    subsample : float, optional

        Specifies the sampling percentage for the input data. Set to 1 if you want to use the entire data. By subsampling, you can speed up computation on large datasets.
        Defaults to 1.

    recalculate : bool, optional

        If true, the illustrative statistics (support, confidence, and lift) of the resulting rule set are recalculated (updated) after the rules are found using sampled data.

        Defaults to True.

    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 0.

    timeout : int, optional

        Specifies the maximum run time for the algorithm in seconds. The algorithm will cease computation if the specified timeout is exceeded.

        Defaults to 3600.

    pmml_export : {'no', 'single-row', 'multi-row'}, optional

        Defines the method of exporting the Apriori model:

        - 'no' : the model will not be exported,
        - 'single-row' : the Apriori model will be exported as a single row PMML,
        - 'multi-row' : the Apriori model will be exported as a multi-row PMML where each row contains a minimum of 5000 characters.

        Defaults to 'no'.

    Attributes
    ----------

    result_ : DataFrame
        Mined association rules and related statistics, structured as follows:
            - 1st column : antecedent(leading) items,
            - 2nd column : consequent(dependent) items,
            - 3rd column : support value,
            - 4th column : confidence value,
            - 5th column : lift value.

        Non-empty only when ``relational`` is False.

    model_ : DataFrame
        Apriori model trained from the input data, structured as follows:
            - 1st column : model ID.
            - 2nd column : model content, i.e. liteApriori model in PMML format.

    Examples
    --------

    Input DataFrame df:

    >>> df.collect()
        CUSTOMER   ITEM
    0          2  item2
    1          2  item3
    ......
    21         8  item2
    22         8  item3

    Initialize a AprioriLite object:

    >>> apl = AprioriLite(min_support=0.1,
                          min_confidence=0.3,
                          subsample=1.0,
                          recalculate=False,
                          timeout=3600,
                          pmml_export='single-row')

    Perform the fit() and obtain the result:

    >>> apl.fit(data=df)
    >>> apl.result_.head(5).collect()
      ANTECEDENT CONSEQUENT   SUPPORT  CONFIDENCE      LIFT
    0      item5      item2  0.222222    1.000000  1.285714
    1      item1      item5  0.222222    0.333333  1.500000
    2      item5      item1  0.222222    1.000000  1.500000
    3      item5      item3  0.111111    0.500000  0.750000
    4      item1      item2  0.444444    0.666667  0.857143

    """
    pmml_export_map = {'no': 0, 'single-row': 1, 'multi-row': 2}
    def __init__(self,#pylint:disable=too-many-arguments
                 min_support,
                 min_confidence,
                 subsample=None,
                 recalculate=None,
                 thread_ratio=None,
                 timeout=None,
                 pmml_export=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(AprioriLite, self).__init__()
        self.min_support = self._arg('min_support', min_support, float, required=True)
        self.min_confidence = self._arg('min_confidence', min_confidence, float, required=True)
        self.subsample = self._arg('subsample', subsample, float)
        self.recalculate = self._arg('recalculate', recalculate, bool)
        self.thread_ratio = self._arg('thread_ratio', thread_ratio, float)
        self.timeout = self._arg('timeout', timeout, int)
        self.pmml_export = self._arg('pmml_export', pmml_export,
                                     self.pmml_export_map)


    def fit(self, data, transaction=None, item=None):
        """
        Association rule mining on the given data.

        Parameters
        ----------

        data : DataFrame
            The input data.
        transaction : str, optional

            Name of the transaction column.

            Defaults to the first column if not provided.

        item : str, optional

            Name of the item column.

            Defaults to the last non-transaction column if not provided.
        """
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, 'training_data', data)
        conn = data.connection_context
        require_pal_usable(conn)
        transaction = self._arg('transaction', transaction, str)
        item = self._arg('item', item, str)
        cols = data.columns
        if len(cols) < 2:
            msg = ("Input data should contain at least 2 columns: " +
                   "one for transaction ID, another for item ID.")
            LOGGER.error(msg)
            raise ValueError(msg)
        if transaction is not None and transaction not in cols:
            msg = ("'{}' is not a recognized column name".format(transaction) +
                   " of the input data.")
            LOGGER.error(msg)
            raise ValueError(msg)
        if item is not None and item not in cols:
            msg = ("'{}' is not a recognized column name".format(item) +
                   " of the input data.")
            LOGGER.error(msg)
            raise ValueError(msg)

        if transaction is None:
            transaction = cols[0]
        cols.remove(transaction)
        if item is None:
            item = cols[-1]
        data_ = data[[transaction] + [item]]

        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        outputs = ['RESUTL', 'MODEL']
        outputs = ['#PAL_APRIORI_{}_TBL_{}_{}'.format(name, self.id, unique_id)
                   for name in outputs]
        result_tbl, model_tbl = outputs

        param_rows = [
            ('MIN_SUPPORT', None, self.min_support, None),
            ('MIN_CONFIDENCE', None, self.min_confidence, None),
            ('SAMPLE_PROPORTION', None, self.subsample, None),
            ('IS_RECALCULATE', self.recalculate, None, None),
            ('THREAD_RATIO', None, self.thread_ratio, None),
            ('TIMEOUT', self.timeout, None, None),
            ('PMML_EXPORT', self.pmml_export, None, None)
        ]
        try:
            self._call_pal_auto(conn,
                                'PAL_LITE_APRIORI',
                                data_,
                                ParameterTable().with_data(param_rows),
                                *outputs)
        except dbapi.Error as db_err:
            #msg = "HANA error while attempting to apply Apriori algorithm."
            LOGGER.exception(str(db_err))
            try_drop(conn, outputs)
            raise
        except Exception as db_err:
            #msg = "HANA error while attempting to apply Apriori algorithm."
            LOGGER.exception(str(db_err))
            try_drop(conn, outputs)
            raise
        #pylint:disable=attribute-defined-outside-init
        self.result_ = conn.table(result_tbl)
        self.model_ = conn.table(model_tbl) if self.pmml_export else None

class FPGrowth(_AssociationBase):#pylint:disable=too-many-instance-attributes
    r"""
    The Frequent Pattern Growth (FP-Growth) algorithm is a technique used for finding frequent patterns in a transaction dataset without generating a candidate itemset.
    This is achieved by building a prefix tree (FP Tree) to compress information and subsequently retrieve frequent itemsets efficiently.

    Parameters
    ----------

    min_support : float, optional

        Specifies the minimum support value, which falls within the valid range of [0, 1].

        Defaults to 0.

    min_confidence : float, optional

        Specifies the minimum confidence value, with an acceptable range between [0, 1].

        Defaults to 0.

    relational : bool, optional

        Determines whether relational logic should be applied within the Apriori algorithm.
        If set to False, a single combined results table will be produced. Conversely, if set to True, the result will be split across three tables: antecedent, consequent, and statistics.

        Defaults to False.

    min_lift : float, optional

        Specifies the minimum lift.

        Defaults to 0.

    max_conseq : int, optional

        Specifies the maximum length of consequent items.

        Defaults to 10.

    max_len : int, optional

        Specifies the total length of both antecedent items and consequent items in the output.

        Defaults to 10.

    ubiquitous : float, optional

        This parameter is used to ignore item sets with support values greater than this threshold during frequent itemset mining.

        Defaults to 1.0.

    thread_ratio : float, optional
        Adjusts the percentage of available threads to use, from 0 to 1. A value of 0 indicates the use of a single thread, while 1 implies the use of all possible current threads.
        Values outside the range will be ignored and this function heuristically determines the number of threads to use.

        Defaults to 0.

    timeout : int, optional

        Specifies the maximum run time for the algorithm in seconds. The algorithm will cease computation if the specified timeout is exceeded.

        Defaults to 3600.


    Attributes
    ----------

    result_ : DataFrame
        Mined association rules and related statistics, structured as follows:

            - 1st column : antecedent(leading) items,
            - 2nd column : consequent(dependent) items,
            - 3rd column : support value,
            - 4th column : confidence value,
            - 5th column : lift value.

        Available only when ``relational`` is False.

    antec_ : DataFrame
        Antecedent items of mined association rules, structured as follows:

            - 1st column : association rule ID,
            - 2nd column : antecedent items of the corresponding association rule.

        Available only when ``relational`` is True.

    conseq_ : DataFrame
        Consequent items of mined association rules, structured as follows:

            - 1st column : association rule ID,
            - 2nd column : consequent items of the corresponding association rule.

        Available only when ``relational`` is True.

    stats_ : DataFrame
        Statistics of the mined association rules, structured as follows:

            - 1st column : rule ID,
            - 2nd column : support value of the rule,
            - 3rd column : confidence value of the rule,
            - 4th column : lift value of the rule.

        Available only when ``relational`` is True.

    Examples
    --------

    Input DataFrame df:

    >>> df.collect()
        TRANS  ITEM
    0       1     1
    1       1     2
    ......
    26     10     3
    27     10     5

    Initialize a FPGrowth object:

    >>> fpg = FPGrowth(min_support=0.2,
                       min_confidence=0.5,
                       relational=False,
                       min_lift=1.0,
                       max_conseq=1,
                       max_len=5,
                       ubiquitous=1.0,
                       thread_ratio=0,
                       timeout=3600)

    Perform fit():

    >>> fpg.fit(data=df, lhs_restrict=[1,2,3])
    >>> fpg.result_.collect()
      ANTECEDENT  CONSEQUENT  SUPPORT  CONFIDENCE      LIFT
    0          2           3      0.5    0.714286  1.190476
    1          3           2      0.5    0.833333  1.190476
    2          3           4      0.3    0.500000  1.250000
    3        1&2           3      0.3    0.600000  1.000000
    4        1&3           2      0.3    0.750000  1.071429
    5        1&3           4      0.2    0.500000  1.250000

    Also, initialize a FPGrowth object and set its parameters with relational logic:

    >>> fpgr = FPGrowth(min_support=0.2,
                        min_confidence=0.5,
                        relational=True,
                        min_lift=1.0,
                        max_conseq=1,
                        max_len=5,
                        ubiquitous=1.0,
                        thread_ratio=0,
                        timeout=3600)

    Perform fit():

    >>> fpgr.fit(data=df, rhs_restrict=[1, 2, 3])
    >>> fpgr.antec_.collect()
       RULE_ID  ANTECEDENTITEM
    0        0               2
    1        1               3
    2        2               3
    ...
    6        4               3
    7        5               1
    8        5               3

    >>> fpgr.conseq_.collect()
       RULE_ID  CONSEQUENTITEM
    0        0               3
    1        1               2
    2        2               4
    3        3               3
    4        4               2
    5        5               4

    >>> fpgr.stats_.collect()
       RULE_ID  SUPPORT  CONFIDENCE      LIFT
    0        0      0.5    0.714286  1.190476
    1        1      0.5    0.833333  1.190476
    2        2      0.3    0.500000  1.250000
    3        3      0.3    0.600000  1.000000
    4        4      0.3    0.750000  1.071429
    5        5      0.2    0.500000  1.250000

    """
    def __init__(self,#pylint:disable=too-many-arguments, too-many-locals
                 min_support=None,
                 min_confidence=None,
                 relational=None,
                 min_lift=None,
                 max_conseq=None,
                 max_len=None,
                 ubiquitous=None,
                 thread_ratio=None,
                 timeout=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(FPGrowth, self).__init__(relational=relational,
                                       min_lift=min_lift,
                                       max_conseq=max_conseq,
                                       max_len=max_len,
                                       ubiquitous=ubiquitous,
                                       thread_ratio=thread_ratio,
                                       timeout=timeout)
        self.min_support = self._arg('min_support', min_support, float)
        self.min_confidence = self._arg('min_confidence', min_confidence, float)

    #pylint:disable=too-many-locals, too-many-statements
    def fit(self, data,#pylint:disable=too-many-arguments, too-many-branches
            transaction=None,
            item=None,
            lhs_restrict=None,
            rhs_restrict=None,
            lhs_complement_rhs=None,
            rhs_complement_lhs=None):
        r"""
        Association rule mining on the given data.

        Parameters
        ----------

        data : DataFrame
            The input data.
        transaction : str, optional

            Name of the transaction column.

            Defaults to the first column if not provided.

        item : str, optional

            Name of the item column.

            Defaults to the last non-transaction column if not provided.

        lhs_restrict : list of int/str, optional

            Specifies items that are only allowed on the left-hand-side of
            association rules.

            Elements in the list should be the same type as the item column.


        rhs_restrict : list of int/str, optional

            Specifies items that are only allowed on the right-hand-side of
            association rules.

            Elements in the list should be the same type as the item column.

        lhs_complement_rhs :  bool, optional

            If you use ``rhs_restrict`` to restrict some items to the left-hand-side
            of the association rules, you can set this parameter to True to restrict
            the complement items to the left-hand-side.

            For example, if you have 100 items (i\ :sub:`1`\,i\ :sub:`2`\,...,i\ :sub:`100`\), and want to restrict \
            i\ :sub:`1`\  and i\ :sub:`2`\  to the right-hand-side, and  i\ :sub:`3`\, i\ :sub:`4`\,..., i\ :sub:`100`\  to the left-hand-side,
            you can set the parameters similarly as follows:

                ...

                rhs_restrict = [i\ :sub:`1`\, i\ :sub:`2`\],

                lhs_complement_rhs = True,

                ...

            Defaults to False.

        rhs_complement_lhs : bool, optional

            If you use ``lhs_restrict`` to restrict some items to the left-hand-side \
            of association rules, you can set this parameter to True to restrict the \
            complement items to the right-hand side.

            Defaults to False.
        """
        conn = data.connection_context
        require_pal_usable(conn)
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, 'training_data', data)
        transaction = self._arg('transaction', transaction, str)
        item = self._arg('item', item, str)
        cols = data.columns
        if len(cols) < 2:
            msg = ("Input data should contain at least 2 columns: " +
                   "one for transaction ID, another for item ID.")
            LOGGER.error(msg)
            raise ValueError(msg)
        if transaction is not None and transaction not in cols:
            msg = ("'{}' is not a recognized column name".format(transaction) +
                   " of the input data.")
            LOGGER.error(msg)
            raise ValueError(msg)
        if item is not None and item not in cols:
            msg = ("'{}' is not a recognized column name".format(item) +
                   " of the input data.")
            LOGGER.error(msg)
            raise ValueError(msg)

        if transaction is None:
            transaction = cols[0]
        cols.remove(transaction)
        if item is None:
            item = cols[-1]
        if any(data.dtypes([opt])[0][1] not in ('INT', 'VARCHAR', 'NVARCHAR') for opt in (transaction, item)):#pylint:disable=line-too-long
            msg = ("Wrong data type for transaction ID or item ID.")
            LOGGER.error(msg)
            raise TypeError(msg)

        if lhs_restrict is not None:
            if data.dtypes([item])[0][1] == 'INT':
                if isinstance(lhs_restrict, list) and all(isinstance(lhs, _INTEGER_TYPES) for lhs in lhs_restrict):#pylint:disable=line-too-long
                    pass
                else:
                    msg = ("Item ID of the input data is of integer type, "+
                           "in this case 'lhs_restrict' should be a list "+
                           "of integers.")
                    LOGGER.error(msg)
                    raise TypeError(msg)
            else:
                lhs_restrict = self._arg('lhs_restrict', lhs_restrict, ListOfStrings)

        if rhs_restrict is not None:
            if data.dtypes([item])[0][1] == 'INT':
                if isinstance(rhs_restrict, list) and all(isinstance(rhs, _INTEGER_TYPES) for rhs in rhs_restrict):#pylint:disable=line-too-long
                    pass
                else:
                    msg = ("Item ID of the input data is of integer type, "+
                           "in this case 'rhs_restrict' should be a list of"+
                           " integers.")
                    LOGGER.error(msg)
                    raise TypeError(msg)
            else:
                rhs_restrict = self._arg('rhs_restrict', rhs_restrict, ListOfStrings)
        lhs_complement_rhs = self._arg('lhs_complement_rhs', lhs_complement_rhs, bool)
        rhs_complement_lhs = self._arg('rhs_complement_lhs', rhs_complement_lhs, bool)
        data_ = data[[transaction]+[item]]
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        outputs = ['RESUTL', 'STATS', 'ANTEC', 'CONSEQ']
        outputs = ['#PAL_APRIORI_{}_TBL_{}_{}'.format(name, self.id, unique_id)
                   for name in outputs]
        result_tbl, stats_tbl, antec_tbl, conseq_tbl = outputs

        param_rows = [
            ('MIN_SUPPORT', None, self.min_support, None),
            ('MIN_CONFIDENCE', None, self.min_confidence, None),
            ('MIN_LIFT', None, self.min_lift, None),
            ('MAX_CONSEQUENT', self.max_conseq, None, None),
            ('MAXITEMLENGTH', self.max_len, None, None),
            ('UBIQUITOUS', None, self.ubiquitous, None),
            ('LHS_IS_COMPLEMENTARY_RHS', lhs_complement_rhs, None, None),
            ('RHS_IS_COMPLEMENTARY_LHS', rhs_complement_lhs, None, None),
            ('THREAD_RATIO', None, self.thread_ratio, None),
            ('TIMEOUT', self.timeout, None, None)
        ]

        if lhs_restrict is not None:
            param_rows.extend(('LHS_RESTRICT', None, None, str(item_id))
                              for item_id in lhs_restrict)
        if rhs_restrict is not None:
            param_rows.extend(('RHS_RESTRICT', None, None, str(item_id))
                              for item_id in rhs_restrict)

        if self.relational:
            fpgrowth_proc = 'PAL_FPGROWTH_RELATIONAL'
            output_tbls = [antec_tbl, conseq_tbl, stats_tbl]
        else:
            fpgrowth_proc = 'PAL_FPGROWTH'
            output_tbls = [result_tbl]

        try:
            self._call_pal_auto(conn,
                                fpgrowth_proc,
                                data_,
                                ParameterTable().with_data(param_rows),
                                *output_tbls)
        except dbapi.Error as db_err:
            LOGGER.exception(str(db_err))
            try_drop(conn, output_tbls)
            raise
        except Exception as db_err:
            LOGGER.exception(str(db_err))
            try_drop(conn, output_tbls)
            raise
        #pylint:disable=attribute-defined-outside-init
        if self.relational:
            self.antec_ = conn.table(antec_tbl)
            self.conseq_ = conn.table(conseq_tbl)
            self.stats_ = conn.table(stats_tbl)
            self.statistics_ = self.stats_
            self.result_ = None
        else:
            self.result_ = conn.table(result_tbl)
            self.antec_ = None
            self.conseq_ = None
            self.stats_ = None
            self.statistics_ = self.stats_

class KORD(PALBase):#pylint:disable=invalid-name
    r"""
    The K-Optimal Rule Discovery (KORD) algorithm is a machine learning tool used for generating top-K association rules based on a user-defined measure.
    Unlike traditional association rule mining, which requires the discovery of frequent itemsets before creating rules, KORD directy identifies optimal rules.
    This algorithm is helpful in tasks like market basket analysis and recommendation systems where discovering associations between items is critical.

    Parameters
    ----------

    k : int, optional

        Specifies the number of top-k highest priority rules to discover.

        Defaults to 10.

    measure : {'leverage', 'lift', 'coverage', 'confidence'}, optional

        Defines the priority measure for the association rules.

        Defaults to 'leverage'.

    min_support : float, optional

        Minimum support value of an association rule, within [0, 1] range.

        Defaults to 0.

    min_confidence : float, optional

        Minimum confidence value of an association rule, within [0, 1] range.

        Defaults to 0.

    min_coverage : float, optional

        Minimum coverage value of an association rule, within [0, 1] range.

        Defaults to the value of ``min_support`` if not provided.

    min_measure : float, optional

        Minimum measure value (either leverage or lift, depending on the ``measure`` setting).

        Defaults to 0.

    max_antec : int, optional

        Maximum number of antecedent items in generated rules.

        Defaults to 4.

    epsilon : float, optional

        Epsilon value used for penalizing the length of rules.

        This parameter is valid only when ``use_epsilon`` is True.

        Defaults to 0.0.

    use_epsilon : bool, optional

        Dictates if the length of rules should be penalized using ``epsilon``.

        Defaults to False.

    max_conseq : int, optional

        Maximum number of consequent items in generated rules. Should not exceed 3.

        Defaults to 1.

    Attributes
    ----------

    antec_ : DataFrame
        Info of antecedent items for the mined association rules, structured as follows:

            - 1st column : rule ID,
            - 2nd column : antecedent items.

    conseq_ : DataFrame
        Info of consequent items for the mined association rules, structured as follows:

            - 1st column : rule ID,
            - 2nd column : consequent items.

    stats_ : DataFrame
        Some basic statistics for the mined association rules, structured as follows:
            - 1st column : rule ID,
            - 2nd column : support value of rules,
            - 3rd column : confidence value of rules,
            - 4th column : lift value of rules,
            - 5th column : leverage value of rules,
            - 6th column : measure value of rules.

    Examples
    --------

    Input DataFrame df:

    >>> df.head(10).collect()
        CUSTOMER   ITEM
    0          2  item2
    1          2  item3
    ...
    8          5  item3
    9          6  item1

    Initialize a KORD object:

    >>> krd = KORD(k=5,
                   measure='lift',
                   min_support=0.1,
                   min_confidence=0.2,
                   epsilon=0.1,
                   use_epsilon=False)

    Perform the fit() and obtain the result:

    >>> krd.fit(data=df, transaction='CUSTOMER', item='ITEM')
    >>> krd.antec_.collect()
       RULE_ID ANTECEDENT_RULE
    0        0           item2
    1        1           item1
    2        2           item2
    3        2           item1
    4        3           item5
    5        4           item2
    >>> krd.conseq_.collect()
       RULE_ID CONSEQUENT_RULE
    0        0           item5
    1        1           item5
    2        2           item5
    3        3           item1
    4        4           item4
    >>> krd.stats_.collect()
       RULE_ID   SUPPORT  CONFIDENCE      LIFT  LEVERAGE   MEASURE
    0        0  0.222222    0.285714  1.285714  0.049383  1.285714
    1        1  0.222222    0.333333  1.500000  0.074074  1.500000
    2        2  0.222222    0.500000  2.250000  0.123457  2.250000
    3        3  0.222222    1.000000  1.500000  0.074074  1.500000
    4        4  0.222222    0.285714  1.285714  0.049383  1.285714
    """
    measure_maps = {'leverage' : 0, 'lift' : 1, 'support' : 2, 'confidence' : 3}
    def __init__(self,#pylint:disable=too-many-arguments
                 k=None,
                 measure=None,
                 min_support=None,
                 min_confidence=None,
                 min_coverage=None,
                 min_measure=None,
                 max_antec=None,
                 epsilon=None,
                 use_epsilon=None,
                 max_conseq=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(KORD, self).__init__()
        self.k = self._arg('k', k, int)
        self.measure = self._arg('measure', measure, self.measure_maps)
        self.min_support = self._arg('min_support', min_support, float)
        self.min_confidence = self._arg('min_confidence', min_confidence, float)
        self.min_coverage = self._arg('min_coverage', min_coverage, float)
        self.min_measure = self._arg('min_measure', min_measure, float)
        self.max_antec = self._arg('max_antec', max_antec, int)
        self.epsilon = self._arg('epsilon', epsilon, float)
        self.use_epsilon = self._arg('use_epsilon', use_epsilon, bool)
        self.max_conseq = self._arg('max_conseq', max_conseq, int)
        if self.max_conseq is not None and self.max_conseq > 3:
            msg = "The value of `max_conseq` should not be greater than 3."
            raise ValueError(msg)

    def fit(self, data, transaction=None, item=None):
        """
        Association rule mining on the given data.

        Parameters
        ----------

        data : DataFrame
            The input data.

        transaction : str, optional

            Column name of transaction ID in the input data.

            Defaults to name of the 1st column if not provided.

        item : str, optional

           Column name of item ID (or items) in the input data.

           Defaults to the name of the last non-transaction column if not provided.
        """
        conn = data.connection_context
        require_pal_usable(conn)
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, 'training_data', data)
        cols = data.columns
        if len(cols) < 2:
            msg = ("The input data has less than 2 columns, which is insufficient "+
                   "for association rule mining.")
            raise ValueError(msg)
        if any(col not in cols + [None] for col in (transaction, item)):
            msg = ("Unrecognized column name for transaction ID or item ID "+
                   "for the input data.")
            raise ValueError(msg)
        transaction = self._arg('transaction', transaction, str)
        item = self._arg('item', item, str)
        if transaction is None:
            transaction = cols[0]
        cols.remove(transaction)
        if item is None:
            item = cols[-1]
        if any(data.dtypes([col])[0][1] not in ('INT', 'VARCHAR', 'NVARCHAR') for col in (transaction, item)):#pylint:disable=line-too-long
            msg = ("Wrong data type for transaction ID or item ID.")
            LOGGER.error(msg)
            raise TypeError(msg)
        data_ = data[[transaction]+[item]]
        param_rows = [
            ("TOPK", self.k, None, None),
            ("MAX_ANTECEDENT", self.max_antec, None, None),
            ("MEASURE_TYPE", self.measure, None, None),
            ("MIN_SUPPORT", None, self.min_support, None),
            ("MIN_CONFIDENCE", None, self.min_confidence, None),
            ("MIN_COVERAGE", None, self.min_coverage, None),
            ("MIN_MEASURE", None, self.min_measure, None),
            ("EPSILON", None, self.epsilon, None),
            ("IS_USE_EPSILON", self.use_epsilon, None, None),
            ("MAX_CONSEQUENT", self.max_conseq, None, None)]
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        tables = ['ANTECEDENT', 'CONSEQUENT', 'STATISTICS']
        tables = ["#PAL_KORD_{}_TBL_{}_{}".format(table, self.id, unique_id)
                  for table in tables]
        antec_tbl, conseq_tbl, stats_tbl = tables
        try:
            self._call_pal_auto(conn,
                                "PAL_KORD",
                                data_,
                                ParameterTable().with_data(param_rows),
                                *tables)
        except dbapi.Error as db_err:
            LOGGER.exception(str(db_err))
            try_drop(conn, tables)
            raise
        except Exception as db_err:
            LOGGER.exception(str(db_err))
            try_drop(conn, tables)
            raise
        self.antec_ = conn.table(antec_tbl)#pylint:disable=attribute-defined-outside-init
        self.conseq_ = conn.table(conseq_tbl)#pylint:disable=attribute-defined-outside-init
        self.stats_ = conn.table(stats_tbl)
        #pylint:disable=attribute-defined-outside-init
        self.statistics_ = self.stats_
        #pylint:disable=attribute-defined-outside-init

class SPM(PALBase):#pylint:disable=invalid-name
    r"""
    The Sequential Pattern Mining (SPM) algorithm is a method in data mining developed to determine frequent patterns that occur in sequential data. This could be employed in several applications from market basket analysis to medical data analysis. Algorithm's purpose is to identify the patterns of purchase or occurrence in a sequence of time, highlighting patterns or trends in the data that may not have been initially apparent.

    Parameters
    ----------

    min_support : float

        Specifies the minimum support value. Any item with support less than the user-specified minimum support value is not included in the frequent item mining phase.

    relational : bool, optional

        Determines if relational logic should be applied in sequential pattern mining. If set to False, a single table for frequent pattern mining results is produced. Conversely, if set to True, the results table is split into two tables: one for mined patterns, and another for statistics.

        Defaults to False.

    ubiquitous : float, optional

        Defines the limit above which items are disregarded during the frequent item mining phase.

        Defaults to 1.0.

    min_len : int, optional

        This parameter indicates the minimum number of items that can be present in a transaction. If transactions contain less than this number, they won't be considered during the pattern mining process.

        Defaults to 1.

    max_len : int, optional

        This parameter indicates the maximum number of items that can be present in a transaction.

        Defaults to 10.

    min_len_out : int, optional

        This denotes the minimum number of items to be included in the mined association rules in the result table.

        Defaults to 1.

    max_len_out : int, optional

        Specifies the maximum number of items of the mined association rules in the result table.

        Defaults to 10.

    calc_lift : bool, optional

        Defines whether or not to compute lift values for all appropriate cases. If set to False, lift values are only computed for cases where the last transaction entails a single item.

        Defaults to False.

    timeout : int, optional

        Specifies the maximum run time for the algorithm in seconds. The algorithm will cease computation if the specified timeout is exceeded.

        Defaults to 3600.

    Attributes
    ----------
    result_ : DataFrame
        The overall frequent pattern mining result, structured as follows:

            - 1st column : mined frequent patterns,
            - 2nd column : support values,
            - 3rd column : confidence values,
            - 4th column : lift values.

        Available only when ``relational`` is False.

    pattern_ : DataFrame
        Result for mined  frequent patterns, structured as follows:
            - 1st column : pattern ID,
            - 2nd column : transaction ID,
            - 3rd column : items.

        Available only when ``relational`` is True.

    stats_ : DataFrame
        Statistics for frequent pattern mining, structured as follows:
            - 1st column : pattern ID,
            - 2nd column : support values,
            - 3rd column : confidence values,
            - 4th column : lift values.

        Available only when ``relational`` is True.

    Examples
    --------

    Input DataFrame df:

    >>> df.collect()
       CUSTID  TRANSID      ITEMS
    0       A        1      Apple
    1       A        1  Blueberry
    ...
    11      C        2  Blueberry
    12      C        3    Dessert

    Initialize a SPM object:

    >>> sp = SPM(min_support=0.5,
                 relational=False,
                 ubiquitous=1.0,
                 max_len=10,
                 min_len=1,
                 calc_lift=True)

    Perform the fit() and obtain the result:

    >>> sp.fit(data=df, customer='CUSTID', transaction='TRANSID', item='ITEMS')
    >>> sp.result_.collect()
                            PATTERN   SUPPORT  CONFIDENCE      LIFT
    0                       {Apple}  1.000000    0.000000  0.000000
    1           {Apple},{Blueberry}  0.666667    0.666667  0.666667
    2             {Apple},{Dessert}  1.000000    1.000000  1.000000
    ...
    10           {Cherry},{Dessert}  0.666667    1.000000  1.000000
    11                    {Dessert}  1.000000    0.000000  0.000000
    """

    def __init__(self,#pylint:disable=too-many-arguments
                 min_support,
                 relational=None,
                 max_len=None,
                 min_len=None,
                 max_len_out=None,
                 min_len_out=None,
                 ubiquitous=None,
                 calc_lift=None,
                 timeout=None):
        setattr(self, 'hanaml_parameters', pal_param_register())
        super(SPM, self).__init__()
        self.min_support = self._arg('min_support', min_support, float, required=True)
        self.relational = self._arg('relational', relational, bool)
        self.max_len = self._arg('max_len', max_len, int)
        self.min_len = self._arg('min_len', min_len, int)
        self.max_len_out = self._arg('max_len_out', max_len_out, int)
        self.min_len_out = self._arg('min_len_out', min_len_out, int)
        self.ubiquitous = self._arg('ubiquitous', ubiquitous, float)
        self.calc_lift = self._arg('calc_lift', calc_lift, int)
        self.timeout = self._arg('timeout', timeout, int)

    def fit(self, data,#pylint:disable=too-many-branches, too-many-locals, too-many-arguments, too-many-statements
            customer=None,
            transaction=None,
            item=None,
            item_restrict=None,
            min_gap=None):
        r"""
        Association rule mining on the given data.

        Parameters
        ----------

        data : DataFrame
            The input data.

        customer : str, optional

            Column name of customer ID in the input data.

            Defaults to name of the 1st column if not provided.

        transaction : str, optional

            Column name of transaction ID in the input data.

            Specially for sequential pattern mining, values of this column
            must reflect the sequence of occurrence as well.

            Defaults to name of the 1st non-customer column if not provided.

        item : str, optional

            Column name of item ID (or items) in the input data.

            Defaults to the name of the last non-customer, non-transaction column if not provided.

        item_restrict : list of int or str, optional

           Specifies the list of items allowed in the mined association rule.

           No default value

        min_gap : int, optional

           Specifies the the minimum time difference between consecutive transactions
           in a sequence.

           No default value.


        """

        conn = data.connection_context
        require_pal_usable(conn)
        setattr(self, 'hanaml_fit_params', pal_param_register())
        setattr(self, 'training_data', data)
        cols = data.columns
        if len(cols) < 3:
            msg = ("Input data has less than 3 columns, insufficient for "+
                   "sequential pattern mining.")
            raise ValueError(msg)
        customer = self._arg('customer', customer, str)
        transaction = self._arg('transaction', transaction, str)
        item = self._arg('item', item, str)
        min_gap = self._arg('min_gap', min_gap, int)
        if any(col not in cols + [None] for col in (customer, transaction, item)):
            msg = ("Unrecognized column name for customer ID, transaction ID "+
                   "or item ID for the input data.")
            raise ValueError(msg)
        if customer is None:
            customer = cols[0]
        cols.remove(customer)
        if transaction is None:
            transaction = cols[0]
        cols.remove(transaction)
        if item is None:
            item = cols[-1]
        if any(data.dtypes([col])[0][1] not in ('INT', 'VARCHAR', 'NVARCHAR') for col in (customer, item)):#pylint:disable=line-too-long
            msg = ("Wrong data type for customer ID or item ID.")
            LOGGER.error(msg)
            raise TypeError(msg)
        if data.dtypes([transaction])[0][1] not in ('INT', 'TIMESTAMP'):
            msg = ("Transaction IDs must be of integer type or timestamp.")
            raise TypeError(msg)
        if item_restrict is not None:
            if data.dtypes([item])[0][1] == 'INT':
                if isinstance(item_restrict, list) and all(isinstance(it, _INTEGER_TYPES) for it in item_restrict):#pylint:disable=line-too-long
                    pass
                else:
                    msg = ("Item column is integer-valued, in this case " +
                           "valid 'item_restrict' must be list of integers.")
                    raise ValueError(msg)
            else:
                item_restrict = self._arg('item_restrict', item_restrict, ListOfStrings)
        used_cols = [customer, transaction, item]
        data_ = data[used_cols]
        param_rows = [
            ('MIN_SUPPORT', None, self.min_support, None),
            ('MAX_EVENT_SIZE', self.max_len, None, None),
            ('MIN_EVENT_SIZE', self.min_len, None, None),
            ('MAX_EVENT_LENGTH', self.max_len_out, None, None),
            ('MIN_EVENT_LENGTH', self.min_len_out, None, None),
            ('UBIQUITOUS', None, self.ubiquitous, None),
            ('CALCULATE_LIFT', self.calc_lift, None, None),
            ('TIMEOUT', self.timeout, None, None),
            ('MIN_GAP', min_gap, None, None)
            ]
        if item_restrict is not None:
            param_rows.extend([('ITEM_RESTRICT', None, None, str(item)) for
                               item in item_restrict])
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        if self.relational in (None, False):
            spm_proc = "PAL_SPM"
            tables = ['RESULT']
            tables = ["#PAL_SPM_{}_TBL_{}_{}".format(tbl, self.id, unique_id)
                      for tbl in tables]
            result_tbl = tables[0]
        else:
            spm_proc = "PAL_SPM_RELATIONAL"
            tables = ['PATTERN', 'STATISTICS']
            tables = ["#PAL_SPM_{}_TBL_{}_{}".format(tbl, self.id, unique_id)
                      for tbl in tables]
            pattern_tbl, stats_tbl = tables

        try:
            self._call_pal_auto(conn,
                                spm_proc,
                                data_,
                                ParameterTable().with_data(param_rows),
                                *tables)
        except dbapi.Error as db_err:
            LOGGER.exception(str(db_err))
            try_drop(conn, tables)
            raise
        except Exception as db_err:
            LOGGER.exception(str(db_err))
            try_drop(conn, tables)
            raise
        if self.relational in (None, False):
            self.result_ = conn.table(result_tbl)#pylint:disable=attribute-defined-outside-init
        else:
            self.pattern_ = conn.table(pattern_tbl)#pylint:disable=attribute-defined-outside-init
            self.stats_ = conn.table(stats_tbl)
            #pylint:disable=attribute-defined-outside-init
            self.statistics_ = self.stats_
            #pylint:disable=attribute-defined-outside-init
