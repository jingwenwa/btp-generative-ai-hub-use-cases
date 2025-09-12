"""
This module contains classes of model selection.

The following classes are available:

    * :class:`ParamSearchCV`
    * :class:`GridSearchCV`
    * :class:`RandomSearchCV`
"""
import logging
import numpy
from .unified_classification import UnifiedClassification
from .unified_regression import UnifiedRegression
from .regression import PolynomialRegression
from .naive_bayes import NaiveBayes
from .kernel_density import KDE
#pylint: disable=too-many-arguments, line-too-long
#pylint: disable=useless-object-inheritance
#disable=old-style-class, super-on-old-class
#pylint: disable=consider-using-f-string
logger = logging.getLogger(__name__) #pylint: disable=invalid-name

class ParamSearchCV(object):
    """
    Exhaustive or random search over specified parameter values for an estimator with crossover validation (CV).

    Parameters
    ----------
    estimator : an estimator object
        This is assumed to implement the PAL estimator interface.

    param_grid : dict
        Dictionary with parameters names (string) as keys and lists of parameter settings
        to try as values in which case the grids spanned by each dictionary in the list
        are explored.

        This enables searching over any sequence of parameter settings.

    train_control : dict
        Controlling parameters for model evaluation and parameter selection.

    scoring : str
        A string of scoring method to evaluate the predictions.
        The options of scoring depend on the specific algorithm of an used estimator.

    search_strategy : str
        The search strategy and the options are ``grid`` or ``random``.

    Attributes
    ----------
    estimator : an estimator object.

    """
    estimator = None
    exist_resampling_method = False
    def __init__(self,
                 estimator,
                 param_grid,
                 train_control,
                 scoring,
                 search_strategy):
        param_values = []
        param_values_dict = {}
        self.exist_resampling_method = False
        for key, val in param_grid.items():
            if isinstance(val, (range, numpy.ndarray)):
                val = list(val)
            if not isinstance(val, list):
                val = [val]
            param_values.extend([(key, val)])
            param_values_dict[key] = val

        for key, val in train_control.items():
            if key == "resampling_method":
                self.exist_resampling_method = True
        if isinstance(estimator, (UnifiedClassification, UnifiedRegression)):
            func = None
            for key_ in estimator.func_dict:
                if estimator.func_dict[key_] == estimator.func:
                    func = key_
                    break
            estimator.__init__(func,
                               **dict(param_search_strategy=search_strategy,
                                      evaluation_metric=scoring,
                                      param_values=param_values_dict,
                                      **train_control))
        else:
            estimator.add_attribute("param_search_strategy", search_strategy)
            estimator.add_attribute("search_strategy", search_strategy)
            estimator.add_attribute("evaluation_metric", scoring.upper())
            for key, val in train_control.items():
                estimator.add_attribute(key, val)
            if isinstance(estimator, NaiveBayes):
                estimator.add_attribute("alpha_values", param_values_dict["alpha"])
            if isinstance(estimator, PolynomialRegression):
                estimator.add_attribute("degree_values", param_values_dict["degree"])
            if isinstance(estimator, KDE):
                estimator.add_attribute("bandwidth_values", param_values_dict["bandwidth"])
            estimator.add_attribute("param_values", param_values)
        self.estimator = estimator

    def set_timeout(self, timeout):
        """
        Specifies the maximum running time for model evaluation or parameter selection.
        Unit is second.
        No timeout when 0 is specified.

        Parameters
        ----------
        timeout : int
            The maximum running time. The unit is second.
        """
        if isinstance(self.estimator, (UnifiedClassification, UnifiedRegression)):
            self.estimator.update_cv_params('timeout', timeout, int)
        else:
            self.estimator.add_attribute("timeout", timeout)

    def set_seed(self, seed, seed_name=None):
        """
        Specifies the seed for random generation.
        Use system time when 0 is specified.

        Parameters
        ----------
        seed : int
            The random seed number.

        seed_name : int, optional
            The name of the random seed.

            Defaults to None.
        """
        if isinstance(self.estimator, (UnifiedClassification, UnifiedRegression)):
            self.estimator.update_cv_params('random_state', seed, int)
        else:
            if seed_name is not None:
                self.estimator.add_attribute(seed_name, seed)
            else:
                self.estimator.add_attribute("random_state", seed)

    def set_resampling_method(self, method):
        """
        Specifies the resampling method for model evaluation or parameter selection.

        Parameters
        ----------
        method : str
            Specifies the resampling method for parameter selection.

              - "cv"
              - "cv_sha"
              - "cv_hyperband"
              - "stratified_cv"
              - "stratified_cv_sha"
              - "stratified_cv_hyperband"
              - "bootstrap"
              - "bootstrap_sha"
              - "bootstrap_hyperband"
              - "stratified_bootstrap"
              - "stratified_bootstrap_sha"
              - "stratified_bootstrap_hyperband"

            Resampling methods with prefix "stratified" can only apply to classification algorithms.

        """
        if isinstance(self.estimator, (UnifiedClassification, UnifiedRegression)):
            self.estimator.update_cv_params('resampling_method', method, str)
        else:
            self.estimator.add_attribute("resampling_method", method)
        self.exist_resampling_method = True

    def set_scoring_metric(self, metric):
        """
        Specifies the scoring metric.

        Parameters
        ----------
        metric : str
            Specifies the evaluation metric for model evaluation or parameter selection.

            - "accuracy"
            - "error_rate"
            - "f1_score"
            - "rmse"
            - "mae"
            - "auc"
            - "nll" (negative log likelihood)

        """
        if isinstance(self.estimator, (UnifiedClassification, UnifiedRegression)):
            self.estimator.update_cv_params('evaluation_metric', metric, str)
        else:
            self.estimator.add_attribute("metric", metric.upper())
            self.estimator.add_attribute("evaluation_metric", metric.upper())

    def fit(self, data, **kwargs):
        """
        Fit the model to the training dataset.

        Parameters
        ----------
        data : DataFrame
            Input DataFrame.

        **kwargs: dict
            A dict of the keyword args passed to the function.
            Please refer to the documentation of the specific function for parameter information.
        """
        if not self.exist_resampling_method:
            err_msg = "'resampling_method' has not been set!"
            logger.error(err_msg)
            raise KeyError(err_msg)
        self.estimator.fit(data, **kwargs)

    def predict(self, data, **kwargs):
        """
        Predict function.

        Parameters
        ----------
        data : DataFrame
            Input DataFrame.

        **kwargs: dict
            A dict of the keyword args passed to the function.
            Please refer to the documentation of the specific function for parameter information.
        """
        return self.estimator.predict(data, **kwargs)

class GridSearchCV(ParamSearchCV):
    """
    Exhaustive search over specified parameter values for an estimator with crossover validation (CV).

    Parameters
    ----------
    estimator : estimator object
        This is assumed to implement the PAL estimator interface.
    param_grid : dict
        Dictionary with parameters names (string) as keys and lists of parameter settings
        to try as values in which case the grids spanned by each dictionary in the list
        are explored.
        This enables searching over any sequence of parameter settings.
    train_control : dict
        Controlling parameters for model evaluation and parameter selection.
    scoring : str
        A string of scoring method to evaluate the predictions.
        The options of scoring depend on the specific algorithm of an used estimator.

    Attributes
    ----------
    estimator : an estimator object.

    Examples
    --------
    >>> uhgc = UnifiedClassification(func='HybridGradientBoostingTree')

    Create a "GridSearchCV" object:

    >>> gscv = GridSearchCV(estimator=uhgc,
                            param_grid={'learning_rate': [0.1, 0.4, 0.7, 1],
                                        'n_estimators': [4, 6, 8, 10],
                                        'split_threshold': [0.1, 0.4, 0.7, 1]},
                            train_control=dict(fold_num=5,
                                               resampling_method='cv',
                                               random_state=1,
                                               ref_metric=['error_rate']),
                            scoring='error_rate')

    Invoke fit():

    >>> gscv.fit(data=df_train,
                 key= 'ID',
                 label='CLASS',
                 partition_method='stratified',
                 partition_random_state=1,
                 stratified_column='CLASS')

    """
    def __init__(self,
                 estimator,
                 param_grid,
                 train_control,
                 scoring):
        super(GridSearchCV, self).__init__(estimator,
                                           param_grid,
                                           train_control,
                                           scoring,
                                           "grid")

class RandomSearchCV(ParamSearchCV):
    """
    Random search over specified parameter values for an estimator with crossover validation (CV).

    Parameters
    ----------
    estimator : estimator object
        This is assumed to implement the PAL estimator interface.
    param_grid : dict
        Dictionary with parameters names (string) as keys and lists of parameter settings
        to try as values in which case the grids spanned by each dictionary in the list
        are explored.

        This enables searching over any sequence of parameter settings.
    train_control : dict
        Controlling parameters for model evaluation and parameter selection.
    scoring : str
        A string of scoring method to evaluate the predictions.
        The options of scoring depend on the specific algorithm of an used estimator.

    Attributes
    ----------
    estimator : an estimator object.

    Examples
    --------
    >>> uhgc = UnifiedClassification(func='HybridGradientBoostingTree')

    Create a "RandomSearchCV" object:

    >>> urscv = RandomSearchCV(estimator=uhgc,
                               param_grid={'learning_rate': [0.1, 0.4, 0.7, 1],
                                           'n_estimators': [4, 6, 8, 10],
                                           'split_threshold': [0.1, 0.4, 0.7, 1]},
                               train_control=dict(fold_num=5,
                                                  resampling_method='cv',
                                                  random_state=0,
                                                  random_search_times=8,
                                                  ref_metric=['error_rate']),
                               scoring='error_rate')

    Invoke fit():

    >>> urscv.fit(data=training_df,
                  key= 'ID',
                  label='CLASS',
                  partition_method='stratified',
                  partition_random_state=1,
                  stratified_column='CLASS')

    """
    def __init__(self,
                 estimator,
                 param_grid,
                 train_control,
                 scoring):
        super(RandomSearchCV, self).__init__(estimator,
                                             param_grid,
                                             train_control,
                                             scoring,
                                             "random")
