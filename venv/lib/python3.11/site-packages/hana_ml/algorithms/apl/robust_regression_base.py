"""
Module defining a common abstract class for SAP HANA APL classification and regression.
"""
from collections import OrderedDict
import logging
import pandas as pd
from hana_ml.ml_exceptions import FitIncompleteError
from hana_ml.algorithms.apl.apl_base import APLBase

logger = logging.getLogger(__name__) #pylint: disable=invalid-name


class RobustRegressionBase(APLBase):
    """
    Common abstract class for classification and regression.
    """
    def __init__(self,
                 conn_context=None,
                 variable_auto_selection=True,
                 polynomial_degree=None,
                 variable_storages=None,
                 variable_value_types=None,
                 variable_missing_strings=None,
                 extra_applyout_settings=None,
                 ** other_params): #pylint: disable=too-many-arguments
        # --- model params
        self.variable_auto_selection = self._arg('variable_auto_selection',
                                                 variable_auto_selection, bool)
        self.polynomial_degree = self._arg('polynomial_degree',
                                           polynomial_degree,
                                           int)

        super(RobustRegressionBase, self).__init__(
            conn_context,
            variable_storages,
            variable_value_types,
            variable_missing_strings,
            extra_applyout_settings,
            ** other_params)

        self._model_type = 'regression/classification'

    @staticmethod
    def _get_target_varname_from_applyout_table(applyout_df,
                                                target_varname=None):
        """
        Finds the name of the label column in the applyout table.
        The column must be rr_<Label>
        Arguments:
        ---------
        - applyout_df: DataFrame,
            The applyout dataframe
        - target_varname: str, optional
            The target variable name for which scores are computed.
            It is useful when there are multiple targets.
            If it is not provided, the first one is taken.

        Returns:
        ------
        The name of the target variable name
        """
        # The column must be rr_<Label>
        if target_varname is None:
            target_varname = [c for c in applyout_df.columns
                              if c.startswith('rr_')]
            target_varname = target_varname[0]
            if (target_varname is not None and len(target_varname) > 3):
                target_varname = target_varname[3:]  # remove 'rr_'
            else:
                raise FitIncompleteError("Cannot determine target variable name")
        else:
            target_varname = target_varname
        return target_varname

    def get_performance_metrics(self):
        """
        Returns the performance metrics of the last trained model.

        Returns
        -------

        OrderedDict or pandas DataFrame
            If no segment column is given, an OrderedDict with metric name as key and metric value
            as value.

            If a segment column is given, a pandas DataFrame which contains the performance metrics
            for each segment.
        """
        if not hasattr(self, 'indicators_'):
            raise FitIncompleteError(
                "The indicators table was not found. Please fit the model.")
        cond = "VARIABLE like 'rr_%' and DETAIL IS NULL"
        df_ind = self.indicators_.filter(cond).collect()
        df_ind = df_ind[df_ind['KEY'] != 'PredictionConfidence'].reset_index(drop=True)
        prediction_confidence = self.get_debrief_report('ClassificationRegression_Performance') \
            .filter('"Partition" = \'Estimation\' and "Indicator" = \'Prediction Confidence\'') \
            .collect()
        segment_column = getattr(self, 'segment_column_name', None)
        if segment_column is None:
            # Create the OrderedDict to be returned
            # converts df_ind.VALUE str to float if possible
            ret = OrderedDict()
            for key, old_v in zip(df_ind.KEY, df_ind.VALUE):
                try:
                    new_v = float(old_v)
                except ValueError:
                    new_v = old_v
                ret[key] = new_v

            if not prediction_confidence.empty:
                ret['PredictionConfidence'] = prediction_confidence['Value'].iloc[0]
            return ret
        else:
            df_ind = df_ind[['OID', 'KEY', 'VALUE']]
            df_ind.columns = [segment_column, 'Metric', 'Value']

            if not prediction_confidence.empty:
                prediction_confidence = prediction_confidence[['Oid', 'Indicator', 'Value']]
                prediction_confidence.columns = [segment_column, 'Metric', 'Value']
                prediction_confidence.replace('Prediction Confidence', 'PredictionConfidence', inplace=True)
                df_ind = pd.concat([df_ind, prediction_confidence])
                df_ind.sort_values(by=[segment_column, 'Metric'], inplace=True, ignore_index=True)

            return df_ind

    def get_feature_importances(self): #pylint: disable=unused-argument
        """
        Returns the feature importances (MaximumSmartVariableContribution).

        Returns
        -------
        OrderedDict or pandas DataFrame
            If no segment column is given, an OrderedDict:
            { feature_name : value }.

            If a segment column is given, a pandas DataFrame which contains the feature importances
            for each segment.
        """
        if not hasattr(self, 'indicators_'):
            raise FitIncompleteError(
                "The indicators table was not found. Please fit the model.")
        cond = "KEY = 'MaximumSmartVariableContribution'"
        df_ind = self.indicators_.filter(cond).collect()
        segment_column = getattr(self, 'segment_column_name', None)
        if segment_column is None:
            kvals = [(var, float(val))
                    for var, val in zip(df_ind.VARIABLE, df_ind.VALUE)]
            # Sort by val desc
            kvals.sort(reverse=True, key=lambda c: c[1])
            ret = OrderedDict(kvals)
            return ret
        else:
            df_ind = df_ind[['OID', 'VARIABLE', 'VALUE']]
            df_ind.columns = [segment_column, 'Feature', 'Importance']
            return df_ind
