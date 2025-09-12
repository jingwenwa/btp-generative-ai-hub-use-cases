"""
This module contains the tracking functionality for the artifacts.


The following class is available:

    * :class:`MLExperiments`
    * :func:`get_tracking_log`
    * :func:`get_tracking_metadata`
    * :func:`delete_tracking_log`
    * :func:`delete_experiment_log`
"""
#pylint: disable=protected-access

import logging
import uuid
from hdbcli import dbapi
from hana_ml.algorithms.pal.pal_base import PALBase, try_drop
from hana_ml.algorithms.pal.sqlgen import ParameterTable
from hana_ml.algorithms.pal.utility import check_pal_function_exist

logger = logging.getLogger(__name__)
class MLExperiments(object):
    """
    This class is used to track the models and datasets.

    Parameters
    ----------
    connection_context: object
        The connection context object.
    experiment_id: str
        The experiment id for the model.
    experiment_description: str, optional
        The experiment description for the model.
    """
    EXPERIMENT_SPLITTER = "_<EXPERIMENT_SPLITTER>_"
    current_run_name = None
    count = 0

    def __init__(self,
                 connection_context,
                 experiment_id,
                 experiment_description=None):
        self.connection_context = connection_context
        self.experiment_id = experiment_id
        self.experiment_description = experiment_description
        self.count = 0

    def get_current_tracking_id(self):
        """
        This function is used to get the current tracking id.

        Returns
        -------
        str
            The current tracking id.
        """
        return self.experiment_id + self.EXPERIMENT_SPLITTER + self.current_run_name

    def autologging(
            self,
            model,
            run_name=None,
            dataset_name=None,
            dataset_source=None,
            track_id=None,
            track_description=None):
        """
        This function is used to track the PAL function call.

        Parameters
        ----------
        model: object
            The model object to be tracked.
        run_name: str, optional
            The name of the run. If not provided, it will be generated automatically.
        dataset_name: str, optional
            The name of the dataset used in the PAL function call.
        dataset_source: str, optional
            The source of the dataset used in the PAL function call.
        track_id: str, optional
            The tracking id for the run in the experiment.
        track_description: str, optional
            The tracking description for the run.

        Returns
        -------
        None
        """
        if run_name is None:
            run_name = "run_" + str(self.count)
            self.count += 1
        if track_id is None:
            track_id = self.experiment_id + self.EXPERIMENT_SPLITTER + run_name
        if track_description is None:
            track_description = self.experiment_description
        model._extend_pal_parameter({
            'LOG_ML_TRACK': 1,
            'TRACK_ID': track_id,
            'TRACK_DESCRIPTION': track_description,
            'DATASET_NAME': dataset_name,
            'DATASET_SOURCE': dataset_source
        })
        self.current_run_name = run_name

    def get_tracking_log_for_current_run(self):
        """
        This function is used to get the tracking log for the current run.

        Returns
        -------
        DataFrame
            The tracking log for the current run.
        """
        current_track_id = self.experiment_id + self.EXPERIMENT_SPLITTER + self.current_run_name
        return get_tracking_log(self.connection_context, current_track_id)

    def get_tracking_metadata_for_current_run(self):
        """
        This function is used to get the tracking metadata for the current run.

        Returns
        -------
        DataFrame
            The tracking metadata for the current run.
        """
        current_track_id = self.experiment_id + self.EXPERIMENT_SPLITTER + self.current_run_name
        return get_tracking_metadata(self.connection_context, current_track_id)

def get_tracking_log(connection_context, track_id):
    """
    This function is used to get the tracking log for the given track_id.

    Parameters
    ----------
    connection_context: object
        The connection context object.
    track_id: str
        The tracking id for the model.
    """
    return connection_context.table(table="TRACK_LOG", schema="PAL_ML_TRACK").filter("EXECUTION_ID = '{}'".format(track_id))

def get_tracking_metadata(connection_context, track_id):
    """
    This function is used to get the tracking metadata for the given track_id.

    Parameters
    ----------
    connection_context: object
        The connection context object.
    track_id: str
        The tracking id for the model.
    """
    return connection_context.table(table="TRACK_METADATA", schema="PAL_ML_TRACK").filter("TRACK_ID = '{}'".format(track_id))

def delete_run_log(connection_context, experiment_id, run_name, remove_mode=False, is_force=False):
    """
    This function is used to delete the tracking log for the given run_name.

    Parameters
    ----------
    connection: object
        The connection object.
    experiment_id: str
        The experiment id for the model.
    run_name: str
        The run name for the model.
    """
    track_id = experiment_id + MLExperiments.EXPERIMENT_SPLITTER + run_name
    _delete_tracking_log(connection_context, track_id, remove_mode, is_force)

def _delete_tracking_log(connection_context, track_id, remove_mode=False, is_force=False):
    """
    This function is used to delete the tracking log for the given track_id.

    Parameters
    ----------
    connection_context: object
        The connection context object.
    track_id: str
        The tracking id for the model.
    """
    # connection_context.execute_sql("""
    #                             DELETE FROM "PAL_ML_TRACK"."TRACK_LOG" WHERE EXECUTION_ID = '{0}';
    #                             DELETE FROM "PAL_ML_TRACK"."TRACK_LOG_HEADER" WHERE EXECUTION_ID = '{0}';
    #                             DELETE FROM "PAL_ML_TRACK"."TRACK_METADATA" WHERE TRACK_ID = '{0}';
    #                             """.format(track_id))
    unique_id = str(uuid.uuid1()).replace('-', '_').upper()
    output = '#PAL_REMOVE_TRACK_INFO' + unique_id
    param_rows = [('REMOVE_MODE', remove_mode, None, None),
                  ('TRACK_ID', None, None, track_id),
                  ('IS_FORCE', is_force, None, None)]
    try:
        if check_pal_function_exist(connection_context, '%REMOVE_MLTRACK_LOG%', like=True):
            if not connection_context.has_table(output):
                PALBase()._call_pal_auto(connection_context,
                                        'PAL_REMOVE_MLTRACK_LOG',
                                        ParameterTable().with_data(param_rows),
                                        output)
        else:
            return False
    except dbapi.Error as db_err:
        logger.error(str(db_err))
        try_drop(connection_context, output)
    except Exception as db_err:
        logger.error(str(db_err))
        try_drop(connection_context, output)

def delete_experiment_log(connection_context, experiment_id, remove_mode=False, is_force=False):
    """
    This function is used to delete the tracking log for the given experiment_id.

    Parameters
    ----------
    connection: object
        The connection object.
    experiment_id: str
        The experiment id for the model.
    """
    track_ids = connection_context.sql("SELECT EXECUTION_ID FROM PAL_ML_TRACK.TRACK_LOG").filter("EXECUTION_ID LIKE '{}%'".format(experiment_id)).distinct().collect()["EXECUTION_ID"].tolist()
    for track_id in track_ids:
        _delete_tracking_log(connection_context, track_id, remove_mode, is_force)
