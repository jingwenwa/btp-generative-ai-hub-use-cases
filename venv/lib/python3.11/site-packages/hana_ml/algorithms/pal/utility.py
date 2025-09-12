"""
This module contains Python API of utility functions.
"""
import json
import logging
import os
from pathlib import Path
import sys
import re
import uuid
from functools import wraps
import pandas as pd
try:
    import mlflow
    from mlflow.models import Model
    from mlflow.tracking._model_registry import DEFAULT_AWAIT_MAX_SLEEP_SECONDS
    from mlflow.models.model import MLMODEL_FILE_NAME, _LOG_MODEL_METADATA_WARNING_TEMPLATE
    from mlflow.protos.databricks_pb2 import RESOURCE_ALREADY_EXISTS
    from mlflow.utils.file_utils import TempDir
    from mlflow.models.signature import ModelSignature
    from mlflow.types.schema import Schema, ColSpec
    from mlflow.exceptions import MlflowException
except ImportError:
    pass
except TypeError:
    pass
from hdbcli import dbapi
try:
    import configparser
except ImportError:
    import ConfigParser as configparser
import hana_ml
from hana_ml.ml_base import try_drop
from hana_ml.dataframe import DataFrame, create_dataframe_from_pandas
from hana_ml.model_storage import ModelStorage
from .sqlgen import ParameterTable, trace_sql
from .pal_base import PALBase, call_pal_auto_with_hint
from .partition import train_test_val_split
from .pal_base import (
    ListOfStrings
)
mylogger = logging.getLogger(__name__) #pylint: disable=invalid-name

_HANAML_MLFLOW_MODEL_STORAGE = 'HANAML_MLFLOW_MODEL_STORAGE'

#pylint: disable=bare-except, line-too-long, invalid-name, no-member, no-self-use, unspecified-encoding
#pylint: disable=consider-using-f-string
#pylint: disable=protected-access
#pylint: disable=too-many-nested-blocks
#pylint: disable=dangerous-default-value, arguments-renamed, missing-function-docstring
#pylint: disable=unused-argument, attribute-defined-outside-init, too-few-public-methods

def _type_map(ttype):
    mapped = None
    if ttype.lower() == 'int':
        mapped = 'integer'
    elif ttype.lower() == 'double':
        mapped = 'double'
    elif 'bool' in ttype.lower():
        mapped = 'boolean'
    elif ttype.lower() == 'bigint':
        mapped = 'long'
    elif 'lob' in ttype.lower():
        mapped = 'binary'
    elif 'time' in ttype.lower():
        mapped = 'datetime'
    else:
        mapped = 'string'
    return mapped

def _key_index_check(key, param_name, index_value):
    if key is not None:
        if isinstance(index_value, str) and key != index_value:
            warn_msg = "Discrepancy between the designated {} column '{}' ".format(param_name, key) +\
            "and the designated index {} column which is '{}'.".format(param_name, index_value)
            mylogger.warning(warn_msg)
    elif isinstance(index_value, str):
        key = index_value
    return key

def version_compare(pkg_version, version):
    """
    If pkg's version is greater than the specified version, it returns True. Otherwise, it returns False.
    """
    pkg_ver_list = pkg_version.split(".")
    ver_list = version.split(".")
    if int(pkg_ver_list[0]) > int(ver_list[0]):
        return True
    if int(pkg_ver_list[0]) == int(ver_list[0]):
        if int(pkg_ver_list[1]) > int(ver_list[1]):
            return True
        if int(pkg_ver_list[1]) == int(ver_list[1]):
            if int(pkg_ver_list[2]) >= int(ver_list[2]):
                return True
    return False

def check_pal_function_exist(connection_context, func_name, like=False):
    """
    Check the existence of pal function.
    """
    operator = '='
    if like:
        operator = 'like'
    exist = connection_context.sql('SELECT * FROM "SYS"."AFL_FUNCTIONS" WHERE AREA_NAME = \'AFLPAL\' and FUNCTION_NAME {} \'{}\';'.format(operator, func_name))
    if len(exist.collect()) > 0:
        return True
    return False

def _map_param(name, value, typ):
    tpl = ()
    if typ in [int, bool]:
        tpl = (name, value, None, None)
    elif typ == float:
        tpl = (name, None, value, None)
    elif typ in [str, ListOfStrings]:
        tpl = (name, None, None, value)
    elif isinstance(typ, dict):
        val = value
        if isinstance(val, (int, float)):
            tpl = (name, val, None, None)
        else:
            tpl = (name, None, None, val)
    return tpl

class AMDPHelper(object):
    """
    AMDP Generation helper.
    """
    def __init__(self):
        self.amdp_template_replace = {}
        self.amdp_template = ''
        self.fit_data = None
        self.predict_data = None
        self.abap_class_mapping_dict = {}
        self.label = None

    def add_amdp_template(self, template_name):
        """
        Add AMDP template
        """
        self.amdp_template = self.load_amdp_template(template_name)

    def add_amdp_name(self, amdp_name):
        """
        Add AMDP name.
        """
        self.amdp_template_replace["<<AMDP_NAME>>"] = amdp_name

    def add_amdp_item(self, template_key, value):
        """
        Add item.
        """
        self.amdp_template_replace[template_key] = value

    def build_amdp_class(self):
        """
        After add_item, generate amdp file from template.
        """
        for key, val in self.amdp_template_replace.items():
            self.amdp_template = self.amdp_template.replace(key, val)

    def write_amdp_file(self, filepath=None, version=1, outdir="out"):
        """
        Write template to file.
        """
        if filepath:
            with open(filepath, "w+") as file:
                file.write(self.amdp_template)
        else:
            create_dir = os.path.join(outdir,
                                      self.amdp_template_replace["<<AMDP_NAME>>"],
                                      "abap")
            os.makedirs(create_dir, exist_ok=True)
            filename = "Z_CL_{}_{}.abap".format(self.amdp_template_replace["<<AMDP_NAME>>"], version)
            with open(os.path.join(create_dir, filename), "w+") as file:
                file.write(self.amdp_template)

    def get_amdp_notfillin_key(self):
        """
        Get AMDP not fillin keys.
        """
        return re.findall("(<<[a-zA-Z_]+>>)", self.amdp_template)

    def load_amdp_template(self, template_name):
        """
        Load AMDP template
        """
        filepath = os.path.join(os.path.dirname(__file__),
                                "..",
                                "..",
                                "artifacts",
                                "generators",
                                "filewriter",
                                "templates",
                                template_name)
        with open(filepath, 'r') as file:
            return file.read()

    def load_abap_class_mapping(self):
        """
        Load ABAP class mapping.
        """
        filepath = os.path.join(os.path.dirname(__file__),
                                "..",
                                "..",
                                "artifacts",
                                "config",
                                "data",
                                "hdbtable_to_abap_datatype_mapping.json")
        with open(filepath, 'r') as file:
            self.abap_class_mapping_dict = json.load(file)

    def abap_class_mapping(self, value):
        """
        Mapping the abap class.
        """
        if 'VARCHAR' in value.upper():
            if 'NVARCHAR' in value.upper():
                return self.abap_class_mapping_dict['NVARCHAR']
            else:
                return self.abap_class_mapping_dict['VARCHAR']
        if 'DECIMAL' in value.upper():
            return self.abap_class_mapping_dict['DECIMAL']
        return self.abap_class_mapping_dict[value]

class Settings:
    """
    Configuration of logging level
    """
    settings = None
    user = None
    @staticmethod
    def load_config(config_file, tag='hana'):
        """
        Load HANA credentials.
        """
        Settings.settings = configparser.ConfigParser()
        Settings.settings.read(config_file)
        try:
            url = Settings.settings.get(tag, "url")
        except:
            url = ""
        try:
            port = Settings.settings.getint(tag, "port")
        except:
            port = 0
        try:
            pwd = Settings.settings.get(tag, "passwd")
        except:
            pwd = ''
        try:
            Settings.user = Settings.settings.get(tag, "user")
        except:
            Settings.user = ""
        Settings._init_logger()
        return url, port, Settings.user, pwd

    @staticmethod
    def _set_log_level(logger, level):
        if level == 'info':
            logger.setLevel(logging.INFO)
        else:
            if level == 'warn':
                logger.setLevel(logging.WARN)
            else:
                if level == 'debug':
                    logger.setLevel(logging.DEBUG)
                else:
                    logger.setLevel(logging.ERROR)

    @staticmethod
    def _init_logger():
        logging.basicConfig()
        for module in ["hana_ml.ml_base", 'hana_ml.dataframe', 'hana_ml.algorithms.pal']:
            try:
                level = Settings.settings.get("logging", module)
            except:
                level = "error"
            logger = logging.getLogger(module)
            Settings._set_log_level(logger, level.lower())

    @staticmethod
    def set_log_level(level='info'):
        """
        Set logging level.

        Parameters
        ----------

        level : {'info', 'warn', 'debug', 'error'}
        """
        logging.basicConfig()
        for module in ["hana_ml.ml_base", 'hana_ml.dataframe', 'hana_ml.algorithms.pal']:
            logger = logging.getLogger(module)
            Settings._set_log_level(logger, level)

class DataSets:
    """
    Load demo data.
    """
    @staticmethod
    def load_bank_data(connection,
                       schema=None,
                       chunk_size=10000,
                       force=False,
                       train_percentage=.50,
                       valid_percentage=.40,
                       test_percentage=.10,
                       full_tbl="DBM2_RFULL_TBL",
                       seed=1234,
                       url="https://raw.githubusercontent.com/SAP-samples/hana-ml-samples/main/Python-API/pal/datasets/bank-additional-full.csv"):
        """
        Load data.
        """
        if schema is None:
            schema = connection.get_current_schema()
        if connection.has_table(full_tbl, schema) and not force:
            print("Table {} exists.".format(full_tbl))
            full_df = connection.table(full_tbl, schema)
        else:
            data = pd.io.parsers.read_csv(url,
                                          header=None,
                                          names=['AGE',
                                                 'JOB',
                                                 'MARITAL',
                                                 'EDUCATION',
                                                 'DBM_DEFAULT',
                                                 'HOUSING',
                                                 'LOAN',
                                                 'CONTACT',
                                                 'DBM_MONTH',
                                                 'DAY_OF_WEEK',
                                                 'DURATION',
                                                 'CAMPAIGN',
                                                 'PDAYS',
                                                 'PREVIOUS',
                                                 'POUTCOME',
                                                 'EMP_VAR_RATE',
                                                 'CONS_PRICE_IDX',
                                                 'CONS_CONF_IDX',
                                                 'EURIBOR3M',
                                                 'NREMPLOYED',
                                                 'LABEL'])
            data.insert(0, "ID", range(0, len(data)))
            data.set_index("ID")
            full_df = create_dataframe_from_pandas(connection,
                                                   pandas_df=data,
                                                   table_name=full_tbl,
                                                   schema=schema,
                                                   force=force,
                                                   chunk_size=chunk_size)
        train_df, test_df, valid_df = train_test_val_split(full_df,
                                                           id_column="ID",
                                                           random_seed=seed,
                                                           partition_method='random',
                                                           training_percentage=train_percentage,
                                                           testing_percentage=test_percentage,
                                                           validation_percentage=valid_percentage)
        return full_df, train_df, valid_df, test_df

    @staticmethod
    def load_titanic_data(connection,
                          schema=None,
                          chunk_size=10000,
                          force=False,
                          train_percentage=.50,
                          valid_percentage=.40,
                          test_percentage=.10,
                          full_tbl="TITANIC_FULL_TBL",
                          seed=1234,
                          url="https://raw.githubusercontent.com/SAP-samples/hana-ml-samples/main/Python-API/pal/datasets/titanic-full.csv"):
        """
        Load data.
        """
        if schema is None:
            schema = connection.get_current_schema()
        if connection.has_table(full_tbl, schema) and not force:
            print("Table {} exists.".format(full_tbl))
            full_df = connection.table(full_tbl, schema)
        else:
            data = pd.io.parsers.read_csv(url,
                                          header=None,
                                          names=['PASSENGER_ID',
                                                 'PCLASS',
                                                 'NAME',
                                                 'SEX',
                                                 'AGE',
                                                 'SIBSP',
                                                 'PARCH',
                                                 'TICKET',
                                                 'FARE',
                                                 'CABIN',
                                                 'EMBARKED',
                                                 'SURVIVED'])
            full_df = create_dataframe_from_pandas(connection,
                                                   pandas_df=data,
                                                   table_name=full_tbl,
                                                   schema=schema,
                                                   force=force,
                                                   chunk_size=chunk_size)
        train_df, test_df, valid_df = train_test_val_split(full_df,
                                                           id_column="PASSENGER_ID",
                                                           random_seed=seed,
                                                           partition_method='random',
                                                           training_percentage=train_percentage,
                                                           testing_percentage=test_percentage,
                                                           validation_percentage=valid_percentage)
        return full_df, train_df, valid_df, test_df

    @staticmethod
    def load_walmart_data(connection,
                          schema=None,
                          chunk_size=10000,
                          force=False,
                          full_tbl="WALMART_TRAIN_TBL",
                          url="https://raw.githubusercontent.com/SAP-samples/hana-ml-samples/main/Python-API/pal/datasets/walmart-train.csv"):
        """
        Load data.
        """
        if schema is None:
            schema = connection.get_current_schema()
        if connection.has_table(full_tbl, schema) and not force:
            print("Table {} exists.".format(full_tbl))
            full_df = connection.table(full_tbl, schema)
        else:
            data = pd.io.parsers.read_csv(url,
                                          header=None,
                                          names=['ITEM_IDENTIFIER',
                                                 'ITEM_WEIGHT',
                                                 'ITEM_FAT_CONTENT',
                                                 'ITEM_VISIBILITY',
                                                 'ITEM_TYPE',
                                                 'ITEM_MRP',
                                                 'OUTLET_IDENTIFIER',
                                                 'OUTLET_ESTABLISHMENT_YEAR',
                                                 'OUTLET_SIZE',
                                                 'OUTLET_LOCATION_IDENTIFIER',
                                                 'OUTLET_TYPE',
                                                 'ITEM_OUTLET_SALES'])
            full_df = create_dataframe_from_pandas(connection,
                                                   pandas_df=data,
                                                   table_name=full_tbl,
                                                   schema=schema,
                                                   force=force,
                                                   chunk_size=chunk_size)
        return full_df

    @staticmethod
    def load_iris_data(connection,
                       schema=None,
                       chunk_size=10000,
                       force=False,
                       train_percentage=.50,
                       valid_percentage=.40,
                       test_percentage=.10,
                       full_tbl="IRIS_DATA_FULL_TBL",
                       seed=1234,
                       url="https://raw.githubusercontent.com/SAP-samples/hana-ml-samples/main/Python-API/pal/datasets/iris.csv"):
        """
        Load data.
        """
        if schema is None:
            schema = connection.get_current_schema()
        if connection.has_table(full_tbl, schema) and not force:
            print("Table {} exists.".format(full_tbl))
            full_df = connection.table(full_tbl, schema)
        else:
            data = pd.io.parsers.read_csv(url,
                                          header=None,
                                          names=['SEPALLENGTHCM',
                                                 'SEPALWIDTHCM',
                                                 'PETALLENGTHCM',
                                                 'PETALWIDTHCM',
                                                 'SPECIES'])
            data.insert(0, "ID", range(0, len(data)))
            data.set_index("ID")
            full_df = create_dataframe_from_pandas(connection,
                                                   pandas_df=data,
                                                   table_name=full_tbl,
                                                   schema=schema,
                                                   force=force,
                                                   chunk_size=chunk_size)
        train_df, test_df, valid_df = train_test_val_split(full_df,
                                                           id_column="ID",
                                                           random_seed=seed,
                                                           partition_method='random',
                                                           training_percentage=train_percentage,
                                                           testing_percentage=test_percentage,
                                                           validation_percentage=valid_percentage)
        return full_df, train_df, valid_df, test_df

    @staticmethod
    def load_boston_housing_data(connection,
                                 schema=None,
                                 chunk_size=10000,
                                 force=False,
                                 train_percentage=.50,
                                 valid_percentage=.40,
                                 test_percentage=.10,
                                 full_tbl="BOSTON_HOUSING_PRICES",
                                 seed=1234,
                                 url="https://raw.githubusercontent.com/SAP-samples/hana-ml-samples/main/Python-API/pal/datasets/boston-house-prices.csv"):
        """
        Load data.
        """
        if schema is None:
            schema = connection.get_current_schema()
        if connection.has_table(full_tbl, schema) and not force:
            print("Table {} exists.".format(full_tbl))
            full_df = connection.table(full_tbl, schema)
        else:
            data = pd.io.parsers.read_csv(url,
                                          header=None,
                                          names=["CRIM",
                                                 "ZN",
                                                 "INDUS",
                                                 "CHAS",
                                                 "NOX",
                                                 "RM",
                                                 "AGE",
                                                 "DIS",
                                                 "RAD",
                                                 "TAX",
                                                 "PTRATIO",
                                                 "BLACK",
                                                 "LSTAT",
                                                 "MEDV",
                                                 "ID"])
            full_df = create_dataframe_from_pandas(connection,
                                                   pandas_df=data,
                                                   table_name=full_tbl,
                                                   schema=schema,
                                                   force=force,
                                                   chunk_size=chunk_size)
        train_df, test_df, valid_df = train_test_val_split(full_df,
                                                           id_column="ID",
                                                           random_seed=seed,
                                                           partition_method='random',
                                                           training_percentage=train_percentage,
                                                           testing_percentage=test_percentage,
                                                           validation_percentage=valid_percentage)
        return full_df, train_df, valid_df, test_df

    @staticmethod
    def load_flight_data(connection,
                         schema=None,
                         chunk_size=10000,
                         force=False,
                         train_percentage=.50,
                         valid_percentage=.40,
                         test_percentage=.10,
                         full_tbl="FLIGHT_DATA_FULL_TBL",
                         seed=1234,
                         url="https://raw.githubusercontent.com/SAP-samples/hana-ml-samples/main/Python-API/pal/datasets/flight.csv"):
        """
        Load data.
        """
        if schema is None:
            schema = connection.get_current_schema()
        if connection.has_table(full_tbl, schema) and not force:
            print("Table {} exists.".format(full_tbl))
            full_df = connection.table(full_tbl, schema)
        else:
            data = pd.io.parsers.read_csv(url,
                                          header=None,
                                          names=['YEAR',
                                                 'MONTH',
                                                 'DAY',
                                                 'DAY_OF_WEEK',
                                                 'AIRLINE',
                                                 'FLIGHT_NUMBER',
                                                 'TAIL_NUMBER',
                                                 'ORIGIN_AIRPORT',
                                                 'DESTINATION_AIRPORT',
                                                 'SCHEDULED_DEPARTURE',
                                                 'DEPARTURE_TIME',
                                                 'DEPARTURE_DELAY',
                                                 'TAXI_OUT',
                                                 'WHEELS_OFF',
                                                 'SCHEDULED_TIME',
                                                 'ELAPSED_TIME',
                                                 'AIR_TIME',
                                                 'DISTANCE',
                                                 'WHEELS_ON',
                                                 'TAXI_IN',
                                                 'SCHEDULED_ARRIVAL',
                                                 'ARRIVAL_TIME',
                                                 'ARRIVAL_DELAY',
                                                 'DIVERTED',
                                                 'CANCELLED',
                                                 'CANCELLATION_REASON',
                                                 'AIR_SYSTEM_DELAY',
                                                 'SECURITY_DELAY',
                                                 'AIRLINE_DELAY',
                                                 'LATE_AIRCRAFT_DELAY',
                                                 'WEATHER_DELAY'])
            full_df = create_dataframe_from_pandas(connection,
                                                   pandas_df=data,
                                                   table_name=full_tbl,
                                                   schema=schema,
                                                   force=force,
                                                   chunk_size=chunk_size)
        train_df, test_df, valid_df = train_test_val_split(full_df.add_id("ID"),
                                                           id_column="ID",
                                                           random_seed=seed,
                                                           partition_method='random',
                                                           training_percentage=train_percentage,
                                                           testing_percentage=test_percentage,
                                                           validation_percentage=valid_percentage)
        return full_df.deselect("ID"), train_df.deselect("ID"), valid_df.deselect("ID"), test_df.deselect("ID")

    @staticmethod
    def load_adult_data(connection,
                        schema=None,
                        chunk_size=10000,
                        force=False,
                        train_percentage=.50,
                        valid_percentage=.40,
                        test_percentage=.10,
                        full_tbl="ADULT_DATA_FULL_TBL",
                        seed=1234,
                        url="https://raw.githubusercontent.com/SAP-samples/hana-ml-samples/main/Python-API/pal/datasets/adult.csv"):
        """
        Load data.
        """
        if schema is None:
            schema = connection.get_current_schema()
        if connection.has_table(full_tbl, schema) and not force:
            print("Table {} exists.".format(full_tbl))
            full_df = connection.table(full_tbl, schema)
        else:
            data = pd.io.parsers.read_csv(url,
                                          header=None,
                                          names=['AGE',
                                                 'WORKCLASS',
                                                 'FNLWGT',
                                                 'EDUCATION',
                                                 'EDUCATIONNUM',
                                                 'MARITALSTATUS',
                                                 'OCCUPATION',
                                                 'RELATIONSHIP',
                                                 'RACE',
                                                 'SEX',
                                                 'CAPITALGAIN',
                                                 'CAPITALLOSS',
                                                 'HOURSPERWEEK',
                                                 'NATIVECOUNTRY',
                                                 'INCOME'])
            data.insert(0, "ID", range(0, len(data)))
            data.set_index("ID")
            full_df = create_dataframe_from_pandas(connection,
                                                   pandas_df=data,
                                                   table_name=full_tbl,
                                                   schema=schema,
                                                   force=force,
                                                   chunk_size=chunk_size)
        train_df, test_df, valid_df = train_test_val_split(full_df,
                                                           id_column="ID",
                                                           random_seed=seed,
                                                           partition_method='random',
                                                           training_percentage=train_percentage,
                                                           testing_percentage=test_percentage,
                                                           validation_percentage=valid_percentage)
        return full_df, train_df, valid_df, test_df

    @staticmethod
    def load_diabetes_data(connection,
                           schema=None,
                           chunk_size=10000,
                           force=False,
                           train_percentage=.50,
                           valid_percentage=.40,
                           test_percentage=.10,
                           full_tbl="PIMA_INDIANS_DIABETES_TBL",
                           seed=1234,
                           url="https://raw.githubusercontent.com/SAP-samples/hana-ml-samples/main/Python-API/pal/datasets/pima-indians-diabetes.csv"):
        """
        Load data.
        """
        if schema is None:
            schema = connection.get_current_schema()
        if connection.has_table(full_tbl, schema) and not force:
            print("Table {} exists.".format(full_tbl))
            full_df = connection.table(full_tbl, schema)
        else:
            data = pd.io.parsers.read_csv(url,
                                          header=None,
                                          names=['PREGNANCIES',
                                                 'GLUCOSE',
                                                 'SKINTHICKNESS',
                                                 'INSULIN',
                                                 'BMI',
                                                 'AGE',
                                                 'CLASS'])
            data.insert(0, "ID", range(0, len(data)))
            data.set_index("ID")
            full_df = create_dataframe_from_pandas(connection,
                                                   pandas_df=data,
                                                   table_name=full_tbl,
                                                   schema=schema,
                                                   force=force,
                                                   chunk_size=chunk_size)
        train_df, test_df, valid_df = train_test_val_split(full_df,
                                                           id_column="ID",
                                                           random_seed=seed,
                                                           partition_method='random',
                                                           training_percentage=train_percentage,
                                                           testing_percentage=test_percentage,
                                                           validation_percentage=valid_percentage)
        return full_df, train_df, valid_df, test_df

    @staticmethod
    def load_shampoo_data(connection,
                          schema=None,
                          chunk_size=10000,
                          force=False,
                          full_tbl="SHAMPOO_SALES_DATA_TBL",
                          url="https://raw.githubusercontent.com/SAP-samples/hana-ml-samples/main/Python-API/pal/datasets/shampoo.csv"):
        """
        Load data.
        """
        if schema is None:
            schema = connection.get_current_schema()
        if connection.has_table(full_tbl, schema) and not force:
            print("Table {} exists.".format(full_tbl))
            full_df = connection.table(full_tbl, schema)
        else:
            data = pd.io.parsers.read_csv(url,
                                          header=None,
                                          names=['ID',
                                                 'SALES'])
            full_df = create_dataframe_from_pandas(connection,
                                                   pandas_df=data,
                                                   table_name=full_tbl,
                                                   schema=schema,
                                                   force=force,
                                                   chunk_size=chunk_size)
        return full_df

    @staticmethod
    def load_apriori_data(connection,
                          schema=None,
                          chunk_size=10000,
                          force=False,
                          full_tbl="PAL_APRIORI_TRANS_TBL",
                          url="https://raw.githubusercontent.com/SAP-samples/hana-ml-samples/main/Python-API/pal/datasets/apriori_item_data.csv"):
        """
        Load data.
        """
        if schema is None:
            schema = connection.get_current_schema()
        if connection.has_table(full_tbl, schema) and not force:
            print("Table {} exists.".format(full_tbl))
            full_df = connection.table(full_tbl, schema)
        else:
            data = pd.io.parsers.read_csv(url,
                                          header=None,
                                          names=['CUSTOMER',
                                                 'ITEM'])
            full_df = create_dataframe_from_pandas(connection,
                                                   pandas_df=data,
                                                   table_name=full_tbl,
                                                   schema=schema,
                                                   force=force,
                                                   chunk_size=chunk_size)
        return full_df


    @staticmethod
    def load_spm_data(connection,
                      schema=None,
                      chunk_size=10000,
                      force=False,
                      full_tbl="PAL_SPM_DATA_TBL",
                      url="https://raw.githubusercontent.com/SAP-samples/hana-ml-samples/main/Python-API/pal/datasets/spm_data.csv"):
        """
        Load data.
        """
        if schema is None:
            schema = connection.get_current_schema()
        if connection.has_table(full_tbl, schema) and not force:
            print("Table {} exists.".format(full_tbl))
            full_df = connection.table(full_tbl, schema)
        else:
            data = pd.io.parsers.read_csv(url,
                                          header=None,
                                          names=['CUSTID',
                                                 'TRANSID',
                                                 'ITEMS'])
            full_df = create_dataframe_from_pandas(connection,
                                                   pandas_df=data,
                                                   table_name=full_tbl,
                                                   schema=schema,
                                                   force=force,
                                                   chunk_size=chunk_size)
        return full_df

    @staticmethod
    def load_covid_data(connection,
                        schema=None,
                        chunk_size=10000,
                        force=False,
                        full_tbl="PAL_COVID_DATA_TBL",
                        url="https://raw.githubusercontent.com/SAP-samples/hana-ml-samples/main/Python-API/pal/datasets/worldwide-aggregated.csv"):
        """
        Load data.
        """
        if schema is None:
            schema = connection.get_current_schema()
        if connection.has_table(full_tbl, schema) and not force:
            print("Table {} exists.".format(full_tbl))
            full_df = connection.table(full_tbl, schema)
        else:
            data = pd.io.parsers.read_csv(url)
            full_df = create_dataframe_from_pandas(connection,
                                                   pandas_df=data,
                                                   table_name=full_tbl,
                                                   schema=schema,
                                                   force=force,
                                                   chunk_size=chunk_size)
        return full_df

    @staticmethod
    def load_bike_data(connection,
                       schema=None,
                       chunk_size=10000,
                       force=False,
                       full_tbl="PAL_BIKE_DATA_TBL",
                       url="https://raw.githubusercontent.com/SAP-samples/hana-ml-samples/main/Python-API/pal/datasets/bike.csv"):
        """
        Load data.
        """
        if schema is None:
            schema = connection.get_current_schema()
        if connection.has_table(full_tbl, schema) and not force:
            print("Table {} exists.".format(full_tbl))
            full_df = connection.table(full_tbl, schema)
        else:
            data = pd.io.parsers.read_csv(url)
            full_df = create_dataframe_from_pandas(connection,
                                                   pandas_df=data,
                                                   table_name=full_tbl,
                                                   schema=schema,
                                                   force=force,
                                                   chunk_size=chunk_size)
        return full_df

    @staticmethod
    def load_cervical_data(connection,
                           schema=None,
                           chunk_size=10000,
                           force=False,
                           full_tbl="PAL_CERVICAL_DATA_TBL",
                           url="https://raw.githubusercontent.com/SAP-samples/hana-ml-samples/main/Python-API/pal/datasets/cervical.csv"):
        """
        Load data.
        """
        if schema is None:
            schema = connection.get_current_schema()
        if connection.has_table(full_tbl, schema) and not force:
            print("Table {} exists.".format(full_tbl))
            full_df = connection.table(full_tbl, schema)
        else:
            data = pd.io.parsers.read_csv(url)
            full_df = create_dataframe_from_pandas(connection,
                                                   pandas_df=data,
                                                   table_name=full_tbl,
                                                   schema=schema,
                                                   force=force,
                                                   chunk_size=chunk_size)
        return full_df

def _get_type_code(data):
    """
    Return the code to indicate data type.
    - 0: int
    - 1: float
    - 2: str
    - 3: list(tuple) of int
    - 4: list(tuple) of float
    - 5: list(tuple) of str
    """
    code = None
    if isinstance(data, (list, tuple)):
        code = 3
        for dat in data:
            if isinstance(dat, float) and code == 3:
                code = 4
            if isinstance(dat, str) and code < 5:
                code = 5
    elif isinstance(data, int):
        code = 0
    elif isinstance(data, float):
        code = 1
    else:
        code = 2
    return code

class _UniversalAPI(PALBase):
    """
    For debugging and testing new PAL functions.
    """
    def __init__(self):
        super(_UniversalAPI, self).__init__()
        self.results_ = {}
    @trace_sql
    def call_func(self,
                  pal_func_name,
                  data,
                  num_outputs,
                  **kwargs):
        """
        Call PAL function in universal api. Only for debugging and testing purpose.
        """
        is_data_list = isinstance(data, (list, tuple))
        if is_data_list:
            conn = data[0].connection_context
        else:
            conn = data.connection_context
        unique_id = str(uuid.uuid1()).replace('-', '_').upper()
        outputs = ['#PAL_UNIVERSAL_API_{}_{}_{}'.format(tbl, self.id, unique_id)
                   for tbl in range(0, num_outputs)]
        param_rows = []
        for key, val in {**kwargs}.items():
            code = _get_type_code(val)
            if code > 2:
                for vv in val:
                    if code == 3:
                        param_rows.extend([(key.upper(), vv, None, None)])
                    if code == 4:
                        param_rows.extend([(key.upper(), None, vv, None)])
                    if code == 5:
                        param_rows.extend([(key.upper(), None, None, vv)])
            else:
                if code == 0:
                    param_rows.extend([(key.upper(), val, None, None)])
                if code == 1:
                    param_rows.extend([(key.upper(), None, val, None)])
                if code == 2:
                    param_rows.extend([(key.upper(), None, None, val)])

        try:
            if is_data_list:
                self._call_pal_auto(conn,
                                    pal_func_name,
                                    *data,
                                    ParameterTable().with_data(param_rows),
                                    *outputs)
            else:
                self._call_pal_auto(conn,
                                    pal_func_name,
                                    data,
                                    ParameterTable().with_data(param_rows),
                                    *outputs)
        except dbapi.Error as db_err:
            mylogger.error(str(db_err))
            try_drop(conn, outputs)
            raise
        except Exception as db_err:
            mylogger.error(str(db_err))
            try_drop(conn, outputs)
            raise
        self.results_[pal_func_name] = []
        for output in outputs:
            self.results_[pal_func_name].append(conn.table(output))

def _get_api_params(ml_object):
    return list(ml_object.hanaml_parameters.keys())

def _get_batch_api_params(object_list, tabular_form=False):
    result = {}
    for ml_object in object_list:
        result[type(ml_object).__name__] = _get_api_params(ml_object)
    if tabular_form:
        api_params = []
        api_class = []
        for key, val in result.items():
            api_params = api_params + val
            api_class = api_class + [key] * len(val)
        return pd.DataFrame({"API_PARAMS": api_params, "API_CLASS": api_class})
    return result

def mlflow_autologging(logtype):
    """
    MLFlow autologging decorator.

    Parameters
    ----------
    logtype : str, optional
        Use to identify the to-be-logged functions.
    """
    def logging_decorator(func):
        @wraps(func)
        def wrapped_function(*args, **kwargs):
            if logtype == 'pal_fit':
                try:
                    model = func(*args, **kwargs)
                except ValueError as err:
                    mylogger.error(err)
                    raise
                if model._is_autologging:
                    log_params = {}
                    for col, val in model.hanaml_parameters.items():
                        if val:
                            if isinstance(val, dict):
                                for ccol, vval in val.items():
                                    log_params[ccol] = vval
                            else:
                                log_params[col] = val
                    for col, val in model.hanaml_fit_params.items():
                        if val:
                            if isinstance(val, dict):
                                for ccol, vval in val.items():
                                    log_params[ccol] = vval
                            else:
                                log_params[col] = val
                    mlflow.log_params(log_params)
                    log_metrics = {}
                    if "Unified" in model.__str__():
                        for _, row in model.statistics_.collect().iterrows():
                            try:
                                row_1 = float(row[1])
                                log_metrics[row[0]] = row_1
                            except:
                                continue
                    if "Automatic" in model.__str__():
                        try:
                            log_metrics.update(json.loads(model.best_pipeline_.collect().iat[0, 2]))
                        except:
                            pass
                    mlflow.log_metrics(log_metrics)
                    #create mlflow model storage
                    flavor_info = {}
                    input_df = None
                    if len(args) > 1:
                        if isinstance(args[1], DataFrame):
                            input_df = args[1]
                        else:
                            input_df = kwargs.get('data', None)
                    else:
                        input_df = kwargs.get('data', None)
                    if input_df is None:
                        raise ValueError("Input data is required for logging.")
                    conn = input_df.connection_context
                    address = conn.address
                    port = conn.port
                    if model._autologging_model_storage_schema:
                        schema = model._autologging_model_storage_schema
                    else:
                        schema = conn.get_current_schema()
                    if model._autologging_model_storage_meta:
                        meta = model._autologging_model_storage_meta
                    else:
                        meta = _HANAML_MLFLOW_MODEL_STORAGE
                    is_exported = False
                    if hasattr(model, 'is_exported'):
                        if model.is_exported:
                            is_exported = True
                    model_storage = ModelStorage(connection_context=conn,
                                                 schema=schema,
                                                 meta=meta)
                    if model.name is None:
                        model.name = type(model).__name__ + str(uuid.uuid1().hex)
                    if model.version is None:
                        model.version = model_storage._get_new_version_no(model.name)
                    if 'label' in {**kwargs}:
                        label = {**kwargs}['label']
                        if label is None:
                            label = input_df.columns[-1]
                    else:
                        label = input_df.columns[-1]
                    input_schema_list = []
                    output_schema_list = []
                    for ky, val in input_df.deselect(label).get_table_structure().items():
                        input_schema_list.append(ColSpec(_type_map(val), ky))
                    for ky, val in input_df.select(label).get_table_structure().items():
                        output_schema_list.append(ColSpec(_type_map(val), ky))
                    if not is_exported:
                        model_storage.save_model(model)
                    flavor_info['python_version'] = sys.version
                    flavor_info['address'] = address
                    flavor_info['port'] = port
                    flavor_info['schema'] = schema
                    flavor_info['meta'] = meta
                    flavor_info['name'] = model.name
                    flavor_info['version'] = model.version
                    flavor_info['hana_ml_version'] = hana_ml.__version__
                    flavor_info['input_schema'] = Schema(input_schema_list)
                    flavor_info['output_schema'] = Schema(output_schema_list)

                    _mlflow_model.log("model", flavor_info=flavor_info, model_storage=model_storage, model=model)
                return model
            return func(*args, **kwargs)
        return wrapped_function
    return logging_decorator

try:
    class _mlflow_model(Model):
        @classmethod
        def log(cls,
                artifact_path,
                flavor_info={},
                registered_model_name=None,
                await_registration_for=DEFAULT_AWAIT_MAX_SLEEP_SECONDS,
                model_storage=None,
                model=None,
                **kwargs):
            with TempDir() as tmp:
                local_path = Path(tmp.path("model")).as_posix()
                run_id = mlflow.tracking.fluent._get_or_start_run().info.run_id
                mlflow_model = cls(artifact_path=artifact_path, run_id=run_id)
                if os.path.exists(local_path):
                    raise MlflowException(
                        message="Path '{}' already exists".format(local_path), error_code=RESOURCE_ALREADY_EXISTS
                    )
                os.makedirs(local_path, exist_ok=True)
                if mlflow_model is None:
                    mlflow_model = Model()
                if 'python_version' not in flavor_info:
                    flavor_info['python_version'] = None
                if 'address' not in flavor_info:
                    flavor_info['address'] = None
                if 'port' not in flavor_info:
                    flavor_info['port'] = None
                if 'schema' not in flavor_info:
                    flavor_info['schema'] = None
                if 'meta' not in flavor_info:
                    flavor_info['meta'] = None
                if 'name' not in flavor_info:
                    flavor_info['name'] = None
                if 'version' not in flavor_info:
                    flavor_info['version'] = None
                if 'hana_ml_version' not in flavor_info:
                    flavor_info['hana_ml_version'] = None
                is_exported = False
                if hasattr(model, 'is_exported'):
                    if model.is_exported:
                        is_exported = True
                if not is_exported:
                    mlflow_model.__dict__.update({ "flavors": {
                                                        'python_function': {
                                                        'loader_module': 'hana_ml.model_storage.ModelStorage.load_mlflow_model',
                                                        'python_version': flavor_info['python_version']},
                                                        'hana_ml': {
                                                            'model_storage': {
                                                            'address': flavor_info['address'],
                                                            'port': flavor_info['port'],
                                                            'schema':flavor_info['schema'],
                                                            'meta': flavor_info['meta'],
                                                            'name' : flavor_info['name'],
                                                            'version' : flavor_info['version'] },
                                                            'hana_ml_version': flavor_info['hana_ml_version']}}
                                                })
                else:
                    mlflow_model.__dict__.update({ "flavors": {
                                                        'python_function': {
                                                        'loader_module': 'hana_ml.model_storage.ModelStorage.load_mlflow_model',
                                                        'python_version': flavor_info['python_version']},
                                                        'hana_ml': {
                                                            'model_storage': {
                                                            'is_exported': True,
                                                            'name' : flavor_info['name'],
                                                            'version' : flavor_info['version'] },
                                                            'hana_ml_version': flavor_info['hana_ml_version']}}
                                                })
                if 'input_schema' not in flavor_info:
                    flavor_info['input_schema'] = Schema([])
                if 'output_schema' not in flavor_info:
                    flavor_info['output_schema'] = Schema([])
                signature = ModelSignature(inputs=flavor_info['input_schema'], outputs=flavor_info['output_schema'])
                mlflow_model.signature = signature
                mlflow_model.save(Path(os.path.join(local_path, MLMODEL_FILE_NAME)).as_posix())
                if is_exported:
                    model_storage.save_model_to_files(model, local_path)
                setattr(model, 'mlflow_model_info', mlflow_model.__dict__)
                mlflow.tracking.fluent.log_artifacts(local_path, artifact_path)
                try:
                    mlflow.tracking.fluent._record_logged_model(mlflow_model)
                except MlflowException:
                    mylogger.warning(_LOG_MODEL_METADATA_WARNING_TEMPLATE, mlflow.get_artifact_uri())
                if hasattr(model, 'registered_model_name'):
                    if model.registered_model_name:
                        registered_model_name = model.registered_model_name
                if registered_model_name is not None:
                    run_id = mlflow.tracking.fluent.active_run().info.run_id
                    mlflow.register_model(
                        "runs:/%s/%s" % (run_id, artifact_path),
                        registered_model_name,
                        await_registration_for=await_registration_for,
                    )
            return mlflow_model.get_model_info()
except:
    pass

def _auto_sql_content_log_cleanup(connection_context, execution_id=None, is_force=True, schema="PAL_CONTENT"):
    if execution_id:
        connection_context.execute_sql("DO BEGIN\nCALL {}.REMOVE_AUTOML_LOG('{}', TO_BOOLEAN({}), info);\nEND;".format(schema, execution_id, 1 if is_force else 0))
    else:
        execution_ids = connection_context.sql(f"SELECT EXECUTION_ID FROM {schema}.AUTOML_LOG ").distinct().collect()["EXECUTION_ID"].values.tolist()
        for vval in execution_ids:
            connection_context.execute_sql("DO BEGIN\nCALL {}.REMOVE_AUTOML_LOG('{}', TO_BOOLEAN({}), info);\nEND;".format(schema, vval, 1 if is_force else 0))

def _afl_progress_log_cleanup(connection_context, execution_id):
    unique_id = str(uuid.uuid1()).replace('-', '_').upper()
    info_tbl  = f'#PAL_CLEANUP_PROGRESS_LOG_INFO_TBL_{unique_id}'
    param_rows = [('PROGRESS_INDICATOR_ID', None, None, execution_id)]
    try:
        call_pal_auto_with_hint(connection_context,
            None,
            'PAL_PROGRESS_INDICATOR_CLEANUP',
            ParameterTable().with_data(param_rows),
            info_tbl)
    except dbapi.Error as db_err:
        raise Exception(str(db_err))
    except Exception as db_err:
        raise Exception(str(db_err))
    finally:
        try_drop(connection_context, info_tbl)

def list_afl_state(connection_context):
    return connection_context.sql("SELECT * FROM SYS.M_AFL_STATES")

def delete_afl_state(connection_context, state_id):
    unique_id = str(uuid.uuid1()).replace('-', '_').upper()
    param_rows = []
    state = connection_context.sql("SELECT 'STATE_ID' NAME, '{}' VALUE FROM     DUMMY UNION\
                                    SELECT 'HINT' NAME, NULL VALUE FROM DUMMY UNION\
                                    SELECT 'HOST' NAME, NULL VALUE FROM DUMMY UNION\
                                    SELECT 'PORT' NAME, NULL VALUE FROM DUMMY".format(state_id))
    deletestate_tbl = '#PAL_DELETESTATE_TBL_{}'.format(unique_id)
    try:
        call_pal_auto_with_hint(connection_context,
                                None,
                                'PAL_DELETE_MODEL_STATE',
                                state,
                                ParameterTable().with_data(param_rows),
                                deletestate_tbl)
    except dbapi.Error as db_err:
        try_drop(connection_context, deletestate_tbl)
        raise Exception(str(db_err))
    except Exception as db_err:
        try_drop(connection_context, deletestate_tbl)
        raise Exception(str(db_err))
    return connection_context.table(deletestate_tbl)

def delete_afl_state_by_description(connection_context, description):
    states = list_afl_state(connection_context).filter("DESCRIPTION='{}'".format(description)).collect()
    for _, row in states.iterrows():
        delete_afl_state(connection_context, row["STATE_ID"])
