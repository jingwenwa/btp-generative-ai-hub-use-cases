"""
This module represents a database query as a dataframe.
Most operations are designed to not bring data back from the database
unless explicitly requested.

The following classes and functions are available:

    * :class:`ConnectionContext`
    * :class:`DataFrame`
    * :func:`quotename`
    * :func:`create_dataframe_from_pandas`
    * :func:`create_dataframe_from_spark`
    * :func:`create_dataframe_from_shapefile`
    * :func:`read_pickle`
    * :func:`melt`
    * :func:`import_csv_from`
"""
#pylint: disable=too-many-lines
#pylint: disable=line-too-long
#pylint: disable=relative-beyond-top-level
#pylint: disable=fixme
#pylint: disable=too-many-locals
#pylint: disable=too-many-branches
#pylint: disable=deprecated-lambda
#pylint: disable=too-many-arguments
#pylint: disable=too-many-format-args
#pylint: disable=too-many-statements
#pylint: disable=bare-except
#pylint: disable=broad-except
#pylint: disable=singleton-comparison
#pylint: disable=deprecated-method
#pylint: disable=protected-access
#pylint: disable=too-many-nested-blocks
#pylint: disable=redefined-outer-name
#pylint: disable=no-self-use
#pylint: disable=consider-using-f-string
#pylint: disable=consider-iterating-dictionary
#pylint: disable=duplicate-code
#pylint: disable=too-many-public-methods
#pylint: disable=too-many-instance-attributes
#pylint: disable=too-many-return-statements
#pylint: disable=cell-var-from-loop
#pylint: disable=import-error
#pylint: disable=logging-format-interpolation
#pylint: disable=invalid-name
#pylint: disable=no-else-continue

import itertools
import logging
import sys
import uuid
import os
import math
import re
import json
from zipfile import ZipFile
import getpass
import numpy as np
import pandas as pd
from hdbcli import dbapi
from tqdm import tqdm
from hana_ml.ml_base import try_drop
from hana_ml.algorithms.pal.sqlgen_ import _TEXT_TYPES, HolidayTable, ParameterTable, create_massive_params, create_params, safety_test, tabletype

try:
    from shapely import wkt
except ImportError as error:
    logging.getLogger(__name__).error("%s: %s", error.__class__.__name__, str(error))
    pass


from .ml_exceptions import BadSQLError
from .ml_base import _exist_hint, execute_logged, logged_without_execute
from .type_codes import get_type_code_map


TYPE_CODES = get_type_code_map()
logger = logging.getLogger(__name__) #pylint: disable=invalid-name

if sys.version_info.major == 2:
    #pylint: disable=undefined-variable
    _INTEGER_TYPES = (int, long)
    _STRING_TYPES = (str, unicode)
else:
    _INTEGER_TYPES = (int,)
    _STRING_TYPES = (str,)


def quotename(name):
    """
    Escapes a schema, table, or column name for use in SQL. hana_ml functions and methods that take schema, table, or column names
    already escape their input by default, but those that take SQL don't (and can't) perform escaping automatically.

    Parameters
    ----------
    name : str
        The schema, table, or column name.

    Returns
    -------
    str
        The escaped name. The string is surrounded in quotation marks,
        and existing quotation marks are escaped by doubling them.
    """

    return '"{}"'.format(name.replace('"', '""'))

def smart_quote(name):
    """
    Add quote to schema.xxx.

    Parameters
    ----------
    name : str
        The targe name to be quoted.
    """
    if '.' in name:
        schema, name_ = name.split('.', 1)
        if schema[0] != '"' or schema[-1] != '"':
            schema = quotename(schema)
        if name_[0] != '"' or name_[-1] != '"':
            name_ = quotename(name_)
        return "{}.{}".format(schema, name_)
    if name[0] == '"' and name[-1] == '"':
        return name
    return quotename(name)

def _univariate_analysis(data, cols=None):
    conn = data.connection_context
    if cols is None:
        cols = data.columns
    data_ = data.select(cols)
    unique_id = str(uuid.uuid1()).replace('-', '_').upper()
    tables = ['CONTINUOUS', 'CATEGORICAL']
    tables = ['#PAL_UNIVARIATE_{}_TBL_{}'.format(name, unique_id) for name in tables]

    param_rows = [('HAS_ID', 0, None, None)
                 ]
    try:
        _call_pal_auto_with_hint(conn,
                                 None,
                                 'PAL_UNIVARIATE_ANALYSIS',
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
    return conn.table(tables[0]), conn.table(tables[1])

def _data_summary(data, select_stat=None):
    conn_context = data.connection_context
    param_rows = [('CATEGORICAL_OUTPUT_FORMAT', 1, None, None),
                  ('HAS_ID', 0, None, None)]
    if select_stat:
        param_rows.extend([('SELECT_STAT_STATE', 1, None, None)])
        if isinstance(select_stat, str):
            select_stat = [select_stat]
        if isinstance(select_stat, (list, tuple)):
            param_rows.extend([('SELECT_STAT', None, None, var) for var in select_stat])
    unique_id = str(uuid.uuid1()).replace('-', '_').upper()
    tables = ['RESULT', 'CATEGORICAL']
    tables = ['#PAL_DATASUMMARY_{}_TBL_{}'.format(name, unique_id) for name in tables]
    try:
        _call_pal_auto_with_hint(conn_context,
                                 None,
                                 'PAL_UNIVARIATE_ANALYSIS',
                                 data,
                                 ParameterTable().with_data(param_rows),
                                 *tables)
    except dbapi.Error as db_err:
        logger.exception(str(db_err))
        try_drop(conn_context, tables)
        raise
    except Exception as db_err:
        logger.exception(str(db_err))
        try_drop(conn_context, tables)
        raise
    return conn_context.table(tables[0])

class ConnectionContext(object):
    """
    Represents a connection to a SAP HANA database instance.

    ConnectionContext includes methods for creating DataFrames from data
    on the SAP HANA. DataFrames are tied to a ConnectionContext, and are unusable
    once their ConnectionContext is closed.

    Parameters
    ----------
    Same as hdbcli.dbapi.connect.

        Please see the `online docs for hdbcli.dbapi.connect\
        <https://help.sap.com/docs/SAP_HANA_CLIENT/f1b440ded6144a54ada97ff95dac7adf/ee592e89dcce4480a99571a4ae7a702f.html?>`_
        for more details.

    Examples
    --------
    Querying data from SAP HANA into a Pandas DataFrame:

    >>> with ConnectionContext('address', port, 'user', 'password') as cc:
    ...     df = (cc.table('MY_TABLE', schema='MY_SCHEMA')
    ...             .filter('COL3 > 5')
    ...             .select('COL1', 'COL2'))
    ...     pandas_df = df.collect()

    The underlying hdbcli.dbapi.connect can be accessed if necessary:

    >>> with ConnectionContext('127.0.0.1', 30215, 'MLGUY', 'manager') as cc:
    ...     cc.connection.setclientinfo('SOMEKEY', 'somevalue')
    ...     df = cc.sql('some sql that needs that session variable')
    ...     ...

    Attributes
    ----------
    connection : hdbcli.dbapi.connect
        The underlying dbapi connection. Use this connection to run SQL directly,
        or to access connection methods like getclientinfo/setclientinfo.
    """

    def __init__(self,            #pylint: disable=too-many-arguments
                 address='',
                 port=0,
                 user='',
                 password=None,
                 autocommit=True,
                 packetsize=None,
                 userkey=None,
                 spatialtypes=1,
                 encrypt=None,
                 sslValidateCertificate=None,
                 pyodbc_connection=None,
                 abap_sql=False,
                 sslKeyStore=None,
                 vectoroutputtype="list",
                 **properties):
        self.address = address
        self.port = port
        self.userkey = userkey
        self.sslKeyStore = sslKeyStore
        key = None
        if 'key' in properties:
            key = properties.pop('key')
        if not pyodbc_connection:
            if password is None and ((userkey is None) and (key is None)):
                while True:
                    password = getpass.getpass("HANA DB User : %s Password : " % user)
                    if password is None:
                        password = ''
                        break
                    if password is not None:
                        break
        if password is None:
            password = ''

        if str(spatialtypes) != '1':
            logger.warning("With 'spatialtypes=%s', this connection does not support spatial features in the dataframe", spatialtypes)
        self.connection = None
        self.pyodbc_connection = pyodbc_connection
        if pyodbc_connection:
            import subprocess
            try:
                import pyodbc
            except BaseException as error:
                try:
                    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'pyodbc'])
                    import pyodbc
                except:
                    logging.getLogger(__name__).error("%s: %s", error.__class__.__name__, str(error))
                    pass
            self.connection = pyodbc.connect(pyodbc_connection)
        else:
            self.connection = dbapi.connect(
                address,
                port,
                user,
                password,
                autocommit=autocommit,
                packetsize=packetsize,
                userkey=userkey,
                spatialtypes=spatialtypes,
                encrypt=encrypt,
                sslValidateCertificate=sslValidateCertificate,
                sslKeyStore=sslKeyStore,
                vectoroutputtype=vectoroutputtype,
                **properties)
        self.properties = properties
        self._pal_check_passed = False
        self.sql_tracer = SqlTrace() # SQLTRACE
        self.last_execute_statement = None
        self._abap_sql = abap_sql
        self.disable_hana_execution = None#for param check in functions
        self.execute_statement = None

    def enable_abap_sql(self):
        """
        Enables ABAP SQL.
        """
        self._abap_sql = True

    def disable_abap_sql(self):
        """
        Disables ABAP SQL.
        """
        self._abap_sql = False

    def close(self):
        """
        Closes the existing connection to a SAP HANA database instance.

        Parameters
        ----------
        None

        Returns
        -------
        None

        Examples
        --------
        cc is a connection to a SAP HANA database instance.

        >>> cc.close()
        """

        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()

    def add_primary_key(self, table, columns, schema=None):
        """
        Adds primary key to the existing table.

        Parameters
        ----------
        table : str
            Table name.

        columns : str of list
            Columns to be primary key.

        schema : str, optional
            Schema name. If None, use the current schema.
        """

        if schema is None:
            table_reference = quotename(table)
        else:
            table_reference = '.'.join(map(quotename, (schema, table)))
        column_str = ''
        if isinstance(columns, str):
            column_str = columns
        else:
            column_str = ', '.join(columns)
        query = "ALTER TABLE {} ADD PRIMARY KEY ({})".format(table_reference, quotename(column_str))
        with self.connection.cursor() as cur:
            cur.execute(query)

    def drop_primary_key(self, table, schema=None):
        """
        Drops primary key from the existing table.

        Parameters
        ----------
        table : str
            Table name.

        column : str
            Column name.

        schema : str, optional
            Schema name. If None, use the current schema.
        """

        if schema is None:
            table_reference = quotename(table)
        else:
            table_reference = '.'.join(map(quotename, (schema, table)))
        query = "ALTER TABLE {} DROP PRIMARY KEY".format(table_reference)
        with self.connection.cursor() as cur:
            cur.execute(query)

    def add_auto_incremented_key(self, table, column, schema=None, starting_point=1, auto_increment=1):
        """
        Set the column as auto-incremented key.

        Parameters
        ----------
        table : str
            Table name.

        column : str
            Column name.

        schema : str, optional
            Schema name. If None, use the current schema.

        starting_point : int, optional
            The starting point value for auto increment.

            Defaults to 1.

        auto_increment : int, optional
            Auto increment value.

            Defaults to 1.
        """

        if schema is None:
            table_reference = quotename(table)
        else:
            table_reference = '.'.join(map(quotename, (schema, table)))
        #add new id column
        query = "ALTER TABLE {} ADD ({} {} GENERATED BY DEFAULT AS IDENTITY (START WITH {} INCREMENT BY {}))".format(table_reference, quotename(column), 'BIGINT', starting_point, auto_increment)
        with self.connection.cursor() as cur:
            cur.execute(query)

    def copy(self):
        """
        Returns a new connection context.
        """
        try:
            if self.userkey:
                conn = ConnectionContext(userkey=self.userkey)
            else:
                conn_config = str(self.connection).replace('<dbapi.Connection Connection object : ', '').replace('>', '').split(',')
                url = conn_config[0]
                port = conn_config[1]
                user = conn_config[2]
                password = conn_config[3]
                conn = ConnectionContext(url, port, user, password)
        except:
            if self.userkey:
                conn = ConnectionContext(userkey=self.userkey, encrypt='true', sslValidateCertificate='false')
            else:
                conn = ConnectionContext(url, port, user, password, encrypt='true', sslValidateCertificate='false')
        return conn

    def create_pse(self, pse_name, purpose=None):
        """
        Creates a new PSE.

        Parameters
        ----------
        pse_name : str
            PSE name.
        """
        query = "CREATE PSE {} ".format(quotename(pse_name))
        self.execute_sql(query)
        if purpose:
            set_pse_sql = "SET PSE {} PURPOSE {}".format(quotename(pse_name), purpose)
            self.execute_sql(set_pse_sql)

    def create_certificate(self, cert_name, certificate_in_pem_format):
        """
        Creates a new certificate.

        Parameters
        ----------
        cert_name : str
            Certificate name.

        certificate_in_pem_format : str
            Certificate in PEM format.
        """
        query = "CREATE CERTIFICATE {} FROM '{}'".format(quotename(cert_name), certificate_in_pem_format)
        self.execute_sql(query)

    def add_certificate_to_pse(self, pse_name, cert_name):
        """
        Adds a certificate to a PSE.

        Parameters
        ----------
        pse_name : str
            PSE name.

        cert_name : str
            Certificate name.
        """
        query = "ALTER PSE {} ADD CERTIFICATE {}".format(quotename(pse_name), quotename(cert_name))
        self.execute_sql(query)

    def create_schema(self, schema):
        """
        Creates a SAP HANA schema.

        Parameters
        ----------
        schema : str
            Schema name.
        """
        sql = "CREATE SCHEMA {}".format(quotename(schema))
        self.execute_sql(sql)

    def create_table(self, table, table_structure, schema=None, table_type='COLUMN', prop='', data_lake=False, data_lake_container='SYSRDL#CG'):
        """
        Creates a SAP HANA table.

        Parameters
        ----------
        table : str
            Table name.
        table_structure : dict
            SAP HANA table structure. {Column name: Column type, ...}
        schema : str, optional
            Schema name. If None, use the current schema.

            Defaults to None.
        table_type : str, optional
            Specify the table type.

            - 'COLUMN', by default.
            - 'ROW'.
            - 'TEMPORARY'.
        data_lake : bool, optional
            If True, create the data lake table by using SYSRDL#CG.REMOTE_EXECUTE().

            Defaults to False.
        data_lake_container : str, optional
            Name of the data lake container.

            Defaults to 'SYSRDL#CG'.

        Examples
        --------
        cc is a connection to a SAP HANA database instance.

        >>> cc.create_table(table='TEST_TBL', table_structure={'test_name': 'VARCHAR(50)'})
        """
        if schema is None:
            table_reference = quotename(table)
        else:
            table_reference = '.'.join(map(quotename, (schema, table)))
        t_type = ' '
        if table_type.upper() == 'COLUMN' and (not table.startswith("#")):
            t_type = ' COLUMN '
            if data_lake:
                t_type = ' '
        elif table_type.upper() == 'TEMPORARY' or table.startswith("#"):
            t_type = ' LOCAL TEMPORARY '
        else:
            t_type = ' ' + table_type + ' '
        query = 'CREATE{}TABLE {} ('.format(t_type, table_reference)
        for key, val in table_structure.items():
            query = query + "{} {}, ".format(quotename(key), val)
        query = query[:-2] + ") {}".format(prop)
        if data_lake:
            query = "CALL {}.REMOTE_EXECUTE ('{}')".format(data_lake_container, query)
        try:
            with self.connection.cursor() as cur:
                execute_logged(cur,
                               query,
                               self.sql_tracer,
                               self)
        except dbapi.Error as ct_err:
            logger.error("%s. Failed to execute `%s`", ct_err, query)
            raise
        except Exception as ct_err:
            logger.error("%s. Failed to execute `%s`", str(ct_err.args[1]), query)
            raise

    def create_remote_source(self, remote_source_name,
                             adapter,
                             configuration,
                             credential_type,
                             credential_string=None,
                             configuration_file=None,
                             check_after_create=False):
        r"""
        This function defines a remote external data source that can be connected to an SAP HANA Cloud instance.

        The mainly supported remote external data sources of this function are:

        - `SAP HANA Database <https://help.sap.com/docs/SAP_HANA_PLATFORM/6b94445c94ae495c83a19646e7c3fd56/f544a23e6a4d4fe3acb843ba5b0969f4.html>`_
        - `Data Lake Relational Engine <https://help.sap.com/docs/hana-cloud-database/sap-hana-cloud-sap-hana-database-data-access-guide/create-sap-hana-cloud-data-lake-relational-engine-remote-source>`_
        - `Data Lake Files Container <https://wiki.one.int.sap/wiki/pages/viewpage.action?pageId=3651667600>`_

        Parameters
        ----------
        remote_source_name : str
            Specifies the name of the remote source to be created.

        adapter : str, optional
            Specifies the adapters for communication between source
            database server and target SAP HANA cloud instance, where
            the source database server can be a SAP HANA database,
            a SAP HANA data lake relational engine, or
            a SAP HANA data lake files container.

        configuration : str
           Specifies the connection parameters for the given adapter.

        credential_type : str
            Specifies the credential type for the remote source.

        credential_string : str, optional
            Specifies the credential string following credential type.

            For example, if ``credential_type`` is 'PASSWORD', then ``credential_string`` should
            look like "USING 'user=<user>;password=<password>'", where <user> and <password> represent
            some valid user and password information of the source system. For another example, if
            ``credential_type`` is 'X509', then ``credential_string`` should look like
            "PSE '<pse_name>'", where <pse_name> is the name of the PSE credential used.

            For some credential type, like 'JWT' or 'KERBEROS', ``credential_string`` is not a neccessity
            and should not be specified.

            Defaults to None (i.e. no crendential string).

        configuration_file : str, optional
            Name of the configuration file for the specified adapter.

            The configuration file is not a necessity as always.

            Defaults to None (i.e. no configuration file).

        check_after_create : bool, optional
            Specifies whether or not to call the **CHECK_REMOTE_SOURCE** procedure to
            whether the remote source definition allows SAP HANA to connect to the
            remote system and authenticate the remote user.

            Defaults to False.

        Examples
        --------
        In general, the SQL statement for create remote source can be illustrated in the following note:

        .. note::

            CREATE REMOTE SOURCE "<remote_source_name>"
            ADAPTER "<adapter>"
            CONFIGURATION '<configuration_string>'
            WITH CREDENTIAL TYPE '<credential_type>' <credential_string>;

        Assume `cc` is the ConnectionContext object to a SAP HANA cloud instance, and the designated task
        is to create a remote source to a remote database server.

        The following example is for the case when the remote database server is a SAP HANA database server:

        >>> hana_database_url = 'xxxxxx.xxx.xxx'
        >>> user, password, port = 'Datalake_User', 'rf@DKFS~sf', 443
        >>> cc.create_remote_source(remote_source_name="MY_REMOTE_SOURCE",
        ...                         adapter="hanaodbc",
        ...                         configuration=f"Driver=libodbcHDB.so;ServerNode={hana_database_url}:{port};",
        ...                         credential_type="password",
        ...                         credential_string=f"USING 'user={user};password={password}'",
        ...                         check_after_create=True)


        The following example is for the case when the remote database server is a SAP HANA cloud,
        data lake relation engine server:

        >>> data_lake_url = "xxxx.xxx.xxxx"
        >>> certificate_string = '-----BEGIN CERTIFICATE-----MIID<omitted contents>Vbd4=-----END CERTIFICATE-----'
        >>> configuration = f"Driver=libdbodbc17_r.so;host={data_lake_url};" +\
        ... f"ENC=TLS(trusted_certificates={certificate_string};direct=yes"
        >>> user, password = "Datalake_User", "rdm&testF+5c"
        >>> cc.create_remote_source(remote_source_name="MY_REMOTE_SOURCE",
        ...                         adapter="iqodbc",
        ...                         configuration=configuration,
        ...                         credential_type="password",
        ...                         credential_string=f"USING 'user={user};password={password}'",
        ...                         check_after_create=True)

        The following example is for the case when the remote database server is a SAP HANA cloud,
        data lake files server:

        >>> data_lake_url = "xxxx.xxx.xxxx"
        >>> pse_name = 'DATA_LAKE_FILE_PSE' #the PSE needs to be created & configurated manually apriori
        >>> cc.create_remote_source(remote_source_name="MY_REMOTE_SOURCE",
        ...                         adapter="file",
        ...                         configuration=f"provider=hdlf;endpoint={data_lake_url};",
        ...                         credential_type="X509",
        ...                         credential_string=f"PSE {pse_name}",
        ...                         check_after_create=True)
        """

        sql = f"CREATE REMOTE SOURCE {quotename(remote_source_name)} ADAPTER {quotename(adapter)} " +\
              (f"CONFIGURATION FILE '{configuration_file}' " if configuration_file is not None else "") +\
              f"CONFIGURATION '{configuration}' " +\
              f"WITH CREDENTIAL TYPE '{credential_type.upper()}'"
        append_str = ";" if credential_string is None else (" " + credential_string + ";")
        sql_input = sql + append_str
        try:
            with self.connection.cursor() as cur:
                execute_logged(cur,
                               sql_input,
                               self.sql_tracer,
                               self)
        except dbapi.Error as cvt_err:
            logger.error("%s. Failed to execute `%s`", cvt_err, sql_input)
            raise
        except Exception as cvt_err:
            logger.error("%s. Failed to execute `%s`", str(cvt_err.args[1]), sql_input)
            raise
        if check_after_create:
            try:
                self.execute_sql(f"CALL CHECK_REMOTE_SOURCE('{remote_source_name}')")
            except dbapi.Error as cvt_err:
                logger.error("%s. Failed to check remote source.", cvt_err)
                raise
            except Exception as cvt_err:
                logger.error("%s. Failed to check remote source.", str(cvt_err.args[1]))
                raise

    def create_virtual_table(self, table, data_lake_table=None, schema=None, data_lake_container='SYSRDL#CG',
                             remote_source=None, remote_database='<NULL>', remote_schema=None, remote_table=None,
                             data_lake_files=None, table_structure=None, file_format=None,
                             delimiter=None, partition_cols=None, manual_refresh_partition=False):
        """
        Creates a SAP virtual HANA table at remote/data lake source.

        Parameters
        ----------
        table : str
            HANA virtual table name.
        data_lake_table : str, optional(deprecated)
            SAP HANA data lake table name.

            Deprecated, please use ``remote_table`` for referencing to a table
            in SAP HANA data lake container relational engine.
        schema : str, optional
            Schema name. If None, use the current schema.

            Defaults to None.
        data_lake_container : str, optional(deprecated)
            Name of the data lake container.

            Defaults to 'SYSRDL#CG'.

            Deprecated, please use ``remote_source``.
        remote_source : str, optional
            Remote source where data of the target virtual table reside.

            Mandatory and valid only if ``data_lake_table`` is None.
        remote_database : str, optional
            Database of the remote source where data of the target virtual table reside.

            Defaults to '<NULL>', i.e. the default database.

            Ineffective when `data_lake_files` is not None.
        remote_schema : str, optional
            The schema under which the corresponding ``remote_table`` of the target SAP HANA
            virtual table reside.

            Required if ``data_lake_table`` is None.

            Defaults to None.
        remote_table : str, optional
            The table name in remote source where data of the target virtual table reside.

            Mandatory and valid only when ``data_lake_table`` is None.

            Defaults to None.
        data_lake_files : str, optional
            Specifies the directory of file(s) in the source data lake to be accessed remotely.

            If a concrete value (not None) of this parameter is specified,
            SQL on Files(SoF) access to data lake files will be triggered.

            Defaults to None.

        table_structure : dict, optional
            Manually define column types based on SAP HANA database table structure
            in terms of <column name> : <data type> key-value pairs.

            Defaults to None.

        file_format : str, optional
            Specifies the file format for the files to be accessed.

            Valid options currently include:

            - 'csv': Comma-Seperated Values
            - 'parquet': Apache Parquet

            It should be specified for SoF access to data lake files.

            Defaults to None.
        delimiter : str, optional
            Specifies the delimiter for separating fields in a **csv** file.

            Valid only when ``file_format`` is 'csv'.

            Defaults to None.
        partition_cols : str or ListOfStrings, optional
            Specifies the columns by which the parquet files are partitioned when being created.
            Please note that the order of parition columns matters.

            Effecitive only when ``file_format`` is 'parquet'.

            Defaults to None.
        manual_refresh_partition : bool, optional
            Specifies whether or not to execute the **AUTO REFRESH PARTITION** SQL clause
            where creating the target virtual table for data like files.

            Defaults to False.
        """
        if schema is None:
            table_reference = quotename(table)
        else:
            table_reference = '.'.join(map(quotename, (schema, table)))
        if data_lake_files is None:
            if all(x is None for x in (data_lake_table, remote_table)):
                msg = '`data_lake_table` and `remote_table` cannot both be set to None.'
                raise ValueError(msg)
            if data_lake_table is not None:
                sql = 'CREATE VIRTUAL TABLE {0} AT "{1}_SOURCE"."<NULL>"."{2}".{3};'.format(table_reference,
                                                                                            data_lake_container,
                                                                                            remote_schema,
                                                                                            quotename(data_lake_table))
            else:
                if any(x is None for x in (remote_source, remote_schema, remote_table)):
                    msg = '`remote_source`, `remote_schema` and `remote_table` must all be specified.'
                    raise ValueError(msg)
                sql = 'CREATE VIRTUAL TABLE {0} AT "{1}"."{2}"."{3}"."{4}";'.format(table_reference,
                                                                                    remote_source,
                                                                                    remote_database,
                                                                                    remote_schema,
                                                                                    remote_table)
        else:
            if None in [remote_source, data_lake_files, table_structure, file_format]:
                msg = '`remote_source`, `data_lake_files`, `table_structure` and `file_format` must all be specified for SoF.'
                raise ValueError(msg)
            if not data_lake_files.startswith('/'):
                data_lake_files = r'/' + data_lake_files
            table_structure_str = ', '.join([f'"{col}" {table_structure[col]}' for col in table_structure])
            sql = 'CREATE VIRTUAL TABLE {0} ({1}) AT "{2}"."{3}" AS {4}'.format(table_reference,
                                                                                table_structure_str,
                                                                                remote_source,
                                                                                data_lake_files,
                                                                                file_format.upper())
            add_sql = ""
            if file_format.lower() == 'csv' and delimiter is not None:
                add_sql = add_sql + f" FIELD DELIMITED BY '{delimiter}'"
            if file_format.lower() == 'parquet' and partition_cols is not None:
                if isinstance(partition_cols, str):
                    partition_cols = [partition_cols]
                partition_by_cols = ", ".join(map(quotename, partition_cols))
                add_sql = add_sql +  f" PARTITION BY ({partition_by_cols})"
            if manual_refresh_partition:
                add_sql = add_sql + " MANUAL REFRESH PARTITION"
            sql = sql + add_sql + ";"
        try:
            with self.connection.cursor() as cur:
                execute_logged(cur,
                               sql,
                               self.sql_tracer,
                               self)
        except dbapi.Error as cvt_err:
            logger.error("%s. Failed to execute `%s`", cvt_err, sql)
            raise
        except Exception as cvt_err:
            logger.error("%s. Failed to execute `%s`", str(cvt_err.args[1]), sql)
            raise

    def drop_procedure(self, proc, schema=None, drop_option=None):
        """
        Drops the specified view.

        Parameters
        ----------
        proc : str
            Procedure name.
        schema : str, optional
            Schema name. If None, use the current schema.
        drop_option : {None, 'CASCADE', 'RESTRICT'}, optional
            Specifies the drop option to use.

            Defaults to None.

        Examples
        --------
        cc is a connection to a SAP HANA database instance.

        >>> cc.drop_proc(proc='TEST_PROC')
        """
        if schema is None:
            schema = self.get_current_schema()
        try:
            with self.connection.cursor() as cur:
                query = 'DROP PROCEDURE {}.{}'.format(quotename(schema), quotename(proc))
                if drop_option:
                    query += ' {}'.format(drop_option)
                execute_logged(cur,
                               query,
                               self.sql_tracer,
                               self)
        except dbapi.Error as db_er:
            logger.error("Fail to drop procedure. %s", db_er)
            pass
        except Exception as db_er:
            logger.error("Fail to drop procedure. %s", str(db_er.args[1]))
            pass

    def drop_view(self, view, schema=None, drop_option=None):
        """
        Drops the specified view.

        Parameters
        ----------
        view : str
            View name.
        schema : str, optional
            Schema name. If None, use the current schema.
        drop_option : {None, 'CASCADE', 'RESTRICT'}, optional
            Specifies the drop option to use.

            Defaults to None.

        Examples
        --------
        cc is a connection to a SAP HANA database instance.

        >>> cc.drop_view(view='TEST_VIEW')
        """
        if schema is None:
            schema = self.get_current_schema()
        try:
            with self.connection.cursor() as cur:
                query = 'DROP VIEW {}.{}'.format(quotename(schema), quotename(view))
                if drop_option:
                    query += ' {}'.format(drop_option)
                execute_logged(cur,
                               query,
                               self.sql_tracer,
                               self)
        except dbapi.Error as db_er:
            logger.error("Fail to drop view. %s", db_er)
            pass
        except Exception as db_er:
            logger.error("Fail to drop view. %s", str(db_er.args[1]))
            pass

    def truncate_table(self, table, schema=None):
        """
        Truncates the specified table.

        Parameters
        ----------
        table : str
            Table name.
        schema : str, optional
            Schema name. If None, use the current schema.
        """
        if schema is None:
            schema = self.get_current_schema()
        try:
            with self.connection.cursor() as cur:
                query = 'TRUNCATE TABLE {}.{}'.format(quotename(schema), quotename(table))
                execute_logged(cur,
                               query,
                               self.sql_tracer,
                               self)
        except dbapi.Error as db_er:
            logger.error("Fail to truncate table. %s", db_er)
            pass
        except Exception as db_er:
            logger.error("Fail to truncate table. %s", str(db_er.args[1]))
            pass

    def drop_table(self, table, schema=None, data_lake=False, data_lake_container='SYSRDL#CG', drop_option=None):
        """
        Drops the specified table.

        Parameters
        ----------
        table : str
            Table name.
        schema : str, optional
            Schema name. If None, use the current schema.
        data_lake : bool, optional
            If True, drop the data lake table.

            Defaults to False.
        data_lake_container : str, optional
            Name of the data lake container.

            Defaults to 'SYSRDL#CG'.
        drop_option : {None, 'CASCADE', 'RESTRICT'}, optional
            Specifies the drop option to use.

            Defaults to None.

        Examples
        --------
        cc is a connection to a SAP HANA database instance.

        >>> cc.drop_table(table='TEST_TBL')
        """
        if schema is None:
            schema = self.get_current_schema()
        try:
            with self.connection.cursor() as cur:
                query = 'DROP TABLE {}.{}'.format(quotename(schema), quotename(table))
                if drop_option:
                    query += ' {}'.format(drop_option)
                if data_lake:
                    query = 'DROP TABLE {}'.format(quotename(table))
                    query = "CALL {}.REMOTE_EXECUTE ('{}')".format(data_lake_container, query)
                execute_logged(cur,
                               query,
                               self.sql_tracer,
                               self)
        except dbapi.Error as db_er:
            logger.error("Fail to drop table. %s", db_er)
            pass
        except Exception as db_er:
            logger.error("Fail to drop table. %s", str(db_er.args[1]))
            pass

    def copy_to_data_lake(self, data, virtual_table, data_lake_table, schema=None, append=False, data_lake_container='SYSRDL#CG'):
        """
        Copies HANA data to a data lake table.

        Parameters
        ----------
        data : DataFrame
            HANA DataFrame.
        virtual_table : str
            HANA virtual table name.
        data_lake_table : str
            HANA data lake table name.
        schema : str, optional
            Schema name. If None, use the current schema.
        append : bool, optional
            Append data to the existing data lake table.

            Defaults to False.
        data_lake_container : str, optional
            Name of the data lake container.

            Defaults to 'SYSRDL#CG'.
        """
        if not append:
            table_structure = data.get_table_structure()
            for key, value in table_structure.items():
                if value == 'NCLOB':
                    table_structure[key] = 'CLOB'
                if value == 'NBLOB':
                    table_structure[key] = 'BLOB'
                if 'NVARCHAR' in value:
                    table_structure[key] = value.replace('NVARCHAR', 'VARCHAR')
            self.create_table(
                table=data_lake_table,
                table_structure=table_structure,
                schema=schema,
                data_lake=True,
                data_lake_container=data_lake_container
            )
            self.create_virtual_table(
                table=virtual_table,
                data_lake_table=data_lake_table,
                schema=schema,
                data_lake_container=data_lake_container)
        if schema is None:
            table_reference = quotename(virtual_table)
        else:
            table_reference = '.'.join(map(quotename, (schema, virtual_table)))
        sql = "INSERT INTO {} {}".format(table_reference, data.select_statement)
        with self.connection.cursor() as cur:
            execute_logged(cur,
                           sql,
                           self.sql_tracer,
                           self)

    def explain_plan_statement(self, statement_name, subquery, force=True):
        """
        Evaluates the execution plan that the database follows when executing an SQL statement and return the result.

        Parameters
        ----------
        statement_name : str,
            Specifies the name of a specific execution plan in the output table for a given SQL.
        subquery : str,
            Specifies the subquery to explain the plan for.
        force : bool, optional
            If force is True, it will delete existing result according to statement_name.

            Defaults to True.
        """
        if force:
            self.execute_sql("DELETE FROM EXPLAIN_PLAN_TABLE WHERE STATEMENT_NAME = '{}';".format(statement_name))
        return self.sql("""
                        EXPLAIN PLAN SET STATEMENT_NAME = '{0}' FOR
                        {1};
                        SELECT * FROM EXPLAIN_PLAN_TABLE WHERE STATEMENT_NAME = '{0}';
                        """.format(statement_name, subquery)).collect()

    def has_schema(self, schema):
        """
        Returns the boolean value for the schema existence.

        Parameters
        ----------
        schema : str
            Schema name.

        Returns
        -------
        bool
            Table existence.

        Examples
        --------
        cc is a connection to a SAP HANA database instance.

        >>> cc.has_schema(schema='MY_SCHEMA')
        True
        """

        cnt_tab = -1
        cnt_tab = DataFrame(self, "SELECT COUNT(*) FROM SYS.SCHEMAS WHERE SCHEMA_NAME='{}'"\
            .format(schema)).collect().iat[0, 0]
        if isinstance(cnt_tab, (list, tuple)):
            return cnt_tab[0] > 0
        return cnt_tab > 0

    def has_table(self, table, schema=None):
        """
        Returns the boolean value for the table existence.

        Parameters
        ----------
        table : str
            Table name.
        schema : str, optional
            Schema name. If None, use the current schema.

        Returns
        -------
        bool
            Table existence.

        Examples
        --------
        cc is a connection to a SAP HANA database instance.

        >>> cc.has_table(table='TEST_TBL')
        True
        """
        if schema is None:
            schema = self.get_current_schema()
        cnt_tab = -1
        if '#' in table:
            connection_id = DataFrame(self, "SELECT SESSION_CONTEXT('CONN_ID') FROM DUMMY").collect().iat[0, 0]
            cnt_tab = DataFrame(self, "SELECT COUNT(*) FROM M_TEMPORARY_TABLES WHERE TABLE_NAME='{}' AND SCHEMA_NAME='{}' AND CONNECTION_ID='{}'"\
            .format(table, schema, connection_id)).collect().iat[0, 0]
        else:
            cnt_tab = DataFrame(self, "SELECT COUNT(*) FROM M_TABLES WHERE TABLE_NAME='{}' AND SCHEMA_NAME='{}'"\
            .format(table, schema)).collect().iat[0, 0]
        if isinstance(cnt_tab, (list, tuple)):
            return cnt_tab[0] > 0
        return cnt_tab > 0

    def hana_version(self):
        """
        Returns the version of a SAP HANA database instance.

        Parameters
        ----------
        None

        Returns
        -------
        str
            The SAP HANA version.

        Examples
        --------
        cc is a connection to a SAP HANA database instance.

        >>> cc.hana_version()
        '4.50.000.00.1581545459 (master)'
        """

        return DataFrame(self, "SELECT VALUE FROM SYS.M_SYSTEM_OVERVIEW WHERE NAME='Version'").collect().iat[0, 0]

    def get_current_schema(self):
        """
        Returns the current schema name.

        Parameters
        ----------
        None

        Returns
        -------
        str
            The current schema name.

        Examples
        --------
        cc is a connection to a SAP HANA database instance.

        >>> cc.get_current_schema()
        'TEST'
        """
        current_schema = DataFrame(self, "SELECT CURRENT_SCHEMA FROM DUMMY").collect().iat[0, 0]
        if isinstance(current_schema, (list, tuple)):
            current_schema = current_schema[0]
        return current_schema

    def get_tables(self, schema=None):
        """
        Returns the tables list given schema.

        Parameters
        ----------
        schema : str, optional
            The schema name. If no specified, use the current schema.
        """
        if schema is None:
            schema = self.get_current_schema()
        return self.sql("SELECT DISTINCT TABLE_NAME FROM M_TABLES WHERE SCHEMA_NAME='{}'".format(schema)).collect()

    def get_schemas(self):
        """
        Returns the schemas list.
        """
        return self.sql("SELECT DISTINCT SCHEMA_NAME FROM SCHEMAS").collect()

    def get_procedures(self, schema=None):
        """
        Returns the procedures list given schema.

        Parameters
        ----------
        schema : str, optional
            The schema name. If no specified, use the current schema.
        """
        if schema is None:
            schema = self.get_current_schema()
        return self.sql("SELECT DISTINCT PROCEDURE_NAME FROM PROCEDURES WHERE SCHEMA_NAME='{}'".format(schema)).collect()

    def get_temporary_tables(self, schema=None, connection_id=None, list_other_connections=False):
        """
        Returns the temporary table list given schema.

        Parameters
        ----------
        schema : str, optional
            The schema name. If no specified, use the current schema.
        conneciont_id : int, optional
            If None, it returns the temporary tables from the current connection.
        list_other_connections : bool, optional
            If True, it will also outputs the temporary tables from other connections.
        """
        if schema is None:
            schema = self.get_current_schema()
        if connection_id is None and not list_other_connections:
            connection_id = self.get_connection_id()
        if list_other_connections:
            sql = "SELECT DISTINCT TABLE_NAME, CONNECTION_ID FROM M_TEMPORARY_TABLES WHERE SCHEMA_NAME='{}'".format(schema)
        else:
            sql = "SELECT DISTINCT TABLE_NAME, CONNECTION_ID FROM M_TEMPORARY_TABLES WHERE SCHEMA_NAME='{}' AND CONNECTION_ID={}".format(schema, connection_id)
        return self.sql(sql).collect()

    def get_connection_id(self):
        """
        Returns the connection id.
        """
        return self.sql("SELECT SESSION_CONTEXT('CONN_ID') FROM DUMMY").collect().iat[0, 0]

    def cancel_session_process(self, connection_id=None):
        """
        Cancels the current process in the given session. If the connection_id is not provided, it will use the current connection.

        Parameters
        ----------
        connection_id : int, optional
            Connection id.

            Defaults to the current connection.
        """
        if connection_id is None:
            connection_id = self.get_connection_id()
        self.execute_sql("ALTER SYSTEM CANCEL WORK IN SESSION '{}'".format(connection_id))

    def restart_session(self, connection_id=None):
        """
        Terminates the current session, drops all the temporary tables and starts a new one.

        Parameters
        ----------
        connection_id : int, optional
            Connection id.

            Defaults to the current connection.
        """
        if connection_id is None:
            self.cancel_session_process()
            self.clean_up_temporary_tables()
        else:
            self.execute_sql("ALTER SYSTEM DISCONNECT SESSION '{}'".format(connection_id))

    def clean_up_temporary_tables(self):
        """
        Drops all the temporary table under the current schema.
        """
        connection_id = self.get_connection_id()
        for _, row in self.get_temporary_tables(connection_id=connection_id).iterrows():
            self.drop_table(row["TABLE_NAME"])

    def hana_major_version(self):
        """
        Returns the major number of SAP HANA version.

        Parameters
        ----------
        None

        Returns
        -------
        str
            The major number of SAP HANA version.

        Examples
        --------
        cc is a connection to a SAP HANA database instance.

        >>> cc.hana_major_version()
        '4'
        """

        return self.hana_version().split(".", 1)[0]

    def is_cloud_version(self):
        """
        Check whether the SAP HANA database instance is cloud version or on-premise.

        Parameters
        ----------
        None

        Returns
        -------
        bool
            If True, the HANA instance is cloud version.
        """

        return int(self.hana_major_version()) >= 4

    def sql(self, sql):
        """
        Returns a SAP HANA DataFrame representing a query.

        Parameters
        ----------
        sql : str
            SQL query. The last sentence must be select statement.

        Returns
        -------
        DataFrame
            The DataFrame for the query.

        Examples
        --------
        cc is a connection to a SAP HANA database instance.

        >>> df = cc.sql('SELECT T.A, T2.B FROM T, T2 WHERE T.C=T2.C')
        """
        sql = sql.rstrip("\n")
        anblock = re.search(r'(do begin|DO BEGIN|DO\nBEGIN|do\nbegin|create function|CREATE FUNCTION|create procedure|CREATE PROCEDURE)[\s\S]+(end;|END;)', sql)
        multiline = None
        if anblock:
            part1, part2 = sql.split(anblock.group(0))
            part1 = part1.split(";")
            if part1[-1] == "":
                part1 = part1[:-1]
            part2 = part2.split(";")
            if part2[-1] == "":
                part2 = part2[:-1]
            multiline = part1 + [anblock.group(0)] + part2
        else:
            multiline = sql.split(";")
        if multiline[-1].strip() == '':
            multiline = multiline[:-1]
        if len(multiline) > 1:
            while '\n' in multiline:
                multiline.remove('\n')
            while '' in multiline:
                multiline.remove('')
            with self.connection.cursor() as cur:
                for line in multiline[:-1]:
                    try:
                        execute_logged(cur,
                                       line,
                                       self.sql_tracer,
                                       self)
                    except dbapi.Error as err:
                        logger.error(err)
                    except Exception as err:
                        logger.error(err)
            if not self.pyodbc_connection:
                if not self.connection.getautocommit():
                    self.connection.commit()
        return DataFrame(self, multiline[-1])

    def execute_sql(self, sql):
        """
        Multiline sql execution.

        Parameters
        ----------
        sql : str or sql file
            SQL query.
        """
        if os.path.isfile(sql):
            with open(sql, "r") as text_file:
                data = text_file.read()
            sql = data
        last_execute = self.sql(sql).select_statement
        with self.connection.cursor() as cur:
            try:
                execute_logged(cur, last_execute, self.sql_tracer, self)
            except dbapi.Error as err:
                logger.error(err)
            except Exception as err:
                logger.error(err)
        if not self.pyodbc_connection:
            if not self.connection.getautocommit():
                self.connection.commit()

    def table(self, table, schema=None, save_source=True, view_params=None):
        """
        Returns a DataFrame that represents the specified table.

        Parameters
        ----------
        table : str
            The table name.
        schema : str, optional, keyword-only
            The schema name. If this value is not provided or set to None, then the value defaults to the
            ConnectionContext's current schema.
        save_source : bool, optional
            If True, save the name of source table.
            Defaults to True.
        view_params : list or tuple, optional
            Parameters for view.

        Returns
        -------
        DataFrame
            The DataFrame that is selecting data from the specified table.

        Examples
        --------
        >>> df1 = cc.table('MY_TABLE')
        >>> df2 = cc.table('MY_OTHER_TABLE', schema='MY_SCHEMA')
        """

        if schema is None:
            table_reference = quotename(table)
        else:
            table_reference = '.'.join(map(quotename, (schema, table)))

        select = 'SELECT * FROM {}'.format(table_reference)
        if view_params:
            mod_params = []
            for elem in view_params:
                if isinstance(elem, str):
                    mod_params.append("'{}'".format(elem))
                else:
                    mod_params.append(str(elem))
            select = "{} ({})".format(select, ", ".join(mod_params))
        result = DataFrame(self, select)

        if save_source:
            result.set_source_table(table, schema)

        # pylint: disable=protected-access
        if table.startswith('#'):
            result._ttab_handling = 'ttab'
            result._ttab_reference = table_reference
        else:
            result._ttab_handling = 'safe'

        # SQLTRACE
        # Checking the trace_sql_active is unnecessary however as this is
        # done in the sql_tracer object as well. But as not to impact
        # current test cases this has been added. If the ehck is removed 2
        # test cases will fail. Changing the test cases would be the better
        # option going forward
        if self.sql_tracer.trace_sql_active:
            self.sql_tracer.trace_object({
                'name': table,
                'table_type': result.generate_table_type(),
                'select': result.select_statement,
                'reference': table_reference,
                'schema': schema
            }, sub_cat='output_tables')

        return result

    def upsert_streams_data(self, table_name, key, data, schema=None):
        """
        This method will enable streams data to SAP HANA through SQL upsert if the provided data type contains bytes.

        parameters
        ----------
        table_name: str
            HANA table name to be upserted streams data.

        key: str
            The key column name.

        data: dict
            The keys of data are column names while the values are the data to upsert. If data contains bytes, the method will use HANA LOB streams method.

        schema: str, optional
            The schema name.

            Defaults to the current schema.

        Examples
        --------
        >>> with open('image.png', 'rb') as f:
                img = f.read()
        >>> with open('image2.png', 'rb') as f:
                img2 = f.read()
        >>> conn.upsert_streams_data(table_name="LOB_STREAMING_TEST", key="id", data={"id":1, "img":img, "other":img2})
        """
        in_blob_dict = {}
        execute_dict = {}
        if key not in data:
            raise ValueError("`key` value must be provided in `data`.")
        tab_struct = self.table(table_name, schema=schema).get_table_structure()
        for col, _ in tab_struct.items():
            if col in data:
                if isinstance(data[col], bytes):
                    in_blob_dict[col] = dbapi.LOB()
                    execute_dict[col] = in_blob_dict[col]
                else:
                    execute_dict[col] = data[col]
            else:
                execute_dict[col] = None
        sql = "UPSERT {0} ({1}) VALUES(:{2}) WHERE {3}=:{4}".format(quotename(table_name),
                                                                    ", ".join(list(map(quotename, list(tab_struct.keys())))),
                                                                    ", :".join(list(tab_struct.keys())),
                                                                    quotename(key),
                                                                    key)
        cursor = self.connection.cursor()
        self.connection.setautocommit(False)
        cursor.execute(sql, execute_dict)
        for col, blb in in_blob_dict.items():
            blb.write(data=data[col])
        for _, blb in in_blob_dict.items():
            blb.close()
        self.connection.commit()
        cursor.close()

    def update_streams_data(self, table_name, key, data, schema=None):
        """
        This method will enable streams data to SAP HANA through SQL update if the provided data type contains bytes.

        parameters
        ----------
        table_name: str
            HANA table name to be updated streams data.

        key: str
            The key column name.

        data: dict
            The keys of data are column names while the values are the data to update. If data contains bytes, the method will use HANA LOB streams method.

        schema: str, optional
            The schema name.

            Defaults to the current schema.

        Examples
        --------
        >>> with open('image.png', 'rb') as f:
                img = f.read()
        >>> with open('image2.png', 'rb') as f:
                img2 = f.read()
        >>> conn.update_streams_data(table_name="LOB_STREAMING_TEST", key="id", data={"id":1, "img":img, "other":img2})
        """
        in_blob_dict = {}
        execute_dict = {}
        if key not in data:
            raise ValueError("`key` value must be provided in `data`.")
        tab_struct = self.table(table_name, schema=schema).get_table_structure()
        set_keys = []
        for col, _ in tab_struct.items():
            if col in data:
                if col != key:
                    set_keys.append(col)
                if isinstance(data[col], bytes):
                    in_blob_dict[col] = dbapi.LOB()
                    execute_dict[col] = in_blob_dict[col]
                else:
                    execute_dict[col] = data[col]

        set_clause = ", ".join(list(map(lambda x: "{} =:{}".format(quotename(x), x), set_keys)))
        sql = "UPDATE {0} SET {1} WHERE {2}=:{3}".format(quotename(table_name),
                                                         set_clause,
                                                         quotename(key),
                                                         key)
        cursor = self.connection.cursor()
        self.connection.setautocommit(False)
        cursor.execute(sql, execute_dict)
        for col, blb in in_blob_dict.items():
            blb.write(data=data[col])
        for _, blb in in_blob_dict.items():
            blb.close()
        self.connection.commit()
        cursor.close()

    def to_sqlalchemy(self, **kwargs):
        """
        Returns a SQLAlchemy engine.
        """
        try:
            from sqlalchemy import create_engine
        except ImportError:
            raise ImportError("SQLAlchemy is not installed. Please install it by running `pip install sqlalchemy-hana`.")
        if self.userkey is None:
            conn_str = self.connection.__str__().replace('<dbapi.Connection Connection object : ', '')[:-1]
            conn_config = conn_str.split(',')
            url = conn_config[0]
            port = conn_config[1]
            user = conn_config[2]
            password = conn_config[3]
            return create_engine('hana://{}:{}@{}:{}/'.format(user, password, url, port), connect_args=self.properties, **kwargs)
        return create_engine('hana://userkey={}'.format(self.userkey), connect_args=self.properties, **kwargs)

    def create_vector_index(self,
                            index_name,
                            table_name,
                            vector_column,
                            schema_name=None,
                            index_type='HNSW',
                            similarity_function='COSINE_SIMILARITY ',
                            build_config=None,
                            search_config=None,
                            online=False):
        """
        Creates an index on a REAL_VECTOR table column.

        Parameters
        ----------

        index_name : str
            The name of the index to create.
        table_name : str
            The name of the table to create the index on.
        vector_column : str
            The name of the column to create the index on.
        schema_name : str, optional
            The name of the schema to create the index in.

            Defaults to None.
        index_type : {'HNSW'}, optional
            The type of index to create. Valid options are 'HNSW'.

            Defaults to 'HNSW'.
        similarity_function : {'COSINE_SIMILARITY', 'L2DISTANCE'}, optional
            The similarity function to use when creating the index.

            Defaults to 'COSINE_SIMILARITY'.
        build_config : dict, optional
            Specifies the build options for HNSW indexes.

            Defaults to None.
        search_config : dict, optional
            Specifies the search options for HNSW indexes.

            Defaults to None.
        online : bool, optional
            Creating an index in online mode results in an operation that acquires a shared table lock. When the ONLINE keyword is omitted, an exclusive table lock is acquired, making the table inaccessible for the duration of the index building.

            Defaults to False.
        """
        if schema_name is None:
            schema_name = self.get_current_schema()
        quoted_index_name = smart_quote(index_name)
        sql = f"CREATE {index_type} VECTOR INDEX {quoted_index_name} ON \"{schema_name}\".\"{table_name}\" (\"{vector_column}\") SIMILARITY FUNCTION {similarity_function}"
        if build_config:
            build_config = json.dumps(build_config)
            sql += f" BUILD CONFIGURATION '{build_config}'"
        if search_config:
            search_config = json.dumps(search_config)
            sql += f" SEARCH CONFIGURATION '{search_config}'"
        if online:
            sql += " ONLINE"
        self.execute_sql(sql)

    def drop_vector_index(self, index_name, online=False):
        """
        Drops an index on a REAL_VECTOR table column.

        Parameters
        ----------
        index_name : str
            The name of the index to drop.
        online : bool, optional
            The ONLINE keyword allows the dropping of the index without serializing with concurrent DML operations. The drop results in an operation that only acquires a shared table lock.
        """
        index_name = smart_quote(index_name)
        sql = f"DROP INDEX {index_name}"
        if online:
            sql += " ONLINE"
        self.execute_sql(sql)

    def embed_query(self, query, model_version='SAP_NEB.20240715'):
        """
        Create a query embedding and return a vector.

        Parameters
        ----------
        query : str or list of str
            The query to embed.
        model_version : str, optional
            Text Embedding Model version. Options are 'SAP_NEB.20240715' and 'SAP_GXY.20250407'.

            Defaults to 'SAP_NEB.20240715'.

        Returns
        -------
        list of float when query is str, list of list of float when query is list of str
        """
        if isinstance(query, (list, tuple)):
            sql = ''
            for i, q in enumerate(query):
                if i > 0:
                    sql += ' UNION ALL '
                sql += f"SELECT \'{q}\' AS TEXT FROM DUMMY"
            return self.sql(sql).add_vector("TEXT", text_type='QUERY', embed_col="EMBEDDING").select(["EMBEDDING"]).collect()["EMBEDDING"].tolist()
        return self.sql(f"SELECT \'{query}\' AS TEXT FROM DUMMY").add_vector("TEXT", text_type='QUERY', embed_col="EMBEDDING", model_version=model_version).select(["EMBEDDING"]).collect()["EMBEDDING"].iat[0]

# SQLTRACE Class for Core Functions
class SqlTrace(object):
    """
    Provides functions to track generated SQL.

    It stores the trace in a dictionary in the following format:

    {
        'algorithm':{
            'function':{
                'subcategory':[]
            }
        }
    }

    Attributes
    ----------
    trace_sql_log : dictionary
        The SQL Trace dictionary
    trace_sql_active : boolean
        Flag to define if tracing should occur
    trace_sql_algo : string
        Current algorithm that is being traced
    trace_sql_function : string
        Current function that is being traced
    trace_history : boolean
        Track multiple runs of the same algorithm. If this attribute is enabled, then
        the algo_tracker tracks the count of the algorithm
        and adds a sequence number to the algorithm name in the dictionary
        so that each run is traced seperately.
    trace_algo_tracker : dictionary
        If trace_history is enabled, then the algorithm tracks the count of the number of times
        the same algorithm is run

    Examples
    --------
    >>> Example snippet of the SQL trace dictionary:
    {
        "RandomForestClassifier": {
            "Fit": {
                "input_tables": [
                    {
                        "name": "#PAL_RANDOM_FOREST_DATA_TBL_0",
                        "type": "table (\"AccountID\" INT,\"ServiceType\" VARCHAR(21),\"ServiceName\" VARCHAR(14),\"DataAllowance_MB\" INT,\"VoiceAllowance_Minutes\" INT,\"SMSAllowance_N_Messages\" INT,\"DataUsage_PCT\" DOUBLE,\"DataUsage_PCT_PM\" DOUBLE,\"DataUsage_PCT_PPM\" DOUBLE,\"VoiceUsage_PCT\" DOUBLE,\"VoiceUsage_PCT_PM\" DOUBLE,\"VoiceUsage_PCT_PPM\" DOUBLE,\"SMSUsage_PCT\" DOUBLE,\"SMSUsage_PCT_PM\" DOUBLE,\"SMSUsage_PCT_PPM\" DOUBLE,\"Revenue_Month\" DOUBLE,\"Revenue_Month_PM\" DOUBLE,\"Revenue_Month_PPM\" DOUBLE,\"Revenue_Month_PPPM\" DOUBLE,\"ServiceFailureRate_PCT\" DOUBLE,\"ServiceFailureRate_PCT_PM\" DOUBLE,\"ServiceFailureRate_PCT_PPM\" DOUBLE,\"CustomerLifetimeValue_USD\" DOUBLE,\"CustomerLifetimeValue_USD_PM\" DOUBLE,\"CustomerLifetimeValue_USD_PPM\" DOUBLE,\"Device_Lifetime\" INT,\"Device_Lifetime_PM\" INT,\"Device_Lifetime_PPM\" INT,\"ContractActivityLABEL\" VARCHAR(8))",
                        "select": "SELECT \"AccountID\", \"ServiceType\", \"ServiceName\", \"DataAllowance_MB\", \"VoiceAllowance_Minutes\", \"SMSAllowance_N_Messages\", \"DataUsage_PCT\", \"DataUsage_PCT_PM\", \"DataUsage_PCT_PPM\", \"VoiceUsage_PCT\", \"VoiceUsage_PCT_PM\", \"VoiceUsage_PCT_PPM\", \"SMSUsage_PCT\", \"SMSUsage_PCT_PM\", \"SMSUsage_PCT_PPM\", \"Revenue_Month\", \"Revenue_Month_PM\", \"Revenue_Month_PPM\", \"Revenue_Month_PPPM\", \"ServiceFailureRate_PCT\", \"ServiceFailureRate_PCT_PM\", \"ServiceFailureRate_PCT_PPM\", \"CustomerLifetimeValue_USD\", \"CustomerLifetimeValue_USD_PM\", \"CustomerLifetimeValue_USD_PPM\", \"Device_Lifetime\", \"Device_Lifetime_PM\", \"Device_Lifetime_PPM\", \"ContractActivityLABEL\" FROM (SELECT a.* FROM #PAL_PARTITION_DATA_TBL_EC70569E_882B_11E9_9ACB_784F436CBD3C a inner join #PAL_PARTITION_RESULT_TBL_EC70569E_882B_11E9_9ACB_784F436CBD3C b        on a.\"AccountID\" = b.\"AccountID\" where b.\"PARTITION_TYPE\" = 1) AS \"DT_2\""
                    }
                ],
                "internal_tables":[
                    {
                        "name": "#PAL_RANDOM_FOREST_PARAM_TBL_0",
                        "type": [
                            [
                                "PARAM_NAME",
                                "NVARCHAR(5000)"
                            ],
                            [
                                "INT_VALUE",
                                "INTEGER"
                            ],
                            [
                                "DOUBLE_VALUE",
                                "DOUBLE"
                            ],
                            [
                                "STRING_VALUE",
                                "NVARCHAR(5000)"
                            ]
                        ]
                    }
                ],
                "output_tables":[
                    {
                        "name": "#PAL_RANDOM_FOREST_MODEL_TBL_0",
                        "type": "table (\"ROW_INDEX\" INT,\"TREE_INDEX\" INT,\"MODEL_CONTENT\" NVARCHAR(5000))",
                        "select": "SELECT * FROM \"#PAL_RANDOM_FOREST_MODEL_TBL_0\"",
                        "reference": "\"#PAL_RANDOM_FOREST_MODEL_TBL_0\"",
                        "schema": null
                    }
                ],
                "function":[
                    {
                        "name": "PAL_RANDOM_DECISION_TREES",
                        "schema": "_SYS_AFL",
                        "type": "pal"
                    }
                ],
                "sql":[
                    "DROP TABLE \"#PAL_RANDOM_FOREST_DATA_TBL_0\"",
                    "CREATE LOCAL TEMPORARY COLUMN TABLE \"#PAL_RANDOM_FOREST_DATA_TBL_0\" AS (SELECT \"AccountID\", \"ServiceType\", \"ServiceName\", \"DataAllowance_MB\", \"VoiceAllowance_Minutes\", \"SMSAllowance_N_Messages\", \"DataUsage_PCT\", \"DataUsage_PCT_PM\", \"DataUsage_PCT_PPM\", \"VoiceUsage_PCT\", \"VoiceUsage_PCT_PM\", \"VoiceUsage_PCT_PPM\", \"SMSUsage_PCT\", \"SMSUsage_PCT_PM\", \"SMSUsage_PCT_PPM\", \"Revenue_Month\", \"Revenue_Month_PM\", \"Revenue_Month_PPM\", \"Revenue_Month_PPPM\", \"ServiceFailureRate_PCT\", \"ServiceFailureRate_PCT_PM\", \"ServiceFailureRate_PCT_PPM\", \"CustomerLifetimeValue_USD\", \"CustomerLifetimeValue_USD_PM\", \"CustomerLifetimeValue_USD_PPM\", \"Device_Lifetime\", \"Device_Lifetime_PM\", \"Device_Lifetime_PPM\", \"ContractActivityLABEL\" FROM (SELECT a.* FROM #PAL_PARTITION_DATA_TBL_EC70569E_882B_11E9_9ACB_784F436CBD3C a inner join #PAL_PARTITION_RESULT_TBL_EC70569E_882B_11E9_9ACB_784F436CBD3C b        on a.\"AccountID\" = b.\"AccountID\" where b.\"PARTITION_TYPE\" = 1) AS \"DT_2\")",
                    "DROP TABLE \"#PAL_RANDOM_FOREST_PARAM_TBL_0\"",
                    "CREATE LOCAL TEMPORARY COLUMN TABLE \"#PAL_RANDOM_FOREST_PARAM_TBL_0\" (\n    \"PARAM_NAME\" NVARCHAR(5000),\n    \"INT_VALUE\" INTEGER,\n    \"DOUBLE_VALUE\" DOUBLE,\n    \"STRING_VALUE\" NVARCHAR(5000)\n)"
                ]
        }

    }
    """

    def __init__(self):
        self.trace_sql_log = {}
        self.trace_sql_active = False
        self.trace_sql_algo = None
        self.trace_sql_function = None
        self.trace_history = False
        self.trace_algo_tracker = {}

    def _set_log_level(self, logger, level):
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

    def set_log_level(self, level='info'):
        """
        Set logging level.

        Parameters
        ----------

        level : {'info', 'warn', 'debug', 'error'}
        """
        logging.basicConfig()
        for module in ["hana_ml.ml_base", 'hana_ml.dataframe', 'hana_ml.algorithms.pal']:
            logger = logging.getLogger(module)
            self._set_log_level(logger, level)

    def enable_sql_trace(self, enable):
        """
        Enables or disables the SQL trace

        Parameters
        ----------
        enable : boolean
            Enables or disables the SQL trace
        """
        if enable:
            self.set_log_level()

        else:
            self.set_log_level("warn")
        self.trace_sql_active = enable

    def set_sql_trace(self, algo_object=None, algo=None, function=None):
        """
        Activates the trace for a certain algorithm and function. Any subsequent calls are placed
        under the respective substructure (algorithm -> function) in the trace dictionary.

        The algo_oject is the actual algorithm instance object from a algorithm class. This object is used
        in case the history of multiple calls to the same algorithm must be traced to check if
        the same object is being used or not.

        Parameters
        ----------
        algo_object : object
            The actual algorithm object.
        algo : string
            The algorithm name
        function : string
            The algorithm function
        """

        if self.trace_history and algo_object:
            new_id = 0
            # Check if we already have run the algo based on the instance object.
            if str(id(algo_object)) in self.trace_algo_tracker:
                new_id = self.trace_algo_tracker[str(id(algo_object))]
            elif algo in self.trace_algo_tracker:
                new_id = self.trace_algo_tracker[algo] + 1
                self.trace_algo_tracker[algo] = new_id
                self.trace_algo_tracker[str(id(algo_object))] = new_id
            else:
                new_id += 1
                self.trace_algo_tracker[algo] = new_id
                self.trace_algo_tracker[str(id(algo_object))] = new_id
            algo += str(new_id)
        self.trace_sql_algo = algo
        self.trace_sql_function = function

    def clean_trace_history(self):
        """
        Cleans the trace history.
        """
        self.trace_sql_log = {}

    def enable_trace_history(self, enable):
        """
        Enables the trace history on the algorithm level. This option allows for multiple calls to the same algorithm to be stored
        separately in the dictionary with a sequence number attached the algorithm name. This behavior
        is only available on the algorithm level. Using the same algorithm instance and calling the same function (such as Fit)
        twice would still overwrite the previous call.

        Parameters
        ----------
        enable : boolean
            Enables or disables the trace history.
        """

        self.trace_history = enable

    def get_sql_trace(self):
        """
        Returns the SQL trace dictionary.

        Returns
        -------
        dict
            A SQL Trace dictionary object.
        """

        return self.trace_sql_log

    def trace_sql(self, value=None):
        """
        Adds the SQL value to the current active algorithm and function in the SQL trace dictionary.

        Parameters
        ----------
        value : str
            The SQL entry.
        """

        self._trace_data(value, 'sql')

    def trace_object(self, value=None, sub_cat='nocat'):
        """
        Traces additional objects outside of the SQL entry. This option supports a more finegrained context
        and information than the SQL trace. For example, the input tables, output tables, and function
        being used for example. These are convenience objects to help you understand how the SQL is being structured. Generally
        speaking, these objects are dictionaries themselves and that is the current use case. However, it is not required to be.

        Parameters
        ----------
        value : object
            An ambiguous type of object that provides additional context outside of the SQL trace itself.
        sub_cat : str
            The subcategory or key that the value must be placed under. For example, 'output_tables'.

        """

        self._trace_data(value, sub_cat)

    def trace_sql_many(self, statement, param_entries=None):
        """
        Converts the insert executemany method on the hdbcli cursor object to multiple INSERT statements.
        This conversion ensures that only pure SQL is passed through.

        Parameters
        ----------
        statement : str
            The SQL statement.
        param_entries : list of tuples, or None
            The data in the INSERT statement.
        """

        if self.trace_sql_active:
            for param_entry in param_entries:
                processed_statement = statement
                for param_value in param_entry:
                    if isinstance(param_value, str):
                        processed_statement = processed_statement.replace('?', "'"+param_value+"'", 1)
                    else:
                        processed_statement = processed_statement.replace('?', str(param_value), 1)
                # Additional processing to assure proper SQL
                processed_statement = processed_statement.replace('None', 'null')
                processed_statement = processed_statement.replace('True', '1')
                processed_statement = processed_statement.replace('False', '0')
                self.trace_sql(processed_statement)

    def _trace_data(self, value=None, sub_cat='nocat'):
        """
        Stores the data in the SQL trace.

        Parameters
        ----------
        value : str or dict
            The value that must be stored in the dictionary.
        sub_cat : str
            The sub category under the function key where the data must be stored.
            the sub_cat becomes the key in the dictionary.

        """

        if self.trace_sql_active:
            if not self.trace_sql_algo in self.trace_sql_log:
                self.trace_sql_log[self.trace_sql_algo] = {}
            if not self.trace_sql_function in self.trace_sql_log[self.trace_sql_algo]:
                self.trace_sql_log[self.trace_sql_algo][self.trace_sql_function] = {}
            if not sub_cat in self.trace_sql_log[self.trace_sql_algo][self.trace_sql_function]:
                self.trace_sql_log[self.trace_sql_algo][self.trace_sql_function][sub_cat] = []

            self.trace_sql_log[self.trace_sql_algo][self.trace_sql_function][sub_cat].append(value)

class DataFrame(object):#pylint: disable=too-many-public-methods
    """
    Represents a frame that is backed by a database SQL statement.

    Parameters
    ----------
    connection_context : ConnectionContext
        The connection to the SAP HANA database instance.
    select_statement : str
        The SQL query backing the dataframe.

        .. note ::
            Parameters beyond ``connection_context`` and ``select_statement`` are intended for internal use. Do not rely on them; they may change without notice.
    """

    # pylint: disable=too-many-public-methods,too-many-instance-attributes
    _df_count = 0

    def __init__(self, connection_context, select_statement, _name=None):
        self.connection_context = connection_context
        self.select_statement = select_statement
        self._columns = None
        self._dtypes = None
        if _name is None:
            self._name = 'DT_{}'.format(DataFrame._df_count)
            self._quoted_name = quotename(self._name)
            DataFrame._df_count += 1
        else:
            self._name = _name
            self._quoted_name = quotename(_name)
        self._ttab_handling = 'unknown'
        self._ttab_reference = None
        self.source_table = None
        self.index = None
        self._validate_columns = True
        self._stats = None
        self._mock_table_structure = None

    @property
    def columns(self):
        """
        Lists the current DataFrame's column names. Computed lazily and cached.
        Each access to this property creates a new copy; mutating the list does not alter or corrupt the DataFrame.

        Parameters
        ----------
        None

        Returns
        -------
        list of str
            A list of column names.

        Examples
        --------
        df is a SAP HANA DataFrame.

        >>> df.columns
        ['sepal length (cm)',
         'sepal width (cm)',
         'petal length (cm)',
         'petal width (cm)',
         'target']
        """

        if self._columns is None:
            self._columns = self.__populate_columns()
        return self._columns[:]

    @property
    def shape(self):
        """
        Computes the shape of the SAP HANA DataFrame.

        Parameters
        ----------
        None

        Returns
        -------
        tuple
            (The number of rows, the number of columns) in the SAP HANA DataFrame.

        Examples
        --------
        df is a SAP HANA DataFrame.

        >>> df.shape
            (1, 3)
        """
        return [self.count(), len(self.columns)]

    @property
    def name(self):
        """
        Returns the name of the DataFrame. This value does not correspond to a SAP HANA table name.
        This value is useful for joining predicates when the joining DataFrames have columns with the same name.

        Parameters
        ----------
        None

        Returns
        -------
        str
            A str of DataFrame name.

        Examples
        --------
        df is a SAP HANA DataFrame.

        >>> df.name
        'DT_1'
        """

        return self._name

    @property
    def quoted_name(self):
        """
        Specifies the escaped name of the original DataFrame.
        Default-generated DataFrame names are safe to use in SQL without escaping, but names set with DataFrame.alias may require escaping.

        Parameters
        ----------
        None

        Returns
        -------
        str
            A str of DataFrame name.

        Examples
        --------
        df is a SAP HANA DataFrame.

        >>> df.quoted_name
        '"DT_1"'
        """

        return self._quoted_name

    @property
    def description(self):
        """
        Return cur.description from the select_statement query.
        """
        desc = None
        with self.connection_context.connection.cursor() as cur:
            cur.execute(self.select_statement)
            desc = cur.description
        return desc

    @property
    def description_ext(self):
        """
        Return cur.description_ext() from the select_statement query.
        """
        desc = None
        with self.connection_context.connection.cursor() as cur:
            if 'pyodbc' in str(type(cur)):
                return desc
            cur.execute(self.select_statement)
            desc = cur.description_ext()
        return desc

    def __getitem__(self, index):
        if isinstance(index, str):
            index = [index]
        if (isinstance(index, list) and
                all(isinstance(col, _STRING_TYPES) for col in index)):
            return self.select(*index)
        raise TypeError(
            '__getitem__ argument not understood: {!r}'.format(index))

    def __getattr__(self, attr):
        # 优先检查是否存在常规属性或方法
        if hasattr(super(), attr):
            return getattr(super(), attr)

        # 如果非列名访问，尝试获取常规属性
        if attr not in self.columns:
            raise AttributeError(
                f"'{type(self).__name__}' object has no attribute '{attr}'"
            )

        # 只有确认是列名时才返回列对象
        col_df = Column(self.connection_context, self.select(attr).select_statement)
        col_df._set_name(attr)
        col_df._set_base_select_statement(self.select_statement)
        return col_df

    def __populate_columns(self):
        if 'WITH HINT' in self.select_statement:
            with_hint_clause = re.findall(r'WITH HINT\(.+\)', self.select_statement)
            if len(with_hint_clause) > 0:
                with_hint_clause = with_hint_clause[0]
            else:
                with_hint_clause = re.findall(r'WITH HINT \(.+\)', self.select_statement)
                if len(with_hint_clause) > 0:
                    with_hint_clause = with_hint_clause[0]
                else:
                    with_hint_clause = ''
            nodata_select = 'SELECT * FROM ({}) WHERE 0=1 {}'.format(self.select_statement.replace(with_hint_clause, ''), with_hint_clause)
        elif 'with hint' in self.select_statement:
            with_hint_clause = re.findall(r'with hint\(.+\)', self.select_statement)
            if len(with_hint_clause) > 0:
                with_hint_clause = with_hint_clause[0]
            else:
                with_hint_clause = re.findall(r'with hint \(.+\)', self.select_statement)
                if len(with_hint_clause) > 0:
                    with_hint_clause = with_hint_clause[0]
                else:
                    with_hint_clause = ''
            nodata_select = 'SELECT * FROM ({}) WHERE 0=1 {}'.format(self.select_statement.replace(with_hint_clause, ''), with_hint_clause)
        else:
            nodata_select = 'SELECT * FROM ({}) WHERE 0=1'.format(
            self.select_statement)
        with self.connection_context.connection.cursor() as cur:
            cur.execute(nodata_select)
            return [descr[0] for descr in cur.description]

    def __run_query(self, query, fetch_size=32000):
        """
        Runs a query over the DataFrame's connection.

        Parameters
        ----------
        query : str
            The SQL statement to run.
        fetch_size : int
            Specify the fetch size to improve the fetch performance.

            Defaults to 32000.

        Returns
        -------
        list of hdbcli.resultrow.ResultRow objects. A list of query results.
        """
        with self.connection_context.connection.cursor() as cur:
            if not self.connection_context.pyodbc_connection:
                cur.setfetchsize(fetch_size)
            cur.execute(query)
            return cur.fetchall()


    @staticmethod
    def _get_list_of_str(cols, message):
        """
        Generates a list of values after checking that all values in the list are strings. If cols is a string, then a list is created and returned.

        Parameters
        ----------
        cols : str or list of str
            The list of columns to check.
        message : str
            An error message in case the type of the cols parameter is incorrect.

        Returns
        -------
        list
            cols if it is already a list; otherwise, a list containing cols.
        """

        if isinstance(cols, _STRING_TYPES):
            cols = [cols]
        if (not cols or not isinstance(cols, list) or
                not all(isinstance(col, _STRING_TYPES) for col in cols)):
            raise TypeError(message)
        return cols

    def _validate_columns_in_dataframe(self, cols):
        """
        Checks if the specified columns are in the DataFrame.
        Raises an error if any column in cols is not in the current DataFrame.

        Parameters
        ----------
        cols : list
            The list of columns to check.
        """
        if self._validate_columns:
            valid_set = set(self.columns)
            check_names = set(col for col in cols
                            if isinstance(col, _STRING_TYPES))
            if not valid_set.issuperset(check_names):
                invalid_names = [name for name in check_names
                                if name not in valid_set]
                message = "Column(s) not in DataFrame: {}".format(invalid_names)
                raise ValueError(message)

    def declare_lttab_usage(self, usage):
        """
        Declares whether this DataFrame makes use of local temporary tables.

        Some SAP HANA PAL execution routines can execute more efficiently if they know up front whether a DataFrame's SELECT statement requires
        access to local temporary tables.

        Parameters
        ----------
        usage : bool
            Specifies whether this DataFrame uses local temporary tables.
        """

        if self._ttab_handling == 'safe':
            if usage:
                raise ValueError(
                    "declare_lttab_usage(True) called on a DataFrame " +
                    "believed to involve no local temporary tables.")
        elif self._ttab_handling in ('unsafe', 'ttab'):
            if not usage:
                raise ValueError(
                    "declare_lttab_usage(False) called on a DataFrame " +
                    "believed to involve local temporary tables.")
        else:
            self._ttab_handling = 'unsafe' if usage else 'safe'

    def disable_validate_columns(self):
        """
        Disable the column validation.
        """
        self._validate_columns = False

    def enable_validate_columns(self):
        """
        Enable the column validation.
        """
        self._validate_columns = True

    def _propagate_safety(self, other):
        # pylint:disable=protected-access
        if self._ttab_handling == 'safe':
            other._ttab_handling = 'safe'
        elif self._ttab_handling in ('ttab', 'unsafe'):
            other._ttab_handling = 'unsafe'

    def _df(self, select_statement, name=None, propagate_safety=False):
        # Because writing "DataFrame(self.connection_context" everywhere
        # is way too verbose.
        retval = DataFrame(self.connection_context, select_statement, name)
        if propagate_safety:
            self._propagate_safety(retval)
        return retval

    def add_vector(self, text_col, text_type='DOCUMENT', model_version='SAP_NEB.20240715', embed_col='EMBEDDING'):
        """
        Returns a new SAP HANA DataFrame with a added vector column.

        Parameters
        ----------
        text_col : str
            The name of the text column.
        text_type : {'DOCUMENT', 'QUERY'}, optional
            The type of text column.

            Defaults to 'DOCUMENT'.
        model_version : str, optional
            Text Embedding Model version. Options are 'SAP_NEB.20240715' and 'SAP_GXY.20250407'.

            Defaults to 'SAP_NEB.20240715'.
        embed_col : str, optional
            The name of the new column.

            Defaults to 'EMBEDDING'.
        """
        # check text_type
        if text_type not in ['DOCUMENT', 'QUERY']:
            raise ValueError("text_type must be 'DOCUMENT' or 'QUERY'")
        select_statement = f"SELECT *, VECTOR_EMBEDDING(\"{text_col}\", '{text_type}', '{model_version}') AS \"{embed_col}\" FROM ({self.select_statement})"
        return self._df(select_statement, propagate_safety=True)

    def add_id(self, id_col=None, ref_col=None, starting_point=1):
        """
        Returns a new SAP HANA DataFrame with a added <id_col> column.

        Parameters
        ----------
        id_col : str, optional
            The name of new ID column.

            Defaults to "ID".
        ref_col : str or list of str, optional
            The id is generated based on ref_col.
        starting_point : int, optional
            The starting point of ID.

            Defaults to 1.

        Returns
        -------
        DataFrame
            A new SAP HANA DataFrame with a added <id_col> column.

        Examples
        --------
        df is a SAP HANA DataFrame.

        >>> df.collect()
            X    Y
        0  20   30
        1  40    5

        >>> df.add_id(id_col='ID')
            ID    X    Y
        0    1   20   30
        1    2   40    5
        """
        starting_point = starting_point - 1
        if id_col is None:
            id_col = "ID"
        order_by = ''
        if ref_col is not None:
            if isinstance(ref_col, (list, tuple)):
                order_by = "ORDER BY {} ASC".format(", ".join(list(map(quotename, ref_col))))
            else:
                order_by = "ORDER BY {} ASC".format(quotename(ref_col))
        select_statement = "SELECT CAST(ROW_NUMBER() OVER({}) AS INTEGER) + {} AS {}, * FROM ({})".format(order_by,
                                                                                                          starting_point,
                                                                                                          quotename(id_col),
                                                                                                          self.select_statement)
        return self._df(select_statement, propagate_safety=True)

    def add_constant(self, column_name, value):
        """
        Adds a new column with constant value.

        Parameters
        ----------
        columns_name: str
            The name of column to be added.

        value: str or numeric
            The constant value to be added.
        """
        if isinstance(value, (int, float)):
            select_statement = "SELECT *, {} AS {} FROM ({})".format(value,
                                                                     quotename(column_name),
                                                                     self.select_statement)
        else:
            select_statement = "SELECT *, '{}' AS {} FROM ({})".format(value,
                                                                       quotename(column_name),
                                                                       self.select_statement)
        return self._df(select_statement, propagate_safety=True)

    def alias(self, alias):
        """
        Returns a new SAP HANA DataFrame with an alias set.

        Parameters
        ----------
        alias : str
            The name of the DataFrame.

        Returns
        -------
        DataFrame
            A SAP HANA DataFrame with an alias set.

        See Also
        --------
        DataFrame.rename_columns : For renaming individual columns.

        """

        retval = self._df(self.select_statement, alias)
        # pylint:disable=protected-access
        retval._ttab_handling = self._ttab_handling
        retval._ttab_reference = self._ttab_reference
        return retval

    def count(self):
        """
        Computes the number of rows in the SAP HANA DataFrame.

        Parameters
        ----------
        None

        Returns
        -------
        int
            The number of rows in the SAP HANA DataFrame.

        Examples
        --------
        df is a SAP HANA DataFrame.

        >>> df.count()
        150
        """

        new_select_statement = "SELECT COUNT(*) FROM ({})".format(self.select_statement)
        try:
            results = self.__run_query(new_select_statement)
        except dbapi.Error as exc:
            logger.error("Failed to get row count for the current Dataframe, %s", exc)
            raise
        except Exception as exc:
            logger.error("Failed to get row count for the current Dataframe, %s", str(exc.args[1]))
            raise
        return results[0][0]

    def diff(self, index, periods=1, diff_datetime="days"):
        """
        Returns a new SAP HANA DataFrame with differenced values.

        Parameters
        ----------
        index : int or str
            Index of the SAP HANA DataFrame.
        periods : int, optional
            Periods to shift for calculating difference, accepts negative values.

            Defaults to 1.
        diff_datetime : {"years", "months", "days", "seconds", "nano100", "workdays"} or dict, optional
            Specifies the difference type. It can also support diff_datetime for particular columns.

            Defaults to "days".

        Returns
        -------
        DataFrame
            DataFrame with differenced values. No calculation happens if it contains string.
        """
        def _check_type(_type):
            if "INT" in _type.upper():
                return "numeric"
            if "DOUBLE" in _type.upper():
                return "numeric"
            if "DECIMAL" in _type.upper():
                return "numeric"
            if "FLOAT" in _type.upper():
                return "numeric"
            if "BOOL" in _type.upper():
                return "numeric"
            if "NUMBER" in _type.upper():
                return "numeric"
            if "TIME" in _type.upper():
                return "datetimes"
            if "DATE" in _type.upper():
                return "datetimes"
            return "unsupported"
        tab_tt = self.get_table_structure()
        diff_type = {}
        if isinstance(diff_datetime, dict):
            for column, _type in tab_tt.items():
                if column not in index:
                    if column in diff_datetime:
                        diff_type[column] = diff_datetime[column]
                    else:
                        if _check_type(_type) == "numeric":
                            diff_type[column] = "numeric"
                        elif _check_type(_type) == "datetimes":
                            diff_type[column] = "days"
                        else:
                            diff_type[column] = "unsupported"
        else:
            for column, _type in tab_tt.items():
                if column not in index:
                    if _check_type(_type) == "numeric":
                        diff_type[column] = "numeric"
                    elif _check_type(_type) == "datetimes":
                        diff_type[column] = diff_datetime
                    else:
                        diff_type[column] = "unsupported"
        select_statement = 'SELECT '
        for column in self.columns:
            if column not in index:
                if diff_type[column] == "numeric":
                    if periods >= 0:
                        select_statement = select_statement + '{col} - LAG({col}, {periods}) OVER(ORDER BY {idx}) {col}, '\
                        .format(col=quotename(column), periods=periods, idx=quotename(index))
                    else:
                        select_statement = select_statement + 'LAG({col}, {periods}) OVER(ORDER BY {idx}) - {col} {col}, '\
                        .format(col=quotename(column), periods=-1 * periods, idx=quotename(index))
                elif diff_type[column] == "unsupported":
                    pass
                else:
                    if periods >= 0:
                        select_statement = select_statement + '{dt_op}(LAG({col}, {periods}) OVER(ORDER BY {idx}), {col}) {col}, '\
                        .format(col=quotename(column), periods=periods, idx=quotename(index), dt_op=diff_type[column].upper() + "_BETWEEN")
                    else:
                        select_statement = select_statement + '{dt_op}({col}, LAG({col}, {periods}) OVER(ORDER BY {idx})) {col}, '\
                        .format(col=quotename(column), periods=-1 * periods, idx=quotename(index), dt_op=diff_type[column].upper() + "_BETWEEN")
        select_statement = select_statement[:-2]
        select_statement = select_statement + ' FROM ({})'.format(self.select_statement)
        return self._df(select_statement, propagate_safety=True)

    def drop(self, cols):
        """
        Returns a new SAP HANA DataFrame without the specified columns.

        Parameters
        ----------
        cols : list of str
            The list of column names to be dropped.

        Returns
        -------
        DataFrame
            A new SAP HANA DataFrame that retains only the columns not listed in ``cols``.

        Examples
        --------
        >>> df.collect()
           A  B
        0  1  3
        1  2  4
        >>> df.drop(['B']).collect()
           A
        0  1
        1  2
        """
        if isinstance(cols, str):
            cols = [cols]
        dropped_set = set(cols)
        if not dropped_set.issubset(self.columns):
            own_columns = set(self.columns)
            invalid_columns = [col for col in cols if col not in own_columns]
            raise ValueError("Can't drop nonexistent column(s): {}".format(invalid_columns))

        cols_kept = [quotename(col) for col in self.columns if col not in dropped_set]
        cols_kept = ', '.join(cols_kept)
        # TO DO: if cols_kept are the same as self.columns, then return nothing/self
        select_template = 'SELECT {} FROM ({}) AS {}'
        new_select_statement = select_template.format(
            cols_kept, self.select_statement, self.quoted_name)
        return self._df(new_select_statement, propagate_safety=True)

    def _generate_colname(self, prefix='GEN_COL'):
        # If the input prefix is safe to use unquoted, the output name
        # will be safe to use unquoted too.
        # Otherwise, you'll probably want to quotename the result before
        # using it in SQL.
        if not prefix:
            prefix = 'GEN_COL'
        if prefix not in self.columns:
            return prefix
        for i in range(1+len(self.columns)):
            gen_col = '{}_{}'.format(prefix, i)
            if gen_col not in self.columns:
                return gen_col
        # To get here, we would have to try more new names than this dataframe
        # has columns, and we would have to find that all of those names
        # were taken.
        raise AssertionError("This shouldn't be reachable.")

    def distinct(self, cols=None):
        """
        Returns a new SAP HANA DataFrame with distinct values for the specified columns.
        If no columns are specified, then the distinct row values from all columns are returned.

        Parameters
        ----------
        cols : str or list of str, optional
            A column or list of columns to consider when getting distinct
            values. Defaults to use all columns.

        Returns
        -------
        DataFrame
            The DataFrame with distinct values for cols.

        Examples
        --------
        Input:

        >>> df.collect()
           A  B    C
        0  1  A  100
        1  1  A  101
        2  1  A  102
        3  1  B  100
        4  1  B  101
        5  1  B  102
        6  1  B  103
        7  2  A  100
        8  2  A  100

        Distinct values in a column:

        >>> df.distinct("B").collect()
           B
        0  A
        1  B

        Distinct values of a subset of columns:

        >>> df.distinct(["A", "B"]).collect()
           A  B
        0  1  B
        1  2  A
        2  1  A

        Distinct values of the entire data set:

        >>> df.distinct().collect()
           A  B    C
        0  1  A  102
        1  1  B  103
        2  1  A  101
        3  2  A  100
        4  1  B  101
        5  1  A  100
        6  1  B  100
        7  1  B  102
        """

        if cols is not None:
            msg = 'Parameter cols must be a string or a list of strings'
            cols = DataFrame._get_list_of_str(cols, msg)
            self._validate_columns_in_dataframe(cols)
        else:
            cols = self.columns
        select_statement = "SELECT DISTINCT {} FROM ({}) AS {}".format(
            ', '.join([quotename(col) for col in cols]),
            self.select_statement, self.quoted_name)
        return self._df(select_statement, propagate_safety=True)

    def drop_duplicates(self, subset=None):
        """
        Returns a new SAP HANA DataFrame with duplicate rows removed. All columns in the
        DataFrame are returned. There is no way to keep specific duplicate rows.

        .. warning::
           Specifying a non-None value of ``subset`` may produce an unstable \
           DataFrame, the contents of which may be different every time you \
           look at it. Specifically, if two rows are duplicates in their \
           ``subset`` columns and have different values in other columns, \
           Then a different row could be picked every time you look at the result.

        Parameters
        ----------
        subset : list of str, optional
            A list of columns to consider when deciding whether rows are \
            duplicates of each other. Defaults to use all columns.

        Returns
        -------
        DataFrame
            A DataFrame with only one copy of duplicate rows.

        Examples
        --------
        Input:

        >>> df.collect()
           A  B    C
        0  1  A  100
        1  1  A  101
        2  1  A  102
        3  1  B  100
        4  1  B  101
        5  1  B  102
        6  1  B  103
        7  2  A  100
        8  2  A  100

        Drop duplicates based on the values of a subset of columns:

        >>> df.drop_duplicates(["A", "B"]).collect()
           A  B    C
        0  1  A  100
        1  1  B  100
        2  2  A  100

        Distinct values on the entire data set:

        >>> df.drop_duplicates().collect()
           A  B    C
        0  1  A  102
        1  1  B  103
        2  1  A  101
        3  2  A  100
        4  1  B  101
        5  1  A  100
        6  1  B  100
        7  1  B  102
        """

        if subset is None:
            return self._df("SELECT DISTINCT * FROM ({}) AS {}".format(
                self.select_statement, self.quoted_name), propagate_safety=True)

        if not subset:
            raise ValueError("drop_duplicates requires at least one column in subset")

        keep_columns = ', '.join([quotename(col) for col in self.columns])
        partition_by = ', '.join([quotename(col) for col in subset])
        seqnum_col = quotename(self._generate_colname('SEQNUM'))
        seqnum_template = "SELECT *, ROW_NUMBER() OVER (PARTITION BY {}) AS {} FROM ({})"
        select_with_seqnum = seqnum_template.format(
            partition_by, seqnum_col, self.select_statement)
        new_select_statement = "SELECT {} FROM ({}) WHERE {} = 1".format(
            keep_columns, select_with_seqnum, seqnum_col)
        return self._df(new_select_statement, propagate_safety=True)

    def dropna(self, how=None, thresh=None, subset=None):
        # need to test
        """
        Returns a new DataFrame with NULLs removed.

        Parameters
        ----------
        how : {'any', 'all'}, optional
            If provided, 'any' eliminates rows with any NULLs, \
            and 'all' eliminates rows that are entirely NULLs. \
            If neither ``how`` nor ``thresh`` are provided, ``how`` \
            defaults to 'any'.
        thresh : int, optional
            If provided, rows with fewer than ``thresh`` non-NULL values \
            are dropped. \
            You cannot specify both ``how`` and ``thresh``.
        subset : list of str, optional
            The columns to consider when looking for NULLs. Values in
            other columns are ignored, whether they are NULL or not.
            Defaults to all columns.

        Returns
        -------
        DataFrame
            A new SAP HANA DataFrame with a SELECT statement that removes NULLs.

        Examples
        --------
        Dropping rows with any NULL:

        >>> df.collect()
             A    B    C
        0  1.0  3.0  5.0
        1  2.0  4.0  NaN
        2  3.0  NaN  NaN
        3  NaN  NaN  NaN
        >>> df.dropna().collect()
             A    B    C
        0  1.0  3.0  5.0

        Dropping rows that are entirely nulls:

        >>> df.dropna(how='all').collect()
             A    B    C
        0  1.0  3.0  5.0
        1  2.0  4.0  NaN
        2  3.0  NaN  NaN

        Dropping rows with less than 2 non-null values:

        >>> df.dropna(thresh=2).collect()
             A    B    C
        0  1.0  3.0  5.0
        1  2.0  4.0  NaN
        """

        if how is not None and thresh is not None:
            raise ValueError("Cannot provide both how and thresh.")

        if subset is not None:
            cols = subset
        else:
            cols = self.columns

        cols = [quotename(col) for col in cols]
        if thresh is None:
            if how in {'any', None}:
                and_or = ' OR '
            elif how == 'all':
                and_or = ' AND '
            else:
                raise ValueError("Invalid value of how: {}".format(how))
            drop_if = and_or.join('{} IS NULL'.format(col) for col in cols)
            keep_if = 'NOT ({})'.format(drop_if)
            retval = self.filter(keep_if)
            self._propagate_safety(retval)
            return retval

        count_expression = '+'.join(
            ['(CASE WHEN {} IS NULL THEN 0 ELSE 1 END)'.format(col) for col in cols])
        count_colname = self._generate_colname('CT')
        select_with_count = 'SELECT *, ({}) AS {} FROM ({}) {}'.format(
            count_expression, count_colname, self.select_statement, self.quoted_name)
        projection = ', '.join([quotename(col) for col in self.columns])
        new_select_statement = 'SELECT {} FROM ({}) WHERE {} >= {}'.format(
            projection, select_with_count, count_colname, thresh)
        return self._df(new_select_statement, propagate_safety=True)

    def deselect(self, cols):
        """
        Returns a new DataFrame without columns derived from the current DataFrame.

        Parameters
        ----------
        cols : str or tuple/list of str.
            The columns are excluded in the new DataFrame.

        Returns
        -------
        DataFrame
            A new DataFrame object excluding the specified columns.

        Examples
        --------
        Input:

        >>> df.collect()
           A  B  C
        0  1  2  3

        Selecting a subset of existing columns:

        >>> df.deselect(['A', 'B']).collect()
           C
        0  3
        """
        columns = []
        if isinstance(cols, str):
            cols = [cols]
        for col in self.columns:
            if col not in cols:
                columns.append(col)
        return self.select(*columns)

    def has_constant_columns(self):
        """
        Returns a sequence of constant columns in the DataFrame.
        """
        constant_column = []
        if len(self.columns) > 1:
            df_max = self.max()
            for col_min, val_min in self.min().items():
                if val_min == df_max[col_min]:
                    constant_column.append(col_min)
        else:
            if self.min() == self.max():
                constant_column = self.columns
        if constant_column:
            return constant_column
        return False

    def drop_constant_columns(self):
        """
        Returns a DataFrame without constant columns.
        """
        constant_cols = self.has_constant_columns()
        if constant_cols:
            if len(constant_cols) == 1:
                logger.warning("There is a constant column {} that has been dropped.".format(", ".join(constant_cols)))
            else:
                logger.warning("There are constant columns {} that have been dropped.".format(", ".join(constant_cols)))
            return self.deselect(constant_cols)
        return self

    def dtypes(self, subset=None):
        """
        Returns a sequence of tuples describing the DataFrame's SQL types.

        The tuples list the name, SQL type name, display_size, internal_size,
        precision and scale corresponding to the DataFrame's columns.

        Parameters
        ----------
        subset : list of str, optional
            The columns that the information is generated from.
            Defaults to all columns.

        Returns
        -------
        dtypes : list of tuples
            Each tuple consists of the name, SQL type name, display_size, internal_size,
            precision and scale for one of the DataFrame's columns. The list is in the order
            specified by the ``subset``, or in the DataFrame's original column
            order if a ``subset`` is not provided.
        """

        if self._dtypes is None:
            with self.connection_context.connection.cursor() as cur:
                cur.execute(self.select_statement)
                if self.connection_context.pyodbc_connection:
                    self._dtypes = [(c[0],
                                     "VARCHAR" if c[1].__name__.upper() == "STR" else c[1].__name__.upper(),
                                     c[3],
                                     c[3],
                                     c[4],
                                     c[5]) for c in cur.description]
                else:
                    self._dtypes = [(c[0], TYPE_CODES[c[1]], c[2], c[3], c[4], c[5]) for c in cur.description]
        if subset is None:
            return self._dtypes[:]
        dtype_map = {descr[0]: descr for descr in self._dtypes}
        return [dtype_map[col] for col in subset]

    _ARBITRARY_PSEUDOTOKEN_LIMIT = 200

    def empty(self):
        """
        Returns True if this DataFrame has 0 rows.

        Parameters
        ----------
        None

        Returns
        -------
        bool
            True if the DataFrame is empty.

        Notes
        -----
        If a DataFrame contains only NULLs, it is not considered empty.

        Examples
        --------
        >>> df1.collect()
        Empty DataFrame
        Columns: [ACTUAL, PREDICT]
        Index: []
        >>> df1.empty()
        True

        >>> df2.collect()
          ACTUAL PREDICT
        0   None    None
        >>> df2.empty()
        False
        """
        return self.count() == 0

    def _token_validate(self, sql):
        """
        Calls IS_SQL_INJECTION_SAFE on input. Does not guarantee injection safety.

        Parameters
        ----------
        sql : str
            A SQL statement.

        Raises
        ------
        hana_ml.ml_exceptions.BadSQLError
            If IS_SQL_INJECTION_SAFE returns 0.

        Notes
        -----
        This method does not guarantee injection safety. Any parts of this library
        that take SQL statements, expressions, or predicates are not safe
        against SQL injection. This method only catches some instances of comments
        and malformed tokens in the input.

        IS_SQL_INJECTION_SAFE may produce false positives or false negatives.
        """

        with self.connection_context.connection.cursor() as cur:
            cur.execute('SELECT IS_SQL_INJECTION_SAFE(?, ?) FROM DUMMY',
                        (sql, self._ARBITRARY_PSEUDOTOKEN_LIMIT))
            val = cur.fetchone()[0]
        if not val:
            msg = 'SQL token validation failed for string {!r}'.format(sql)
            raise BadSQLError(msg)

    def filter(self, condition):
        """
        Selects rows that match the given condition.

        Very little checking is done on the condition string.
        Use only with trusted inputs.

        Parameters
        ----------
        condition : str or list
            A filter condition. Format as SQL <condition>.

        Returns
        -------
        DataFrame
            A DataFrame with rows that match the given condition.

        Raises
        ------
        hana_ml.ml_exceptions.BadSQLError
            If comments or malformed tokens are detected in ``condition``.
            May have false positives and false negatives.

        Examples
        --------
        >>> df.collect()
           A  B
        0  1  3
        1  2  4
        >>> df.filter('B < 4').collect()
           A  B
        0  1  3
        """
        if isinstance(condition, Column):
            condition = [condition]
        if isinstance(condition, list):
            new_condition = " AND ".join(list(map(str, condition)))
            return self._df('SELECT * FROM ({}) AS {} WHERE {}'.format(
            self.select_statement, self.quoted_name, new_condition))
        return self._df('SELECT * FROM ({}) AS {} WHERE {}'.format(
            self.select_statement, self.quoted_name, condition))

    def has(self, col):
        """
        Returns True if a column is in the DataFrame.

        Parameters
        ----------
        col : str
            The name of column to search in the projection list of this DataFrame.

        Returns
        -------
        bool
            Returns True if the column exists in the DataFrame's projection list.

        Examples
        --------
        >>> df.columns
        ['A', 'B']
        >>> df.has('A')
        True
        >>> df.has('C')
        False
        """

        # df.has(col) doesn't really give much benefit over using
        # col in df.columns directly. It may not be worth
        # having this method at all.
        return col in self.columns

    def head(self, n=1): #pylint: disable=invalid-name
        """
        Returns a DataFrame of the first ``n`` rows in the current DataFrame.

        Parameters
        ----------
        n : int, optional
            The number of rows returned.

            Defaults to 1.

        Returns
        -------
        DataFrame
            A new DataFrame of the first ``n`` rows of the current DataFrame.

        """
        if self.connection_context._abap_sql:
            head_select = 'SELECT * FROM ({}) UP TO {} ROWS'.format(self.select_statement, n)
            return self._df(head_select, propagate_safety=True)
        head_select = 'SELECT TOP {} * FROM ({}) dt'.format(
            n, self.select_statement)
        return self._df(head_select, propagate_safety=True)

    def hasna(self, cols=None):
        """
        Returns True if a DataFrame contains NULLs.

        Parameters
        ----------
        cols : str or list of str, optional
            A column or list of columns to be checked for NULL values.
            Defaults to all columns.

        Returns
        -------
        bool
            True if this DataFrame contains NULLs.

        Examples
        --------

        >>> df1.collect()
          ACTUAL PREDICT
        0   1.0    None

        >>> df1.hasna()
        True
        """

        if cols is not None:
            msg = 'Parameter cols must be a string or a list of strings'
            cols = DataFrame._get_list_of_str(cols, msg)
            self._validate_columns_in_dataframe(cols)
        else:
            cols = self.columns

        count_cols = []
        for col in cols:
            quoted_col = quotename(col)
            count_cols.append("count({})".format(quoted_col))
        minus_expression = ' + '.join(count_cols)

        count_statement = "SELECT COUNT(*)*{} - ({}) FROM ({})".format(
            len(cols),
            minus_expression,
            self.select_statement)
        try:
            count = self.__run_query(count_statement)
        except dbapi.Error as exc:
            logger.error("Failed to get NULL value count for the current Dataframe, %s", exc)
            raise
        except Exception as exc:
            logger.error("Failed to get NULL value count for the current Dataframe, %s", str(exc.args[1]))
            raise
        return count[0][0] > 0

    def fillna(self, value, subset=None):
        """
        Returns a DataFrame with NULLs replaced with a specified value.

        Parameters
        ----------
        value : int or float
            The value that replaces NULL. ``value`` should have a type that is
            appropriate for the selected columns.
        subset : list of str, optional
            A list of columns whose NULL values will be replaced.
            Defaults to all columns.

        Returns
        -------
        DataFrame
            A new DataFrame with the NULL values replaced.
        """

        if subset is not None:
            filled_set = set(subset)
        else:
            filled_set = set(self.columns)
        if not isinstance(value, (float, str) + _INTEGER_TYPES):
            raise TypeError("Fill values currently must be ints, str or floats.")
        #untouched columns
        select_values = []
        tab_struct = self.get_table_structure()
        for col in self.columns:
            quoted_col = quotename(col)
            if col in filled_set:
                #pylint: disable=W0511
                if isinstance(value, str) and ('VARCHAR' in tab_struct[col].upper() or
                                               'TIME' in tab_struct[col].upper() or
                                               'DATE' in tab_struct[col].upper() or
                                               'TEXT' in tab_struct[col].upper()):
                    select_values.append("COALESCE({0}, '{1}') AS {0}".format(quoted_col, value))
                elif isinstance(value, (int, float)) and ('INT' in tab_struct[col].upper() or
                                                          'DOUBLE' in tab_struct[col].upper() or
                                                          'DECIMAL' in tab_struct[col].upper()):
                    select_values.append("COALESCE({0}, {1}) AS {0}".format(quoted_col, value))
                else:
                    select_values.append(quoted_col)
            else:
                select_values.append(quoted_col)
        cols = ', '.join(select_values)
        new_select_statement = 'SELECT {} FROM ({}) dt'.format(cols, self.select_statement)
        return self._df(new_select_statement, propagate_safety=True)

    def get_table_structure(self):
        """
        Returns dict format table structure.
        """
        if self._mock_table_structure:
            return self._mock_table_structure
        table_structure = {}
        for item in self.dtypes():
            if 'VARCHAR' in item[1].upper():
                table_structure[item[0]] = "{}({})".format(item[1], item[4])
            elif item[1].upper() == 'DECIMAL':
                table_structure[item[0]] = "{}({}, {})".format(item[1], item[4], item[5])
            else:
                table_structure[item[0]] = item[1]
        return table_structure

    def join(self, other, condition=None, how='inner', select=None):
        """
        Returns a new DataFrame that is a join of the current DataFrame with
        another specified DataFrame.

        Parameters
        ----------
        other : DataFrame or list of DataFrame
            The DataFrame to join with.

        condition : str, optional
            The join predicate. If index has been set, use the index as key to join.

            Defaults to None.

        how : {'inner', 'left', 'right', 'outer', 'cross'}, optional
            The type of join. Defaults to 'inner'.

            Defaults to 'inner'.

        select : list, optional
            If provided, each element specifies a column in the result.
            A string in the ``select`` list should be the name of a column in
            one of the input DataFrames. A (expression, name) tuple creates
            a new column with the given name, computed from the given
            expression.

            If this value is not provided, defaults to selecting all columns from both
            DataFrames, with the left DataFrame's columns first.

        Returns
        -------
        DataFrame
            A new DataFrame object made from the join() of the current DataFrame
            with another DataFrame.

        Raises
        ------
        hana_ml.ml_exceptions.BadSQLError
            If comments or malformed tokens are detected in ``condition``
            or in a column expression.
            May have false positives and false negatives.

        Examples
        --------
        Use the expression selection functionality to disambiguate duplicate
        column names in a join():

        >>> df1.collect()
           A  B    C
        0  1  2  3.5
        1  2  4  5.6
        2  3  3  1.1

        >>> df2.collect()
           A  B     D
        0  2  1  14.0
        1  3  4   5.6
        2  4  3   0.0

        Old method:

        >>> df1.alias('L').join(df2.alias('R'), 'L.A = R.A', select=[
        ...     ('L.A', 'A'),
        ...     ('L.B', 'B_LEFT'),
        ...     ('R.B', 'B_RIGHT'),
        ...     'C',
        ...     'D']).collect()
           A  B_LEFT  B_RIGHT    C     D
        0  2       4        1  5.6  14.0
        1  3       3        4  1.1   5.6

        New method:

        >>> df1.set_index("A").join(df2.rename_columns({"B":"B2"}).set_index("A")).collect()
           A  B B2    C   D
        0  2  4  1  5.6  14
        1  3  3  4  1.1 5.6
        """

        #if left and right joins are improper (ex: int on string) then there will be a sql error
        join_type_map = {
            'inner': 'INNER',
            'left': 'LEFT OUTER',
            'right': 'RIGHT OUTER',
            'outer': 'FULL OUTER',
            'cross': 'CROSS'
        }
        join_type = join_type_map[how]
        if condition is not None:
            if how not in ['inner', 'outer', 'left', 'right']:
                raise ValueError('Invalid value for "how" argument: {!r}'.format(how))
            self._token_validate(condition)
            on_clause = 'ON ' + condition
            if select is None:
                projection_string = '*'
            else:
                projection_string = self._stringify_select_list(select)

            select_template = 'SELECT {} FROM ({}) AS {} {} JOIN ({}) AS {} {}'
            new_select_statement = select_template.format(
                projection_string,
                self.select_statement, self.quoted_name, join_type,
                other.select_statement, other.quoted_name, on_clause)
        else:
            if not isinstance(other, (list, tuple)):
                other = [other]
            if self.index is None:
                raise ValueError("Index has not been set!")
            sel_part1 = "SELECT " + ", ".join(['T0.' + quotename(col) for col in self.columns]) + ", "
            sel_part2 = "\nFROM ({}) T0 ".format(self.select_statement)
            for idx, hana_df in enumerate(other):
                if hana_df.index is None:
                    raise ValueError("Index has not been set!")
                if isinstance(self.index, (list, tuple)):
                    sel_part1 = sel_part1 + ", ".join(["T{}.".format(idx + 1) + quotename(col) for col in hana_df.columns if col not in hana_df.index])
                    conditions = []
                    if len(self.index) != len(hana_df.index):
                        raise ValueError("Index list lenth does not match!")
                    for idx_item, other_idex_item in zip(self.index, hana_df.index):
                        conditions.append("T0.{0} = T{1}.{2}".format(quotename(idx_item), idx + 1, quotename(other_idex_item)))
                    sel_part2 = sel_part2 + "{0} JOIN ({1}) T{2}\n {3} {4}\n".format(join_type,
                           hana_df.select_statement,
                           idx + 1,
                           "ON" if how != "cross" else "",
                           " AND ".join(conditions) if how != "cross" else "")
                else:
                    sel_part1 = sel_part1 + ", ".join(["T{}.".format(idx + 1) + quotename(col) for col in hana_df.columns if col != hana_df.index])
                    _on_clause = "ON T0.{0} = T{1}.{2}".format(quotename(self.index), idx + 1, quotename(hana_df.index))
                    sel_part2 = sel_part2 + "{0} JOIN ({1}) T{2}\n {3}\n".format(join_type,
                     hana_df.select_statement,
                     idx + 1,
                     _on_clause if how != "cross" else "")
                if idx < len(other) - 1:
                    sel_part1 = sel_part1 + ", "
            new_select_statement = sel_part1 + sel_part2
        return self._df(new_select_statement)

    def set_name(self, name):
        """
        Sets the name of the DataFrame.

        Parameters
        ----------
        name : str
            The name of dataframe.
        """
        self._name = name

    def set_index(self, keys):
        """
        Sets the index of the DataFrame.

        Parameters
        ----------
        keys : str or list of str
            This parameter can be either a single column key or a list of column keys.
        """
        if isinstance(keys, str):
            if keys not in self.columns:
                raise ValueError("keys not in columns!")
        else:
            for key in keys:
                if key not in self.columns:
                    raise ValueError("keys not in columns!")
        self.index = keys
        return self

    def smart_save(self, table, schema=None, table_type=None, force=False, save_source=True, append=False, data_lake=False, data_lake_container='SYSRDL#CG', view_structure=None):
        """
        Enhancement to save function. To save in a smart way by checking the table if exists in the select_statement.
        If so, its content will be stored in a temporary table before dropping when force=True.

        Parameters
        ----------
        table : str
            The table name or (schema name, table name) tuple. If no schema
            is provided, then the table or view is created in the current
            schema.
        schema : str, optional
            The schema name. If not provided, the current schema is used.
        table_type : str, optional
            The type of table to create. The value is case insensitive.

            Permanent table options:

              - "ROW"
              - "COLUMN"
              - "HISTORY COLUMN"

            Temporary table options:

              - "GLOBAL TEMPORARY"
              - "GLOBAL TEMPORARY COLUMN"
              - "LOCAL TEMPORARY"
              - "LOCAL TEMPORARY COLUMN"

            Not a table:

              - "VIEW"

            Defaults to 'LOCAL TEMPORARY COLUMN' if ``where`` starts
            with '#'. Otherwise, the default is 'COLUMN'.
        force : bool, optional
            If force is True, it will replace the existing table.

            Defaults to False.
        save_source : bool, optional
            If True, it will save the name of source table.

            Defaults to True.
        append : bool, optional
            If True, it will use the existing table and append data to it.

            Defaults to False.
        data_lake : bool, optional
            If True, it will save the table to HANA data lake.

            Defaults to False.
        data_lake_container : str, optional
            Name of the data lake container.

            Defaults to 'SYSRDL#CG'.
        view_structure : dict, optional
            Define the parameters in the view. Only valid when `table_type="VIEW"`.

            Defaults to None.

        Returns
        -------
        DataFrame
            A DataFrame that represents the new table or view.
        """
        where = (schema, table)
        temp_tbl = "#TEMP_{}".format(str(uuid.uuid1()).replace('-', '_').upper())
        if table in self.select_statement:
            temp_df = self.save(temp_tbl, force=True)
            return temp_df.save(where, table_type=table_type, force=force, save_source=save_source, append=append, data_lake=data_lake, data_lake_container=data_lake_container, view_structure=view_structure)
        else:
            return self.save(where, table_type=table_type, force=force, save_source=save_source, append=append, data_lake=data_lake, data_lake_container=data_lake_container, view_structure=view_structure)

    def save(self, where, table_type=None, force=False, save_source=True, append=False, data_lake=False, data_lake_container='SYSRDL#CG', view_structure=None):
        """
        Creates a table or view holding the current DataFrame's data.

        Parameters
        ----------
        where : str or (str, str) tuple
            The table name or (schema name, table name) tuple. If no schema
            is provided, then the table or view is created in the current
            schema.
        table_type : str, optional
            The type of table to create. The value is case insensitive.

            Permanent table options:

              - "ROW"
              - "COLUMN"
              - "HISTORY COLUMN"

            Temporary table options:

              - "GLOBAL TEMPORARY"
              - "GLOBAL TEMPORARY COLUMN"
              - "LOCAL TEMPORARY"
              - "LOCAL TEMPORARY COLUMN"

            Not a table:

              - "VIEW"

            Defaults to 'LOCAL TEMPORARY COLUMN' if ``where`` starts
            with '#'. Otherwise, the default is 'COLUMN'.
        force : bool, optional
            If force is True, it will replace the existing table.

            Defaults to False.
        save_source : bool, optional
            If True, it will save the name of source table.

            Defaults to True.
        append : bool, optional
            If True, it will use the existing table and append data to it.

            Defaults to False.
        data_lake : bool, optional
            If True, it will save the table to HANA data lake.

            Defaults to False.
        data_lake_container : str, optional
            Name of the data lake container.

            Defaults to 'SYSRDL#CG'.
        view_structure : dict, optional
            Define the parameters in the view. Only valid when `table_type="VIEW"`.

            Defaults to None.

        Returns
        -------
        DataFrame
            A DataFrame that represents the new table or view.

        .. note::
           For this operation to succeed, the table name must not be in
           use, the schema must exist, and the user must have permission
           to create tables (or views) in the target schema.
        """

        if isinstance(where, tuple):
            schema, table = where
        else:
            schema, table = None, where
        if view_structure:
            if table_type != "VIEW":
                raise ValueError('table_type must be "VIEW".')
        if data_lake:
            if force:
                self.connection_context.drop_table(table=table, data_lake=True, data_lake_container=data_lake_container) #drop data lake table
                self.connection_context.drop_table(table=table, schema=schema) #drop virtual table
            self.connection_context.copy_to_data_lake(data=self,
                                                      virtual_table=table,
                                                      data_lake_table=table,
                                                      schema=schema,
                                                      append=append,
                                                      data_lake_container=data_lake_container)
        else:
            if table_type is None:
                if table.startswith('#'):
                    table_type = 'LOCAL TEMPORARY COLUMN'
                else:
                    table_type = 'COLUMN'
            if table_type.upper() not in {
                    'ROW',
                    'COLUMN',
                    'HISTORY COLUMN',
                    'GLOBAL TEMPORARY',
                    'GLOBAL TEMPORARY COLUMN',
                    'LOCAL TEMPORARY',
                    'LOCAL TEMPORARY COLUMN',
                    'VIEW'}:
                raise ValueError("{!r} is not a valid value of table_type".format(
                    table_type))

            has_table = False
            try:
                has_table = self.connection_context.has_table(table, schema)
            except dbapi.Error as err:
                logger.warning(err)
                pass
            except Exception as err:
                logger.warning(err)
                pass
            if schema is None:
                where_string = quotename(table)
            else:
                where_string = '{}.{}'.format(*map(quotename, where))

            if has_table:
                if not (force or append):
                    logger.warning("Table already exists. Please set force=True to drop table or append=True to append data.")
            table_type = table_type.upper()
            not_created = True
            if table_type != 'VIEW':
                table_type += ' TABLE'
            with self.connection_context.connection.cursor() as cur:
                if (force is True) and (table_type != 'VIEW') and (append is False):
                    if has_table:
                        try:
                            execute_logged(cur,
                                           "DROP TABLE {};".format(where_string),
                                           self.connection_context.sql_tracer,
                                           self.connection_context)
                            has_table = False
                        except dbapi.Error:
                            pass
                        except Exception as err:
                            logger.error(str(err))
                            pass
                if (force is True) and (table_type == 'VIEW') and (append is False):
                    try:
                        execute_logged(cur,
                                       "DROP VIEW {};".format(where_string),
                                       self.connection_context.sql_tracer,
                                       self.connection_context)
                    except dbapi.Error:
                        pass
                    except Exception as err:
                        logger.error(str(err))
                        pass
                if not has_table:
                    if view_structure:
                        view_tt = []
                        for param_key, param_value in view_structure.items():
                            view_tt.append("IN {} {}".format(param_key, param_value))
                        execute_logged(cur,
                                    "CREATE {} {} ({}) AS ({})".format(table_type,
                                                                       where_string,
                                                                       ", ".join(view_tt),
                                                                       self.select_statement),
                                    self.connection_context.sql_tracer,
                                    self.connection_context)
                    else:
                        execute_logged(cur,
                                       "CREATE {} {} AS ({})".format(table_type,
                                                                     where_string,
                                                                     self.select_statement),
                                       self.connection_context.sql_tracer,
                                       self.connection_context)
                    not_created = False
                if append and not_created:
                    execute_logged(cur,
                                   "INSERT INTO {} {}".format(where_string, self.select_statement),
                                   self.connection_context.sql_tracer,
                                   self.connection_context)

            if not self.connection_context.pyodbc_connection:
                if not self.connection_context.connection.getautocommit():
                    self.connection_context.connection.commit()
        return self.connection_context.table(table, schema=schema, save_source=save_source)

    def save_nativedisktable(self, where, force=False, save_source=True):
        """
        Materializes dataframe to a SAP HANA native disk.

        Parameters
        ----------
        where : str or (str, str) tuple
            The table name or (schema name, table name) tuple. If no schema
            is provided, then the table or view is created in the current
            schema.
        force : bool, optional
            If force is True, it will replace the existing table.
        save_source : bool, optional
            If True, it will save the name of source table.
            Defaults to True.

        Returns
        -------
        DataFrame
            A DataFrame that represents the new table.

        """
        if isinstance(where, tuple):
            schema, table = where
        else:
            schema, table = None, where
        if schema is None:
            where_string = quotename(table)
        else:
            where_string = '{}.{}'.format(*map(quotename, where))
        with self.connection_context.connection.cursor() as cur:
            if force is True:
                try:
                    execute_logged(cur,
                                   "DROP TABLE {};".format(where_string),
                                   self.connection_context.sql_tracer,
                                   self.connection_context)
                except dbapi.Error:
                    pass
                except Exception as err:
                    logger.error(str(err))
                    pass
            execute_logged(cur,
                           "CREATE COLUMN TABLE {} AS ({})".format(where_string, self.select_statement),
                           self.connection_context.sql_tracer,
                           self.connection_context)
            execute_logged(cur,
                           "ALTER TABLE {} PAGE LOADABLE CASCADE".format(where_string),
                           self.connection_context.sql_tracer,
                           self.connection_context)
        if not self.connection_context.pyodbc_connection:
            if not self.connection_context.connection.getautocommit():
                self.connection_context.connection.commit()
        return self.connection_context.table(table, schema=schema, save_source=save_source)

    def split_column(self, column, separator, new_column_names):
        """
        Returns a new DataFrame with splitted column.

        Parameters
        ----------
        column : str
            A column or list of columns to be splitted.
        separator : str
            The separator.
        new_column_names : list of str
            The splitted column names

        Examples
        --------
        >>> df.collect()
           ID     COL
        0   1   1,2,3
        1   2   3,4,4

        >>> df.split_column(column="COL", separator=",", new_column_names=["COL1", "COL2", "COL3"]).collect()
           ID    COL COL1 COL2 COL3
        0   1  1,2,3    1    2    3
        1   2  3,4,4    3    4    4

        Returns
        -------
        DataFrame
            New DataFrame object with splitted columns as specified.
        """
        substr_list = []
        temp_substr = "SUBSTR_AFTER ({}, '{}') ".format(quotename(column), separator)
        count = 0
        for col in new_column_names:
            if count == 0:
                col_str = "SUBSTR_BEFORE ({}, '{}') {}".format(quotename(column), separator, quotename(col))
            elif count == len(new_column_names) - 1:
                col_str = temp_substr + "{}".format(quotename(col))
            else:
                col_str = "SUBSTR_BEFORE ({}, '{}') {}".format(temp_substr, separator, quotename(col))
                temp_substr = "SUBSTR_AFTER ({}, '{}') ".format(temp_substr, separator)
            count = count + 1
            substr_list.append(col_str)
        new_select_statement = "SELECT *, {} FROM ({})".format(", ".join(substr_list), self.select_statement)
        return self._df(new_select_statement, propagate_safety=True)

    def concat_columns(self, columns, separator):
        """
        Returns a new DataFrame with splitted column.

        Parameters
        ----------
        columns : list of str
            A list of columns to be concatenated.
        separator : str
            The separator.

        Examples
        --------
        >>> df.collect()
           ID  COL1 COL2 COL3
        0   1     1    2    3
        1   2     3    4    4

        >>> df.concat_columns(columns=["COL1", "COL2", "COL3"], separator=",").collect()
           ID  COL1 COL2 COL3 COL1,COL2,COL3
        0   1     1    2    3          1,2,3
        1   2     3    4    4          3,4,4

        Returns
        -------
        DataFrame
            New DataFrame object with concat column as specified.
        """
        concat_str = " || '{}' || ".format(separator)
        new_select_statement = 'SELECT *, {} "{}" FROM ({})'.format(concat_str.join([quotename(col) for col in columns]),
                                                                    separator.join(columns),
                                                                    self.select_statement)
        return self._df(new_select_statement, propagate_safety=True)

    def nullif(self, value):
        """
        Replace certain value with NULL value.

        Parameters
        ----------
        value: scalar or dict
            To-be-replaced value. If the type is dict, its key indicates the column name.
        """
        columns = self.columns
        col_list = []
        if isinstance(value, dict):
            for col in columns:
                if col in value:
                    if isinstance(value[col], str):
                        col_list.append("NULLIF({0}, '{1}') {0}".format(quotename(col), value[col]))
                    else:
                        col_list.append("NULLIF({0}, {1}) {0}".format(quotename(col), value[col]))
                else:
                    col_list.append(quotename(col))
        else:
            for col in columns:
                if isinstance(value, str):
                    col_list.append("NULLIF({0}, '{1}') {0}".format(quotename(col), value))
                else:
                    col_list.append("NULLIF({0}, {1}) {0}".format(quotename(col), value))
        new_select_statement = "SELECT {} FROM ({})".format(", ".join(col_list), self.select_statement)
        return self._df(new_select_statement, propagate_safety=True)

    def replace(self, to_replace=None, value=None, regex=False):
        """
        Returns a new DataFrame with replaced value.

        Parameters
        ----------
        to_replace : numeric, str or dict, optional
            The value/pattern to be replaced. If regex is True, the regex will be used instead of value.

            * numeric or str: the value equal to `to_replace` will be replaced by `value`.

            * dict:

              .. only:: html

                - ``value`` is None: the value equal to the key of ``to_replace`` will be replaced by its value.
                  If it is nested JSON, the to-be-replaced columns will be restricted by the first-level keys.
                - ``value`` is numeric or str: the value equal to the value of ``to_replace`` will be replaced
                  by ``value``. The to-be-replaced columns will be restricted by the keys of ``to_replace``.
                - ``value`` is dict: the value equal to the value of ``to_replace`` will be replaced by the value of
                  ``value`` under the same key.
                  The to-be-replaced columns and the replaced value will be restricted by the keys of
                  ``to_replace`` and ``value``.

              .. only:: latex

                ======================== ========================================================================
                Cases                    Treatment
                ======================== ========================================================================
                ``value`` is None        The value equal to the key of ``to_replace`` will be replaced by its \
value. If it is nested JSON, the to-be-replaced columns will be restricted by the first-level keys.
                ``value`` is numeric/str The value equal to the value of ``to_replace`` will be replaced by \
``value``. The to-be-replaced columns will be restricted by the keys of ``to_replace``.
                ``value`` is dict        The value equal to the value of ``to_replace`` will be replaced by the \
value of ``value`` under the same key. The to-be-replaced columns and the replaced value will be \
restricted by the keys of ``to_replace`` and ``value``.
                ======================== ========================================================================

            * None: ``regex`` will be used.
        value : numeric, str or dict, optional
            Value to replace.

            - numeric or str: The value to replace ``to_replace`` or according to the pattern if regex is given.
            - dict: the replacement will take place under the columns equal to the keys.
        regex : bool or str, optional
            Use regex or not.

            - bool: use regex if True.
            - str: work the same as ``to_replace`` if ``to_replace`` is None.

        Examples
        --------
        >>> df.collect()
            Aa  Bb  Cb
        0    0   5   a
        1   10   0   b
        2    2   7   c
        3    3   8   d
        4    4   9   e

        >>> df.replace(to_replace=0, value=5).collect()
            Aa  Bb  Cb
        0    5   5   a
        1   10   5   b
        2    2   7   c
        3    3   8   d
        4    4   9   e

        >>> df.replace(to_replace={0: 10, 1: 100}).collect()
            Aa  Bb  Cb
        0   10   5   a
        1   10  10   b
        2    2   7   c
        3    3   8   d
        4    4   9   e

        >>> df.replace(to_replace={'Aa': 0, 'Bb': 5}, value=100).collect()
            Aa   Bb  Cb
        0  100  100   a
        1   10    0   b
        2    2    7   c
        3    3    8   d
        4    4    9   e

        >>> df.replace(to_replace={'Aa': 0, 'Bb': 5}, value={'Aa': 100, 'Bb': 50}).collect()
             Aa  Bb  Cb
        0   100  50   a
        1    10   0   b
        2     2   7   c
        3     3   8   d
        4     4   9   e

        >>> df.replace(to_replace={'Aa': {0: 100, 4: 400}}).collect()
             Aa  Bb  Cb
        0   100   5   a
        1    10   0   b
        2     2   7   c
        3     3   8   d
        4   400   9   e

        >>> df2.collect()
              A    B
        0   bat  abc
        1   foo  bar
        2  bait  xyz

        >>> df2.replace(to_replace=r'^ba.$', value='new', regex=True).collect()
              A    B
        0   new  abc
        1   foo  new
        2  bait  xyz

        >>> df2.replace(to_replace={'A': r'^ba.$'}, value={'A': 'new'}, regex=True).collect()
              A    B
        0   new  abc
        1   foo  bar
        2  bait  xyz

        >>> df2.replace(regex=r'^ba.$', value='new').collect()
             A     B
        0   new  abc
        1   foo  new
        2  bait  xyz

        >>> df2.replace(regex={r'^ba.$': 'new', 'foo': 'xyz'}).collect()
              A    B
        0   new  abc
        1   xyz  new
        2  bait  xyz

        Returns
        -------
        DataFrame
            New DataFrame object with replaced values.
        """
        if to_replace is None:
            if isinstance(regex, (str, dict)):
                to_replace = regex
            else:
                raise ValueError("to_replace or regex has not been set.")
        columns = self.columns
        regex_str_0 = "REPLACE_REGEXPR('^"
        regex_str_1 = "$'"
        if regex:
            regex_str_0 = "REPLACE_REGEXPR('"
            regex_str_1 = "'"
        sql_rep = []
        if value:
            if isinstance(to_replace, dict):
                if isinstance(value, dict):
                    for col, col_type in self.get_table_structure().items():
                        if col in to_replace:
                            if value[col] is None:
                                if isinstance(to_replace[col], str):
                                    sql_rep.append("CASE WHEN {0} = '{1}' THEN NULL ELSE {0} END {0}".format(quotename(col), to_replace[col]))
                                else:
                                    sql_rep.append("CASE WHEN {0} = {1} THEN NULL ELSE {0} END {0}".format(quotename(col), to_replace[col]))
                            else:
                                sql_rep.append("{3}{1}{4} IN {0} WITH '{2}' OCCURRENCE ALL) {0}".format(quotename(col),
                                                                                                        to_replace[col],
                                                                                                        value[col],
                                                                                                        regex_str_0,
                                                                                                        regex_str_1))
                        else:
                            sql_rep.append(quotename(col))
                else:
                    for col in columns:
                        if col in to_replace:
                            sql_rep.append("{3}{1}{4} IN {0} WITH '{2}' OCCURRENCE ALL) {0}".format(quotename(col),
                                                                                                    to_replace[col],
                                                                                                    value,
                                                                                                    regex_str_0,
                                                                                                    regex_str_1))
                        else:
                            sql_rep.append(quotename(col))
            else:
                for col in columns:
                    sql_rep.append("{3}{1}{4} IN {0} WITH '{2}' OCCURRENCE ALL) {0}".format(quotename(col),
                                                                                            to_replace,
                                                                                            value,
                                                                                            regex_str_0,
                                                                                            regex_str_1))
            sql = "SELECT {} FROM ({})".format(", ".join(sql_rep), self.select_statement)
        else:
            if isinstance(to_replace, dict):
                if isinstance(list(to_replace.values())[0], dict):
                    sql = self.select_statement
                    for kkey, vval in to_replace.items():
                        for to_rep, val in vval.items():
                            sql_rep = []
                            for col, col_type in self.get_table_structure().items():
                                if kkey == col:
                                    if val is None:
                                        if isinstance(to_rep, str):
                                            sql_rep.append("CASE WHEN {0} = '{1}' THEN NULL ELSE {0} END {0}".format(quotename(col), to_rep))
                                        else:
                                            sql_rep.append("CASE WHEN {0} = {1} THEN NULL ELSE {0} END {0}".format(quotename(col), to_rep))
                                    else:
                                        sql_rep.append("{3}{1}{4} IN {0} WITH '{2}' OCCURRENCE ALL) {0}".format(quotename(col),
                                                                                                                to_rep,
                                                                                                                val,
                                                                                                                regex_str_0,
                                                                                                                regex_str_1))
                                else:
                                    sql_rep.append(quotename(col))
                            sql = "SELECT {} FROM ({})".format(", ".join(sql_rep), sql)
                else:
                    sql = self.select_statement
                    for to_rep, val in to_replace.items():
                        sql_rep = []
                        for col in columns:
                            sql_rep.append("{3}{1}{4} IN {0} WITH '{2}' OCCURRENCE ALL) {0}".format(quotename(col),
                                                                                                    to_rep,
                                                                                                    val,
                                                                                                    regex_str_0,
                                                                                                    regex_str_1))
                        sql = "SELECT {} FROM ({})".format(", ".join(sql_rep), sql)
            else:
                null_replace = []
                for col, col_type in self.get_table_structure().items():
                    if isinstance(to_replace, str) and ('VARCHAR' in col_type.upper() or 'TIME' in col_type.upper() or 'DATE' in col_type.upper() or 'LOB' in col_type.upper()):
                        null_replace.append("CASE WHEN {0} = '{1}' THEN NULL ELSE {0} END {0}".format(quotename(col), to_replace))
                    elif (not isinstance(to_replace, str)) and ('DOUBLE' in col_type.upper() or 'INT' in col_type.upper()):
                        null_replace.append("CASE WHEN {0} = {1} THEN NULL ELSE {0} END {0}".format(quotename(col), to_replace))
                    else:
                        null_replace.append(quotename(col))
                sql = "SELECT {} FROM ({})".format(", ".join(null_replace), self.select_statement)
        return self._df(sql, propagate_safety=True)

    def sort(self, cols, desc=False):
        """
        Returns a new DataFrame sorted by the specified columns.

        Parameters
        ----------
        cols : str or list of str
            A column or list of columns to sort by.
            If a list is specified, then the sort order in parameter desc is used
            for all columns.
        desc : bool, optional
            Set to True to sort in descending order. Defaults to False,
            for ascending order. Default value is False.

        Returns
        -------
        DataFrame
            New DataFrame object with rows sorted as specified.
        """

        # Issue: DataFrames constructed from a sorted DataFrame may not
        # respect its order, since there's no guarantee that subqueries
        # and derived tables will preserve order.
        #
        # collect() is the only thing we can really guarantee will work.
        #
        # We'd have to change our model to propagate ordering constraints
        # explicitly, outside of the select_statement, to guarantee
        # ordering.
        if not cols:
            raise ValueError("Can't sort by 0 columns")
        cols = DataFrame._get_list_of_str(cols,
                                          'Parameter cols must be a string or a list of strings')
        self._validate_columns_in_dataframe(cols)

        cols = [quotename(c) for c in cols]
        template = '{} DESC' if desc else '{} ASC'
        order_by = 'ORDER BY ' + ', '.join(template.format(col) for col in cols)
        new_select_statement = 'SELECT * FROM ({}) AS {} {}'.format(
            self.select_statement, self.quoted_name, order_by)
        return self._df(new_select_statement, propagate_safety=True)

    def sort_values(self, by, ascending=True):
        """
        Returns a new DataFrame sorted by the specified columns.

        Parameters
        ----------
        by : str or list of str
            A column or list of columns to sort by.
            If a list is specified, then the sort order in parameter ascending is used
            for all columns.

        ascending : bool, optional
            Set to False to sort in descending order.

            Defaults to True, for ascending order.

        Returns
        -------
        DataFrame
            New DataFrame object with rows sorted as specified.
        """
        return self.sort(cols=by, desc=not ascending)

    def sort_index(self, ascending=True):
        """
        Returns a new DataFrame sorted by the index.

        Parameters
        ----------

        ascending : bool, optional
            Set to False to sort in descending order. Defaults to False,
            for ascending order.

            Defaults to True

        Returns
        -------
        DataFrame
            New DataFrame object with rows sorted as specified.
        """
        if self.index is None:
            raise ValueError("Index has not been set!")
        sorted_index = self.index
        return self.sort(cols=sorted_index, desc=not ascending).set_index(sorted_index)

    def sort_by_similarity(self,
                           embed_col,
                           query=None,
                           query_vector=None,
                           ascending=False,
                           similarity_function='COSINE_SIMILARITY',
                           use_vector_index=False,
                           model_version='SAP_NEB.20240715'):
        """
        Returns a new DataFrame sorted by the similarity of the specified column to a query vector.

        Parameters
        ----------
        embed_col : str
            The column containing the embeddings to compare to the query vector.
        query : str, optional
            The query to compare to the embeddings in the specified column. Either query or query_vector must be specified.
        query_vector : list of float, optional
            The query vector to compare to the embeddings in the specified column. Either query or query_vector must be specified.
        ascending : bool, optional
            Set to True to sort in ascending order. Defaults to False,
            for descending order.
        similarity_function : {'COSINE_SIMILARITY', 'L2DISTANCE'}, optional
            The similarity function to use. Defaults to 'COSINE_SIMILARITY'.
        use_vector_index : bool, optional
            Set to True to use the vector index for the specified column.
            Defaults to False.
        model_version : str, optional
            Text Embedding Model version. Options are 'SAP_NEB.20240715' and 'SAP_GXY.20250407'.

            Defaults to 'SAP_NEB.20240715'.
        """
        if similarity_function.upper() not in {'COSINE_SIMILARITY', 'L2DISTANCE'}:
            raise ValueError("similarity_function must be 'COSINE_SIMILARITY' or 'L2DISTANCE'.")
        if query_vector is None and query is None:
            raise ValueError("Either query or query_vector must be specified.")
        if query_vector:
            if not isinstance(query_vector, list):
                raise ValueError("query_vector must be a list of floats.")
            query_vector_str = json.dumps(query_vector)
            sql = f"SELECT *, {similarity_function}(\"{embed_col}\", TO_REAL_VECTOR('{query_vector_str}')) AS SIMILARITY FROM ({self.select_statement}) ORDER BY SIMILARITY {'ASC' if ascending else 'DESC'}"
            if not use_vector_index:
                sql += " WITH HINT (NO_VECTOR_INDEX)"
        else:
            if not isinstance(query, str):
                raise ValueError("query must be a string.")
            sql = f"SELECT *, {similarity_function}(\"{embed_col}\", VECTOR_EMBEDDING('{query}', 'QUERY', '{model_version}')) AS SIMILARITY FROM ({self.select_statement}) ORDER BY SIMILARITY {'ASC' if ascending else 'DESC'}"
        return self._df(sql)

    def _stringify_select_list(self, select):
        projection = []
        for col in select:
            if isinstance(col, Column):
                col_name = col._col_name
                if col_name is None:
                    col_name = col.name
                projection.append('{} AS {}'.format(col._new_col if col._new_col else quotename(col.name), quotename(col_name)))
            elif isinstance(col, _STRING_TYPES):
                if '*' in col:
                    projection.append(col)
                else:
                    projection.append(quotename(col))
            else:
                expr, name = col
                self._token_validate(expr)
                projection.append('{} AS {}'.format(expr, quotename(name)))
        return ', '.join(projection)

    def select(self, *cols):
        """
        Returns a new DataFrame with columns derived from the current DataFrame.

        .. warning::
            There is no check that inputs interpreted as SQL expressions are
            actually valid expressions; an "expression" like
            "A FROM TAB; DROP TABLE IMPORTANT_THINGS; SELECT A" can cause
            a lot of damage.

        Parameters
        ----------
        cols : str or (str, str) tuple.
            The columns in the new DataFrame. A string is treated as the name
            of a column to select; a (str, str) tuple is treated as
            (SQL expression, alias). As a special case, '*' is expanded
            to all columns of the original DataFrame.

        Returns
        -------
        DataFrame
            A new DataFrame object with the specified columns.

        Raises
        ------
        hana_ml.ml_exceptions.BadSQLError
            If comments or malformed tokens are detected in a column
            expression. May have false positives and false negatives.

        Examples
        --------
        Input:

        >>> df.collect()
           A  B  C
        0  1  2  3

        Selecting a subset of existing columns:

        >>> df.select('A', 'B').collect()
           A  B
        0  1  2

        Computing a new column based on existing columns:

        >>> df.select('*', ('B * 4', 'D')).collect()
           A  B  C  D
        0  1  2  3  8
        """

        columns = []
        for col in cols:
            if isinstance(col, Column):
                columns.append(col)
            elif isinstance(col, list):
                # Compatibility with old df.select(['a', 'b']) style.
                columns.extend(col)
            elif isinstance(col, str) and col == '*':
                columns.extend(self.columns)
            else:
                columns.append(col)

        self._validate_columns_in_dataframe(columns)

        projection_string = self._stringify_select_list(columns)
        new_select_statement = 'SELECT {} FROM ({}) AS {}'.format(
            projection_string, self.select_statement, self.quoted_name)

        newdf = self._df(new_select_statement)
        res_cols = []
        for col in columns:
            if isinstance(col, Column):
                col_name = col._col_name
                if col_name is None:
                    col_name = col.name
                res_cols.append(col_name)
            elif isinstance(col, _STRING_TYPES):
                res_cols.append(col)
            else:
                res_cols.append(col[1])
        newdf._columns = res_cols
        if all(isinstance(col, _STRING_TYPES) for col in columns):
            self._propagate_safety(newdf)
        return newdf

    def set_operations(self,
                       other,
                       all=True,  # pylint:disable=redefined-builtin
                       op='UNION'):
        """
        Combines this DataFrame's rows and another DataFrame's rows into
        one DataFrame. This operation is equivalent to a SQL UNION/INTERSECT/EXCEPT ALL.

        Parameters
        ----------
        other : DataFrame, list of DataFrame
            The right side of the operation.
        all : bool, optional
            If True, keep duplicate rows; equivalent to UNION/INTERSECT/EXCEPT ALL in SQL.
            If False, keep only one copy of duplicate rows (even if they
            come from the same side of the operation); equivalent to a UNION/INTERSECT/EXCEPT
            or a UNION/INTERSECT/EXCEPT ALL followed by DISTINCT in SQL.
            Defaults to True.

        Returns
        -------
        DataFrame
            The combined data from ``self`` and ``other``.

        Examples
        --------
        We have two DataFrames we want to union, with some duplicate rows:

        >>> df1.collect()
           A  B
        0  1  2
        1  1  2
        2  2  3

        >>> df2.collect()
           A  B
        0  2  3
        1  3  4

        >>> df1.set_operations(df2, op='UNION').collect()
           A  B
        0  1  2
        1  1  2
        2  2  3
        3  2  3
        4  3  4

        If we want to use except instead of union, we can do:

        >>> df1.union(df2, all=False, op='except').collect()
           A  B
        0  1  2
        1  1  2
        """
        op = op.upper()
        if op not in {'UNION', 'INTERSECT', 'EXCEPT'}:
            raise ValueError('op must be "UNION", "INTERSECT" or "EXCEPT"')
        if op != 'UNION':
            all = False # INTERSECT and EXCEPT do not support ALL
        if isinstance(other, (list, tuple)):
            new_select_list = ['({})'.format(self.select_statement)]
            for other_df in other:
                new_select_list.append('({})'.format(other_df.select_statement))
            new_select = ' {} ALL '.format(op).join(new_select_list) if all else ' {} '.format(op).join(new_select_list)
        else:
            new_select = '(({}) {} ({}))'.format(
                self.select_statement,
                '{} ALL'.format(op) if all else '{}'.format(op),
                other.select_statement)
        retval = self._df(new_select)
        if isinstance(other, (list, tuple)):
            for other_df in other:
                if self._ttab_handling == other_df._ttab_handling == 'safe':
                    retval._ttab_handling = 'safe'
                elif {self._ttab_handling, other_df._ttab_handling} & {'ttab', 'unsafe'}:
                    retval._ttab_handling = 'unsafe'
        else:
            if self._ttab_handling == other._ttab_handling == 'safe':
                retval._ttab_handling = 'safe'
            elif {self._ttab_handling, other._ttab_handling} & {'ttab', 'unsafe'}:
                retval._ttab_handling = 'unsafe'
        return retval

    def union(self,
              other,
              all=True, # pylint:disable=redefined-builtin
              # by='position',
             ):
        """
        Combines this DataFrame's rows and another DataFrame's rows into
        one DataFrame. This operation is equivalent to a SQL UNION ALL.

        Parameters
        ----------
        other : DataFrame, list of DataFrame
            The right side of the union.
        all : bool, optional
            If True, keep duplicate rows; equivalent to UNION ALL in SQL.
            If False, keep only one copy of duplicate rows (even if they
            come from the same side of the union); equivalent to a UNION
            or a UNION ALL followed by DISTINCT in SQL.
            Defaults to True.

        Returns
        -------
        DataFrame
            The combined data from ``self`` and ``other``.

        Examples
        --------
        We have two DataFrames we want to union, with some duplicate rows:

        >>> df1.collect()
           A  B
        0  1  2
        1  1  2
        2  2  3

        >>> df2.collect()
           A  B
        0  2  3
        1  3  4

        union() produces a DataFrame that contains all rows of both df1
        and df2, like a UNION ALL:

        >>> df1.union(df2).collect()
           A  B
        0  1  2
        1  1  2
        2  2  3
        3  2  3
        4  3  4

        To get the deduplication effect of a UNION DISTINCT, pass
        all=False or call distinct() after union():

        >>> df1.union(df2, all=False).collect()
           A  B
        0  1  2
        1  2  3
        2  3  4
        >>> df1.union(df2).distinct().collect()
           A  B
        0  1  2
        1  2  3
        2  3  4
        """
        return self.set_operations(other=other, all=all, op='UNION')

    def collect(self, fetch_size=32000, geometries=True, convert_geo_to_shapely=True):
        """
        Copies the current DataFrame to a new Pandas DataFrame.

        Parameters
        ----------
        fetch_size : int, optional
            Fetch size in hdbcli.
        geometries : bool, optional
            With this flag set to `True` (default),
            the geometries are converted to Well-Known-Text representations
            in the resulting `Pandas` dataframe. Even if they are converted
            to Shapely objects (see ``convert_geo_to_shapely``), when you
            print the dataframe, the geometry columns are represented as
            Well-Known-Text.

            If you need the raw binary values, set this flag to `False`
        convert_geo_to_shapely : bool, optional
            If set to `True` (default), all geometry columns will be
            converted to a `Shapely` object, so that the dataframe can
            be directly used in visualization libraries for example.
            If your processing does not support `Shapely` objects, you
            can switch this conversion off. In this case the columns
            remain of type `String`.

            .. note::

                Before the conversion, it is checked, if any value
                in the column is `NULL`. If so, this column will **not** be
                converted to a `Shapely` object, because `NULL` WKT strings
                are not supported by `Shapely`.

        Returns
        -------
        pandas.DataFrame
            A Pandas DataFrame that contains the current DataFrame's data.

        Examples
        --------
        Viewing a hana_ml DataFrame doesn't execute the underlying SQL or fetch the data:

        >>> df = cc.table('T')
        >>> df
        <hana_ml.dataframe.DataFrame object at 0x7f2b7f24ddd8>

        Using collect() executes the SQL and fetches the results into a Pandas DataFrame:

        >>> df.collect()
           A  B
        0  1  3
        1  2  4

        >>> type(df.collect())
        <class 'pandas.core.frame.DataFrame'>
        """
        select_statement = self.select_statement
        if not self.connection_context.pyodbc_connection:
            try:
                if geometries:
                    geo_cols = self.geometries
                    if len(geo_cols) == 0:
                        select_statement = self.select_statement
                    else:
                        sql_cols = ""

                        for col in self.columns:
                            if col in geo_cols:
                                sql_cols = ", ".join((sql_cols, '"{}".ST_AsWKT()'.format(col)))
                            else:
                                sql_cols = ", ".join((sql_cols, '"{}"'.format(col)))

                        sql_cols = sql_cols[2:]  # Clean up leading ", "

                        select_statement = "SELECT {cols} from ({source})".format(
                            cols=sql_cols, source=self.select_statement
                        )
            except dbapi.Error as exc:
                logger.warning("Errors in checking geometries, %s", exc)
                pass
            except Exception as exc:
                logger.warning("Errors in checking geometries, %s", str(exc.args[1]))
                pass
        # pylint: disable=W0511
        # TODO: This produces wrong dtypes for an empty DataFrame.
        # We can't rely on type inference for an empty DataFrame;
        # we will need to provide dtype information explicitly.
        # The pandas.DataFrame constructor's dtype argument only allows
        # a single dtype for the whole DataFrame, but we should be able
        # to use the astype method instead.
        if not self.connection_context.pyodbc_connection:
            try:
                results = self.__run_query(select_statement, fetch_size)
            except dbapi.Error as exc:
                logger.error("Failed to retrieve data for the current dataframe, %s", exc)
                raise
            except Exception as exc:
                logger.error("Failed to retrieve data for the current dataframe, %s", str(exc.args[1]))
                raise
            result_df = pd.DataFrame(results, columns=self.columns)
        else:
            result_df = pd.read_sql(select_statement, self.connection_context.connection)

        # Convert geometries to shapely objects in the pandas data frame,
        # so that they can be directly used in other python frameworks
        if not self.connection_context.pyodbc_connection:
            try:
                if geometries and convert_geo_to_shapely:
                    for col in geo_cols:
                        if result_df[col].isnull().values.any():
                            logger.warning("Column '%s' does contain Null values and can't be converted to a Shapely object. Will remain to be String.", col)
                        else:
                            result_df[col] = result_df[col].apply(wkt.loads)
            except dbapi.Error as exc:
                logger.warning("Errors in Converting geometries to shapely objects, %s", exc)
                pass
            except Exception as exc:
                logger.warning("Errors in Converting geometries to shapely objects, %s", str(exc.args[1]))
                pass

        return result_df

    @property
    def geometries(self) -> list:
        """
        Returns the geometries of a data frame. The list is empty if there
        are none.

        Returns
        -------
        list
            List with geometry columns
        """
        res_list = []
        try:
            res_list = [col[0] for col in self.dtypes() if col[1] in ["GEOMETRY", "POINT"]]
        except Exception as err:
            logger.warning(err)
            pass
        return res_list

    @property
    def srids(self) -> dict:
        """
        Returns the srid for each geometry column in the dataframe.
        If none is found, the dictionary will be {}.

        For dataframes based on HANA catalog objects, the information is
        read from the catalog. For Dataframes, which do not have a catalog
        object (e.g. are based on SQL statements, or temporary tables ),
        the SRID is derived by selecting the first row in the table and
        read it directly from the EWKB. For columns with multiple SRSes
        (SRID NULL), this means, that you might get back a SRS, which differs
        from other entries in the same column.

        **Known Limitation**: For dataframes which don't have catalog objects
        and do not contain data, no SRID can be provided.

        Returns
        -------
        dict
            Dictionary with the SRID per column: `{<column_name>: <srid>}`
            Returns `{}` when none are found.
        """

        def get_from_data_row():
            """Helper to derive the SRID from a table row"""
            srid = {}

            # Get the relevant columns from the data source
            geo_cols_sql = ""
            geo_cols_list = []
            for col in self.dtypes():
                if col[1] in ["GEOMETRY", "POINT"]:
                    geo_cols_sql = ", ".join(
                        (geo_cols_sql, "{}.ST_SRID() ".format(quotename(col[0])))
                    )
                    geo_cols_list.append(col[0])
            geo_cols_sql = geo_cols_sql[2:]

            if geo_cols_sql == "":  # No Geometries Found
                return srid

            try:  # to get one record with values for the geo columns
                loc_result = self.__run_query(
                    "SELECT TOP 1 {} FROM({})".format(
                        geo_cols_sql, self.select_statement
                    )
                )

                # Turn the result into a dictionary
                if len(loc_result) == 1:
                    for index, col in enumerate(loc_result[0]):
                        try:
                            col_srid = int(col)
                            srid[geo_cols_list[index]] = col_srid
                        except Exception as err:
                            logger.warning(err)
                            pass
                else:
                    # Return a dictionary with geo cols and None Value
                    srid = dict.fromkeys(geo_cols_list, None)

            except Exception as ex:
                logger.error(str(ex))
                raise ex

            return srid

        # Read from DB for catalog objects
        if self.source_table:
            sql = """
                    SELECT COLUMN_NAME, SRS_ID
                      FROM ST_GEOMETRY_COLUMNS
                     WHERE SCHEMA_NAME = '{}'
                       AND TABLE_NAME = '{}'
                  """.format(self.source_table["SCHEMA_NAME"], self.source_table["TABLE_NAME"])
            result = self.__run_query(sql)

            # Fallback to reading the SRID from the table data (if there are any)
            if len(result) == 0:  # Will for example happen for temp. tables
                return get_from_data_row()
            return dict(result)
        # Fallback to reading the SRID from the table data (if there are any)
        return get_from_data_row()

    def rename_columns(self, names):
        """
        Returns a DataFrame with renamed columns.

        Parameters
        ----------
        names : list or dict
            If a list, specifies new names for every column in this DataFrame.
            If a dict, each dict entry maps an existing name to a new name,
            and not all columns need to be renamed.

        Returns
        -------
        DataFrame
            The same data as the original DataFrame with new column names.

        See Also
        --------
        DataFrame.alias : For renaming the DataFrame itself.

        Examples
        --------

        >>> df.collect()
           A  B
        0  1  3
        1  2  4

        >>> df.rename_columns(['C', 'D']).collect()
           C  D
        0  1  3
        1  2  4

        >>> df.rename_columns({'B': 'D'}).collect()
           A  D
        0  1  3
        1  2  4
        """

        if isinstance(names, list):
            if len(names) != len(self.columns):
                if len(names) > len(self.columns):
                    problem = "Too many"
                else:
                    problem = "Not enough"
                raise ValueError(problem + ' columns in rename_columns list.')
            names = dict(zip(self.columns, names))
        elif isinstance(names, dict):
            bad_names = set(names).difference(self.columns)
            if bad_names:
                raise ValueError("Column(s) not in DataFrame: {}".format(
                    sorted(bad_names)))
        else:
            raise TypeError("names should be a list or dict, not {}".format(
                type(names)))
        retval = self.select(*[(quotename(orig), names[orig]) if orig in names
                               else orig
                               for orig in self.columns])
        self._propagate_safety(retval)
        return retval

    def auto_cast(self, type_convert):
        """
        Returns a DataFrame with converted column types.

        Parameters
        ----------
        type_convert : dict
            Specifies the original type and the new type in Dict. e.g. {"INT": "DOUBLE"}

        Returns
        -------
        DataFrame
            The same data as this DataFrame, but with columns cast to the specified type.
        """
        new_cast = {}
        processded_type_convert = {}
        for kkey, vval in type_convert.items():
            processed_kkey = kkey
            processed_vval = vval
            if "," in kkey:
                processed_kkey = kkey.replace(", ", ",").replace(",", ", ")
            if "," in vval:
                processed_vval = vval.replace(", ", ",").replace(",", ", ")
            processded_type_convert[processed_kkey] = processed_vval

        for kkey, vval in self.get_table_structure().items():
            if vval in processded_type_convert:
                new_cast[kkey] = processded_type_convert[vval]
            elif "DECIMAL" in vval:
                if "DECIMAL" in processded_type_convert:
                    new_cast[kkey] = processded_type_convert["DECIMAL"]
        return self.cast(new_cast)

    def cast(self, cols, new_type=None):
        """
        Returns a DataFrame with columns cast to a new type.

        The name of the column in the returned DataFrame is the same as the original column.
         .. warning::
           Type compatibility between the existing column type and the new type is not checked.
           An incompatibility results in an error.

        Parameters
        ----------
        cols : str, list of str or dict
            The column(s) to be cast to a different type.
        new_type : str
            The database datatype to cast the column(s) to.
            No checks are performed to see if the new type is valid.
            An invalid type can lead to SQL errors or even SQL injection vulnerabilities.

        Returns
        -------
        DataFrame
            The same data as this DataFrame, but with columns cast to the specified type.

        Examples
        --------
        Input:

        >>> df1 = cc.sql('SELECT "AGE", "PDAYS", "HOUSING" FROM DBM_TRAINING_TBL')
        >>> df1.dtypes()
        [('AGE', 'INT', 10, 10, 10, 0), ('PDAYS', 'INT', 10, 10, 10, 0), ('HOUSING', 'VARCHAR', 100, 100, 100, 0)]

        Casting a column to NVARCHAR(20):

        >>> df2 = df1.cast('AGE', 'NVARCHAR(20)')
        >>> df2.dtypes()
        [('AGE', 'NVARCHAR', 20, 20, 20, 0), ('PDAYS', 'INT', 10, 10, 10, 0), ('HOUSING', 'VARCHAR', 100, 100, 100, 0)]

        Casting a list of columns to NVARCHAR(50):

        >>> df3 = df1.cast(['AGE', 'PDAYS'], 'NVARCHAR(50)')
        >>> df3.dtypes()
        [('AGE', 'NVARCHAR', 50, 50, 50, 0), ('PDAYS', 'NVARCHAR', 50, 50, 50, 0), ('HOUSING', 'VARCHAR', 100, 100, 100, 0)]
        >>> df4 = df1.cast({'AGE': 'VARCHAR(50)', {'PDAYS': 'INT'}})
        >>> df4.dtypes()
        [('AGE', 'VARCHAR', 50, 50, 50, 0), ('PDAYS', 'INT', 50, 50, 50, 0), ('HOUSING', 'VARCHAR', 100, 100, 100, 0)]
        """
        if isinstance(cols, dict):
            projection = [quotename(col) if not col in cols\
                else 'CAST({0} AS {1}) AS {0}'.format(quotename(col), cols[col])\
                    for col in self.columns]
        else:
            cols = DataFrame._get_list_of_str(cols,
                                            'Parameter cols must be a string or a list of strings')
            self._validate_columns_in_dataframe(cols)

            # Need to check for valid types in newtype, skip for now
            projection = [quotename(col) if not col in cols\
                else 'CAST({0} AS {1}) AS {0}'.format(quotename(col), new_type)\
                    for col in self.columns]

        cast_select = 'SELECT {} FROM ({}) AS {}'.format(', '.join(projection),
                                                         self.select_statement,
                                                         self.quoted_name)
        return self._df(cast_select, propagate_safety=True)

    def tail(self, n=1, ref_col=None): #pylint: disable=invalid-name
        """
        Returns a DataFrame of the last ``n`` rows in the current DataFrame.

        Parameters
        ----------
        n : int, optional
            The number of rows returned.

            Defaults to 1.
        ref_col : str or list of str, optional
            Sorting the dataframe based on the ref_col column.

            Defaults to None.

        Returns
        -------
        DataFrame
            A new DataFrame of the last ``n`` rows of the current DataFrame.

        """
        row_numbers = self.count()
        columns = ", ".join(list(map(quotename, self.columns)))
        new_id = 'IDX' + str(uuid.uuid1()).replace('-', '_').upper()
        if ref_col is None:
            order_by = ""
        else:
            if isinstance(ref_col, (list, tuple)):
                order_by = "ORDER BY {} ASC".format(", ".join(list(map(quotename, ref_col))))
            else:
                order_by = "ORDER BY {} ASC".format(quotename(ref_col))
        head_select = 'SELECT {0} FROM (SELECT  ROW_NUMBER() OVER({5}) {1} , * FROM ({2})) WHERE {1} <= {3} AND {1} > {4}'.format(
            columns,
            new_id,
            self.select_statement,
            row_numbers,
            row_numbers - n,
            order_by)
        return self._df(head_select, propagate_safety=True)

    def to_head(self, col):
        """
        Returns a DataFrame with specified column as the first item in the projection.

        Parameters
        ----------
        col : str
            The column to move to the first position.

        Returns
        -------
        DataFrame
            The same data as this DataFrame but with the specified column in the first position.

        Examples
        --------
        Input:

        >>> df1 = cc.table("DBM_TRAINING")
        >>> import pprint
        >>> pprint.pprint(df1.columns)
        ['ID',
         'AGE',
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
         'LABEL']

        Moving the column 'LABEL' to head:

        >>> df2 = df1.to_head('LABEL')
        >>> pprint.pprint(df2.columns)
        ['LABEL',
         'ID',
         'AGE',
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
         'NREMPLOYED']
        """

        if not isinstance(col, _STRING_TYPES):
            raise TypeError('Parameter col must be a string')

        if col not in self.columns:
            raise ValueError('Column {} is not a column in this DataFrame'.format(quotename(col)))
        cols = self.columns
        cols.insert(0, cols.pop(cols.index(col)))
        return self[cols]

    def to_tail(self, col):
        """
        Returns a DataFrame with specified column as the last item in the projection.

        Parameters
        ----------
        col : str
            The column to move to the last position.

        Returns
        -------
        DataFrame
            The same data as this DataFrame but with the specified column in the last position.

        Examples
        --------
        Input:

        >>> df1 = cc.table("DBM_TRAINING")
        >>> import pprint
        >>> pprint.pprint(df1.columns)
        ['LABEL',
         'ID',
         'AGE',
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
         'NREMPLOYED']

        Moving the column 'LABEL' to head:

        >>> df2 = df1.to_tail('LABEL')
        >>> pprint.pprint(df2.columns)
        ['ID',
         'AGE',
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
         'LABEL']
        """

        if not isinstance(col, _STRING_TYPES):
            raise TypeError('Parameter col must be a string')

        if col not in self.columns:
            raise ValueError('Column {} is not a column in this DataFrame'.format(quotename(col)))
        cols = self.columns
        cols.insert(len(cols), cols.pop(cols.index(col)))
        return self[cols]

    def summary(self, cols=None, select_stat=None, pivoted=True):
        """
        Returns a DataFrame that contains various statistics for the requested column(s).

        Parameters
        ----------
        cols : str or list, optional
            The column(s) to be described. Defaults to all columns.

        select_stat : str or list, optional
            The statistics to be selected. "nulls", "unique" and "valid observations" are always computed.
            - "min": the minimum value

            - "lower_quartile": the lower quartile

            - "median": the median value

            - "upper_quartile": the upper quartile

            - "max": the max value

            - "mean": the average

            - "lower_mean_ci" : the lower bound of the mean value

            - "upper_mean_ci": the upper bound of the mean value

            - "trimmed_mean": the average of data after taking out 5% of the largest elements and the same amount of the smallest elements

            - "variance": the variance

            - "sd": the standard deviation

            - "skewness": the measure of the asymmetry of a distribution

            - "kurtosis": the measure of the tailedness of a distribution

        pivoted : bool, optional
            Whether output format is pivoted.

            Default to True.

        Returns
        -------
        DataFrame
            A DataFrame that contains (selected) statistics for the specified column(s).
        """
        if not self.connection_context.is_cloud_version():
            logger.warning("Summary function only supports HANA cloud version!")
        if cols is None:
            cols = self.columns
        data = self.select(cols)
        result = _data_summary(data=data, select_stat=select_stat)
        if not pivoted:
            return result
        pivoted_result = result.pivot_table(values="STAT_VALUE", index="VARIABLE_NAME", columns="STAT_NAME")
        return pivoted_result.select(["VARIABLE_NAME"] + sorted(pivoted_result.deselect("VARIABLE_NAME").columns))

    @property
    def stats(self):
        """
        The statistics of the dataframe.
        """
        if self._stats is not None:
            return self._stats
        try:
            logger.info("Calling PAL summary.")
            temp = self.summary()
            if 'unique' not in temp.columns:
                logger.info("Not latest PAL library. Use describe function instead.")
                temp = self._describe()
        except:
            logger.info("Failed to call describe function. Try to cast table types.")
            cast_dict = {}
            for col_name, col_type in self.get_table_structure().items():
                if 'BIGINT' in col_type or\
                'DECIMAL' in col_type:
                    cast_dict[col_name] = 'DOUBLE'
                    logger.warning("%s has been cast from %s to DOUBLE", col_name, col_type)
                if 'LOB' in col_type or\
                'TIME' in col_type or\
                'DATE' in col_type or\
                'TEXT' in col_type:
                    cast_dict[col_name] = 'VARCHAR(5000)'
                    logger.warning("%s has been cast from %s to VARCHAR", col_name, col_type)
            temp = self.cast(cast_dict)._describe()
        self._stats = temp.collect()
        return self._stats

    def _describe(self, cols=None):
        if cols is None:
            cols = self.columns
        numeric, non_numeric = _univariate_analysis(data=self, cols=cols)
        non_numeric_m = non_numeric.filter("CATEGORY!='__PAL_NULL__' AND STAT_NAME='count'")
        non_numeric_final = self.connection_context.sql("""
        SELECT VARIABLE_NAME, STAT_NAME, SUM("STAT_VALUE") OVER (PARTITION BY "VARIABLE_NAME","STAT_NAME") AS "STAT_VALUE"
        FROM ({})
        """.format(non_numeric_m.select_statement)).replace(to_replace={'STAT_NAME': {'count': 'valid observations'}})
        final_df = numeric.union(non_numeric_final)
        row_count = self.count()
        final_p = final_df.pivot_table(values="STAT_VALUE", index="VARIABLE_NAME", columns="STAT_NAME").add_constant("count", row_count)
        if "nulls" not in final_p.columns:
            final_p = self.connection_context.sql("SELECT *, \"count\" - \"valid observations\" AS \"nulls\" FROM ({})".format(final_p.select_statement))
        final_p_m = final_p.select(["VARIABLE_NAME"] + sorted(final_p.deselect("VARIABLE_NAME").columns))
        unique_cols = []
        if "unique" in final_p_m.columns:
            return final_p_m.cast({"count": "INT", "nulls": "INT", "unique": "INT", "valid observations": "INT"})
        for col in cols:
            unique_cols.append("COUNT(DISTINCT \"{0}\") AS \"{0}\"".format(col))

        unique_df = self.connection_context.sql("""
        SELECT {} FROM ({});
        """.format(", ".join(unique_cols), self.select_statement))
        unique_pf = unique_df.collect()
        unique_pf = unique_pf.transpose()
        unique_pf.reset_index(inplace=True)
        unique_pf = unique_pf.rename(columns = {'index':'VARIABLE_NAME', unique_pf.columns[1]: 'unique'})
        temp_tab = '#unqiue_pf_transpose{}'.format(uuid.uuid1()).replace('-', '_').upper()
        unique_df_p = create_dataframe_from_pandas(self.connection_context, table_name=temp_tab, pandas_df=unique_pf, disable_progressbar=True)
        return unique_df_p.set_index("VARIABLE_NAME").join(final_p_m.set_index("VARIABLE_NAME"), how='left')

    def describe(self, cols=None, version='v1'):
        # The disable of line lengths is for the example in docstring.
        # The example is copy pasted after a run and may result in the output not quite lining up
        """
        Returns a DataFrame that contains various statistics for the requested column(s).

        Parameters
        ----------
        cols : str or list, optional
            The column(s) to be described. Defaults to all columns.

        version : {'v1', 'v2'}, optional
            Version v2 will use PAL instead of dynamic SQL.

            Defaults to 'v1'.

        Returns
        -------
        DataFrame
            A DataFrame that contains statistics for the specified column(s)
            in the current DataFrame.

            The statistics included are:

                - the count of rows ("count"),

                - the number of distinct values ("unique"),

                - the number of nulls ("nulls"),

                - the average ("mean"),

                - the standard deviation("std")

                - the median ("median"),

                - the minimum value ("min"),

                - the maximum value ("max"),

                - the 25% percentile when treated as continuous variable ("25_percent_cont"),

                - the 25% percentile when treated as discrete variable ("25_percent_disc"),

                - the 50% percentile when treated as continuous variable ("50_percent_cont"),

                - the 50% percentile when treated as discrete variable ("50_percent_disc"),

                - the 75% percentile when treated as continuous variable ("75_percent_cont"),

                - the 75% percentile when treated as discrete variable ("75_percent_disc").

            For columns that are strings, statistics such as average ("mean"),
            standard deviation ("std"), median ("median"), and the various percentiles
            are NULLs.

            If the list of columns contain both string and numeric data types,
            minimum and maximum values become NULLs.

        Examples
        --------
        Input:

        >>> df1 = cc.table("DBM_TRAINING")
        >>> import pprint
        >>> pprint.pprint(df2.columns)
        ['LABEL',
         'ID',
         'AGE',
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
         'NREMPLOYED']

        Describe a few numeric columns and collect them to return a Pandas DataFrame:

        >>> df1.describe(['AGE', 'PDAYS']).collect()
          column  count  unique  nulls        mean         std  min  max  median
        0    AGE  16895      78      0   40.051376   10.716907   17   98      38
        1  PDAYS  16895      24      0  944.406688  226.331944    0  999     999
           25_percent_cont  25_percent_disc  50_percent_cont  50_percent_disc
        0             32.0               32             38.0               38
        1            999.0              999            999.0              999
           75_percent_cont  75_percent_disc
        0             47.0               47
        1            999.0              999

        Describe some non-numeric columns and collect them to return a Pandas DataFrame:

        >>> df1.describe(['JOB', 'MARITAL']).collect()
            column  count  unique  nulls  mean   std       min      max median
        0      JOB  16895      12      0  None  None    admin.  unknown   None
        1  MARITAL  16895       4      0  None  None  divorced  unknown   None
          25_percent_cont 25_percent_disc 50_percent_cont 50_percent_disc
        0            None            None            None            None
        1            None            None            None            None
          75_percent_cont 75_percent_disc
        0            None            None
        1            None            None

        Describe all columns in a DataFrame:

        >>> df1.describe().collect()
                    column  count  unique  nulls          mean           std
        0               ID  16895   16895      0  21282.286652  12209.759725
        1              AGE  16895      78      0     40.051376     10.716907
        2         DURATION  16895    1267      0    263.965670    264.331384
        3         CAMPAIGN  16895      35      0      2.344658      2.428449
        4            PDAYS  16895      24      0    944.406688    226.331944
        5         PREVIOUS  16895       7      0      0.209529      0.539450
        6     EMP_VAR_RATE  16895      10      0     -0.038798      1.621945
        7   CONS_PRICE_IDX  16895      26      0     93.538844      0.579189
        8    CONS_CONF_IDX  16895      26      0    -40.334123      4.865720
        9        EURIBOR3M  16895     283      0      3.499297      1.777986
        10      NREMPLOYED  16895      11      0   5160.371885     75.320580
        11             JOB  16895      12      0           NaN           NaN
        12         MARITAL  16895       4      0           NaN           NaN
        13       EDUCATION  16895       8      0           NaN           NaN
        14     DBM_DEFAULT  16895       2      0           NaN           NaN
        15         HOUSING  16895       3      0           NaN           NaN
        16            LOAN  16895       3      0           NaN           NaN
        17         CONTACT  16895       2      0           NaN           NaN
        18       DBM_MONTH  16895      10      0           NaN           NaN
        19     DAY_OF_WEEK  16895       5      0           NaN           NaN
        20        POUTCOME  16895       3      0           NaN           NaN
        21           LABEL  16895       2      0           NaN           NaN
                 min        max     median  25_percent_cont  25_percent_disc
        0      5.000  41187.000  21786.000        10583.500        10583.000
        1     17.000     98.000     38.000           32.000           32.000
        2      0.000   4918.000    184.000          107.000          107.000
        3      1.000     43.000      2.000            1.000            1.000
        4      0.000    999.000    999.000          999.000          999.000
        5      0.000      6.000      0.000            0.000            0.000
        6     -3.400      1.400      1.100           -1.800           -1.800
        7     92.201     94.767     93.444           93.075           93.075
        8    -50.800    -26.900    -41.800          -42.700          -42.700
        9      0.634      5.045      4.856            1.313            1.313
        10  4963.000   5228.000   5191.000         5099.000         5099.000
        11       NaN        NaN        NaN              NaN              NaN
        12       NaN        NaN        NaN              NaN              NaN
        13       NaN        NaN        NaN              NaN              NaN
        14       NaN        NaN        NaN              NaN              NaN
        15       NaN        NaN        NaN              NaN              NaN
        16       NaN        NaN        NaN              NaN              NaN
        17       NaN        NaN        NaN              NaN              NaN
        18       NaN        NaN        NaN              NaN              NaN
        19       NaN        NaN        NaN              NaN              NaN
        20       NaN        NaN        NaN              NaN              NaN
        21       NaN        NaN        NaN              NaN              NaN
            50_percent_cont  50_percent_disc  75_percent_cont  75_percent_disc
        0         21786.000        21786.000        32067.500        32068.000
        1            38.000           38.000           47.000           47.000
        2           184.000          184.000          324.000          324.000
        3             2.000            2.000            3.000            3.000
        4           999.000          999.000          999.000          999.000
        5             0.000            0.000            0.000            0.000
        6             1.100            1.100            1.400            1.400
        7            93.444           93.444           93.994           93.994
        8           -41.800          -41.800          -36.400          -36.400
        9             4.856            4.856            4.961            4.961
        10         5191.000         5191.000         5228.000         5228.000
        11              NaN              NaN              NaN              NaN
        12              NaN              NaN              NaN              NaN
        13              NaN              NaN              NaN              NaN
        14              NaN              NaN              NaN              NaN
        15              NaN              NaN              NaN              NaN
        16              NaN              NaN              NaN              NaN
        17              NaN              NaN              NaN              NaN
        18              NaN              NaN              NaN              NaN
        19              NaN              NaN              NaN              NaN
        20              NaN              NaN              NaN              NaN
        21              NaN              NaN              NaN              NaN
        """

        #pylint:disable=too-many-locals

        if cols is not None:
            msg = 'Parameter cols must be a string or a list of strings'
            cols = DataFrame._get_list_of_str(cols, msg)
            self._validate_columns_in_dataframe(cols)
        else:
            cols = self.columns
        if version == 'v2':
            return self._describe(cols=cols)
        dtypes = self.dtypes(cols)
        # Not sure if this is the complete list but should cover most data types
        # Note that we don't cover BLOB/LOB types
        numerics = [col_info[0] for col_info in dtypes
                    if col_info[1] == "INT" or
                    col_info[1] == 'SMALLINT' or
                    col_info[1] == 'TINYINT' or
                    col_info[1] == 'BIGINT' or
                    col_info[1] == 'INTEGER' or
                    col_info[1] == 'DOUBLE' or
                    col_info[1] == 'DECIMAL' or
                    col_info[1] == 'FLOAT']
        non_numerics = [col_info[0] for col_info in dtypes
                        if col_info[1] == "NCHAR" or
                        col_info[1] == 'NVARCHAR' or
                        col_info[1] == 'CHAR' or
                        col_info[1] == 'VARCHAR' or
                        col_info[1] == 'STRING' or
                        col_info[1] == 'TIMESTAMP' or
                        col_info[1] == 'DATE' or
                        col_info[1] == 'NCLOB']

        # The reason to separate numerics and non-numerics is the calculation
        # of min and max. These functions are of different types when calculating
        # for different types. So, a numeric type will return a numeric value
        # while a character column will return a character value for min and max
        # When both column types are present, the values for min and max would be null
        # for non numeric types
        sql_numerics = None
        sql_non_numerics = None
        min_max = 'MIN({0}) as "min", MAX({0}) as "max", '
        if numerics:
            sql_for_numerics = ('select {3} as "column", COUNT({0}) as "count", ' +
                                'COUNT(DISTINCT {0}) as "unique", ' +
                                'SUM(CASE WHEN {0} is NULL THEN 1 ELSE 0 END) as "nulls", ' +
                                'AVG(TO_DOUBLE({0})) as "mean", STDDEV({0}) as "std", ' +
                                min_max + 'MEDIAN({0}) as "median" ' +
                                'FROM ({1}) AS {2}')
            union = [sql_for_numerics.format(quotename(col), self.select_statement,
                                            self.quoted_name,
                                            "'{}'".format(col.replace("'", "''")))
                    for col in numerics]
            sql_simple_stats = " UNION ALL ".join(union)

            percentiles = ('SELECT {3} as "column", * FROM (SELECT ' +
                        'percentile_cont(0.25) WITHIN GROUP (ORDER BY {0}) ' +
                        'AS "25_percent_cont", '+
                        'percentile_disc(0.25) WITHIN GROUP (ORDER BY {0}) ' +
                        'AS "25_percent_disc", '+
                        'percentile_cont(0.50) WITHIN GROUP (ORDER BY {0}) ' +
                        'AS "50_percent_cont", '+
                        'percentile_disc(0.50) WITHIN GROUP (ORDER BY {0}) ' +
                        'AS "50_percent_disc", '+
                        'percentile_cont(0.75) WITHIN GROUP (ORDER BY {0}) ' +
                        'AS "75_percent_cont", '+
                        'percentile_disc(0.75) WITHIN GROUP (ORDER BY {0}) ' +
                        'AS "75_percent_disc" '+
                        'FROM ({1}) AS {2})')
            union = [percentiles.format(quotename(col), self.select_statement,
                                        self.quoted_name,
                                        "'{}'".format(col.replace("'", "''"))) for col in numerics]
            sql_percentiles = " UNION ALL ".join(union)

            sql_numerics = ('SELECT {0}.*, '.format('"SimpleStats"') +
                            ', '.join(['{0}."{1}_percent_cont", {0}."{1}_percent_disc"'.
                                    format('"Percentiles"', percentile)
                                    for percentile in [25, 50, 75]]) +
                            ' FROM ({0}) AS {1}, ({2}) AS {3}'.
                            format(sql_simple_stats, '"SimpleStats"',
                                sql_percentiles, '"Percentiles"') +
                            ' WHERE {0}."column" = {1}."column"'.
                            format('"SimpleStats"', '"Percentiles"'))
            # This is to handle the case for non-numerics since min and max values
            # are now not compatible between numerics and non-numerics
            min_max = 'CAST(NULL AS DOUBLE) AS "min", CAST(NULL AS DOUBLE) AS "max", '

        if non_numerics:
            sql_for_non_numerics = ('select {3} as "column", COUNT({0}) as "count", ' +
                                    'COUNT(DISTINCT {0}) as "unique", ' +
                                    'SUM(CASE WHEN {0} IS NULL THEN 1 ELSE 0 END) as "nulls", ' +
                                    'CAST(NULL as DOUBLE) AS "mean", ' +
                                    'CAST(NULL as double) as "std", ' +
                                    'CAST(NULL as DOUBLE) AS "min", ' +
                                    'CAST(NULL as DOUBLE) AS "max", ' +
                                    'CAST(NULL as DOUBLE) AS "median", ' +
                                    'CAST(NULL AS DOUBLE) AS "25_percent_cont", ' +
                                    'CAST(NULL AS DOUBLE) AS "25_percent_disc", ' +
                                    'CAST(NULL AS DOUBLE) AS "50_percent_cont", ' +
                                    'CAST(NULL AS DOUBLE) AS "50_percent_disc", ' +
                                    'CAST(NULL AS DOUBLE) AS "75_percent_cont", ' +
                                    'CAST(NULL AS DOUBLE) AS "75_percent_disc" ' +
                                    'FROM ({1}) AS {2}')
            union = [sql_for_non_numerics.format(quotename(col), self.select_statement,
                                                self.quoted_name,
                                                "'{}'".format(col.replace("'", "''")))
                    for col in non_numerics]
            sql_non_numerics = " UNION ALL ".join(union)

        if sql_numerics is None and sql_non_numerics is None:
            raise ValueError('Parameter cols cannot be described')

        if sql_numerics is not None and sql_non_numerics is not None:
            sql_combined = ('SELECT * FROM ({0}) AS {1} UNION ALL SELECT * FROM ({2}) AS {3}'.
                            format(sql_numerics,
                                '"Numerics"',
                                sql_non_numerics,
                                '"NonNumerics"'))
            return self._df(sql_combined, propagate_safety=True)
        if sql_numerics is not None:
            return self._df(sql_numerics, propagate_safety=True)

        return self._df(sql_non_numerics, propagate_safety=True)

    def bin(self, col, strategy='uniform_number', bins=None, bin_width=None,  #pylint: disable=too-many-arguments
            bin_column='BIN_NUMBER'):
        """
        Returns a DataFrame with the original columns as well as bin assignments.

        The name of the columns in the returned DataFrame is the same as the
        original column. Column "BIN_NUMBER" or the specified value in
        ``bin_column`` is added and corresponds to the bin assigned.

        Parameters
        ----------
        col : str
            The column on which binning is performed.
            The column must be numeric.
        strategy : {'uniform_number', 'uniform_size'}, optional
            Binning methods:

                - 'uniform_number': Equal widths based on the number of bins.
                - 'uniform_size': Equal widths based on the bin size.

            Default value is 'uniform_number'.

        bins : int, optional
            The number of equal-width bins.
            Only valid when ``strategy`` is 'uniform_number'.

            Defaults to 10.

        bin_width : int, optional
            The interval width of each bin.
            Only valid when ``strategy`` is 'uniform_size'.

        bin_column : str, optional
            The name of the output column that contains the bin number.

        Returns
        -------
        DataFrame
            A binned dataset with the same data as this DataFrame,
            as well as an additional column "BIN_NUMBER" or the value specified
            in ``bin_column``. This additional column contains the
            assigned bin for each row.

        Examples
        --------
        Input:

        >>> df.collect()
           C1   C2    C3       C4
        0   1  1.2   2.0      1.0
        1   2  1.4   4.0      3.0
        2   3  1.6   6.0      9.0
        3   4  1.8   8.0     27.0
        4   5  2.0  10.0     81.0
        5   6  2.2  12.0    243.0
        6   7  2.4  14.0    729.0
        7   8  2.6  16.0   2187.0
        8   9  2.8  18.0   6561.0
        9  10  3.0  20.0  19683.0

        Create five bins of equal widths on C1:

        >>> df.bin('C1', strategy='uniform_number', bins=5).collect()
           C1   C2    C3       C4  BIN_NUMBER
        0   1  1.2   2.0      1.0           1
        1   2  1.4   4.0      3.0           1
        2   3  1.6   6.0      9.0           2
        3   4  1.8   8.0     27.0           2
        4   5  2.0  10.0     81.0           3
        5   6  2.2  12.0    243.0           3
        6   7  2.4  14.0    729.0           4
        7   8  2.6  16.0   2187.0           4
        8   9  2.8  18.0   6561.0           5
        9  10  3.0  20.0  19683.0           5

        Create five bins of equal widths on C2:

        >>> df.bin('C3', strategy='uniform_number', bins=5).collect()
           C1   C2    C3       C4  BIN_NUMBER
        0   1  1.2   2.0      1.0           1
        1   2  1.4   4.0      3.0           1
        2   3  1.6   6.0      9.0           2
        3   4  1.8   8.0     27.0           2
        4   5  2.0  10.0     81.0           3
        5   6  2.2  12.0    243.0           3
        6   7  2.4  14.0    729.0           4
        7   8  2.6  16.0   2187.0           4
        8   9  2.8  18.0   6561.0           5
        9  10  3.0  20.0  19683.0           5

        Create five bins of equal widths on a column that varies significantly:

        >>> df.bin('C4', strategy='uniform_number', bins=5).collect()
           C1   C2    C3       C4  BIN_NUMBER
        0   1  1.2   2.0      1.0           1
        1   2  1.4   4.0      3.0           1
        2   3  1.6   6.0      9.0           1
        3   4  1.8   8.0     27.0           1
        4   5  2.0  10.0     81.0           1
        5   6  2.2  12.0    243.0           1
        6   7  2.4  14.0    729.0           1
        7   8  2.6  16.0   2187.0           1
        8   9  2.8  18.0   6561.0           2
        9  10  3.0  20.0  19683.0           5

        Create bins of equal width:

        >>> df.bin('C1', strategy='uniform_size', bin_width=3).collect()
           C1   C2    C3       C4  BIN_NUMBER
        0   1  1.2   2.0      1.0           1
        1   2  1.4   4.0      3.0           1
        2   3  1.6   6.0      9.0           2
        3   4  1.8   8.0     27.0           2
        4   5  2.0  10.0     81.0           2
        5   6  2.2  12.0    243.0           3
        6   7  2.4  14.0    729.0           3
        7   8  2.6  16.0   2187.0           3
        8   9  2.8  18.0   6561.0           4
        9  10  3.0  20.0  19683.0           4
        """

        if not isinstance(col, _STRING_TYPES):
            raise TypeError('Parameter col must be a string.')

        self._validate_columns_in_dataframe([col])

        if not self.is_numeric(col):
            raise ValueError('Parameter col must be a numeric column.')
        if not isinstance(strategy, _STRING_TYPES):
            raise TypeError('Parameter strategy must be a string')
        strategy = strategy.lower()
        if strategy not in ['uniform_number', 'uniform_size']:
            raise TypeError('Parameter strategy must be one of "uniform_number", "uniform_size".')

        if strategy == 'uniform_number':
            if bins is None:
                bins = 10
            if bin_width is not None:
                raise ValueError('Parameter bin_size invalid with strategy "uniform_number"')
        elif strategy == 'uniform_size':
            if (not isinstance(bin_width, _INTEGER_TYPES) and
                    not isinstance(bin_width, float)):
                raise TypeError('Parameter bin_width must be a numeric.')

        sql_template = 'SELECT *, BINNING(VALUE => {}, {} => {}) OVER() AS {} FROM ({}) AS {}'

        bin_select = sql_template.format(
            quotename(col),
            'BIN_COUNT' if strategy == 'uniform_number' else 'BIN_WIDTH',
            bins if strategy == 'uniform_number' else bin_width,
            quotename(bin_column), self.select_statement, self.quoted_name)
        return self._df(bin_select, propagate_safety=True)

    def agg(self, agg_list, group_by=None):
        """
        Returns a SAP HANA DataFrame with the group_by column along with the aggregates.
        This method supports all aggregation functions in the SAP HANA database instance, such as 'max', 'min',
        'count', 'avg', 'sum', 'median', 'stddev', 'var'.
        The name of the column in the returned DataFrame is the same as the
        original column.

        Aggregation functions can be referred to `SAP HANA aggregate functions\
        <https://help.sap.com/viewer/7c78579ce9b14a669c1f3295b0d8ca16/Cloud/en-US/6fff7f0ae9184d1db47a25791545a1b6.html>`_.

        Parameters
        ----------

        agg_list : A list of tuples

            A list of tuples. Each tuple is a triplet.
            The triplet consists of (aggregate_operator, expression, name) where:

                - aggregate_operator is one of ['max', 'min', 'count', 'avg',
                  'sum', 'median', 'stddev', 'var', ...].
                  The operator name is identical to SAP HANA sql naming and we support
                  all aggregation functions in the SAP HANA database instance.

                - expression is a str that is a column or column expression

                - name is the name of this aggregate in the project list.

        group_by : str or list of str, optional

            The group by column. Only a column is allowed although
            expressions are allowed in SQL. To group by an expression, create a
            DataFrame  by providing the entire SQL.
            So, if you have a table T with columns C1, C2, and C3 that are all
            integers, to calculate the max(C1) grouped by (C2+C3) a DataFrame
            would need to be created as below:

              >>> cc.sql('SELECT "C2"+"C3", max("C1") FROM "T" GROUP BY "C2"+"C3"')

        Returns
        -------

        DataFrame
            A DataFrame containing the group_by column (if it exists), as well as
            the aggregate expressions that are aliased with the specified names.

        Examples
        --------

        Input:

        >>> df.collect()
            ID  SEPALLENGTHCM  SEPALWIDTHCM  PETALLENGTHCM  PETALWIDTHCM          SPECIES
        0    1            5.1           3.5            1.4           0.2      Iris-setosa
        1    2            4.9           3.0            1.4           0.2      Iris-setosa
        2    3            4.7           3.2            1.3           0.2      Iris-setosa
        3   51            7.0           3.2            4.7           1.4  Iris-versicolor
        4   52            6.4           3.2            4.5           1.5  Iris-versicolor
        5  101            6.3           3.3            6.0           2.5   Iris-virginica
        6  102            5.8           2.7            5.1           1.9   Iris-virginica
        7  103            7.1           3.0            5.9           2.1   Iris-virginica
        8  104            6.3           2.9            5.6           1.8   Iris-virginica

        Another way to do a count:

        >>> df.agg([('count', 'SPECIES', 'COUNT')]).collect()
            COUNT
        0      9

        Get counts by SPECIES:

        >>> df.agg([('count', 'SPECIES', 'COUNT')], group_by='SPECIES').collect()
                   SPECIES  COUNT
        0  Iris-versicolor      2
        1   Iris-virginica      4
        2      Iris-setosa      3

        Get max values of SEPALLENGTHCM by SPECIES:

        >>> df.agg([('max', 'SEPALLENGTHCM', 'MAX_SEPAL_LENGTH')], group_by='SPECIES').collect()
                   SPECIES  MAX_SEPAL_LENGTH
        0  Iris-versicolor               7.0
        1   Iris-virginica               7.1
        2      Iris-setosa               5.1

        Get max and min values of SEPALLENGTHCM by SPECIES:

        >>> df.agg([('max', 'SEPALLENGTHCM', 'MAX_SEPAL_LENGTH'),
            ('min', 'SEPALLENGTHCM', 'MIN_SEPAL_LENGTH')], group_by=['SPECIES']).collect()
                   SPECIES  MAX_SEPAL_LENGTH  MIN_SEPAL_LENGTH
        0  Iris-versicolor               7.0               6.4
        1   Iris-virginica               7.1               5.8
        2      Iris-setosa               5.1               4.7

        Get aggregate grouping by multiple columns:

        >>> df.agg([('count', 'SEPALLENGTHCM', 'COUNT_SEPAL_LENGTH')],
                    group_by=['SPECIES', 'PETALLENGTHCM']).collect()
                   SPECIES  PETALLENGTHCM  COUNT_SEPAL_LENGTH
        0   Iris-virginica            6.0                   1
        1      Iris-setosa            1.3                   1
        2   Iris-virginica            5.9                   1
        3   Iris-virginica            5.6                   1
        4      Iris-setosa            1.4                   2
        5  Iris-versicolor            4.7                   1
        6  Iris-versicolor            4.5                   1
        7   Iris-virginica            5.1                   1
        """

        if group_by is not None:
            msg = 'Parameter group_by must be a string or a list of strings.'
            group_by = DataFrame._get_list_of_str(group_by, msg)
            self._validate_columns_in_dataframe(group_by)
            group_by = [quotename(gb) for gb in group_by]

        if not isinstance(agg_list, list):
            raise TypeError('Parameter agg_list must be a list.')
        if not agg_list:
            raise ValueError('Parameter agg_list must contain at least one tuple.')
        aggregates = []
        for item in agg_list:
            if not isinstance(item, tuple):
                raise TypeError('Parameter agg_list must be a tuple with 3 elements.')
            if len(item) != 3:
                raise TypeError('Parameter agg_list must be a list of tuples with 3 elements.')
            (agg, expr, name) = item
            agg = agg.lower()
            aggregates.append('{}({}) AS {}'.format(agg, quotename(expr), quotename(name)))

        sql = ('SELECT {} {} FROM ({}) AS {}{}'
               .format('' if group_by is None else (','.join(group_by) + ','),
                       ', '.join(aggregates),
                       self.select_statement,
                       self.quoted_name,
                       '' if group_by is None
                       else ' GROUP BY {}'.format(','.join(group_by))))
        return self._df(sql)

    def is_numeric(self, cols=None):
        """
        Returns True if the column(s) in the DataFrame are numeric.

        Parameters
        ----------
        cols : str or list, optional
            The column(s) to be tested for being numeric.

            Defaults to all columns.

        Returns
        -------
        bool
            True if all the columns are numeric.

        Examples
        --------
        Input:

        >>> df.head(5).collect()
           ID  SEPALLENGTHCM  SEPALWIDTHCM  PETALLENGTHCM  PETALWIDTHCM      SPECIES
        0   1            5.1           3.5            1.4           0.2  Iris-setosa
        1   2            4.9           3.0            1.4           0.2  Iris-setosa
        2   3            4.7           3.2            1.3           0.2  Iris-setosa
        3   4            4.6           3.1            1.5           0.2  Iris-setosa
        4   5            5.0           3.6            1.4           0.2  Iris-setosa

        >>> pprint.pprint(df.dtypes())
        [('ID', 'INT', 10, 10, 10, 0),
         ('SEPALLENGTHCM', 'DOUBLE', 15, 15, 15, 0),
         ('SEPALWIDTHCM', 'DOUBLE', 15, 15, 15, 0),
         ('PETALLENGTHCM', 'DOUBLE', 15, 15, 15, 0),
         ('PETALWIDTHCM', 'DOUBLE', 15, 15, 15, 0),
         ('SPECIES', 'NVARCHAR', 15, 15, 15, 0)]

        Test a single column:

        >>> df.is_numeric('ID')
        True
        >>> df.is_numeric('SEPALLENGTHCM')
        True
        >>> df.is_numeric(['SPECIES'])
        False

        Test a list of columns:

        >>> df.is_numeric(['SEPALLENGTHCM', 'PETALLENGTHCM', 'PETALWIDTHCM'])
        True
        >>> df.is_numeric(['SEPALLENGTHCM', 'PETALLENGTHCM', 'SPECIES'])
        False
        """

        if cols is not None:
            msg = 'Parameter cols must be a string or a list of strings.'
            cols = DataFrame._get_list_of_str(cols, msg)
            self._validate_columns_in_dataframe(cols)
        else:
            cols = self.columns

        return all(col_info[1] in ['INT', 'TINYINT', 'BIGINT', 'SMALLINT',
                                   'INTEGER', 'DOUBLE', 'DECIMAL', 'FLOAT']
                   for col_info in self.dtypes(cols))

    def corr(self, first_col, second_col):
        """
        Returns a DataFrame that gives the correlation coefficient between two
        numeric columns.

        All rows with NULL values for ``first_col`` or ``second_col`` are removed
        prior to calculating the correlation coefficient.

        Let `col1` be the values of ``first_col`` and `col2` be the values of ``second_col``,
        then the correlation coefficient is:

            1/(n-1) * sum((col1 - avg(col1)) * (col2 - avg(col2))) /
            (stddev(col1) * stddev(col2))

        Parameters
        ----------
        first_col : str
            The first column for calculating the correlation coefficient.
        second_col : str
            The second column for calculating the correlation coefficient.

        Returns
        -------
        DataFrame
            A DataFrame with one value that contains the correlation coefficient.
            The name of the column is CORR_COEFF.

        Examples
        --------
        Input:

        >>> df.columns
        ['C1', 'C2', 'C3', 'C4']
        >>> df.collect()
           C1   C2      C3       C4
        0   1  1.2     2.0      1.0
        1   2  1.4     4.0      3.0
        2   3  1.6     8.0      9.0
        3   4  1.8    16.0     27.0
        4   5  2.0    32.0     81.0
        5   6  2.2    64.0    243.0
        6   7  2.4   128.0    729.0
        7   8  2.6   256.0   2187.0
        8   9  2.8   512.0   6561.0
        9  10  3.0  1024.0  19683.0

        Correlation with columns that are well correlated:

        >>> df.corr('C1', 'C2').collect()
           CORR_COEFF
        0         1.0

        >>> df.corr('C1', 'C3').collect()
           CORR_COEFF
        0         1.0

        Correlation with a column whose value is three times its previous value:

        >>> df.corr('C1', 'C4').collect()
           CORR_COEFF
        0    0.696325
        """

        if not isinstance(first_col, _STRING_TYPES):
            raise TypeError('Parameter first_col must be a string.')
        if not isinstance(second_col, _STRING_TYPES):
            raise TypeError('Parameter second_col must be a string.')
        if not self.is_numeric([first_col, second_col]):
            raise ValueError('Correlation columns {0} and {1} must be numeric.'.
                             format(first_col, second_col))
        corr_select = "SELECT CORR(\"{}\", \"{}\") FROM ({})".format(first_col,
                                                                     second_col,
                                                                     self.select_statement)

        dfc = self._df(corr_select, propagate_safety=True)
        return dfc

    def min(self):
        """
        Gets the minimum value of the columns. It simplifies the use of agg function.

        Returns
        -------

            scalar or Series
        """
        if len(self.columns) > 1:
            agg_list = []
            for col in self.columns:
                agg_list.append(('min', col, col))
            return self.agg(agg_list).collect(geometries=False).iloc[0]
        return self.agg([('min', self.columns[0], self.columns[0])]).collect(geometries=False).iat[0, 0]

    def max(self):
        """
        Gets the maximum value of the columns. It simplifies the use of agg function.

        Returns
        -------

            scalar or Series
        """
        if len(self.columns) > 1:
            agg_list = []
            for col in self.columns:
                agg_list.append(('max', col, col))
            return self.agg(agg_list).collect(geometries=False).iloc[0]
        return self.agg([('max', self.columns[0], self.columns[0])]).collect(geometries=False).iat[0, 0]

    def sum(self):
        """
        Gets the summation of the columns. It simplifies the use of agg function.

        Returns
        -------

            scalar or Series
        """
        if len(self.columns) > 1:
            agg_list = []
            for col in self.columns:
                agg_list.append(('sum', col, col))
            return self.agg(agg_list).collect(geometries=False).iloc[0]
        return self.agg([('sum', self.columns[0], self.columns[0])]).collect(geometries=False).iat[0, 0]

    def median(self):
        """
        Gets the median value of the columns. It simplifies the use of agg function.

        Returns
        -------

            scalar or Series
        """
        if len(self.columns) > 1:
            agg_list = []
            for col in self.columns:
                agg_list.append(('median', col, col))
            return self.agg(agg_list).collect(geometries=False).iloc[0]
        return self.agg([('median', self.columns[0], self.columns[0])]).collect(geometries=False).iat[0, 0]

    def mean(self):
        """
        Gets the mean value of the columns. It simplifies the use of agg function.

        Returns
        -------

            scalar or Series
        """
        if len(self.columns) > 1:
            agg_list = []
            for col in self.columns:
                agg_list.append(('avg', col, col))
            return self.agg(agg_list).collect(geometries=False).iloc[0]
        return self.agg([('avg', self.columns[0], self.columns[0])]).collect(geometries=False).iat[0, 0]

    def stddev(self):
        """
        Gets the stddev value of the columns. It simplifies the use of agg function.

        Returns
        -------

            scalar or Series
        """
        if len(self.columns) > 1:
            agg_list = []
            for col in self.columns:
                agg_list.append(('stddev', col, col))
            return self.agg(agg_list).collect(geometries=False).iloc[0]
        return self.agg([('stddev', self.columns[0], self.columns[0])]).collect(geometries=False).iat[0, 0]

    def value_counts(self, subset=None):
        """
        Gets the value counts of the columns. It simplifies the use of agg function.

        Parameters
        ----------
        subset : list, optional
            Columns to use when counting unique combinations.

        Returns
        -------

            DataFrame
        """
        if subset is None:
            subset = self.columns
        count_df = []
        id_df = []
        for col in subset:
            id_df.append(self.select(col).rename_columns({col: "VALUES"}).cast("VALUES", 'NVARCHAR(255)'))
            count_df.append(self.agg([("count", col, "NUM_{}".format(col))], group_by=col).cast(col, 'NVARCHAR(255)').set_index(col))
        idf = id_df[0].union(id_df[1:]).distinct().set_index("VALUES")
        idf = id_df[0].union(id_df[1:]).distinct().set_index("VALUES")
        return idf.join(count_df, how="left")

    def pivot_table(self, values, index, columns, aggfunc='avg'):
        """
        Returns a DataFrame that gives the pivoted table.

        ``aggfunc`` is identical to `SAP HANA aggregate functions\
        <https://help.sap.com/viewer/7c78579ce9b14a669c1f3295b0d8ca16/Cloud/en-US/6fff7f0ae9184d1db47a25791545a1b6.html>`_.

        Parameters
        ----------
        values : str or list of str
            The targeted values for pivoting.
        index : str or list of str
            The index of the DataFrame.
        columns : str or list of str
            The pivoting columns.
        aggfunc : {'avg', 'max', 'min',... }, optional
            ``aggfunc`` is identical to SAP HANA aggregate functions.
            Defaults to 'avg'.

        Returns
        -------
        DataFrame
            A pivoted DataFrame.

        Examples
        --------
        df is a SAP HANA DataFrame.

        >>> df.pivot_table(values='C2', index='C1', columns='C3', aggfunc='max')

        """
        columns_tmp = None
        index_tmp = None
        if self.hasna(columns):
            logger.warning("`columns` contains NULL value! It will be replaced by the string 'None'.")
        new_df = self
        if isinstance(columns, list):
            columns_tmp = " || '_' || ".join(quotename(col) for col in columns)
            new_df = self.fillna('None', columns)
        else:
            columns_tmp = quotename(columns)
            new_df = self.fillna('None', [columns])
        if isinstance(index, str) is False:
            index_tmp = ", ".join(quotename(col) for col in index)
        else:
            index_tmp = quotename(index)
        col_set = self.__run_query('SELECT ' + ' distinct ' + columns_tmp + ' FROM (' + self.select_statement + ')')
        col_set = set(map(lambda x: str(x[0]), col_set))
        sql_script = 'SELECT ' + index_tmp + ', '
        for col in col_set:
            if not isinstance(values, list):
                sql_script = sql_script + aggfunc + '(CASE WHEN ' + columns_tmp + \
                '=' + "'" + col + "'" + ' THEN ' + quotename(values) + ' END) AS ' + quotename(col) + ','
            else:
                for val in values:
                    sql_script = sql_script + aggfunc + '(CASE WHEN ' + columns_tmp + \
                    '=' + "'" + col + "'" + ' THEN ' + quotename(val) + ' END) AS ' + quotename(col + '|' + val) + ','
        sql_script = sql_script[:-1]
        sql_script = sql_script + ' FROM (' + new_df.select_statement + ') GROUP BY ' + index_tmp
        return self._df(sql_script)

    # SQLTRACE
    def generate_table_type(self):
        """
        Generates a SAP HANA table type based on the dtypes function of the DataFrame. This is a convenience method for SQL tracing.

        Parameters
        ----------
        None

        Returns
        -------
        str
            The table type in string form.

        """

        dtypes = self.dtypes()
        type_string = "" + "("
        first = True
        for rows in dtypes:
            if first:
                first = False
            else:
                type_string = type_string + ","
            if 'VARCHAR' in rows[1]:
                type_part = "\"" + rows[0] +"\" " + rows[1] + "(" + str(rows[2]) + ")"
                type_string = type_string + type_part
            elif rows[1] == 'DECIMAL':
                type_part = "\"" + rows[0] +"\" " + rows[1] + "(" + str(rows[4]) + "," + str(rows[5]) + ")"
                type_string = type_string + type_part
            else:
                type_part = "\"" + rows[0] +"\""  + " " + rows[1]
                type_string = type_string + type_part
        type_string = type_string + ")"
        table_types = "table {}".format(type_string)
        return table_types

    def rearrange(self, key=None, features=None, label=None, type_ts=False,
                  for_predict=False):
        """
        Utility function to generate a new dataframe with [key, features, label] for non time-series dataset
        and [key, label, features] for time-series dataset.

        Parameters
        ----------
        key : str, optional
            Name of the ID column.

            If ``key`` is not provided, then:

                - if ``data`` is indexed by a single column, then ``key`` defaults
                  to that index column;

                - otherwise, it is assumed that ``data`` contains no ID column.

        features : list of str, optional
            Names of the feature columns.

            If ``features`` is not provided, it defaults to all non-ID, non-label columns.

        label : str, optional
            Name of the dependent variable.

            Defaults to

               - the name of the last non-ID column if ``type_ts`` is False
               - the name of the 1st non-ID column if ``type_ts`` is True.

        type_ts : str, optional
            Specifies whether or not the input DataFrame is time-series data.

            Defaults to False.

        for_predict : str, optional
            Specifies whether ``data`` is for predict, in which case ``label`` should not be provided.

            Defaults to False.
        """
        if isinstance(self.index, str):
            if key is not None and self.index != key:
                msg = "Discrepancy between the designated key column '{}' ".format(key) +\
                "and the designated index column '{}'.".format(self.index)
                logger.warning(msg)
        key = self.index if key is None else key
        cols = self.columns
        if key is not None:
            id_col = [key]
            cols.remove(key)
        elif type_ts == True:
            msg = "Parameter 'key' must be provided for time-series data."
            logger.error(msg)
            raise ValueError(msg)
        else:
            id_col = []
        if label is None:
            if not for_predict:
                label = cols[0] if type_ts else cols[-1]
            else:
                label = []
        if label:
            cols.remove(label)
        features = [features] if isinstance(features, str) else features
        if features is None:
            features = cols
        label_cols = [label] if label else label
        return self[id_col + label_cols + features] if type_ts else self[id_col + features + label_cols]

    def set_source_table(self, table, schema=None):
        """
        Specifies the source table for the current dataframe.

        Parameters
        ----------
        table : str
            The table name.
        schema : str, optional, keyword-only
            The schema name. If this value is not provided or set to None, then the value defaults to the
            ConnectionContext's current schema.
        """
        if schema is None:
            try:
                schema = self.connection_context.get_current_schema()
            except:
                schema = "DM_PAL_FOR_UNITTEST"
        self.source_table = {"SCHEMA_NAME": schema, "TABLE_NAME": table}

    def to_pickle(self, path, compression='infer', protocol=4):
        """
        Pickle object to file.

        Parameters
        ----------
        path : str
            File path where the pickled object will be stored.
        compression : {'infer', 'gzip', 'bz2', 'zip', 'xz', None}
            A string representing the compression to use in the output file. By default,
            infers from the file extension in specified path.

            Defaults to 'infer'.
        protocol : int
            Int which indicates which protocol should be used by the pickler, default HIGHEST_PROTOCOL.
        """
        pandas_df = self.collect()
        pandas_df.to_pickle(path, compression, protocol)

    def to_datetime(self, cols):
        """
        Converts target columns to the specified date format. Default format is "YYYY-MM-DD HH24:MI:SS".

        Parameters
        ----------
        cols : str, list of str or dict
            If cols is str or list of str, the default format will be used. Otherwise, the specified format should be provided in dict.
            E.g. cols={"DATETIME": "MM/DD/YYYY HH24:MI:SS"}
        """
        selected_cols = []
        for col in self.columns:
            if col in cols:
                if isinstance(cols, dict):
                    if ':' in cols[col]:
                        if '/' in cols[col] or ' ' in cols[col]:
                            conv = 'TO_TIMESTAMP'
                        else:
                            conv = 'TO_TIME'
                    else:
                        conv = 'TO_DATE'
                    selected_cols.append("{0}({1}, '{2}') {1}".format(conv, quotename(col), cols[col]))
                else:
                    selected_cols.append("TO_TIMESTAMP({0}, 'YYYY-MM-DD HH24:MI:SS') {0}".format(quotename(col)))
            else:
                selected_cols.append(quotename(col))
        return self._df("SELECT {} FROM ({})".format(", ".join(selected_cols), self.select_statement))

    def generate_feature(self,
                         targets,
                         group_by=None,
                         agg_func=None,
                         trans_func=None,
                         order_by=None,
                         trans_param=None,
                         rolling_window=None,
                         second_targets=None):
        """
        Add additional features to the existing dataframe using agg_func and trans_func.

        Parameters
        ----------
        targets : str or list of str
            The column(s) in data to be feature engineered.
        group_by : str, optional
            The column in data for group by when performing agg_func.
        agg_func : str, optional
            HANA aggregation operations. SUM, COUNT, MIN, MAX, ...
        trans_func : str, optional
            HANA transformation operations. MONTH, YEAR, LAG, ...

            A special transformation is `GEOHASH_HIERARCHY`. This creates features
            based on a GeoHash. The default length of 20 for the hash can be
            influenced by respective trans parameters. Providing for example
            `range(3, 11)`, the operation adds 7 features with a length of the
            GeoHash between 3 and 10.
        order_by : str, optional
            LEAD, LAG function requires an OVER(ORDER_BY) window specification.
        trans_param : list, optional
            Parameters for transformation operations corresponding to targets.
        rolling_window : int, optional
            Window size for rolling function. If negative, it will use the points before CURRENT ROW.
        second_targets : str or list of str
            The second column(s) in data to be feature engineered like CORR.

        Returns
        -------

        DataFrame
            SAP HANA DataFrame with new features.

        Examples
        --------

        >>> df.head(5).collect()
                           TIME  TEMPERATURE    HUMIDITY      OXYGEN          CO2
        0   2021-01-01 12:00:00    19.972199   29.271170   23.154523   504.806395
        1   2021-01-01 12:00:10    19.910014   27.931855   23.009835   507.515937
        2   2021-01-01 12:00:20    19.834676   26.051309   22.756407   510.111974
        3   2021-01-01 12:00:30    19.952517   26.007655   22.737376   516.993696
        4   2021-01-01 12:00:40    20.163497   26.056979   22.469276   528.337481
        >>> df.generate_feature(targets=["TEMPERATURE", "HUMIDITY", "OXYGEN", "CO2"],
                                trans_func="LAG",
                                order_by="TIME",
                                trans_param=[range(1, 7), range(1, 5), range(1, 5), range(1,7)]).dropna().deselect("TIME").head(2).collect()
          TEMPERATURE     HUMIDITY     OXYGEN          CO2 LAG(TEMPERATURE, 1)  ...  LAG(CO2, 4)
        0   20.978001   26.187823   21.982030   522.731895           20.701740  ...  510.111974
        1   21.234148   25.703989   21.804864   528.066402           20.978001  ...  516.993696
        """
        def geohash_hierarchy(column_name: str, max_length: int) -> str:
            """Helper for the GeoHash Transformation"""
            geohash = '{}.ST_GeoHash({})'.format(quotename(column_name), max_length)
            geohash = geohash + ' AS "GEOHASH_HIERARCHY({},{})"'.format(column_name, max_length)

            return geohash

        view_sql = self.select_statement
        if not isinstance(targets, (tuple, list)):
            targets = [targets]
        if second_targets:
            if not isinstance(second_targets, (tuple, list)):
                second_targets = [second_targets]
            second_targets = list(map(lambda x: ', ' + quotename(x), second_targets))
        else:
            second_targets = [''] * len(targets)
        dummy_list = range(2, 2 + len(targets))
        if agg_func is not None:
            if group_by is None:
                raise Exception("group_by cannot be None!")
            agg_keyword_list = ['"{}({}{})"'.format(agg_func, target, second_target) for target, second_target in zip(targets, second_targets)]
            agg_sql_list = ["SELECT {}, {}({}) {} FROM ({}) GROUP BY {}".format(quotename(group_by),\
                agg_func, quotename(target), agg_keyword, view_sql, quotename(group_by))\
                for target, agg_keyword in zip(targets, agg_keyword_list)]
            view_sql_select = "SELECT T1.* "
            view_sql_join = ""
            for agg_sql, agg_keyword, dummy in zip(agg_sql_list, agg_keyword_list, dummy_list):
                view_sql_select += ", T{}.{} ".format(dummy, agg_keyword)
                view_sql_join += "INNER JOIN ({1}) T{0} ON T1.{2}=T{0}.{2} ".format(dummy, agg_sql,\
                    quotename(group_by))
            view_sql = view_sql_select + "FROM ({}) T1 ".format(view_sql) + view_sql_join

        if not isinstance(trans_param, (tuple, list)):
            if trans_param is not None:
                trans_param = [trans_param]

        trans_keyword_list = []
        if trans_func is not None:
            partition_by = ''
            rows_between = ''
            rolling_name = ''
            if group_by:
                partition_by = 'PARTITION BY {} '.format(quotename(group_by))
            if rolling_window:
                if not isinstance(rolling_window, str):
                    if rolling_window >= 0:
                        rows_between = 'ROWS BETWEEN CURRENT ROW AND {} FOLLOWING'.format(rolling_window)
                        rolling_name = 'F{}'.format(rolling_window)
                    else:
                        rows_between = 'ROWS BETWEEN {} PRECEDING AND CURRENT ROW'.format(rolling_window * -1)
                        rolling_name = 'P{}'.format(rolling_window * -1)
                else:
                    if rolling_window.upper() == 'INF':
                        rows_between = 'ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING'
                        rolling_name = 'F'
                    elif rolling_window.upper() =='-INF':
                        rows_between = 'ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW'
                        rolling_name = 'P'
                    else:
                        pass
            if trans_param is not None:
                for target, param in zip(targets, trans_param):
                    if isinstance(param, (tuple, list, range)):
                        for t_param in param:
                            temp_param = t_param
                            if isinstance(t_param, (tuple, list)):
                                temp_param = ', '.join(t_param)
                            if trans_func.upper() == 'GEOHASH_HIERARCHY':  # Needs special handling
                                temp_trans = geohash_hierarchy(target, t_param)
                            else:
                                temp_trans = '{}({},{})'.format(trans_func, quotename(target), temp_param)
                                if order_by is not None:
                                    temp_trans = temp_trans + ' OVER({}ORDER BY {} {}) AS "{}{}" '.format(partition_by, quotename(order_by),
                                                                                                          rows_between, temp_trans.replace('"', ''),
                                                                                                          rolling_name)
                                else:
                                    temp_trans = temp_trans + ' AS "{}"'.format(temp_trans.replace('"', ''))
                            trans_keyword_list.append(temp_trans)
                    else:
                        if trans_func.upper() == 'GEOHASH_HIERARCHY':  # Needs special handling
                            temp_trans = geohash_hierarchy(target, param)
                        else:
                            temp_trans = '{}({},{})'.format(trans_func, quotename(target), param)
                            if order_by is not None:
                                temp_trans = temp_trans + ' OVER({}ORDER BY {} {}) AS "{}{}" '.format(partition_by, quotename(order_by),
                                                                                                      rows_between, temp_trans.replace('"', ''),
                                                                                                      rolling_name)
                            else:
                                temp_trans = temp_trans + ' AS "{}"'.format(temp_trans.replace('"', ''))
                        trans_keyword_list.append(temp_trans)
            else:
                for target, second_target in zip(targets, second_targets):
                    if trans_func.upper() == 'GEOHASH_HIERARCHY':  # Needs special handling
                        temp_trans = geohash_hierarchy(target, 20)
                    else:
                        temp_trans = '{}({}{})'.format(trans_func, quotename(target), second_target)
                        if order_by is not None:
                            temp_trans = temp_trans + ' OVER({}ORDER BY {} {}) AS "{}{}" '.format(partition_by, quotename(order_by),
                                                                                                  rows_between, temp_trans.replace('"', ''),
                                                                                                  rolling_name)
                        else:
                            temp_trans = temp_trans + ' AS "{}"'.format(temp_trans.replace('"', ''))
                    trans_keyword_list.append(temp_trans)

            view_sql = "SELECT *, " + ", ".join(trans_keyword_list) + " FROM ({})".format(view_sql)
        return self._df(view_sql)

    def mutate(self, **kwargs):
        """
        Mutate columns in the dataframe.

        Examples
        --------
        >>> data.mutate(new_col=data.ID+2, data=data.data*3)
        """
        columns = self.columns
        col_dict = dict(kwargs)
        new_cols = []
        for kkey, vval in col_dict.items():
            if isinstance(vval, Column):
                vval.col_name(kkey)
                new_cols.append(vval)
                if kkey in columns:
                    columns.remove(kkey)
        return self.select(columns+new_cols)

def read_pickle(connection_context, path, table_name,
                compression='infer', schema=None,
                force=False, replace=True,
                object_type_as_bin=False,
                table_structure=None,
                **kwargs):
    """
    Loads a pickled DataFrame object from file.

    Parameters
    ----------
    connection_context : ConnectionContext
        A connection to a SAP HANA database instance.
    path : str
        File path where the pickled object will be loaded.
    table_name : str
        The table name in the SAP HANA database.
    compression : {'infer', 'gzip', 'bz2', 'zip', 'xz', None}, optional
        For on-the-fly decompression of on-disk data.

        If 'infer', then use gzip, bz2, xz or zip if path ends in '.gz', '.bz2', '.xz', or '.zip' respectively,
        and no decompression otherwise.

        Set to None for no decompression.

        Defaults to 'infer'.
    schema : str, optional, keyword-only
        The schema name. If this value is not provided or set to None, then the value defaults to the
        ConnectionContext's current schema.

        Defaults to the current schema.
    force : bool, optional
        If force is True, then the SAP HANA table with table_name is dropped.

        Defaults to False.
    replace : bool, optional
        If replace is True, then the SAP HANA table performs the missing value handling.

        Defaults to True.
    object_type_as_bin : bool, optional
        If True, the object type will be considered BLOB in SAP HANA.

        Defaults to False.
    table_structure : dict, optional
        Manually define column types based on SAP HANA DB table structure.

        Defaults to None.
    """
    pandas_df = pd.read_pickle(path, compression)
    return create_dataframe_from_pandas(connection_context, pandas_df, table_name, schema, force, replace, object_type_as_bin, table_structure, **kwargs)

#Internal
def export_into(connection_context, tables, condition="", directory=None, export_format="BINARY", replace=False, scramble=False, threads=1):
    """
    Exports SAP HANA tables into the disk.

    Parameters
    ----------
    connection_context : ConnectionContext
        A connection to the SAP HANA database instance.
    tables : str or list of str
        Table(s) ready for export.
    condition : str, optional
        Exports a subset of table data. The WHERE clause follows, and is associated with, a single table in the EXPORT statement.

        Defaults to "".
    directory : str, optional
        Export location.

        Defaults to '/tmp/HANAML_<uuid>.tgz'.
    export_format : {"BINARY, "CSV"}, optional

        Defaults to "BINARY".
    replace : bool, optional
        Defines the behavior if the export data already exists in the specified directory. If REPLACE is not specified, then an error is returned if previously exported data exists in the specified export directory.

        Defaults to False.
    scramble : bool, optional
        Obfuscates CSV format exported data.

        Defaults to False.
    threads : int
        The number of threads used for export.
    """
    if isinstance(tables, str):
        tables = [tables]
    if directory is None:
        directory = "/tmp/HANAML_{}.tgz".format(uuid.uuid1())
    export_options = []
    if replace is True:
        export_options.append("REPLACE")
    if scramble is True:
        export_options.append("SCRAMBLE")
    if threads > 1:
        export_options.append("THREADS {}".format(threads))
    options = ""
    if len(export_options) > 1:
        options = "WITH {}".format(" ".join(export_options))
    query = "EXPORT {0} {1} AS {2} INTO '{3}' {4}".format(", ".join(tables), condition, export_format, directory, options)
    with connection_context.connection.cursor() as cur:
        execute_logged(cur,
                       query,
                       connection_context.sql_tracer,
                       connection_context)
    if not connection_context.pyodbc_connection:
        if not connection_context.connection.getautocommit():
            connection_context.connection.commit()
    return directory

#Internal
def import_from(connection_context, directory, replace=False, threads=1):
    """
    Imports data into the SAP HANA system.

    Parameters
    ----------
    connection_context : ConnectionContext
        A connection to the SAP HANA database instance.

    directory : str
        Specifies the location where the import source is found. Specify <archive_file_name> if the import data is in an archive file. The archive file must have the file extension .tar.gz or .tgz.

    replace : bool, optional
        Defines the behavior if the import data already exists in the database. When specified, if a table defined in the import data currently exists in the database, then it is dropped and recreated before the data is imported. If the REPLACE option is not specified, then an error is thrown if an existing database table is defined in the import data.

        Defaults to False.
    threads : int, optional
        The number of threads used for import.

        Defaults to 1.
    """
    import_options = []
    if replace is True:
        import_options.append("REPLACE")
    if threads > 1:
        import_options.append("THREADS {}".format(threads))
    options = ""
    if len(import_options) > 1:
        options = "WITH {}".format(" ".join(import_options))
    query = "IMPORT ALL FROM {0} {1}".format(directory, options)
    with connection_context.connection.cursor() as cur:
        execute_logged(cur,
                       query,
                       connection_context.sql_tracer,
                       connection_context)
    if not connection_context.pyodbc_connection:
        if not connection_context.connection.getautocommit():
            connection_context.connection.commit()

def data_manipulation(connection_context, table_name, unload=True, schema=None, persistent_memory=None):
    """
    Loads/unloads the data from memory within the SAP HANA database environment. Note that this method has no impact on the client.

    Parameters
    ----------
    connection_context : ConnectionContext
        A connection to the SAP HANA database instance.

    table_name : str
        The table name in the SAP HANA database

    unload : bool, optional
        - True : Unload the data from memory.
        - False : Load the data from memory.

        Defaults to True.
    schema : str, optional
        The schema name.

        Defaults to None.
    persistent_memory : {'retain', 'delete'}, optional
        Only works when persistent memory is enabled.

        Defaults to None.

    Returns
    -------
    None

    Examples
    --------
    >>> data_manipulation(conn, 'TEST_TBL')
    """
    load_sql = "LOAD {} all".format(quotename(table_name))
    if schema is not None:
        load_sql = "LOAD {}.{} all".format(quotename(schema), quotename(table_name))
    if unload is True:
        load_sql = "UNLOAD {}".format(quotename(table_name))
        if schema is not None:
            load_sql = "UNLOAD {}.{}".format(quotename(schema), quotename(table_name))
        if persistent_memory is not None:
            load_sql = load_sql + " {} PERSISTENT MEMORY".format(persistent_memory.upper())
    with connection_context.connection.cursor() as cur:
        execute_logged(cur,
                       load_sql,
                       connection_context.sql_tracer,
                       connection_context)
    if not connection_context.pyodbc_connection:
        if not connection_context.connection.getautocommit():
            connection_context.connection.commit()

def _get_dtype(dtype, allow_bigint, object_type_as_bin, primary_key=False, is_spark=False):
    if is_spark:
        if ('long' in dtype.lower()) and allow_bigint:
            return 'BIGINT'
        elif 'int' in dtype.lower() or 'bool' in dtype.lower():
            return 'INT'
        elif 'float' in dtype.lower() or 'double' in dtype.lower():
            return 'DOUBLE'
        elif 'timestamp' in dtype.lower():
            return 'TIMESTAMP'
        elif 'byte' in dtype.lower() and object_type_as_bin:
            return 'BLOB'
        elif primary_key and 'str' in dtype.lower():
            return 'NVARCHAR(255)'
        else:
            return 'VARCHAR(5000)'
    if ('int64' in dtype.lower()) and allow_bigint:
        return 'BIGINT'
    elif 'int' in dtype.lower() or 'bool' in dtype.lower():
        return 'INT'
    elif 'float' in dtype.lower() or 'double' in dtype.lower():
        return 'DOUBLE'
    elif 'datetime' in dtype.lower():
        return 'TIMESTAMP'
    elif 'object' in dtype.lower() and object_type_as_bin:
        return 'BLOB'
    elif 'string' in dtype.lower() and object_type_as_bin:
        return 'BLOB'
    elif primary_key and 'str' in dtype.lower():
        return 'NVARCHAR(255)'
    else:
        return 'VARCHAR(5000)'

def _prepare_lat_lon_for_import(pandas_df: pd.DataFrame, lon_lat: tuple):
    """ Helper to prepare the latitude/longitude columns for import"""
    column_name = "{lon}_{lat}_GEO".format(lon=lon_lat[0], lat=lon_lat[1])

    # Add the point as WKT to the dataframe
    pandas_df[column_name] = "POINT(" + pandas_df[lon_lat[0]].astype(str).str.cat(
        pandas_df[lon_lat[1]].astype(str) + ")", sep=" "
    )

    # Replace cells with invalid geometry
    pandas_df.loc[
        pandas_df[column_name] == "POINT(nan nan)", [column_name]
    ] = "POINT EMPTY"

    return column_name, pandas_df


def _prepare_geo_cols_for_import(pandas_df: pd.DataFrame, col: str):
    """ Helper to convert the different geometry formats to WKT for downstream processing"""
    if len(pandas_df) > 0:
        sample_value = pandas_df.iloc[0][col]

        if isinstance(sample_value, memoryview):
            pass  # Nothing to do will be handled by HANA

        elif str(type(sample_value))[:25] == "<class 'shapely.geometry.":
            pandas_df[col] = pandas_df[col].apply(
                lambda val: np.nan if val is None or isinstance(val, float) else val.wkt
            )

    column_name = "{}_GEO".format(col)
    pandas_df = pandas_df.rename(columns={col: column_name})

    return column_name, pandas_df


def create_dataframe_from_pandas(
        connection_context,
        pandas_df,
        table_name,
        schema=None,
        force=False,
        replace=False,
        object_type_as_bin=False,
        table_structure=None,
        drop_exist_tab=True,
        allow_bigint=False,
        geo_cols: list = None,  # Spatial variable
        srid: int = 4326,  # Spatial variable
        primary_key: str = None,  # Graph variable
        not_nulls: list = None,  # Graph variable
        chunk_size=50000,
        disable_progressbar=False,
        upsert=False,
        append=False,
        table_type=""
):
    """
    Uploads data from a Pandas DataFrame to a SAP HANA database and returns an SAP HANA DataFrame.

    Parameters
    ----------
    connection_context : ConnectionContext
        A connection to the SAP HANA database instance.
    pandas_df : pandas.DataFrame
        A Pandas DataFrame for uploading to the SAP HANA database. This can
        also be a GeoPandas dataframe, which will automatically be converted
        to a Pandas DataFrame.
    table_name : str
        The table name in the SAP HANA database.
    schema : str, optional, keyword-only
        The schema name. If this value is not provided or set to None, then the value defaults to the
        ConnectionContext's current schema.

        Defaults to the current schema.
    force : bool, optional
        If force is True, then the SAP HANA table with table_name is truncated or dropped.

        Defaults to False.
    replace : bool, optional
        If replace is True, NULL will be replaced by 0.

        Defaults to False.
    object_type_as_bin : bool, optional
        If True, the object type will be considered CLOB in SAP HANA.

        Defaults to False.
    table_structure : dict
        Manually define column types based on SAP HANA DB table structure.
    drop_exist_tab : bool, optional
        If force is True, drop the existing table when drop_exist_tab is True and truncate the existing table when it is False.

        Defaults to True.
    allow_bigint : bool, optional
        allow_bigint decides whether int64 is mapped into INT or BIGINT in HANA.

        Defaults to False.
    geo_cols : list, optional but required for spatial functions
        Specifies the columns of the dataframe, which are treated as geometries.
        List elements can be either strings or tuples.

        **Strings** represent columns which contain geometries in (E)WKT format.
        If the provided DataFrame is a GeoPandas DataFrame, you do not need
        to add the geometry column to the geo_cols. It will be detected and
        added automatically.

        The column name in the HANA Table will be `<column_name>_GEO`


        **Tuples** must consist of two or strings: `(<longitude column>, <latitude column>)`

        `longitude column`: Dataframe column, that contains the longitude values

        `latitude column`: Dataframe column, that contains the latitude values

        They will be combined to a `POINT(<longiturd> <latitude>`) geometry.

        The column name in the HANA Table will be `<longitude>_<latitude>_GEO`

        Defaults to None.
    srid : int, optional but required for spatial functions
        Spatial reference system id. If the SRS is not created yet, the
        system tries to create it automatically.

        Defaults to 4326.
    primary_key : str, optional but required for Graph functions
        Name of the column in a node table which contains the unique identification of the node and corresponds with the
        edge table.

        Defaults to None.
    not_nulls : list, optional but required for Graph functions
        Contains all column names which should get a not null constraint. This is primarily for creating node and edge
        tables for a graph workspace

        Defaults to None.
    chunk_size : int, optional
        Specify the chunk size for upload.

        Defaults to 50000.
    disable_progressbar : bool, optional
        Disable the progress bar.

        Defaults to False.
    upsert : bool, optional
        Enable upsert with primary key if True.

        Defaults to False.
    append : bool, optional
        Enable append if True.

        Defaults to False.
    table_type : {"", "ROW", "COLUMN"}, optional
        Specify the table type. Valid options could be
        "ROW", "COLUMN" and "" (i.e. empty string).

        Defaults to "".

    Returns
    -------
    DataFrame
        A SAP HANA DataFrame that contains the data in the pandas_df.

    Examples
    --------
    >>> create_dataframe_from_pandas(connection_context,p_df,'test',force=False,replace=True,drop_exist_tab=False)
    <hana_ml.dataframe.DataFrame at 0x7efbcb26fbe0>

    >>> create_dataframe_from_pandas(
            connection_context,
            p_df,
            "geo_table",
            force=False,
            replace=True,
            drop_exist_tab=False,
            geo_cols=["geometry", ("long", "lat")],
        )
    <hana_ml.dataframe.DataFrame at 0x5eabcb27fbe0>
    """
    def _format_pandas_real_vector(df, cols):
        import struct
        def _apply_str_list(vval):
            return str(list(vval))
        def createFvecs(values):
            return struct.pack("<I%sf" % len(values), len(values), *values)
        if df.shape[0] == 0:
            return df
        for col in cols:
            if 'memoryview' in str(type(list(df[col])[0])):
                df[col] = df[col].apply(_apply_str_list)
            # if df[col] is list apply createFvecs
            elif isinstance(df[col].iloc[0], list):
                df[col] = df[col].apply(createFvecs)
        return df
    # Initialized list makes life easier
    if not geo_cols:
        geo_cols = []

    real_vector_cols = []
    # Turn geopandas df in a pandas df
    # checking by isinstance requires importing the geopandas so check
    # for a string to change into standard pandas
    if str(type(pandas_df)) == "<class 'geopandas.geodataframe.GeoDataFrame'>":
        # Automatically add the geopandas geometry to the list of geometries,
        # if it's not already in
        if pandas_df.geometry.name not in geo_cols:
            geo_cols.append(pandas_df.geometry.name)

        pandas_df = pd.DataFrame(pandas_df)

    # Check if the parameters are correct and convert geo columns

    prepared_geo_cols = []  # List of ge columns in the df, after they are prepared for import. Base for further processing.
    if not isinstance(geo_cols, list):
        raise ValueError("geo_cols needs to be a list of columns")

    for col in geo_cols:
        if isinstance(col, tuple):
            if len(col) < 2:
                raise ValueError("Column names for both, latitude and longitude are required")

            if col[0] not in pandas_df.columns:
                raise ValueError("Column '{}' not found in dataframe".format(col[0]))

            if col[1] not in pandas_df.columns:
                raise ValueError("Column '{}' not found in dataframe".format(col[1]))

            geo_col_name, pandas_df = _prepare_lat_lon_for_import(pandas_df, col)
            prepared_geo_cols.append(geo_col_name)

        elif isinstance(col, str):
            if col not in pandas_df.columns:
                raise ValueError("Column '{}' not found in dataframe".format(col))

            geo_col_name, pandas_df = _prepare_geo_cols_for_import(pandas_df, col)
            prepared_geo_cols.append(geo_col_name)

        else:
            raise ValueError("Unsupported datatype of '{}'".format(col))

    # Check if the SRS is already created and try to create it if not
    if prepared_geo_cols:
        from .spatial import create_predefined_srs  # pylint: disable=import-outside-toplevel
        create_predefined_srs(connection_context=connection_context, srid=srid)

    if schema is None:
        table_reference = quotename(table_name)
    else:
        table_reference = '.'.join(map(quotename, (schema, table_name)))
    if upsert:
        drop_exist_tab = False
    if append:
        drop_exist_tab = False
    cursor = connection_context.connection.cursor()
    tab_exist = connection_context.has_table(table=table_name, schema=schema)

    if force is True:
        sql_script = 'DROP TABLE {};'.format(table_reference)
        if (tab_exist == True) and (drop_exist_tab == False):
            sql_script = 'TRUNCATE TABLE {};'.format(table_reference)
            logger.info("Table already exists. Begin to truncate table.")
        # execute drop table with try catch
        try:
            execute_logged(cursor,
                           sql_script,
                           connection_context.sql_tracer,
                           connection_context)
        except dbapi.Error:
            pass
        except Exception as err:
            logger.error(str(err))
            pass
    # create table script
    if (force == False) and (tab_exist == True):
        if append == False and upsert == False:
            logger.warning("Table already exists. To set append=True or upsert=True can append or upsert data into the existing table. To set `force=True` can empty the table.")
    replace_subset = []
    if table_structure:#if table_structure is provided and non-empty
        for col in pandas_df.columns:
            if col in table_structure:
                if 'real_vector' in table_structure[col].lower():
                    real_vector_cols.append(col)
        pandas_df = _format_pandas_real_vector(pandas_df, real_vector_cols)
    if (tab_exist == False) or ((tab_exist == True) and (drop_exist_tab == True)):
        sql_script = 'CREATE {} TABLE {} ('.format(table_type, table_reference)
        if '#' in table_name[:2]:
            sql_script = 'CREATE LOCAL TEMPORARY {} TABLE {} ('.format(table_type, table_reference)
        dtypes_list = list(map(str, pandas_df.dtypes.values))

        for col, dtype in zip(pandas_df.columns, dtypes_list):
            if table_structure is not None and col in table_structure:
                sql_script = sql_script + '"{}" {}, '.format(col, table_structure[col])
            else:
                # Adjust the column dtype with a primary key and not_null in the case it is required
                if primary_key and col == primary_key:
                    sql_key = ' primary key,'
                    sql_null = ''
                elif not_nulls and col in not_nulls:
                    sql_key = ''
                    sql_null = ' not null,'
                else:
                    # primary key takes precendence over not_null and therefore takes the , if neither is exists for this column
                    sql_key = ','
                    sql_null = ''

                # Limit the sql_key to a NVARCHAR 255
                if sql_key not in ['', ',']:
                    dtype_col = _get_dtype(dtype=dtype, allow_bigint=allow_bigint,
                                           object_type_as_bin=object_type_as_bin,
                                           primary_key=True)
                    sql_script = sql_script + '"{}" {}{}{} '.format(col, dtype_col, sql_key, sql_null)
                elif sql_null != '':
                    dtype_col = _get_dtype(dtype=dtype, allow_bigint=allow_bigint,
                                           object_type_as_bin=object_type_as_bin)
                    sql_script = sql_script + '"{}" {}{}{} '.format(col, dtype_col, sql_key, sql_null)
                elif col in prepared_geo_cols:
                    sql_script = sql_script + '"{}" ST_GEOMETRY({}), '.format(col, srid)
                else:
                    dtype_col = _get_dtype(dtype=dtype, allow_bigint=allow_bigint,
                                           object_type_as_bin=object_type_as_bin)
                    sql_script = sql_script + '"{}" {}, '.format(col, dtype_col)
                    if 'int' in dtype.lower() or 'float' in dtype.lower() or 'double' in dtype.lower():
                        replace_subset.append(col)

        sql_script = sql_script[:-2]
        sql_script = sql_script + ');'
        try:
            execute_logged(cursor,
                           sql_script,
                           connection_context.sql_tracer,
                           connection_context)
        except dbapi.Error as db_er:
            logger.error(str(db_er))
            cursor.close()
            raise
        except Exception as db_er:
            logger.error(str(db_er.args[1]))
            cursor.close()
            raise
    if pandas_df.isnull().values.any() and replace:
        logger.info("Replace nan with 0 in numeric columns.")

    shape0 = len(pandas_df)

    if shape0 > 0:
        # Prepare the columns for insert statement
        parms = ""
        for col in pandas_df.columns:
            if col in prepared_geo_cols:
                parms = ','.join((parms, 'ST_GEOMFROMWKT(?, {})'.format(srid)))
            elif col in real_vector_cols and isinstance(pandas_df[col].iloc[0], str):
                parms = ','.join((parms, 'TO_REAL_VECTOR(?)'))
            else:
                parms = ','.join((parms, '?'))

        parms = parms[1:]
        column_list_clause = ', '.join(list(map(quotename, pandas_df.columns)))
        sql = 'INSERT INTO {} ({}) VALUES ({})'.format(table_reference, column_list_clause, parms)
        if upsert:
            sql = 'UPSERT {} ({}) VALUES ({}) WITH PRIMARY KEY'.format(table_reference, column_list_clause, parms)
        num_regular_chunks = math.floor(float(shape0)/float(chunk_size))
        cum = 0
        has_dtype_time = "time" in str(pandas_df.dtypes).lower()
        has_dtype_numpy = "numpy" in " ".join([str(dtype.type) for dtype in pandas_df.dtypes]).lower()

        if has_dtype_time and has_dtype_numpy:
            for chunk in tqdm(range(1, num_regular_chunks + 2), disable=disable_progressbar):
                begin = cum
                cum = cum + chunk_size

                if chunk <= num_regular_chunks:
                    rows = tuple(map(tuple, [[None if element is None or pd.isnull(element) else (str(element) if 'time' in str(type(element)) else (element.item() if (type(element).__module__ == np.__name__) else element)) for element in lines] for lines in pandas_df.iloc[begin:cum].values])) #pylint: disable=line-too-long
                    cursor.executemany(sql, rows)
                else:
                    rows = tuple(map(tuple, [[None if element is None or pd.isnull(element) else (str(element) if 'time' in str(type(element)) else (element.item() if (type(element).__module__ == np.__name__) else element)) for element in lines] for lines in pandas_df.iloc[begin:].values])) #pylint: disable=line-too-long
                    if rows:
                        cursor.executemany(sql, rows)
        elif has_dtype_numpy:
            for chunk in tqdm(range(1, num_regular_chunks + 2), disable=disable_progressbar):
                begin = cum
                cum = cum + chunk_size

                if chunk <= num_regular_chunks:
                    rows = tuple(map(tuple, [[None if element is None or pd.isnull(element) else (element.item() if (type(element).__module__ == np.__name__) else element) for element in lines] for lines in pandas_df.iloc[begin:cum].values])) #pylint: disable=line-too-long
                    cursor.executemany(sql, rows)
                else:
                    rows = tuple(map(tuple, [[None if element is None or pd.isnull(element) else (element.item() if (type(element).__module__ == np.__name__) else element) for element in lines] for lines in pandas_df.iloc[begin:].values])) #pylint: disable=line-too-long
                    if rows:
                        cursor.executemany(sql, rows)
        else:
            for chunk in tqdm(range(1, num_regular_chunks + 2), disable=disable_progressbar):
                begin = cum
                cum = cum + chunk_size

                if chunk <= num_regular_chunks:
                    rows = [tuple(x) for x in pandas_df.iloc[begin:cum].where(pd.notnull(pandas_df), None).values]
                    cursor.executemany(sql, rows)
                else:
                    rows = [tuple(x) for x in pandas_df.iloc[begin:].where(pd.notnull(pandas_df), None).values]
                    if rows:
                        cursor.executemany(sql, rows)

    cursor.close()
    if not connection_context.pyodbc_connection:
        if not connection_context.connection.getautocommit():
            connection_context.connection.commit()

    res_df = DataFrame(connection_context, 'SELECT * FROM {}'.format(table_reference))
    res_df.set_source_table(table_name, schema)

    if replace:
        return res_df.fillna(0, subset=replace_subset)

    return res_df

def create_dataframe_from_spark(
        connection_context,
        spark_df,
        table_name,
        schema=None,
        force=False,
        object_type_as_bin=False,
        table_structure=None,
        drop_exist_tab=True,
        allow_bigint=False,
        primary_key: str = None,
        not_nulls = None,
        chunk_size=50000,
        disable_progressbar=False,
        upsert=False,
        append=False
):
    """
    Uploads data from a Spark DataFrame to a SAP HANA database and returns an SAP HANA DataFrame.

    Parameters
    ----------
    connection_context : ConnectionContext
        A connection to the SAP HANA database instance.
    spark_df : pandas.DataFrame
        A Spark DataFrame for uploading to the SAP HANA database.
    table_name : str
        The table name in the SAP HANA database.
    schema : str, optional, keyword-only
        The schema name. If this value is not provided or set to None, then the value defaults to the
        ConnectionContext's current schema.

        Defaults to the current schema.
    force : bool, optional
        If force is True, then the SAP HANA table with table_name is truncated or dropped.

        Defaults to False.
    object_type_as_bin : bool, optional
        If True, the object type will be considered CLOB in SAP HANA.

        Defaults to False.
    table_structure : dict
        Manually define column types based on SAP HANA DB table structure.
    drop_exist_tab : bool, optional
        If force is True, drop the existing table when drop_exist_tab is True and truncate the existing table when it is False.

        Defaults to True.
    allow_bigint : bool, optional
        allow_bigint decides whether int64 is mapped into INT or BIGINT in HANA.

        Defaults to False.
    primary_key : str, optional
        Name of the column in a node table which contains the unique identification of the node and corresponds with the
        edge table.

        Defaults to None.
    not_nulls : list, optional
        Contains all column names which should get a not null constraint.

        Defaults to None.
    chunk_size : int, optional
        Specify the chunk size for upload.

        Defaults to 50000.
    disable_progressbar : bool, optional
        Disable the progress bar.

        Defaults to False.
    upsert : bool, optional
        Enable upsert with primary key if True.

        Defaults to False.
    append : bool, optional
        Enable append if True.

        Defaults to False.

    Returns
    -------
    DataFrame
        A SAP HANA DataFrame that contains the data in the Spark DataFrame.

    Examples
    --------
    >>> create_dataframe_from_spark(connection_context,spark_df,'test',force=False,replace=True,drop_exist_tab=False)

    """
    if schema is None:
        table_reference = quotename(table_name)
    else:
        table_reference = '.'.join(map(quotename, (schema, table_name)))
    if upsert:
        drop_exist_tab = False
    if append:
        drop_exist_tab = False
    cursor = connection_context.connection.cursor()
    tab_exist = connection_context.has_table(table=table_name, schema=schema)

    if force is True:
        sql_script = 'DROP TABLE {};'.format(table_reference)
        if (tab_exist == True) and (drop_exist_tab == False):
            sql_script = 'TRUNCATE TABLE {};'.format(table_reference)
            logger.info("Table already exists. Begin to truncate table.")
        # execute drop table with try catch
        try:
            execute_logged(cursor,
                           sql_script,
                           connection_context.sql_tracer,
                           connection_context)
        except dbapi.Error:
            pass
        except Exception as err:
            logger.error(str(err))
            pass
    # create table script
    if (force == False) and (tab_exist == True):
        if append == False and upsert == False:
            logger.warning("Table already exists. To set append=True or upsert=True can append or upsert data into the existing table. To set `force=True` can empty the table.")

    if (tab_exist == False) or ((tab_exist == True) and (drop_exist_tab == True)):
        sql_script = 'CREATE TABLE {} ('.format(table_reference)
        if '#' in table_name[:2]:
            sql_script = 'CREATE LOCAL TEMPORARY TABLE {} ('.format(table_reference)

        for col, dtype in spark_df.dtypes:
            if table_structure is not None and col in table_structure:
                sql_script = sql_script + '"{}" {}, '.format(col, table_structure[col])
            else:
                # Adjust the column dtype with a primary key and not_null in the case it is required
                if primary_key and col == primary_key:
                    sql_key = ' primary key,'
                    sql_null = ''
                elif not_nulls and col in not_nulls:
                    sql_key = ''
                    sql_null = ' not null,'
                else:
                    # primary key takes precendence over not_null and therefore takes the , if neither is exists for this column
                    sql_key = ','
                    sql_null = ''

                # Limit the sql_key to a NVARCHAR 255
                if sql_key not in ['', ',']:
                    dtype_col = _get_dtype(dtype=dtype, allow_bigint=allow_bigint,
                                           object_type_as_bin=object_type_as_bin, is_spark=True)
                    sql_script = sql_script + '"{}" {}{}{} '.format(col, dtype_col, sql_key, sql_null)
                elif sql_null != '':
                    dtype_col = _get_dtype(dtype=dtype, allow_bigint=allow_bigint,
                                           object_type_as_bin=object_type_as_bin, is_spark=True)
                    sql_script = sql_script + '"{}" {}{}{} '.format(col, dtype_col, sql_key, sql_null)
                else:
                    dtype_col = _get_dtype(dtype=dtype, allow_bigint=allow_bigint,
                                           object_type_as_bin=object_type_as_bin, is_spark=True)
                    sql_script = sql_script + '"{}" {}{}{} '.format(col, dtype_col, sql_key, sql_null)

        sql_script = sql_script[:-2]
        sql_script = sql_script + ');'
        try:
            execute_logged(cursor,
                           sql_script,
                           connection_context.sql_tracer,
                           connection_context)
        except dbapi.Error as db_er:
            logger.error(str(db_er))
            cursor.close()
            raise
        except Exception as db_er:
            logger.error(str(db_er.args[1]))
            cursor.close()
            raise
    shape0 = spark_df.count()

    if shape0 > 0:
        # Prepare the columns for insert statement
        parms = ""
        for col in spark_df.columns:
            parms = ','.join((parms, '?'))

        parms = parms[1:]

        sql = 'INSERT INTO {} VALUES ({})'.format(table_reference, parms)
        if upsert:
            sql = 'UPSERT {} VALUES ({}) WITH PRIMARY KEY'.format(table_reference, parms)

        spark_rdd = spark_df.rdd.zipWithIndex()
        if chunk_size > shape0:
            chunk_size = shape0
        for ite in tqdm(range(0, shape0, chunk_size), disable=disable_progressbar):
            rows = spark_rdd.filter(lambda element: ite <= element[1] < ite + chunk_size).map(lambda element: element[0]).collect()
            cursor.executemany(sql, rows)

    cursor.close()
    if not connection_context.pyodbc_connection:
        if not connection_context.connection.getautocommit():
            connection_context.connection.commit()

    res_df = DataFrame(connection_context, 'SELECT * FROM {}'.format(table_reference))
    res_df.set_source_table(table_name, schema)

    return res_df


def melt(frame, id_vars=None, value_vars=None, var_name=None, value_name=None):
    """
    Unpivots a DataFrame from wide format to long format, optionally leaving identifier variables set.

    Parameters
    ----------
    frame : DataFrame
        A SAP HANA DataFrame.

    id_vars : str, tuple or list, optional
        Column(s) to use as identifier variables.

        Defaults to None.

    value_vars : tuple or list, optional
        Column(s) to unpivot. If not specified, uses all columns that are not set as id_vars.

    var_name : scalar, optional
        Name to use for the 'variable' column. If None it uses frame.columns.name or 'variable'.

    value_name : scalar, default 'value', optional
        Name to use for the 'value' column.

    Returns
    -------
    DataFrame
        Unpivoted DataFrame.

    Examples
    --------
    >>> data.collect()
       A B C
    0  a 1 2
    1  b 3 4
    2  c 5 6

    >>> melt(data, id_vars=['A'], value_vars=['B', 'C']).collect()
       A variable value
    0  a        B     1
    1  a        C     2
    2  b        B     5
    3  b        C     6
    4  c        B     3
    5  c        C     4
    """
    if value_vars is None:
        if id_vars is None:
            value_vars = frame.columns
        if isinstance(id_vars, (list, tuple)):
            value_vars = [element for element in frame.columns if element not in id_vars]
        if isinstance(id_vars, str):
            value_vars = [element for element in frame.columns if element != id_vars]
    if isinstance(id_vars, (list, tuple)):
        id_vars = ", ".join(list(map(quotename,id_vars)))
    else:
        id_vars = quotename(id_vars)
    if not isinstance(value_vars, (list, tuple)):
        value_vars = [value_vars]
    if var_name is None:
        var_name = "variable"
    if value_name is None:
        value_name = "value"
    var_list = []
    for var in value_vars:
        if id_vars is not None:
            var_list.append("SELECT {0}, '{1}' AS {2}, \"{1}\" AS {3} FROM ({4})"
                            .format(id_vars,
                                    var,
                                    quotename(var_name),
                                    quotename(value_name),
                                    frame.select_statement))
        else:
            var_list.append("SELECT '{0}' AS {1}, \"{0}\" AS {2} FROM ({3})"
                            .format(var,
                                    quotename(var_name),
                                    quotename(value_name),
                                    frame.select_statement))
    exec_sql = ' UNION '.join(var_list)
    if len(value_vars) < 1:
        exec_sql = frame.select_statement
    return DataFrame(frame.connection_context, exec_sql)


def create_dataframe_from_shapefile(
        connection_context: ConnectionContext,
        shp_file: str,
        srid: int,
        table_name: str,
        schema: str = None,
) -> DataFrame:
    """
    Given a shapefile change the file into a DataFrame so that it is
    backed in SAP HANA. Expects that the shapefile name is a zip and/or
    will have both shp and dbf parts to create the single table. Once the
    table is created temporarily and locally, import the table as a
    shapefile into the target table through direct insertion. Then return
    the SAP HANA Dataframe that can be visualized.

    Parameters
    ----------
    connection_context : ConnectionContext
        A connection to the SAP HANA database instance.

    shp_file : str
        Path to a zipfile, shapefile or dbf-file. Filename suffix will
        be dropped and attempt to load both dbf and shp by that base name.
        Allowed suffixes are: .zip, .shp, .shx, and .dbf

    srid : int
        The spatial reference id that applies to the list of columns in geo_cols.
        If the SRS is not created yet, the system tries to create it automatically.

    table_name : str
        The table name in the SAP HANA database.

    schema : str, optional, keyword-only
        The schema name. If this value is not provided or set to None,
        then the value defaults to the ConnectionContext's current schema.

        Defaults to the current schema.

    Returns
    -------
    DataFrame
        A SAP HANA DataFrame with geometric columns that contains the data
        from the shp_file.

    Examples
    --------
    >>> cc = connection_context
    >>> shapefile_path = os.path.join(os.getcwd(), 'myshape.shp')
    >>> hana_df = create_dataframe_from_shapefile(
    ...   connection_context=cc,
    ...   shp_file=shapefile_path,
    ...   srid=4326,
    ...   table_name="myshape_tbl")
    """

    def log_and_raise_value_error(message):
        logger.error(message)
        raise ValueError(message)

    file_base, file_extension = os.path.splitext(shp_file)

    shp_file_path = ""
    dbf_file_path = ""

    if file_extension == ".zip":
        # Check if the file exists
        if not os.path.isfile(shp_file):
            log_and_raise_value_error(
                "Archive '{filename}' was not found.".format(filename=shp_file)
            )

        # Extract the shapefile and the dbf file from the zip archive
        with ZipFile(shp_file, "r") as zip_file:
            # Search the .shp file in the zip-file and extract it
            try:
                shp_file_path = [
                    zip_file.extract(shp_file_name, os.path.dirname(shp_file))
                    for shp_file_name in zip_file.namelist()
                    if shp_file_name[-4:] == ".shp"
                ][0]
            except IndexError:
                log_and_raise_value_error("No .shp file found in archive.")

            # Search the .dbf file in the zip-file and extract it
            try:
                dbf_file_path = [
                    zip_file.extract(shp_file_name, os.path.dirname(shp_file))
                    for shp_file_name in zip_file.namelist()
                    if shp_file_name[-4:] == ".dbf"
                ][0]
            except IndexError:
                log_and_raise_value_error("No .dbf file found in archive.")

    elif file_extension in [".dbf", ".shp", ".shx"]:
        # These are the two mandatory files we need, derived from the
        # filename without extension
        shp_file_path = file_base + ".shp"
        dbf_file_path = file_base + ".dbf"

        # Check if the files exist
        if not os.path.isfile(shp_file_path):
            log_and_raise_value_error(
                "'{filename}' was not found.".format(filename=shp_file_path)
            )

        if not os.path.isfile(dbf_file_path):
            log_and_raise_value_error(
                "'{filename}' was not found.".format(filename=dbf_file_path)
            )

    else:
        log_and_raise_value_error(
            "'{extension}' is an invalid file type.".format(extension=file_extension)
        )

    logger.info("Importing %s", shp_file_path)
    logger.info("Importing %s", dbf_file_path)

    # Check if the SRS is already created and try to create it
    from .spatial import create_predefined_srs  # pylint: disable=import-outside-toplevel
    create_predefined_srs(connection_context=connection_context, srid=srid)

    # Load the files to an temporary HANA DB table
    shp_file_handle = open(shp_file_path, "rb")
    dbf_file_handle = open(dbf_file_path, "rb")

    if not schema:
        schema = connection_context.get_current_schema()

    table_reference = '"{schema}"."{table}"'.format(schema=schema, table=table_name)

    # Drop the local and target tables in the case they exist
    try:
        connection_context.connection.cursor().execute("DROP TABLE #IMPORT_TABLE")
    except dbapi.ProgrammingError:
        # Silently pass the does not exist (invalid table name)
        pass

    try:
        connection_context.connection.cursor().execute(
            "DROP TABLE {}".format(table_reference)
        )
    except dbapi.ProgrammingError:
        # Silently pass the does not exist (invalid table name)
        pass

    try:
        with connection_context.connection.cursor() as cursor:
            cursor.execute(
                """
            CREATE LOCAL TEMPORARY TABLE #IMPORT_TABLE (
                FILENAME VARCHAR(64),
                PATH VARCHAR(255),
                CONTENT BLOB)
            """
            )

            cursor.execute(
                "INSERT INTO #IMPORT_TABLE VALUES (?,'',?)",
                (table_name + ".shp", shp_file_handle.read()),
            )

            cursor.execute(
                "INSERT INTO #IMPORT_TABLE VALUES (?,'',?)",
                (table_name + ".dbf", dbf_file_handle.read()),
            )

            cursor.execute(
                """
                IMPORT {table_reference} AS SHAPEFILE FROM #IMPORT_TABLE WITH SRID {srid}
                """.format(
                    table_reference=table_reference, srid=srid
                )
            )

    except dbapi.ProgrammingError as exception:
        logger.error(exception)
        raise exception

    finally:
        shp_file_handle.close()
        dbf_file_handle.close()

    return DataFrame(
        connection_context,
        "SELECT * FROM {}".format(table_reference),
    )

def import_csv_from(connection_context,
                    directory,
                    table,
                    schema=None,
                    threads=1,
                    record_delimiter=None,
                    field_delimiter=None,
                    escape_character=None,
                    column_list_first_row=None,
                    credential=None):
    r"""
    Imports a csv file into the SAP HANA system.
    More details is shown in the **<storage_path>** section of chapter
    `IMPORT FROM Statement (Data Import Export) <https://help.sap.com/docs/HANA_CLOUD_DATABASE/c1d3f60099654ecfb3fe36ac93c121bb/20f712e175191014907393741fadcb97.html>`_
    of SAP HANA Cloud, SAP HANA Database SQL Reference Guide.

    Parameters
    ----------
    connection_context : ConnectionContext
        A connection to the SAP HANA database instance.

    directory : str
        Specifies the cloud storage location for the import. The locations HANA cloud support are Azure, Amazon(AWS) Google Cloud, SAP HANA Cloud, Data Lake Files(HDLFS).

    table : str
        Specifies the name of target table.

        Defaults to None.

    schema : str, optional
        Specifies the schema name of target table.

        Defaults to None.

    threads : int, optional
        Specifies the number of threads that can be used for concurrent import.
        ``threads`` and ``batch`` provide high loading performance by enabling parallel loading and also by committing many records at once. In general, for column tables, a good setting to use is 10 parallel loading threads, with a commit frequency of 10,000 records or greater.

        Defaults to 1 and then the maximum allowed value is 256.

    record_delimiter : str, optional
        Specifies the record delimiters used in the CSV file.

        Defaults to None.

    field_delimiter : str, optional
        Specifies the field delimiters used in the CSV file.

        Defaults to None.

    escape_character : str, optional
        Specifies the escape character used in the import data.

        Defaults to None.

    column_list_first_row : bool, optional
        Indicates that the column list is stored in the first row.

        Defaults to None.

    credential : str, optional
        Specifies the name of the credential defined in the CREATE CREDENTIAL statement. Since the credentials are defined within the credential, they no longer appear as plain text as part of import statements. The WITH CREDENTIAL clause cannot be specified when ``directory`` contains credentials. The WITH CREDENTIAL clause is required for imports to SAP HANA Cloud, Data Lake Files, but is optional for all other cloud platforms.

        Defaults to None.

    Examples
    --------
    Assume we have a connection to a SAP HANA instance called conn and we want to import test.csv into a table called Test:

    >>> import_csv_from(connection_context=conn,
                        directory='hdlfs://XXXXXXXX.com/test.csv',
                        table="Test",
                        threads=10,
                        column_list_first_row=True,
                        credential='XXXCredential')

    """
    table_reference = ""
    if table is not None:
        if schema is None:
            table_reference = quotename(table)
        else:
            table_reference = '.'.join(map(quotename, (schema, table)))

    import_options = []
    if credential is not None:
        import_options.append("CREDENTIAL '{}'".format(credential))
    if threads > 1:
        import_options.append("THREADS {}".format(threads))
    if record_delimiter is not None:
        import_options.append("RECORD DELIMITED BY '{}'".format(record_delimiter))
    if field_delimiter is not None:
        import_options.append("FIELD DELIMITED BY '{}'".format(field_delimiter))
    if escape_character is not None:
        import_options.append("ESCAPE '{}'".format(escape_character))
    if column_list_first_row is True:
        import_options.append("COLUMN LIST IN FIRST ROW")

    query = "IMPORT FROM CSV FILE '{}' INTO {}".format(directory, table_reference)

    if len(import_options) > 0:
        query = "{} WITH {};".format(query, " ".join(import_options))

    with connection_context.connection.cursor() as cur:
        execute_logged(cur,
                       query,
                       connection_context.sql_tracer,
                       connection_context)
    if not connection_context.pyodbc_connection:
        if not connection_context.connection.getautocommit():
            connection_context.connection.commit()

class Column(DataFrame):
    """
    Extensions to DataFrame for column operations.
    """
    name = None
    base_select_statement = None
    _complex = False
    _new_col = None
    _col_name = None
    def col_name(self, name):
        """
        Set up column name.
        """
        self._col_name = name
    def _set_name(self, name):
        self.name = name
    def _set_base_select_statement(self, select_statement):
        self.base_select_statement = select_statement
    def __str__(self):
        return self.name
    def _smart_quote(self, name):
        if self._complex:
            return name
        return quotename(name)
    def __add__(self, obj):
        if isinstance(obj, Column):
            if self.base_select_statement != obj.base_select_statement:
                raise ValueError("Not the same dataframe!")
            new_name = "(" + self._smart_quote(self.name) + '+' + obj._smart_quote(obj.name) + ")"
            new_sql = "SELECT {} FROM ({})".format(new_name, self.base_select_statement)
            result = Column(self.connection_context, new_sql)
            result._set_name(new_name)
            result._set_base_select_statement(self.base_select_statement)
            result._complex = True
            result._new_col = new_name
            return result
        if isinstance(obj, str):
            new_name = "(" + self._smart_quote(self.name) + '||' + "'" + obj + "'" + ")"
            new_sql = "SELECT {} FROM ({})".format(new_name, self.base_select_statement)
            result = Column(self.connection_context, new_sql)
            result._set_name(new_name)
            result._set_base_select_statement(self.base_select_statement)
            result._complex = True
            result._new_col = new_name
            return result
        new_name = "(" + self._smart_quote(self.name) + '+' + str(obj) + ")"
        new_sql = "SELECT {} FROM ({})".format(new_name, self.base_select_statement)
        result = Column(self.connection_context, new_sql)
        result._set_name(new_name)
        result._set_base_select_statement(self.base_select_statement)
        result._complex = True
        result._new_col = new_name
        return result
    def __sub__(self, obj):
        if isinstance(obj, Column):
            if self.base_select_statement != obj.base_select_statement:
                raise ValueError("Not the same dataframe!")
            new_name = "(" + self._smart_quote(self.name) + '-' + obj._smart_quote(obj.name) + ")"
            new_sql = "SELECT {} FROM ({})".format(new_name, self.base_select_statement)
            result = Column(self.connection_context, new_sql)
            result._set_name(new_name)
            result._set_base_select_statement(self.base_select_statement)
            result._complex = True
            result._new_col = new_name
            return result
        if isinstance(obj, str):
            raise TypeError("Type doesn't match. String is not allowed for the subtraction.")
        new_name = "(" + self._smart_quote(self.name) + '-' + str(obj) + ")"
        new_sql = "SELECT {} FROM ({})".format(new_name, self.base_select_statement)
        result = Column(self.connection_context, new_sql)
        result._set_name(new_name)
        result._set_base_select_statement(self.base_select_statement)
        result._complex = True
        result._new_col = new_name
        return result
    def __mul__(self, obj):
        if isinstance(obj, Column):
            if self.base_select_statement != obj.base_select_statement:
                raise ValueError("Not the same dataframe!")
            new_name = "(" + self._smart_quote(self.name) + '*' + obj._smart_quote(obj.name) + ")"
            new_sql = "SELECT {} FROM ({})".format(new_name, self.base_select_statement)
            result = Column(self.connection_context, new_sql)
            result._set_name(new_name)
            result._set_base_select_statement(self.base_select_statement)
            result._complex = True
            result._new_col = new_name
            return result
        if isinstance(obj, str):
            raise TypeError("Type doesn't match. String is not allowed for the multiplication.")
        new_name = "(" + self._smart_quote(self.name) + '*' + str(obj) + ")"
        new_sql = "SELECT {} FROM ({})".format(new_name, self.base_select_statement)
        result = Column(self.connection_context, new_sql)
        result._set_name(new_name)
        result._set_base_select_statement(self.base_select_statement)
        result._complex = True
        result._new_col = new_name
        return result
    def __truediv__(self, obj):
        if isinstance(obj, Column):
            if self.base_select_statement != obj.base_select_statement:
                raise ValueError("Not the same dataframe!")
            new_name = "(" + self._smart_quote(self.name) + '/' + obj._smart_quote(obj.name) + ")"
            new_sql = "SELECT {} FROM ({})".format(new_name, self.base_select_statement)
            result = Column(self.connection_context, new_sql)
            result._set_name(new_name)
            result._set_base_select_statement(self.base_select_statement)
            result._complex = True
            result._new_col = new_name
            return result
        if isinstance(obj, str):
            raise TypeError("Type doesn't match. String is not allowed for the division.")
        new_name = "(" + self._smart_quote(self.name) + '/' + str(obj) + ")"
        new_sql = "SELECT {} FROM ({})".format(new_name, self.base_select_statement)
        result = Column(self.connection_context, new_sql)
        result._set_name(new_name)
        result._set_base_select_statement(self.base_select_statement)
        result._complex = True
        result._new_col = new_name
        return result
    def _map_op(self, op):
        if op == '==':
            return '='
        if op == '|':
            return ' OR '
        if op == '&':
            return ' AND '
        return op
    def _compare_op(self, obj, op):
        if isinstance(obj, Column):
            if self.base_select_statement != obj.base_select_statement:
                raise ValueError("Not the same dataframe!")
            new_name = "(" + self._smart_quote(self.name) + self._map_op(op) + obj._smart_quote(obj.name) + ")"
            new_col = "CASE WHEN {} {} {} THEN {} ELSE {} END".format(quotename(self.name), op, quotename(obj.name), 1, 0)
            new_sql = "SELECT {} AS {} FROM ({})".format(new_col, quotename(new_name), self.base_select_statement)
            result = Column(self.connection_context, new_sql)
            result._set_name(new_name)
            result._set_base_select_statement(self.base_select_statement)
            result._complex = True
            result._new_col = new_col
            return result
        if isinstance(obj, str):
            new_name = "(" + self._smart_quote(self.name) + self._map_op(op) + "'" + obj + "'" + ")"
            new_col = "CASE WHEN {} {} '{}' THEN {} ELSE {} END".format(quotename(self.name), op, obj, 1, 0)
            new_sql = "SELECT {} AS {} FROM ({})".format(new_col, quotename(new_name), self.base_select_statement)
            result = Column(self.connection_context, new_sql)
            result._set_name(new_name)
            result._set_base_select_statement(self.base_select_statement)
            result._complex = True
            result._new_col = new_col
            return result
        new_name = "(" + self._smart_quote(self.name) + self._map_op(op) + str(obj) + ")"
        new_col = "CASE WHEN {} {} {} THEN {} ELSE {} END".format(quotename(self.name), op, obj, 1, 0)
        new_sql = "SELECT {} AS {} FROM ({})".format(new_col, quotename(new_name), self.base_select_statement)
        result = Column(self.connection_context, new_sql)
        result._set_name(new_name)
        result._set_base_select_statement(self.base_select_statement)
        result._complex = True
        result._new_col = new_col
        return result
    def __lt__(self, obj):
        return self._compare_op(obj, "<")
    def __gt__(self, obj):
        return self._compare_op(obj, ">")
    def __le__(self, obj):
        return self._compare_op(obj, "<=")
    def __ge__(self, obj):
        return self._compare_op(obj, ">=")
    def __eq__(self, obj):
        return self._compare_op(obj, "==")
    def __ne__(self, obj):
        return self._compare_op(obj, "!=")
    def __and__(self, obj):
        return self._compare_op(obj, "&")
    def __or__(self, obj):
        return self._compare_op(obj, "|")
    def _single_quote(self, text):
        return "'" + text + "'"
    def isin(self, values):
        """
        Whether each element in the DataFrame is contained in values.
        """
        in_set = None
        if isinstance(values[0], str):
            in_set = ", ".join(list(map(self._single_quote, values)))
        else:
            in_set = ", ".join(list(map(str, values)))
        new_name = "(" + self._smart_quote(self.name) + " IN " + "({})".format(in_set) + ")"
        new_col = "CASE WHEN {} THEN 1 ELSE 0 END".format(new_name)
        new_sql = "SELECT {} AS {} FROM ({})".format(new_col, quotename(new_name), self.base_select_statement)
        result = Column(self.connection_context, new_sql)
        result._set_name(new_name)
        result._set_base_select_statement(self.base_select_statement)
        result._complex = True
        result._new_col = new_col
        return result
    def to_dataframe(self):
        """
        Convert to DataFrame.
        """
        return DataFrame(self.connection_context, self.select_statement)


def _call_pal_auto_with_hint(conn,
                             with_hint,
                             funcname,
                             *args):
    """
    Uses an anonymous block to call a PAL function.

    DataFrames that are not known to be safe in anonymous blocks will be
    temporarily materialized.

    Parameters
    ----------
    conn : ConnectionContext
        HANA connection to use.
    with_hint : str,
        If 'PARALLEL_BY_PARAMETER_PARTITIONS(p1)', it will use parallel with hint PARALLEL_BY_PARAMETER_PARTITIONS.
    funcname : str
        Name of the PAL function to execute. Should not include the "_SYS_AFL."
        part.
    arg1, arg2, ..., argN : DataFrame, ParameterTable, or str
        Arguments for the PAL function, in the same order the PAL function
        takes them.
        DataFrames represent input tables, a ParameterTable object represents
        the parameter table, and strings represent names for output
        tables. Output names with a leading "#" will produce local temporary
        tables.
    """
    adjusted_args = list(args)
    temporaries = []
    unknown_indices = []
    materialized = {}

    def materialize_at(i):
        "Materialize the i'th element of adjusted_args."
        tag = str(uuid.uuid4()).upper().replace('-', '_')
        name = '#{}_MATERIALIZED_INPUT_{}'.format(funcname, tag)
        materialize_dict = {name: adjusted_args[i].select_statement}
        adjusted_args[i] = adjusted_args[i].save(name)
        temporaries.append(name)
        return materialize_dict

    def try_exec(cur, sql, conn):
        """
        Try to execute the given sql. Returns True on success, False if
        execution fails due to an anonymous block trying to read a local
        temporary table. Other exceptions are propagated.
        """
        conn.last_execute_statement = sql

        if _exist_hint(sql) and 'pyodbc' not in str(type(cur)):
            try:
                cur.prepare(sql)
                cur.executeprepared()
            except dbapi.Error as err:
                if not err.errortext.startswith(
                        'feature not supported: Cannot use local temporary table'):
                    raise
                if "has invalid SQL type" in str(err):
                    logger.error(" [HINT] The error may be due to the unsupported type such as BIGINT. Try to use enable_convert_bigint() to allow the bigint conversion.")
                else:
                    logger.info("Facing the error %s. Try to fix...", str(err))
                return False
            return True
        else:
            try:
                cur.execute(sql)
            except dbapi.Error as err:
                if not err.errortext.startswith(
                        'feature not supported: Cannot use local temporary table'):
                    raise
                if "has invalid SQL type" in str(err):
                    logger.error(" [HINT] The error may be due to the unsupported type such as BIGINT. Try to use enable_convert_bigint() to allow the bigint conversion.")
                else:
                    logger.info("Facing the error %s. Try to fix...", str(err))
                return False
            except Exception as err:
                if 'feature not supported: Cannot use local temporary table' not in str(err):
                    raise
                if "has invalid SQL type" in str(err):
                    logger.error(" [HINT] The error may be due to the unsupported type such as BIGINT. Try to use enable_convert_bigint() to allow the bigint conversion.")
                else:
                    logger.info("Facing the error %s. Try to fix...", str(err))
                return False
            return True


    try:
        for i, argument in enumerate(args):
            if isinstance(argument, DataFrame):
                # pylint: disable=protected-access
                if argument._ttab_handling == 'unknown':
                    unknown_indices.append(i)
                elif argument._ttab_handling == 'unsafe':
                    materialized.update(materialize_at(i))

        # SQLTRACE added sql_tracer
        sql = _call_pal_tabvar(funcname,
                               conn.sql_tracer,
                               with_hint,
                               *adjusted_args)
        # SQLTRACE
        conn.sql_tracer.trace_object({
            'name':funcname,
            'schema': '_SYS_AFL',
            'type': 'pal'
        }, sub_cat='function')

        # Optimistic execution.
        with conn.connection.cursor() as cur:
            logged_without_execute(sql, conn.sql_tracer, conn)
            if try_exec(cur, sql, conn):
                # Optimistic execution succeeded, meaning all arguments with
                # unknown ttab safety are safe.
                for i in unknown_indices:
                    adjusted_args[i].declare_lttab_usage(False)
                return sql, materialized

        # If we reach this point, optimistic execution failed.

        if len(unknown_indices) == 1:
            # Only one argument of unknown ttab safety, so that one needs
            # materialization.
            adjusted_args[unknown_indices[0]].declare_lttab_usage(True)
            materialized.update(materialize_at(unknown_indices[0]))
        else:
            # Multiple arguments of unknown safety. Test which ones are safe.
            for i in unknown_indices:
                with conn.connection.cursor() as cur:
                    ttab_used = not try_exec(cur, safety_test(adjusted_args[i]), conn)
                adjusted_args[i].declare_lttab_usage(ttab_used)
                if ttab_used:
                    materialized.update(materialize_at(i))

        # SQLTRACE added sql_tracer
        sql = _call_pal_tabvar(funcname,
                              conn.sql_tracer,
                              with_hint,
                              *adjusted_args)
        with conn.connection.cursor() as cur:
            execute_logged(cur, sql, conn.sql_tracer, conn) # SQLTRACE added sql_tracer
        return sql, materialized
    finally:
        try_drop(conn, temporaries)


def _call_pal_tabvar(name, sql_tracer, with_hint, *args): # SQLTRACE added sql_tracer
    """
    Return SQL to call a PAL function, using table variables.

    Parameters
    ----------

    name : str
        Name of the PAL function to execute. Should not include the "_SYS_AFL."
        part.
    arg1, arg2, ..., argN : DataFrame, ParameterTable, or str
        Arguments for the PAL function, in the same order the PAL function
        takes them.
        DataFrames represent input tables, a ParameterTable object represents
        the parameter table, and strings represent names for output
        tables. Output names with a leading "#" will produce local temporary
        tables.

    Returns
    -------

    str
        SQL string that would call the PAL function with the given inputs
        and place output in tables with the given output names.
    """
    # pylint:disable=too-many-locals,too-many-branches
    if not isinstance(name, _TEXT_TYPES):
        raise TypeError("name argument not a string - it may have been omitted")

    in_palarg_gen = ('in_'+str(i) for i in itertools.count())
    out_palarg_gen = ('out_'+str(i) for i in itertools.count())
    group_id_type = "VARCHAR(5000)"
    pal_args = []
    assign_dfs = []
    arg_dfs = []
    output_names = []
    param_rows = None
    holiday_rows = None
    # pylint:disable=protected-access
    for arg in args:
        if isinstance(arg, DataFrame):
            in_name = next(in_palarg_gen)
            record = (in_name, arg)
            if arg._ttab_handling in ('safe', 'unknown'):
                assign_dfs.append(record)
            elif arg._ttab_handling == 'ttab':
                arg_dfs.append(record)
            else:
                raise ValueError("call_pal_tabvar can't handle input DataFrame.")
            pal_args.append(':'+in_name)
            # SQLTRACE
            if sql_tracer:
                sql_tracer.trace_object({
                    'name': in_name, # Generated name is unknown
                    'auto_name': in_name,
                    'table_type': arg.generate_table_type(),
                    'select': arg.select_statement
                }, sub_cat='auto')
        elif isinstance(arg, ParameterTable):
            if param_rows is not None:
                # I know there are a few PAL functions with no parameter table,
                # such as PAL_CHISQUARED_GOF_TEST, but I don't know any with
                # multiple parameter tables. We can adjust this if we need to.
                raise TypeError('Multiple parameter tables not supported')
            #print(arg.data)
            param_rows = arg.data if arg.data is not None else []
            # if len(param_rows) > 0 and len(param_rows[0]) == 5:
            #     group_id_type = arg.spec[0][1]
            pal_args.append(':params')
        elif isinstance(arg, HolidayTable):
            holiday_rows = arg.data if arg.data is not None else []
            pal_args.append(':holidays')
        elif isinstance(arg, _TEXT_TYPES):
            output_names.append(arg)
            pal_args.append(next(out_palarg_gen))
            # SQLTRACE Getting mapping of out variable to internal table name
            if sql_tracer:
                sql_tracer.trace_object({
                    'auto_name': 'out_'+str(len(output_names)-1),
                    'name': arg,
                    'table_type': 'auto',
                    'select': None
                }, sub_cat='auto')
        else:
            raise TypeError('Unexpected argument type {}'.format(type(arg)))

    if arg_dfs and with_hint != "NO_HANA_EXECUTION":
        header = 'DO ({})\nBEGIN\n'.format(',\n    '.join(
            'IN {} {} => {}'.format(argname, tabletype(df), df._ttab_reference)
            for argname, df in arg_dfs))
    else:
        header = 'DO\nBEGIN\n'

    if param_rows is not None:
        if len(param_rows) > 0 and len(param_rows[0]) == 5:
            param_table_creation = create_massive_params(param_rows,
                                                         group_id_type,
                                                         holiday_rows)
        elif 'MASSIVE' in name.upper():
            param_table_creation = create_massive_params(param_rows, group_id_type, holiday_rows)
        else:
            param_table_creation = create_params(param_rows, holiday_rows)
    else:
        param_table_creation = ''
    input_assignments = ''.join('{} = {};\n'.format(argname, df_in.select_statement)
                                for argname, df_in in assign_dfs)
    invocation = 'CALL _SYS_AFL.{}({});\n'.format(name, ', '.join(pal_args))
    with_hint_anonymous_block = False
    if with_hint is not None and with_hint != "NO_HANA_EXECUTION":
        if not with_hint.startswith('*'):
            invocation = 'CALL _SYS_AFL.{}({}) WITH HINT ({});\n'\
                .format(name, ', '.join(pal_args), with_hint)
        else:
            with_hint_anonymous_block=True
            with_hint = with_hint[1:]
    extract_outputs = ''.join(
        'CREATE {}COLUMN TABLE {} AS (SELECT * FROM :out_{});\n'.format(
            'LOCAL TEMPORARY ' if output_name.startswith('#') else '',
            quotename(output_name),
            i
        ) for i, output_name in enumerate(output_names)
    )
    if with_hint_anonymous_block:
        return (
            header
            + param_table_creation
            + input_assignments
            + invocation
            + extract_outputs
            + 'END WITH HINT ({})\n'.format(with_hint)
        )
    return (
        header
        + param_table_creation
        + input_assignments
        + invocation
        + extract_outputs
        + 'END\n'
    )
