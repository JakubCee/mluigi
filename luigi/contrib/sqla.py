# -*- coding: utf-8 -*-
#
# Copyright (c) 2015 Gouthaman Balaraman
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#
"""
Support for SQLAlchemy. Provides SQLAlchemyTarget for storing in databases
supported by SQLAlchemy. The user would be responsible for installing the
required database driver to connect using SQLAlchemy.

Minimal example of a job to copy data to database using SQLAlchemy is as shown
below:

.. code-block:: python

    from sqlalchemy import String
    import luigi
    from luigi.contrib import sqla

    class SQLATask(sqla.CopyToTable):
        # columns defines the table schema, with each element corresponding
        # to a column in the format (args, kwargs) which will be sent to
        # the sqlalchemy.Column(*args, **kwargs)
        columns = [
            (["item", String(64)], {"primary_key": True}),
            (["property", String(64)], {})
        ]
        connection_string = "sqlite://"  # in memory SQLite database
        table = "item_property"  # name of the table to store data

        def rows(self):
            for row in [("item1", "property1"), ("item2", "property2")]:
                yield row

    if __name__ == '__main__':
        task = SQLATask()
        luigi.build([task], local_scheduler=True)


If the target table where the data needs to be copied already exists, then
the column schema definition can be skipped and instead the reflect flag
can be set as True. Here is a modified version of the above example:

.. code-block:: python

    from sqlalchemy import String
    import luigi
    from luigi.contrib import sqla

    class SQLATask(sqla.CopyToTable):

        # If database table is already created, then the schema can be loaded
        # by setting the reflect flag to True
        reflect = True
        connection_string = "sqlite://"  # in memory SQLite database
        table = "item_property"  # name of the table to store data

        def rows(self):
            for row in [("item1", "property1"), ("item2", "property2")]:
                yield row

    if __name__ == '__main__':
        task = SQLATask()
        luigi.build([task], local_scheduler=True)


In the above examples, the data that needs to be copied was directly provided by
overriding the rows method. Alternately, if the data comes from another task, the
modified example would look as shown below:

.. code-block:: python

    from sqlalchemy import String
    import luigi
    from luigi.contrib import sqla
    from luigi.mock import MockTarget

    class BaseTask(luigi.Task):
        def output(self):
            return MockTarget("BaseTask")

        def run(self):
            out = self.output().open("w")
            TASK_LIST = ["item%d\\tproperty%d\\n" % (i, i) for i in range(10)]
            for task in TASK_LIST:
                out.write(task)
            out.close()

    class SQLATask(sqla.CopyToTable):
        # columns defines the table schema, with each element corresponding
        # to a column in the format (args, kwargs) which will be sent to
        # the sqlalchemy.Column(*args, **kwargs)
        columns = [
            (["item", String(64)], {"primary_key": True}),
            (["property", String(64)], {})
        ]
        connection_string = "sqlite://"  # in memory SQLite database
        table = "item_property"  # name of the table to store data

        def requires(self):
            return BaseTask()

    if __name__ == '__main__':
        task1, task2 = SQLATask(), BaseTask()
        luigi.build([task1, task2], local_scheduler=True)


In the above example, the output from `BaseTask` is copied into the
database. Here we did not have to implement the `rows` method because
by default `rows` implementation assumes every line is a row with
column values separated by a tab. One can define `column_separator`
option for the task if the values are say comma separated instead of
tab separated.

You can pass in database specific connection arguments by setting the connect_args
dictionary.  The options will be passed directly to the DBAPI's connect method as
keyword arguments.

The other option to `sqla.CopyToTable` that can be of help with performance aspect is the
`chunk_size`. The default is 5000. This is the number of rows that will be inserted in
a transaction at a time. Depending on the size of the inserts, this value can be tuned
for performance.

See here for a `tutorial on building task pipelines using luigi
<http://gouthamanbalaraman.com/blog/building-luigi-task-pipeline.html>`_ and
using `SQLAlchemy in workflow pipelines <http://gouthamanbalaraman.com/blog/sqlalchemy-luigi-workflow-pipeline.html>`_.

Author: Gouthaman Balaraman
Date: 01/02/2015
"""


import abc
import collections
import datetime
import itertools
import json
import logging
import time

import luigi
import os
import sqlalchemy
import pandas as pd

from luigi.mock import MockTarget


class SQLAlchemyTarget(luigi.Target):
    """Database target using SQLAlchemy.
    
    This will rarely have to be directly instantiated by the user.
    
    Typical usage would be to override `luigi.contrib.sqla.CopyToTable` class
    to create a task to write to the database.


    """
    marker_table = None
    _engine_dict = {}  # dict of sqlalchemy engine instances
    Connection = collections.namedtuple("Connection", "engine pid")

    def __init__(self, connection_string, target_table, update_id, echo=False, connect_args=None, fast_executemany=True):
        """
        Constructor for the SQLAlchemyTarget.

        :param connection_string: SQLAlchemy connection string
        :type connection_string: str
        :param target_table: The table name for the data
        :type target_table: str
        :param update_id: An identifier for this data set
        :type update_id: str
        :param echo: Flag to setup SQLAlchemy logging
        :type echo: bool
        :param connect_args: A dictionary of connection arguments
        :type connect_args: dict
        """
        if connect_args is None:
            connect_args = {}

        self.target_table = target_table
        self.update_id = update_id
        self.connection_string = connection_string
        self.echo = echo
        self.connect_args = connect_args
        self.marker_table_bound = None
        self.fast_executemany = fast_executemany


    @property
    def engine(self):
        """
        :returns: Recreate the engine connection if it wasn't originally created
        by the current process.

        """
        pid = os.getpid()
        conn = SQLAlchemyTarget._engine_dict.get(self.connection_string)
        if not conn or conn.pid != pid:
            # create and reset connection
            engine = sqlalchemy.create_engine(
                self.connection_string,
                echo=self.echo,
                connect_args=self.connect_args,
                fast_executemany=self.fast_executemany
            )
            SQLAlchemyTarget._engine_dict[self.connection_string] = self.Connection(engine, pid)
        return SQLAlchemyTarget._engine_dict[self.connection_string].engine

    def touch(self):
        """Mark this update as complete."""
        if self.marker_table_bound is None:
            self.create_marker_table()

        table = self.marker_table_bound
        id_exists = self.exists()
        with self.engine.begin() as conn:
            if not id_exists:
                ins = table.insert().values(update_id=self.update_id, target_table=self.target_table,
                                            inserted=datetime.datetime.now())
            else:
                ins = table.update().where(sqlalchemy.and_(table.c.update_id == self.update_id,
                                                           table.c.target_table == self.target_table)).\
                    values(update_id=self.update_id, target_table=self.target_table,
                           inserted=datetime.datetime.now())
            conn.execute(ins)
        assert self.exists()

    def exists(self):
        row = None
        if self.marker_table_bound is None:
            self.create_marker_table()
        with self.engine.begin() as conn:
            table = self.marker_table_bound
            s = sqlalchemy.select([table]).where(sqlalchemy.and_(table.c.update_id == self.update_id,
                                                                 table.c.target_table == self.target_table)).limit(1)
            row = conn.execute(s).fetchone()
        return row is not None

    def create_marker_table(self):
        """Create marker table if it doesn't exist.
        
        Using a separate connection since the transaction might have to be reset.
        """
        if self.marker_table is None:
            self.marker_table = luigi.configuration.get_config().get('sqlalchemy', 'marker-table', 'table_updates')

        engine = self.engine

        with engine.begin() as con:
            metadata = sqlalchemy.MetaData()
            if not con.dialect.has_table(con, self.marker_table):
                self.marker_table_bound = sqlalchemy.Table(
                    self.marker_table, metadata,
                    sqlalchemy.Column("update_id", sqlalchemy.String(128), primary_key=True),
                    sqlalchemy.Column("target_table", sqlalchemy.String(128)),
                    sqlalchemy.Column("inserted", sqlalchemy.DateTime, default=datetime.datetime.now()))
                metadata.create_all(engine)
            else:
                metadata.reflect(only=[self.marker_table], bind=engine)
                self.marker_table_bound = metadata.tables[self.marker_table]

    def open(self, mode):
        raise NotImplementedError("Cannot open() SQLAlchemyTarget")


class CopyToTable(luigi.Task):
    """An abstract task for inserting a data set into SQLAlchemy RDBMS
    
    Usage:
    
    * subclass and override the required `connection_string`, `table` and `columns` attributes.
    * optionally override the `schema` attribute to use a different schema for
      the target table.


    """
    _logger = logging.getLogger('luigi-interface')

    echo = False
    fast_executemany=True
    connect_args = {}

    @property
    @abc.abstractmethod
    def connection_string(self):
        """Conn string for engine"""
        return ""

    @property
    @abc.abstractmethod
    def table(self):
        """Table where rows will be inserted"""
        return ""

    # specify the columns that define the schema. The format for the columns is a list
    # of tuples. For example :
    # columns = [
    #            (["id", sqlalchemy.Integer], dict(primary_key=True)),
    #            (["name", sqlalchemy.String(64)], {}),
    #            (["value", sqlalchemy.String(64)], {})
    #        ]
    # The tuple (args_list, kwargs_dict) here is the args and kwargs
    # that need to be passed to sqlalchemy.Column(*args, **kwargs).
    # If the tables have already been setup by another process, then you can
    # completely ignore the columns. Instead set the reflect value to True below
    columns = []

    # Specify the database schema of the target table, if supported by the
    # RDBMS. Note that this doesn't change the schema of the marker table.
    # The schema MUST already exist in the database, or this will task fail.
    schema = ''

    # options
    column_separator = "\t"  # how columns are separated in the file copied into postgres
    chunk_size = 5000   # default chunk size for insert
    reflect = False  # Set this to true only if the table has already been created by alternate means
    truncate = False  # Set to True if table should be truncated before insert

    def create_table(self, engine):
        """Override to provide code for creating the target table.
        
        By default it will be created using types specified in columns.
        If the table exists, then it binds to the existing table.
        
        If overridden, use the provided connection object for setting up the table in order to
        create the table and insert data using the same transaction.

        :param engine: The sqlalchemy engine instance
        :type engine: object

        """
        def construct_sqla_columns(columns):
            """

            :param columns: 

            """
            retval = [sqlalchemy.Column(*c[0], **c[1]) for c in columns]
            return retval

        needs_setup = (len(self.columns) == 0) or (False in [len(c) == 2 for c in self.columns]) if not self.reflect else False
        if needs_setup:
            # only names of columns specified, no types
            raise NotImplementedError("create_table() not implemented for %r and columns types not specified" % self.table)
        else:
            # if columns is specified as (name, type) tuples
            with engine.begin() as con:

                if self.schema:
                    metadata = sqlalchemy.MetaData(schema=self.schema)
                else:
                    metadata = sqlalchemy.MetaData()

                try:
                    if not con.dialect.has_table(con, self.table, self.schema or None):
                        sqla_columns = construct_sqla_columns(self.columns)
                        self.table_bound = sqlalchemy.Table(self.table, metadata, *sqla_columns)
                        metadata.create_all(engine)
                    else:
                        full_table = '.'.join([self.schema, self.table]) if self.schema else self.table
                        metadata.reflect(only=[self.table], bind=engine)
                        self.table_bound = metadata.tables[full_table]
                except Exception as e:
                    self._logger.exception(self.table + str(e))

    def update_id(self):
        """This update id will be a unique identifier for this insert on this table."""
        return self.task_id

    def output(self):
        """ """
        return SQLAlchemyTarget(
            connection_string=self.connection_string,
            target_table=self.table,
            update_id=self.update_id(),
            connect_args=self.connect_args,
            echo=self.echo,
            fast_executemany=self.fast_executemany
        )

    def rows(self):
        """


        :returns: This method can be overridden for custom file types or formats.

        """
        with self.input().open('r') as fobj:
            for line in fobj:
                yield line.strip("\n").split(self.column_separator)

    def run(self):
        """ """
        self._logger.info("Running task copy to table for update id %s for table %s" % (self.update_id(), self.table))
        output = self.output()
        engine = output.engine
        self.create_table(engine)
        if self.truncate:
            with engine.begin() as con:
                con.execute("TRUNCATE TABLE [%s];" % self.table)
                self._logger.info("Table [%s] is truncated" % self.table)

        with engine.begin() as conn:
            rows = iter(self.rows())
            ins_rows = [dict(zip(("_" + c.key for c in self.table_bound.c), row))
                        for row in itertools.islice(rows, self.chunk_size)]
            while ins_rows:
                self.copy(conn, ins_rows, self.table_bound)
                ins_rows = [dict(zip(("_" + c.key for c in self.table_bound.c), row))
                            for row in itertools.islice(rows, self.chunk_size)]
                self._logger.info("Finished inserting %d rows into SQLAlchemy target" % len(ins_rows))
        output.touch()
        self._logger.info("Finished inserting rows into SQLAlchemy target")

    def copy(self, conn, ins_rows, table_bound):
        """This method does the actual insertion of the rows of data given by ins_rows into the
        database. A task that needs row updates instead of insertions should overload this method.

        :param conn: The sqlalchemy connection object
        :param ins_rows: The dictionary of rows with the keys in the format _<column_name>. For example
        if you have a table with a column name "property", then the key in the dictionary
        would be "_property". This format is consistent with the bindparam usage in sqlalchemy.
        :param table_bound: The object referring to the table

        """
        bound_cols = dict((c, sqlalchemy.bindparam("_" + c.key)) for c in table_bound.columns)
        ins = table_bound.insert().values(bound_cols)
        conn.execute(ins, ins_rows)


class SQLAlchemyProcedure(luigi.Task):
    """Execute SQL Procedure on server. Must specify `connection_string` and `procedure_call` arguments.
    Argument `out_file` should be `json` file.

    Attrs:
    procedure_call: str = "EXEC SPC_SOME_PROCEDURE @value = 'some value to insert'"
    connection_string: str = conn

    Other attrs:
    echo = False
    engine_kwargs = {}
    keep_output: bool = True  # LocalTarget as json or MockTarget
    """
    @property
    @abc.abstractmethod
    def connection_string(self):
        """ """
        return ""

    @property
    @abc.abstractmethod
    def procedure_call(self):
        """Valid full command to be executed on SQL."""
        return ""

    @property
    def out_file(self):
        """Overwrite with path to output.
        Defaults to SqlProc_[YMD]_[TASK_ID].json
        """
        return f"SqlProc_{self.ts.strftime('%Y%m%d')}_{self.task_id}.json"

    echo = False
    engine_kwargs = {}
    _engine_dict = {}
    Connection = collections.namedtuple("Connection", ["engine", "pid"])
    keep_output: bool = True
    ts = datetime.datetime.now()

    @property
    def engine(self):
        """


        :returns: Recreate the engine connection if it wasn't originally created
        by the current process.

        """
        pid = os.getpid()
        conn = self._engine_dict.get(self.connection_string)
        if not conn or conn.pid != pid:
            # create and reset connection
            engine = sqlalchemy.create_engine(
                self.connection_string,
                echo=self.echo,
                **self.engine_kwargs
            )
            SQLAlchemyProcedure._engine_dict[self.connection_string] = self.Connection(engine=engine, pid=pid)
        return SQLAlchemyProcedure._engine_dict[self.connection_string].engine

    def run(self):
        """Execute procedure and output json with summary data."""
        if self.procedure_call:
            with self.engine.connect() as conn, conn.begin():
                start = time.perf_counter()
                conn.execute(sqlalchemy.text(self.procedure_call))
                exec_time = time.perf_counter() - start
                with self.output().open('w') as f:
                    data = {
                        "exec_time": self.ts.isoformat(),
                        "conn_string": self.connection_string,
                        "procedure_call": self.procedure_call,
                        "execution_time_sec": round(exec_time, 4)
                    }
                    json.dump(data, fp=f, indent=4)

    def output(self):
        """Return json file or MockTarget if param ``keep_output`` is set to False"""
        if self.keep_output:
            return luigi.LocalTarget(self.out_file)
        else:
            return MockTarget(self.out_file)


class SqlToExcelTask(luigi.Task):
    """Export data from server into Excel.

    Attrs:
    sheet_cmd_dict: dict[str, str] = {"Sheetname": "SELECT * FROM TABLE"}
    out_file: str = Filename for output, defaults to Export_[YMD_HMS].xlsx
    connection_string

    Other attrs:

    pd_read_sql_kwargs: dict = {}  # additional kwargs for pandas `read_sql` method
    pd_writer_kwargs: dict = {}  # additional kwargs for pandas Excel Writer object
    pd_to_excel_kwargs: dict = {}  # additional kwargs for pandas `to_excel` method
    col_max_width: int = None  # Specify max width for columns in output
    """

    @property
    def out_file(self):
        """Location where data will be dumped"""
        return f"Export_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

    @property
    @abc.abstractmethod
    def connection_string(self):
        """Connection string like
        `mssql+pyodbc://?odbc_connect=DRIVER={ODBC+Driver+17+for+SQL+Server};SERVER=MSTM1BDB33\DB01;DATABASE={db}};Trusted_Connection=yes`
        """
        return ""

    @property
    @abc.abstractmethod
    def sheet_cmd_dict(self):
        """Dictionary where key is Sheetname in Excel and cmd is valid pandas call to get data from connection."""
        return {}

    # additional kwargs for pandas `read_sql` method
    pd_read_sql_kwargs: dict = {}
    # additional kwargs for pandas Excel Writer object
    pd_writer_kwargs: dict = {}
    # additional kwargs for pandas `to_excel` method
    pd_to_excel_kwargs: dict = {}
    # Specify max width for columns in output
    col_max_width: int = None

    def output(self):
        return luigi.LocalExcelTarget(str(self.out_file))

    def _get_engine(self):
        self.engine = sqlalchemy.create_engine(self.connection_string)
        return self.engine

    def _get_data(self):
        """Connect to SQL and load dataframes into self._dataframes."""

        self._dataframes = {}
        engine = self._get_engine()
        for sheet_name, cmd in self.sheet_cmd_dict.items():
            df = pd.read_sql(cmd, engine, **self.pd_read_sql_kwargs)
            self._dataframes[sheet_name] = df
        return self._dataframes

    def run(self):
        """Load data, import each rowset into sheet

        """
        if not self.connection_string:
            raise AttributeError("Connection string is empty")
        dataframes_dict = self._get_data()
        # init writer
        with pd.ExcelWriter(self.output().path, engine='xlsxwriter', **self.pd_writer_kwargs) as writer:
            # put each df into writer
            for sheet, data in dataframes_dict.items():
                data.to_excel(writer, sheet_name=sheet[:31], index=False, **self.pd_to_excel_kwargs)

                # adjust columns
                if self.col_max_width:
                    for column in data:
                        col_width_max = min(
                            max(data[column].astype(str).map(len).max(), len(column)),
                            self.col_max_width,
                        )
                        col_idx = data.columns.get_loc(column)
                        writer.sheets[sheet[:31]].set_column(col_idx, col_idx, col_width_max+3)
