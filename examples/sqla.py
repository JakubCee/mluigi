import luigi
from luigi.contrib import sqla
from datetime import datetime


class SQLATask(sqla.CopyToTable):
    # If database table is already created, then the schema can be loaded
    # by setting the reflect flag to True

    force = luigi.BoolParameter(default=False)
    n = luigi.IntParameter(default=10)
    reflect = True
    connection_string = "mssql+pyodbc://?odbc_connect=DRIVER={ODBC+Driver+17+for+SQL+Server};SERVER=MSTM1BDB33\DB01;DATABASE=TESTING_DB;Trusted_Connection=yes"  # in memory SQLite database
    sql_object = "luigi_test"  # name of the table to store data
    fast_executemany = True
    echo = False
    truncate = False

    def rows(self):
        for i in range(self.n):
            yield (i, f"Name_{i}", 5.33 * i, datetime.now() )

    def complete(self):
        if self.force:
            return False
        else:
            return super().complete()


class ProcTest(sqla.ExecProcedure):
    force = luigi.BoolParameter(default=False)
    connection_string = "mssql+pyodbc://?odbc_connect=DRIVER={ODBC+Driver+17+for+SQL+Server};SERVER=MSTM1BDB33\DB01;DATABASE=TESTING_DB;Trusted_Connection=yes"  # in memory SQLite database
    sql_params = "@val = 'luigi_teste'"
    sql_object = "SP_INSERT"

    def complete(self):
        if self.force:
            return False
        else:
            return super().complete()

if __name__ == '__main__':
    #task = SQLATask(force=True, n=1)
    task = ProcTest(force=True)
    luigi.build([task], local_scheduler=True)

