import logging

import luigi
from luigi.contrib import sqla
from datetime import datetime
from luigi.mock import MockTarget

class MockAlwaysExists(MockTarget):
    def exists(self):
        return True


class SQLATask(sqla.CopyToTable):
    # If database table is already created, then the schema can be loaded
    # by setting the reflect flag to True

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


class ProcTest(sqla.ExecProcedure):
    connection_string = "mssql+pyodbc://?odbc_connect=DRIVER={ODBC+Driver+17+for+SQL+Server};SERVER=MSTM1BDB33\DB01;DATABASE=TESTING_DB;Trusted_Connection=yes"  # in memory SQLite database
    sql_params = "@val = 'luigi_teste'"
    sql_object = "SP_INSERT"
    expire_at = datetime(2023, 1, 10, 1)


class MiddleTask(luigi.Task):

    def requires(self):
        return ProcTest()

    def output(self):
        return luigi.mock.MockTarget(f"{self.__class__.__name__}.txt")

    def run(self):
        print("Do some stuff in run() of MiddleTask")
        self.output().open('w').close()


class LastTask(luigi.Task):
    def requires(self):
        return MiddleTask()

    def output(self):
        return luigi.mock.MockTarget(f"{self.__class__.__name__}.txt")

    def run(self):
        print("Do stuff in LastTask")
        self.output().open('w').close()



if __name__ == '__main__':
    #task = SQLATask(force=True, n=1)
    #t = ProcTest(force=True)

    task = ProcTest()
    print(task.output().expire_at)
    print(task.output().exists())
    luigi.build([task], local_scheduler=True, detailed_summary=True, log_level='INFO')

