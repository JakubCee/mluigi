import logging

import luigi
from luigi.contrib import sqla
from datetime import datetime, timedelta
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
    sql_params = f"@val = '{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}'"
    sql_object = "SP_INSERT"
    expire_at = luigi.DateSecondParameter()


class MiddleTask(luigi.Task):
    expire_at = luigi.DateSecondParameter()

    def requires(self):
        return ProcTest(expire_at=self.expire_at)

    def output(self):
        return luigi.mock.MockTarget(f"{self.__class__.__name__}.txt")

    def run(self):
        self.output().open('w').close()


class LastTask(luigi.Task):
    expire_at = luigi.DateSecondParameter()
    def requires(self):
        return MiddleTask(expire_at=self.expire_at)

    def output(self):
        return luigi.mock.MockTarget(f"{self.__class__.__name__}.txt")

    def run(self):
        self.output().open('w').close()



# if __name__ == '__main__':
#     #task = SQLATask(force=True, n=1)
#     #t = ProcTest(force=True)
#
#     task = ProcTest(expire_at=datetime.now() - timedelta(seconds=5))
#     run_results = luigi.build([task], local_scheduler=True, detailed_summary=True, log_level='INFO')
#     print(run_results.summary_text)
