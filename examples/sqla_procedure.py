"""
Example
=======

Run as::
    $ Set-Variable LUIGI_CONFIG_PATH "path/to/config/luigi.cfg"
    $ luigi --module examples.sql_procedure SqlTestInsertProcedure --local-scheduler


"""
from pathlib import Path
import luigi
from luigi.contrib.smtp import SmtpMail
from pathlib import Path
import os
from luigi.format import Nop
from dotenv import load_dotenv
from datetime import datetime
from luigi.contrib.sqla import SQLAlchemyProcedure


load_dotenv("local_testing.env", override=True)


class SqlTestInsertProcedure(SQLAlchemyProcedure):
    connection_string = os.getenv("TEST_CONNECTION_STRING").format(server=r"MSTM1BDB33\DB01", database="TESTING_DB",
                                                                   driver="ODBC+Driver+17+for+SQL+Server")
    procedure_call = f"EXEC [SP_INSERT] @VAL = 'test_at_{datetime.now().isoformat()}'"
    out_file = "DailyProcedure.json"


if __name__ == "__main__":
    luigi.run()
