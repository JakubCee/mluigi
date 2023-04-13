"""
Get data from SQLA into excel

"""
import os
import luigi
from luigi.contrib.sqla import SqlToExcelTask
from luigi.contrib.smtp import SmtpMail
from dotenv import load_dotenv

load_dotenv("local_testing.env", override=True)


class SqlExcelDump(SqlToExcelTask):
    connection_string = os.getenv("TEST_CONNECTION_STRING").format(server=r"MSTM1BDB33\DB01", database="TESTING_DB",
                                                                   driver="ODBC+Driver+17+for+SQL+Server")
    sheet_cmd_dict = {"YS": "SELECT * FROM sys.tables",
                       "MYCAL": "CALENDAR"}
    col_max_width = 200
    pd_writer_kwargs = {"datetime_format": "DD.MMM.YYYY"}
    out_file = "SqlToExcelDumpTest.xlsx"


class SendDataByMail(SmtpMail):
    to = os.getenv("MAIL_TEST_TO")

    def requires(self):
        return SqlExcelDump()

    def output(self):
        return luigi.LocalTarget("Outputmail.txt")






if __name__ == "__main__":
    from pathlib import Path

    Path("SqlToExcelDumpTest.xlsx").unlink(missing_ok=True)

    task = SqlExcelDump()
    luigi.build([task], local_scheduler=True)





