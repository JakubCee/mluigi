from luigi.contrib.sharepoint import SharepointTarget
import os
import luigi
from luigi.contrib.sqla import SqlToExcelTask
from dotenv import load_dotenv
from luigi.format import Nop
from luigi import LocalExcelTarget

load_dotenv("local_testing.env", override=True)

SITE_URL = os.getenv("SHP_SITE_URL")
API_ID = os.getenv("SHP_API_ID")
API_KEY = os.getenv("SHP_API_KEY")


class SqlExcelDump(SqlToExcelTask):
    connection_string = os.getenv("TEST_CONNECTION_STRING")
    sheet_cmd_dict = {"YS": "SELECT * FROM sys.tables",
                      "MYCAL": "CALENDAR"}
    col_max_width = 200
    pd_writer_kwargs = {"datetime_format": "DD.MMM.YYYY"}
    out_file = "SqlToExcelDumpTest.xlsx"


class InputFileToCopy(luigi.ExternalTask):
    def output(self):
        return luigi.LocalTarget("test_file_large.bin", format=Nop)

class CopyToShp(luigi.Task):

    def requires(self):
        return InputFileToCopy()

    def output(self):
        return SharepointTarget(path="/xUnitTests_SHP/test_mluigi/test_pipes/large_bin_file.bin",
                                site_url=SITE_URL,
                                api_id=API_ID,
                                api_key=API_KEY,
                                format=Nop
                                )

    def run(self):
        with self.input().open('r') as inf:
            with self.output().temporary_path() as tp:
                with open(tp, "wb") as out_file:
                    out_file.write(inf.read())

            # with self.output().open("w") as spf:
            #     spf.write(inf.read())


class FromSpToLocal(luigi.Task):
    def output(self):
        return luigi.LocalTarget("test_file_large.bin", format=Nop)

    def run(self):
        input = SharepointTarget(path="/xUnitTests_SHP/test_mluigi/test_download/FileBinary.xlsx",
                                 site_url=SITE_URL,
                                 api_id=API_ID,
                                 api_key=API_KEY,
                                 format=Nop
                                 )
        spi = input.open("r")
        content = spi.read()

        with self.output().open('w') as xlo:
            xlo.write(content)


task = CopyToShp()
luigi.build([task], local_scheduler=True)

