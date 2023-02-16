from luigi.contrib.sharepoint import SharepointTarget
import os
import luigi
from luigi.contrib.sqla import SqlToExcelTask
from dotenv import load_dotenv
from luigi.format import Nop
from datetime import datetime


TS = datetime.now().strftime("%Y%m%d%H%M%S_")

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
    is_large = luigi.BoolParameter(default=False)

    def run(self):
        content = b"X\n\xe2\x28\xa1"
        content = content if not self.is_large else content * 21_000_000
        with self.output().open('w') as f:
            f.write(content)

    def output(self):
        return luigi.LocalTarget("test_file_large.bin", format=Nop)


class CopyToShpTempFile(luigi.Task):
    is_large = luigi.BoolParameter(default=False)

    def requires(self):
        return InputFileToCopy(is_large=self.is_large)

    def output(self):
        return SharepointTarget(path=f"/xUnitTests_SHP/test_mluigi/test_pipes/large_bin_file_from_temp{TS}.bin",
                                site_url=SITE_URL,
                                api_id=API_ID,
                                api_key=API_KEY,
                                format=Nop
                                )

    def run(self):
        with self.output().temporary_path() as tp:
            with open(tp, "wb") as out_file:
                with self.input().open('r') as i:
                    out_file.write(i.read())


class CopyToShp(luigi.Task):
    is_large = luigi.BoolParameter(default=True)

    def requires(self):
        return InputFileToCopy(is_large=self.is_large)

    def output(self):
        return SharepointTarget(path=f"/xUnitTests_SHP/test_mluigi/test_pipes/large_bin_file{TS}.bin",
                                site_url=SITE_URL,
                                api_id=API_ID,
                                api_key=API_KEY,
                                format=Nop
                                )

    def run(self):
        with self.output().open('w') as f:
            with self.input().open('r') as i:
                f.write(i.read())


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


task = CopyToShp(is_large=False)
luigi.build([task], local_scheduler=True)

