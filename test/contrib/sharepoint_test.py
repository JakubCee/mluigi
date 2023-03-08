import os
import uuid
from datetime import datetime

import pytest
from pathlib import Path
import luigi
from luigi.contrib import sharepoint as sp
from dotenv import load_dotenv

load_dotenv("C:/apps/_PACKAGES/mluigi/local_testing.env", override=True)

SITE_URL = os.getenv("SHP_SITE_URL")
API_ID = os.getenv("SHP_API_ID")
API_KEY = os.getenv("SHP_API_KEY")


@pytest.fixture(scope="module")
def SPClient():
    yield sp.SharepointClient(site_url=SITE_URL, api_key=API_KEY, api_id=API_ID)

@pytest.fixture()
def large_file(tmp_path):
    content = b'El ni\xc3\xb1o come camar\xc3\xb3n\n' * 500_000
    loc = tmp_path / "large_file.bin"
    with open(loc, "wb") as f:
        f.write(content)
    yield loc


class TestSharepointClient:
    def test_ctx(self, SPClient):
        assert SPClient.conn

    @pytest.mark.parametrize("path, exists", [
        ("xUnitTests_SHP/test_mluigi/test_exists", True),
        ("xUnitTests_SHP/test_mluigi/test_notexists", False),
        ("xUnitTests_SHP/test_mluigi/test_exists/File1.txt", True),
        ("xUnitTests_SHP/test_mluigi/test_exists/File1NotExists.txt", False),
        ("test_exists/File1NotExists.txt", False),
        #("/", True),  ## TODO : throws http error 400 - bad request for url
    ])
    def test_exists(self, SPClient, path, exists):
        assert SPClient.exists(path) == exists

    @pytest.mark.parametrize("path, isdir", [
        ("xUnitTests_SHP/test_mluigi/test_exists", True),
        ("xUnitTests_SHP/test_mluigi/test_notexists", False),
        ("xUnitTests_SHP/test_mluigi/test_exists/File1.txt", False),
        ("xUnitTests_SHP/test_mluigi/test_exists/File1NotExists.txt", False),
        ("test_exists/File1NotExists.txt", False),
        ("/", True),
    ])
    def test_isdir(self, SPClient, path, isdir):
        assert SPClient.isdir(path) == isdir

    def test_list_dir(self, SPClient):
        assert SPClient.listdir("xUnitTests_SHP/test_mluigi/test_exists/File1.txt") == None
        assert len(SPClient.listdir("xUnitTests_SHP/test_mluigi/test_exists")) >= 1

        exp_list = ['/teams/OSAReport/xUnitTests_SHP/test_mluigi/test_listdir/File1.txt',
                    '/teams/OSAReport/xUnitTests_SHP/test_mluigi/test_listdir/Dir1/File1_1.txt',
                    '/teams/OSAReport/xUnitTests_SHP/test_mluigi/test_listdir/Dir1/File1_2.txt',
                    '/teams/OSAReport/xUnitTests_SHP/test_mluigi/test_listdir/Dir2/File2_1.txt',
                    '/teams/OSAReport/xUnitTests_SHP/test_mluigi/test_listdir/Dir2/File2_2.txt',
                    '/teams/OSAReport/xUnitTests_SHP/test_mluigi/test_listdir/Dir1',
                    '/teams/OSAReport/xUnitTests_SHP/test_mluigi/test_listdir/Dir2',]
        returned = SPClient.listdir("/teams/OSAReport/xUnitTests_SHP/test_mluigi/test_listdir")
        for l in returned:
            assert l in exp_list, f"{l} is not present in Expected list"
        assert len(SPClient.listdir("/teams/OSAReport/xUnitTests_SHP/test_mluigi", recursive=False)) < len(exp_list)

    def test_download_file(self, SPClient):
        stream = SPClient.download_as_bytes("/xUnitTests_SHP/test_mluigi/test_download/FileBinary.xlsx")
        assert stream

    def test_download_dir(self, SPClient):
        with pytest.raises(FileExistsError, match="is not a file"):
            SPClient.download_as_bytes("/xUnitTests_SHP/test_mluigi/test_download")

    def test_download_nonexisting(self, SPClient):
        with pytest.raises(FileExistsError, match="does not exist"):
            SPClient.download_as_bytes("/xUnitTests_SHP/test_mluigi/test_download/nonexistingpath")

    def test_copy_file(self, SPClient):
        SPClient.copy(path="/xUnitTests_SHP/test_mluigi/test_copy/from/Original.xlsx",
                      dest="/xUnitTests_SHP/test_mluigi/test_copy/to/Copy.xlsx")

    @pytest.mark.parametrize("sp_filepath, in_session", [
        ("/xUnitTests_SHP/test_mluigi/test_upload", True),
        ("/xUnitTests_SHP/test_mluigi/test_upload", False),
    ])
    def test_upload_file(self, SPClient, large_file, sp_filepath, in_session):
        SPClient.upload(large_file, sp_filepath, in_session=in_session)
        full_sp_path = sp_filepath + "/large_file.bin"
        assert SPClient.exists(full_sp_path)
        SPClient.remove(full_sp_path)
        assert not SPClient.exists(full_sp_path)

