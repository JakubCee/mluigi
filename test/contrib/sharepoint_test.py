import os

import pytest
from pathlib import Path
from luigi.contrib import sharepoint as sp
from dotenv import load_dotenv

load_dotenv("C:/apps/_PACKAGES/mluigi/local_testing.env", override=True)

SITE_URL = os.getenv("SHP_SITE_URL")
API_ID = os.getenv("SHP_API_ID")
API_KEY = os.getenv("SHP_API_KEY")


@pytest.fixture()
def SPClient():
    yield sp.SharepointClient(site_url=SITE_URL, api_key=API_KEY, api_id=API_ID)


class TestSharepointClient:
    def test_ctx(self, SPClient):
        assert SPClient.conn

    @pytest.mark.parametrize("path, exists", [
        ("xUnitTests_SHP/test_mluigi/test_exists", True),
        ("xUnitTests_SHP/test_mluigi/test_notexists", False),
        ("xUnitTests_SHP/test_mluigi/test_exists/File1.txt", True),
        ("xUnitTests_SHP/test_mluigi/test_exists/File1NotExists.txt", False),
        ("test_exists/File1NotExists.txt", False),
        ("/", True),
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

        exp_list = ['/teams/OSAReport/xUnitTests_SHP/test_mluigi/test_delete',
                    '/teams/OSAReport/xUnitTests_SHP/test_mluigi/test_delete/File1.txt',
                    '/teams/OSAReport/xUnitTests_SHP/test_mluigi/test_delete/File2.txt',
                    '/teams/OSAReport/xUnitTests_SHP/test_mluigi/test_deletess',
                    '/teams/OSAReport/xUnitTests_SHP/test_mluigi/test_exists',
                    '/teams/OSAReport/xUnitTests_SHP/test_mluigi/test_exists/File1.txt']
        assert SPClient.listdir("/teams/OSAReport/xUnitTests_SHP/test_mluigi") == exp_list
        assert len(SPClient.listdir("/teams/OSAReport/xUnitTests_SHP/test_mluigi", recursive=False)) < len(exp_list)
