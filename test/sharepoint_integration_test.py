import uuid
import os
import pytest
from datetime import datetime
from pathlib import Path
from luigi.contrib import sharepoint as sp
from dotenv import load_dotenv

load_dotenv("C:/apps/_PACKAGES/mluigi/local_testing.env", override=True)

SITE_URL = os.getenv("SHP_SITE_URL")
API_ID = os.getenv("SHP_API_ID")
API_KEY = os.getenv("SHP_API_KEY")
ts = datetime.now().strftime('%Y%m%d_%H%M%S')
TEST_ID = str(uuid.uuid4())
SHP_TEST_ROOT = Path("xUnitTests_SHP") / ts


@pytest.fixture
def setup_structure(tmp_path):
    TEST_ID = str(uuid.uuid4())
    root = tmp_path
    uploaded = root / "uploaded"
    copied = root / "copied"
    moved = root / "moved"

    bin_content_small = TEST_ID.encode() + b'\nEl ni\xc3\xb1o come camar\xc3\xb3n\n'
    bin_content_large = bin_content_small * 100_000
    text_content = f"{TEST_ID} at {ts}\n"

    path_type_content = [
        (root, "folder", None),
        (uploaded, "folder", None),
        (uploaded / "large_file_bin.bin", "filebin", bin_content_large),
        (uploaded / "small_file_bin.bin", "filebin", bin_content_small),
        (uploaded / "test_file.txt", "filetxt", text_content),
        (copied, "folder", None),
        (moved, "folder", None),
    ]

    for p in path_type_content:
        pth, p_type, content = p
        if p_type == "folder":
            pth.mkdir(parents=True, exist_ok=True)
        else:
            (pth.parent).mkdir(parents=True, exist_ok=True)
        if p_type == "filebin":
            pth.write_bytes(content)
        elif p_type == "filetxt":
            pth.write_text(content)
    yield root


@pytest.fixture(scope="module")
def SPClient():
    yield sp.SharepointClient(site_url=SITE_URL, api_key=API_KEY, api_id=API_ID)



@pytest.fixture
def sp_structure(SPClient, setup_structure):
    for i in setup_structure.rglob("*"):
        if i.is_file():
            session = True if "large" in i.name else False
            sh_path_file = SHP_TEST_ROOT / (i.relative_to(setup_structure))
            sh_path_folder = sh_path_file.parent

            # upload file
            SPClient.upload(local_path=i, dest_path=sh_path_folder, in_session=session)
            # check if exists and is not a dir
            assert SPClient.exists(sh_path_file)
            assert not SPClient.isdir(sh_path_file)
            # download back and check content
            actual_content = SPClient.download_as_bytes(sh_path_file)
            with open(i, "rb") as f:
                assert actual_content == f.read()

        # test mkdir and if exists
        if i.is_dir():
            sh_path_dir = SHP_TEST_ROOT / (i.relative_to(setup_structure))
            # test mk_dir
            SPClient.mkdir(sh_path_dir)
            # check if exists and is a dir
            SPClient.exists(sh_path_dir)
            SPClient.isdir(sh_path_dir)

    yield

    SPClient.remove(SHP_TEST_ROOT, recursive=True)


def test_listdir(sp_structure, setup_structure, SPClient):
    # test list_dir
    actual_structure = set([Path(p) for p in SPClient.listdir(SHP_TEST_ROOT)])
    expected_structure = {Path("/teams/OSAReport") / SHP_TEST_ROOT / (p.relative_to(setup_structure))
                          for p in setup_structure.rglob("*")}
    assert actual_structure == expected_structure


def test_rename(sp_structure, setup_structure, SPClient):
    # rename file
    for p in SPClient.listdir(SHP_TEST_ROOT):
        if not SPClient.isdir(p):
            parent = Path(p).parent
            new_name = Path(p).stem
            suffix = Path(p).suffix
            new_path = f"{new_name}_renamed{suffix}"
            SPClient.rename(p, new_path)
            assert SPClient.exists((Path(parent / new_path)).as_posix())
            assert not SPClient.exists(p)


def test_copy(sp_structure, setup_structure, SPClient):
    origin = SHP_TEST_ROOT/"uploaded"
    dest_copy = SHP_TEST_ROOT / "copied"

    for p in SPClient.listdir(origin):
        dest_c = (dest_copy / Path(p).name).as_posix()
        SPClient.copy(p, dest_c)
        assert SPClient.exists(dest_c)


def test_move(sp_structure, setup_structure, SPClient):
    origin = SHP_TEST_ROOT/"uploaded"
    dest_move = SHP_TEST_ROOT / "moved"

    for p in SPClient.listdir(origin):
        dest_m = (dest_move / Path(p).name).as_posix()
        SPClient.move(p, dest_m)
        assert SPClient.exists(dest_m)
        assert not SPClient.exists(p)


def test_delete(sp_structure, setup_structure, SPClient):
    origin = SHP_TEST_ROOT / "uploaded"
    for p in SPClient.listdir(origin):
        SPClient.remove(p)
        assert not SPClient.exists(p)




