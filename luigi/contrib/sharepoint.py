import typing
from pathlib import Path
import logging
import ntpath
import os
import random
import tempfile
import time
from contextlib import contextmanager
from functools import wraps
from urllib.parse import urlparse
from collections import namedtuple
from dataclasses import dataclass
from io import BytesIO


#sharepoint_obj = namedtuple("sharepoint_obj", ["path", "type", "obj", "exists"])

@dataclass
class ShpObj:
    type: str
    exists: bool
    path: str = None
    obj: typing.Any = None

    def __post_init__(self):
        if self.obj:
            try:
                self.path = self.obj.properties["ServerRelativeUrl"]
            except KeyError:
                pass



import luigi.format
from luigi.target import FileSystem, FileSystemTarget, AtomicLocalFile

logger = logging.getLogger('luigi-interface')

try:
    from office365.runtime.auth.client_credential import ClientCredential
    from office365.sharepoint.client_context import ClientContext
except ImportError:
    logger.warning("Loading SharePoint module without required package `Office365-REST-Python-Client`. "
                   "Will crash at runtime if SharePoint functionality is used.")


def _as_parts(path):
    return [p for p in path.split("/") if p]


def _safe_url(trim_site: bool = False):
    def inner(func):
        def wrapper(self, path, *args, **kwargs):
            site_parts = [
                str(p) for p in urlparse(self.conn.base_url).path.split("/") if p
            ]
            rel_parts_input = _as_parts(path)
            if rel_parts_input[:2] == site_parts:
                rel_parts_only = rel_parts_input[2:]
            else:
                rel_parts_only = rel_parts_input

            if trim_site:
                path = "/".join(rel_parts_only)
            else:
                path = "/".join(site_parts + rel_parts_only)

            path = "/" + path
            logger.info(f"Decorated path is now: {path}")
            return func(self, path, *args, **kwargs)
        return wrapper
    return inner


class SharepointClient(FileSystem):
    """
    SharePoint client for authentication, will be used by :py:class:`SharepointTarget` class.
    """
    def __init__(self, site_url: str, api_id: str, api_key: str):
        client_credentials = ClientCredential(api_id, api_key)

        self.conn = ClientContext(site_url).with_credentials(client_credentials)

    @_safe_url(trim_site=False)
    def _get_path_type(self, path):
        try:
            dir = self.conn.web.get_folder_by_server_relative_path(path).select(["Exists"]).get().execute_query()
            if dir.exists:
                return ShpObj(exists=True, obj=dir, type="folder")

            file = self.conn.web.get_file_by_server_relative_path(path).select(["Exists"]).get().execute_query()
            if file.exists:
                return ShpObj(exists=True, obj=file, type="file")
            else:
                return ShpObj(exists=False, obj=file, type="file")
        except Exception as e:
            return ShpObj(exists=False, obj=None, type=None)

    def exists(self, path):
        spo = self._get_path_type(path)
        return spo.exists

    def isdir(self, path):
        spo = self._get_path_type(path)
        if spo.exists:
            return spo.type == "folder"
        else:
            logger.warning(f"Unable to determine type of path as  `{path}` does not exist.")
            return False

    @_safe_url(trim_site=False)
    def listdir(self, path, **kwargs):
        def enum_folder(parent_folder, recursive: bool = False):
            """Expand parent folder and yield files and folders. Folders will be scanned recursively if needed."""
            parent_folder.expand(["Files", "Folders"]).get().execute_query()
            for file in parent_folder.files:  ## type: File
                yield ShpObj(exists=True, obj=file, type="file")
            for folder in parent_folder.folders:
                yield ShpObj(exists=True, obj=folder, type="folder")
            if recursive:
                for folder in parent_folder.folders:  ## type: Folder
                    yield ShpObj(exists=True, obj=folder, type="folder")
                    yield from enum_folder(folder, recursive=recursive)

        spo = self._get_path_type(path)
        if not spo.exists or spo.type == "file":
            return None
        root_folder = self.conn.web.get_folder_by_server_relative_path(path)
        listed = set()
        for i in enum_folder(root_folder, recursive=kwargs.get("recursive", True)):
            if kwargs.get("what", "all") == "all":
                listed.add(i.path)
            else:
                if i.type == kwargs.get("what"):
                    listed.add(i.path)
        return sorted(list(listed))

    @_safe_url(trim_site=True)
    def mkdir(self, path, parents=True, raise_if_exists=False):
        tf = self.conn.web.ensure_folder_path(path).execute_query()
        return ShpObj(exists=True, obj=tf, type="folder")

    def remove(self, path, recursive=True, skip_trash=True):
        spo = self._get_path_type(path)
        if spo.exists:
            spo.obj.delete_object().execute_query()

    def rename(self, path, new_name):
        spo = self._get_path_type(path)
        spo.obj = spo.obj.rename(new_name).execute_query()
        return spo

    def move(self, path, dest):
        pass

    def copy(self, path, dest):
        pass

    @_safe_url(trim_site=False)
    def download_as_bytes(self, path):
        spo = self._get_path_type(path)
        if not spo.exists:
            raise FileExistsError(f"SP file `{path}` does not exists.")
        if spo.type == "folder":
            raise FileExistsError(f"SP path `{path}` is not a file.")
        if spo.type == "file" and spo.exists:
            with BytesIO() as stream:
                self.conn.web.get_file_by_server_relative_path(path).download(stream).execute_query()
                return stream.getvalue()


    def upload(self, local_path, dest_path, in_session=False):
        is_big = os.path.getsize(local_path) > 100_000_000
        if in_session or is_big:
            self._upload_large_file(local_path=local_path, dest_path=dest_path)
        else:
            self._upload_small_file(local_path=local_path, dest_path=dest_path)

    def _upload_prepare(self, local_path, dest_path):
        local_path = Path(local_path)
        spo = self._get_path_type(dest_path)
        if spo.type == 'file':
            logger.warning("Destination path must be a folder!")
            return
        if not spo.exists:
            spo = self.ensure_path(dest_path)
        return local_path, spo

    def _upload_large_file(self, local_path, dest_path):
        local_path, spo = self._upload_prepare(local_path, dest_path)
        chunk_size = 4_000_000
        file_size = os.path.getsize(local_path)

        def print_progress(offset):
            m = 1_000_000
            print(f"Uploaded {offset/m}MB from {file_size/m}MB...[{offset/file_size:0.0%}]")

        with open(local_path, "rb") as f:
            upl_file = spo.obj.files.create_upload_session(f, chunk_size, print_progress).execute_query()
        assert self.exists(upl_file.properties["ServerRelativeUrl"])

    def _upload_small_file(self, local_path, dest_path):
        local_path, spo = self._upload_prepare(local_path, dest_path)
        with open(local_path, "rb") as f:
            content = f.read()
        upl_file = spo.obj.upload_file(local_path.name, content).execute_query()
        assert self.exists(upl_file.properties["ServerRelativeUrl"])





class SharepointTarget:
    pass




if __name__ == "__main__":
    from pathlib import Path
    from dotenv import load_dotenv

    load_dotenv("C:/apps/_PACKAGES/mluigi/local_testing.env", override=True)

    SITE_URL = os.getenv("SHP_SITE_URL")
    API_ID = os.getenv("SHP_API_ID")
    API_KEY = os.getenv("SHP_API_KEY")

    shpc = SharepointClient(site_url=SITE_URL, api_key=API_KEY, api_id=API_ID)
    stream = shpc.download_as_bytes("/xUnitTests_SHP/test_mluigi/test_exists")

    with open("op.pdf", "wb") as f:
        f.write(stream)


