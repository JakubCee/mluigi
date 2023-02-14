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
    def ensure_path(self, path):
        tf = self.conn.web.ensure_folder_path(path).execute_query()
        return ShpObj(exists=True, obj=tf, type="folder")

    def remove(self, path, recursive=True, skip_trash=True):
        spo = self._get_path_type(path)
        if spo.exists:
            spo.obj.delete_object().execute_query()

    def move(self, path, dest):
        pass

    def copy(self, path, dest):
        pass

    def download_as_bytes(self, path):
        pass

    def upload(self, local_path, dest_path):
        pass

    def _upload_large_file(self, local_path, dest_path):
        pass

    def _upload_small_file(self, local_path, dest_path):
        pass





class SharepointTarget:
    pass




if __name__ == "__main__":
    import pytest
    from pathlib import Path
    from dotenv import load_dotenv

    load_dotenv("C:/apps/_PACKAGES/mluigi/local_testing.env", override=True)

    SITE_URL = os.getenv("SHP_SITE_URL")
    API_ID = os.getenv("SHP_API_ID")
    API_KEY = os.getenv("SHP_API_KEY")


    shpc = SharepointClient(site_url=SITE_URL, api_key=API_KEY, api_id=API_ID)

    p = shpc.listdir("/xUnitTests_SHP/test_mluigi", recursive=True, what="folder")
    print(p)
    #print(shpc.listdir("xUnitTests_SHP/test_mluigi/test_exists"))
