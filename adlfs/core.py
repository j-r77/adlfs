# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function

import asyncio
import logging
from typing import AsyncIterator, TypeVar, List

from azure.core.exceptions import ResourceNotFoundError
from azure.core.async_paging import AsyncItemPaged
from azure.datalake.store import AzureDLFileSystem, lib
from azure.datalake.store.core import AzureDLFile, AzureDLPath
from azure.storage.blob._shared.base_client import create_configuration
from azure.storage.blob._models import BlobBlock
from azure.storage.blob.aio._models import BlobPrefix
from azure.storage.blob.aio import BlobServiceClient
from fsspec import AbstractFileSystem
from fsspec.spec import AbstractBufferedFile
from fsspec.utils import infer_storage_options, tokenize

logger = logging.getLogger(__name__)

T = TypeVar("T")

class AzureDatalakeFileSystem(AbstractFileSystem):
    """
    Access Azure Datalake Gen1 as if it were a file system.

    This exposes a filesystem-like API on top of Azure Datalake Storage

    Parameters
    -----------
    tenant_id:  string
        Azure tenant, also known as the subscription id
    client_id: string
        The username or serivceprincipal id
    client_secret: string
        The access key
    store_name: string (optional)
        The name of the datalake account being accessed.  Should be inferred from the urlpath
        if using with Dask read_xxx and to_xxx methods.

    Examples
    --------
    >>> adl = AzureDatalakeFileSystem(tenant_id="xxxx", client_id="xxxx",
    ...                               client_secret="xxxx")

    >>> adl.ls('')

    Sharded Parquet & CSV files can be read as

    >>> storage_options = dict(tennant_id=TENNANT_ID, client_id=CLIENT_ID,
    ...                        client_secret=CLIENT_SECRET)  # doctest: +SKIP
    >>> ddf = dd.read_parquet('adl://store_name/folder/filename.parquet',
    ...                       storage_options=storage_options)  # doctest: +SKIP

    >>> ddf = dd.read_csv('adl://store_name/folder/*.csv'
    ...                   storage_options=storage_options)  # doctest: +SKIP


    Sharded Parquet and CSV files can be written as

    >>> ddf.to_parquet("adl://store_name/folder/filename.parquet",
    ...                storage_options=storage_options)  # doctest: +SKIP

    >>> ddf.to_csv('adl://store_name/folder/*.csv'
    ...            storage_options=storage_options)  # doctest: +SKIP
    """

    protocol = "adl"

    def __init__(self, tenant_id, client_id, client_secret, store_name):
        super().__init__()
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.store_name = store_name
        self.do_connect()

    @staticmethod
    def _get_kwargs_from_urls(paths):
        """ Get the store_name from the urlpath and pass to storage_options """
        ops = infer_storage_options(paths)
        out = {}
        if ops.get("host", None):
            out["store_name"] = ops["host"]
        return out

    @classmethod
    def _strip_protocol(cls, path):
        ops = infer_storage_options(path)
        return ops["path"]

    def do_connect(self):
        """Establish connection object."""
        token = lib.auth(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )
        self.azure_fs = AzureDLFileSystem(token=token, store_name=self.store_name)

    def ls(self, path, detail=False, invalidate_cache=True, **kwargs):
        files = self.azure_fs.ls(
            path=path, detail=detail, invalidate_cache=invalidate_cache
        )

        for file in files:
            if "type" in file and file["type"] == "DIRECTORY":
                file["type"] = "directory"

        return files

    def info(self, path, invalidate_cache=True, expected_error_code=404, **kwargs):
        info = self.azure_fs.info(
            path=path,
            invalidate_cache=invalidate_cache,
            expected_error_code=expected_error_code,
        )
        info["size"] = info["length"]
        return info

    def _trim_filename(self, fn, **kwargs):
        """ Determine what kind of filestore this is and return the path """
        so = infer_storage_options(fn)
        fileparts = so["path"]
        return fileparts

    def glob(self, path, details=False, invalidate_cache=True, **kwargs):
        """For a template path, return matching files"""
        adlpaths = self._trim_filename(path)
        filepaths = self.azure_fs.glob(
            adlpaths, details=details, invalidate_cache=invalidate_cache
        )
        return filepaths

    def isdir(self, path, **kwargs):
        """Is this entry directory-like?"""
        try:
            return self.info(path)["type"].lower() == "directory"
        except FileNotFoundError:
            return False

    def isfile(self, path, **kwargs):
        """Is this entry file-like?"""
        try:
            return self.azure_fs.info(path)["type"].lower() == "file"
        except Exception:
            return False

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs,
    ):
        return AzureDatalakeFile(self, path, mode=mode)

    def read_block(self, fn, offset, length, delimiter=None, **kwargs):
        return self.azure_fs.read_block(fn, offset, length, delimiter)

    def ukey(self, path):
        return tokenize(self.info(path)["modificationTime"])

    def size(self, path):
        return self.info(path)["length"]

    def __getstate__(self):
        dic = self.__dict__.copy()
        logger.debug("Serialize with state: %s", dic)
        return dic

    def __setstate__(self, state):
        logger.debug("De-serialize with state: %s", state)
        self.__dict__.update(state)
        self.do_connect()


class AzureDatalakeFile(AzureDLFile):
    # TODO: refoctor this. I suspect we actually want to compose an
    # AbstractBufferedFile with an AzureDLFile.

    def __init__(
        self,
        fs,
        path,
        mode="rb",
        autocommit=True,
        block_size=2 ** 25,
        cache_type="bytes",
        cache_options=None,
        *,
        delimiter=None,
        **kwargs,
    ):
        super().__init__(
            azure=fs.azure_fs,
            path=AzureDLPath(path),
            mode=mode,
            blocksize=block_size,
            delimiter=delimiter,
        )
        self.fs = fs
        self.path = AzureDLPath(path)
        self.mode = mode

    def seek(self, loc, whence=0, **kwargs):
        """ Set current file location

        Parameters
        ----------
        loc: int
            byte location
        whence: {0, 1, 2}
            from start of file, current location or end of file, resp.
        """
        loc = int(loc)
        if not self.mode == "rb":
            raise ValueError("Seek only available in read mode")
        if whence == 0:
            nloc = loc
        elif whence == 1:
            nloc = self.loc + loc
        elif whence == 2:
            nloc = self.size + loc
        else:
            raise ValueError("invalid whence (%s, should be 0, 1 or 2)" % whence)
        if nloc < 0:
            raise ValueError("Seek before start of file")
        self.loc = nloc
        return self.loc


class AzureBlobFileSystem(AbstractFileSystem):
    """
    Access Azure Datalake Gen2 and Azure Storage if it were a file system using Multiprotocol Access

    Parameters
    ----------
    account_name:
        The storage account name. This is used to authenticate requests
        signed with an account key and to construct the storage endpoint. It
        is required unless a connection string is given, or if a custom
        domain is used with anonymous authentication.
    container: str,
        The container name.  Required to create a filesystem client.
    account_key:
        The storage account key. This is used for shared key authentication.
        If any of account key, sas token or client_id is specified, anonymous access
        will be used.
    sas_token:
        A shared access signature token to use to authenticate requests
        instead of the account key. If account key and sas token are both
        specified, account key will be used to sign. If any of account key, sas token
        or client_id are specified, anonymous access will be used.
    is_emulated:
        Whether to use the emulator. Defaults to False. If specified, will
        override all other parameters besides connection string and request
        session.
    protocol:
        The protocol to use for requests. Defaults to https.
    endpoint_suffix:
        The host base component of the url, minus the account name. Defaults
        to Azure (core.windows.net). Override this to use the China cloud
        (core.chinacloudapi.cn).
    custom_domain:
        The custom domain to use. This can be set in the Azure Portal. For
        example, 'www.mydomain.com'.
    request_session:
        The session object to use for http requests.
    connection_string:
        If specified, this will override all other parameters besides
        request session. See
        http://azure.microsoft.com/en-us/documentation/articles/storage-configure-connection-string/
        for the connection string format.
    socket_timeout:
        If specified, this will override the default socket timeout. The timeout specified is in seconds.
        See DEFAULT_SOCKET_TIMEOUT in _constants.py for the default value.
    token_credential:
        A token credential used to authenticate HTTPS requests. The token value
        should be updated before its expiration.
    blocksize:
        The block size to use for download/upload operations. Defaults to the value of
        ``BlockBlobService.MAX_BLOCK_SIZE``
    client_id:
        Client ID to use when authenticating using an AD Service Principal client/secret.
    client_secret:
        Client secret to use when authenticating using an AD Service Principal client/secret.
    tenant_id:
        Tenant ID to use when authenticating using an AD Service Principal client/secret.

    Examples
    --------
    >>> abfs = AzureBlobFileSystem(account_name="XXXX", account_key="XXXX", container_name="XXXX")
    >>> adl.ls('')

    **  Sharded Parquet & csv files can be read as: **
        ------------------------------------------
        ddf = dd.read_csv('abfs://container_name/folder/*.csv', storage_options={
        ...    'account_name': ACCOUNT_NAME, 'account_key': ACCOUNT_KEY})

        ddf = dd.read_parquet('abfs://container_name/folder.parquet', storage_options={
        ...    'account_name': ACCOUNT_NAME, 'account_key': ACCOUNT_KEY,})
    """

    protocol = "abfs"

    def __init__(
        self,
        account_name: str,
        account_key: str = None,
        connection_string: str = None,
        credential: str = None,
        sas_token: str = None,
        request_session=None,
        socket_timeout=None,
        token_credential=None,
        blocksize=create_configuration(storage_sdk="blob").max_block_size,
        client_id=None,
        client_secret=None,
        tenant_id=None,
    ):
        AbstractFileSystem.__init__(self)
        self.account_name = account_name
        self.account_key = account_key
        self.connection_string = connection_string
        self.credential = credential
        self.sas_token = sas_token
        self.request_session = request_session
        self.socket_timeout = socket_timeout
        self.token_credential = token_credential
        self.blocksize = blocksize
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id

        if (
            self.token_credential is None
            and self.account_key is None
            and self.sas_token is None
            and self.client_id is not None
        ):
            self.token_credential = self._get_token_from_service_principal()
        self.do_connect()

    @classmethod
    def _strip_protocol(cls, path):
        logging.debug(f"_strip_protocol for {path}")
        ops = infer_storage_options(path)

        # we need to make sure that the path retains
        # the format {host}/{path}
        # here host is the container_name
        if ops.get("host", None):
            ops["path"] = ops["host"] + ops["path"]
        ops["path"] = ops["path"].lstrip("/")

        logging.debug(f"_strip_protocol({path}) = {ops}")
        return ops["path"]

    def _get_token_from_service_principal(self):
        from azure.common.credentials import ServicePrincipalCredentials
        from azure.storage.common import TokenCredential

        sp_cred = ServicePrincipalCredentials(
            client_id=self.client_id,
            secret=self.client_secret,
            tenant=self.tenant_id,
            resource="https://storage.azure.com/",
        )

        token_cred = TokenCredential(sp_cred.token["access_token"])
        return token_cred

    def do_connect(self):
        self.account_url: str = f"https://{self.account_name}.blob.core.windows.net"
        if self.credential is not None:
            self.service_client = BlobServiceClient(
                account_url=self.account_url, credential=self.credential
            )
        elif self.connection_string is not None:
            self.service_client = BlobServiceClient.from_connection_string(
                conn_str=self.connection_string
            )
        elif self.account_key is not None:
            self.service_client = BlobServiceClient(
                account_url=self.account_url, credential=self.account_key,
            )
        else:
            raise ValueError("unable to connect with provided params!!")

    def split_path(self, path, delimiter="/", return_container: bool = False, **kwargs):
        """
        Normalize ABFS path string into bucket and key.
        Parameters
        ----------
        path : string
            Input path, like `abfs://my_container/path/to/file`
        Examples
        --------
        >>> split_path("abfs://my_container/path/to/file")
        ['my_container', 'path/to/file']
        """

        if path in ["", delimiter]:
            return "", ""
        if path.endswith(delimiter):
            path = path.rstrip(delimiter)
        path = self._strip_protocol(path)
        path = path.lstrip(delimiter)
        if "/" not in path:
            # this means path is the container_name
            return path, ""
        else:
            return path.split(delimiter, 1)

    async def _async_list_containers(self, client, include_metadata: bool = False):
        return client.list_containers(include_metadata=include_metadata)
    
    async def _async_walk_blobs(self, client, name_starts_with, result=None, path=None, glob: bool = False, detail: bool = False):
        if result is None:
            return client.walk_blobs(name_starts_with=name_starts_with)
        else:
            blobs = client.walk_blobs(name_starts_with=name_starts_with)
            async for blob in blobs:
                print(f"blob:  {blob}")
                if isinstance(blob, BlobPrefix) and (blob.name != path) and (blob.name == f"{path}/"):
                    await self._async_walk_blobs(client, name_starts_with=blob.name, result=result, 
                                                path=path, glob = glob, detail=detail)
                else:
                    if detail:
                        blob_details = await self._details(blob, glob = glob)
                        result.append(blob_details)
                    else:
                        result.append(f"{blob.container}/{blob.name}")
            if not result:
                raise FileNotFoundError(f"File {path} was not found!!")
            print(f"Returning from walk blobs... {result}")
            return result


    async def exists(self, path):
        """Is there a file at the given path"""
        try:
            await self.info(path)
            return True
        except:  # noqa: E722
            # any exception allowed bar FileNotFoundError?
            return False


    async def info(self, path, glob: bool = False, **kwargs):
        """Give details of entry at path

        Returns a single dictionary, with exactly the same information as ``ls``
        would with ``detail=True``.

        The default implementation should calls ls and could be overridden by a
        shortcut. kwargs are passed on to ```ls()``.

        Some file systems might not be able to measure the file's size, in
        which case, the returned dict will include ``'size': None``.

        Returns
        -------
        dict with keys: name (full path in the FS), size (in bytes), type (file,
        directory, or something else) and other FS-specific keys.
        """
        print(f"in info...")
        print(path)
        print(self._parent(path))
        out = await self.ls(self._parent(path), detail=True, glob = glob, **kwargs)
        print(f'returned out... {out}')
        out = [o for o in out if o["name"].rstrip("/") == path]
        print(f"New out:  {out}")
        if out:
            return out[0]
        print(f"new run of out...")
        out = await self.ls(path, detail=True, glob = glob, **kwargs)
        print(f"Returned out again.. {out}")
        path = path.rstrip("/")
        out1 = [o for o in out if o["name"].rstrip("/") == path]
        if len(out1) == 1:
            if "size" not in out1[0]:
                out1[0]["size"] = None
            return out1[0]
        elif len(out1) > 1 or out:
            return {"name": path, "size": 0, "type": "directory"}
        else:
            raise FileNotFoundError(path)
    
    async def size(self, path):
        """Size in bytes of file"""
        result = await self.info(path).get("size", None)
        return result

    async def isdir(self, path):
        """Is this entry directory-like?"""
        try:
            result = await self.info(path)
            return result["type"] == "directory"
        except IOError:
            return False

    async def isfile(self, path):
        """Is this entry file-like?"""
        try:
            result = await self.info(path)
            return result["type"] == "file"
        except:  # noqa: E722
            return False

    async def find(self, path, maxdepth=None, withdirs=False, glob: bool = False, **kwargs):
        """List all files below path.
        Like posix ``find`` command without conditions
        Parameters
        ----------
        path : str
        maxdepth: int or None
            If not None, the maximum number of levels to descend
        withdirs: bool
            Whether to include directory paths in the output. This is True
            when used by glob, but users usually only want files.
        kwargs are passed to ``ls``.
        """
        # TODO: allow equivalent of -name parameter
        print("find...")
        path = self._strip_protocol(path)
        out = dict()
        detail = kwargs.pop("detail", False)
        async for path, dirs, files in self.walk(path, maxdepth, detail=True, glob=glob, **kwargs):
            if withdirs:
                files.update(dirs)
            out.update({info["name"]: info for name, info in files.items()})
        if await self.isfile(path) and path not in out:
            # walk works on directories, but find should also return [path]
            # when path happens to be a file
            out[path] = {}
        names = sorted(out)
        if not detail:
            return names
        else:
            return {name: out[name] for name in names}

        
    async def ls(self,
        path: str,
        detail: bool = False,
        invalidate_cache: bool = True,
        delimiter: str = "/",
        glob: bool = False,
        **kwargs,
    ):
        """
        Create a list of blob names from a blob container

        Parameters
        ----------
        path:  Path to an Azure Blob with its container name
        detail:  If False, return a list of blob names, else a list of dictionaries with blob details
        invalidate_cache:  Boolean
        """

        logging.debug(f"abfs.ls() is searching for {path}")
        print(f'In ls:  {path}')
        container, path = self.split_path(path)
        print(f'Container, path:  {container}, {path}')
        try:
            if (container in ["", delimiter]) and (path in ["", delimiter]):
                # This is the case where only the containers are being returned
                # print(
                #     "Returning a list of containers in the azure blob storage account"
                # )
                contents = await self._async_list_containers(self.service_client)
                # print('Have containers...')
                if detail:
                        outdetails = []
                        async for c in contents:
                            print('async for running')
                            result = await self._details(c, glob = glob)
                            outdetails.append(result)
                        return outdetails
                else:
                    return [f"{c.name}{delimiter}" async for c in contents]

            elif (container not in ["", delimiter]) and (path in ["", delimiter]):
                # This is the case where the container name is passed
                container_client = self.service_client.get_container_client(
                    container=container
                )
                blobs = await self._async_walk_blobs(container_client, name_starts_with=path, glob=glob)
                result = []
                async for blob in blobs:
                    if detail:
                        r = await self._details(blob, glob = glob)
                        result.append(r)
                    else:
                        result.append(f"{blob.container}{delimiter}{blob.name}")
                return result
            else:
                print("Fetch container client...")
                container_client = self.service_client.get_container_client(
                    container=container
                )
                result = []
                blobs = await self._async_walk_blobs(container_client, name_starts_with=path, glob=glob, result=result, path=path, detail=detail)
                return blobs
                    
        except Exception as e:
            raise FileNotFoundError(f"File {path} does not exist.  Failed for {e}")
                    
  
    async def _details(self, content, delimiter="/", glob: bool = False, **kwargs):
        print("get details...")
        data = {}
        print(content)
        if content.has_key("container"):  # NOQA
            data["name"] = f"{content.container}{delimiter}{content.name}"
            if content.has_key("size"):  # NOQA
                data["size"] = content.size
            else:
                data["size"] = 0
            if data["size"] == 0:
                data["type"] = "directory"
            else:
                data["type"] = "file"
            if data['type'] == 'directory':
                if not data['name'].endswith(delimiter):
                    data['name'] = data['name'] + delimiter
        else:
            data["name"] = f"{content.name}{delimiter}"
            data["size"] = 0
            data["type"] = "directory"
        if glob:
            print(f"Glob is true for {data}")
            if data['type'] == "directory":
                data["name"] = data["name"].rstrip(delimiter)
        return data

    async def glob(self, path, **kwargs):
        """
        Find files by glob-matching.
        If the path ends with '/' and does not contain "*", it is essentially
        the same as ``ls(path)``, returning only files.
        We support ``"**"``,
        ``"?"`` and ``"[..]"``.
        kwargs are passed to ``ls``.
        """
        import re
        from glob import has_magic

        print("glob")
        ends = path.endswith("/")
        print(ends)
        path = self._strip_protocol(path)
        indstar = path.find("*") if path.find("*") >= 0 else len(path)
        indques = path.find("?") if path.find("?") >= 0 else len(path)
        indbrace = path.find("[") if path.find("[") >= 0 else len(path)

        ind = min(indstar, indques, indbrace)

        detail = kwargs.pop("detail", False)

        if not has_magic(path):
            root = path
            depth = 1
            if ends:
                path += "/*"
            elif await self.exists(path):
                if not detail:
                    return [path]
                else:
                    out = await self.info(path, glob = True)
                    return {path: out}
            else:
                if not detail:
                    return []  # glob of non-existent returns empty
                else:
                    return {}
        elif "/" in path[:ind]:
            ind2 = path[:ind].rindex("/")
            root = path[: ind2 + 1]
            depth = 20 if "**" in path else path[ind2 + 1 :].count("/") + 1
        else:
            root = ""
            depth = 20 if "**" in path else 1

        print(f"finding {root}")
        allpaths = await self.find(root, maxdepth=depth, withdirs=True, detail=True, glob=True, **kwargs)
        pattern = (
            "^"
            + (
                path.replace("\\", r"\\")
                .replace(".", r"\.")
                .replace("+", r"\+")
                .replace("//", "/")
                .replace("(", r"\(")
                .replace(")", r"\)")
                .replace("|", r"\|")
                .rstrip("/")
                .replace("?", ".")
            )
            + "$"
        )
        pattern = re.sub("[*]{2}", "=PLACEHOLDER=", pattern)
        pattern = re.sub("[*]", "[^/]*", pattern)
        pattern = re.compile(pattern.replace("=PLACEHOLDER=", ".*"))
        out = {
            p: allpaths[p]
            for p in sorted(allpaths)
            if pattern.match(p.replace("//", "/").rstrip("/"))
        }
        if detail:
            return out
        else:
            return list(out)

    async def walk(self, path, maxdepth=None, glob: bool = False, **kwargs):
        """ Return all files belows path

        List all files, recursing into subdirectories; output is iterator-style,
        like ``os.walk()``. For a simple list of files, ``find()`` is available.

        Note that the "files" outputted will include anything that is not
        a directory, such as links.

        Parameters
        ----------
        path: str
            Root to recurse into
        maxdepth: int
            Maximum recursion depth. None means limitless, but not recommended
            on link-based file-systems.
        kwargs: passed to ``ls``
        """
        path = self._strip_protocol(path)
        full_dirs = {}
        dirs = {}
        files = {}

        detail = kwargs.pop("detail", False)
        try:
            listing = await self.ls(path, detail=True, glob=glob, **kwargs)
        except (FileNotFoundError, IOError):
            yield [], [], []

        for info in listing:
            # each info name must be at least [path]/part , but here
            # we check also for names like [path]/part/
            pathname = info["name"].rstrip("/")
            name = pathname.rsplit("/", 1)[-1]
            if info["type"] == "directory" and pathname != path:
                # do not include "self" path
                full_dirs[pathname] = info
                dirs[name] = info
            elif pathname == path:
                # file-like with same name as give path
                files[""] = info
            else:
                files[name] = info

        if detail:
            yield path, dirs, files
        else:
            yield path, list(dirs), list(files)

        if maxdepth is not None:
            maxdepth -= 1
            if maxdepth < 1:
                return

        for d in full_dirs:
            await self.walk(d, maxdepth=maxdepth, detail=detail, glob=glob, **kwargs)

    def mkdir(self, path, delimiter="/", exists_ok=False, **kwargs):
        container_name, path = self.split_path(path, delimiter=delimiter)
        if not exists_ok:
            if (container_name not in self.ls("")) and (not path):
                # create new container
                self.service_client.create_container(name=container_name)
            elif (container_name in self.ls("")) and path:
                ## attempt to create prefix
                container_client = self.service_client.get_container_client(
                    container=container_name
                )
                container_client.upload_blob(name=path, data="")
            else:
                ## everything else
                raise RuntimeError(f"Cannot create {container_name}{delimiter}{path}.")
        else:
            if container_name in self.ls("") and path:
                container_client = self.service_client.get_container_client(
                    container=container_name
                )
                container_client.upload_blob(name=path, data="")

    def rmdir(self, path, delimiter="/", **kwargs):
        container_name, path = self.split_path(path, delimiter=delimiter)
        if (container_name + delimiter in self.ls("")) and (not path):
            # delete container
            self.service_client.delete_container(container_name)

    def _rm(self, path, delimiter="/", **kwargs):
        if self.isfile(path):
            container_name, path = self.split_path(path, delimiter=delimiter)
            container_client = self.service_client.get_container_client(
                container=container_name
            )
            logging.debug(f"Delete blob {path} in {container_name}")
            container_client.delete_blob(path)
        elif self.isdir(path):
            container_name, path = self.split_path(path, delimiter=delimiter)
            container_client = self.service_client.get_container_client(
                container=container_name
            )
            if (container_name + delimiter in self.ls("")) and (not path):
                logging.debug(f"Delete container {container_name}")
                container_client.delete_container(container_name)
        else:
            raise RuntimeError(f"cannot delete {path}")

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs,
    ):
        """ Open a file on the datalake, or a block blob """
        logging.debug(f"_open:  {path}")
        return AzureBlobFile(
            fs=self,
            path=path,
            mode=mode,
            block_size=block_size or self.blocksize,
            autocommit=autocommit,
            cache_options=cache_options,
            **kwargs,
        )


class AzureBlobFile(AbstractBufferedFile):
    """ File-like operations on Azure Blobs """

    def __init__(
        self,
        fs,
        path,
        mode="rb",
        block_size="default",
        autocommit=True,
        cache_type="readahead",
        cache_options=None,
        **kwargs,
    ):
        container_name, blob = fs.split_path(path)
        self.container_name = container_name
        self.blob = blob
        self.container_client = fs.service_client.get_container_client(
            self.container_name
        )

        super().__init__(
            fs=fs,
            path=path,
            mode=mode,
            block_size=block_size,
            autocommit=autocommit,
            cache_type=cache_type,
            cache_options=cache_options,
            **kwargs,
        )

    def _fetch_range(self, start, end, **kwargs):
        blob = self.container_client.download_blob(
            blob=self.blob, offset=start, length=end,
        )
        return blob.readall()

    def _initiate_upload(self, **kwargs):
        self.blob_client = self.container_client.get_blob_client(blob=self.blob)
        self._block_list = []
        try:
            self.container_client.delete_blob(self.blob)
        except ResourceNotFoundError:
            pass
        except Exception as e:
            raise (f"Failed for {e}")
        else:
            return super()._initiate_upload()

    def _upload_chunk(self, final=False, **kwargs):
        data = self.buffer.getvalue()
        length = len(data)
        block_id = len(self._block_list)
        block_id = f"{block_id:07d}"
        self.blob_client.stage_block(
            block_id=block_id, data=data, length=length,
        )
        self._block_list.append(block_id)

        if final:
            block_list = [BlobBlock(_id) for _id in self._block_list]
            self.blob_client.commit_block_list(block_list=block_list,)
