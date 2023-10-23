import gzip
import logging
import mimetypes
import os
import posixpath
import shutil
from contextlib import closing
from datetime import timezone
from functools import wraps, partial
from io import TextIOBase
from tempfile import SpooledTemporaryFile
from threading import local
from urllib.parse import urljoin, urlsplit, urlunsplit

import boto3
from botocore.client import Config
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
from django.conf import settings
from django.core import checks
from django.core.exceptions import ImproperlyConfigured
from django.core.files.base import File
from django.core.files.storage import Storage
from django.core.signals import setting_changed
from django.utils.deconstruct import deconstructible
from django.utils.encoding import filepath_to_uri, force_bytes, force_str
from django.utils.timezone import make_naive

log = logging.getLogger(__name__)


from dataclasses import dataclass, field, fields


@deconstructible
@dataclass
class Endpoints:
    endpoint_url: str | None = None
    endpoint_url_presigning: str | None = None


def _wrap_errors(func):
    @wraps(func)
    def _do_wrap_errors(self, name, *args, **kwargs):
        try:
            return func(self, name, *args, **kwargs)
        except ClientError as ex:
            code = ex.response.get("Error", {}).get("Code", "Unknown")
            err_cls = OSError
            if code == "NoSuchKey":
                err_cls = FileNotFoundError
            raise err_cls(f"S3Storage error at {name!r}: {force_str(ex)}")

    return _do_wrap_errors


def _callable_setting(value, name):
    return value(name) if callable(value) else value


def _to_sys_path(name):
    return name.replace("/", os.sep)


def _to_posix_path(name):
    return name.replace(os.sep, "/")


def _wrap_path_impl(func):
    @wraps(func)
    def do_wrap_path_impl(self, name, *args, **kwargs):
        # The default implementations of most storage methods assume that system-native paths are used. But we deal with
        # posix paths. We fix this by converting paths to system form, passing them to the default implementation, then
        # converting them back to posix paths.
        return _to_posix_path(func(self, _to_sys_path(name), *args, **kwargs))

    return do_wrap_path_impl


def unpickle_helper(cls, kwargs):
    return cls(**kwargs)


_UNCOMPRESSED_SIZE_META_KEY = "uncompressed_size"


class S3File(File):

    """
    A file returned from Amazon S3.
    """

    def __init__(self, file, name, storage):
        super().__init__(file, name)
        self._storage = storage

    def open(self, mode="rb"):
        if self.closed:
            self.file = self._storage.open(self.name, mode).file
        return super().open(mode)


@dataclass
class Settings:
    # default_auth_settings
    AWS_REGION: str = "us-east-1"
    AWS_ACCESS_KEY_ID: str = ""
    AWS_SECRET_ACCESS_KEY: str = ""
    AWS_SESSION_TOKEN: str = ""
    # default_s3_settings
    AWS_S3_ADDRESSING_STYLE: str = "auto"
    AWS_S3_ENDPOINTS: dict = field(
        default_factory=lambda: {
            's3': Endpoints(),
        }
    )
    AWS_S3_KEY_PREFIX: str = ""
    AWS_S3_MAX_AGE_SECONDS: int = 60 * 60  # 1 hours.
    AWS_S3_REDUCED_REDUNDANCY: bool = False
    AWS_S3_CONTENT_DISPOSITION: str = ""
    AWS_S3_CONTENT_LANGUAGE: str = ""
    AWS_S3_METADATA: dict = field(default_factory=dict)
    AWS_S3_ENCRYPT_KEY: bool = False
    AWS_S3_KMS_ENCRYPTION_KEY_ID: str = ""
    AWS_S3_GZIP: bool = True
    AWS_S3_SIGNATURE_VERSION: str = "s3v4"
    AWS_S3_FILE_OVERWRITE: bool = False
    AWS_S3_USE_THREADS: bool = True
    AWS_S3_MAX_POOL_CONNECTIONS: int = 10
    AWS_S3_CONNECT_TIMEOUT: int = 60  # 60 seconds
    AWS_S3_BUCKET_NAME: str = 'Deprecated'

    @classmethod
    def from_kwargs_and_django_settings(cls, kwargs_settings, django_settings):
        # The name of all settings as a set
        settings_keys = {f.name for f in fields(cls)}

        # From django settings, find the relevant settings by name and create a dictionary of the settings
        django_settings_keys = set(dir(django_settings)).intersection(settings_keys)
        d_settings = {ds: getattr(django_settings, ds) for ds in django_settings_keys}

        # Settings from kwargs, convert keys to uppercase.
        k_settings = {
            k.upper(): v for k, v in kwargs_settings.items() if k.upper() in settings_keys
        }

        # Combine settings from kwargs and Django. kwargs settings should have higher override Django settings.
        merged_settings = d_settings | k_settings

        return cls(**merged_settings)

    @property
    def transfer_config(self):
        return TransferConfig(use_threads=self.AWS_S3_USE_THREADS)

    @property
    def _client_config(self):
        return Config(
            s3={"addressing_style": self.AWS_S3_ADDRESSING_STYLE},
            signature_version=self.AWS_S3_SIGNATURE_VERSION,
            max_pool_connections=self.AWS_S3_MAX_POOL_CONNECTIONS,
            connect_timeout=self.AWS_S3_CONNECT_TIMEOUT,
        )

    def boto3_client_kwargs(self):
        return {
            'config': self._client_config,
            'region_name': self.AWS_REGION,
            'aws_access_key_id': self.AWS_ACCESS_KEY_ID or None,
            'aws_secret_access_key': self.AWS_SECRET_ACCESS_KEY or None,
            'aws_session_token': self.AWS_SESSION_TOKEN or None,
        }


@deconstructible
class S3Storage(Storage):

    """
    An implementation of Django file storage over S3.
    """

    def _setup(self):
        self.settings = Settings.from_kwargs_and_django_settings(self._kwargs_settings, settings)

        self._clients = {}
        self._clients_presigning = {}

        for schema, endpoints in self.settings.AWS_S3_ENDPOINTS.items():
            endpoints: Endpoints = endpoints

            # Primary client
            client_settings = self.settings.boto3_client_kwargs()
            client_settings['endpoint_url'] = endpoints.endpoint_url

            self._clients[schema] = self.session.client('s3', **client_settings)

            # Presigning client. Create a client for presigning
            # if the endpoint_url_presigning is set to something unique.
            # Otherwise, use the same as for the primary client.
            if (
                endpoints.endpoint_url_presigning
                and endpoints.endpoint_url_presigning != endpoints.endpoint_url
            ):
                client_settings = self.settings.boto3_client_kwargs()
                client_settings['endpoint_url'] = endpoints.endpoint_url_presigning
                self._clients_presigning[schema] = self.session.client('s3', **client_settings)
            else:
                self._clients_presigning[schema] = self._clients[schema]

    def s3_client(self, schema: str):
        return self._clients[schema]

    def s3_client_presigning(self, schema: str):
        return self._clients_presigning[schema]

    def _setting_changed_received(self, setting, **kwargs):
        if setting.startswith("AWS_"):
            self._setup()

    def __init__(self, **kwargs):
        # Check for unknown kwargs.
        settings_keys = {f.name for f in fields(Settings)}
        for kwarg_key in kwargs.keys():
            if kwarg_key.upper() not in settings_keys:
                raise ImproperlyConfigured(f"Unknown S3Storage parameters: {kwarg_key}")

        self._kwargs_settings = kwargs

        self.session = boto3.Session()
        self._setup()

        # Re-initialize the storage if an AWS setting changes.
        setting_changed.connect(self._setting_changed_received)

        # All done!
        super().__init__()

    def __reduce__(self):
        return unpickle_helper, (self.__class__, self._kwargs_settings)

    # Helpers.

    def _schema(self, name) -> str:
        url_split = urlsplit(name)
        return url_split.scheme

    def _get_key_name(self, name):
        if name.startswith("/"):
            name = name[1:]
        return posixpath.normpath(
            posixpath.join(self.settings.AWS_S3_KEY_PREFIX, _to_posix_path(name))
        )

    def _object_params(self, name):
        url_split = urlsplit(name)
        assert url_split.netloc and url_split.netloc != ''
        params = {
            "Bucket": url_split.netloc,
            "Key": self._get_key_name(url_split.path),
        }
        return params

    def _object_put_params(self, name):
        # Set basic params.
        params = {
            "CacheControl": "{privacy},max-age={max_age}".format(
                privacy="private",
                max_age=self.settings.AWS_S3_MAX_AGE_SECONDS,
            ),
            "Metadata": {
                key: _callable_setting(value, name)
                for key, value in self.settings.AWS_S3_METADATA.items()
            },
            "StorageClass": "REDUCED_REDUNDANCY"
            if self.settings.AWS_S3_REDUCED_REDUNDANCY
            else "STANDARD",
        }
        params.update(self._object_params(name))
        # Set content disposition.
        content_disposition = _callable_setting(self.settings.AWS_S3_CONTENT_DISPOSITION, name)
        if content_disposition:
            params["ContentDisposition"] = content_disposition
        # Set content langauge.
        content_langauge = _callable_setting(self.settings.AWS_S3_CONTENT_LANGUAGE, name)
        if content_langauge:
            params["ContentLanguage"] = content_langauge
        # Set server-side encryption.
        if self.settings.AWS_S3_ENCRYPT_KEY:  # If this if False / None / empty then no encryption
            if isinstance(self.settings.AWS_S3_ENCRYPT_KEY, str):
                params["ServerSideEncryption"] = self.settings.AWS_S3_ENCRYPT_KEY
                if self.settings.AWS_S3_KMS_ENCRYPTION_KEY_ID:
                    params["SSEKMSKeyId"] = self.settings.AWS_S3_KMS_ENCRYPTION_KEY_ID
            else:
                params["ServerSideEncryption"] = "AES256"
        # All done!
        return params

    def new_temporary_file(self):
        """Returns a new file to use when opening from or saving to S3"""
        return SpooledTemporaryFile(max_size=1024 * 1024 * 10)  # 10 MB.

    @_wrap_errors
    def _open(self, name, mode="rb"):
        if mode != "rb":
            raise ValueError("S3 files can only be opened in read-only mode")
        # Load the key into a temporary file. It would be nice to stream the
        # content, but S3 doesn't support seeking, which is sometimes needed.
        schema = self._schema(name)
        obj = self.s3_client(schema).get_object(**self._object_params(name))
        content = self.new_temporary_file()
        shutil.copyfileobj(obj["Body"], content)
        content.seek(0)
        # Un-gzip if required.
        if obj.get("ContentEncoding") == "gzip":
            content = gzip.GzipFile(name, "rb", fileobj=content)
        # All done!
        return S3File(content, name, self)

    @_wrap_errors
    def _save(self, name, content):
        put_params = self._object_put_params(name)
        temp_files = []
        # The Django file storage API always rewinds the file before saving,
        # therefor so should we.
        content.seek(0)
        # Convert content to bytes.
        if isinstance(content.file, TextIOBase):
            temp_file = self.new_temporary_file()
            temp_files.append(temp_file)
            for chunk in content.chunks():
                temp_file.write(force_bytes(chunk))
            temp_file.seek(0)
            content = temp_file
        # Calculate the content type.
        content_type, _ = mimetypes.guess_type(name, strict=False)
        content_type = content_type or "application/octet-stream"
        put_params["ContentType"] = content_type
        # Calculate the content encoding.
        if self.settings.AWS_S3_GZIP:
            # Check if the content type is compressible.
            content_type_family, content_type_subtype = content_type.lower().split("/")
            content_type_subtype = content_type_subtype.split("+")[-1]
            if content_type_family == "text" or content_type_subtype in (
                "xml",
                "json",
                "html",
                "javascript",
            ):
                # Compress the content.
                temp_file = self.new_temporary_file()
                temp_files.append(temp_file)
                with closing(gzip.GzipFile(name, "wb", 9, temp_file)) as gzip_file:
                    shutil.copyfileobj(content, gzip_file)
                # Only use the compressed version if the zipped version is actually smaller!
                orig_size = content.tell()
                if temp_file.tell() < orig_size:
                    temp_file.seek(0)
                    content = temp_file
                    put_params["ContentEncoding"] = "gzip"
                    put_params["Metadata"][_UNCOMPRESSED_SIZE_META_KEY] = f"{orig_size:d}"
                else:
                    content.seek(0)
        # Save the file.
        # HACK: Patch the file object to prevent `upload_fileobj` from closing it.
        # https://github.com/boto/boto3/issues/929
        original_close = content.close
        content.close = lambda: None
        try:
            schema = self._schema(name)
            self.s3_client(schema).upload_fileobj(
                content,
                put_params.pop('Bucket'),
                put_params.pop('Key'),
                ExtraArgs=put_params,
                Config=self.settings.transfer_config,
            )
        finally:
            # Restore the original close method.
            content.close = original_close
        # Close all temp files.
        for temp_file in temp_files:
            temp_file.close()
        # All done!
        return name

    # Subsiduary storage methods.

    @_wrap_path_impl
    def get_valid_name(self, name):
        return super().get_valid_name(name)

    @_wrap_path_impl
    def get_available_name(self, name, max_length=None):
        if self.settings.AWS_S3_FILE_OVERWRITE:
            return _to_posix_path(name)
        return super().get_available_name(name, max_length=max_length)

    @_wrap_path_impl
    def generate_filename(self, filename):
        url_split = urlsplit(filename)
        path = super().generate_filename(url_split.path)
        return url_split._replace(path=path).geturl()

    @_wrap_errors
    def meta(self, name):
        """Returns a dictionary of metadata associated with the key."""
        schema = self._schema(name)
        return self.s3_client(schema).head_object(**self._object_params(name))

    @_wrap_errors
    def delete(self, name):
        schema = self._schema(name)
        self.s3_client(schema).delete_object(**self._object_params(name))

    @_wrap_errors
    def copy(self, src_name, dst_name):
        schema = self._schema(src_name)
        self.s3_client(schema).copy_object(
            CopySource=self._object_params(src_name), **self._object_params(dst_name)
        )

    @_wrap_errors
    def rename(self, src_name, dst_name):
        self.copy(src_name, dst_name)
        schema = self._schema(src_name)
        self.s3_client(schema).delete_object(**self._object_params(src_name))

    def exists(self, name):
        name = _to_posix_path(name)
        if name.endswith("/"):
            # This looks like a directory, but on S3 directories are virtual, so we need to see if the key starts
            # with this prefix.
            try:
                schema = self._schema(name)
                params = self._object_params(name)
                results = self.s3_client(schema).list_objects_v2(
                    Bucket=params['Bucket'],
                    MaxKeys=1,
                    Prefix=params['Key']
                    + "/",  # Add the slash again, since _get_key_name removes it.
                )
            except ClientError:
                return False
            else:
                return "Contents" in results
        # This may be a file or a directory. Check if getting the file metadata throws an error.
        try:
            self.meta(name)
        except OSError:
            # It's not a file, but it might be a directory. Check again that it's not a directory.
            return self.exists(name + "/")
        else:
            return True

    def listdir(self, path):
        schema = self._schema(path)
        params = self._object_params(path)
        key = params['Key']
        path = "" if key == "." else key + "/"
        # Look through the paths, parsing out directories and paths.
        files = []
        dirs = []
        paginator = self.s3_client(schema).get_paginator("list_objects_v2")
        pages = paginator.paginate(
            Bucket=params['Bucket'],
            Delimiter="/",
            Prefix=path,
        )
        for page in pages:
            for entry in page.get("Contents", ()):
                files.append(posixpath.relpath(entry["Key"], path))
            for entry in page.get("CommonPrefixes", ()):
                dirs.append(posixpath.relpath(entry["Prefix"], path))
        # All done!
        return dirs, files

    def size(self, name):
        meta = self.meta(name)
        try:
            if meta["ContentEncoding"] == "gzip":
                return int(meta["Metadata"]["uncompressed_size"])
        except KeyError:
            return meta["ContentLength"]

    def url(self, name, extra_params=None, client_method="get_object"):
        # Otherwise, generate the URL.
        params = extra_params.copy() if extra_params else {}
        params.update(self._object_params(name))
        schema = self._schema(name)
        url = self.s3_client_presigning(schema).generate_presigned_url(
            ClientMethod=client_method,
            Params=params,
            ExpiresIn=self.settings.AWS_S3_MAX_AGE_SECONDS,
        )
        # All done!
        return url

    def modified_time(self, name):
        return make_naive(self.meta(name)["LastModified"], timezone.utc)

    created_time = accessed_time = modified_time

    def get_modified_time(self, name):
        timestamp = self.meta(name)["LastModified"]
        return timestamp if settings.USE_TZ else make_naive(timestamp)

    get_created_time = get_accessed_time = get_modified_time
