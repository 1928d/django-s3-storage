django-s3-storage
=================

**django-s3-storage** provides a Django Amazon S3 file storage.
**!! Fork for 1928 Diagnostics !!**


Features
--------

- Django file storage for Amazon S3.
- Django static file storage for Amazon S3.
- Works in Python 3!

Customized for 1928 Diagnostics
----------------------------------------------------------

This fork of the django-s3-storage has been adapted to meet the specific needs of 1928 Diagnostics. The primary difference is its utilization of full S3 URLs in place of relative paths. This approach allows for dynamic configuration of the bucket, enabling support for multiple buckets within a single field.

**Key Features:**

- Full S3 URLs: Support for multiple buckets for the same field since the bucket is sorted in the database instead of being a static configuration.
- Support for Multiple S3 Backends: Multiple S3 backends are supported, determined by the scheme-part of the URL (e.g., s3://, s3-minio://).
- Read-only mode: File can be protected.
- Streamlined for 1928 Diagnostics: Features that were not required for 1928 Diagnostics have been removed.

Authentication settings
-----------------------

Use the following settings to authenticate with Amazon AWS.

.. code:: python

    # The AWS region to connect to.
    AWS_REGION = "us-east-1"

    # The AWS access key to use.
    AWS_ACCESS_KEY_ID = ""

    # The AWS secret access key to use.
    AWS_SECRET_ACCESS_KEY = ""

    # The optional AWS session token to use.
    AWS_SESSION_TOKEN = ""


File storage settings
---------------------

Use the following settings to configure the S3 file storage. You must provide at least ``AWS_S3_BUCKET_NAME``.

.. code:: python

    # The name of the bucket to store files in. Deprecated, will be removed in the future.
    # Some migration have this configuration, so they need to be squashed before we can remove it.
    AWS_S3_BUCKET_NAME = "Deprecated"

    # How to construct S3 URLs ("auto", "path", "virtual").
    AWS_S3_ADDRESSING_STYLE = "auto"

    # A dictionary of S3 endpoints. The key should be the scheme of the URL. e.g. 's3' for 's3://' or 's3-minio' for 's3-minio://'.
    # Each endpoint can have an additional URL for presigning. 
    # This is useful when the internal and external hostname is not the same. For example, s3-minio://minio:9000 and s3-minio://localhost:9000
    # Leave as None for default region URL
    AWS_S3_ENDPOINTS={'s3': {endpoint_url: None, endpoint_url_presigning: None}}

    # A prefix to be applied to every stored file. This will be joined to every filename using the "/" separator.
    AWS_S3_KEY_PREFIX = ""

    # How long generated URLs are valid for. This affects the expiry of authentication tokens if `AWS_S3_BUCKET_AUTH`
    # is True. It also affects the "Cache-Control" header of the files.
    # Important: Changing this setting will not affect existing files.
    AWS_S3_MAX_AGE_SECONDS = 60 * 60  # 1 hours.

    # If True, then files will be stored with reduced redundancy. Check the S3 documentation and make sure you
    # understand the consequences before enabling.
    # Important: Changing this setting will not affect existing files.
    AWS_S3_REDUCED_REDUNDANCY = False

    # The Content-Disposition header used when the file is downloaded. This can be a string, or a function taking a
    # single `name` argument.
    # Important: Changing this setting will not affect existing files.
    AWS_S3_CONTENT_DISPOSITION = ""

    # The Content-Language header used when the file is downloaded. This can be a string, or a function taking a
    # single `name` argument.
    # Important: Changing this setting will not affect existing files.
    AWS_S3_CONTENT_LANGUAGE = ""

    # A mapping of custom metadata for each file. Each value can be a string, or a function taking a
    # single `name` argument.
    # Important: Changing this setting will not affect existing files.
    AWS_S3_METADATA = {}

    # If True, then files will be stored using AES256 server-side encryption.
    # If this is a string value (e.g., "aws:kms"), that encryption type will be used.
    # Otherwise, server-side encryption is not be enabled.
    # Important: Changing this setting will not affect existing files.
    AWS_S3_ENCRYPT_KEY = False

    # The AWS S3 KMS encryption key ID (the `SSEKMSKeyId` parameter) is set from this string if present.
    # This is only relevant if AWS S3 KMS server-side encryption is enabled (above).
    AWS_S3_KMS_ENCRYPTION_KEY_ID = ""

    # If True, then text files will be stored using gzip content encoding. Files will only be gzipped if their
    # compressed size is smaller than their uncompressed size.
    # Important: Changing this setting will not affect existing files.
    AWS_S3_GZIP = True

    # The signature version to use for S3 requests.
    AWS_S3_SIGNATURE_VERSION = None

    # If True, then files with the same name will overwrite each other. By default it's set to False to have
    # extra characters appended.
    AWS_S3_FILE_OVERWRITE = False

    # If True, use default behaviour for boto3 of using threads when doing S3 operations. If gevent or similar
    # is used it must be disabled
    AWS_S3_USE_THREADS = True

    # Max pool of connections for massive S3 interactions
    AWS_S3_MAX_POOL_CONNECTIONS = 10

    # Time to raise timeout when submitting a new file
    AWS_S3_CONNECT_TIMEOUT = 60

    # Read-only mode that disables save, delete and rename.
    AWS_S3_READ_ONLY = False

**Important:** Several of these settings (noted above) will not affect existing files. To sync the new settings to
existing files, run ``./manage.py s3_sync_meta django.core.files.storage.default_storage``.

These settings can be provided in field storage definition like this:

.. code:: python

    from django.db import models

    from django_s3_storage.storage import S3Storage

    storage = S3Storage(aws_s3_bucket_name='test_bucket')


    class Car(models.Model):
        name = models.CharField(max_length=255)
        photo = models.ImageField(storage=storage)

**Note:** settings key in storage definition should be `lowercase`.

Custom URLs
-----------

Sometimes the default settings aren't flexible enough and custom handling of object is needed. For
example, the ``Content-Disposition`` might be set to force download of a file instead of opening
it:

.. code:: python

    url = storage.url("foo/bar.pdf", extra_params={"ResponseContentDisposition": "attachment"})

Another example is a link to a specific version of the file (within a bucket that has versioning
enabled):

.. code:: python

    url = storage.url("foo/bar.pdf", extra_params={"VersionId": "FRy3fTduRtqHsRAoNp0REzPJj_WunDfl"})

The ``extra_params`` dict accepts the same parameters as `get_object() <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.get_object>`_.


Pre-signed URL uploads
----------------------

Pre-signed URLs allow temporary access to S3 objects without AWS credentials. A pre-signed URL allows HTTP clients to
upload files directly, improving performance and reducing the load on your server.

To generate a presigned URL allowing a file upload with HTTP ``PUT``:

.. code:: python

    url = storage.url("foo/bar.pdf", client_method="put_object")


How does django-s3-storage compare with django-storages?
--------------------------------------------------------

`django-storages <https://github.com/jschneier/django-storages>`_ supports a variety of other storage backends,
whereas django-s3-storage provides similar features, but only supports S3. It was originally written to support
Python 3 at a time when the future of django-storages was unclear. It's a small, well-tested and self-contained
library that aims to do one thing very well.

The author of django-s3-storage is not aware of significant differences in functionality with django-storages.
If you notice some differences, please file an issue!


Migration from django-storages
------------------------------

If your are updating a project that used `django-storages <https://pypi.python.org/pypi/django-storages>`_ just for S3 file storage, migration is trivial.

Follow the installation instructions, replacing 'storages' in ``INSTALLED_APPS``. Be sure to scrutinize the rest of your settings file for changes, most notably ``AWS_S3_BUCKET_NAME`` for ``AWS_STORAGE_BUCKET_NAME``.


Support and announcements
-------------------------

Downloads and bug tracking can be found at the `main project
website <http://github.com/etianen/django-s3-storage>`_.


More information
----------------

The django-s3-storage project was developed by Dave Hall. You can get the code
from the `django-s3-storage project site <http://github.com/etianen/django-s3-storage>`_.

Dave Hall is a freelance web developer, based in Cambridge, UK. You can usually
find him on the Internet in a number of different places:

-  `Website <http://www.etianen.com/>`_
-  `Twitter <http://twitter.com/etianen>`_
-  `Google Profile <http://www.google.com/profiles/david.etianen>`_
