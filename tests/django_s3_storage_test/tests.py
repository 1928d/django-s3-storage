import posixpath
import time
from contextlib import contextmanager
from datetime import timedelta
from io import StringIO
from urllib.parse import urlsplit, urlunsplit

import requests
from django.core.exceptions import ImproperlyConfigured
from django.core.files.base import ContentFile
from django.core.files.storage import default_storage
from django.core.management import CommandError, call_command
from django.test import SimpleTestCase
from django.utils import timezone
from django.utils.timezone import is_naive, make_naive, utc

from django_s3_storage.storage import Endpoints, S3Storage


def helper_old_to_new_name(name) -> str:
    return f's3://1928-django-s3-storage-test/{name}'


class TestS3Storage(SimpleTestCase):
    # def tearDown(self):
    #     # clean up the dir
    #     for entry in default_storage.listdir(helper_old_to_new_name("")):
    #         print(entry)
    #         # default_storage.delete("/".join(entry))

    # Helpers.

    @contextmanager
    def save_file(
        self,
        name=f"s3://1928-django-s3-storage-test/foo.txt",
        content=b"foo",
        storage=default_storage,
    ):
        name = storage.save(name, ContentFile(content, name))
        try:
            time.sleep(1)  # Let S3 process the save.
            yield name
        finally:
            storage.delete(name)

    # Configuration tets.

    def testSettingsImported(self):
        self.assertEqual(S3Storage().settings.AWS_S3_CONTENT_LANGUAGE, "")
        with self.settings(AWS_S3_CONTENT_LANGUAGE="foo"):
            self.assertEqual(S3Storage().settings.AWS_S3_CONTENT_LANGUAGE, "foo")

    def testSettingsOverwrittenByKwargs(self):
        self.assertEqual(S3Storage().settings.AWS_S3_CONTENT_LANGUAGE, "")
        self.assertEqual(
            S3Storage(aws_s3_content_language="foo").settings.AWS_S3_CONTENT_LANGUAGE, "foo"
        )

    def testSettingsUnknown(self):
        self.assertRaises(
            ImproperlyConfigured,
            lambda: S3Storage(
                foo=True,
            ),
        )

    # Storage tests.

    def testOpenMissing(self):
        self.assertRaises(OSError, lambda: default_storage.open(helper_old_to_new_name("foo.txt")))

    def testOpenWriteMode(self):
        self.assertRaises(
            ValueError, lambda: default_storage.open(helper_old_to_new_name("foo.txt"), "wb")
        )

    def testSaveAndOpen(self):
        with self.save_file() as name:
            self.assertEqual(name, helper_old_to_new_name("foo.txt"))
            handle = default_storage.open(name)
            self.assertEqual(handle.read(), b"foo")
            # Re-open the file.
            handle.close()
            handle.open()
            self.assertEqual(handle.read(), b"foo")

    def testSaveTextMode(self):
        with self.save_file(content=b"foo"):
            self.assertEqual(default_storage.open(helper_old_to_new_name("foo.txt")).read(), b"foo")

    def testSaveGzipped(self):
        # Tiny files are not gzipped.
        with self.save_file():
            self.assertEqual(
                default_storage.meta(helper_old_to_new_name("foo.txt")).get("ContentEncoding"), None
            )
            self.assertEqual(default_storage.open(helper_old_to_new_name("foo.txt")).read(), b"foo")
            self.assertEqual(
                requests.get(default_storage.url(helper_old_to_new_name("foo.txt"))).content, b"foo"
            )
        # Large files are gzipped.
        with self.save_file(content=b"foo" * 1000):
            self.assertEqual(
                default_storage.meta(helper_old_to_new_name("foo.txt")).get("ContentEncoding"),
                "gzip",
            )
            self.assertEqual(
                default_storage.open(helper_old_to_new_name("foo.txt")).read(), b"foo" * 1000
            )
            self.assertEqual(
                requests.get(default_storage.url(helper_old_to_new_name("foo.txt"))).content,
                b"foo" * 1000,
            )

    def testGzippedSize(self):
        content = b"foo" * 4096
        with self.settings(AWS_S3_GZIP=False):
            name = helper_old_to_new_name("foo/bar.txt")
            with self.save_file(name=name, content=content):
                meta = default_storage.meta(name)
                self.assertNotEqual(meta.get("ContentEncoding", ""), "gzip")
                self.assertNotIn("uncompressed_size", meta["Metadata"])
                self.assertEqual(default_storage.size(name), len(content))
        with self.settings(AWS_S3_GZIP=True):
            name = helper_old_to_new_name("foo/bar.txt.gz")
            with self.save_file(name=name, content=content):
                meta = default_storage.meta(name)
                self.assertEqual(meta["ContentEncoding"], "gzip")
                self.assertIn("uncompressed_size", meta["Metadata"])
                self.assertEqual(meta["Metadata"], {"uncompressed_size": str(len(content))})
                self.assertEqual(default_storage.size(name), len(content))

    def testUrl(self):
        with self.save_file():
            url = default_storage.url(helper_old_to_new_name("foo.txt"))
            # The URL should contain query string authentication.
            self.assertTrue(urlsplit(url).query)
            response = requests.get(url)
            # The URL should be accessible, but be marked as private.
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.content, b"foo")
            self.assertEqual(response.headers["cache-control"], "private,max-age=3600")
            # With the query string removed, the URL should not be accessible.
            url_unauthenticated = urlunsplit(
                urlsplit(url)[:3]
                + (
                    "",
                    "",
                )
            )
            response_unauthenticated = requests.get(url_unauthenticated)
            self.assertEqual(response_unauthenticated.status_code, 403)

    def testCustomUrlContentDisposition(self):
        name = helper_old_to_new_name("foo/bar.txt")
        with self.save_file(name=name, content=b"foo" * 4096):
            url = default_storage.url(
                name, extra_params={"ResponseContentDisposition": "attachment"}
            )
            self.assertIn("response-content-disposition=attachment", url)
            rsp = requests.get(url)
            self.assertEqual(rsp.status_code, 200)
            self.assertIn("Content-Disposition", rsp.headers)
            self.assertEqual(rsp.headers["Content-Disposition"], "attachment")

    def testCustomUrlWhenPublicURL(self):
        with self.settings(AWS_S3_PUBLIC_URL="/foo/", AWS_S3_BUCKET_AUTH=False):
            name = helper_old_to_new_name("bar.txt")
            with self.save_file(name=name, content=b"foo" * 4096):
                self.assertRaises(
                    ValueError,
                    default_storage.url,
                    name,
                    extra_params={"ResponseContentDisposition": "attachment"},
                )

    def testExists(self):
        self.assertFalse(default_storage.exists(helper_old_to_new_name("foo.txt")))
        with self.save_file():
            self.assertTrue(default_storage.exists(helper_old_to_new_name("foo.txt")))
            self.assertFalse(default_storage.exists(helper_old_to_new_name("fo")))

    def testExistsDir(self):
        self.assertFalse(default_storage.exists(helper_old_to_new_name("foo/")))
        name = helper_old_to_new_name("foo/bar.txt")
        with self.save_file(name=name):
            self.assertTrue(default_storage.exists(helper_old_to_new_name("foo/")))

    def testExistsRelative(self):
        self.assertFalse(
            default_storage.exists(helper_old_to_new_name("admin/css/../img/sorting-icons.svg"))
        )
        name = helper_old_to_new_name("admin/img/sorting-icons.svg")
        with self.save_file(name=name):
            self.assertTrue(
                default_storage.exists(helper_old_to_new_name("admin/css/../img/sorting-icons.svg"))
            )

    def testSize(self):
        with self.save_file():
            self.assertEqual(default_storage.size(helper_old_to_new_name("foo.txt")), 3)

    def testDelete(self):
        with self.save_file():
            self.assertTrue(default_storage.exists(helper_old_to_new_name("foo.txt")))
            default_storage.delete(helper_old_to_new_name("foo.txt"))
        self.assertFalse(default_storage.exists(helper_old_to_new_name("foo.txt")))

    def testCopy(self):
        with self.save_file():
            self.assertTrue(default_storage.exists(helper_old_to_new_name("foo.txt")))
            default_storage.copy(
                helper_old_to_new_name("foo.txt"), helper_old_to_new_name("bar.txt")
            )
            self.assertTrue(default_storage.exists(helper_old_to_new_name("foo.txt")))
        self.assertTrue(default_storage.exists(helper_old_to_new_name("bar.txt")))

    def testRename(self):
        with self.save_file():
            self.assertTrue(default_storage.exists(helper_old_to_new_name("foo.txt")))
            default_storage.rename(
                helper_old_to_new_name("foo.txt"), helper_old_to_new_name("bar.txt")
            )
            self.assertFalse(default_storage.exists(helper_old_to_new_name("foo.txt")))
        self.assertTrue(default_storage.exists(helper_old_to_new_name("bar.txt")))

    def testModifiedTime(self):
        with self.save_file():
            modified_time = default_storage.modified_time(helper_old_to_new_name("foo.txt"))
            # Check that the timestamps are roughly equals.
            self.assertLess(
                abs(modified_time - make_naive(timezone.now(), utc)), timedelta(seconds=10)
            )
            # All other timestamps are slaved to modified time.
            self.assertEqual(
                default_storage.accessed_time(helper_old_to_new_name("foo.txt")), modified_time
            )
            self.assertEqual(
                default_storage.created_time(helper_old_to_new_name("foo.txt")), modified_time
            )

    def testGetModifiedTime(self):
        tzname = "America/Argentina/Buenos_Aires"
        with self.settings(USE_TZ=False, TIME_ZONE=tzname), self.save_file():
            modified_time = default_storage.get_modified_time(helper_old_to_new_name("foo.txt"))
            self.assertTrue(is_naive(modified_time))
            # Check that the timestamps are roughly equals in the correct timezone
            self.assertLess(abs(modified_time - timezone.now()), timedelta(seconds=10))
            # All other timestamps are slaved to modified time.
            self.assertEqual(
                default_storage.get_accessed_time(helper_old_to_new_name("foo.txt")), modified_time
            )
            self.assertEqual(
                default_storage.get_created_time(helper_old_to_new_name("foo.txt")), modified_time
            )

        with self.save_file():
            modified_time = default_storage.get_modified_time(helper_old_to_new_name("foo.txt"))
            self.assertFalse(is_naive(modified_time))
            # Check that the timestamps are roughly equals
            self.assertLess(abs(modified_time - timezone.now()), timedelta(seconds=10))
            # All other timestamps are slaved to modified time.
            self.assertEqual(
                default_storage.get_accessed_time(helper_old_to_new_name("foo.txt")), modified_time
            )
            self.assertEqual(
                default_storage.get_created_time(helper_old_to_new_name("foo.txt")), modified_time
            )

    def testListdir(self):
        self.assertEqual(default_storage.listdir(helper_old_to_new_name("")), ([], []))
        self.assertEqual(default_storage.listdir(helper_old_to_new_name("/")), ([], []))
        with self.save_file(), self.save_file(name=helper_old_to_new_name("bar/bat.txt")):
            self.assertEqual(
                default_storage.listdir(helper_old_to_new_name("")), (["bar"], ["foo.txt"])
            )
            self.assertEqual(
                default_storage.listdir(helper_old_to_new_name("bar")), ([], ["bat.txt"])
            )
            self.assertEqual(
                default_storage.listdir(helper_old_to_new_name("bar/")), ([], ["bat.txt"])
            )

    def testPublicUrl(self):
        with self.settings(AWS_S3_PUBLIC_URL="/foo/", AWS_S3_BUCKET_AUTH=False):
            self.assertEqual(default_storage.url("bar.txt"), "/foo/bar.txt")

    def testNonOverwrite(self):
        with self.save_file() as name_1, self.save_file() as name_2:
            self.assertEqual(name_1, helper_old_to_new_name("foo.txt"))
            self.assertNotEqual(name_1, name_2)

    def testOverwrite(self):
        with self.settings(AWS_S3_FILE_OVERWRITE=True):
            with self.save_file() as name_1, self.save_file() as name_2:
                self.assertEqual(name_1, helper_old_to_new_name("foo.txt"))
                self.assertEqual(name_2, helper_old_to_new_name("foo.txt"))

    def testClientConfig(self):
        storage = S3Storage()

        # Default settings, s3_client_presigning is same as s3_client
        for schema in storage._clients.keys():
            self.assertIs(storage.s3_client(schema), storage.s3_client_presigning(schema))

        # s3_client_presigning is same as s3_client if the endpoints are the same.
        with self.settings(
            AWS_S3_ENDPOINTS={
                's3': Endpoints(
                    endpoint_url='http://localhost:9000',
                    endpoint_url_presigning='http://localhost:9000',
                )
            }
        ):
            for schema in storage._clients.keys():
                self.assertIs(storage.s3_client(schema), storage.s3_client_presigning(schema))

        # s3_client_presigning is same as s3_client if endpoint_url_presigning is not set,
        with self.settings(
            AWS_S3_ENDPOINTS={
                's3': Endpoints(
                    endpoint_url='http://localhost:9000',
                )
            }
        ):
            for schema in storage._clients.keys():
                self.assertIs(storage.s3_client(schema), storage.s3_client_presigning(schema))

        # s3_client_presigning is NOT same as s3_client if the endpoints are not equal.
        with self.settings(
            AWS_S3_ENDPOINTS={
                's3': Endpoints(
                    endpoint_url='http://localhost:9000',
                    endpoint_url_presigning='http://example.com:9000',
                )
            }
        ):
            for schema in storage._clients.keys():
                self.assertIsNot(storage.s3_client(schema), storage.s3_client_presigning(schema))

    def testMultiSchemaConfig(self):
        storage = S3Storage()

        # Four different endpoints should give four clients
        with self.settings(
            AWS_S3_ENDPOINTS={
                's3': Endpoints(
                    endpoint_url='http://localhost:9000',
                    endpoint_url_presigning='http://localhost2:9000',
                ),
                's3-something': Endpoints(
                    endpoint_url='http://something:9000',
                    endpoint_url_presigning='http://something2:9000',
                ),
            }
        ):
            self.assertEqual(list(storage._clients.keys()), ['s3', 's3-something'])

            client_1 = storage.s3_client('s3')
            client_2 = storage.s3_client_presigning('s3')
            client_3 = storage.s3_client('s3-something')
            client_4 = storage.s3_client_presigning('s3-something')

            self.assertIsNot(client_1, client_2)
            self.assertIsNot(client_1, client_3)
            self.assertIsNot(client_1, client_4)

            self.assertIsNot(client_2, client_3)
            self.assertIsNot(client_2, client_4)

            self.assertIsNot(client_3, client_4)
