import json
import hashlib
import random

try:
    from string import letters
except ImportError:
    from string import ascii_letters as letters

try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse

from tornado.testing import AsyncTestCase, gen_test
from testnado.service_case_helpers import ServiceCaseHelpers

from tests.helpers.service_helpers import fetch_token
from tornadorax.services.storage_service import StorageService
from tornadorax.services.storage_service import SegmentWriter
from tornadorax.services.storage_service import MissingTempURLKey
from tornadorax.services.storage_service import StreamError


OBJECT_BODY = "".join([
    random.choice(letters) for i in range(2048)
]).encode("utf8")


class TestStorage(ServiceCaseHelpers, AsyncTestCase):

    def setUp(self):
        super(TestStorage, self).setUp()
        self.storage_service = self.add_service()
        self.storage_service.add_method(
            "PUT", "/v1/container/object", object_write_handle)

        self.storage_service.add_method(
            "GET", "/v1/container/object", object_read_handle)

        self.storage_service.add_method(
            "HEAD", "/v1/container/object", object_info_handle)

        self.storage_service.add_method(
            "PUT", "/v1/container/manifest", object_write_handle)

        self.storage_service.add_method(
            "PUT", "/v1/container/manifest/segments/\d+", object_write_handle)

        self.client = StorageService(
            self.storage_service.url("/v1"), fetch_token=fetch_token,
            ioloop=self.io_loop)

    @gen_test
    def test_generate_tempurl_requires_key(self):
        self.start_services()
        container = yield self.client.fetch_container("container")
        obj = yield container.fetch_object("object")
        with self.assertRaises(MissingTempURLKey):
            obj.generate_tempurl(method="GET", expires=1000)

    @gen_test
    def test_generate_tempurl(self):
        self.start_services()
        container = yield self.client.fetch_container("container")
        obj = yield container.fetch_object("object", tempurl_key="foobar")
        url = obj.generate_tempurl(method="GET", expires=1000)
        url, params = url.split("?")
        self.assertEqual(url, obj.object_url)
        params = dict(urlparse.parse_qsl(params))
        # pregenerated based on parameters and 'foobar' key
        expected = "a42206ca0c6e654e46cd5e33b2b1f92aab81194d"
        self.assertEqual("1000", params["temp_url_expires"])
        self.assertEqual(expected, params["temp_url_sig"])

    @gen_test
    def test_generate_tempurl_forces_integer_expiration(self):
        self.start_services()
        container = yield self.client.fetch_container("container")
        obj = yield container.fetch_object("object", tempurl_key="foobar")
        url = obj.generate_tempurl(method="GET", expires=1000.112)
        params = dict(urlparse.parse_qsl(url.split("?")[1]))
        # pregenerated based on parameters and 'foobar' key
        expected = "a42206ca0c6e654e46cd5e33b2b1f92aab81194d"
        self.assertEqual("1000", params["temp_url_expires"])
        self.assertEqual(expected, params["temp_url_sig"])

    @gen_test
    def test_upload_stream_stores_contents(self):
        self.start_services()

        container = yield self.client.fetch_container("container")
        obj = yield container.fetch_object("object")
        writer = yield obj.upload_stream(mimetype="text/html")
        yield writer.write("CON".encode("ascii"))
        yield writer.write("TENTS".encode("ascii"))
        result = yield writer.finish()

        self.assertEqual("success", result["status"])
        self.assertEqual(8, result["length"])
        self.assertEqual(
            hashlib.md5(b"CONTENTS").hexdigest(), result["md5sum"])

        request = self.storage_service.assert_requested(
            "PUT", "/v1/container/object", headers={
                "X-Auth-Token": "TOKEN", "Content-type": "text/html"})

        self.assertEqual(b"CONTENTS", request.body)

    @gen_test
    def test_upload_stream_allows_extra_metadata(self):
        self.start_services()
        container = yield self.client.fetch_container("container")
        obj = yield container.fetch_object("object")
        writer = yield obj.upload_stream(
            mimetype="text/html", metadata={"foo": "bar", "cat": "mouse"})
        yield writer.write(b"CONTENTS")
        result = yield writer.finish()

        self.assertEqual("success", result["status"])
        request = self.storage_service.assert_requested(
            "PUT", "/v1/container/object")

        self.assertEqual("bar", request.headers["X-Object-Meta-foo"])
        self.assertEqual("mouse", request.headers["X-Object-Meta-cat"])

    @gen_test
    def test_upload_stream_raises_error(self):

        self.storage_service.add_method(
            "PUT", "/v1/container/object", object_write_error_handle)

        self.start_services()
        container = yield self.client.fetch_container("container")
        obj = yield container.fetch_object("object")
        writer = yield obj.upload_stream(mimetype="text/html")
        yield writer.write(b"CONTENTS")
        result = yield writer.finish()

        self.assertEqual("error", result["status"])
        self.assertEqual(401, result["code"])
        self.assertEqual(b"ERROR", result["body"])

    @gen_test
    def test_upload_stream_allows_content_length(self):
        self.start_services()
        container = yield self.client.fetch_container("container")
        obj = yield container.fetch_object("object")
        writer = yield obj.upload_stream(
            mimetype="text/html", content_length=8)
        yield writer.write(b"CONTENTS")
        yield writer.finish()

        request = self.storage_service.assert_requested(
            "PUT", "/v1/container/object")
        self.assertEqual("8", request.headers["Content-length"])

    @gen_test
    def test_upload_stream_allows_custom_metadata(self):
        self.start_services()
        container = yield self.client.fetch_container("container")
        obj = yield container.fetch_object("manifest")
        segment_writer = SegmentWriter.with_defaults(segment_size=10)
        writer = yield obj.upload_stream(
            mimetype="text/html", writer=segment_writer,
            metadata={"cat": "dog"})
        yield writer.write(b"lincoln")
        result = yield writer.finish()
        self.assertEqual("success", result["status"])

        request = self.storage_service.assert_requested(
            "PUT", "/v1/container/manifest")
        self.assertEqual("dog", request.headers["X-Object-Meta-cat"])

    @gen_test
    def test_upload_stream_allows_segmentation(self):
        # big, nasty segment test. should be broken up later, especially
        # with retry, etc.

        self.start_services()
        container = yield self.client.fetch_container("container")
        obj = yield container.fetch_object("manifest")
        segment_writer = SegmentWriter.with_defaults(segment_size=4)
        writer = yield obj.upload_stream(
            mimetype="text/html", writer=segment_writer)
        yield writer.write(b"abe")
        yield writer.write(b" lincoln")
        yield writer.write(b" wins")
        result = yield writer.finish()
        self.assertEqual("success", result["status"])

        expected = [
            ("abe ", "/container/manifest/segments/000001"),
            ("linc", "/container/manifest/segments/000002"),
            ("oln ", "/container/manifest/segments/000003"),
            ("wins", "/container/manifest/segments/000004")
        ]

        for content, segment_path in expected:
            request = self.storage_service.assert_requested(
                "PUT", "/v1{}".format(segment_path),
                headers={"X-Auth-Token": "TOKEN"})
            self.assertEqual(content, request.body.decode("utf8"))

        request = self.storage_service.assert_requested(
            "PUT", "/v1/container/manifest",
            headers={"X-Auth-Token": "TOKEN"})
        self.assertEqual(b"put", request.arguments["multipart-manifest"][0])
        self.assertEqual("text/html", request.headers["Content-type"])

        body = json.loads(request.body.decode("utf8"))
        self.assertEqual(4, len(body))

        for i in range(len(body)):
            segment_info = body[i]
            expected_body, expected_path = expected[i]
            expected_etag = hashlib.md5(
                expected_body.encode("ascii")).hexdigest()
            self.assertEqual(expected_path, segment_info["path"])
            self.assertEqual(expected_etag, segment_info["etag"])
            self.assertEqual(4, segment_info["size_bytes"])

    @gen_test
    def test_upload_stream_allows_custom_segments(self):
        self.start_services()
        container = yield self.client.fetch_container("container")
        obj = yield container.fetch_object("manifest")
        # bad segment size so we can ensure it's not using it
        segment_writer = SegmentWriter.with_defaults(segment_size=1)
        writer = yield obj.upload_stream(
            mimetype="text/html", writer=segment_writer)

        segment1 = writer.create_segment()
        yield segment1.write(b"foo")
        yield segment1.write(b"bar")
        yield segment1.write(b"one")
        yield writer.close_segment(segment1)
        segment2 = writer.create_segment()
        yield segment2.write(b"foobar2")
        yield writer.close_segment(segment2)
        result = yield writer.finish()

        self.assertEqual("success", result["status"])

        request = self.storage_service.assert_requested(
            "PUT", "/v1/container/manifest/segments/000001")
        self.assertEqual(b"foobarone", request.body)

        request = self.storage_service.assert_requested(
            "PUT", "/v1/container/manifest/segments/000002")
        self.assertEqual(b"foobar2", request.body)

    @gen_test
    def test_upload_segment_allows_dynamic_segments(self):
        self.start_services()

        container = yield self.client.fetch_container("container")
        obj = yield container.fetch_object("manifest")

        writer = yield obj.upload_stream(
            mimetype="text/html",
            writer=SegmentWriter.with_defaults(dynamic=True))

        segment1 = writer.create_segment("001")
        yield segment1.write(b"foo")
        yield writer.close_segment(segment1)

        segment2 = writer.create_segment("005")
        yield segment2.write(b"bar")
        yield writer.close_segment(segment2)

        result = yield writer.finish()
        self.assertEqual("success", result["status"])

        request = self.storage_service.assert_requested(
            "PUT", "/v1/container/manifest/segments/001")
        self.assertEqual(b"foo", request.body)

        request = self.storage_service.assert_requested(
            "PUT", "/v1/container/manifest/segments/005")
        self.assertEqual(b"bar", request.body)

        request = self.storage_service.assert_requested(
            "PUT", "/v1/container/manifest")
        self.assertEqual(
            "container/manifest/segments",
            request.headers["X-Object-Manifest"])
        self.assertEqual("text/html", request.headers["Content-type"])
        self.assertEqual(b"", request.body)

    # Need to add tests that verify etags, retry manifests, etc.

    @gen_test
    def test_read_chunk(self):
        self.start_services()

        container = yield self.client.fetch_container("container")
        obj = yield container.fetch_object("object")
        chunk1 = yield obj.read(0, 1023)
        chunk2 = yield obj.read(1024)
        total = yield obj.read()
        self.assertEqual(chunk1, OBJECT_BODY[:1024])
        self.assertEqual(chunk2, OBJECT_BODY[1024:])
        self.assertEqual(OBJECT_BODY, total)

    @gen_test
    def test_read_stream_returns_body_in_chunks(self):
        self.start_services()
        container = yield self.client.fetch_container("container")
        obj = yield container.fetch_object("object")
        reader = yield obj.read_stream()
        body = bytearray()
        for read_chunk in reader:
            chunk = yield read_chunk
            body.extend(chunk)
        self.assertEqual(OBJECT_BODY, body)

    @gen_test
    def test_read_stream_raises_with_uncomsumed_chunk(self):
        self.start_services()
        container = yield self.client.fetch_container("container")
        obj = yield container.fetch_object("object")
        reader = yield obj.read_stream()
        next(reader)
        with self.assertRaises(StreamError):
            next(reader)

    @gen_test
    def test_read_stream_raises_with_bad_response(self):
        self.start_services()
        container = yield self.client.fetch_container("container")
        obj = yield container.fetch_object("object2")
        reader = yield obj.read_stream()
        with self.assertRaises(StreamError):
            for read_chunk in reader:
                yield read_chunk

    @gen_test
    def test_info_returns_metadata_about_object(self):
        self.start_services()
        container = yield self.client.fetch_container("container")
        obj = yield container.fetch_object("object")
        info = yield obj.info()
        self.assertEqual("success", info["status"])
        self.assertEqual({"foo": "foo", "bar": "bar"}, info["metadata"])
        self.assertEqual("value", info["x-foobar"])
        self.assertEqual(1024, info["length"])
        self.assertEqual("text/plain", info["type"])
        self.assertEqual("md5sum", info["etag"])

    @gen_test
    def test_info_returns_error_with_bad_response(self):
        self.start_services()
        container = yield self.client.fetch_container("container")
        obj = yield container.fetch_object("object2")
        info = yield obj.info()
        self.assertEqual("error", info["status"])
        self.assertEqual(404, info["code"])


def object_write_handle(handler):
    handler.set_status(201)
    handler.set_header(
        "ETag", hashlib.md5(handler.request.body).hexdigest())
    handler.finish()


def object_write_error_handle(handler):
    handler.set_status(401)
    handler.write("ERROR")


def object_read_handle(handler):
    handler.set_status(200)
    range_string = handler.request.headers.get("Range", "bytes=0-")
    range_parts = range_string.split("=")[1].rsplit("-", 1)
    start, end = range_parts
    if not end:
        end = len(OBJECT_BODY) - 1
    start, end = (int(start), int(end) + 1)

    for i in range(0, end-start, 1024):
        offset = start + i
        offset_end = offset + 1024 if end > offset + 1024 else end
        handler.write(OBJECT_BODY[offset:offset_end])
        handler.flush()

    handler.finish()


def object_info_handle(handler):
    handler.set_status(200)
    handler.set_header("X-Object-Meta-Foo", "foo")
    handler.set_header("X-Object-Meta-Bar", "bar")
    handler.set_header("Etag", "md5sum")
    handler.set_header("Content-length", "1024")
    handler.set_header("Content-type", "text/plain")
    handler.set_header("X-Foobar", "value")
    handler.finish()
