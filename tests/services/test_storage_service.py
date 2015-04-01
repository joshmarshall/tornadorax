import json
import hashlib

from tornado.testing import AsyncTestCase, gen_test
from testnado.service_case_helpers import ServiceCaseHelpers

from tests.helpers.service_helpers import fetch_token
from tornadorax.services.storage_service import StorageService
from tornadorax.services.storage_service import SegmentWriter


class TestStorage(ServiceCaseHelpers, AsyncTestCase):

    def setUp(self):
        super(TestStorage, self).setUp()

        self.object_write_requests = []

        def object_write_handle(handler):
            request = handler.request
            self.object_write_requests.append(request)
            handler.set_status(201)
            handler.write("")
            handler.set_header("ETag", hashlib.md5(request.body).hexdigest())

        self.storage_service = self.add_service()
        self.storage_service.add_method(
            "PUT", "/v1/container/object", object_write_handle)

        self.client = StorageService(
            self.storage_service.url("/v1"), fetch_token=fetch_token,
            ioloop=self.io_loop)

    @gen_test
    def test_upload_stream_stores_contents(self):
        self.start_services()

        container = yield self.client.fetch_container("container")
        obj = yield container.fetch_object("object")
        writer = yield obj.upload_stream(mimetype="text/html")
        yield writer.write("CON")
        yield writer.write("TENTS")
        result = yield writer.finish()

        self.assertEqual("success", result["status"])
        self.assertEqual(8, result["length"])
        self.assertEqual(hashlib.md5("CONTENTS").hexdigest(), result["md5sum"])

        self.assertEqual(1, len(self.object_write_requests))
        request = self.object_write_requests[0]
        self.assertEqual("TOKEN", request.headers["X-Auth-Token"])
        self.assertEqual("text/html", request.headers["Content-type"])
        self.assertEqual("CONTENTS", request.body)

    @gen_test
    def test_upload_stream_raises_error(self):

        def object_write_handle(handler):
            handler.set_status(401)
            handler.write("ERROR")

        self.storage_service.add_method(
            "PUT", "/v1/container/object", object_write_handle)

        self.start_services()
        container = yield self.client.fetch_container("container")
        obj = yield container.fetch_object("object")
        writer = yield obj.upload_stream(mimetype="text/html")
        yield writer.write("CONTENTS")
        result = yield writer.finish()

        self.assertEqual("error", result["status"])
        self.assertEqual(401, result["code"])
        self.assertEqual("ERROR", result["body"])

    @gen_test
    def test_upload_stream_allows_content_length(self):
        self.start_services()
        container = yield self.client.fetch_container("container")
        obj = yield container.fetch_object("object")
        writer = yield obj.upload_stream(
            mimetype="text/html", content_length=8)
        yield writer.write("CONTENTS")
        yield writer.finish()

        request = self.object_write_requests[0]
        self.assertEqual("8", request.headers["Content-length"])

    @gen_test
    def test_upload_stream_allows_segmentation(self):
        # big, nasty segment test. should be broken up later, especially
        # with retry, etc.

        def request_write_handle(handler):
            handler.set_status(201)
            handler.set_header(
                "ETag", hashlib.md5(handler.request.body).hexdigest())
            handler.finish()

        self.storage_service.add_method(
            "PUT", "/v1/container/manifest", request_write_handle)

        self.storage_service.add_method(
            "PUT", "/v1/container/manifest/segments/\d+", request_write_handle)

        self.start_services()
        container = yield self.client.fetch_container("container")
        obj = yield container.fetch_object("manifest")
        segment_writer = SegmentWriter.with_segment_size(4)
        writer = yield obj.upload_stream(
            mimetype="text/html", writer=segment_writer)
        yield writer.write("abe")
        yield writer.write(" lincoln")
        yield writer.write(" wins")
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
            self.assertEqual(content, request.body)

        request = self.storage_service.assert_requested(
            "PUT", "/v1/container/manifest",
            headers={"X-Auth-Token": "TOKEN"})
        self.assertEqual("put", request.arguments["multipart-manifest"][0])
        self.assertEqual("text/html", request.headers["Content-type"])

        body = json.loads(request.body)
        self.assertEqual(4, len(body))

        for i in range(len(body)):
            segment_info = body[i]
            expected_body, expected_path = expected[i]
            expected_etag = hashlib.md5(expected_body).hexdigest()
            self.assertEqual(expected_path, segment_info["path"])
            self.assertEqual(expected_etag, segment_info["etag"])
            self.assertEqual(4, segment_info["size_bytes"])

    @gen_test
    def test_upload_stream_allows_custom_segments(self):

        def request_write_handle(handler):
            handler.set_status(201)
            handler.set_header(
                "ETag", hashlib.md5(handler.request.body).hexdigest())
            handler.finish()

        self.storage_service.add_method(
            "PUT", "/v1/container/manifest", request_write_handle)

        self.storage_service.add_method(
            "PUT", "/v1/container/manifest/segments/\d+", request_write_handle)

        self.start_services()
        container = yield self.client.fetch_container("container")
        obj = yield container.fetch_object("manifest")
        # bad segment size so we can ensure it's not using it
        segment_writer = SegmentWriter.with_segment_size(1)
        writer = yield obj.upload_stream(
            mimetype="text/html", writer=segment_writer)

        segment1 = writer.create_segment()
        yield segment1.write("foo")
        yield segment1.write("bar")
        yield segment1.write("one")
        yield writer.close_segment(segment1)
        segment2 = writer.create_segment()
        yield segment2.write("foobar2")
        yield writer.close_segment(segment2)
        result = yield writer.finish()

        self.assertEqual("success", result["status"])

        request = self.storage_service.assert_requested(
            "PUT", "/v1/container/manifest/segments/000001")
        self.assertEqual("foobarone", request.body)

        request = self.storage_service.assert_requested(
            "PUT", "/v1/container/manifest/segments/000002")
        self.assertEqual("foobar2", request.body)

    # Need to add tests that verify etags, retry manifests, etc.
