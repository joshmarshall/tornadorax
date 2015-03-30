import hashlib

from tornado import gen
from tornado.testing import AsyncTestCase, gen_test
from testnado.service_case_helpers import ServiceCaseHelpers

from tornadorax.services.storage_service import StorageService


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

        @gen.coroutine
        def fetch_token():
            raise gen.Return("TOKEN")

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

    # TODO: Add support for large files in BodyWriter. It can either
    # use static large objects, in which case there may be a maximum number
    # of segment files, or dynamic large objects, in which case there may
    # be an eventual consistency issue
