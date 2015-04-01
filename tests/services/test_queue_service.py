import json

from tornado.testing import AsyncTestCase, gen_test
from testnado.service_case_helpers import ServiceCaseHelpers

from tests.helpers.service_helpers import fetch_token
from tornadorax.services.queue_service import QueueService


class TestQueue(ServiceCaseHelpers, AsyncTestCase):

    def setUp(self):
        super(TestQueue, self).setUp()

        def health_handle(handler):
            handler.set_status(204)

        def get_message_handle(handler):
            self.get_message_requests.append(handler.request)
            base_url = handler.request.full_url().split("?")[0]
            next_url = base_url + "?marker=" + str(
                len(self.get_message_requests))
            handler.write({
                "links": [{"rel": "next", "href": next_url}],
                "messages": [{"ttl": 10, "age": 10, "body": {"event": "foo"}}]
            })

        def post_message_handle(handler):
            self.post_message_requests.append(handler.request)
            handler.set_status(201)
            handler.finish({
                "resources": [
                    "/v1/queues/myqueue/messages/1",
                ]
            })

        self.get_message_requests = []
        self.post_message_requests = []

        self.queue_service = self.add_service()
        self.queue_service.add_method("GET", "/v1/health", health_handle)
        self.queue_service.add_method(
            "GET", "/v1/queues/myqueue/messages", get_message_handle)
        self.queue_service.add_method(
            "POST", "/v1/queues/myqueue/messages", post_message_handle)

        self.client = QueueService(
            self.queue_service.url("v1"), fetch_token=fetch_token,
            ioloop=self.io_loop)

    @gen_test
    def test_fetch_messages_returns_empty_list(self):

        def get_message_handle(handler):
            handler.set_status(204)
            handler.finish("")

        self.queue_service.add_method(
            "GET", "/v1/queues/myqueue/messages", get_message_handle)
        self.start_services()

        queue = yield self.client.fetch_queue("myqueue")
        result = yield queue.fetch_messages()
        self.assertEqual("success", result["status"])
        messages = result["messages"]
        self.assertEqual(0, len(messages))

    @gen_test
    def test_fetch_messages_returns_messages_on_success(self):
        self.start_services()
        queue = yield self.client.fetch_queue("myqueue")
        result = yield queue.fetch_messages()
        self.assertEqual("success", result["status"])
        messages = result["messages"]
        self.assertEqual(1, len(messages))
        self.assertEqual({"event": "foo"}, messages[0]["body"])
        self.queue_service.assert_requested(
            "GET", "/v1/queues/myqueue/messages", headers={
                "X-Auth-Token": "TOKEN",
                "Client-Id": queue.receive_client_id})

    @gen_test
    def test_fetch_messages_returns_reason_on_failure(self):
        def fail_get_message(handler):
            handler.set_status(404)
            handler.write("ERROR")

        self.queue_service.add_method(
            "GET", "/v1/queues/myqueue/messages", fail_get_message)
        self.start_services()
        queue = yield self.client.fetch_queue("myqueue")
        result = yield queue.fetch_messages()
        self.assertEqual("error", result["status"])
        self.assertEqual(404, result["code"])
        self.assertEqual("ERROR", result["body"])

    @gen_test
    def test_push_message_succeeds(self):
        self.start_services()

        message = {"event": "foo"}
        queue = yield self.client.fetch_queue("myqueue")
        result = yield queue.push_message(message, ttl=60)
        self.assertEqual("success", result["status"])
        self.assertTrue("resource" in result)

        self.queue_service.assert_requested(
            "POST", "/v1/queues/myqueue/messages", headers={
                "X-Auth-Token": "TOKEN",
                "Client-Id": queue.send_client_id})

        request = self.post_message_requests[0]
        request = json.loads(request.body)
        self.assertEqual([{"ttl": 60, "body": message}], request)

    @gen_test
    def test_push_message_returns_reason_on_failure(self):
        def fail_push_message(handler):
            handler.set_status(404)
            handler.write("ERROR")

        self.queue_service.add_method(
            "POST", "/v1/queues/myqueue/messages", fail_push_message)

        self.start_services()
        queue = yield self.client.fetch_queue("myqueue")
        result = yield queue.push_message("FOOBAR", 60)
        self.assertEqual("error", result["status"])
        self.assertEqual(404, result["code"])
        self.assertEqual("ERROR", result["body"])

    @gen_test
    def test_wait_for_message_returns_after_multiple_fetches(self):
        # placeholder for a little more friendly interface for
        # tornaodo for longer polling

        # message = yield.queue.wait_for_messages()
        pass
