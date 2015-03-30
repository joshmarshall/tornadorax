import json
import time

import mock
from tornado.testing import AsyncTestCase, gen_test
from testnado.service_case_helpers import ServiceCaseHelpers

from tornadorax import service_registry
from tornadorax.identity_client import IdentityClient, NoServiceCatalog
from tests.helpers.foo_service import FooService
from tests.samples import RAX_AUTH_DATA


class TestIdentity(ServiceCaseHelpers, AsyncTestCase):

    def setUp(self):
        super(TestIdentity, self).setUp()

        def id_handle(handler):
            self.identity_requests.append(handler.request)
            auth_data = json.dumps(RAX_AUTH_DATA)
            handler.finish(auth_data)

        self.identity_service = self.add_service()
        self.identity_service.add_method("POST", "/v2.0/tokens", id_handle)

        self.identity_requests = []

        self.credentials = {
            "passwordCredentials": {
                "username": "user",
                "password": "password"
            }
        }
        self.client = IdentityClient(
            self.identity_service.base_url, self.credentials, self.io_loop)

        service_registry.register_service("foo:service", FooService)

        self.addCleanup(lambda: reload(service_registry))

    @gen_test
    def test_authorize_requests_a_token(self):
        self.start_services()
        result = yield self.client.authorize()
        self.assertEqual("success", result["status"])
        self.assertEqual("TOKEN", result["token"])

        self.assertEqual(1, len(self.identity_requests))
        request = self.identity_requests[0]
        self.assertEqual("application/json", request.headers["Content-type"])
        body = json.loads(request.body)
        self.assertEqual(body["auth"], self.credentials)

    @gen_test
    def test_authorize_returns_failure(self):

        def id_handle(handler):
            handler.set_status(401)
            handler.write("UNAUTHORIZED")

        self.identity_service.add_method("POST", "/v2.0/tokens", id_handle)
        self.start_services()

        result = yield self.client.authorize()
        self.assertEqual("error", result["status"])
        self.assertEqual(401, result["code"])
        self.assertEqual("UNAUTHORIZED", result["body"])

    @gen_test
    def test_authorize_retries_on_500s(self):
        responses = [(500, ""), (501, ""), (503, ""), (201, RAX_AUTH_DATA)]

        def id_handle(handler):
            status, body = responses.pop(0)
            self.identity_requests.append(time.time())
            handler.set_status(status)
            handler.write(body)

        self.identity_service.add_method("POST", "/v2.0/tokens", id_handle)
        self.start_services()

        with mock.patch("tornadorax.utilities.generate_backoff") as mocked:
            mocked.return_value = 0.05
            result = yield self.client.authorize()

        self.assertEqual("success", result["status"])
        self.assertEqual("TOKEN", result["token"])
        self.assertEqual(4, len(self.identity_requests))

    @gen_test
    def test_build_service_returns_properly_configured_instance(self):
        self.start_services()

        yield self.client.authorize()
        service = self.client.build_service("foo:service")
        service.assert_service_url_equals("https://dfw.public.com/v1")
        yield service.assert_token_equals("TOKEN")

    @gen_test
    def test_build_service_raises_exception_if_not_authorized(self):
        with self.assertRaises(NoServiceCatalog):
            self.client.build_service("foo:service")

    @gen_test
    def test_fetch_token_returns_token_value(self):
        self.start_services()
        yield self.client.authorize()
        self.identity_requests = []
        result = yield self.client.fetch_token()
        self.assertEqual("TOKEN", result)
        # it shouldn't make another identity call
        self.assertEqual(0, len(self.identity_requests))

    @gen_test
    def test_fetch_token_authorizes_if_token_unset(self):
        self.start_services()
        result = yield self.client.fetch_token()
        self.assertEqual("TOKEN", result)
        self.assertEqual(1, len(self.identity_requests))

    @gen_test
    def test_fetch_token_authorizes_if_token_expired(self):
        self.start_services()
        yield self.client.authorize()
        self.identity_requests = []
        # this is a bit invasive, playing with this attribute.
        # setting it to 55 seconds ahead since we give a minute
        # of leeway for (slight) clock skew
        self.client.token_expires = time.time() + 55
        result = yield self.client.fetch_token()
        self.assertEqual("TOKEN", result)
        self.assertEqual(1, len(self.identity_requests))
