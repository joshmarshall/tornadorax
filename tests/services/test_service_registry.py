from tornado import gen
from tornado.testing import AsyncTestCase

from tests.helpers.foo_service import FooService
from tornadorax.services import service_registry


ENDPOINTS = [
    {
        "internalURL": "https://dfw1.foo.com/v1",
        "publicURL": "https://dfw2.foo.com/v1",
        "region": "DFW",
    },
    {
        "internalURL": "https://ord1.foo.com/v1",
        "publicURL": "https://ord2.foo.com/v1",
        "region": "ORD",
    }
]


@gen.coroutine
def fetch_token():
    raise gen.Return("TOKEN")


class TestServiceRegistry(AsyncTestCase):

    def setUp(self):
        super(TestServiceRegistry, self).setUp()
        service_registry.register_service("foo:service", FooService)
        self.addCleanup(lambda: reload(service_registry))

    def build(self, service_type="foo:service", **kwargs):
        service_catalog = [{
            "type": service_type,
            "name": service_type,
            "endpoints": ENDPOINTS
        }]
        kwargs.setdefault("service_catalog", service_catalog)
        kwargs.setdefault("fetch_token", fetch_token)
        kwargs.setdefault("ioloop", self.io_loop)
        return service_registry.build_service(service_type, **kwargs)

    def test_service_registry_includes_registered_services(self):
        service = self.build()
        # just select the first public URL without preferences
        service.assert_service_url_equals("https://dfw2.foo.com/v1")

    def test_service_registry_allows_internal_option(self):
        service = self.build(internal=True)
        service.assert_service_url_equals("https://dfw1.foo.com/v1")

    def test_service_registry_allows_region_option(self):
        service = self.build(internal=True, region="ORD")
        service.assert_service_url_equals("https://ord1.foo.com/v1")

    def test_service_registry_raises_error_with_invalid_region(self):
        with self.assertRaises(service_registry.InvalidRegion):
            self.build(internal=True, region="FOOBAR")

    def test_service_registry_raises_error_with_invalid_service(self):
        with self.assertRaises(service_registry.InvalidServiceType):
            self.build("other:service")

    def test_service_registry_raises_error_with_uncatalogued_service(self):
        with self.assertRaises(service_registry.InvalidServiceType):
            self.build(service_catalog=[])

    def test_service_registry_already_includes_core_services(self):
        self.build("rax:queues")
        self.build("object-store")
        self.build("rax:load-balancer")
