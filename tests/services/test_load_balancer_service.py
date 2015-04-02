import json

from tornado.testing import AsyncTestCase, gen_test
from testnado.service_case_helpers import ServiceCaseHelpers

from tests.samples import load_balancer_samples
from tests.helpers.service_helpers import fetch_token
from tornadorax import errors
from tornadorax.services.load_balancer_service import LoadBalancerService


class TestLoadBalancer(ServiceCaseHelpers, AsyncTestCase):

    def setUp(self):
        super(TestLoadBalancer, self).setUp()

        def handle_lb_list(handler):
            handler.write(load_balancer_samples.LIST_RESPONSE)

        def handle_lb_add(handler):
            handler.set_status(201)
            handler.write(load_balancer_samples.CREATE_RESPONSE)

        def handle_lb_get(handler):
            handler.set_status(200)
            handler.write(load_balancer_samples.GET_RESPONSE)

        def handle_lb_post(handler):
            handler.set_status(200)
            handler.write(load_balancer_samples.GET_RESPONSE)

        def handle_lb_delete(handler):
            handler.set_status(202)
            handler.finish()

        self.lb_service = self.add_service()
        self.lb_service.add_method(
            "GET", "/v1/account/loadbalancers", handle_lb_list)
        self.lb_service.add_method(
            "POST", "/v1/account/loadbalancers", handle_lb_add)
        self.lb_service.add_method(
            "GET", "/v1/account/loadbalancers/3", handle_lb_get)
        self.lb_service.add_method(
            "POST", "/v1/account/loadbalancers/3", handle_lb_post)
        self.lb_service.add_method(
            "DELETE", "/v1/account/loadbalancers/3", handle_lb_delete)

        self.client = LoadBalancerService(
            self.lb_service.url("v1/account"), fetch_token=fetch_token,
            ioloop=self.io_loop)

    @gen_test
    def test_load_balancer_fetch_returns_current_load_balancers(self):
        self.start_services()
        lbs = yield self.client.fetch_load_balancers()
        self.lb_service.assert_requested(
            "GET", "/v1/account/loadbalancers",
            headers={"X-Auth-Token": "TOKEN"})
        self.assertEqual(2, len(lbs))
        self.assertEqual("lb1", lbs[0].name)
        self.assertEqual(1, lbs[0].id)
        self.assertEqual("lb2", lbs[1].name)
        self.assertEqual(2, lbs[1].id)

    @gen_test
    def test_load_balancer_fetch_raises_service_error(self):
        self.start_services()
        self.lb_service.add_method(
            "GET", "/v1/account/loadbalancers",
            lambda x: x.set_status(500))
        with self.assertRaises(errors.ServiceError):
            yield self.client.fetch_load_balancers()

    @gen_test
    def test_load_balancer_fetch_returns_single_load_balancer(self):
        self.start_services()
        lb = yield self.client.fetch_load_balancer("3")
        self.lb_service.assert_requested(
            "GET", "/v1/account/loadbalancers/3",
            headers={"X-Auth-Token": "TOKEN"})

        self.assertEqual(lb.name, "a-new-loadbalancer")
        self.assertEqual(lb.id, 3)

    @gen_test
    def test_load_balancer_get_raises_service_error(self):
        self.start_services()
        self.lb_service.add_method(
            "GET", "/v1/account/loadbalancers/3",
            lambda x: x.set_status(500))
        with self.assertRaises(errors.ServiceError):
            yield self.client.fetch_load_balancer("3")

    @gen_test
    def test_load_balancer_delete_removes_single_load_balancer(self):
        self.start_services()
        yield self.client.delete_load_balancer("3")
        self.lb_service.assert_requested(
            "DELETE", "/v1/account/loadbalancers/3",
            headers={"X-Auth-Token": "TOKEN"})

    @gen_test
    def test_load_balancer_delete_raises_service_error(self):
        self.lb_service.add_method(
            "DELETE", "/v1/account/loadbalancers/3",
            lambda x: x.set_status(500))
        self.start_services()
        with self.assertRaises(errors.ServiceError) as exc_context:
            yield self.client.delete_load_balancer("3")
        self.assertEqual(500, exc_context.exception.code)

    @gen_test
    def test_load_balancer_create_builds_new_resource(self):
        self.start_services()
        lb = yield self.client.create_load_balancer(
            "a-new-loadbalancer", port=8888, protocol="HTTP",
            virtual_ips=[{"foo": "bar"}])
        request = self.lb_service.assert_requested(
            "POST", "/v1/account/loadbalancers",
            headers={
                "X-Auth-Token": "TOKEN",
                "Content-type": "application/json"
            })
        body = json.loads(request.body)["loadBalancer"]
        self.assertEqual("a-new-loadbalancer", body["name"])
        self.assertEqual(8888, body["port"])
        self.assertEqual("HTTP", body["protocol"])
        self.assertEqual([{"foo": "bar"}], body["virtualIps"])
        self.assertEqual("a-new-loadbalancer", lb.name)
        self.assertEqual(3, lb.id)

    @gen_test
    def test_load_balancer_create_raises_service_error(self):
        self.lb_service.add_method(
            "POST", "/v1/account/loadbalancers",
            lambda x: x.set_status(500))
        self.start_services()
        with self.assertRaises(errors.ServiceError) as exc_context:
            yield self.client.create_load_balancer(
                name="foo", protocol="foo", port=80, virtual_ips=[])
        self.assertEqual(500, exc_context.exception.code)
