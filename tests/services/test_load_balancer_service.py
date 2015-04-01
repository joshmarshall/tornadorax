from tornado.testing import AsyncTestCase, gen_test
from testnado.service_case_helpers import ServiceCaseHelpers

from tests.samples import load_balancer_samples
from tests.helpers.service_helpers import fetch_token
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
            handler.set_status(200)
            handler.write(load_balancer_samples.GET_RESPONSE)

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
    def test_load_balancer_list_returns_current_load_balancers(self):
        self.start_services()
        lbs = yield self.client.fetch_load_balancers()
        self.lb_service.assert_requested(
            "GET", "/v1/account/loadbalancers",
            headers={"X-Auth-Token": "TOKEN"})
        self.assertEqual(2, len(lbs))
        self.assertEqual("lb1", lbs[0].name)
        self.assertEqual("lb2", lbs[1].name)
