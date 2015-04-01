import json

from tornado import gen
from tornado.httpclient import AsyncHTTPClient


class LoadBalancerService(object):

    def __init__(self, service_url, fetch_token, ioloop):
        self.service_url = service_url
        self.fetch_token = fetch_token
        self.ioloop = ioloop
        self.client = AsyncHTTPClient(io_loop=self.ioloop)

    @gen.coroutine
    def fetch_load_balancers(self):
        url = "{0}/loadbalancers".format(self.service_url)
        token = yield self.fetch_token()
        response = yield self.client.fetch(
            url, headers={"X-Auth-Token": token})
        body = json.loads(response.body)
        load_balancers = []
        for lb_config in body["loadBalancers"]:
            load_balancers.append(
                LoadBalancer(lb_config["id"], lb_config["name"]))
        raise gen.Return(load_balancers)


class LoadBalancer(object):

    def __init__(self, lb_id, name):
        self.name = name
