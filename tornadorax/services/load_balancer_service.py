import json

from tornado import gen
from tornado.httpclient import AsyncHTTPClient
from tornadorax import errors


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
            url, headers={"X-Auth-Token": token}, raise_error=False)
        errors.check_service_response(response, "loadbalancers")
        body = json.loads(response.body)
        load_balancers = []
        for lb_config in body["loadBalancers"]:
            load_balancers.append(
                LoadBalancer(lb_config["id"], lb_config["name"]))
        raise gen.Return(load_balancers)

    @gen.coroutine
    def fetch_load_balancer(self, lb_id):
        url = "{0}/loadbalancers/{1}".format(self.service_url, lb_id)
        token = yield self.fetch_token()
        response = yield self.client.fetch(
            url, headers={"X-Auth-Token": token}, raise_error=False)
        errors.check_service_response(response, "loadbalancers")
        body = json.loads(response.body)
        lb_config = body["loadBalancer"]
        lb = LoadBalancer(lb_config["id"], lb_config["name"])
        raise gen.Return(lb)

    @gen.coroutine
    def delete_load_balancer(self, lb_id):
        url = "{0}/loadbalancers/{1}".format(self.service_url, lb_id)
        token = yield self.fetch_token()
        response = yield self.client.fetch(
            url, method="DELETE", headers={"X-Auth-Token": token},
            raise_error=False)
        errors.check_service_response(response, "loadbalancers")

    @gen.coroutine
    def create_load_balancer(self, name, protocol, port, virtual_ips):
        url = "{0}/loadbalancers".format(self.service_url)
        token = yield self.fetch_token()
        body = json.dumps({
            "loadBalancer": {
                "name": name,
                "port": port,
                "protocol": protocol,
                "virtualIps": virtual_ips
            }
        })
        headers = {"X-Auth-Token": token, "Content-type": "application/json"}
        response = yield self.client.fetch(
            url, method="POST", headers=headers, body=body, raise_error=False)
        errors.check_service_response(response, "loadbalancers")
        lb_config = json.loads(response.body)["loadBalancer"]
        lb = LoadBalancer.from_config(lb_config)
        raise gen.Return(lb)


class LoadBalancer(object):

    @classmethod
    def from_config(cls, config):
        return cls(name=config["name"], lb_id=config["id"])

    def __init__(self, lb_id, name):
        self.id = lb_id
        self.name = name
