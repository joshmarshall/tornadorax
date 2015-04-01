from tornadorax.services.queue_service import QueueService
from tornadorax.services.storage_service import StorageService
from tornadorax.services.load_balancer_service import LoadBalancerService


SERVICES = {}


def register_service(service_type, service_class):
    SERVICES[service_type] = service_class


# registering default services included in this project
register_service("rax:queues", QueueService)
register_service("rax:load-balancer", LoadBalancerService)
register_service("object-store", StorageService)


def build_service(
        service_type, service_catalog, fetch_token, ioloop, region=None,
        internal=False):

    if service_type not in SERVICES:
        raise InvalidServiceType(
            "Unregistered service type {0}".format(service_type))

    services = dict([(s["type"], s) for s in service_catalog])

    if service_type not in services:
        raise InvalidServiceType(
            "Server catalog does not contain service type {0}".format(
                service_type))

    # some of this could be left to an internal service classmethod
    # if it turns out these structures deviate widely across providers
    # and services

    endpoints = services[service_type]["endpoints"]
    url_key = "internalURL" if internal else "publicURL"
    regions = dict([(e["region"], e) for e in endpoints])
    region_keys = [e["region"] for e in endpoints]
    region_key = region or region_keys[0]

    if region_key not in regions:
        raise InvalidRegion(
            "Unknown region {0} for service {1}".format(
                region_key, service_type))

    service_url = regions[region_key][url_key]
    return SERVICES[service_type](service_url, fetch_token, ioloop)


class InvalidRegion(Exception):
    pass


class InvalidServiceType(Exception):
    pass
