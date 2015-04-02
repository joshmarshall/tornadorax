LIST_RESPONSE = {
    "loadBalancers": [
        {
            "name": "lb1",
            "id": 1,
            "protocol": "HTTP",
            "port": 80,
            "algorithm": "RANDOM",
            "status": "ACTIVE",
            "nodeCount": 3,
            "virtualIps": [
                {
                    "id": 401,
                    "address": "206.55.130.1",
                    "type": "PUBLIC",
                    "ipVersion": "IPV4"
                }
            ],
            "created": {
                "time": "2010-11-30T03:23:42Z"
            },
            "updated": {
                "time": "2010-11-30T03:23:44Z"
            }
        },
        {
            "name": "lb2",
            "id": 2,
            "protocol": "HTTPS",
            "port": 443,
            "algorithm": "RANDOM",
            "status": "ACTIVE",
            "nodeCount": 4,
            "virtualIps": [
                {
                    "id": 402,
                    "address": "206.55.130.2",
                    "type": "PUBLIC",
                    "ipVersion": "IPV4"
                }
            ],
            "created": {
                "time": "2010-11-30T03:23:42Z"
            },
            "updated": {
                "time": "2010-11-30T03:23:44Z"
            }
        }
    ]
}


CREATE_RESPONSE = {
    "loadBalancer": {
        "name": "a-new-loadbalancer",
        "id": 3,
        "protocol": "HTTP",
        "halfClosed": "true",
        "port": 80,
        "algorithm": "RANDOM",
        "status": "BUILD",
        "timeout": 30,
        "cluster": {
            "name": "cluster1"
        },
        "nodes": [],
        "virtualIps": [
            {
                "address": "206.10.10.210",
                "id": 39,
                "type": "PUBLIC",
                "ipVersion": "IPV4"
            }
        ],
        "connectionLogging": {
            "enabled": False
        }
    }
}

GET_RESPONSE = {
    "loadBalancer": {
        "name": "a-new-loadbalancer",
        "id": 3,
        "protocol": "HTTP",
        "halfClosed": "true",
        "port": 80,
        "algorithm": "RANDOM",
        "status": "BUILD",
        "timeout": 30,
        "cluster": {
            "name": "cluster1"
        },
        "nodes": [],
        "virtualIps": [
            {
                "address": "206.10.10.210",
                "id": 39,
                "type": "PUBLIC",
                "ipVersion": "IPV4"
            }
        ],
        "connectionLogging": {
            "enabled": False
        }
    }
}
