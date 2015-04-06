import datetime


FUTURE_EXPIRATION = datetime.datetime.utcnow() + \
    datetime.timedelta(days=60)


KEYSTONE_RESPONSE = {
    "access": {
        "token": {
            "expires": FUTURE_EXPIRATION.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "id": "TOKEN"
        },
        "serviceCatalog": [
            {
                "endpoints": [
                    {
                        "internalURL": "https://dfw.internal.com/v1",
                        "publicURL": "https://dfw.public.com/v1",
                        "region": "DFW"
                    },
                    {
                        "internalURL": "https://ord.internal.com/v1",
                        "publicURL": "https://ord.public.com/v1",
                        "region": "ORD"
                    }
                ],
                "type": "foo:service",
                "name": "Foo Service"
            },
            {
                "endpoints": [
                    {
                        "internalURL": "https://ord.queuest.com/v1",
                        "publicURL": "{{QUEUE_URL}}/v1",
                        "region": "ORD"
                    },
                    {
                        "internalURL": "https://dfw.queues.com/v1/111",
                        "publicURL": "https://dfw-int.queues.com/v1/111",
                        "region": "DFW"
                    }
                ],
                "name": "cloudQueues",
                "type": "rax:queues"
            }
        ]
    }
}
