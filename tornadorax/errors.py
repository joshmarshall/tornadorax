class ServiceError(Exception):
    # raised for 500 or other error code

    def __init__(self, message, code, body):
        self.code = code
        self.body = body
        super(ServiceError, self).__init__(message)


def check_service_response(response, service):
    if response.code >= 400:
        message = "Service {0} failed with {1}\n{2}".format(
            service, response.code, response.body)
        raise ServiceError(message, code=response.code, body=response.body)
