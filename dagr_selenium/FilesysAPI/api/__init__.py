from dagr_selenium.JSONHTTPErrors import (JSONHTTPBadRequest,
                                          JSONHTTPInternalServerError)


class APIManager():
    from . import APIv0
    from . import APIv1
    api_versions = {
        'v0': APIv0,
        'v1': APIv1
    }

    @classmethod
    async def handle_request(self, request, handler_method):
        if handler := APIManager.api_versions.get(request.headers.get('api-version', 'v0'), None) is None:
            raise JSONHTTPBadRequest(reason='Invalid api version')

        if not hasattr(handler, handler_method):
            print(dir(handler))
            print(f"Invalid handler name:'{handler_method}'")
            raise JSONHTTPInternalServerError(reason='Invalid handler name')

        return await (getattr(handler, handler_method)(request))