from json import dumps

from aiohttp.web import HTTPBadRequest, HTTPInternalServerError

class JSONHTTPInternalServerError(HTTPInternalServerError):
    def __init__(
        self,
        *,
        headers=None,
        reason=None,
    ) -> None:
        super().__init__(
            headers=headers,  text=dumps(reason), content_type='application/json'
        )


class JSONHTTPBadRequest(HTTPBadRequest):
    def __init__(
        self,
        *,
        headers=None,
        reason=None,
    ) -> None:
        super().__init__(
            headers=headers,  text=dumps(reason), content_type='application/json'
        )
