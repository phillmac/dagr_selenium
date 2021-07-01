from json import dumps

from aiohttp.web import HTTPBadRequest


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
