#! /usr/bin/env python3

from aiohttp import web
from pathlib import Path

async def add_url(request):
    post_contents = await request.post()
    url = post_contents.get('url')
    if url is None:
        return web.HTTPBadRequest(reason='not ok: url missing')
    with Path('dagr_urls').open('a') as p:
        print(url, file=p)
    print(f"Added {url} to buffer")
    return web.Response(text='ok')

def run_app():
    app = web.Application()
    app.router.add_post('/url', add_url)
    web.run_app(app, host='192.168.42.226', port=3002)

if __name__ == '__main__':
    print('Staring http server')
    run_app()
