#!/usr/local/bin/python3.4
import asyncio
import json

from aiohttp import web

import outgoing

DEFAULT_CHANNEL = 1

@asyncio.coroutine
def handle(request):
    channel = request.match_info.get('channel_id', DEFAULT_CHANNEL)

    data = yield from request.json()
    data.update({'channel': channel})

    msg = outgoing.Msg(**data)
    enqueued = outgoing.send_msg(msg)

    print('{0} tasks enqueued'.format(outgoing.queue.qsize()))

    # start a worker task in case there arent any running
    loop.create_task(outgoing.consume_outgoing('worker {0}'.format(msg.uuid),
                                               outgoing.queue))

    payload = {'result': {'id': msg.uuid}, 'description': 'message submitted'}
    return web.Response(body=json.dumps(payload).encode('utf-8'))


@asyncio.coroutine
def serve(loop):
    app = web.Application(loop=loop)
    app.router.add_route('POST', '/channel/{channel_id:\d+}', handle)

    handler = app.make_handler()
    srv = yield from loop.create_server(handler,
                                        '127.0.0.1', 8080)
    print("Server started at http://127.0.0.1:8080")
    return srv

loop = asyncio.get_event_loop()
loop.run_until_complete(serve(loop))

try:
    loop.run_forever()
except KeyboardInterrupt:
    pass

loop.close()
