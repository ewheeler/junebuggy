#!/usr/local/bin/python3.4
import uuid
import asyncio
import collections
import json
import datetime

import aiohttp


# TODO channel class and per-channel send_urls
SEND_URL = 'http://requestb.in/q9pf40q9'

HIGH_PRIORITY = 1
NORMAL_PRIORITY = 5

queue = asyncio.PriorityQueue()


@asyncio.coroutine
def call_external_service(msg_task):
    """ Call external aggregator or MNO to send msg."""
    payload = {'to': msg_task.msg.to, 'from': msg_task.msg.sender,
               'message_id': msg_task.msg.uuid, 'text': msg_task.msg.content,
               'timestamp': datetime.datetime.utcnow().isoformat()}

    response = yield from aiohttp.request(method='POST',
                                          url=SEND_URL,
                                          data=json.dumps(payload))
    resp = yield from response.read()
    print('{0} external service says: {1} {2}'.format(msg_task.msg.uuid,
                                                      response.status,
                                                      resp.decode()))
    assert response.status == 200
    return response.status


@asyncio.coroutine
def callback_with_status(result, msg_task):
    """ Call event_url provided by caller with
        status of external service request."""
    print('{0} callback to event_url for {1}'.format(msg_task.msg.uuid,
                                                     msg_task.msg.content))

    payload = {'event_type': 'delivery_succeeded', 'message_id': msg_task.msg.uuid,
               'channel_id': msg_task.msg.channel, 'event_details': str(result),
               'timestamp': datetime.datetime.utcnow().isoformat()}

    response = yield from aiohttp.request(method='POST',
                                          url=msg_task.msg.event_url,
                                          data=json.dumps(payload))
    resp = yield from response.read()
    print('{0} event_url says: {1} {2}'.format(msg_task.msg.uuid,
                                               response.status, resp.decode()))
    assert response.status == 200


@asyncio.coroutine
def consume_outgoing(worker, work_queue):
    """ Consume queued outgoing messages."""
    while not work_queue.empty():
        msg_task = yield from work_queue.get()
        print('{0} grabbed item: {1}'.format(worker, msg_task.msg.content))

        result = yield from call_external_service(msg_task)
        print('{0} item: {1} resulted in status: {2}'.format(worker,
                                                             msg_task.msg.content,
                                                             result))

        if msg_task.msg.event_url is not None:
            yield from callback_with_status(result, msg_task)


def send_msg(msg):
    """ Enqueued outgoing messages."""
    try:
        print('enqueuing: {0}'.format(msg))
        queue.put_nowait(MsgTask(msg.priority, msg))
        return True
    except asyncio.QueueFull:
        return False


class Msg(object):
    def __init__(self, to=None, sender=None, content=None, priority=NORMAL_PRIORITY,
                 event_url=None, channel=None, channel_data=None, reply_to=None):
        assert ((to is not None) or (reply_to is not None))
        self.uuid = uuid.uuid4().hex
        self.to = to
        self.sender = sender
        self.content = content
        self.priority = int(priority)
        self.event_url = event_url
        self.channel = channel
        self.channel_data = channel_data
        self.reply_to = reply_to

    def __lt__(self, other):
        return self.priority < other.priority


# entries for priority queues have to be tuples of (priority, data),
# so use a namedtuple for maximum prettiness
MsgTask = collections.namedtuple('MsgTask', ['priority', 'msg'])


if __name__ == "__main__":
    print('testing outgoing queue consumption')
    loop = asyncio.get_event_loop()

    for n in range(20):
        msg = Msg(to=n*100, sender='me', content=n, priority=NORMAL_PRIORITY,
                  event_url='http://requestb.in/q9pf40q9')
        send_msg(msg)

    # this Msg with higher priority will be consumed before the
    # previously-enqueued lower priority msgs
    msg = Msg(to=5000, sender='me', content=50, priority=HIGH_PRIORITY,
              event_url='http://requestb.in/q9pf40q9')
    send_msg(msg)

    print('{0} tasks enqueued'.format(queue.qsize()))

    consumer_tasks = [
        asyncio.ensure_future(consume_outgoing('worker1', queue)),
        asyncio.ensure_future(consume_outgoing('worker2', queue))]

    loop.run_until_complete(asyncio.wait(consumer_tasks))

    loop.close()
