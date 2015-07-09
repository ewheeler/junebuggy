##Junebuggy

Sketch of some Python 3.4 code based on [Junebuggy's specs](https://docs.google.com/document/d/1emR0gwVPK2Xzh-qe3brD2moDMLytOmVALI3YUtd2HeA/edit)
to see how it feels to use `asyncio` for queueing and routing short messages.

#Installation:
clone this repo, then
```
$ pip3.4 install aiohttp
```

#Usage: 
```
$ python3.4 server.py
```
and then
```
$ curl -H "Content-Type: application/json" -X POST -d '{"to":"8675309","sender":"8500","priority":"1","content":"foo","event_url":"http://requestb.in/q9pf40q9"}' http://localhost:8080/channel/2
```
