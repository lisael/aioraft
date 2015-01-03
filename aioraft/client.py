import asyncio
import json


class Connection:
    def __init__(self, host, port, loop):
        self.host = host
        self.port = port
        self.loop = loop
        self.open = False
        self._reader = None
        self._writer = None

    @asyncio.coroutine
    def connect(self):
        if not self.open:
            self._reader, self._writer = yield from asyncio.open_connection(
                self.host, self.port, loop=self.loop)
            self.open = True

    @asyncio.coroutine
    def read(self, n=-1):
        yield from self.connect()
        data = yield from self._reader.read(n)
        return data

    @asyncio.coroutine
    def readline(self):
        yield from self.connect()
        data = yield from self._reader.readline()
        return data

    @asyncio.coroutine
    def readexactly(self, n):
        yield from self.connect()
        data = yield from self._reader.readexactly(n)
        return data

    def write(self, data):
        return self._writer.write(data)

    def writelines(self, data):
        return self._writer.writelines(data)

    def can_write_eof(self):
        return self._writer.can_write_eof()

    def write_eof(self):
        return self._writer.write_eof()

    def get_extra_info(self, name, default=None):
        return self._writer.get_extra_info(name, default)

    def close(self):
        return self._writer.close()

    @asyncio.coroutine
    def drain(self):
        yield from self._writer.drain()



class Client:
    def __init__(self, host='127.0.0.1', port='2437', loop=None, timeout=5):
        self.loop = loop if loop else asyncio.get_event_loop()
        self.remote_host = host
        self.remote_port = port
        self.conn = Connection(host, port, loop)
        self.timeout = timeout

    @asyncio.coroutine
    def map(self, timeout=None):
        timeout = timeout if timeout is not None else self.timeout
        data = yield from asyncio.wait_for(self._map(), timeout)
        return data

    @asyncio.coroutine
    def _map(self):
        yield from self.conn.connect()
        self.conn.write(b'map\n\n')
        yield from self.conn.drain()
        code = yield from self.conn.readline()
        code = int(code)
        data = yield from self.conn.readline()
        return data

    @asyncio.coroutine
    def set(self, key, value, timeout=None):
        timeout = timeout if timeout is not None else self.timeout
        data = yield from asyncio.wait_for(self._set(key, value), timeout)
        return data

    @asyncio.coroutine
    def _set(self, key, value):
        yield from self.conn.connect()
        self.conn.writelines([b'set\n', bytes(json.dumps(dict(value=value, key=key)), 'utf-8'), b'\n'])
        yield from self.conn.drain()
        code = yield from self.conn.readline()
        code = int(code)
        data = yield from self.conn.readline()
        return data

    @asyncio.coroutine
    def get(self, key, timeout=None):
        timeout = timeout if timeout is not None else self.timeout
        data = yield from asyncio.wait_for(self._get(key), timeout)
        return data

    @asyncio.coroutine
    def _get(self, key):
        yield from self.conn.connect()
        self.conn.writelines([b'get\n',
                bytes(json.dumps(dict(key=key)), 'utf-8'), b'\n'])
        yield from self.conn.drain()
        code = yield from self.conn.readline()
        code = int(code)
        data = yield from self.conn.readline()
        return data


class NodeClient(Client):
    def __init__(self, *args, **kwargs):
        node = kwargs.pop('node')
        self.node_host = node.host
        self.node_port = node.port
        super(NodeClient, self).__init__(*args, **kwargs)

    @asyncio.coroutine
    def join(self, host, port, timeout=None):
        timeout = timeout if timeout is not None else self.timeout
        data = asyncio.wait_for(self._join(host, port), timeout)
        return data

    @asyncio.coroutine
    def _join(self, host, port):
        yield from self.conn.connect()
        self.conn.writelines([
            b'join\n',
            bytes(json.dumps(dict(host=host, port=port)),'utf-8'),
            b'\n'
        ])
        yield from self.conn.drain()
        code = yield from self.conn.readline()
        code = int(code)
        data = yield from self.conn.readline()
        return data

    @asyncio.coroutine
    def replicate(self, timeout=None, **kwargs):
        timeout = timeout if timeout is not None else self.timeout
        data = yield from asyncio.wait_for(self._replicate(kwargs), timeout)
        return data

    @asyncio.coroutine
    def _replicate(self, kwargs):
        yield from self.conn.connect()
        self.conn.writelines([b'replicate\n', bytes(json.dumps(kwargs), 'utf-8'), b'\n'])
        yield from self.conn.drain()
        code = yield from self.conn.readline()
        code = int(code)
        data = yield from self.conn.readline()
        return data
