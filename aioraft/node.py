import asyncio
import json
import random
from math import ceil
import logging

from aioraft.client import NodeClient
from aioraft.log import action_map, SetLogEntry
from aioraft.entry import DirEntry


logger = logging.getLogger(__name__)


class Node:
    def __init__(self, host, port, peers=None, loop=None):
        self.host = host
        self.port = port
        self.loop = loop if loop else asyncio.get_event_loop()
        self.peers = set(peers) if peers is not None else set()
        self.raft_index = 0
        self._leader_node = tuple()
        self._leader = None
        self.heartbeat_timeout = 0.5
        self.term = 0
        self.current_term = 0
        self.data = DirEntry('root', node=self)
        self._clients = [NodeClient(*peer, loop=self.loop, timeout=0.5, node=self) for peer in self.peers]
        self.pending_logs = {}

    @property
    def term_timeout(self):
        # between 150 and 300
        return random.random() / 6.6 + 0.150

    @asyncio.coroutine
    def map(self, writer):
        map_ = dict(term=self.term, raft_index=self.raft_index,
            leader=self._leader_node, peers=list(self.peers))
        map_ = bytes(json.dumps(map_), 'ascii')
        writer.writelines([b'200\n', map_, b'\n'])
        yield from writer.drain()

    @asyncio.coroutine
    def leader(self, writer):
        raise NotImplementedError('`get` not implemented on %s instances' %
            self.__class__)

    @asyncio.coroutine
    def get(self, writer, args):
        raise NotImplementedError('`get` not implemented on %s instances' %
            self.__class__)

    @asyncio.coroutine
    def set(self, writer, args):
        raise NotImplementedError('`set` not implemented on %s instances' %
            self.__class__)

    @asyncio.coroutine
    def term(self, writer, args):
        raise NotImplementedError('`term` not implemented on %s instances' %
            self.__class__)

    @asyncio.coroutine
    def beat(self, writer, args):
        raise NotImplementedError('`beat` not implemented on %s instances' %
            self.__class__)

    @asyncio.coroutine
    def join(self, writer, args):
        raise NotImplementedError('`join` not implemented on %s instances' %
            self.__class__)

    @asyncio.coroutine
    def leave(self, writer, args):
        raise NotImplementedError('`leave` not implemented on %s instances' %
            self.__class__)

    @asyncio.coroutine
    def _handler(self, reader, writer):
        logger.debug('handle')
        while True:
            cmd = yield from reader.readline()
            method = cmd.decode().strip()
            if method not in self.implements:
                writer.writelines([b'500\n',bytes('ERROR : method `%s` not implemented\n' % method.strip(), 'ascii')])
                yield from writer.drain()
                continue
            args = yield from reader.readline()
            args = args.decode('utf-8').strip()
            if args:
                args = json.loads(args)
            else:
                args = dict()
            try:
                yield from getattr(self, method)(writer, **args)
            except Exception as e:
                raise
                writer.writelines([b'500\n',bytes('ERROR : %s' % e, 'utf-8\n')])
                yield from writer.drain()


class Follower(Node):
    implements = set(['get', 'set', 'map', 'replicate'])

    def __init__(self, *args, **kwargs):
        super(Follower, self).__init__(*args, **kwargs)
        self.loop.create_task(self.async_init())

    @asyncio.coroutine
    def async_init(self):
        yield from self.update_map()
        if self._leader:
            logger.info('joining')
            yield from self._leader.join(self.host, self.port)
        elif not self.peers:
            logger.info('No other peers, I am the leader')
            # no peers, promote oneself
            self.__class__ = Leader
            self._leader = None
            self.term = 1
            self._leader_node = (self.host, self.port)

    @asyncio.coroutine
    def broadcast(self, function, attrs, exclude=None, wait_majority=False):
        """ Broacast :arg:function to all clients except hosts in :arg:exclude.
            If :arg:wait_majority is True, return as soon as the majority of
            clients returned without error
            If :arg:wait_majority is an integer, return as soon as
            :arg:wait_majority clients returned without error
        """
        exclude = exclude if exclude is not None else []
        coros = [getattr(c, function)(**attrs)
            for c in self.clients
            if (c.host, c.port) not in exclude
        ]
        if wait_majority:
            # let the coro finnish even after we return
            coros = [asyncio.shield(c) for c in coros]
            if wait_majority is True:
                maj = ceil((len(coros) + len(exclude)) / 2.)
            else:
                maj = wait_majority
            success = 0
            while True:
                w = asyncio.wait(coros,
                    loop=self.loop, return_when=asyncio.FIRST_COMPLETED)
                try:
                    done, coros = yield from asyncio.wait_for(w,
                        loop=self.loop, timeout=self.heartbeat_timeout)
                except asyncio.TimeoutError:
                    [c.cancel() for c in coros]
                    return None, None
                success += len([d for d in done if d.exception() is None])
                if success >= maj:
                    return done, coros
                if success + len(coros) < maj:
                    # not any chance to succed
                    [c.cancel() for c in coros]
                    return False
        else:
            done, pending = yield from asyncio.wait(coros, loop=self.loop,
                timeout=self.heartbeat_timeout)
            [c.cancel() for c in pending]
            return done, pending

    def update_clients(self):
        current_clients = set([(c.remote_host, c.remote_port) for c in self._clients])
        new_clients = self.peers.difference(current_clients)
        for c in new_clients:
            self._clients.append(NodeClient(*c, loop=self.loop, node=self))

    @asyncio.coroutine
    def update_map(self):
        clients = self.clients
        if clients:
            term, leader = self.term, self._leader_node
            for c in clients:
                map_ = yield from c.map()
                result = json.loads(map_.decode())
                print(result)
                for p in result['peers']:
                    self.peers.add(p)
                # chose the leader if it's in a higher term
                term, leader = max((term, leader), (result['term'], tuple(result['leader'])))
            self.term, self._leader_node = max((self.term, self._leader_node), (term, leader))
            self.update_clients()
            if self._leader_node:
                self._leader = NodeClient(*self._leader_node, loop=self.loop, node=self)

    @asyncio.coroutine
    def get(self, writer, key):
        logger.debug('get %s' % key)
        data = self.data.get_entry(key, create=False)
        writer.write(b'200\n')
        writer.write(bytes(json.dumps(dict(
            key=key,
            value=data.value,
            index=data.index)), 'utf-8'))
        writer.write(b'\n')
        yield from writer.drain()

    @asyncio.coroutine
    def replicate(self, writer, **kwargs):
        index = kwargs['raft_index']
        log = self.pending_logs.get(index, None)
        if log is None:
            action = kwargs.pop('action')
            log = action_map[action](self, **kwargs)
            self.pending_logs[log.raft_index] = log
        else:
            log.commit()
        writer.write(b'200\n\n')
        yield from writer.drain()

    @asyncio.coroutine
    def join(self, writer, host, port):
        result = yield from self._leader.join(host, port)
        writer.write(b'200\n%s\n' % result)
        yield from writer.drain()


    @property
    def clients(self):
        if self._leader:
            return [self._leader] + self._clients
        else:
            return self._clients


class Leader(Follower):
    implements = set(Follower.implements)
    implements.update({'join'})

    @property
    def clients(self):
        return self._clients

    @asyncio.coroutine
    def join(self, writer, host, port):
        self.peers.add((host, port))
        self.set(None, '_raft/peers/%s:%s' % (host, port), '')
        self.update_clients()
        writer.write(b'200\n\n')
        yield from writer.drain()

    @asyncio.coroutine
    def set(self, writer, key, value):
        logger.debug('set %s %s' % (key, value))
        entry = SetLogEntry(self, self.next_index, key, value)
        if self._clients:
            data = yield from entry.replicate()
        else:
            data = entry.commit()
        if writer is None:
            return
        writer.write(b'200\n')
        writer.write(bytes(json.dumps(dict(
            key=key,
            value=value,
            index=entry.raft_index)), 'utf-8'))
        writer.write(b'\n')
        yield from writer.drain()

    @property
    def next_index(self):
        self.raft_index += 1
        return self.raft_index


def init(host='127.0.0.1', port='2437', peers=None, loop=None):
    loop = loop if loop else asyncio.get_event_loop()
    node = Follower(host=host, port=port, peers=peers, loop=loop)
    server_coro = asyncio.start_server(node._handler, host, port, loop=loop)
    server = loop.run_until_complete(server_coro)
    return node, server

def close(server, loop = None):
    loop = loop if loop else asyncio.get_event_loop()
    server.close()
    loop.run_until_complete(server.wait_closed())

