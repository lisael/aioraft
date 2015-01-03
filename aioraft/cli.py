import argparse
import os
import asyncio
import sys
import logging
import json
import pprint

from asyncio.streams import StreamWriter, FlowControlMixin

from aioraft.client import Client
from aioraft import settings


log = logging.getLogger('aioraft.cli')
client, reader, writer = None, None, None


@asyncio.coroutine
def stdio(loop=None):
    if loop is None:
        loop = asyncio.get_event_loop()

    reader = asyncio.StreamReader()
    reader_protocol = asyncio.StreamReaderProtocol(reader)

    writer_transport, writer_protocol = yield from loop.connect_write_pipe(FlowControlMixin, os.fdopen(0, 'wb'))
    writer = StreamWriter(writer_transport, writer_protocol, None, loop)

    yield from loop.connect_read_pipe(lambda: reader_protocol, sys.stdin)

    return reader, writer

@asyncio.coroutine
def async_input(message):
    if isinstance(message, str):
        message = message.encode('utf8')

    global reader, writer
    if (reader, writer) == (None, None):
        reader, writer = yield from stdio()

    writer.write(message)
    line = yield from reader.readline()
    return line.decode('utf8').strip()



ARGS_CMDS = {'set', 'get'}
SIMPLE_CMDS = {'map'}
CMDS = ARGS_CMDS.union(SIMPLE_CMDS)


class CLIParser:
    @staticmethod
    def parse_get(args):
        # TODO: check 1 arg
        return [args]

    @staticmethod
    def parse_set(args):
        key, value = args.split(' ', 1)
        return key, value

@asyncio.coroutine
def main(args):
    global client
    client = Client(args.host, args.port)
    while True:
        command = yield from async_input("raft://%s:%s > " % (client.remote_host, client.remote_port))
        command = command.strip()
        chunks = command.split(' ', 1)
        if not any(chunks):
            # empty line
            continue
        cmd = chunks[0]
        if len(chunks) == 1:
            # simple command. TODO: check
            result = yield from getattr(client, cmd)()
        else:
            # comand w/ args
            result = yield from getattr(client, cmd)(*getattr(CLIParser, "parse_%s" % cmd)(chunks[1]))
        result = json.loads(result.decode('utf-8'))
        print(json.dumps(result, sort_keys=True, indent=2))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="aioraft")
    # global args
    parser.add_argument("--host", "-H",
                        help="Host (default '"
                        + settings.HOST + "')",
                        default=settings.HOST)
    parser.add_argument("--port", "-P",
                        help="Port (default '" + settings.PORT + "')",
                        default=settings.PORT)
    args = parser.parse_args()

    try:
        asyncio.get_event_loop().run_until_complete(main(args))
    except KeyboardInterrupt:
        log.info("closing connection")
        client.conn.close()
        asyncio.get_event_loop().close()



