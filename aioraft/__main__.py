import argparse
import json
import asyncio
import logging

from aioraft.node import init, close
from aioraft import settings

log = logging.getLogger('aioraft')

def run(args):
    loop = asyncio.get_event_loop()
    if args.peers:
        peers = [tuple(p.split(':')) for p in args.peers.split(',')]
    else:
        peers = []
    node, server = init(args.host, args.port, peers)
    log.info('Serving on {}'.format(server.sockets[0].getsockname()))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    close(server)
    loop.close()

parser = argparse.ArgumentParser(prog="aioraft")

# global args
parser.add_argument("--host", "-H",
                    help="Host (default '"
                    + settings.HOST + "')",
                    default=settings.HOST)
parser.add_argument("--port", "-P",
                    help="Port (default '" + settings.PORT + "')",
                    default=settings.PORT)
PEERS = ','.join([p.strip() for p in settings.PEERS])
parser.add_argument("--peers", "-p",
                    help="Comma separated list of <host>:<port> peers (default '"
                    + PEERS + "')", default=PEERS)
args = parser.parse_args()

run(args)
