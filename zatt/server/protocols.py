import asyncio
import os
import msgpack
import logging
import socket
from .states import Follower
from .config import config
from .utils import extended_msgpack_serializer

logger = logging.getLogger(__name__)


class Orchestrator():
    """The orchestrator manages the current node state,
    switching between Follower, Candidate and Leader when necessary.
    Only one Orchestrator """
    def __init__(self):
        os.makedirs(config.storage, exist_ok=True)
        self.state = Follower(orchestrator=self)
        self.config = config

    def change_state(self, new_state):
        self.state.teardown()
        logger.debug('State change:' + new_state.__name__)
        self.state = new_state(old_state=self.state)

    def data_received_peer(self, sender, message):
        self.state.data_received_peer(sender, message)

    def data_received_client(self, transport, message):
        self.state.data_received_client(transport, message)

    def send(self, transport, message):
        transport.sendto(msgpack.packb(message, use_bin_type=True,
                         default=extended_msgpack_serializer))

    def send_peer(self, recipient, message):
        if recipient != self.state.volatile['address']:
            self.peer_transport.sendto(
                msgpack.packb(message, use_bin_type=True), tuple(recipient))

    def broadcast_peers(self, message):
        for recipient in self.state.volatile['cluster']:
            self.send_peer(recipient, message)

    def redir_leader(self, address, message):
        self.send_message(address, message)

    def send_client(self, address, message):
        msg = message.copy()
        self.send_message(address, msg)

    def send_message(self, address, message):
        address = tuple(address)
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(address)
            sock.send(msgpack.packb(message, use_bin_type=True))
        except socket.error:
            return False
        finally:
            sock.close()
        return True


class PeerProtocol(asyncio.Protocol):
    """UDP protocol for communicating with peers."""
    def __init__(self, orchestrator, first_message=None):

        self.orchestrator = orchestrator
        self.first_message = first_message

    def connection_made(self, transport):
        self.transport = transport
        if self.first_message:
            transport.sendto(
                msgpack.packb(self.first_message, use_bin_type=True))

    def datagram_received(self, data, sender):
        message = msgpack.unpackb(data, encoding='utf-8')
        self.orchestrator.data_received_peer(sender, message)

    def error_received(self, ex):
        print('Error:', ex)


class ClientProtocol(asyncio.Protocol):
    """TCP protocol for communicating with clients."""
    def __init__(self, orchestrator):
        self.orchestrator = orchestrator

    def connection_made(self, transport):
        logger.debug('Established connection with client %s:%s',
                     *transport.get_extra_info('peername'))
        self.transport = transport

    def data_received(self, data):
        message = msgpack.unpackb(data, encoding='utf-8')
        self.orchestrator.data_received_client(self, message)

    def connection_lost(self, exc):
        logger.debug('Closed connection with client %s:%s',
                     *self.transport.get_extra_info('peername'))

    def send(self, message):
        self.transport.write(msgpack.packb(
            message, use_bin_type=True, default=extended_msgpack_serializer))
        self.transport.close()
