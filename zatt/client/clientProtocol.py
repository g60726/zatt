import asyncio
import logging
import msgpack
import random
import socket
import json
from zatt.common import crypto
from zatt.server.utils import extended_msgpack_serializer

logger = logging.getLogger(__name__)

class State:
    def __init__(self, orchestrator=None):
        self.orchestrator = orchestrator

    def data_received_command(self, transport, message):
        pass # Do nothing if InProgress state

    def data_received_server(self, transport, message):
        pass # Do nothing if Idle state

    def send_leader_message(self, message):
        success = False
        while not success: # Keep trying until a live server is found
            rand = random.choice(self.orchestrator.server_cluster)
            success = self.send_server_message(tuple(rand), message)

    def broadcast_server_message(self, message):
        for server in self.orchestrator.server_cluster:
            self.send_server_message(tuple(server), message)

    def send_server_message(self, address, message):
        msg = message.copy()
        msg['client'] = self.orchestrator.config.client_address
        msg['req_id'] = self.orchestrator.req_id
        signature = crypto.sign_message( \
            json.dumps(msg), \
            self.orchestrator.private_key)
        signed = [json.dumps(msg), signature]
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(address)
            sock.send(msgpack.packb(signed, use_bin_type=True))
        except socket.error:
            return False
        finally:
            sock.close()
        return True

    def send_message(self, message):
        if message['type'] == 'get':
            self.broadcast_server_message(message)
        elif message['type'] == 'append':
            self.send_leader_message(message)
        else:
            self.send_leader_message(message)

class Idle(State):
    def __init__(self, orchestrator=None):
        super().__init__(orchestrator)
        self.orchestrator.req_id += 1

    def data_received_command(self, transport, message):
        self.orchestrator.transport = transport
        self.orchestrator.message = message

        super().send_message(message)

        self.orchestrator.change_state(InProgress)
        self.orchestrator.state.start_timer()

class InProgress(State):
    def __init__(self, orchestrator=None):
        super().__init__(orchestrator)
        self.responses = {}
        self.retry_counter = 0

    def data_received_server(self, transport, message):
        logger.debug(message)
        if message['req_id'] == self.orchestrator.req_id:
            self.responses[tuple(message['server_address'])] = message
        if len(self.responses) >= self.orchestrator.quorum:
            self.request_timer.cancel()
            self.orchestrator.change_state(Idle)
            self.orchestrator.transport.send(message)

    def start_timer(self):
        timeout = 0.5
        loop = asyncio.get_event_loop()
        self.request_timer = \
            loop.call_later(timeout, self.timed_out)

    def timed_out(self):
        self.retry_counter += 1
        if self.retry_counter > self.orchestrator.retry_attempts:
            super().broadcast_server_message({'type': 'timeout'})
            self.orchestrator.change_state(Idle)
            self.orchestrator.transport.send( \
                {'type': 'result', 'success': False})
            logger.info("Time out in client protocol!!")
        else:
            # Retry
            logger.info("Retransmitting: " + str(self.retry_counter))
            super().send_message(self.orchestrator.message)
            self.start_timer()

class Orchestrator():
    def __init__(self, config):
        self.server_cluster = list(config.cluster)
        self.config = config
        self.public_keys = config.public_keys
        self.private_key = config.client_private_key
        self.req_id = 0
        self.retry_attempts = 3
        self.quorum = 2 # TODO: LE calculate based on the server_cluster
        self.state = Idle(orchestrator=self)

    def change_state(self, new_state):
        self.state = new_state(orchestrator=self)

    def data_received_command(self, transport, message):
        self.state.data_received_command(transport, message)

    def data_received_server(self, transport, message):
        actualMsg = json.loads(message[0])
        isValid = crypto.verify_message( \
            message[0], \
            self.public_keys[tuple(actualMsg['server_address'])], \
            message[1])
        if not isValid:
            return
        self.state.data_received_server(transport, actualMsg)

class ServerProtocol(asyncio.Protocol):
    """TCP protocol for communicating with servers."""
    def __init__(self, orchestrator):
        self.orchestrator = orchestrator

    def connection_made(self, transport):
        logger.debug('Established connection with %s:%s',
                     *transport.get_extra_info('peername'))
        self.transport = transport

    def data_received(self, data):
        message = msgpack.unpackb(data, encoding='utf-8')
        # TODO: make a better distinction between server and command
        if not isinstance(message, dict):
            self.orchestrator.data_received_server(self, message)
        else:
            self.orchestrator.data_received_command(self, message)

    def connection_lost(self, exc):
        logger.debug('Closed connection with %s:%s',
                     *self.transport.get_extra_info('peername'))

    def send(self, message):
        self.transport.write(msgpack.packb(
            message, use_bin_type=True, default=extended_msgpack_serializer))
        self.transport.close()