import asyncio
import logging
import msgpack
import random
import socket

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
			success = send_server_message(tuple(rand), message)

	def broadcast_server_message(self, message):
		for server in self.orchestrator.server_cluster:
			send_server_message(tuple(server), message)

    def send_server_message(self, address, message):
	    try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(address)
            sock.send(msgpack.packb(message, use_bin_type=True))
        except socket.error:
        	return False
        finally:
            sock.close()
        return True

class Idle(State):
	def __init__(self, orchestrator=None):
		super().__init__(orchestrator)
		self.orchestrator.num_timeout = 0

	def data_received_command(self, transport, message):
		self.orchestrator.transport = transport
		self.orchestrator.message = message
		super().send_leader_message(message)
        self.orchestrator.change_state(InProgress)
        self.orchestrator.state.start_timer()

class InProgress(State):
	def __init__(self, orchestrator=None):
		super().__init__(orchestrator)

	def data_received_server(self, transport, message):
		self.request_timer.cancel()
		self.orchestrator.change_state(Idle)
		self.orchestrator.transport.send(message)

	def start_timer():
		timeout = 0.5
		loop = asyncio.get_event_loop()
		self.request_timer = \
			loop.call_later(timeout, self.timed_out)

	def timed_out():
		self.orchestrator.num_timeout += 1
		super().broadcast_server_message({'type': 'timeout'})

		if self.orchestrator.num_timeout > self.orchestrator.max_timeout:
			# give up
			self.orchestrator.transport.send( \
				{'type': 'result', 'success': False})
		else:
			# retry
			super().send_leader_message(self.orchestrator.message)
			self.start_timer()

class Orchestrator():
    def __init__(self, servers):
        self.state = Idle(orchestrator=self)
        self.server_cluster = servers
        self.max_timeout = 5
        self.num_timeout = 0

    def change_state(self, new_state):
        self.state = new_state(orchestrator=self)

    def data_received_command(self, transport, message):
        self.state.data_received_command(transport, message)

    def data_received_server(self, transport, message):
        self.state.data_received_server(transport, message)

class ServerProtocol(asyncio.Protocol):
    """TCP protocol for communicating with servers."""
    def __init__(self, orchestrator):
        self.orchestrator = orchestrator

    def connection_made(self, transport):
        logger.debug('Established connection with client %s:%s',
                     *transport.get_extra_info('peername'))
        self.transport = transport

    def data_received(self, data):
        message = msgpack.unpackb(data, encoding='utf-8')
        if 'server_address' in message:
			self.orchestrator.data_received_server(self, message)
		else:
			self.orchestrator.data_received_command(self, message)

    def connection_lost(self, exc):
        logger.debug('Closed connection with client %s:%s',
                     *self.transport.get_extra_info('peername'))

    def send(self, message):
        self.transport.write(msgpack.packb(
            message, use_bin_type=True, default=extended_msgpack_serializer))
        self.transport.close()