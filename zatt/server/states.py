import asyncio
import logging
import statistics
from random import randrange
from os.path import join
from .utils import PersistentDict, TallyCounter
from .log import LogManager
from .config import config
from collections import defaultdict
from zatt.common import crypto
import math
import json

logger = logging.getLogger(__name__)


class State:
    """Abstract state for subclassing."""
    def __init__(self, old_state=None, orchestrator=None):
        """State is initialized passing an orchestator instance when first
        deployed. Subsequent state changes use the old_state parameter to
        preserve the environment.
        """
        if old_state:
            self.orchestrator = old_state.orchestrator
            self.persist = old_state.persist
            self.volatile = old_state.volatile
            self.log = old_state.log
        else:
            self.orchestrator = orchestrator
            self.persist = PersistentDict(join(config.storage, 'state'),
                                          {'votedFor': None, 'currentTerm': 0})
            self.volatile = {'leaderId': None, 'cluster': config.cluster,
                'address': config.address, 'private_key': config.private_key,
                'public_keys': config.public_keys, 'clients': config.clients,
                'client_keys': config.client_keys}
            self.log = LogManager()

    def data_received_peer(self, peer, msg):
        """Receive peer messages from orchestrator and pass them to the
        appropriate method."""
        # TODO: remove this check once we signed everthing
        actualMsg = msg
        if not isinstance(msg, dict):
            isValid = crypto.verify_message(msg[0], self.volatile['public_keys'][tuple(peer)], msg[1])
            if not isValid:
                return
            actualMsg = json.loads(msg[0])

        logger.debug('Received %s from %s', actualMsg['type'], peer)

        # TODO: Dennis, there should be some sort of security check here...
        if self.persist['currentTerm'] < actualMsg['term']:
            self.persist['currentTerm'] = actualMsg['term']
            if not type(self) is Follower:
                logger.info('Remote term is higher, converting to Follower')
                self.orchestrator.change_state(Follower)
                self.orchestrator.state.data_received_peer(peer, actualMsg)
                return
        method = getattr(self, 'on_peer_' + actualMsg['type'], None)
        if method:
            if actualMsg['type'] == 'response_append':
                method(peer, msg) # msg = [string representation of message, signature of message]
            else:    
                method(peer, actualMsg)
        else:
            logger.info('Unrecognized message from %s: %s', peer, actualMsg)

    def data_received_client(self, protocol, msg):
        """Receive client messages from orchestrator and pass them to the
        appropriate method."""
        method = getattr(self, 'on_client_' + msg['type'], None)
        if method:
            method(protocol, msg)
        else:
            logger.info('Unrecognized message from %s: %s',
                        protocol.transport.get_extra_info('peername'), msg)

    def on_client_append(self, protocol, msg):
        """Redirect client to leader upon receiving a client_append message."""
        if self.volatile['leaderId']:
            self.orchestrator.redir_leader( \
                tuple(self.volatile['leaderId']), msg)
            logger.info('Redirect client %s:%s to leader',
                         *protocol.transport.get_extra_info('peername'))

    def on_client_get(self, protocol, msg):
        """Return state machine to client."""
        state_machine = self.log.state_machine.data.copy()
        self.orchestrator.send_client(msg['client'], \
            {'type': 'result', 'success': True, 'req_id': msg['req_id'], \
                'data': state_machine})

    # Given a msg (a dict), sign it an return a list with the strignified msg and its signature
    def sign_message(self, msg):
        signature = crypto.sign_message(json.dumps(msg), self.volatile['private_key'])
        return [json.dumps(msg), signature]


class Follower(State):
    """Follower state."""
    def __init__(self, old_state=None, orchestrator=None):
        """Initialize parent and start election timer."""
        super().__init__(old_state, orchestrator)
        self.persist['votedFor'] = None
        self.restart_election_timer()
        self.waiting_clients = {}

    def teardown(self):
        """Stop timers before changing state."""
        self.election_timer.cancel()
        for clients in self.waiting_clients.values():
            for client in clients:
                self.orchestrator.send_client(client['addr'], \
                    {'type': 'result', 'success': False, \
                        'req_id': client['req_id']})
                logger.error('Sent unsuccessful response to client')

    def restart_election_timer(self):
        """Delays transition to the Candidate state by timer."""
        if hasattr(self, 'election_timer'):
            self.election_timer.cancel()

        timeout = randrange(1, 4) * 10 ** (0 if config.debug else -1)
        loop = asyncio.get_event_loop()
        self.election_timer = loop.\
            call_later(timeout, self.orchestrator.change_state, Candidate)
        logger.debug('Election timer restarted: %s s', timeout)

    def on_peer_request_vote(self, peer, msg):
        """Grant this node's vote to Candidates."""
        term_is_current = msg['term'] >= self.persist['currentTerm']
        can_vote = self.persist['votedFor'] in [tuple(msg['candidateId']),
                                                None]
        index_is_current = (msg['lastLogTerm'] > self.log.term() or
                            (msg['lastLogTerm'] == self.log.term() and
                             msg['lastLogIndex'] >= self.log.index))
        granted = term_is_current and can_vote and index_is_current

        if granted:
            self.persist['votedFor'] = msg['candidateId']
            self.restart_election_timer()

        logger.debug('Voting for %s. Term:%s Vote:%s Index:%s',
                     peer, term_is_current, can_vote, index_is_current)

        response = {'type': 'response_vote', 'voteGranted': granted,
                    'term': self.persist['currentTerm']}
        response = self.sign_message(response)
        self.orchestrator.send_peer(peer, response)

    def on_peer_update_waiting_clients(self, peer, msg):
        new_client = {'addr': msg['client'], 'req_id': msg['req_id']}
        if msg['logIndex'] in self.waiting_clients:
            self.waiting_clients[msg['logIndex']].append(new_client)
        else:
            self.waiting_clients[msg['logIndex']] = [new_client]


    def on_peer_append_entries(self, peer, msg):
        """Manages incoming log entries from the Leader.
        Data from log compaction is always accepted.
        In the end, the log is scanned for a new cluster config.
        """
        self.restart_election_timer()

        term_is_current = msg['term'] >= self.persist['currentTerm']
        prev_log_term_match = msg['prevLogTerm'] is None or\
            (self.log.index >= msg['prevLogIndex'] and\
            self.log.term(msg['prevLogIndex']) == msg['prevLogTerm'])
        success = term_is_current and prev_log_term_match

        if success:
            self.log.append_entries(msg['entries'], msg['prevLogIndex'])
            # for everything log from self.log.commitIndex to msg['leaderCommit'],
            # need to check if this log's signedPrepares before committing
            startIndex = self.log.commitIndex
            # TODO: Dennis should there be a check for number of signedPrepares?
            # TODO: Dennis should we commit if msg['isCommit'] isn't true?
            for index in range(startIndex, msg['leaderCommit'] + 1):
                isValid = self.checkSignedPrepares(index, msg['signedPrepares'])
                if isValid:
                    self.log.commit(index)
                    self.send_client_append_response()
                else: # as soon as one log is invalid, break
                    logger.info("Invalid signature!!")
                    break
            self.volatile['leaderId'] = msg['leaderId']
            logger.debug('Log index is now %s', self.log.index)
        else:
            logger.warning('Could not append entries. cause: %s', 'wrong\
                term' if not term_is_current else 'prev log term mismatch')

        if not msg['isCommit']: # if it's a commit, don't need to notify the leader that commit is successful
            resp = {'type': 'response_append', 'success': success,
                    'term': self.persist['currentTerm'],
                    'matchIndex': self.log.index}
            resp = self.sign_message(resp)
            self.orchestrator.send_peer(peer, resp)

    # TODO: implement this
    # signedPrepares is a list of tuples = (string representation of msg, signature, and sender)
    def checkSignedPrepares(self, index, signedPrepares):
        for signedPrepare in signedPrepares:
            # FIXME: Getting Message too long error
            isValid = crypto.verify_message(signedPrepare[0], self.volatile['public_keys'][tuple(signedPrepare[2])], eval(signedPrepare[1]))
            if not isValid:
                logger.error('OMGGGGGGGGGGGGGGGG')
                return False
            msg = json.loads(signedPrepare[0])
            if not(msg['type'] == 'response_append' and msg['success']):
                logger.error('OMGGGGGGGGGGGGGGGG2222222222222222')
                return False
        return True

    def send_client_append_response(self):
        """Respond to client upon commitment of log entries."""
        to_delete = []
        for client_index, clients in self.waiting_clients.items():
            if client_index <= self.log.commitIndex:
                for client in clients:
                    self.orchestrator.send_client(client['addr'], \
                        {'type': 'result', 'success': True, \
                            'req_id': client['req_id']})
                    logger.debug('Sent successful response to client')
                to_delete.append(client_index)
        for index in to_delete:
            del self.waiting_clients[index]


class Candidate(Follower):
    """Candidate state. Notice that this state subclasses Follower."""
    def __init__(self, old_state=None, orchestrator=None):
        """Initialize parent, increase term, vote for self, ask for votes."""
        super().__init__(old_state, orchestrator)
        self.persist['currentTerm'] += 1
        self.votes_count = 0
        logger.debug('New Election. Term: %s', self.persist['currentTerm'])
        self.send_vote_requests()

        def vote_self():
            self.persist['votedFor'] = self.volatile['address']
            self.on_peer_response_vote(
                self.volatile['address'], {'voteGranted': True})
        loop = asyncio.get_event_loop()
        loop.call_soon(vote_self)

    def send_vote_requests(self):
        """Ask peers for votes."""
        logger.debug('Broadcasting request_vote')
        msg = {'type': 'request_vote', 'term': self.persist['currentTerm'],
               'candidateId': self.volatile['address'],
               'lastLogIndex': self.log.index,
               'lastLogTerm': self.log.term()}
        self.orchestrator.broadcast_peers(msg)

    def on_peer_append_entries(self, peer, msg):
        """Transition back to Follower upon receiving an append_entries."""
        logger.debug('Converting to Follower')
        self.orchestrator.change_state(Follower)
        self.orchestrator.state.on_peer_append_entries(peer, msg)

    def on_peer_response_vote(self, peer, msg):
        """Register peers votes, transition to Leader upon majority vote."""
        self.votes_count += msg['voteGranted']
        logger.debug('Vote count: %s', self.votes_count)
        if self.votes_count > len(self.volatile['cluster']) / 2:
            self.orchestrator.change_state(Leader)


class Leader(State):
    """Leader state."""
    def __init__(self, old_state=None, orchestrator=None):
        """Initialize parent, sets leader variables, start periodic
        append_entries"""
        super().__init__(old_state, orchestrator)
        logger.info('Leader of term: %s', self.persist['currentTerm'])
        self.volatile['leaderId'] = self.volatile['address']
        self.matchIndex = {p: 0 for p in self.volatile['cluster']}
        self.nextIndex = {p: self.log.commitIndex + 1 for p in self.matchIndex}
        self.waiting_clients = {}
        self.signedPrepares = defaultdict(set) # maps index to a set of signed prepares
        self.signedSeen = defaultdict(dict)
        self.send_append_entries()

        if 'cluster' not in self.log.state_machine:
            self.log.append_entries([
                {'term': self.persist['currentTerm'],
                 'data':{'key': 'cluster',
                         'value': tuple(self.volatile['cluster']),
                         'action': 'change'}}],
                self.log.index)
            self.log.commit(self.log.index)

    def teardown(self):
        """Stop timers before changing state."""
        self.append_timer.cancel()
        if hasattr(self, 'config_timer'):
            self.config_timer.cancel()
        for clients in self.waiting_clients.values():
            for client in clients:
                self.orchestrator.send_client(client['addr'], \
                    {'type': 'result', 'success': False, \
                        'req_id': client['req_id']})
                logger.error('Sent unsuccessful response to client')

    def send_append_entries(self, isCommit=False):
        """Send append_entries to the cluster, containing:
        - nothing: if remote node is up to date.
        - compacted log: if remote node has to catch up.
        - log entries: if available.
        Finally schedules itself for later execution.
        Also incorporate a special case for committing entries,
        in such case isCommit = True."""
        signedPrepares = list(self.signedPrepares[self.log.commitIndex])
        newSignedPrepares = []
        for signedPrepare in signedPrepares:
            newSignedPrepares.append(list(signedPrepare))
        for peer in self.volatile['cluster']:
            if peer == self.volatile['address']:
                continue
            msg = {'type': 'append_entries',
                   'isCommit': isCommit,
                   'term': self.persist['currentTerm'],
                   'leaderCommit': self.log.commitIndex,
                   'leaderId': self.volatile['address'],
                   'signedPrepares': newSignedPrepares,
                   'prevLogIndex': self.nextIndex[peer] - 1,
                   'entries': self.log[self.nextIndex[peer]:
                                       self.nextIndex[peer] + 100]}
            msg.update({'prevLogTerm': self.log.term(msg['prevLogIndex'])})

            logger.debug('Sending %s entries to %s. Start index %s',
                         len(msg['entries']), peer, self.nextIndex[peer])

            newMsg = self.sign_message(msg)
            self.orchestrator.send_peer(peer, newMsg)

        if not isCommit:
            timeout = randrange(1, 4) * 10 ** (-1 if config.debug else -2)
            loop = asyncio.get_event_loop()
            self.append_timer = loop.call_later(timeout, self.send_append_entries)

    def on_peer_response_append(self, peer, msg):
        """Handle peer response to append_entries.
        If successful RPC, try to commit new entries.
        If RPC unsuccessful, backtrack."""
        actualMsg = json.loads(msg[0])
        if actualMsg['success']:
            self.matchIndex[peer] = actualMsg['matchIndex']
            self.nextIndex[peer] = actualMsg['matchIndex'] + 1

            # because leader is also calling on_peer_response_append itself, this is
            # basically the leader trying to update its own matchIndex and nextIndex
            # self.matchIndex[self.volatile['address']] = self.log.index
            # self.nextIndex[self.volatile['address']] = self.log.index + 1

            # signature (msg[1]) is of type bytes, and json can't serialize bytes so I cast it to str
            if peer not in self.signedSeen[actualMsg['matchIndex']]:
                self.signedSeen[actualMsg['matchIndex']][peer] = 1;
                self.signedPrepares[actualMsg['matchIndex']].\
                    add((msg[0], str(msg[1]), peer))
                logger.info(self.signedPrepares[actualMsg['matchIndex']])
            
            totalServers = len(self.volatile['cluster'])
            minRequiredServers = 2 # TODO: Dennis int(math.ceil(1.0 * totalServers / 3 * 2) + 1)
            if len(self.signedPrepares[actualMsg['matchIndex']]) >= minRequiredServers:
                self.log.commit(actualMsg['matchIndex'])
                # let follower know a new log has been committed
                self.send_append_entries(True)
                # send response back to client, followers also need to do this
                self.send_client_append_response()
        else:
            self.nextIndex[peer] = max(0, self.nextIndex[peer] - 1)

    def on_client_append(self, protocol, msg):
        """Append new entries to Leader log."""
        entry = {'term': self.persist['currentTerm'], 'data': msg['data']}
        if msg['data']['key'] == 'cluster':
            self.orchestrator.send_client(msg['client'], \
                {'type': 'result', 'success': False})
        self.log.append_entries([entry], self.log.index)
        new_client = {'addr': msg['client'], 'req_id': msg['req_id']}
        if self.log.index in self.waiting_clients:
            self.waiting_clients[self.log.index].append(new_client)
        else:
            self.waiting_clients[self.log.index] = [new_client]

        # Need to also update follower's waiting_clients
        for peer in self.volatile['cluster']:
            if peer == self.volatile['address']:
                continue
            msg = {'type': 'update_waiting_clients',
                   'logIndex': self.log.index,
                   'req_id' : msg['req_id'],
                   'client': msg['client'],
                   'term': self.persist['currentTerm']}
            newMsg = self.sign_message(msg)
            self.orchestrator.send_peer(peer, newMsg)

        resp = {'type': 'response_append', 'success': True, 'matchIndex': self.log.commitIndex}
        resp = self.sign_message(resp)
        self.on_peer_response_append(self.volatile['address'], resp)

    def send_client_append_response(self):
        """Respond to client upon commitment of log entries."""
        to_delete = []
        for client_index, clients in self.waiting_clients.items():
            if client_index <= self.log.commitIndex:
                for client in clients:
                    self.orchestrator.send_client(client['addr'], \
                        {'type': 'result', 'success': True, \
                            'req_id': client['req_id']})
                to_delete.append(client_index)
        for index in to_delete:
            del self.waiting_clients[index]
