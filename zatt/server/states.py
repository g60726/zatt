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
            self.sig_log = old_state.sig_log
        else:
            self.orchestrator = orchestrator
            self.persist = PersistentDict(join(config.storage, 'state'),
                                          {'votedFor': None, 'currentTerm': 0})
            self.volatile = {'leaderId': None, 'cluster': config.cluster,
                'address': config.address, 'private_key': config.private_key,
                'public_keys': config.public_keys, 'clients': config.clients,
                'client_keys': config.client_keys, 'candidateID':0}
            self.log = LogManager('log')
            self.sig_log = LogManager('sigs', machine=None)

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
    def __init__(self, old_state=None, orchestrator=None, ID=None):
        """Initialize parent and start election timer."""
        super().__init__(old_state, orchestrator)
        self.persist['votedFor'] = self.persist['currentTerm'] % len(self.volatile['cluster'])
        self.restart_election_timer()
        self.ID = ID
        self.on_election = False
        self.volatile['address'] = self.ID
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
            call_later(timeout, self.start_vote)
        logger.debug('Election timer restarted: %s s', timeout)
    ###start_vote
    def start_vote(self):
        self.on_election = True
        msg = {'type': 'start_vote',
               'voteGranted': True,
               'term': self.persist['currentTerm'],
               'votedFor':self.persist['votedFor'],
               'lastLogTerm': self.log.term(),
               'lastLogIndex': self.log.index}
        #broad_cast start vote msg to all followers
        self.orchestrator.broadcast_peers(msg)
               
    ###followers receive start_vote msg
    def on_peer_start_vote(self, msg):
        """Grant this node's vote to Candidates."""
        self.on_election = True
        term_is_current = msg['term'] >= self.persist['currentTerm']
        index_is_current = (msg['lastLogTerm'] > self.log.term() or
                            (msg['lastLogTerm'] == self.log.term() and
                             msg['lastLogIndex'] >= self.log.index))
                             granted = term_is_current and index_is_current
        transform = self.ID == msg['votedFor'] % len(self.volatile['cluster'])
        if granted:
            if transform:
            #if follower ID == v mode R, then transform to candidate
                self.orchestrator.change_state(Candidate)
            else:
                #else just vote to others
                message = {'type': 'receive_vote',
                           'voteGranted': granted,
                           'term': self.persist['currentTerm'],
                           'votedFor':self.persist['votedFor'],
                           'lastLogTerm': self.log.term(),
                           'lastLogIndex': self.log.index}
                self.orchestrator.broadcast_peers(message)
    #follower receive votes from other followers, is receive any vote, transform to candidate
    def on_peer_receive_vote(self,msg):
        term_is_current = msg['term'] >= self.persist['currentTerm']
        index_is_current = (msg['lastLogTerm'] > self.log.term() or
                            (msg['lastLogTerm'] == self.log.term() and
                            msg['lastLogIndex'] >= self.log.index))
        granted = term_is_current and index_is_current
        transform = self.ID == msg['votedFor']
        if granted and transform:
            self.orchestrator.change_state(Candidate)
    #follower finishes election
    def on_peer_finish_election(self,msg):
        self.on_election = False
        self.persist['currentTerm'] = msg['term']
        self.persist['votedFor'] = msg['votedFor']

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

    def on_peer_append_prepare(self, peer, msg):
        # TODO: verify client signature
        client_msg = msg['message']

        # keep track of the new client (to respond to upon commit)
        new_client = {'addr': client_msg['client'], \
                      'req_id': client_msg['req_id']}
        if msg['logIndex'] in self.waiting_clients:
            self.waiting_clients[msg['logIndex']].append(new_client)
        else:
            self.waiting_clients[msg['logIndex']] = [new_client]

        # append new entry at tip of log
        entry = {'term': msg['term'], \
                 'data': client_msg['data'], \
                 'log_index': msg['logIndex']}
        self.log.append_entries([entry], msg['logIndex'])

        # respond with successful prepare to leader
        self.volatile['leaderId'] = msg['leaderId']
        (entry, sig) = self.sign_message(entry)
        resp = {'type': 'response_prepare', \
                'term': msg['term'], \
                'logIndex': msg['logIndex'], \
                'entry': entry, \
                'entrySig': str(sig)}
        resp = self.sign_message(resp)
        self.orchestrator.send_peer(self.volatile['leaderId'], resp)

    def on_peer_append_entries(self, peer, msg):
        """ Manages incoming log entries from the Leader """
        self.restart_election_timer()
        self.volatile['leaderId'] = msg['leaderId']

        term_is_current = msg['term'] >= self.persist['currentTerm']
        prev_log_term_match = True
        if msg['prevLogEntry'] is not None:
            prev_term = msg['prevLogEntry']['term']
            prev_log_index = msg['prevLogEntry']['log_index']
            prev_log_term_match = (self.log.index >  prev_log_index and \
                 self.log.term(prev_log_index+1) == prev_term and \
                 len(msg['prevLogSigs']) >= 2)
        # TODO: verify signatures on the provided entry

        success = term_is_current and prev_log_term_match

        # attempt to append to log
        if success:
            for index in range(len(msg['sigs'])):
                if len(msg['sigs'][index]) >= 2:
                # TODO: verify signatures on the provided entry
                    entry = msg['entries'][index]
                    log_idx = entry['log_index']
                    # record entries, signatures, and persist the proof
                    self.log.append_entries([entry], log_idx)
                    self.sig_log.append_entries([msg['sigs'][index]], log_idx)
                    self.log.commit(log_idx+1)
                    logger.info('Log index is now %s', self.log.commitIndex)
                else:
                    logger.info("Invalid signature!!")
                    break
            self.send_client_append_response()
        # could not append to log
        else:
            logger.warning('Could not append entries. cause: %s', 'wrong\
                term' if not term_is_current else 'prev log term mismatch')

        # respond to leader with success/fail of the log append
        resp = {'type': 'response_append', \
                'success': success, \
                'term': self.persist['currentTerm'], \
                'matchIndex': self.log.commitIndex}
        resp = self.sign_message(resp)
        self.orchestrator.send_peer(peer, resp)

    # # TODO: implement this
    # # signedPrepares is a list of tuples = (string representation of msg, signature, and sender)
    # def checkSignedPrepares(self, index, signedPrepares):
    #     for signedPrepare in signedPrepares:
    #         isValid = crypto.verify_message(signedPrepare[0], self.volatile['public_keys'][tuple(signedPrepare[2])], eval(signedPrepare[1]))
    #         if not isValid:
    #             logger.error('OMGGGGGGGGGGGGGGGG')
    #             return False
    #         msg = json.loads(signedPrepare[0])
    #         if not(msg['type'] == 'response_append' and msg['success']):
    #             logger.error('OMGGGGGGGGGGGGGGGG2222222222222222')
    #             return False
    #     return True

    def send_client_append_response(self):
        """Respond to client upon commitment of log entries."""
        to_delete = []
        for client_index, clients in self.waiting_clients.items():
            if client_index < self.log.commitIndex:
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
        #self.persist['currentTerm'] += 1
        self.votes_count = 1
        logger.debug('New Election. Term: %s', self.persist['currentTerm'])
        self.send_vote_requests()

        def vote_self():
            self.persist['votedFor'] = self.volatile['address']
            self.on_peer_response_vote(
                self.volatile['address'], {'voteGranted': True})
        loop = asyncio.get_event_loop()
        loop.call_soon(vote_self)
    def on_peer_request_vote(self,peer, msg):
        #if candidate term is smaller than peer's term, send vote and stand down
        if self.persist['currentTerm'] <= msg['term']:
            self.orchestrator.change_state(Follower)
            can_vote = self.persist['votedFor'] == msg['candidateId']
            index_is_current = (msg['lastLogTerm'] > self.log.term() or (msg['lastLogTerm'] == self.log.term() and
                                                                         msg['lastLogIndex'] >= self.log.index))
            granted = can_vote and index_is_current
            if granted:
                response = {'type': 'response_vote', 'voteGranted': granted,'term': self.persist['currentTerm']}
                self.orchestrator.send_peer(peer, response)
            self.orchestrator.change_state(Follower)
        #else tell the sender candidate stand down
        else:
            response = {'type': 'response_vote', 'voteGranted': False,'term': self.persist['currentTerm']}
            self.orchestrator.send_peer(peer, response)
    def send_vote_requests(self):
        """Ask peers for votes."""
        logger.debug('Broadcasting request_vote')
        msg = {'type': 'request_vote', 'term': self.persist['currentTerm'],
               'candidateId': self.volatile['address'],
               'lastLogIndex': self.log.index,
               'lastLogTerm': self.log.term()}
        self.orchestrator.broadcast_peers(msg)
    #candidate finishes election
    def on_peer_finish_election(self,msg):
        self.on_election = False
        self.persist['currentTerm'] = msg['term']
        self.persist['votedFor'] = msg['votedFor']
        self.orchestrator.change_state(Follower)
    
    def on_peer_append_entries(self, peer, msg):
        """Transition back to Follower upon receiving an append_entries."""
        logger.debug('Converting to Follower')
        self.orchestrator.change_state(Follower)
        self.orchestrator.state.on_peer_append_entries(peer, msg)

    def on_peer_response_vote(self, peer, msg):
        """Register peers votes, transition to Leader upon majority vote."""
        if msg['voteGranted'] == False:
            self.orchestrator.change_state(Follower)
        else:
            self.votes_count += msg['voteGranted']
            logger.debug('Vote count: %s', self.votes_count)
            if self.votes_count > len(self.volatile['cluster']) / 2:
                #transform to leader
                self.orchestrator.change_state(Leader)
                #braodcast current parameters to synchronize everyone
                self.persist['currentTerm'] += 1
                self.persist['votedFor'] += 1
                msg = {'type': 'finish_election',
                    'term': self.persist['currentTerm'],
                        'votedFor': self.persist['votedFor']}
                self.orchestrator.broadcast_peers(msg)
    #candidate receive any vote, increment the counter
    def on_peer_receive_vote(self,msg):
        """Register peers votes, transition to Leader upon majority vote."""
        if self.ID == msg['votedFor']:
            self.votes_count += msg['voteGranted']
            logger.debug('Vote count: %s', self.votes_count)
            if self.votes_count > int(math.ceil(1.0 * len(self.volatile['cluster']) / 3 * 2) + 1):
                #reset counter and transform to normal Raft election process
                self.votes_count = 0
                self.send_vote_requests()


class Leader(State):
    """Leader state."""
    def __init__(self, old_state=None, orchestrator=None):
        """ Initialize parent, sets leader variables, start periodic
            append_entries """
        super().__init__(old_state, orchestrator)
        logger.info('Leader of term: %s', self.persist['currentTerm'])
        self.volatile['leaderId'] = self.volatile['address']
        self.matchIndex = {p: 0 for p in self.volatile['cluster']}
        self.nextIndex = {p: self.log.commitIndex + 1 for p in self.matchIndex}
        self.waiting_clients = {}
        self.signedPrepares = {}
        self.send_append_entries()

    def teardown(self):
        """ Stop timers before changing state """
        self.append_timer.cancel()
        if hasattr(self, 'config_timer'):
            self.config_timer.cancel()
        for clients in self.waiting_clients.values():
            for client in clients:
                self.orchestrator.send_client(client['addr'], \
                    {'type': 'result', 'success': False, \
                        'req_id': client['req_id']})
                logger.error('Sent unsuccessful response to client')

    def send_append_entries(self):
        # send append_entries with next needed log entries to each follower
        for peer in self.volatile['cluster']:
            if peer == self.volatile['address']:
                continue
            prevLogEntry = None
            prevLogSigs = None
            if self.nextIndex[peer] > 0:
                prevLogEntry = self.log[self.nextIndex[peer] - 1]
                prevLogSigs = self.sig_log[self.nextIndex[peer] - 1]
            msg = {'type': 'append_entries',
                   'term': self.persist['currentTerm'],
                   'leaderCommit': self.log.commitIndex,
                   'leaderId': self.volatile['address'],
                   'prevLogEntry': prevLogEntry,
                   'prevLogSigs': prevLogSigs,
                   'entries': self.log[self.nextIndex[peer]: \
                                       self.nextIndex[peer] + 100], \
                   'sigs': self.sig_log[self.nextIndex[peer]: \
                                        self.nextIndex[peer] + 100]}

            logger.debug('Sending %s entries to %s. Start index %s',
                         len(msg['entries']), peer, self.nextIndex[peer])

            self.orchestrator.send_peer(peer, self.sign_message(msg))

        # schedule heartbeat for next execution
        timeout = randrange(1, 4) * 10 ** (-1 if config.debug else -2)
        loop = asyncio.get_event_loop()
        self.append_timer = loop.call_later(timeout, self.send_append_entries)

    def on_peer_response_append(self, peer, msg):
        # record peer's signed prepare
        if msg['success']:
            # update leader's understanding of peer's updated log indices
            self.matchIndex[peer] = msg['matchIndex']
            self.nextIndex[peer] = msg['matchIndex'] + 1

            # because leader is also calling on_peer_response_append itself, 
            # this is the leader trying to update its own indices
            self.matchIndex[self.volatile['address']] = self.log.index
            self.nextIndex[self.volatile['address']] = self.log.index + 1
        # backtrack 1 index and try again
        else:
            self.nextIndex[peer] = max(0, self.nextIndex[peer] - 1)


    def on_peer_response_prepare(self, peer, msg):
        # store peer's signed prepare
        idx = msg['logIndex']
        if idx in self.signedPrepares:
            # TODO: also check that entries match up
            if peer not in self.signedPrepares[idx]:
                sig = (json.loads(msg['entry']), str(msg['entrySig']))
                self.signedPrepares[idx]['sigs'][self.peerToString(peer)] = sig

        # try to commit entries with a quorum of signatures
        to_delete = []
        for log_idx in self.signedPrepares:
            totalServers = len(self.volatile['cluster'])
            minRequiredServers = 2 # TODO: int(math.ceil(1.0 * totalServers / 3 * 2) + 1)
            if len(self.signedPrepares[log_idx]['sigs']) >= minRequiredServers:
                commit_idx = self.log.commit(log_idx+1)
                if commit_idx > log_idx:
                    # record signatures and persist the proof
                    self.sig_log.append_entries( \
                            [self.signedPrepares[log_idx]['sigs']], log_idx)
                    to_delete.append(log_idx)

                    # send response back to client
                    self.send_client_append_response()
        for log_idx in to_delete:
            del self.signedPrepares[log_idx]


    def on_client_append(self, protocol, msg):
        # TODO: verify sig of client msg
        # keep track of the new client (to respond to upon commit)
        new_client = {'addr': msg['client'], 'req_id': msg['req_id']}
        if self.log.index in self.waiting_clients:
            self.waiting_clients[self.log.index].append(new_client)
        else:
            self.waiting_clients[self.log.index] = [new_client]

        # append new entry to tip of Leader log
        log_index = self.log.index
        entry = {'term': self.persist['currentTerm'], \
                 'data': msg['data'], \
                 'log_index': self.log.index}
        self.log.append_entries([entry], self.log.index)

        # put together the prepare message
        prepare = {'type': 'append_prepare', \
                   'term': self.persist['currentTerm'], \
                   'leaderId': self.volatile['address'], \
                   'logIndex': log_index, \
                   'message': msg} # TODO: does this send?
        self.signedPrepares[log_index] = {'prepare_msg': prepare, \
                                                'sigs': {}}

        # TODO: delegate to periodic function
        # tell followers to prepare new entry
        for peer in self.volatile['cluster']:
            if peer == self.volatile['address']:
                continue
            self.orchestrator.send_peer(peer, self.sign_message(prepare))

        # respond with successful prepare to self
        (entry, sig) = self.sign_message(entry)
        resp = {'type': 'response_prepare', \
                'term': self.persist['currentTerm'], \
                'logIndex': log_index, \
                'entry': entry, \
                'entrySig': str(sig)}
        self.on_peer_response_prepare(self.volatile['address'], resp)

    def send_client_append_response(self):
        """ Respond to client upon commitment of log entries """
        to_delete = []
        for client_index, clients in self.waiting_clients.items():
            if client_index < self.log.commitIndex:
                for client in clients:
                    # TODO: sign message
                    self.orchestrator.send_client(client['addr'], \
                        {'type': 'result', 'success': True, \
                            'req_id': client['req_id']})
                to_delete.append(client_index)
        for index in to_delete:
            del self.waiting_clients[index]

    def peerToString(self, peer):
        return peer[0] + ":" + str(peer[1])
