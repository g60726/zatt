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
                'client_keys': config.client_keys, 'node_id': config.id}
            self.log = LogManager('log')
            self.sig_log = LogManager('sigs', machine=None)
        self.waiting_clients = {}
        logger.info(self.volatile['node_id'])

    def data_received_peer(self, peer, msg):
        """Receive peer messages from orchestrator and pass them to the
        appropriate method."""
        # TODO: LE remove this check once we signed everthing
        # Verify peer's signature
        actualMsg = msg
        if not isinstance(msg, dict):
            isValid = crypto.verify_message( \
                msg[0], \
                self.volatile['public_keys'][tuple(peer)], \
                msg[1])
            if not isValid:
                return
            actualMsg = json.loads(msg[0])

        logger.debug('Received %s from %s', actualMsg['type'], peer)

        # TODO: LE this needs to check leader proof
        if self.persist['currentTerm'] < actualMsg['term']:
            self.persist['currentTerm'] = actualMsg['term']
            if not type(self) is Follower:
                logger.info('Remote term is higher, converting to Follower')
                self.orchestrator.change_state(Follower)
                self.orchestrator.state.data_received_peer(peer, msg)
                return

        # Serve peer's request
        method = getattr(self, 'on_peer_' + actualMsg['type'], None)
        if method:
            method(peer, actualMsg)
        else:
            logger.info('Unrecognized message from %s: %s', peer, actualMsg)

    def data_received_client(self, protocol, msg):
        """Receive client messages from orchestrator and pass them to the
        appropriate method."""
        # Verify client's signature
        actualMsg = json.loads(msg[0])
        isValid = crypto.verify_message( \
            msg[0], \
            self.volatile['client_keys'][tuple(actualMsg['client'])], \
            msg[1])
        if not isValid:
            return

        # Serve client's request
        method = getattr(self, 'on_client_' + actualMsg['type'], None)
        if method:
            method(protocol, actualMsg, msg)
        else:
            logger.info('Unrecognized message from %s: %s',
                        protocol.transport.get_extra_info('peername'), msg)

    def on_client_append(self, protocol, msg, orig):
        """Redirect client to leader upon receiving a client_append message."""
        if self.volatile['leaderId']:
            self.orchestrator.redir_leader( \
                tuple(self.volatile['leaderId']), orig)
            logger.debug('Redirect client %s:%s to leader',
                         *protocol.transport.get_extra_info('peername'))

    def on_client_get(self, protocol, msg, orig):
        """Return state machine to client."""
        state_machine = self.log.state_machine.data.copy()
        resp = {'type': 'result', 'success': True, 'req_id': msg['req_id'], \
                'data': state_machine, \
                'server_address': self.volatile['address']}
        self.orchestrator.send_client(msg['client'], self.sign_message(resp))

    def send_client_append_response(self):
        """ Respond to client upon commitment of log entries """
        for client_addr, client_req in self.waiting_clients.items():
            if client_req['log_idx'] < self.log.commitIndex:
                resp = {'type': 'result', 'success': True, \
                        'req_id': client_req['req_id'], \
                        'server_address': self.volatile['address']}
                self.orchestrator.send_client(client_addr, 
                                              self.sign_message(resp))

    def store_client_new_req(self, msg):
        # keep track of the new client (to respond to upon commit or retransmit)
        new_req = {'log_idx': self.log.index, 'req_id': msg['req_id']}
        client_addr = tuple(msg['client'])
        if client_addr in self.waiting_clients:
            curr_req = self.waiting_clients[client_addr]
            if curr_req['req_id'] != new_req['req_id']:
                # client providing new request
                self.waiting_clients[client_addr] = new_req
                return True
            else:
                # client requesting retransmit
                self.send_client_append_response()
                return False
        else:
            self.waiting_clients[client_addr] = new_req
            return True
        return False

    def sign_message(self, msg):
        signature = crypto.sign_message(json.dumps(msg), self.volatile['private_key'])
        return [json.dumps(msg), signature]

    def verify_sig(self, peer, data, sig):
        if peer in self.volatile['public_keys']:
            key = self.volatile['public_keys'][peer]
        elif peer in self.volatile['client_keys']:
            key = self.volatile['client_keys'][peer]
        else:
            return False
        return crypto.verify_message(
                    json.dumps(data), \
                    key, \
                    eval(sig))

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

    def restart_election_timer(self):
        """Delays transition to the Candidate state by timer."""
        if hasattr(self, 'election_timer'):
            self.election_timer.cancel()

        timeout = randrange(1, 4) * 10 ** (0 if config.debug else -1)
        loop = asyncio.get_event_loop()
        self.election_timer = loop.call_later(timeout, self.start_vote)
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
        client_msg = msg['message']
        client_addr = tuple(client_msg['client'])
        if not super().verify_sig(client_addr, client_msg, msg['client_sig']):
            return

        new_req = super().store_client_new_req(client_msg)
        if new_req:
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
                 len(msg['prevLogSigs']) >= 2) and \
                 self.verify_prepares( \
                    msg['prevLogEntry'], \
                    msg['prevLogSigs'])

        success = term_is_current and prev_log_term_match

        # attempt to append to log
        if success:
            for index in range(len(msg['sigs'])):
                sig_check = self.verify_prepares( \
                    msg['entries'][index], \
                    msg['sigs'][index])

                if len(msg['sigs'][index]) >= 2 and sig_check:
                    entry = msg['entries'][index]
                    log_idx = entry['log_index']
                    # record entries, signatures, and persist the proof
                    self.log.append_entries([entry], log_idx)
                    self.sig_log.append_entries([msg['sigs'][index]], log_idx)
                    self.log.commit(log_idx+1)
                    logger.debug('Log index is now %s', self.log.commitIndex)
                else:
                    logger.info("Invalid signature!!")
                    break
            super().send_client_append_response()
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

    def verify_prepares(self, entry, prepares):
        val_entry = json.dumps(entry)
        sig_check = True
        for key in prepares:
            data = prepares[key][0]
            sig = prepares[key][1]
            sender = self.string_to_peer(key)

            if not super().verify_sig(sender, data, sig):
                sig_check = False
                break

            if json.dumps(data) != val_entry:
                sig_check = False
                break
            
        return sig_check

    def string_to_peer(self, str):
        arr = str.split(':')
        return (arr[0], int(arr[1]))


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
        self.prepares = {}
        self.send_append_entries()

    def teardown(self):
        """ Stop timers before changing state """
        self.append_timer.cancel()
        if hasattr(self, 'config_timer'):
            self.config_timer.cancel()

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
        if idx in self.prepares and peer not in self.prepares[idx]:
            if self.log.index > idx:
                if msg['entry'] == json.dumps(self.log[idx+1]):
                    sig = (json.loads(msg['entry']), str(msg['entrySig']))
                    self.prepares[idx]['sigs'][self.peer_to_string(peer)] = sig
                else:
                    logger.info(msg['entry'])
                    logger.info(self.log[idx+1])
            else:
                logger.info(msg)
                logger.info(self.log[self.log.index])
                logger.info(self.waiting_clients)

        # try to commit entries with a quorum of signatures
        to_delete = []
        for log_idx in self.prepares:
            totalServers = len(self.volatile['cluster'])
            minRequiredServers = 2 # TODO: LE int(math.ceil(1.0 * totalServers / 3 * 2) + 1)
            if len(self.prepares[log_idx]['sigs']) >= minRequiredServers:
                commit_idx = self.log.commit(log_idx+1)
                if commit_idx > log_idx:
                    # record signatures and persist the proof
                    self.sig_log.append_entries( \
                            [self.prepares[log_idx]['sigs']], log_idx)
                    to_delete.append(log_idx)

                    # send response back to client
                    super().send_client_append_response()
        for log_idx in to_delete:
            del self.prepares[log_idx]


    def on_client_append(self, protocol, msg, orig):
        new_req = super().store_client_new_req(msg)

        # append new entry to tip of Leader log
        log_index = 0
        if new_req:
            log_index = self.log.index
            entry = {'term': self.persist['currentTerm'], \
                     'data': msg['data'], \
                     'log_index': self.log.index}
            self.log.append_entries([entry], self.log.index)
            logger.info("Received new req, appending to " + str(log_index))
        else:
            log_index = self.waiting_clients[tuple(msg['client'])]['log_idx']
            logger.info("Retransmitting req at log index: " + str(log_index))

        # put together the prepare message
        prepare = {'type': 'append_prepare', \
                   'term': self.persist['currentTerm'], \
                   'leaderId': self.volatile['address'], \
                   'logIndex': log_index, \
                   'message': msg, \
                   'client_sig': str(orig[1])}

        if new_req:
            self.prepares[log_index] = {'prepare_msg': prepare, \
                                                'sigs': {}}
            # respond with successful prepare to self
            (entry, sig) = self.sign_message(entry)
            resp = {'type': 'response_prepare', \
                    'term': self.persist['currentTerm'], \
                    'logIndex': log_index, \
                    'entry': entry, \
                    'entrySig': str(sig)}
            self.on_peer_response_prepare(self.volatile['address'], resp)

        # TODO: Dennis delegate to periodic function
        # tell followers to prepare new entry
        for peer in self.volatile['cluster']:
            if peer == self.volatile['address']:
                continue
            self.orchestrator.send_peer(peer, self.sign_message(prepare))

    def peer_to_string(self, peer):
        return peer[0] + ":" + str(peer[1])
