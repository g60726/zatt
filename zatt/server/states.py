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
from datetime import datetime

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
            self.prepare_log = old_state.prepare_log
            self.commit_log = old_state.commit_log
        else:
            self.orchestrator = orchestrator
            self.persist = PersistentDict(join(config.storage, 'state'),
                                          {'startTerm': 0, 'currentTerm': 0})
            self.volatile = {'leaderId': None, 'cluster': config.cluster,
                'address': config.address, 'private_key': config.private_key,
                'public_keys': config.public_keys, 'clients': config.clients,
                'client_keys': config.client_keys, 'node_id': int(config.id),
                'start_votes': {}, 'server_ids': config.server_ids,
                'lead_votes': {}}
            self.log = LogManager('log')
            self.prepare_log = LogManager('prep', machine=None)
            self.commit_log = LogManager('commit', machine=None)
        self.waiting_clients = {}
        self.quorum = int(math.ceil(
            (len(self.volatile['cluster'])-1) / 3.0 * 2.0) + 1)

    def data_received_peer(self, peer, msg):
        """Receive peer messages from orchestrator and pass them to the
        appropriate method."""
        # Verify peer's signature
        actualMsg = msg
        isValid = crypto.verify_message( \
            msg[0], \
            self.volatile['public_keys'][tuple(peer)], \
            msg[1])
        if not isValid:
            return
        actualMsg = json.loads(msg[0])

        logger.debug('Received %s from %s', actualMsg['type'], peer)

        if self.persist['currentTerm'] < actualMsg['term']:
            # check to make sure peer is indeed leaderf
            is_leader = False
            if 'lead_votes' in actualMsg:
                for addr in actualMsg['lead_votes']:
                    data = actualMsg['lead_votes'][addr]['data']
                    if not (data['type'] == "response_vote" and \
                            data['start_term'] == actualMsg['term'] and \
                            data['vote_granted']):
                        return
                    is_leader = self.verify_sig( \
                        self.string_to_peer(addr), \
                        data, \
                        actualMsg['lead_votes'][addr]['sig'])
                    if not is_leader:
                        break

            # update term to leader's term if the leader proof checks out
            if is_leader:
                self.persist['currentTerm'] = actualMsg['term']
                self.persist['startTerm'] = actualMsg['term']
                self.volatile['start_votes'] = {}
                if not type(self) is Follower:
                    logger.debug('Remote term is higher, converting to Follower')
                    self.orchestrator.change_state(Follower)
                    self.orchestrator.state.data_received_peer(peer, msg)
                    return
                else:
                    self.on_election = False
                    print(str(datetime.now()) + " "+ "Became Follower of term: "+str(self.persist['currentTerm']))

        # Serve peer's request
        method = getattr(self, 'on_peer_' + actualMsg['type'], None)
        if method:
            method(peer, actualMsg, msg)
        else:
            logger.debug('Unrecognized message from %s: %s', peer, actualMsg)

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
            logger.debug('Unrecognized message from %s: %s',
                        protocol.transport.get_extra_info('peername'), msg)

    def on_peer_request_vote(self, peer, msg, orig):
        """ Verify/compare logs and grant vote to peer. """
        term_is_current = msg['term'] >= self.persist['currentTerm']
        enough_start_votes = ( len(msg['start_votes']) >= self.quorum )
        sig_check = False
        for addr in msg['start_votes']:
            data = msg['start_votes'][addr]['data']
            cand_id = data['start_term'] % len(self.volatile['cluster'])
            node_id = self.volatile['server_ids'][peer]
            if not (data['type'] == "start_vote" and \
                cand_id == node_id):
                return
            sig_check = self.verify_sig( \
                self.string_to_peer(addr), \
                data, \
                msg['start_votes'][addr]['sig'])
            if not sig_check: break

        # demote to Follower because election has started
        if term_is_current and enough_start_votes and sig_check:
            if type(self) is Leader:
                self.orchestrator.change_state(Follower)
                self.orchestrator.state.data_received_peer(peer, orig)
                return

            elif type(self) is Candidate:
                # only respond to vote request from higher start terms
                if msg['start_term'] < self.persist['startTerm']:
                    return
                else:
                    self.orchestrator.change_state(Follower)
                    self.orchestrator.state.data_received_peer(peer, orig)
                    return

        # verify logs are up-to-date
        log_current = False
        if msg['last_entry'] is None:
            if self.log.commitIndex == -1:
                log_current = True
        else:
            # verify log entry
            if self.log.commitIndex <= msg['last_commit']:
                if self.verify_prepares(msg['last_entry'], msg['last_sig']):
                    log_current = True

        # grant vote if all conditions met
        granted = term_is_current and enough_start_votes and log_current \
            and sig_check
        if granted:
            print(str(datetime.now()) + " "+ "Casting vote for term: " + str(msg['start_term']))
            self.persist['startTerm'] = msg['start_term']
            self.on_election = True
            response = {'type': 'response_vote', 
                        'term': self.persist['currentTerm'],
                        'vote_granted': True,
                        'start_term': msg['start_term']}
            response = self.sign_message(response)
            self.orchestrator.send_peer(peer, response)

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

    def verify_prepares(self, entry, prepares):
        val_entry = json.dumps(entry)
        sig_check = True
        for key in prepares:
            data = prepares[key][0]
            sig = prepares[key][1]
            sender = self.string_to_peer(key)

            if not self.verify_sig(sender, data, sig):
                sig_check = False
                break

            if json.dumps(data) != val_entry:
                sig_check = False
                break
            
        return sig_check

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

    def peer_to_string(self, peer):
        return peer[0] + ":" + str(peer[1])

    def string_to_peer(self, str):
        arr = str.split(':')
        return (arr[0], int(arr[1]))

class Follower(State):
    """Follower state."""
    def __init__(self, old_state=None, orchestrator=None, ID=None):
        """Initialize parent and start election timer."""
        super().__init__(old_state, orchestrator)
        print(str(datetime.now()) + " "+ "Became Follower of term: "+str(self.persist['currentTerm']))
        if not type(self) is Candidate:
            self.volatile['start_votes'] = {}
            self.on_election = False
        self.restart_election_timer()
        self.waiting_clients = {}

    def teardown(self):
        """Stop timers before changing state."""
        self.election_timer.cancel()

    def restart_election_timer(self):
        """Delays transition to the Candidate state by timer."""
        if hasattr(self, 'election_timer'):
            self.election_timer.cancel()

        timeout = randrange(1, 4) * 5 ** (0 if config.debug else -1)
        loop = asyncio.get_event_loop()
        self.election_timer = loop.call_later(timeout, self.start_vote)
        logger.debug('Election timer restarted: %s s', timeout)

    def start_vote(self):
        """ Timeout detected! Broadcast message to start new election cycle. """
        print(str(datetime.now()) + " "+ "Timed out!")
        self.on_election = True
        self.persist['startTerm'] += 1

        msg = {'type': 'start_vote',
               'term': self.persist['currentTerm'],
               'start_term': self.persist['startTerm']}
        signed = self.sign_message(msg)

        # broadcast start_vote to peers
        self.orchestrator.broadcast_peers(signed)
        self.on_peer_start_vote(self.volatile['address'], msg, signed)
        self.restart_election_timer()
               
    def on_peer_start_vote(self, peer, msg, orig):
        """ Collect peer votes to start an election. """
        candidate_id = msg['start_term'] % len(self.volatile['cluster'])

        if candidate_id == self.volatile['node_id']:
            key = self.peer_to_string(peer)
            value = {'data': msg, 'sig': str(orig[1])}
            if key not in self.volatile['start_votes']:
                self.volatile['start_votes'][key] = value
            # If enough votes to start election, become a Candidate
            if len(self.volatile['start_votes']) >= self.quorum:
                self.persist['startTerm'] = msg['start_term']
                self.orchestrator.change_state(Candidate)

    def on_peer_append_req(self, peer, msg, orig):
        if self.on_election or msg['term'] != self.persist['currentTerm']:
            return

        # Do not allow overwriting of already prepared messages
        if self.prepare_log.index > msg['logIndex']:
            return

        client_msg = msg['entries']
        client_addr = tuple(client_msg['client'])
        if not super().verify_sig(client_addr, client_msg, msg['sigs']):
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
            resp = {'type': 'response_append', \
                    'term': msg['term'], \
                    'logIndex': msg['logIndex'], \
                    'entry': entry, \
                    'entrySig': str(sig)}
            resp = self.sign_message(resp)
            self.orchestrator.send_peer(self.volatile['leaderId'], resp)

    def on_peer_append_prepare(self, peer, msg, orig):
        if self.on_election or msg['term'] != self.persist['currentTerm']:
            return

        # Do not allow overwriting of already prepared messages
        if self.prepare_log.index > msg['logIndex']:
            return

        sig_check = self.verify_prepares( \
                msg['entries'], \
                msg['sigs'])
        if len(msg['sigs']) >= self.quorum and sig_check:
            self.prepare_log.append_entries([msg['sigs']], msg['entries']['log_index'])

            # append new entry at tip of log
            entry = msg['entries']

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

    def on_peer_append_commit(self, peer, msg, orig):
        added = 0
        for index in range(len(msg['sigs'])):
            if msg['entries'][index]['log_index'] >= self.log.commitIndex:
                sig_check = self.verify_prepares( \
                    msg['entries'][index], \
                    msg['sigs'][index])
                if len(msg['sigs'][index]) >= self.quorum and sig_check:
                    entry = msg['entries'][index]
                    log_idx = entry['log_index']
                    # record entries, signatures, and persist the proof
                    self.log.append_entries([entry], log_idx)
                    self.commit_log.append_entries([msg['sigs'][index]], log_idx)
                    self.log.commit(log_idx+1)
                    added += 1
                    logger.debug('Log index is now %s', self.log.commitIndex)
                else:
                    logger.debug("Invalid signature!!")
                    break
        if added > 0:
            print(str(datetime.now()) + " "+ "Successfully appended to log: "+str(self.log.commitIndex-1))
            super().send_client_append_response()

        resp = {'type': 'response_success', \
                'term': self.persist['currentTerm'], \
                'matchIndex': self.log.commitIndex}
        resp = self.sign_message(resp)
        self.orchestrator.send_peer(peer, resp)

    def on_peer_append_entries(self, peer, msg, orig):
        """ Manages incoming log entries from the Leader """
        if self.on_election or msg['term'] != self.persist['currentTerm']:
            return

        self.restart_election_timer()
        self.volatile['leaderId'] = msg['leaderId']

        term_is_current = msg['term'] >= self.persist['currentTerm']
        prev_log_term_match = True
        if msg['prevLogEntry'] is not None:
            prev_term = msg['prevLogEntry']['term']
            prev_log_index = msg['prevLogEntry']['log_index']
            prev_log_term_match = (self.log.index >  prev_log_index and \
                 self.log.term(prev_log_index+1) == prev_term and \
                 len(msg['prevLogSigs']) >= self.quorum) and \
                 self.verify_prepares( \
                    msg['prevLogEntry'], \
                    msg['prevLogSigs'])

        success = term_is_current and prev_log_term_match

        # could not append to log
        if not success:
            logger.debug('Could not append entries. cause: %s', 'wrong\
                term' if not term_is_current else 'prev log term mismatch')
            resp = {'type': 'response_fail', \
                    'term': self.persist['currentTerm'], \
                    'matchIndex': self.log.commitIndex}
            resp = self.sign_message(resp)
            self.orchestrator.send_peer(peer, resp)
            return

        if msg['subType'] == 'append_commit':
            self.on_peer_append_commit(peer, msg, orig)
            return
        elif msg['subType'] == 'append_prepare':
            self.on_peer_append_prepare(peer, msg, orig)
            return
        elif msg['subType'] == 'append_req':
            self.on_peer_append_req(peer, msg, orig)
            return

    def on_client_timeout(self, protocol, msg, orig):
        """ Vote to initiate start of election. """
        self.start_vote()
        

class Candidate(Follower):
    """Candidate state. Notice that this state subclasses Follower."""
    def __init__(self, old_state=None, orchestrator=None):
        """Initialize parent, increase term, vote for self, ask for votes."""
        super().__init__(old_state, orchestrator)
        self.on_election = True
        start_terms = [self.volatile['start_votes'][key]['data']['start_term'] \
            for key in self.volatile['start_votes']]
        self.persist['startTerm'] = min(start_terms)
        print(str(datetime.now()) + " "+ "Became Candidate for term: " + str(self.persist['startTerm']))
        self.volatile['lead_votes'] = {}
        self.send_vote_requests()

    def send_vote_requests(self):
        """ Ask peers for votes. """
        entry = None if self.log.commitIndex == -1 \
                    else self.log[self.log.commitIndex]
        sig = None if self.log.commitIndex == -1 \
                    else self.prepare_log[self.log.commitIndex]
        msg = {'type': 'request_vote', \
               'term': self.persist['currentTerm'], \
               'start_term': self.persist['startTerm'], \
               'start_votes': self.volatile['start_votes'], \
               'last_commit': self.log.commitIndex, \
               'last_entry': entry, \
               'last_sig': sig}
        self.orchestrator.broadcast_peers(self.sign_message(msg))

        # vote for self
        response = {'type': 'response_vote', 
                    'term': self.persist['currentTerm'],
                    'vote_granted': True,
                    'start_term': self.persist['startTerm']}
        self.on_peer_response_vote(self.volatile['address'], response, \
                self.sign_message(response))

    def on_peer_response_vote(self, peer, msg, orig):
        """ Register peers votes, transition to Leader upon majority vote. """
        same_term = ( msg['start_term'] == self.persist['startTerm'] )

        if same_term and msg['vote_granted']:
            key = self.peer_to_string(peer)
            if not key in self.volatile['lead_votes']:
                value = {'data': msg, 'sig': str(orig[1])}
                self.volatile['lead_votes'][key] = value
            # if gathered enough votes, become Leader
            if len(self.volatile['lead_votes']) >= self.quorum:
                self.persist['currentTerm'] = msg['start_term']
                self.orchestrator.change_state(Leader)

class Leader(State):
    """Leader state."""
    def __init__(self, old_state=None, orchestrator=None):
        """ Initialize parent, sets leader variables, start periodic
            append_entries """
        super().__init__(old_state, orchestrator)
        logger.debug('Leader of term: %s', self.persist['currentTerm'])
        self.volatile['leaderId'] = self.volatile['address']
        self.matchIndex = {p: 0 for p in self.volatile['cluster']}
        self.nextIndex = {p: self.log.commitIndex + 1 for p in self.matchIndex}
        self.prepares = {}
        self.commits = {}
        self.send_append_entries()
        print(str(datetime.now()) + " "+ "Became Leader of term: "+ str(self.persist['currentTerm']))

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
                prevLogSigs = self.prepare_log[self.nextIndex[peer] - 1]

            if self.nextIndex[peer] < 0 or self.nextIndex[peer] > self.log.index:
                # no entries to send, send heartbeat
                log_index = self.nextIndex[peer]
                sub_type = 'heartbeat'
                entries = self.log[self.nextIndex[peer]: \
                                        self.nextIndex[peer] + 100]
                sigs = self.prepare_log[self.nextIndex[peer]: \
                                        self.nextIndex[peer] + 100]
            elif self.prepare_log.index < self.nextIndex[peer]:
                # new request, send copy of client message
                log_index = self.prepare_log.index
                sub_type = 'append_req'
                entries = self.prepares[log_index]['append_req']['message']
                sigs = self.prepares[log_index]['append_req']['client_sig']
            elif self.commit_log.index < self.nextIndex[peer]:
                # request signed by quorum of peers, send prepare
                log_index = self.commit_log.index
                sub_type = 'append_prepare'
                entries = self.log[log_index+1]
                sigs = self.prepare_log[log_index+1]
            else:
                # prepare confirmed by quorum of peers, send commit
                log_index = self.nextIndex[peer]
                sub_type = 'append_commit'
                entries = [self.log[log_index]]
                sigs = [self.commit_log[log_index]]

            msg = {'type': 'append_entries',
                   'subType': sub_type,
                   'term': self.persist['currentTerm'],
                   'leaderId': self.volatile['address'],
                   'lead_votes': self.volatile['lead_votes'],
                   'prevLogEntry': prevLogEntry,
                   'prevLogSigs': prevLogSigs,
                   'logIndex': log_index,
                   'entries': entries,
                   'sigs': sigs}

            logger.debug('Sending %s entries to %s. Start index %s',
                         len(msg['entries']), peer, self.nextIndex[peer])

            self.orchestrator.send_peer(peer, self.sign_message(msg))

        # schedule heartbeat for next execution
        timeout = randrange(1, 4) * 10 ** (-1 if config.debug else -2)
        loop = asyncio.get_event_loop()
        self.append_timer = loop.call_later(timeout, self.send_append_entries)

    def on_peer_response_fail(self, peer, msg, orig):
        # backtrack 1 index and try again
        self.nextIndex[peer] = max(0, self.nextIndex[peer] - 1)

    def on_peer_response_success(self, peer, msg, orig):
        # update leader's understanding of peer's updated log indices
        self.matchIndex[peer] = min(self.log.index, msg['matchIndex'])
        self.nextIndex[peer] = min(self.log.index, msg['matchIndex'] + 1)

        # because leader is also calling on_peer_response_success itself, 
        # this is the leader trying to update its own indices
        self.matchIndex[self.volatile['address']] = self.log.index
        self.nextIndex[self.volatile['address']] = self.log.index + 1

    def on_peer_response_append(self, peer, msg, orig):
        # store peer's signed prepare
        idx = msg['logIndex']
        if idx in self.prepares and peer not in self.prepares[idx]:
            if self.log.index > idx:
                if msg['entry'] == json.dumps(self.log[idx+1]):
                    sig_check = self.verify_sig( \
                        peer, \
                        json.loads(msg['entry']), \
                        str(msg['entrySig']))
                    if not sig_check:
                        return
                    sig = (json.loads(msg['entry']), str(msg['entrySig']))
                    self.prepares[idx]['sigs'][self.peer_to_string(peer)] = sig

        # try to commit entries with a quorum of signatures
        to_delete = []
        for log_idx in self.prepares:
            if len(self.prepares[log_idx]['sigs']) >= self.quorum:
                # record signatures and persist the proof
                self.prepare_log.append_entries( \
                        [self.prepares[log_idx]['sigs']], log_idx)
                to_delete.append(log_idx)

                # respond with successful prepare to self
                (entry, sig) = self.sign_message(self.log[log_idx+1])
                resp = {'type': 'response_prepare', \
                        'term': self.persist['currentTerm'], \
                        'logIndex': log_idx, \
                        'entry': entry, \
                        'entrySig': str(sig)}
                self.on_peer_response_prepare(self.volatile['address'], resp, \
                    self.sign_message(resp))

        for log_idx in to_delete:
            del self.prepares[log_idx]

    def on_peer_response_prepare(self, peer, msg, orig):
        # store peer's signed prepare
        idx = msg['logIndex']
        if idx in self.commits and peer not in self.commits[idx]:
            if self.log.index > idx:
                if msg['entry'] == json.dumps(self.log[idx+1]):
                    sig_check = self.verify_sig( \
                        peer, \
                        json.loads(msg['entry']), \
                        str(msg['entrySig']))
                    if not sig_check:
                        return
                    sig = (json.loads(msg['entry']), str(msg['entrySig']))
                    self.commits[idx]['sigs'][self.peer_to_string(peer)] = sig

        # try to commit entries with a quorum of signatures
        to_delete = []
        for log_idx in self.commits:
            if len(self.commits[log_idx]['sigs']) >= self.quorum:
                commit_idx = self.log.commit(log_idx+1)
                if commit_idx > log_idx:
                    # record signatures and persist the proof
                    self.commit_log.append_entries( \
                            [self.commits[log_idx]['sigs']], log_idx)
                    to_delete.append(log_idx)

                    # send response back to client
                    super().send_client_append_response()
        for log_idx in to_delete:
            del self.commits[log_idx]


    def on_client_append(self, protocol, msg, orig):
        new_req = super().store_client_new_req(msg)

        if new_req:
            # append new entry to tip of Leader log
            log_index = self.log.index
            entry = self.create_entry(msg)
            self.log.append_entries([entry], log_index)
            append_req = self.create_append_req(orig, log_index)
            
            self.prepares[log_index] = {'append_req': append_req, \
                                                'sigs': {}}
            self.commits[log_index] = {'sigs': {}}

            print(str(datetime.now()) + " Received new req, appending to " \
                + str(log_index))

            # respond with successful prepare to self
            (entry, sig) = self.sign_message(entry)
            resp = {'type': 'response_append', \
                    'term': self.persist['currentTerm'], \
                    'logIndex': log_index, \
                    'entry': entry, \
                    'entrySig': str(sig)}
            self.on_peer_response_append(self.volatile['address'], resp, \
                self.sign_message(resp))
        else:
            log_index = self.waiting_clients[tuple(msg['client'])]['log_idx']
            logger.debug("Retransmitting req at log index: " + str(log_index))

    def create_entry(self, msg):
        entry = {'term': self.persist['currentTerm'], \
             'data': msg['data'], \
             'log_index': self.log.index}
        return entry

    def create_append_req(self, orig_client_msg, idx):
        append_req = {'type': 'append_req', \
               'term': self.persist['currentTerm'], \
               'leaderId': self.volatile['address'], \
               'lead_votes': self.volatile['lead_votes'], \
               'logIndex': idx, \
               'message': json.loads(orig_client_msg[0]), \
               'client_sig': str(orig_client_msg[1])}
        return append_req

    def create_append_prepare():
        pass

    def create_append_commit():
        pass
