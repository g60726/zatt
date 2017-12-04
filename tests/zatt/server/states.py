import asyncio
import logging
import statistics
from random import randrange
from os.path import join
from .utils import PersistentDict, TallyCounter
from .log import LogManager
from .config import config

logger = logging.getLogger(__name__)


class State:
    """Abstract state for subclassing."""
    def __init__(self, old_state=None, orchestrator=None):
        """State is initialized passing an orchestrator instance when first
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
                             'address':config.address, 'candidateID':0}
            self.log = LogManager()
            self._update_cluster()
        self.stats = TallyCounter(['read', 'write', 'append'])
        self.candyCollection = {}

    def data_received_peer(self, peer, msg):
        """Receive peer messages from orchestrator and pass them to the
        appropriate method."""
        logger.debug('Received %s from %s', msg['type'], peer)

        if self.persist['currentTerm'] < msg['term']:
            self.persist['currentTerm'] = msg['term']
            if not type(self) is Follower:
                logger.info('Remote term is higher, converting to Follower')
                self.orchestrator.change_state(Follower)
                self.orchestrator.state.data_received_peer(peer, msg)
                return
        method = getattr(self, 'on_peer_' + msg['type'], None)
        if method:
            method(peer, msg)
        else:
            logger.info('Unrecognized message from %s: %s', peer, msg)
###receive leader election request
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
        msg = {'type': 'redirect',
               'leader': self.volatile['leaderId']}
        protocol.send(msg)
        logger.debug('Redirect client %s:%s to leader',
                     *protocol.transport.get_extra_info('peername'))

    def on_client_config(self, protocol, msg):
        """Redirect client to leader upon receiving a client_config message."""
        return self.on_client_append(protocol, msg)

    def on_client_diagnostic(self, protocol, msg):
        """Return internal state to client."""
        msg = {'status': self.__class__.__name__,
               'persist': {'votedFor': self.persist['votedFor'],
                           'currentTerm': self.persist['currentTerm']},
               'volatile': self.volatile,
               'log': {'commitIndex': self.log.commitIndex},
               'stats': self.stats.data}
        msg['volatile']['cluster'] = list(msg['volatile']['cluster'])

        if type(self) is Leader:
            msg.update({'leaderStatus':
                        {'netIndex': tuple(self.nextIndex.items()),
                         'matchIndex': tuple(self.matchIndex.items()),
                         'waiting_clients': {k: len(v) for (k, v) in
                                             self.waiting_clients.items()}}})
        protocol.send(msg)

    def on_client_get(self, protocol, msg):
        """Redirect client to leader upon receiving a client_get message."""
        msg = {'type': 'redirect',
               'leader': self.volatile['leaderId']}
        protocol.send(msg)
        logger.debug('Redirect client %s:%s to leader',
                     *protocol.transport.get_extra_info('peername'))

    def _update_cluster(self, entries=None):
        """Scans compacted log and log, looking for the latest cluster
        configuration."""
        if 'cluster' in self.log.compacted.data:
            self.volatile['cluster'] = self.log.compacted.data['cluster']
        for entry in (self.log if entries is None else entries):
            if entry['data']['key'] == 'cluster':
                self.volatile['cluster'] = entry['data']['value']
        self.volatile['cluster'] = tuple(map(tuple, self.volatile['cluster']))


class Follower(State):
    """Follower state."""
    #add a follower ID
    def __init__(self, old_state=None, orchestrator=None, ID=None):
        """Initialize parent and start election timer."""
        super().__init__(old_state, orchestrator)
        self.persist['votedFor'] = self.persist['currentTerm'] % len(self.volatile['cluster'])
        self.restart_election_timer()
        self.ID = ID
        self.on_election = False
        #self.nextTermToVote
        #self.track = {'term':, 'termToVote':}
        #v mode R is preset candidate
        '''
        if self.ID == self.persist['currentTerm'] % len(self.volatile['cluster']):
            self.orchestrator.change_state(Candidate)
        '''

    def teardown(self):
        """Stop timers before changing state."""
        self.election_timer.cancel()

    def restart_election_timer(self):
        """Delays transition to the Candidate state by timer."""
        if hasattr(self, 'election_timer'):
            self.election_timer.cancel()

        timeout = randrange(1, 4) * 10 ** (0 if config.debug else -1)
        loop = asyncio.get_event_loop()
        '''
        self.election_timer = loop.\
            call_later(timeout, self.orchestrator.change_state, Candidate)
        '''
        #if time out then start_vote
        self.election_timer = loop.\
            call_later(timeout, self.start_vote)
        logger.debug('Election timer restarted: %s s', timeout)

    ###start_vote
    def start_vote(self):
        self.on_election = True
        msg = {'type': 'start_vote',
               'voteGranted': granted,
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
        
            '''
            method = getattr(self, 'on_candy_' + msg['type'], None) #msg['type'] = start_vote
            if method:
                method(msg)
            '''
    #follower finishes election
    def on_peer_finish_election(self,msg):
        self.on_election = False
        self.persist['currentTerm'] = msg['term']
        self.persist['votedFor'] = msg['votedFor']

    def on_peer_request_vote(self, peer, msg):
        """Grant this node's vote to Candidates."""
        term_is_current = msg['term'] >= self.persist['currentTerm']
        can_vote = self.persist['votedFor'] == msg['candidateId']
        index_is_current = (msg['lastLogTerm'] > self.log.term() or (msg['lastLogTerm'] == self.log.term() and
                            msg['lastLogIndex'] >= self.log.index))
        granted = term_is_current and can_vote and index_is_current
                                                
        if granted:
            #self.persist['votedFor'] = msg['candidateId']
            #self.restart_election_timer()
            logger.debug('Voting for %s. Term:%s Vote:%s Index:%s',peer, term_is_current, can_vote,index_is_current)
            response = {'type': 'response_vote', 'voteGranted': granted,'term': self.persist['currentTerm']}
            self.orchestrator.send_peer(peer, response)

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

        if 'compact_data' in msg:
            self.log = LogManager(compact_count=msg['compact_count'],
                                  compact_term=msg['compact_term'],
                                  compact_data=msg['compact_data'])
            self.volatile['leaderId'] = msg['leaderId']
            logger.debug('Initialized Log with compact data from Leader')
        elif success:
            self.log.append_entries(msg['entries'], msg['prevLogIndex'])
            self.log.commit(msg['leaderCommit'])
            self.volatile['leaderId'] = msg['leaderId']
            logger.debug('Log index is now %s', self.log.index)
            self.stats.increment('append', len(msg['entries']))
        else:
            logger.warning('Could not append entries. cause: %s', 'wrong\
                term' if not term_is_current else 'prev log term mismatch')

        self._update_cluster()

        resp = {'type': 'response_append', 'success': success,
                'term': self.persist['currentTerm'],
                'matchIndex': self.log.index}
        self.orchestrator.send_peer(peer, resp)


class Candidate(Follower):
    """Candidate state. Notice that this state subclasses Follower."""
    def __init__(self, old_state=None, orchestrator=None):
        """Initialize parent, increase term, vote for self, ask for votes."""
        super().__init__(old_state, orchestrator)
        #self.persist['currentTerm'] += 1
        self.votes_count = 1
        logger.debug('New Election. Term: %s', self.persist['currentTerm'])
        #self.send_vote_requests()

        def vote_self():
            self.persist['votedFor'] = self.volatile['address']
            self.on_peer_response_vote(
                self.volatile['address'], {'voteGranted': True})
        
        loop = asyncio.get_event_loop()
        loop.call_soon(vote_self)
    
    def on_peer_request_vote(self,peer, msg):
        #if candidate term is smaller than peer's term, stand down
        if self.persist['currentTerm'] < msg['term']:
            self.orchestrator.change_state(Follower)
        else:
            #vote back to senders if fullfilled conditions
            can_vote = self.persist['votedFor'] == msg['candidateId']
            index_is_current = (msg['lastLogTerm'] > self.log.term() or (msg['lastLogTerm'] == self.log.term() and
                                msg['lastLogIndex'] >= self.log.index))
            granted = can_vote and index_is_current
                                                                     
            if granted:
                logger.debug('Voting for %s. Term:%s Vote:%s Index:%s',peer, term_is_current, can_vote,index_is_current)
                response = {'type': 'response_vote', 'voteGranted': granted,'term': self.persist['currentTerm']}
                self.orchestrator.send_peer(peer, response)

    def send_vote_requests(self):
        """Ask peers for votes."""
        logger.debug('Broadcasting request_vote')
        msg = {'type': 'request_vote', 'term': self.persist['currentTerm'],
               'candidateId': self.ID,
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
        self.votes_count += msg['voteGranted']
        logger.debug('Vote count: %s', self.votes_count)
        if self.votes_count > len(self.volatile['cluster']) / 2:
            #transform to leader
            self.orchestrator.change_state(Leader)
            #braodcast current parameters to synchronize everyone
            self.persist['currentTerm'] += 1
            self.persist['votedFor'] += 1
            msg = {'type': 'finish_election',
                   'term': self.persist['currentTerm']
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
#Question: currently the first candidate receiving enough votes transforms to leader, how should I improve it?

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
                client.send({'type': 'result', 'success': False})
                logger.error('Sent unsuccessful response to client')

    def send_append_entries(self):
        """Send append_entries to the cluster, containing:
        - nothing: if remote node is up to date.
        - compacted log: if remote node has to catch up.
        - log entries: if available.
        Finally schedules itself for later esecution."""
        for peer in self.volatile['cluster']:
            if peer == self.volatile['address']:
                continue
            msg = {'type': 'append_entries',
                   'term': self.persist['currentTerm'],
                   'leaderCommit': self.log.commitIndex,
                   'leaderId': self.volatile['address'],
                   'prevLogIndex': self.nextIndex[peer] - 1,
                   'entries': self.log[self.nextIndex[peer]:
                                       self.nextIndex[peer] + 100]}
            msg.update({'prevLogTerm': self.log.term(msg['prevLogIndex'])})

            if self.nextIndex[peer] <= self.log.compacted.index:
                msg.update({'compact_data': self.log.compacted.data,
                            'compact_term': self.log.compacted.term,
                            'compact_count': self.log.compacted.count})

            logger.debug('Sending %s entries to %s. Start index %s',
                         len(msg['entries']), peer, self.nextIndex[peer])
            self.orchestrator.send_peer(peer, msg)

        timeout = randrange(1, 4) * 10 ** (-1 if config.debug else -2)
        loop = asyncio.get_event_loop()
        self.append_timer = loop.call_later(timeout, self.send_append_entries)

    def on_peer_response_append(self, peer, msg):
        """Handle peer response to append_entries.
        If successful RPC, try to commit new entries.
        If RPC unsuccessful, backtrack."""
        if msg['success']:
            self.matchIndex[peer] = msg['matchIndex']
            self.nextIndex[peer] = msg['matchIndex'] + 1

            self.matchIndex[self.volatile['address']] = self.log.index
            self.nextIndex[self.volatile['address']] = self.log.index + 1
            index = statistics.median_low(self.matchIndex.values())
            self.log.commit(index)
            self.send_client_append_response()
        else:
            self.nextIndex[peer] = max(0, self.nextIndex[peer] - 1)

    def on_client_append(self, protocol, msg):
        """Append new entries to Leader log."""
        entry = {'term': self.persist['currentTerm'], 'data': msg['data']}
        if msg['data']['key'] == 'cluster':
            protocol.send({'type': 'result', 'success': False})
        self.log.append_entries([entry], self.log.index)
        if self.log.index in self.waiting_clients:
            self.waiting_clients[self.log.index].append(protocol)
        else:
            self.waiting_clients[self.log.index] = [protocol]
        self.on_peer_response_append(
            self.volatile['address'], {'success': True,
                                       'matchIndex': self.log.commitIndex})

    def on_client_get(self, protocol, msg):
        """Return state machine to client."""
        state_machine = self.log.state_machine.data.copy()
        self.stats.increment('read')
        protocol.send(state_machine)

    def send_client_append_response(self):
        """Respond to client upon commitment of log entries."""
        to_delete = []
        for client_index, clients in self.waiting_clients.items():
            if client_index <= self.log.commitIndex:
                for client in clients:
                    client.send({'type': 'result', 'success': True})  # TODO
                    logger.debug('Sent successful response to client')
                    self.stats.increment('write')
                to_delete.append(client_index)
        for index in to_delete:
            del self.waiting_clients[index]

    def on_client_config(self, protocol, msg):
        """Push new cluster config. When uncommitted cluster changes
        are already present, retries until they are committed
        before proceding."""
        pending_configs = tuple(filter(lambda x: x['data']['key'] == 'cluster',
                                self.log[self.log.commitIndex + 1:]))
        if pending_configs:
            timeout = randrange(1, 4) * 10 ** (0 if config.debug else -1)
            loop = asyncio.get_event_loop()
            self.config_timer = loop.\
                call_later(timeout, self.on_client_config, protocol, msg)
            return

        success = True
        cluster = set(self.volatile['cluster'])
        peer = (msg['address'], int(msg['port']))
        if msg['action'] == 'add' and peer not in cluster:
            logger.info('Adding node %s', peer)
            cluster.add(peer)
            self.nextIndex[peer] = 0
            self.matchIndex[peer] = 0
        elif msg['action'] == 'delete' and peer in cluster:
            logger.info('Removing node %s', peer)
            cluster.remove(peer)
            del self.nextIndex[peer]
            del self.matchIndex[peer]
        else:
            success = False
        if success:
            self.log.append_entries([
                {'term': self.persist['currentTerm'],
                 'data':{'key': 'cluster', 'value': tuple(cluster),
                         'action': 'change'}}],
                self.log.index)
            self.volatile['cluster'] = cluster
        protocol.send({'type': 'result', 'success': success})
