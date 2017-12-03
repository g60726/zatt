import os
import msgpack
import collections
import asyncio
import logging
from .config import config
from zatt.server import utils

logger = logging.getLogger(__name__)


class Log(collections.UserList):
    def __init__(self, erase_log=False):
        super().__init__()
        self.path = os.path.join(config.storage, 'log')
        #  load
        logger.debug('Initializing log')
        if erase_log and os.path.isfile(self.path):
            os.remove(self.path)
            logger.debug('Using parameters')
        elif os.path.isfile(self.path):
            self.data = utils.msgpack_appendable_unpack(self.path)
            logger.debug('Using persisted data')

    def append_entries(self, entries, start):
        if len(self.data) >= start:
            self.replace(self.data[:start] + entries)
        else:
            self.data += entries
            utils.msgpack_appendable_pack(entries, self.path)

    def replace(self, new_data):
        if os.path.isfile(self.path):
            os.remove(self.path)
        self.data = new_data
        utils.msgpack_appendable_pack(self.data, self.path)


class Compactor():
    def __init__(self, count=0, term=None, data={}):
        self.count = count
        self.term = term
        self.data = data
        self.path = os.path.join(config.storage, 'compact')
        #  load
        logger.debug('Initializing compactor')
        if count or term or data:
            self.persist()
            logger.debug('Using parameters')
        elif os.path.isfile(self.path):
            with open(self.path, 'rb') as f:
                self.__dict__.update(msgpack.unpack(f, encoding='utf-8'))
            logger.debug('Using persisted data')

    @property
    def index(self):
        return self.count - 1

    def persist(self):
        with open(self.path, 'wb+') as f:
            raw = {'count': self.count, 'term': self.term, 'data': self.data}
            msgpack.pack(raw, f, use_bin_type=True)


class DictStateMachine(collections.UserDict):
    def __init__(self, data={}, lastApplied=0):
        super().__init__(data)
        self.lastApplied = lastApplied

    def apply(self, items, end):
        items = items[self.lastApplied + 1:end + 1]
        for item in items:
            self.lastApplied += 1
            item = item['data']
            if item['action'] == 'change':
                self.data[item['key']] = item['value']
            elif item['action'] == 'delete':
                del self.data[item['key']]


class LogManager:
    """Instantiate and manage the components of the "Log" subsystem.
    That is: the log, the compactor and the state machine."""
    def __init__(self, compact_count=0, compact_term=None, compact_data={},
                 machine=DictStateMachine):
        erase_log = compact_count or compact_term or compact_data
        self.log = Log(erase_log)
        self.compacted = Compactor(compact_count, compact_term, compact_data)
        self.state_machine = machine(data=self.compacted.data,
                                     lastApplied=self.compacted.index)
        self.commitIndex = self.compacted.index + len(self.log)
        self.state_machine.apply(self, self.commitIndex)

    def __getitem__(self, index):
        """Get item or slice from the log, based on absolute log indexes.
        Item(s) already compacted cannot be requested."""
        if type(index) is slice:
            start = index.start - self.compacted.count if index.start else None
            stop = index.stop - self.compacted.count if index.stop else None
            return self.log[start:stop:index.step]
        elif type(index) is int:
            return self.log[index - self.compacted.count]

    @property
    def index(self):
        """Log tip index."""
        return self.compacted.index + len(self.log)

    def term(self, index=None):
        """Return a term given a log index. If no index is passed, return
        log tip term."""
        if index is None:
            return self.term(self.index)
        elif index == -1:
            return 0
        elif not len(self.log) or index <= self.compacted.index:
            return self.compacted.term
        else:
            return self[index]['term']

    def append_entries(self, entries, prevLogIndex):
        # TODO: check to make sure serialized
        self.log.append_entries(entries, prevLogIndex - self.compacted.index)
        if entries:
            logger.debug('Appending. New log: %s', self.log.data)

    def commit(self, leaderCommit):
        # enforce serial commits
        if leaderCommit == self.commitIndex + 1:
            self.commitIndex = min(leaderCommit, self.index)  # no overshoots
            logger.debug('Advancing commit to %s', self.commitIndex)
            # above is the actual commit operation, just incrementing the counter!
            # the state machine application could be asynchronous
            self.state_machine.apply(self, self.commitIndex)
            logger.debug('State machine: %s', self.state_machine.data)
        return self.commitIndex
