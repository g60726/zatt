import os
import json
import time
import asyncio
import logging
import collections
import msgpack

MAX_MSGPACK_ARRAY_HEADER_LEN = 5
logger = logging.getLogger(__name__)


class PersistentDict(collections.UserDict):
    """Dictionary data structure that is automatically persisted to disk
    as json."""
    def __init__(self, path=None, data={}):
        if os.path.isfile(path):
            with open(path, 'r') as f:
                data = json.loads(f.read())
        self.path = path
        super().__init__(data)

    def __setitem__(self, key, value):
        self.data[self.__keytransform__(key)] = value
        self.persist()

    def __delitem__(self, key):
        del self.data[self.__keytransform__(key)]
        self.persist()

    def __keytransform__(self, key):
        return key

    def persist(self):
        with open(self.path, 'w+') as f:
            f.write(json.dumps(self.data))


class TallyCounter:
    def __init__(self, categories=[]):
        self.data = {c: {'current': 0, 'past': collections.deque(maxlen=10)}
                     for c in categories}
        loop = asyncio.get_event_loop()
        loop.call_later(1, self._tick)

    def _tick(self):
        for name, category in self.data.items():
            if category['current']:
                logger.debug('Completed %s %s (%s ms/op)', category['current'],
                             name, 1/category['current'] * 1000)

            category['past'].append({time.time(): category['current']})
            category['current'] = 0
        loop = asyncio.get_event_loop()
        loop.call_later(1, self._tick)

    def increment(self, category, amount=1):
        self.data[category]['current'] += amount


def msgpack_appendable_pack(o, path):
    open(path, 'a+').close()  # touch
    with open(path, mode='r+b') as f:
        packer = msgpack.Packer()
        unpacker = msgpack.Unpacker(f)

        if type(o) == list:
            try:
                previous_len = unpacker.read_array_header()
            except msgpack.OutOfData:
                previous_len = 0

            # calculate and replace header
            header = packer.pack_array_header(previous_len + len(o))
            f.seek(0)
            f.write(header)
            f.write(bytes(1) * (MAX_MSGPACK_ARRAY_HEADER_LEN - len(header)))

            # append new elements
            f.seek(0, 2)
            for element in o:
                f.write(packer.pack(element))
        else:
            f.write(packer.pack(o))


def msgpack_appendable_unpack(path):
    # if not list?
    # return msgpack.unpackb(f.read())
    with open(path, 'rb') as f:
        packer = msgpack.Packer()
        unpacker = msgpack.Unpacker(f, encoding='utf-8')
        length = unpacker.read_array_header()

        header_lenght = len(packer.pack_array_header(length))
        unpacker.read_bytes(MAX_MSGPACK_ARRAY_HEADER_LEN - header_lenght)
        f.seek(MAX_MSGPACK_ARRAY_HEADER_LEN)

        return [unpacker.unpack() for _ in range(length)]


def extended_msgpack_serializer(obj):
    """msgpack serializer for objects not serializable by default"""

    if isinstance(obj, collections.deque):
        serial = list(obj)
        return serial
    else:
        raise TypeError("Type not serializable")
