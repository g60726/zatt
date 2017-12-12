import asyncio
import logging
import random
import json
from zatt.common import crypto
from zatt.server.config import config

logger = logging.getLogger(__name__)

timeout = ((1*5**-1) + (4*5**-1))/2.0

def generate_request_vote(term):
    msg =   {
                'type': 'request_vote', \
                'term': random.randint(term, term+1), \
                'start_term': random.randint(term+1, term+5), \
                'start_votes': {}, \
                'last_commit': random.randint(term, term+5), \
                'last_entry': {}, \
                'last_sig': {}
            }
    return msg

def generate_start_vote(term):
    msg =   {
                'type': 'start_vote',
                'term': random.randint(term, term+1),
                'start_term': random.randint(term+1, term+5)
            }
    return msg

def generate_response_vote(term):
    msg =   {   
                'type': 'response_vote', 
                'term': random.randint(term, term+1),
                'vote_granted': True,
                'start_term': random.randint(term+1, term+5)
            }
    return msg

def generate_response_prepare(term):
    msg =   {
                'type': 'response_prepare',
                'term': random.randint(term, term+1),
                'logIndex': random.randint(term, term+5),
                'entry': {},
                'entrySig': "Not a sig"
            }
    return msg

def generate_response_append(term):
    msg =   {
                'type': 'response_append',
                'term': random.randint(term, term+1),
                'logIndex': random.randint(term, term+5),
                'entry': {},
                'entrySig': "Not a sig"
            }
    return msg

def generate_response_fail(term):
    msg =   {
                'type': 'response_fail',
                'term': random.randint(term, term+1),
                'matchIndex': random.randint(term, term+5)
            }
    return msg

def generate_response_success(term):
    msg =   {
                'type': 'response_success',
                'term': random.randint(term, term+1),
                'matchIndex': random.randint(term, term+5)
            }
    return msg

message_generators = \
    [ \
        generate_request_vote, generate_start_vote, generate_response_vote, \
        generate_response_append, generate_response_prepare, \
        generate_response_fail, generate_response_success \
    ]

def generate_random_message(term):
    return random.choice(message_generators)(term)

class ChaosMonkey:
    """ ChaosMonkey state to simulate Byzantine failures. """
    def __init__(self, old_state=None, orchestrator=None):
        self.orchestrator = orchestrator
        self.volatile = {'leaderId': None, 'cluster': config.cluster,
            'address': config.address, 'private_key': config.private_key,
            'public_keys': config.public_keys, 'clients': config.clients,
            'client_keys': config.client_keys, 'node_id': int(config.id),
            'start_votes': {}, 'server_ids': config.server_ids,
            'lead_votes': {}}
        self.term = 0
        loop = asyncio.get_event_loop()
        loop.call_later(timeout, self.send_random_message)

    def send_random_message(self):
        msg = generate_random_message(self.term)
        print(msg)
        signed = self.sign_message(msg)
        self.orchestrator.broadcast_peers(signed)
        loop = asyncio.get_event_loop()
        loop.call_later(timeout, self.send_random_message)
        pass

    def sign_message(self, msg):
        signature = crypto.sign_message(json.dumps(msg), self.volatile['private_key'])
        return [json.dumps(msg), signature]

    def data_received_peer(self, peer, msg):
        actualMsg = json.loads(msg[0])
        self.term = actualMsg['term']

    def data_received_client(self, protocol, msg):
        pass