from itertools import islice
from time import time, sleep
from urllib import parse

from socialreaper import Source, Iter, ApiError, IterError
from socialreaper.apis import API
from socialreaper.tools import flatten, CSV

import networkx as nx
from tqdm import tqdm


class VenmoAPI(API):
    def __init__(self):
        super().__init__()

        self.url = "https://venmo.com/api/v5"
        self.request_rate = 30
        self.retry_rate = 30
        self.last_request = time()

    def api_call(self, edge, parameters, return_results=True):
        req = self.get("%s/%s" % (self.url, edge), params=parameters)

        time_diff = time() - self.last_request
        if time_diff < self.request_rate:
            sleep(self.request_rate - time_diff)

        self.last_request = time()

        if not req:
            return None

        if return_results:
            return req.json()

    def public_feed(self, since=None, until=None, limit=None):
        parameters = {
            "since": since,
            "until": until,
            "limit": limit
        }
        return self.api_call('public', parameters)


class Venmo(Source):
    def __init__(self):
        super().__init__()
        self.api = VenmoAPI()

    class VenmoIter(Iter):
        def __init__(self, api):
            super().__init__()
            self.api = api
            self.params = {'limit': 50}

        def get_until(self):
            if self.page_count == 1:
                return
            elif self.response.get('paging'):
                self.params = dict(parse.parse_qsl(parse.urlparse(self.response['paging']['next']).query))
                self.params['limit'] = 50
            else:
                raise StopIteration

        def read_response(self):
            if self.response.get('data'):
                return self.response['data']
            else:
                raise StopIteration

        def get_data(self):
            self.page_count += 1

            self.get_until()

            try:
                self.response = self.api.public_feed(**self.params)
                self.data = self.read_response()
            except ApiError as e:
                raise IterError(e, vars(self))

    def feed(self):
        return self.VenmoIter(self.api)


class NodeFactory:
    def __init__(self, graph, feed, limit=100):
        self.graph = graph
        self.feed = feed
        self.limit = limit
        self.data = []

        for data in tqdm(islice(feed, limit), total=limit, desc='Transactions'):
            self.data.append(data)

            sender, receivers, details = self.create_nodes(data)

            sid, sender = sender

            self.graph.add_node(sid, **flatten(sender))
            for rid, receiver in receivers:
                self.graph.add_node(rid, **flatten(receiver))
                self.graph.add_edge(sid, rid, **flatten(details))

        CSV(self.data)

    def create_nodes(self, data):
        sender, details = self.parse_sender(data)
        receivers = [self.parse_transaction(t) for t in data['transactions']]

        return sender, receivers, details

    @staticmethod
    def parse_sender(data):
        sid = data['actor']['id']
        sender = dict(data['actor'])
        details = dict(data)

        del details['actor']
        del details['transactions']
        details['t_type'] = details.pop('type')

        return (sid, sender), details

    @staticmethod
    def parse_transaction(t):
        receiver = t['target']

        if receiver == 'a phone number':
            rid, receiver = 'phone', {}
        elif receiver == 'an email':
            rid, receiver = 'email', {}
        elif receiver['name'] == 'a user on iMessage':
            rid = 'iMessage'
        else:
            rid = receiver['id']

        return rid, receiver

if __name__ == "__main__":
    G = nx.DiGraph()
    feed = Venmo().feed()
    limit = 100000 # approx. 16-17 hours

    nf = NodeFactory(G, feed, limit)

    nx.write_gexf(G, 'out.gexf')
