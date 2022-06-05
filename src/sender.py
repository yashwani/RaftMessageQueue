from utils import *
from threading import Thread, Lock

REQUEST_TIMEOUT = 1000  # milliseconds


def peer2peer(context, port, message):
    """ keeps trying to contact a node until success """
    while True:
        try:
            socket = context.socket(zmq.REQ)
            socket.connect(f"tcp://localhost:{port}")
            # print(f"Sending message {message} to port {port}")
            send_json(socket, message)
            recv_json(socket)
            break
        except Exception:
            continue


class Sender:
    def __init__(self, sender_port, peers, port_to_id, request):
        self.req_id = 0
        self.peers = peers
        self.sender_port = sender_port
        self.context = zmq.Context()
        self.port_to_id = port_to_id
        self.request = request

    def broadcast_appendEntries(self, is_heartbeat):
        Thread(target=self.broadcast_appendEntries_helper, args=[is_heartbeat]).start()

    def broadcast_appendEntries_helper(self, is_heartbeat):
        """
        differs from broadcast_helper in that it broadcasts node_specific
        messages
        """
        for peer_port in self.peers:
            peer_id = self.port_to_id[peer_port]
            message = self.request.APPEND_ENTRIES(peer_id, is_heartbeat)
            self.spawn_peer2peer(message, peer_port)


    def broadcast(self, message):
        """ Launches broadcast RPC in a thread """
        Thread(target=self.broadcast_helper, args=[message]).start()

    def broadcast_helper(self, message):
        """ Sends p2p request to every peer port """
        for peer_port in self.peers:
            self.spawn_peer2peer(message, peer_port)

    def spawn_peer2peer(self, message, peer_port):
        """ Sends a p2p request with a timeout """
        message['sender_port'] = self.sender_port  # add sender's port to all messages
        self.req_id += 1
        message['req_id'] = self.req_id  # assign unique id to each request

        t = Thread(target=peer2peer, args=(self.context, peer_port, message))
        t.start()
        t.join(timeout=0.002)



