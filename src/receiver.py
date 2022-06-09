from threading import Thread
import zmq
from utils import *
import time
from messages import Reply


class Receiver:
    def __init__(self, port, received_msg_handler, raftnode):
        """
        Launches a receiver thread that uses {received_msg_handler} to handle requests at {port}
        """
        self.reply = Reply(raftnode)
        self.received_msg_handler = received_msg_handler
        context = zmq.Context()
        self.socket = context.socket(zmq.REP)
        self.socket.bind(f"tcp://127.0.0.1:{port}")
        self.thread = Thread(target=self.receive_loop)
        self.thread.start()

    def receive_loop(self):
        """
        Loops infinitely, receiving, processing, and replying to messages

        External client-facing communication obeys the rep/req zmq interface, and the appropriate reply is sent back

        Internal node communication is handled asynchronously. All "replies" to requests are implemented as requests
        by the replying node. Therefore, internal zmq.REP communication uses a dummy reply.
        """
        while True:
            request = recv_json(self.socket)  # client message

            reply = self.received_msg_handler(request)  # blocks if external communication
            if self.is_internal_communication():
                reply = self.reply.DUMMY_REPLY
            send_json(self.socket, reply)

            time.sleep(0.000001)  # allow cooperative scheduling

    def is_internal_communication(self):
        return self.receiver_type == "receive_internal_handler"

    def receiver_type(self):
        return self.received_msg_handler.__name__.split('_')[1]
