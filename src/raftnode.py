from receiver import Receiver
from sender import Sender
import time
from timer import Timers
from messages import Reply, Request, ReplyFn
from mqueue import MessageQueue

FOLLOWER = "Follower"
CANDIDATE = "Candidate"
LEADER = "Leader"


class Raftnode:
    def __init__(self, node_id, external_port, internal_port, peer_ports, port_to_id):
        print(f"Starting server {node_id} at internal port {internal_port}")

        """Constants """
        self.TOTAL_NUM_NODES = len(peer_ports) + 1
        self.MAJORITY = self.TOTAL_NUM_NODES / 2.0
        self.node_id = node_id

        """ Set up shared variables """
        self.role = "Follower"
        self.initialize_state()

        """ initialize reply function """
        self.replyfn = ReplyFn(self)
        self.request = Request(self)

        """ fork a thread for external receiver """
        Receiver(external_port, self.receive_external_handler, self)
        """ fork a thread for internal receiver"""
        Receiver(internal_port, self.receive_internal_handler, self)
        """ create threads for election & heartbeat timers """
        #NOTE: WANT LARGE TIME DIFFERENCE BETWEEN ELECTION TIMEOUT AND HEARTBEAT TIMEOUT
        self.electionTimeout = Timers(1000, 1500, self.switchToCandidate, None)
        self.heartbeatTimeout = Timers(200, 400, self.broadcast_heartbeat, None)

        """ create a broadcaster  """
        self.sender = Sender(internal_port, peer_ports, port_to_id, self.request)

        """ create a message queue """
        self.mqueue = MessageQueue()

        """ create a flag to signal client request processing is done """
        self.client_req_done = False
        self.response_to_client = None

        """ initialize by switching to follower """
        self.switchToFollower()



    def receive_external_handler(self, message):
        """
            Blocking function passed to external receiver that adds entry to leader
            and replicates on followers before returning back to the client
        """
        mtype = message['type']
        method = message['method']

        if mtype == "status" and method == "GET":
            return {'role': self.role, 'term': self.currentTerm}
        if mtype == "debug":
            self.log[0] = message
            print(self.log)
        if mtype == "topic" and method == "GET":
            return self.mqueue.get_topics()


        if self.role == "Leader":
            self.add_to_log(message)
        while not self.client_req_done:
            time.sleep(0.000002)

        self.client_req_done = False
        return self.response_to_client



    def receive_internal_handler(self, message):
        """
            Non-blocking unction passed to internal receiver that handles
            processing of all internal communication
        """
        if message['type'] != 'internal':
            print("ERROR: internal receiver somehow got a non-internal message")

        subtype = message['subtype']
        if subtype == "RequestVote":
            self.replyfn.process_request_vote(message)

        if subtype == "RequestVoteResponse":
            self.replyfn.process_request_vote_response(message)

        if subtype == "AppendEntries":
            self.replyfn.process_append_entries(message)

        if subtype == "AppendEntriesResponse":
            self.replyfn.process_append_entries_response(message)


    def switchToFollower(self):
        """ Switches node role to Follower """
        self.votedFor = None
        self.role = "Follower"
        self.electionTimeout.restart()

    def switchToCandidate(self):
        """ Switches node role to Candidate """
        self.role = "Candidate"
        self.electionTimeout.restart()
        self.currentTerm += 1
        self.num_votes = 0
        self.num_votes += 1  # vote for self
        self.votedFor = self.node_id
        self.replyfn.check_election_winner()  # wins election if only 1 node in system
        self.sender.broadcast(self.request.REQUEST_VOTE())

    def switchToLeader(self):
        """ Switches node role to Leader """
        self.initialize_leader_state()
        self.role = "Leader"
        self.electionTimeout.cancel()
        self.heartbeatTimeout.restart()

    def add_to_log(self, command):
        """ Leader adds  entry to log, broadcasts new entry to followers, and attempts committing the entry"""
        last_index = self.last_log_index()
        self.log.append({"index": last_index + 1, "term": self.currentTerm, "command": command})
        self.sender.broadcast_appendEntries(is_heartbeat=False)
        self.attempt_update_commit_index_helper()
        self.attempt_apply_entry()

    def broadcast_heartbeat(self):
        """ Broadcasts the heartbeat appendEntries RPC """
        self.sender.broadcast_appendEntries(is_heartbeat=True)
        self.heartbeatTimeout.restart()

    def isRole(self, role):
        """ Returns if provided role is node's role """
        return self.role == role

    def initialize_leader_state(self):
        """ Initialization of state on Leader  """
        self.nextIndex = [self.last_log_index() + 1 for _ in range(self.TOTAL_NUM_NODES)]
        self.matchIndex = [0 for _ in range(self.TOTAL_NUM_NODES)]

    def initialize_state(self):
        """ Initialization of state on all nodes """
        self.currentTerm = 0
        self.votedFor = None
        self.num_votes = 0
        self.log = []
        self.commitIndex = 0
        self.lastApplied = 0

    def last_log_index(self):
        """ Returns index of last entry in log """
        if len(self.log) == 0:
            return 0
        return self.log[-1]["index"]

    def last_log_term(self):
        """ Returns term of last entry in log """
        if len(self.log) == 0:
            return 0
        return self.log[-1]["term"]

    def prev_log_index(self, node_id):
        """ Returns index of log entry immediately preceding new ones """
        if self.nextIndex[node_id] <= 0:
            return 0
        else:
            return self.nextIndex[node_id] - 1

    def prev_log_term(self, node_id):
        """ Returns term of prev_log_index entry """
        prev_log_index = self.prev_log_index(node_id)
        arr_prev_log_index = prev_log_index - 1
        if arr_prev_log_index < 0:  # outside array bounds
            return 0
        return self.log[arr_prev_log_index]["term"]

    def entries(self, node_id):
        """ Entries to be sent to a particular Follower by the Leader """
        nextIndex = self.nextIndex[node_id]
        arr_nextIndex = nextIndex - 1
        return self.log[arr_nextIndex:]

    def leaderCommit(self):
        """ leader's commitIndex """
        return self.commitIndex

    def getCurrentTerm(self):
        """ rRturns the current term """
        return self.currentTerm

    def leader_attempt_update_commit_index(self):
        """
            Leader attempts to update commit index upon reply
            from a follower to AppendEntries RPC
        """
        while self.attempt_update_commit_index_helper():
            time.sleep(0.00001)

    def attempt_update_commit_index_helper(self):
        """ Attempt to update leader's commit index """
        N = self.commitIndex + 1
        if N > len(self.log):  # can't increase commit index if no entry to commit
            return False
        if len(self.log) == 0:
            return False

        # if majority of match index is greater than equal to N, +1 because count self (leader)
        is_majority = sum(i >= N for i in self.matchIndex) + 1 > self.MAJORITY
        arr_N = N-1
        same_term = self.log[arr_N]["term"] == self.currentTerm
        if is_majority and same_term:
            self.commitIndex = N
            return True

        return False

    def attempt_apply_entry(self):
        """ Attempt applying entries locally """
        while self.attempt_apply_entry_helper():
            time.sleep(0.00001)

    def attempt_apply_entry_helper(self):
        """ Apply all entries in log between lastApplies and commitIndex """
        if self.commitIndex > self.lastApplied:
            self.lastApplied += 1
            arr_idx_to_apply = self.lastApplied - 1
            print(f"Applying index {self.lastApplied}, command: {self.log[arr_idx_to_apply]}")
            self.response_to_client = self.mqueue.process(self.log[arr_idx_to_apply]["command"])
            if self.isRole(LEADER):
                self.client_req_done = True
            return True
        return False

