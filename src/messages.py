FOLLOWER = "Follower"
CANDIDATE = "Candidate"
LEADER = "Leader"


class Reply:
    """ Lists all possible replies that can be send by nodes"""

    def __init__(self, raftnode):
        self.N = raftnode
        self.DUMMY_REPLY = {"type": "dummy reply"}

    def VOTE_GRANTED_REPLY(self):
        return {'type': 'internal', 'subtype': 'RequestVoteResponse',
                'term': self.N.getCurrentTerm(),
                'voteGranted': True}

    def VOTE_WITHHELD_REPLY(self):
        return {'type': 'internal', 'subtype': 'RequestVoteResponse',
                'term': self.N.getCurrentTerm(),
                'voteGranted': False}

    def APPEND_ENTRIES_SUCCESS(self, matchIndex):
        return {'type': 'internal', 'subtype': 'AppendEntriesResponse',
                'term': self.N.getCurrentTerm(),
                'success': True, "node_id": self.N.node_id, "matchIndex": matchIndex}

    def APPEND_ENTRIES_FAILURE(self):
        return {'type': 'internal', 'subtype': 'AppendEntriesResponse',
                'term': self.N.getCurrentTerm(),
                'success': False, "node_id": self.N.node_id}


class Request:
    """ Lists out all possible requests that can be sent out by nodes """

    def __init__(self, raftnode):
        self.N = raftnode

    def REQUEST_VOTE(self):
        return {'type': 'internal', 'subtype': 'RequestVote', 'term': self.N.getCurrentTerm(),
                'candidateId': self.N.node_id, 'lastLogIndex': self.N.last_log_index(),
                'lastLogTerm': self.N.last_log_term()}

    def APPEND_ENTRIES(self, node_id, is_heartbeat):
        if is_heartbeat:
            return {'type': 'internal', 'subtype': 'AppendEntries', 'term': self.N.getCurrentTerm(),
                    'prevLogIndex': self.N.prev_log_index(node_id), 'prevLogTerm': self.N.prev_log_term(node_id),
                    'entries': [], 'leaderCommit': self.N.commitIndex}
        else:
            return {'type': 'internal', 'subtype': 'AppendEntries', 'term': self.N.getCurrentTerm(),
                    'prevLogIndex': self.N.prev_log_index(node_id), 'prevLogTerm': self.N.prev_log_term(node_id),
                    'entries': self.N.entries(node_id), 'leaderCommit': self.N.commitIndex}


class ReplyFn:
    """
    Handles all replying logic for any incoming request
    """

    def __init__(self, raftnode):
        self.N = raftnode
        self.reply = Reply(self.N)
        self.request = Request(self.N)

    def process_append_entries(self, request):
        """ Election Logic"""

        self.all_server_rules(request)
        self.N.electionTimeout.restart()
        self.N.votedFor = None

        #  1. Reply false if term < currentTerm
        if request['term'] < self.N.currentTerm:
            self.N.sender.spawn_peer2peer(self.reply.APPEND_ENTRIES_FAILURE(), request["sender_port"])
            print("failure1")
            return

        # 2. Reply false if log doesn’t contain an entry at prevLogIndex
        # whose term matches prevLogTerm

        if len(self.N.log) == 0 or request['prevLogIndex'] == 0:  # if no entries and prevLogIndex is 0 then continue
            pass
        elif len(self.N.log) < request['prevLogIndex']:  # if no entry at prevLogIndex then fail
            self.N.sender.spawn_peer2peer(self.reply.APPEND_ENTRIES_FAILURE(), request["sender_port"])
            print("failure2")
            return
        else:  # if the entry at prevLogIndex's term doesn't match up then fail
            arr_prevLogIndex = request['prevLogIndex'] - 1

            if self.N.log[arr_prevLogIndex]["term"] != request["prevLogTerm"]:
                print(self.N.log)
                print(f"local prev log term is {self.N.log[arr_prevLogIndex]['term']}")
                print(f"Request prev log term is {request['prevLogTerm']}")
                print(f'Request prev log index is {request["prevLogTerm"]}')
                self.N.sender.spawn_peer2peer(self.reply.APPEND_ENTRIES_FAILURE(), request["sender_port"])
                print("failure 3")
                return

        # 3. If an existing entry conflicts with a new one (same index
        # but different terms), delete the existing entry and all that
        # follow it

        shouldBreak = False
        existing_conflict_idx = len(self.N.log)
        for existing_idx, existing_entry in enumerate(self.N.log):
            for new_idx, new_entry in enumerate(request["entries"]):
                if existing_entry["index"] == new_entry["index"] and existing_entry["term"] != new_entry["term"]:
                    existing_conflict_idx = existing_idx
                    shouldBreak = True
                    break
            if shouldBreak:
                break

        self.N.log = self.N.log[:existing_conflict_idx]  # delete existing

        # add any entries in leader log not in follower log
        if len(self.N.log) == 0:
            self.N.log = self.N.log + request["entries"]
        else:
            new_entries_start = 0
            for new_idx, new_entry in enumerate(request["entries"]):
                inLog = False
                for existing_idx, existing_entry in enumerate(self.N.log):
                    inLog = inLog or new_entry["index"] == existing_entry["index"]
                if not inLog:
                    new_entries_start = new_idx
                    break

            self.N.log = self.N.log + request["entries"][new_entries_start:]  # append new

        if request["entries"]:
            print("############### LOG POTENTIALLY UPDATED ######################")
            print(self.N.log)
            print("############### LOG POTENTIALLY UPDATED ######################")

        # If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        if request["leaderCommit"] > self.N.commitIndex:
            idx_last_new_entry = 0
            if len(self.N.log) > 0:
                idx_last_new_entry = self.N.log[-1]["index"]
            self.N.commitIndex = min(request["leaderCommit"], idx_last_new_entry)
        self.N.attempt_apply_entry()

        if self.N.log:
            matchIndex = self.N.log[-1]["index"]
            print(matchIndex)
        else:
            matchIndex = 0

        print(f"sending back match index {matchIndex}")
        return self.N.sender.spawn_peer2peer(self.reply.APPEND_ENTRIES_SUCCESS(matchIndex), request["sender_port"])

    def process_append_entries_response(self, request):
        # self.all_server_rules(request)
        follower_id = request["node_id"]

        self.all_server_rules(request)
        if self.N.isRole(LEADER):
            if request["success"]:
                self.N.matchIndex[follower_id] = request["matchIndex"]
                print(f"On append entry response, matchIndex is {self.N.matchIndex}")
                print(f"nextIndex is {self.N.nextIndex}")
                self.N.nextIndex[follower_id] = self.N.matchIndex[follower_id] + 1

                print("#####################################")
                print("try tp update leader commit index and apply entry")
                print("############### ######################")

            else:
                self.N.nextIndex[follower_id] -= 1
                self.N.sender.spawn_peer2peer(self.request.APPEND_ENTRIES(follower_id, is_heartbeat=False),
                                              request["sender_port"])

            self.N.leader_attempt_update_commit_index()
            self.N.attempt_apply_entry()

    def process_request_vote(self, request):
        # self.all_server_rules(request)


        logCheck, termCheck, votedForCheck = False, True, False

        # checking to see if candidate's log is more up to date than ours
        # if candidate's term is greater than ours, pass check. If terms same but their index greater than ours, pass check
        if (request['lastLogTerm'] > self.N.last_log_term()) or (request['lastLogTerm'] == self.N.last_log_term() and
                                                                 request['lastLogIndex'] >= self.N.last_log_index()):
            logCheck = True

        # if candidate's term is less than our term, fail term check
        if request['term'] < self.N.getCurrentTerm():
            termCheck = False

        print(f"Request term is {request['term']}")
        print(f"Current term is {self.N.getCurrentTerm()}")

        if self.N.votedFor is None or self.N.votedFor == request['candidateId']:
            votedForCheck = True

        print(f"logcheck: {logCheck}, termCheck: {termCheck}, votedForCheck: {votedForCheck}")
        if logCheck and termCheck and votedForCheck:
            self.N.currentTerm = request['term']
            self.N.votedFor = request['candidateId']
            self.N.switchToFollower()
            self.N.sender.spawn_peer2peer(self.reply.VOTE_GRANTED_REPLY(), request['sender_port'])
        else:
            self.N.sender.spawn_peer2peer(self.reply.VOTE_WITHHELD_REPLY(), request['sender_port'])

    def process_request_vote_response(self, request):
        self.N.num_votes += request['voteGranted']
        self.check_election_winner()

    def check_election_winner(self):
        if self.N.num_votes > self.N.MAJORITY and self.N.isRole(CANDIDATE):
            self.N.switchToLeader()

    def all_server_rules(self, message):
        """ rules applied on all servers """
        # if RPC term greater than currentTerm, set currentTerm = RPC term and convert to follower

        if message['term'] > self.N.getCurrentTerm():
            self.N.currentTerm = message['term']
            if not self.N.isRole(FOLLOWER):
                self.N.switchToFollower()
