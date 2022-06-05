class ReplyFn:
    """
    Handles all replying logic for any incoming request
    """

    # self.reply = Reply(self.N)

    def process_append_entries(self, log, commitIndex, currentTerm, request):
        self.currentTerm = currentTerm
        self.log = log
        self.commitIndex = commitIndex
        """ Election Logic"""

        # self.all_server_rules(request)
        # self.N.electionTimeout.restart()
        # self.N.votedFor = None

        #  1. Reply false if term < currentTerm
        if request['term'] < self.currentTerm:
            # self.reply.APPEND_ENTRIES_FAILURE()
            print("Failure reply1")
            return

        # 2. Reply false if log doesn’t contain an entry at prevLogIndex
        # whose term matches prevLogTerm


        if len(self.log) == 0 and request['prevLogIndex'] == 0: #if no entries and prevLogIndex is 0 then continue
            pass
        elif len(self.log) < request['prevLogIndex']: #if no entry at prevLogIndex then fail
            print("Failure reply2")
            return
        else: #if the entry at prevLogIndex's term doesn't match up then fail
            arr_prevLogIndex = request['prevLogIndex'] - 1
            if self.log[arr_prevLogIndex]["term"] != request["prevLogTerm"]:
                print("Failure reply3")
                return


        # 3. If an existing entry conflicts with a new one (same index
        # but different terms), delete the existing entry and all that
        # follow it

        shouldBreak = False
        existing_conflict_idx = len(self.log)
        for existing_idx, existing_entry in enumerate(self.log):
            for new_idx, new_entry in enumerate(request["entries"]):
                if existing_entry["index"] == new_entry["index"] and existing_entry["term"] != new_entry["term"]:
                    existing_conflict_idx = existing_idx
                    shouldBreak = True
                    break
            if shouldBreak:
                break

        print(existing_conflict_idx)
        self.log = self.log[:existing_conflict_idx]  # delete existing

        if len(self.log) == 0:
            print(request["entries"])
            self.log = self.log + request["entries"]
        else:
            new_entries_start = 0
            for new_idx, new_entry in enumerate(request["entries"]):
                inLog = False
                for existing_idx, existing_entry in enumerate(self.log):
                    inLog = inLog or new_entry["index"] == existing_entry["index"]
                if not inLog:
                    new_entries_start = new_idx
                    break

            self.log = self.log + request["entries"][new_entries_start:]  # append new

        # If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        if request["leaderCommit"] > self.commitIndex:
            self.commitIndex = min(request["leaderCommit"], self.log[-1]["index"])

        # match index is highest log entry replicated on this server
        matchIndex = self.log[-1]["index"]

        print(f"success reply with match index {matchIndex}")
        print(self.log)
        return self.log


def check_equal(follower, leader):
    result = True
    for e1, e2 in zip(leader, follower):
        result = result and e1["index"] == e2["index"] and e1["term"] == e2["term"]
    return result


"""
TEST CASES ###########################################################################################################
"""


def entry(index, term):
    return {"index": index, "term": term}


# LEADER
leader_indices = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
leader_terms = [1, 1, 1, 4, 4, 5, 5, 6, 6, 6]

# uncomment for leader log with only one thing
# leader_indices = [1]
# leader_terms = [1]

leader_log = []
for index, term in zip(leader_indices, leader_terms):
    leader_log.append(entry(index, term))

print(leader_log)
print()
print()


# ######### FOLLOWERS ######################################################################################
def test(indices, terms):
    leader_entry = -1
    while True:
        request = {}
        request["entries"] = leader_log[leader_entry:]

        if leader_entry == -1 * len(leader_log):
            request["prevLogIndex"] = 0
            request["prevLogTerm"] = 0
        else:
            request["prevLogIndex"] = leader_indices[len(leader_indices) + leader_entry]
            request["prevLogTerm"] = leader_terms[len(leader_terms) + leader_entry]

        request["term"] = leader_log[-1]["term"]
        request["leaderCommit"] = 6

        follower_log = []
        for index, term in zip(indices, terms):
            follower_log.append(entry(index, term))

        updatedLog = ReplyFn().process_append_entries(follower_log, 4, 0, request)
        if updatedLog is None:
            leader_entry -= 1
        else:
            print(f"Leader log and follower log are {check_equal(updatedLog, leader_log)}")
            break
    return check_equal(updatedLog, leader_log)


test([1, 2, 3, 4, 5, 6, 7, 8, 9], [1, 1, 1, 4, 4, 5, 5, 6, 6])
print()
test([1, 2, 3, 4], [1, 1, 1, 4])
print()
test([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], [1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6])
print()
test([], [])
print()
test([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,12], [1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7,7])
print()
test([1, 2, 3, 4, 5, 6, 7], [1, 1, 1, 4, 4, 4, 4])
print()
test([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], [1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3])

#all tests pass


