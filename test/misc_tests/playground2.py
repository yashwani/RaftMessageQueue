class ReplyFn:
    """
    Handles all replying logic for any incoming request
    """

    # self.reply = Reply(self.N)

    def process_append_entries(self, log, commitIndex, currentTerm, request):
        self.currentTerm = currentTerm
        self.log = log
        self.commitIndex = commitIndex

        success = False
        # matchIndex is the index that requester and this node match up to
        matchIndex = 0
        if self.currentTerm < request['term']:
            self.currentTerm = request['term']
            # self.switchToFollower()
            # self.votedFor = None
        if self.currentTerm == request['term']:
            # self.N.switchToFollower()
            # if we've found that we have the right index/term of our logs this is a successful append entries request
            # print(request['prevLogIndex'])
            # print("request entries we want to append:" + ' '.join(request['entries']))
            if request['prevLogIndex'] == 0 or ((request['prevLogIndex'] <= len(self.log)) and
                                                (self.log[request['prevLogIndex'] - 1]['term'] == request[
                                                    'prevLogTerm'])):
                success = True
                indx = request['prevLogIndex']
                # for all the items after leader's prevLogIndex, we need to get rid of all non matching items, then add in leader's items
                # print("requests: " + ' '.join(request['entries']))
                if len(self.log) == 0:
                    self.log += request['entries']
                    indx = request['prevLogIndex']
                else:
                    for i in range(len(request['entries'])):
                        print("i:" + str(i))
                        print(indx)
                        indx += 1
                        if self.log[int(indx - 1)]['term'] != request['entries'][int(i)]['term']:
                            while (len(self.log)) > (indx - 2):
                                self.log.pop()
                        # print("entry: ")

                        self.log.append(request['entries'][i])
        # print("my log: " + ' '.join(self.N.log))
        # matchIndex = indx
        self.commitIndex = max(self.commitIndex, request['leaderCommit'])
        # we need to let the leader know that we are now matching their index


        if success == True:
            matchIndex = indx
            print(f"success reply with match index {matchIndex}")
            return self.log
        else:
            return None


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
        request["term"] = leader_log[leader_entry]["term"]
        request["prevLogIndex"] = leader_indices[len(leader_indices) + leader_entry]
        request["prevLogTerm"] = leader_terms[len(leader_terms) + leader_entry]
        request["leaderCommit"] = 6

        follower_log = []
        for index, term in zip(indices, terms):
            follower_log.append(entry(index, term))

        updatedLog = ReplyFn().process_append_entries(follower_log, 4, 4, request)
        if updatedLog is None:
            leader_entry -= 1
        else:
            print(f"Leader log and follower log are {check_equal(updatedLog, leader_log)}")
            break
    return check_equal(updatedLog, leader_log)


test([1, 2, 3, 4, 5, 6, 7, 8, 9], [1, 1, 1, 4, 4, 5, 5, 6, 6])
print()
# test([1, 2, 3, 4], [1, 1, 1, 4])
# print()
# test([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], [1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6])
# print()
# test([], [])


"""
Change log:
indx reference before assignment --> added . Moved matchIndx inside if else

"""