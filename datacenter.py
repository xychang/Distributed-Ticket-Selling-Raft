# this script handles the logic of datacenter
import json
import bisect
from threading import Lock
from threading import Timer

CONFIG = json.load(open('config.json'))


class logEntry(object):
    """
    The log entry
    """
    def __init__(self, term, index, command=None):
        self.term = term
        self.index = index
        self.command = command


class datacenter(object):
    """
    This class handles the protocol logic of datacenters
    """
    def __init__(self, datacenter_id, server):
        """
        we use datacenter_id instead of pid because it is potentially
        deployed on multiple servers, and many have the same pid
        """
        self.datacenter_id = datacenter_id
        self.datacenters = CONFIG['datacenters']
        # update the datacenters, so that the id and port are all int
        self.datacenters = dict([(int(x), y) for x, y in self.datacenters.items()])
        self.total_ticket = CONFIG['total_ticket']

        self.current_term = 0
        self.voted_for = None

        # keep a list of log entries
        self.log = []

        # record the index of the latest comitted entry
        self.commit_idx = 0

        # store the server object to be used for making requests
        self.server = server
        self.leader_id = "none"

        self.election_timeout = random.uniform(CONFIG["T"], 2*CONFIG["T"])

        # become candidate after timeout
        self.timeout_timer = Timer(self.election_timeout, self.startElection)

    def isLeader(self):
        """
        determine if the current datacenter is the leader
        """
        return self.datacenter_id == self.leader_id

    def getLatest(self):
        if len(self.log) == 0:
            return 0, 0
        return self.log[-1].term, self.log[-1].index

    def startElection(self):
        """
        start the election process
        """
        self.timeout_timer.cancel()
        # need to restart election if the election failed
        self.timeout_timer = Timer(self.election_timeout, self.startElection)
        self.current_term += 1
        self.votes = [self.datacenter_id]

        # send RequestVote to all other servers
        # (index & term of last log entry)
        self.server.requestVote(self.datacenter_id,
                                self.current_term, *self.getLatest())

    def handleRequestVote(self, candidate_id, candidate_term,
                          candidate_log_term, candidate_log_index):
        if candidate_term < self.current_term:
            self.server.requestVoteReply(self.datacenter_id,
                                         self.current_term, False)
        self.current_term = max(candidate_term, self.current_term)


    def handleRequestVoteReply(self, follower_id, follower_term, vote_granted):
        pass

