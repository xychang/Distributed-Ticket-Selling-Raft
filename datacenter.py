# this script handles the logic of datacenter

import json
import bisect
from threading import Lock
from threading import Timer
import logging
import random

CONFIG = json.load(open('config.json'))


class LogEntry(object):
    """
    The log entry
    """
    def __init__(self, term, index=None, command=None):
        if index is None:
            self.term, self.index, self.command = term
        else:
            self.term = term
            self.index = index
            self.command = command

    def getVals(self):
        return self.term, self.index, self.command


class datacenter(object):
    """
    This class handles the protocol logic of datacenters
    """
    def __init__(self, datacenter_id, server):
        """
        we use datacenter_id instead of pid because it is potentially
        deployed on multiple servers, and many have the same pid
        """
        logging.debug('DC-{} initialized'.format(datacenter_id))
        self.datacenter_id = datacenter_id
        self.datacenters = CONFIG['datacenters']
        # update the datacenters, so that the id and port are all int
        # self.datacenters = dict([(x, y) for x, y in self.datacenters.items()])
        self.total_ticket = CONFIG['total_ticket']

        self.current_term = 0
        self.voted_for = None

        self.role = 'follower'

        # keep a list of log entries
        # put a dummy entry in front
        self.log = [LogEntry(0, 0)]

        # record the index of the latest comitted entry
        # 0 means the dummy entry is already comitted
        self.commit_idx = 0

        # store the server object to be used for making requests
        self.server = server
        self.leader_id = None

        self.election_timeout = random.uniform(CONFIG['T'], 2*CONFIG['T'])

        # become candidate after timeout
        self.election_timer = Timer(self.election_timeout, self.startElection)
        self.election_timer.start()
        logging.debug('DC-{} started election countdown'.format(datacenter_id))

        # used by leader only
        self.heartbeat_timeout = CONFIG['heartbeat_timeout']
        self.heartbeat_timer = None

    def resetHeartbeatTimeout(self):
        """
        reset heartbeat timeout
        """
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()
        self.heartbeat_timer = Timer(self.heartbeat_timeout, self.sendHeartbeat)
        self.heartbeat_timer.start()

    def resetElectionTimeout(self):
        """
        reset election timeout
        """
        self.election_timer.cancel()
        # need to restart election if the election failed
        self.election_timer = Timer(self.election_timeout, self.startElection)
        self.election_timer.start()
        logging.debug('DC-{} reset election countdown'
                      .format(self.datacenter_id))

    def isLeader(self):
        """
        determine if the current datacenter is the leader
        """
        return self.datacenter_id == self.leader_id

    def getLatest(self):
        """
        get term and index of latest entry in log
        """
        return (self.log[-1].term, self.log[-1].index)

    def startElection(self):
        """
        start the election process
        """
        self.role = 'candidate'
        self.leader_id = None
        self.resetElectionTimeout()
        self.current_term += 1
        self.votes = [self.datacenter_id]
        self.voted_for = self.datacenter_id

        #logging.debug('DC-{} become candidate for term {}'.format(self.current_term))

        # send RequestVote to all other servers
        # (index & term of last log entry)
        self.server.requestVote(self.current_term, self.getLatest()[0], \
                                self.getLatest()[1])

    def handleRequestVote(self, candidate_id, candidate_term,
                          candidate_log_term, candidate_log_index):
        """
        Handle incoming requestVote message
        :type candidate_id: str
        :type candidate_term: int
        :type candidate_log_term: int
        :type candidate_log_index: int
        """
        if candidate_term < self.current_term:
            self.server.requestVoteReply(
                candidate_id, self.current_term, False)
            return
        if candidate_term > self.current_term:
            self.current_term = candidate_term
            self.stepDown()
        self.current_term = max(candidate_term, self.current_term)
        grant_vote = (not self.voted_for or self.voted_for == candidate_id) and\
            candidate_log_index >= self.getLatest()[1]
        if grant_vote:
            self.voted_for = candidate_id
            self.resetElectionTimeout()
            logging.debug('DC-{} voted for DC-{} in term {}'
                          .format(self.datacenter_id,
                                  candidate_id, self.current_term))
        self.server.requestVoteReply(
            candidate_id, self.current_term, grant_vote)

    def becomeLeader(self):
        """
        do things to be done as a leader
        """
        logging.debug('DC-{} become leader for term {}'
                      .format(self.datacenter_id, self.current_term))

        # no need to wait for heartbeat anymore
        self.election_timer.cancel()

        self.role = 'leader'
        self.leader_id = self.datacenter_id
        # keep track of the entries known to be logged in each data center
        self.loggedIndices = dict([(center_id, 0)
                                   for center_id in self.datacenters])
        # initialize a record of nextIdx
        self.nextIndices = dict([(center_id, self.getLatest()[1]+1)
                                 for center_id in self.datacenters])

        self.sendHeartbeat()
        self.heartbeat_timer = Timer(self.heartbeat_timeout, self.sendHeartbeat)
        self.heartbeat_timer.start()

    def stepDown(self, new_leader=None):
        logging.debug('DC-{} update itself to term {}'
                      .format(self.datacenter_id, self.current_term))
        # if candidate or leader, step down and acknowledge the new leader
        if self.isLeader():
            # if the datacenter was leader
            self.heartbeat_timer.cancel()
        self.leader_id = new_leader
        # need to restart election if the election failed
        self.resetElectionTimeout()
        # convert to follower, not sure what's needed yet
        self.role = 'follower'
        self.voted_for = None

    def handleRequestVoteReply(self, follower_id, follower_term, vote_granted):
        """
        handle the reply from requestVote RPC
        :type follower_id: str
        :type follower_term: int
        :type vote_granted: bool
        """
        if vote_granted:
            self.votes.append(follower_id)
            logging.debug('DC-{} get another vote in term {}, votes got: {}'
                          .format(self.datacenter_id,
                                  self.current_term, self.votes))

            if not self.isLeader() and len(self.votes) > len(self.datacenters)/2:
                self.becomeLeader()
        else:
            if follower_term > self.current_term:
                self.current_term = follower_term
                self.stepDown()

    def sendAppendEntry(self, center_id):
        """
        send an append entry message to the specified datacenter
        :type center_id: str
        """
        prevEntry = self.log[self.nextIndices[center_id]-1]
        self.server.appendEntry(center_id, self.current_term,
                                prevEntry.index, prevEntry.term,
                                self.log[self.nextIndices[center_id]:],
                                self.commit_idx)

    def sendHeartbeat(self):
        last_log_term, last_log_idx = self.getLatest()

        for center_id in self.datacenters:
            if center_id != self.datacenter_id:
                # send a heartbeat message to datacenter (center_id)
                self.sendAppendEntry(center_id)
        self.resetHeartbeatTimeout()

    def handleAppendEntryReply(self, follower_id, follower_term, success,
                               follower_last_index):
        """
        handle replies to appendEntry message
        decide if an entry can be committed
        :type follower_id: str
        :type follower_term: int
        :type success: bool
        :type follower_last_index: int
        """
        if follower_term > self.current_term:
            self.current_term = follower_term
            self.stepDown()
            return
        # if I am no longer the leader, ignore the message
        if not self.isLeader(): return
        # if the leader is still in it's term
        # adjust nextIndices for follower
        self.nextIndices[follower_id] = follower_last_index + 1
        logging.debug('update nextIndex of {} to {}'
                      .format(follower_id, follower_last_index + 1))
        if not success:
            self.sendAppendEntry(follower_id)
            return
        # check if there is any log entry committed
        # to do that, we need to keep tabs on the successfully
        # committed entries
        self.loggedIndices[follower_id] = follower_last_index
        # find out the index most followers have reached
        majority_idx = sorted(self.loggedIndices.values())[len(self.datacenters)/2-1]
        # commit entries only when at least one entry in current term
        # has reached majority
        if self.log[majority_idx].term != self.current_term:
            return
        # if we have something to commit
        # if majority_idx < self.commit_idx, do nothing
        map(self.commitEntry, self.log[self.commit_idx+1:majority_idx+1])
        if majority_idx != self.commit_idx:
            logging.debug('log committed upto {}'.format(majority_idx))
        self.commit_idx = max(self.commit_idx, majority_idx)

    def handleAppendEntry(self, leader_id, leader_term, leader_prev_log_idx,
                          leader_prev_log_term, entries, leader_commit_idx):
        """
        handle appendEntry RPC
        accept it if the leader's term is up-to-date
        otherwise reject it
        :type leader_id: str
        :type leader_term: int
        :type leader_prev_log_idx: int
        :type leader_prev_log_term: int
        :type entries: List[LogEntry]
               A heartbest is a meesage with [] is entries
        :type leader_commit_idx: int
        """
        _, my_prev_log_idx = self.getLatest()
        if self.current_term > leader_term:
            success = False
        else:
            self.current_term = max(self.current_term, leader_term)
            self.stepDown(leader_id)
            if my_prev_log_idx < leader_prev_log_idx \
               or self.log[leader_prev_log_idx].term != leader_prev_log_term:
                logging.debug('DC-{} log inconsistent with leader at {}'
                              .format(self.datacenter_id, leader_prev_log_idx))
                success = False
            else:
                # remove all entries going after leader's last entry
                if my_prev_log_idx > leader_prev_log_idx:
                    self.log = self.log[:leader_prev_log_idx+1]
                    logging.debug('DC-{} remove redundent logs after {}'
                                  .format(self.datacenter_id,
                                          leader_prev_log_idx))
                # Append any new entries not already in the log
                for entry in entries:
                    logging.debug('DC-{} adding {} to log'
                                  .format(self.datacenter_id, entry))
                    self.log.append(LogEntry(entry.term, entry.index))
                # check the leader's committed idx
                if leader_commit_idx > self.commit_idx:
                    map(self.commitEntry,
                        self.log[self.commit_idx+1:leader_commit_idx+1])
                    logging.debug('DC-{} comitting upto {}'
                                  .format(self.datacenter_id,
                                          leader_commit_idx))
                    self.commit_idx = leader_commit_idx
                success = True
        # reply along with the lastest log entry
        # so that the leader will know how much to update the
        # nextIndices record
        # if failed, reply index of highest possible match:
        #  leader_prev_log_idx-1
        self.server.appendEntryReply(leader_id, self.current_term, success,
                                     self.getLatest()[1] if success
                                     else leader_prev_log_idx-1)
