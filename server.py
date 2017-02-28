# this script handles the message passing of raft
import json
import datacenter
from threading import Timer

CONFIG = json.load(open('config.json'))


class server(object):
    """
    This class is a virtual server with it's own local storage
    A controller for crashing condition
    A controller for network condition
    And full support for message passing
    """

    def __init__(self, center_id, port):
        self.ip = gethostbyname('')
        self.port = port
        self.center_id = center_id
        logging.info('DC-{} server running at {:s}:{:4d}'
                     .format(self.id, self.ip, self.port))
        try:
            self.listener = socket(AF_INET, SOCK_STREAM)
            self.listener.bind((self.ip, self.port))
            self.listener.listen(5) # Max connections
            logging.info('DC-{} listener start successfully...'
                         .format(self.id))
        except Exception as e:
            # socket create fail
            logging.warning("Socket create fail.{0}".format(e))
        self.dc = datacenter(center_id, self)
        self.waitConnection()

    def sendMessage(self, target_meta, message):
        """
        send a message to the target server
        should be a UDP packet, without gauranteed delivery
        :type target_meta: e.g. { "port": 12348 }
        :type message: str
        """
        pass

    def requestVote(self, current_term, latest_log_term):
        # broadcast the requestVote message to all other datacenters
        def sendMsg():
            message = ('REQ_VOTE:{datacenter_id},' +
                       '{current_term},{latest_log_term}\n').format(
                datacenter_id=self.center_id,
                current_term=current_term,
                latest_log_term=latest_log_term
            )
            for center_id in self.dc.datacenters:
                self.sendMessage(self.dc.datacenters[center_id], message)
        Timer(CONFIG['messageDelay'], sendMsg).start()

    def requestVoteReply(self, target_id, current_term, grant_vote):
        # send reply to requestVote message
        def sendMsg():
            message = ('REQ_VOTE_REPLY:{datacenter_id},' +
                       '{current_term},{grant_vote}').format(
                        datacenter_id=self.center_id,
                        current_term=current_term,
                        grant_vote=grant_vote)
            self.sendMessage(self.dc.datacenters[target_id], message)
        Timer(CONFIG['messageDelay'], sendMsg).start()

    def appendEntry(self, target_id, current_term, prev_log_idx,
                    prev_log_term, entries, commit_idx):
        def sendMsg():
            message = ('APPEND:{datacenter_id},{current_term},' +
                       '{prev_log_idx},{prev_log_term},{entries},' +
                       '{commit_idx}').format(
                           datacenter_id=self.center_id,
                           current_term=current_term,
                           prev_log_idx=prev_log_idx,
                           prev_log_term=prev_log_term,
                           entries=json.dumps([x.getVals() for x in entries]),
                           commit_idx=commit_idx)
            self.sendMessage(self.dc.datacenters[target_id], message)
        Timer(CONFIG['messageDelay'], sendMsg).start()

    def appendEntryReply(self, target_id, current_term, success,
                         follower_last_index):
        def sendMsg():
            message = ('APPEND_REPLY:{datacenter_id},{current_term},' +
                       '{follower_last_index}').format(
                           datacenter_id=self.center_id,
                           current_term=current_term,
                           follower_last_index=follower_last_index)
            self.sendMessage(self.dc.datacenters[target_id], message)
        Timer(CONFIG['messageDelay'], sendMsg).start()

    def handleIncommingMessage(self, message_type, content):
        # handle incomming messages
        # Message types:
        # messages from servers
        # 1. requestVote RPC
        if message_type == 'REQ_VOTE':
            candidate_id, candidate_term, candidate_log_term,\
                candidate_log_index = content.split(',')
            self.dc.handleRequestVote(
                candidate_id, int(candidate_term),
                int(candidate_log_term), int(candidate_log_index))
        # 2. requestVoteReply RPC
        elif message_type == 'REQ_VOTE_REPLY':
            follower_id, follower_term, vote_granted \
                = content.split(',')
            self.dc.handleRequestVoteReply(
                follower_id, int(follower_term),
                vote_granted == 'True')
        # 3. appendEntry RPC
        elif message_type == 'APPEND':
            leader_id, leader_term, leader_prev_log_idx,\
                leader_prev_log_term, entries, leader_commit_idx =\
                content.split(',')
            self.dc.handleAppendEntry(
                leader_id, int(leader_term),
                int(leader_prev_log_idx),
                int(leader_prev_log_term),
                json.loads(map(datacenter.LogEntry, entries)),
                int(leader_commit_idx))
        # 4. appendEntryReply RPC
        elif message_type == 'APPEND_REPLY':
            follower_id, follower_term, success, \
                follower_last_index = content.split(',')
            self.dc.handleAppendEntryReply(
                follower_id, int(follower_term),
                success == 'True',
                int(follower_last_index))
        # messages from clients
        # 1. buy
        # 2. show
        # 3. change

    def waitConnection(self):
        '''
        This function is used to wait for incomming connections,
        either from peer datacenters or from clients
        Incomming connections from clients are stored for later response
        '''
        num = 0
        while True:
            try:
                conn, addr = self.listener.accept()
                logging.debug('Connection from {address} connected!'
                              .format(address=addr))
                msg = conn.recv(1024)
                for line in msg.split('\n'):
                    if len(line) == 0: continue
                    handleIncommingMessage(*line.split(':'))

            except Exception as e:
                logging.error('Error with incomming connection. {0}'.format(e))
