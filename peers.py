from hash_functions import get_hash, hash_size

from client import pulse_response
from logging import debug
import multiprocessing
from net_functions import *
from threading import Thread
import random
from hash_functions import hash_size
from time import sleep
import copy

MAX_TABLE_SIZE = 5
TABLE_OFFSET_PERCENT = 0.2

def handle_table(table):
    sleep(5)
    # Configure logging
    logging.basicConfig(format='%(levelname)s %(asctime)s({}) %(funcName)s: %(message)s [%(thread)s] %(lineno)d'.format(table.us.address), \
            level=logging.DEBUG)
    
    #Have this function always running to maintian a healty finger table.
    while True:
        debug("Starting pulse process")
        #Check to see if each peer in the table is alive or not.
        for peer in copy.deepcopy(table.table):
            address = peer.address
            try:
                conn = open_connection(peer.address)
                if table.pulse(address) == False:
                    table.remove(address)
            except ConnectionRefusedError:
                debug("Peer in exception: {}".format(peer))
                table.remove(address)
            
        #Find a random offset and add the peer to our finger table if they aren't already in there
        diff = table.size-len(table.table)
        if diff > 0:
            for i in range(0, diff):
                rand = random.randint(0, hash_size())
                table_offset = hash_size()*TABLE_OFFSET_PERCENT
                
                bottom = table.us.hash-table_offset
                top = table.us.hash+table_offset
                
                while bottom < rand < top:
                    rand = random.randint(0, hash_size())
               
                peer_to_add = table.owns(rand)

                #This check makes sure peers aren't added twice in our finger table.
                if table.contains(peer_to_add):
                    continue
                else:
                    table.add_address(peer_to_add)

        sleep(60)

class Peer:
    def __init__(self, address, port):
        self.address = "{}:{}".format(address, port)
        self.hash = get_hash(self.address)

    def __repr__(self):
        return "<Peer: {} : {}>".format(self.address, self.hash)
    
class FingerTable:
    def __init__(self, our_address, port, size=MAX_TABLE_SIZE):
        self.us = Peer(our_address, port)
        self.size = size
        self.table = [self.us]
        Thread(target=handle_table, args=(self,)).start()
        self.successors = []

    def get(self, hsh):
        options = [x for x in self.table if x.hash <= hsh]
        if len(options) == 0:
            if len(self.table) == 0:
                return self.us
            return max(self.table, key=lambda p: p.hash)
        else:
            # Find the 'closest' owner of this hash
            return max(options, key=lambda p: p.hash)
    
    def contains(self, addr):
        return len([x for x in self.table if x.address == addr]) >= 1

        

    def add(self, peer):
        # FIXME: Enforce size limit, remove items if needed
        if self.contains(peer.address) == False:
            self.table.append(peer)

    def add_address(self, peer_addr):
        self.add(Peer(*get_addr_tuple(peer_addr)))

    def set_successors(self, peer1, peer2):
        self.successors = [ peer1, peer2 ]
        self.add(peer1)
        self.add(peer2)

    def get_successors(self):
        return self.successors

    def get_predecessor(self):
        return self.owns(self.prev_hash())

    def remove(self, peer_addr):
        self.table = [x for x in self.table if x.address != peer_addr]

    def our_hash(self):
        return self.us.hash

    """
    Get the hash directly before ours
    (The last hash belonging to somebody else before us)
    """
    def prev_hash(self):
        return (self.our_hash() - 1) % hash_size()

    def empty(self):
        return len(self.table) is 0

    def __repr__(self):
        s = ""
        for peer in self.table:
            s += "  {},\n".format(peer)

        successors = "Successors: \n    {}\n    {}".format(*self.successors) \
                if self.successors else "Successors: None"
        return "<FingerTable({}): [\n{}] {}>" \
                .format(self.us.address, s[:], successors)

    def owns(self, key, peer=None):
        """
        Using peer as a starting point, find the owner
        """
        # If the key is ours, return us
        if self.get(key).hash == self.us.hash:
            return self.us.address
        
        if peer == None:
            logging.debug("Attempting to find owner of {}".format(key))
            return self.owns(key, self.get(key).address) 
        
        debug("Checking owner of {} through {}".format(key, peer))
            
        # TODO: Perhaps change this to be non-recursive
        # Check if peer self.owns the hash
        conn = open_connection(peer)
        conn.sendall("OWN".encode())    
        sendKey(conn, key)

        # Check if the closest reported is the same as the peer we're querying
        closest = get_addr_str(recvAddress(conn))
        debug("{} reported closest peer as {}".format(peer, closest))
        if closest == peer:
            logging.debug("{} is the owner of {}".format(peer, key))
            # This is the owner of the file
            return peer
        else:
            # We need to go deeper
            return self.owns(key, closest)

    def pulse(self, peer_addr):
        logging.debug("Attempting to pulse {}".format(peer_addr))
        if peer_addr == self.us.address:
            return True
        
        conn = open_connection(peer_addr)
        #Client sends protocol message for PULSE request.
        conn.sendall("PUL".encode())

        logging.debug("Spawning process for pulse")
        #Start up another process that waits 5 seconds for a response.
        p = multiprocessing.Process(target=pulse_response, args=(conn,))
        p.start()

        p.join(5)
        
        #If the peer hasn't sent us a pulse response within the allotted time
        #kill the function; peer is dead.
        if p.is_alive():
            logging.debug("Pulse response never returned. Killing function call.")
            p.terminate()
            p.join()
            return False
        
        return True
        
