from hash_functions import get_hash, hash_size

from client import owns, pulse, open_connection
from logging import debug
from net_functions import *
from threading import Thread
import random
from hash_functions import hash_size
from time import sleep

MAX_TABLE_SIZE = 5
TABLE_OFFSET_PERCENT = 0.2

def handle_table(table):
    #Have this function always running to maintian a healty finger table.
    while True:
        #Check to see if each peer in the table is alive or not.
        for peer in table:
            conn = open_connection(peer)
            if pulse(peer.address) == False:
                table.remove(peer.addresss)
        
        #Find a random offset and add the peer to our finger table if they aren't already in there
        for i in range(0, table.size-len(table.table))
            rand = random.randint(0, hash_size())
            table_offset = hash_size()*TABLE_OFFSET_PERCENT
            
            bottom = table.us.hash-table_offset
            top = table.us.hash+table_offset
            
            while bottom < rand < top:
                rand = random.randint(0, hash_size())
           
            peer_to_add = owns(rand)

            #This check makes sure peers aren't added twice in our finger table.
            if table.contains(peer_to_add):
                continue
            else:
                table.add_address(peer_to_add)

        sleep(10)

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
        Thread(target=handle_table, args=(self,))
        self.successors = []
        self.predecessor = None

    def get(self, hsh):
        options = [x for x in self.table if x.hash <= hsh]
        if len(options) == 0:
            return max(self.table, key=lambda p: p.hash)
        else:
            # Find the 'closest' owner of this hash
            return max(options, key=lambda p: p.hash)
    
    def contains(addr):
        return len([x for x in self.table if x.address == addr]) >= 1

        

    def add(self, peer):
        # FIXME: Enforce size limit, remove items if needed
        self.table.append(peer)

    def add_address(self, peer_addr):
        self.add(Peer(*get_addr_tuple(peer_addr)))

    def set_successors(self, peer1, peer2):
        self.successors = [ peer1, peer2 ]
        self.add(peer1)
        self.add(peer2)

    def get_successors(self):
        return self.successors

    def set_predecessor(self, peer):
        self.predecessor = peer
        self.add(self.predecessor)

    def get_predecessor(self):
        return self.predecessor

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
        return "<FingerTable({}): [\n{}]>".format(self.us.address, s[:])

