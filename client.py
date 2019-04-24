from socket import *
from net_functions import *
import hashlib
import math
import os
import threading

DEFAULT_PORT = 3000
DEFAULT_REPO_PATH = "DHT_files"

class Peer:
    def __init__(self, address, port):
        self.address = "{}:{}".format(address, port)
        self.hash = int.from_bytes(hashlib.sha1(self.address.encode()).digest(), byteorder="big")

    def __repr__(self):
        return "<Peer: {} : {}>".format(self.address, self.hash)

class FingerTable:
    def __init__(self, our_address, size=5):
        self.our_address = our_address
        self.size = size
        self.table = []

    def get(self, hsh):
        options = [x for x in self.table if x.hash < hsh]
        if len(options) == 0:
            return self.our_address
        else:
            # Find the 'closest' owner of this hash
            return max(options)

    def add(self, peer):
        self.table.append(peer)

    def set_successor(self, peer):
        self.successor = peer
        self.add(peer)

    def get_successor(self):
        return self.successor

    def set_predeccesor(self, peer):
        self.predeccesor = peer
        self.add(predeccesor)

    def remove(peer_addr):
        self.table = [x for x in self.table if x.address != peer_addr]

    def __repr__(self):
        s = ""
        for peer in self.table:
            s += str(peer)
        return "<FingerTable: [{}]>".format(s)

REPO_PATH = None
lock = threading.Lock
peers = None

"""
Attempt to join the network through this peer
"""
def connect(peer_addr):
    conn = socket(AF_INET, SOCK_STREAM)
    conn.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    conn.connect(peer_addr.split(':')) #FIXME: check if tuple is needed
    
    conn.sendall("CON".encode())
    sendAddress(conn, peer_addr)
    if recvBool(conn):
        # Proceed with connection
        successor = Peer(*recvAddress(conn))
        peers.set_successor(successor)
        
        num_items = recvInt(conn)
        for i in range(num_items):
            key = recvKey(conn)
            val = recvVal(conn)
            insert_val(key, val)
        sendBool(conn, True)
    else:
        return False

def disconnect(pred_addr):
    #TODO
    lock.acquire():

    lock.release()

#Start of "public" functions.

def exists(key):
    #TODO
    pass

def get(key):
    #TODO
    pass

def insert(key, value):
    #TODO
    pass

def owns(key):
    #TODO
    pass

def remove(key):
    #TODO
    pass

def pulse():
    # TODO
    pass


"""
Add the value to our local storage
"""
def insert_val(key, val):
    with open(repo_path(key), "w") as f:
        f.write(val)


"""
Get the value corresponding to key from local storage
"""
def get_val(key):
    if exists_local(key):
        with open(repo_path(key)) as f:
            return f.read()
    else:
        return None


"""
Check if the item exists locally
(If we don't own the key space then it does not)
"""
def exists_local(key):
    # TODO: Check if this is our key
    return os.path.isfile(repo_path(key))


"""
Get the path to the file corresponding to the key
"""
def repo_path(key):
    return os.path.join(REPO_PATH, key)

def debug():
    peer = Peer("10.92.16.58", 3000)
    print(peers.get(70))
    peers.add(peer)
    print(peers.get(math.inf))
    pass

def handle_connection(conn_info):
    pass

if __name__ == "__main__":
    # The address of the first peer to connect to
    address = None
    if len(sys.argv) == 2:
        # Get from command line arg
        address = sys.argv[1]
    else:
        address = os.environ.get('ADDRESS', None)

    port = os.environ.get('PORT', DEFAULT_PORT)
    REPO_PATH = os.environ.get('REPOSITORY', DEFAULT_REPO_PATH)


    local_ip = getLocalIPAddress()
    peers = FingerTable("{}:{}".format(local_ip, port))

    debug()
    print(peers)

    # Setup the TCP socket
    listener = socket(AF_INET, SOCK_STREAM)
    listener.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1) # Allow address to be reused
    listener.bind((address, port))
    listener.listen()

    print("Server is listening on {}:{}".format(address, port))

    # Determine if we're the first person on the DHT
    # If so, call connect on the peer
    if address:
        # Connect to DHT
        connect(address)

    while True:
        print("Awaiting connection")
        conn, clientAddr = listener.accept()
        print("Received connection: {}".format(clientAddr))

        threading.Thread(target=handle_connection, args=(listener.accept(),), daemon=True).start()

        print("Closing connection: {}".format(clientAddr))
        print("-" * 15)
        conn.close()
