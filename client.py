from socket import *
from net_functions import *
import hashlib
import math
import os
import sys
import threading

from peers import *
DEFAULT_PORT = 3000
DEFAULT_REPO_PATH = "DHT_files"

REPO_PATH = None
lock = threading.Lock
peers = None

def get_addr_tuple(addr_str):
    split = addr_str.split(":")
    return (split[0], int(split[1]))

"""
Attempt to join the network through this peer
"""
def connect(peer_addr):
    conn = socket(AF_INET, SOCK_STREAM)
    conn.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    conn.connect(get_addr_tuple(peer_addr)) #FIXME: check if tuple is needed
    # TODO: Determine our predecessor through the connection
    # Find the hash right before ours

    # Now that we have the predecessor
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

def disconnect():
    #TODO
    lock.acquire()

    lock.release()

#Start of "public" functions.

def exists(key):
    #Client sends protocol message for EXISTS request
    conn.sendall("EXI".encode())
    conn.sendall(key.encode())
    response = recvStatus(conn);

    if response == response.N:
        owns(key)
        return

    print("Item does exist in keyspace.") if response == result.T else print("Item no longer exists in keyspace.")
    return
    
def get(key):
    #Client sends protocol message for GET request
    conn.sendall("GET".encode())
    conn.sendall(key.encode())
    response = recvStatus(conn);
    
    #They responded with T so they will also send valSize and fileData
    if response == result.T:
        #TODO the the item. It exists and is there
        valSize = recvInt(conn)
        fileData = recvAll(conn, valSize)
        insert_val(key, fileData)

    #They responded with F meaning the peer owns the space but the items not there.
    elif response == result.F:
        print("Item no longer exists.")
        return
    
    #Peer does not own this keyspace.
    else:
        owns(key)

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

def peer_exists(key, conn):
    pass

def peer_get(key, conn):
    pass

def peer_insert(key, value, conn):
    pass

def peer_owns(key, conn):
    pass

def peer_remove(key, conn):
    pass
    
"""
Add the value to our local storage
"""
def insert_val(key, val):
    with open(repo_path(key), "wb") as f:
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

    # Determine if we're the first person on the DHT
    # If so, call connect on the peer
    if address:
        # Connect to DHT
        connect(address)

    # Setup the TCP socket
    listener = socket(AF_INET, SOCK_STREAM)
    listener.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1) # Allow address to be reused
    listener.bind((local_ip, port))
    listener.listen()
    print("Server is listening on {}:{}".format(local_ip, port))

    while True:
        print("Awaiting connection")
        conn, clientAddr = listener.accept()
        print("Received connection: {}".format(clientAddr))

        threading.Thread(target=handle_connection, args=(listener.accept(),), daemon=True).start()

        print("Closing connection: {}".format(clientAddr))
        print("-" * 15)
        conn.close()
