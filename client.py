from socket import *
from net_functions import *
import hashlib
import math
import multiprocessing
import os
import sys
import threading
import time

from peers import *
DEFAULT_PORT = 3000
DEFAULT_REPO_PATH = "DHT_files"

REPO_PATH = None
lock = threading.Lock
peers = None

def get_addr_tuple(addr_str):
    split = addr_str.split(":")
    return (split[0], int(split[1]))

def get_addr_str(addr_tuple):
    return "{}:{}".format(*addr_tuple)

"""
Attempt to join the network through this peer
"""
def connect(peer_addr):
    conn = open_connection(peer_addr)
    # Find the hash right before ours
    closest_known = peers.get(peers.prev_hash())
    peer_addr = owns(peers.prev_hash(), closest_known)

    # Now that we have the predecessor
    conn.sendall("CON".encode())
    sendAddress(conn, peer_addr)
    if recvBool(conn):
        # Proceed with connection
        successor = Peer(*recvAddress(conn))
        peers.set_successor(successor)
        
        # Receive the items that now belong to us
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
    #Client sends protocol message for EXISTS request.
    conn.sendall("EXI".encode())
    sendKey(key)
    response = recvStatus(conn)

    #Peer does not own this keyspace. Find out who does.
    if response is Result.N:
        owns(key)
        return exists(key)

    #At this point either the item exists or it doesn't.
    return True if response is Result.T else return False
    
def get(key):
    #Client sends protocol message for GET request.
    conn.sendall("GET".encode())
    sendKey(conn, key)
    response = recvStatus(conn)
    
    #They responded with T so they will also send valSize and fileData.
    if response is Result.T:
        #Download the item. It exists and is there.
        fileData = recvVal(conn)
        insert_val(hsh, fileData)

    #They responded with F meaning the peer owns the space but the items not there.
    elif response is Result.F:
        print("Item no longer exists.")
        return
    
    #Peer does not own this keyspace. Find out who does.
    else:
        owns(key)
        return

def insert(key, value):
    #TODO
    pass

"""
Using peer as a starting point, find the owner
"""
def owns(peer, key):
    # Check if peer owns the hash
    conn = open_connection(peer.address)
    conn.sendall("OWN".encode())    
    sendKey(conn, key)

    # Check if the closest reported is the same as the peer we're querying
    closest = get_addr_str(recvAddress(conn))
    if closest is peer:
        # This is the owner of the file
        return peer.address
    else:
        # We need to go deeper
        return owns(key, closest)

def remove(conn, key):
    #client sends protocol message for REMOVE request.
    conn.sendall("REM".encode())
    sendKey(conn, key)
    response = recvStatus(conn)
    
    #Peer does not own this keyspace. Find out who does
    if response == Result.N:
        owns(conn, key)

    #At this point ither the item was removed successfully or it wasn't
    return True if response is Result.T else return False

def pulse(conn):
    #Client sends protocol message for PULSE request.
    conn.sendall("PUL".encode())

    #Start up another process that waits 5 seconds for a response.
    p = multiprocessing.Process(target=pulse_response)
    p.start()

    p.join(5)
    
    #If the peer hasn't sent us a pulse response within the allotted time
    #kill the function; peer is dead.
    if p.is_alive():
        print("Pulse response never returned. Killing function call.")
        p.terminate()
        p.join()
        return False
    
    return True
    
def peer_exists(conn, key):
    #Check to see if we own the specified key
    if owns(key, peers.get(key)) == peers.our_address:
        data = get_val(key)
        
        #Nothing exists at the specified key
        if data is None:
            sendStatus(conn, Result.F)
            return False
        
        #Something does exist at the specified key
        else:
            sendStatus(conn, Result.T)
            sendVal(conn, data)
            return True

    #We don't own the specified key
    else:
        sendStatus(conn, Result.N)
        return None

def peer_get(conn, key):
    #Check to see if we own the specified key
    if owns(key, peers.get(key)) == peers.our_address:
        data = get_val(key)

        #Nothing exists at the specified key
        if data is None:
            sendStatus(conn, Result.F)
            return

        #Something does exist at the key so send it back
        else:
            sendStatus(conn, Result.T)
            sendVal(conn, data)
            return
    #We don't own the specified key
    else:
        sendStatus(conn, Result.N)
        return

def peer_insert(conn, key, value):
    pass

def peer_owns(conn, key):
    # Find the closest peer
    closest = peers.get(key)
    # Pulse peer
    conn = open_connection(closest.address)
    if pulse(conn):
        # Tell peer they're good
        sendAddress(conn, closest.address)
    else:
       # Remove cloest from peer list 
       peers.remove(closest.address)
       # TODO: Verify this can't cause any problems if the new peer is dead
       sendAddress(conn, peers.get(key).address)
    pass

def peer_remove(conn, key):
    #Check to see if we own the specified key
    if owns(key, peers.get(key)) == peers.our_address:
        data = get_val(key)

        #Nothing exists at the specified key so it can't be removed
        if data is None:
            sendStatus(conn, Result.F)
            return

        #Something does exist at the key so remove it
        else:
            remove_val(key)
            sendStatus(conn, Result.T)
            return
    #We don't own the specified key
    else:
        sendStatus(conn, Result.N)
        return
    pass

def peer_connect(conn):
    pass

def peer_disconnect(conn):
    pass

def pulse_response():
    response = recvStatus(conn);

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
Remove the value corresponding to key from local storage
"""
def remove_val(key):
    os.remove(repo_path(key))


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

def handle_connection(conn):
    request = recvAll(conn, 3).decode()
    if request == "GET":
        peer_get(conn, recvKey(conn))
    elif request == "EXI":
        peer_exists(conn, recvKey(conn))
    elif request == "OWN":
        peer_owns(conn, recvKey(conn))
    elif request == "INS":
        peer_insert(conn, recvValrecvKey(conn), recvVal(conn))
    elif request == "REM":
        peer_remove(conn, recvKey(conn))
    elif request == "CON":
        peer_connect(conn)
    elif request == "DIS":
        peer_disconnect(conn)
    elif request == "PUL":
        sendBool(conn, True)
    else:
        return

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
