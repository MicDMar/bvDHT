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

REPO_PATH = DEFAULT_REPO_PATH
lock = threading.Lock
peers = None

def connect(peer_addr):
    """
    Attempt to join the network through this peer
    """
    # Add peer to our finger table
    peers.add_address(peer_addr)
    
    # Find the hash right before ours
    closest_known = peers.get(peers.prev_hash())
    peer_addr = owns(peers.prev_hash(), closest_known)

    # Now that we have the predecessor
    conn.sendall("CON".encode())
    sendAddress(conn, peer_addr)
    if recvBool(conn):
        # Proceed with connection
        successor0 = Peer(*recvAddress(conn))
        successor1 = Peer(*recvAddress(conn))
        peers.set_successors(successor0, successor1)
        
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
    if response is Result.T:
        return True
    else:
        return False
    
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
    # Identify who to send to
    owner = owns(key)
    conn = open_connection(owner.address) 
    
    # TODO
    conn.sendall("INS".encode()) 
    result = recvStatus(conn)

    if result is Result.T:
        # We're clear to send the data
        sendVal(conn, value)
        if recvBool(conn) is False:
            # We're not good, probably won't happen
            pass
    elif result is Result.N:
        # We need to find the actual owner, it changed 
        # TODO: Make sure this is okay
        insert(key, value)

    

def owns(key, peer=None):
    """
    Using peer as a starting point, find the owner
    """
    if peer is None:
        return owns(key, peers.get(key).address) 
        
    # TODO: Perhaps change this to be non-recursive
    # Check if peer owns the hash
    conn = open_connection(peer.address)
    conn.sendall("OWN".encode())    
    sendKey(conn, key)

    # Check if the closest reported is the same as the peer we're querying
    closest = get_addr_str(recvAddress(conn))
    if closest is peer.address:
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
        owns(key, conn)

    #At this point ither the item was removed successfully or it wasn't
    if response is Result.T:
        return True
    else:
        return False

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

def peer_insert(conn, key):
    # Determine if we own the key
    if peers.get(key) == peers.our_address:
        # We own the key
        sendStatus(conn, Result.T)

        # Receive and store the data
        data = recvVal(conn)
        insert_val(key, data)

        # Confirm we received the data
        sendStatus(conn, Result.T)
    else:
        # We do not own the key
        sendStatus(conn, Result.N)

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

def peer_info(conn):
    """
    Send diagnostic information to a peer
    """
    res = "{}\n{}".format(peers, local_info())
    sendVal(conn, res.encode()) 

def peer_connect(conn):
    # First, check if we own the keyspace required  
    peer = Peer(*recvAddress(conn))
    if peers.get(peer.hash) == peers.our_hash:
        # They do belong to our keyspace
        sendStatus(conn, Result.T)

        # Alert client of their successors
        succ1, succ2 = peers.get_successors()
        sendAddress(succ1.address) 
        sendAddress(succ2.address) 
        
        # Update our successor list
        peers.set_successors(peer, succ1)

        # Determine what items need to be sent over to client
        def check_transfer_ownership(key):
            """
            Given a key within our keyspace, check if it is eligible to transfer to peer
            Note: key MUST be within our keyspace. This will be broken otherwise
            """
            # TODO: Verify interval for key
            if key <= peer.hash:
                return True
            if key < peers.our_hash:
                return True
            return False
        keys = [k for k in keys_local() if check_transfer_ownership(int(k))] 

        sendInt(conn, len(keys))
        for key in keys:
            sendVal(get_val(key))

        # Receive confirmation that items were accepted
        sendBool(conn, True)

        # TODO: Give up control of the keys (delete)
        for key in keys:
            remove_val(key)
    else:
        sendStatus(conn, Result.N)

def peer_disconnect(conn):
    pass

def pulse_response():
    response = recvStatus(conn);

def insert_val(key, val):
    """
    Add the value to our local storage
    """
    with open(repo_path(key), "wb") as f:
        f.write(val)


def get_val(key):
    """
    Get the value corresponding to key from local storage
    """
    if exists_local(key):
        with open(repo_path(key)) as f:
            return f.read()
    else:
        return None

def remove_val(key):
    """
    Remove the value corresponding to key from local storage
    """
    os.remove(repo_path(key))


def exists_local(key):
    """
    Check if the item exists locally
    (If we don't own the key space then it does not)
    """
    # TODO: Check if this is our key
    return os.path.isfile(repo_path(key))

def repo_path(key=None):
    """
    Get the path to the file corresponding to the key
    """
    if key:
        return os.path.join(REPO_PATH, key)
    else:
        return REPO_PATH

def keys_local():
    """
    Get a list of keys we are storing locally
    """
    return os.listdir(repo_path())

def local_info():
    """
    Build a string with information on the values stored here
    """
    def val_size(key):
        """
        Retrieve information relating to size of val
        """
        return os.path.getsize(repo_path(key))
        
    s = ""
    for key in keys_local(): 
        s += "{}: {} bytes,\n".format(key, val_size(key))
    return """Stored data: [
        {}
    ]""".format(s)
    

def debug():
    peer = Peer("10.92.16.58", 3000)
    print(peers.get(70))
    peers.add(peer)
    print(peers.get(math.inf))
    pass

def handle_connection(conn_info):
    conn, clientAddr = conn_info
    print("Received connection from {}".format(clientAddr))
    
    request = recvAll(conn, 3).decode()
    print("{} from {}".format(request, clientAddr))
    if request == "GET":
        peer_get(conn, recvKey(conn))
    elif request == "EXI":
        peer_exists(conn, recvKey(conn))
    elif request == "OWN":
        peer_owns(conn, recvKey(conn))
    elif request == "INS":
        peer_insert(conn, recvKey(conn))
    elif request == "REM":
        peer_remove(conn, recvKey(conn))
    elif request == "CON":
        peer_connect(conn)
    elif request == "DIS":
        peer_disconnect(conn)
    elif request == "PUL":
        sendBool(conn, True)
    elif request == "INF":
        peer_info(conn)
    else:
        return
    
    print("Closing connection to {}".format(clientAddr))
    conn.close()

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
        threading.Thread(target=handle_connection, args=(listener.accept(),), daemon=True).start()

