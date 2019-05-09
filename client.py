from socket import *
from net_functions import *
import hashlib
import math
import multiprocessing
import os
import sys
import threading
import time
import logging

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
    logging.debug("Attempting to join DHT through {}".format(peer_addr))
    # Add peer to our finger table
    peers.add_address(peer_addr)
    
    # Find the hash right before ours
    closest_known = peers.get(peers.prev_hash())
    peer_addr = owns(peers.prev_hash(), closest_known.address)
    peers.set_predecessor(Peer(*get_addr_tuple(peer_addr)))
    logging.debug("Predecessor found: {}".format(peer_addr))

    conn = open_connection(peer_addr)

    # Now that we have the predecessor
    conn.sendall("CON".encode())
    sendAddress(conn, peer_addr)
    if recvBool(conn):
        logging.debug("Peer will accept connection")
        # Proceed with connection
        successor0 = Peer(*recvAddress(conn))
        successor1 = Peer(*recvAddress(conn))
        peers.set_successors(successor0, successor1)
        logging.debug("Successor: {}".format(successor0.address))
        logging.debug("Successor's successor: {}".format(successor1.address))
        
        # Receive the items that now belong to us
        num_items = recvInt(conn)
        logging.debug("About to receive {} items".format(num_items))
        for i in range(num_items):
            key = recvKey(conn)
            val = recvVal(conn)
            logging.debug("Received {}: len({})".format(key, len(val)))
            insert_val(key, val)

        logging.debug("Successfully joined DHT")
        sendBool(conn, True)
    else:
        logging.debug("Peer refused connection to DHT")
        return False

def disconnect():
    if peers.empty():
        # We're the only one on the network, just leave
        return
    logging.debug("Attemtping to acquire lock for disconnect")
    lock.acquire()
    # TODO: Wrap this in a loop up until we get to leave
    pred = peers.get_predecessor()
    logging.debug("Disconnecting through {}".format(pred.address))
    conn = open_connection(pred)
    conn.sendall("DIS".encode()) 
    sendAddress(conn, peers.our_address)

    response = recvStatus(conn)
    if response == Result.T:
        logging.debug("Predecessor will accept disconnect")
        succ1, succ2 = peers.get_successors()

        sendAddress(conn, succ1.address)
        sendAddress(conn, succ2.address)
        logging.debug("Sent successors: \n{}\n{}".format(succ1, succ2))

        keys = keys_local()
        sendInt(conn, len(keys))
        logging.debug("Sending {} keys".format(len(keys)))

        for key in keys:
            sendKey(key)
            sendVal(get_val(key))

        # TODO: Timeout for if they aren't listening
        if recvStatus(conn) == Result.T:
            logging.debug("Successfully disconnected")
            # Success!
            return
    elif response == Result.N:
        logging.debug("{} will not accept disconnect".format(pred.address))
    
    lock.release()

#Start of "public" functions.

def exists(key):
    #Client sends protocol message for EXISTS request.
    logging.debug("Attempting to see if {} exists".format(key))
    peer_addr = owns(key)
    conn = open_connection(peer_addr)
    
    conn.sendall("EXI".encode())
    sendKey(key)
    response = recvStatus(conn)

    #Peer does not own this keyspace. Find out who does.
    if response == Result.N:
        logging.debug("{} does not own the key {}".format(peer_addr, key))
        return exists(key)

    #At this point either the item exists or it doesn't.
    if response == Result.T:
        logging.debug("{} exists on peer {}".format(key, peer_addr))
        return True
    else:
        return False
    
def get(key):
    #Client sends protocol message for GET request.
    logging.debug("Attempting to get {}".format(key))
    peer_addr = owns(key)
    conn.sendall("GET".encode())
    sendKey(conn, key)
    response = recvStatus(conn)
    
    #They responded with T so they will also send valSize and fileData.
    if response == Result.T:
        logging.debug("{} is sending {}".format(peer_addr, key))
        #Download the item. It exists and == there.
        fileData = recvVal(conn)
        insert_val(hsh, fileData)

    #They responded with F meaning the peer owns the space but the items not there.
    elif response == Result.F:
        logging.debug("{} does not exist.".format(key))
        return
    
    #Peer does not own this keyspace. Find out who does.
    else:
        owns(key)
        return

def insert(key, value):
    # Identify who to send to
    logging.debug("Attempting to add {} to DHT".format(key))
    owner = owns(key)
    logging.debug("Owner of key is {}".format(owner)) 
    conn = open_connection(owner) 
    
    # TODO
    conn.sendall("INS".encode()) 
    result = recvStatus(conn)

    if result == Result.T:
        logging.debug("{} will accept {}".format(owner, key))
        # We're clear to send the data
        sendVal(conn, value)
        if recvBool(conn) == False:
            logging.debug("{} did not store the data")
            # We're not good, probably won't happen
            pass
    elif result == Result.N:
        logging.debug("{} does not own the keyspace for {}".format(owner, key))
        # We need to find the actual owner, it changed 
        # TODO: Make sure this is okay
        insert(key, value)

    

def owns(key, peer=None):
    """
    Using peer as a starting point, find the owner
    """
    if peer == None:
        logging.debug("Attempting to find owner of {}".format(key))
        return owns(key, peers.get(key).address) 
    
    logging.debug("Checking owner of {} through {}".format(key, peer))
        
    # TODO: Perhaps change this to be non-recursive
    # Check if peer owns the hash
    conn = open_connection(peer)
    conn.sendall("OWN".encode())    
    sendKey(conn, key)

    # Check if the closest reported == the same as the peer we're querying
    closest = get_addr_str(recvAddress(conn))
    logging.debug("{} reported closest peer as {}".format(peer, closest))
    if closest == peer:
        logging.debug("{} is the owner of {}".format(peer, key))
        # This is the owner of the file
        return peer
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
        logging.debug("Peer does not own {}".format(key))
        owns(key, conn)

    #At this point either the item was removed successfully or it wasn't
    if response == Result.T:
        logging.debug("Successfully removed {}".format())
        return True
    else:
        logging.debug("Couldn't remove {}".format())
        return False

def pulse(peer_addr):
    logging.debug("Attempting to pulse {}".format(peer_addr))
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
    
def peer_exists(conn, key):
    #Check to see if we own the specified key
    logging.debug("Peer asking if {} exists".format(key))
    if owns(key, peers.get(key).address) == peers.our_address:
        logging.debug("We own {}".format(key))
        data = get_val(key)
        
        if key in keys_local():
            #Something does exist at the specified key
            logging.debug("{} is in our repo".format(key))
            sendStatus(conn, Result.T)
            return True
        else:
            #Nothing exists at the specified key
            logging.debug("{} is not in our repo".format(key))
            sendStatus(conn, Result.F)
            return False

    #We don't own the specified key
    else:
        logging.debug("We don't own {}".format(key))
        sendStatus(conn, Result.N)
        return None

def peer_get(conn, key):
    logging.debug("Attempting to get {}".format(key))

    #Check to see if we own the specified key
    if owns(key, peers.get(key).address) == peers.our_address:
        data = get_val(key)

        #Nothing exists at the specified key
        if data == None:
            logging.debug("Nothing to get at {}".format(key))
            sendStatus(conn, Result.F)
            return

        #Something does exist at the key so send it back
        else:
            sendStatus(conn, Result.T)
            sendVal(conn, data)
            logging.debug("Sucessfully got {}".format(key))
            return
    #We don't own the specified key
    else:
        logging.debug("Peer does not own {}".format(key))
        sendStatus(conn, Result.N)
        return

def peer_insert(conn, key):
    logging.debug("Attempting to insert {}".format(key))

    # Determine if we own the key
    if peers.get(key) == peers.us.address:
        # We own the key
        sendStatus(conn, Result.T)

        # Receive and store the data
        data = recvVal(conn)
        insert_val(key, data)

        # Confirm we received the data
        sendStatus(conn, Result.T)
        logging.debug("Successfully inserted {}".format(key))
    else:
        logging.debug("Failed to insert {}".format(key))
        # We do not own the key
        sendStatus(conn, Result.N)

def peer_owns(conn, key):
    # Find the closest peer
    logging.debug("Peer wants to know who owns {}".format(key))
    closest = peers.get(key)
        
    # Pulse peer
    logging.debug("About to pulse {} if not us".format(closest))
    if closest.address == peers.us.address or pulse(closest.address):
        logging.debug("Closest known is alive and well, or us")
        # Tell peer they're good
        sendAddress(conn, closest.address)
    else:
        logging.debug("Peer didn't respond to pulse. Removing from table")
        # Remove cloest from peer list 
        peers.remove(closest.address)
        # TODO: Verify this can't cause any problems if the new peer is dead
        closest = peers.get(key).address
        logging.debug("Next closest is {}".format(closest))
        sendAddress(conn, closest)

def peer_remove(conn, key):
    logging.debug("Attempting to remove {}".format(key))

    #Check to see if we own the specified key
    if owns(key, peers.get(key).address) == peers.our_address:
        data = get_val(key)

        #Nothing exists at the specified key so it can't be removed
        if data == None:
            logging.debug("Nothing to remove at {}".format(key))
            sendStatus(conn, Result.F)
            return

        #Something does exist at the key so remove it
        else:
            remove_val(key)
            sendStatus(conn, Result.T)
            logging.debug("Successfully removed {}".format(key))
            return
    #We don't own the specified key
    else:
        logging.debug("Peer does not own {}".format(key))
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
    logging.debug("{} is attempting to join DHT".format(peer.address))
    if peers.get(peer.hash) == peers.our_hash:
        logging.debug("We own the keyspace. Allowing peer to join")
        # They do belong to our keyspace
        sendStatus(conn, Result.T)

        # Alert client of their successors
        succ1, succ2 = peers.get_successors()
        sendAddress(succ1.address) 
        sendAddress(succ2.address) 
        
        # Update our successor list
        peers.set_successors(peer, succ1)
        logging.debug("New successors:\n{}\n{}".format(succ1, succ2))

        # Determine what items need to be sent over to client
        def check_transfer_ownership(key):
            """
            Given a key within our keyspace, check if it == eligible to transfer to peer
            Note: key MUST be within our keyspace. This will be broken otherwise
            """
            # TODO: Verify interval for key
            if key <= peer.hash or key < peers.our_hash:
                logging.debug("{} will be transfered".format(key))
                return True
            logging.debug("{} will NOT be transfered".format(key))
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
    addr = getAddress(conn)
    logging.debug("{} wants to disconnect. shameful display".format(addr))
    
    # Check if they're our successor
    succ, _ = peers.get_successors()

    if addr == succ.address:
        logging.debug("{} IS our successor. Allowing disconnect".format(addr))
        # We will take the keys
        sendStatus(conn, Result.T)
        
        succ0 = Peer(*recvAddress(conn))
        succ1 = Peer(*recvAddress(conn))
        peers.set_successors(succ0, succ1)
        logging.debug("New Successors: \n{}\n{}".format(succ0, succ1))

        num_items = recvInt(conn)
        logging.debug("Receiving {} items".format(num_items))
        for i in range(num_items):
            key = recvKey(conn)
            val = recvVal(conn)
            logging.debug("Received {}: {} bytes".format(key, len(val)))
            insert_val(key, val)

        logging.debug("Successfully received all items")
        sendStatus(conn, Result.T)
    else:
        logging.debug("{} is not our successor. They won't be leaving from us".format(addr))
        sendStatus(conn, Result.N)

def pulse_response(conn):
    response = recvStatus(conn);

def insert_val(key, val):
    """
    Add the value to our local storage
    """
    logging.debug("Adding key: {}. {} bytes".format(key, len(val)))
    with open(repo_path(key), "wb") as f:
        f.write(val)


def get_val(key):
    """
    Get the value corresponding to key from local storage
    """
    if exists_local(key):
        with open(repo_path(key), "rb") as f:
            return f.read()
    else:
        return None

def remove_val(key):
    """
    Remove the value corresponding to key from local storage
    """
    logging.debug("Removing key: {}".format(key))
    os.remove(repo_path(key))


def exists_local(key):
    """
    Check if the item exists locally
    (If we don't own the key space then it does not)
    """
    # TODO: Check if this == our key
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
    

def handle_connection(conn_info):
    conn, clientAddr = conn_info
    logging.debug("Received connection from {}".format(clientAddr))
    
    request = recvAll(conn, 3).decode()
    logging.debug("{} from {}".format(request, clientAddr))
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
    
    logging.debug("Closing connection to {}".format(clientAddr))
    conn.close()

def handle_cli():
    def print_commands():
        print("Options are: insert,remove,get,exists,disconnect,help")
        
    while True:
        print_commands()
        command = input("What would you like to do? ")
        
        if command == "insert":
            name = get_hash(input("Enter the name of the data: "))
            val = input("Enter the data: ")
            insert(name, val)
        elif command == "remove":
            name = get_hash(input("Enter the name of the data: "))
            remove(name)
        elif command == "get":
            name = get_hash(input("Enter the name of the data: "))
            get(name)
            pass
        elif command == "exists":
            name = get_hash(input("Enter the name of the data: "))
            if exists(name):
                print("{} exists!".format(name))
            else:
                print("{} does not exist.".format(name))
        elif command == "disconnect":
            disconnect()
            pass
        elif command == "help":
            print_commands()
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
    local_addr = "{}:{}".format(local_ip, port)
    peers = FingerTable("{}:{}".format(local_ip, port))
    
    # Configure logging
    logging.basicConfig(format='%(levelname)s %(asctime)s({}): %(message)s [%(thread)s]'.format(local_addr), \
            level=logging.DEBUG)

    logging.debug(peers)

    # Determine if we're the first person on the DHT
    # If so, call connect on the peer
    if address:
        # Connect to DHT
        connect(address)

    # TODO: Populate fingertable with peers from the circle 

    # TODO: Launch a thread to manage fingertable

    # Setup the TCP socket
    listener = socket(AF_INET, SOCK_STREAM)
    listener.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1) # Allow address to be reused
    listener.bind((local_ip, port))
    listener.listen()
    logging.info("Server is listening on {}:{}".format(local_ip, port))

    threading.Thread(target=handle_cli).start()

    while True:
        try:
            threading.Thread(target=handle_connection, args=(listener.accept(),), daemon=True).start()
        except KeyboardInterrupt:
            logging.info("Exit requested. Disconnecting.")
            disconnect()
            logging.info("Goodbye")
            break

