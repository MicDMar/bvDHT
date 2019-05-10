from enum import Enum
from socket import *
import logging

############################
############################
##                        ##
##  Network-Related Code  ##
##                        ##
############################
############################

def get_addr_tuple(addr_str):
    split = addr_str.split(":")
    return (split[0], int(split[1]))

def get_addr_str(addr_tuple):
    return "{}:{}".format(*addr_tuple)

def open_connection(addr):
    logging.debug("Opening connection to {}".format(addr))
    conn = socket(AF_INET, SOCK_STREAM)
    conn.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    conn.connect(get_addr_tuple(addr)) 
    return conn

# Note: In all of the following functions, conn is a socket object

##############################
# recvAll equivalent to sendall

def recvAll(conn, msgLength):
    msg = b''
    while len(msg) < msgLength:
        retVal = conn.recv(msgLength - len(msg))
        msg += retVal
        if len(retVal) == 0:
            break    
    return msg


######################
# Send/Recv 8-byte int

def sendInt(conn, i):
    conn.sendall(i.to_bytes(8, byteorder="big"))

def recvInt(conn):
    return int.from_bytes(recvAll(conn, 8), byteorder="big")

####################
# Send/Recv DHT Keys

# Note: key is an integer representation of the key. That is, it is derived
#       from hashlib.sha1(bStr).digest()

def sendKey(conn, key):
    conn.sendall(key.to_bytes(20, byteorder="big"))

def recvKey(conn):
    return int.from_bytes(recvAll(conn, 20), byteorder="big")


####################
# Send/Recv DHT Vals

def sendVal(conn, val):
    valSz = len(val)
    sendInt(conn, valSz)
    conn.sendall(val)

def recvVal(conn):
    valLen = recvInt(conn)
    return recvAll(conn, valLen)


#############################
# Send/Recv Network Addresses

def sendAddress(conn, addr):
    sendInt(conn, len(addr))
    conn.sendall(addr.encode())

def recvAddress(conn):
    msgLen = recvInt(conn)
    strAddr = recvAll(conn, msgLen).decode().split(":")
    logging.debug("Received address: {}".format(strAddr))
    return (strAddr[0], int(strAddr[1]))

def getLocalIPAddress():
    s = socket(AF_INET, SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    return s.getsockname()[0]

#############################
# Send/Recv Network bool
def recvBool(conn):
    return recvAll(conn, 1).decode("UTF-8") == "T"

def sendBool(conn, boool):
    msg = "T" if boool else "F"
    conn.sendall(msg.encode())

#############################
# Send/Recv Network Result/Status
class Result(Enum):
    N = -1
    F = 0
    T = 1
    
def recvStatus(conn):
    msg = recvAll(conn, 1).decode("UTF-8")
    return Result[msg]

def sendStatus(conn, status):
    msg = status.name.encode()
    conn.sendall(msg)
