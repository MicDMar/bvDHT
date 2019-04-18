from socket import *
from net_functions import *
import threading
import os

DEFAULT_ADDRESS = '0.0.0.0'
DEFAULT_PORT = 3000
DEFAULT_REPO_PATH = "DHT_files"

def connect(peer_addr):
  #TODO

def disconnect(pred_addr):
  #TODO

#Start of "public" functions.

def exists(key):
  #TODO

def get(key):
  #TODO

def insert(key, value):
  #TODO

def owns(key):
  #TODO

def remove(key):
  #TODO


if __name__ == "__main__":
    address = os.environ.get('ADDRESS', DEFAULT_ADDRESS)
    port = os.environ.get('PORT', DEFAULT_PORT)
    repo = os.environ.get('REPOSITORY', DEFAULT_REPO_PATH)
    
    # Setup the TCP socket
    listener = socket(AF_INET, SOCK_STREAM)
    listener.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1) # Allow address to be reused
    listener.bind((address, port))
    listener.listen()

    print("Server is listening on {}:{}".format(address, port))
    
    while True:
        print("Awaiting connection")
        conn, clientAddr = listener.accept()
        print("Received connection: {}".format(clientAddr))

        file_list = get_files(repo)
        num_files = len(file_list)
        print("Found {} files in in repsitory: {}".format(num_files, repo))
        
        # Send the client the list of files
        send_int(conn, num_files)
        for i in range(num_files):
            send_string(conn, file_list[i])

        # Receive which file the client wants
        selected_file = int.from_bytes(get_n_bytes(conn, 4), "big")
        # Check if the number is invalid
        if 0 <= selected_file > len(file_list):
            print("Invalid file selected: {}".format(selected_file))
            conn.close()
            continue
            
        # Read the contents of the selected file
        print("Sending file: {}".format(file_list[selected_file - 1]))
        with open(join(repo, file_list[selected_file - 1]), "rb") as f:
            # Send the contents
            contents = f.read()
            send_int(conn, len(contents))
            conn.send(contents)
             
        print("Closing connection: {}".format(clientAddr))
        print("-" * 15)
        conn.close()
