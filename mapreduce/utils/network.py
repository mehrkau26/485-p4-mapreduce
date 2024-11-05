
import socket
import json
import threading
import time


#continuously listens for messages on a socket, and calls a callback "handler" function when a message is received
def tcp_server(host, port, signals, handle_func):
    # this is TCP
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        # allows server to listen to incomining client connections on port 8000 localhost
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR,1)
        sock.bind((host, port)) # binds socket to port
        sock.listen()
        print(f"Server listening on {host}:{port}")

        sock.settimeout(1)
        
        while not signals["shutdown"]:
            try:
                clientsocket, address = sock.accept()
            except socket.timeout:
                continue
            print("Connection from", address[0])

            clientsocket.settimeout(1)
            with clientsocket:
                message_chunks = []
                while True:
                    try:
                        data = clientsocket.recv(4096)
                    except socket.timeout:
                        continue
                    if not data:
                        break
                    message_chunks.append(data)

            message_bytes = b''.join(message_chunks)
            message_str = message_bytes.decode("utf-8")

            try:
                message_dict = json.loads(message_str)
            except json.JSONDecodeError:
                continue
            # if message_dict["message_type"] == "shutdown":
            #     handle_func.appendleft(message_dict)
            # else:
            #     handle_func.append(message_dict)
            # #for message in handle_func:
                #print(message)
            handle_func(message_dict)
        print("thread has been terminated")

def tcp_client(host, port, message_dict):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            message = json.dumps(message_dict)
            print(f"Connecting to server at {host}:{port}")
            # connect to the server
            sock.connect((host, port))
            print(f"message being sent: ", message)

            sock.sendall(message.encode('utf-8'))
    except:
        return False
    return True

def udp_server(host, port, signals, handle_func):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        sock.settimeout(1)

        while True:
            try:
                message_bytes = sock.recv(4096)
            except socket.timeout:
                continue
            message_str = message_bytes.decode("utf-8")
            message_dict = json.loads(message_str)
            print(message_dict)

def udp_client(host, port, message):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.connect((host, port))

        # Send a message
        message = json.dumps({"hello": "world"})
        sock.sendall(message.encode('utf-8'))
