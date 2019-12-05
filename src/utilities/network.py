import socket
import struct
import pickle
from typing import List
from threading import Thread
from queue import Queue
from time import sleep
from utilities.atomic_int import AtomicInteger
from datetime import datetime

class ServerClientConnection:
    def __init__(self, cid, sock, address):
        self.cid = cid
        self.socket = sock
        self.address = address
        self.listen_thread = None
        self.messages = Queue()
        self.last_heartbeat = None

class ConnectionManager:
    NONE = 0
    MESSAGE = 1
    HEARTBEAT = 2
    CONFIG = 3

    def __init__(self):
        pass

    def _send(self, sock, data, message_type=MESSAGE):
        dumped = pickle.dumps(data)
        sock.send(struct.pack('ii', len(dumped), message_type))
        sock.send(dumped)

    def _recv(self, sock):
        meta_data = struct.unpack("ii", sock.recv(8))
        data = pickle.loads(sock.recv(meta_data[0]))
        return ({
            'size': meta_data[0],
            'type': meta_data[1]
        }, data)

class ClientConnectionManager(ConnectionManager):
    def __init__(self, host='127.0.0.1', port=1234):
        super().__init__()
        self.messages = Queue()
        self.running = True

        # construct a socket
        while True:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                self.socket.connect((host, port))
                break
            except ConnectionRefusedError:
                print("server is not yet up")
                self.socket.close()
                sleep(1)

        # gather the configurations and other items
        meta, configuration = self._recv(self.socket)
        assert meta['type'] == ConnectionManager.CONFIG
        self.heartbeat_rate = configuration['heartbeat']
        self.client_id = configuration['client_id']
        print(f"Client {self.client_id} configuration\n"
              f"- heartbeat rate: {self.heartbeat_rate}")

        # start the workers
        self.heartbeat_thread = Thread(target=self._send_heartbeat)
        self.manage_incoming = Thread(target=self._manage_incoming_messages)
        self.heartbeat_thread.start()
        self.manage_incoming.start()

    def close(self):
        self.running = False
        self.socket.close()
        self.heartbeat_thread.join()
        self.manage_incoming.join()

    def _send_heartbeat(self):
        while self.running:
            try:
                self._send(self.socket, '__heartbeat__', ConnectionManager.HEARTBEAT)
                sleep(float(self.heartbeat_rate) / 2.0)
            except OSError:
                pass

    def _manage_incoming_messages(self):
        while self.running:
            try:
                meta, data = self._recv(self.socket)
                self.messages.put(data)
            except OSError:
                pass

    def send_message(self, data):
        self._send(self.socket, data)

    def has_message(self):
        return not self.messages.empty()

    def get_next_message(self):
        return self.messages.get()


class ServerConnectionManager(ConnectionManager):
    def __init__(self, host='127.0.0.1', port=1234, heartbeat_max_interval=5):
        super().__init__()

        # how many seconds should there at most be between heartbeats
        self.heartbeat_max_interval = heartbeat_max_interval
        self.next_id = AtomicInteger(0)

        # initialize a socket for incoming connections
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind((host, port))

        # create a thread to wait for incoming connections
        self.running = True
        self.clients = []
        self.accept_thread = Thread(target=self._accept_new_clients)
        self.accept_thread.start()

    def close(self):
        self.running = False
        self.socket.close()
        self.accept_thread.join()
        for client in self.get_clients():
            client.listen_thread.join()

    def _accept_new_clients(self):
        self.socket.listen(5)
        while self.running:
            try:
                client, address = self.socket.accept()
                next_client_id = self.next_id.get_inc()

                # first we send the client configuration
                self._send(client, {
                    'client_id': next_client_id,
                    'heartbeat': self.heartbeat_max_interval
                }, ConnectionManager.CONFIG)

                # construct a client management object
                new_client = ServerClientConnection(next_client_id, client, address)
                new_client.listen_thread = Thread(target=self._manage_client, args=(new_client, ))
                new_client.listen_thread.start()
                self.clients.append(new_client)
            except ConnectionAbortedError:
                break

    def _manage_client(self, client: ServerClientConnection):
        while self.running:
            try:
                meta, data = self._recv(client.socket)
                if meta.get('type') == ConnectionManager.HEARTBEAT:
                    client.last_heartbeat = datetime.now()
                else:
                    client.messages.put(data)
            except struct.error:
                pass
        client.socket.close()

    def get_clients(self) -> List[ServerClientConnection]:
        return self.clients[:]

    def get_next_message(self, clients: List[ServerClientConnection]):
        for client in clients:
            if not client.messages.empty():
                return client, client.messages.get()
        return None

    def send_message(self, client: ServerClientConnection, data):
        self._send(client.socket, data)

    def broadcast_message(self, clients: List[ServerClientConnection], data):
        for client in clients:
            self.send_message(client, data)