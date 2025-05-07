import socket
import threading
import time
from datetime import datetime

class TupleSpaceServer:
    def __init__(self):
        self.tuples = {}  # 键值对存储
        self.lock = threading.Lock()
        self.stats = {
            'total_clients': 0,
            'total_ops': 0, 'reads': 0, 'gets': 0, 'puts': 0, 'errors': 0,
            'start_time': datetime.now()
        }
        self.running = True

    def handle_client(self, conn, addr):
        with conn:
            self.stats['total_clients'] += 1
            while True:
                data = conn.recv(1024).decode()
                if not data:
                    break
                response = self.process_request(data)
                conn.sendall(response.encode())

    def process_request(self, data):
        try:
            msg_len = int(data[:3])
            cmd = data[4]
            content = data[5:msg_len+3].split(' ', 1)
            key = content[0]
            self.stats['total_ops'] += 1

            with self.lock:
                if cmd == 'R':  # READ
                    self.stats['reads'] += 1
                    if key in self.tuples:
                        value = self.tuples[key]
                        return f"{len(f'OK ({key}, {value}) read')+3:03d} OK ({key}, {value}) read"
                    else:
                        self.stats['errors'] += 1
                        return f"{len(f'ERR {key} does not exist')+3:03d} ERR {key} does not exist"
                elif cmd == 'G':  # GET
                    self.stats['gets'] += 1
                    if key in self.tuples:
                        value = self.tuples.pop(key)
                        return f"{len(f'OK ({key}, {value}) removed')+3:03d} OK ({key}, {value}) removed"
                    else:
                        self.stats['errors'] += 1
                        return f"{len(f'ERR {key} does not exist')+3:03d} ERR {key} does not exist"
                elif cmd == 'P':  # PUT
                    self.stats['puts'] += 1
                    if ' ' not in content[1]:
                        return self.error_response("Invalid format")
                    key, value = content[1].split(' ', 1)
                    if len(key) + len(value) > 970:
                        return self.error_response("Key-value too long")
                    if key in self.tuples:
                        self.stats['errors'] += 1
                        return f"{len(f'ERR {key} already exists')+3:03d} ERR {key} already exists"
                    else:
                        self.tuples[key] = value
                        return f"{len(f'OK ({key}, {value}) added')+3:03d} OK ({key}, {value}) added"
        except Exception as e:
            return self.error_response(str(e))

   