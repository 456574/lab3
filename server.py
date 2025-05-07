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

    def error_response(self, msg):
        return f"{len(msg)+3:03d} ERR {msg}"

    def start_server(self, port):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('localhost', port))
            s.listen()
            print(f"Server listening on port {port}")
            threading.Thread(target=self.print_stats, daemon=True).start()
            while self.running:
                conn, addr = s.accept()
                client_thread = threading.Thread(target=self.handle_client, args=(conn, addr))
                client_thread.start()

    def print_stats(self):
        while self.running:
            time.sleep(10)
            with self.lock:
                num_tuples = len(self.tuples)
                avg_key = sum(len(k) for k in self.tuples)/num_tuples if num_tuples else 0
                avg_val = sum(len(v) for v in self.tuples.values())/num_tuples if num_tuples else 0
                print(f"[Stats] Tuples: {num_tuples} | Avg key: {avg_key:.1f} | Avg value: {avg_val:.1f} | "
                      f"Clients: {self.stats['total_clients']} | Ops: {self.stats['total_ops']} "
                      f"(R:{self.stats['reads']} G:{self.stats['gets']} P:{self.stats['puts']} ERR:{self.stats['errors']})")

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2 or not (50000 <= int(sys.argv[1]) <=59999):
        print("Usage: python server.py <port 50000-59999>")
        exit(1)
    server = TupleSpaceServer()
    server.start_server(int(sys.argv[1]))




    