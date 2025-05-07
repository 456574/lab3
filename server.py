import socket
import threading
import time
from datetime import datetime

class TupleSpaceServer:
    def __init__(self):
        # Initialize the tuple space as a dictionary for key-value storage
        self.tuples = {}  
        # Lock for thread-safe operations on shared resources
        self.lock = threading.Lock()  
        # Statistics tracking
        self.stats = {
            'total_clients': 0,         # Total connected clients
            'total_ops': 0,             # Total operations performed
            'reads': 0, 'gets': 0,      # Counters for READ/GET operations
            'puts': 0, 'errors': 0,     # Counters for PUT operations and errors
            'start_time': datetime.now()# Server start time
        }
        # Flag to control server execution
        self.running = True  

    def handle_client(self, conn, addr):
        """Handle a client connection in a dedicated thread."""
        with conn:
            self.stats['total_clients'] += 1
            while True:
                # Receive data from client (up to 1024 bytes)
                data = conn.recv(1024).decode()
                if not data:  # Connection closed by client
                    break
                # Process the request and get response
                response = self.process_request(data)
                # Send response back to client
                conn.sendall(response.encode())

    def process_request(self, data):
        """Parse and execute client requests based on the protocol."""
        try:
            # Extract message length (first 3 characters)
            msg_len = int(data[:3])
            # Extract command type (4th character: R/G/P)
            cmd = data[4]
            # Split content into key and optional value
            content = data[5:msg_len+3].split(' ', 1)
            key = content[0]
            self.stats['total_ops'] += 1  # Update total operations

            with self.lock:  # Ensure thread-safe access to shared data
                if cmd == 'R':  # READ operation
                    self.stats['reads'] += 1
                    if key in self.tuples:
                        value = self.tuples[key]
                        # Format success response with length prefix
                        return f"{len(f'OK ({key}, {value}) read')+3:03d} OK ({key}, {value}) read"
                    else:
                        self.stats['errors'] += 1
                        return f"{len(f'ERR {key} does not exist')+3:03d} ERR {key} does not exist"

                elif cmd == 'G':  # GET operation
                    self.stats['gets'] += 1
                    if key in self.tuples:
                        value = self.tuples.pop(key)  # Remove and return value
                        return f"{len(f'OK ({key}, {value}) removed')+3:03d} OK ({key}, {value}) removed"
                    else:
                        self.stats['errors'] += 1
                        return f"{len(f'ERR {key} does not exist')+3:03d} ERR {key} does not exist"

                elif cmd == 'P':  # PUT operation
                    self.stats['puts'] += 1
                    if ' ' not in content[1]:
                        return self.error_response("Invalid format")
                    # Split key and value (assumes first space separates them)
                    key, value = content[1].split(' ', 1)
                    # Validate total collated size
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
