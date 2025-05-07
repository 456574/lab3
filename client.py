import socket
import sys
import re

def send_request(host, port, request_file):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        with open(request_file, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = re.split(r'\s+', line, 2)
                if len(parts) < 2:
                    print(f"Ignoring invalid line: {line}")
                    continue
                
                cmd = parts[0]
                key = parts[1]
                value = parts[2] if len(parts) > 2 else ""
                
                if cmd == 'PUT':
                    if len(key) + len(value) > 970:
                        print(f"{line}: ERR key-value too long")
                        continue
                    msg = f"P {key} {value}"
                elif cmd == 'READ':
                    msg = f"R {key}"
                elif cmd == 'GET':
                    msg = f"G {key}"
                else:
                    print(f"Invalid command: {cmd}")
                    continue
                
                


    