import socket
import sys
import re

def send_request(host, port, request_file):
    """Send requests from a file to the server and display responses."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        # Establish TCP connection to the server
        s.connect((host, port))
        with open(request_file, 'r') as f:
            for line in f:
                line = line.strip()  # Remove leading/trailing whitespace
                if not line:
                    continue  # Skip empty lines
                
                # Split line into command parts (max 3 splits: cmd, key, value)
                parts = re.split(r'\s+', line, 2)
                if len(parts) < 2:
                    print(f"Ignoring invalid line: {line}")  # Missing key
                    continue
                
                cmd = parts[0].upper()  # Normalize command (PUT/READ/GET)
                key = parts[1]
                value = parts[2] if len(parts) > 2 else ""  # Optional value
                
                # Validate and construct protocol message
                if cmd == 'PUT':
                    # Check collated size (key + value <= 970)
                    if len(key) + len(value) > 970:
                        print(f"{line}: ERR key-value too long")
                        continue
                    msg = f"P {key} {value}"  # PUT format: P <key> <value>
                elif cmd == 'READ':
                    msg = f"R {key}"          # READ format: R <key>
                elif cmd == 'GET':
                    msg = f"G {key}"          # GET format: G <key>
                else:
                    print(f"Invalid command: {cmd}")
                    continue
                
                 # Calculate total message length (NNN includes command + content)
                msg_len = len(msg) + 4  # 3-digit prefix + 1 space (e.g., "123 P...")
                if msg_len > 999:
                    print(f"{line}: ERR message too long")
                    continue
                # Format full message: "NNN CMD..."
                full_msg = f"{msg_len:03d} {msg}"
                # Send message to server
                s.sendall(full_msg.encode())
                # Receive and parse response
                response = s.recv(1024).decode()
                if not response:
                    print(f"{line}: No response from server")
                    continue
                # Extract response length (first 3 digits)
                resp_len = int(response[:3])
                # Extract response content (status + details)
                resp_content = response[4:resp_len+3]  # Skip NNN and space
                print(f"{line}: {resp_content}")  # Format: "CMD...: RESPONSE"

if __name__ == "__main__":
    # Validate command-line arguments
    if len(sys.argv) != 4:
        print("Usage: python client.py <host> <port> <request_file>")
        exit(1)
    # Start client with provided arguments
    send_request(sys.argv[1], int(sys.argv[2]), sys.argv[3])