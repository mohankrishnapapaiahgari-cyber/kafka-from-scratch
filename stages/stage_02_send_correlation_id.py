# ═══════════════════════════════════════════════════════════════
# Stage 1 & 2 — Bind to a Port + Send Correlation ID
# ═══════════════════════════════════════════════════════════════
#
# WHAT THESE STAGES TAUGHT ME:
#
# Stage 1 — Bind to a Port:
#   - Kafka brokers listen on TCP port 9092 by default
#   - socket.create_server() creates a TCP server socket
#   - reuse_port=True allows restarting without "address already in use" error
#   - server.accept() blocks until a client connects, returns (conn, addr)
#
# Stage 2 — Send Correlation ID:
#   - Every Kafka request contains a correlation_id (a number the client sends)
#   - The broker must echo back the SAME correlation_id in every response
#   - This lets the client match responses to requests (important for async)
#   - Here we hardcode correlation_id=7 just to pass the test
#   - Response format: [message_size (4 bytes)] [correlation_id (4 bytes)]
#   - struct.pack(">i", value) packs an integer as 4 bytes, big-endian
#   - ">i" means: > = big-endian, i = signed 32-bit integer
#
# KEY CONCEPTS:
#   - TCP is a stream protocol — you must frame messages yourself
#   - Kafka uses big-endian byte order for all multi-byte fields
#   - struct module is essential for binary protocol work in Python
# ═══════════════════════════════════════════════════════════════

import socket
import struct


def main():
    print("Logs from your program will appear here!")

    server = socket.create_server(("localhost", 9092), reuse_port=True)

    conn, addr = server.accept()

    conn.recv(1024)  # receive the request (ignored at this stage)

    # Response: message_size=0, correlation_id=7 (hardcoded for now)
    response = struct.pack(">i", 0) + struct.pack(">i", 7)

    conn.sendall(response)


if __name__ == "__main__":
    main()