import socket
import struct


def main():
    print("Logs from your program will appear here!")

    server = socket.create_server(
        ("localhost", 9092),
        reuse_port=True
    )

    conn, addr = server.accept()

    conn.recv(1024)

    response = (
        struct.pack(">i", 0) +
        struct.pack(">i", 7)
    )

    conn.sendall(response)


if __name__ == "__main__":
    main()