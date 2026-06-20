import socket
import struct


def main():
    print("Logs from your program will appear here!")

    server = socket.create_server(
        ("localhost", 9092),
        reuse_port=True
    )

    conn, addr = server.accept()

    with conn:
        request = conn.recv(1024)

        correlation_id = struct.unpack(
            ">i",
            request[8:12]
        )[0]

        response = (
            struct.pack(">i", 0) +
            struct.pack(">i", correlation_id)
        )

        conn.sendall(response)


if __name__ == "__main__":
    main()