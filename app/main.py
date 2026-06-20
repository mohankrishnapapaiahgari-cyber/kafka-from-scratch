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

        api_version = struct.unpack(
            ">h",
            request[6:8]
        )[0]

        correlation_id = struct.unpack(
            ">i",
            request[8:12]
        )[0]

        if 0 <= api_version <= 4:
            error_code = 0
        else:
            error_code = 35

        response = (
            struct.pack(">i", 0)
            + struct.pack(">i", correlation_id)
            + struct.pack(">h", error_code)
        )

        conn.sendall(response)


if __name__ == "__main__":
    main()