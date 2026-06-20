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
        while True:
            request = conn.recv(1024)
            if not request:
                break

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
        
        response_body = (
            struct.pack(">h", error_code)      # error_code

            + b"\x02"                # compact array length = 1+1

            + struct.pack(">h", 18)   # api_key
            + struct.pack(">h", 0)    # min_version
            + struct.pack(">h", 4)    # max_version
            + b"\x00"                # tag buffer

            + struct.pack(">i", 0)    # throttle_time_ms
            + b"\x00"                # tag buffer
        )

        message_size = 4 + len(response_body)

        response = (
            struct.pack(">i", message_size)
            + struct.pack(">i", correlation_id)
            + response_body
        )

        conn.sendall(response)


if __name__ == "__main__":
    main()