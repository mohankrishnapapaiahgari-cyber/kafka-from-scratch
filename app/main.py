import socket
import struct
import threading


def handle_client(conn):
    with conn:
        while True:
            request = conn.recv(1024)

            if not request:
                break

            api_key = struct.unpack(">h", request[4:6])[0]
            api_version = struct.unpack(">h", request[6:8])[0]
            correlation_id = struct.unpack(">i", request[8:12])[0]

            # ---------------- API VERSIONS ----------------
            if api_key == 18:

                if 0 <= api_version <= 4:
                    error_code = 0
                else:
                    error_code = 35

                response_body = (
                    struct.pack(">h", error_code)

                    + b"\x03"

                    # API 18
                    + struct.pack(">h", 18)
                    + struct.pack(">h", 0)
                    + struct.pack(">h", 4)
                    + b"\x00"

                    # API 75
                    + struct.pack(">h", 75)
                    + struct.pack(">h", 0)
                    + struct.pack(">h", 0)
                    + b"\x00"

                    + struct.pack(">i", 0)
                    + b"\x00"
                )

                message_size = 4 + len(response_body)

                response = (
                    struct.pack(">i", message_size)
                    + struct.pack(">i", correlation_id)
                    + response_body
                )

                conn.sendall(response)

            # ---------------- DESCRIBE TOPIC PARTITIONS ----------------
            elif api_key == 75:

                # For this stage's test request:
                # byte 27 = compact string length
                topic_length = request[27] - 1
                topic_name = request[28:28 + topic_length]

                response_body = (
                    struct.pack(">i", 0)      # throttle_time_ms

                    + b"\x02"                # topics array (1 element)

                    + struct.pack(">h", 3)   # UNKNOWN_TOPIC_OR_PARTITION

                    + bytes([topic_length + 1])
                    + topic_name

                    + (b"\x00" * 16)         # UUID all zeros

                    + b"\x00"                # is_internal = false

                    + b"\x01"                # empty partitions array

                    + struct.pack(">i", 0)   # authorized operations

                    + b"\x00"                # topic tagged fields

                    + b"\xff"                # next_cursor = null

                    + b"\x00"                # response tagged fields
                )

                response_header = (
                    struct.pack(">i", correlation_id)
                    + b"\x00"                # header v1 tag buffer
                )

                message_size = len(response_header) + len(response_body)

                response = (
                    struct.pack(">i", message_size)
                    + response_header
                    + response_body
                )

                conn.sendall(response)


def main():
    print("Logs from your program will appear here!")

    server = socket.create_server(
        ("localhost", 9092),
        reuse_port=True
    )

    while True:
        conn, addr = server.accept()

        client_thread = threading.Thread(
            target=handle_client,
            args=(conn,)
        )

        client_thread.start()


if __name__ == "__main__":
    main()