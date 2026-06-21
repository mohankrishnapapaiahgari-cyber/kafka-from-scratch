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

            # -------------------------------------------------
            # ApiVersions
            # -------------------------------------------------
            if api_key == 18:

                error_code = 0 if 0 <= api_version <= 4 else 35

                response_body = (
                    struct.pack(">h", error_code)

                    + b"\x03"

                    # ApiVersions
                    + struct.pack(">h", 18)
                    + struct.pack(">h", 0)
                    + struct.pack(">h", 4)
                    + b"\x00"

                    # DescribeTopicPartitions
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

            # -------------------------------------------------
            # DescribeTopicPartitions
            # -------------------------------------------------
            elif api_key == 75:

                # Request format used by CodeCrafters VT6:
                #
                # 0-3   message size
                # 4-5   api key
                # 6-7   api version
                # 8-11  correlation id
                # 12-13 client id length
                # ...   client id
                # ...   tagged fields
                # ...   topics array
                #
                pos = 12

                client_id_len = struct.unpack(">h", request[pos:pos+2])[0]
                pos += 2 + client_id_len

                # tagged fields after header
                pos += 1

                # topics compact array length
                pos += 1

                # topic name compact string
                topic_name_len = request[pos] - 1
                pos += 1

                topic_name = request[pos:pos + topic_name_len]

                response_header = (
                    struct.pack(">i", correlation_id)
                    + b"\x00"
                )

                response_body = (
                    struct.pack(">i", 0)      # throttle_time_ms

                    + b"\x02"                # topics array length = 1+1

                    + struct.pack(">h", 3)   # UNKNOWN_TOPIC_OR_PARTITION

                    + bytes([topic_name_len + 1])
                    + topic_name

                    + (b"\x00" * 16)         # UUID

                    + b"\x00"                # is_internal

                    + b"\x01"                # empty partitions array

                    + struct.pack(">i", 0)   # authorized operations

                    + b"\x00"                # topic tagged fields

                    + b"\xff"                # next_cursor = null

                    + b"\x00"                # tagged fields
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

        threading.Thread(
            target=handle_client,
            args=(conn,)
        ).start()


if __name__ == "__main__":
    main()