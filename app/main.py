import socket
import struct
import threading


def handle_client(conn):
    with conn:
        while True:
            request = conn.recv(4096)

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

                pos = 12

                client_id_len = struct.unpack(">h", request[pos:pos + 2])[0]
                pos += 2 + client_id_len

                # tagged fields
                pos += 1

                # topics compact array length
                pos += 1

                # topic name
                topic_name_len = request[pos] - 1
                pos += 1

                topic_name = request[pos:pos + topic_name_len].decode()
                pos += topic_name_len

                response_header = (
                    struct.pack(">i", correlation_id)
                    + b"\x00"
                )

                # -------------------------------------------------
                # Known topic
                # -------------------------------------------------

                if topic_name == "test-topic":

                    topic_uuid = bytes.fromhex(
                        "00000000000040008000000000000091"
                    )

                    partitions = (

                        b"\x02"                      # compact array len = 1

                        + struct.pack(">h", 0)      # error code

                        + struct.pack(">i", 1)      # partition index

                        + struct.pack(">i", 1)      # leader id

                        + struct.pack(">i", 0)      # leader epoch

                        + b"\x02"                   # replicas length

                        + struct.pack(">i", 1)

                        + b"\x02"                   # ISR length

                        + struct.pack(">i", 1)

                        + b"\x01"                   # eligible leaders

                        + b"\x01"                   # last known ELR

                        + b"\x00"                   # tagged fields
                    )

                    response_body = (

                        struct.pack(">i", 0)

                        + b"\x02"

                        + struct.pack(">h", 0)

                        + bytes([len(topic_name) + 1])
                        + topic_name.encode()

                        + topic_uuid

                        + b"\x00"

                        + partitions

                        + struct.pack(">i", 0)

                        + b"\x00"

                        + b"\xff"

                        + b"\x00"
                    )

                # -------------------------------------------------
                # Unknown topic
                # -------------------------------------------------

                else:

                    response_body = (

                        struct.pack(">i", 0)

                        + b"\x02"

                        + struct.pack(">h", 3)

                        + bytes([len(topic_name) + 1])
                        + topic_name.encode()

                        + (b"\x00" * 16)

                        + b"\x00"

                        + b"\x01"

                        + struct.pack(">i", 0)

                        + b"\x00"

                        + b"\xff"

                        + b"\x00"
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
            args=(conn,),
            daemon=True
        ).start()


if __name__ == "__main__":
    main()