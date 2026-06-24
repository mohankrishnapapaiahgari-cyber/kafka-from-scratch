import socket
import struct
import threading
import os


# ─────────────────────────────────────────
# Varint helpers
# ─────────────────────────────────────────

def read_varint(data, pos):
    result = 0
    shift = 0
    while True:
        byte = data[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if (byte & 0x80) == 0:
            break
        shift += 7
    return result, pos


def read_signed_varint(data, pos):
    val, pos = read_varint(data, pos)
    val = (val >> 1) ^ -(val & 1)
    return val, pos


# ─────────────────────────────────────────
# Metadata log parser
# ─────────────────────────────────────────

def parse_metadata_log():
    log_path = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"

    topic_name_to_uuid  = {}
    topic_uuid_to_parts = {}

    if not os.path.exists(log_path):
        return topic_name_to_uuid, topic_uuid_to_parts

    with open(log_path, "rb") as f:
        data = f.read()

    pos = 0
    while pos < len(data):
        if pos + 61 > len(data):
            break

        batch_length = struct.unpack_from(">i", data, pos + 8)[0]
        batch_end    = pos + 8 + 4 + batch_length

        records_count = struct.unpack_from(">i", data, pos + 57)[0]
        rec_pos = pos + 61

        for _ in range(records_count):
            if rec_pos >= batch_end:
                break

            rec_len, rec_pos = read_signed_varint(data, rec_pos)
            rec_end = rec_pos + rec_len

            rec_pos += 1                              # attributes
            _, rec_pos = read_signed_varint(data, rec_pos)  # timestamp_delta
            _, rec_pos = read_signed_varint(data, rec_pos)  # offset_delta

            key_len, rec_pos = read_signed_varint(data, rec_pos)
            if key_len > 0:
                rec_pos += key_len

            val_len, rec_pos = read_signed_varint(data, rec_pos)
            if val_len > 0:
                value = data[rec_pos: rec_pos + val_len]
                parse_record_value(value, topic_name_to_uuid, topic_uuid_to_parts)

            rec_pos += val_len
            _, rec_pos = read_varint(data, rec_pos)  # headers count

        pos = batch_end

    return topic_name_to_uuid, topic_uuid_to_parts


def parse_record_value(value, topic_name_to_uuid, topic_uuid_to_parts):
    if len(value) < 3:
        return
    rec_type = value[1]
    if rec_type == 2:
        parse_topic_record(value, topic_name_to_uuid, topic_uuid_to_parts)
    elif rec_type == 3:
        parse_partition_record(value, topic_uuid_to_parts)


def parse_topic_record(value, topic_name_to_uuid, topic_uuid_to_parts):
    pos = 3
    name_len   = value[pos] - 1
    pos += 1
    topic_name = value[pos: pos + name_len].decode("utf-8")
    pos += name_len
    topic_uuid = value[pos: pos + 16]

    topic_name_to_uuid[topic_name] = topic_uuid
    if topic_uuid not in topic_uuid_to_parts:
        topic_uuid_to_parts[topic_uuid] = []


def parse_partition_record(value, topic_uuid_to_parts):
    pos = 3
    partition_id = struct.unpack_from(">i", value, pos)[0]; pos += 4
    topic_uuid   = value[pos: pos + 16];                    pos += 16

    # skip replica array
    count, pos = read_varint(value, pos); pos += (count - 1) * 4
    # skip isr array
    count, pos = read_varint(value, pos); pos += (count - 1) * 4
    # skip removing replicas
    count, pos = read_varint(value, pos); pos += (count - 1) * 4
    # skip adding replicas
    count, pos = read_varint(value, pos); pos += (count - 1) * 4

    leader       = struct.unpack_from(">i", value, pos)[0]; pos += 4
    leader_epoch = struct.unpack_from(">i", value, pos)[0]; pos += 4

    if topic_uuid not in topic_uuid_to_parts:
        topic_uuid_to_parts[topic_uuid] = []

    topic_uuid_to_parts[topic_uuid].append({
        "partition_id":  partition_id,
        "leader":        leader,
        "leader_epoch":  leader_epoch,
    })


# ─────────────────────────────────────────
# Response builders
# ─────────────────────────────────────────

def build_single_topic_entry(topic_name, topic_name_to_uuid, topic_uuid_to_parts):
    """Build the bytes for one topic entry inside the topics array."""
    name_bytes = topic_name.encode("utf-8")

    if topic_name not in topic_name_to_uuid:
        # Unknown topic
        return (
            struct.pack(">h", 3)              # error = UNKNOWN_TOPIC_OR_PARTITION
            + bytes([len(name_bytes) + 1])    # compact string length
            + name_bytes
            + (b"\x00" * 16)                 # UUID = all zeros
            + b"\x00"                         # is_internal
            + b"\x01"                         # empty partitions array
            + struct.pack(">i", 0)            # authorized_operations
            + b"\x00"                         # topic tagged fields
        )

    # Known topic
    topic_uuid = topic_name_to_uuid[topic_name]
    partitions = sorted(
        topic_uuid_to_parts.get(topic_uuid, []),
        key=lambda p: p["partition_id"]       # sort partitions by index
    )

    partition_data = b""
    for p in partitions:
        partition_data += (
            struct.pack(">h", 0)              # error_code = NONE
            + struct.pack(">i", p["partition_id"])
            + struct.pack(">i", p["leader"])
            + struct.pack(">i", p["leader_epoch"])
            + b"\x02" + struct.pack(">i", 1) # replicas compact array [1]
            + b"\x02" + struct.pack(">i", 1) # isr compact array [1]
            + b"\x01"                         # eligible_leader_replicas (empty)
            + b"\x01"                         # last_known_elr (empty)
            + b"\x01"                         # offline_replicas (empty)
            + b"\x00"                         # partition tagged fields
        )

    parts_array = bytes([len(partitions) + 1]) + partition_data

    return (
        struct.pack(">h", 0)                  # error_code = NONE
        + bytes([len(name_bytes) + 1])        # compact string length
        + name_bytes
        + topic_uuid                          # 16 byte UUID
        + b"\x00"                             # is_internal
        + parts_array
        + struct.pack(">i", 0x00000df8)       # authorized_operations
        + b"\x00"                             # topic tagged fields
    )


def build_describe_topic_partitions_response(correlation_id, topic_names,
                                              topic_name_to_uuid,
                                              topic_uuid_to_parts):
    """
    Build full DescribeTopicPartitions response for a LIST of topic names.
    Topics must be sorted alphabetically.
    """
    # ── Sort topics alphabetically ───────────────────────────
    sorted_names = sorted(topic_names)

    # ── Build each topic entry ───────────────────────────────
    topics_data = b""
    for name in sorted_names:
        topics_data += build_single_topic_entry(
            name, topic_name_to_uuid, topic_uuid_to_parts
        )

    response_header = (
        struct.pack(">i", correlation_id)
        + b"\x00"                             # tagged fields
    )

    response_body = (
        struct.pack(">i", 0)                  # throttle_time_ms
        + bytes([len(sorted_names) + 1])      # compact array length = N + 1
        + topics_data
        + b"\xff"                             # next_cursor = null
        + b"\x00"                             # response tagged fields
    )

    message_size = len(response_header) + len(response_body)
    return (
        struct.pack(">i", message_size)
        + response_header
        + response_body
    )


# ─────────────────────────────────────────
# Request parser for DescribeTopicPartitions
# ─────────────────────────────────────────

def parse_topic_names_from_request(request):
    """
    Parse ALL topic names from a DescribeTopicPartitions request.
    Returns a list of topic name strings.
    """
    pos = 12

    # client_id (int16 length + bytes)
    client_id_len = struct.unpack(">h", request[pos: pos + 2])[0]
    pos += 2 + max(client_id_len, 0)

    # tagged fields after request header
    pos += 1

    # topics compact array length (varint): stored as actual_count + 1
    topics_array_len, pos = read_varint(request, pos)
    num_topics = topics_array_len - 1

    topic_names = []
    for _ in range(num_topics):
        # topic name compact string: stored length = actual + 1
        name_len = request[pos] - 1
        pos += 1
        topic_name = request[pos: pos + name_len].decode("utf-8")
        pos += name_len
        pos += 1    # topic tagged fields
        topic_names.append(topic_name)

    return topic_names


# ─────────────────────────────────────────
# Client handler
# ─────────────────────────────────────────

def handle_client(conn):
    topic_name_to_uuid, topic_uuid_to_parts = parse_metadata_log()

    with conn:
        while True:
            raw = conn.recv(4)
            if not raw or len(raw) < 4:
                break

            msg_size = struct.unpack(">i", raw)[0]
            request  = raw + conn.recv(msg_size)

            api_key        = struct.unpack(">h", request[4:6])[0]
            api_version    = struct.unpack(">h", request[6:8])[0]
            correlation_id = struct.unpack(">i", request[8:12])[0]

            # ── ApiVersions ──────────────────────────────────
            if api_key == 18:
                error_code = 0 if 0 <= api_version <= 4 else 35

                response_body = (
                    struct.pack(">h", error_code)
                    + b"\x03"
                    + struct.pack(">h", 18)
                    + struct.pack(">h", 0)
                    + struct.pack(">h", 4)
                    + b"\x00"
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

            # ── DescribeTopicPartitions ──────────────────────
            elif api_key == 75:
                topic_names = parse_topic_names_from_request(request)

                response = build_describe_topic_partitions_response(
                    correlation_id, topic_names,
                    topic_name_to_uuid, topic_uuid_to_parts
                )
                conn.sendall(response)


# ─────────────────────────────────────────
# Main
# ─────────────────────────────────────────

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