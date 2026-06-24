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
    """
    Parse /tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log
    Returns:
        topic_name_to_uuid : dict  { topic_name_str -> uuid_bytes (16 bytes) }
        topic_uuid_to_parts: dict  { uuid_bytes -> list of partition dicts }
    """
    log_path = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"

    topic_name_to_uuid = {}   # str  -> bytes(16)
    topic_uuid_to_parts = {}  # bytes(16) -> [{"partition_id":int, "leader":int, ...}]

    if not os.path.exists(log_path):
        return topic_name_to_uuid, topic_uuid_to_parts

    with open(log_path, "rb") as f:
        data = f.read()

    pos = 0
    while pos < len(data):

        # ── RecordBatch header ──────────────────────────────
        if pos + 61 > len(data):
            break

        base_offset = struct.unpack_from(">q", data, pos)[0]
        batch_length = struct.unpack_from(">i", data, pos + 8)[0]
        # batch_length covers from byte 12 to end of batch
        batch_end = pos + 8 + 4 + batch_length  # 8=offset, 4=length field itself

        records_count = struct.unpack_from(">i", data, pos + 57)[0]
        rec_pos = pos + 61  # records start here

        # ── Parse each Record ───────────────────────────────
        for _ in range(records_count):
            if rec_pos >= batch_end:
                break

            # record length (signed varint)
            rec_len, rec_pos = read_signed_varint(data, rec_pos)

            rec_start = rec_pos
            rec_end   = rec_pos + rec_len

            # attributes (1 byte)
            rec_pos += 1

            # timestamp_delta (signed varint)
            _, rec_pos = read_signed_varint(data, rec_pos)

            # offset_delta (signed varint)
            _, rec_pos = read_signed_varint(data, rec_pos)

            # key_length (signed varint) — negative means null
            key_len, rec_pos = read_signed_varint(data, rec_pos)
            if key_len > 0:
                rec_pos += key_len

            # value_length (signed varint)
            val_len, rec_pos = read_signed_varint(data, rec_pos)

            if val_len > 0:
                value = data[rec_pos: rec_pos + val_len]
                parse_record_value(
                    value,
                    topic_name_to_uuid,
                    topic_uuid_to_parts
                )

            rec_pos += val_len

            # headers count (varint, always 0)
            _, rec_pos = read_varint(data, rec_pos)

        pos = batch_end

    return topic_name_to_uuid, topic_uuid_to_parts


def parse_record_value(value, topic_name_to_uuid, topic_uuid_to_parts):
    """Dispatch on record type and extract fields."""
    if len(value) < 3:
        return

    # frame_version = value[0]
    rec_type = value[1]
    # version    = value[2]

    if rec_type == 2:
        parse_topic_record(value, topic_name_to_uuid, topic_uuid_to_parts)
    elif rec_type == 3:
        parse_partition_record(value, topic_uuid_to_parts)


def parse_topic_record(value, topic_name_to_uuid, topic_uuid_to_parts):
    """
    TopicRecord (type=2):
      [0] frame_version
      [1] type = 2
      [2] version
      [3] compact string length (actual_len + 1)
      [4..4+name_len] topic name
      then 16 bytes UUID
    """
    pos = 3  # skip frame_version, type, version

    name_len = value[pos] - 1   # compact string: stored length is actual+1
    pos += 1

    topic_name = value[pos: pos + name_len].decode("utf-8")
    pos += name_len

    topic_uuid = value[pos: pos + 16]
    pos += 16

    topic_name_to_uuid[topic_name] = topic_uuid
    if topic_uuid not in topic_uuid_to_parts:
        topic_uuid_to_parts[topic_uuid] = []


def parse_partition_record(value, topic_uuid_to_parts):
    """
    PartitionRecord (type=3):
      [0] frame_version
      [1] type = 3
      [2] version
      [3-6] partition_id (int32)
      [7-22] topic_uuid (16 bytes)
      then compact arrays: replicas, isr, removing, adding
      then leader (int32), leader_epoch (int32), partition_epoch (int32)
      then directories compact array
    """
    pos = 3

    partition_id = struct.unpack_from(">i", value, pos)[0]
    pos += 4

    topic_uuid = value[pos: pos + 16]
    pos += 16

    # skip replica array
    replica_count, pos = read_varint(value, pos)
    replica_count -= 1  # compact array: stored = actual + 1
    pos += replica_count * 4

    # skip isr array
    isr_count, pos = read_varint(value, pos)
    isr_count -= 1
    pos += isr_count * 4

    # skip removing_replicas
    rem_count, pos = read_varint(value, pos)
    rem_count -= 1
    pos += rem_count * 4

    # skip adding_replicas
    add_count, pos = read_varint(value, pos)
    add_count -= 1
    pos += add_count * 4

    leader       = struct.unpack_from(">i", value, pos)[0]; pos += 4
    leader_epoch = struct.unpack_from(">i", value, pos)[0]; pos += 4

    partition_info = {
        "partition_id":  partition_id,
        "leader":        leader,
        "leader_epoch":  leader_epoch,
    }

    if topic_uuid not in topic_uuid_to_parts:
        topic_uuid_to_parts[topic_uuid] = []
    topic_uuid_to_parts[topic_uuid].append(partition_info)


# ─────────────────────────────────────────
# Response builders
# ─────────────────────────────────────────

def build_describe_topic_partitions_response(correlation_id, topic_name,
                                              topic_name_to_uuid,
                                              topic_uuid_to_parts):
    """Build the full DescribeTopicPartitions response bytes."""

    response_header = (
        struct.pack(">i", correlation_id)
        + b"\x00"   # tagged fields
    )

    if topic_name not in topic_name_to_uuid:
        # ── Unknown topic ───────────────────────────────────
        name_bytes = topic_name.encode("utf-8")
        response_body = (
            struct.pack(">i", 0)          # throttle_time_ms
            + b"\x02"                     # topics array length (1 topic)
            + struct.pack(">h", 3)        # error = UNKNOWN_TOPIC_OR_PARTITION
            + bytes([len(name_bytes) + 1])# compact string length
            + name_bytes
            + (b"\x00" * 16)             # UUID = all zeros
            + b"\x00"                    # is_internal
            + b"\x01"                    # empty partitions array
            + struct.pack(">i", 0)       # authorized_operations
            + b"\x00"                    # topic tagged fields
            + b"\xff"                    # next_cursor = null
            + b"\x00"                    # response tagged fields
        )
    else:
        # ── Known topic ─────────────────────────────────────
        topic_uuid  = topic_name_to_uuid[topic_name]
        partitions  = topic_uuid_to_parts.get(topic_uuid, [])
        name_bytes  = topic_name.encode("utf-8")

        # Build partition array entries
        partition_data = b""
        for p in partitions:
            partition_data += (
                struct.pack(">h", 0)              # error_code = NONE
                + struct.pack(">i", p["partition_id"])
                + struct.pack(">i", p["leader"])
                + struct.pack(">i", p["leader_epoch"])
                # replica array (compact): [1] = one broker id = 1
                + b"\x02"
                + struct.pack(">i", 1)
                # isr array (compact): [1]
                + b"\x02"
                + struct.pack(">i", 1)
                # eligible_leader_replicas (compact empty)
                + b"\x01"
                # last_known_elr (compact empty)
                + b"\x01"
                # offline_replicas (compact empty)
                + b"\x01"
                + b"\x00"                         # partition tagged fields
            )

        # array length = number of partitions + 1 (compact array encoding)
        parts_array = bytes([len(partitions) + 1]) + partition_data

        response_body = (
            struct.pack(">i", 0)          # throttle_time_ms
            + b"\x02"                     # topics array: 1 topic
            + struct.pack(">h", 0)        # error_code = NONE
            + bytes([len(name_bytes) + 1])
            + name_bytes
            + topic_uuid                  # 16 byte UUID
            + b"\x00"                     # is_internal
            + parts_array
            + struct.pack(">i", 0x00000df8)  # authorized_operations
            + b"\x00"                     # topic tagged fields
            + b"\xff"                     # next_cursor = null
            + b"\x00"                     # response tagged fields
        )

    message_size = len(response_header) + len(response_body)
    return (
        struct.pack(">i", message_size)
        + response_header
        + response_body
    )


# ─────────────────────────────────────────
# Client handler
# ─────────────────────────────────────────

def handle_client(conn):
    # Parse metadata once per connection
    topic_name_to_uuid, topic_uuid_to_parts = parse_metadata_log()

    with conn:
        while True:
            raw = conn.recv(4)
            if not raw or len(raw) < 4:
                break

            msg_size = struct.unpack(">i", raw)[0]
            request  = raw + conn.recv(msg_size)

            api_key     = struct.unpack(">h", request[4:6])[0]
            api_version = struct.unpack(">h", request[6:8])[0]
            correlation_id = struct.unpack(">i", request[8:12])[0]

            # ── ApiVersions ─────────────────────────────────
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
                pos = 12
                client_id_len = struct.unpack(">h", request[pos:pos+2])[0]
                pos += 2 + client_id_len
                pos += 1   # tagged fields after header
                pos += 1   # topics compact array length

                topic_name_len = request[pos] - 1
                pos += 1
                topic_name = request[pos: pos + topic_name_len].decode("utf-8")

                response = build_describe_topic_partitions_response(
                    correlation_id, topic_name,
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