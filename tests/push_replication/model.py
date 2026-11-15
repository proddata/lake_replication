"""Independent Python encoder for cross-language protocol/import qualification."""
import hashlib
import struct

STREAM = bytes([1]) * 16
TABLES = [bytes([2]) * 16, bytes([3]) * 16]


def row_bytes(row):
    encoded = struct.pack('!I', len(row))
    for index, value in enumerate(row, 1):
        encoded += struct.pack('!I', index)
        if value is None:
            encoded += b'\x01'
        else:
            value = str(value).encode()
            encoded += b'\x03' + struct.pack('!I', len(value)) + value
    return encoded


def batch(identity, sequence, rows, *, generation=1, operation=0, truncate_options=0):
    stream = STREAM + struct.pack('!QIQ', identity['system_identifier'], identity['database_oid'], generation)
    end = sequence * 256
    batch_id = hashlib.sha256(b'lake-push-batch-v1' + stream + struct.pack('!Q', end)).digest()[:16]
    events = []
    counts = {}
    for ordinal, (table, row) in enumerate(rows):
        table_id = TABLES[table]
        counts[table_id] = counts.get(table_id, 0) + 1
        payload = struct.pack('!IB', ordinal, 1) + table_id + struct.pack('!IB', 1, operation)
        payload += bytes([truncate_options]) if operation == 3 else row_bytes(row)
        events.append((table, ordinal, payload))
    header = (b'LKPUSH' + struct.pack('!H', 1) + stream + struct.pack('!Q', sequence) + batch_id
              + struct.pack('!IQQQqII', sequence, end - 32, end - 16, end, sequence * 1000,
                            len(events), len(counts)))
    for table_id, count in sorted(counts.items()):
        header += table_id + struct.pack('!II', 1, count)
    body = header + b''.join(struct.pack('!I', len(payload)) + payload for _, _, payload in events)
    return dict(header=header, digest=hashlib.sha256(body).digest(), events=events)


def insert_sql(sequence, encoded):
    statements = ['BEGIN;', f"INSERT INTO lake_capture_log.manifests VALUES({sequence},"
                  f"'{encoded['header'].hex()}','{encoded['digest'].hex()}');"]
    for table, ordinal, payload in encoded['events']:
        statements.append(f"INSERT INTO lake_capture_log.events_{'ab'[table]} VALUES "
                          f"({sequence},{ordinal},'{payload.hex()}');")
    return '\n'.join(statements + ['COMMIT;'])
