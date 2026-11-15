"""A bounded persistent psql session for snapshot qualification."""
import queue
import subprocess
import threading
import time


class Session:
    def __init__(self, args):
        self.process = subprocess.Popen(args, text=True, stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                        bufsize=1)
        self.output = queue.Queue()
        self.errors = []
        self.sequence = 0

        def read_output():
            for line in self.process.stdout:
                self.output.put(line.rstrip('\n'))
            self.output.put(None)

        def read_errors():
            for line in self.process.stderr:
                self.errors.append(line)

        self.readers = [threading.Thread(target=read_output, daemon=True),
                        threading.Thread(target=read_errors, daemon=True)]
        for reader in self.readers:
            reader.start()

    def query(self, statement, timeout=30):
        self.sequence += 1
        marker = f'push_query_finished_{self.sequence}'
        self.process.stdin.write(statement.rstrip(';') + ';\n\\echo ' + marker + '\n')
        self.process.stdin.flush()
        rows = []
        deadline = time.monotonic() + timeout
        while True:
            row = self.output.get(timeout=max(0.01, deadline - time.monotonic()))
            if row is None:
                raise RuntimeError('psql exited: ' + ''.join(self.errors)[-2000:])
            if row == marker:
                return rows
            rows.append(row)

    def close(self):
        if self.process.poll() is None:
            self.process.stdin.close()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        for reader in self.readers:
            reader.join(timeout=2)
        for stream in [self.process.stdout, self.process.stderr]:
            stream.close()
