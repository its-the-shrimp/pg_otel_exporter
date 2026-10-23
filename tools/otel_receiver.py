#!/usr/bin/env python3
"""OTLP/HTTP receiver for smoke-testing pg_otel_exporter.

Prints path/size + printable strings, and saves raw bodies for decoding.
"""
import os
import re
from http.server import BaseHTTPRequestHandler, HTTPServer

DUMP = "/tmp/opencode/dumps"
n = 0


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        global n
        size = int(self.headers.get("content-length", 0))
        body = self.rfile.read(size)
        n += 1
        os.makedirs(DUMP, exist_ok=True)
        with open(f"{DUMP}/{n:03d}_{self.path.replace('/','_')}.bin", "wb") as f:
            f.write(body)
        strings = re.findall(rb"[\x20-\x7e]{4,}", body)
        print(f"\n=== {self.path} ({size} bytes)", flush=True)
        for s in strings[:60]:
            print("  ", s.decode(), flush=True)
        self.send_response(200)
        self.end_headers()

    def log_message(self, *a):
        pass


HTTPServer(("127.0.0.1", 4319), Handler).serve_forever()
