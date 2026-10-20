#!/usr/bin/env python3

import base64
import hashlib
import json
import os
import shutil
import socket
import struct
import subprocess
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse


ROOT = Path(__file__).resolve().parents[1]
TOPIC = "e2e-test"
RUN_ID = "e2e123abc"
EXPECTED_ANALYSE_TASKS = 5
TIMEOUT = 300


class E2EServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address, handler):
        super().__init__(address, handler)
        self.messages = []
        self.messages_lock = threading.Lock()
        self.connection_closed = threading.Event()
        self.registration_received = threading.Event()
        self.registration_body = None

    def record_message(self, payload):
        with self.messages_lock:
            self.messages.append(payload)

    def snapshot(self):
        with self.messages_lock:
            return list(self.messages)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        # Keep test output concise.
        pass

    def respond_json(self, status, data):
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        if urlparse(self.path).path != "/register":
            self.respond_json(404, {"error": "not found"})
            return

        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)

        try:
            registration = json.loads(body or b"{}")
        except json.JSONDecodeError:
            self.respond_json(400, {"error": "invalid JSON"})
            return

        self.server.registration_body = registration
        self.server.registration_received.set()

        # The observer expects a JSON response containing the WebSocket URL.
        self.respond_json(
            200,
            {"url": f"ws://127.0.0.1:{self.server.server_port}/events"},
        )

    @staticmethod
    def read_exact(stream, size):
        chunks = []
        remaining = size

        while remaining:
            chunk = stream.read(remaining)
            if not chunk:
                raise EOFError("WebSocket connection closed")
            chunks.append(chunk)
            remaining -= len(chunk)

        return b"".join(chunks)

    def read_frame(self):
        first, second = self.read_exact(self.rfile, 2)
        fin = bool(first & 0x80)
        opcode = first & 0x0F
        masked = bool(second & 0x80)
        length = second & 0x7F

        if length == 126:
            length = struct.unpack("!H", self.read_exact(self.rfile, 2))[0]
        elif length == 127:
            length = struct.unpack("!Q", self.read_exact(self.rfile, 8))[0]

        mask = self.read_exact(self.rfile, 4) if masked else b""
        payload = self.read_exact(self.rfile, length)

        if masked:
            payload = bytes(
                byte ^ mask[i % 4]
                for i, byte in enumerate(payload)
            )

        return fin, opcode, payload

    def send_frame(self, opcode, payload=b""):
        # Server-to-client WebSocket frames are not masked.
        header = bytes([0x80 | opcode])
        length = len(payload)

        if length < 126:
            header += bytes([length])
        elif length <= 65535:
            header += bytes([126]) + struct.pack("!H", length)
        else:
            header += bytes([127]) + struct.pack("!Q", length)

        self.wfile.write(header + payload)
        self.wfile.flush()

    def do_GET(self):
        if urlparse(self.path).path != "/events":
            self.send_error(404)
            return

        key = self.headers.get("Sec-WebSocket-Key")
        upgrade = self.headers.get("Upgrade", "").lower()

        if not key or upgrade != "websocket":
            self.send_error(400, "Expected WebSocket upgrade")
            return

        accept = base64.b64encode(
            hashlib.sha1(
                (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()
            ).digest()
        ).decode()

        self.send_response_only(101, "Switching Protocols")
        self.send_header("Upgrade", "websocket")
        self.send_header("Connection", "Upgrade")
        self.send_header("Sec-WebSocket-Accept", accept)
        self.end_headers()
        self.wfile.flush()

        fragments = bytearray()
        fragment_opcode = None

        try:
            self.connection.settimeout(30)

            while True:
                fin, opcode, payload = self.read_frame()

                if opcode == 0x8:  # Close
                    self.send_frame(0x8, payload)
                    self.server.connection_closed.set()
                    return

                if opcode == 0x9:  # Ping
                    self.send_frame(0xA, payload)
                    continue

                if opcode == 0x1:  # Text frame
                    fragments = bytearray(payload)
                    fragment_opcode = opcode
                elif opcode == 0x0 and fragment_opcode is not None:
                    fragments.extend(payload)
                else:
                    continue

                if fin and fragment_opcode == 0x1:
                    try:
                        message = json.loads(fragments.decode("utf-8"))
                    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                        raise AssertionError(
                            f"Invalid JSON received: {fragments!r}"
                        ) from exc

                    self.server.record_message(message)
                    fragments = bytearray()
                    fragment_opcode = None

        except (EOFError, OSError, socket.timeout):
            self.server.connection_closed.set()


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def main():
    pipeline = ROOT / "validation" / "discover.nf"
    if not pipeline.exists():
        pipeline = ROOT / "src" / "validation" / "discover.nf"

    check(pipeline.exists(), f"Pipeline not found: {pipeline}")

    nextflow = shutil.which("nextflow")
    check(nextflow is not None, "Could not find 'nextflow' on PATH")

    server = E2EServer(("127.0.0.1", 0), Handler)
    server_thread = threading.Thread(
        target=server.serve_forever,
        daemon=True,
    )
    server_thread.start()

    try:
        with tempfile.TemporaryDirectory(prefix="nf-ws-events-e2e-") as tmp:
            tmpdir = Path(tmp)
            workdir = tmpdir / "work"
            config = tmpdir / "e2e.config"

            # The project config supplies your plugin declaration.
            # This additional config overrides the test endpoint and settings.
            config.write_text(
                f"""
wsEvents {{
    enabled = true
    url_register = 'http://127.0.0.1:{server.server_port}/register'
    topic = '{TOPIC}'
    runId = '{RUN_ID}'
    failOnSendError = true

    events {{
        workflowEvents = true
        processStart = true
        processComplete = true
        processCached = true
    }}
}}
""",
                encoding="utf-8",
            )

            command = [
                nextflow,
                "run",
                str(pipeline),
                "-c",
                str(config),
                "-work-dir",
                str(workdir),
                "-ansi-log",
                "false",
            ]

            print("Running:", " ".join(command))
            result = subprocess.run(
                command,
                cwd=ROOT,
                text=True,
                capture_output=True,
                timeout=TIMEOUT,
                env=os.environ.copy(),
            )

            print(result.stdout)
            if result.returncode != 0:
                print(result.stderr)
                raise AssertionError(
                    f"Nextflow exited with code {result.returncode}"
                )

            check(
                server.registration_received.wait(10),
                "Registration endpoint was never called",
            )
            check(
                server.connection_closed.wait(15),
                "WebSocket did not close within 15 seconds",
            )

            messages = server.snapshot()
            check(messages, "No WebSocket messages received")

            # Validate registration request.
            registration = server.registration_body
            check(isinstance(registration, dict), "Invalid registration body")
            check(
                registration.get("topic") == TOPIC,
                f"Unexpected registration topic: {registration}",
            )
            check(
                str(registration.get("user_id")) != "",
                "Registration request has no user_id",
            )

            # Validate every received event.
            for event in messages:
                check(event.get("topic") == TOPIC,
                      f"Unexpected topic: {event}")
                message = event.get("message")
                check(isinstance(message, dict),
                      f"Missing message object: {event}")
                check(message.get("run_id") == RUN_ID,
                      f"Unexpected run_id: {message}")
                check(message.get("run_name"),
                      f"Missing run_name: {message}")
                check(message.get("timestamp"),
                      f"Missing timestamp: {message}")

            workflow_events = [
                e["message"] for e in messages
                if e["message"].get("step") == "workflow"
            ]

            check(
                [m.get("status") for m in workflow_events]
                == ["started", "completed"],
                f"Unexpected workflow lifecycle: {workflow_events}",
            )

            process_events = [
                e["message"] for e in messages
                if e["message"].get("step") != "workflow"
            ]

            started = [
                m for m in process_events if m.get("status") == "started"
            ]
            completed = [
                m for m in process_events if m.get("status") == "completed"
            ]

            check(
                len(started) == 11,
                f"Expected 11 process-start events, got {len(started)}",
            )
            check(
                len(completed) == 11,
                f"Expected 11 process-completion events, got {len(completed)}",
            )

            step_counts = {}
            for message in started:
                step = message.get("step", "")
                # The process name appears before the tag in "NAME (tag)".
                process_name = step.split(" (", 1)[0]
                step_counts[process_name] = step_counts.get(process_name, 0) + 1

                check(message.get("task_id") is not None,
                      f"Start event missing task_id: {message}")
                check(message.get("task_tag") is not None,
                      f"Start event missing task_tag: {message}")

            check(step_counts.get("DISCOVER") == 1,
                  f"Unexpected DISCOVER count: {step_counts}")
            check(step_counts.get("ANALYSE") == EXPECTED_ANALYSE_TASKS,
                  f"Unexpected ANALYSE count: {step_counts}")
            check(step_counts.get("SUMMARISE") == EXPECTED_ANALYSE_TASKS,
                  f"Unexpected SUMMARISE count: {step_counts}")

            for message in completed:
                check(message.get("exit_code") == 0,
                      f"Expected exit_code 0: {message}")
                check(message.get("task_id") is not None,
                      f"Completion event missing task_id: {message}")

            print("\nE2E TEST PASSED")
            print(f"Registration: {registration}")
            print(f"Received JSON messages: {len(messages)}")
            print(f"Process start events: {len(started)}")
            print(f"Process completion events: {len(completed)}")
            print(f"Workflow events: {len(workflow_events)}")
            print(f"Process counts: {step_counts}")
            print("WebSocket closed normally.")

    finally:
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=2)


if __name__ == "__main__":
    main()