#!/usr/bin/env python3
"""Transport-only benchmark for the Tempera Browser fast path.

This deliberately benchmarks only loopback JSONL transport/gateway overhead.
It does not claim browser DOM, CDP, page-settle, or model latency. A run is
valid only if every response is correct; timing output is informational and
must remain tagged with the host/runner that produced it.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

READ_TIMEOUT_SECONDS = 10.0


def percentile(sorted_values: list[float], p: float) -> float:
    if not sorted_values:
        raise ValueError("cannot compute percentile of empty sample")
    index = round((len(sorted_values) - 1) * p)
    return sorted_values[max(0, min(index, len(sorted_values) - 1))]


def summarize(samples_us: list[float], sent: int, received: int) -> dict[str, Any]:
    ordered = sorted(samples_us)
    return {
        "samples": len(ordered),
        "latencyMicros": {
            "min": round(ordered[0], 3),
            "p50": round(percentile(ordered, 0.50), 3),
            "p90": round(percentile(ordered, 0.90), 3),
            "p95": round(percentile(ordered, 0.95), 3),
            "p99": round(percentile(ordered, 0.99), 3),
            "max": round(ordered[-1], 3),
            "mean": round(sum(ordered) / len(ordered), 3),
        },
        "bytes": {"sent": sent, "received": received},
    }


def free_address() -> tuple[str, int]:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()


class MockDaemon:
    """Concurrent persistent JSONL daemon with canonical-shaped responses."""

    def __init__(self) -> None:
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(128)
        self.listener.settimeout(0.2)
        self.address = self.listener.getsockname()
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self._accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._lock = threading.Lock()
        self.requests = 0
        self.native_fused = 0

    def start(self) -> None:
        self._accept_thread.start()

    def close(self) -> None:
        self._stop.set()
        try:
            self.listener.close()
        except OSError:
            pass
        self._accept_thread.join(timeout=2)
        for thread in self._threads:
            thread.join(timeout=2)

    def _accept_loop(self) -> None:
        while not self._stop.is_set():
            try:
                connection, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            connection.settimeout(READ_TIMEOUT_SECONDS)
            thread = threading.Thread(
                target=self._serve_connection, args=(connection,), daemon=True
            )
            self._threads.append(thread)
            thread.start()

    def _serve_connection(self, connection: socket.socket) -> None:
        with connection:
            reader = connection.makefile("rb")
            writer = connection.makefile("wb")
            try:
                while not self._stop.is_set():
                    line = reader.readline()
                    if not line:
                        return
                    request = json.loads(line)
                    action = request.get("action")
                    with self._lock:
                        self.requests += 1
                        if action == "__tempera_act_observe_v1":
                            self.native_fused += 1
                    if action == "__tempera_act_observe_v1":
                        response = {
                            "success": True,
                            "data": {
                                "schemaVersion": "tempera.browser.native-fusion/v1",
                                "action": {"success": True, "data": {"acted": True}},
                                "observation": {
                                    "success": True,
                                    "data": {
                                        "url": "about:blank",
                                        "title": "fixture",
                                        "nodes": [],
                                    },
                                },
                                "observationDigest": "state64:bench",
                                "nativeFused": True,
                            },
                        }
                    else:
                        response = {
                            "success": True,
                            "data": {
                                "url": "about:blank",
                                "title": "fixture",
                                "nodes": [],
                            },
                        }
                    payload = json.dumps(response, separators=(",", ":")).encode() + b"\n"
                    writer.write(payload)
                    writer.flush()
            except (BrokenPipeError, ConnectionResetError, OSError, json.JSONDecodeError):
                return
            finally:
                reader.close()
                writer.close()


def connect(address: tuple[str, int]) -> socket.socket:
    connection = socket.create_connection(address, timeout=READ_TIMEOUT_SECONDS)
    connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    connection.settimeout(READ_TIMEOUT_SECONDS)
    return connection


def wait_for_gateway(process: subprocess.Popen[bytes], address: tuple[str, int]) -> None:
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"gateway exited before becoming ready: {process.returncode}")
        try:
            with socket.create_connection(address, timeout=0.05):
                return
        except OSError:
            time.sleep(0.01)
    raise RuntimeError("gateway did not become ready")


def round_trip(connection: socket.socket, request: dict[str, Any]) -> tuple[float, int, int, dict[str, Any]]:
    payload = json.dumps(request, separators=(",", ":")).encode() + b"\n"
    started = time.perf_counter_ns()
    connection.sendall(payload)
    reader = connection.makefile("rb")
    try:
        response_line = reader.readline()
    finally:
        # makefile owns a duplicate buffered wrapper, not the underlying socket.
        reader.close()
    elapsed_us = (time.perf_counter_ns() - started) / 1_000.0
    if not response_line:
        raise RuntimeError("peer closed before benchmark response")
    response = json.loads(response_line)
    if response.get("success") is False or response.get("ok") is False:
        raise RuntimeError(f"benchmark request failed: {response}")
    return elapsed_us, len(payload), len(response_line), response


def benchmark_series(
    address: tuple[str, int],
    requests: list[dict[str, Any]],
    warmup: int,
) -> dict[str, Any]:
    samples: list[float] = []
    sent = 0
    received = 0
    with connect(address) as connection:
        for request in requests[:warmup]:
            round_trip(connection, request)
        for request in requests[warmup:]:
            elapsed, request_bytes, response_bytes, _ = round_trip(connection, request)
            samples.append(elapsed)
            sent += request_bytes
            received += response_bytes
    return summarize(samples, sent, received)


def benchmark_concurrent(
    address: tuple[str, int], clients: int, per_client: int
) -> dict[str, Any]:
    samples: list[float] = []
    sent = 0
    received = 0
    lock = threading.Lock()
    barrier = threading.Barrier(clients)
    failures: list[str] = []

    def worker(worker_id: int) -> None:
        nonlocal sent, received
        local_samples: list[float] = []
        local_sent = 0
        local_received = 0
        try:
            with connect(address) as connection:
                barrier.wait(timeout=READ_TIMEOUT_SECONDS)
                for index in range(per_client):
                    request = {
                        "command": {"name": "snapshot"},
                        "sessionId": f"bench-{worker_id}",
                        "targetId": f"target-{worker_id}",
                        "nonce": index,
                    }
                    elapsed, request_bytes, response_bytes, _ = round_trip(
                        connection, request
                    )
                    local_samples.append(elapsed)
                    local_sent += request_bytes
                    local_received += response_bytes
        except Exception as error:  # noqa: BLE001 - benchmark must report worker failures
            with lock:
                failures.append(f"worker {worker_id}: {error}")
            return
        with lock:
            samples.extend(local_samples)
            sent += local_sent
            received += local_received

    threads = [threading.Thread(target=worker, args=(index,)) for index in range(clients)]
    started = time.perf_counter()
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=READ_TIMEOUT_SECONDS * 2)
    wall_seconds = time.perf_counter() - started
    if failures:
        raise RuntimeError("; ".join(failures))
    if any(thread.is_alive() for thread in threads):
        raise RuntimeError("concurrent benchmark worker timed out")
    result = summarize(samples, sent, received)
    result["clients"] = clients
    result["perClient"] = per_client
    result["wallMillis"] = round(wall_seconds * 1_000.0, 3)
    result["throughputOpsPerSecond"] = round(len(samples) / wall_seconds, 1)
    return result


def gateway_stats(address: tuple[str, int]) -> dict[str, Any]:
    with connect(address) as connection:
        _, _, _, response = round_trip(connection, {"command": {"name": "gatewayStats"}})
    return response.get("result", {})


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--gateway", required=True, type=Path)
    parser.add_argument("--iterations", type=int, default=4_000)
    parser.add_argument("--warmup", type=int, default=250)
    parser.add_argument("--concurrent-per-client", type=int, default=250)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.iterations <= args.warmup or args.warmup < 0:
        parser.error("iterations must be greater than warmup")
    if not args.gateway.is_file():
        parser.error(f"gateway binary not found: {args.gateway}")

    daemon = MockDaemon()
    daemon.start()
    gateway_address = free_address()
    process = subprocess.Popen(
        [
            str(args.gateway),
            "--listen",
            f"{gateway_address[0]}:{gateway_address[1]}",
            "--upstream",
            f"{daemon.address[0]}:{daemon.address[1]}",
            "--observe-ttl-ms",
            "8",
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )

    try:
        wait_for_gateway(process, gateway_address)
        total = args.iterations + args.warmup
        direct_requests = [
            {
                "action": "snapshot",
                "id": index,
                "options": {"interactive": True},
            }
            for index in range(total)
        ]
        uncached_requests = [
            {
                "command": {"name": "snapshot"},
                "sessionId": "bench",
                "targetId": "fixture",
                "nonce": index,
            }
            for index in range(total)
        ]
        cached_request = {
            "command": {"name": "snapshot"},
            "sessionId": "bench-cached",
            "targetId": "fixture",
        }
        cached_requests = [cached_request for _ in range(total)]
        fused_requests = [
            {
                "command": {"name": "actObserve"},
                "id": index,
                "arguments": {
                    "actionRequest": {"action": "click", "selector": "@e1"},
                    "observeRequest": {"action": "snapshot"},
                },
            }
            for index in range(total)
        ]

        direct = benchmark_series(daemon.address, direct_requests, args.warmup)
        uncached = benchmark_series(gateway_address, uncached_requests, args.warmup)
        cached = benchmark_series(gateway_address, cached_requests, args.warmup)
        fused = benchmark_series(gateway_address, fused_requests, args.warmup)
        concurrency = {
            str(clients): benchmark_concurrent(
                gateway_address, clients, args.concurrent_per_client
            )
            for clients in (1, 8, 32)
        }
        stats = gateway_stats(gateway_address)

        direct_p50 = direct["latencyMicros"]["p50"]
        result = {
            "schemaVersion": "tempera.browser.fastpath.transport-benchmark/v1",
            "scope": "transport-only; fake canonical daemon; no browser/page/model work",
            "host": {
                "platform": platform.platform(),
                "machine": platform.machine(),
                "python": platform.python_version(),
                "ci": os.environ.get("CI") == "true",
                "runner": os.environ.get("RUNNER_NAME"),
                "runnerOs": os.environ.get("RUNNER_OS"),
                "runnerArch": os.environ.get("RUNNER_ARCH"),
            },
            "config": {
                "iterations": args.iterations,
                "warmup": args.warmup,
                "observeTtlMs": 8,
                "concurrentPerClient": args.concurrent_per_client,
            },
            "results": {
                "directPersistentJsonl": direct,
                "gatewayUncachedObservation": uncached,
                "gatewayCachedObservation": cached,
                "gatewayNativeFusedEnvelope": fused,
                "concurrentUncachedObservations": concurrency,
            },
            "derived": {
                "uncachedGatewayP50AddedMicros": round(
                    uncached["latencyMicros"]["p50"] - direct_p50, 3
                ),
                "fusedGatewayP50AddedMicros": round(
                    fused["latencyMicros"]["p50"] - direct_p50, 3
                ),
            },
            "gatewayStats": stats,
            "mockDaemon": {
                "requests": daemon.requests,
                "nativeFusedRequests": daemon.native_fused,
            },
            "validity": {
                "allRequestsSucceeded": True,
                "universalMultiplierClaim": False,
                "note": "Compare only runs from equivalent hosts/builds; this isolates loopback gateway cost.",
            },
        }
        encoded = json.dumps(result, indent=2, sort_keys=True)
        print(encoded)
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(encoded + "\n", encoding="utf-8")
        return 0
    finally:
        process.terminate()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=3)
        daemon.close()


if __name__ == "__main__":
    raise SystemExit(main())
