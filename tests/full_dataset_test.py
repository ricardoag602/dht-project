#!/usr/bin/env python3
"""Verify reliable setup, rebuild, and lookup with a 1996 dataset."""

from pathlib import Path
import socket
import subprocess
import tempfile
import time


ROOT = Path(__file__).resolve().parents[1]
PEER_NAMES = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot"]


def reserve_udp_ports(count: int) -> list[int]:
    sockets: list[socket.socket] = []
    try:
        for _ in range(count):
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.bind(("127.0.0.1", 0))
            sockets.append(sock)
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


def manager_request(manager_port: int, message: str) -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.settimeout(1)
        sock.sendto(message.encode(), ("127.0.0.1", manager_port))
        return sock.recvfrom(65535)[0].decode()


def main() -> None:
    ports = reserve_udp_ports(1 + len(PEER_NAMES) * 2)
    manager_port = ports[0]
    peer_ports = {
        name: (ports[1 + index * 2], ports[2 + index * 2])
        for index, name in enumerate(PEER_NAMES)
    }

    with tempfile.TemporaryDirectory(prefix="dht-full-dataset-") as temp_dir:
        log_dir = Path(temp_dir)
        log_files = []
        processes: dict[str, subprocess.Popen[str]] = {}
        manager_log = (log_dir / "manager.log").open("w+")
        log_files.append(manager_log)
        manager = subprocess.Popen(
            [str(ROOT / "manager"), str(manager_port)],
            cwd=ROOT,
            stdout=manager_log,
            stderr=subprocess.STDOUT,
            text=True,
        )
        query_peer = PEER_NAMES[-1]

        try:
            time.sleep(0.3)
            if manager.poll() is not None:
                raise RuntimeError("Manager failed to start")

            for name in PEER_NAMES:
                log = (log_dir / f"{name}.log").open("w+")
                log_files.append(log)
                processes[name] = subprocess.Popen(
                    [str(ROOT / "peer"), "127.0.0.1", str(manager_port)],
                    cwd=ROOT,
                    stdin=subprocess.PIPE,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    text=True,
                )

            def send(name: str, command: str) -> None:
                process = processes[name]
                if process.stdin is None or process.poll() is not None:
                    raise RuntimeError(f"Peer {name} is not running")
                process.stdin.write(command + "\n")
                process.stdin.flush()

            for name in PEER_NAMES:
                manager_peer_port, p2p_port = peer_ports[name]
                send(
                    name,
                    f"register {name} 127.0.0.1 "
                    f"{manager_peer_port} {p2p_port}",
                )
            time.sleep(0.8)

            send("alpha", "setup-dht alpha 5 1996")
            deadline = time.monotonic() + 120
            query_peer = ""
            while time.monotonic() < deadline:
                if any(process.poll() is not None for process in processes.values()):
                    raise RuntimeError("A peer exited during full-dataset setup")
                for candidate in PEER_NAMES:
                    response = manager_request(
                        manager_port, f"query-dht|{candidate}"
                    )
                    if response.startswith("SUCCESS|"):
                        query_peer = candidate
                        break
                if query_peer:
                    break
                time.sleep(0.25)
            if not query_peer:
                raise TimeoutError("Full 1996 dataset did not finish within 120 seconds")

            send("alpha", "leave-dht alpha")
            deadline = time.monotonic() + 120
            while time.monotonic() < deadline:
                response = manager_request(manager_port, "query-dht|alpha")
                if response.startswith("SUCCESS|"):
                    break
                time.sleep(0.25)
            if not response.startswith("SUCCESS|"):
                raise TimeoutError("Full 1996 rebuild did not finish within 120 seconds")

            query_peer = "alpha"
            send(query_peer, f"query-dht {query_peer}")
            time.sleep(0.2)
            send(query_peer, "find-event 5539287")
            time.sleep(2)
            send(query_peer, f"deregister {query_peer}")
            processes[query_peer].wait(timeout=5)
        except Exception:
            for log in log_files:
                log.flush()
                log.seek(0)
                print(f"\n--- {Path(log.name).name} ---\n{log.read()}")
            raise
        finally:
            for process in processes.values():
                if process.poll() is None:
                    process.terminate()
                    process.wait(timeout=3)
            if manager.poll() is None:
                manager.terminate()
                manager.wait(timeout=3)

            query_log = log_files[1 + PEER_NAMES.index(query_peer)]
            query_log.flush()
            query_log.seek(0)
            query_output = query_log.read()
            for log in log_files:
                log.close()

        if "alpha is now Free." not in query_output:
            raise AssertionError("Full 1996 leave/rebuild did not complete")
        if "Storm event 5539287 found:" not in query_output:
            raise AssertionError("Required 1996 event was not found after setup")

    print("1996 dataset setup, rebuild, and lookup test passed.")


if __name__ == "__main__":
    main()
