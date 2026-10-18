#!/usr/bin/env python3
"""End-to-end smoke test for the manager and five local peer processes."""

from pathlib import Path
import socket
import subprocess
import tempfile
import time


ROOT = Path(__file__).resolve().parents[1]
PEER_NAMES = ["alpha", "bravo", "charlie", "delta", "echo"]


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


def main() -> None:
    ports = reserve_udp_ports(1 + len(PEER_NAMES) * 2)
    manager_port = ports[0]
    peer_ports = {
        name: (ports[1 + index * 2], ports[2 + index * 2])
        for index, name in enumerate(PEER_NAMES)
    }

    with tempfile.TemporaryDirectory(prefix="dht-integration-") as temp_dir:
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

            send("alpha", "setup-dht alpha 3 1950")
            time.sleep(2.0)

            for name in PEER_NAMES[1:]:
                send(name, f"query-dht {name}")
            time.sleep(0.5)
            for name in PEER_NAMES[1:]:
                send(name, "find-event 10096222")
            time.sleep(1.0)

            send("alpha", "leave-dht alpha")
            time.sleep(2.0)
            send("alpha", "query-dht alpha")
            time.sleep(0.5)
            send("alpha", "find-event 10096222")
            time.sleep(1.0)

            send("alpha", "join-dht alpha")
            time.sleep(2.0)

            for name in PEER_NAMES[1:]:
                send(name, f"query-dht {name}")
            time.sleep(0.5)
            for name in PEER_NAMES[1:]:
                send(name, "find-event 99999999")
            time.sleep(1.0)

            for name in PEER_NAMES:
                send(name, f"teardown-dht {name}")
            time.sleep(2.0)

            for name in PEER_NAMES:
                send(name, f"deregister {name}")
            for process in processes.values():
                process.wait(timeout=5)

            combined_logs = ""
            for log in log_files[1:]:
                log.flush()
                log.seek(0)
                combined_logs += log.read()

            required_messages = [
                "Storm event 10096222 found:",
                "alpha is now Free.",
                "alpha joined the rebuilt DHT.",
                "Storm event 99999999 not found in the DHT.",
                "DHT teardown completed.",
                "Deregistered successfully; peer is exiting.",
            ]
            missing = [
                message for message in required_messages if message not in combined_logs
            ]
            if missing:
                raise AssertionError(
                    "Missing expected end-to-end output: " + ", ".join(missing)
                )
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
                    process.wait(timeout=2)
            if manager.poll() is None:
                manager.terminate()
                manager.wait(timeout=2)
            for log in log_files:
                log.close()

    print("End-to-end DHT integration test passed.")


if __name__ == "__main__":
    main()
