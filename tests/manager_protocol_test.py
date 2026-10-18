#!/usr/bin/env python3
"""Black-box coverage for the manager's complete state machine."""

from pathlib import Path
import socket
import subprocess
import time


ROOT = Path(__file__).resolve().parents[1]


def available_udp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def main() -> None:
    manager_port = available_udp_port()
    manager = subprocess.Popen(
        [str(ROOT / "manager"), str(manager_port)],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    client.settimeout(5)

    def request(*fields: object) -> list[str]:
        message = "|".join(str(field) for field in fields).encode()
        client.sendto(message, ("127.0.0.1", manager_port))
        response, _ = client.recvfrom(65536)
        return response.decode().split("|")

    try:
        time.sleep(0.5)
        if manager.poll() is not None:
            output, _ = manager.communicate()
            raise RuntimeError(f"Manager exited during startup:\n{output}")
        names = ["alpha", "bravo", "charlie", "delta"]
        for index, name in enumerate(names):
            manager_peer_port = 31000 + index * 2
            p2p_port = manager_peer_port + 1
            assert request(
                "register",
                name,
                "127.0.0.1",
                manager_peer_port,
                p2p_port,
            ) == ["SUCCESS"]

        setup = request("setup-dht", "alpha", 3, 1950)
        assert setup[0] == "SUCCESS" and len(setup) == 10
        members = [setup[index] for index in range(1, len(setup), 3)]
        assert members[0] == "alpha" and len(set(members)) == 3
        free_peer = next(name for name in names if name not in members)

        assert request("query-dht", free_peer) == ["FAILURE"]
        assert request("dht-complete", "alpha") == ["SUCCESS"]
        query = request("query-dht", free_peer)
        assert query[0] == "SUCCESS" and query[1] in members

        leave = request("leave-dht", "alpha")
        assert leave == ["SUCCESS", "1950"]
        assert request("query-dht", free_peer) == ["FAILURE"]
        new_leader = members[1]
        assert request("dht-rebuilt", "alpha", new_leader) == ["SUCCESS"]

        join = request("join-dht", "alpha")
        assert join[0:3] == ["SUCCESS", "1950", "2"]
        assert request("dht-rebuilt", "alpha", new_leader) == ["SUCCESS"]

        assert request("teardown-dht", new_leader) == ["SUCCESS"]
        assert request("register", "echo", "127.0.0.1", 31998, 31999) == [
            "FAILURE"
        ]
        assert request("teardown-complete", new_leader) == ["SUCCESS"]

        for name in names:
            assert request("deregister", name) == ["SUCCESS"]
    finally:
        client.close()
        manager.terminate()
        manager.wait(timeout=2)

    print("Manager protocol tests passed.")


if __name__ == "__main__":
    main()
