"""Starts and stops the bundled engines with Docker Compose."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

COMPOSE_FILE = Path(__file__).resolve().parent / "docker-compose.yml"
PROJECT = "sqlglot-conformance"


class DockerError(Exception):
    pass


def compose(*args: str) -> None:
    if shutil.which("docker") is None:
        raise DockerError(
            "Docker is required to run the bundled engines: https://docs.docker.com/get-docker/. "
            "Alternatively point the harness at existing servers, see conformance/README.md."
        )

    command = ["docker", "compose", "--file", str(COMPOSE_FILE), "--project-name", PROJECT, *args]
    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError as e:
        raise DockerError(f"'{' '.join(command)}' failed with exit code {e.returncode}") from e


def up(services: list[str]) -> None:
    compose("up", "--detach", "--wait", *services)


def down() -> None:
    compose("down", "--volumes", "--remove-orphans")


def ps() -> None:
    compose("ps")
