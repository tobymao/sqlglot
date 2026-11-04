from __future__ import annotations

from conformance.engines.base import Engine, EngineError, EngineUnavailable
from conformance.engines.postgres import Postgres
from conformance.engines.spark import Spark

ENGINES: dict[str, type[Engine]] = {engine.name: engine for engine in (Postgres, Spark)}

__all__ = ["ENGINES", "Engine", "EngineError", "EngineUnavailable"]
