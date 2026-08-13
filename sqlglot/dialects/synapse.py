from __future__ import annotations

from sqlglot.dialects.tsql import TSQL
from sqlglot.generators.synapse import SynapseGenerator
from sqlglot.parsers.synapse import SynapseParser


class Synapse(TSQL):
    """Microsoft Azure Synapse Analytics SQL dialect."""

    Parser = SynapseParser
    Generator = SynapseGenerator