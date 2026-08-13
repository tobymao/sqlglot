from __future__ import annotations

from sqlglot.dialects.tsql import TSQL
from sqlglot.generators.synapse import SynapseGenerator
from sqlglot.parsers.synapse import SynapseParser


class Synapse(TSQL):
    Parser = SynapseParser

    Generator = SynapseGenerator
