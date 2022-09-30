import io
from datetime import datetime


def get_debugger(enable=True, file=None):
    """
    Create debugger to capture incremental output of an optimizer.

    Args:
        enable (bool): If False, return a dummy debugger
        file (str|io.IOBase|None): File to write results to
    Returns:
        Debugger
    """
    if not enable:
        return AbstractDebugger()
    return Debugger(file)


class AbstractDebugger:
    def record(self, rule, expression):
        """
        Record a rule output.

        Args:
            rule (str|function): Optimizer rule that was executed
            expression (sqlglot.Expression): resulting AST
        """

    def initialize(self):
        """Initialize the output"""

    def finalize(self):
        """Finalize the output"""


class Debugger(AbstractDebugger):
    SEP = "-" * 41 + "\n"

    def __init__(self, file=None):
        self.file = file
        self.initialized = False

    def record(self, rule, expression):
        if not self.initialized:
            self.initialize()
        rule_name = rule if isinstance(rule, str) else rule.__name__
        output = f"-- {rule_name}\n{expression.sql(pretty=True)}\n\n"
        self._write(output)

    def initialize(self):
        import sqlglot

        self._write(self.SEP)
        self._write(f"-- Optimize on {datetime.utcnow().isoformat()}\n")
        self._write(f"-- SQLGlot version {sqlglot.__version__}\n")
        self._write(self.SEP)
        self.initialized = True

    def _write(self, output):
        if self.file is None or self.file is True:
            print(output)

        if isinstance(self.file, str):
            with open(self.file, "a") as fp:
                fp.write(output)

        if isinstance(self.file, io.IOBase):
            self.file.write(output)
