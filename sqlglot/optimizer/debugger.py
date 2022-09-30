import io


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


class Debugger(AbstractDebugger):
    def __init__(self, file=None):
        self.file = file
        self.output = []

    def record(self, rule, expression):
        rule_name = rule if isinstance(rule, str) else rule.__name__
        output = f"-- {rule_name}\n{expression.sql(pretty=True)}\n\n"
        self._write(output)
        self.output.append(f"-- {rule_name}\n{expression.sql(pretty=True)}\n\n")

    def _write(self, output):
        if self.file is None or self.file is True:
            print(output)

        if isinstance(self.file, str):
            with open(self.file, "w+") as fp:
                fp.write("".join(self.output))

        if isinstance(self.file, io.IOBase):
            self.file.write(output)
