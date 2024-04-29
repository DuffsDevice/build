import sys
import textwrap

class GlobalIndenter:
    LEVEL = 0
    def __enter__(self):
        class StreamProxy:
            def __init__(self, stream):
                self.stream = stream
            def write(self, stuff):
                self.stream.write(
                    textwrap.indent(stuff, '  ' * GlobalIndenter.LEVEL)
                )
            def flush(self):
                self.stream.flush()
        self.stdout = sys.stdout
        self.stderr = sys.stderr
        sys.stdout = StreamProxy(sys.stdout)
        sys.stderr = StreamProxy(sys.stderr)

    def __exit__(self, exception_type, exception_value, traceback):
        sys.stdout = self.stdout
        sys.stderr = self.stderr

class IndentationGuard:
    def __enter__(self):
        GlobalIndenter.LEVEL += 1

    def __exit__(self, exception_type, exception_value, traceback):
        GlobalIndenter.LEVEL -= 1