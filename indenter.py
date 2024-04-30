import sys
import textwrap

class GlobalIndenter:
    LEVEL = 0
    def __enter__(self):
        class StreamProxy:
            def __init__(self, stream):
                self.stream = stream
                self.encoding = getattr(stream, "encoding")
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
    def __init__(self, prefix=None, postfix=None) -> None:
        self.prefix = prefix
        self.postfix = postfix

    def __enter__(self):
        if self.prefix is not None:
            print(self.prefix)
        GlobalIndenter.LEVEL += 1

    def __exit__(self, exception_type, exception_value, traceback):
        GlobalIndenter.LEVEL -= 1
        if self.postfix is not None:
            print(self.postfix)