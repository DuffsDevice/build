import sys
import regex
from datetime import datetime

class GlobalStreamModification:
    SCOPES = []

class StreamModificationScope:
    def __init__(self, *stream_swappers):
        self.swappers   = stream_swappers
        self.backups    = []
        self.modifiers  = []
    def __enter__(self):
        class StreamProxy:
            def __init__(self, stream, modifiers):
                self.stream     = stream
                self.modifiers  = modifiers
                self.encoding   = getattr(stream, "encoding")
                self.buffer     = None
                self.format     = True
            def write(self, stuff: str):
                lines = stuff.splitlines(True)
                if self.buffer is not None:
                    lines[0] = self.buffer + lines[0]
                    self.buffer = None
                for line in lines:
                    if "\n" in line:
                        if self.format:
                            for modifier in reversed(self.modifiers):
                                line = modifier(line)
                        else:
                            self.format = True
                        self.stream.write(line)
                    else:
                        self.buffer = line

            def flush(self):
                if self.buffer is not None:
                    self.stream.write(self.buffer)
                    self.format = False
                    self.buffer = None
                self.stream.flush()

        GlobalStreamModification.SCOPES.append(self)
        for swapper in self.swappers:
            stream = swapper(None)
            swapper(StreamProxy(stream, self.modifiers))
            self.backups.append(stream)
    def __exit__(self, *_):
        GlobalStreamModification.SCOPES.pop()
        for swapper, backup in zip(self.swappers, self.backups):
            swapper(backup)

def STDOUT(new_value):
    value = sys.stdout
    sys.stdout = new_value or sys.stdout
    return value

def STDERR(new_value):
    value = sys.stderr
    sys.stderr = new_value or sys.stderr
    return value

class StreamModifier:
    def __init__(self, modifier=None):
        self.modifier = modifier
    def __call__(self, line):
        return self.modifier(line) if self.modifier else line
    def __enter__(self):
        assert GlobalStreamModification.SCOPES
        GlobalStreamModification.SCOPES[-1].modifiers.append(self)
    def __exit__(self, *_):
        GlobalStreamModification.SCOPES[-1].modifiers.pop()

class EliminateLogging(StreamModifier):
    def __call__(self, line):
        color_modifier = r"(\s+?:\\x1b\[[0-9;a-z]+m])*"
        line = regex.sub(
            # Remove Color Codes \x1b[...m
            color_modifier
            + r"(INFO|WARNING|DEBUG|ERROR|FATAL|WARN|CRITICAL)"
            + color_modifier
            + r"\s+\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"
            + color_modifier
            , ""
            , line
        )
        return line

class Indent(StreamModifier):
    def __init__(self, prefix = "  "):
        self.prefix = prefix
        StreamModifier.__init__(self)
    def __call__(self, line):
        return self.prefix + line

class DateAndTime(StreamModifier):
    def __init__(self, format_string="%Y-%m-%d %H:%M:%S "):
        self.format_string = format_string
        StreamModifier.__init__(self)
    def __call__(self, line):
        return datetime.now().strftime(self.format_string) + line
