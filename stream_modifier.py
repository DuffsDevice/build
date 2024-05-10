import sys
import regex
import threading
from datetime import datetime

def get_relevant_threads(thread):
    result = [thread]
    if thread != threading.main_thread():
        parent = getattr(thread, "parent", threading.main_thread())
        result.extend(get_relevant_threads(parent))
    return result

def this_thread():
    return threading.current_thread()

class GlobalModification:
    SCOPES      = [] # List of tuples "(thread, scope)"
    SEMAPHORE   = threading.Semaphore()
    @classmethod
    def register(cls, scope):
        with cls.SEMAPHORE:
            cls.SCOPES.append((this_thread(), scope))
    @classmethod
    def unregister(cls, scope):
        with cls.SEMAPHORE:
            cls.SCOPES = [
                (t, s)
                for t, s in cls.SCOPES
                if s is not scope
            ]
    @classmethod
    def active_scope(cls):
        relevant_threads = get_relevant_threads(this_thread())
        for relevant_thread in relevant_threads:
            for thread, scope in reversed(cls.SCOPES):
                if thread is relevant_thread:
                    return scope
        return None

class Scope:
    def __init__(self, stream_swapper):
        self.swapper    = stream_swapper
        self.stream     = None
        self.modifiers  = []  # List of tuples "(thread, modifier)"
        self.write_lock = threading.Semaphore()
    @property
    def destination(self):
        relevant_threads = get_relevant_threads(this_thread())
        for thread, modifier in reversed(self.modifiers):
            if thread in relevant_threads:
                return modifier
        return self.stream
    def register(self, modifier):
        modifier.destination = self.destination
        self.modifiers.append((this_thread(), modifier))
    def unregister(self, modifier):
        old_destination = modifier.destination = None
        modifier.destination = None
        self.modifiers = [
            (t, m)
            for t, m in self.modifiers
            if m is not modifier
        ]
        for _, modifier in self.modifiers:
            if modifier.destination is modifier:
                modifier.destination = old_destination
    def __enter__(self):
        GlobalModification.register(self)
        self.stream = self.swapper(None)
        self.swapper(StreamStub(self.stream, self))
    def __exit__(self, *_):
        GlobalModification.unregister(self)
        self.swapper(self.stream).flush()
    def write(self, content):
        self.write_lock.acquire()
        self.destination.write(content)
        self.write_lock.release()
    def flush(self):
        self.destination.flush()

class Redirect:
    def __init__(self, from_swapper, to_swapper):
        self.from_swapper   = from_swapper
        self.to_swapper     = to_swapper
        self.stream         = None
    def __enter__(self):
        self.stream = self.from_swapper(StreamRedirector(self.to_swapper))
    def __exit__(self, *_):
        self.from_swapper(self.stream).flush()

class StreamStub:
    def __init__(self, stream, scope: Scope):
        self.stream = stream
        self.scope  = scope
    @property
    def encoding(self):
        return self.stream.encoding
    def write(self, content):
        self.scope.write(content)
    def flush(self):
        self.scope.flush()

class StreamRedirector:
    def __init__(self, wrapper) -> None:
        self.wrapper = wrapper
    def write(self, content):
        self.wrapper().write(content)
    def flush(self, ):
        self.wrapper().flush()
    @property
    def encoding(self):
        return self.wrapper().encoding

def STDOUT(new_value=None):
    value = sys.stdout
    sys.stdout = new_value or sys.stdout
    return value

def STDERR(new_value=None):
    value = sys.stderr
    sys.stderr = new_value or sys.stderr
    return value

class Modifier:
    def __init__(self, scope=None):
        self.scope          = scope
        self.destination    = None
    def __call__(self, content):
        return content
    def enter(self):
        pass
    def exit(self):
        pass
    def write(self, content):
        self.destination.write(self(content))
    @property
    def encoding(self):
        return self.destination.encoding
    def flush(self):
        self.destination.flush()
    def __enter__(self):
        if self.scope is None:
            self.scope = GlobalModification.active_scope()
        assert isinstance(self.scope, Scope)
        self.scope.register(self)
        self.enter()
    def __exit__(self, *_):
        self.exit()
        self.scope.unregister(self)
    def __or__(self, other):
        if isinstance(other, MultiModifier):
            return MultiModifier(self, *other.modifiers)
        assert isinstance(other, Modifier)
        return MultiModifier(self, other)

class MultiModifier:
    def __init__(self, *modifiers) -> None:
        self.modifiers = modifiers
    def __or__(self, other):
        if isinstance(other, MultiModifier):
            return MultiModifier(*self.modifiers, *other.modifiers)
        assert isinstance(other, Modifier)
        return MultiModifier(*self.modifiers, other)
    def __enter__(self):
        for modifier in self.modifiers:
            modifier.__enter__()
    def __exit__(self, *_):
        for modifier in reversed(self.modifiers):
            modifier.__exit__()

class LineModifier(Modifier):
    def __init__(self, **kwargs):
        self.buffer         = ""
        Modifier.__init__(self, **kwargs)
    def __call__(self, content):
        return content
    def write(self, content):
        lines       = content.splitlines(keepends=True)
        if not lines:
            return
        lines[0]    = self.buffer + lines[0]
        self.buffer = ""
        result      = ""
        for line in lines:
            if "\n" in line:
                result += self(line)
            else:
                self.buffer = line
        self.destination.write(result)
    def exit(self):
        self.destination.write(self(self.buffer))

class LinePrefixModifier(Modifier):
    def __init__(self, **kwargs):
        self.is_beginning_of_line = True
        Modifier.__init__(self, **kwargs)
    def __call__(self, content):
        return content
    def write(self, content):
        lines   = content.splitlines(keepends=True)
        content = ""
        for line in lines:
            if self.is_beginning_of_line:
                content += self()
            content += line
            self.is_beginning_of_line = "\n" in line
        self.destination.write(content)

class Noop(Modifier):
    def __init__(self):  # pylint: disable=super-init-not-called
        pass
    def __enter__(self):    pass
    def __exit__(self, *_): pass

class PrintChunksModifier(Modifier):
    def __init__(self, lines=10, timeout=3, only_complete_lines=False, **kwargs):
        self.lines                  = lines
        self.timeout                = timeout
        self.only_complete_lines    = only_complete_lines
        self.completed_lines        = []
        self.buffer                 = ""
        self.semaphore              = threading.Semaphore()
        self.timeout_timer          = None
        Modifier.__init__(self, **kwargs)
    def exit(self):
        with self.semaphore:
            if self.timeout_timer is not None:
                if not self.timeout_timer.finished:
                    self.timeout_timer.cancel()
                self.timeout_timer = None
        self.write_to_destination(write_all=True)
    def write(self, content: str):
        if not content:
            return
        with self.semaphore:
            lines       = content.splitlines(keepends=True)
            lines[0]    = self.buffer + lines[0]
            self.buffer = ""
            for line in lines:
                if "\n" in line:
                    self.completed_lines.append(line)
                else:
                    self.buffer = line
        if len(self.completed_lines) >= self.lines:
            self.write_to_destination()
        else:
            self.start_timeout()
    def write_to_destination(self, triggered_by_timeout=False, write_all=None):
        with self.semaphore:
            if self.timeout_timer is not None:
                if not self.timeout_timer.finished:
                    self.timeout_timer.cancel()
                self.timeout_timer = None
            content = "".join(self.completed_lines)
            if write_all is None:
                write_all = triggered_by_timeout and not self.only_complete_lines
            if write_all:
                content     += self.buffer
                self.buffer = ""
            with self.scope.write_lock if triggered_by_timeout else Noop():
                self.destination.write("".join(self.completed_lines))
                self.completed_lines = []
    def start_timeout(self):
        if self.timeout and self.timeout_timer is None:
            self.timeout_timer = threading.Timer(
                self.timeout
                , self.write_to_destination
                , kwargs=dict(triggered_by_timeout=True)
            )
            self.timeout_timer.daemon = True
            self.timeout_timer.start()

class EliminateLogging(LineModifier):
    def __init__(self, keep_level=True, **kwargs):
        self.keep_level = keep_level
        LineModifier.__init__(self, **kwargs)
    def __call__(self, line):
        # Remove Color Codes \x1b[...m
        return regex.sub(
            "("
            r"(?:\s*\x1b\[[;0-9a-z]+m)*"
            r"(INFO|WARNING|DEBUG|ERROR|FATAL|WARN|CRITICAL)\s+"
            ")"
            r"(?:\s*\x1b\[[;0-9a-z]+m)*"
            r"\s*\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}"
            , r"\1" if self.keep_level else ""
            , line
            , 1
        )

class Prefix(LinePrefixModifier):
    def __init__(self, prefix, **kwargs):
        self.prefix = prefix
        LinePrefixModifier.__init__(self, **kwargs)
    def __call__(self):
        return self.prefix

class Indent(Prefix):
    def __init__(self, **kwargs):
        Prefix.__init__(self, prefix="  ", **kwargs)

class DateAndTime(LinePrefixModifier):
    def __init__(self, format_string="%Y-%m-%d %H:%M:%S", space_after=True, **kwargs):
        self.format_string = format_string + (" " if space_after else "")
        LinePrefixModifier.__init__(self, **kwargs)
    def __call__(self):
        return datetime.now().strftime(self.format_string)

class ConsoleStyle(Modifier):
    NORMAL      = 0
    BOLD        = (1, 21)
    DIM         = (2, 22)
    UNDERLINED  = (4, 24)
    BLINK       = (5, 25)
    INVERTED    = (7, 27)
    HIDDEN      = (8, 28)
    class Foreground:
        DEFAULT = 39
        BLACK = (30, 39)
        RED = (31, 39)
        GREEN = (32, 39)
        YELLOW = (33, 39)
        BLUE = (34, 39)
        MAGENTA = (35, 39)
        CYAN = (36, 39)
        LIGHT_GRAY = (37, 39)
        DARK_GRAY = (90, 39)
        LIGHT_RED = (91, 39)
        LIGHT_GREEN = (92, 39)
        LIGHT_YELLOW = (93, 39)
        LIGHT_BLUE = (94, 39)
        LIGHT_MAGENTA = (95, 39)
        LIGHT_CYAN = (96, 39)
        WHITE = (97, 39)
    class Background:
        DEFAULT = 49
        BLACK = (40, 49)
        RED = (41, 49)
        GREEN = (42, 49)
        YELLOW = (43, 49)
        BLUE = (44, 49)
        MAGENTA = (45, 49)
        CYAN = (46, 49)
        LIGHT_GRAY = (47, 49)
        DARK_GRAY = (100, 49)
        LIGHT_RED = (101, 49)
        LIGHT_GREEN = (102, 49)
        LIGHT_YELLOW = (103, 49)
        LIGHT_BLUE = (104, 49)
        LIGHT_MAGENTA = (105, 49)
        LIGHT_CYAN = (106, 49)
        WHITE = (107, 49)
    def __init__(self, *styles, **kwargs):
        self.styles = styles
        Modifier.__init__(self, **kwargs)
    def __call__(self, content):
        if self.styles:
            enablers = [
                str(style[0]) if isinstance(style, tuple) else str(style)
                for style in self.styles
            ]
            content = "\x1b[" + ";".join(enablers) + "m" + content
            disablers = [
                str(style[1])
                for style in self.styles
                if isinstance(style, tuple)
            ]
            if disablers:
                content += "\x1b[" + ";".join(disablers) + "m"
        return content

class SaveToFile(Modifier):
    def __init__(
        self
        , file
        , mode="wt"
        , file_encoding="iso-8859-1"
        , remove_escape_codes=True
        , **kwargs
    ):
        self.file = file
        self.mode = mode
        self.file_encoding = file_encoding
        self.remove_escape_codes = remove_escape_codes
        self.stream = None
        Modifier.__init__(self, **kwargs)
    def __call__(self, content):
        if self.remove_escape_codes:
            content = regex.sub(
                r"\x1b\[[;0-9a-z]+m"
                , ""
                , content
            )
        self.stream.write(content)
        return content
    def enter(self):
        self.stream = open(self.file, mode=self.mode, encoding=self.encoding)
    def exit(self):
        #self.stream.close()
        pass