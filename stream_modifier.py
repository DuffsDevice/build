import sys
import regex
import threading
import itertools
from datetime import datetime
from build import hierarchical_threads

class Target:
    def __init__(self, expression, **kwargs) -> None:
        self.expression = expression
        self.kwargs     = kwargs
    def set(self, value):
        self.kwargs["__new_value"] = value
        exec(self.expression + " = __new_value", {}, self.kwargs)  # pylint:disable=exec-used
    def get(self):
        return eval(self.expression, {}, self.kwargs)  # pylint:disable=eval-used

class RAIIMultiplexer:
    def __init__(self, *modifiers) -> None:
        self.modifiers = modifiers
    def __or__(self, other):
        if isinstance(other, RAIIMultiplexer):
            return RAIIMultiplexer(*self.modifiers, *other.modifiers)
        assert getattr(other, "__exit__") and getattr(other, "__enter__")
        return RAIIMultiplexer(*self.modifiers, other)
    def __enter__(self):
        for modifier in self.modifiers:
            modifier.__enter__()
    def __exit__(self, *_):
        for modifier in reversed(self.modifiers):
            modifier.__exit__()

class GlobalModification:
    SCOPES      = [] # List of tuples "(thread, scope)"
    SEMAPHORE   = threading.Semaphore()
    @classmethod
    def register(cls, scope):
        with cls.SEMAPHORE:
            cls.SCOPES.append((threading.current_thread(), scope))
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
        relevant_threads = threading.current_thread().hierarchy
        for relevant_thread in relevant_threads:
            for thread, scope in reversed(cls.SCOPES):
                if thread is relevant_thread:
                    return scope
        return None

class Scope:
    def __init__(self, *targets: Target):
        self.targets        = targets
        self.target_backups = []
        self.modifiers      = []  # List of tuples "(thread, modifier)"
        self.write_lock     = threading.Semaphore()
    @property
    def destination(self):
        relevant_threads = threading.current_thread().hierarchy
        for thread, modifier in reversed(self.modifiers):
            if thread in relevant_threads:
                return modifier
        return self
    def register(self, modifier):
        modifier.destination = self.destination
        self.modifiers.append((threading.current_thread(), modifier))
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
        self.target_backups = [
            target.get() for target in self.targets
        ]
        for target_id, target, backup in zip(
            itertools.count()
            , self.targets
            , self.target_backups
        ):
            target.set(TargetStub(backup, self, target_id))
    def __exit__(self, *_):
        GlobalModification.unregister(self)
        for target, backup in zip(self.targets, self.target_backups):
            target.set(backup)
    def write(self, target_id:int, content:str):
        self.write_lock.acquire()
        self.destination(target_id, content)
        self.write_lock.release()
    def __call__(self, target_id:int, content:str):
        target_backup = self.target_backups[target_id]
        if hasattr(target_backup, "write"):
            target_backup.write(content)
            if hasattr(target_backup, "flush"):
                target_backup.flush()
        assert(callable(target_backup))
        target_backup(content)
    def __or__(self, other):
        if isinstance(other, RAIIMultiplexer):
            return RAIIMultiplexer(self, *other.modifiers)
        assert isinstance(other, Scope)
        return RAIIMultiplexer(self, other)

class Redirect:
    def __init__(self, from_target, to_target):
        self.from_target   = from_target
        self.to_target     = to_target
        self.stream         = None
    def __enter__(self):
        self.stream = self.from_target.set(
            TargetRedirector(self.to_target.get())
        )
    def __exit__(self, *_):
        pass

class TargetStub:
    def __init__(self, actual_target, scope: Scope, target_id: int):
        self.actual_target  = actual_target
        self.scope          = scope
        self.target_id      = target_id
    @property
    def encoding(self):
        return self.actual_target.encoding
    def write(self, content:str):
        self.scope.write(self.target_id, content)
    def __call__(self, content:str):
        self.scope.write(self.target_id, content)
    def flush(self):
        pass

class TargetRedirector:
    def __init__(self, destination_target: Target) -> None:
        self.destination_target = destination_target
    def write(self, content:str):
        self.destination_target.get()(content)
    def flush(self):
        pass
    @property
    def encoding(self):
        return self.destination_target.get().encoding

STDOUT = Target("sys.stdout", sys=sys)
STDERR = Target("sys.stderr", sys=sys)

class Modifier:
    def __init__(self, scope=None, scope_optional=False):
        self.scope          = scope
        self.destination    = None
        self.scope_optional = scope_optional
    def modify(self, content:str):
        return content
    def enter(self):
        pass
    def exit(self):
        pass
    def __call__(self, target_id:int, content:str):
        self.destination(target_id, self.modify(content))
    def __enter__(self):
        if self.scope is None:
            self.scope = GlobalModification.active_scope()
        if self.scope is not None:
            assert isinstance(self.scope, Scope)
            self.scope.register(self)
            self.enter()
        else:
            assert self.scope_optional
    def __exit__(self, *_):
        if self.scope:
            self.exit()
            self.scope.unregister(self)
    def __or__(self, other):
        if isinstance(other, RAIIMultiplexer):
            return RAIIMultiplexer(self, *other.modifiers)
        assert isinstance(other, Modifier)
        return RAIIMultiplexer(self, other)

class LineModifier(Modifier):
    def __init__(self, **kwargs):
        Modifier.__init__(self, **kwargs)
    def modify(self, content:str):
        return content
    def __call__(self, target_id:int, content:str):
        lines       = content.splitlines(keepends=True)
        result      = ""
        for line in lines:
            result += self.modify(line)
        self.destination(target_id, result)

class LinePrefixModifier(Modifier):
    def __init__(self, first_line=True, **kwargs):
        self.is_beginning_of_line = first_line
        Modifier.__init__(self, **kwargs)
    def modify(self, content:str):
        return content
    prefix = ""
    def __call__(self, target_id:int, content:str):
        lines   = content.splitlines(keepends=True)
        content = ""
        for line in lines:
            if self.is_beginning_of_line:
                content += self.prefix
            content += line
            self.is_beginning_of_line = True
        self.destination(target_id, content)

class Noop(Modifier):
    def __init__(self):  # pylint: disable=super-init-not-called
        pass
    def __enter__(self):    pass
    def __exit__(self, *_): pass

class PrintChunksModifier(Modifier):
    def __init__(self, lines=10, timeout=3, **kwargs):
        self.lines                  = lines
        self.timeout                = timeout
        self.lines_per_target_id    = {}  # Mapping from target_id to list of lines
        self.semaphore              = threading.Semaphore()
        self.timeout_timer          = None
        Modifier.__init__(self, **kwargs)
    def exit(self):
        with self.semaphore:
            if self.timeout_timer is not None:
                if not self.timeout_timer.finished:
                    self.timeout_timer.cancel()
                self.timeout_timer = None
        self.write_to_destination(True)
    def __call__(self, target_id:int, content: str):
        if not content:
            return
        with self.semaphore:
            lines = content.splitlines(keepends=True)
            for line in lines:
                self.lines_per_target_id[target_id].append(line)
        if len(self.lines_per_target_id[target_id]) >= self.lines:
            self.write_to_destination()
        else:
            self.start_timeout()
    def write_to_destination(self, triggered_by_timeout=False):
        with self.semaphore:
            if self.timeout_timer is not None:
                if not self.timeout_timer.finished:
                    self.timeout_timer.cancel()
                self.timeout_timer = None
            with self.scope.write_lock if triggered_by_timeout else Noop():
                for target_id, lines in self.lines_per_target_id.items():
                    if len(self.lines_per_target_id) >= self.lines or triggered_by_timeout:
                        for line in lines:
                            self.destination(target_id, line)
                        del self.lines_per_target_id[target_id]
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
    def modify(self, line):
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

class Indent(Prefix):
    def __init__(self, **kwargs):
        Prefix.__init__(self, prefix="  ", **kwargs)

class DateAndTime(LinePrefixModifier):
    def __init__(self, format_string="%Y-%m-%d %H:%M:%S", space_after=True, **kwargs):
        self.format_string = format_string + (" " if space_after else "")
        LinePrefixModifier.__init__(self, **kwargs)
    @property
    def prefix(self):
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
    def modify(self, content:str):
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
    def modify(self, content:str):
        if self.remove_escape_codes:
            content = regex.sub(
                r"\x1b\[[;0-9a-z]+m"
                , ""
                , content
            )
        print(content, file=self.stream)
        return content
    def enter(self):
        self.stream = open(self.file, mode=self.mode, encoding=self.file_encoding)
    def exit(self):
        #self.stream.close()
        pass
