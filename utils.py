import os
import shutil
import logging
import threading
import functools
import ruamel.yaml
from ruamel.yaml import comments
from ruamel.yaml.scalarstring import (
    SingleQuotedScalarString
    , DoubleQuotedScalarString
)
from ruamel.yaml import parser
import xml.etree.ElementTree as xml

from build import stream_modifier
from build import shell

# Prepare yaml
yaml = ruamel.yaml.YAML()
yaml.preserve_quotes=True

def print_using_scope_handler(scope, levels, message, *args, **kwargs):
    if args:
        message = (message % args)
    if kwargs:
        message.format(**kwargs)
    if not scope:
        print(message)
    for level in ensure_sequence(levels):
        printer = scope.get(f"_print.{level}")
        if printer is not None:
            printer(message)
            return
    scope.get("_print", print)(message)

def print_debug(scope, message="", *args, **kwargs):
    print_using_scope_handler(scope, ["debug", "info"], message, *args, **kwargs)

def print_info(scope, message="", *args, **kwargs):
    print_using_scope_handler(scope, "info", message, *args, **kwargs)

def print_warning(scope, message="", *args, **kwargs):
    print_using_scope_handler(scope, ["warning", "info"], message, *args, **kwargs)

def print_error(scope, message="", *args, **kwargs):
    print_using_scope_handler(scope, ["error", "warning", "info"], message, *args, **kwargs)

def print_critical(scope, message="", *args, **kwargs):
    print_using_scope_handler(scope, ["critical", "error", "warning", "info"], message, *args, **kwargs)

def print_fatal(scope, message="", *args, **kwargs):
    print_using_scope_handler(scope, ["fatal", "critical", "error", "warning", "info"], message, *args, **kwargs)

def is_tuple(value):
    return isinstance(value, tuple)

def is_sequence(value):
    return (
        isinstance(value, list)
        or isinstance(value, set)
        or isinstance(value, tuple)
        or isinstance(value, comments.CommentedKeySeq)
    )

def is_mapping(value):
    return isinstance(value, dict) or isinstance(value, comments.CommentedKeyMap)

def is_printable(value, fail_fast_length=7):
    if value is None:
        return True
    elif any(isinstance(value, type) for type in [str, bool, int, float, complex]):
        return True
    elif is_sequence(value):
        if len(value) > fail_fast_length:
            return False
        for v in value:
            if not is_printable(v, fail_fast_length/len(value) if fail_fast_length is not None else None):
                return False
        return True
    elif is_mapping(value):
        if fail_fast_length is not None and len(value) > fail_fast_length:
            return False
        for k, v in value.items():
            if not is_printable(k, fail_fast_length/len(value) if fail_fast_length is not None else None):
                return False
            if not is_printable(v, fail_fast_length/len(value) if fail_fast_length is not None else None):
                return False
        return True
    return hasattr(value, '__dict__') and '__str__' in value.__dict__

def intelligent_repr(value, max_length=1000):
    print_normally = is_printable(value, max_length/4)
    if print_normally:
        result = repr(value)
        if len(result) <= max_length:
            return result
    return f"{type(value).__module__}.{type(value).__qualname__}@{id(value)}"

def ensure_sequence(value):
    return value if is_sequence(value) else [value]

class Comment:
    def __init__(self, value) -> None:
        self.value = value
    def do_print(self, scope, value):
        print_info(scope, f"----------  {value}  ----------")
    def print(self, scope):
        self.do_print(
            scope
            , execute(self.value, scope, str_is_python=False)
        )
class DebugComment(Comment):
    def do_print(self, scope, value):
        print_debug(scope, f"({value})")
class ImportantComment(Comment):
    def do_print(self, scope, value):
        line_length = max([len(line) for line in value.splitlines()]) + 4
        print_info(scope, "")
        print_info(scope, "!#" * line_length)
        print_info(scope, "  " + value + "  ")
        print_info(scope, "!" * line_length)
        print_info(scope, "")

def print_comment(scope, comment, comment_types = [DebugComment, Comment, ImportantComment]):
    for line in comment.splitlines():
        if line.strip():
            line = line.lstrip(" \t#")
            importance = 1
            while line and line[0] == "!":
                importance += 1
                line = line[1:]
            line = line.lstrip()
            if importance > len(comment_types):
                importance = len(comment_types)
            comment_types[importance-1](line).print(scope)

def print_pre_comments(scope, value):
    value = ensure_sequence(value)
    if len(value) in [1, 2]:
        for comment in ensure_sequence(value[0]):
            if comment is not None:
                print_comment(scope, comment.value)
    elif len(value) == 4:
        for comment_id in [0, 1]:
            for comment in ensure_sequence(value[comment_id]):
                if comment is not None:
                    print_comment(scope, comment.value)

def print_post_comments(scope, value):
    value = ensure_sequence(value)
    if len(value) == 2:
        for comment in ensure_sequence(value[1]):
            if comment is not None:
                print_comment(scope, comment.value)
    elif len(value) == 4:
        for comment_id in [2, 3]:
            for comment in ensure_sequence(value[comment_id]):
                if comment is not None:
                    print_comment(scope, comment.value)

class Semaphore:
    def __init__(self, count=1):
        self._count = 0
        self._max_count = count
        self._lock = threading.Condition()
    @property
    def count(self):
        with self._lock:
            return self._count
    def acquire(self, count=1):
        with self._lock:
            while self._max_count - self._count < count:
                self._lock.wait()
            self._count += count
    def release(self, count=1):
        with self._lock:
            self._count -= count
            self._lock.notify()

def construct(cls, constructor, node):
    result              = None
    if isinstance(node, ruamel.yaml.ScalarNode):
        if node.style == "'":
            result = cls(SingleQuotedScalarString(node.value, anchor=node.anchor))
        elif node.style == '"':
            result = cls(DoubleQuotedScalarString(node.value, anchor=node.anchor))
        else:
            result = cls(node.value)
    elif isinstance(node, ruamel.yaml.SequenceNode):
        data = constructor.construct_sequence(node)
        try:
            result = cls(*data)
        except TypeError as e:
            raise RuntimeError(f"Cannot construct class '{cls.__name__}'") from e
    else:
        data = ruamel.yaml.CommentedMap()
        constructor.construct_mapping(node, maptyp=data, deep=True)
        data = {
            variable.replace("-", "_"): value
            for variable, value in data.items()
        }
        try:
            result = cls(**data)
        except TypeError as e:
            raise RuntimeError(f"Cannot construct class '{cls.__name__}'") from e

    return result

# Hashable dictionary class
class HashableDict(dict):
    def __hash__(self) -> int:
        return id(self)

# Custom Exception class
class BuildError(RuntimeError):
    pass

# Custom Exception class used when calling !raise without parameter
class MissingExceptionInRaise(BuildError):
    pass

# Base Class
class Executable:
    yaml_tag = ""
    @classmethod
    def from_yaml(cls, constructor, node):
        return construct(cls, constructor, node)
    @classmethod
    def to_yaml(cls, representer, self):
        assert isinstance(cls.yaml_tag, str)
        attributes = vars(self)
        if len(attributes) == 1:
            return representer.represent_scalar(cls.yaml_tag, next(iter(attributes.values())))
        return representer.represent_mapping(
            cls.yaml_tag
            , {
                attribute.strip("_").replace("_", "-"): value
                for attribute, value in vars(self).items()
            }
        )
    def __call__(self, _):
        raise BuildError(f"Executable Class '{type(self)}' has no implementation!")
    @property
    def type(self):
        return type(self).yaml_tag[1:]
    @property
    def detail(self):
        return None
    @property
    def print_indented(self):
        return True
    @property
    def description(self):
        result = self.type
        if self.detail:
            result += f" [{self.detail}]"
        return result
    @property
    def print_description(self):
        return True
    def __hash__(self):
        return id(self)

@yaml.register_class
class Path(Executable):
    yaml_tag = "!path"
    def __init__(
        self
        , *path_parts
        , path=None

        # Modifiers
        , directory=None
        , subdirectory=None
        , filename=None
        , name=None
        , name_suffix=None
        , name_prefix=None
        , extension=None
        , extenion_suffix=None
        , extenion_prefix=None
    ):
        self.path_parts = path_parts or [path]
        self.directory = directory
        self.subdirectory = subdirectory
        self.filename = filename
        self.name = name
        self.name_suffix = name_suffix
        self.name_prefix = name_prefix
        self.extension = extension
        self.extension_suffix = extenion_suffix
        self.extension_prefix = extenion_prefix
    def normalize_path(self, path, scope):
        if is_sequence(path):
            result = [execute(part, scope) for part in self.path_parts]
            result = [
                part.replace('"', '')
                if isinstance(part, str)
                else "".join(part)
                if is_sequence(part)
                else part
                for part in result
                if part is not None
            ]
            path = os.path.join(*result) if result else ""
        path = os.path.normpath(path)
        return path
    def __call__(self, scope, **_):
        result = self.normalize_path(self.path_parts, scope)

        # Modify?
        new_directory = execute(self.directory, scope)
        subdirectory = execute(self.subdirectory, scope)
        new_filename = execute(self.filename, scope)
        new_name = execute(self.name, scope)
        name_prefix = execute(self.name_prefix, scope)
        name_suffix = execute(self.name_suffix, scope)
        new_extension = execute(self.extension, scope)
        extension_prefix = execute(self.extension_prefix, scope)
        extension_suffix = execute(self.extension_suffix, scope)
        if (
            new_directory is not None
            or subdirectory is not None
            or new_filename is not None
            or new_name is not None
            or name_prefix is not None
            or name_suffix is not None
            or new_extension is not None
            or extension_prefix is not None
            or extension_suffix is not None
        ):
            directory = os.path.dirname(result)
            filename = os.path.basename(result)
            if new_directory is not None:  # Modify directory?
                directory = self.normalize_path(new_directory, scope)
            if subdirectory is not None:  # Modify directory?
                directory = os.path.join(directory, self.normalize_path(subdirectory, scope))
            if new_filename is not None:  # Modify filename?
                filename = os.path.normpath(new_filename)
                assert os.path.isabs(filename) is False
                directory = os.path.join(directory, os.path.dirname(filename))
                filename = os.path.basename(filename)
            name, extension = os.path.splitext(filename)
            if new_extension is not None:  # Modify extension?
                extension = new_extension
                if extension and extension[0] != ".":
                    extension = "." + extension
            if extension_prefix is not None:  # Add extension prefix?
                if not extension:
                    raise BuildError("Cannot apply prefix to filename witout extension")
                extension = "." + extension_prefix + extension[1:]
            if extension_suffix is not None:  # Add extension suffix?
                if not extension:
                    raise BuildError("Cannot apply suffix to filename witout extension")
                extension = extension + extension_suffix
            if new_name is not None:  # Modify name?
                name = new_name
            if name_prefix is not None:  # Add name prefix?
                name = name_prefix + name
            if name_suffix is not None:  # Modify name?
                name = name + name_suffix
            result = os.path.join(directory, name + extension)  # Put the path back together

        return result
    @property
    def print_description(self):
        return False

@yaml.register_class
class Format(Executable):
    yaml_tag = "!format"
    def __init__(self, pattern, *args, **kwargs):
        self.pattern    = pattern
        self.args       = args or kwargs or None
    def __call__(self, scope, **_):
        args    = execute(self.args, scope)
        if is_sequence(self.args):
            return str(execute(self.pattern, scope)) % args
        elif is_mapping(self.args):
            scope = Scope(args, scope)
        return execute(self.pattern, scope, str_is_python=False)
    @property
    def print_description(self):
        return False

@yaml.register_class
class Python(Executable):
    yaml_tag = "!python"
    def __init__(self, code):
        self.code = code
    def __call__(self, scope, **_):
        return exec(self.code, {}, scope)  #pylint: disable=exec-used

@yaml.register_class
class Assert(Executable):
    yaml_tag = "!assert"
    def __init__(self, predicate, message=None):
        self.predicate = predicate
        self.message = message
    def __call__(self, scope, **_):
        if not execute(self.predicate, scope):
            if self.message:
                raise BuildError(execute(self.message, scope))
            raise BuildError(f"assertion failed: {self.predicate}")
        print_info(scope, "ok")
    @property
    def detail(self):
        return self.predicate

@yaml.register_class
class Info(Executable):
    yaml_tag = "!info"
    def __init__(self, message):
        self.message = message
    def __call__(self, scope, **_):
        Comment(self.message).print(scope)
    @property
    def print_description(self):
        return False
    @property
    def print_indented(self):
        return False

@yaml.register_class
class Important(Executable):
    yaml_tag = "!important"
    def __init__(self, message):
        self.message = message
    def __call__(self, scope, **_):
        ImportantComment(self.message).print(scope)
    @property
    def print_description(self):
        return False
    @property
    def print_indented(self):
        return False

@yaml.register_class
class Warning(Executable):  # pylint: disable=redefined-builtin
    yaml_tag = "!warning"
    def __init__(self, message):
        self.message = message
    def __call__(self, scope, **_):
        return logging.warning(self.message.format_map(scope))
    @property
    def print_description(self):
        return False
    @property
    def print_indented(self):
        return False

@yaml.register_class
class Error(Executable):
    yaml_tag = "!error"
    def __init__(self, message):
        self.message = message
    def __call__(self, scope, **_):
        return logging.error(self.message.format_map(scope))
    @property
    def print_description(self):
        return False
    @property
    def print_indented(self):
        return False

@yaml.register_class
class Debug(Executable):
    yaml_tag = "!debug"
    def __init__(self, message):
        self.message = message
    def __call__(self, scope, **_):
        return logging.debug(self.message.format_map(scope))
    @property
    def print_description(self):
        return False
    @property
    def print_indented(self):
        return False

@yaml.register_class
class FileBasename(Executable):
    yaml_tag = "!file.basename"
    def __init__(self, file):
        self.file = file
    def __call__(self, scope, **_):
        return os.path.basename(execute(self.file, scope))
    @property
    def detail(self):
        return self.file

@yaml.register_class
class FileDirname(Executable):
    yaml_tag = "!file.dirname"
    def __init__(self, file):
        self.file = file
    def __call__(self, scope, **_):
        return os.path.dirname(execute(self.file, scope))
    @property
    def detail(self):
        return self.file

@yaml.register_class
class FileExtension(Executable):
    yaml_tag = "!file.extension"
    def __init__(self, file):
        self.file = file
    def __call__(self, scope, **_):
        return os.path.splitext(execute(self.file, scope))[1]
    @property
    def detail(self):
        return self.file

@yaml.register_class
class FileNoExtension(Executable):
    yaml_tag = "!file.noextension"
    def __init__(self, file):
        self.file = file
    def __call__(self, scope, **_):
        return os.path.splitext(execute(self.file, scope))[0]
    @property
    def detail(self):
        return self.file

@yaml.register_class
class FileExists(Executable):
    yaml_tag = "!file.exists"
    def __init__(self, file):
        self.file = file
    def __call__(self, scope, **_):
        return os.path.isfile(execute(self.file, scope))
    @property
    def detail(self):
        return self.file

@yaml.register_class
class FileRemove(Executable):
    yaml_tag = "!file.remove"
    def __init__(self, file, must_exist=False):
        self.file       = file
        self.must_exist = must_exist
    def __call__(self, scope, **_):
        file = execute(self.file, scope)
        if os.path.isfile(file):
            print_info(scope, f" - found file: {file}")
            os.remove(file)
            return True
        print_info(scope, f" - file not found: {file}")
        if self.must_exist:
            raise BuildError("cannot remove non-existent file")
        return False
    @property
    def detail(self):
        return self.file

@yaml.register_class
class FileCopy(Executable):
    yaml_tag = "!file.copy"
    def __init__(self, **arguments):
        self.from_      = arguments["from"]
        self.to_        = arguments["to"]
        self.must_exist = arguments.get("must_exist", True)
        self.make_dirs  = arguments.get("make_dirs", True)
        self.override   = arguments.get("override", True)
    def __call__(self, scope, **_):
        source      = execute(self.from_, scope)
        destination = execute(self.to_, scope)
        if os.path.isfile(source):
            print_info(scope, f"found source: {source}")
            if self.make_dirs:
                destination_dir = os.path.dirname(destination)
                if destination_dir and not os.path.isdir(destination_dir):
                    print_info(scope, f"creating destination directory: {destination_dir}")
                    os.makedirs(destination_dir)
            try:
                print_info(scope, f"destination: {destination}")
                shutil.copyfile(source, destination)
                return True
            except Exception as e:
                raise BuildError("error during copying") from e
        elif self.must_exist:
            raise BuildError(f"cannot copy non-existent file: {source}")
        return False
    @property
    def detail(self):
        return self.from_

@yaml.register_class
class YamlRead(Executable):
    yaml_tag = "!yaml.read"
    def __init__(self, file):
        self.file = file
    def __call__(self, scope, **_):
        with open(execute(self.file, scope), "rt", encoding="iso-8859-1") as file:
            parser = ruamel.yaml.YAML()
            return parser.load(file.read())
    @property
    def detail(self):
        return self.file

@yaml.register_class
class XmlRead(Executable):
    yaml_tag = "!xml.read"
    def __init__(
        self
        , file
        , path=None
        , match_one=False
        , extract_text=False
        , extract_attribute=None
        , when_no_match=None
        , when_no_file=None
    ):
        self.file = file
        self.path = path
        self.match_one = match_one
        self.extract_text = extract_text
        self.extract_attribute = extract_attribute
        self.when_no_file = when_no_file
        self.when_no_match = when_no_match
        if extract_text and extract_attribute:
            raise BuildError("can only pass either 'extract-text' or 'extract-attribute'")
    def __call__(self, scope, **_):
        file        = execute(self.file, scope)
        path        = execute(self.path, scope)
        match_one   = execute(self.match_one, scope)
        if not os.path.isfile(file):
            if self.when_no_file is not None:
                return execute(self.when_no_file, scope)
            raise BuildError(f"cannot read non-existent xml file '{file}'")
        try:
            result = xml.parse(file)
            if path is not None:
                result = result.findall(path)
                if not result and self.when_no_match:
                    result = execute(self.when_no_match, scope)
                elif match_one:
                    if len(result) != 1:
                        raise BuildError(
                            f"more than one xml element ({len(result)}) matched path '{path}' in file '{file}'"
                        )
                    result = result[0]
        except xml.ParseError as e:
            raise BuildError(f"could not parse xml file '{file}'") from e
        if self.extract_text:
            if match_one:
                result = result.text
            else:
                result = [node.text for node in result]
        elif self.extract_attribute:
            if match_one:
                result = result.attrib[self.extract_attribute]
            else:
                 result = [node.attrib[self.extract_attribute] for node in result]

        return result
    @property
    def detail(self):
        return self.file

@yaml.register_class
class YamlWrite(Executable):
    yaml_tag = "!yaml.write"
    def __init__(self, file, data):
        self.file = file
        self.data = data
    def __call__(self, scope, **_):
        data = execute(self.data, scope)
        with open(execute(self.file, scope), "wt", encoding="iso-8859-1") as file:
            parser = ruamel.yaml.YAML()
            parser.dump(data, file)
    @property
    def detail(self):
        return self.file

@yaml.register_class
class Call(Executable):
    yaml_tag = "!call"
    def __init__(   # pylint: disable=redefined-builtin, redefined-outer-name
        self
        , program=None              # Path to executable
        , cwd=None                  # Directory from where to execute the program
        , args=None                 # List or dictionary of arguments
        , check=True                # Whether to check the return value of the application to be zero
        , print=True                # Whether to print stdout and stderr of the execution
        , when_success=None         # What to execute, when the return value is zero
        , when_fail=None            # What to execute, when the return value is non-zerp (set "check=False" first)
        , shell=False               # Whether to use an explicit shell to dispatch the command
        , log_file=None             # Path to log file, if one shall be created
        , separate_process=False    # Whether to execute as process (perhaps multi-processor), instead of subprocess (same process)
    ):
        self.program = program
        self.cwd = cwd
        self.args = args
        self.check = check
        self.print = print
        self.log_file = log_file
        self.when_success = when_success
        self.when_fail = when_fail
        self.shell = shell
        self.separate_process = separate_process
    @property
    def detail(self):
        return self.program
    def __call__(self, scope, **_):
        command    = [execute(self.program, scope)]
        arguments  = execute(self.args, scope)
        if not is_sequence(arguments):
            arguments = [arguments]
        for argument_or_list in arguments:
            if not is_sequence(argument_or_list):
                argument_or_list = [argument_or_list]
            for argument in argument_or_list:
                if is_mapping(argument):
                    for parameter, value in argument.items():
                        argument = f"--{parameter}"
                        if value is None:
                            value = []
                        elif not is_sequence(value):
                            value = [value]
                        for val in value:
                            val = str(val)
                            argument += f' "{val}"' if val.count(" ") else f" {val}"
                        command.append(argument)
                elif argument is not None:
                    argument = str(argument)
                    if argument:
                        argument = f'"{argument}"' if argument.count(" ") else f" {argument}"
                        command.append(argument)

        # Prepare options to pass to shell.run
        options = {}
        options["cwd"] = os.path.abspath(execute(self.cwd, scope) or os.curdir)
        options["shell"] = True if execute(self.shell, scope) else False
        options["print_stdout"] = functools.partial(print_info, scope) if execute(self.print, scope) else None
        options["print_stderr"] = options["print_stdout"]
        options["separate_process"] = True if execute(self.separate_process, scope) else False
        if log_file := execute(self.log_file, scope):
            options["print_file"] = open(log_file, "wb")

        # Print command
        print_info(scope, "command: ")
        prefix = "  "
        for part in command:
            print_info(scope, prefix + part.strip())
            prefix = "    "
        print_info(scope, f"working directory: {options['cwd']}")

        # Call the program
        with stream_modifier.Prefix("  ", first_line=False):
            with stream_modifier.EliminateLogging():
                result = shell.run(
                    " ".join(command)
                    , print_prefix="executing... (process-id: {process.pid})"
                    , **options
                )

        # Give Status
        print_info(scope, f"finished, took {result.duration:.2f}s")
        print_info(scope, f"return code: {result.returncode}")

        # Check return code?
        if execute(self.check, scope):
            if result.returncode != 0:
                raise BuildError(
                    f"{self.description} finished with non-zero status code '{result.returncode}'"
                )
        return result.returncode

@yaml.register_class
class For(Executable):
    yaml_tag = "!for"
    def __init__(self, **arguments):
        self.for_ = arguments.get("for")
        self.in_ = arguments.get("in")
        self.do_ = arguments.get("do")
        self.flatten = arguments.get("flatten", False)
    def __call__(self, scope, **args):
        variable = execute(self.for_, scope, str_is_python=False)
        iterable = execute(self.in_, scope)
        result = []
        for element in iterable:
            if is_sequence(variable):
                for sub_variable, part in zip(variable, element):
                    scope[sub_variable] = part
            else:
                scope[variable] = element
            if is_sequence(self.do_):
                results = []
                for statement in self.do_:
                    results.append(execute(
                        statement
                        , scope
                        , dict_is_assignment=args["dict_is_assignment"]
                        , print_description=args["print_description"]
                    ))
                if self.flatten:
                    result.extend(results)
                else:
                    result.append(results)
            else:
                result.append(execute(
                    self.do_
                    , scope
                    , dict_is_assignment=args["dict_is_assignment"]
                    , print_description=args["print_description"]
                ))
        return result

@yaml.register_class
class If(Executable):
    yaml_tag = "!if"
    def __init__(self, **arguments):
        self.if_    = arguments.get("when") or arguments.get("condition") or arguments["if"]
        self.then_  = arguments.get("then", None)
        self.else_  = arguments.get("else", None)
        assert self.then_ is not None or self.else_ is not None
    def __call__(self, scope, **args):
        if execute(self.if_, scope):
            what_to_execute = self.then_
        else:
            what_to_execute = self.else_
        return execute(
            what_to_execute
            , scope
            , dict_is_assignment=args["dict_is_assignment"]
            , print_description=args["print_description"]
        )
    @property
    def print_description(self):
        return False
    @property
    def print_indented(self):
        return False

@yaml.register_class
class Not(Executable):
    yaml_tag = "!not"
    def __init__(self, value):
        self.value  = value
    def __call__(self, scope, **_):
        return not execute(self.value, scope)
    @property
    def print_description(self):
        return False
    @property
    def print_indened(self):
        return False

@yaml.register_class
class Try(Executable):
    yaml_tag = "!try"
    def __init__(self, **arguments):
        self.try_       = arguments.get("code") or arguments.get("what") or arguments["try"]
        self.except_    = arguments.get("except", None)
        self.as_        = arguments.get("as", "exception")
        self.finally_   = arguments.get("finally", None)
        assert self.try_ is not None
    def __call__(self, scope, **args):
        try:
            result = execute(self.try_, scope, dict_is_assignment=args["dict_is_assignment"])
        except Exception as e:  # pylint: disable=broad-exception-caught
            if self.except_ is not None:
                found = False
                for exception_type in ensure_sequence(self.except_):
                    if isinstance(e, exception_type):
                        found = True
                if not found:
                    raise
            result = execute(
                self.try_
                , Scope(
                    {execute(self.as_, scope, str_is_python=False): e}
                    , scope
                )
                , dict_is_assignment=args["dict_is_assignment"]
                , print_description=args["print_description"]
            )
        finally:
            if self.finally_ is not None:
                result = execute(self.finally_, scope)
        return result

@yaml.register_class
class WithRessource(Executable):
    yaml_tag = "!with.ressource"
    def __init__(self, ressource, do, count=1):
        self.ressource  = ressource
        self.do         = do
        self.count      = count
    def __call__(self, scope, **args):
        ressource   = execute(self.ressource, scope)
        count       = execute(self.count, scope)
        result      = None
        assert isinstance(ressource, threading.Semaphore)
        ressource.acquire(count)
        try:
            result = execute(
                self.do
                , scope
                , dict_is_assignment=args["dict_is_assignment"]
                , print_description=args["print_description"]
            )
        except Exception:
            ressource.release(count)
            raise
        ressource.release(count)
        return result
    @property
    def detail(self):
        return f"{self.ressource}:{self.count}"

@yaml.register_class
class Set(Executable):
    yaml_tag = "!set"
    def __init__(self, **assignments):
        self.assignments = assignments
    def __call__(self, scope, **_):
        for variable, value in self.assignments.items():
            value       = execute(value, scope)
            variable    = execute(variable, scope, str_is_python=False)
            print_info(scope, f"variable [{variable}] = {intelligent_repr(value)}")
            scope[variable] = value
    @property
    def print_indented(self):
        return False

class ReturnException(Exception):
    def __init__(self, value) -> None:
        self.value = value

@yaml.register_class
class Return(Executable):
    yaml_tag = "!return"
    class NOT_SET:
        pass
    def __init__(self, value="__notset__", **dictionary):
        self.value = dictionary if value == "__notset__" else value
    def __call__(self, scope, **_):
        raise ReturnException(execute(self.value, scope))
    @property
    def detail(self):
        return self.value

@yaml.register_class
class Raise(Executable):
    yaml_tag = "!raise"
    def __init__(self, exception=None):
        self.exception = exception
    def __call__(self, scope, **_):
        if self.exception is None:
            raise MissingExceptionInRaise()
        raise execute(self.exception, scope)
    @property
    def detail(self):
        return self.exception

@yaml.register_class
class Function(Executable):
    yaml_tag = "!function"
    def __init__(self, body, arguments=None):
        self.body = body
        self.arguments = arguments or []
    def __call__(self, scope, **_):
        def result(*args, **kwargs):
            function_scope = Scope({}, scope)
            for argument in self.arguments:
                argument = execute(argument, scope, str_is_python=False)
                if argument in kwargs:
                    function_scope[argument] = kwargs[argument]
                    kwargs.pop(argument)
                else:
                    function_scope[argument] = args[0]
                    args = args[1:]
            if not self.arguments and args:
                function_scope["value"] = args[0]
            function_scope["args"] = args
            function_scope["kwargs"] = kwargs
            try:
                return execute(self.body, function_scope)
            except ReturnException as return_exception:
                return return_exception.value
        return result

def load_config(path):
    result = yaml.load(open(path, "rt", encoding="iso-8859-1").read())
    return result

class Scope:
    def __init__(self, variables=None, base=None):
        self.__variables = variables or {}
        self.__base = base
    def __getitem__(self, key):
        if key == "_scope":
            return self
        if key in self.__variables:
            return self.__variables[key]
        if isinstance(key, int):
            return self.__variables[self.__variables.keys()[key]]
        if self.__base:
            return self.__base[key]
        raise AttributeError(f"unknown identifier '{key}'")
    def __delattr__(self, name: str) -> None:
        del self.__variables[name]
    def __setitem__(self, key, value):
        self.__variables[key] = value
    def __contains__(self, key):
        if key == "_scope":
            return True
        if key in self.__variables:
            return True
        if self.__base:
            return key in self.__base
        return False
    def __getattr__(self, key):
        if key == "_scope":
            return self
        if key in vars(Scope):
            return getattr(self, key)
        return self[key]
    def __len__(self):
        return len(self.__variables)
    def get(self, key, fallback=None):
        if key in self.__variables:
            return self.__variables[key]
        if self.__base:
            return self.__base.get(key, fallback)
        return fallback
    def to_dict(self):
        result = {}
        if self.__base:
            result.update(self.__base.items())
        result.update(self.__variables)
        return result

def execute(
    value
    , scope
    , str_is_python=None
    , dict_is_assignment=False
    , deep=True
    , print_description=False
):
    result = value

    # Print comments before the object
    print_pre_comments(scope, getattr(value, "comment", None))

    if isinstance(value, Executable):
        if print_description and value.print_description:
            print_info(scope, value.description)
        with (
            stream_modifier.Prefix("  ", scope_optional=True)
            if value.print_indented
            else stream_modifier.Noop()
        ):
            result = value(
                scope
                , str_is_python=str_is_python
                , dict_is_assignment=dict_is_assignment
                , print_description=print_description
                , deep=deep
            )
    elif isinstance(value, str):
        if isinstance(value, DoubleQuotedScalarString):
            str_is_python = False
        elif isinstance(value, SingleQuotedScalarString):
            str_is_python = True
        elif str_is_python is None:
            str_is_python = True
        if str_is_python is False:
            result = "f"+repr(result)
        try:
            result = eval(result, {}, scope)  # pylint: disable=eval-used
        except (
            TypeError
            , ValueError
            , SyntaxError
            , ArithmeticError
            , AttributeError
            , NameError
        ) as error:
            raise BuildError(f"error executing expression: {result}") from error
    elif is_mapping(value) and deep:
        result = HashableDict()
        for key, val in value.items():
            # Print comments before the item
            if isinstance(value, comments.CommentedBase):
                print_pre_comments(scope, value.ca.items.get(key))
            key = execute(key, scope, str_is_python=False)
            val = execute(
                val
                , scope
                , str_is_python=str_is_python
                , print_description=print_description
            )
            if isinstance(value, comments.CommentedBase):
                print_post_comments(scope, value.ca.items.get(key))
            if dict_is_assignment:
                print_info(scope, f"variable [{key}] = {intelligent_repr(val)}")
                scope[key] = val
            result[key] = val
        if isinstance(value, comments.CommentedBase):
            print_post_comments(scope, [[],value.ca.end])
    elif is_sequence(value) and deep:
        result = []
        for idx, val in enumerate(value):
            # Print comments before the item
            if isinstance(value, comments.CommentedBase):
                print_pre_comments(scope, value.ca.items.get(idx))
            result.append(execute(
                val
                , scope
                , str_is_python=str_is_python
                , dict_is_assignment=dict_is_assignment
                , print_description=print_description
            ))
            # Print comments after the item
            if isinstance(value, comments.CommentedBase):
                print_post_comments(scope, value.ca.items.get(idx))
        if isinstance(value, comments.CommentedBase):
            print_post_comments(scope, [[],value.ca.end])
    elif is_tuple(value) and deep:
        result = (execute(val, scope, str_is_python=str_is_python) for val in value)

    # Print comments after the item
    print_post_comments(scope, getattr(value, "comment", None))

    return result
